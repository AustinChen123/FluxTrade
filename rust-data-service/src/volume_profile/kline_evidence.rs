//! Immutable source pair for one daily job. Trusted directories, single worker;
//! caller owns official provenance and observed <= wall-clock validation.
//! No network, cleanup, or hardware power-loss guarantee.
use anyhow::{ensure, Result};
use rustix::fd::AsFd;
use serde::{Deserialize, Serialize};

use super::{
    daily,
    directory::{Directory, Phase},
    handoff::sha,
    mvp, Window,
};

const FILE: &str = "daily-kline-evidence.json";
use super::binance_kline::{self, RAW_LIMIT};
const ARTIFACT_LIMIT: u64 = (RAW_LIMIT * 6 + 4096) as u64;
const DAY: i64 = 24 * mvp::HOUR;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    schema_version: u8,
    job_id: String,
    product_id: String,
    start_ms: i64,
    end_ms: i64,
    interval: String,
    config_sha256: String,
}

impl Identity {
    pub fn new(job_id: String, day: Window) -> Result<Self> {
        daily::hourly_staging_job_id(&job_id, day.start_ms())?;
        ensure!(
            day.start_ms() % DAY == 0 && day.start_ms().checked_add(DAY) == Some(day.end_ms()),
            "invalid daily identity"
        );
        ensure!(
            day.end_ms() <= 253_402_300_799_999,
            "invalid daily identity"
        );
        Ok(Self {
            schema_version: 1,
            job_id,
            product_id: "BINANCE:BTCUSDT-SPOT".into(),
            start_ms: day.start_ms(),
            end_ms: day.end_ms(),
            interval: "1d".into(),
            config_sha256: mvp::config_hex(),
        })
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            *self
                == Self::new(
                    self.job_id.clone(),
                    Window::new(self.start_ms, self.end_ms)?
                )?,
            "evidence identity mismatch"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    identity: Identity,
    raw: String,
    response_sha256: String,
    response_bytes: u64,
    observed_at_ms: i64,
}

impl Evidence {
    fn new(identity: Identity, raw: &[u8], observed_at_ms: i64) -> Result<Self> {
        identity.validate()?;
        ensure!(raw.len() <= RAW_LIMIT, "kline response limit");
        ensure!(
            observed_at_ms >= identity.end_ms && observed_at_ms <= 253_402_300_799_999,
            "invalid observation"
        );
        let raw = std::str::from_utf8(raw)?;
        binance_kline::parse(
            raw.as_bytes(),
            Window::new(identity.start_ms, identity.end_ms)?,
        )?;
        Ok(Self {
            identity,
            raw: raw.into(),
            response_sha256: sha(raw.as_bytes()),
            response_bytes: raw.len() as u64,
            observed_at_ms,
        })
    }
    pub fn raw(&self) -> &[u8] {
        self.raw.as_bytes()
    }
    pub fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
}

fn load(directory: &Directory, expected: &Identity) -> Result<Evidence> {
    let evidence: Evidence = serde_json::from_slice(&directory.read(FILE, ARTIFACT_LIMIT)?)?;
    ensure!(&evidence.identity == expected, "evidence identity mismatch");
    ensure!(
        evidence == Evidence::new(expected.clone(), evidence.raw(), evidence.observed_at_ms)?,
        "evidence integrity failure"
    );
    Ok(evidence)
}

/// Recovery never creates missing directories/files, and confirms job fsync.
pub fn recover(root: &impl AsFd, expected: &Identity) -> Result<Evidence> {
    expected.validate()?;
    let directory = Directory::open_existing(root, &expected.job_id)?;
    let evidence = load(&directory, expected)?;
    directory.sync()?;
    Ok(evidence)
}

/// Exact raw replay retains the stored first observation, even if caller retries later.
pub fn persist(
    root: &impl AsFd,
    identity: Identity,
    raw: &[u8],
    observed_at_ms: i64,
) -> Result<Evidence> {
    let evidence = Evidence::new(identity, raw, observed_at_ms)?;
    let directory = Directory::open(root, &evidence.identity.job_id)?;
    persist_with(&directory, evidence, |_| Ok(()))
}

fn persist_with(
    directory: &Directory,
    intended: Evidence,
    mut hook: impl FnMut(Phase) -> Result<()>,
) -> Result<Evidence> {
    match load(directory, &intended.identity) {
        Ok(existing) => {
            ensure!(existing.raw == intended.raw, "immutable evidence conflict");
            directory.sync()?;
            return Ok(existing);
        }
        Err(error)
            if error.downcast_ref::<rustix::io::Errno>() == Some(&rustix::io::Errno::NOENT) => {}
        Err(error) => return Err(error),
    }
    let bytes = serde_json::to_vec(&intended)?;
    ensure!(
        bytes.len() as u64 <= ARTIFACT_LIMIT,
        "evidence artifact limit"
    );
    if let Err(error) = directory.replace_with(FILE, &bytes, &mut hook) {
        // Reread alone is not durability: confirm fsync after matching ACK-unknown.
        if load(directory, &intended.identity).ok().as_ref() != Some(&intended) {
            return Err(error);
        }
        hook(Phase::BeforeDirectorySync)?;
        directory.sync()?;
        hook(Phase::DirectorySynced)?;
    }
    Ok(intended)
}

#[cfg(test)]
mod tests;
