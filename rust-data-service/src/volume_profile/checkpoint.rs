//! Immutable checkpoint descriptions; no filesystem, compression or publication.
//! Raw-byte digests/lengths; store owns decompression and job-scoped loading.
use anyhow::{ensure, Result};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};

use super::binance_spot::{AggregateTrade, Pages, Progress, Request, PRODUCT_ID};
use super::{Window, ALGORITHM_VERSION};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    schema: u32,
    algorithm: String,
    job_id: String,
    product_id: String,
    start_ms: i64,
    end_ms: i64,
    grid_id: String,
    config_sha256: [u8; 32],
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

impl Identity {
    pub fn new(
        job_id: String,
        window: Window,
        grid_id: String,
        config_sha256: [u8; 32],
    ) -> Result<Self> {
        ensure!(safe_id(&job_id) && safe_id(&grid_id), "unsafe identity");
        Ok(Self {
            schema: 1,
            algorithm: ALGORITHM_VERSION.into(),
            job_id,
            product_id: PRODUCT_ID.into(),
            start_ms: window.start_ms(),
            end_ms: window.end_ms(),
            grid_id,
            config_sha256,
        })
    }
    pub fn job_id(&self) -> &str {
        &self.job_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointProgress {
    Next(u64),
    EmptyPage,
    WindowEnd,
}

impl TryFrom<Progress> for CheckpointProgress {
    type Error = anyhow::Error;
    fn try_from(value: Progress) -> Result<Self> {
        Ok(match value {
            Progress::Continue(Request::Next { from_id }) => Self::Next(from_id),
            Progress::EmptyPage => Self::EmptyPage,
            Progress::WindowEnd => Self::WindowEnd,
            _ => anyhow::bail!("invalid post-page progress"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageEntry {
    sequence: u64,
    byte_length: u64,
    sha256: [u8; 32],
    progress: CheckpointProgress,
}

impl PageEntry {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
    pub(crate) fn byte_length(&self) -> u64 {
        self.byte_length
    }
    pub(crate) fn progress(&self) -> &CheckpointProgress {
        &self.progress
    }
    pub fn new(sequence: u64, body: &[u8], progress: CheckpointProgress) -> Self {
        Self {
            sequence,
            byte_length: body.len() as u64,
            sha256: hash(body),
            progress,
        }
    }
    /// Opaque store-owned name, independent of untrusted source/manifest strings.
    pub fn name(&self) -> String {
        format!("page-{:020}.bin", self.sequence)
    }
}

fn hash(body: &[u8]) -> [u8; 32] {
    digest(&SHA256, body).as_ref().try_into().unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    identity: Identity,
    entries: Vec<PageEntry>,
}

impl Manifest {
    pub(crate) fn identity(&self) -> &Identity {
        &self.identity
    }
    pub(crate) fn entries(&self) -> &[PageEntry] {
        &self.entries
    }
    pub fn new(identity: Identity, entries: Vec<PageEntry>) -> Self {
        Self { identity, entries }
    }
}

pub struct Recovered {
    pages: Pages,
    trades: Vec<AggregateTrade>,
    manifest_sha256: [u8; 32],
}

impl Recovered {
    pub fn pages(&self) -> &Pages {
        &self.pages
    }
    pub fn trades(&self) -> &[AggregateTrade] {
        &self.trades
    }
    pub(crate) fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }
}

/// Caller supplies authoritative expected identity, not one copied from disk.
/// Only referenced pages are loaded; no partial recovery escapes on failure.
pub fn recover(
    manifest: &Manifest,
    expected: &Identity,
    mut load: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<Recovered> {
    let identity = &manifest.identity;
    ensure!(identity == expected, "checkpoint identity mismatch");
    ensure!(
        identity.schema == 1
            && identity.algorithm == ALGORITHM_VERSION
            && identity.product_id == PRODUCT_ID,
        "unsupported checkpoint identity"
    );
    ensure!(
        safe_id(&identity.job_id) && safe_id(&identity.grid_id),
        "unsafe identity"
    );
    let mut pages = Pages::new(Window::new(identity.start_ms, identity.end_ms)?);
    let mut trades = Vec::new();
    for (index, entry) in manifest.entries.iter().enumerate() {
        ensure!(entry.sequence == index as u64, "page sequence mismatch");
        pages.request()?;
        let body = load(&entry.name())?;
        ensure!(
            body.len() as u64 == entry.byte_length && hash(&body) == entry.sha256,
            "page length/digest mismatch"
        );
        let (accepted, progress) = pages.accept(&body)?;
        ensure!(
            CheckpointProgress::try_from(progress)? == entry.progress,
            "page progress mismatch"
        );
        trades.extend(accepted);
    }
    Ok(Recovered {
        pages,
        trades,
        manifest_sha256: hash(&serde_json::to_vec(manifest)?),
    })
}

#[cfg(test)]
mod tests;
