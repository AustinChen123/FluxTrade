//! Explicit raw-retention transition, not DB publication or source-manifest deletion.
//! Caller MUST exclusively own the entire trusted staging root/daily family (all
//! 24 collectors, fetch, assemble and cleanup) for the whole call. Not enabled by
//! any production caller. Tombstones are a second defense, not a concurrent lock.
//! No DB retention state is updated; retained manifests remain publication evidence.
use super::{
    checkpoint::{Identity, Manifest},
    compressed_page, daily,
    directory::{Directory, Phase},
    handoff::sha,
    kline_evidence, mvp,
    store::Store,
    Window,
};
use anyhow::{ensure, Result};
use rustix::fd::AsFd;
use serde::{Deserialize, Serialize};

pub(crate) const MARKER: &str = "cleanup-marker.json";
pub(crate) const TOMBSTONE: &str = "raw-retired.json";
use super::kline_evidence::FILE as KLINE;
use super::store::MANIFEST;
const LIMIT: u64 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hour {
    manifest_sha256: String,
    page_count: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    schema: u8,
    identity: kline_evidence::Identity,
    content_sha256: String,
    handoff_sha256: String,
    hours: Vec<Hour>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    reason: &'static str,
    hours: usize,
    referenced_pages: u64,
    content_sha256: String,
    handoff_sha256: String,
}

fn hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn hourly(id: &kline_evidence::Identity, hour: usize) -> Result<Identity> {
    let start = id.start_ms() + hour as i64 * mvp::HOUR;
    Identity::new(
        daily::hourly_staging_job_id(id.job_id(), start)?,
        Window::new(start, start + mvp::HOUR)?,
        mvp::GRID.into(),
        mvp::config_hash(),
    )
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Marker,
    Tombstone(usize),
    Page(usize, u64),
    Kline,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Write(Phase),
    BeforeUnlink,
    Unlinked,
    BeforeSync,
    Synced,
}

/// Hash arguments attest external authorization; this does not inspect a DB.
pub fn run(
    root: &impl AsFd,
    job_id: &str,
    day: Window,
    content: &str,
    handoff: Option<&str>,
) -> Result<Report> {
    run_with(root, job_id, day, content, handoff, |_, _| Ok(()))
}
fn run_with(
    root: &impl AsFd,
    job_id: &str,
    day: Window,
    content: &str,
    handoff: Option<&str>,
    mut hook: impl FnMut(Target, Step) -> Result<()>,
) -> Result<Report> {
    ensure!(
        hex(content) && handoff.is_none_or(hex),
        "invalid expected digest"
    );
    let identity = kline_evidence::Identity::new(job_id.into(), day)?;
    let directory = Directory::open_existing(root, job_id)?;
    let marker: Marker = if directory.exists(MARKER)? {
        serde_json::from_slice(&directory.read(MARKER, LIMIT)?)?
    } else {
        let expected = handoff.ok_or_else(|| anyhow::anyhow!("initial handoff digest required"))?;
        let evidence = kline_evidence::recover(root, &identity)?;
        let mut inputs = Vec::with_capacity(24);
        let mut hours = Vec::with_capacity(24);
        for hour in 0..24 {
            let id = hourly(&identity, hour)?;
            let dir = Directory::open_existing(root, id.job_id())?;
            let raw = dir.read(MANIFEST, mvp::MANIFEST)?;
            let store = Store::open(
                root,
                id,
                compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW)?,
                mvp::MANIFEST,
            )?;
            let pair = store.recover()?;
            ensure!(
                raw == serde_json::to_vec(&pair.0)?,
                "noncanonical retained manifest"
            );
            hours.push(Hour {
                manifest_sha256: sha(&raw),
                page_count: pair.0.entries().len() as u64,
            });
            inputs.push(pair);
        }
        let mut bytes = daily::assemble(
            job_id,
            day,
            mvp::config_hash(),
            &inputs,
            evidence.raw(),
            evidence.observed_at_ms(),
        )?
        .to_bytes()?;
        let wire: serde_json::Value = serde_json::from_slice(&bytes)?;
        ensure!(
            wire["content_sha256"].as_str() == Some(content),
            "content mismatch"
        );
        bytes.push(b'\n');
        ensure!(sha(&bytes) == expected, "handoff mismatch");
        let intended = Marker {
            schema: 1,
            identity: identity.clone(),
            content_sha256: content.into(),
            handoff_sha256: expected.into(),
            hours,
        };
        let encoded = serde_json::to_vec(&intended)?;
        durable(&directory, MARKER, &encoded, Target::Marker, &mut hook)?;
        intended
    };
    ensure!(
        marker.schema == 1
            && marker.identity == identity
            && marker.content_sha256 == content
            && hex(&marker.handoff_sha256)
            && handoff.is_none_or(|h| h == marker.handoff_sha256)
            && marker.hours.len() == 24,
        "cleanup marker mismatch"
    );
    // Confirm even an existing visible marker before any tombstone or unlink.
    confirm(&directory, Target::Marker, &mut hook)?;
    let mut retained = Vec::with_capacity(24);
    for (hour, expected) in marker.hours.iter().enumerate() {
        let id = hourly(&identity, hour)?;
        let dir = Directory::open_existing(root, id.job_id())?;
        let raw = dir.read(MANIFEST, mvp::MANIFEST)?;
        let manifest: Manifest = serde_json::from_slice(&raw)?;
        ensure!(
            hex(&expected.manifest_sha256)
                && sha(&raw) == expected.manifest_sha256
                && manifest.identity() == &id
                && expected.page_count > 0
                && manifest.entries().len() as u64 == expected.page_count,
            "retained manifest mismatch"
        );
        for (sequence, entry) in manifest.entries().iter().enumerate() {
            ensure!(
                entry.sequence() == sequence as u64,
                "retained page order mismatch"
            );
        }
        retained.push((dir, manifest));
    }
    let marker_sha = sha(&serde_json::to_vec(&marker)?);
    // Finish ALL durable tombstones before the first deletion.
    for (hour, (dir, _)) in retained.iter().enumerate() {
        let bytes = serde_json::to_vec(
            &serde_json::json!({"schema":1,"marker_sha256":marker_sha,"hour":hour}),
        )?;
        if dir.exists(TOMBSTONE)? {
            ensure!(dir.read(TOMBSTONE, LIMIT)? == bytes, "tombstone mismatch");
            confirm(dir, Target::Tombstone(hour), &mut hook)?;
        } else {
            durable(dir, TOMBSTONE, &bytes, Target::Tombstone(hour), &mut hook)?;
        }
    }
    for (hour, (dir, manifest)) in retained.iter().enumerate() {
        for entry in manifest.entries() {
            remove(
                dir,
                &entry.name(),
                Target::Page(hour, entry.sequence()),
                &mut hook,
            )?;
        }
    }
    remove(&directory, KLINE, Target::Kline, &mut hook)?;
    Ok(Report {
        reason: "COMPLETE",
        hours: 24,
        referenced_pages: marker.hours.iter().map(|h| h.page_count).sum(),
        content_sha256: content.into(),
        handoff_sha256: marker.handoff_sha256,
    })
}
fn confirm(
    dir: &Directory,
    target: Target,
    hook: &mut impl FnMut(Target, Step) -> Result<()>,
) -> Result<()> {
    hook(target, Step::BeforeSync)?;
    dir.sync()?;
    hook(target, Step::Synced)
}
fn durable(
    dir: &Directory,
    name: &str,
    bytes: &[u8],
    target: Target,
    hook: &mut impl FnMut(Target, Step) -> Result<()>,
) -> Result<()> {
    ensure!(bytes.len() as u64 <= LIMIT, "cleanup metadata limit");
    if let Err(error) = dir.replace_with(name, bytes, |phase| hook(target, Step::Write(phase))) {
        if dir.read(name, LIMIT).ok().as_deref() != Some(bytes) {
            return Err(error);
        }
        confirm(dir, target, hook)?;
    }
    Ok(())
}
fn remove(
    dir: &Directory,
    name: &str,
    target: Target,
    hook: &mut impl FnMut(Target, Step) -> Result<()>,
) -> Result<()> {
    hook(target, Step::BeforeUnlink)?;
    dir.unlink_if_present(name)?;
    hook(target, Step::Unlinked)?;
    confirm(dir, target, hook)
}

#[cfg(test)]
mod tests;
