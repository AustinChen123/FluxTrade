//! Deterministic assembly from Store-recovered hourly inputs, with no I/O.
//! Callers must supply actual Store recovery results: this does not re-read raw
//! page bytes or authenticate manifests. Terminal recovery, identity, trade
//! continuity and exact official kline reconciliation bound the resulting claim.
use anyhow::{ensure, Result};
use ring::digest::{digest, SHA256};
use rust_decimal::Decimal;

use super::binance_spot::PRODUCT_ID;
use super::checkpoint::{CheckpointProgress, Identity, Manifest, Recovered};
use super::handoff::{sha, Handoff, HourEvidence, Reconciliation, GRID_ID};
use super::{Grid, VolumeProfile, Window};

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

/// v1 mapping: SHA256(domain || safe ASCII daily ID || NUL || signed-i64 BE hour).
pub fn hourly_staging_job_id(daily_job_id: &str, hour_start_ms: i64) -> Result<String> {
    ensure!(
        !daily_job_id.is_empty()
            && daily_job_id.len() <= 64
            && daily_job_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
        "invalid daily job ID"
    );
    ensure!(
        hour_start_ms >= 0
            && hour_start_ms % HOUR == 0
            && hour_start_ms.checked_add(HOUR).is_some(),
        "invalid hourly start"
    );
    let mut bytes = b"fluxtrade:volume-profile:hourly-staging:v1\0".to_vec();
    bytes.extend_from_slice(daily_job_id.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&hour_start_ms.to_be_bytes());
    Ok(sha(&bytes))
}

fn official_kline(raw: &[u8], profile: &VolumeProfile) -> Result<Reconciliation> {
    let kline = super::binance_kline::parse(raw, profile.window())?;
    Reconciliation::new(
        profile,
        kline.response_sha256,
        kline.base_volume,
        kline.quote_volume,
        kline.constituent_trade_count,
    )
}

/// Consumes no external state. Errors discard the local accumulator and handoff.
pub fn assemble(
    daily_job_id: &str,
    day: Window,
    config_sha256: [u8; 32],
    inputs: &[(Manifest, Recovered)],
    official_kline_raw: &[u8],
    observed_at_ms: i64,
) -> Result<Handoff> {
    ensure!(
        day.start_ms() % DAY == 0 && day.end_ms() - day.start_ms() == DAY && inputs.len() == 24,
        "invalid daily coverage"
    );
    let mut profile = VolumeProfile::new(
        PRODUCT_ID,
        Grid::new(Decimal::ZERO, Decimal::TEN, "USDT")?,
        day,
    )?;
    let mut evidence = Vec::with_capacity(24);
    let mut previous_id: Option<u64> = None;
    for (index, (manifest, recovered)) in inputs.iter().enumerate() {
        let start = day.start_ms() + index as i64 * HOUR;
        let expected = Identity::new(
            hourly_staging_job_id(daily_job_id, start)?,
            Window::new(start, start + HOUR)?,
            GRID_ID.into(),
            config_sha256,
        )?;
        ensure!(
            manifest.identity() == &expected,
            "hourly manifest identity mismatch"
        );
        let manifest_bytes = serde_json::to_vec(manifest)?;
        ensure!(
            digest(&SHA256, &manifest_bytes).as_ref() == recovered.manifest_sha256(),
            "manifest/recovery binding mismatch"
        );
        let entries = manifest.entries();
        ensure!(
            !entries.is_empty() && recovered.pages().is_stopped(),
            "hour is not terminal"
        );
        for (sequence, entry) in entries.iter().enumerate() {
            ensure!(
                entry.sequence() == sequence as u64 && entry.byte_length() > 0,
                "malformed manifest entry"
            );
            ensure!(
                matches!(entry.progress(), CheckpointProgress::Next(_))
                    == (sequence + 1 < entries.len()),
                "invalid terminal manifest order"
            );
        }
        let mut previous_time = start;
        for trade in recovered.trades() {
            ensure!(
                trade.timestamp_ms >= previous_time && trade.timestamp_ms < start + HOUR,
                "trade outside hourly chronological scope"
            );
            ensure!(
                trade.first_trade_id <= trade.last_trade_id,
                "invalid constituent ID range"
            );
            ensure!(
                previous_id.is_none_or(|id| id.checked_add(1) == Some(trade.id)),
                "aggregate ID gap/overlap"
            );
            profile.add(PRODUCT_ID, trade.timestamp_ms, trade.price, trade.quantity)?;
            previous_id = Some(trade.id);
            previous_time = trade.timestamp_ms;
        }
        evidence.push(HourEvidence::new(
            start,
            sha(&manifest_bytes),
            entries.len() as u64,
            recovered.trades().first().map(|trade| trade.id),
            recovered.trades().last().map(|trade| trade.id),
            recovered.trades().len() as u64,
        )?);
    }
    let reconciliation = official_kline(official_kline_raw, &profile)?;
    let config_hex: String = config_sha256
        .iter()
        .flat_map(|b| {
            let hex = b"0123456789abcdef";
            [
                hex[(b >> 4) as usize] as char,
                hex[(b & 15) as usize] as char,
            ]
        })
        .collect();
    Handoff::new(
        daily_job_id.into(),
        config_hex,
        GRID_ID.into(),
        &profile,
        evidence,
        reconciliation,
        observed_at_ms,
    )
}

#[cfg(test)]
mod tests;
