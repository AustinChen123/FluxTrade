//! Deterministic assembly from Store-recovered hourly inputs, with no I/O.
//! Callers must supply actual Store recovery results: this does not re-read raw
//! page bytes or authenticate manifests. Terminal recovery, identity, trade
//! continuity and exact official kline reconciliation bound the resulting claim.
use anyhow::{ensure, Result};
use ring::digest::{digest, SHA256};
use rust_decimal::Decimal;
use serde_json::Value;

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

fn nonnegative(value: &Value) -> Result<Decimal> {
    let text = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("expected kline decimal text"))?;
    ensure!(
        !text.is_empty() && text.bytes().all(|c| c.is_ascii_digit() || c == b'.'),
        "invalid kline decimal text"
    );
    let value = Decimal::from_str_exact(text)?;
    ensure!(value >= Decimal::ZERO, "negative kline value");
    Ok(value)
}

fn official_kline(raw: &[u8], profile: &VolumeProfile) -> Result<Reconciliation> {
    ensure!(raw.len() <= 65_536, "kline response byte limit");
    let value: Value = serde_json::from_slice(raw)?;
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected kline rows"))?;
    ensure!(rows.len() == 1, "expected one official daily kline");
    let row = rows[0]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected kline row"))?;
    ensure!(
        row.len() == 12
            && row[0].as_i64() == Some(profile.window().start_ms())
            && row[6].as_i64() == Some(profile.window().end_ms() - 1),
        "kline shape/window mismatch"
    );
    let mut numbers = Vec::new();
    for index in [1, 2, 3, 4, 5, 7, 9, 10, 11] {
        numbers.push(nonnegative(&row[index])?);
    }
    let count = row[8]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("invalid constituent trade count"))?;
    Reconciliation::new(profile, sha(raw), numbers[4], numbers[5], count)
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
