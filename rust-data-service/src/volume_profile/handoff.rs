//! Version-one daily Binance spot handoff. Evidence attests caller-validated
//! official inputs; this boundary performs no collection, I/O or publication.
use anyhow::{ensure, Result};
use ring::digest::{digest, SHA256};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};

use super::{binance_spot::PRODUCT_ID, VolumeProfile, ALGORITHM_VERSION};

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;
const MAX_EPOCH: i64 = 253_402_300_799_999;
const MAX_BYTES: usize = 65_536;
pub(crate) use super::mvp::GRID as GRID_ID;

fn safe(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}
fn hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn decimal(value: Decimal) -> String {
    if value.is_zero() {
        "0".into()
    } else {
        value.normalize().to_string()
    }
}
pub(crate) fn sha(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .flat_map(|b| {
            [
                HEX[(b >> 4) as usize] as char,
                HEX[(b & 15) as usize] as char,
            ]
        })
        .collect()
}

#[derive(Debug, Clone, Serialize)]
pub struct HourEvidence {
    start_ms: i64,
    end_ms: i64,
    manifest_sha256: String,
    page_count: u64,
    first_aggregate_id: Option<u64>,
    last_aggregate_id: Option<u64>,
    aggregate_count: u64,
}
impl HourEvidence {
    pub fn new(
        start_ms: i64,
        manifest_sha256: String,
        page_count: u64,
        first: Option<u64>,
        last: Option<u64>,
        count: u64,
    ) -> Result<Self> {
        ensure!(
            start_ms >= 0 && start_ms % HOUR == 0 && start_ms <= MAX_EPOCH - HOUR,
            "invalid hour"
        );
        ensure!(
            hex(&manifest_sha256) && page_count > 0 && count <= i64::MAX as u64,
            "invalid hour evidence"
        );
        let ids_valid = match (first, last) {
            (None, None) => count == 0,
            (Some(a), Some(b)) => {
                count > 0 && a <= b && u128::from(count) == u128::from(b) - u128::from(a) + 1
            }
            _ => false,
        };
        ensure!(ids_valid, "invalid aggregate ID range");
        Ok(Self {
            start_ms,
            end_ms: start_ms + HOUR,
            manifest_sha256,
            page_count,
            first_aggregate_id: first,
            last_aggregate_id: last,
            aggregate_count: count,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Reconciliation {
    source: &'static str,
    product_id: String,
    window_start_ms: i64,
    window_end_ms: i64,
    interval: &'static str,
    response_sha256: String,
    expected_base_volume: String,
    expected_quote_volume: String,
    actual_base_volume: String,
    actual_quote_volume: String,
    actual_aggregate_trade_count: u64,
    official_constituent_trade_count: u64,
    result: &'static str,
}
impl Reconciliation {
    /// Kline constituent trade count is diagnostic, never aggTrade record count.
    pub fn new(
        profile: &VolumeProfile,
        response_sha256: String,
        expected_base: Decimal,
        expected_quote: Decimal,
        official_count: u64,
    ) -> Result<Self> {
        let total = profile.totals();
        ensure!(
            profile.product_id() == PRODUCT_ID
                && profile.grid().unit() == "USDT"
                && profile.window().start_ms() % DAY == 0
                && profile.window().end_ms() - profile.window().start_ms() == DAY,
            "invalid official reconciliation scope"
        );
        ensure!(
            hex(&response_sha256)
                && expected_base == total.base_volume
                && expected_quote == total.quote_volume,
            "non-exact reconciliation"
        );
        ensure!(
            (total.aggregate_count == 0) == (official_count == 0),
            "invalid official count"
        );
        Ok(Self {
            source: "BINANCE_SPOT_1D_KLINE",
            product_id: profile.product_id().into(),
            window_start_ms: profile.window().start_ms(),
            window_end_ms: profile.window().end_ms(),
            interval: "1d",
            response_sha256,
            expected_base_volume: decimal(expected_base),
            expected_quote_volume: decimal(expected_quote),
            actual_base_volume: decimal(total.base_volume),
            actual_quote_volume: decimal(total.quote_volume),
            actual_aggregate_trade_count: total.aggregate_count,
            official_constituent_trade_count: official_count,
            result: "EXACT",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Handoff {
    wire: Value,
}
impl Handoff {
    pub fn new(
        job_id: String,
        config_sha256: String,
        grid_id: String,
        profile: &VolumeProfile,
        hours: Vec<HourEvidence>,
        reconciliation: Reconciliation,
        source_available_at_ms: i64,
    ) -> Result<Self> {
        let window = profile.window();
        ensure!(
            safe(&job_id, 64) && safe(&grid_id, 64) && hex(&config_sha256),
            "invalid handoff identity"
        );
        ensure!(
            profile.product_id() == PRODUCT_ID
                && profile.grid().unit() == "USDT"
                && grid_id == GRID_ID
                && profile.grid().origin() == Decimal::ZERO
                && profile.grid().step() == Decimal::TEN,
            "invalid source product"
        );
        ensure!(
            window.start_ms() % DAY == 0
                && window.end_ms() - window.start_ms() == DAY
                && source_available_at_ms >= window.end_ms()
                && source_available_at_ms <= MAX_EPOCH,
            "invalid daily window"
        );
        ensure!(
            hours.len() == 24
                && profile.totals().aggregate_count <= i64::MAX as u64
                && profile.bins().len() <= 4096,
            "invalid coverage"
        );
        let (mut count, mut prior) = (0_u64, None::<u64>);
        for (index, hour) in hours.iter().enumerate() {
            ensure!(
                hour.start_ms == window.start_ms() + index as i64 * HOUR,
                "hour coverage mismatch"
            );
            count = count
                .checked_add(hour.aggregate_count)
                .ok_or_else(|| anyhow::anyhow!("count overflow"))?;
            if let Some(first) = hour.first_aggregate_id {
                ensure!(
                    prior.is_none_or(|last| last.checked_add(1) == Some(first)),
                    "aggregate ID gap/overlap"
                );
                prior = hour.last_aggregate_id; // Empty hours do not break continuity.
            }
        }
        ensure!(
            count == profile.totals().aggregate_count,
            "hour count mismatch"
        );
        ensure!(
            reconciliation.product_id == profile.product_id()
                && reconciliation.window_start_ms == window.start_ms()
                && reconciliation.window_end_ms == window.end_ms()
                && reconciliation.actual_base_volume == decimal(profile.totals().base_volume)
                && reconciliation.actual_quote_volume == decimal(profile.totals().quote_volume)
                && reconciliation.actual_aggregate_trade_count == count,
            "reconciliation scope mismatch"
        );
        let bins: Vec<Value> = profile.bins().iter().map(|(index, volume)| json!({
            "bin_index": index, "base_volume": decimal(volume.base_volume),
            "quote_volume": decimal(volume.quote_volume), "aggregate_count": volume.aggregate_count
        })).collect();
        let content = json!({"schema_version": 1, "product_id": profile.product_id(),
            "window_start_ms": window.start_ms(), "window_end_ms": window.end_ms(), "period": "1d", "timezone": "UTC",
            "grid_id": grid_id, "bin_origin": decimal(profile.grid().origin()), "bin_step": decimal(profile.grid().step()),
            "algorithm_version": ALGORITHM_VERSION, "bins": bins});
        let content_sha256 = sha(&serde_json::to_vec(&content)?);
        let result = Self {
            wire: json!({"schema_version": 1, "job_id": job_id, "config_sha256": config_sha256,
            "content": content, "content_sha256": content_sha256, "hours": hours, "reconciliation": reconciliation,
            "source_available_at_ms": source_available_at_ms, "availability_basis": "OBSERVED", "raw_retention_state": "PRESENT"}),
        };
        result.to_bytes()?;
        Ok(result)
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(&self.wire)?;
        ensure!(bytes.len() <= MAX_BYTES, "handoff byte limit");
        // Match the existing canonical JSON depth/node envelope, not just bytes.
        fn nodes(value: &Value) -> usize {
            match value {
                Value::Object(map) => 1 + map.values().map(|v| 1 + nodes(v)).sum::<usize>(),
                Value::Array(items) => 1 + items.iter().map(nodes).sum::<usize>(),
                _ => 1,
            }
        }
        ensure!(nodes(&self.wire) <= 4096, "handoff node limit");
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
