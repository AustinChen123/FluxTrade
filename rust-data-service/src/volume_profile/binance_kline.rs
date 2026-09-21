//! Fixed Binance BTCUSDT spot daily-kline wire validation; no transport or aggregation.
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use serde_json::Value;

use super::{handoff::sha, mvp, Window};

pub const RAW_LIMIT: usize = 65_536;

pub struct DailyKline {
    pub(crate) response_sha256: String,
    pub(crate) base_volume: Decimal,
    pub(crate) quote_volume: Decimal,
    pub(crate) constituent_trade_count: u64,
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

pub fn parse(raw: &[u8], day: Window) -> Result<DailyKline> {
    let duration = 24 * mvp::HOUR;
    ensure!(
        day.start_ms() % duration == 0
            && day.start_ms().checked_add(duration) == Some(day.end_ms()),
        "invalid daily window"
    );
    ensure!(raw.len() <= RAW_LIMIT, "kline response byte limit");
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
            && row[0].as_i64() == Some(day.start_ms())
            && row[6].as_i64() == Some(day.end_ms() - 1),
        "kline shape/window mismatch"
    );
    let mut numbers = Vec::new();
    for index in [1, 2, 3, 4, 5, 7, 9, 10, 11] {
        numbers.push(nonnegative(&row[index])?);
    }
    let count = row[8]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("invalid constituent trade count"))?;
    Ok(DailyKline {
        response_sha256: sha(raw),
        base_volume: numbers[4],
        quote_volume: numbers[5],
        constituent_trade_count: count,
    })
}
