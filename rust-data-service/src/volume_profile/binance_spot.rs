//! Public Binance spot aggTrades protocol only; no transport or completeness claim.
//! Official contract: binance/binance-spot-api-docs rest-api.md, aggTrades.
use std::collections::BTreeMap;

use anyhow::{bail, ensure, Result};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::Window;

pub const PRODUCT_ID: &str = "BINANCE:BTCUSDT-SPOT";
pub const ENDPOINT: &str = "https://data-api.binance.vision/api/v3/aggTrades";
pub const SYMBOL: &str = "BTCUSDT";
pub const PAGE_LIMIT: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AggregateTrade {
    #[serde(rename = "a")]
    pub id: u64,
    #[serde(rename = "p", deserialize_with = "positive_decimal")]
    pub price: Decimal,
    #[serde(rename = "q", deserialize_with = "positive_decimal")]
    pub quantity: Decimal,
    #[serde(rename = "T")]
    pub timestamp_ms: i64,
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
    #[serde(rename = "M")]
    pub best_match: bool,
}

fn positive_decimal<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Decimal, D::Error> {
    let text = String::deserialize(d)?;
    if text.is_empty() || !text.bytes().all(|c| c.is_ascii_digit() || c == b'.') {
        return Err(serde::de::Error::custom("expected unsigned decimal text"));
    }
    let value = Decimal::from_str_exact(&text).map_err(serde::de::Error::custom)?;
    if value <= Decimal::ZERO {
        return Err(serde::de::Error::custom("expected positive decimal"));
    }
    Ok(value)
}

/// Parameters are inclusive on Binance; symbol and limit are the constants above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    First { start_time: i64, end_time: i64 },
    Next { from_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    Continue(Request),
    EmptyPage,
    WindowEnd,
}

/// Own one product/window only. Retain seen content to validate replay overlaps;
/// the later job owner must impose resource budgets and persist its checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pages {
    window: Window,
    seen: BTreeMap<u64, AggregateTrade>,
    stopped: bool,
}

impl Pages {
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }
    pub fn new(window: Window) -> Self {
        Self {
            window,
            seen: BTreeMap::new(),
            stopped: false,
        }
    }

    pub fn request(&self) -> Result<Request> {
        ensure!(!self.stopped, "pagination already stopped");
        match self.seen.last_key_value() {
            Some((&id, _)) => Ok(Request::Next {
                from_id: id
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("aggregate ID overflow"))?,
            }),
            None => Ok(Request::First {
                start_time: self.window.start_ms(),
                end_time: self.window.end_ms() - 1,
            }),
        }
    }

    /// Validate an entire page before mutation. Identical overlaps are omitted.
    /// Even a short page continues by ID, preserving all same-millisecond trades.
    pub fn accept(&mut self, body: &[u8]) -> Result<(Vec<AggregateTrade>, Progress)> {
        ensure!(!self.stopped, "pagination already stopped");
        let page: Vec<AggregateTrade> = serde_json::from_slice(body)?;
        ensure!(page.len() <= PAGE_LIMIT, "page exceeds limit");
        let mut previous = None;
        let mut last = self.seen.last_key_value().map(|(_, trade)| trade.clone());
        let mut added = Vec::new();
        let mut reached_end = false;
        for trade in &page {
            ensure!(
                trade.timestamp_ms >= self.window.start_ms(),
                "timestamp before window"
            );
            ensure!(
                trade.first_trade_id <= trade.last_trade_id,
                "invalid constituent ID range"
            );
            if let Some(id) = previous {
                ensure!(trade.id > id, "page IDs not increasing");
            }
            previous = Some(trade.id);
            if let Some(existing) = self.seen.get(&trade.id) {
                ensure!(trade == existing, "conflicting aggregate overlap");
                continue;
            }
            if let Some(prior) = &last {
                ensure!(
                    prior.id.checked_add(1) == Some(trade.id),
                    "aggregate ID gap or unknown overlap"
                );
                ensure!(
                    trade.timestamp_ms >= prior.timestamp_ms,
                    "timestamps not chronological"
                );
            }
            last = Some(trade.clone());
            if trade.timestamp_ms >= self.window.end_ms() {
                reached_end = true;
            } else {
                added.push(trade.clone());
            }
        }
        let progress = if reached_end {
            Progress::WindowEnd
        } else if page.is_empty() {
            Progress::EmptyPage
        } else {
            ensure!(!added.is_empty(), "overlap-only page makes no progress");
            let id = last.as_ref().unwrap().id;
            match id.checked_add(1) {
                Some(from_id) => Progress::Continue(Request::Next { from_id }),
                None => bail!("aggregate ID overflow"),
            }
        };
        self.seen
            .extend(added.iter().map(|trade| (trade.id, trade.clone())));
        self.stopped = !matches!(progress, Progress::Continue(_));
        Ok((added, progress))
    }
}

#[cfg(test)]
mod tests;
