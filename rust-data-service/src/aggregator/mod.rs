use crate::model::Candlestick;
use anyhow::{bail, Result};
use std::collections::HashMap;

const MAX_SOURCE_TIMEFRAME_MS: i64 = 5 * 60 * 1000;

#[derive(Clone)]
struct AggregationBuffer {
    candle: Candlestick,
    last_timestamp: i64,
}

pub struct CandleAggregator {
    // key: (product_id, source_timeframe, target_timeframe)
    buffers: HashMap<(String, String, String), AggregationBuffer>,
}

impl CandleAggregator {
    pub fn new() -> Self {
        Self {
            buffers: HashMap::new(),
        }
    }

    /// Adds one source candle and returns a candle only when the target window is closed.
    ///
    /// The source duration must divide the target duration exactly. Equal source and target
    /// durations are returned immediately because the input candle is already closed.
    pub fn add_candle(
        &mut self,
        candle: &Candlestick,
        target_tf: &str,
    ) -> Result<Option<Candlestick>> {
        candle.validate()?;
        let source_ms = Self::parse_timeframe_millis(&candle.timeframe)?;
        let target_ms = Self::parse_timeframe_millis(target_tf)?;
        if source_ms > MAX_SOURCE_TIMEFRAME_MS {
            bail!("source timeframe cannot exceed five minutes");
        }
        if source_ms > target_ms {
            bail!("source timeframe cannot exceed target timeframe");
        }
        if target_ms % source_ms != 0 {
            bail!("source timeframe must divide target timeframe exactly");
        }
        if candle.timestamp < 0 || candle.timestamp % source_ms != 0 {
            bail!("candle timestamp must align to its non-negative UTC source interval");
        }

        let bucket_start = (candle.timestamp / target_ms) * target_ms;
        let key = (
            candle.product_id.clone(),
            candle.timeframe.clone(),
            target_tf.to_string(),
        );

        if let Some(mut buffer) = self.buffers.get(&key).cloned() {
            if candle.timestamp <= buffer.last_timestamp {
                bail!("candle timestamps must be strictly increasing");
            }
            if source_ms == target_ms {
                let mut completed = candle.clone();
                completed.timeframe = target_tf.to_string();
                self.buffers.insert(
                    key,
                    AggregationBuffer {
                        candle: completed.clone(),
                        last_timestamp: candle.timestamp,
                    },
                );
                return Ok(Some(completed));
            }
            if bucket_start > buffer.candle.timestamp {
                // Window closed, return buffer and start new one
                let completed = buffer.candle;

                // Initialize new buffer with current candle
                let mut new_buffer = candle.clone();
                new_buffer.timeframe = target_tf.to_string();
                new_buffer.timestamp = bucket_start;
                self.buffers.insert(
                    key,
                    AggregationBuffer {
                        candle: new_buffer,
                        last_timestamp: candle.timestamp,
                    },
                );

                Ok(Some(completed))
            } else {
                // Update current buffer
                buffer.candle.high = buffer.candle.high.max(candle.high);
                buffer.candle.low = buffer.candle.low.min(candle.low);
                buffer.candle.close = candle.close;
                buffer.candle.volume += candle.volume;
                buffer.last_timestamp = candle.timestamp;
                self.buffers.insert(key, buffer);
                Ok(None)
            }
        } else {
            let mut new_buffer = candle.clone();
            new_buffer.timeframe = target_tf.to_string();
            new_buffer.timestamp = bucket_start;
            let completed = new_buffer.clone();
            self.buffers.insert(
                key,
                AggregationBuffer {
                    candle: new_buffer,
                    last_timestamp: candle.timestamp,
                },
            );
            if source_ms == target_ms {
                Ok(Some(completed))
            } else {
                Ok(None)
            }
        }
    }

    pub fn can_aggregate(source_tf: &str, target_tf: &str) -> Result<bool> {
        let source_ms = Self::parse_timeframe_millis(source_tf)?;
        let target_ms = Self::parse_timeframe_millis(target_tf)?;
        Ok(source_ms <= MAX_SOURCE_TIMEFRAME_MS
            && source_ms <= target_ms
            && target_ms % source_ms == 0)
    }

    fn parse_timeframe_millis(tf: &str) -> Result<i64> {
        let unit = tf
            .chars()
            .last()
            .ok_or_else(|| anyhow::anyhow!("target timeframe cannot be empty"))?;
        let val_str = tf
            .strip_suffix(unit)
            .ok_or_else(|| anyhow::anyhow!("invalid target timeframe: {tf}"))?;
        let val = val_str
            .parse::<i64>()
            .map_err(|_| anyhow::anyhow!("invalid target timeframe: {tf}"))?;
        if val <= 0 {
            bail!("target timeframe must be positive");
        }

        let unit_ms = match unit {
            's' => 1_000,
            'm' => 60_000,
            'h' => 60 * 60_000,
            'd' => 24 * 60 * 60_000,
            _ => bail!("unsupported timeframe unit: {unit}"),
        };
        val.checked_mul(unit_ms)
            .ok_or_else(|| anyhow::anyhow!("timeframe is too large"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_aggregation_5m() {
        let mut aggregator = CandleAggregator::new();
        let product = "BINANCE:BTCUSDT-PERP".to_string();
        let base_ts = 1737583200000i64; // 2026-01-22 22:00:00 (multiple of 5m)

        // 22:00 candle
        let c1 = Candlestick {
            product_id: product.clone(),
            timeframe: "1m".to_string(),
            timestamp: base_ts,
            open: dec!(100),
            high: dec!(110),
            low: dec!(90),
            close: dec!(105),
            volume: dec!(10),
        };

        // 22:04 candle
        let c2 = Candlestick {
            product_id: product.clone(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + 4 * 60 * 1000,
            open: dec!(105),
            high: dec!(115),
            low: dec!(100),
            close: dec!(112),
            volume: dec!(10),
        };

        // 22:05 candle (starts new 5m bucket)
        let c3 = Candlestick {
            product_id: product.clone(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + 5 * 60 * 1000,
            open: dec!(112),
            high: dec!(120),
            low: dec!(110),
            close: dec!(118),
            volume: dec!(10),
        };

        assert!(aggregator.add_candle(&c1, "5m").unwrap().is_none());
        assert!(aggregator.add_candle(&c2, "5m").unwrap().is_none());

        let completed = aggregator
            .add_candle(&c3, "5m")
            .unwrap()
            .expect("Should complete 5m candle");

        assert_eq!(completed.timestamp, base_ts);
        assert_eq!(completed.open, dec!(100));
        assert_eq!(completed.high, dec!(115));
        assert_eq!(completed.low, dec!(90));
        assert_eq!(completed.close, dec!(112));
        assert_eq!(completed.volume, dec!(20));
        assert_eq!(completed.timeframe, "5m");
    }

    #[test]
    fn aggregation_rejects_invalid_input_matrix() {
        let candle = Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1_737_583_200_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(10),
        };

        let mut invalid_ohlc = candle.clone();
        invalid_ohlc.high = dec!(99);
        let mut negative_volume = candle.clone();
        negative_volume.volume = dec!(-1);
        let mut invalid_product = candle.clone();
        invalid_product.product_id = "MNQ".to_string();
        for invalid in [invalid_ohlc, negative_volume, invalid_product] {
            assert!(CandleAggregator::new().add_candle(&invalid, "5m").is_err());
        }

        for target in ["", "m", "0m", "-5m", "5x", "5分", "999999999999999999d"] {
            assert!(
                CandleAggregator::new().add_candle(&candle, target).is_err(),
                "{target}"
            );
        }

        let mut wrong_source = candle.clone();
        wrong_source.timeframe = "6m".to_string();
        assert!(CandleAggregator::new()
            .add_candle(&wrong_source, "5m")
            .is_err());
    }

    #[test]
    fn aggregation_support_matrix_preserves_exact_bucket_semantics() {
        for (source, target) in [
            ("30s", "5m"),
            ("1m", "5m"),
            ("3m", "15m"),
            ("5m", "5m"),
            ("5m", "15m"),
        ] {
            assert!(CandleAggregator::can_aggregate(source, target).unwrap());
        }
        for (source, target) in [("2m", "5m"), ("4m", "5m"), ("6m", "5m"), ("6m", "15m")] {
            assert!(!CandleAggregator::can_aggregate(source, target).unwrap());
        }
    }

    #[test]
    fn equal_timeframe_is_an_ordered_passthrough() {
        let candle = Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "5m".to_string(),
            timestamp: 1_737_583_200_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(10),
        };
        let mut aggregator = CandleAggregator::new();

        let completed = aggregator
            .add_candle(&candle, "5m")
            .unwrap()
            .expect("equal timeframe should pass through");

        assert_eq!(completed.timestamp, candle.timestamp);
        assert_eq!(completed.open, candle.open);
        assert!(aggregator.add_candle(&candle, "5m").is_err());
    }

    #[test]
    fn aggregation_rejects_non_increasing_or_misaligned_timestamps() {
        let mut aggregator = CandleAggregator::new();
        let candle = Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1_737_583_500_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(10),
        };
        assert!(aggregator.add_candle(&candle, "5m").unwrap().is_none());

        let mut previous = candle.clone();
        previous.timestamp -= 5 * 60 * 1000;
        assert!(aggregator.add_candle(&previous, "5m").is_err());

        let mut duplicate_aggregator = CandleAggregator::new();
        assert!(duplicate_aggregator
            .add_candle(&candle, "5m")
            .unwrap()
            .is_none());
        assert!(duplicate_aggregator.add_candle(&candle, "5m").is_err());

        let mut misaligned = candle.clone();
        misaligned.timestamp += 1;
        assert!(CandleAggregator::new()
            .add_candle(&misaligned, "5m")
            .is_err());
    }
}
