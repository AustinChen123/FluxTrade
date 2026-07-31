use crate::model::Candlestick;
use anyhow::{bail, Result};
use std::collections::HashMap;

const MAX_SOURCE_TIMEFRAME_MS: i64 = 5 * 60 * 1000;

#[derive(Clone)]
struct AggregationBuffer {
    candle: Candlestick,
    last_timestamp: i64,
    eligible: bool,
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

    pub fn reset_product(&mut self, product_id: &str) {
        self.buffers
            .retain(|(buffer_product_id, _, _), _| buffer_product_id != product_id);
    }

    /// Adds one source candle and returns a candle only when the target window is closed.
    ///
    /// The source duration must divide the target duration exactly. Equal source and target
    /// durations are returned immediately because the input candle is already closed. A
    /// partial first bucket is never emitted: every returned target candle must start on its
    /// target timeframe boundary.
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
                Self::validate_target_alignment(&completed, target_ms)?;
                self.buffers.insert(
                    key,
                    AggregationBuffer {
                        candle: completed.clone(),
                        last_timestamp: candle.timestamp,
                        eligible: true,
                    },
                );
                return Ok(Some(completed));
            }
            if bucket_start > buffer.candle.timestamp {
                let previous_bucket_eligible = buffer.eligible;
                let completed = Self::completed_buffer(buffer, target_ms)?;

                // Initialize new buffer with current candle
                let mut new_buffer = candle.clone();
                new_buffer.timeframe = target_tf.to_string();
                new_buffer.timestamp = bucket_start;
                self.buffers.insert(
                    key,
                    AggregationBuffer {
                        candle: new_buffer,
                        last_timestamp: candle.timestamp,
                        eligible: previous_bucket_eligible || candle.timestamp == bucket_start,
                    },
                );

                Ok(completed)
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
            if source_ms == target_ms {
                Self::validate_target_alignment(&completed, target_ms)?;
            }
            self.buffers.insert(
                key,
                AggregationBuffer {
                    candle: new_buffer,
                    last_timestamp: candle.timestamp,
                    eligible: candle.timestamp == bucket_start,
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

    fn completed_buffer(buffer: AggregationBuffer, target_ms: i64) -> Result<Option<Candlestick>> {
        if !buffer.eligible {
            return Ok(None);
        }
        Self::validate_target_alignment(&buffer.candle, target_ms)?;
        Ok(Some(buffer.candle))
    }

    fn validate_target_alignment(candle: &Candlestick, target_ms: i64) -> Result<()> {
        if candle.timestamp < 0 || candle.timestamp % target_ms != 0 {
            bail!("aggregated candle timestamp must align to its target timeframe");
        }
        Ok(())
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

        let middle = (1..4)
            .map(|minute| Candlestick {
                product_id: product.clone(),
                timeframe: "1m".to_string(),
                timestamp: base_ts + minute * 60 * 1000,
                open: dec!(105),
                high: dec!(111),
                low: dec!(95),
                close: dec!(106),
                volume: dec!(10),
            })
            .collect::<Vec<_>>();

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
        for candle in middle {
            assert!(aggregator.add_candle(&candle, "5m").unwrap().is_none());
        }
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
        assert_eq!(completed.volume, dec!(50));
        assert_eq!(completed.timeframe, "5m");
    }

    #[test]
    fn aggregation_emits_only_target_aligned_buckets() {
        let mut aggregator = CandleAggregator::new();
        let base_ts = 1_800_000_000_000i64;
        let candle = |minute: i64| Candlestick {
            product_id: "RITHMIC:MNQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1),
        };

        // Start mid-window to prove that a partial 00:00 bucket is never promoted.
        let emitted = (2..=15)
            .filter_map(|minute| aggregator.add_candle(&candle(minute), "5m").unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            emitted
                .iter()
                .map(|candle| candle.timestamp)
                .collect::<Vec<_>>(),
            vec![base_ts + 5 * 60_000, base_ts + 10 * 60_000]
        );
        assert!(emitted
            .iter()
            .all(|candle| candle.timestamp % (5 * 60 * 1000) == 0));
    }

    #[test]
    fn aggregation_discards_partial_startup_bucket_then_recovers() {
        let mut aggregator = CandleAggregator::new();
        let base_ts = 1_800_000_000_000i64;
        let candle = |minute: i64| Candlestick {
            product_id: "RITHMIC:MNQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1),
        };

        for minute in 2..5 {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }
        assert!(aggregator.add_candle(&candle(5), "5m").unwrap().is_none());
        for minute in 6..10 {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }

        let completed = aggregator
            .add_candle(&candle(10), "5m")
            .unwrap()
            .expect("the next complete bucket should be emitted");
        assert_eq!(completed.timestamp, base_ts + 5 * 60_000);
        assert_eq!(completed.volume, dec!(5));
    }

    #[test]
    fn aggregation_does_not_promote_a_later_partial_bucket() {
        let mut aggregator = CandleAggregator::new();
        let base_ts = 1_800_000_000_000i64;
        let candle = |minute: i64| Candlestick {
            product_id: "RITHMIC:MNQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1),
        };

        assert!(aggregator.add_candle(&candle(2), "5m").unwrap().is_none());
        assert!(aggregator.add_candle(&candle(7), "5m").unwrap().is_none());
        assert!(aggregator.add_candle(&candle(10), "5m").unwrap().is_none());

        for minute in 11..15 {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }
        let completed = aggregator
            .add_candle(&candle(15), "5m")
            .unwrap()
            .expect("eligibility should recover only from an aligned bucket start");
        assert_eq!(completed.timestamp, base_ts + 10 * 60_000);
        assert_eq!(completed.volume, dec!(5));
    }

    #[test]
    fn aggregation_reset_discards_pre_reconnect_partial_bucket() {
        let mut aggregator = CandleAggregator::new();
        let product_id = "RITHMIC:MNQ-202609";
        let base_ts = 1_800_000_000_000i64;
        let candle = |minute: i64| Candlestick {
            product_id: product_id.to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1),
        };

        for minute in 0..2 {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }
        aggregator.reset_product(product_id);
        for minute in 3..6 {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn aggregation_allows_minutes_without_trades_after_bucket_start() {
        let mut aggregator = CandleAggregator::new();
        let base_ts = 1_800_000_000_000i64;
        let candle = |minute: i64| Candlestick {
            product_id: "RITHMIC:MNQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1),
        };

        for minute in [0, 1, 3, 4] {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }
        let completed = aggregator
            .add_candle(&candle(5), "5m")
            .unwrap()
            .expect("a no-trade minute does not make the observed OHLCV partial");
        assert_eq!(completed.volume, dec!(4));
    }

    #[test]
    fn aggregation_preserves_continuity_across_session_and_no_trade_gaps() {
        let mut aggregator = CandleAggregator::new();
        let base_ts = 1_800_000_000_000i64;
        let candle = |minute: i64| Candlestick {
            product_id: "RITHMIC:MNQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1),
        };

        for minute in 0..5 {
            assert!(aggregator
                .add_candle(&candle(minute), "5m")
                .unwrap()
                .is_none());
        }
        let completed = aggregator
            .add_candle(&candle(21), "5m")
            .unwrap()
            .expect("a later gap must not invalidate the already complete bucket");
        assert_eq!(completed.timestamp, base_ts);
        assert!(aggregator.add_candle(&candle(24), "5m").unwrap().is_none());
        let sparse = aggregator
            .add_candle(&candle(25), "5m")
            .unwrap()
            .expect("a no-trade bucket start must not suppress later observed trades");
        assert_eq!(sparse.timestamp, base_ts + 20 * 60_000);
        assert_eq!(sparse.volume, dec!(2));
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
