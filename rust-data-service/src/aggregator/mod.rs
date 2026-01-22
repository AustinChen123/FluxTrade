use crate::model::Candlestick;
use std::collections::HashMap;

#[allow(dead_code)]
pub struct CandleAggregator {
    // key: (product_id, target_timeframe), value: partial candlestick
    buffers: HashMap<(String, String), Candlestick>,
}

impl CandleAggregator {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            buffers: HashMap::new(),
        }
    }

    /// Adds a 1m candle and returns an aggregated candle if a timeframe window is closed.
    /// timeframe: target timeframe like "5m", "15m", "1h"
    #[allow(dead_code)]
    pub fn add_candle(&mut self, candle: &Candlestick, target_tf: &str) -> Option<Candlestick> {
        if candle.timeframe != "1m" {
            // Currently only support aggregating from 1m
            return None;
        }

        let tf_minutes = match self.parse_timeframe(target_tf) {
            Some(m) => m,
            None => return None,
        };

        let interval_ms = tf_minutes * 60 * 1000;
        let bucket_start = (candle.timestamp / interval_ms) * interval_ms;
        
        let key = (candle.product_id.clone(), target_tf.to_string());
        
        if let Some(mut buffer) = self.buffers.get(&key).cloned() {
            if bucket_start > buffer.timestamp {
                // Window closed, return buffer and start new one
                let completed = buffer.clone();
                
                // Initialize new buffer with current candle
                let mut new_buffer = candle.clone();
                new_buffer.timeframe = target_tf.to_string();
                new_buffer.timestamp = bucket_start;
                self.buffers.insert(key, new_buffer);
                
                return Some(completed);
            } else {
                // Update current buffer
                buffer.high = buffer.high.max(candle.high);
                buffer.low = buffer.low.min(candle.low);
                buffer.close = candle.close;
                buffer.volume += candle.volume;
                self.buffers.insert(key, buffer);
                None
            }
        } else {
            // First candle for this key
            let mut new_buffer = candle.clone();
            new_buffer.timeframe = target_tf.to_string();
            new_buffer.timestamp = bucket_start;
            self.buffers.insert(key, new_buffer);
            None
        }
    }

    fn parse_timeframe(&self, tf: &str) -> Option<i64> {
        let unit = tf.chars().last()?;
        let val_str = &tf[0..tf.len() - 1];
        let val = val_str.parse::<i64>().ok()?;

        match unit {
            'm' => Some(val),
            'h' => Some(val * 60),
            'd' => Some(val * 60 * 24),
            _ => None,
        }
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
            open: dec!(100), high: dec!(110), low: dec!(90), close: dec!(105), volume: dec!(10),
        };
        
        // 22:04 candle
        let c2 = Candlestick {
            product_id: product.clone(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + 4 * 60 * 1000,
            open: dec!(105), high: dec!(115), low: dec!(100), close: dec!(112), volume: dec!(10),
        };

        // 22:05 candle (starts new 5m bucket)
        let c3 = Candlestick {
            product_id: product.clone(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + 5 * 60 * 1000,
            open: dec!(112), high: dec!(120), low: dec!(110), close: dec!(118), volume: dec!(10),
        };

        assert!(aggregator.add_candle(&c1, "5m").is_none());
        assert!(aggregator.add_candle(&c2, "5m").is_none());
        
        let completed = aggregator.add_candle(&c3, "5m").expect("Should complete 5m candle");
        
        assert_eq!(completed.timestamp, base_ts);
        assert_eq!(completed.open, dec!(100));
        assert_eq!(completed.high, dec!(115));
        assert_eq!(completed.low, dec!(90));
        assert_eq!(completed.close, dec!(112));
        assert_eq!(completed.volume, dec!(20));
        assert_eq!(completed.timeframe, "5m");
    }
}
