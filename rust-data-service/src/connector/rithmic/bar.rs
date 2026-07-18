use super::market::LastTradeUpdate;
use crate::model::{validate_product_id, Candlestick};
use anyhow::{ensure, Result};

const MINUTE_MS: i64 = 60_000;

pub(crate) struct MinuteBarBuilder {
    product_id: String,
    exchange: String,
    symbol: String,
    current: Option<Candlestick>,
}

impl MinuteBarBuilder {
    pub(crate) fn new(product_id: String, exchange: String, symbol: String) -> Result<Self> {
        validate_product_id(&product_id)?;
        ensure!(
            !exchange.trim().is_empty(),
            "Rithmic exchange must not be empty"
        );
        ensure!(
            !symbol.trim().is_empty(),
            "Rithmic symbol must not be empty"
        );
        Ok(Self {
            product_id,
            exchange,
            symbol,
            current: None,
        })
    }

    pub(crate) fn push(&mut self, trade: &LastTradeUpdate) -> Result<Option<Candlestick>> {
        ensure!(
            trade.exchange == self.exchange && trade.symbol == self.symbol,
            "Rithmic trade does not match minute-bar instrument"
        );
        if trade.is_snapshot {
            return Ok(None);
        }

        let bucket = trade.timestamp / MINUTE_MS * MINUTE_MS;
        let next = Candlestick {
            product_id: self.product_id.clone(),
            timeframe: "1m".to_string(),
            timestamp: bucket,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: trade.quantity,
        };

        let Some(current) = &mut self.current else {
            self.current = Some(next);
            return Ok(None);
        };
        ensure!(
            bucket >= current.timestamp,
            "Rithmic trade timestamp moved behind the active minute"
        );
        if bucket > current.timestamp {
            return Ok(self.current.replace(next));
        }

        current.high = current.high.max(trade.price);
        current.low = current.low.min(trade.price);
        current.close = trade.price;
        current.volume += trade.quantity;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::rithmic::market::Aggressor;
    use rust_decimal_macros::dec;

    fn trade(timestamp: i64, price: rust_decimal::Decimal) -> LastTradeUpdate {
        LastTradeUpdate {
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            price,
            quantity: dec!(2),
            aggressor: Some(Aggressor::Buy),
            timestamp,
            is_snapshot: false,
        }
    }

    #[test]
    fn builds_completed_minute_from_ordered_trades() {
        let mut builder = MinuteBarBuilder::new(
            "RITHMIC:NQ-202609".to_string(),
            "CME".to_string(),
            "NQU6".to_string(),
        )
        .unwrap();
        let base = 1_800_000_000_000;

        assert!(builder
            .push(&trade(base + 1_000, dec!(100)))
            .unwrap()
            .is_none());
        assert!(builder
            .push(&trade(base + 20_000, dec!(101)))
            .unwrap()
            .is_none());
        assert!(builder
            .push(&trade(base + 40_000, dec!(99)))
            .unwrap()
            .is_none());
        let completed = builder
            .push(&trade(base + MINUTE_MS, dec!(102)))
            .unwrap()
            .unwrap();

        assert_eq!(completed.product_id, "RITHMIC:NQ-202609");
        assert_eq!(completed.timeframe, "1m");
        assert_eq!(completed.timestamp, base);
        assert_eq!(completed.open, dec!(100));
        assert_eq!(completed.high, dec!(101));
        assert_eq!(completed.low, dec!(99));
        assert_eq!(completed.close, dec!(99));
        assert_eq!(completed.volume, dec!(6));
    }

    #[test]
    fn snapshot_mismatch_and_out_of_order_matrix_is_safe() {
        let mut builder = MinuteBarBuilder::new(
            "RITHMIC:NQ-202609".to_string(),
            "CME".to_string(),
            "NQU6".to_string(),
        )
        .unwrap();
        let base = 1_800_000_000_000;
        let mut snapshot = trade(base, dec!(100));
        snapshot.is_snapshot = true;
        assert!(builder.push(&snapshot).unwrap().is_none());

        let mut wrong_instrument = trade(base, dec!(100));
        wrong_instrument.symbol = "MNQU6".to_string();
        assert!(builder.push(&wrong_instrument).is_err());

        builder.push(&trade(base + MINUTE_MS, dec!(100))).unwrap();
        assert!(builder.push(&trade(base, dec!(100))).is_err());
    }
}
