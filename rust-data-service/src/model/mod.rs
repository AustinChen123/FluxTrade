use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candlestick {
    pub product_id: String,
    pub timeframe: String,
    pub timestamp: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl Candlestick {
    #[allow(dead_code)]
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.open <= Decimal::ZERO
            || self.high <= Decimal::ZERO
            || self.low <= Decimal::ZERO
            || self.close <= Decimal::ZERO
        {
            anyhow::bail!("Prices must be positive");
        }
        if self.volume < Decimal::ZERO {
            anyhow::bail!("Volume cannot be negative");
        }
        if !self.product_id.contains(':') || !self.product_id.ends_with("-PERP") {
            anyhow::bail!("Invalid product_id format: {}", self.product_id);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: String, // Exchange Trade ID
    pub product_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: String, // "buy" or "sell"
    pub timestamp: i64,
}

impl Trade {
    #[allow(dead_code)]
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.price <= Decimal::ZERO {
            anyhow::bail!("Price must be positive");
        }
        if self.quantity <= Decimal::ZERO {
            anyhow::bail!("Quantity must be positive");
        }
        if self.side != "buy" && self.side != "sell" {
            anyhow::bail!("Side must be 'buy' or 'sell'");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_candlestick_validation() {
        let make_valid = || Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1600000000,
            open: dec!(50000),
            high: dec!(51000),
            low: dec!(49000),
            close: dec!(50500),
            volume: dec!(10),
        };

        assert!(make_valid().validate().is_ok());

        let mut invalid_candle = make_valid();
        invalid_candle.product_id = "INVALID_FORMAT".to_string();
        assert!(invalid_candle.validate().is_err());

        let mut zero_price_candle = make_valid();
        zero_price_candle.open = dec!(0);
        assert!(zero_price_candle.validate().is_err());
    }

    #[test]
    fn test_trade_validation() {
        let make_valid = || Trade {
            id: "trade_123".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            price: dec!(50000),
            quantity: dec!(0.1),
            side: "buy".to_string(),
            timestamp: 1600000000,
        };

        assert!(make_valid().validate().is_ok());

        let mut invalid_side_trade = make_valid();
        invalid_side_trade.side = "invalid".to_string();
        assert!(invalid_side_trade.validate().is_err());

        let mut negative_qty_trade = make_valid();
        negative_qty_trade.quantity = dec!(-1);
        assert!(negative_qty_trade.validate().is_err());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub product_id: String,
    pub timestamp: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    pub exchange: String,
    pub asset: String,
    pub balance: Decimal,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionUpdate {
    pub exchange: String,
    pub symbol: String,
    pub amount: Decimal,
    pub entry_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserStreamEvent {
    Account(AccountUpdate),
    Position(PositionUpdate),
}
