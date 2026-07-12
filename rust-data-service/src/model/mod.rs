use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub fn validate_product_id(product_id: &str) -> anyhow::Result<()> {
    let (venue, instrument) = product_id
        .split_once(':')
        .filter(|(_, instrument)| !instrument.contains(':'))
        .ok_or_else(|| anyhow::anyhow!("Invalid product_id format: {product_id}"))?;
    if venue.is_empty()
        || !venue
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    {
        anyhow::bail!("Invalid product_id format: {product_id}");
    }

    if let Some(symbol) = instrument.strip_suffix("-PERP") {
        if !symbol.is_empty()
            && symbol
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            return Ok(());
        }
    } else if let Some((root, expiry)) = instrument.rsplit_once('-') {
        let month = expiry.get(4..6).and_then(|value| value.parse::<u8>().ok());
        if !root.is_empty()
            && root.starts_with(|c: char| c.is_ascii_uppercase())
            && root
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
            && expiry.len() == 6
            && expiry.chars().all(|c| c.is_ascii_digit())
            && month.is_some_and(|value| (1..=12).contains(&value))
        {
            return Ok(());
        }
    }

    anyhow::bail!("Invalid product_id format: {product_id}")
}

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
        if self.high < self.open || self.high < self.close {
            anyhow::bail!("OHLC invariant violated: high must be >= open and close");
        }
        if self.low > self.open || self.low > self.close {
            anyhow::bail!("OHLC invariant violated: low must be <= open and close");
        }
        if self.volume < Decimal::ZERO {
            anyhow::bail!("Volume cannot be negative");
        }
        validate_product_id(&self.product_id)?;
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
        validate_product_id(&self.product_id)?;
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
    fn product_id_validation_matrix() {
        for product_id in [
            "BINANCE:BTCUSDT-PERP",
            "BACKPACK:SOL_USDC-PERP",
            "RITHMIC:MNQ-202509",
            "CME:ES-202512",
        ] {
            assert!(validate_product_id(product_id).is_ok(), "{product_id}");
        }

        for product_id in [
            "RITHMIC:MNQ",
            "RITHMIC:MNQ-202500",
            "RITHMIC:MNQ-202513",
            "RITHMIC:MNQ-20259",
            "rithmic:MNQ-202509",
            "RITHMIC:mnq-202509",
            "RITHMIC::MNQ-202509",
        ] {
            assert!(validate_product_id(product_id).is_err(), "{product_id}");
        }
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
