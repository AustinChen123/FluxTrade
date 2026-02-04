use ::pyo3::prelude::*;
use rust_decimal::Decimal;
use std::str::FromStr;

#[pyclass]
#[derive(Clone, Debug)]
pub struct Candlestick {
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub timeframe: String,
    #[pyo3(get, set)]
    pub timestamp: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

#[pymethods]
impl Candlestick {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (product_id, timeframe, timestamp, open, high, low, close, volume))]
    fn new(
        product_id: String,
        timeframe: String,
        timestamp: i64,
        open: String,
        high: String,
        low: String,
        close: String,
        volume: String,
    ) -> PyResult<Self> {
        Ok(Candlestick {
            product_id,
            timeframe,
            timestamp,
            open: parse_decimal(&open, "open")?,
            high: parse_decimal(&high, "high")?,
            low: parse_decimal(&low, "low")?,
            close: parse_decimal(&close, "close")?,
            volume: parse_decimal(&volume, "volume")?,
        })
    }

    #[getter]
    fn open(&self) -> String {
        self.open.to_string()
    }
    #[setter]
    fn set_open(&mut self, val: String) -> PyResult<()> {
        self.open = parse_decimal(&val, "open")?;
        Ok(())
    }

    #[getter]
    fn high(&self) -> String {
        self.high.to_string()
    }
    #[setter]
    fn set_high(&mut self, val: String) -> PyResult<()> {
        self.high = parse_decimal(&val, "high")?;
        Ok(())
    }

    #[getter]
    fn low(&self) -> String {
        self.low.to_string()
    }
    #[setter]
    fn set_low(&mut self, val: String) -> PyResult<()> {
        self.low = parse_decimal(&val, "low")?;
        Ok(())
    }

    #[getter]
    fn close(&self) -> String {
        self.close.to_string()
    }
    #[setter]
    fn set_close(&mut self, val: String) -> PyResult<()> {
        self.close = parse_decimal(&val, "close")?;
        Ok(())
    }

    #[getter]
    fn volume(&self) -> String {
        self.volume.to_string()
    }
    #[setter]
    fn set_volume(&mut self, val: String) -> PyResult<()> {
        self.volume = parse_decimal(&val, "volume")?;
        Ok(())
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Trade {
    #[pyo3(get, set)]
    pub id: String,
    #[pyo3(get, set)]
    pub product_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    #[pyo3(get, set)]
    pub side: String,
    #[pyo3(get, set)]
    pub timestamp: i64,
}

#[pymethods]
impl Trade {
    #[new]
    #[pyo3(signature = (id, product_id, price, quantity, side, timestamp))]
    fn new(
        id: String,
        product_id: String,
        price: String,
        quantity: String,
        side: String,
        timestamp: i64,
    ) -> PyResult<Self> {
        Ok(Trade {
            id,
            product_id,
            price: parse_decimal(&price, "price")?,
            quantity: parse_decimal(&quantity, "quantity")?,
            side,
            timestamp,
        })
    }

    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }
    #[setter]
    fn set_price(&mut self, val: String) -> PyResult<()> {
        self.price = parse_decimal(&val, "price")?;
        Ok(())
    }

    #[getter]
    fn quantity(&self) -> String {
        self.quantity.to_string()
    }
    #[setter]
    fn set_quantity(&mut self, val: String) -> PyResult<()> {
        self.quantity = parse_decimal(&val, "quantity")?;
        Ok(())
    }
}

/// Order types:
///   "MARKET"         — fill at next candle open
///   "LIMIT"          — fill when price touches order.price
///   "STOP_LOSS"      — triggers when price moves against position past trigger_price
///   "TAKE_PROFIT"    — triggers when price moves in favor past trigger_price
///   "TRAILING_STOP"  — SL that trails price by trailing_distance
#[pyclass]
#[derive(Clone, Debug)]
pub struct Order {
    #[pyo3(get, set)]
    pub id: String,
    #[pyo3(get, set)]
    pub product_id: String,
    /// "LONG" or "SHORT"
    #[pyo3(get, set)]
    pub side: String,
    /// "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
    #[pyo3(get, set)]
    pub order_type: String,
    /// Limit price (for LIMIT orders)
    pub price: Decimal,
    pub quantity: Decimal,
    #[pyo3(get, set)]
    pub timestamp: i64,
    /// Trigger price for SL/TP conditional orders
    pub trigger_price: Option<Decimal>,
    /// Distance for trailing stop
    pub trailing_distance: Option<Decimal>,
    /// Linked order ID for OCO (one-cancels-other)
    #[pyo3(get, set)]
    pub linked_order_id: Option<String>,
}

#[pymethods]
impl Order {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (id, product_id, side, order_type, price, quantity, timestamp, trigger_price=None, trailing_distance=None, linked_order_id=None))]
    fn new(
        id: String,
        product_id: String,
        side: String,
        order_type: String,
        price: String,
        quantity: String,
        timestamp: i64,
        trigger_price: Option<String>,
        trailing_distance: Option<String>,
        linked_order_id: Option<String>,
    ) -> PyResult<Self> {
        Ok(Order {
            id,
            product_id,
            side,
            order_type,
            price: parse_decimal(&price, "price")?,
            quantity: parse_decimal(&quantity, "quantity")?,
            timestamp,
            trigger_price: parse_optional_decimal(trigger_price, "trigger_price")?,
            trailing_distance: parse_optional_decimal(trailing_distance, "trailing_distance")?,
            linked_order_id,
        })
    }

    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }
    #[setter]
    fn set_price(&mut self, val: String) -> PyResult<()> {
        self.price = parse_decimal(&val, "price")?;
        Ok(())
    }

    #[getter]
    fn quantity(&self) -> String {
        self.quantity.to_string()
    }
    #[setter]
    fn set_quantity(&mut self, val: String) -> PyResult<()> {
        self.quantity = parse_decimal(&val, "quantity")?;
        Ok(())
    }

    #[getter]
    fn trigger_price(&self) -> Option<String> {
        self.trigger_price.map(|d| d.to_string())
    }
    #[setter]
    fn set_trigger_price(&mut self, val: Option<String>) -> PyResult<()> {
        self.trigger_price = parse_optional_decimal(val, "trigger_price")?;
        Ok(())
    }

    #[getter]
    fn trailing_distance(&self) -> Option<String> {
        self.trailing_distance.map(|d| d.to_string())
    }
    #[setter]
    fn set_trailing_distance(&mut self, val: Option<String>) -> PyResult<()> {
        self.trailing_distance = parse_optional_decimal(val, "trailing_distance")?;
        Ok(())
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct FillEvent {
    #[pyo3(get, set)]
    pub order_id: String,
    #[pyo3(get, set)]
    pub product_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    #[pyo3(get, set)]
    pub timestamp: i64,
    /// "MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"
    #[pyo3(get, set)]
    pub fill_type: String,
}

#[pymethods]
impl FillEvent {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (order_id, product_id, price, quantity, fee, timestamp, fill_type="MARKET".to_string()))]
    fn new(
        order_id: String,
        product_id: String,
        price: String,
        quantity: String,
        fee: String,
        timestamp: i64,
        fill_type: String,
    ) -> PyResult<Self> {
        Ok(FillEvent {
            order_id,
            product_id,
            price: parse_decimal(&price, "price")?,
            quantity: parse_decimal(&quantity, "quantity")?,
            fee: parse_decimal(&fee, "fee")?,
            timestamp,
            fill_type,
        })
    }

    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }
    #[setter]
    fn set_price(&mut self, val: String) -> PyResult<()> {
        self.price = parse_decimal(&val, "price")?;
        Ok(())
    }

    #[getter]
    fn quantity(&self) -> String {
        self.quantity.to_string()
    }
    #[setter]
    fn set_quantity(&mut self, val: String) -> PyResult<()> {
        self.quantity = parse_decimal(&val, "quantity")?;
        Ok(())
    }

    #[getter]
    fn fee(&self) -> String {
        self.fee.to_string()
    }
    #[setter]
    fn set_fee(&mut self, val: String) -> PyResult<()> {
        self.fee = parse_decimal(&val, "fee")?;
        Ok(())
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Position {
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub side: String, // "LONG", "SHORT", or "FLAT"
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub unrealized_pnl: Decimal,
}

#[pymethods]
impl Position {
    #[new]
    #[pyo3(signature = (product_id, side, quantity, entry_price, unrealized_pnl))]
    fn new(
        product_id: String,
        side: String,
        quantity: String,
        entry_price: String,
        unrealized_pnl: String,
    ) -> PyResult<Self> {
        Ok(Position {
            product_id,
            side,
            quantity: parse_decimal(&quantity, "quantity")?,
            entry_price: parse_decimal(&entry_price, "entry_price")?,
            unrealized_pnl: parse_decimal(&unrealized_pnl, "unrealized_pnl")?,
        })
    }

    #[getter]
    fn quantity(&self) -> String {
        self.quantity.to_string()
    }
    #[setter]
    fn set_quantity(&mut self, val: String) -> PyResult<()> {
        self.quantity = parse_decimal(&val, "quantity")?;
        Ok(())
    }

    #[getter]
    fn entry_price(&self) -> String {
        self.entry_price.to_string()
    }
    #[setter]
    fn set_entry_price(&mut self, val: String) -> PyResult<()> {
        self.entry_price = parse_decimal(&val, "entry_price")?;
        Ok(())
    }

    #[getter]
    fn unrealized_pnl(&self) -> String {
        self.unrealized_pnl.to_string()
    }
    #[setter]
    fn set_unrealized_pnl(&mut self, val: String) -> PyResult<()> {
        self.unrealized_pnl = parse_decimal(&val, "unrealized_pnl")?;
        Ok(())
    }
}

// ── Parsing helpers ─────────────────────────────────────────────

fn parse_decimal(s: &str, field: &str) -> PyResult<Decimal> {
    Decimal::from_str(s).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid decimal for '{field}': {e}"))
    })
}

fn parse_optional_decimal(s: Option<String>, field: &str) -> PyResult<Option<Decimal>> {
    match s {
        Some(v) => Ok(Some(parse_decimal(&v, field)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_candlestick_construction_and_fields() {
        let c = Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "15m".to_string(),
            timestamp: 1700000000,
            open: dec!(50000),
            high: dec!(51000),
            low: dec!(49000),
            close: dec!(50500),
            volume: dec!(123.45),
        };
        assert_eq!(c.product_id, "BINANCE:BTCUSDT-PERP");
        assert_eq!(c.timeframe, "15m");
        assert_eq!(c.timestamp, 1700000000);
        assert_eq!(c.open, dec!(50000));
        assert_eq!(c.high, dec!(51000));
        assert_eq!(c.low, dec!(49000));
        assert_eq!(c.close, dec!(50500));
        assert_eq!(c.volume, dec!(123.45));
    }

    #[test]
    fn test_candlestick_clone() {
        let c = Candlestick {
            product_id: "TEST".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1000,
            open: dec!(1),
            high: dec!(2),
            low: dec!(0.5),
            close: dec!(1.5),
            volume: dec!(10),
        };
        let c2 = c.clone();
        assert_eq!(c.product_id, c2.product_id);
        assert_eq!(c.open, c2.open);
    }

    #[test]
    fn test_trade_construction() {
        let t = Trade {
            id: "t1".to_string(),
            product_id: "BINANCE:ETHUSDT-PERP".to_string(),
            price: dec!(3000),
            quantity: dec!(2.5),
            side: "buy".to_string(),
            timestamp: 1700000000,
        };
        assert_eq!(t.id, "t1");
        assert_eq!(t.side, "buy");
        assert_eq!(t.price, dec!(3000));
        assert_eq!(t.quantity, dec!(2.5));
    }

    #[test]
    fn test_order_with_all_optional_fields() {
        let o = Order {
            id: "o1".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "LONG".to_string(),
            order_type: "TRAILING_STOP".to_string(),
            price: Decimal::ZERO,
            quantity: dec!(1),
            timestamp: 1000,
            trigger_price: Some(dec!(49000)),
            trailing_distance: Some(dec!(1000)),
            linked_order_id: Some("o2".to_string()),
        };
        assert_eq!(o.trigger_price, Some(dec!(49000)));
        assert_eq!(o.trailing_distance, Some(dec!(1000)));
        assert_eq!(o.linked_order_id.as_deref(), Some("o2"));
    }

    #[test]
    fn test_order_with_no_optional_fields() {
        let o = Order {
            id: "o3".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "SHORT".to_string(),
            order_type: "MARKET".to_string(),
            price: dec!(50000),
            quantity: dec!(0.5),
            timestamp: 2000,
            trigger_price: None,
            trailing_distance: None,
            linked_order_id: None,
        };
        assert!(o.trigger_price.is_none());
        assert!(o.trailing_distance.is_none());
        assert!(o.linked_order_id.is_none());
    }

    #[test]
    fn test_fill_event_construction() {
        let f = FillEvent {
            order_id: "o1".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            price: dec!(50000),
            quantity: dec!(1),
            fee: dec!(30),
            timestamp: 1000,
            fill_type: "STOP_LOSS".to_string(),
        };
        assert_eq!(f.fill_type, "STOP_LOSS");
        assert_eq!(f.fee, dec!(30));
    }

    #[test]
    fn test_position_construction_and_sides() {
        let long = Position {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "LONG".to_string(),
            quantity: dec!(1),
            entry_price: dec!(50000),
            unrealized_pnl: dec!(500),
        };
        assert_eq!(long.side, "LONG");

        let flat = Position {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "FLAT".to_string(),
            quantity: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
        };
        assert_eq!(flat.side, "FLAT");
        assert_eq!(flat.quantity, Decimal::ZERO);
    }

    #[test]
    fn test_order_clone_independence() {
        let o1 = Order {
            id: "orig".to_string(),
            product_id: "TEST".to_string(),
            side: "LONG".to_string(),
            order_type: "LIMIT".to_string(),
            price: dec!(100),
            quantity: dec!(1),
            timestamp: 1000,
            trigger_price: Some(dec!(90)),
            trailing_distance: None,
            linked_order_id: Some("linked".to_string()),
        };
        let mut o2 = o1.clone();
        o2.id = "cloned".to_string();
        o2.trigger_price = Some(dec!(80));

        assert_eq!(o1.id, "orig");
        assert_eq!(o1.trigger_price, Some(dec!(90)));
        assert_eq!(o2.id, "cloned");
        assert_eq!(o2.trigger_price, Some(dec!(80)));
    }
}
