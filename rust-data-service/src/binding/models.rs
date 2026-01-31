use ::pyo3::prelude::*;

#[pyclass]
#[derive(Clone, Debug)]
pub struct Candlestick {
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub timeframe: String,
    #[pyo3(get, set)]
    pub timestamp: i64,
    #[pyo3(get, set)]
    pub open: f64,
    #[pyo3(get, set)]
    pub high: f64,
    #[pyo3(get, set)]
    pub low: f64,
    #[pyo3(get, set)]
    pub close: f64,
    #[pyo3(get, set)]
    pub volume: f64,
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
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) -> Self {
        Candlestick {
            product_id,
            timeframe,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Trade {
    #[pyo3(get, set)]
    pub id: String,
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub price: f64,
    #[pyo3(get, set)]
    pub quantity: f64,
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
        price: f64,
        quantity: f64,
        side: String,
        timestamp: i64,
    ) -> Self {
        Trade {
            id,
            product_id,
            price,
            quantity,
            side,
            timestamp,
        }
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
    #[pyo3(get, set)]
    pub price: f64,
    #[pyo3(get, set)]
    pub quantity: f64,
    #[pyo3(get, set)]
    pub timestamp: i64,
    /// Trigger price for SL/TP conditional orders
    #[pyo3(get, set)]
    pub trigger_price: Option<f64>,
    /// Distance for trailing stop
    #[pyo3(get, set)]
    pub trailing_distance: Option<f64>,
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
        price: f64,
        quantity: f64,
        timestamp: i64,
        trigger_price: Option<f64>,
        trailing_distance: Option<f64>,
        linked_order_id: Option<String>,
    ) -> Self {
        Order {
            id,
            product_id,
            side,
            order_type,
            price,
            quantity,
            timestamp,
            trigger_price,
            trailing_distance,
            linked_order_id,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct FillEvent {
    #[pyo3(get, set)]
    pub order_id: String,
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub price: f64,
    #[pyo3(get, set)]
    pub quantity: f64,
    #[pyo3(get, set)]
    pub fee: f64,
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
        price: f64,
        quantity: f64,
        fee: f64,
        timestamp: i64,
        fill_type: String,
    ) -> Self {
        FillEvent {
            order_id,
            product_id,
            price,
            quantity,
            fee,
            timestamp,
            fill_type,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Position {
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub side: String, // "LONG", "SHORT", or "FLAT"
    #[pyo3(get, set)]
    pub quantity: f64,
    #[pyo3(get, set)]
    pub entry_price: f64,
    #[pyo3(get, set)]
    pub unrealized_pnl: f64,
}

#[pymethods]
impl Position {
    #[new]
    #[pyo3(signature = (product_id, side, quantity, entry_price, unrealized_pnl))]
    fn new(
        product_id: String,
        side: String,
        quantity: f64,
        entry_price: f64,
        unrealized_pnl: f64,
    ) -> Self {
        Position {
            product_id,
            side,
            quantity,
            entry_price,
            unrealized_pnl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candlestick_construction_and_fields() {
        let c = Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "15m".to_string(),
            timestamp: 1700000000,
            open: 50_000.0,
            high: 51_000.0,
            low: 49_000.0,
            close: 50_500.0,
            volume: 123.45,
        };
        assert_eq!(c.product_id, "BINANCE:BTCUSDT-PERP");
        assert_eq!(c.timeframe, "15m");
        assert_eq!(c.timestamp, 1700000000);
        assert!((c.open - 50_000.0).abs() < f64::EPSILON);
        assert!((c.high - 51_000.0).abs() < f64::EPSILON);
        assert!((c.low - 49_000.0).abs() < f64::EPSILON);
        assert!((c.close - 50_500.0).abs() < f64::EPSILON);
        assert!((c.volume - 123.45).abs() < f64::EPSILON);
    }

    #[test]
    fn test_candlestick_clone() {
        let c = Candlestick {
            product_id: "TEST".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1000,
            open: 1.0, high: 2.0, low: 0.5, close: 1.5, volume: 10.0,
        };
        let c2 = c.clone();
        assert_eq!(c.product_id, c2.product_id);
        assert!((c.open - c2.open).abs() < f64::EPSILON);
    }

    #[test]
    fn test_trade_construction() {
        let t = Trade {
            id: "t1".to_string(),
            product_id: "BINANCE:ETHUSDT-PERP".to_string(),
            price: 3_000.0,
            quantity: 2.5,
            side: "buy".to_string(),
            timestamp: 1700000000,
        };
        assert_eq!(t.id, "t1");
        assert_eq!(t.side, "buy");
        assert!((t.price - 3_000.0).abs() < f64::EPSILON);
        assert!((t.quantity - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_order_with_all_optional_fields() {
        let o = Order {
            id: "o1".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "LONG".to_string(),
            order_type: "TRAILING_STOP".to_string(),
            price: 0.0,
            quantity: 1.0,
            timestamp: 1000,
            trigger_price: Some(49_000.0),
            trailing_distance: Some(1_000.0),
            linked_order_id: Some("o2".to_string()),
        };
        assert_eq!(o.trigger_price, Some(49_000.0));
        assert_eq!(o.trailing_distance, Some(1_000.0));
        assert_eq!(o.linked_order_id.as_deref(), Some("o2"));
    }

    #[test]
    fn test_order_with_no_optional_fields() {
        let o = Order {
            id: "o3".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "SHORT".to_string(),
            order_type: "MARKET".to_string(),
            price: 50_000.0,
            quantity: 0.5,
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
            price: 50_000.0,
            quantity: 1.0,
            fee: 30.0,
            timestamp: 1000,
            fill_type: "STOP_LOSS".to_string(),
        };
        assert_eq!(f.fill_type, "STOP_LOSS");
        assert!((f.fee - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_position_construction_and_sides() {
        let long = Position {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 500.0,
        };
        assert_eq!(long.side, "LONG");

        let flat = Position {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            side: "FLAT".to_string(),
            quantity: 0.0,
            entry_price: 0.0,
            unrealized_pnl: 0.0,
        };
        assert_eq!(flat.side, "FLAT");
        assert!((flat.quantity - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_order_clone_independence() {
        let o1 = Order {
            id: "orig".to_string(),
            product_id: "TEST".to_string(),
            side: "LONG".to_string(),
            order_type: "LIMIT".to_string(),
            price: 100.0,
            quantity: 1.0,
            timestamp: 1000,
            trigger_price: Some(90.0),
            trailing_distance: None,
            linked_order_id: Some("linked".to_string()),
        };
        let mut o2 = o1.clone();
        o2.id = "cloned".to_string();
        o2.trigger_price = Some(80.0);

        assert_eq!(o1.id, "orig");
        assert_eq!(o1.trigger_price, Some(90.0));
        assert_eq!(o2.id, "cloned");
        assert_eq!(o2.trigger_price, Some(80.0));
    }
}
