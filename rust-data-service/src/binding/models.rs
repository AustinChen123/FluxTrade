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
