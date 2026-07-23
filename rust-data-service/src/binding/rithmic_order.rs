use super::rithmic_ledger::PyLedgerOrder;
use crate::rithmic_ledger::{
    ledger::TransactionType,
    order::{
        BracketOrder, NewOrder, OrderAck, OrderEvent, OrderSide, OrderType, ProtectionLeg,
        ProtectionModification,
    },
    order_runtime::OrderRuntimeHandle,
};
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Mutex};

#[pyclass(frozen, name = "RithmicOrderAck")]
pub struct PyOrderAck {
    #[pyo3(get)]
    pub client_order_id: String,
    #[pyo3(get)]
    pub basket_id: String,
}

impl From<OrderAck> for PyOrderAck {
    fn from(value: OrderAck) -> Self {
        Self {
            client_order_id: value.client_order_id,
            basket_id: value.basket_id,
        }
    }
}

#[pyclass(frozen, name = "RithmicOrderEvent")]
pub struct PyOrderEvent {
    #[pyo3(get)]
    pub account_id: String,
    #[pyo3(get)]
    pub client_order_id: Option<String>,
    #[pyo3(get)]
    pub basket_id: String,
    #[pyo3(get)]
    pub original_basket_id: Option<String>,
    #[pyo3(get)]
    pub linked_basket_ids: Option<String>,
    #[pyo3(get)]
    pub exchange_order_id: Option<String>,
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub notification_type: String,
    #[pyo3(get)]
    pub transaction_type: String,
    #[pyo3(get)]
    pub quantity: String,
    #[pyo3(get)]
    pub price: Option<String>,
    #[pyo3(get)]
    pub trigger_price: Option<String>,
    #[pyo3(get)]
    pub price_type: Option<String>,
    #[pyo3(get)]
    pub bracket_type: Option<String>,
    #[pyo3(get)]
    pub last_fill_quantity: Option<String>,
    #[pyo3(get)]
    pub last_fill_price: Option<String>,
    #[pyo3(get)]
    pub cumulative_filled_quantity: Option<String>,
    #[pyo3(get)]
    pub cumulative_average_price: Option<String>,
    #[pyo3(get)]
    pub timestamp_ms: Option<i64>,
}

impl From<OrderEvent> for PyOrderEvent {
    fn from(value: OrderEvent) -> Self {
        Self {
            account_id: value.account.account_id,
            client_order_id: value.client_order_id,
            basket_id: value.basket_id,
            original_basket_id: value.original_basket_id,
            linked_basket_ids: value.linked_basket_ids,
            exchange_order_id: value.exchange_order_id,
            exchange: value.exchange,
            symbol: value.symbol,
            status: value.status,
            notification_type: value.notification_type,
            transaction_type: transaction_type_name(value.transaction_type).to_string(),
            quantity: value.quantity.to_string(),
            price: decimal_text(value.price),
            trigger_price: decimal_text(value.trigger_price),
            price_type: value.price_type,
            bracket_type: value.bracket_type,
            last_fill_quantity: decimal_text(value.last_fill_quantity),
            last_fill_price: decimal_text(value.last_fill_price),
            cumulative_filled_quantity: decimal_text(value.cumulative_filled_quantity),
            cumulative_average_price: decimal_text(value.cumulative_average_price),
            timestamp_ms: value.timestamp_ms,
        }
    }
}

#[pyclass(name = "RithmicOrderClient")]
pub struct PyOrderClient {
    runtime: Mutex<OrderRuntimeHandle>,
}

#[pymethods]
impl PyOrderClient {
    #[new]
    #[pyo3(signature = (profile, account_id=None))]
    fn new(py: Python<'_>, profile: String, account_id: Option<String>) -> PyResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let runtime = py
            .allow_threads(|| OrderRuntimeHandle::start(profile, account_id))
            .map_err(runtime_error)?;
        Ok(Self {
            runtime: Mutex::new(runtime),
        })
    }

    #[getter]
    fn is_connected(&self) -> PyResult<bool> {
        Ok(self.lock_runtime()?.is_connected())
    }

    /// Successful (re)connect count. A strictly higher value than a previously
    /// observed one means the order session reconnected in between, which the
    /// engine uses to trigger owned-order reconciliation.
    fn connection_generation(&self) -> PyResult<u64> {
        Ok(self.lock_runtime()?.connection_generation())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (client_order_id, exchange, symbol, quantity, side, order_type, price=None))]
    fn submit(
        &self,
        py: Python<'_>,
        client_order_id: String,
        exchange: String,
        symbol: String,
        quantity: &str,
        side: &str,
        order_type: &str,
        price: Option<&str>,
    ) -> PyResult<PyOrderAck> {
        let order = NewOrder {
            client_order_id,
            exchange,
            symbol,
            quantity: decimal(quantity, "quantity")?,
            price: price.map(|value| decimal(value, "price")).transpose()?,
            side: parse_side(side)?,
            order_type: parse_order_type(order_type)?,
        };
        let runtime = &self.runtime;
        py.allow_threads(|| {
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic order runtime lock is unavailable"))?
                .submit(order)
        })
        .map(PyOrderAck::from)
        .map_err(runtime_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        client_order_id,
        exchange,
        symbol,
        quantity,
        side,
        order_type,
        price=None,
        stop_ticks=None,
        target_ticks=None
    ))]
    fn submit_bracket(
        &self,
        py: Python<'_>,
        client_order_id: String,
        exchange: String,
        symbol: String,
        quantity: &str,
        side: &str,
        order_type: &str,
        price: Option<&str>,
        stop_ticks: Option<i32>,
        target_ticks: Option<i32>,
    ) -> PyResult<PyOrderAck> {
        let order = BracketOrder {
            entry: NewOrder {
                client_order_id,
                exchange,
                symbol,
                quantity: decimal(quantity, "quantity")?,
                price: price.map(|value| decimal(value, "price")).transpose()?,
                side: parse_side(side)?,
                order_type: parse_order_type(order_type)?,
            },
            stop_ticks,
            target_ticks,
        };
        let runtime = &self.runtime;
        py.allow_threads(|| {
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic order runtime lock is unavailable"))?
                .submit_bracket(order)
        })
        .map(PyOrderAck::from)
        .map_err(runtime_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (basket_id, exchange, symbol, quantity, leg_type, price))]
    fn modify_protection(
        &self,
        py: Python<'_>,
        basket_id: String,
        exchange: String,
        symbol: String,
        quantity: &str,
        leg_type: &str,
        price: &str,
    ) -> PyResult<bool> {
        let leg = match leg_type.trim().to_ascii_lowercase().as_str() {
            "stop_loss" => ProtectionLeg::StopLoss,
            "take_profit" => ProtectionLeg::TakeProfit,
            _ => return Err(PyValueError::new_err("unsupported Rithmic protection leg")),
        };
        let modification = ProtectionModification {
            basket_id,
            exchange,
            symbol,
            quantity: decimal(quantity, "quantity")?,
            leg,
            price: decimal(price, "price")?,
        };
        let runtime = &self.runtime;
        py.allow_threads(|| {
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic order runtime lock is unavailable"))?
                .modify(modification)
        })
        .map(|()| true)
        .map_err(runtime_error)
    }

    fn cancel(&self, py: Python<'_>, basket_id: String) -> PyResult<bool> {
        let runtime = &self.runtime;
        py.allow_threads(|| {
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic order runtime lock is unavailable"))?
                .cancel(basket_id)
        })
        .map(|()| true)
        .map_err(runtime_error)
    }

    fn lookup(
        &self,
        py: Python<'_>,
        client_order_id: String,
        exchange: String,
        symbol: String,
    ) -> PyResult<Option<PyLedgerOrder>> {
        let runtime = &self.runtime;
        py.allow_threads(|| {
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic order runtime lock is unavailable"))?
                .lookup(client_order_id, exchange, symbol)
        })
        .map(|snapshot| snapshot.map(PyLedgerOrder::from))
        .map_err(runtime_error)
    }

    fn poll_event(&self) -> PyResult<Option<PyOrderEvent>> {
        self.lock_runtime()?
            .try_next_event()
            .map(|event| event.map(PyOrderEvent::from))
            .map_err(runtime_error)
    }
}

impl PyOrderClient {
    fn lock_runtime(&self) -> PyResult<std::sync::MutexGuard<'_, OrderRuntimeHandle>> {
        self.runtime
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Rithmic order runtime lock is unavailable"))
    }
}

fn decimal(value: &str, field: &str) -> PyResult<Decimal> {
    Decimal::from_str(value)
        .map_err(|_| PyValueError::new_err(format!("invalid Rithmic order {field}")))
}

fn parse_side(value: &str) -> PyResult<OrderSide> {
    match value.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(PyValueError::new_err(
            "Rithmic order side must be buy or sell",
        )),
    }
}

fn parse_order_type(value: &str) -> PyResult<OrderType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "market" => Ok(OrderType::Market),
        "limit" => Ok(OrderType::Limit),
        _ => Err(PyValueError::new_err(
            "Rithmic order type must be market or limit",
        )),
    }
}

fn transaction_type_name(value: TransactionType) -> &'static str {
    match value {
        TransactionType::Buy => "BUY",
        TransactionType::Sell => "SELL",
        TransactionType::ShortSell => "SHORT_SELL",
    }
}

fn decimal_text(value: Option<Decimal>) -> Option<String> {
    value.map(|value| value.to_string())
}

fn runtime_error(error: anyhow::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{error:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_boundary_parses_only_supported_side_and_order_type() {
        assert_eq!(parse_side("BUY").unwrap(), OrderSide::Buy);
        assert_eq!(parse_side("sell").unwrap(), OrderSide::Sell);
        assert!(parse_side("long").is_err());
        assert_eq!(parse_order_type("market").unwrap(), OrderType::Market);
        assert_eq!(parse_order_type("LIMIT").unwrap(), OrderType::Limit);
        assert!(parse_order_type("stop").is_err());
    }

    #[test]
    fn python_boundary_keeps_decimal_values_as_strings() {
        assert_eq!(
            decimal("20000.25", "price").unwrap().to_string(),
            "20000.25"
        );
        assert!(decimal("nan", "price").is_err());
    }
}
