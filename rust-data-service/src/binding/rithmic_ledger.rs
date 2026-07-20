use crate::rithmic_ledger::{
    ledger::{AccountSummarySnapshot, InstrumentPositionSnapshot, OrderSnapshot, TransactionType},
    ledger_runtime::RemoteLedgerSnapshot,
};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::Mutex;

// Recovery snapshots are low frequency. Use per-profile locks only if concurrent
// account recovery becomes necessary.
static SNAPSHOT_LOCK: Mutex<()> = Mutex::new(());

#[pyclass(frozen, name = "RithmicLedgerOrder")]
#[derive(Clone)]
pub struct PyLedgerOrder {
    #[pyo3(get)]
    pub client_order_id: Option<String>,
    #[pyo3(get)]
    pub exchange_order_id: Option<String>,
    #[pyo3(get)]
    pub basket_id: String,
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub transaction_type: String,
    #[pyo3(get)]
    pub quantity: String,
    #[pyo3(get)]
    pub filled_quantity: Option<String>,
    #[pyo3(get)]
    pub unfilled_quantity: Option<String>,
    #[pyo3(get)]
    pub average_fill_price: Option<String>,
    #[pyo3(get)]
    pub timestamp_ms: Option<i64>,
}

#[pyclass(frozen, name = "RithmicLedgerPosition")]
#[derive(Clone)]
pub struct PyLedgerPosition {
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub net_quantity: String,
    #[pyo3(get)]
    pub average_open_fill_price: Option<String>,
    #[pyo3(get)]
    pub open_pnl: Option<String>,
    #[pyo3(get)]
    pub day_pnl: Option<String>,
    #[pyo3(get)]
    pub timestamp_ms: Option<i64>,
}

#[pyclass(frozen, name = "RithmicLedgerAccountSummary")]
#[derive(Clone)]
pub struct PyLedgerAccountSummary {
    #[pyo3(get)]
    pub account_balance: Option<String>,
    #[pyo3(get)]
    pub cash_on_hand: Option<String>,
    #[pyo3(get)]
    pub available_buying_power: Option<String>,
    #[pyo3(get)]
    pub day_pnl: Option<String>,
    #[pyo3(get)]
    pub net_quantity: Option<String>,
    #[pyo3(get)]
    pub timestamp_ms: Option<i64>,
}

#[pyclass(frozen, name = "RithmicLedgerSnapshot")]
pub struct PyLedgerSnapshot {
    #[pyo3(get)]
    pub account_id: String,
    #[pyo3(get)]
    pub account_currency: Option<String>,
    orders: Vec<PyLedgerOrder>,
    positions: Vec<PyLedgerPosition>,
    account_summary: Option<PyLedgerAccountSummary>,
}

#[pymethods]
impl PyLedgerSnapshot {
    #[getter]
    fn orders(&self) -> Vec<PyLedgerOrder> {
        self.orders.clone()
    }

    #[getter]
    fn positions(&self) -> Vec<PyLedgerPosition> {
        self.positions.clone()
    }

    #[getter]
    fn account_summary(&self) -> Option<PyLedgerAccountSummary> {
        self.account_summary.clone()
    }
}

#[pyfunction]
#[pyo3(signature = (profile, account_id=None))]
pub fn rithmic_ledger_snapshot(
    py: Python<'_>,
    profile: &str,
    account_id: Option<&str>,
) -> PyResult<PyLedgerSnapshot> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    py.allow_threads(|| {
        let _guard = SNAPSHOT_LOCK
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Rithmic ledger snapshot lock is unavailable"))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        runtime
            .block_on(crate::rithmic_ledger::ledger_runtime::run(
                profile, account_id,
            ))
            .map(PyLedgerSnapshot::from)
            .map_err(runtime_error)
    })
}

impl From<RemoteLedgerSnapshot> for PyLedgerSnapshot {
    fn from(snapshot: RemoteLedgerSnapshot) -> Self {
        Self {
            account_id: snapshot.account.identity.account_id,
            account_currency: snapshot.account.currency,
            orders: snapshot
                .orders
                .into_iter()
                .map(PyLedgerOrder::from)
                .collect(),
            positions: snapshot
                .positions
                .into_iter()
                .map(PyLedgerPosition::from)
                .collect(),
            account_summary: snapshot.account_summary.map(PyLedgerAccountSummary::from),
        }
    }
}

impl From<OrderSnapshot> for PyLedgerOrder {
    fn from(order: OrderSnapshot) -> Self {
        Self {
            client_order_id: order.client_order_id,
            exchange_order_id: order.exchange_order_id,
            basket_id: order.basket_id,
            exchange: order.exchange,
            symbol: order.symbol,
            status: order.status,
            transaction_type: match order.transaction_type {
                TransactionType::Buy => "BUY",
                TransactionType::Sell => "SELL",
                TransactionType::ShortSell => "SHORT_SELL",
            }
            .to_string(),
            quantity: order.quantity.to_string(),
            filled_quantity: order.filled_quantity.map(|value| value.to_string()),
            unfilled_quantity: order.unfilled_quantity.map(|value| value.to_string()),
            average_fill_price: order.average_fill_price.map(|value| value.to_string()),
            timestamp_ms: order.timestamp_ms,
        }
    }
}

impl From<InstrumentPositionSnapshot> for PyLedgerPosition {
    fn from(position: InstrumentPositionSnapshot) -> Self {
        Self {
            exchange: position.exchange,
            symbol: position.symbol,
            net_quantity: position.net_quantity.to_string(),
            average_open_fill_price: position
                .average_open_fill_price
                .map(|value| value.to_string()),
            open_pnl: position.open_pnl.map(|value| value.to_string()),
            day_pnl: position.day_pnl.map(|value| value.to_string()),
            timestamp_ms: position.timestamp_ms,
        }
    }
}

impl From<AccountSummarySnapshot> for PyLedgerAccountSummary {
    fn from(summary: AccountSummarySnapshot) -> Self {
        Self {
            account_balance: summary.account_balance.map(|value| value.to_string()),
            cash_on_hand: summary.cash_on_hand.map(|value| value.to_string()),
            available_buying_power: summary
                .available_buying_power
                .map(|value| value.to_string()),
            day_pnl: summary.day_pnl.map(|value| value.to_string()),
            net_quantity: summary.net_quantity.map(|value| value.to_string()),
            timestamp_ms: summary.timestamp_ms,
        }
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rithmic_ledger::ledger::{Account, AccountIdentity};
    use rust_decimal_macros::dec;
    use std::sync::TryLockError;

    fn account() -> AccountIdentity {
        AccountIdentity {
            fcm_id: "FCM".to_string(),
            ib_id: "IB".to_string(),
            account_id: "ACCOUNT".to_string(),
        }
    }

    #[test]
    fn mapping_preserves_decimal_strings_and_external_orders() {
        let account = account();
        let snapshot = PyLedgerSnapshot::from(RemoteLedgerSnapshot {
            account: Account {
                identity: account.clone(),
                name: None,
                currency: Some("USD".to_string()),
            },
            orders: vec![OrderSnapshot {
                account: account.clone(),
                client_order_id: None,
                basket_id: "BASKET".to_string(),
                exchange_order_id: Some("EXCHANGE".to_string()),
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                status: "OPEN".to_string(),
                transaction_type: TransactionType::ShortSell,
                quantity: dec!(2),
                filled_quantity: Some(dec!(1)),
                unfilled_quantity: Some(dec!(1)),
                average_fill_price: Some(dec!(20000.25)),
                timestamp_ms: Some(1_700_000_000_123),
            }],
            positions: vec![InstrumentPositionSnapshot {
                account: account.clone(),
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                net_quantity: -1,
                average_open_fill_price: Some(dec!(20000.25)),
                open_pnl: Some(dec!(-12.50)),
                day_pnl: Some(dec!(25.75)),
                timestamp_ms: None,
            }],
            account_summary: Some(AccountSummarySnapshot {
                account,
                account_balance: Some(dec!(25000.10)),
                cash_on_hand: None,
                available_buying_power: Some(dec!(1000.25)),
                day_pnl: Some(dec!(-12.50)),
                net_quantity: Some(-1),
                timestamp_ms: None,
            }),
        });

        assert_eq!(snapshot.account_id, "ACCOUNT");
        assert_eq!(snapshot.account_currency.as_deref(), Some("USD"));
        assert_eq!(snapshot.orders[0].client_order_id, None);
        assert_eq!(snapshot.orders[0].basket_id, "BASKET");
        assert_eq!(snapshot.orders[0].transaction_type, "SHORT_SELL");
        assert_eq!(
            snapshot.orders[0].average_fill_price.as_deref(),
            Some("20000.25")
        );
        assert_eq!(snapshot.positions[0].net_quantity, "-1");
        assert_eq!(snapshot.positions[0].open_pnl.as_deref(), Some("-12.50"));
        assert_eq!(
            snapshot
                .account_summary
                .as_ref()
                .and_then(|summary| summary.account_balance.as_deref()),
            Some("25000.10")
        );
    }

    #[test]
    fn snapshot_calls_are_process_serialized() {
        let guard = SNAPSHOT_LOCK.lock().unwrap();
        assert!(matches!(
            SNAPSHOT_LOCK.try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        drop(guard);
        assert!(SNAPSHOT_LOCK.try_lock().is_ok());
    }

    #[test]
    #[ignore = "requires live Rithmic credentials"]
    fn live_python_boundary_returns_remote_snapshot() {
        let profile = std::env::var("RITHMIC_PYO3_LIVE_PROFILE")
            .expect("RITHMIC_PYO3_LIVE_PROFILE is required");
        Python::with_gil(|py| {
            let module = PyModule::new(py, "fluxtrade_core").unwrap();
            crate::fluxtrade_core(&module).unwrap();
            let snapshot = module
                .getattr("rithmic_ledger_snapshot")
                .unwrap()
                .call1((profile,))
                .unwrap();

            let account_id: String = snapshot.getattr("account_id").unwrap().extract().unwrap();
            assert!(!account_id.trim().is_empty());
            assert!(snapshot.getattr("orders").unwrap().len().is_ok());
            assert!(snapshot.getattr("positions").unwrap().len().is_ok());
        });
    }
}
