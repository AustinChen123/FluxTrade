use crate::rithmic_ledger::{
    ledger::{
        AccountSummarySnapshot, FillSnapshot, InstrumentPositionSnapshot, OrderSnapshot,
        TransactionType,
    },
    ledger_runtime::{RecoveryQuery, RemoteLedgerSnapshot},
    profile_lock::ProfileLease,
};
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};

#[pyclass(frozen, name = "RithmicLedgerOrder")]
#[derive(Clone)]
pub struct PyLedgerOrder {
    #[pyo3(get)]
    pub client_order_id: Option<String>,
    #[pyo3(get)]
    pub window_name: Option<String>,
    #[pyo3(get)]
    pub originator_window_name: Option<String>,
    #[pyo3(get)]
    pub exchange_order_id: Option<String>,
    #[pyo3(get)]
    pub basket_id: String,
    #[pyo3(get)]
    pub original_basket_id: Option<String>,
    #[pyo3(get)]
    pub linked_basket_ids: Option<String>,
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub notification_type: Option<String>,
    #[pyo3(get)]
    pub completion_reason: Option<String>,
    #[pyo3(get)]
    pub report_text: Option<String>,
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

#[pyclass(frozen, name = "RithmicLedgerFill")]
#[derive(Clone)]
pub struct PyLedgerFill {
    #[pyo3(get)]
    pub basket_id: String,
    #[pyo3(get)]
    pub exchange_order_id: Option<String>,
    #[pyo3(get)]
    pub fill_id: String,
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub transaction_type: String,
    #[pyo3(get)]
    pub fill_quantity: String,
    #[pyo3(get)]
    pub fill_price: String,
    #[pyo3(get)]
    pub cumulative_filled_quantity: Option<String>,
    #[pyo3(get)]
    pub cumulative_average_price: Option<String>,
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
    order_history: Vec<PyLedgerOrder>,
    fills: Vec<PyLedgerFill>,
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
    fn order_history(&self) -> Vec<PyLedgerOrder> {
        self.order_history.clone()
    }

    #[getter]
    fn fills(&self) -> Vec<PyLedgerFill> {
        self.fills.clone()
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
#[pyo3(signature = (profile, account_id=None, *, recovery_basket_ids=None, fill_start_index=None, fill_finish_index=None))]
pub fn rithmic_ledger_snapshot(
    py: Python<'_>,
    profile: &str,
    account_id: Option<&str>,
    recovery_basket_ids: Option<Vec<String>>,
    fill_start_index: Option<i32>,
    fill_finish_index: Option<i32>,
) -> PyResult<PyLedgerSnapshot> {
    let recovery = match (
        recovery_basket_ids.as_ref(),
        fill_start_index,
        fill_finish_index,
    ) {
        (None, None, None) => None,
        (Some(basket_ids), Some(start), Some(finish)) => Some(RecoveryQuery {
            basket_ids,
            fill_start_index: start,
            fill_finish_index: finish,
        }),
        _ => {
            return Err(PyValueError::new_err(
                "recovery_basket_ids, fill_start_index, and fill_finish_index must be provided together",
            ));
        }
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    py.allow_threads(|| {
        let _lease = ProfileLease::acquire(profile).map_err(runtime_error)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        let snapshot = match recovery {
            Some(recovery) => {
                runtime.block_on(crate::rithmic_ledger::ledger_runtime::run_with_recovery(
                    profile,
                    account_id,
                    Some(recovery),
                ))
            }
            None => runtime.block_on(crate::rithmic_ledger::ledger_runtime::run(
                profile, account_id,
            )),
        };
        snapshot.map(PyLedgerSnapshot::from).map_err(runtime_error)
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
            order_history: snapshot
                .order_history
                .into_iter()
                .map(PyLedgerOrder::from)
                .collect(),
            fills: snapshot.fills.into_iter().map(PyLedgerFill::from).collect(),
            positions: snapshot
                .positions
                .into_iter()
                .map(PyLedgerPosition::from)
                .collect(),
            account_summary: snapshot.account_summary.map(PyLedgerAccountSummary::from),
        }
    }
}

impl From<FillSnapshot> for PyLedgerFill {
    fn from(fill: FillSnapshot) -> Self {
        Self {
            basket_id: fill.basket_id,
            exchange_order_id: fill.exchange_order_id,
            fill_id: fill.fill_id,
            exchange: fill.exchange,
            symbol: fill.symbol,
            transaction_type: fill.transaction_type,
            fill_quantity: fill.fill_quantity.to_string(),
            fill_price: fill.fill_price.to_string(),
            cumulative_filled_quantity: fill
                .cumulative_filled_quantity
                .map(|value| value.to_string()),
            cumulative_average_price: fill.cumulative_average_price.map(|value| value.to_string()),
            timestamp_ms: fill.timestamp_ms,
        }
    }
}

impl From<OrderSnapshot> for PyLedgerOrder {
    fn from(order: OrderSnapshot) -> Self {
        Self {
            client_order_id: order.client_order_id,
            window_name: order.window_name,
            originator_window_name: order.originator_window_name,
            exchange_order_id: order.exchange_order_id,
            basket_id: order.basket_id,
            original_basket_id: order.original_basket_id,
            linked_basket_ids: order.linked_basket_ids,
            exchange: order.exchange,
            symbol: order.symbol,
            status: order.status,
            notification_type: order.notification_type,
            completion_reason: order.completion_reason,
            report_text: order.report_text,
            transaction_type: match order.transaction_type {
                TransactionType::Buy => "BUY",
                TransactionType::Sell => "SELL",
                TransactionType::ShortSell => "SHORT_SELL",
            }
            .to_string(),
            quantity: order.quantity.to_string(),
            price: order.price.map(|value| value.to_string()),
            trigger_price: order.trigger_price.map(|value| value.to_string()),
            price_type: order.price_type,
            bracket_type: order.bracket_type,
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
                window_name: Some("window".to_string()),
                originator_window_name: Some("originator".to_string()),
                basket_id: "BASKET".to_string(),
                original_basket_id: None,
                linked_basket_ids: None,
                exchange_order_id: Some("EXCHANGE".to_string()),
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                status: "OPEN".to_string(),
                notification_type: Some("STATUS".to_string()),
                completion_reason: None,
                report_text: None,
                transaction_type: TransactionType::ShortSell,
                quantity: dec!(2),
                price: Some(dec!(20001.25)),
                trigger_price: Some(dec!(19999.25)),
                price_type: Some("limit".to_string()),
                bracket_type: Some("target_and_stop_static".to_string()),
                filled_quantity: Some(dec!(1)),
                unfilled_quantity: Some(dec!(1)),
                average_fill_price: Some(dec!(20000.25)),
                timestamp_ms: Some(1_700_000_000_123),
            }],
            order_history: Vec::new(),
            fills: vec![FillSnapshot {
                account: account.clone(),
                basket_id: "BASKET".to_string(),
                exchange_order_id: Some("EXCHANGE".to_string()),
                fill_id: "FILL".to_string(),
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                transaction_type: "BUY".to_string(),
                fill_quantity: dec!(1),
                fill_price: dec!(20000.25),
                cumulative_filled_quantity: Some(dec!(1)),
                cumulative_average_price: Some(dec!(20000.25)),
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
        assert_eq!(snapshot.orders[0].window_name.as_deref(), Some("window"));
        assert_eq!(
            snapshot.orders[0].originator_window_name.as_deref(),
            Some("originator")
        );
        assert_eq!(snapshot.orders[0].basket_id, "BASKET");
        assert_eq!(
            snapshot.orders[0].notification_type.as_deref(),
            Some("STATUS")
        );
        assert_eq!(snapshot.orders[0].transaction_type, "SHORT_SELL");
        assert_eq!(snapshot.orders[0].price.as_deref(), Some("20001.25"));
        assert_eq!(
            snapshot.orders[0].trigger_price.as_deref(),
            Some("19999.25")
        );
        assert_eq!(snapshot.orders[0].price_type.as_deref(), Some("limit"));
        assert_eq!(
            snapshot.orders[0].bracket_type.as_deref(),
            Some("target_and_stop_static")
        );
        assert_eq!(snapshot.fills[0].fill_quantity, "1");
        assert_eq!(snapshot.fills[0].fill_price, "20000.25");
        assert_eq!(snapshot.fills[0].transaction_type, "BUY");
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
    fn snapshot_calls_are_process_serialized_by_profile() {
        let first = ProfileLease::acquire("snapshot-serialization-test").unwrap();
        assert!(ProfileLease::acquire("snapshot-serialization-test").is_err());
        assert!(ProfileLease::acquire("another-profile").is_ok());
        drop(first);
        assert!(ProfileLease::acquire("snapshot-serialization-test").is_ok());
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
