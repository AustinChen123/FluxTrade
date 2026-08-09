use crate::rithmic_ledger::{
    ledger::{
        AccountSummarySnapshot, FillSnapshot, InstrumentPositionSnapshot, OrderSnapshot,
        TransactionType,
    },
    ledger_runtime::{LedgerSnapshotFailure, RecoveryQuery, RemoteLedgerSnapshot},
    profile_lock::ProfileLease,
};
use anyhow::Context;
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};

const PROFILE_LEASE_FAILURE: LedgerSnapshotFailure = LedgerSnapshotFailure::new(
    "profile_lease",
    "profile_lease_failed",
    "profile lease failed",
);
const RUNTIME_INITIALIZATION_FAILURE: LedgerSnapshotFailure = LedgerSnapshotFailure::new(
    "runtime_initialization",
    "runtime_initialization_failed",
    "runtime initialization failed",
);
const UNCLASSIFIED_FAILURE: LedgerSnapshotFailure = LedgerSnapshotFailure::new(
    "unclassified_internal",
    "unclassified_ledger_snapshot_failure",
    "ledger snapshot failed before safe classification",
);

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
    let result = py.allow_threads(|| -> anyhow::Result<PyLedgerSnapshot> {
        let _lease = ProfileLease::acquire(profile).context(PROFILE_LEASE_FAILURE)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context(RUNTIME_INITIALIZATION_FAILURE)?;
        match recovery {
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
        }
        .map(PyLedgerSnapshot::from)
    });
    result.map_err(|error| runtime_error(py, error))
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

fn runtime_error(py: Python<'_>, error: anyhow::Error) -> PyErr {
    let failure = error
        .downcast_ref::<LedgerSnapshotFailure>()
        .copied()
        .unwrap_or(UNCLASSIFIED_FAILURE);
    let target = PyRuntimeError::new_err("Rithmic ledger snapshot failed")
        .into_value(py)
        .into_bound(py)
        .into_any();
    project_runtime_error_target(failure, target)
}

fn project_runtime_error_target(failure: LedgerSnapshotFailure, target: Bound<'_, PyAny>) -> PyErr {
    if set_diagnostic_attributes(&target, failure).is_err() {
        return PyRuntimeError::new_err("Rithmic ledger snapshot failed");
    }
    PyErr::from_value(target)
}

fn set_diagnostic_attributes(
    target: &Bound<'_, PyAny>,
    failure: LedgerSnapshotFailure,
) -> PyResult<()> {
    let [stage, stable_error_code, safe_cause] = failure.safe_fields();
    target
        .setattr("stage", stage)
        .and_then(|_| target.setattr("stable_error_code", stable_error_code))
        .and_then(|_| target.setattr("safe_cause", safe_cause))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rithmic_ledger::ledger::{Account, AccountIdentity};
    use rust_decimal_macros::dec;

    #[test]
    fn runtime_error_projects_only_the_independent_safe_ledger() {
        const SENTINELS: [&str; 8] = [
            "PROVIDER", "RP", "ACCOUNT", "BASKET", "STATUS", "PROFILE", "URL", "USER",
        ];
        const EXPECTED: &str = "profile_lease|profile_lease_failed|profile lease failed
runtime_initialization|runtime_initialization_failed|runtime initialization failed
request_validation|invalid_ledger_snapshot_request|ledger snapshot request validation failed
order_config|order_config_failed|ORDER config failed
order_connect|order_connect_failed|ORDER connect failed
order_heartbeat|order_heartbeat_failed|ORDER heartbeat failed
order_login_info|order_login_info_failed|ORDER login info failed
order_account_list|order_account_list_failed|ORDER account list failed
order_snapshot|order_snapshot_failed|ORDER snapshot failed
order_history|order_history_failed|ORDER history failed
fill_history|fill_history_failed|fill history failed
pnl_config|pnl_config_failed|PNL config failed
pnl_connect|pnl_connect_failed|PNL connect failed
pnl_heartbeat|pnl_heartbeat_failed|PNL heartbeat failed
pnl_request|pnl_request_failed|PNL request failed
pnl_snapshot|pnl_snapshot_failed|PNL snapshot failed
unclassified_internal|unclassified_ledger_snapshot_failure|ledger snapshot failed before safe classification";
        let stages = [
            PROFILE_LEASE_FAILURE,
            RUNTIME_INITIALIZATION_FAILURE,
            LedgerSnapshotFailure::REQUEST_VALIDATION,
            LedgerSnapshotFailure::ORDER_CONFIG,
            LedgerSnapshotFailure::ORDER_CONNECT,
            LedgerSnapshotFailure::ORDER_HEARTBEAT,
            LedgerSnapshotFailure::ORDER_LOGIN_INFO,
            LedgerSnapshotFailure::ORDER_ACCOUNT_LIST,
            LedgerSnapshotFailure::ORDER_SNAPSHOT,
            LedgerSnapshotFailure::ORDER_HISTORY,
            LedgerSnapshotFailure::FILL_HISTORY,
            LedgerSnapshotFailure::PNL_CONFIG,
            LedgerSnapshotFailure::PNL_CONNECT,
            LedgerSnapshotFailure::PNL_HEARTBEAT,
            LedgerSnapshotFailure::PNL_REQUEST,
            LedgerSnapshotFailure::PNL_SNAPSHOT,
            UNCLASSIFIED_FAILURE,
        ];
        assert_eq!(stages.len(), 17);
        assert_eq!(EXPECTED.lines().count(), 17);
        Python::with_gil(|py| {
            for (index, (stage, expected)) in stages.into_iter().zip(EXPECTED.lines()).enumerate() {
                let source = anyhow::Error::new(std::io::Error::other(
                    "provider=PROVIDER rp_code=RP account=ACCOUNT basket=BASKET status=STATUS profile=PROFILE URL=URL user=USER",
                ));
                let source = if index == 16 {
                    source
                } else {
                    source.context(stage)
                };
                assert!(source.downcast_ref::<std::io::Error>().is_some());
                assert_eq!(source.chain().count(), if index == 16 { 1 } else { 2 });
                let error = runtime_error(py, source);
                let value = error.value(py);
                assert!(value.is_instance_of::<PyRuntimeError>());
                assert_eq!(value.get_type().name().unwrap(), "RuntimeError");
                let message = value.to_string();
                let args = value
                    .getattr("args")
                    .unwrap()
                    .extract::<(String,)>()
                    .unwrap();
                assert_eq!(message, "Rithmic ledger snapshot failed");
                assert_eq!(args, ("Rithmic ledger snapshot failed".to_string(),));
                for sentinel in SENTINELS {
                    assert!(!message.contains(sentinel));
                    assert!(!args.0.contains(sentinel));
                }
                let expected: Vec<_> = expected.split('|').collect();
                for (attribute, expected) in ["stage", "stable_error_code", "safe_cause"]
                    .into_iter()
                    .zip(expected)
                {
                    let actual = value
                        .getattr(attribute)
                        .unwrap()
                        .extract::<String>()
                        .unwrap();
                    assert_eq!(actual, expected);
                    for sentinel in SENTINELS {
                        assert!(!actual.contains(sentinel));
                    }
                }
            }
        });
    }

    #[test]
    fn rejected_diagnostic_target_returns_fixed_runtime_error() {
        Python::with_gil(|py| {
            let error =
                project_runtime_error_target(PROFILE_LEASE_FAILURE, py.None().into_bound(py));
            let value = error.value(py);
            assert!(value.is_instance_of::<PyRuntimeError>());
            assert_eq!(value.to_string(), "Rithmic ledger snapshot failed");
            assert_eq!(
                value
                    .getattr("args")
                    .unwrap()
                    .extract::<(String,)>()
                    .unwrap(),
                ("Rithmic ledger snapshot failed".to_string(),)
            );
        });
    }

    #[test]
    fn profile_lease_failure_uses_public_safe_boundary() {
        let profile = "snapshot-diagnostic-profile-sentinel";
        let lease = ProfileLease::acquire(profile).unwrap();
        Python::with_gil(|py| {
            let error = rithmic_ledger_snapshot(py, profile, None, None, None, None)
                .err()
                .expect("held profile lease must reject the snapshot");
            let value = error.value(py);
            assert_eq!(value.to_string(), "Rithmic ledger snapshot failed");
            assert_eq!(
                value
                    .getattr("args")
                    .unwrap()
                    .extract::<(String,)>()
                    .unwrap(),
                ("Rithmic ledger snapshot failed".to_string(),)
            );
            assert!(!value.to_string().contains(profile));
            assert!(!value.getattr("args").unwrap().to_string().contains(profile));
            for (attribute, expected) in [
                ("stage", "profile_lease"),
                ("stable_error_code", "profile_lease_failed"),
                ("safe_cause", "profile lease failed"),
            ] {
                let actual = value
                    .getattr(attribute)
                    .unwrap()
                    .extract::<String>()
                    .unwrap();
                assert_eq!(actual, expected);
                assert!(!actual.contains(profile));
            }
        });
        drop(lease);
    }

    #[test]
    fn production_seams_keep_gil_release_runtime_context_and_fallback() {
        let source = include_str!("rithmic_ledger.rs");
        let allow_threads = source
            .split_once("let result = py.allow_threads")
            .unwrap()
            .1
            .split_once("\n    });\n")
            .unwrap()
            .0;
        assert!(allow_threads.contains(".map(PyLedgerSnapshot::from)"));
        assert!(allow_threads
            .contains(".build()\n            .context(RUNTIME_INITIALIZATION_FAILURE)?"));
        let runtime_error = source.split_once("fn runtime_error").unwrap().1;
        assert!(runtime_error.contains("project_runtime_error_target(failure, target)"));
    }

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
