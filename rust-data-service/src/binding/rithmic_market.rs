use crate::rithmic_ledger::{
    last_trade_snapshot,
    market::{Aggressor, LastTradeUpdate},
    profile_lock::ProfileLease,
};
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};
use std::time::Duration;

/// One LastTrade received from an explicitly exclusive offline TICKER session.
#[pyclass(frozen, name = "RithmicLastTradeSnapshot")]
pub struct PyLastTradeSnapshot {
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub price: String,
    #[pyo3(get)]
    pub quantity: String,
    #[pyo3(get)]
    pub aggressor: Option<String>,
    #[pyo3(get)]
    pub timestamp_ms: i64,
    #[pyo3(get)]
    pub is_snapshot: bool,
}

#[pyfunction]
#[pyo3(signature = (profile, exchange, symbol, *, exclusive_session, timeout_seconds=15))]
pub fn rithmic_offline_last_trade_snapshot(
    py: Python<'_>,
    profile: &str,
    exchange: &str,
    symbol: &str,
    exclusive_session: bool,
    timeout_seconds: u64,
) -> PyResult<PyLastTradeSnapshot> {
    if !exclusive_session {
        return Err(PyValueError::new_err(
            "exclusive_session=True is required after stopping all other Rithmic TICKER clients",
        ));
    }
    if !(1..=120).contains(&timeout_seconds) {
        return Err(PyValueError::new_err(
            "timeout_seconds must be between 1 and 120",
        ));
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    py.allow_threads(|| {
        let _lease = ProfileLease::acquire(profile).map_err(runtime_error)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        runtime
            .block_on(last_trade_snapshot::run(
                profile,
                exchange,
                symbol,
                Duration::from_secs(timeout_seconds),
            ))
            .map(PyLastTradeSnapshot::from)
            .map_err(runtime_error)
    })
}

impl From<LastTradeUpdate> for PyLastTradeSnapshot {
    fn from(trade: LastTradeUpdate) -> Self {
        Self {
            exchange: trade.exchange,
            symbol: trade.symbol,
            price: trade.price.to_string(),
            quantity: trade.quantity.to_string(),
            aggressor: trade.aggressor.map(|aggressor| match aggressor {
                Aggressor::Buy => "BUY".to_string(),
                Aggressor::Sell => "SELL".to_string(),
            }),
            timestamp_ms: trade.timestamp,
            is_snapshot: trade.is_snapshot,
        }
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn python_snapshot_preserves_decimal_text_and_metadata() {
        let snapshot = PyLastTradeSnapshot::from(LastTradeUpdate {
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            price: dec!(29784.75),
            quantity: dec!(2),
            aggressor: Some(Aggressor::Sell),
            timestamp: 1_800_000_001_234,
            is_snapshot: true,
        });

        assert_eq!(snapshot.price, "29784.75");
        assert_eq!(snapshot.quantity, "2");
        assert_eq!(snapshot.aggressor.as_deref(), Some("SELL"));
        assert_eq!(snapshot.timestamp_ms, 1_800_000_001_234);
        assert!(snapshot.is_snapshot);
    }

    #[test]
    fn offline_snapshot_requires_explicit_exclusive_session_confirmation() {
        Python::with_gil(|py| {
            let error =
                match rithmic_offline_last_trade_snapshot(py, "unused", "CME", "NQU6", false, 15) {
                    Ok(_) => panic!("offline snapshot must require explicit confirmation"),
                    Err(error) => error,
                };
            assert!(error.to_string().contains("exclusive_session=True"));
        });
    }
}
