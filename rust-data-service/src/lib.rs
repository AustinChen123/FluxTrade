use ::pyo3::prelude::*;

mod aggregator;
mod binding;
mod model;
#[cfg(feature = "rithmic")]
mod rithmic_ledger;

/// A Python module implemented in Rust.
#[pymodule]
fn fluxtrade_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Data Models
    m.add_class::<binding::models::Candlestick>()?;
    m.add_class::<binding::models::Order>()?;
    m.add_class::<binding::models::Trade>()?;
    m.add_class::<binding::models::FillEvent>()?;
    m.add_class::<binding::models::Position>()?;
    m.add_class::<binding::scaled::ScaledCandlestick>()?;
    m.add_class::<binding::aggregator::PyCandleAggregator>()?;

    #[cfg(feature = "rithmic")]
    {
        m.add_class::<binding::rithmic_ledger::PyLedgerOrder>()?;
        m.add_class::<binding::rithmic_ledger::PyLedgerFill>()?;
        m.add_class::<binding::rithmic_ledger::PyLedgerPosition>()?;
        m.add_class::<binding::rithmic_ledger::PyLedgerAccountSummary>()?;
        m.add_class::<binding::rithmic_ledger::PyLedgerSnapshot>()?;
        m.add_class::<binding::rithmic_market::PyPriceSnapshot>()?;
        m.add_class::<binding::rithmic_order::PyOrderAck>()?;
        m.add_class::<binding::rithmic_order::PyOrderEvent>()?;
        m.add_class::<binding::rithmic_order::PyOrderClient>()?;
        m.add_function(wrap_pyfunction!(
            binding::rithmic_ledger::rithmic_ledger_snapshot,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            binding::rithmic_market::rithmic_price_snapshot,
            m
        )?)?;
    }

    // Core Engine
    m.add_class::<binding::matcher::PyMatchingEngine>()?;

    Ok(())
}

#[cfg(all(test, feature = "rithmic"))]
mod tests {
    use super::*;

    #[test]
    fn rithmic_ledger_python_surface_is_registered() {
        Python::with_gil(|py| {
            let module = PyModule::new(py, "fluxtrade_core").unwrap();
            fluxtrade_core(&module).unwrap();

            for name in [
                "RithmicLedgerOrder",
                "RithmicLedgerFill",
                "RithmicLedgerPosition",
                "RithmicLedgerAccountSummary",
                "RithmicLedgerSnapshot",
                "rithmic_ledger_snapshot",
                "RithmicPriceSnapshot",
                "rithmic_price_snapshot",
                "RithmicOrderAck",
                "RithmicOrderEvent",
                "RithmicOrderClient",
            ] {
                assert!(
                    module.hasattr(name).unwrap(),
                    "missing Python export: {name}"
                );
            }
        });
    }
}
