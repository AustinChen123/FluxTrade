use ::pyo3::prelude::*;

mod binding;

/// A Python module implemented in Rust.
#[pymodule]
fn fluxtrade_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Data Models
    m.add_class::<binding::models::Candlestick>()?;
    m.add_class::<binding::models::Order>()?;
    m.add_class::<binding::models::Trade>()?;
    m.add_class::<binding::models::FillEvent>()?;
    m.add_class::<binding::models::Position>()?;
    
    // Core Engine
    m.add_class::<binding::matcher::PyMatchingEngine>()?;
    
    Ok(())
}
