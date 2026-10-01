//! Opaque synchronous Python boundary; financial ownership stays in Session.
use super::*;
use pyo3::{
    create_exception,
    exceptions::{PyLookupError, PyRuntimeError, PyValueError},
    prelude::*,
    types::PyString,
};
create_exception!(fluxtrade_core, ScenarioReplayInputError, PyValueError);
create_exception!(fluxtrade_core, ScenarioReplayLookupError, PyLookupError);
create_exception!(fluxtrade_core, ScenarioReplayConflictError, PyValueError);
create_exception!(fluxtrade_core, ScenarioReplayInvariantError, PyRuntimeError);

fn error(error: BoundaryError) -> PyErr {
    match error {
        BoundaryError::Input(reason) => ScenarioReplayInputError::new_err(reason),
        BoundaryError::Lookup(reason) => ScenarioReplayLookupError::new_err(reason),
        BoundaryError::Conflict(reason) => ScenarioReplayConflictError::new_err(reason),
        BoundaryError::Invariant => ScenarioReplayInvariantError::new_err("NATIVE_INVARIANT"),
    }
}
fn text(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let value = value
        .downcast::<PyString>()
        .map_err(|_| ScenarioReplayInputError::new_err("INVALID_SCHEMA"))?;
    value
        .to_str()
        .map(str::to_owned)
        .map_err(|_| ScenarioReplayInputError::new_err("INVALID_SCHEMA"))
}
#[pyclass(name = "_SyntheticScenarioReplaySession", module = "fluxtrade_core")]
struct PySession {
    inner: Session,
}
impl PySession {
    fn request(&self, value: &Bound<'_, PyAny>) -> PyResult<String> {
        if self.inner.poisoned {
            return Err(error(BoundaryError::Invariant));
        }
        text(value)
    }
}
#[pymethods]
impl PySession {
    #[new]
    #[pyo3(signature = (profile_id, account_key, configuration=None))]
    fn new(
        profile_id: &Bound<'_, PyAny>,
        account_key: &Bound<'_, PyAny>,
        configuration: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let configuration = configuration.map(text).transpose()?;
        Ok(Self {
            inner: Session::new(
                &text(profile_id)?,
                &text(account_key)?,
                configuration.as_deref(),
            )
            .map_err(error)?,
        })
    }
    #[pyo3(signature = (request))]
    fn apply_group(&mut self, request: &Bound<'_, PyAny>) -> PyResult<String> {
        self.inner
            .apply_group(&self.request(request)?)
            .map_err(error)
    }
    #[pyo3(signature = (request))]
    fn capture_snapshot(&mut self, request: &Bound<'_, PyAny>) -> PyResult<String> {
        self.inner
            .capture_snapshot(&self.request(request)?)
            .map_err(error)
    }
    #[pyo3(signature = (request))]
    fn build_delivery(&mut self, request: &Bound<'_, PyAny>) -> PyResult<String> {
        self.inner
            .build_delivery(&self.request(request)?)
            .map_err(error)
    }
    #[pyo3(signature = (request))]
    fn historical_market_step(&mut self, request: &Bound<'_, PyAny>) -> PyResult<String> {
        self.inner
            .historical_market_step(&self.request(request)?)
            .map_err(error)
    }
    fn _historical_working_orders(&mut self) -> PyResult<String> {
        self.inner.historical_working_orders().map_err(error)
    }
    fn inspect_state(&mut self) -> PyResult<String> {
        self.inner.inspect_state().map_err(error)
    }
}
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySession>()?;
    let py = module.py();
    module.add(
        "ScenarioReplayInputError",
        py.get_type::<ScenarioReplayInputError>(),
    )?;
    module.add(
        "ScenarioReplayLookupError",
        py.get_type::<ScenarioReplayLookupError>(),
    )?;
    module.add(
        "ScenarioReplayConflictError",
        py.get_type::<ScenarioReplayConflictError>(),
    )?;
    module.add(
        "ScenarioReplayInvariantError",
        py.get_type::<ScenarioReplayInvariantError>(),
    )?;
    Ok(())
}
#[cfg(test)]
mod tests;
