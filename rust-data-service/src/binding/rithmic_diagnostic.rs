use crate::rithmic_ledger::order_field_policy::{NumericFieldDiagnostic, QuantityFieldDiagnostic};
use anyhow::Error;
use pyo3::{prelude::*, types::PyDict};

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_ATTACHMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_CONSTRUCTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn fail_next_attachment_for_test() {
    FAIL_NEXT_ATTACHMENT.with(|fail| fail.set(true));
}

#[cfg(test)]
pub(crate) fn fail_next_construction_for_test() {
    FAIL_NEXT_CONSTRUCTION.with(|fail| fail.set(true));
}

pub(crate) fn attach_safe_diagnostic(
    target: &Bound<'_, PyAny>,
    diagnostic: Bound<'_, PyDict>,
) -> PyResult<()> {
    #[cfg(test)]
    if FAIL_NEXT_ATTACHMENT.with(std::cell::Cell::take) {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "injected safe diagnostic attachment failure",
        ));
    }
    target.setattr("safe_diagnostic", diagnostic)
}

pub(crate) fn safe_diagnostic<'py>(
    py: Python<'py>,
    error: &Error,
    source_stage: &str,
    stable_error_code: &str,
    safe_cause: &str,
    retryable: bool,
    classification: Option<(&str, &str, &str, Option<u32>)>,
) -> PyResult<Bound<'py, PyDict>> {
    #[cfg(test)]
    if FAIL_NEXT_CONSTRUCTION.with(std::cell::Cell::take) {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "injected safe diagnostic construction failure",
        ));
    }
    let value = PyDict::new(py);
    value.set_item("schema_version", 1)?;
    value.set_item("source_stage", source_stage)?;
    value.set_item("stable_error_code", stable_error_code)?;
    value.set_item("safe_cause", safe_cause)?;
    value.set_item("retryable", retryable)?;
    if let Some((connect_phase, connect_failure_kind, session_failure_reason, code)) =
        classification
    {
        value.set_item("connect_phase", connect_phase)?;
        value.set_item("connect_failure_kind", connect_failure_kind)?;
        value.set_item("session_failure_reason", session_failure_reason)?;
        if let Some(code) = code {
            value.set_item("provider_response_code", code)?;
        }
    }
    if let Some(diagnostic) = error.downcast_ref::<NumericFieldDiagnostic>() {
        let (template, context, notify_type, field, raw_bits, failure) = diagnostic.safe_fields();
        value.set_item("template_id", template)?;
        value.set_item("context", context)?;
        value.set_item("field", field)?;
        value.set_item("raw_bits", raw_bits)?;
        value.set_item("failure_class", failure)?;
        if let Some(notify_type) = notify_type {
            value.set_item("notify_type", notify_type)?;
        }
    } else if let Some(diagnostic) = error.downcast_ref::<QuantityFieldDiagnostic>() {
        let (template, context, notify_type, field, raw_value, failure) = diagnostic.safe_fields();
        value.set_item("template_id", template)?;
        value.set_item("context", context)?;
        value.set_item("field", field)?;
        value.set_item("failure_class", failure)?;
        if let Some(raw_value) = raw_value {
            value.set_item("raw_bits", raw_value as u32 as u64)?;
        }
        if let Some(notify_type) = notify_type {
            value.set_item("notify_type", notify_type)?;
        }
    }
    Ok(value)
}
