use super::*;

fn assert_safe_json(py: Python<'_>, actual: &Bound<'_, PyAny>, expected: serde_json::Value) {
    let encoded = serde_json::to_string(&expected).unwrap();
    let expected = py
        .import("json")
        .unwrap()
        .call_method1("loads", (encoded,))
        .unwrap();
    assert!(actual.eq(expected).unwrap());
}

#[test]
fn numeric_binding_safe_projection_is_exact_and_redacted() {
    let _project_import_guard = crate::binding::embedded_python_project_import_lock();
    Python::with_gil(|py| {
        let error = crate::rithmic_ledger::order_event::invalid_price_diagnostic_for_binding_test();
        let projected = runtime_error(py, error);
        let value = projected.value(py);
        let diagnostic = value
            .getattr("diagnostic")
            .unwrap()
            .extract::<String>()
            .unwrap();
        assert!(diagnostic.contains("template=352 context=live notify_type=1(STATUS) field=price"));
        assert!(!diagnostic.contains("BASKET_ID_SECRET_123"));
        assert!(!diagnostic.contains("ORDER_ID_SECRET_456"));
        let safe = value.getattr("safe_diagnostic").unwrap();
        let expected = serde_json::json!({
            "schema_version": 1,
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "negative",
            "retryable": false,
            "template_id": 352,
            "context": "live",
            "notify_type": 1,
            "field": "price",
            "raw_bits": 13_830_554_455_654_793_216u64,
            "failure_class": "negative"
        });
        let mut wrong_bits = expected.clone();
        wrong_bits["raw_bits"] = serde_json::json!(13_830_554_455_654_793_217u64);
        assert_safe_json(py, &safe, expected);
        assert!(!safe
            .eq(py
                .import("json")
                .unwrap()
                .call_method1("loads", (serde_json::to_string(&wrong_bits).unwrap(),))
                .unwrap())
            .unwrap());
        assert!(!safe.to_string().contains("BASKET_ID_SECRET_123"));
        assert!(!safe.to_string().contains("ORDER_ID_SECRET_456"));
    });
}

#[test]
fn quantity_binding_projection_preserves_negative_zero_and_missing() {
    let _project_import_guard = crate::binding::embedded_python_project_import_lock();
    Python::with_gil(|py| {
        let expected = |safe_cause, failure_class, raw_bits| {
            serde_json::json!({
                "schema_version": 1,
                "source_stage": "order_event",
                "stable_error_code": "rithmic_order_event_failed",
                "safe_cause": safe_cause,
                "retryable": false,
                "template_id": 352,
                "context": "live",
                "notify_type": 1,
                "field": "quantity",
                "raw_bits": raw_bits,
                "failure_class": failure_class
            })
        };
        let negative =
            crate::rithmic_ledger::order_event::invalid_quantity_diagnostic_for_binding_test(-2);
        let safe = runtime_error(py, negative)
            .value(py)
            .getattr("safe_diagnostic")
            .unwrap();
        assert_safe_json(
            py,
            &safe,
            expected("negative", "negative", 4_294_967_294u64),
        );
        let zero =
            crate::rithmic_ledger::order_event::invalid_quantity_diagnostic_for_binding_test(0);
        let safe = runtime_error(py, zero)
            .value(py)
            .getattr("safe_diagnostic")
            .unwrap();
        assert_safe_json(py, &safe, expected("zero", "zero", 0u64));
        let missing = anyhow::Error::new(
            crate::rithmic_ledger::order_field_policy::QuantityFieldDiagnostic::new(
                crate::rithmic_ledger::order_field_policy::NumericFieldContext::new(
                    352,
                    "live",
                    Some(1),
                    Some("STATUS"),
                    None,
                    None,
                ),
                "quantity",
                None,
                "missing",
            ),
        );
        let safe = runtime_error(py, missing)
            .value(py)
            .getattr("safe_diagnostic")
            .unwrap();
        assert_safe_json(
            py,
            &safe,
            serde_json::json!({
                "schema_version": 1,
                "source_stage": "order_event",
                "stable_error_code": "rithmic_order_event_failed",
                "safe_cause": "missing",
                "retryable": false,
                "template_id": 352,
                "context": "live",
                "notify_type": 1,
                "field": "quantity",
                "failure_class": "missing"
            }),
        );
    });
}

#[test]
fn live_order_warning_redacts_price_and_quantity_diagnostics() {
    let _project_import_guard = crate::binding::embedded_python_project_import_lock();
    Python::with_gil(|py| {
        let source = std::ffi::CString::new(
            r#"
import logging
import threading
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
class Handler(logging.Handler):
    def __init__(self): super().__init__(logging.WARNING); self.messages = []
    def emit(self, record): self.messages.append(record.getMessage())
class Client:
    def poll_event(self): raise native_error
class Adapter:
    _client_lock = threading.Lock()
    logger = logging.getLogger("RithmicAdapter")
    def _require_client(self): return Client()
def run(expected_field):
    logger = logging.getLogger("RithmicAdapter")
    logger.setLevel(logging.WARNING)
    handler = Handler(); logger.addHandler(handler)
    try:
        try: RithmicExchangeAdapter.poll_order_event(Adapter())
        except Exception: pass
    finally: logger.removeHandler(handler)
    assert any(f"field={expected_field}" in message for message in handler.messages), handler.messages
    assert all("BASKET_ID_SECRET_123" not in message for message in handler.messages)
    assert all("ORDER_ID_SECRET_456" not in message for message in handler.messages)
"#,
        )
        .unwrap();
        let filename = std::ffi::CString::new("rithmic_live_warning_test.py").unwrap();
        let module_name = std::ffi::CString::new("rithmic_live_warning_test").unwrap();
        let module = PyModule::from_code(
            py,
            source.as_c_str(),
            filename.as_c_str(),
            module_name.as_c_str(),
        )
        .unwrap();
        for (field, error) in [
            ("price", runtime_error(py, crate::rithmic_ledger::order_event::invalid_price_diagnostic_for_binding_test())),
            ("quantity", runtime_error(py, crate::rithmic_ledger::order_event::invalid_quantity_diagnostic_for_binding_test(-2))),
        ] {
            module.setattr("native_error", error.value(py)).unwrap();
            module.getattr("run").unwrap().call1((field,)).unwrap();
        }
    });
}
