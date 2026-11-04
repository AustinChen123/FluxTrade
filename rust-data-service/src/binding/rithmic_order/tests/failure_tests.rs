use super::*;

#[test]
fn safe_diagnostic_attachment_failure_preserves_legacy_order_diagnostic() {
    let _project_import_guard = crate::binding::embedded_python_project_import_lock();
    Python::with_gil(|py| {
        crate::binding::rithmic_diagnostic::fail_next_attachment_for_test();
        let error = crate::rithmic_ledger::order_event::invalid_price_diagnostic_for_binding_test();
        let projected = runtime_error(py, error);
        let value = projected.value(py);
        assert!(value.is_instance_of::<PyRuntimeError>());
        assert!(value
            .getattr("diagnostic")
            .unwrap()
            .extract::<String>()
            .unwrap()
            .contains("field=price"));
        let diagnostic = value
            .getattr("diagnostic")
            .unwrap()
            .extract::<String>()
            .unwrap();
        assert_eq!(
            value
                .getattr("args")
                .unwrap()
                .extract::<(String,)>()
                .unwrap()
                .0,
            format!("Rithmic order event failed: {diagnostic}")
        );
        assert!(!value.hasattr("safe_diagnostic").unwrap());
    });
}

#[test]
fn safe_diagnostic_construction_failure_preserves_legacy_order_disposition() {
    let _project_import_guard = crate::binding::embedded_python_project_import_lock();
    Python::with_gil(|py| {
        crate::binding::rithmic_diagnostic::fail_next_construction_for_test();
        let error = crate::rithmic_ledger::order_event::invalid_price_diagnostic_for_binding_test();
        let projected = runtime_error(py, error);
        let value = projected.value(py);
        assert!(value.is_instance_of::<PyRuntimeError>());
        let diagnostic = value
            .getattr("diagnostic")
            .unwrap()
            .extract::<String>()
            .unwrap();
        assert!(diagnostic.contains("field=price"));
        assert!(diagnostic.contains("failure=negative"));
        assert_eq!(
            value
                .getattr("args")
                .unwrap()
                .extract::<(String,)>()
                .unwrap()
                .0,
            format!("Rithmic order event failed: {diagnostic}")
        );
        assert!(!value.hasattr("safe_diagnostic").unwrap());
    });
}

#[test]
fn native_order_projection_variants_traverse_classifier_and_terminal_decision() {
    let _project_import_guard = crate::binding::embedded_python_project_import_lock();
    Python::with_gil(|py| {
        let sys = pyo3::types::PyModule::import(py, "sys").unwrap();
        let paths = sys.getattr("path").unwrap();
        let version_info = sys.getattr("version_info").unwrap();
        let major: u8 = version_info.get_item(0).unwrap().extract().unwrap();
        let minor: u8 = version_info.get_item(1).unwrap().extract().unwrap();
        let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("rust-data-service should be inside the repository");
        for path in [
            repository_root.join(format!(
                "python-strategy/.venv/lib/python{major}.{minor}/site-packages"
            )),
            repository_root.join("python-strategy"),
        ] {
            paths
                .call_method1("insert", (0, path.to_string_lossy().as_ref()))
                .unwrap();
        }
        let source = std::ffi::CString::new(
                r#"
import logging
import threading
from src.core.adapters.rithmic_owned_order_reconciliation import _classify_ledger_snapshot_failure
from src.core.adapters.rithmic_order_event_stream import RithmicOrderEventStreamService

def run(native_error, expect_detail):
    assert _classify_ledger_snapshot_failure(native_error) == (
        "RuntimeError", "unclassified_internal",
        "unclassified_ledger_snapshot_failure",
        "ledger snapshot failed before safe classification", False,
    )
    calls = {"halt": 0, "lockdown": [], "diagnostics": []}
    class Adapter:
        def start_order_event_stream(self): pass
        def poll_order_event(self): raise native_error
    service = RithmicOrderEventStreamService(
        adapter=Adapter(), stop_event=threading.Event(), is_running=lambda: True,
        publish_worker=lambda _worker: None, reconcile_if_needed=lambda: True,
        process_event=lambda _event: {"action": "applied"},
        lockdown=calls["lockdown"].append,
        assert_runtime_leadership=lambda: None,
        halt_submissions=lambda: calls.__setitem__("halt", calls["halt"] + 1),
        on_runtime_started=lambda: True, logger=logging.getLogger("f4-native-order"),
        diagnostic_dispatcher=type("Sink", (), {"submit": lambda _self, detail: calls["diagnostics"].append(detail) or True})(),
    )
    service._run()
    assert calls["halt"] == 1 and calls["lockdown"] == [], calls
    assert sum(detail is not None for detail in calls["diagnostics"]) == int(expect_detail), calls
"#
                .trim()
                .to_string(),
            )
            .unwrap();
        let filename = std::ffi::CString::new("rithmic_native_order_decision.py").unwrap();
        let module_name = std::ffi::CString::new("rithmic_native_order_decision").unwrap();
        let module = pyo3::types::PyModule::from_code(
            py,
            source.as_c_str(),
            filename.as_c_str(),
            module_name.as_c_str(),
        )
        .unwrap();

        for projection in ["normal", "attachment_failure", "construction_failure"] {
            if projection == "attachment_failure" {
                crate::binding::rithmic_diagnostic::fail_next_attachment_for_test();
            } else if projection == "construction_failure" {
                crate::binding::rithmic_diagnostic::fail_next_construction_for_test();
            }
            let error = runtime_error(
                py,
                crate::rithmic_ledger::order_event::invalid_price_diagnostic_for_binding_test(),
            );
            let value = error.value(py);
            let has_detail = value.hasattr("safe_diagnostic").unwrap();
            assert_eq!(has_detail, projection == "normal");
            module.setattr("native_error", value).unwrap();
            module
                .getattr("run")
                .unwrap()
                .call1((value, has_detail))
                .unwrap_or_else(|error| {
                    panic!(
                        "native order {projection} terminal decision failed: {}",
                        error.value(py)
                    )
                });
        }
    });
}
