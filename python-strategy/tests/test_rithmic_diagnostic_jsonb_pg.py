"""Cross-language Rithmic diagnostic persistence against disposable PostgreSQL."""

from __future__ import annotations

import os
import logging
import threading
import time
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from tests.test_rithmic_diagnostic_pg_support import (
    NoopAdapter,
    create_diagnostic_schema,
    postgres_url,
)

from src.core.adapters.rithmic_runtime_recovery import RithmicRuntimeRecoveryService
from src.core.adapters.rithmic_ledger_recovery import RithmicLedgerRecoveryService
from src.core.adapters.rithmic_order_event_stream import RithmicOrderEventStreamService

pytestmark = pytest.mark.skipif(
    os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
    reason="set FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS=1 to use disposable PostgreSQL",
)


def test_rust_binding_diagnostic_is_committed_as_postgres_jsonb() -> None:
    import fluxtrade_core

    from src.core.ops_diagnostic_dispatcher import OpsDiagnosticDispatcher
    from src.core.safe_diagnostics import diagnostic_from_exception

    url = postgres_url()
    admin_engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3})
    schema = f"f4diag_{uuid4().hex}"
    worker_engine = None
    try:
        create_diagnostic_schema(admin_engine, schema)
        worker_engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 3,
                "options": f"-csearch_path={schema}",
            },
        )
        rithmic_ledger_snapshot = getattr(fluxtrade_core, "rithmic_ledger_snapshot")
        with pytest.raises(RuntimeError) as caught:
            rithmic_ledger_snapshot("", None)
        diagnostic = diagnostic_from_exception(caught.value)
        assert diagnostic is not None
        assert diagnostic.as_dict()["source_stage"] == "profile_lease"
        assert diagnostic.as_dict()["stable_error_code"] == "profile_lease_failed"

        factory = sessionmaker(bind=worker_engine, expire_on_commit=False)
        dispatcher = OpsDiagnosticDispatcher(
            session_factory=factory, health_interval_seconds=60
        )
        numeric_error = RuntimeError("numeric projection")
        numeric_tuple = {
            "schema_version": 1,
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "negative",
            "retryable": False,
            "template_id": 352,
            "context": "live",
            "notify_type": 1,
            "field": "price",
            "raw_bits": 13_830_554_455_654_793_216,
            "failure_class": "negative",
        }
        setattr(
            numeric_error,
            "safe_diagnostic",
            numeric_tuple,
        )
        numeric = diagnostic_from_exception(numeric_error)
        quantity_error = RuntimeError("quantity projection")
        quantity_tuple = {
            "schema_version": 1,
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "negative",
            "retryable": False,
            "template_id": 352,
            "context": "live",
            "notify_type": 1,
            "field": "quantity",
            "raw_bits": 4_294_967_294,
            "failure_class": "negative",
        }
        setattr(quantity_error, "safe_diagnostic", quantity_tuple)
        quantity = diagnostic_from_exception(quantity_error)
        zero_quantity_error = RuntimeError("zero quantity projection")
        zero_quantity_tuple = {
            **quantity_tuple,
            "safe_cause": "zero",
            "raw_bits": 0,
            "failure_class": "zero",
        }
        setattr(zero_quantity_error, "safe_diagnostic", zero_quantity_tuple)
        zero_quantity = diagnostic_from_exception(zero_quantity_error)
        missing_quantity_error = RuntimeError("missing quantity projection")
        missing_quantity_tuple = {
            "schema_version": 1,
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "missing",
            "retryable": False,
            "template_id": 352,
            "context": "live",
            "notify_type": 1,
            "field": "quantity",
            "failure_class": "missing",
        }
        setattr(missing_quantity_error, "safe_diagnostic", missing_quantity_tuple)
        missing_quantity = diagnostic_from_exception(missing_quantity_error)
        recovery_error = RuntimeError("terminal recovery projection")
        setattr(
            recovery_error,
            "safe_diagnostic",
            {
                "schema_version": 1,
                "source_stage": "order_snapshot",
                "stable_error_code": "order_snapshot_failed",
                "retryable": True,
            },
        )
        recovery = diagnostic_from_exception(recovery_error)
        assert numeric is not None and quantity is not None
        assert missing_quantity is not None and zero_quantity is not None
        assert recovery is not None
        assert dispatcher.submit(diagnostic)
        assert dispatcher.submit(numeric)
        assert dispatcher.submit(quantity)
        assert dispatcher.submit(missing_quantity)
        assert dispatcher.submit(zero_quantity)
        driver_failure_error = RuntimeError("driver failure fixture")
        setattr(
            driver_failure_error,
            "safe_diagnostic",
            {
                "schema_version": 1,
                "source_stage": "order_event",
                "stable_error_code": "rithmic_order_event_failed",
                "retryable": False,
                "raw_bits": 123,
            },
        )
        driver_failure = diagnostic_from_exception(driver_failure_error)
        assert driver_failure is not None and dispatcher.submit(driver_failure)

        def fail_reconcile(*_args: object) -> dict[str, object]:
            raise recovery_error

        recovery_service = RithmicRuntimeRecoveryService(
            adapter=NoopAdapter(),
            profile="profile",
            account_id="ACCOUNT_SENTINEL",
            halt_for_reconcile=lambda **_kwargs: True,
            stop_order_event_stream=lambda **_kwargs: True,
            reconcile_owned_orders=fail_reconcile,
            publish_authoritative_summary=lambda _summary: None,
            assert_runtime_leadership=lambda: None,
            start_order_event_stream=lambda: True,
            resume_after_reconcile=lambda: None,
            lockdown=lambda _reason: None,
            logger=logging.getLogger("f4-jsonb-recovery"),
            maintenance_active=lambda: False,
            diagnostic_sink=dispatcher.submit,
        )
        assert recovery_service.run_once() is False
        terminal_error = RuntimeError("terminal stream failure with ACCOUNT_SECRET")
        setattr(
            terminal_error,
            "safe_diagnostic",
            {
                "schema_version": 1,
                "source_stage": "order_event",
                "stable_error_code": "rithmic_order_event_failed",
                "safe_cause": "unclassified",
                "retryable": False,
            },
        )

        class TerminalAdapter:
            def start_order_event_stream(self) -> None:
                return None

            def poll_order_event(self) -> None:
                raise terminal_error

        terminal_service = RithmicOrderEventStreamService(
            adapter=TerminalAdapter(),
            stop_event=threading.Event(),
            is_running=lambda: True,
            publish_worker=lambda _worker: None,
            reconcile_if_needed=lambda: True,
            process_event=lambda _event: {"action": "applied"},
            lockdown=lambda _reason: None,
            assert_runtime_leadership=lambda: None,
            halt_submissions=lambda: None,
            on_runtime_started=lambda: True,
            logger=logging.getLogger("f4-jsonb-terminal"),
            diagnostic_dispatcher=dispatcher,
        )
        terminal_service._run()

        startup_error = RuntimeError("startup snapshot failure")
        startup_envelope = {
            "schema_version": 1,
            "source_stage": "ledger_snapshot",
            "stable_error_code": "unclassified_ledger_snapshot_failure",
            "safe_cause": "unclassified",
            "retryable": False,
        }
        setattr(startup_error, "safe_diagnostic", startup_envelope)
        startup_recovery = RithmicLedgerRecoveryService(
            profile="profile",
            account_id="ACCOUNT_SENTINEL",
            reconcile_owned_orders=lambda *_args: (_ for _ in ()).throw(
                startup_error
            ),
            now_seconds=lambda: 0.0,
            publish_authoritative_balance=lambda **_values: None,
            logger=logging.getLogger("f4-jsonb-startup"),
        )
        startup_summary = startup_recovery.reconcile_startup()
        assert startup_summary["safe_diagnostic"] == startup_envelope
        from src.core.engine import StrategyEngine

        startup_classifier = StrategyEngine.__new__(StrategyEngine)
        startup_classifier._startup_auto_recovery_allowed = False
        startup_classifier.ops_diagnostic_dispatcher = dispatcher
        assert startup_classifier._can_auto_resume_after_startup_recovery(
            startup_summary
        ) is False
        terminal_diagnostic = diagnostic_from_exception(terminal_error)
        startup_diagnostic = diagnostic_from_exception(startup_error)
        assert terminal_diagnostic is not None and startup_diagnostic is not None
        deadline = time.monotonic() + 5
        while (
            dispatcher.counters["committed"] + dispatcher.counters["dropped"] < 9
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        dispatcher.stop()
        assert dispatcher.counters["accepted"] == 9
        assert dispatcher.counters["committed"] == 8
        assert dispatcher.counters["dropped"] == 1

        with admin_engine.connect() as connection:
            rows = connection.execute(
                text(
                    f'SELECT event_type, event_subtype, payload, '
                    f'jsonb_typeof(payload) FROM "{schema}".system_events ORDER BY id'
                )
            ).all()
        assert len(rows) == 8
        for row, expected in zip(
            rows,
            (
                diagnostic,
                numeric,
                quantity,
                missing_quantity,
                zero_quantity,
                recovery,
                terminal_diagnostic,
                startup_diagnostic,
            ),
            strict=True,
        ):
            assert row.event_type == "ops"
            assert row.event_subtype == "rithmic_diagnostic"
            assert row.jsonb_typeof == "object"
            assert row.payload == {"safe_diagnostic": expected.as_dict()}
        assert rows[1].payload["safe_diagnostic"] == numeric_tuple
        assert rows[2].payload["safe_diagnostic"] == quantity_tuple
        assert rows[2].payload["safe_diagnostic"]["raw_bits"] == 4_294_967_294
        assert rows[3].payload["safe_diagnostic"] == missing_quantity_tuple
        assert "raw_bits" not in rows[3].payload["safe_diagnostic"]
        assert rows[4].payload["safe_diagnostic"] == zero_quantity_tuple
        assert rows[5].payload["safe_diagnostic"]["source_stage"] == "order_snapshot"
        assert rows[6].payload == {"safe_diagnostic": terminal_diagnostic.as_dict()}
        assert rows[7].payload == {
            "safe_diagnostic": startup_envelope
        }
    finally:
        if worker_engine is not None:
            worker_engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()
