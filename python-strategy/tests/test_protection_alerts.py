from datetime import UTC, datetime

import pytest

from src.core.protection_alerts import (
    MAX_RECORD_BYTES,
    ProtectionAlert,
    stable_event_id,
)
from src.core.safe_diagnostics import SafeDiagnostic


def test_alert_is_bounded_redacted_versioned_and_stably_identified():
    alert = ProtectionAlert.create(
        event_type="protection_loss",
        severity="critical",
        reason="declared protection ended without fill",
        entry_id="entry-1",
        evidence_id="event-1",
        occurred_at=datetime(2026, 10, 31, tzinfo=UTC),
        diagnostics=(
            ("terminal_status", "cancelled"),
            ("provider_payload", "must-not-appear"),
            ("api_key", "also-hidden"),
        ),
        account_id="a" * 300,
    )
    line = alert.to_json_line()
    assert alert.event_id == stable_event_id("protection_loss", "entry-1", "event-1")
    assert alert.schema_version == 1
    assert len(line) <= MAX_RECORD_BYTES
    assert b"must-not-appear" not in line
    assert b"also-hidden" not in line
    assert b'"terminal_status":"cancelled"' in line
    assert alert.account_id is not None
    assert len(alert.account_id) == 128
    naive = ProtectionAlert.create(
        event_type="x",
        severity="warning",
        reason="y",
        entry_id="e",
        evidence_id="v",
        occurred_at=datetime(2026, 10, 31),
    )
    assert naive.occurred_at.endswith("+00:00")
    with pytest.raises(ValueError, match="schema version"):
        ProtectionAlert(
            "id", "type", "warning", "2026-10-31T00:00:00+00:00", "reason",
            schema_version=2,
        )


def test_alert_serializes_only_validated_nested_safe_diagnostic():
    diagnostic = SafeDiagnostic.create(
        {
            "source_stage": "order_event",
            "stable_error_code": "rithmic_order_event_failed",
            "safe_cause": "nonfinite",
            "template_id": 352,
            "field": "price",
            "raw_bits": 9221120237041090560,
            "retryable": False,
        }
    )
    assert diagnostic is not None
    alert = ProtectionAlert.create(
        event_type="protection_loss",
        severity="critical",
        reason="bounded reason",
        entry_id="entry-1",
        evidence_id="event-2",
        safe_diagnostic=diagnostic,
    )
    line = alert.to_json_line()
    assert alert.safe_diagnostic_status == "included"
    assert b'"schema_version":1' in line
    assert b'"raw_bits":9221120237041090560' in line

    invalid = ProtectionAlert.create(
        event_type="protection_loss",
        severity="critical",
        reason="bounded reason",
        entry_id="entry-1",
        evidence_id="event-3",
        safe_diagnostic=SafeDiagnostic(
            schema_version=2, fields=(("stable_error_code", "future_code"),)
        ),
    )
    invalid_line = invalid.to_json_line()
    assert invalid.safe_diagnostic_status == "invalid_omitted"
    assert b'"safe_diagnostic":null' in invalid_line
