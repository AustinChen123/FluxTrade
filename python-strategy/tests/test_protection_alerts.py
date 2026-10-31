from datetime import UTC, datetime

import pytest

from src.core.protection_alerts import (
    MAX_RECORD_BYTES,
    ProtectionAlert,
    stable_event_id,
)


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
