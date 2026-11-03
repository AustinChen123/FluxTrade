"""Shared validation and rendering for bounded Rithmic diagnostics."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_MAX_DETAIL_BYTES = 2048
_MAX_FIELDS = 16
_MAX_TEXT = 96
_SAFE_TEXT = frozenset(
    {
        "source_stage",
        "stable_error_code",
        "safe_cause",
        "connect_phase",
        "connect_failure_kind",
        "session_failure_reason",
        "failure_class",
        "field",
        "context",
    }
)
_SAFE_INTS = frozenset(
    {
        "template_id",
        "notify_type",
        "raw_bits",
        "field_number",
        "payload_len",
        "provider_response_code",
    }
)
_SAFE_BOOLS = frozenset({"retryable"})
_SAFE_TOKEN = re.compile(r"^[a-z][a-z_]{0,63}$")
_SAFE_FIELD_NAMES = frozenset(
    {
        "price",
        "trigger_price",
        "last_fill_price",
        "average_fill_price",
        "cumulative_average_price",
        "quantity",
        "filled_quantity",
        "cumulative_filled_quantity",
        "unfilled_quantity",
        "total_unfilled_size",
        "last_fill_quantity",
        "total_fill_size",
        "fill_size",
        "fill_price",
        "avg_fill_price",
    }
)
_SAFE_DRIFT_CODES = frozenset(
    {
        "rithmic_external_order_detected",
        "rithmic_order_event_requires_reconciliation",
        "rithmic_external_order_detected_during_clear",
    }
)
_SAFE_RITHMIC_REASONS = frozenset(
    {
        *_SAFE_DRIFT_CODES,
        "rithmic_runtime_reconciliation_unresolved",
        "rithmic_runtime_reconciliation_drain_timeout",
        "rithmic_runtime_reconciliation_failed",
        "rithmic_runtime_reconciliation_stream_disconnected",
        "rithmic_runtime_reconciliation_stream_restart_failed",
        "rithmic_runtime_reconciliation_stream_stop_timeout",
        "rithmic_runtime_reconciliation_unavailable",
        "rithmic_order_stream_not_started",
        "rithmic_emergency_flatten_event_stream_stop_timeout",
        "rithmic_portfolio_exit_event_stream_stop_timeout",
        "rithmic_portfolio_exit_order_stream_restart_failed",
        "rithmic_portfolio_exit_preflight_reconciliation_blocked",
        "rithmic_strategy_exit_event_stream_stop_timeout",
        "rithmic_strategy_exit_order_stream_restart_failed",
        "rithmic_strategy_exit_post_cancel_reconciliation_blocked",
        "rithmic_strategy_exit_preflight_reconciliation_blocked",
        "rithmic_reconciliation_blocked",
        "rithmic_reconciliation_required",
        "rithmic_order_stream_stop_timeout",
        "rithmic_order_stream_restart_failed",
        "rithmic_startup_recovery_unresolved",
        "rithmic_ledger_recovery_unresolved",
    }
)
_SAFE_CAUSES = frozenset(
    {
        "negative",
        "nonfinite",
        "decimal_unrepresentable",
        "zero",
        "missing",
        "unclassified",
        "snapshot_unavailable",
    }
)
_LEDGER_CAUSE_CODES = {
    "profile lease failed": "profile_lease_failed",
    "runtime initialization failed": "runtime_initialization_failed",
    "ledger snapshot request validation failed": "request_validation_failed",
    "ORDER config failed": "order_config_failed",
    "ORDER connect failed": "order_connect_failed",
    "ORDER heartbeat failed": "order_heartbeat_failed",
    "ORDER login info failed": "order_login_info_failed",
    "ORDER account list failed": "order_account_list_failed",
    "ORDER snapshot failed": "order_snapshot_failed",
    "ORDER history failed": "order_history_failed",
    "fill history failed": "fill_history_failed",
    "PNL config failed": "pnl_config_failed",
    "PNL connect failed": "pnl_connect_failed",
    "PNL heartbeat failed": "pnl_heartbeat_failed",
    "PNL request failed": "pnl_request_failed",
    "PNL snapshot failed": "pnl_snapshot_failed",
    "ledger snapshot failed before safe classification": (
        "unclassified_ledger_snapshot_failure"
    ),
}
_SAFE_STAGES = frozenset(
    {
        "order_event",
        "ledger_snapshot",
        "external_order_drift",
        "profile_lease",
        "runtime_initialization",
        "request_validation",
        "order_config",
        "order_connect",
        "order_heartbeat",
        "order_login_info",
        "order_account_list",
        "order_snapshot",
        "order_history",
        "fill_history",
        "pnl_config",
        "pnl_connect",
        "pnl_heartbeat",
        "pnl_request",
        "pnl_snapshot",
        "unclassified_internal",
    }
)
_SAFE_CODES = frozenset(
    {
        "rithmic_order_event_failed",
        "rithmic_numeric_validation_failed",
        "invalid_ledger_snapshot_request",
        "unclassified_ledger_snapshot_failure",
        *(_LEDGER_CAUSE_CODES.values()),
        *_SAFE_DRIFT_CODES,
    }
)
_SAFE_CONNECT_PHASES = frozenset(
    {
        "discovery_websocket",
        "discovery_exchange",
        "login_websocket",
        "login_exchange",
        "initial_heartbeat",
        "unclassified",
    }
)
_SAFE_CONNECT_KINDS = frozenset(
    {"maintenance", "handshake_rejected", "session_fatal", "transport", "unclassified"}
)
_SAFE_SESSION_REASONS = frozenset(
    {
        "response_code_rejected",
        "response_code_missing",
        "heartbeat_interval_missing",
        "heartbeat_interval_invalid",
        "forced_logout",
        "handshake_rejected",
        "system_unavailable",
        "unclassified",
    }
)


@dataclass(frozen=True, slots=True)
class SafeDiagnostic:
    """Immutable allow-listed detail. It can never carry exception objects."""

    schema_version: int
    fields: tuple[tuple[str, str | int | bool], ...]

    @classmethod
    def create(cls, values: Mapping[str, object]) -> SafeDiagnostic | None:
        if len(values) > _MAX_FIELDS:
            return None
        fields: list[tuple[str, str | int | bool]] = []
        for key, value in values.items():
            if type(key) is not str:
                return None
            if key in _SAFE_TEXT and type(value) is str and 0 < len(value) <= _MAX_TEXT:
                if _SAFE_TOKEN.fullmatch(value) and (
                    key != "field" or value in _SAFE_FIELD_NAMES
                ) and (
                    key != "safe_cause"
                    or value in _SAFE_CAUSES
                    or value in frozenset(_LEDGER_CAUSE_CODES.values())
                ) and (
                    key != "stable_error_code" or value in _SAFE_CODES
                ) and (key != "source_stage" or value in _SAFE_STAGES) and (
                    key != "connect_phase" or value in _SAFE_CONNECT_PHASES
                ) and (
                    key != "connect_failure_kind" or value in _SAFE_CONNECT_KINDS
                ) and (
                    key != "session_failure_reason" or value in _SAFE_SESSION_REASONS
                ) and (
                    key != "failure_class" or value in _SAFE_CAUSES
                ) and (key != "context" or value in {"live", "snapshot", "orders", "history"}):
                    fields.append((key, value))
                else:
                    return None
            elif key in _SAFE_INTS and type(value) is int and 0 <= value < 2**64:
                fields.append((key, value))
            elif key in _SAFE_BOOLS and type(value) is bool:
                fields.append((key, value))
            else:
                return None
        if not fields:
            return None
        diagnostic = cls(schema_version=1, fields=tuple(fields))
        encoded = json.dumps(diagnostic.as_dict(), separators=(",", ":")).encode()
        if len(encoded) > _MAX_DETAIL_BYTES:
            return None
        return diagnostic

    def as_dict(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version, **dict(self.fields)}


def diagnostic_from_exception(error: BaseException) -> SafeDiagnostic | None:
    """Accept only the binding's structured attribute and validate it again."""
    try:
        values = getattr(error, "safe_diagnostic", None)
        if (
            type(values) is not dict
            or values.get("schema_version") != 1
            or type(values.get("schema_version")) is not int
        ):
            return None
        normalized = {key: value for key, value in values.items() if key != "schema_version"}
        cause = normalized.get("safe_cause")
        if type(cause) is str and cause in _LEDGER_CAUSE_CODES:
            normalized["safe_cause"] = _LEDGER_CAUSE_CODES[cause]
        return SafeDiagnostic.create(normalized)
    except Exception:
        return None


def diagnostic_from_payload(value: object) -> SafeDiagnostic | None:
    """Revalidate a serialized safe envelope before forwarding it."""
    if type(value) is not dict or value.get("schema_version") != 1:
        return None
    return SafeDiagnostic.create(
        {key: item for key, item in value.items() if key != "schema_version"}
    )


def diagnostic_from_drift_reason(reason: str) -> SafeDiagnostic | None:
    """Project a stable reason code and discard its free-form identity detail."""
    code = reason.split(":", 1)[0].strip()
    if code not in _SAFE_DRIFT_CODES:
        return None
    return SafeDiagnostic.create(
        {"source_stage": "external_order_drift", "stable_error_code": code}
    )


def safe_lockdown_reason(reason: str | None) -> str | None:
    """Render only the stable Rithmic code; remove all embedded identifiers."""
    if reason is None:
        return None
    code = reason.split(":", 1)[0].strip()
    if code in _SAFE_RITHMIC_REASONS:
        return f"{code}: diagnostic_details_redacted"
    if code.startswith("rithmic_") and code.replace("_", "").isalpha():
        return "rithmic_unclassified_drift: diagnostic_details_redacted"
    return reason[:160]
