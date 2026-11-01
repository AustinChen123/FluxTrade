"""Bounded, provider-neutral protection alert delivery."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime

DEFAULT_ALERTS_PATH = "/var/log/fluxtrade/protection-alerts.jsonl"
ALERTS_PATH_ENV = "FLUXTRADE_PROTECTION_ALERTS_PATH"
MAX_RECORD_BYTES = 4096
_MAX_DIAGNOSTICS = 8
_REDACTED_KEY_PARTS = ("token", "secret", "password", "credential", "api_key")
_DIAGNOSTIC_KEYS = frozenset(
    {"terminal_status", "fill_state", "source", "venue", "reason_code"}
)
_SAFE_DIAGNOSTIC_VALUE = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")


def _bounded(value: str | None, size: int) -> str | None:
    if value is None:
        return None
    return value[:size]


@dataclass(frozen=True, slots=True)
class ProtectionAlert:
    """Immutable, bounded alert record; diagnostics are an allowlisted tuple."""

    event_id: str
    event_type: str
    severity: str
    occurred_at: str
    reason: str
    strategy_id: str | None = None
    product_id: str | None = None
    account_id: str | None = None
    entry_declaration: str | None = None
    declaration_revision: str | None = None
    entry_id: str | None = None
    order_id: str | None = None
    evidence_id: str | None = None
    diagnostics: tuple[tuple[str, str], ...] = ()
    schema_version: int = 1

    def __post_init__(self) -> None:
        for field_name, size in (
            ("event_id", 80), ("event_type", 80), ("severity", 24),
            ("occurred_at", 40), ("reason", 160), ("strategy_id", 128),
            ("product_id", 128), ("account_id", 128),
            ("entry_declaration", 40), ("declaration_revision", 80),
            ("entry_id", 128), ("order_id", 128), ("evidence_id", 128),
        ):
            object.__setattr__(self, field_name, _bounded(getattr(self, field_name), size))
        diagnostics: list[tuple[str, str]] = []
        for key, value in self.diagnostics:
            safe_key = str(key)[:48]
            safe_value = str(value)[:64]
            if (
                safe_key not in _DIAGNOSTIC_KEYS
                or any(part in safe_key.lower() for part in _REDACTED_KEY_PARTS)
                or _SAFE_DIAGNOSTIC_VALUE.fullmatch(safe_value) is None
            ):
                continue
            diagnostics.append((safe_key, safe_value))
            if len(diagnostics) == _MAX_DIAGNOSTICS:
                break
        object.__setattr__(self, "diagnostics", tuple(diagnostics))
        if self.schema_version != 1:
            raise ValueError("unsupported protection alert schema version")
        try:
            instant = datetime.fromisoformat(self.occurred_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("occurred_at must be an ISO 8601 timestamp") from exc
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)
        object.__setattr__(self, "occurred_at", instant.astimezone(UTC).isoformat())

    @classmethod
    def create(
        cls,
        *,
        event_type: str,
        severity: str,
        reason: str,
        entry_id: str,
        evidence_id: str,
        occurred_at: datetime | None = None,
        **values: object,
    ) -> ProtectionAlert:
        event_id = stable_event_id(event_type, entry_id, evidence_id)
        instant = occurred_at or datetime.now(UTC)
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)
        timestamp = instant.astimezone(UTC).isoformat()
        return cls(
            event_id=event_id,
            event_type=event_type,
            severity=severity,
            occurred_at=timestamp,
            reason=reason,
            entry_id=entry_id,
            evidence_id=evidence_id,
            **values,  # type: ignore[arg-type]
        )

    def to_json_line(self) -> bytes:
        payload = {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "severity": self.severity,
            "occurred_at": self.occurred_at,
            "reason": self.reason,
            "strategy_id": self.strategy_id,
            "product_id": self.product_id,
            "account_id": self.account_id,
            "entry_declaration": self.entry_declaration,
            "declaration_revision": self.declaration_revision,
            "entry_id": self.entry_id,
            "order_id": self.order_id,
            "evidence_id": self.evidence_id,
            "diagnostics": dict(self.diagnostics),
        }
        encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        record = encoded.encode("ascii") + b"\n"
        if len(record) > MAX_RECORD_BYTES:
            raise ValueError("protection alert record exceeds maximum size")
        return record


def stable_event_id(event_type: str, entry_id: str, evidence_id: str) -> str:
    """Return a replay correlation key; this is not durable deduplication."""
    material = "\0".join((event_type[:80], entry_id[:128], evidence_id[:128]))
    return hashlib.sha256(material.encode("utf-8", errors="replace")).hexdigest()


