from __future__ import annotations

import json
from hashlib import sha256

import pytest

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.spider_management.model_time import (
    ModelTimeRecord,
    ModelTimeUnavailable,
    decode_model_time_transition,
    model_time_decision_payload,
    model_time_envelope_identity,
)

_HASH = "a" * 64
_SCOPE = {
    "environment": "paper",
    "execution_scope_id": "scope-1",
    "strategy_id": "strategy-1",
}
_IDENTITY = {
    "generation": 1,
    "confirmation_request_id": "b" * 64,
    "configuration_sha256": "c" * 64,
}
_PRODUCTS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
_UTC = 1_800_000_000_000
_MIN_UTC_MS = -62_135_596_800_000
_MAX_UTC_MS = 253_402_300_799_999


def _envelope(
    event: str = "ANCHOR",
    *,
    epoch: str = "epoch-1",
    utc: int = _UTC,
    elapsed: int = 100,
    previous_id: str | None = None,
    snapshots: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "SPIDER_MODEL_TIME_DELIVERY_V1",
        "scope": _SCOPE,
        "effective_identity": _IDENTITY,
        "observation_id": "d" * 64,
        "predecessor_observation_id": previous_id,
        "checkpoint_before_sha256": _HASH,
        "provenance": {"kind": "MODEL", "evidence_sha256": "e" * 64},
        "event": event,
        "clock": {
            "epoch_id": epoch,
            "observed_utc_ms": utc,
            "elapsed_ms": elapsed,
            "time_zone": "UTC",
            "time_policy_id": "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1",
        },
        "snapshots": snapshots,
    }


def _snapshots(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "earn": "0",
        "trading": {"eq": "100", "availEq": "90"},
        "positions": {"BTC-USDT-SWAP": "1.25", "ETH-USDT-SWAP": "-2"},
        "open_orders": [],
        "as_of_ms": _UTC,
    }
    values.update(overrides)
    return values


def _record(**overrides: object) -> ModelTimeRecord:
    values: dict[str, object] = {
        "epoch_id": "epoch-1",
        "observed_utc_ms": _UTC,
        "elapsed_ms": 100,
        "source_now_ms": 500,
        "observation_id": "d" * 64,
        "time_zone": "UTC",
        "checkpoint_sha256": _HASH,
    }
    values.update(overrides)
    return ModelTimeRecord(**values)  # type: ignore[arg-type]


def _decode(value: dict[str, object], *, previous: ModelTimeRecord | None, now: int = 500):
    return decode_model_time_transition(
        json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii"),
        scope=_SCOPE,
        effective_identity=_IDENTITY,
        evidence_sha256="e" * 64,
        ordered_products=_PRODUCTS,
        checkpoint_now_ms=now,
        checkpoint_sha256=_HASH,
        previous=previous,
    )


def test_anchor_does_not_advance_checkpoint_clock_or_invoke_timer() -> None:
    result = _decode(_envelope(), previous=None)

    assert result.kind == "ANCHOR"
    assert result.source_now_ms == 500
    assert result.record.observed_utc_ms == _UTC
    assert result.record.elapsed_ms == 100
    assert result.should_invoke_timer is False
    assert result.snapshots is None


def test_timer_uses_elapsed_delta_and_derives_utc_local_time() -> None:
    result = _decode(
        _envelope(
            "TIMER", utc=_UTC + 20_000, elapsed=1_100, previous_id="d" * 64,
            snapshots=_snapshots(),
        ),
        previous=_record(),
    )

    assert result.source_now_ms == 1_500
    assert result.should_invoke_timer is True
    assert result.local_hour == 8
    assert result.local_minute == 0
    assert result.snapshots == _snapshots()


def test_restart_counts_utc_downtime_once_and_preserves_new_elapsed_origin() -> None:
    restarted = _decode(
        _envelope(
            "RESTART", epoch="epoch-2", utc=_UTC + 20_000, elapsed=7,
            previous_id="d" * 64,
        ),
        previous=_record(),
    )
    timer = _decode(
        _envelope(
            "TIMER", epoch="epoch-2", utc=_UTC + 20_010, elapsed=12,
            previous_id="d" * 64, snapshots=_snapshots(as_of_ms=_UTC + 20_010),
        ),
        previous=restarted.record,
        now=restarted.source_now_ms,
    )

    assert restarted.source_now_ms == 20_500
    assert restarted.should_invoke_timer is False
    assert restarted.record.elapsed_ms == 7
    assert timer.source_now_ms == 20_505


@pytest.mark.parametrize(
    "event, changes",
    [
        ("ANCHOR", {"predecessor_observation_id": "d" * 64}),
        ("ANCHOR", {"snapshots": {}}),
    ],
)
def test_anchor_shape_rejects_predecessor_or_snapshot(event: str, changes: dict[str, object]) -> None:
    value = _envelope(event)
    value.update(changes)
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=None if event == "ANCHOR" else _record())


def test_timer_rejects_epoch_change_when_every_other_field_is_valid() -> None:
    value = _envelope("TIMER", previous_id="d" * 64, snapshots=_snapshots())
    clock = value["clock"]
    assert isinstance(clock, dict)
    clock["epoch_id"] = "epoch-2"

    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record())


@pytest.mark.parametrize("utc_ms", [_MIN_UTC_MS, _MAX_UTC_MS])
def test_anchor_accepts_utc_millisecond_domain_endpoints(utc_ms: int) -> None:
    result = _decode(_envelope(utc=utc_ms), previous=None)

    assert result.record.observed_utc_ms == utc_ms


@pytest.mark.parametrize("utc_ms", [_MIN_UTC_MS - 1, _MAX_UTC_MS + 1])
def test_anchor_rejects_utc_values_outside_year_one_through_9999(utc_ms: int) -> None:
    with pytest.raises(ModelTimeUnavailable):
        _decode(_envelope(utc=utc_ms), previous=None)


@pytest.mark.parametrize(
    "clock_changes",
    [
        {"observed_utc_ms": _UTC - 1},
        {"elapsed_ms": 99},
        {"elapsed_ms": True},
        {"observed_utc_ms": 1.5},
        {"epoch_id": ""},
        {"time_zone": "Europe/Berlin"},
        {"time_policy_id": "OTHER"},
    ],
)
def test_timer_rejects_regression_invalid_types_and_wrong_clock_policy(
    clock_changes: dict[str, object],
) -> None:
    value = _envelope("TIMER", previous_id="d" * 64, snapshots=_snapshots())
    cast_clock = value["clock"]
    assert isinstance(cast_clock, dict)
    cast_clock.update(clock_changes)

    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record())


@pytest.mark.parametrize(
    "snapshot_changes",
    [
        {"extra": "shape"},
        {"earn": 0.5},
        {"earn": "NaN"},
        {"earn": "1.00"},
        {"earn": "1e3"},
        {"trading": {"eq": "1", "availEq": False}},
        {"positions": {"BTC-USDT-SWAP": "1"}},
        {"positions": {"BTC-USDT-SWAP": "1", "ETH-USDT-SWAP": 2.0}},
        {"as_of_ms": _UTC + 1},
        {"open_orders": [{"local_order_id": "x"}]},
        {"open_orders": [{
            "local_order_id": "x", "state": "unknown", "price": "1",
            "quantity_contracts": "1", "cumulative_contracts": "0", "created_at_ms": _UTC,
        }]},
        {"open_orders": [{
            "local_order_id": "x", "state": "live", "price": 1.0,
            "quantity_contracts": "1", "cumulative_contracts": "0", "created_at_ms": _UTC,
        }]},
    ],
)
def test_timer_rejects_open_snapshot_shapes_and_noncanonical_money(
    snapshot_changes: dict[str, object],
) -> None:
    value = _envelope(
        "TIMER", previous_id="d" * 64,
        snapshots=_snapshots(**snapshot_changes),
    )
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record())


def test_old_predecessor_cannot_reapply_restart_or_timer() -> None:
    value = _envelope(
        "RESTART", epoch="epoch-2", utc=_UTC + 1, elapsed=0,
        previous_id="0" * 64,
    )
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record())


@pytest.mark.parametrize("event", ["TIMER", "RESTART"])
def test_non_anchor_matching_checkpoint_rejects_source_clock_overflow(event: str) -> None:
    previous = _record(source_now_ms=2**63 - 1)
    value = _envelope(
        event,
        epoch="epoch-1" if event == "TIMER" else "epoch-2",
        utc=_UTC + 1,
        elapsed=101 if event == "TIMER" else 0,
        previous_id=previous.observation_id,
        snapshots=_snapshots(as_of_ms=_UTC + 1) if event == "TIMER" else None,
    )

    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=previous, now=previous.source_now_ms)


def test_restart_requires_new_epoch_and_rejects_wrong_identity_or_checkpoint() -> None:
    value = _envelope(
        "RESTART", epoch="epoch-1", utc=_UTC + 1, previous_id="d" * 64,
    )
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record())

    value = _envelope(
        "RESTART", epoch="epoch-2", utc=_UTC + 1, previous_id="d" * 64,
    )
    value["checkpoint_before_sha256"] = "f" * 64
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record())


@pytest.mark.parametrize(
    "field, replacement",
    [
        ("scope", {"environment": "paper", "execution_scope_id": "scope-1"}),
        ("effective_identity", {**_IDENTITY, "generation": True}),
        ("provenance", {"kind": "PROVIDER", "evidence_sha256": "e" * 64}),
        ("observation_id", "not-a-sha256"),
    ],
)
def test_anchor_rejects_invalid_scope_identity_provenance_and_observation_id(
    field: str, replacement: object,
) -> None:
    value = _envelope()
    value[field] = replacement
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=None)


def test_rejects_malformed_predecessor_record_observation_hash() -> None:
    value = _envelope("TIMER", previous_id="not-a-sha256", snapshots=_snapshots())
    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record(observation_id="not-a-sha256"))


@pytest.mark.parametrize("event", ["TIMER", "RESTART"])
@pytest.mark.parametrize("checkpoint_now_ms", [499, 501])
def test_non_anchor_rejects_checkpoint_clock_mismatch(
    event: str, checkpoint_now_ms: int,
) -> None:
    value = _envelope(
        event,
        epoch="epoch-1" if event == "TIMER" else "epoch-2",
        utc=_UTC + 1,
        elapsed=101 if event == "TIMER" else 0,
        previous_id="d" * 64,
        snapshots=_snapshots(as_of_ms=_UTC + 1) if event == "TIMER" else None,
    )

    with pytest.raises(ModelTimeUnavailable):
        _decode(value, previous=_record(), now=checkpoint_now_ms)


@pytest.mark.parametrize("event,delta_ms", [("TIMER", 0), ("TIMER", 7), ("RESTART", 0), ("RESTART", 7)])
def test_non_anchor_matching_checkpoint_preserves_exact_delta_and_never_regresses(
    event: str, delta_ms: int,
) -> None:
    value = _envelope(
        event,
        epoch="epoch-1" if event == "TIMER" else "epoch-2",
        utc=_UTC + delta_ms,
        elapsed=100 + delta_ms if event == "TIMER" else 0,
        previous_id="d" * 64,
        snapshots=_snapshots(as_of_ms=_UTC + delta_ms) if event == "TIMER" else None,
    )

    result = _decode(value, previous=_record(), now=500)

    assert result.source_now_ms == 500 + delta_ms
    assert result.source_now_ms >= 500


def test_model_time_identity_extracts_only_canonical_closed_routing_identity() -> None:
    raw = canonical_bytes(_envelope())

    assert model_time_envelope_identity(raw) == (_SCOPE, "d" * 64)
    assert model_time_envelope_identity(raw + b" ") is None
    assert model_time_envelope_identity(b"not-json") is None

    malformed = _envelope()
    malformed["extra"] = True
    assert model_time_envelope_identity(canonical_bytes(malformed)) is None


def test_time_decision_uses_closed_schema_and_shared_transition_result() -> None:
    value = _envelope()
    raw = canonical_bytes(value)
    transition = _decode(value, previous=None)

    decision = model_time_decision_payload(
        raw,
        transition,
        scope=_SCOPE,
        generation=1,
        configuration_sha256="c" * 64,
        trigger_delivery_id="d" * 64,
        before_checkpoint_sha256=_HASH,
        after_checkpoint_sha256=_HASH,
        events=[],
        candidates=[],
        outcome="CONSUMED",
        reason_code=None,
    )

    assert set(decision) == {
        "schema_version", "scope", "generation", "configuration_sha256",
        "trigger_delivery_id", "trigger_kind", "before_checkpoint_sha256",
        "after_checkpoint_sha256", "time_input_sha256", "time_result", "events",
        "candidates", "outcome", "reason_code",
    }
    assert decision["schema_version"] == "SPIDER_SOURCE_TIME_DECISION_V1"
    assert decision["time_input_sha256"] == sha256(raw).hexdigest()
    assert decision["time_result"] == {
        "event": "ANCHOR",
        "observation_id": "d" * 64,
        "predecessor_observation_id": None,
        "epoch_id": "epoch-1",
        "observed_utc_ms": _UTC,
        "elapsed_ms": 100,
        "time_zone": "UTC",
        "time_policy_id": "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1",
        "source_now_ms": 500,
    }

    with pytest.raises(ValueError):
        model_time_decision_payload(
            raw,
            transition,
            scope=_SCOPE,
            generation=1,
            configuration_sha256="c" * 64,
            trigger_delivery_id="d" * 64,
            before_checkpoint_sha256=_HASH,
            after_checkpoint_sha256="f" * 64,
            events=[],
            candidates=[],
            outcome="CONSUMED",
            reason_code=None,
        )


def test_policy_failed_time_decision_keeps_checkpoint_and_no_candidates() -> None:
    value = _envelope("TIMER", previous_id="d" * 64, snapshots=_snapshots())
    raw = canonical_bytes(value)
    transition = _decode(value, previous=_record())

    decision = model_time_decision_payload(
        raw,
        transition,
        scope=_SCOPE,
        generation=1,
        configuration_sha256="c" * 64,
        trigger_delivery_id="d" * 64,
        before_checkpoint_sha256=_HASH,
        after_checkpoint_sha256=_HASH,
        events=[{"kind": "alert"}],
        candidates=[],
        outcome="POLICY_FAILED",
        reason_code="SOURCE_POLICY_FAILED",
    )

    assert decision["outcome"] == "POLICY_FAILED"
    assert decision["reason_code"] == "SOURCE_POLICY_FAILED"
    assert decision["events"] == [{"kind": "alert"}]
    assert decision["candidates"] == []
