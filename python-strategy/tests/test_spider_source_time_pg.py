"""Actual-owner PostgreSQL acceptance for MODEL SOURCE_TIME deliveries."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, localcontext
from hashlib import sha256
from threading import RLock
import os
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.spider_management.orm import (
    SpiderRuntimeDelivery,
    SpiderRuntimeScope,
    SpiderRuntimeTransmission,
)
from src.core.spider_management.runtime_owner import (
    RuntimeScopeKey,
    SpiderRuntimeOwner,
)
from src.core.strategy_state_manager import StrategyStateManager
from test_spider_source_market_pg import (
    _RedisProbe,
    _financial_facts,
    _owner,
    _source_rows,
    operation_pg as _operation_pg,
)
from test_spider_financial_delivery_pg import _install_owner
from test_spider_model_bootstrap import _admitted_run

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]

_UTC = 1_800_000_000_000
_NOON_UTC = _UTC + 14_400_000
_POLICY_ID = "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1"


def _observation_id(label: str) -> str:
    return sha256(label.encode("ascii")).hexdigest()


def _runtime(factory: Any, scope: RuntimeScopeKey) -> SpiderRuntimeScope:
    with factory() as session:
        return session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()


def _checkpoint_doc(raw: bytes) -> dict[str, Any]:
    return cast(dict[str, Any], decode_canonical(raw))


def _capital(doc: dict[str, Any]) -> dict[str, object]:
    return {
        cast(str, row["key"]): row["value"]
        for row in cast(list[dict[str, object]], doc["capital"])
    }


def _row_leverages(doc: dict[str, Any]) -> tuple[tuple[str, Decimal], ...]:
    rows = cast(list[dict[str, Any]], doc["rows"])
    return tuple(
        (
            cast(str, cast(dict[str, object], row["name"])["value"]),
            Decimal(cast(str, cast(dict[str, object], row["leverage"])["value"])),
        )
        for row in rows
    )


def _model_time(
    scope: RuntimeScopeKey,
    prepared: Any,
    runtime: SpiderRuntimeScope,
    *,
    observation_id: str,
    event: str,
    epoch_id: str,
    observed_utc_ms: int,
    elapsed_ms: int,
    predecessor_id: str | None = None,
    checkpoint_sha256: str | None = None,
    snapshots: dict[str, object] | None = None,
    provenance: dict[str, str] | None = None,
) -> bytes:
    return canonical_bytes({
        "schema_version": "SPIDER_MODEL_TIME_DELIVERY_V1",
        "scope": {
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
        },
        "effective_identity": {
            "generation": runtime.generation,
            "confirmation_request_id": runtime.confirmation_request_id,
            "configuration_sha256": runtime.configuration_sha256,
        },
        "observation_id": observation_id,
        "predecessor_observation_id": predecessor_id,
        "checkpoint_before_sha256": checkpoint_sha256 or runtime.source_checkpoint_sha256,
        "provenance": provenance or {
            "kind": "MODEL",
            "evidence_sha256": prepared.historical_input_artifact_sha256,
        },
        "event": event,
        "clock": {
            "epoch_id": epoch_id,
            "observed_utc_ms": observed_utc_ms,
            "elapsed_ms": elapsed_ms,
            "time_zone": "UTC",
            "time_policy_id": _POLICY_ID,
        },
        "snapshots": snapshots,
    })


def _timer_snapshots(prepared: Any, observed_utc_ms: int) -> dict[str, object]:
    checkpoint = _checkpoint_doc(bytes(prepared.build_checkpoint_bytes))
    products = [
        cast(dict[str, Any], item)["product_id"]
        for item in cast(list[object], checkpoint["markets"])
    ]
    return {
        "earn": "0",
        "trading": {"eq": "100", "availEq": "90"},
        "positions": {cast(str, product): "0" for product in products},
        "open_orders": None,
        "as_of_ms": observed_utc_ms,
    }


def _time_rows(factory: Any, scope: RuntimeScopeKey) -> list[SpiderRuntimeDelivery]:
    return [
        row for row in _source_rows(factory, scope)
        if row.kind == "SOURCE_TIME"
    ]


def _decision(row: SpiderRuntimeDelivery) -> dict[str, Any]:
    assert row.decision_payload is not None
    return cast(dict[str, Any], decode_canonical(bytes(row.decision_payload)))


def _new_owner(
    factory: Any,
    scope: RuntimeScopeKey,
    order_manager: Any,
    confirmation: Any,
    source: Any,
) -> SpiderRuntimeOwner:
    lifecycle_lock = RLock()
    return SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={confirmation.request_id: source},
        strategy_state_manager=StrategyStateManager(factory, _RedisProbe()),
        lifecycle_lock=lambda _strategy_id: lifecycle_lock,
        order_event_lock=RLock(),
    )


def _owner_with_leverage_up(engine: Engine):
    run = _admitted_run()
    parameters = dict(run.parameters)
    parameters["LeverageUp"] = Decimal("0.01")
    run = replace(run, parameters=tuple(sorted(parameters.items())))
    _installed, scope, _operation_id, order_manager, prepared, _initial, confirmation, source = (
        _install_owner(engine, admitted_run=run)
    )
    factory = sessionmaker(engine)
    owner = _new_owner(factory, scope, order_manager, confirmation, source)
    return owner, scope, factory, prepared


def _assert_no_transmissions(factory: Any, scope: RuntimeScopeKey) -> None:
    with factory() as session:
        rows = session.execute(
            select(SpiderRuntimeTransmission.transmission_group_id).where(
                SpiderRuntimeTransmission.environment == scope.environment,
                SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
            )
        ).all()
    assert rows == []


def test_anchor_duplicate_conflict_and_fresh_owner_readback_without_new_input(
    operation_pg: Engine,
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, order_manager, confirmation, source = _owner(operation_pg)
    before_runtime = _runtime(factory, scope)
    before_checkpoint = bytes(before_runtime.source_checkpoint)
    before_financial = _financial_facts(factory, scope)
    anchor = _model_time(
        scope, prepared, before_runtime,
        observation_id=_observation_id("anchor-one"),
        event="ANCHOR", epoch_id="epoch-one", observed_utc_ms=_UTC,
        elapsed_ms=100,
    )

    accepted = owner.receive_source_time(anchor)
    assert accepted.success and accepted.data is not None
    assert accepted.data["disposition"] == "ACCEPTED"
    assert accepted.data["delivery_disposition"] == "CONSUMED"
    assert accepted.data["delivery_sequence"] == 1
    first_readback = owner.readback(scope, operation_id)
    assert first_readback is not None
    after_runtime = _runtime(factory, scope)
    assert bytes(after_runtime.source_checkpoint) == before_checkpoint
    assert after_runtime.source_checkpoint_sha256 == sha256(before_checkpoint).hexdigest()
    assert _financial_facts(factory, scope) == before_financial
    rows = _time_rows(factory, scope)
    assert len(rows) == 1
    assert rows[0].delivery_id == _observation_id("anchor-one")
    assert rows[0].source_payload == anchor
    assert rows[0].content_sha256 == sha256(anchor).hexdigest()
    decision = _decision(rows[0])
    assert decision["schema_version"] == "SPIDER_SOURCE_TIME_DECISION_V1"
    assert decision["events"] == [] and decision["candidates"] == []
    assert decision["before_checkpoint_sha256"] == decision["after_checkpoint_sha256"]

    duplicate = owner.receive_source_time(anchor)
    assert duplicate.success and duplicate.data is not None
    assert duplicate.data["disposition"] == "DUPLICATE"
    assert duplicate.data["delivery_sequence"] == accepted.data["delivery_sequence"]
    conflict_value = cast(dict[str, Any], decode_canonical(anchor))
    cast(dict[str, Any], conflict_value["clock"])["elapsed_ms"] = 101
    conflict = owner.receive_source_time(canonical_bytes(conflict_value))
    assert not conflict.success and conflict.data is not None
    assert conflict.data["disposition"] == "CONFLICT"
    assert len(_time_rows(factory, scope)) == 1
    assert owner.readback(scope, operation_id) == first_readback
    _assert_no_transmissions(factory, scope)

    fresh = _new_owner(factory, scope, order_manager, confirmation, source)
    assert fresh.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert fresh.readback(scope, operation_id) == first_readback
    assert len(_time_rows(factory, scope)) == 1


def test_timer_uses_elapsed_delta_and_runs_once_at_utc_noon(operation_pg: Engine) -> None:
    owner, scope, factory, prepared = _owner_with_leverage_up(operation_pg)
    initial_runtime = _runtime(factory, scope)
    initial_doc = _checkpoint_doc(bytes(initial_runtime.source_checkpoint))
    anchor = _model_time(
        scope, prepared, initial_runtime,
        observation_id=_observation_id("timer-anchor"),
        event="ANCHOR", epoch_id="timer-epoch", observed_utc_ms=_NOON_UTC - 20_000,
        elapsed_ms=1_000,
    )
    assert owner.receive_source_time(anchor).data is not None
    after_anchor = _runtime(factory, scope)
    assert bytes(after_anchor.source_checkpoint) == bytes(initial_runtime.source_checkpoint)

    timer = _model_time(
        scope, prepared, after_anchor,
        observation_id=_observation_id("timer-once"),
        event="TIMER", epoch_id="timer-epoch", observed_utc_ms=_NOON_UTC,
        elapsed_ms=2_000, predecessor_id=_observation_id("timer-anchor"),
        snapshots=_timer_snapshots(prepared, _NOON_UTC),
    )
    first = owner.receive_source_time(timer)
    assert first.success and first.data is not None
    assert first.data["delivery_disposition"] == "CONSUMED"
    after_timer = _runtime(factory, scope)
    after_doc = _checkpoint_doc(bytes(after_timer.source_checkpoint))
    assert after_doc["now_ms"] == initial_doc["now_ms"] + 1_000
    assert _capital(after_doc)["day_ago"] != _capital(initial_doc).get("day_ago")
    before_leverages = _row_leverages(initial_doc)
    after_leverages = _row_leverages(after_doc)
    assert [name for name, _ in after_leverages] == [name for name, _ in before_leverages]
    with localcontext() as context:
        context.prec = 50
        factor = (Decimal(1) + Decimal("0.01")) ** (Decimal(5) / Decimal(86_400))
        cap = Decimal("20") / len(before_leverages)
        for (_, before_leverage), (_, after_leverage) in zip(
            before_leverages, after_leverages, strict=True
        ):
            assert before_leverage * factor < cap
            assert after_leverage == before_leverage * factor
    row = _time_rows(factory, scope)[-1]
    decision = _decision(row)
    assert decision["outcome"] == "CONSUMED"
    assert decision["time_result"]["source_now_ms"] == after_doc["now_ms"]
    assert decision["time_result"]["observed_utc_ms"] == _NOON_UTC
    assert decision["events"]
    assert decision["candidates"] == []

    duplicate = owner.receive_source_time(timer)
    assert duplicate.success and duplicate.data is not None
    assert duplicate.data["disposition"] == "DUPLICATE"
    assert duplicate.data["delivery_sequence"] == first.data["delivery_sequence"]
    after_duplicate = _runtime(factory, scope)
    assert bytes(after_duplicate.source_checkpoint) == bytes(after_timer.source_checkpoint)
    assert _capital(_checkpoint_doc(bytes(after_duplicate.source_checkpoint)))["day_ago"] == _capital(after_doc)["day_ago"]
    assert _row_leverages(_checkpoint_doc(bytes(after_duplicate.source_checkpoint))) == after_leverages
    assert len(_time_rows(factory, scope)) == 2
    _assert_no_transmissions(factory, scope)


def test_restart_counts_downtime_once_preserves_anchors_then_uses_new_elapsed_origin(
    operation_pg: Engine,
) -> None:
    owner, scope, _operation_id, factory, prepared, _initial, *_ = _owner(operation_pg)
    runtime = _runtime(factory, scope)
    anchor = _model_time(
        scope, prepared, runtime,
        observation_id=_observation_id("restart-anchor"),
        event="ANCHOR", epoch_id="old-epoch", observed_utc_ms=_UTC,
        elapsed_ms=900,
    )
    assert owner.receive_source_time(anchor).data is not None
    before_restart = _runtime(factory, scope)
    before_doc = _checkpoint_doc(bytes(before_restart.source_checkpoint))
    restart = _model_time(
        scope, prepared, before_restart,
        observation_id=_observation_id("restart-once"),
        event="RESTART", epoch_id="new-epoch", observed_utc_ms=_UTC + 20_000,
        elapsed_ms=0, predecessor_id=_observation_id("restart-anchor"),
    )
    accepted = owner.receive_source_time(restart)
    assert accepted.success and accepted.data is not None
    after_restart = _runtime(factory, scope)
    after_doc = _checkpoint_doc(bytes(after_restart.source_checkpoint))
    assert after_doc["now_ms"] == before_doc["now_ms"] + 20_000
    before_anchors = dict(before_doc)
    after_anchors = dict(after_doc)
    del before_anchors["now_ms"]
    del after_anchors["now_ms"]
    assert after_anchors == before_anchors
    restart_row = _time_rows(factory, scope)[-1]
    restart_decision = _decision(restart_row)
    assert restart_decision["events"] == []
    assert restart_decision["candidates"] == []
    assert restart_decision["before_checkpoint_sha256"] != restart_decision["after_checkpoint_sha256"]

    repeated_restart = owner.receive_source_time(restart)
    assert repeated_restart.success and repeated_restart.data is not None
    assert repeated_restart.data["disposition"] == "DUPLICATE"
    assert len(_time_rows(factory, scope)) == 2
    assert bytes(_runtime(factory, scope).source_checkpoint) == bytes(after_restart.source_checkpoint)

    timer = _model_time(
        scope, prepared, after_restart,
        observation_id=_observation_id("new-epoch-timer"),
        event="TIMER", epoch_id="new-epoch", observed_utc_ms=_UTC + 20_010,
        elapsed_ms=5, predecessor_id=_observation_id("restart-once"),
        snapshots=_timer_snapshots(prepared, _UTC + 20_010),
    )
    timer_result = owner.receive_source_time(timer)
    assert timer_result.success and timer_result.data is not None
    final_runtime = _runtime(factory, scope)
    final_doc = _checkpoint_doc(bytes(final_runtime.source_checkpoint))
    assert final_doc["now_ms"] == after_doc["now_ms"] + 5
    final_rows = _time_rows(factory, scope)
    assert [row.delivery_id for row in final_rows] == [
        _observation_id("restart-anchor"),
        _observation_id("restart-once"),
        _observation_id("new-epoch-timer"),
    ]
    assert final_rows[-1].disposition == "CONSUMED"
    _assert_no_transmissions(factory, scope)


def test_invalid_predecessor_epoch_checkpoint_and_provenance_reject_before_append(
    operation_pg: Engine,
) -> None:
    owner, scope, _operation_id, factory, prepared, _initial, *_ = _owner(operation_pg)
    runtime = _runtime(factory, scope)
    anchor_id = _observation_id("invalid-matrix-anchor")
    anchor = _model_time(
        scope, prepared, runtime,
        observation_id=anchor_id, event="ANCHOR", epoch_id="valid-epoch",
        observed_utc_ms=_UTC, elapsed_ms=0,
    )
    accepted = owner.receive_source_time(anchor)
    assert accepted.success
    current = _runtime(factory, scope)
    baseline_checkpoint = bytes(current.source_checkpoint)
    baseline_sha = current.source_checkpoint_sha256
    baseline_frontiers = (
        current.last_received_sequence,
        current.last_consumed_sequence,
        current.last_tape_sequence,
    )
    baseline_financial = _financial_facts(factory, scope)

    cases: tuple[tuple[str, str | None, str, str | None, dict[str, str] | None], ...] = (
        ("wrong-predecessor", _observation_id("stale-parent"), "valid-epoch", baseline_sha, None),
        ("wrong-epoch", anchor_id, "other-epoch", baseline_sha, None),
        ("wrong-checkpoint", anchor_id, "valid-epoch", "f" * 64, None),
        ("wrong-provenance", anchor_id, "valid-epoch", baseline_sha,
         {"kind": "PROVIDER", "evidence_sha256": "e" * 64}),
    )
    for label, predecessor_id, epoch_id, checkpoint_sha256, provenance in cases:
        invalid = _model_time(
            scope, prepared, current,
            observation_id=_observation_id(label),
            event="TIMER", epoch_id=epoch_id,
            observed_utc_ms=_UTC + 1_000, elapsed_ms=1_000,
            predecessor_id=predecessor_id,
            checkpoint_sha256=checkpoint_sha256,
            snapshots=_timer_snapshots(prepared, _UTC + 1_000),
            provenance=provenance,
        )
        result = owner.receive_source_time(invalid)
        assert not result.success and result.data is not None
        assert result.data["disposition"] == "REJECTED"
        after = _runtime(factory, scope)
        assert (
            after.last_received_sequence,
            after.last_consumed_sequence,
            after.last_tape_sequence,
        ) == baseline_frontiers
        assert bytes(after.source_checkpoint) == baseline_checkpoint
        assert after.source_checkpoint_sha256 == baseline_sha
        assert _financial_facts(factory, scope) == baseline_financial
        assert len(_time_rows(factory, scope)) == 1
    _assert_no_transmissions(factory, scope)
