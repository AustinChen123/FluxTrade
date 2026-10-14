"""Durable SOURCE_TIME frontier and failure-exit proofs on isolated PostgreSQL."""

from __future__ import annotations

import os
from decimal import Decimal
from hashlib import sha256
from typing import Any, cast

import pytest
from sqlalchemy import event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport, classify_okx_batch_response
from src.core.clock import BacktestClock
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent, IExchangeAdapter
from src.core.orm_models import Order, Trade
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)
from src.core.spider_management.runtime_owner import RuntimeScopeKey, SpiderRuntimeOwner
from test_spider_control_command_pg import _command, _dispatch
from test_spider_financial_delivery_pg import (
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_source_market_pg import (
    _financial_facts,
    _market_payload,
    _owner,
    _source_rows,
    operation_pg as _operation_pg,
)
from test_spider_source_time_pg import (
    _UTC,
    _decision,
    _model_time,
    _new_owner,
    _observation_id,
    _runtime,
    _time_rows,
    _timer_snapshots,
)

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _time(
    scope: RuntimeScopeKey,
    prepared: Any,
    runtime: SpiderRuntimeScope,
    observation: str,
    event_name: str,
    *,
    epoch: str = "recovery-epoch",
    utc_ms: int = _UTC,
    elapsed_ms: int = 0,
    predecessor: str | None = None,
) -> bytes:
    return _model_time(
        scope,
        prepared,
        runtime,
        observation_id=_observation_id(observation),
        event=event_name,
        epoch_id=epoch,
        observed_utc_ms=utc_ms,
        elapsed_ms=elapsed_ms,
        predecessor_id=predecessor,
        snapshots=(
            _timer_snapshots(prepared, utc_ms) if event_name == "TIMER" else None
        ),
    )


def _time_state(factory: Any, scope: RuntimeScopeKey) -> tuple[Any, ...]:
    runtime = _runtime(factory, scope)
    deliveries = tuple(
        (
            row.sequence,
            row.delivery_id,
            row.kind,
            row.disposition,
            row.suppression_reason,
            row.content_sha256,
            bytes(row.source_payload),
            row.decision_sha256,
        )
        for row in _source_rows(factory, scope)
    )
    with factory() as session:
        actions = tuple(
            (
                action.action_id,
                action.trigger_delivery_id,
                action.operation,
                action.original_payload,
                action.outcome_state,
            )
            for action in session.execute(
                select(SpiderRuntimeAction)
                .where(SpiderRuntimeAction.strategy_id == scope.strategy_id)
                .order_by(SpiderRuntimeAction.candidate_ordinal)
            ).scalars()
        )
        tapes = tuple(
            (row.ordinal, row.kind, row.delivery_sequence)
            for row in session.execute(
                select(SpiderRuntimeTapeIndex)
                .where(SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id)
                .order_by(SpiderRuntimeTapeIndex.ordinal)
            ).scalars()
        )
    return (
        bytes(runtime.source_checkpoint),
        runtime.source_checkpoint_sha256,
        runtime.last_received_sequence,
        runtime.last_consumed_sequence,
        runtime.last_tape_sequence,
        deliveries,
        actions,
        tapes,
    )


def _stop(owner: SpiderRuntimeOwner, scope: RuntimeScopeKey, operation_id: str, key: str):
    readback = owner.readback(scope, operation_id)
    assert readback is not None
    return _dispatch(owner, _command(
        scope, key=key, version=cast(int, readback["state_version"])
    ))


@pytest.mark.parametrize("event_name", ["ANCHOR", "TIMER", "RESTART"])
def test_stop_suppresses_each_source_time_event_without_advancing_time_predecessor(
    operation_pg: Engine, event_name: str, monkeypatch
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, *_ = _owner(operation_pg)
    checkpoint_before = bytes(_runtime(factory, scope).source_checkpoint)
    anchor_id = _observation_id("stop-anchor")
    if event_name != "ANCHOR":
        anchor = _time(scope, prepared, _runtime(factory, scope), "stop-anchor", "ANCHOR")
        accepted_anchor = owner.receive_source_time(anchor)
        assert accepted_anchor.success and accepted_anchor.data is not None
        assert accepted_anchor.data["delivery_disposition"] == "CONSUMED"
        checkpoint_before = bytes(_runtime(factory, scope).source_checkpoint)

    stopped = _stop(owner, scope, operation_id, f"stop-time-{event_name.lower()}")
    assert stopped.success
    financial_before = _financial_facts(factory, scope)
    runtime_before = _runtime(factory, scope)
    raw = _time(
        scope,
        prepared,
        runtime_before,
        f"stop-{event_name.lower()}-input",
        event_name,
        epoch="next-epoch" if event_name == "RESTART" else "recovery-epoch",
        utc_ms=_UTC + 20_000 if event_name == "RESTART" else _UTC + 1_000,
        elapsed_ms=0 if event_name == "RESTART" else 1_000,
        predecessor=anchor_id if event_name != "ANCHOR" else None,
    )
    from src.core.backtest.spider_policy import Policy

    timer_calls: list[int] = []
    original_timer = Policy.timer_action

    def counted_timer(self, now_ms, **kwargs):
        timer_calls.append(now_ms)
        return original_timer(self, now_ms, **kwargs)

    monkeypatch.setattr(Policy, "timer_action", counted_timer)
    result = owner.receive_source_time(raw)
    assert result.success and result.data is not None
    assert result.data["delivery_disposition"] == "SUPPRESSED"
    row = next(row for row in _time_rows(factory, scope) if row.delivery_id == _observation_id(f"stop-{event_name.lower()}-input"))
    assert row.disposition == "SUPPRESSED"
    assert row.suppression_reason == "STOP_CONTROL"
    assert bytes(_runtime(factory, scope).source_checkpoint) == checkpoint_before
    assert timer_calls == []
    consumed = [row.delivery_id for row in _time_rows(factory, scope) if row.disposition == "CONSUMED"]
    assert consumed == ([] if event_name == "ANCHOR" else [anchor_id])
    after = owner.readback(scope, operation_id)
    assert after is not None and after["runtime_control"] == "STOP"
    assert _financial_facts(factory, scope) == financial_before


def test_drain_holds_time_prefix_and_rejects_new_identity_until_prefix_resolves(
    operation_pg: Engine, monkeypatch
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, order_manager, confirmation, source = _owner(operation_pg)
    monkeypatch.setattr(owner, "_drain_source_market_scope", lambda _scope: None)
    financial_before = _financial_facts(factory, scope)
    anchor = _time(scope, prepared, _runtime(factory, scope), "drain-anchor", "ANCHOR")
    pending = owner.receive_source_time(anchor)
    assert pending.success and pending.data is not None
    assert pending.data["delivery_disposition"] == "PENDING"
    with factory.begin() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            ).with_for_update()
        ).scalar_one()
        runtime.control_mode = "DRAIN"
    monkeypatch.undo()
    owner._drain_source_market_scope(scope)
    before = _time_state(factory, scope)
    assert _time_rows(factory, scope)[0].disposition == "PENDING"

    later = _time(
        scope,
        prepared,
        _runtime(factory, scope),
        "drain-later-timer",
        "TIMER",
        utc_ms=_UTC + 1_000,
        elapsed_ms=1_000,
        predecessor=_observation_id("drain-anchor"),
    )
    rejected = owner.receive_source_time(later)
    assert not rejected.success and rejected.data is not None
    assert rejected.data["disposition"] == "REJECTED"
    assert _time_state(factory, scope) == before
    assert _financial_facts(factory, scope) == financial_before
    cold = _new_owner(factory, scope, order_manager, confirmation, source)
    assert cold.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert _time_state(factory, scope) == before
    assert cold.readback(scope, operation_id) is not None


@pytest.mark.parametrize("blocker", ["market", "financial"])
def test_earlier_unresolved_market_or_financial_prefix_blocks_time_and_cold_start_mints_none(
    operation_pg: Engine, blocker: str, monkeypatch
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, order_manager, confirmation, source = _owner(operation_pg)
    monkeypatch.setattr(owner, "_drain_source_market_scope", lambda _scope: None)
    if blocker == "market":
        result = owner.receive_source_market(
            _market_payload(scope, prepared, "d" * 64, price="24.5")
        )
        assert result.success and result.data is not None
        assert result.data["delivery_disposition"] == "PENDING"
    else:
        class AdapterProbe:
            def supports_runtime_reconciliation(self) -> bool:
                return False

            def close(self) -> None:
                pass

        with factory() as session:
            order = session.execute(
                select(Order).where(Order.strategy_id == scope.strategy_id)
                .order_by(Order.id)
            ).scalars().first()
        assert order is not None
        execution = ExecutionEngine(
            db_session=None,
            clock=BacktestClock(1_800_000_000),
            adapter=cast(IExchangeAdapter, AdapterProbe()),
            db_session_factory=factory,
            repository_transaction_participant=owner.participate_in_order_transaction,
            is_backtest=True,
        )
        result = execution.process_exchange_order_event(ExchangeOrderEvent(
            status="open",
            product_id=order.product_id,
            client_order_id=order.client_order_id,
            exchange_order_id="source-time-prefix-order",
        ))
        assert result["action"] == "applied"
    prior_rows = tuple((row.kind, row.disposition, bytes(row.source_payload)) for row in _source_rows(factory, scope))
    anchor = _time(scope, prepared, _runtime(factory, scope), f"blocked-{blocker}-time", "ANCHOR")
    rejected = owner.receive_source_time(anchor)
    assert not rejected.success and rejected.data is not None
    assert rejected.data["disposition"] == "REJECTED"
    assert _time_rows(factory, scope) == []
    assert tuple((row.kind, row.disposition, bytes(row.source_payload)) for row in _source_rows(factory, scope)) == prior_rows
    financial_before = _financial_facts(factory, scope)
    monkeypatch.undo()
    cold = _new_owner(factory, scope, order_manager, confirmation, source)
    assert cold.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert _time_rows(factory, scope) == []
    after = _runtime(factory, scope)
    assert after.last_received_sequence == after.last_consumed_sequence
    assert len(_source_rows(factory, scope)) == len(prior_rows)
    assert cold.readback(scope, operation_id) is not None
    if blocker == "market":
        assert _financial_facts(factory, scope) == financial_before
    else:
        facts_after_cold_start = _financial_facts(factory, scope)
        assert facts_after_cold_start == financial_before
        assert cold.restore_owned_runtimes() == frozenset({scope.strategy_id})
        assert _financial_facts(factory, scope) == facts_after_cold_start
        assert _time_rows(factory, scope) == []


def test_policy_failure_is_a_nonretryable_pending_prefix_then_stop_proves_it_cold(
    operation_pg: Engine, caplog
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, order_manager, confirmation, source = _owner(operation_pg)
    anchor = _time(scope, prepared, _runtime(factory, scope), "failed-timer-anchor", "ANCHOR")
    assert owner.receive_source_time(anchor).data is not None
    runtime = _runtime(factory, scope)
    checkpoint_before = bytes(runtime.source_checkpoint)
    financial_before = _financial_facts(factory, scope)
    actions_before = _time_state(factory, scope)[6]
    timer = _time(
        scope,
        prepared,
        runtime,
        "failed-timer-once",
        "TIMER",
        utc_ms=_UTC + 61_001,
        elapsed_ms=61_001,
        predecessor=_observation_id("failed-timer-anchor"),
    )
    # Negative earn offsets the positive trading equity to zero total.  The
    # original Policy emits its normal prefix, then fails at its real zero-total
    # risk division; this avoids an injected exception or absent-cache shortcut.
    timer_value = cast(dict[str, Any], decode_canonical(timer))
    snapshots = cast(dict[str, Any], timer_value["snapshots"])
    snapshots["earn"] = "-100"
    timer = canonical_bytes(timer_value)
    first = owner.receive_source_time(timer)
    assert first.success and first.data is not None
    assert first.data["delivery_disposition"] == "PENDING"
    row = _time_rows(factory, scope)[-1]
    assert row.disposition == "PENDING"
    decision = _decision(row)
    assert decision["outcome"] == "POLICY_FAILED"
    assert decision["before_checkpoint_sha256"] == decision["after_checkpoint_sha256"]
    event_kinds = [cast(dict[str, object], item)["kind"] for item in cast(list[object], decision["events"])]
    assert event_kinds[0] == "request_earn"
    assert "earn" not in event_kinds
    after_failure = _runtime(factory, scope)
    assert bytes(after_failure.source_checkpoint) == checkpoint_before
    assert after_failure.last_consumed_sequence == 1
    assert _time_state(factory, scope)[6] == actions_before
    assert _financial_facts(factory, scope) == financial_before
    assert decision["events"]
    assert _time_state(factory, scope)[6] == actions_before

    duplicate = owner.receive_source_time(timer)
    assert duplicate.success and duplicate.data is not None
    assert duplicate.data["disposition"] == "DUPLICATE"
    assert _time_rows(factory, scope)[-1].disposition == "PENDING"
    assert bytes(_runtime(factory, scope).source_checkpoint) == checkpoint_before
    assert _time_state(factory, scope)[6] == actions_before
    assert _financial_facts(factory, scope) == financial_before

    stopped = _stop(owner, scope, operation_id, "failed-timer-stop")
    assert stopped.success
    owner._drain_stopped_scope(scope)
    suppressed = _time_rows(factory, scope)[-1]
    assert suppressed.disposition == "SUPPRESSED"
    assert suppressed.suppression_reason == "STOP_CONTROL"
    assert bytes(_runtime(factory, scope).source_checkpoint) == checkpoint_before
    cold = _new_owner(factory, scope, order_manager, confirmation, source)
    assert cold.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert cold.readback(scope, operation_id) is not None
    assert _time_rows(factory, scope)[-1].disposition == "SUPPRESSED"
    assert _financial_facts(factory, scope) == financial_before
    warnings = [
        record.getMessage() for record in caplog.records
        if "SPIDER_SOURCE_TIME_UNAVAILABLE" in record.getMessage()
        and "reason=SOURCE_POLICY_FAILED" in record.getMessage()
    ]
    assert len(warnings) == 1


@pytest.mark.parametrize("fault", ["rollback", "commit_proved", "commit_unproved"])
def test_time_receipt_rollback_or_lost_commit_uses_only_same_id_independent_proof(
    operation_pg: Engine, fault: str, monkeypatch
) -> None:
    owner, scope, _operation_id, factory, prepared, *_ = _owner(operation_pg)
    runtime_before = _runtime(factory, scope)
    checkpoint_before = bytes(runtime_before.source_checkpoint)
    financial_before = _financial_facts(factory, scope)
    durable_before = _time_state(factory, scope)
    anchor = _time(scope, prepared, runtime_before, f"receipt-{fault}", "ANCHOR")
    observation_id = _observation_id(f"receipt-{fault}")
    flags = {"fired": False, "rolled_back": False, "committed": False}

    def before_flush(session: Session, _flush_context: object, _instances: object) -> None:
        if any(
            isinstance(item, SpiderRuntimeDelivery)
            and item.kind == "SOURCE_TIME"
            and item.delivery_id == observation_id
            for item in session.new
        ):
            flags["fired"] = True
            session.info["source_time_receipt_commit"] = True
            if fault == "rollback":
                raise RuntimeError("injected SOURCE_TIME receipt rollback")

    def after_commit(session: Session) -> None:
        if fault != "rollback" and session.info.pop("source_time_receipt_commit", False):
            flags["committed"] = True
            raise RuntimeError("injected SOURCE_TIME receipt response loss")

    def after_rollback(session: Session) -> None:
        if session.info.pop("source_time_receipt_commit", False):
            flags["rolled_back"] = True

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    event.listen(Session, "after_rollback", after_rollback)
    original_proof = owner._source_time_receipt_readback
    proof_hidden = {"value": fault == "commit_unproved"}

    def maybe_unavailable(scope_arg, observation_arg, raw_arg):
        if proof_hidden["value"]:
            return None
        return original_proof(scope_arg, observation_arg, raw_arg)

    if fault == "commit_unproved":
        monkeypatch.setattr(owner, "_source_time_receipt_readback", maybe_unavailable)
    try:
        first = owner.receive_source_time(anchor)
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
        event.remove(Session, "after_rollback", after_rollback)
    assert flags["fired"]
    if fault == "rollback":
        assert flags["rolled_back"] and not flags["committed"]
        assert not first.success and first.data is not None
        assert first.data["disposition"] == "UNKNOWN"
        assert _source_rows(factory, scope) == []
        after = _runtime(factory, scope)
        assert (
            after.last_received_sequence,
            after.last_consumed_sequence,
            after.last_tape_sequence,
        ) == (0, 0, 0)
        assert bytes(after.source_checkpoint) == checkpoint_before
        assert _financial_facts(factory, scope) == financial_before
        assert _time_state(factory, scope) == durable_before
    else:
        assert flags["committed"] and not flags["rolled_back"]
        assert len(_time_rows(factory, scope)) == 1
        saved = _time_rows(factory, scope)[0]
        assert saved.delivery_id == observation_id
        assert saved.source_payload == anchor
        assert saved.disposition == "PENDING"
        if fault == "commit_unproved":
            assert not first.success and first.data is not None
            assert first.data["disposition"] == "UNKNOWN"
            proof_hidden["value"] = False
        else:
            assert first.success and first.data is not None
            assert first.data["disposition"] == "ACCEPTED"
    retry = owner.receive_source_time(anchor)
    assert retry.success and retry.data is not None
    assert retry.data["disposition"] == ("ACCEPTED" if fault == "rollback" else "DUPLICATE")
    rows = _time_rows(factory, scope)
    assert len(rows) == 1 and rows[0].delivery_id == observation_id
    assert rows[0].source_payload == anchor and rows[0].disposition == "CONSUMED"
    assert bytes(_runtime(factory, scope).source_checkpoint) == checkpoint_before


def _candidate_timer_setup(engine: Engine):
    owner, scope, operation_id, _factory, prepared, _initial, order_manager, confirmation, source = _owner(engine)
    owner = _transport_enabled_owner(owner, scope)
    group_id, request, members = _saved_initial_place_request(engine, scope)

    class OfflineAcceptedBatch:
        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            return {
                "code": "0",
                "data": [
                    {
                        "clOrdId": member["wire_clOrdId"],
                        "ordId": f"synthetic-time-order-{index}",
                        "sCode": "0",
                    }
                    for index, member in enumerate(members)
                ],
                "msg": "synthetic accepted initial PLACE",
            }

        def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise AssertionError("SOURCE_TIME candidates are persisted, never transmitted")

    sent = owner.transmit_initial_place_group(
        scope,
        group_id,
        scope.execution_scope_id,
        request,
        OkxSpiderBatchTransport(cast(Any, OfflineAcceptedBatch())),
    )
    assert sent.disposition == "RECORDED"
    assert sent.action_outcomes == tuple(
        (cast(str, member["action_id"]), "ACCEPTED") for member in members
    )
    market = owner.receive_source_market(
        _market_payload(scope, prepared, "c" * 64, price="25", ratio="3")
    )
    assert market.success and market.data is not None
    assert market.data["delivery_disposition"] == "CONSUMED"
    runtime = _runtime(owner._sessions, scope)
    anchor = _time(scope, prepared, runtime, "candidate-anchor", "ANCHOR")
    anchored = owner.receive_source_time(anchor)
    assert anchored.success and anchored.data is not None
    assert anchored.data["delivery_disposition"] == "CONSUMED"
    runtime = _runtime(owner._sessions, scope)
    timer = _time(
        scope,
        prepared,
        runtime,
        "candidate-timer",
        "TIMER",
        utc_ms=_UTC + 1_000,
        elapsed_ms=1_000,
        predecessor=_observation_id("candidate-anchor"),
    )
    return owner, scope, operation_id, owner._sessions, prepared, timer, order_manager, confirmation, source


@pytest.mark.parametrize("fault", ["rollback", "commit_proved", "commit_unproved"])
def test_timer_candidate_transaction_rolls_back_or_proves_same_delivery_commit(
    operation_pg: Engine, fault: str, monkeypatch
) -> None:
    owner, scope, operation_id, factory, _prepared, timer, *_ = _candidate_timer_setup(operation_pg)
    timer_id = _observation_id("candidate-timer")
    before_runtime = _runtime(factory, scope)
    before_checkpoint = bytes(before_runtime.source_checkpoint)
    before_financial = _financial_facts(factory, scope)
    before_actions = _time_state(factory, scope)[6]
    stage_calls: list[str] = []
    original_stage = SpiderRuntimeOwner._stage_normal_candidates

    def counted_stage(self, session, stage_scope, runtime, plan, delivery, events):
        if delivery.delivery_id == timer_id:
            stage_calls.append(delivery.delivery_id)
        return original_stage(self, session, stage_scope, runtime, plan, delivery, events)

    monkeypatch.setattr(SpiderRuntimeOwner, "_stage_normal_candidates", counted_stage)
    flags = {"staged": False, "rolled_back": False, "committed": False}

    def before_flush(session: Session, _flush_context: object, _instances: object) -> None:
        candidate = any(
            isinstance(item, SpiderRuntimeAction)
            and item.trigger_delivery_id == timer_id
            and item.operation == "CANCEL"
            for item in session.new
        )
        if not candidate:
            return
        delivery = next(
            (
                item for item in session.dirty
                if isinstance(item, SpiderRuntimeDelivery)
                and item.delivery_id == timer_id
                and item.disposition == "CONSUMED"
            ),
            None,
        )
        if delivery is None:
            return
        flags["staged"] = True
        session.info["source_time_txb_candidate"] = True
        if fault == "rollback":
            raise RuntimeError("injected SOURCE_TIME TX B candidate rollback")

    def after_commit(session: Session) -> None:
        if fault != "rollback" and session.info.pop("source_time_txb_candidate", False):
            flags["committed"] = True
            raise RuntimeError("injected SOURCE_TIME TX B commit response loss")

    def after_rollback(session: Session) -> None:
        if session.info.pop("source_time_txb_candidate", False):
            flags["rolled_back"] = True

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    event.listen(Session, "after_rollback", after_rollback)
    original_readback = owner.readback
    proof_hidden = {"value": fault == "commit_unproved"}

    def maybe_unavailable(scope_arg, operation_arg):
        if proof_hidden["value"]:
            return None
        return original_readback(scope_arg, operation_arg)

    if fault == "commit_unproved":
        monkeypatch.setattr(owner, "readback", maybe_unavailable)
    try:
        first = owner.receive_source_time(timer)
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
        event.remove(Session, "after_rollback", after_rollback)
    assert flags["staged"] is True
    row = next(row for row in _time_rows(factory, scope) if row.delivery_id == timer_id)
    if fault == "rollback":
        assert flags["rolled_back"] is True and flags["committed"] is False
        assert row.disposition == "PENDING" and row.decision_payload is None
        after = _runtime(factory, scope)
        assert after.last_received_sequence == row.sequence
        assert after.last_consumed_sequence == row.sequence - 1
        assert after.last_tape_sequence == before_runtime.last_tape_sequence + 1
        assert after.source_checkpoint == before_checkpoint
        assert _time_state(factory, scope)[6] == before_actions
        assert _time_state(factory, scope)[7][-1] == (
            before_runtime.last_tape_sequence + 1, "DELIVERY", row.sequence
        )
        assert _financial_facts(factory, scope) == before_financial
        assert len(stage_calls) == 1
    else:
        assert flags["committed"] is True and flags["rolled_back"] is False
        assert row.disposition == "CONSUMED" and row.decision_payload is not None
        after = _runtime(factory, scope)
        assert after.last_consumed_sequence == row.sequence
        assert after.last_tape_sequence == before_runtime.last_tape_sequence + 1
        assert bytes(after.source_checkpoint) != before_checkpoint
        assert _time_state(factory, scope)[7][-1] == (
            before_runtime.last_tape_sequence + 1, "DELIVERY", row.sequence
        )
        assert _financial_facts(factory, scope) == before_financial
        candidates = tuple(action for action in _time_state(factory, scope)[6] if action[1] == timer_id)
        assert candidates and all(candidate[2] == "CANCEL" for candidate in candidates)
        decision = _decision(row)
        assert decision["outcome"] == "CONSUMED"
        assert decision["candidates"]
        assert len(stage_calls) == 1
        if fault == "commit_unproved":
            assert not first.success and first.data is not None
            assert first.data["disposition"] == "UNKNOWN"
            proof_hidden["value"] = False
        else:
            assert first.success and first.data is not None
    duplicate = owner.receive_source_time(timer)
    assert duplicate.success and duplicate.data is not None
    assert duplicate.data["disposition"] == "DUPLICATE"
    after_duplicate_state = _time_state(factory, scope)
    candidates_after = tuple(action for action in after_duplicate_state[6] if action[1] == timer_id)
    assert candidates_after
    assert len(stage_calls) == (2 if fault == "rollback" else 1)
    # A second notification proves neither branch replays a committed callback.
    repeated = owner.receive_source_time(timer)
    assert repeated.success and repeated.data is not None
    assert repeated.data["disposition"] == "DUPLICATE"
    assert _time_state(factory, scope) == after_duplicate_state
    assert len(stage_calls) == (2 if fault == "rollback" else 1)


@pytest.mark.parametrize("tamper", ["input", "decision", "checkpoint", "predecessor", "generation"])
def test_tampered_source_time_proof_is_locally_unavailable_without_writes(
    operation_pg: Engine, tamper: str
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, order_manager, confirmation, source = _owner(operation_pg)
    anchor = _time(scope, prepared, _runtime(factory, scope), f"tamper-{tamper}-anchor", "ANCHOR")
    assert owner.receive_source_time(anchor).success
    timer = _time(
        scope, prepared, _runtime(factory, scope), f"tamper-{tamper}-timer", "TIMER",
        utc_ms=_UTC + 1_000, elapsed_ms=1_000,
        predecessor=_observation_id(f"tamper-{tamper}-anchor"),
    )
    accepted = owner.receive_source_time(timer)
    assert accepted.success and accepted.data is not None
    timer_id = _observation_id(f"tamper-{tamper}-timer")
    with factory() as session:
        row = session.execute(select(SpiderRuntimeDelivery).where(
            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            SpiderRuntimeDelivery.delivery_id == timer_id,
        )).scalar_one()
        if tamper == "input":
            value = cast(dict[str, Any], decode_canonical(bytes(row.source_payload)))
            cast(dict[str, Any], value["clock"])["elapsed_ms"] = 2_000
            row.source_payload = canonical_bytes(value)
            row.content_sha256 = sha256(bytes(row.source_payload)).hexdigest()
        elif tamper == "predecessor":
            value = cast(dict[str, Any], decode_canonical(bytes(row.source_payload)))
            value["predecessor_observation_id"] = _observation_id("wrong-valid-predecessor")
            row.source_payload = canonical_bytes(value)
            row.content_sha256 = sha256(bytes(row.source_payload)).hexdigest()
        elif tamper == "decision":
            assert row.decision_payload is not None
            value = cast(dict[str, Any], decode_canonical(bytes(row.decision_payload)))
            value["after_checkpoint_sha256"] = _observation_id("wrong-decision-checkpoint")
            decision_bytes = canonical_bytes(value)
            row.decision_payload = decision_bytes
            row.decision_sha256 = sha256(decision_bytes).hexdigest()
        elif tamper == "generation":
            assert row.decision_payload is not None
            value = cast(dict[str, Any], decode_canonical(bytes(row.decision_payload)))
            value["generation"] = True
            decision_bytes = canonical_bytes(value)
            row.decision_payload = decision_bytes
            row.decision_sha256 = sha256(decision_bytes).hexdigest()
        else:
            runtime = session.execute(select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )).scalar_one()
            checkpoint = cast(dict[str, Any], decode_canonical(bytes(runtime.source_checkpoint)))
            checkpoint["now_ms"] += 1
            runtime.source_checkpoint = canonical_bytes(checkpoint)
            runtime.source_checkpoint_sha256 = sha256(bytes(runtime.source_checkpoint)).hexdigest()
        session.commit()
    durable_tampered = _time_state(factory, scope)
    facts = _financial_facts(factory, scope)
    cold = _new_owner(factory, scope, order_manager, confirmation, source)
    assert cold.readback(scope, operation_id) is None
    assert cold.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert cold.readback(scope, operation_id) is None
    assert _time_state(factory, scope) == durable_tampered
    assert _financial_facts(factory, scope) == facts


def test_timer_cancel_candidate_late_fill_and_cold_duplicate_are_financially_once(
    operation_pg: Engine, mock_clock: Any, mock_exchange_adapter: Any,
) -> None:
    owner, scope, operation_id, factory, _prepared, timer, order_manager, confirmation, source = _candidate_timer_setup(operation_pg)
    timer_id = _observation_id("candidate-timer")
    result = owner.receive_source_time(timer)
    assert result.success and result.data is not None
    assert result.data["delivery_disposition"] == "CONSUMED"
    with factory() as session:
        candidate = session.execute(select(SpiderRuntimeAction).where(
            SpiderRuntimeAction.strategy_id == scope.strategy_id,
            SpiderRuntimeAction.trigger_delivery_id == timer_id,
            SpiderRuntimeAction.operation == "CANCEL",
        ).order_by(SpiderRuntimeAction.candidate_ordinal)).scalars().first()
        assert candidate is not None and candidate.local_order_id is not None
        timer_row = next(row for row in _time_rows(factory, scope) if row.delivery_id == timer_id)
        decision = _decision(timer_row)
        planned = [
            cast(dict[str, object], value)
            for value in cast(list[object], decision["candidates"])
            if type(value) is dict
            and cast(dict[str, object], value).get("operation") == "CANCEL"
            and cast(dict[str, object], value).get("disposition") == "PLANNED"
        ]
        selected = next(value for value in planned if value["action_id"] == candidate.action_id)
        event_index = selected["source_event_index"]
        event_candidates = sorted(
            [value for value in planned if value["source_event_index"] == event_index],
            key=lambda value: cast(int, value["candidate_ordinal"]),
        )
        assert event_candidates
        initial_targets: list[tuple[str, str, str | None, str, object, object, object]] = []
        cancel_members: list[dict[str, object]] = []
        cancel_payloads: list[dict[str, object]] = []
        source_alias_ids: list[str] = []
        cancel_action_ids: list[str] = []
        runtime = _runtime(factory, scope)
        plan = session.get(SpiderRuntimePlan, runtime.initial_plan_id)
        assert plan is not None
        first_order: Order | None = None
        for saved_candidate in event_candidates:
            action = session.get(SpiderRuntimeAction, cast(str, saved_candidate["action_id"]))
            assert action is not None and action.parent_action_id is not None
            target = session.get(SpiderRuntimeAction, action.parent_action_id)
            assert target is not None and target.local_order_id is not None
            binding = session.get(SpiderRuntimeBinding, target.action_id)
            order = session.get(Order, target.local_order_id)
            assert binding is not None and order is not None and order.price is not None and order.quantity > 0
            assert target.outcome_reference is not None
            place_transmission = session.get(SpiderRuntimeTransmission, target.outcome_reference)
            assert place_transmission is not None and place_transmission.response_payload is not None
            place_outcomes = classify_okx_batch_response(
                bytes(place_transmission.request_payload), bytes(place_transmission.response_payload)
            )
            assert place_outcomes is not None
            place_outcome = place_outcomes[target.action_id]
            assert place_outcome[0] == "ACCEPTED" and place_outcome[2] is not None
            provider_order_id = place_outcome[2]
            target_intent = cast(dict[str, object], order.intent_payload)
            binding_bytes = bytes.fromhex(cast(str, target_intent["provider_binding_bytes_hex"]))
            source_target = cast(dict[str, object], decode_canonical(bytes(target.original_payload)))
            cancel_payload = {"instId": source_target["instId"], "ordId": provider_order_id}
            cancel_payloads.append(cancel_payload)
            source_alias_ids.append(binding.source_client_order_id)
            cancel_action_ids.append(action.action_id)
            cancel_members.append({
                "cancel_action_id": action.action_id,
                "product_id": target.product_id,
                "target_place_action_id": target.action_id,
                "target_local_order_id": order.id,
                "target_binding_sha256": sha256(binding_bytes).hexdigest(),
                "target_wire_clOrdId": target_intent["provider_wire_client_order_id"],
                "target_ordId": provider_order_id,
                "request_payload": cancel_payload,
            })
            initial_targets.append((
                order.id, order.product_id, order.client_order_id, provider_order_id,
                order.price, order.quantity, binding.source_client_order_id,
            ))
            if action.action_id == candidate.action_id:
                first_order = order
        assert first_order is not None
        order_id, product_id = first_order.id, first_order.product_id
        client_order_id, price, quantity = (
            first_order.client_order_id, first_order.price, first_order.quantity
        )
        provider_order_id = next(cast(str, target[3]) for target in initial_targets if target[0] == first_order.id)
        cancel_group_id = sha256(canonical_bytes({
            "domain": "SPIDER_TRANSMISSION_GROUP_V1",
            "scope": {
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
            },
            "operation_id": runtime.current_operation_id,
            "plan_id": plan.plan_id,
            "generation": runtime.generation,
            "configuration_sha256": runtime.configuration_sha256,
            "operation": "CANCEL",
            "ordered_action_ids": cancel_action_ids,
        })).hexdigest()
        cancel_request: dict[str, object] = {
            "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
            "scope": {
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
            },
            "executing_account_ref": scope.execution_scope_id,
            "transmission_group_id": cancel_group_id,
            "operation": "CANCEL",
            "members": cancel_members,
        }
        cancel_request["payload_sha256"] = sha256(canonical_bytes(cancel_request)).hexdigest()
        cancel_request_bytes = canonical_bytes(cancel_request)

    execution = ExecutionEngine(
        db_session=None, clock=mock_clock, adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        owned_order_event_observer=owner.observe_order_event,
        is_backtest=True,
    )
    for target_order_id, target_product_id, target_client_id, target_provider_id, *_ in initial_targets:
        assert execution.process_exchange_order_event(ExchangeOrderEvent(
            status="open", product_id=target_product_id, client_order_id=target_client_id,
            exchange_order_id=target_provider_id, cumulative_filled_quantity=Decimal("0"),
        ))["action"] == "applied"
        owner.observe_order_event(target_order_id)

    class OfflineCancelBatch:
        def privatePostTradeCancelBatchOrders(self, orders: list[dict[str, object]]) -> object:
            assert orders == cancel_payloads
            return {
                "code": "0",
                "data": [
                    {"ordId": cast(str, target[3]), "clOrdId": "", "sCode": "0"}
                    for target in initial_targets
                ],
                "msg": "synthetic accepted timer cancellation",
            }

        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise AssertionError("CANCEL candidate must use the cancel endpoint")

    financial_before_cancel_ack = _financial_facts(factory, scope)
    cancel_ack = owner._transmit_normal_cancel_group(
        scope, cancel_request_bytes, OkxSpiderBatchTransport(cast(Any, OfflineCancelBatch()))
    )
    assert cancel_ack.disposition == "RECORDED"
    assert cancel_ack.action_outcomes == tuple((action_id, "ACCEPTED") for action_id in cancel_action_ids)
    assert _financial_facts(factory, scope) == financial_before_cancel_ack
    cancel_result_rows = [
        row for row in _source_rows(factory, scope)
        if row.kind == "TRANSPORT_RESULT"
        and cast(dict[str, object], decode_canonical(bytes(row.source_payload))).get(
            "transmission_group_id"
        ) == cancel_group_id
    ]
    assert len(cancel_result_rows) == 1
    cancel_result = cancel_result_rows[0]
    assert cancel_result.disposition == "CONSUMED"
    cancel_decision = _decision(cancel_result)
    assert cancel_decision["outcome"] == "CONSUMED"
    cancel_input = cast(dict[str, object], cancel_decision["source_input"])
    assert cancel_input["type"] == "REST_ACK"
    entries = cast(list[dict[str, object]], cancel_input["entries"])
    assert [entry["ordId"] for entry in entries] == source_alias_ids
    assert all(entry["ordId"] != provider_id for entry, provider_id in zip(entries, [target[3] for target in initial_targets], strict=True))
    event_value = ExchangeOrderEvent(
        status="filled", product_id=product_id, client_order_id=client_order_id,
        cumulative_filled_quantity=quantity, cumulative_average_price=price,
        last_fill_quantity=quantity, last_fill_price=price,
        fee=Decimal("0.01"), fee_asset="USDT",
        last_fill_exchange_trade_id="source-time-cancel-late-fill",
    )
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="canceled", product_id=product_id, client_order_id=client_order_id,
        exchange_order_id=provider_order_id, cumulative_filled_quantity=Decimal("0"),
    ))["action"] == "applied"
    assert execution.process_exchange_order_event(event_value)["action"] == "applied"
    owner.observe_order_event(order_id)
    with factory() as session:
        trades = tuple(session.execute(select(Trade).where(Trade.order_id == order_id)).scalars())
        assert len(trades) == 1
        trade_fact = (trades[0].price, trades[0].quantity, trades[0].fee, trades[0].exchange_trade_id)
        assert trade_fact == (price, quantity, Decimal("0.01"), "source-time-cancel-late-fill")
    delivery_rows = _source_rows(factory, scope)
    financial_rows = [
        row for row in delivery_rows
        if row.local_order_id == order_id and row.kind in {"FILL", "ORDER_STATE"}
    ]
    assert {row.kind for row in financial_rows} == {"FILL", "ORDER_STATE"}
    assert all(row.disposition == "CONSUMED" for row in financial_rows)
    after_consumption = _runtime(factory, scope)
    assert after_consumption.last_consumed_sequence == after_consumption.last_received_sequence
    facts = _financial_facts(factory, scope)
    state = _time_state(factory, scope)
    cold = _transport_enabled_owner(
        _new_owner(factory, scope, order_manager, confirmation, source), scope
    )
    assert cold.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert cold.readback(scope, operation_id) is not None
    assert cold.readback(scope, operation_id) is not None
    duplicate_execution = ExecutionEngine(
        db_session=None, clock=mock_clock, adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=cold.participate_in_order_transaction,
        is_backtest=True,
    )
    duplicate_execution.process_exchange_order_event(event_value)
    cold.observe_order_event(order_id)
    assert _financial_facts(factory, scope) == facts
    assert _time_state(factory, scope) == state
    with factory() as session:
        assert len(tuple(session.execute(select(Trade).where(Trade.order_id == order_id)).scalars())) == 1
