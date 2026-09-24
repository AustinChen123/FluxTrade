"""Durable order reuse across terminal-profile commit faults, not broker exactly-once."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from conftest import MockClock, MockExchangeAdapter
from src.core.bootstrap_hydration_reader import BootstrapHydrationReader
from src.core.execution import ExecutionEngine
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.orm_models import Order, SignalAudit, Strategy as StrategyRow
from src.core.pending_market_replay import PendingMarketReplayService
from src.core.repositories import LiveOrderRepository
from src.core.signal_processor import SignalProcessor
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_registry import StrategyRegistry
from test_profile_bootstrap_pg_fixture import (
    Consumer,
    build,
    evidence,
    PRODUCT,
    CUTOVER,
    MINUTE,
)
from test_profile_context_enrichment import context
from test_profile_pending_recovery_pg import terminal_counts
from test_signal_processor import make_candle

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


class LedgerAdapter(MockExchangeAdapter):
    """Immutable call snapshots; adapter Order objects are deliberately mutable."""

    def __init__(self):
        super().__init__()
        self.ledger: tuple[tuple[object, ...], ...] = ()

    def place_order(self, order):
        self.ledger += (
            (order.id, order.client_order_id, order.product_id, order.quantity),
        )
        return super().place_order(order)


def order_evidence(engine):
    with Session(engine) as db:
        orders = tuple(
            db.execute(
                select(
                    Order.id,
                    Order.client_order_id,
                    Order.exchange_order_id,
                    Order.status,
                ).order_by(Order.id)
            ).all()
        )
        audits = tuple(
            db.execute(
                select(
                    SignalAudit.id,
                    SignalAudit.order_id,
                    SignalAudit.client_order_id,
                    SignalAudit.intent_payload,
                    SignalAudit.outcome_payload,
                ).order_by(SignalAudit.id)
            ).all()
        )
    return orders, audits


@pytest.mark.parametrize("fault_phase", ["before_commit", "ack_lost"])
def test_formal_signal_execution_survives_terminal_fault(
    profile_repository_pg, monkeypatch, fault_phase
):
    engine = profile_repository_pg
    original, initial_reader, seeds, inputs, _, _ = build(engine, True)
    monkeypatch.setattr(Consumer, "fresh_instance_for_replay", lambda _: Consumer())
    normal_sessions = sessionmaker(engine)
    with normal_sessions.begin() as db:
        db.add(StrategyRow(id="s", name="profile-recovery"))
    start = CUTOVER + 3 * MINUTE
    candles, pins = [], []
    prefix = initial_reader.prepare(original, start)
    for index in range(2):
        timestamp = start + index * MINUTE
        candles.append(
            make_candle().model_copy(
                update={
                    "product_id": PRODUCT,
                    "timestamp": timestamp,
                    "open": Decimal(60 + index),
                    "high": Decimal(62),
                    "low": Decimal(59),
                    "close": Decimal(60 + index),
                    "volume": Decimal(1),
                }
            )
        )
        item = replace(evidence(4).profiles[0], decision_time_ms=timestamp + MINUTE)
        value = MarketDataDecisionInput(
            prefix.plan.seed.key.decision_key(timestamp),
            original.requirements.profile_requirements,
            timestamp + MINUTE,
            StrategyMarketDataContext(timestamp + MINUTE, (item,)),
        )
        pins.append(inputs.pin(value))
    monkeypatch.setattr(inputs, "pin", MagicMock(side_effect=AssertionError("repin")))
    monkeypatch.setattr(
        inputs, "pin_confirmed", MagicMock(side_effect=AssertionError("repin"))
    )
    cache = MagicMock()
    cache.live_requests.side_effect = AssertionError("cache")
    cache.decision_many.side_effect = AssertionError("cache")
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="deployment",
        identity_resolver=lambda s: strategy_decision_composition("deployment", s),
        cache=cache,
        input_store=inputs,
        utc_ms=lambda: start + MINUTE,
        monotonic_ms=lambda: 100,
    )
    adapter, registry, account = LedgerAdapter(), StrategyRegistry(), MagicMock()
    account.get_position.return_value = None
    registry.register(original)
    executions, handlers, applications, published, order_ids = [], [], [], [], []

    def new_execution():
        execution = ExecutionEngine(
            None,
            MockClock(),
            adapter,
            order_repository=LiveOrderRepository(db_session_factory=normal_sessions),
            db_session_factory=normal_sessions,
            audit_external_orders=True,
            is_backtest=False,
        )
        executions.append(execution)
        return execution

    current_execution = [new_execution()]

    def forward(signal, candle):
        handlers.append((signal.timestamp, signal.metadata["client_order_id"]))
        order_id = current_execution[0].execute_signal(signal, candle)
        assert order_id is not None
        order_ids.append(order_id)
        return True

    processor = SignalProcessor(
        registry,
        current_execution[0],
        signal_handler=forward,
        strategy_context_loader=lambda s, c, _: replace(
            context(),
            strategy_id=s.strategy_id,
            product_id=c.product_id,
            timestamp=c.timestamp,
        ),
    )
    hydration = StrategyHydrationService(
        signal_processor=processor, account_service=account
    )
    hydration.hydrate_candles(
        original, prefix.candles, decision_scope_loader=prefix.decision_scope_loader
    )
    assert not handlers and not adapter.ledger
    armed, fault = [True], DBAPIError(None, None, RuntimeError("terminal boundary"))

    class FaultSession(Session):
        def commit(self):
            if armed[0]:
                armed[0] = False
                assert terminal_counts(self, start) == (1, 1, 1)
                assert len(adapter.ledger) == 1 and len(original.trace) == 5
                if fault_phase == "ack_lost":
                    super().commit()
                    with normal_sessions() as visible:
                        assert terminal_counts(visible, start) == (1, 1, 1)
                raise fault
            return super().commit()

    application = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=sessionmaker(engine, class_=FaultSession),
    )
    reader = BootstrapHydrationReader(
        db_session_factory=normal_sessions,
        seed_store=seeds,
        application=application,
        decision_owner=owner,
        environment="live",
        identity_resolver=lambda s: strategy_decision_composition("deployment", s),
        max_seed_candles=2,
        max_recorded_candles=5,
    )

    def publish(replacement):
        assert replacement is not registry.get("s")
        published.append(replacement)
        registry.register(replacement)

    replay = PendingMarketReplayService(
        db_session_factory=normal_sessions,
        live_candle_application=application,
        strategy_hydration=hydration,
        list_active_strategies=registry.list_active,
        publish_replacement=publish,
        bootstrap_hydration_reader=reader,
    )

    def apply_new(candle):
        applications.append(registry.get("s"))
        scope = owner.begin_candle(candle)
        processor.on_candle(candle, emit_signals=True, decision_scope=scope)
        return scope.build()

    with pytest.raises(DBAPIError) as caught:
        application.apply(
            candles[0], apply_new=apply_new, rebuild_applied=replay.rebuild_applied
        )
    assert caught.value is fault
    first_orders, first_audits = order_evidence(engine)
    assert len(first_orders) == len(first_audits) == 1
    assert first_orders[0].status == "SUBMITTED"
    assert first_orders[0].client_order_id and first_orders[0].exchange_order_id
    assert first_audits[0].order_id == first_orders[0].id
    assert first_audits[0].client_order_id == first_orders[0].client_order_id
    assert first_audits[0].intent_payload and first_audits[0].outcome_payload
    first_ledger = adapter.ledger
    with normal_sessions() as db:
        assert terminal_counts(db, start) == (
            (0, 0, 0) if fault_phase == "before_commit" else (1, 1, 1)
        )
    current_execution[0] = new_execution()  # No in-memory execution/repository reuse.
    assert executions[0] is not executions[1]
    replay.replay(candles[0], apply_new=apply_new)
    assert published[-1] is not original and len(published[-1].trace) == 5
    assert published[-1].accumulator == Decimal(145993)
    assert (
        len(handlers)
        == len(applications)
        == (2 if fault_phase == "before_commit" else 1)
    )
    assert {row[1] for row in handlers} == {first_orders[0].client_order_id}
    assert order_ids == [first_orders[0].id] * len(handlers)
    assert adapter.ledger == first_ledger and order_evidence(engine) == (
        first_orders,
        first_audits,
    )
    replay.replay(candles[0], apply_new=apply_new)
    assert len(handlers) == (2 if fault_phase == "before_commit" else 1)
    assert adapter.ledger == first_ledger and order_evidence(engine) == (
        first_orders,
        first_audits,
    )
    replay.replay(candles[1], apply_new=apply_new)
    orders, audits = order_evidence(engine)
    assert len(orders) == len(audits) == len(adapter.ledger) == 2
    assert len({row.client_order_id for row in orders}) == 2
    assert first_orders[0] in orders and first_audits[0] in audits
    assert adapter.ledger[0] == first_ledger[0]
    with normal_sessions() as db:
        assert (
            terminal_counts(db, start)
            == terminal_counts(db, start + MINUTE)
            == (1, 1, 1)
        )
    for record in pins:
        confirmed = inputs.get(record.value.key)
        assert confirmed is not None
        assert (
            confirmed.value.canonical_bytes,
            confirmed.value.input_digest,
            confirmed.recorded_at,
        ) == (
            record.value.canonical_bytes,
            record.value.input_digest,
            record.recorded_at,
        )
    assert not cache.mock_calls
