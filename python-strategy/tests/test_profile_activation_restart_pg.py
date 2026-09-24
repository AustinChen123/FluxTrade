"""Real consume → fresh runtime restore; not full candle cutover acceptance."""

from dataclasses import replace
from decimal import Decimal
import logging
import os
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event, text
from sqlalchemy.orm import Session, sessionmaker

from src.core.market_data.profiles.bootstrap_seed import BootstrapKey
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.decision_application import (
    MarketDataDecisionBatch,
    MarketDataDecisionOutcome,
)
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.models import StrategyStatus
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.signal_processor import SignalProcessor
from src.core.strategy_activation_intent import (
    ProfileActivationCommand,
    ProfileActivationIntent,
    ProfileActivationRequest,
)
from src.core.strategy_activation_request_store import ProfileActivationRequestStore
from src.core.strategy_activation_service import (
    StrategyActivationService,
    consume_profile_activation_request,
)
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_registry import StrategyRegistry
from src.core.strategy_state_manager import StrategyStateManager
from test_profile_bootstrap_pg_fixture import (
    Consumer,
    build,
    PRODUCT,
    DAY,
    MINUTE,
    CUTOVER,
    evidence,
)
from test_signal_processor import make_candle
from test_profile_context_enrichment import context
from test_strategy_activation_request_store_pg import prepare

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


@pytest.mark.parametrize("stop_gap", [False, True])
def test_consumed_active_restart_rehydrates_without_durable_mutation(
    profile_repository_pg,
    stop_gap,
):
    engine = profile_repository_pg
    original, reader, seeds, inputs, active_sessions, forbidden = build(engine, True)
    identity = strategy_decision_composition("deployment", original)
    key = BootstrapKey(
        "live",
        identity.execution_scope_id,
        original.strategy_id,
        identity.strategy_version,
        identity.config_hash,
        PRODUCT,
        "1m",
    )
    request = ProfileActivationRequest(
        "operator",
        "restart-acceptance",
        ProfileActivationCommand.START,
        ProfileActivationIntent(key, original.requirements, 0),
    )
    prepare(engine, request)
    sessions = sessionmaker(engine)
    store = ProfileActivationRequestStore(sessions)
    admitted = store.admit(request, current=request.intent)
    manager = StrategyStateManager(MagicMock(), MagicMock())
    with Session(engine) as db, db.begin():
        consumed = consume_profile_activation_request(
            db, manager, request, terminal_at=admitted.requested_at
        )
    assert store.confirm(request) == consumed.record

    def durable_state():
        with engine.connect() as db:
            state = db.execute(
                text("SELECT status,version FROM strategy_state WHERE strategy_id='s'")
            ).one()
            audit = db.execute(
                text(
                    "SELECT from_status,to_status,actor,reason,transitioned_at "
                    "FROM strategy_state_transitions WHERE strategy_id='s'"
                )
            ).all()
            return state, audit

    before = durable_state()
    assert before == (
        ("ACTIVE", 1),
        [
            (
                "READY",
                "ACTIVE",
                "operator",
                "ACTIVATION_COMMITTED",
                admitted.requested_at,
            )
        ],
    )
    registry, execution = StrategyRegistry(), MagicMock()
    processor = SignalProcessor(
        registry,
        execution,
        strategy_context_loader=lambda s, c, _: replace(
            context(),
            strategy_id=s.strategy_id,
            product_id=c.product_id,
            timestamp=c.timestamp,
        ),
    )
    processor._process_signals = forbidden
    account = MagicMock()
    account.get_position.return_value = None
    hydration = StrategyHydrationService(
        signal_processor=processor, account_service=account
    )
    if stop_gap:
        first_request, first_record = request, consumed.record
        with Session(engine) as db, db.begin():
            manager.transition_in_transaction(
                db,
                "s",
                StrategyStatus.STOPPED,
                actor="operator",
                reason="stop",
                changed_at=admitted.requested_at,
                expected_version=1,
            )
        application = LiveCandleApplicationService(
            environment_identity=lambda: "live", db_session_factory=sessions
        )
        for index in (5, 6):
            candle = make_candle().model_copy(
                update={
                    "product_id": PRODUCT,
                    "timestamp": DAY + index * MINUTE,
                    "open": Decimal(10),
                    "high": Decimal(11),
                    "low": Decimal(9),
                    "close": Decimal(10),
                    "volume": Decimal(1),
                }
            )
            batch = MarketDataDecisionBatch(
                "live", "deployment", PRODUCT, "1m", candle.timestamp, (), ()
            )
            application.apply(
                candle, apply_new=lambda _, b=batch: b, rebuild_applied=forbidden
            )
        request = replace(
            request,
            idempotency_key="resume-generation",
            command=ProfileActivationCommand.RESUME,
            intent=replace(request.intent, expected_state_version=2),
        )
        admitted = store.admit(request, current=request.intent)
        candidate, cutover = Consumer(), DAY + 7 * MINUTE
        seed_before = seeds.get(key)
        assert (
            reader.prepare_initial_seed_under_admission(
                candidate, cutover, factory=forbidden
            )
            == seed_before
        )
        bound = reader.prepare(candidate, cutover, "BEFORE_PENDING")
        hydration.hydrate_candles(
            candidate, bound.candles, decision_scope_loader=bound.decision_scope_loader
        )
        assert candidate.accumulator == Decimal(14593)
        assert [item[0] for item in candidate.trace] == [
            DAY,
            DAY + MINUTE,
            CUTOVER,
            CUTOVER + 2 * MINUTE,
        ]
        with Session(engine) as db, db.begin():
            consumed = consume_profile_activation_request(
                db, manager, request, terminal_at=admitted.requested_at
            )
        assert store.confirm(first_request) == first_record
        fixed = evidence(4)
        fixed = replace(
            fixed,
            decision_time_ms=cutover + MINUTE,
            profiles=(replace(fixed.profiles[0], decision_time_ms=cutover + MINUTE),),
        )
        pinned = MarketDataDecisionInput(
            key.decision_key(cutover),
            original.requirements.profile_requirements,
            cutover + MINUTE,
            fixed,
        )
        inputs.pin(pinned)
        outcome = MarketDataDecisionOutcome(
            pinned.key, "APPLIED", pinned.input_id, pinned.input_digest
        )
        batch = MarketDataDecisionBatch(
            "live", "deployment", PRODUCT, "1m", cutover, (pinned.key,), (outcome,)
        )
        candle = make_candle().model_copy(
            update={
                "product_id": PRODUCT,
                "timestamp": cutover,
                "open": Decimal(80),
                "high": Decimal(81),
                "low": Decimal(79),
                "close": Decimal(80),
                "volume": Decimal(1),
            }
        )
        application.apply(candle, apply_new=lambda _: batch, rebuild_applied=forbidden)
        reader._max_recorded = (
            4  # Actual participants, not the elapsed wall-clock slots.
        )
        before = durable_state()
        assert before[0] == ("ACTIVE", 3) and len(before[1]) == 3

    class RestoredConsumer(Consumer):
        def __init__(self, strategy_id, product_id):
            super().__init__()
            assert (strategy_id, product_id) == (self.strategy_id, self.product_id)

    def validate(strategies, *, profile_warmup_ready=False):
        assert profile_warmup_ready is True and len(strategies) == 1

    service = StrategyActivationService(
        db_session_factory=sessions,
        transition_to_running=forbidden,
        transition_to_error=forbidden,
        hydration=hydration,
        register_strategy=registry.register,
        register_portfolio=forbidden,
        unregister_runtime_artifact=forbidden,
        environment_identity=lambda: "live",
        assert_context_capabilities=validate,
        event_logger=logging.getLogger(__name__),
        profile_request_store=ProfileActivationRequestStore(sessions),
        profile_identity_resolver=lambda s: strategy_decision_composition(
            "deployment", s
        ),
        bootstrap_reader=reader,
    )
    assert registry.list_active() == []
    statements = []

    def capture(conn, cursor, statement, parameters, ctx, many):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture)
    try:
        assert (
            service.activate_locked(
                "s",
                artifact_cls=RestoredConsumer,
                actor="system",
                reason="startup_restore",
                force=True,
                expected_version=None,
                resolve_product_id=lambda _: PRODUCT,
                assert_live_readiness=lambda _: None,
                build_portfolio_definition=forbidden,
            )
            is True
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    restored = registry.get("s")
    assert type(restored) is RestoredConsumer and restored is not original
    assert restored.accumulator == Decimal(146013 if stop_gap else 14593)
    assert [item[0] for item in restored.trace] == [
        DAY,
        DAY + MINUTE,
        CUTOVER,
        CUTOVER + 2 * MINUTE,
    ] + ([DAY + 7 * MINUTE] if stop_gap else [])
    assert not active_sessions and not forbidden.mock_calls and not execution.mock_calls
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for sql in statements
    )
    assert any(
        "max(market_data_decision_outcome.bar_start_ms)" in sql for sql in statements
    )
    assert durable_state() == before
    assert store.confirm(request) == consumed.record
