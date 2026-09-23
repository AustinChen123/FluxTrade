"""Real terminal-commit failure recovery; not order/execution deduplication."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select, func
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from src.core.bootstrap_hydration_reader import BootstrapHydrationReader
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.market_data.profiles.orm import Base
from src.core.pending_market_replay import PendingMarketReplayService
from src.core.signal_processor import SignalProcessor
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_registry import StrategyRegistry
from test_profile_bootstrap_pg_fixture import (
    Consumer,
    build,
    evidence,
    digest,
    PRODUCT,
    CUTOVER,
    MINUTE,
)
from test_profile_context_enrichment import context
from test_signal_processor import make_candle

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


def terminal_counts(db, start):
    result = []
    for name, column in (
        ("market_data_application", "timestamp"),
        ("market_data_decision_batch", "bar_start_ms"),
        ("market_data_decision_outcome", "bar_start_ms"),
    ):
        table = Base.metadata.tables[name]
        result.append(
            db.scalar(
                select(func.count())
                .select_from(table)
                .where(table.c.product_id == PRODUCT, table.c[column] == start)
            )
        )
    return tuple(result)


@pytest.mark.parametrize("fault_phase", ["before_commit", "ack_lost"])
def test_real_pending_rewind_or_ack_rebuild(
    profile_repository_pg, monkeypatch, fault_phase
):
    engine = profile_repository_pg
    original, fixture_reader, seeds, inputs, _, _ = build(engine, True)
    monkeypatch.setattr(Consumer, "fresh_instance_for_replay", lambda _: Consumer())
    start = CUTOVER + 3 * MINUTE
    candle = make_candle().model_copy(
        update={
            "product_id": PRODUCT,
            "timestamp": start,
            "open": Decimal(60),
            "high": Decimal(61),
            "low": Decimal(59),
            "close": Decimal(60),
            "volume": Decimal(1),
        }
    )
    prefix = fixture_reader.prepare(original, start)
    key = prefix.plan.seed.key.decision_key(start)
    item = replace(evidence(4).profiles[0], decision_time_ms=start + MINUTE)
    pinned = MarketDataDecisionInput(
        key,
        original.requirements.profile_requirements,
        start + MINUTE,
        StrategyMarketDataContext(start + MINUTE, (item,)),
    )
    first_pin = inputs.pin(pinned)
    expected_input = (first_pin.value.canonical_bytes, first_pin.recorded_at)
    cache = MagicMock()
    cache.live_requests.side_effect = AssertionError("must reuse original pin")
    cache.decision_many.side_effect = AssertionError("must reuse original pin")
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="deployment",
        identity_resolver=lambda s: strategy_decision_composition("deployment", s),
        cache=cache,
        input_store=inputs,
        utc_ms=lambda: start + MINUTE,
        monotonic_ms=lambda: 100,
    )
    registry, execution = StrategyRegistry(), MagicMock()
    registry.register(original)
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
    forbidden_signals = MagicMock(
        side_effect=AssertionError("warmup dispatched signals")
    )
    monkeypatch.setattr(processor, "_process_signals", forbidden_signals)
    account = MagicMock()
    account.get_position.return_value = None
    hydration = StrategyHydrationService(
        signal_processor=processor, account_service=account
    )
    hydration.hydrate_candles(
        original, prefix.candles, decision_scope_loader=prefix.decision_scope_loader
    )
    assert original.accumulator == Decimal(14593) and len(original.trace) == 4
    failure = DBAPIError(None, None, RuntimeError("injected commit boundary"))
    armed = [True]
    commit_proof = []

    class FaultSession(Session):
        def commit(self):
            if armed[0]:
                armed[0] = False
                assert terminal_counts(self, start) == (1, 1, 1)
                assert len(original.trace) == 5  # Callback already changed memory.
                if fault_phase == "before_commit":
                    commit_proof.append("rolled_back")
                    raise failure
                super().commit()
                # Independent fresh connection proves commit preceded ACK loss.
                with Session(engine) as observed:
                    assert terminal_counts(observed, start) == (1, 1, 1)
                commit_proof.append("committed")
                raise failure
            return super().commit()

    sessions = sessionmaker(engine, class_=FaultSession)
    application = LiveCandleApplicationService(
        environment_identity=lambda: "live", db_session_factory=sessions
    )
    reader = BootstrapHydrationReader(
        db_session_factory=sessions,
        seed_store=seeds,
        application=application,
        decision_owner=owner,
        environment="live",
        identity_resolver=lambda s: strategy_decision_composition("deployment", s),
        max_seed_candles=2,
        max_recorded_candles=4,
    )
    published, applications = [], []

    def publish(replacement):
        assert replacement is not registry.get("s")
        published.append(replacement)
        registry.register(replacement)

    replay = PendingMarketReplayService(
        db_session_factory=sessions,
        live_candle_application=application,
        strategy_hydration=hydration,
        list_active_strategies=registry.list_active,
        publish_replacement=publish,
        bootstrap_hydration_reader=reader,
    )

    def apply_new(current):
        applications.append(registry.get("s"))
        scope = owner.begin_candle(current)
        processor.on_candle(current, emit_signals=False, decision_scope=scope)
        return scope.build()

    with pytest.raises(DBAPIError) as caught:
        application.apply(
            candle, apply_new=apply_new, rebuild_applied=replay.rebuild_applied
        )
    assert caught.value is failure and not armed[0]
    assert commit_proof == [
        "rolled_back" if fault_phase == "before_commit" else "committed"
    ]
    with Session(engine) as db:
        assert terminal_counts(db, start) == (
            (0, 0, 0) if fault_phase == "before_commit" else (1, 1, 1)
        )
    dirty_state = original.state()
    replay.replay(candle, apply_new=apply_new)
    recovered = registry.get("s")
    assert isinstance(recovered, Consumer) and recovered is not original
    assert len(published) == 1
    assert applications == (
        [original, recovered] if fault_phase == "before_commit" else [original]
    )
    # Literal fixture oracle, independent of Reader/plan outputs.
    profile_id = "5766b5d445bededce8aae025da6629f16784fd3c8463672452c7e0fb70dfd3a4"
    expected_trace = [
        [
            86400000,
            86460000,
            "MODELED",
            "FRESH",
            "1",
            profile_id,
            "45893ea38f8dc48bffd69b0082da521c9aafba30660a5d49af2614eab86a557b",
        ],
        [
            86460000,
            86520000,
            "MODELED",
            "MISSING",
            "0",
            None,
            "290cb4ec74cc6ef50d844047068820f59155e18f8e6414ba318dcaa307cd6bf5",
        ],
        [
            86520000,
            86580000,
            "LIVE_OBSERVED",
            "FRESH",
            "3",
            profile_id,
            "d7c98ec544318074ace807e4e8aa5af60edb357a8cd4c3007ca5dfbe150f374e",
        ],
        [
            86640000,
            86700000,
            "LIVE_OBSERVED",
            "INVALID",
            "0",
            None,
            "c89d95845b784b278a9b58efd23b1f1c146a293e1e65c616f0015e2c7e551c6b",
        ],
        [
            86700000,
            86760000,
            "LIVE_OBSERVED",
            "INVALID",
            "0",
            None,
            pinned.context.digest,
        ],
    ]
    expected = {"accumulator": "145993", "trace": expected_trace}
    assert recovered.state() == expected and digest(recovered.state()) == digest(
        expected
    )
    assert original.state() == dirty_state  # Discarded instance was never replayed.
    replay.replay(candle, apply_new=apply_new)
    repeated = registry.get("s")
    assert isinstance(repeated, Consumer) and repeated is not recovered
    assert repeated.state() == expected and len(published) == 2
    assert len(applications) == (2 if fault_phase == "before_commit" else 1)
    with Session(engine) as db:
        assert terminal_counts(db, start) == (1, 1, 1)
    confirmed = inputs.get(key)
    assert confirmed is not None
    assert (confirmed.value.canonical_bytes, confirmed.recorded_at) == expected_input
    assert not cache.mock_calls and not execution.mock_calls
    forbidden_signals.assert_not_called()
