"""Pinned FRESH revocation suppresses signals, not callbacks or recorded hydration."""

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from src.core.bootstrap_hydration_reader import BootstrapHydrationReader
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.composite_types import CompositeProfilePoc
from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.market_data.profiles.invalidation import (
    ProfileInvalidationStore,
    ConfirmedInvalidationRequest,
)
from src.core.market_data.profiles.read_types import DailyProfileRef
from src.core.market_data.profiles.repository import ProfileRepository
from src.core.market_data.profiles.types import ProfileBin
from src.core.pending_market_replay import PendingMarketReplayService
from src.core.signal_processor import SignalProcessor
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_registry import StrategyRegistry
from test_migrations import _publication_pg
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
from test_profile_pending_recovery_pg import terminal_counts
from test_signal_processor import make_candle

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


@pytest.mark.parametrize("mode", ["before_commit", "ack_lost", "not_revoked"])
def test_authoritative_revocation_survives_pending_recovery(
    profile_repository_pg, monkeypatch, mode
):
    engine = profile_repository_pg
    normal = sessionmaker(engine)
    original, initial_reader, seeds, inputs, _, _ = build(engine, True)
    monkeypatch.setattr(Consumer, "fresh_instance_for_replay", lambda _: Consumer())
    start = CUTOVER + 3 * MINUTE
    prefix = initial_reader.prepare(original, start)
    publication = _publication_pg()
    bins = (ProfileBin(0, Decimal(7), Decimal(70), 1),)
    publication = replace(
        publication,
        content=replace(publication.content, grid_id="btc_spot_usdt_10_v1", bins=bins),
        source_available_at=datetime(1970, 1, 2, tzinfo=timezone.utc),
    )
    published = ProfileRepository(normal).publish(publication)
    item = evidence(2).profiles[0]
    assert item.profile is not None
    reference = DailyProfileRef(
        published.snapshot_id, published.revision, published.content_sha256, 0, 86400000
    )
    profile = replace(
        item.profile,
        manifest=replace(item.profile.manifest, days=(reference,)),
        bins=bins,
        base_volume=Decimal(7),
        quote_volume=Decimal(70),
        aggregate_count=1,
        poc=CompositeProfilePoc(0, Decimal(0), Decimal(10)),
    )
    item = replace(item, profile=profile, decision_time_ms=start + MINUTE)
    pinned = MarketDataDecisionInput(
        prefix.plan.seed.key.decision_key(start),
        original.requirements.profile_requirements,
        start + MINUTE,
        StrategyMarketDataContext(start + MINUTE, (item,)),
    )
    initial_pin = inputs.pin(pinned)
    before_input = (
        pinned.canonical_bytes,
        pinned.input_digest,
        initial_pin.recorded_at,
    )
    forbidden_pin = MagicMock(side_effect=AssertionError("must not repin"))
    monkeypatch.setattr(inputs, "pin", forbidden_pin)
    monkeypatch.setattr(inputs, "pin_confirmed", forbidden_pin)
    invalidations = ProfileInvalidationStore(normal)
    if mode != "not_revoked":
        invalidations.append_confirmed(
            ConfirmedInvalidationRequest(
                "revoked", published.snapshot_id, "BAD_DATA", None, "test"
            )
        )
    checker = MagicMock(wraps=invalidations.any_revoked)
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
        revoked_checker=checker,
    )
    seen = []
    callback = Consumer.on_candle

    def consume(self, candle, context=None):
        if candle.timestamp == start:
            assert context is not None and context.market_data is not None
            assert context.market_data.canonical_bytes == pinned.context.canonical_bytes
            seen.append(context.market_data.digest)
        return callback(self, candle, context)

    monkeypatch.setattr(Consumer, "on_candle", consume)
    registry, execution, handler = StrategyRegistry(), MagicMock(), MagicMock()
    execution.execute_signal.return_value = "mock-order"
    handler.side_effect = lambda signal, candle: bool(
        execution.execute_signal(signal, candle)
    )
    registry.register(original)
    processor = SignalProcessor(
        registry,
        execution,
        signal_handler=handler,
        strategy_context_loader=lambda s, c, _: replace(
            context(),
            strategy_id=s.strategy_id,
            product_id=c.product_id,
            timestamp=c.timestamp,
        ),
    )
    account = MagicMock()
    account.get_position.return_value = None
    hydration = StrategyHydrationService(
        signal_processor=processor, account_service=account
    )
    hydration.hydrate_candles(
        original, prefix.candles, decision_scope_loader=prefix.decision_scope_loader
    )
    assert original.accumulator == Decimal(14593) and not seen
    assert not handler.mock_calls and not execution.mock_calls
    armed, failure = (
        [mode != "not_revoked"],
        DBAPIError(None, None, RuntimeError("terminal failure")),
    )

    class FaultSession(Session):
        def commit(self):
            if armed[0]:
                armed[0] = False
                assert terminal_counts(self, start) == (1, 1, 1) and len(seen) == 1
                if mode == "ack_lost":
                    super().commit()
                    with normal() as db:
                        assert terminal_counts(db, start) == (1, 1, 1)
                raise failure
            return super().commit()

    application = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=sessionmaker(engine, class_=FaultSession),
    )
    reader = BootstrapHydrationReader(
        db_session_factory=normal,
        seed_store=seeds,
        application=application,
        decision_owner=owner,
        environment="live",
        identity_resolver=lambda s: strategy_decision_composition("deployment", s),
        max_seed_candles=2,
        max_recorded_candles=4,
    )
    replacements, applications = [], []

    def publish(replacement):
        assert replacement is not registry.get("s")
        replacements.append(replacement)
        registry.register(replacement)

    replay = PendingMarketReplayService(
        db_session_factory=normal,
        live_candle_application=application,
        strategy_hydration=hydration,
        list_active_strategies=registry.list_active,
        publish_replacement=publish,
        bootstrap_hydration_reader=reader,
    )
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

    def apply_new(current):
        applications.append(registry.get("s"))
        scope = owner.begin_candle(current)
        processor.on_candle(current, decision_scope=scope)
        return scope.build()

    if mode == "not_revoked":
        application.apply(
            candle, apply_new=apply_new, rebuild_applied=replay.rebuild_applied
        )
    else:
        with pytest.raises(DBAPIError) as caught:
            application.apply(
                candle, apply_new=apply_new, rebuild_applied=replay.rebuild_applied
            )
        assert caught.value is failure
    with normal() as db:
        assert terminal_counts(db, start) == (
            (0, 0, 0) if mode == "before_commit" else (1, 1, 1)
        )
    dirty = original.state()
    replay.replay(candle, apply_new=apply_new)
    recovered = registry.get("s")
    assert isinstance(recovered, Consumer) and recovered is not original
    assert recovered.accumulator == Decimal(
        145998
    )  # 14593 * 10 + close60 + volume7 + FRESH1.
    assert recovered.trace[-1] == [
        86700000,
        86760000,
        "LIVE_OBSERVED",
        "FRESH",
        "7",
        profile.composite_id,
        pinned.context.digest,
    ]
    assert len(recovered.trace) == 5 and original.state() == dirty
    assert (
        len(applications) == checker.call_count == (2 if mode == "before_commit" else 1)
    )
    assert (
        handler.call_count
        == execution.execute_signal.call_count
        == (1 if mode == "not_revoked" else 0)
    )
    before_repeat = digest(recovered.state())
    replay.replay(candle, apply_new=apply_new)
    repeated = registry.get("s")
    assert isinstance(repeated, Consumer) and repeated is not recovered
    assert digest(repeated.state()) == before_repeat and len(replacements) == 2
    assert (
        len(applications) == checker.call_count == (2 if mode == "before_commit" else 1)
    )
    assert (
        handler.call_count
        == execution.execute_signal.call_count
        == (1 if mode == "not_revoked" else 0)
    )
    assert seen == [pinned.context.digest] * 3
    for call in checker.call_args_list:
        assert call.args == ((published.snapshot_id,),)
    with normal() as db:
        assert terminal_counts(db, start) == (1, 1, 1)
        _, record = application.read_applied_candle(
            product_id=PRODUCT, timeframe="1m", bar_start_ms=start, db=db
        )
    outcome = record.batch.outcomes[0]
    assert outcome.disposition == "APPLIED" and outcome.signal_suppressed is (
        mode != "not_revoked"
    )
    assert outcome.suppression_reason == (
        None if mode == "not_revoked" else "SNAPSHOT_REVOKED"
    )
    assert (outcome.input_id, outcome.input_digest) == (
        pinned.input_id,
        pinned.input_digest,
    )
    confirmed = inputs.get(pinned.key)
    assert confirmed is not None
    assert (
        confirmed.value.canonical_bytes,
        confirmed.value.input_digest,
        confirmed.recorded_at,
    ) == before_input
    forbidden_pin.assert_not_called()
    assert not cache.mock_calls
