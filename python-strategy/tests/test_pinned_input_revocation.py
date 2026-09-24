from contextlib import nullcontext
from dataclasses import replace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from src.core.market_data.profiles.invalidation import (
    ProfileInvalidationStore,
    MAX_INVALIDATION_MEMBERSHIP_IDS,
)
from src.core.market_data.profiles.decision_context import (
    StrategyMarketDataContext,
    ProfileDecisionStatus,
)
from src.core.market_data.profiles.decision_input_store import DecisionInputRecord
from src.core.market_data.profiles.decision_owner import (
    MarketDataDecisionOwner,
    MarketDataDecisionOwnerError,
)
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from test_profile_decision_input import sample
from test_profile_decision_owner import Strategy, NOW, DAY, base_context
from test_signal_processor import DummyStrategy, make_candle, make_signal


def membership():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    session.execute.return_value.scalar_one.return_value = False
    sessions = MagicMock(side_effect=lambda: nullcontext(session))
    return ProfileInvalidationStore(sessions), session, sessions


def test_nonempty_membership_false_is_database_result():
    store, session, sessions = membership()
    assert store.any_revoked(("a" * 64,)) is False
    sessions.assert_called_once()
    session.begin.assert_called_once()
    assert session.execute.call_count == 3
    session.execute.return_value.scalar_one.assert_called_once()
    assert "SELECT EXISTS" in str(session.execute.call_args.args[0])


def test_membership_single_read_canonical_ids_and_full_bound():
    store, session, sessions = membership()
    assert store.any_revoked(()) is False
    sessions.assert_not_called()
    session.execute.return_value.scalar_one.return_value = True
    assert store.any_revoked(("b" * 64, "a" * 64, "b" * 64)) is True
    statements = [call.args[0] for call in session.execute.call_args_list]
    assert (
        str(statements[0])
        == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"
    )
    assert "set_config('TimeZone', 'UTC', true)" in str(statements[1])
    compiled = statements[2].compile(dialect=dialect())
    sql = str(compiled)
    assert "SELECT EXISTS (SELECT market_data_invalidation.snapshot_id" in sql
    assert "WHERE market_data_invalidation.snapshot_id IN" in sql
    assert compiled.params == {"snapshot_id_1": ["a" * 64, "b" * 64]}
    assert len(statements) == 3 and "FOR UPDATE" not in sql and "LIMIT" not in sql
    assert "recorded_at" not in sql and "JOIN" not in sql
    assert MAX_INVALIDATION_MEMBERSHIP_IDS == 2880
    assert store.any_revoked(tuple(f"{i:064x}" for i in range(2880))) is True


@pytest.mark.parametrize(
    "bad",
    [
        [],
        (True,),
        ("A" * 64,),
        ("x",),
        (type("S", (str,), {})("a" * 64),),
        type("T", (tuple,), {})(()),
        tuple(f"{i:064x}" for i in range(2881)),
    ],
)
def test_membership_invalid_before_session(bad):
    store, _, sessions = membership()
    with pytest.raises(ValueError):
        store.any_revoked(bad)
    sessions.assert_not_called()


@pytest.mark.parametrize("bad", [None, 1, "true"])
def test_membership_requires_boolean_database_scalar(bad):
    store, session, _ = membership()
    session.execute.return_value.scalar_one.return_value = bad
    with pytest.raises(ValueError, match="invalid invalidation membership result"):
        store.any_revoked(("a" * 64,))


def test_membership_database_failure_propagates_without_retry():
    store, session, sessions = membership()
    failure = RuntimeError("database sentinel")
    session.execute.return_value.scalar_one.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        store.any_revoked(("a" * 64,))
    assert caught.value is failure and session.execute.call_count == 3
    sessions.assert_called_once()


def multi_profile():
    value = sample()
    original = value.context.profiles[0]
    assert original.profile is not None
    first = original.profile.manifest.days[0]
    second = replace(
        first, snapshot_id="b" * 64, window_start_ms=DAY, window_end_ms=2 * DAY
    )
    third = replace(second, snapshot_id="c" * 64, revision=2)
    items = []
    for refs, start in (((first, second), 0), ((third,), DAY)):
        profile = replace(
            original.profile, manifest=replace(original.profile.manifest, days=refs)
        )
        items.append(
            replace(
                original,
                profile=profile,
                request=replace(original.request, start_ms=start, end_ms=2 * DAY),
                decision_time_ms=2 * DAY + 3,
                available_at_ms=2 * DAY,
                validation_checked_at_ms=2 * DAY + 1,
                observed_at_ms=2 * DAY + 2,
            )
        )
    return replace(
        value,
        requirements=tuple(
            replace(value.requirements[0], window_days=n) for n in (2, 1)
        ),
        decision_time_ms=2 * DAY + 3,
        context=StrategyMarketDataContext(2 * DAY + 3, tuple(items)),
    )


def run(value, checker, *, existing=True, plain=False):
    class Target(Strategy):
        @property
        def requirements(self):
            return replace(
                super().requirements, profile_requirements=value.requirements
            )

        def on_candle(self, candle, context=None):
            assert context is not None and context.market_data is value.context
            self.candles_received.append(candle)
            return make_signal("s1")

    strategy = DummyStrategy("s1", result=make_signal()) if plain else Target("s1")
    store, cache = MagicMock(), MagicMock()
    store.get.side_effect = (
        lambda key: DecisionInputRecord(replace(value, key=key), NOW, True)
        if existing
        else None
    )
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="scope",
        identity_resolver=lambda s: strategy_decision_composition("scope", s),
        cache=cache,
        input_store=store,
        utc_ms=lambda: value.decision_time_ms,
        monotonic_ms=lambda: 1,
        revoked_checker=checker,
    )
    registry = StrategyRegistry()
    registry.register(strategy)
    handler = MagicMock()
    runner = SignalProcessor(
        registry,
        MagicMock(),
        signal_handler=handler,
        strategy_context_loader=lambda *_: base_context(strategy),
    )
    candle = make_candle()
    scope = owner.begin_candle(candle)
    return runner, candle, scope, strategy, handler, store, cache


@pytest.mark.parametrize("revoked", [None, "b" * 64, "c" * 64])
def test_all_pinned_days_and_auxiliary_profiles_checked_once(revoked):
    value = multi_profile()
    checker = MagicMock(side_effect=lambda ids: revoked in ids)
    runner, candle, scope, strategy, handler, store, cache = run(value, checker)
    assert strategy.product_id != value.context.profiles[0].request.product_id
    before = value.canonical_bytes
    runner.on_candle(candle, decision_scope=scope)
    checker.assert_called_once_with(("a" * 64, "b" * 64, "c" * 64))
    assert strategy.candles_received == [candle] and value.canonical_bytes == before
    assert handler.call_count == (1 if revoked is None else 0)
    batch = scope.build()
    assert batch is not None
    outcome = batch.outcomes[0]
    assert outcome.disposition == "APPLIED" and outcome.signal_suppressed is (
        revoked is not None
    )
    assert outcome.suppression_reason == ("SNAPSHOT_REVOKED" if revoked else None)
    store.pin_confirmed.assert_not_called()
    assert not cache.mock_calls


@pytest.mark.parametrize("result", [1, None, "SECRET", RuntimeError("SECRET")])
def test_checker_failure_prevents_callback_and_batch(result):
    checker = (
        MagicMock(side_effect=result)
        if isinstance(result, Exception)
        else MagicMock(return_value=result)
    )
    runner, candle, scope, strategy, handler, _, _ = run(sample(), checker)
    with pytest.raises(
        RuntimeError if isinstance(result, Exception) else MarketDataDecisionOwnerError
    ) as caught:
        runner.on_candle(candle, decision_scope=scope)
    if isinstance(result, Exception):
        assert caught.value is result
    assert not strategy.candles_received and scope.build() is None
    handler.assert_not_called()


@pytest.mark.parametrize("mode", ["no_refs", "no_checker", "plain", "new"])
def test_unconfigured_nonprofile_no_refs_and_new_path_not_checked(mode):
    checker = (
        None
        if mode == "no_checker"
        else MagicMock(side_effect=AssertionError("checker"))
    )
    value = (
        sample(status=ProfileDecisionStatus.MISSING) if mode == "no_refs" else sample()
    )
    runner, candle, scope, _, handler, store, cache = run(
        value, checker, existing=mode != "new", plain=mode == "plain"
    )
    if mode == "new":
        stop = RuntimeError("new input reaches cache, not checker")
        cache.live_requests.side_effect = stop
        with pytest.raises(RuntimeError) as caught:
            runner.on_candle(candle, decision_scope=scope)
        assert caught.value is stop
    else:
        runner.on_candle(candle, decision_scope=scope)
        handler.assert_called_once()
    if checker is not None:
        checker.assert_not_called()
    if mode == "plain":
        store.get.assert_not_called()
