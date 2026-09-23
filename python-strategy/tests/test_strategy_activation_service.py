from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import nullcontext
from decimal import Decimal
from unittest.mock import MagicMock, call
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy.orm import Session
from src.core import strategy_activation_service as consumption
from src.core.strategy_activation_intent import ProfileActivationCommand as Command
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestRecord as Record,
    ProfileActivationRequestStatus as RequestStatus,
    ProfileActivationRequestConflict as Conflict,
)
from src.core.strategy_state_manager import (
    StrategyStateManager,
    LockedStrategyState,
    StrategyStateTransitionResult,
)
from test_profile_activation_request import request
from src.core.market_data.profiles import bootstrap_hydration as bootstrap

from src.core.models import Candlestick, Signal, StrategyStatus
from src.core.portfolio_runtime import (
    PortfolioDefinition,
    PortfolioFactory,
    PortfolioSleeve,
)
from src.core.strategy_activation_service import (
    ContextCapabilityValidator,
    StrategyActivationService,
)
from src.core.strategy_context import StrategyContext
from src.core.strategy_state_manager import StaleStrategyStateVersion
from src.strategies.base import BaseStrategy, StrategyRequirements


def consume_setup(monkeypatch, status=StrategyStatus.READY, command=Command.START):
    value = replace(request(), command=command)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    session = MagicMock(spec=Session)
    session.is_active = True
    session.in_transaction.return_value = True
    session.get_bind.return_value.dialect.name = "postgresql"
    factory, redis = MagicMock(), MagicMock()
    manager = StrategyStateManager(factory, redis)
    calls = MagicMock()
    calls.state.return_value = LockedStrategyState(
        value.intent.key.strategy_id, status, 0
    )
    calls.request.return_value = Record(value, RequestStatus.PENDING, now)
    calls.terminal.return_value = Record(
        value, RequestStatus.CONSUMED, now, now, "ACTIVATION_COMMITTED"
    )
    calls.transition.return_value = StrategyStateTransitionResult(
        value.intent.key.strategy_id, status, StrategyStatus.ACTIVE, 1, now
    )
    monkeypatch.setattr(manager, "lock_state_in_transaction", calls.state)
    monkeypatch.setattr(manager, "transition_in_transaction", calls.transition)
    monkeypatch.setattr(consumption, "lock_profile_activation_request", calls.request)
    monkeypatch.setattr(
        consumption, "terminalize_profile_activation_request", calls.terminal
    )
    return value, now, session, manager, calls, factory, redis


@pytest.mark.parametrize(
    "status,command",
    [
        (StrategyStatus.READY, Command.START),
        (StrategyStatus.STOPPED, Command.RESUME),
        (StrategyStatus.ERROR, Command.FORCE_RECOVER),
    ],
)
def test_consume_order_arguments_and_transaction_local_result(
    monkeypatch, status, command
):
    value, now, session, manager, calls, factory, redis = consume_setup(
        monkeypatch, status, command
    )
    result = consumption.consume_profile_activation_request(
        session, manager, value, terminal_at=now
    )
    assert result.disposition is consumption.ProfileActivationConsumption.CONSUMED_NOW
    assert result.record is calls.terminal.return_value
    assert [call[0] for call in calls.mock_calls] == [
        "state",
        "request",
        "transition",
        "terminal",
    ]
    calls.state.assert_called_once_with(session, value.intent.key.strategy_id)
    calls.request.assert_called_once_with(session, value)
    calls.transition.assert_called_once_with(
        session,
        value.intent.key.strategy_id,
        StrategyStatus.ACTIVE,
        actor=value.actor,
        reason="ACTIVATION_COMMITTED",
        changed_at=now,
        force=command is Command.FORCE_RECOVER,
        expected_version=0,
    )
    calls.terminal.assert_called_once_with(
        session,
        value,
        status=RequestStatus.CONSUMED,
        terminal_at=now,
        terminal_reason="ACTIVATION_COMMITTED",
    )
    for name in ("begin", "commit", "rollback", "execute"):
        getattr(session, name).assert_not_called()
    assert not factory.mock_calls and not redis.mock_calls


@pytest.mark.parametrize(
    "case",
    ["stale", "illegal", "cancelled", "stale_terminal", "reason", "time", "early"],
)
def test_consume_conflicts_before_mutation(monkeypatch, case):
    value, now, session, manager, calls, _, _ = consume_setup(monkeypatch)
    error = Conflict
    if case == "stale":
        calls.state.return_value = replace(calls.state.return_value, version=1)
        error = StaleStrategyStateVersion
    elif case == "illegal":
        calls.state.return_value = replace(
            calls.state.return_value, status=StrategyStatus.ACTIVE
        )
    elif case == "early":
        now -= timedelta(milliseconds=1)
        error = consumption.ProfileActivationRequestValidationError
    else:
        status = {
            "cancelled": RequestStatus.CANCELLED,
            "stale_terminal": RequestStatus.STALE,
        }.get(case, RequestStatus.CONSUMED)
        calls.request.return_value = Record(
            value,
            status,
            now,
            now,
            "OTHER" if case == "reason" else "ACTIVATION_COMMITTED",
        )
        if case == "time":
            now += timedelta(milliseconds=1)
    with pytest.raises(error):
        consumption.consume_profile_activation_request(
            session, cast(Any, manager), cast(Any, value), terminal_at=cast(Any, now)
        )
    calls.transition.assert_not_called()
    calls.terminal.assert_not_called()


@pytest.mark.parametrize(
    "status,version",
    [
        (StrategyStatus.ACTIVE, 1),
        (StrategyStatus.STOPPED, 2),
        (StrategyStatus.ERROR, 3),
    ],
)
def test_consumed_reentry_never_restarts(monkeypatch, status, version):
    value, now, session, manager, calls, _, _ = consume_setup(monkeypatch, status)
    calls.request.return_value = calls.terminal.return_value
    calls.state.return_value = replace(calls.state.return_value, version=version)
    result = consumption.consume_profile_activation_request(
        session, manager, value, terminal_at=now
    )
    assert (
        result.disposition is consumption.ProfileActivationConsumption.ALREADY_CONSUMED
    )
    calls.transition.assert_not_called()
    calls.terminal.assert_not_called()


@pytest.mark.parametrize("phase", ["state", "request", "transition", "terminal"])
@pytest.mark.parametrize("error", [RuntimeError("failure"), BaseException("failure")])
def test_consume_exception_identity(monkeypatch, phase, error):
    value, now, session, manager, calls, factory, redis = consume_setup(monkeypatch)
    getattr(calls, phase).side_effect = error
    with pytest.raises(type(error)) as caught:
        consumption.consume_profile_activation_request(
            session, manager, value, terminal_at=now
        )
    assert caught.value is error
    assert not factory.mock_calls and not redis.mock_calls


@pytest.mark.parametrize(
    "bad",
    [
        "request",
        "manager",
        "clock",
        "naive",
        "precision",
        "inactive",
        "transaction",
        "dialect",
    ],
)
def test_consume_invalid_arguments_before_locks(monkeypatch, bad):
    value, now, session, manager, calls, _, _ = consume_setup(monkeypatch)
    if bad == "request":
        value = None
    elif bad == "manager":
        manager = None
    elif bad == "clock":
        now = None
    elif bad == "naive":
        now = now.replace(tzinfo=None)
    elif bad == "precision":
        now = now.replace(microsecond=1)
    elif bad == "inactive":
        session.is_active = False
    elif bad == "transaction":
        session.in_transaction.return_value = False
    else:
        session.get_bind.return_value.dialect.name = "sqlite"
    with pytest.raises(consumption.ProfileActivationRequestValidationError):
        consumption.consume_profile_activation_request(
            session, cast(Any, manager), cast(Any, value), terminal_at=cast(Any, now)
        )
    assert not calls.mock_calls


@pytest.mark.parametrize("environment", ["live", "backtest"])
def test_cutover_candle_multiple_matching_pending(environment):
    value, now = request(), datetime(2026, 1, 1, tzinfo=UTC)
    second = replace(
        value,
        intent=replace(
            value.intent, key=replace(value.intent.key, strategy_id="second")
        ),
    )
    other = replace(
        value,
        intent=replace(
            value.intent,
            key=replace(value.intent.key, timeframe="5m"),
            requirements=replace(value.intent.requirements, timeframe="5m"),
        ),
    )
    rows = tuple(
        Record(item, RequestStatus.PENDING, now) for item in (value, other, second)
    )
    store = MagicMock()
    store.list_pending.return_value = rows
    service, *_ = _build_service(
        state=None, environment=environment, profile_request_store=store
    )
    service.cutover_pending_request = MagicMock()
    candle = MagicMock(product_id=value.intent.key.product_id, timeframe="1m")
    assert service.cutover_pending_candle(candle) == (2 if environment == "live" else 0)
    assert service.cutover_pending_request.call_args_list == (
        [call(rows[0], candle), call(rows[2], candle)] if environment == "live" else []
    )
    if environment == "live":
        error = RuntimeError("cutover failure")
        service.cutover_pending_request.side_effect = error
        with pytest.raises(RuntimeError) as caught:
            service.cutover_pending_candle(candle)
        assert caught.value is error
    else:
        store.list_pending.assert_not_called()


class _Strategy(BaseStrategy):
    events: list[str] = []

    def __init__(self, strategy_id: str, product_id: str) -> None:
        self.events.append(f"construct:{strategy_id}")
        super().__init__(strategy_id, product_id)

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        del candle, context
        raise AssertionError("activation tests never execute strategy signals")


class _Portfolio(PortfolioFactory):
    def build(self, *, portfolio_id, product_id, config):
        del portfolio_id, product_id, config
        raise AssertionError("the test injects the validated definition builder")


class _ProfileStrategy(_Strategy):
    @property
    def requirements(self):
        return replace(request().intent.requirements, product_id=self.product_id)


@pytest.mark.parametrize(
    "phase",
    "success capabilities missing_owner missing_loader commit register postcommit factory_key seed_scope seed_config bound_scope bound_config".split(),
)
def test_single_pending_cutover_order_and_failures(monkeypatch, phase, engine_factory):
    value, now = request(), datetime(2026, 1, 1, tzinfo=UTC)
    value = replace(
        value,
        intent=replace(
            value.intent,
            requirements=replace(value.intent.requirements, lookback_window=0),
        ),
    )

    class CutoverStrategy(_ProfileStrategy):
        def replay_configuration(self):
            return {}

        @property
        def requirements(self):
            return value.intent.requirements

    record = Record(value, RequestStatus.PENDING, now)
    candle = Candlestick(
        product_id=value.intent.key.product_id,
        timeframe="1m",
        timestamp=60_000,
        open=Decimal(1),
        high=Decimal(1),
        low=Decimal(1),
        close=Decimal(1),
        volume=Decimal(1),
    )
    reader, manager, resolver, factory = [MagicMock() for _ in range(4)]
    unregister_locked, forbidden_lock = MagicMock(), MagicMock()
    resolver.return_value = consumption.MarketDataDecisionCompositionIdentity(
        "deployment", "v1", "a" * 64
    )
    service, _, db, legacy, hydration, runtime = _build_service(
        state=None,
        environment="live",
        profile_identity_resolver=resolver,
        bootstrap_reader=reader,
        state_manager=manager,
        artifact_resolver=lambda _: CutoverStrategy,
        profile_seed_factory=factory,
        cutover_unregister_locked=unregister_locked,
    )
    log, error = [], RuntimeError("fixed cutover fault")

    def action(name):
        def run(*args, **kwargs):
            log.append(name)
            if phase == name:
                raise error

        return run

    def seed(instance, boundary, *, factory):
        action("seed")()
        assert boundary == candle.timestamp
        factory(
            replace(value.intent.key, execution_scope_id="other")
            if phase == "factory_key"
            else value.intent.key
        )
        return consumption.BootstrapSeedRecord(make_seed("seed"), now, True)

    def capabilities(strategies, *, profile_warmup_ready=False):
        assert profile_warmup_ready is True and len(strategies) == 1
        action("capabilities")()

    service._assert_context_capabilities = capabilities
    if phase.startswith("missing_"):
        engine = engine_factory()
        engine.runtime_environment = MagicMock(identity="live")
        engine._strategy_context_loader_enabled = phase != "missing_loader"
        engine._market_data_decision_owner = (
            None if phase == "missing_owner" else MagicMock()
        )
        service._assert_context_capabilities = (
            engine._assert_strategy_context_capabilities
        )

    def make_seed(stage):
        key = value.intent.key
        if phase.startswith(stage + "_"):
            key = (
                replace(key, execution_scope_id="other")
                if phase.endswith("scope")
                else replace(key, config_hash="b" * 64)
            )
        return consumption.BootstrapSeed(
            key,
            value.intent.requirements.profile_requirements,
            candle.timestamp,
            0,
            "policy",
            "a" * 64,
            "b" * 64,
            (),
            max_seed_candles=1,
        )

    def prepare(instance, *_):
        log.append("prepare")
        seed = make_seed("bound")
        plan = bootstrap.BootstrapHydrationPlan(
            seed, (), None, 1, 1, seed.key, seed.requirements, 0
        )
        return bootstrap.BoundBootstrapHydration(
            plan,
            instance,
            replace(
                resolver.return_value,
                execution_scope_id=seed.key.execution_scope_id,
                config_hash=seed.key.config_hash,
            ),
            (),
        )

    reader.prepare_initial_seed_under_admission.side_effect = seed
    reader.prepare.side_effect = prepare
    hydration.hydrate_candles.side_effect = action("hydrate")

    def consume(session, state_manager, expected, *, terminal_at):
        action("consume")()
        assert session is db and state_manager is manager and expected is value
        return consumption.ProfileActivationConsumptionResult(
            consumption.ProfileActivationConsumption.CONSUMED_NOW,
            Record(
                value, RequestStatus.CONSUMED, now, terminal_at, "ACTIVATION_COMMITTED"
            ),
            StrategyStateTransitionResult(
                "strategy", StrategyStatus.READY, StrategyStatus.ACTIVE, 1, terminal_at
            ),
        )

    monkeypatch.setattr(consumption, "consume_profile_activation_request", consume)
    db.begin.return_value.__exit__.side_effect = action("commit")
    runtime.register_strategy.side_effect = action("register")
    manager.after_committed_transition.side_effect = action("postcommit")
    cleanup_fence = nullcontext()
    if phase in ("register", "postcommit"):
        engine = engine_factory()
        cleanup_fence = engine._market_processing_lock
        forbidden_lock.__enter__.side_effect = AssertionError("market reacquisition")
        engine._runtime_artifacts._market_processing_lock = forbidden_lock
        unregister_locked.side_effect = (
            engine._strategy_activation._cutover_unregister_locked
        )
    if phase == "success":
        service.cutover_pending_request(record, candle)
        assert log == (
            "capabilities seed prepare hydrate consume commit register postcommit".split()
        )
        instance = runtime.register_strategy.call_args.args[0]
        factory.assert_called_once_with(instance, value.intent.key, candle.timestamp)
        reader.prepare.assert_called_once_with(
            instance, candle.timestamp, "BEFORE_PENDING"
        )
        assert hydration.hydrate_candles.call_args.args == (instance, ())
    else:
        with pytest.raises((RuntimeError, ValueError)) as caught:
            with cleanup_fence:
                service.cutover_pending_request(record, candle)
        if phase in ("capabilities", "commit", "register", "postcommit"):
            assert caught.value is error
        if phase in ("register", "postcommit"):
            unregister_locked.assert_called_once_with("strategy")
            runtime.unregister.assert_not_called()
            legacy.transition_to_error.assert_called_once()
            forbidden_lock.__enter__.assert_not_called()
        else:
            runtime.register_strategy.assert_not_called()
            manager.after_committed_transition.assert_not_called()
        if phase.startswith(("seed_", "bound_", "factory_")):
            hydration.hydrate_candles.assert_not_called()
            assert "consume" not in log
        if phase == "factory_key":
            factory.assert_not_called()
        if phase == "capabilities" or phase.startswith("missing_"):
            reader.prepare_initial_seed_under_admission.assert_not_called()
            hydration.hydrate_candles.assert_not_called()
            assert "consume" not in log


@pytest.mark.parametrize("capability", ["configured", "store", "resolver", "nonlive"])
def test_persistent_pending_channels_after_fresh_service(capability):
    store, resolver = MagicMock(), MagicMock()
    value, now = request(), datetime(2026, 1, 1, tzinfo=UTC)
    other = replace(
        value,
        intent=replace(
            value.intent,
            key=replace(value.intent.key, timeframe="5m"),
            requirements=replace(value.intent.requirements, timeframe="5m"),
        ),
    )
    store.list_pending.return_value = tuple(
        Record(item, RequestStatus.PENDING, now) for item in (other, value, other)
    )
    for _ in range(2):
        service, *_ = _build_service(
            state=None,
            environment="simulated" if capability == "nonlive" else "live",
            profile_request_store=None if capability == "store" else store,
            profile_identity_resolver=None if capability == "resolver" else resolver,
        )
        assert service.persistent_pending_channels() == (
            tuple(
                sorted(
                    {
                        consumption.to_stream_key(
                            value.intent.key.product_id, timeframe
                        )
                        for timeframe in ("1m", "5m")
                    }
                )
            )
            if capability == "configured"
            else ()
        )
    assert store.list_pending.call_count == (2 if capability == "configured" else 0)
    resolver.assert_not_called()


def test_pending_channel_query_errors_propagate():
    error, store = RuntimeError("failure"), MagicMock()
    store.list_pending.side_effect = error
    service, *_ = _build_service(
        state=None,
        environment="live",
        profile_request_store=store,
        profile_identity_resolver=MagicMock(),
    )
    with pytest.raises(RuntimeError) as caught:
        service.persistent_pending_channels()
    assert caught.value is error


@pytest.mark.parametrize(
    "command,status",
    [
        (Command.START, StrategyStatus.READY),
        (Command.RESUME, StrategyStatus.STOPPED),
        (Command.FORCE_RECOVER, StrategyStatus.ERROR),
    ],
)
@pytest.mark.parametrize(
    "admission",
    list(consumption.ProfileActivationAdmissionStatus)
    + [
        RequestStatus.CONSUMED,
        RequestStatus.CANCELLED,
        RequestStatus.STALE,
        "missing_store",
        "missing_resolver",
        "missing_key",
        "illegal",
        "identity_error",
        "admission_error",
    ],
)
def test_live_profile_durable_waiting_has_no_activation_side_effects(
    command, status, admission
):
    store, resolver = MagicMock(), MagicMock()
    resolver.return_value = consumption.MarketDataDecisionCompositionIdentity(
        "deployment", "v1", "a" * 64
    )

    def admit(value, *, current):
        assert current is value.intent
        assert value.command is command and value.idempotency_key == "key"
        assert value.actor == "operator" and current.expected_state_version == 7
        assert current.key.execution_scope_id == "deployment"
        if (
            type(admission) is consumption.ProfileActivationAdmissionStatus
            and admission is not consumption.ProfileActivationAdmissionStatus.CONFIRMED
        ):
            return consumption.ProfileActivationAdmissionResult(admission)
        terminal = (
            admission if type(admission) is RequestStatus else RequestStatus.PENDING
        )
        now = datetime(2026, 1, 1, tzinfo=UTC)
        record = Record(
            value,
            terminal,
            now,
            None if terminal is RequestStatus.PENDING else now,
            None if terminal is RequestStatus.PENDING else "DONE",
        )
        return consumption.ProfileActivationAdmissionResult(
            consumption.ProfileActivationAdmissionStatus.CONFIRMED, record
        )

    store.admit_confirmed.side_effect = admit
    if admission == "identity_error":
        resolver.side_effect = RuntimeError("SECRET")
    if admission == "admission_error":
        store.admit_confirmed.side_effect = RuntimeError("SECRET")
    if admission == "illegal":
        status = (
            StrategyStatus.WARNING
            if command is not Command.START
            else StrategyStatus.STOPPED
        )
    service, events, db, state_manager, hydration, runtime = _build_service(
        state=_State([], status=status),
        environment="live",
        profile_request_store=None if admission == "missing_store" else store,
        profile_identity_resolver=None if admission == "missing_resolver" else resolver,
    )
    result = service.activate_locked(
        "strategy",
        artifact_cls=_ProfileStrategy,
        actor="operator",
        reason=None,
        force=command is not Command.START,
        expected_version=None,
        resolve_product_id=lambda _: "BINANCE:BTCUSDT-PERP",
        assert_live_readiness=lambda _: None,
        build_portfolio_definition=MagicMock(),
        activation_command=command.value,
        idempotency_key=None if admission == "missing_key" else "key",
    )
    waiting = admission in (
        consumption.ProfileActivationAdmissionStatus.CONFIRMED,
        RequestStatus.CONSUMED,
    )
    assert result is (
        consumption.StrategyStartDisposition.WAITING_FOR_CUTOVER if waiting else False
    )
    assert (
        not hydration.mock_calls
        and not runtime.mock_calls
        and not state_manager.mock_calls
    )
    db.commit.assert_not_called()
    assert store.admit_confirmed.call_count == (
        1 if admission == "admission_error" or not isinstance(admission, str) else 0
    )
    if admission in ("identity_error", "admission_error"):
        cast(Any, service._logger).error.assert_called_once_with(
            "profile_activation_admission_failed phase=%s",
            "identity" if admission == "identity_error" else "admission",
        )
        assert "SECRET" not in str(cast(Any, service._logger).mock_calls)
    if admission == "illegal":
        resolver.assert_not_called()


@pytest.mark.parametrize("phase", ["identity", "admission"])
def test_profile_admission_baseexception_propagates(phase):
    store, resolver, error = MagicMock(), MagicMock(), BaseException("SECRET")
    resolver.return_value = consumption.MarketDataDecisionCompositionIdentity(
        "deployment", "v1", "a" * 64
    )
    (resolver if phase == "identity" else store.admit_confirmed).side_effect = error
    service, *_ = _build_service(
        state=None, profile_request_store=store, profile_identity_resolver=resolver
    )
    with pytest.raises(BaseException) as caught:
        service._admit_profile(
            _ProfileStrategy("strategy", "BINANCE:BTCUSDT-PERP"),
            0,
            StrategyStatus.READY,
            "operator",
            "START",
            "key",
        )
    assert caught.value is error


class _State:
    events: list[str]
    strategy_id: str
    status: StrategyStatus
    version: int
    config_json: str
    performance_json: str
    uptime_start: object

    def __init__(self, events: list[str], *, status=StrategyStatus.READY) -> None:
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "strategy_id", "strategy")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "version", 7)
        object.__setattr__(
            self,
            "config_json",
            json.dumps({"product_id": "BINANCE:BTCUSDT-PERP"}),
        )
        object.__setattr__(self, "performance_json", "unchanged")
        object.__setattr__(self, "uptime_start", "unchanged")

    def __setattr__(self, name, value):
        if name in {"performance_json", "uptime_start"}:
            self.events.append(f"set:{name}")
        object.__setattr__(self, name, value)


def _build_service(
    *,
    state: object | None,
    environment: str = "simulated",
    assert_context_capabilities: ContextCapabilityValidator | None = None,
    profile_request_store=None,
    profile_identity_resolver=None,
    **cutover_capabilities,
):
    events: list[str] = []
    if isinstance(state, _State):
        object.__setattr__(state, "events", events)
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = state
    db.commit.side_effect = lambda: events.append("commit")
    state_manager = MagicMock()
    hydration = MagicMock()
    runtime_artifacts = MagicMock()
    service = StrategyActivationService(
        db_session_factory=lambda: nullcontext(db),
        transition_to_running=state_manager.transition_to_running,
        transition_to_error=state_manager.transition_to_error,
        hydration=hydration,
        register_strategy=runtime_artifacts.register_strategy,
        register_portfolio=runtime_artifacts.register_portfolio,
        unregister_runtime_artifact=runtime_artifacts.unregister,
        environment_identity=lambda: environment,
        assert_context_capabilities=(
            assert_context_capabilities
            or (lambda strategies, *, profile_warmup_ready=False: None)
        ),
        event_logger=MagicMock(),
        profile_request_store=profile_request_store,
        profile_identity_resolver=profile_identity_resolver,
        **cutover_capabilities,
    )
    return service, events, db, state_manager, hydration, runtime_artifacts


def _activate(
    service: StrategyActivationService,
    *,
    artifact_cls: type[BaseStrategy] | type[PortfolioFactory] | None = _Strategy,
    actor: str = "operator",
    reason: str | None = "requested",
    force: bool = False,
    expected_version: int | None = None,
    resolve_product_id: Callable[[dict], str] = lambda config: config["product_id"],
    assert_live_readiness: Callable[
        [type[BaseStrategy] | type[PortfolioFactory]], None
    ] = lambda _artifact_cls: None,
    build_portfolio_definition: Callable[..., PortfolioDefinition] = MagicMock(),
) -> bool | consumption.StrategyStartDisposition:
    return service.activate_locked(
        "strategy",
        artifact_cls=artifact_cls,
        actor=actor,
        reason=reason,
        force=force,
        expected_version=expected_version,
        resolve_product_id=resolve_product_id,
        assert_live_readiness=assert_live_readiness,
        build_portfolio_definition=build_portfolio_definition,
    )


def test_unloaded_artifact_returns_before_state_or_runtime_work() -> None:
    service, _events, db, state_manager, hydration, runtime_artifacts = _build_service(
        state=None
    )

    assert not _activate(service, artifact_cls=None)

    db.query.assert_not_called()
    state_manager.transition_to_running.assert_not_called()
    state_manager.transition_to_error.assert_not_called()
    hydration.assert_not_called()
    runtime_artifacts.register_strategy.assert_not_called()
    runtime_artifacts.register_portfolio.assert_not_called()
    runtime_artifacts.unregister.assert_not_called()


def test_stale_expected_version_precedes_runtime_mutation() -> None:
    state = _State([])
    service, _events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state
    )

    with pytest.raises(
        StaleStrategyStateVersion,
        match="strategy expected version 6, found 7",
    ):
        _activate(service, expected_version=6)

    state_manager.transition_to_running.assert_not_called()
    state_manager.transition_to_error.assert_not_called()
    hydration.assert_not_called()
    runtime_artifacts.register_strategy.assert_not_called()
    runtime_artifacts.register_portfolio.assert_not_called()
    runtime_artifacts.unregister.assert_not_called()


@pytest.mark.parametrize(
    ("status", "force", "expected"),
    [
        (None, False, False),
        (StrategyStatus.ERROR, False, False),
        (StrategyStatus.ERROR, True, True),
        (StrategyStatus.READY, False, True),
        (StrategyStatus.WARNING, False, True),
        (StrategyStatus.STOPPED, False, True),
        (StrategyStatus.DISCOVERED, False, True),
    ],
)
def test_state_and_force_matrix(status, force, expected) -> None:
    state = None if status is None else _State([], status=status)
    service, _events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state
    )

    assert _activate(service, force=force) is expected

    if expected:
        hydration.warm_up.assert_called_once()
        runtime_artifacts.register_strategy.assert_called_once()
        state_manager.transition_to_running.assert_called_once()
    else:
        hydration.assert_not_called()
        runtime_artifacts.register_strategy.assert_not_called()
        runtime_artifacts.register_portfolio.assert_not_called()
        runtime_artifacts.unregister.assert_not_called()
        state_manager.transition_to_running.assert_not_called()
        state_manager.transition_to_error.assert_not_called()


@pytest.mark.parametrize("environment", ["simulated", "live"])
def test_standalone_order_and_running_call_shape(environment: str) -> None:
    state = _State([])
    service, events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state, environment=environment
    )
    _Strategy.events = events
    hydration.warm_up.side_effect = lambda *_args: events.append("warm")
    hydration.fresh_instance_for_replay.side_effect = lambda instance: (
        events.append("refresh") or MagicMock()
    )
    runtime_artifacts.register_strategy.side_effect = lambda _instance: events.append(
        "register"
    )
    state_manager.transition_to_running.side_effect = (
        lambda *_args, **_kwargs: events.append("running")
    )

    assert _activate(service, expected_version=7)

    expected = ["construct:strategy", "warm"]
    if environment == "live":
        expected.append("refresh")
    expected.extend(["register", "set:uptime_start", "commit", "running"])
    assert events == expected
    if environment == "live":
        assert (
            runtime_artifacts.register_strategy.call_args.args[0]
            is hydration.warm_up.call_args.args[1]
        )
    state_manager.transition_to_running.assert_called_once_with(
        "strategy",
        actor="operator",
        force=False,
        reason="requested",
        expected_version=7,
    )


def test_absent_expected_version_is_omitted_from_running_transition() -> None:
    service, _events, _db, state_manager, _hydration, _runtime_artifacts = (
        _build_service(state=_State([]))
    )

    assert _activate(service)

    state_manager.transition_to_running.assert_called_once_with(
        "strategy",
        actor="operator",
        force=False,
        reason="requested",
    )


def test_portfolio_publishes_only_after_every_sleeve_is_ready() -> None:
    state = _State([])
    service, events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state, environment="live"
    )
    _Strategy.events = events
    sleeves = tuple(
        PortfolioSleeve(_Strategy(f"strategy.sleeve_{index}", "PRODUCT"))
        for index in range(2)
    )
    events.clear()
    definition = PortfolioDefinition(
        portfolio_id="strategy",
        product_id="PRODUCT",
        sleeves=sleeves,
        max_gross_quantity=Decimal("2"),
    )
    hydration.warm_up.side_effect = lambda _db, strategy: events.append(
        f"warm:{strategy.strategy_id}"
    )
    hydration.fresh_instance_for_replay.side_effect = lambda strategy: (
        events.append(f"refresh:{strategy.strategy_id}") or strategy
    )
    runtime_artifacts.register_portfolio.side_effect = lambda _definition: (
        events.append("register_portfolio")
    )
    state_manager.transition_to_running.side_effect = (
        lambda *_args, **_kwargs: events.append("running")
    )

    assert _activate(
        service,
        artifact_cls=_Portfolio,
        resolve_product_id=lambda _config: "PRODUCT",
        build_portfolio_definition=lambda *_args, **_kwargs: definition,
    )

    assert events == [
        "warm:strategy.sleeve_0",
        "refresh:strategy.sleeve_0",
        "warm:strategy.sleeve_1",
        "refresh:strategy.sleeve_1",
        "register_portfolio",
        "set:uptime_start",
        "commit",
        "running",
    ]


def test_standalone_capability_rejection_precedes_warm_up_and_registration() -> None:
    state = _State([])
    failure = RuntimeError("strategy_context_capability_missing: ENTRY_RISK")
    validator = MagicMock(side_effect=failure)
    service, _events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state,
        environment="live",
        assert_context_capabilities=validator,
    )

    assert not _activate(service)

    validated = validator.call_args.args[0]
    assert len(validated) == 1
    assert validated[0].strategy_id == "strategy"
    hydration.assert_not_called()
    runtime_artifacts.register_strategy.assert_not_called()
    state_manager.transition_to_running.assert_not_called()
    state_manager.transition_to_error.assert_called_once_with(
        "strategy",
        str(failure),
        actor="system",
        expected_version=None,
    )


def test_mixed_portfolio_capabilities_are_validated_atomically() -> None:
    state = _State([])
    failure = RuntimeError("strategy_context_capability_missing: ENTRY_RISK")
    validator = MagicMock(side_effect=failure)
    service, _events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state,
        environment="live",
        assert_context_capabilities=validator,
    )
    definition = PortfolioDefinition(
        portfolio_id="strategy",
        product_id="PRODUCT",
        sleeves=tuple(
            PortfolioSleeve(_Strategy(f"strategy.sleeve_{index}", "PRODUCT"))
            for index in range(2)
        ),
        max_gross_quantity=Decimal("2"),
    )

    assert not _activate(
        service,
        artifact_cls=_Portfolio,
        resolve_product_id=lambda _config: "PRODUCT",
        build_portfolio_definition=lambda *_args, **_kwargs: definition,
    )

    assert validator.call_args.args[0] == tuple(
        sleeve.strategy for sleeve in definition.sleeves
    )
    hydration.assert_not_called()
    runtime_artifacts.register_portfolio.assert_not_called()
    state_manager.transition_to_running.assert_not_called()


def test_second_sleeve_failure_never_publishes_partial_portfolio() -> None:
    state = _State([])
    service, events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state
    )
    _Strategy.events = []
    definition = PortfolioDefinition(
        portfolio_id="strategy",
        product_id="PRODUCT",
        sleeves=tuple(
            PortfolioSleeve(_Strategy(f"strategy.sleeve_{index}", "PRODUCT"))
            for index in range(2)
        ),
        max_gross_quantity=Decimal("2"),
    )
    failure = RuntimeError("second sleeve failed")
    hydration.warm_up.side_effect = [None, failure]
    runtime_artifacts.unregister.side_effect = lambda _strategy_id: events.append(
        "unregister"
    )
    state_manager.transition_to_error.side_effect = (
        lambda *_args, **_kwargs: events.append("transition_error")
    )

    assert not _activate(
        service,
        artifact_cls=_Portfolio,
        build_portfolio_definition=lambda *_args, **_kwargs: definition,
        resolve_product_id=lambda _config: "PRODUCT",
    )

    runtime_artifacts.register_portfolio.assert_not_called()
    runtime_artifacts.unregister.assert_called_once_with("strategy")
    assert json.loads(state.performance_json) == {"error": "second sleeve failed"}
    assert events == [
        "unregister",
        "set:performance_json",
        "commit",
        "transition_error",
    ]


@pytest.mark.parametrize("failure_owner", ["product", "readiness", "register"])
def test_activation_body_failure_preserves_cleanup_error_order(failure_owner) -> None:
    state = _State([])
    service, events, _db, state_manager, hydration, runtime_artifacts = _build_service(
        state=state
    )
    failure = RuntimeError(f"{failure_owner} failed")

    def resolve(config):
        return config["product_id"]

    def readiness(_artifact_cls):
        return None

    if failure_owner == "product":
        resolve = MagicMock(side_effect=failure)
    elif failure_owner == "readiness":
        readiness = MagicMock(side_effect=failure)
    else:
        runtime_artifacts.register_strategy.side_effect = failure
    runtime_artifacts.unregister.side_effect = lambda _strategy_id: events.append(
        "unregister"
    )
    state_manager.transition_to_error.side_effect = (
        lambda *_args, **_kwargs: events.append("transition_error")
    )

    assert not _activate(
        service,
        expected_version=7,
        resolve_product_id=resolve,
        assert_live_readiness=readiness,
    )

    assert events == [
        "unregister",
        "set:performance_json",
        "commit",
        "transition_error",
    ]
    state_manager.transition_to_error.assert_called_once_with(
        "strategy",
        str(failure),
        actor="system",
        expected_version=7,
    )
    if failure_owner in {"product", "readiness"}:
        hydration.assert_not_called()


def test_running_transition_failure_withdraws_runtime_without_error_transition() -> (
    None
):
    state = _State([])
    service, _events, _db, state_manager, _hydration, runtime_artifacts = (
        _build_service(state=state)
    )
    failure = RuntimeError("running transition failed")
    state_manager.transition_to_running.side_effect = failure

    assert not _activate(service)

    runtime_artifacts.unregister.assert_called_once_with("strategy")
    state_manager.transition_to_error.assert_not_called()
