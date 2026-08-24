from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import nullcontext
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.core.models import Candlestick, Signal, StrategyStatus
from src.core.portfolio_runtime import (
    PortfolioDefinition,
    PortfolioFactory,
    PortfolioSleeve,
)
from src.core.strategy_activation_service import StrategyActivationService
from src.core.strategy_context import StrategyContext
from src.core.strategy_state_manager import StaleStrategyStateVersion
from src.strategies.base import BaseStrategy, StrategyRequirements


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
    assert_context_capabilities: Callable[[tuple[BaseStrategy, ...]], None]
    | None = None,
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
            assert_context_capabilities or (lambda _strategies: None)
        ),
        event_logger=MagicMock(),
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
) -> bool:
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
