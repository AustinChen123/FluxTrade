"""Tests for src/core/command_router.py."""

from __future__ import annotations

from unittest.mock import MagicMock
import pytest

from src.core.command_router import (
    CommandResult,
    CommandRouter,
    StrategyStartDisposition,
)
from src.core.health_monitor import HealthMonitor
from src.core.models import Candlestick, Signal, SignalType
from src.core.strategy_context import StrategyContext
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy, StrategyRequirements


class DummyStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        product_id: str = "BINANCE:BTCUSDT-PERP",
        timeframe: str = "1m",
    ):
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, self._timeframe, 10)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self._timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )


def test_command_result_dataclass() -> None:
    result = CommandResult(True, "ok", {"value": 1})

    assert result.success is True
    assert result.message == "ok"
    assert result.data == {"value": 1}


def test_start_delegates_to_state_manager() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())
    router.state_manager.transition_to_running.return_value = None

    result = router.handle(
        {
            "command": "START",
            "params": {
                "id": "s1",
                "actor": "operator@example.com",
                "reason": "deployment",
            },
        }
    )

    assert result.success is True
    router.state_manager.transition_to_running.assert_called_once_with(
        "s1",
        actor="operator@example.com",
        reason="deployment",
    )


def test_stop_delegates_to_state_manager() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    result = router.handle(
        {
            "command": "STOP",
            "strategy_id": "s1",
            "params": {"actor": "operator@example.com"},
            "reason": "maintenance",
        }
    )

    assert result.success is True
    router.state_manager.transition_to_stopped.assert_called_once_with(
        "s1",
        actor="operator@example.com",
        reason="maintenance",
    )


def test_resume_delegates_to_forced_running_transition() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())
    router.state_manager.transition_to_running.return_value = None

    result = router.handle(
        {
            "cmd": "resume",
            "strategy_id": "s1",
            "params": {
                "actor": "operator@example.com",
                "reason": "operator confirmed",
            },
        }
    )

    assert result.success is True
    router.state_manager.transition_to_running.assert_called_once_with(
        "s1",
        actor="operator@example.com",
        force=True,
        reason="operator confirmed",
    )


def test_force_recover_delegates_to_forced_running_transition() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())
    router.state_manager.transition_to_running.return_value = None

    result = router.handle(
        {
            "cmd": "force_recover",
            "params": {
                "strategy_id": "s1",
                "actor": "operator@example.com",
                "reason": "manual reset",
            },
        }
    )

    assert result.success is True
    router.state_manager.transition_to_running.assert_called_once_with(
        "s1",
        actor="operator@example.com",
        force=True,
        reason="manual reset",
    )


def test_force_recover_forwards_expected_version() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())
    router.state_manager.transition_to_running.return_value = None

    result = router.handle(
        {
            "command": "FORCE_RECOVER",
            "params": {
                "strategy_id": "s1",
                "expected_version": 4,
            },
        }
    )

    assert result.success is True
    router.state_manager.transition_to_running.assert_called_once_with(
        "s1",
        actor="operator",
        force=True,
        reason=None,
        expected_version=4,
    )


def test_lifecycle_command_rejects_invalid_expected_version() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    result = router.handle(
        {
            "command": "STOP",
            "params": {
                "strategy_id": "s1",
                "expected_version": True,
            },
        }
    )

    assert result == CommandResult(
        False,
        "expected_version must be a non-negative integer",
    )
    router.state_manager.transition_to_stopped.assert_not_called()


def test_reload_rejects_unimplemented_command() -> None:
    registry = StrategyRegistry()
    router = CommandRouter(registry, MagicMock())

    result = router.handle({"command": "RELOAD", "params": {"strategy_id": "s1"}})

    assert result.success is False
    assert result.message == "Strategy reload is not implemented: s1"


def test_list_returns_active_strategy_metadata() -> None:
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1", timeframe="1m"))
    registry.register(DummyStrategy("s2", timeframe="5m"))
    router = CommandRouter(registry, MagicMock())

    result = router.handle({"command": "LIST"})

    assert result == CommandResult(
        True,
        "Listed active strategies",
        {
            "strategies": [
                {
                    "strategy_id": "s1",
                    "product_id": "BINANCE:BTCUSDT-PERP",
                    "timeframe": "1m",
                },
                {
                    "strategy_id": "s2",
                    "product_id": "BINANCE:BTCUSDT-PERP",
                    "timeframe": "5m",
                },
            ]
        },
    )


def test_health_check_returns_per_strategy_status(
    monkeypatch,
) -> None:
    now = 100.0
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: now)
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1"))
    registry.register(DummyStrategy("s2"))
    monitor = HealthMonitor(registry)
    monitor.update_heartbeat("s1")
    router = CommandRouter(registry, MagicMock(), health_monitor=monitor)

    result = router.handle({"command": "HEALTH_CHECK"})

    assert result == CommandResult(
        True,
        "Health check complete",
        {"healthy": {"s1": True, "s2": False}},
    )


def test_unknown_command_returns_failure() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    result = router.handle({"command": "NOPE"})

    assert result.success is False
    assert result.message == "Unknown command: NOPE"


def test_malformed_command_returns_failure() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    assert router.handle({}).success is False
    assert router.handle({"command": "START"}).success is False
    assert router.handle({"command": "LIST", "params": "bad"}).success is False


@pytest.mark.parametrize("command", ["START", "RESUME", "FORCE_RECOVER"])
@pytest.mark.parametrize("disposition", [None, *StrategyStartDisposition])
def test_start_completion_contract(command, disposition):
    state = MagicMock()
    state.transition_to_running.return_value = disposition
    result = CommandRouter(StrategyRegistry(), state).handle(
        {"command": command, "id": "s"}
    )
    waiting = disposition is StrategyStartDisposition.WAITING_FOR_CUTOVER
    assert result.success is True and result.completed is (not waiting)
    if waiting:
        assert "Accepted" in result.message and "waiting" in result.message
        assert "Started" not in result.message and "Resumed" not in result.message
    state.transition_to_running.assert_called_once()


@pytest.mark.parametrize("unknown", [True, False, 1, "ACTIVE", object()])
def test_unknown_transition_result_fails_closed(unknown):
    state = MagicMock()
    state.transition_to_running.return_value = unknown
    with pytest.raises(ValueError):
        CommandRouter(StrategyRegistry(), state).handle({"command": "START", "id": "s"})


@pytest.mark.parametrize(
    "value", [True, False, StrategyStartDisposition.WAITING_FOR_CUTOVER, None]
)
def test_engine_adapter_preserves_typed_disposition(value):
    from src.core.engine import _EngineLifecycleAdapter

    engine = MagicMock()
    engine.activate_strategy.return_value = value
    adapter = _EngineLifecycleAdapter(engine)
    if value is False or value is None:
        with pytest.raises(RuntimeError):
            adapter.transition_to_running("s")
    else:
        expected = StrategyStartDisposition.ACTIVE if value is True else value
        assert adapter.transition_to_running("s") is expected
    engine.activate_strategy.assert_called_once_with("s")
