from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.models import Candlestick, OrderSide, Trade
from src.core.pending_market_replay import PendingMarketReplayService
from src.strategies.base import BaseStrategy


def _candle(
    *,
    product_id: str = "BINANCE:BTCUSDT-PERP",
    timeframe: str = "1m",
    timestamp: int = 1_704_067_200_000,
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=timestamp,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("2"),
    )


def _strategy(strategy_id: str, product_id: str, timeframe: str) -> MagicMock:
    strategy = MagicMock(name=strategy_id)
    strategy.strategy_id = strategy_id
    strategy.product_id = product_id
    strategy.requirements = SimpleNamespace(timeframe=timeframe)
    return strategy


def _service(
    *,
    application: MagicMock | None = None,
    hydration: MagicMock | None = None,
    active: list[MagicMock] | None = None,
    publish: MagicMock | None = None,
    events: list[str] | None = None,
) -> tuple[
    PendingMarketReplayService,
    MagicMock,
    MagicMock,
    MagicMock,
    MagicMock,
]:
    application = application or MagicMock()
    application.assert_newer = MagicMock()
    hydration = hydration or MagicMock()
    publish = publish or MagicMock()
    active = [] if active is None else active
    events = [] if events is None else events
    db = MagicMock()

    @contextmanager
    def session_factory():
        events.append("db-enter")
        try:
            yield db
        finally:
            events.append("db-exit")

    def publish_replacement(replacement: BaseStrategy) -> None:
        events.append(f"publish:{replacement.strategy_id}")
        publish(replacement)

    service = PendingMarketReplayService(
        db_session_factory=session_factory,
        live_candle_application=application,
        strategy_hydration=hydration,
        list_active_strategies=lambda: tuple(active),
        publish_replacement=publish_replacement,
    )
    return service, application, hydration, publish, db


def test_empty_rewind_performs_no_owner_or_database_work() -> None:
    events: list[str] = []
    service, application, hydration, publish, _db = _service(events=events)

    service.rewind_pending(())

    assert events == []
    application.was_applied.assert_not_called()
    hydration.warm_up.assert_not_called()
    publish.assert_not_called()


def test_pending_trade_is_rejected_before_application_or_publication() -> None:
    service, application, hydration, publish, _db = _service()
    trade = Trade(
        id="trade-1",
        product_id="BINANCE:BTCUSDT-PERP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=OrderSide.BUY,
        timestamp=1_704_067_200_000,
    )

    with pytest.raises(
        RuntimeError,
        match="pending trade replay has no durable strategy-state boundary",
    ):
        service.replay(trade, apply_new=MagicMock())

    application.replay.assert_not_called()
    hydration.warm_up.assert_not_called()
    publish.assert_not_called()


def test_rewind_uses_earliest_cutoff_and_publishes_after_all_hydration() -> None:
    events: list[str] = []
    active = [
        _strategy("a", "BINANCE:BTCUSDT-PERP", "1m"),
        _strategy("b", "BINANCE:ETHUSDT-PERP", "5m"),
        _strategy("unrelated", "BINANCE:BTCUSDT-PERP", "5m"),
    ]
    hydration = MagicMock()
    replacements = {
        strategy.strategy_id: _strategy(
            f"replacement-{strategy.strategy_id}",
            strategy.product_id,
            strategy.requirements.timeframe,
        )
        for strategy in active
    }
    hydration.fresh_instance_for_replay.side_effect = lambda strategy: replacements[
        strategy.strategy_id
    ]
    hydration.warm_up.side_effect = lambda _db, replacement, **_kwargs: events.append(
        f"warm:{replacement.strategy_id}"
    )
    service, application, _hydration, publish, db = _service(
        hydration=hydration,
        active=active,
        events=events,
    )
    application.was_applied.return_value = False
    candles = (
        _candle(timestamp=300),
        _candle(timestamp=100),
        _candle(product_id="BINANCE:ETHUSDT-PERP", timeframe="5m", timestamp=200),
    )

    service.rewind_pending(candles)

    assert application.was_applied.call_args_list == [
        call(candle, db=db) for candle in candles
    ]
    assert application.assert_newer.call_args_list == [
        call(candle, db=db) for candle in candles
    ]
    assert hydration.warm_up.call_args_list == [
        call(db, replacements["a"], before_timestamp=100),
        call(db, replacements["b"], before_timestamp=200),
    ]
    assert events == [
        "db-enter",
        "warm:replacement-a",
        "warm:replacement-b",
        "db-exit",
        "publish:replacement-a",
        "publish:replacement-b",
    ]
    assert publish.call_args_list == [
        call(replacements["a"]),
        call(replacements["b"]),
    ]


def test_hydration_failure_publishes_no_partial_replacement() -> None:
    active = [
        _strategy("a", "BINANCE:BTCUSDT-PERP", "1m"),
        _strategy("b", "BINANCE:BTCUSDT-PERP", "1m"),
    ]
    hydration = MagicMock()
    hydration.fresh_instance_for_replay.side_effect = lambda current: _strategy(
        f"replacement-{current.strategy_id}",
        current.product_id,
        current.requirements.timeframe,
    )
    hydration.warm_up.side_effect = [None, RuntimeError("warm-up failed")]
    service, application, _hydration, publish, _db = _service(
        hydration=hydration,
        active=active,
    )
    application.was_applied.return_value = False

    with pytest.raises(RuntimeError, match="warm-up failed"):
        service.rewind_pending((_candle(),))

    publish.assert_not_called()


def test_rebuild_requires_receipt_and_replays_through_candle() -> None:
    current = _strategy("a", "BINANCE:BTCUSDT-PERP", "1m")
    replacement = _strategy("replacement-a", current.product_id, "1m")
    hydration = MagicMock()
    hydration.fresh_instance_for_replay.return_value = replacement
    service, application, _hydration, publish, db = _service(
        hydration=hydration,
        active=[current],
    )
    candle = _candle(timestamp=500)
    application.was_applied.return_value = True

    service.rebuild_applied(candle)

    application.was_applied.assert_called_once_with(candle, db=db)
    hydration.warm_up.assert_called_once_with(
        db,
        replacement,
        before_timestamp=501,
    )
    publish.assert_called_once_with(replacement)


def test_rebuild_rejects_unapplied_candle_before_hydration() -> None:
    service, application, hydration, publish, _db = _service()
    application.was_applied.return_value = False

    with pytest.raises(
        RuntimeError,
        match="cannot rebuild strategy through an unapplied candle",
    ):
        service.rebuild_applied(_candle())

    hydration.fresh_instance_for_replay.assert_not_called()
    publish.assert_not_called()


def test_replay_delegates_exact_rewind_apply_and_rebuild_callbacks() -> None:
    service, application, _hydration, _publish, _db = _service()
    candle = _candle()
    apply_new = MagicMock()

    with (
        patch.object(service, "rewind_pending") as rewind_pending,
        patch.object(service, "rebuild_applied") as rebuild_applied,
    ):
        service.replay(candle, apply_new=apply_new)

        application.replay.assert_called_once()
        args = application.replay.call_args
        assert args.args == (candle,)
        assert args.kwargs["apply_new"] is apply_new
        args.kwargs["rewind_pending"](candle)
        args.kwargs["rebuild_applied"](candle)

        rewind_pending.assert_called_once_with((candle,))
        rebuild_applied.assert_called_once_with(candle)
