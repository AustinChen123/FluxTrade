from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.models import Candlestick, Signal
from src.core.strategy_context import StrategyContext
from src.core.strategy_hydration_service import StrategyHydrationService
from src.strategies.base import BaseStrategy
from src.strategies.base import StrategyRequirements


class _Strategy(BaseStrategy):
    def __init__(
        self,
        *,
        strategy_id: str = "s1",
        product_id: str = "BINANCE:BTCUSDT-PERP",
        lookback: int = 2,
        configuration: object = ("stable",),
    ) -> None:
        super().__init__(strategy_id, product_id)
        self._requirements = StrategyRequirements(product_id, "1m", lookback)
        self._configuration = configuration

    @property
    def requirements(self) -> StrategyRequirements:
        return self._requirements

    def replay_configuration(self) -> object:
        return self._configuration

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        del candle, context
        raise AssertionError("hydration owner must delegate warm-up to SignalProcessor")

    def fresh_instance_for_replay(self) -> _Strategy:
        return _Strategy(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            lookback=self.requirements.lookback_window,
            configuration=self._configuration,
        )


def _service() -> tuple[StrategyHydrationService, MagicMock, MagicMock]:
    signal_processor = MagicMock()
    account_service = MagicMock()
    account_service.get_position.return_value = None
    service = StrategyHydrationService(
        signal_processor=signal_processor,
        account_service=account_service,
    )
    return service, signal_processor, account_service


def _row(timestamp: int) -> SimpleNamespace:
    return SimpleNamespace(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=timestamp,
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        volume=Decimal("3.00"),
    )


def _db_with_rows(rows: list[SimpleNamespace]) -> tuple[MagicMock, MagicMock]:
    db = MagicMock()
    query = MagicMock()
    db.query.return_value = query
    query.filter.return_value = query
    query.order_by.return_value = query
    query.limit.return_value = query
    query.all.return_value = rows
    return db, query


def test_zero_lookback_skips_query_but_still_synchronizes_position() -> None:
    service, signal_processor, account_service = _service()
    db = MagicMock()
    strategy = _Strategy(lookback=0)

    assert service.warm_up(db, strategy) == 0

    db.query.assert_not_called()
    signal_processor.warm_up.assert_not_called()
    account_service.get_position.assert_called_once_with(
        strategy.strategy_id,
        strategy.product_id,
    )
    signal_processor.set_position_state.assert_called_once_with(strategy, None)


def test_warm_up_orders_rows_and_preserves_exact_values() -> None:
    service, signal_processor, _account_service = _service()
    db, query = _db_with_rows([_row(2000), _row(1000)])
    strategy = _Strategy()

    assert service.warm_up(db, strategy, before_timestamp=3000) == 2

    assert query.filter.call_count == 2
    candles = signal_processor.warm_up.call_args.args[1]
    assert [candle.timestamp for candle in candles] == [1000, 2000]
    assert [candle.close for candle in candles] == [
        Decimal("100.50"),
        Decimal("100.50"),
    ]
    signal_processor.warm_up.assert_called_once_with(strategy, candles)


def test_incomplete_warm_up_fails_before_signal_or_position_sync() -> None:
    service, signal_processor, account_service = _service()
    db, _query = _db_with_rows([_row(1000)])
    strategy = _Strategy(lookback=2)

    with pytest.raises(
        RuntimeError,
        match="warmup_insufficient_candles: strategy_id=s1 available=1 required=2",
    ):
        service.warm_up(db, strategy)

    signal_processor.warm_up.assert_not_called()
    account_service.get_position.assert_not_called()


@pytest.mark.parametrize(
    ("position", "side"),
    [
        (None, None),
        (SimpleNamespace(side=SimpleNamespace(value="LONG")), "LONG"),
        (SimpleNamespace(side="SHORT"), "SHORT"),
    ],
)
def test_position_sync_passes_exact_authoritative_side(position, side) -> None:
    service, signal_processor, account_service = _service()
    strategy = _Strategy()
    account_service.get_position.return_value = position
    signal_processor.set_position_state.return_value = True

    service.sync_position_state(strategy)

    signal_processor.set_position_state.assert_called_once_with(strategy, side)


def test_position_read_failure_preserves_cause() -> None:
    service, _signal_processor, account_service = _service()
    failure = ConnectionError("position store unavailable")
    account_service.get_position.side_effect = failure

    with pytest.raises(RuntimeError, match="position_state_sync_failed") as raised:
        service.sync_position_state(_Strategy())

    assert raised.value.__cause__ is failure


def test_nonflat_position_without_sync_support_fails_closed() -> None:
    service, signal_processor, account_service = _service()
    account_service.get_position.return_value = SimpleNamespace(side="LONG")
    signal_processor.set_position_state.return_value = False

    with pytest.raises(RuntimeError, match="position_state_sync_unsupported"):
        service.sync_position_state(_Strategy())


def test_fresh_replay_instance_preserves_exact_identity() -> None:
    service, _signal_processor, _account_service = _service()
    current = _Strategy()

    replacement = service.fresh_instance_for_replay(current)

    assert replacement is not current
    assert type(replacement) is type(current)
    assert replacement.strategy_id == current.strategy_id
    assert replacement.product_id == current.product_id
    assert replacement.requirements == current.requirements
    assert replacement.replay_configuration() == current.replay_configuration()


@pytest.mark.parametrize(
    "mutation",
    ["same", "type", "strategy_id", "product_id", "requirements", "configuration"],
)
def test_incompatible_replay_instance_fails_closed(mutation: str) -> None:
    service, _signal_processor, _account_service = _service()
    current = _Strategy()
    replacement: object = current.fresh_instance_for_replay()
    if mutation == "same":
        replacement = current
    elif mutation == "type":
        replacement = SimpleNamespace(
            strategy_id=current.strategy_id,
            product_id=current.product_id,
            requirements=current.requirements,
            replay_configuration=current.replay_configuration,
        )
    elif mutation == "strategy_id":
        replacement.strategy_id = "other"
    elif mutation == "product_id":
        replacement.product_id = "OTHER:PRODUCT"
    elif mutation == "requirements":
        replacement._requirements = StrategyRequirements(
            current.product_id,
            "5m",
            2,
        )
    else:
        replacement._configuration = ("changed",)
    current.fresh_instance_for_replay = MagicMock(return_value=replacement)

    with pytest.raises(
        RuntimeError,
        match="strategy recovery factory did not return a distinct compatible instance",
    ):
        service.fresh_instance_for_replay(current)
