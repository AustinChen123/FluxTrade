"""Immutable value snapshots passed from a completed full backtest."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from src.core.analytics import ClosedTrade
from src.core.models import PositionSide


def _require_finite_decimal(value: object, field_name: str) -> Decimal:
    if type(value) is not Decimal:
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return value


@dataclass(frozen=True, slots=True)
class ClosedTradeSnapshot:
    entry_time: int
    exit_time: int
    entry_price: Decimal
    exit_price: Decimal
    side: PositionSide
    quantity: Decimal
    pnl: Decimal
    fee: Decimal

    def __post_init__(self) -> None:
        if type(self.entry_time) is not int or type(self.exit_time) is not int:
            raise TypeError("closed trade timestamps must be integers")
        if type(self.side) is not PositionSide:
            raise TypeError("closed trade side must be PositionSide")
        for name in ("entry_price", "exit_price", "quantity", "pnl", "fee"):
            _require_finite_decimal(getattr(self, name), name)

    @classmethod
    def copy_of(cls, trade: ClosedTrade) -> ClosedTradeSnapshot:
        if type(trade) is not ClosedTrade:
            raise TypeError("closed trades must be exact ClosedTrade values")
        return cls(
            entry_time=trade.entry_time,
            exit_time=trade.exit_time,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price,
            side=trade.side,
            quantity=trade.quantity,
            pnl=trade.pnl,
            fee=trade.fee,
        )


@dataclass(frozen=True, slots=True)
class FullBacktestOutcome:
    initial_balance: Decimal
    mark_to_market_pnl: Decimal
    max_drawdown: Decimal
    trade_sharpe: Decimal
    sortino_ratio: Decimal
    calmar_ratio: Decimal
    equity_samples: tuple[tuple[int, Decimal], ...]
    closed_trades: tuple[ClosedTradeSnapshot, ...]

    def __post_init__(self) -> None:
        for name in (
            "initial_balance",
            "mark_to_market_pnl",
            "max_drawdown",
            "trade_sharpe",
            "sortino_ratio",
            "calmar_ratio",
        ):
            _require_finite_decimal(getattr(self, name), name)
        if type(self.equity_samples) is not tuple:
            raise TypeError("equity_samples must be a tuple")
        for sample in self.equity_samples:
            if type(sample) is not tuple or len(sample) != 2:
                raise TypeError("equity samples must be timestamp/value tuples")
            timestamp, equity = sample
            if type(timestamp) is not int:
                raise TypeError("equity sample timestamp must be an integer")
            _require_finite_decimal(equity, "equity sample")
        if type(self.closed_trades) is not tuple or any(
            type(trade) is not ClosedTradeSnapshot for trade in self.closed_trades
        ):
            raise TypeError("closed_trades must be a tuple of snapshots")

    @classmethod
    def from_completed_result(
        cls,
        *,
        initial_balance: Decimal,
        result: Mapping[str, object],
        equity_samples: Sequence[tuple[int, Decimal]],
    ) -> FullBacktestOutcome:
        if not isinstance(result, Mapping):
            raise TypeError("completed result must be a mapping")
        metric_names = (
            "mark_to_market_pnl",
            "max_drawdown",
            "trade_sharpe",
            "sortino_ratio",
            "calmar_ratio",
        )
        metrics: dict[str, Decimal] = {}
        for name in metric_names:
            if name not in result:
                raise ValueError(f"completed result is missing {name}")
            metrics[name] = _require_finite_decimal(result[name], name)
        if not isinstance(equity_samples, Sequence):
            raise TypeError("equity_samples must be a sequence")
        copied_samples: list[tuple[int, Decimal]] = []
        for sample in equity_samples:
            if not isinstance(sample, Sequence) or len(sample) != 2:
                raise TypeError("equity samples must contain timestamp/value pairs")
            timestamp, equity = sample
            if type(timestamp) is not int:
                raise TypeError("equity sample timestamp must be an integer")
            copied_samples.append(
                (timestamp, _require_finite_decimal(equity, "equity sample"))
            )

        closed_trades = result["closed_trades"]
        if not isinstance(closed_trades, Sequence):
            raise TypeError("closed_trades must be a sequence")
        snapshots = tuple(ClosedTradeSnapshot.copy_of(trade) for trade in closed_trades)
        return cls(
            initial_balance=_require_finite_decimal(initial_balance, "initial_balance"),
            **metrics,
            equity_samples=tuple(copied_samples),
            closed_trades=snapshots,
        )
