"""Fast in-memory backtest path for parameter search workloads.

This runner intentionally bypasses persistence, signal audit, journal export,
and report generation. It keeps the same candle ordering and Rust matching
adapter as the full BacktestRunner so research-mode results can be compared
against the production replay path.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, Optional

from src.core.adapters.simulated import SimulatedAdapter
from src.core.analytics import calculate_metrics
from src.core.backtest.loader import get_candles_generator
from src.core.clock import BacktestClock
from src.core.db import SessionLocal
from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick, OrderSide, Signal, SignalType
from src.core.orm_models import Order
from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ResearchTrade:
    """Minimal trade shape consumed by calculate_metrics()."""

    id: str
    order_id: str
    product_id: str
    side: OrderSide
    price: Decimal
    quantity: Decimal
    fee: Decimal
    timestamp: int
    strategy_id: Optional[str] = None


class ResearchBacktestRunner:
    """In-memory fast runner for GA fitness and parameter search.

    Scope:
    - Uses strategy.on_candle() and SimulatedAdapter/Rust matching directly.
    - Preserves full-runner ordering: existing orders fill before new signals.
    - Does not apply RiskManager checks, DB writes, signal audits, or reports.
    """

    def __init__(
        self,
        start_time: int,
        end_time: int,
        product_id: str,
        timeframe: str,
        initial_balance: float = 10000.0,
        data_source: Optional[IDataSource] = None,
        fee_config: Optional[Dict[str, float]] = None,
        max_drawdown_limit: Optional[float] = None,
        balance_check_interval: int = 0,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        self.initial_balance = initial_balance
        self.data_source = data_source
        self.fee_config = fee_config or {}
        self.max_drawdown_limit = max_drawdown_limit
        self.balance_check_interval = balance_check_interval
        self.clock = BacktestClock(start_time=start_time / 1000)
        self._strategies: list[BaseStrategy] = []

    def add_strategy(self, strategy: BaseStrategy) -> None:
        self._strategies.append(strategy)

    def run(self) -> dict:
        if not self._strategies:
            logger.warning("No strategies added. Exiting.")
            return {}

        adapter = SimulatedAdapter(
            initial_balance=Decimal(str(self.initial_balance)),
            maker_fee=Decimal(str(self.fee_config.get("maker", 0))),
            taker_fee=Decimal(str(self.fee_config.get("taker", 0))),
        )
        trades: list[ResearchTrade] = []
        stop_threshold = self._stop_threshold()

        candle_count = 0
        for candle in self._iter_candles():
            self.clock.set_time(candle.timestamp / 1000)

            fills = adapter.on_market_data(candle)
            trades.extend(self._fills_to_trades(fills, candle))

            for strategy in self._strategies:
                if strategy.product_id != candle.product_id:
                    continue
                if strategy.requirements.timeframe != candle.timeframe:
                    continue
                signals = self._signals_from_strategy(strategy, candle)
                for signal in signals:
                    order = self._order_from_signal(signal, candle)
                    if order is not None:
                        adapter.place_order(order)

            candle_count += 1
            if (
                stop_threshold is not None
                and self.balance_check_interval > 0
                and candle_count % self.balance_check_interval == 0
                and adapter.get_balance() < stop_threshold
            ):
                logger.warning("Stopping research backtest at drawdown threshold")
                break

        final_balance = adapter.get_balance()
        total_pnl = final_balance - Decimal(str(self.initial_balance))
        metrics = calculate_metrics(trades, initial_balance=self.initial_balance)
        return {
            "total_pnl": total_pnl,
            "max_drawdown": metrics.get("max_drawdown", Decimal("0")),
            "win_rate": metrics.get("win_rate", 0.0),
            "total_trades": int(metrics.get("total_trades", 0)),
            "trade_sharpe": metrics.get("trade_sharpe", Decimal("0")),
            "profit_factor": metrics.get("profit_factor", Decimal("0")),
            "sortino_ratio": metrics.get("sortino_ratio", Decimal("0")),
            "calmar_ratio": metrics.get("calmar_ratio", Decimal("0")),
            "avg_hold_time_hours": metrics.get("avg_hold_time_hours", Decimal("0")),
            "max_consecutive_wins": int(metrics.get("max_consecutive_wins", 0)),
            "max_consecutive_losses": int(metrics.get("max_consecutive_losses", 0)),
            "closed_trades": metrics.get("closed_trades", []),
            "raw_trades": trades,
            "raw_trade_count": len(trades),
            "candle_count": candle_count,
            "report_dir": None,
        }

    def _iter_candles(self) -> Iterable[Candlestick]:
        if self.data_source:
            return self.data_source.get_candles(
                self.product_id,
                self.timeframe,
                self.start_time,
                self.end_time,
            )

        session = SessionLocal()

        def generator():
            try:
                yield from get_candles_generator(
                    session,
                    self.product_id,
                    self.timeframe,
                    self.start_time,
                    self.end_time,
                )
            finally:
                session.close()

        return generator()

    def _stop_threshold(self) -> Optional[Decimal]:
        if self.max_drawdown_limit is None:
            return None
        return Decimal(str(self.initial_balance)) * Decimal(str(1 - self.max_drawdown_limit))

    def _signals_from_strategy(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
    ) -> list[Signal]:
        result = strategy.on_candle(candle)
        if result is None:
            return []
        if isinstance(result, Signal):
            signals = [result]
        elif isinstance(result, list):
            signals = result
        else:
            raise TypeError("strategy.on_candle() must return None, Signal, or list[Signal]")
        return [signal for signal in signals if signal.type != SignalType.NO_SIGNAL]

    def _order_from_signal(
        self,
        signal: Signal,
        candle: Candlestick,
    ) -> Optional[Order]:
        side = self._determine_side(signal.type)
        if side is None:
            return None

        quantity = signal.quantity if signal.quantity and signal.quantity > 0 else Decimal("0.01")
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value:
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        order_id = str(uuid.uuid4())
        return Order(
            id=order_id,
            exchange_order_id=f"sim_{order_id[:8]}",
            strategy_id=signal.strategy_id,
            product_id=signal.product_id,
            exchange_id=signal.product_id.split(":")[0],
            type=order_type,
            side=side,
            price=limit_price,
            trigger_price=None,
            quantity=quantity,
            status="open",
            timestamp=candle.timestamp,
            filled_quantity=Decimal("0"),
            filled_price=Decimal("0"),
        )

    def _fills_to_trades(self, fills: list[dict], candle: Candlestick) -> list[ResearchTrade]:
        trades: list[ResearchTrade] = []
        for fill in fills:
            order = fill["order"]
            trade_id = str(uuid.uuid4())
            trades.append(
                ResearchTrade(
                    id=trade_id,
                    order_id=order.id,
                    product_id=order.product_id,
                    strategy_id=order.strategy_id,
                    side=order.side,
                    price=fill["price"],
                    quantity=fill["quantity"],
                    fee=fill.get("fee") or Decimal("0"),
                    timestamp=candle.timestamp,
                )
            )
        return trades

    @staticmethod
    def _determine_side(signal_type: SignalType) -> Optional[OrderSide]:
        if signal_type == SignalType.LONG:
            return OrderSide.BUY
        if signal_type == SignalType.SHORT:
            return OrderSide.SELL
        if signal_type == SignalType.EXIT_LONG:
            return OrderSide.SELL
        if signal_type == SignalType.EXIT_SHORT:
            return OrderSide.BUY
        return None
