"""Fast in-memory backtest path for parameter search workloads.

This runner intentionally bypasses persistence, signal audit, journal export,
and report generation. It keeps the same candle ordering and Rust matching
adapter as the full BacktestRunner so research-mode results can be compared
against the production replay path.
"""

from __future__ import annotations

import logging
import uuid
import inspect
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Sequence, cast

from src.core.adapters.simulated import SimulatedAdapter
from src.core.analytics import (
    annualized_sharpe_from_moments,
    calculate_metrics,
    utc_daily_return_metrics,
)
from src.core.backtest.equity import require_strategy_position_scope
from src.core.backtest.loader import get_candles_generator
from src.core.clock import BacktestClock
from src.core.conditional_order_intents import (
    conditional_oco_pairs,
    conditional_order_intents,
)
from src.core.db import SessionLocal
from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick, OrderSide, Signal, SignalType
from src.core.orm_models import Order
from src.core.precision import PrecisionCodec
from src.core.product_registry import (
    InstrumentSpec,
    calculate_required_capital,
    resolve_contract_multiplier,
)
from src.core.strategy_context import RejectionSnapshot, StrategyContext
from src.core.signal_processor import apply_strategy_position_state
from src.strategies.base import BaseStrategy

if TYPE_CHECKING:
    from src.core.capital_allocator import CapitalAllocator

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
        precision_codec: PrecisionCodec | None = None,
        prepared_scaled_candles: Sequence[Any] | None = None,
        capital_allocator: CapitalAllocator | None = None,
        instrument_spec: InstrumentSpec | None = None,
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
        self.precision_codec = precision_codec
        self.prepared_scaled_candles = prepared_scaled_candles
        self.capital_allocator = capital_allocator
        self.instrument_spec = instrument_spec
        self.contract_multiplier = resolve_contract_multiplier(instrument_spec)
        self._reserved_entry_capital: dict[str, tuple[str, Decimal]] = {}
        self._latest_rejections: dict[str, tuple[RejectionSnapshot, ...]] = {}
        self.clock = BacktestClock(start_time=start_time / 1000)
        self._strategies: list[BaseStrategy] = []

    def add_strategy(self, strategy: BaseStrategy) -> None:
        self._strategies.append(strategy)

    def run(self) -> dict:
        if not self._strategies:
            logger.warning("No strategies added. Exiting.")
            return {}

        self._reserved_entry_capital = {}
        self._latest_rejections = {}
        adapter = SimulatedAdapter(
            initial_balance=Decimal(str(self.initial_balance)),
            maker_fee=Decimal(str(self.fee_config.get("maker", 0))),
            taker_fee=Decimal(str(self.fee_config.get("taker", 0))),
            precision_codec=self.precision_codec,
            instrument_spec=self.instrument_spec,
        )
        self._ensure_capital_allocator_supported(adapter)
        trades: list[ResearchTrade] = []
        stop_drawdown_amount = self._stop_drawdown_amount()
        context_support = {
            strategy.strategy_id: _strategy_accepts_context(strategy)
            for strategy in self._strategies
        }
        initial_equity = Decimal(str(self.initial_balance))
        peak_equity_by_strategy = {
            strategy.strategy_id: initial_equity
            for strategy in self._strategies
        }
        max_drawdown_by_strategy = {
            strategy.strategy_id: Decimal("0")
            for strategy in self._strategies
        }
        portfolio_peak_equity = initial_equity
        portfolio_max_drawdown = Decimal("0")
        equity_samples: list[tuple[int, Decimal]] = []
        observed_position_sides: dict[tuple[str, str], str | None] = {}

        candle_count = 0
        for candle, prepared_candle in self._iter_replay_candles(adapter):
            self.clock.set_time(candle.timestamp / 1000)

            if prepared_candle is None:
                fills = adapter.on_market_data(candle)
            else:
                fills = adapter.on_prepared_market_data(prepared_candle)
            trades.extend(self._fills_to_trades(fills, candle))
            self._sync_capital_usage(adapter, candle)

            active_strategies = [
                strategy
                for strategy in self._strategies
                if strategy.product_id == candle.product_id
                and strategy.requirements.timeframe == candle.timeframe
            ]
            require_strategy_position_scope(
                adapter,
                [strategy.strategy_id for strategy in active_strategies],
            )
            contexts: list[StrategyContext] = []
            for strategy in active_strategies:
                position = adapter.get_position(
                    strategy.product_id,
                    strategy_id=strategy.strategy_id,
                )
                position_side = (
                    None
                    if position is None
                    else str(getattr(position.side, "value", position.side)).upper()
                )
                position_key = (strategy.strategy_id, strategy.product_id)
                if (
                    position_key not in observed_position_sides
                    or observed_position_sides[position_key] != position_side
                ):
                    apply_strategy_position_state(strategy, position_side)
                    observed_position_sides[position_key] = position_side
                context = self._strategy_context(
                    adapter=adapter,
                    strategy=strategy,
                    candle=candle,
                    latest_fills=fills,
                    peak_equity_by_strategy=peak_equity_by_strategy,
                    max_drawdown_by_strategy=max_drawdown_by_strategy,
                )
                contexts.append(context)
                decision_context = None
                if context_support[strategy.strategy_id]:
                    decision_context = context
                signals = self._signals_from_strategy(strategy, candle, decision_context)
                for signal in signals:
                    if self._capital_rejects_entry(signal, candle):
                        self._record_capital_rejection(signal, candle)
                        continue
                    if self._exit_without_position(signal, adapter):
                        continue
                    entry_order = self._order_from_signal(signal, candle, adapter)
                    if entry_order is not None:
                        orders = [
                            entry_order,
                            *self._conditional_orders_from_signal(
                                signal,
                                entry_order,
                                candle,
                            ),
                        ]
                        for order in orders:
                            adapter.validate_order(order)
                        for order in orders:
                            adapter.place_order(order)
                        self._reserve_entry_capital(signal, entry_order, candle)

            portfolio_current_equity = (
                contexts[0].available_cash
                + sum(
                    (context.unrealized_pnl for context in contexts),
                    start=Decimal("0"),
                )
                if contexts
                else adapter.get_balance()
            )
            equity_samples.append((candle.timestamp, portfolio_current_equity))
            portfolio_peak_equity = max(
                portfolio_peak_equity,
                portfolio_current_equity,
            )
            portfolio_max_drawdown = max(
                portfolio_max_drawdown,
                portfolio_peak_equity - portfolio_current_equity,
            )
            candle_count += 1
            if (
                stop_drawdown_amount is not None
                and self.balance_check_interval > 0
                and candle_count % self.balance_check_interval == 0
                and portfolio_max_drawdown >= stop_drawdown_amount
            ):
                logger.warning("Stopping research backtest at drawdown threshold")
                break

        final_balance = adapter.get_balance()
        total_pnl = final_balance - Decimal(str(self.initial_balance))
        metrics = calculate_metrics(
            trades,
            initial_balance=self.initial_balance,
            contract_multiplier=self.contract_multiplier,
            equity_samples=equity_samples,
        )
        daily_return_metrics = utc_daily_return_metrics(
            equity_samples,
            initial_balance=Decimal(str(self.initial_balance)),
            start_time=self.start_time,
            end_time=self.end_time,
        )
        daily_return_moments: dict[str, Decimal | int] = {
            name: cast(Decimal | int, daily_return_metrics[name])
            for name in (
                "count",
                "sum",
                "sum_squares",
                "sum_cubes",
                "sum_fourth",
            )
        }
        return {
            "total_pnl": total_pnl,
            "mark_to_market_pnl": metrics.get(
                "mark_to_market_pnl",
                total_pnl,
            ),
            "max_drawdown": metrics.get("max_drawdown", Decimal("0")),
            "win_rate": metrics.get("win_rate", 0.0),
            "total_trades": int(metrics.get("total_trades", 0)),
            "closed_trade_count": int(metrics.get("closed_trade_count", 0)),
            "trade_sharpe": metrics.get("trade_sharpe", Decimal("0")),
            "profit_factor": metrics.get("profit_factor", Decimal("0")),
            "sortino_ratio": metrics.get("sortino_ratio", Decimal("0")),
            "calmar_ratio": metrics.get("calmar_ratio", Decimal("0")),
            "max_drawdown_days": metrics.get("max_drawdown_days", Decimal("0")),
            "avg_hold_time_hours": metrics.get("avg_hold_time_hours", Decimal("0")),
            "max_consecutive_wins": int(metrics.get("max_consecutive_wins", 0)),
            "max_consecutive_losses": int(metrics.get("max_consecutive_losses", 0)),
            "monthly_returns": metrics.get("monthly_returns", {}),
            "daily_return_moments": daily_return_moments,
            "equity_sample_count": daily_return_metrics["equity_sample_count"],
            "yearly_mark_to_market_returns": daily_return_metrics[
                "yearly_returns"
            ],
            "annualized_sharpe": annualized_sharpe_from_moments(
                daily_return_moments
            ),
            "trade_pnl_quality": metrics.get("trade_sharpe", Decimal("0")),
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

    def _iter_replay_candles(self, adapter: SimulatedAdapter):
        candles = self._iter_candles()
        if self.precision_codec is None:
            for candle in candles:
                yield candle, None
            return

        rows = list(candles)
        prepared = self.prepared_scaled_candles
        if prepared is None:
            prepared = [adapter.prepare_scaled_candle(candle) for candle in rows]
        if len(prepared) != len(rows):
            raise ValueError("prepared_scaled_candles length must match replay candles")
        for candle, prepared_candle in zip(rows, prepared):
            self._validate_prepared_scaled_candle(candle, prepared_candle)
            yield candle, prepared_candle

    @staticmethod
    def prepare_scaled_candles(
        candles: Iterable[Candlestick],
        precision_codec: PrecisionCodec,
    ) -> list[Any]:
        adapter = SimulatedAdapter(precision_codec=precision_codec)
        return [adapter.prepare_scaled_candle(candle) for candle in candles]

    @staticmethod
    def _validate_prepared_scaled_candle(candle: Candlestick, prepared_candle: Any) -> None:
        if getattr(prepared_candle, "product_id", None) != candle.product_id:
            raise ValueError("prepared_scaled_candles product_id must match replay candles")
        if getattr(prepared_candle, "timeframe", None) != candle.timeframe:
            raise ValueError("prepared_scaled_candles timeframe must match replay candles")
        if getattr(prepared_candle, "timestamp", None) != candle.timestamp:
            raise ValueError("prepared_scaled_candles timestamp must match replay candles")

    def _stop_drawdown_amount(self) -> Optional[Decimal]:
        if self.max_drawdown_limit is None:
            return None
        return Decimal(str(self.initial_balance)) * Decimal(str(self.max_drawdown_limit))

    def _strategy_context(
        self,
        *,
        adapter: SimulatedAdapter,
        strategy: BaseStrategy,
        candle: Candlestick,
        latest_fills: list[dict],
        peak_equity_by_strategy: dict[str, Decimal],
        max_drawdown_by_strategy: dict[str, Decimal],
    ) -> StrategyContext:
        strategy_id = strategy.strategy_id
        initial_balance = Decimal(str(self.initial_balance))
        latest_rejections = self._pop_latest_rejections(strategy_id)
        context = adapter.get_strategy_context(
            strategy_id=strategy_id,
            product_id=candle.product_id,
            timestamp=candle.timestamp,
            initial_balance=initial_balance,
            mark_price=candle.close,
            peak_equity=peak_equity_by_strategy[strategy_id],
            max_drawdown=max_drawdown_by_strategy[strategy_id],
            latest_fills=latest_fills,
            latest_rejections=latest_rejections,
            capital_allocator=self.capital_allocator,
        )

        peak_equity = max(peak_equity_by_strategy[strategy_id], context.total_equity)
        current_drawdown = max(peak_equity - context.total_equity, Decimal("0"))
        max_drawdown = max(max_drawdown_by_strategy[strategy_id], current_drawdown)
        peak_equity_by_strategy[strategy_id] = peak_equity
        max_drawdown_by_strategy[strategy_id] = max_drawdown

        if (
            context.current_drawdown == current_drawdown
            and context.max_drawdown == max_drawdown
        ):
            return context

        return replace(
            context,
            current_drawdown=current_drawdown,
            max_drawdown=max_drawdown,
        )

    def _sync_capital_usage(
        self,
        adapter: SimulatedAdapter,
        candle: Candlestick,
    ) -> None:
        if self.capital_allocator is None:
            return
        self._ensure_capital_allocator_supported(adapter)
        open_order_ids = {order.id for order in adapter.get_open_orders()}
        self._reserved_entry_capital = {
            order_id: reservation
            for order_id, reservation in self._reserved_entry_capital.items()
            if order_id in open_order_ids
        }
        for strategy in self._strategies:
            if strategy.product_id != candle.product_id:
                continue
            used = Decimal("0")
            if adapter.supports_strategy_positions:
                position = adapter.get_position(candle.product_id, strategy_id=strategy.strategy_id)
                if position is not None:
                    used = calculate_required_capital(
                        position.quantity,
                        candle.close,
                        self.instrument_spec,
                    )
            used += sum(
                reserved
                for strategy_id, reserved in self._reserved_entry_capital.values()
                if strategy_id == strategy.strategy_id
            )
            self.capital_allocator.set_usage(strategy.strategy_id, used)

    def _ensure_capital_allocator_supported(self, adapter: SimulatedAdapter) -> None:
        if self.capital_allocator is None:
            return
        if adapter.supports_strategy_positions:
            return
        raise RuntimeError(
            "capital_allocator requires a Rust engine with strategy-scoped positions"
        )

    def _signals_from_strategy(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> list[Signal]:
        if context is not None:
            result = strategy.on_candle(candle, context)
        else:
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

    def _capital_rejects_entry(
        self,
        signal: Signal,
        candle: Candlestick,
    ) -> bool:
        if self.capital_allocator is None:
            return False
        if signal.type not in (SignalType.LONG, SignalType.SHORT):
            return False

        required = self._entry_required_capital(signal, candle)
        available = self.capital_allocator.get_available(signal.strategy_id)
        if required <= available:
            return False

        logger.info(
            "Research entry rejected by capital allocation: strategy_id=%s "
            "required=%s available=%s",
            signal.strategy_id,
            required,
            available,
        )
        return True

    def _record_capital_rejection(self, signal: Signal, candle: Candlestick) -> None:
        if self.capital_allocator is None:
            return
        required = self._entry_required_capital(signal, candle)
        available = self.capital_allocator.get_available(signal.strategy_id)
        rejection = RejectionSnapshot(
            reason=(
                "capital_allocation_rejected: "
                f"required={required} available={available} strategy_id={signal.strategy_id}"
            ),
            timestamp=candle.timestamp,
        )
        existing = self._latest_rejections.get(signal.strategy_id, ())
        self._latest_rejections[signal.strategy_id] = existing + (rejection,)

    @staticmethod
    def _exit_without_position(signal: Signal, adapter: SimulatedAdapter) -> bool:
        if signal.type not in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            return False
        position = adapter.get_position(
            signal.product_id,
            strategy_id=signal.strategy_id,
        )
        if position is None:
            return True
        position_side = getattr(position.side, "value", position.side)
        if signal.type == SignalType.EXIT_LONG:
            return position_side != "LONG"
        return position_side != "SHORT"

    def _pop_latest_rejections(self, strategy_id: str) -> tuple[RejectionSnapshot, ...]:
        return self._latest_rejections.pop(strategy_id, ())

    def _reserve_entry_capital(
        self,
        signal: Signal,
        order: Order,
        candle: Candlestick,
    ) -> None:
        if self.capital_allocator is None:
            return
        if signal.type not in (SignalType.LONG, SignalType.SHORT):
            return

        required = self._entry_required_capital(signal, candle)
        self._reserved_entry_capital[order.id] = (signal.strategy_id, required)
        current_used = self.capital_allocator.get_used(signal.strategy_id)
        self.capital_allocator.set_usage(signal.strategy_id, current_used + required)

    def _order_from_signal(
        self,
        signal: Signal,
        candle: Candlestick,
        adapter: SimulatedAdapter,
    ) -> Optional[Order]:
        side = self._determine_side(signal.type)
        if side is None:
            return None

        quantity = self._quantity_for_signal(signal, adapter)
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

    def _quantity_for_signal(self, signal: Signal, adapter: SimulatedAdapter) -> Decimal:
        if signal.type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            position = self._position_for_exit_signal(signal, adapter)
            if position is not None and position.quantity > 0:
                requested_quantity = (
                    signal.quantity
                    if signal.quantity is not None and signal.quantity > 0
                    else position.quantity
                )
                return min(requested_quantity, position.quantity)
        if signal.quantity and signal.quantity > 0:
            return signal.quantity
        return Decimal("0.01")

    @staticmethod
    def _conditional_orders_from_signal(
        signal: Signal,
        entry_order: Order,
        candle: Candlestick,
    ) -> list[Order]:
        close_side = (
            OrderSide.SELL
            if entry_order.side.lower() == OrderSide.BUY.value
            else OrderSide.BUY
        )
        orders = []
        intents = conditional_order_intents(signal)
        for intent in intents:
            order_id = str(uuid.uuid4())
            order = Order(
                id=order_id,
                exchange_order_id=f"sim_{order_id[:8]}",
                strategy_id=signal.strategy_id,
                product_id=signal.product_id,
                exchange_id=signal.product_id.split(":")[0],
                type=intent.order_type,
                side=close_side,
                price=None,
                trigger_price=intent.trigger_price,
                quantity=entry_order.quantity,
                status="open",
                timestamp=candle.timestamp,
                filled_quantity=Decimal("0"),
                filled_price=Decimal("0"),
            )
            if intent.trailing_distance is not None:
                order._trailing_distance = intent.trailing_distance
            orders.append(order)

        for first_index, second_index in conditional_oco_pairs(intents):
            first = orders[first_index]
            second = orders[second_index]
            first._linked_order_id = second.id
            second._linked_order_id = first.id
        return orders

    def _position_for_exit_signal(self, signal: Signal, adapter: SimulatedAdapter):
        position = adapter.get_position(
            signal.product_id,
            strategy_id=signal.strategy_id,
        )
        if position is None:
            return None
        position_side = getattr(position.side, "value", position.side)
        if signal.type == SignalType.EXIT_LONG and position_side == "LONG":
            return position
        if signal.type == SignalType.EXIT_SHORT and position_side == "SHORT":
            return position
        return None

    def _entry_required_capital(self, signal: Signal, candle: Candlestick) -> Decimal:
        quantity = signal.quantity if signal.quantity and signal.quantity > 0 else Decimal("0.01")
        price = self._signal_execution_price(signal, candle)
        return calculate_required_capital(quantity, price, self.instrument_spec)

    @staticmethod
    def _signal_execution_price(signal: Signal, candle: Candlestick) -> Decimal:
        if signal.price and signal.price > 0:
            return signal.price
        if signal.value and signal.value > 0:
            return signal.value
        return candle.close

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


def _strategy_accepts_context(strategy: BaseStrategy) -> bool:
    signature = inspect.signature(strategy.on_candle)
    parameters = list(signature.parameters.values())
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters):
        return True
    if "context" in signature.parameters:
        return True
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    return len(positional) >= 2
