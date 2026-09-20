"""Fast in-memory backtest path for parameter search workloads.

This runner intentionally bypasses persistence, signal audit, journal export,
and report generation. It keeps the same candle ordering and Rust matching
adapter as the full BacktestRunner so research-mode results can be compared
against the production replay path.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Sequence, cast

from src.core.adapters.simulated import (
    SimulatedAdapter,
    resolve_market_slippage_configuration,
)
from src.core.analytics import (
    InitialBalanceInput,
    annualized_sharpe_from_moments,
    calculate_metrics,
    utc_daily_return_metrics,
)
from src.core.backtest.endpoint_state import build_replay_endpoint_state
from src.core.backtest.equity import require_strategy_position_scope
from src.core.backtest.external_funding import (
    ExternalFundingEvent,
    ExternalFundingTimeline,
    build_external_funding_timeline,
)
from src.core.backtest.flow_neutral_performance import (
    FlowNeutralPerformanceTracker,
    return_metric_inputs,
)
from src.core.backtest.run_evidence import (
    BacktestDatasetEvidence,
    build_backtest_run_provenance,
    canonical_decision_snapshot,
    canonical_fill_records,
    strategy_configuration_contract,
)
from src.core.backtest.loader import get_candles_generator
from src.core.clock import BacktestClock
from src.core.conditional_order_intents import (
    conditional_oco_pairs,
    conditional_order_intents,
)
from src.core.db import SessionLocal
from src.core.interfaces.data_source import IDataSource
from src.core.interfaces.exchange import ExchangeError
from src.core.models import Candlestick, OrderSide, Signal, SignalType
from src.core.orm_models import Order
from src.core.precision import PrecisionCodec
from src.core.product_registry import (
    InstrumentSpec,
    MarketType,
    calculate_required_capital,
    resolve_contract_multiplier,
)
from src.core.signal_order_intent import (
    InvalidSignalOrderIntent,
    normalize_signal_quantity,
    resolve_signal_order_intent,
)
from src.core.strategy_context import RejectionSnapshot, StrategyContext
from src.core.signal_processor import (
    StrategyContextInvocationMode,
    apply_strategy_position_state,
    invoke_strategy_on_candle,
    strategy_context_invocation_mode,
)
from src.strategies.base import BaseStrategy

if TYPE_CHECKING:
    from src.core.capital_allocator import CapitalAllocator

logger = logging.getLogger(__name__)
_DEFAULT_ENTRY_QUANTITY = Decimal("0.01")


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
    fee_asset: str | None = None
    fill_sequence: int | None = None
    reference_price: Decimal | None = None
    slippage_per_unit: Decimal | None = None
    slippage_cost: Decimal | None = None


@dataclass(frozen=True, slots=True)
class InvalidOrderIntentRejection:
    """Durable invalid-intent diagnostic with strategy ownership."""

    strategy_id: str
    product_id: str
    reason: str
    timestamp: int


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
        initial_balance: InitialBalanceInput = Decimal("10000"),
        data_source: Optional[IDataSource] = None,
        fee_config: Mapping[str, Decimal | float] | None = None,
        max_drawdown_limit: Optional[float] = None,
        balance_check_interval: int = 0,
        precision_codec: PrecisionCodec | None = None,
        prepared_scaled_candles: Sequence[Any] | None = None,
        capital_allocator: CapitalAllocator | None = None,
        instrument_spec: InstrumentSpec | None = None,
        spot_fee_asset: str = "quote",
        external_funding_events: Sequence[ExternalFundingEvent] = (),
        external_funding_account_id: str | None = None,
        market_slippage_bps: Decimal = Decimal("0"),
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        self.initial_balance = initial_balance
        self.data_source = data_source
        self.fee_config = fee_config or {}
        self.instrument_spec = instrument_spec
        self.market_slippage_bps, _ = resolve_market_slippage_configuration(
            market_slippage_bps,
            instrument_spec=instrument_spec,
            precision_codec=precision_codec,
        )
        self.max_drawdown_limit = max_drawdown_limit
        self.balance_check_interval = balance_check_interval
        self.precision_codec = precision_codec
        self.prepared_scaled_candles = prepared_scaled_candles
        self.capital_allocator = capital_allocator
        self.spot_fee_asset = spot_fee_asset
        self.external_funding_events = tuple(external_funding_events)
        self.external_funding_account_id = external_funding_account_id
        self.contract_multiplier = resolve_contract_multiplier(instrument_spec)
        self._reserved_entry_capital: dict[str, tuple[str, Decimal]] = {}
        self._latest_rejections: dict[str, tuple[RejectionSnapshot, ...]] = {}
        self._invalid_order_intent_rejections: list[InvalidOrderIntentRejection] = []
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
        self._invalid_order_intent_rejections = []
        funding_timeline = build_external_funding_timeline(
            self.external_funding_events,
            account_id=self.external_funding_account_id,
            quote_asset=(
                self.instrument_spec.quote
                if self.instrument_spec is not None
                and self.instrument_spec.market_type == MarketType.SPOT
                else None
            ),
            start_time=self.start_time,
        )
        adapter = SimulatedAdapter(
            initial_balance=Decimal(str(self.initial_balance)),
            maker_fee=Decimal(str(self.fee_config.get("maker", 0))),
            taker_fee=Decimal(str(self.fee_config.get("taker", 0))),
            precision_codec=self.precision_codec,
            instrument_spec=self.instrument_spec,
            spot_fee_asset=self.spot_fee_asset,
            external_funding_timeline=funding_timeline,
            market_slippage_bps=self.market_slippage_bps,
        )
        performance_tracker = (
            FlowNeutralPerformanceTracker(Decimal(str(self.initial_balance)))
            if adapter.is_cash_spot_settlement
            else None
        )
        self._ensure_capital_allocator_supported(adapter)
        trades: list[ResearchTrade] = []
        stop_drawdown_amount = self._stop_drawdown_amount()
        context_invocation_modes = {
            id(strategy): strategy_context_invocation_mode(strategy)
            for strategy in self._strategies
        }
        initial_equity = Decimal(str(self.initial_balance))
        peak_equity_by_strategy = {
            strategy.strategy_id: initial_equity for strategy in self._strategies
        }
        max_drawdown_by_strategy = {
            strategy.strategy_id: Decimal("0") for strategy in self._strategies
        }
        portfolio_peak_equity = initial_equity
        portfolio_max_drawdown = Decimal("0")
        equity_samples: list[tuple[int, Decimal]] = []
        observed_position_sides: dict[tuple[str, str], str | None] = {}
        final_mark: Decimal | None = None
        end_timestamp: int | None = None
        halted_early = False
        recorded_funding_count = 0
        configuration_contract = {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "product_id": self.product_id,
            "timeframe": self.timeframe,
            "initial_balance": Decimal(str(self.initial_balance)),
            "max_drawdown_limit": self.max_drawdown_limit,
            "fee_config": self.fee_config,
            "market_slippage_bps": self.market_slippage_bps,
            "instrument_spec": self.instrument_spec,
            "spot_fee_asset": self.spot_fee_asset,
            "external_funding_events": self.external_funding_events,
            "external_funding_account_id": self.external_funding_account_id,
            "strategies": strategy_configuration_contract(self._strategies),
        }
        runner_configuration_contract = {
            **configuration_contract,
            "runner_kind": "research",
            "balance_check_interval": self.balance_check_interval,
            "prepared_scaled_candles": self.prepared_scaled_candles is not None,
            "precision_codec": self.precision_codec,
            "capital_allocator": (
                None
                if self.capital_allocator is None
                else {
                    "total_balance": self.capital_allocator.total_balance,
                    "allocations": {
                        strategy.strategy_id: self.capital_allocator.get_allocation(
                            strategy.strategy_id
                        )
                        for strategy in self._strategies
                    },
                    "used": {
                        strategy.strategy_id: self.capital_allocator.get_used(
                            strategy.strategy_id
                        )
                        for strategy in self._strategies
                    },
                }
            ),
        }
        dataset_evidence = BacktestDatasetEvidence()
        decision_snapshots: list[dict[str, object]] = []

        candle_count = 0
        for candle, prepared_candle in self._iter_replay_candles(adapter):
            dataset_evidence.observe(candle)
            self.clock.set_time(candle.timestamp / 1000)

            if prepared_candle is None:
                fills = adapter.on_market_data(candle)
            else:
                fills = adapter.on_prepared_market_data(prepared_candle)
            trades.extend(self._fills_to_trades(fills, candle))
            for rejection in adapter.drain_order_rejections():
                order = rejection["order"]
                snapshot = RejectionSnapshot(
                    reason=rejection["reason"],
                    timestamp=rejection["timestamp"],
                    order_id=order.id,
                )
                existing = self._latest_rejections.get(order.strategy_id, ())
                self._latest_rejections[order.strategy_id] = existing + (snapshot,)
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
                decision_snapshots.append(canonical_decision_snapshot(context))
                contexts.append(context)
                signals = self._signals_from_strategy(
                    strategy,
                    candle,
                    context,
                    context_invocation_modes[id(strategy)],
                )
                for signal in signals:
                    if signal.type == SignalType.NO_SIGNAL:
                        continue
                    try:
                        signal = normalize_signal_quantity(
                            signal,
                            default_entry_quantity=_DEFAULT_ENTRY_QUANTITY,
                        )
                        resolve_signal_order_intent(signal)
                    except InvalidSignalOrderIntent as exc:
                        self._record_invalid_order_intent(signal, candle, str(exc))
                        continue
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
                        placed_orders: list[Order] = []
                        try:
                            for order in orders:
                                adapter.validate_order(order)
                            for order in orders:
                                adapter.place_order(order)
                                placed_orders.append(order)
                        except ExchangeError as exc:
                            for placed_order in placed_orders:
                                if placed_order.exchange_order_id is not None:
                                    adapter.cancel_order(
                                        placed_order.exchange_order_id,
                                        placed_order.product_id,
                                        order_type=placed_order.type,
                                    )
                            self._record_rejection(signal, candle, str(exc))
                            continue
                        self._reserve_entry_capital(signal, entry_order, candle)

            portfolio_current_equity = (
                adapter.get_total_equity(candle.close)
                if adapter.is_cash_spot_settlement
                else (
                    contexts[0].available_cash
                    + sum(
                        (context.unrealized_pnl for context in contexts),
                        start=Decimal("0"),
                    )
                    if contexts
                    else adapter.get_balance()
                )
            )
            if performance_tracker is not None:
                if funding_timeline is not None:
                    applications = funding_timeline.applications_since(
                        recorded_funding_count
                    )
                    for application in applications:
                        performance_tracker.record_funding(application)
                    recorded_funding_count += len(applications)
                performance_tracker.observe(
                    timestamp=candle.timestamp,
                    equity=portfolio_current_equity,
                )
            equity_samples.append((candle.timestamp, portfolio_current_equity))
            final_mark = candle.close
            end_timestamp = candle.timestamp
            portfolio_peak_equity = max(
                portfolio_peak_equity,
                portfolio_current_equity,
            )
            portfolio_max_drawdown = max(
                portfolio_max_drawdown,
                portfolio_peak_equity - portfolio_current_equity,
            )
            candle_count += 1
            flow_drawdown_reached = (
                performance_tracker is not None
                and self.max_drawdown_limit is not None
                and performance_tracker.unitized_max_drawdown
                >= Decimal(str(self.max_drawdown_limit))
            )
            raw_drawdown_reached = (
                performance_tracker is None
                and stop_drawdown_amount is not None
                and portfolio_max_drawdown >= stop_drawdown_amount
            )
            if (
                self.balance_check_interval > 0
                and candle_count % self.balance_check_interval == 0
                and (flow_drawdown_reached or raw_drawdown_reached)
            ):
                logger.warning("Stopping research backtest at drawdown threshold")
                halted_early = True
                break

        endpoint_state = build_replay_endpoint_state(
            positions=adapter.get_all_positions(),
            working_orders=adapter.get_matching_open_orders(),
            final_mark=final_mark,
            end_timestamp=end_timestamp,
            halted_early=halted_early,
        )
        flow_neutral_performance = (
            performance_tracker.report() if performance_tracker is not None else None
        )
        cash_spot_account_snapshot = (
            adapter.get_cash_spot_account_snapshot(final_mark)
            if adapter.is_cash_spot_settlement and final_mark is not None
            else None
        )
        final_balance = adapter.get_balance()
        final_equity = (
            adapter.get_total_equity(final_mark)
            if adapter.is_cash_spot_settlement and final_mark is not None
            else final_balance
        )
        total_pnl = (
            flow_neutral_performance.net_pnl
            if flow_neutral_performance is not None
            and flow_neutral_performance.net_pnl is not None
            else final_equity - Decimal(str(self.initial_balance))
        )
        metrics = calculate_metrics(
            trades,
            initial_balance=self.initial_balance,
            contract_multiplier=self.contract_multiplier,
            equity_samples=equity_samples,
            spot_base_asset=(
                self.instrument_spec.base
                if adapter.is_cash_spot_settlement and self.instrument_spec is not None
                else None
            ),
            spot_quote_asset=(
                self.instrument_spec.quote
                if adapter.is_cash_spot_settlement and self.instrument_spec is not None
                else None
            ),
        )
        fill_records = canonical_fill_records(trades)
        if flow_neutral_performance is not None:
            metrics.update(flow_neutral_performance.metric_fields())
            if flow_neutral_performance.net_pnl is not None:
                metrics["total_pnl"] = flow_neutral_performance.net_pnl
                metrics["mark_to_market_pnl"] = flow_neutral_performance.net_pnl
        (
            return_samples,
            return_initial,
            return_start_time,
            return_end_time,
        ) = return_metric_inputs(
            equity_samples,
            Decimal(str(self.initial_balance)),
            self.start_time,
            self.end_time,
            flow_neutral_performance,
        )
        daily_return_metrics = utc_daily_return_metrics(
            return_samples,
            initial_balance=return_initial,
            start_time=return_start_time,
            end_time=return_end_time,
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
        result = {
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
            "yearly_mark_to_market_returns": daily_return_metrics["yearly_returns"],
            "annualized_sharpe": annualized_sharpe_from_moments(daily_return_moments),
            "trade_pnl_quality": metrics.get("trade_sharpe", Decimal("0")),
            "closed_trades": metrics.get("closed_trades", []),
            "raw_trades": trades,
            "raw_trade_count": len(trades),
            "candle_count": candle_count,
            "invalid_order_intent_count": len(self._invalid_order_intent_rejections),
            "invalid_order_intent_rejections": tuple(
                self._invalid_order_intent_rejections
            ),
            "report_dir": None,
            "endpoint_state": endpoint_state,
            "cash_spot_account_snapshot": cash_spot_account_snapshot,
            "flow_neutral_performance": flow_neutral_performance,
            "external_funding_applications": (
                funding_timeline.applications if funding_timeline is not None else ()
            ),
            "external_funding_checkpoint": (
                funding_timeline.checkpoint() if funding_timeline is not None else None
            ),
            "yearly_time_weighted_returns": daily_return_metrics["yearly_returns"],
            "fill_records": fill_records,
            "decision_snapshots": tuple(decision_snapshots),
            "provenance": build_backtest_run_provenance(
                runner_kind="research",
                dataset=dataset_evidence,
                configuration=configuration_contract,
                runner_configuration=runner_configuration_contract,
                program_components=(
                    type(self),
                    SimulatedAdapter,
                    ExternalFundingTimeline,
                    FlowNeutralPerformanceTracker,
                    *(type(strategy) for strategy in self._strategies),
                ),
            ),
        }
        if flow_neutral_performance is not None:
            result.update(flow_neutral_performance.metric_fields())
        return result

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
    def _validate_prepared_scaled_candle(
        candle: Candlestick, prepared_candle: Any
    ) -> None:
        if getattr(prepared_candle, "product_id", None) != candle.product_id:
            raise ValueError(
                "prepared_scaled_candles product_id must match replay candles"
            )
        if getattr(prepared_candle, "timeframe", None) != candle.timeframe:
            raise ValueError(
                "prepared_scaled_candles timeframe must match replay candles"
            )
        if getattr(prepared_candle, "timestamp", None) != candle.timestamp:
            raise ValueError(
                "prepared_scaled_candles timestamp must match replay candles"
            )

    def _stop_drawdown_amount(self) -> Optional[Decimal]:
        if self.max_drawdown_limit is None:
            return None
        return Decimal(str(self.initial_balance)) * Decimal(
            str(self.max_drawdown_limit)
        )

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
                position = adapter.get_position(
                    candle.product_id, strategy_id=strategy.strategy_id
                )
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
        invocation_mode: StrategyContextInvocationMode | None = None,
    ) -> list[Signal]:
        result = invoke_strategy_on_candle(
            strategy,
            candle,
            context,
            invocation_mode,
        )
        if result is None:
            return []
        if isinstance(result, Signal):
            signals = [result]
        elif isinstance(result, list):
            signals = result
        else:
            raise TypeError(
                "strategy.on_candle() must return None, Signal, or list[Signal]"
            )
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
        self._record_rejection(
            signal,
            candle,
            (
                "capital_allocation_rejected: "
                f"required={required} available={available} strategy_id={signal.strategy_id}"
            ),
        )

    def _record_rejection(
        self,
        signal: Signal,
        candle: Candlestick,
        reason: str,
    ) -> None:
        rejection = RejectionSnapshot(reason=reason, timestamp=candle.timestamp)
        existing = self._latest_rejections.get(signal.strategy_id, ())
        self._latest_rejections[signal.strategy_id] = existing + (rejection,)

    def _record_invalid_order_intent(
        self,
        signal: Signal,
        candle: Candlestick,
        reason: str,
    ) -> None:
        logger.warning(
            "Research signal order intent rejected: strategy=%s product=%s reason=%s",
            signal.strategy_id,
            signal.product_id,
            reason,
        )
        self._record_rejection(signal, candle, reason)
        self._invalid_order_intent_rejections.append(
            InvalidOrderIntentRejection(
                strategy_id=signal.strategy_id,
                product_id=signal.product_id,
                reason=reason,
                timestamp=candle.timestamp,
            )
        )

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
        resolved_intent = resolve_signal_order_intent(signal)

        order_id = str(uuid.uuid4())
        order = Order(
            id=order_id,
            exchange_order_id=f"sim_{order_id[:8]}",
            strategy_id=signal.strategy_id,
            product_id=signal.product_id,
            exchange_id=signal.product_id.split(":")[0],
            type=resolved_intent.order_type,
            side=side,
            price=resolved_intent.limit_price,
            trigger_price=None,
            quantity=quantity,
            status="open",
            timestamp=candle.timestamp,
            filled_quantity=Decimal("0"),
            filled_price=Decimal("0"),
        )
        if resolved_intent.order_type == "market":
            order.min_notional_reference_price = candle.close
        return order

    def _quantity_for_signal(
        self, signal: Signal, adapter: SimulatedAdapter
    ) -> Decimal:
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
        return _DEFAULT_ENTRY_QUANTITY

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
        quantity = (
            signal.quantity
            if signal.quantity is not None and signal.quantity > 0
            else _DEFAULT_ENTRY_QUANTITY
        )
        price = self._signal_execution_price(signal, candle)
        return calculate_required_capital(quantity, price, self.instrument_spec)

    @staticmethod
    def _signal_execution_price(signal: Signal, candle: Candlestick) -> Decimal:
        resolved_intent = resolve_signal_order_intent(signal)
        if resolved_intent.limit_price is not None:
            return resolved_intent.limit_price
        return candle.close

    def _fills_to_trades(
        self, fills: list[dict], candle: Candlestick
    ) -> list[ResearchTrade]:
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
                    fee=fill.get("fee_quantity", fill.get("fee")) or Decimal("0"),
                    timestamp=candle.timestamp,
                    fee_asset=fill.get("fee_asset"),
                    reference_price=fill.get("reference_price"),
                    slippage_per_unit=fill.get("slippage_per_unit"),
                    slippage_cost=fill.get("slippage_cost"),
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
