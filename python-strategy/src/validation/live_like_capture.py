"""Validation-only live-like projection into the canonical TradingOutcome."""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import ContextManager, Protocol, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.adapters.simulated import SimulatedAdapter
from src.core.backtest.endpoint_state import build_replay_endpoint_state
from src.core.clock import BacktestClock
from src.core.engine import StrategyEngine
from src.core.journal import StrategyJournal
from src.core.models import Candlestick, OrderSide, Signal, Trade as MarketTrade
from src.core.orm_models import Order, Trade
from src.validation.backtest_capture import (
    build_normal_backtest_trading_outcome,
    capture_signal_batch,
    exact_decimal_subtract,
)
from src.validation.trading_outcome import SignalObservation, TradingOutcome

__all__ = [
    "LiveLikeOutcomeCapture",
    "LiveLikeOutcomeCaptureError",
]


class LiveLikeOutcomeCaptureError(RuntimeError):
    """Safe outer failure for the bounded live-like projection."""

    stage = "live_like_outcome_capture"


class _CaptureConsumer(Protocol):
    def acquire_service_ownership(self) -> None: ...

    def start(self) -> None: ...

    def request_stop(self) -> None: ...

    def assert_no_unresolved_deliveries(self) -> None: ...

    def cleanup_consumer_group(self) -> None: ...

    def stop(self) -> None: ...


ConsumerFactory = Callable[..., _CaptureConsumer]
SessionFactory = Callable[[], ContextManager[Session]]


class LiveLikeOutcomeCapture:
    """Observe one deterministic consumer run without owning trading state."""

    def __init__(
        self,
        *,
        strategy_id: str,
        product_id: str,
        initial_balance: Decimal,
        expected_deliveries: int,
        completion_timeout_seconds: float = 5.0,
        shutdown_timeout_seconds: float = 5.0,
    ) -> None:
        if type(strategy_id) is not str or not strategy_id:
            raise ValueError("strategy_id must be a non-empty exact string")
        if type(product_id) is not str or not product_id:
            raise ValueError("product_id must be a non-empty exact string")
        if (
            type(initial_balance) is not Decimal
            or not initial_balance.is_finite()
            or initial_balance <= 0
        ):
            raise ValueError("initial_balance must be a positive finite Decimal")
        if type(expected_deliveries) is not int or expected_deliveries <= 0:
            raise ValueError("expected_deliveries must be a positive exact integer")
        if completion_timeout_seconds <= 0 or shutdown_timeout_seconds <= 0:
            raise ValueError("capture timeouts must be positive")

        self.strategy_id = strategy_id
        self.product_id = product_id
        self.initial_balance = initial_balance
        self.expected_deliveries = expected_deliveries
        self.completion_timeout_seconds = completion_timeout_seconds
        self.shutdown_timeout_seconds = shutdown_timeout_seconds
        self.journal = StrategyJournal(strategy_id)
        self._signals: list[SignalObservation] = []
        self._processed_deliveries = 0
        self._last_candle: Candlestick | None = None
        self._first_callback_failure: Exception | None = None
        self._wakeup = threading.Event()
        self._started = False

    @property
    def processed_deliveries(self) -> int:
        return self._processed_deliveries

    @property
    def observed_signals(self) -> tuple[SignalObservation, ...]:
        return tuple(self._signals)

    def observe_signal_batch(self, batch: tuple[Signal, ...]) -> None:
        """Freeze finalized signals at the existing observer boundary."""
        self._signals.extend(capture_signal_batch(batch))

    def run(
        self,
        *,
        engine: StrategyEngine,
        adapter: SimulatedAdapter,
        clock: BacktestClock,
        session_factory: SessionFactory,
        channels: Sequence[str],
        group_name: str,
        consumer_factory: ConsumerFactory,
    ) -> TradingOutcome:
        """Consume the bounded input, cleanly close it, and project one outcome."""
        if self._started:
            raise LiveLikeOutcomeCaptureError("live-like outcome capture failed")
        if (
            engine.clock is not clock
            or engine.execution_engine.adapter is not adapter
            or engine.execution_engine.journal is not self.journal
            or engine._db_session_factory is not session_factory
        ):
            raise LiveLikeOutcomeCaptureError("live-like outcome capture failed")
        self._started = True

        consumer: _CaptureConsumer | None = None
        thread: threading.Thread | None = None
        thread_failures: list[BaseException] = []
        primary: Exception | None = None
        outcome: TradingOutcome | None = None
        ephemeral_cleanup_eligible = False

        def apply(model: Candlestick | MarketTrade) -> None:
            try:
                if type(model) is not Candlestick:
                    raise TypeError("live-like capture accepts Candlestick values only")
                if model.product_id != self.product_id:
                    raise ValueError("live-like candle product does not match capture")
                clock.set_time(model.timestamp / 1_000)
                engine.on_market_data(model)
                self._processed_deliveries += 1
                self._last_candle = model
                if self._processed_deliveries > self.expected_deliveries:
                    raise ValueError("live-like capture received excess deliveries")
                if self._processed_deliveries == self.expected_deliveries:
                    self._wakeup.set()
            except Exception as error:
                if self._first_callback_failure is None:
                    self._first_callback_failure = error
                self._wakeup.set()
                raise

        def consume() -> None:
            try:
                assert consumer is not None
                consumer.start()
            except BaseException as error:
                thread_failures.append(error)
            finally:
                self._wakeup.set()

        try:
            consumer = consumer_factory(
                channels=list(channels),
                on_message_callback=apply,
                pending_replay_callback=None,
                runtime_environment=engine.runtime_environment,
                group_name=group_name,
                ephemeral_group=True,
            )
            consumer.acquire_service_ownership()
            thread = threading.Thread(
                target=consume,
                name=f"live-like-capture-{group_name}",
                daemon=False,
            )
            thread.start()
            if not self._wakeup.wait(self.completion_timeout_seconds):
                raise TimeoutError("live-like consumer did not complete in time")
            consumer.request_stop()
            thread.join(timeout=self.shutdown_timeout_seconds)
            if thread.is_alive():
                raise TimeoutError("live-like consumer did not stop in time")
            if self._first_callback_failure is not None:
                raise self._first_callback_failure
            if thread_failures:
                failure = thread_failures[0]
                if isinstance(failure, Exception):
                    raise failure
                raise RuntimeError("live-like consumer terminated") from failure
            if self._processed_deliveries != self.expected_deliveries:
                raise ValueError("live-like delivery count is incomplete")
            consumer.assert_no_unresolved_deliveries()
            ephemeral_cleanup_eligible = True
            outcome = self._build_outcome(
                adapter=adapter,
                session_factory=session_factory,
            )
        except Exception as error:
            primary = error
        finally:
            secondary: list[Exception] = []
            if consumer is not None:
                try:
                    consumer.request_stop()
                except Exception as error:
                    secondary.append(error)
            if thread is not None and thread.is_alive():
                thread.join(timeout=self.shutdown_timeout_seconds)
                if thread.is_alive():
                    secondary.append(
                        TimeoutError(
                            "live-like consumer remained active during cleanup"
                        )
                    )
            if consumer is not None and ephemeral_cleanup_eligible:
                try:
                    consumer.cleanup_consumer_group()
                except Exception as error:
                    secondary.append(error)
            if consumer is not None:
                try:
                    consumer.stop()
                except Exception as error:
                    secondary.append(error)
            try:
                engine.shutdown()
            except Exception as error:
                secondary.append(error)
            if primary is None and secondary:
                primary = secondary.pop(0)
            if primary is not None:
                for error in secondary:
                    primary.add_note(
                        f"secondary live-like cleanup failure: {type(error).__name__}"
                    )

        if primary is not None:
            if isinstance(primary, LiveLikeOutcomeCaptureError):
                raise primary
            outer = LiveLikeOutcomeCaptureError("live-like outcome capture failed")
            for note in getattr(primary, "__notes__", ()):
                outer.add_note(note)
            raise outer from primary
        assert outcome is not None
        return outcome

    def _build_outcome(
        self,
        *,
        adapter: SimulatedAdapter,
        session_factory: SessionFactory,
    ) -> TradingOutcome:
        last_candle = self._last_candle
        if last_candle is None:
            raise ValueError("live-like capture has no completed candle")
        journal = self._project_journal(tuple(self.journal.to_dicts()))
        fills = self._project_persisted_fills(
            journal=journal,
            session_factory=session_factory,
        )
        endpoint_state = build_replay_endpoint_state(
            positions=adapter.get_all_positions(),
            working_orders=adapter.get_matching_open_orders(),
            final_mark=last_candle.close,
            end_timestamp=last_candle.timestamp,
        )
        total_pnl = exact_decimal_subtract(
            adapter.get_balance(),
            self.initial_balance,
        )
        return build_normal_backtest_trading_outcome(
            signals=tuple(self._signals),
            fills=fills,
            journal=journal,
            endpoint_state=endpoint_state,
            initial_balance=self.initial_balance,
            total_pnl=total_pnl,
        )

    @staticmethod
    def _project_journal(
        journal: tuple[dict[str, object], ...],
    ) -> tuple[dict[str, object], ...]:
        projected: list[dict[str, object]] = []
        for row in journal:
            if type(row) is not dict or type(row.get("data")) is not dict:
                raise ValueError("live-like journal rows must be exact dictionaries")
            copied = dict(row)
            data = dict(cast(dict[str, object], row["data"]))
            side = data.get("side")
            if copied.get("tag") == "fill" and type(side) is str:
                try:
                    data["side"] = OrderSide(side)
                except ValueError as error:
                    raise ValueError(
                        "live-like fill journal side is invalid"
                    ) from error
            copied["data"] = data
            projected.append(copied)
        return tuple(projected)

    @staticmethod
    def _project_persisted_fills(
        *,
        journal: tuple[dict[str, object], ...],
        session_factory: SessionFactory,
    ) -> tuple[dict[str, object], ...]:
        if len(journal) % 2:
            raise ValueError("live-like journal must contain entry/fill pairs")
        with session_factory() as session:
            orders = tuple(session.scalars(select(Order)).all())
            trades = tuple(session.scalars(select(Trade)).all())
        orders_by_id = {str(order.id): order for order in orders}
        if len(orders_by_id) != len(orders):
            raise ValueError("live-like persisted order identity is duplicated")
        trades_by_order: dict[str, list[Trade]] = {}
        for trade in trades:
            trades_by_order.setdefault(str(trade.order_id), []).append(trade)

        projected: list[dict[str, object]] = []
        used_trade_ids: set[str] = set()
        used_order_ids: set[str] = set()
        for sequence, row in enumerate(journal[1::2]):
            if type(row) is not dict or row.get("tag") != "fill":
                raise ValueError("live-like journal must alternate entry and fill")
            order_id = row.get("trade_id")
            if type(order_id) is not str or not order_id:
                raise ValueError("live-like fill journal requires an order identity")
            order = orders_by_id.get(order_id)
            matching = trades_by_order.get(order_id, [])
            if order is None or len(matching) != 1:
                raise ValueError(
                    "live-like journal fill must match one persisted order and trade"
                )
            trade = matching[0]
            trade_id = str(trade.id)
            if trade_id in used_trade_ids or order_id in used_order_ids:
                raise ValueError("live-like persisted fill identity is duplicated")
            used_trade_ids.add(trade_id)
            used_order_ids.add(order_id)
            projected.append(
                {
                    "id": trade_id,
                    "strategy_id": str(order.strategy_id),
                    "order_id": order_id,
                    "exchange_trade_id": (
                        None
                        if trade.exchange_trade_id is None
                        else str(trade.exchange_trade_id)
                    ),
                    "product_id": str(trade.product_id),
                    "side": str(trade.side),
                    "price": cast(Decimal, trade.price),
                    "quantity": cast(Decimal, trade.quantity),
                    "fee": cast(Decimal, trade.fee),
                    "fee_asset": (
                        None if trade.fee_asset is None else str(trade.fee_asset)
                    ),
                    "timestamp": int(cast(int, trade.timestamp)),
                    "fill_sequence": sequence,
                }
            )
        if len(used_trade_ids) != len(trades) or len(used_order_ids) != len(orders):
            raise ValueError("live-like persistence contains unpaired orders or trades")
        return tuple(projected)
