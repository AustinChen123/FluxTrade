import logging
import time as _time
from decimal import Decimal
from typing import Callable, ContextManager, Optional
from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType, Candlestick, OrderSide
from src.core.order_manager import OrderManager
from src.core.interfaces.exchange import IExchangeAdapter, ExchangeError
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository
from src.core.journal import StrategyJournal
from src.core.metrics import ORDERS_TOTAL, EXECUTION_LATENCY
from src.core.audit_service import (
    build_signal_intent_audit,
    write_signal_audit_intent,
    write_signal_audit_outcome,
)
from src.core.client_order_id import generate_client_order_id

class ExecutionEngine:
    def __init__(
        self,
        db_session: Session,
        clock: Clock,
        adapter: IExchangeAdapter,
        order_repository: Optional[IOrderRepository] = None,
        journal: Optional[StrategyJournal] = None,
        is_backtest: Optional[bool] = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        audit_external_orders: bool = False,
    ):
        self.logger = logging.getLogger("ExecutionEngine")
        self.clock = clock
        self._db_session_factory = db_session_factory
        self.audit_external_orders = audit_external_orders
        if order_repository:
            self.order_manager = OrderManager(order_repository, clock, is_backtest=is_backtest)
        else:
            from src.core.repositories import LiveOrderRepository
            self.order_manager = OrderManager(
                LiveOrderRepository(db_session, db_session_factory=db_session_factory),
                clock,
                is_backtest=is_backtest,
            )

        self.default_quantity = Decimal("0.01")
        self.adapter = adapter
        self.journal = journal
        self.logger.info("ExecutionEngine initialized with adapter: %s", type(adapter).__name__)

    def process_market_data(self, candle: Candlestick):
        """
        Passes market data to the adapter (if applicable) to check for simulated fills.
        """
        fills = self.adapter.on_market_data(candle)

        if fills:
            for fill in fills:
                order = fill['order']
                price = fill['price']
                qty = fill['quantity']
                fee = fill.get('fee')
                fill_type = fill.get('fill_type', 'MARKET')

                self.logger.info("Execution: Adapter fill for %s at %s (fee=%s)", order.id, price, fee)
                self.order_manager.fill_order(
                    order=order,
                    fill_price=price,
                    fill_quantity=qty,
                    fee=fee,
                )

                if self.journal is not None:
                    self._journal_fill(order, price, qty, fee, fill_type, candle)

    def execute_signal(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """
        Converts Signal to Order and delegates execution to the Adapter.
        Also places SL/TP/Trailing orders when specified in the signal.
        Returns the Order ID (Internal) if successful.
        """
        if self.audit_external_orders:
            return self._execute_signal_with_audit(signal, candle)
        return self._execute_signal_core(signal, candle)

    def _execute_signal_core(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """Current non-audited signal execution path."""
        side = self._determine_side(signal.type)
        if not side:
            return None

        # Determine Quantity
        qty = signal.quantity if signal.quantity and signal.quantity > 0 else self.default_quantity

        # Determine Order Type and Price
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value:
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        # 1. Create Entry Order in DB
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type=order_type,
            quantity=qty,
            price=limit_price
        )

        # 2. Execute via Adapter
        try:
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            exchange_id = self.adapter.place_order(order)
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            self.order_manager.update_exchange_order_id(order, exchange_id)
            self.logger.info("Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id)
            ORDERS_TOTAL.labels(order_type=order_type, status="placed").inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            ORDERS_TOTAL.labels(order_type=order_type, status="failed").inc()
            return None

        # 3. Journal: record entry
        if self.journal is not None:
            self.journal.log(
                "entry",
                {
                    "order_id": str(order.id),
                    "side": side,
                    "order_type": order_type,
                    "quantity": str(qty),
                    "price": str(limit_price) if limit_price else "market",
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "trailing_distance": str(signal.trailing_distance) if signal.trailing_distance else None,
                },
                timestamp=signal.timestamp,
                trade_id=str(order.id),
            )

        # 4. Place conditional orders (SL/TP/Trailing)
        if signal.stop_loss or signal.take_profit or signal.trailing_distance:
            self._place_conditional_orders(signal, order, qty)

        return order.id

    def _execute_signal_with_audit(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """Fail-stop external execution path with committed intent/outcome audits."""
        if self._db_session_factory is None:
            raise RuntimeError("audit_external_orders requires db_session_factory")

        side = self._determine_side(signal.type)
        if not side:
            return None

        qty = signal.quantity if signal.quantity and signal.quantity > 0 else self.default_quantity
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value:
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        client_order_id = generate_client_order_id(
            signal.strategy_id,
            "execution",
            signal.type.value.lower(),
        )
        intent_payload = {
            "signal": signal.model_dump(mode="json"),
            "order": {
                "side": side.value,
                "order_type": order_type,
                "quantity": qty,
                "price": limit_price,
                "client_order_id": client_order_id,
            },
        }
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type=order_type,
            quantity=qty,
            price=limit_price,
            client_order_id=client_order_id,
            intent_payload=intent_payload,
        )

        with self._db_session_factory() as db:
            audit = build_signal_intent_audit(
                clock=self.clock,
                signal=signal,
                client_order_id=client_order_id,
                intent_payload=intent_payload,
            )
            write_signal_audit_intent(db, audit)

        try:
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            exchange_id = self.adapter.place_order(order)
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            self.order_manager.update_exchange_order_id(order, exchange_id)
            self.logger.info("Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id)
            ORDERS_TOTAL.labels(order_type=order_type, status="placed").inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            ORDERS_TOTAL.labels(order_type=order_type, status="failed").inc()
            with self._db_session_factory() as db:
                write_signal_audit_outcome(
                    db,
                    audit,
                    order_id=order.id,
                    risk_message=str(e),
                    outcome_payload={"status": "failed", "error": str(e)},
                )
            raise

        with self._db_session_factory() as db:
            write_signal_audit_outcome(
                db,
                audit,
                order_id=order.id,
                risk_message="placed",
                outcome_payload={"status": "placed", "exchange_order_id": exchange_id},
            )

        if self.journal is not None:
            self.journal.log(
                "entry",
                {
                    "order_id": str(order.id),
                    "side": side,
                    "order_type": order_type,
                    "quantity": str(qty),
                    "price": str(limit_price) if limit_price else "market",
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "trailing_distance": str(signal.trailing_distance) if signal.trailing_distance else None,
                },
                timestamp=signal.timestamp,
                trade_id=str(order.id),
            )

        if signal.stop_loss or signal.take_profit or signal.trailing_distance:
            self._place_conditional_orders(signal, order, qty)

        return order.id

    def _place_conditional_orders(self, signal: Signal, entry_order, qty: Decimal):
        """Submit SL/TP/Trailing orders linked via OCO to each other."""
        # Closing side is opposite of entry
        close_side = OrderSide.SELL if entry_order.side.lower() == "buy" else OrderSide.BUY

        sl_order = None
        tp_order = None

        # Create SL order
        if signal.stop_loss:
            sl_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="stop_loss",
                quantity=qty,
                trigger_price=signal.stop_loss,
            )

        # Create TP order
        if signal.take_profit:
            tp_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="take_profit",
                quantity=qty,
                trigger_price=signal.take_profit,
            )

        # Link OCO: SL and TP cancel each other
        if sl_order and tp_order:
            sl_order._linked_order_id = tp_order.id
            tp_order._linked_order_id = sl_order.id

        # Place orders via adapter
        if sl_order:
            try:
                ex_id = self.adapter.place_order(sl_order)
                self.order_manager.update_exchange_order_id(sl_order, ex_id)
            except ExchangeError as e:
                self.logger.error("Failed to place SL order: %s", e)

        if tp_order:
            try:
                ex_id = self.adapter.place_order(tp_order)
                self.order_manager.update_exchange_order_id(tp_order, ex_id)
            except ExchangeError as e:
                self.logger.error("Failed to place TP order: %s", e)

        # Create Trailing Stop order
        if signal.trailing_distance:
            ts_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="trailing_stop",
                quantity=qty,
                trigger_price=signal.stop_loss,
            )
            ts_order._trailing_distance = signal.trailing_distance
            try:
                ex_id = self.adapter.place_order(ts_order)
                self.order_manager.update_exchange_order_id(ts_order, ex_id)
            except ExchangeError as e:
                self.logger.error("Failed to place trailing stop order: %s", e)

    def _journal_fill(self, order, price, qty, fee, fill_type: str, candle: Optional[Candlestick] = None) -> None:
        """Record a fill event to the journal."""
        tag_map = {
            "STOP_LOSS": "sl_hit",
            "TAKE_PROFIT": "tp_hit",
            "TRAILING_STOP": "trailing_hit",
            "MARKET": "fill",
            "LIMIT": "fill",
        }
        tag = tag_map.get(fill_type, "fill")
        ts = candle.timestamp if candle else 0
        self.journal.log(
            tag,
            {
                "order_id": str(order.id),
                "side": order.side,
                "price": str(price),
                "quantity": str(qty),
                "fee": str(fee) if fee else "0",
                "fill_type": fill_type,
            },
            timestamp=ts,
            trade_id=str(order.id),
        )

    def _determine_side(self, signal_type: SignalType) -> Optional[OrderSide]:
        if signal_type == SignalType.LONG:
            return OrderSide.BUY
        elif signal_type == SignalType.SHORT:
            return OrderSide.SELL
        elif signal_type == SignalType.EXIT_LONG:
            return OrderSide.SELL
        elif signal_type == SignalType.EXIT_SHORT:
            return OrderSide.BUY
        return None
