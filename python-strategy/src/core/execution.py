import logging
import time as _time
from decimal import Decimal
from typing import Callable, ContextManager, Optional
from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType, Candlestick, OrderSide, OrderStatus, PositionSide
from src.core.orm_models import Strategy
from src.core.order_manager import OrderManager
from src.core.interfaces.exchange import IExchangeAdapter, ExchangeError, NetworkError
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.interfaces.exchange import ExchangeOrderLookupUnsupported
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository
from src.core.journal import StrategyJournal
from src.core.metrics import ORDERS_TOTAL, EXECUTION_LATENCY
from src.core.audit_service import (
    build_signal_intent_audit,
    write_signal_audit_intent,
    write_signal_audit_outcome,
    write_system_event,
)
from src.core.client_order_id import (
    generate_client_order_id,
    parse_client_order_id,
)
from src.core.fill_delta import (
    delta_price_from_cumulative_average,
    fill_delta_from_cumulative,
)
from src.core.order_event_sync import (
    OrderEventApplier,
    exchange_snapshot_to_order_event,
)
from src.core.order_reconciliation import OrderReconciler

OPS_KILL_SWITCH_STRATEGY_ID = "__ops_kill_switch__"


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
        self._db_session = db_session
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
        self._order_event_applier = OrderEventApplier(
            order_manager=self.order_manager,
            journal_fill=(
                self._journal_exchange_order_event_fill if self.journal is not None else None
            ),
            fail_pending_conditionals_for_terminal_entry=(
                self._fail_pending_conditional_orders_for_terminal_entry
            ),
            protective_terminal_without_fill_failure=(
                self._protective_terminal_without_fill_failure
            ),
            write_conditional_warning=self._try_write_conditional_order_event_warning,
            place_pending_conditionals_for_entry=(
                self._place_pending_conditional_orders_for_entry
            ),
            protective_partial_fill_requires_resize=(
                self._protective_partial_fill_requires_resize
            ),
            cancel_linked_conditional_for_protection_fill=(
                self._cancel_linked_conditional_order_for_protection_fill
            ),
        )
        self._order_reconciler = OrderReconciler(
            adapter=self.adapter,
            order_manager=self.order_manager,
            clock=self.clock,
            db_session_factory=self._db_session_factory,
            process_exchange_order_event=self.process_exchange_order_event,
            place_pending_protection_for_filled_entries=(
                self.place_pending_protection_for_filled_entries
            ),
            fail_pending_conditionals_for_terminal_entry=(
                self._fail_pending_conditional_orders_for_terminal_entry
            ),
            protective_terminal_without_fill_failure=(
                self._protective_terminal_without_fill_failure
            ),
            cancel_protective_order_when_sibling_closed=(
                self._cancel_protective_order_when_sibling_closed
            ),
            cancel_linked_conditional_for_protection_fill=(
                self._cancel_linked_conditional_order_for_protection_fill
            ),
            logger=self.logger,
        )
        self.logger.info("ExecutionEngine initialized with adapter: %s", type(adapter).__name__)

    def list_recoverable_client_orders(self):
        return self._order_reconciler.list_recoverable_client_orders()

    def record_recoverable_order_scan(self) -> dict:
        return self._order_reconciler.record_recoverable_order_scan()

    def reconcile_recoverable_client_orders(self) -> dict:
        return self._order_reconciler.reconcile_recoverable_client_orders()

    def _fail_pending_conditional_orders_for_terminal_entry(self, entry_order) -> None:
        """Clear pending protection for an entry that terminated with zero fills.

        No fill means no position, so the NEW conditional orders will never be
        placed; leaving them pollutes the recoverable scan and resync forever.
        Callers must only invoke this for CANCELLED/FAILED terminals — a FILLED
        entry with missing fill details still has a live position and must keep
        its pending protection.
        """
        if entry_order.type not in {"market", "limit"}:
            return
        if (entry_order.filled_quantity or Decimal("0")) > 0:
            return
        for order in self.order_manager.repo.list_orders_by_statuses(
            {OrderStatus.NEW.value}
        ):
            if (
                isinstance(order.intent_payload, dict)
                and order.intent_payload.get("pending_entry_order_id")
                == str(entry_order.id)
            ):
                self.order_manager.fail_order(order, "entry_terminal_without_fill")

    def _protective_terminal_without_fill_failure(self, order) -> dict | None:
        """A protective leg terminating with zero fill while its entry holds a
        filled position is a protection gap -- unless the OCO sibling closed the
        position (FILLED/LIQUIDATED), which makes the cancellation expected."""
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if (order.filled_quantity or Decimal("0")) > 0:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        entry_order_id = order.intent_payload.get("pending_entry_order_id")
        if not entry_order_id:
            return None
        entry = self.order_manager.repo.get_order(str(entry_order_id))
        if entry is None or (entry.filled_quantity or Decimal("0")) <= 0:
            return None
        linked_order_id = order.intent_payload.get("linked_order_id")
        if linked_order_id:
            sibling = self.order_manager.repo.get_order(str(linked_order_id))
            if sibling is not None and sibling.status in {
                OrderStatus.FILLED.value,
                OrderStatus.LIQUIDATED.value,
            }:
                return None
        return {
            "order_id": str(order.id),
            "order_type": order.type,
            "entry_order_id": str(entry_order_id),
            "reason": "protective_terminal_without_fill",
        }

    def resync_recoverable_order_events(self) -> dict[str, object]:
        return self._order_reconciler.resync_recoverable_order_events()

    @staticmethod
    def _exchange_snapshot_to_order_event(
        product_id: str,
        snapshot,
    ) -> ExchangeOrderEvent:
        return exchange_snapshot_to_order_event(product_id, snapshot)

    @staticmethod
    def _resync_action_verification_blocked(action: str) -> bool:
        return OrderReconciler._resync_action_verification_blocked(action)

    @staticmethod
    def _reconcile_decision(local_status: str, exchange_status: Optional[str]) -> str:
        return OrderReconciler._reconcile_decision(local_status, exchange_status)

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

    def process_exchange_order_event(self, event: ExchangeOrderEvent) -> dict[str, object]:
        return self._order_event_applier.process_exchange_order_event(event)

    @staticmethod
    def _fill_delta_from_cumulative(
        *,
        local_filled: Decimal,
        local_average_price: Decimal | None,
        cumulative_filled: Decimal,
        cumulative_average_price: Decimal | None,
    ) -> dict[str, Decimal | None]:
        return fill_delta_from_cumulative(
            local_filled=local_filled,
            local_average_price=local_average_price,
            cumulative_filled=cumulative_filled,
            cumulative_average_price=cumulative_average_price,
        )

    @staticmethod
    def _delta_price_from_cumulative_average(
        *,
        local_filled: Decimal,
        local_average_price: Decimal | None,
        cumulative_filled: Decimal,
        cumulative_average_price: Decimal,
        delta: Decimal,
    ) -> Decimal | None:
        return delta_price_from_cumulative_average(
            local_filled=local_filled,
            local_average_price=local_average_price,
            cumulative_filled=cumulative_filled,
            cumulative_average_price=cumulative_average_price,
            delta=delta,
        )

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a known order through the exchange adapter."""
        order = self.order_manager.repo.get_order(order_id)
        if order is None:
            return False
        if order.status == OrderStatus.CANCELLED.value:
            return True

        client_order_id = getattr(order, "client_order_id", None)
        if client_order_id and self.adapter.cancel_order_by_client_id(
            client_order_id,
            order.product_id,
            order_type=order.type,
        ):
            self.order_manager.mark_cancelled(order)
            return True

        exchange_order_id = order.exchange_order_id or order.id
        if not self.adapter.cancel_order(
            exchange_order_id,
            order.product_id,
            order_type=order.type,
        ):
            return False

        self.order_manager.mark_cancelled(order)
        return True

    def flatten_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        quantity: Decimal,
        reference_price: Optional[Decimal] = None,
    ) -> Optional[str]:
        """Close a live position with a reduce-only market order, bypassing
        strategy signal flow.

        Places a market order in the OPPOSITE direction for the full quantity.
        The order is persisted via order_manager before adapter placement so
        that a crash after placement leaves an auditable record.

        Args:
            strategy_id: owning strategy identifier.
            product_id: product to flatten (e.g. "BINANCE:BTCUSDT-PERP").
            side: current position side — "LONG" or "SHORT".
            quantity: absolute quantity to close (Decimal, positive).

        Returns:
            Internal order id string on success, or None on failure.
        """
        normalized_side = str(side).upper()
        if normalized_side == PositionSide.LONG.value:
            order_side = OrderSide.SELL
            signal_type = SignalType.EXIT_LONG
        elif normalized_side == PositionSide.SHORT.value:
            order_side = OrderSide.BUY
            signal_type = SignalType.EXIT_SHORT
        else:
            self.logger.error("Cannot flatten unsupported position side: %s", side)
            return None
        if quantity <= 0:
            self.logger.error("Cannot flatten non-positive quantity: %s", quantity)
            return None

        order_strategy_id = self._flatten_order_strategy_id(strategy_id)
        signal = Signal(
            strategy_id=order_strategy_id,
            product_id=product_id,
            timeframe="ops",
            timestamp=int(self.clock.now() * 1000),
            type=signal_type,
            quantity=quantity,
        )
        order = self.order_manager.create_order(
            signal=signal,
            side=order_side,
            order_type="market",
            quantity=quantity,
            client_order_id=generate_client_order_id(
                order_strategy_id,
                "ops",
                "flatten",
            ),
            intent_payload={"reduce_only": True, "source": "kill_switch"},
        )
        if reference_price is not None:
            order.min_notional_reference_price = reference_price
        order_id = str(order.id)
        submit_attempted = False
        try:
            self.order_manager.mark_submitted_unconfirmed(order)
            submit_attempted = True
            exchange_id = self.adapter.place_order(order)
            self.order_manager.mark_submitted(order, exchange_id)
            ORDERS_TOTAL.labels(
                order_type="market",
                status="placed",
                reason="kill_switch_flatten",
            ).inc()
            return order_id
        except ExchangeError as e:
            self.logger.error("Flatten order failed: %s", e)
            adoption = self._adopt_order_after_ambiguous_submit_error(
                order,
                e,
                submit_attempted=submit_attempted,
            )
            if adoption["action"] == "adopted":
                ORDERS_TOTAL.labels(
                    order_type="market",
                    status="placed",
                    reason="kill_switch_flatten_adopted_after_submit_error",
                ).inc()
                return order_id
            if adoption.get("verification_blocked") or adoption.get("unresolved"):
                ORDERS_TOTAL.labels(
                    order_type="market",
                    status="failed",
                    reason=str(adoption["action"]),
                ).inc()
                return None
            self.order_manager.fail_order(order, str(e))
            self._record_order_rejection(
                order=order,
                order_type="market",
                error=e,
                phase="kill_switch_flatten",
            )
            return None

    def _flatten_order_strategy_id(self, strategy_id: str) -> str:
        if strategy_id != "LIVE":
            return strategy_id
        self._ensure_ops_strategy()
        return OPS_KILL_SWITCH_STRATEGY_ID

    def _ensure_ops_strategy(self) -> None:
        def ensure(session: Session) -> None:
            if session.get(Strategy, OPS_KILL_SWITCH_STRATEGY_ID) is None:
                session.add(
                    Strategy(
                        id=OPS_KILL_SWITCH_STRATEGY_ID,
                        name="Kill Switch Ops",
                        configuration_json="{}",
                    )
                )
                session.commit()

        if self._db_session_factory is not None:
            with self._db_session_factory() as session:
                ensure(session)
            return
        ensure(self._db_session)

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
        qty = self._quantity_for_signal(signal)

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
        self._attach_min_notional_reference_price(order, candle)
        conditional_orders = self._create_conditional_orders(signal, order, qty, candle)
        try:
            self._validate_order_group([order, *conditional_orders])
        except ExchangeError as e:
            self.logger.error("Execution validation failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            for conditional_order in conditional_orders:
                self.order_manager.fail_order(conditional_order, str(e))
            self._record_order_rejection(
                order=order,
                order_type=order_type,
                error=e,
                phase="validation",
            )
            return None

        # 2. Execute via Adapter
        try:
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            exchange_id = self.adapter.place_order(order)
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            self.order_manager.update_exchange_order_id(order, exchange_id)
            self.logger.info("Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id)
            ORDERS_TOTAL.labels(
                order_type=order_type,
                status="placed",
                reason="none",
            ).inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            for conditional_order in conditional_orders:
                self.order_manager.fail_order(conditional_order, "entry_placement_failed")
            self._record_order_rejection(
                order=order,
                order_type=order_type,
                error=e,
                phase="entry_placement",
            )
            return None

        # 3. Journal: record entry
        if self.journal is not None:
            self.journal.log(
                "entry",
                {
                    "order_id": str(order.id),
                    "side": side,
                    "order_type": order_type,
                    # Post-placement order fields: quantization may have adjusted
                    # the submitted values away from the pre-validation locals.
                    "quantity": str(order.quantity),
                    "price": str(order.price) if order.price else "market",
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "trailing_distance": str(signal.trailing_distance) if signal.trailing_distance else None,
                },
                timestamp=signal.timestamp,
                trade_id=str(order.id),
            )

        # 4. Place conditional orders (SL/TP/Trailing)
        if conditional_orders:
            self._place_conditional_orders(conditional_orders)

        return order.id

    def _execute_signal_with_audit(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """Fail-stop external execution path with committed intent/outcome audits."""
        if self._db_session_factory is None:
            raise RuntimeError("audit_external_orders requires db_session_factory")

        side = self._determine_side(signal.type)
        if not side:
            return None

        qty = self._quantity_for_signal(signal)
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value:
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        client_order_id = self._client_order_id_for_signal(signal)
        existing_order = self.order_manager.repo.get_order_by_client_order_id(client_order_id)
        if existing_order is not None:
            self.logger.info("Order already exists for client_order_id=%s", client_order_id)
            return existing_order.id

        intent_payload = {
            "signal": signal.model_dump(mode="json"),
            "order": {
                "side": side.value,
                "order_type": order_type,
                "quantity": qty,
                "price": limit_price,
                "min_notional_reference_price": candle.close if candle else None,
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
        self._attach_min_notional_reference_price(order, candle)
        conditional_orders = self._create_conditional_orders(signal, order, qty, candle)

        with self._db_session_factory() as db:
            audit = build_signal_intent_audit(
                clock=self.clock,
                signal=signal,
                client_order_id=client_order_id,
                intent_payload=intent_payload,
            )
            write_signal_audit_intent(db, audit)

        submit_attempted = False
        try:
            self._validate_order_group([order, *conditional_orders])
            self.order_manager.mark_submitted_unconfirmed(order)
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            submit_attempted = True
            exchange_id = self.adapter.place_order(order)
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            self.order_manager.mark_submitted(order, exchange_id)
            self.logger.info("Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id)
            ORDERS_TOTAL.labels(
                order_type=order_type,
                status="placed",
                reason="none",
            ).inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            adoption = self._adopt_order_after_ambiguous_submit_error(
                order,
                e,
                submit_attempted=submit_attempted,
            )
            if adoption["action"] == "adopted":
                exchange_id = str(adoption["exchange_order_id"])
                order.exchange_order_id = exchange_id
                ORDERS_TOTAL.labels(
                    order_type=order_type,
                    status="placed",
                    reason="adopted_after_submit_error",
                ).inc()
            elif adoption.get("terminal"):
                for conditional_order in conditional_orders:
                    self.order_manager.fail_order(
                        conditional_order,
                        "entry_placement_terminal_after_submit_error",
                    )
                ORDERS_TOTAL.labels(
                    order_type=order_type,
                    status="failed",
                    reason="terminal_after_submit_error",
                ).inc()
                with self._db_session_factory() as db:
                    write_signal_audit_outcome(
                        db,
                        audit,
                        order_id=order.id,
                        risk_message=str(e),
                        outcome_payload={
                            "status": "terminal_after_submit_error",
                            "error": str(e),
                            "adoption": adoption,
                        },
                    )
                raise
            elif adoption["verification_blocked"] or adoption.get("unresolved"):
                self._mark_conditional_orders_pending_after_uncertain_submit(
                    entry_order=order,
                    conditional_orders=conditional_orders,
                    adoption=adoption,
                )
                reason = self._record_order_rejection(
                    order=order,
                    order_type=order_type,
                    error=e,
                    phase="audited_execution",
                    write_event=False,
                )
                with self._db_session_factory() as db:
                    self._write_order_rejection_event(
                        db,
                        order=order,
                        order_type=order_type,
                        reason=reason,
                        error=e,
                        phase="audited_execution",
                    )
                    self._write_pending_protection_warning(
                        db,
                        entry_order=order,
                        conditional_orders=conditional_orders,
                        adoption=adoption,
                        error=e,
                    )
                    write_signal_audit_outcome(
                        db,
                        audit,
                        order_id=order.id,
                        risk_message=str(e),
                        outcome_payload={
                            "status": (
                                "unresolved"
                                if adoption.get("unresolved")
                                else "verification_blocked"
                            ),
                            "error": str(e),
                            "adoption": adoption,
                        },
                    )
                raise
            else:
                self.order_manager.fail_order(order, str(e))
                for conditional_order in conditional_orders:
                    self.order_manager.fail_order(conditional_order, str(e))
                reason = self._record_order_rejection(
                    order=order,
                    order_type=order_type,
                    error=e,
                    phase="audited_execution",
                    write_event=False,
                )
                with self._db_session_factory() as db:
                    self._write_order_rejection_event(
                        db,
                        order=order,
                        order_type=order_type,
                        reason=reason,
                        error=e,
                        phase="audited_execution",
                    )
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
                    # Post-placement order fields: quantization may have adjusted
                    # the submitted values away from the pre-validation locals.
                    "quantity": str(order.quantity),
                    "price": str(order.price) if order.price else "market",
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "trailing_distance": str(signal.trailing_distance) if signal.trailing_distance else None,
                },
                timestamp=signal.timestamp,
                trade_id=str(order.id),
            )

        return order.id

    def _adopt_order_after_ambiguous_submit_error(
        self,
        order,
        error: ExchangeError,
        *,
        submit_attempted: bool,
    ) -> dict[str, object]:
        if not submit_attempted:
            return {
                "action": "submit_not_attempted",
                "verification_blocked": False,
            }
        if not self._is_ambiguous_submit_error(error):
            return {
                "action": "not_ambiguous",
                "verification_blocked": False,
            }
        if not order.client_order_id:
            return {
                "action": "verification_blocked_missing_client_order_id",
                "verification_blocked": True,
            }
        try:
            snapshot = self.adapter.get_order_by_client_id(
                order.client_order_id,
                order.product_id,
                order_type=order.type,
            )
        except ExchangeOrderLookupUnsupported:
            return {
                "action": "verification_blocked_order_lookup_unsupported",
                "verification_blocked": True,
            }
        except ExchangeError as lookup_error:
            return {
                "action": "verification_blocked_order_lookup_failed",
                "reason": str(lookup_error),
                "verification_blocked": True,
            }

        if snapshot is None:
            return {
                "action": "verification_blocked_order_snapshot_missing",
                "verification_blocked": True,
            }

        event_result = self.process_exchange_order_event(
            self._exchange_snapshot_to_order_event(order.product_id, snapshot)
        )
        if event_result["action"] != "applied":
            return {
                "action": event_result["action"],
                "event_result": event_result,
                "verification_blocked": self._resync_action_verification_blocked(
                    str(event_result["action"])
                ),
                "unresolved": str(event_result["action"]).startswith("unresolved_"),
            }
        if event_result.get("state") in {
            "cancelled",
            "rejected",
            "expired",
            "failed",
            "liquidated",
        }:
            return {
                "action": "terminal_after_submit_error",
                "event_result": event_result,
                "exchange_order_id": event_result.get("exchange_order_id")
                or snapshot.exchange_order_id,
                "verification_blocked": False,
                "terminal": True,
            }
        exchange_order_id = event_result.get("exchange_order_id") or snapshot.exchange_order_id
        if exchange_order_id is None:
            return {
                "action": "verification_blocked_order_snapshot_missing_exchange_order_id",
                "event_result": event_result,
                "verification_blocked": True,
            }
        return {
            "action": "adopted",
            "event_result": event_result,
            "exchange_order_id": exchange_order_id,
            "verification_blocked": False,
        }

    @staticmethod
    def _is_ambiguous_submit_error(error: ExchangeError) -> bool:
        return isinstance(error, NetworkError)

    def _write_pending_protection_warning(
        self,
        db: Session,
        *,
        entry_order,
        conditional_orders: list,
        adoption: dict[str, object],
        error: ExchangeError,
    ) -> None:
        if not conditional_orders:
            return
        write_system_event(
            db,
            event_type="system_error",
            event_subtype="protective_orders_pending_after_submit_uncertainty",
            related_strategy_id=entry_order.strategy_id,
            related_order_id=str(entry_order.id),
            payload={
                "entry_order_id": str(entry_order.id),
                "client_order_id": entry_order.client_order_id,
                "product_id": entry_order.product_id,
                "conditional_order_ids": [
                    str(conditional_order.id)
                    for conditional_order in conditional_orders
                ],
                "conditional_order_statuses": {
                    str(conditional_order.id): conditional_order.status
                    for conditional_order in conditional_orders
                },
                "adoption_action": adoption["action"],
                "error": str(error),
                "operator_action": (
                    "entry_submit_outcome_uncertain; verify exchange position "
                    "and place or cancel pending protective orders manually"
                ),
            },
        )

    def _mark_conditional_orders_pending_after_uncertain_submit(
        self,
        *,
        entry_order,
        conditional_orders: list,
        adoption: dict[str, object],
    ) -> None:
        """Keep pending legs recoverable after an uncertain entry submit.

        Adoption may already have placed some legs; anything not NEW has live
        or terminal exchange state and must keep its status, exchange id, and
        payload untouched.
        """
        for conditional_order in conditional_orders:
            if conditional_order.status != OrderStatus.NEW.value:
                continue
            payload = dict(conditional_order.intent_payload or {})
            payload.update(
                {
                    "pending_entry_order_id": str(entry_order.id),
                    "pending_client_order_id": entry_order.client_order_id,
                    "pending_reason": "entry_submit_outcome_uncertain",
                    "adoption_action": str(adoption["action"]),
                }
            )
            conditional_order.intent_payload = payload
            self.order_manager.repo.update_order(conditional_order)

    def _attach_min_notional_reference_price(
        self,
        order,
        candle: Optional[Candlestick],
    ) -> None:
        if candle is not None:
            order.min_notional_reference_price = candle.close

    def _client_order_id_for_signal(self, signal: Signal) -> str:
        client_order_id = (signal.metadata or {}).get("client_order_id")
        if isinstance(client_order_id, str):
            parse_client_order_id(client_order_id)
            return client_order_id
        return generate_client_order_id(
            signal.strategy_id,
            "execution",
            signal.type.value.lower(),
        )

    def _quantity_for_signal(self, signal: Signal) -> Decimal:
        if signal.quantity and signal.quantity > 0:
            return signal.quantity
        if signal.type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            position = self._position_for_exit_signal(signal)
            if position is not None and position.quantity > 0:
                return position.quantity
        return self.default_quantity

    def _position_for_exit_signal(self, signal: Signal):
        try:
            position = self.adapter.get_position(
                signal.product_id,
                strategy_id=signal.strategy_id,
            )
        except TypeError:
            position = self.adapter.get_position(signal.product_id)
        if position is None:
            return None

        position_side = getattr(position.side, "value", position.side)
        if signal.type == SignalType.EXIT_LONG and position_side == PositionSide.LONG.value:
            return position
        if signal.type == SignalType.EXIT_SHORT and position_side == PositionSide.SHORT.value:
            return position
        return None

    def _create_conditional_orders(
        self,
        signal: Signal,
        entry_order,
        qty: Decimal,
        candle: Optional[Candlestick],
    ) -> list:
        """Create SL/TP/Trailing orders linked via OCO before external placement."""
        # Closing side is opposite of entry
        close_side = OrderSide.SELL if entry_order.side.lower() == "buy" else OrderSide.BUY

        sl_order = None
        tp_order = None
        conditional_orders = []

        # Create SL order
        if signal.stop_loss:
            sl_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="stop_loss",
                quantity=qty,
                trigger_price=signal.stop_loss,
                client_order_id=self._conditional_client_order_id(
                    entry_order.client_order_id,
                    "sl",
                ),
            )
            self._attach_min_notional_reference_price(sl_order, candle)
            conditional_orders.append(sl_order)

        # Create TP order
        if signal.take_profit:
            tp_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="take_profit",
                quantity=qty,
                trigger_price=signal.take_profit,
                client_order_id=self._conditional_client_order_id(
                    entry_order.client_order_id,
                    "tp",
                ),
            )
            self._attach_min_notional_reference_price(tp_order, candle)
            conditional_orders.append(tp_order)

        # Link OCO: SL and TP cancel each other
        if sl_order and tp_order:
            sl_order._linked_order_id = tp_order.id
            tp_order._linked_order_id = sl_order.id

        # Create Trailing Stop order
        if signal.trailing_distance:
            ts_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="trailing_stop",
                quantity=qty,
                trigger_price=signal.stop_loss,
                client_order_id=self._conditional_client_order_id(
                    entry_order.client_order_id,
                    "tr",
                ),
            )
            ts_order._trailing_distance = signal.trailing_distance
            self._attach_min_notional_reference_price(ts_order, candle)
            conditional_orders.append(ts_order)

        for conditional_order in conditional_orders:
            linked_order_id = getattr(conditional_order, "_linked_order_id", None)
            conditional_order.status = OrderStatus.NEW.value
            conditional_order.exchange_order_id = None
            conditional_order.intent_payload = {
                "pending_entry_order_id": str(entry_order.id),
                "linked_order_id": str(linked_order_id) if linked_order_id else None,
                "placement_mode": "place-after-fill",
            }
            self.order_manager.repo.update_order(conditional_order)

        return conditional_orders

    @staticmethod
    def _conditional_client_order_id(
        entry_client_order_id: str | None,
        suffix: str,
    ) -> str | None:
        if not entry_client_order_id:
            return None
        parts = parse_client_order_id(entry_client_order_id)
        conditional_id = (
            f"{parts.strategy_id}-{parts.instance_id}-{suffix}-{parts.ts_ns}"
        )
        parse_client_order_id(conditional_id)
        return conditional_id

    def _place_pending_conditional_orders_for_entry(self, entry_order) -> list[dict]:
        if entry_order.type not in {"market", "limit"}:
            return []
        protected_quantity = entry_order.filled_quantity or Decimal("0")
        if protected_quantity <= 0:
            return []
        related_orders = [
            order
            for order in self.order_manager.repo.list_orders_by_statuses(
                {
                    OrderStatus.NEW.value,
                    OrderStatus.SUBMITTED_UNCONFIRMED.value,
                    OrderStatus.SUBMITTED.value,
                    OrderStatus.PARTIALLY_FILLED.value,
                }
            )
            if isinstance(order.intent_payload, dict)
            and order.intent_payload.get("pending_entry_order_id") == str(entry_order.id)
        ]
        pending = [
            order for order in related_orders if order.status == OrderStatus.NEW.value
        ]
        failures = self._underprotected_conditional_order_failures(
            related_orders,
            protected_quantity,
            pending_statuses={OrderStatus.NEW.value},
        )
        if not pending:
            if failures:
                return failures
            return []
        for order in pending:
            order.quantity = protected_quantity
            self.order_manager.repo.update_order(order)
        placement_candidates = []
        for order in pending:
            lookup_failure = self._adopt_pending_conditional_order_before_submit(order)
            if lookup_failure is None:
                if order.status == OrderStatus.NEW.value:
                    placement_candidates.append(order)
                continue
            failures.append(lookup_failure)
        if placement_candidates:
            failures.extend(self._place_conditional_orders(placement_candidates))
        return failures

    @staticmethod
    def _underprotected_conditional_order_failures(
        related_orders: list,
        protected_quantity: Decimal,
        *,
        pending_statuses: set[str],
    ) -> list[dict]:
        return [
            {
                "order_id": str(order.id),
                "order_type": order.type,
                "reason": "conditional_order_resize_required_after_entry_fill",
                "current_quantity": str(order.quantity),
                "required_quantity": str(protected_quantity),
            }
            for order in related_orders
            if order.status not in pending_statuses
            and (order.quantity or Decimal("0")) < protected_quantity
        ]

    def _adopt_pending_conditional_order_before_submit(self, order) -> dict | None:
        if not order.client_order_id:
            return None
        try:
            snapshot = self.adapter.get_order_by_client_id(
                order.client_order_id,
                order.product_id,
                order_type=order.type,
            )
        except ExchangeOrderLookupUnsupported:
            return {
                "order_id": str(order.id),
                "order_type": order.type,
                "reason": "verification_blocked_order_lookup_unsupported",
            }
        except ExchangeError as e:
            return {
                "order_id": str(order.id),
                "order_type": order.type,
                "reason": "verification_blocked_order_lookup_failed",
                "error": str(e),
            }
        if snapshot is None:
            return None

        event_result = self.process_exchange_order_event(
            self._exchange_snapshot_to_order_event(order.product_id, snapshot)
        )
        if event_result["action"] == "applied":
            return None
        return {
            "order_id": str(order.id),
            "order_type": order.type,
            "reason": str(event_result["action"]),
            "event_result": event_result,
        }

    def place_pending_protection_for_filled_entries(self) -> dict[str, object]:
        """Retry NEW pending protective orders whose entry already has fills.

        Crash/replay safety net: entry fills are persisted before protective
        placement, so a crash between the two steps leaves NEW conditional
        orders behind while no further fill delta will ever re-trigger
        placement (a fully filled entry drops out of the recoverable scan).
        Placing protection is the fail-safe direction for a naked position.
        """
        pending = [
            order
            for order in self.order_manager.repo.list_orders_by_statuses(
                {OrderStatus.NEW.value}
            )
            if isinstance(order.intent_payload, dict)
            and order.intent_payload.get("pending_entry_order_id")
        ]
        entry_ids = {
            str(order.intent_payload["pending_entry_order_id"]) for order in pending
        }
        attempted = 0
        failures: list[dict] = []
        for entry_id in sorted(entry_ids):
            entry = self.order_manager.repo.get_order(entry_id)
            if entry is None or (entry.filled_quantity or Decimal("0")) <= 0:
                continue
            attempted += 1
            entry_failures = self._place_pending_conditional_orders_for_entry(entry)
            if entry_failures:
                failures.extend(entry_failures)
                self._try_write_conditional_order_event_warning(
                    event_subtype="conditional_order_placement_failed_after_entry_fill",
                    order=entry,
                    failures=entry_failures,
                )
        if failures:
            self.logger.error(
                "Pending protection recovery has %s placement failure(s)",
                len(failures),
            )
        return {
            "pending_count": len(pending),
            "entries_attempted": attempted,
            "failures": failures,
        }

    @staticmethod
    def _protective_partial_fill_requires_resize(order, event_state: str) -> dict | None:
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if event_state in {"filled", "liquidated"}:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        linked_order_id = order.intent_payload.get("linked_order_id")
        if not linked_order_id:
            return None
        return {
            "order_id": str(order.id),
            "order_type": order.type,
            "linked_order_id": str(linked_order_id),
            "reason": "protective_partial_fill_requires_resize",
        }

    def _cancel_linked_conditional_order_for_protection_fill(self, order) -> dict | None:
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        linked_order_id = order.intent_payload.get("linked_order_id")
        if not linked_order_id:
            return None
        linked_order = self.order_manager.repo.get_order(str(linked_order_id))
        if linked_order is None:
            return None
        if linked_order.status in {
            OrderStatus.CANCELLED.value,
            OrderStatus.FILLED.value,
            OrderStatus.FAILED.value,
            OrderStatus.LIQUIDATED.value,
        }:
            return None
        if self.cancel_order(str(linked_order.id)):
            return None
        return {
            "order_id": str(linked_order.id),
            "order_type": linked_order.type,
            "exchange_order_id": linked_order.exchange_order_id,
            "reason": "cancel_order_returned_false",
        }

    def _cancel_protective_order_when_sibling_closed(self, order) -> dict | None:
        """Inverse of the protection-fill cancel: an open protective leg whose
        OCO sibling already closed the position must be cancelled, not have
        its tracking restored."""
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        linked_order_id = order.intent_payload.get("linked_order_id")
        if not linked_order_id:
            return None
        sibling = self.order_manager.repo.get_order(str(linked_order_id))
        if sibling is None or sibling.status not in {
            OrderStatus.FILLED.value,
            OrderStatus.LIQUIDATED.value,
        }:
            return None
        if self.cancel_order(str(order.id)):
            return {"cancelled": True}
        return {"cancelled": False}

    def _validate_order_group(self, orders: list) -> None:
        validate_order = getattr(self.adapter, "validate_order", None)
        if validate_order is None:
            return
        for order in orders:
            validate_order(order)
            self.order_manager.repo.update_order(order)

    def _record_order_rejection(
        self,
        *,
        order,
        order_type: str,
        error: ExchangeError,
        phase: str,
        write_event: bool = True,
    ) -> str:
        reason = self._order_rejection_reason(error)
        ORDERS_TOTAL.labels(
            order_type=order_type,
            status="failed",
            reason=reason,
        ).inc()
        if write_event:
            self._try_write_order_rejection_event(
                order=order,
                order_type=order_type,
                reason=reason,
                error=error,
                phase=phase,
            )
        return reason

    @staticmethod
    def _order_rejection_reason(error: ExchangeError) -> str:
        message = str(error)
        token = message.split(":", 1)[0].strip()
        normalized = "".join(
            char if char.isalnum() else "_"
            for char in token.lower()
        ).strip("_")
        return normalized or "exchange_error"

    def _try_write_order_rejection_event(
        self,
        *,
        order,
        order_type: str,
        reason: str,
        error: ExchangeError,
        phase: str,
    ) -> None:
        if self._db_session_factory is None:
            return
        try:
            with self._db_session_factory() as db:
                self._write_order_rejection_event(
                    db,
                    order=order,
                    order_type=order_type,
                    reason=reason,
                    error=error,
                    phase=phase,
                )
                db.commit()
        except Exception:
            self.logger.exception("Failed to write order rejection system event")

    def _write_order_rejection_event(
        self,
        db: Session,
        *,
        order,
        order_type: str,
        reason: str,
        error: ExchangeError,
        phase: str,
    ) -> None:
        write_system_event(
            db,
            event_type="system_error",
            event_subtype="order_rejected",
            related_strategy_id=order.strategy_id,
            related_order_id=str(order.id),
            payload={
                "order_id": str(order.id),
                "product_id": order.product_id,
                "order_type": order_type,
                "phase": phase,
                "reason": reason,
                "error": str(error),
            },
        )

    def _place_conditional_orders(self, conditional_orders: list) -> list[dict]:
        """Submit prevalidated SL/TP/Trailing orders linked via OCO to each other."""
        failures = []
        for order in conditional_orders:
            submit_attempted = False
            try:
                if order.client_order_id:
                    self.order_manager.mark_submitted_unconfirmed(order)
                submit_attempted = True
                ex_id = self.adapter.place_order(order)
                self.order_manager.mark_submitted(order, ex_id)
                ORDERS_TOTAL.labels(order_type=order.type, status="placed", reason="none").inc()
            except ExchangeError as e:
                label = {
                    "stop_loss": "SL",
                    "take_profit": "TP",
                    "trailing_stop": "trailing stop",
                }.get(order.type, order.type)
                self.logger.error("Failed to place %s order: %s", label, e)
                failures.extend(
                    self._handle_conditional_order_placement_error(
                        order,
                        e,
                        submit_attempted=submit_attempted,
                    )
                )
        return failures

    def _handle_conditional_order_placement_error(
        self,
        order,
        error: ExchangeError,
        *,
        submit_attempted: bool,
    ) -> list[dict]:
        adoption = self._adopt_order_after_ambiguous_submit_error(
            order,
            error,
            submit_attempted=submit_attempted,
        )
        if (
            submit_attempted
            and adoption["action"] == "verification_blocked_missing_client_order_id"
        ):
            self.order_manager.mark_submitted_unconfirmed(order)
            ORDERS_TOTAL.labels(order_type=order.type, status="failed", reason="verification_blocked_missing_client_order_id").inc()
            return [
                {
                    "order_id": str(order.id),
                    "order_type": order.type,
                    "reason": "verification_blocked_missing_client_order_id",
                    "adoption": adoption,
                    "operator_action": (
                        "conditional_submit_outcome_uncertain_without_client_id; "
                        "verify exchange manually"
                    ),
                }
            ]
        if adoption["action"] == "adopted":
            ORDERS_TOTAL.labels(order_type=order.type, status="placed", reason="adopted_after_submit_error").inc()
            return []
        if adoption.get("terminal"):
            ORDERS_TOTAL.labels(order_type=order.type, status="failed", reason="terminal_after_submit_error").inc()
            return [
                {
                    "order_id": str(order.id),
                    "order_type": order.type,
                    "reason": "terminal_after_submit_error",
                    "adoption": adoption,
                }
            ]
        if adoption.get("verification_blocked") or adoption.get("unresolved"):
            ORDERS_TOTAL.labels(order_type=order.type, status="failed", reason=str(adoption["action"])).inc()
            return [
                {
                    "order_id": str(order.id),
                    "order_type": order.type,
                    "reason": str(adoption["action"]),
                    "adoption": adoption,
                }
            ]
        self.order_manager.fail_order(order, str(error))
        ORDERS_TOTAL.labels(order_type=order.type, status="failed", reason=self._order_rejection_reason(error)).inc()
        return [
            {
                "order_id": str(order.id),
                "order_type": order.type,
                "reason": str(error),
                "adoption": adoption,
            }
        ]

    def _try_write_conditional_order_event_warning(
        self,
        *,
        event_subtype: str,
        order,
        failures: list[dict],
    ) -> None:
        if self._db_session_factory is None:
            return
        try:
            with self._db_session_factory() as db:
                write_system_event(
                    db,
                    event_type="system_error",
                    event_subtype=event_subtype,
                    related_strategy_id=order.strategy_id,
                    related_order_id=str(order.id),
                    payload={
                        "order_id": str(order.id),
                        "product_id": order.product_id,
                        "failures": failures,
                    },
                )
                db.commit()
        except Exception:
            self.logger.exception("Failed to write conditional order warning event")

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

    def _journal_exchange_order_event_fill(
        self,
        order,
        event: ExchangeOrderEvent,
        fill_price: Decimal,
        fill_quantity: Decimal,
    ) -> None:
        self.journal.log(
            "fill",
            {
                "order_id": str(order.id),
                "side": order.side,
                "signal_price": self._intent_signal_price(order),
                "submitted_price": str(order.price) if order.price else "market",
                "fill_price": str(fill_price),
                "quantity": str(fill_quantity),
                "fee": str(event.fee) if event.fee is not None else "0",
                "fee_asset": event.fee_asset,
                "exchange_order_id": event.exchange_order_id,
                "client_order_id": event.client_order_id,
                "exchange_status": event.status,
            },
            timestamp=event.event_timestamp or int(self.clock.now() * 1000),
            trade_id=str(order.id),
        )

    @staticmethod
    def _intent_signal_price(order) -> str | None:
        intent_payload = getattr(order, "intent_payload", None)
        if not isinstance(intent_payload, dict):
            return None
        order_payload = intent_payload.get("order")
        if not isinstance(order_payload, dict):
            return None
        price = order_payload.get("price")
        return str(price) if price is not None else None

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
