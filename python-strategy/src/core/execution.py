import logging
import threading
import time as _time
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, ContextManager, Optional, cast
from sqlalchemy.orm import Session
from src.core.models import (
    Signal,
    SignalType,
    Candlestick,
    OrderSide,
    OrderStatus,
    PositionSide,
)
from src.core.orm_models import Strategy
from src.core.order_manager import OrderManager
from src.core.runtime_capabilities import (
    OrderAccountIdentityResolver,
    OrderEventProcessor,
    PendingProtectionFillProcessor,
    audit_pending_protection_without_venue_policy,
    process_order_event_without_venue_policy,
)
from src.core.interfaces.exchange import IExchangeAdapter, ExchangeError, NetworkError
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository
from src.core.journal import StrategyJournal
from src.core.metrics import ORDERS_TOTAL, EXECUTION_LATENCY
from src.core.audit_service import (
    build_signal_audit,
    build_signal_intent_audit,
    commit_signal_audit,
    write_signal_audit_intent,
    write_signal_audit_outcome,
    write_system_event,
)
from src.core.client_order_id import (
    generate_client_order_id,
    parse_client_order_id,
)
from src.core.execution_conditional_orders import ConditionalOrderLifecycleOwner
from src.core.execution_ambiguous_submit_adoption import (
    adopt_order_after_ambiguous_submit_error,
)
from src.core.execution_order_cancellation import cancel_known_order
from src.core.fill_delta import (
    FillDelta,
    delta_price_from_cumulative_average,
    fill_delta_from_cumulative,
)
from src.core.execution_submission_gate import ExecutionSubmissionGate
from src.core.execution_protection_modification import (
    ProtectionModificationAdapter,
    ProtectionModificationRepository,
    modify_attached_protection,
)
from src.core import execution_failure_diagnostics
from src.core import execution_journal
from src.core import execution_portfolio_exposure
from src.core import execution_verified_net_reduction
from src.core.order_event_sync import OrderEventApplier
from src.core.order_reconciliation import OrderReconciler
from src.core.portfolio_runtime import PortfolioExposureSnapshot
from src.core.signal_order_intent import (
    InvalidSignalOrderIntent,
    ResolvedOrderIntent,
    normalize_signal_quantity,
    resolve_signal_order_intent,
)

OPS_KILL_SWITCH_STRATEGY_ID = "__ops_kill_switch__"


@dataclass(frozen=True)
class FlattenPending:
    order_id: str
    reason: str


@dataclass(frozen=True)
class ExitDecision:
    """Resolved position truth for one EXIT signal."""

    allowed: bool
    reason: str
    quantity: Decimal | None = None
    position_quantity: Decimal | None = None


class ExecutionEngine:
    def __init__(
        self,
        db_session: Session | None,
        clock: Clock,
        adapter: IExchangeAdapter,
        order_repository: Optional[IOrderRepository] = None,
        journal: Optional[StrategyJournal] = None,
        is_backtest: Optional[bool] = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        audit_external_orders: bool = False,
        account_service=None,
        order_account_identity_resolver: OrderAccountIdentityResolver | None = None,
        operation_guard: Callable[[], None] | None = None,
        order_event_processor: OrderEventProcessor | None = None,
        pending_protection_fill_processor: PendingProtectionFillProcessor | None = None,
    ):
        self.logger = logging.getLogger("ExecutionEngine")
        self.clock = clock
        self._db_session_factory = db_session_factory
        self._db_session = db_session
        self.audit_external_orders = audit_external_orders
        self._operation_guard = operation_guard or (lambda: None)
        self._order_event_processor = (
            order_event_processor or process_order_event_without_venue_policy
        )
        self._pending_protection_fill_processor = (
            pending_protection_fill_processor
            or audit_pending_protection_without_venue_policy
        )
        self._position_loader = (
            getattr(
                account_service,
                "get_position_for_exit",
                getattr(account_service, "get_position", None),
            )
            if account_service is not None
            else None
        )
        if order_repository:
            self.order_manager = OrderManager(
                order_repository,
                clock,
                is_backtest=is_backtest,
                order_account_identity_resolver=order_account_identity_resolver,
            )
        else:
            from src.core.repositories import LiveOrderRepository

            self.order_manager = OrderManager(
                LiveOrderRepository(db_session, db_session_factory=db_session_factory),
                clock,
                is_backtest=is_backtest,
                order_account_identity_resolver=order_account_identity_resolver,
            )

        self.default_quantity = Decimal("0.01")
        self.adapter = adapter
        self.journal = journal
        self._order_event_apply_lock = threading.RLock()
        self._submission_gate_owner = ExecutionSubmissionGate(
            self._log_submission_drain_callback_failure
        )
        self._conditional_order_lifecycle = ConditionalOrderLifecycleOwner(
            order_manager=self.order_manager,
            adapter=self.adapter,
            submission_gate=self._submission_gate_owner,
            pending_protection_fill_processor=(self._pending_protection_fill_processor),
            process_exchange_order_event=self.process_exchange_order_event,
            assert_external_operation_allowed=(self._assert_external_operation_allowed),
            record_order_ack=self._record_order_ack,
            write_warning=self._try_write_conditional_order_event_warning,
            logger=self.logger,
        )
        self._order_event_applier = OrderEventApplier(
            order_manager=self.order_manager,
            journal_fill=(
                self._journal_exchange_order_event_fill
                if self.journal is not None
                else None
            ),
            fail_pending_conditionals_for_terminal_entry=(
                self._fail_pending_conditional_orders_for_terminal_entry
            ),
            protective_terminal_without_fill_failure=(
                self._protective_terminal_without_fill_failure
            ),
            write_conditional_warning=self._conditional_order_lifecycle.write_warning,
            place_pending_conditionals_for_entry=(
                self._conditional_order_lifecycle.place_pending_for_entry
            ),
            protective_partial_fill_requires_resize=(
                self._protective_partial_fill_requires_resize
            ),
            cancel_linked_conditional_for_protection_fill=(
                self._cancel_linked_conditional_order_for_protection_fill
            ),
            remote_follow_up_required=self._remote_follow_up_required,
        )
        self._order_reconciler = OrderReconciler(
            adapter=self.adapter,
            order_manager=self.order_manager,
            clock=self.clock,
            db_session_factory=self._db_session_factory,
            process_exchange_order_event=self.process_exchange_order_event,
            place_pending_protection_for_filled_entries=(
                self._conditional_order_lifecycle.recover_pending_protection
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
            local_positions_loader=(
                getattr(account_service, "get_all_positions", None)
                if account_service is not None
                else None
            ),
            logger=self.logger,
        )
        self.logger.info(
            "ExecutionEngine initialized with adapter: %s", type(adapter).__name__
        )

    @property
    def _submissions_halted(self) -> bool:
        return self._submission_gate_owner.submissions_halted

    @property
    def _reconcile_halt(self) -> bool:
        return self._submission_gate_owner.reconcile_halted

    @property
    def order_event_stream_failed(self) -> bool:
        """Report the permanent stream-failure latch to entry admission."""
        return self._submission_gate_owner.order_event_stream_failed

    @property
    def _submissions_in_flight(self) -> int:
        return self._submission_gate_owner.in_flight

    def _log_submission_drain_callback_failure(self) -> None:
        self.logger.exception("Submission drain callback failed")

    def _assert_external_operation_allowed(self) -> None:
        try:
            self._operation_guard()
        except Exception as error:
            raise ExchangeError("external_operation_fenced") from error

    def list_recoverable_client_orders(self):
        return self._order_reconciler.list_recoverable_client_orders()

    def record_recoverable_order_scan(self) -> dict:
        return self._order_reconciler.record_recoverable_order_scan()

    def reconcile_recoverable_client_orders(self) -> dict:
        with self._order_event_apply_lock:
            return self._order_reconciler.reconcile_recoverable_client_orders()

    def reconcile_owned_orders(self, *, snapshot_loader=None) -> dict[str, object]:
        return self._order_reconciler.reconcile_owned_orders(
            snapshot_loader=snapshot_loader
        )

    def portfolio_exposure_snapshot(
        self,
        strategy_ids: tuple[str, ...],
        product_id: str,
        requested_intents: Mapping[str, str],
    ) -> PortfolioExposureSnapshot:
        return execution_portfolio_exposure.project_portfolio_exposure(
            position_loader=self._position_loader,
            order_repository=self.order_manager.repo,
            order_event_lock=self._order_event_apply_lock,
            strategy_ids=strategy_ids,
            product_id=product_id,
            requested_intents=requested_intents,
        )

    def execute_authoritative_exit_signal(
        self,
        signal: Signal,
        candle: Optional[Candlestick],
        executor: Callable[[Signal, ExitDecision], dict[str, object]],
    ) -> bool:
        """Audit and execute one venue-native, full-position EXIT operation."""
        reconcile_generation = self._begin_authoritative_exit(timeout=30.0)
        if reconcile_generation is None:
            self.logger.warning(
                "Authoritative EXIT rejected by submission gate: strategy=%s "
                "product=%s",
                signal.strategy_id,
                signal.product_id,
            )
            self._audit_non_submission(signal, candle, "submission_gate_halted")
            return False

        resume_after_exit = False
        try:
            self._assert_external_operation_allowed()
            decision = self._classify_exit_signal(signal)
            if decision is None or not decision.allowed:
                reason = "not_exit" if decision is None else decision.reason
                if (
                    reason == "already_flat"
                    and self._completed_verified_net_reduction_replay(signal)
                ):
                    self.logger.info(
                        "Authoritative EXIT replay already completed: "
                        "strategy=%s product=%s",
                        signal.strategy_id,
                        signal.product_id,
                    )
                    resume_after_exit = True
                    return True
                self.logger.warning(
                    "Authoritative EXIT not submitted: strategy=%s "
                    "product=%s reason=%s",
                    signal.strategy_id,
                    signal.product_id,
                    reason,
                )
                self._audit_non_submission(signal, candle, reason)
                resume_after_exit = reason in {"not_exit", "already_flat"}
                return False
            if decision.quantity != decision.position_quantity:
                self.logger.warning(
                    "Authoritative partial EXIT is unsupported: strategy=%s "
                    "product=%s requested=%s position=%s",
                    signal.strategy_id,
                    signal.product_id,
                    decision.quantity,
                    decision.position_quantity,
                )
                self._audit_non_submission(
                    signal,
                    candle,
                    "authoritative_partial_exit_unsupported",
                )
                return False

            client_order_id = self._client_order_id_for_signal(signal)
            intent_payload = {
                "signal": signal.model_dump(mode="json"),
                "operation": {
                    "type": "authoritative_position_exit",
                    "quantity": decision.position_quantity,
                    "client_order_id": client_order_id,
                },
            }
            audit = None
            if self.audit_external_orders:
                if self._db_session_factory is None:
                    raise RuntimeError(
                        "audit_external_orders requires db_session_factory"
                    )
                audit = build_signal_intent_audit(
                    clock=self.clock,
                    signal=signal,
                    client_order_id=client_order_id,
                    intent_payload=intent_payload,
                )
                with self._db_session_factory() as db:
                    write_signal_audit_intent(db, audit)

            try:
                outcome = executor(signal, decision)
            except Exception as error:
                if audit is not None:
                    assert self._db_session_factory is not None
                    with self._db_session_factory() as db:
                        write_signal_audit_outcome(
                            db,
                            audit,
                            risk_message=str(error),
                            outcome_payload={
                                "status": "verification_blocked",
                                "error_type": type(error).__name__,
                            },
                        )
                raise

            if audit is not None:
                assert self._db_session_factory is not None
                with self._db_session_factory() as db:
                    write_signal_audit_outcome(
                        db,
                        audit,
                        outcome_payload=outcome,
                    )
            resume_after_exit = True
            return True
        finally:
            self._finish_authoritative_exit(
                resume_after_reconcile=resume_after_exit,
                reconcile_generation=reconcile_generation,
            )

    def _completed_verified_net_reduction_replay(self, signal: Signal) -> bool:
        client_order_id = self._client_order_id_for_signal(signal)
        existing_order = self.order_manager.repo.get_order_by_client_order_id(
            client_order_id
        )
        if existing_order is None:
            return False
        return execution_verified_net_reduction.completed_replay(
            signal,
            existing_order,
            expected_side=self._determine_side(signal.type),
        )

    def record_verified_net_reduction(
        self,
        signal: Signal,
        order_id: str,
        *,
        remaining_remote_quantity: Decimal,
    ) -> None:
        execution_verified_net_reduction.validate_remaining_remote_quantity(
            remaining_remote_quantity
        )
        order = self.order_manager.repo.get_order(order_id)
        if order is None:
            raise RuntimeError("verified_net_reduction_order_missing")
        client_order_id = self._client_order_id_for_signal(signal)
        execution_verified_net_reduction.record_verification(
            self.order_manager.repo,
            signal,
            order,
            client_order_id=client_order_id,
            expected_side=self._determine_side(signal.type),
            remaining_remote_quantity=remaining_remote_quantity,
        )

    def submit_verified_net_reduction(
        self,
        signal: Signal,
        decision: ExitDecision,
        *,
        candle: Optional[Candlestick],
        preflight_remote_quantity: Decimal,
    ) -> str:
        """Submit one audited market reduction inside an authoritative exit gate.

        This is intentionally not a reduce-only order. Venues without native
        reduce-only semantics may use it only after the caller has fenced all
        submissions and verified the exact remote net position.
        """
        if signal.type not in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            raise ValueError("verified_net_reduction_requires_exit_signal")
        if not self.audit_external_orders or self._db_session_factory is None:
            raise RuntimeError("verified_net_reduction_requires_external_order_audit")
        if not self._submission_gate_owner.authoritative_exit_active:
            raise RuntimeError("verified_net_reduction_requires_exclusive_exit_gate")
        if decision.quantity is None or decision.quantity <= 0:
            raise ValueError("verified_net_reduction_quantity_invalid")
        if (
            decision.position_quantity is None
            or decision.quantity > decision.position_quantity
        ):
            raise ValueError("verified_net_reduction_exceeds_strategy_position")
        if preflight_remote_quantity < decision.quantity:
            raise ValueError("verified_net_reduction_exceeds_remote_position")

        side = self._determine_side(signal.type)
        assert side is not None
        client_order_id = self._client_order_id_for_signal(signal)
        existing_order = self.order_manager.repo.get_order_by_client_order_id(
            client_order_id
        )
        if existing_order is not None:
            return str(existing_order.id)

        intent_payload = {
            "signal": signal.model_dump(mode="json"),
            "order": {
                "side": side.value,
                "order_type": "market",
                "quantity": decision.quantity,
                "price": None,
                "min_notional_reference_price": (
                    candle.close if candle is not None else None
                ),
                "client_order_id": client_order_id,
            },
            "source": "authoritative_net_reduction",
            "preflight_remote_quantity": str(preflight_remote_quantity),
            "strategy_position_quantity": str(decision.position_quantity),
        }
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type="market",
            quantity=decision.quantity,
            client_order_id=client_order_id,
            intent_payload=intent_payload,
        )
        self._attach_min_notional_reference_price(order, candle)
        order_id = str(order.id)
        submit_attempted = False
        try:
            self._validate_order_group([order])
            self._assert_external_operation_allowed()
            self.order_manager.mark_submitted_unconfirmed(order)
            submit_attempted = True
            exchange_id = self.adapter.place_order(order)
            self._record_order_ack(order, exchange_id, order_id=order_id)
            return order_id
        except ExchangeError as error:
            adoption = self._adopt_order_after_ambiguous_submit_error(
                order,
                error,
                submit_attempted=submit_attempted,
            )
            if adoption["action"] == "adopted":
                return order_id
            if adoption.get("verification_blocked") or adoption.get("unresolved"):
                self._submission_gate_owner.claim_reconcile_halt()
            elif not adoption.get("terminal"):
                self.order_manager.fail_order(order, str(error))
            self._record_order_rejection(
                order=order,
                order_type="market",
                error=error,
                phase="verified_net_reduction",
            )
            raise

    def _begin_authoritative_exit(self, *, timeout: float) -> int | None:
        """Own the shared gate without counting this operation in its drain."""
        return self._submission_gate_owner.begin_authoritative_exit(timeout=timeout)

    def _finish_authoritative_exit(
        self,
        *,
        resume_after_reconcile: bool,
        reconcile_generation: int,
    ) -> None:
        self._submission_gate_owner.finish_authoritative_exit(
            resume_after_reconcile=resume_after_reconcile,
            reconcile_generation=reconcile_generation,
        )

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
            {
                OrderStatus.NEW.value,
                OrderStatus.SUBMITTED_UNCONFIRMED.value,
                OrderStatus.SUBMITTED.value,
            }
        ):
            if (
                isinstance(order.intent_payload, dict)
                and order.intent_payload.get("pending_entry_order_id")
                == str(entry_order.id)
                and (
                    order.status == OrderStatus.NEW.value
                    or order.intent_payload.get("placement_mode") == "attach-at-entry"
                )
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

    def _remote_follow_up_required(self, order, event_state: str) -> bool:
        if order.type in {"market", "limit"}:
            protected_quantity = order.filled_quantity or Decimal("0")
            related = [
                candidate
                for candidate in self.order_manager.repo.list_orders_by_statuses(
                    {
                        OrderStatus.NEW.value,
                        OrderStatus.SUBMITTED_UNCONFIRMED.value,
                        OrderStatus.SUBMITTED.value,
                        OrderStatus.PARTIALLY_FILLED.value,
                    }
                )
                if isinstance(candidate.intent_payload, dict)
                and candidate.intent_payload.get("pending_entry_order_id")
                == str(order.id)
            ]
            return any(
                candidate.status == OrderStatus.NEW.value
                or (candidate.quantity or Decimal("0")) < protected_quantity
                for candidate in related
            )
        if (
            self._protective_partial_fill_requires_resize(order, event_state)
            is not None
        ):
            return True
        if event_state not in {"filled", "liquidated"}:
            return False
        if not isinstance(order.intent_payload, dict):
            return False
        linked_order_id = order.intent_payload.get("linked_order_id")
        if not linked_order_id:
            return False
        linked = self.order_manager.repo.get_order(str(linked_order_id))
        return linked is not None and linked.status not in {
            OrderStatus.CANCELLED.value,
            OrderStatus.FILLED.value,
            OrderStatus.FAILED.value,
            OrderStatus.LIQUIDATED.value,
        }

    def resync_recoverable_order_events(self) -> dict[str, object]:
        return self._order_reconciler.resync_recoverable_order_events()

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
                order = fill["order"]
                price = fill["price"]
                qty = fill["quantity"]
                fee = fill.get("fee")
                fill_type = fill.get("fill_type", "MARKET")

                self.logger.info(
                    "Execution: Adapter fill for %s at %s (fee=%s)",
                    order.id,
                    price,
                    fee,
                )
                self.order_manager.fill_order(
                    order=order,
                    fill_price=price,
                    fill_quantity=qty,
                    fee=fee,
                )
                if self.audit_external_orders and order.type in {"market", "limit"}:
                    failures = self._place_pending_conditional_orders_for_entry(order)
                    if failures:
                        self._submission_gate_owner.claim_reconcile_halt()
                        raise ExchangeError(
                            "conditional_order_placement_failed_after_simulated_fill: "
                            f"{failures}"
                        )
                for cancelled_order in fill.get("cancelled_orders", []):
                    self.order_manager.mark_cancelled(cancelled_order)

                if self.journal is not None:
                    self._journal_fill(order, price, qty, fee, fill_type, candle)

    def process_exchange_order_event(
        self,
        event: ExchangeOrderEvent,
        *,
        allow_remote_side_effects: bool = True,
    ) -> dict[str, object]:
        with self._order_event_apply_lock:
            return self._order_event_processor(
                self.order_manager.repo,
                event,
                lambda: self._order_event_applier.process_exchange_order_event(
                    event,
                    allow_remote_side_effects=allow_remote_side_effects,
                ),
            )

    def _record_order_ack(
        self,
        order,
        exchange_order_id: str,
        *,
        order_id: str | None = None,
    ) -> None:
        with self._order_event_apply_lock:
            current = (
                self.order_manager.repo.get_order(order_id or str(order.id)) or order
            )
            if (
                current.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
                or not current.client_order_id
            ):
                self.order_manager.mark_submitted(current, exchange_order_id)
            elif not current.exchange_order_id:
                self.order_manager.update_exchange_order_id(current, exchange_order_id)

    @staticmethod
    def _fill_delta_from_cumulative(
        *,
        local_filled: Decimal,
        local_average_price: Decimal | None,
        cumulative_filled: Decimal,
        cumulative_average_price: Decimal | None,
    ) -> FillDelta:
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
        """Request cancellation of a known order through the adapter.

        Adapters that deliver terminal cancellation through ordered events only
        acknowledge the request here. Other adapters complete the local
        terminal transition synchronously.
        """
        return cancel_known_order(
            repository=self.order_manager.repo,
            adapter=self.adapter,
            order_id=order_id,
            assert_external_operation_allowed=self._assert_external_operation_allowed,
            mark_cancelled=self.order_manager.mark_cancelled,
            fail_pending_conditional_orders_for_terminal_entry=(
                self._fail_pending_conditional_orders_for_terminal_entry
            ),
        )

    def flatten_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        quantity: Decimal,
        reference_price: Optional[Decimal] = None,
    ) -> Optional[str] | FlattenPending:
        """Close a live position with a reduce-only market order.

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

        active_flatten = self._active_flatten_order(product_id)
        if active_flatten is not None:
            self.logger.warning(
                "Reusing active kill-switch flatten order %s for %s",
                active_flatten.id,
                product_id,
            )
            if active_flatten.status == OrderStatus.SUBMITTED_UNCONFIRMED.value:
                return FlattenPending(
                    str(active_flatten.id),
                    "submission_unconfirmed",
                )
            return str(active_flatten.id)

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
        try:
            self._validate_order_group([order])
        except ExchangeError as e:
            self.logger.error("Flatten order validation failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            self._record_order_rejection(
                order=order,
                order_type="market",
                error=e,
                phase="kill_switch_validation",
            )
            return None

        submit_attempted = False
        try:
            self._assert_external_operation_allowed()
            self.order_manager.mark_submitted_unconfirmed(order)
            submit_attempted = True
            exchange_id = self.adapter.place_order(order)
            self._record_order_ack(order, exchange_id, order_id=order_id)
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
                return FlattenPending(order_id, str(adoption["action"]))
            self.order_manager.fail_order(order, str(e))
            self._record_order_rejection(
                order=order,
                order_type="market",
                error=e,
                phase="kill_switch_flatten",
            )
            return None

    def exit_authoritative_position(
        self,
        product_id: str,
        *,
        account_id: str,
    ) -> bool:
        """Exit one server-side position without deriving side or quantity locally."""
        self._assert_external_operation_allowed()
        return self.adapter.exit_authoritative_position(
            product_id,
            account_id=account_id,
        )

    def _active_flatten_order(self, product_id: str):
        active_statuses = {
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        for order in self.order_manager.repo.list_orders_by_statuses(active_statuses):
            if order.product_id != product_id:
                continue
            payload = order.intent_payload
            if isinstance(payload, dict) and payload.get("source") == "kill_switch":
                return order
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
        if self._db_session is None:
            raise RuntimeError("ExecutionEngine requires a database session")
        ensure(self._db_session)

    def halt_and_drain(self, timeout: float = 30.0) -> bool:
        """Halt new submissions and wait for in-flight ones to complete.

        Sets the submission gate so that any subsequent call to execute_signal
        or _place_pending_conditional_orders_for_entry is rejected immediately.
        Then waits up to *timeout* seconds for in-flight submissions to finish.

        Returns True if all in-flight submissions completed within the timeout,
        False if the timeout was reached with work still in flight.
        """
        return self._submission_gate_owner.halt_and_drain(timeout=timeout)

    def run_when_submissions_drained(self, callback: Callable[[], None]) -> None:
        self._submission_gate_owner.run_when_submissions_drained(callback)

    def resume_submissions(self) -> None:
        self._submission_gate_owner.resume_submissions()

    def halt_for_reconcile(self, timeout: float = 0.0) -> bool:
        """Raise the independent reconcile gate and drain in-flight submissions.

        Separate from the kill-switch halt so that resuming after reconcile can
        never clear an active kill-switch halt. Returns True if no submissions
        were in flight within *timeout*.
        """
        return self._submission_gate_owner.halt_for_reconcile(timeout=timeout)

    def resume_after_reconcile(self) -> None:
        """Clear only the reconcile gate; leaves any kill-switch halt untouched."""
        self._submission_gate_owner.resume_after_reconcile()

    def latch_order_event_stream_failure(self) -> None:
        """Permanently reject money-path operations after stream ownership fails."""
        self._submission_gate_owner.latch_order_event_stream_failure()

    def modify_protection(
        self,
        entry_order_id: str,
        *,
        stop_loss: Decimal | None = None,
        take_profit: Decimal | None = None,
    ) -> dict[str, str]:
        requested = [
            ("stop_loss", stop_loss),
            ("take_profit", take_profit),
        ]
        requested = [
            (leg_type, price) for leg_type, price in requested if price is not None
        ]
        if len(requested) != 1:
            raise ExchangeError("modify_protection_requires_exactly_one_leg")
        if self._submission_gate_owner.try_begin_submission() is not None:
            raise ExchangeError("modify_protection_submission_gate_halted")
        try:
            leg_type, price = requested[0]
            return modify_attached_protection(
                repository=cast(
                    ProtectionModificationRepository,
                    self.order_manager.repo,
                ),
                adapter=cast(ProtectionModificationAdapter, self.adapter),
                clock=self.clock,
                order_event_lock=self._order_event_apply_lock,
                assert_operation_allowed=self._assert_external_operation_allowed,
                halt_for_reconcile=self.halt_for_reconcile,
                entry_order_id=entry_order_id,
                leg_type=leg_type,
                price=price,
            )
        except NetworkError:
            self._submission_gate_owner.claim_reconcile_halt()
            raise
        finally:
            self._finish_submission()

    def _finish_submission(self) -> None:
        self._submission_gate_owner.finish_submission()

    def execute_signal(
        self, signal: Signal, candle: Optional[Candlestick] = None
    ) -> Optional[str]:
        """
        Converts Signal to Order and delegates execution to the Adapter.
        Also places SL/TP/Trailing orders when specified in the signal.
        Returns the Order ID (Internal) if successful.
        """
        # Gate: reject if the submission gate has been halted by a kill switch
        # or while owned-order reconciliation runs after a reconnect.
        admission_rejection = self._submission_gate_owner.try_begin_submission()
        if admission_rejection is not None:
            cause = (
                "kill switch active"
                if admission_rejection == "kill_switch_halted"
                else "reconnect reconcile"
            )
            self.logger.warning(
                "execute_signal rejected: submission gate is halted (%s)", cause
            )
            return None
        try:
            self._assert_external_operation_allowed()
            normalized_signal = self._normalize_signal_quantity_or_reject(
                signal,
                candle,
            )
            if normalized_signal is None:
                return None
            signal = normalized_signal
            exit_decision = self._classify_exit_signal(signal)
            if exit_decision is not None and not exit_decision.allowed:
                self.logger.warning(
                    "EXIT signal not submitted: strategy=%s product=%s reason=%s",
                    signal.strategy_id,
                    signal.product_id,
                    exit_decision.reason,
                )
                self._audit_non_submission(
                    signal,
                    candle,
                    exit_decision.reason,
                )
                return None
            if self.audit_external_orders:
                return self._execute_signal_with_audit(
                    signal,
                    candle,
                    exit_decision=exit_decision,
                )
            return self._execute_signal_core(
                signal,
                candle,
                exit_decision=exit_decision,
            )
        finally:
            self._finish_submission()

    def _execute_signal_core(
        self,
        signal: Signal,
        candle: Optional[Candlestick] = None,
        *,
        exit_decision: ExitDecision | None = None,
    ) -> Optional[str]:
        """Current non-audited signal execution path."""
        side = self._determine_side(signal.type)
        if not side:
            return None

        # Determine Quantity
        qty = self._quantity_for_signal(signal, exit_decision=exit_decision)

        resolved_intent = self._resolve_order_intent_or_reject(signal, candle)
        if resolved_intent is None:
            return None
        order_type = resolved_intent.order_type
        limit_price = resolved_intent.limit_price

        # 1. Create Entry Order in DB
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type=order_type,
            quantity=qty,
            price=limit_price,
            intent_payload=self._signal_order_intent(signal, resolved_intent) or None,
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
            exchange_id, atomic_group = self._place_entry_order(
                order,
                conditional_orders,
            )
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            if not atomic_group:
                self.order_manager.update_exchange_order_id(order, exchange_id)
            self.logger.info(
                "Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id
            )
            ORDERS_TOTAL.labels(
                order_type=order_type,
                status="placed",
                reason="none",
            ).inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            for conditional_order in conditional_orders:
                self.order_manager.fail_order(
                    conditional_order, "entry_placement_failed"
                )
            self._record_order_rejection(
                order=order,
                order_type=order_type,
                error=e,
                phase="entry_placement",
            )
            return None

        # 3. Journal: record entry
        self._journal_entry(signal, order, side, order_type)

        # 4. Place conditional orders (SL/TP/Trailing)
        if conditional_orders and not atomic_group:
            self._place_conditional_orders(conditional_orders)

        return order.id

    def _execute_signal_with_audit(
        self,
        signal: Signal,
        candle: Optional[Candlestick] = None,
        *,
        exit_decision: ExitDecision | None = None,
    ) -> Optional[str]:
        """Fail-stop external execution path with committed intent/outcome audits."""
        if self._db_session_factory is None:
            raise RuntimeError("audit_external_orders requires db_session_factory")

        side = self._determine_side(signal.type)
        if not side:
            return None

        qty = self._quantity_for_signal(signal, exit_decision=exit_decision)
        resolved_intent = self._resolve_order_intent_or_reject(signal, candle)
        if resolved_intent is None:
            return None
        order_type = resolved_intent.order_type
        limit_price = resolved_intent.limit_price

        client_order_id = self._client_order_id_for_signal(signal)
        existing_order = self.order_manager.repo.get_order_by_client_order_id(
            client_order_id
        )
        if existing_order is not None:
            self.logger.info(
                "Order already exists for client_order_id=%s", client_order_id
            )
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
                **self._signal_order_intent(signal, resolved_intent),
            },
            **self._signal_order_intent(signal, resolved_intent),
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
        order_id = str(order.id)
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
        atomic_group = False
        try:
            self._validate_order_group([order, *conditional_orders])
            atomic_group = self._supports_atomic_order_group(
                [order, *conditional_orders]
            )
            self.order_manager.mark_submitted_unconfirmed(order)
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            submit_attempted = True
            exchange_id, atomic_group = self._place_entry_order(
                order,
                conditional_orders,
            )
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            if not atomic_group:
                self._record_order_ack(order, exchange_id, order_id=order_id)
            self.logger.info(
                "Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id
            )
            ORDERS_TOTAL.labels(
                order_type=order_type,
                status="placed",
                reason="none",
            ).inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            if atomic_group and isinstance(e, NetworkError):
                self._submission_gate_owner.claim_reconcile_halt()
                adoption = {
                    "action": "verification_blocked_native_bracket_submit",
                    "verification_blocked": True,
                    "unresolved": True,
                }
            else:
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
                    execution_failure_diagnostics.write_order_rejection_event(
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
                    execution_failure_diagnostics.write_order_rejection_event(
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

        self._journal_entry(signal, order, side, order_type)

        return order.id

    def _adopt_order_after_ambiguous_submit_error(
        self,
        order,
        error: ExchangeError,
        *,
        submit_attempted: bool,
    ) -> dict[str, object]:
        return adopt_order_after_ambiguous_submit_error(
            adapter=self.adapter,
            process_exchange_order_event=self.process_exchange_order_event,
            order=order,
            error=error,
            submit_attempted=submit_attempted,
        )

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

    def _place_entry_order(
        self, entry_order, conditional_orders: list
    ) -> tuple[str, bool]:
        orders = [entry_order, *conditional_orders]
        atomic_group = self._supports_atomic_order_group(orders)
        if not atomic_group:
            self._assert_external_operation_allowed()
            return self.adapter.place_order(entry_order), False

        with self._order_event_apply_lock:
            for order in orders:
                if order.client_order_id:
                    self.order_manager.mark_submitted_unconfirmed(order)
            self._assert_external_operation_allowed()
            exchange_id = self.adapter.place_order_group(orders)
            try:
                self._record_order_ack(
                    entry_order,
                    exchange_id,
                    order_id=str(entry_order.id),
                )
                for conditional_order in conditional_orders:
                    current = (
                        self.order_manager.repo.get_order(str(conditional_order.id))
                        or conditional_order
                    )
                    payload = dict(current.intent_payload or {})
                    payload.update(
                        {
                            "native_parent_basket_id": str(exchange_id),
                            "native_parent_client_order_id": str(
                                entry_order.client_order_id
                            ),
                        }
                    )
                    current.intent_payload = payload
                    self.order_manager.repo.update_order(current)
                    if current.status == OrderStatus.SUBMITTED_UNCONFIRMED.value:
                        self.order_manager.mark_submitted(current)
            except Exception:
                self.halt_for_reconcile()
                raise
            return exchange_id, True

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

    def _quantity_for_signal(
        self,
        signal: Signal,
        *,
        exit_decision: ExitDecision | None = None,
    ) -> Decimal:
        if exit_decision is not None and exit_decision.quantity is not None:
            return exit_decision.quantity
        if signal.quantity and signal.quantity > 0:
            return signal.quantity
        return self.default_quantity

    def _classify_exit_signal(self, signal: Signal) -> ExitDecision | None:
        if signal.type not in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            return None
        try:
            position = self._load_strategy_position(signal)
        except Exception as error:
            self._submission_gate_owner.claim_reconcile_halt()
            self.logger.error(
                "EXIT position lookup failed; submissions halted: "
                "strategy=%s product=%s error=%s",
                signal.strategy_id,
                signal.product_id,
                error,
            )
            return ExitDecision(False, "position_unknown")
        if position is None:
            return ExitDecision(False, "already_flat")

        position_side = getattr(position.side, "value", position.side)
        expected_side = (
            PositionSide.LONG.value
            if signal.type == SignalType.EXIT_LONG
            else PositionSide.SHORT.value
        )
        if position_side != expected_side:
            self._submission_gate_owner.claim_reconcile_halt()
            return ExitDecision(False, "position_side_mismatch")

        position_quantity = Decimal(str(position.quantity))
        if position_quantity <= 0:
            return ExitDecision(False, "already_flat")
        requested_quantity = (
            signal.quantity
            if signal.quantity is not None and signal.quantity > 0
            else position_quantity
        )
        return ExitDecision(
            True,
            "position_matched",
            min(requested_quantity, position_quantity),
            position_quantity,
        )

    def _load_strategy_position(self, signal: Signal):
        if self._position_loader is not None:
            return self._position_loader(signal.strategy_id, signal.product_id)
        try:
            return self.adapter.get_position(
                signal.product_id,
                strategy_id=signal.strategy_id,
            )
        except TypeError:
            return self.adapter.get_position(signal.product_id)

    def _audit_non_submission(
        self,
        signal: Signal,
        candle: Optional[Candlestick],
        reason: str,
    ) -> None:
        if not self.audit_external_orders:
            return
        if self._db_session_factory is None:
            raise RuntimeError("audit_external_orders requires db_session_factory")
        audit = build_signal_audit(
            clock=self.clock,
            signal=signal,
            candle=candle,
            risk_passed=False,
            risk_message=reason,
            order_id=None,
        )
        with self._db_session_factory() as db:
            commit_signal_audit(db, audit)

    @staticmethod
    def _signal_order_intent(
        signal: Signal,
        resolved_intent: ResolvedOrderIntent,
    ) -> dict[str, object]:
        payload: dict[str, object] = {}
        if signal.type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            payload.update({"reduce_only": True, "source": "strategy_exit"})
        if resolved_intent.uses_legacy_value_fallback:
            payload["legacy_price_source"] = "signal.value"
        return payload

    def _resolve_order_intent_or_reject(
        self,
        signal: Signal,
        candle: Optional[Candlestick],
    ) -> ResolvedOrderIntent | None:
        try:
            return resolve_signal_order_intent(signal)
        except InvalidSignalOrderIntent as exc:
            reason = str(exc)
            self.logger.warning(
                "Signal order intent rejected: strategy=%s product=%s reason=%s",
                signal.strategy_id,
                signal.product_id,
                reason,
            )
            self._audit_non_submission(signal, candle, reason)
            return None

    def _normalize_signal_quantity_or_reject(
        self,
        signal: Signal,
        candle: Optional[Candlestick],
    ) -> Signal | None:
        try:
            return normalize_signal_quantity(
                signal,
                default_entry_quantity=self.default_quantity,
            )
        except InvalidSignalOrderIntent as exc:
            reason = str(exc)
            self.logger.warning(
                "Signal quantity rejected: strategy=%s product=%s reason=%s",
                signal.strategy_id,
                signal.product_id,
                reason,
            )
            self._audit_non_submission(signal, candle, reason)
            return None

    def _create_conditional_orders(
        self,
        signal: Signal,
        entry_order,
        qty: Decimal,
        candle: Optional[Candlestick],
    ) -> list:
        return self._conditional_order_lifecycle.create_orders(
            signal=signal,
            entry_order=entry_order,
            quantity=qty,
            candle=candle,
            attach_min_notional_reference_price=(
                self._attach_min_notional_reference_price
            ),
        )

    def _place_pending_conditional_orders_for_entry(self, entry_order) -> list[dict]:
        return self._conditional_order_lifecycle.place_pending_for_entry(entry_order)

    def place_pending_protection_for_filled_entries(self) -> dict[str, object]:
        return self._conditional_order_lifecycle.recover_pending_protection()

    @staticmethod
    def _protective_partial_fill_requires_resize(
        order, event_state: str
    ) -> dict | None:
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

    def _cancel_linked_conditional_order_for_protection_fill(
        self, order
    ) -> dict | None:
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        if order.intent_payload.get("placement_mode") == "attach-at-entry":
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
        validate_group = getattr(type(self.adapter), "validate_order_group", None)
        if validate_group is not None:
            validate_group(self.adapter, orders)
            for order in orders:
                self.order_manager.repo.update_order(order)
            return
        validate_order = getattr(self.adapter, "validate_order", None)
        if validate_order is None:
            return
        for order in orders:
            validate_order(order)
            self.order_manager.repo.update_order(order)

    def _supports_atomic_order_group(self, orders: list) -> bool:
        supports_group = getattr(
            type(self.adapter), "supports_atomic_order_group", None
        )
        return bool(supports_group and supports_group(self.adapter, orders))

    def _record_order_rejection(
        self,
        *,
        order,
        order_type: str,
        error: ExchangeError,
        phase: str,
        write_event: bool = True,
    ) -> str:
        return execution_failure_diagnostics.record_order_rejection(
            db_session_factory=self._db_session_factory,
            logger=self.logger,
            order=order,
            order_type=order_type,
            error=error,
            phase=phase,
            write_event=write_event,
        )

    def _place_conditional_orders(self, conditional_orders: list) -> list[dict]:
        return self._conditional_order_lifecycle.place_orders(conditional_orders)

    def _try_write_conditional_order_event_warning(
        self,
        *,
        event_subtype: str,
        order,
        failures: list[dict],
    ) -> None:
        execution_failure_diagnostics.try_write_conditional_order_event_warning(
            db_session_factory=self._db_session_factory,
            logger=self.logger,
            event_subtype=event_subtype,
            order=order,
            failures=failures,
        )

    def _journal_entry(self, signal, order, side, order_type) -> None:
        if self.journal is None:
            return
        execution_journal.journal_entry(
            self.journal,
            signal,
            order,
            side,
            order_type,
        )

    def _journal_fill(
        self,
        order,
        price,
        qty,
        fee,
        fill_type: str,
        candle: Optional[Candlestick] = None,
    ) -> None:
        execution_journal.journal_fill(
            self.journal,
            order,
            price,
            qty,
            fee,
            fill_type,
            candle,
        )

    def _journal_exchange_order_event_fill(
        self,
        order,
        event: ExchangeOrderEvent,
        fill_price: Decimal,
        fill_quantity: Decimal,
    ) -> None:
        execution_journal.journal_exchange_order_event_fill(
            self.journal,
            self.clock,
            order,
            event,
            fill_price,
            fill_quantity,
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
