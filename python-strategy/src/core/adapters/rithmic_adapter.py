import logging
import threading
from decimal import Decimal, InvalidOperation
from typing import Callable

from src.core.adapters.rithmic_ledger_position import (
    project_rithmic_ledger_positions,
)
from src.core.adapters.rithmic_native_bracket import (
    NativeBracketPlan,
    build_native_bracket_plan,
    build_native_protection_request,
    build_restored_native_bracket_groups,
    merge_native_bracket_groups,
    resolve_native_bracket_event_client_order_id,
    supports_native_bracket_group,
)
from src.core.adapters.rithmic_order_observation import (
    RithmicUnmappedOrderEvent as RithmicUnmappedOrderEvent,
    project_rithmic_order_event,
    project_rithmic_order_snapshot,
    resolve_rithmic_order_event_identity,
)
from src.core.interfaces.exchange import (
    EntryAdmissionGate,
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    IExchangeAdapter,
    NetworkError,
    OwnedOrderReconciliationContext,
    OwnedOrderReconciler,
)
from src.core.models import Position
from src.core.orm_models import Order
from src.core.rithmic_publisher_liveness_gate import RithmicPublisherLivenessGate
from src.core.runtime_environment import RuntimeEnvironment
from src.core.product_registry import (
    InstrumentSpec,
    instrument_spec_from_product,
    quantize_order_values,
    to_rithmic_symbol,
)


class RithmicExchangeAdapter(IExchangeAdapter):
    """Rithmic ORDER adapter with explicit startup after ledger recovery."""

    requires_runtime_capabilities = True

    def __init__(
        self,
        *,
        profile: str,
        account_id: str | None,
        instruments: dict[str, dict],
        client_factory: Callable | None = None,
    ):
        if not profile.strip():
            raise ExchangeError("rithmic_profile_required")
        if not isinstance(account_id, str) or not account_id.strip():
            raise ExchangeError("rithmic_account_id_required")
        if not instruments:
            raise ExchangeError("rithmic_instruments_required")
        self.profile = profile
        self.account_id = account_id.strip()
        self.logger = logging.getLogger("RithmicAdapter")
        self._client_factory = client_factory
        self._client = None
        self._client_lock = threading.Lock()
        self._client_order_ids_lock = threading.Lock()
        self._submitted_client_order_ids: set[str] = set()
        self._native_brackets_by_parent: dict[str, dict[str, str]] = {}
        self._native_bracket_parent_client_order_ids: set[str] = set()
        self._instrument_specs: dict[str, InstrumentSpec] = {}
        self._route_exchanges: dict[str, str] = {}
        self._products_by_native_identity: dict[tuple[str, str], str] = {}
        for product_id, raw in instruments.items():
            route_exchange = str(raw.get("exchange") or "").strip().upper()
            if not route_exchange:
                raise ExchangeError(
                    f"rithmic_instrument_exchange_required: product_id={product_id}"
                )
            try:
                spec = instrument_spec_from_product(
                    product_id,
                    quantity_step=_required_decimal(raw, "quantity_step", product_id),
                    price_tick=_required_decimal(raw, "price_tick", product_id),
                    multiplier=_optional_decimal(raw, "multiplier", product_id),
                    tick_value=_optional_decimal(raw, "tick_value", product_id),
                    session_calendar_id=raw.get("session_calendar_id"),
                )
                native_symbol = to_rithmic_symbol(product_id)
            except ValueError as error:
                raise ExchangeError(
                    f"invalid_rithmic_instrument: product_id={product_id}: {error}"
                ) from error
            identity = (route_exchange, native_symbol)
            if identity in self._products_by_native_identity:
                raise ExchangeError(
                    f"duplicate_rithmic_native_instrument: exchange={route_exchange} "
                    f"symbol={native_symbol}"
                )
            self._instrument_specs[product_id] = spec
            self._route_exchanges[product_id] = route_exchange
            self._products_by_native_identity[identity] = product_id

    @classmethod
    def from_config(cls, config: dict) -> "RithmicExchangeAdapter":
        return cls(
            profile=str(config.get("rithmic_profile") or ""),
            account_id=config.get("account_id"),
            instruments=config.get("rithmic_instruments") or {},
        )

    def create_entry_admission_gate(
        self,
        environment: RuntimeEnvironment,
        *,
        logger: logging.Logger,
    ) -> EntryAdmissionGate:
        return RithmicPublisherLivenessGate.for_environment(
            environment,
            logger=logger,
        )

    def create_owned_order_reconciler(
        self,
        context: OwnedOrderReconciliationContext,
    ) -> OwnedOrderReconciler:
        from src.core.adapters.rithmic_owned_order_reconciliation import (
            RithmicOwnedOrderReconciler,
        )

        return RithmicOwnedOrderReconciler(
            adapter=self,
            profile=self.profile,
            account_id=self.account_id,
            context=context,
        )

    def start_order_event_stream(self) -> None:
        with self._client_lock:
            if self._client is not None:
                return
            factory = self._client_factory
            if factory is None:
                from fluxtrade_core import RithmicOrderClient

                factory = RithmicOrderClient
            try:
                self._client = factory(self.profile, self.account_id)
            except RuntimeError as error:
                raise NetworkError(f"rithmic_order_start_failed: {error}") from error

    def close(self) -> None:
        with self._client_lock:
            self._client = None

    def place_order(self, order: Order) -> str:
        self.validate_order(order)
        client_order_id = str(order.client_order_id)
        with self._client_order_ids_lock:
            if client_order_id in self._submitted_client_order_ids:
                raise ExchangeError(
                    f"duplicate_rithmic_client_order_id: {client_order_id}"
                )
            self._submitted_client_order_ids.add(client_order_id)
        try:
            with self._client_lock:
                ack = self._require_client().submit(
                    client_order_id,
                    self._route_exchanges[order.product_id],
                    to_rithmic_symbol(order.product_id),
                    str(order.quantity),
                    _order_side(order),
                    str(order.type).lower(),
                    str(order.price) if order.price is not None else None,
                )
        except RuntimeError as error:
            mapped = _map_runtime_error("rithmic_order_submit_failed", error)
            if not isinstance(mapped, NetworkError):
                with self._client_order_ids_lock:
                    self._submitted_client_order_ids.discard(client_order_id)
            raise mapped from error
        return str(ack.basket_id)

    def validate_order_group(self, orders: list[Order]) -> None:
        if not self.supports_atomic_order_group(orders):
            for order in orders:
                self.validate_order(order)
            return
        self._native_bracket_plan(orders, persist=True)

    def supports_atomic_order_group(self, orders: list[Order]) -> bool:
        return supports_native_bracket_group(orders)

    def place_order_group(self, orders: list[Order]) -> str:
        plan = self._native_bracket_plan(orders, persist=True)
        entry = plan["entry"]
        client_order_id = str(entry.client_order_id)
        with self._client_order_ids_lock:
            if client_order_id in self._submitted_client_order_ids:
                raise ExchangeError(
                    f"duplicate_rithmic_client_order_id: {client_order_id}"
                )
            self._submitted_client_order_ids.add(client_order_id)
        try:
            with self._client_lock:
                self._native_bracket_parent_client_order_ids.add(client_order_id)
                ack = self._require_client().submit_bracket(
                    client_order_id,
                    self._route_exchanges[entry.product_id],
                    to_rithmic_symbol(entry.product_id),
                    str(entry.quantity),
                    _order_side(entry),
                    str(entry.type).lower(),
                    str(entry.price) if entry.price is not None else None,
                    plan["stop_ticks"],
                    plan["target_ticks"],
                )
                basket_id = str(ack.basket_id)
                self._native_brackets_by_parent[basket_id] = {
                    "entry": client_order_id,
                    **plan["leg_client_order_ids"],
                }
        except RuntimeError as error:
            mapped = _map_runtime_error("rithmic_bracket_submit_failed", error)
            if not isinstance(mapped, NetworkError):
                with self._client_order_ids_lock:
                    self._submitted_client_order_ids.discard(client_order_id)
                with self._client_lock:
                    self._native_bracket_parent_client_order_ids.discard(
                        client_order_id
                    )
            raise mapped from error
        return basket_id

    def restore_order_groups(self, orders: list[Order]) -> None:
        restored = build_restored_native_bracket_groups(orders)
        with self._client_lock:
            (
                self._native_brackets_by_parent,
                self._native_bracket_parent_client_order_ids,
            ) = merge_native_bracket_groups(
                self._native_brackets_by_parent,
                self._native_bracket_parent_client_order_ids,
                restored,
            )

    def validate_order(self, order: Order) -> None:
        intent_payload = getattr(order, "intent_payload", None)
        if (
            isinstance(intent_payload, dict)
            and intent_payload.get("reduce_only") is True
        ):
            raise ExchangeError("rithmic_reduce_only_unsupported")
        spec = self.get_instrument_spec(order.product_id)
        order_type = str(order.type or "").lower()
        if order_type not in {"market", "limit"}:
            raise ExchangeError(
                f"rithmic_order_type_unsupported: order_type={order_type}"
            )
        if not order.client_order_id:
            raise ExchangeError("rithmic_client_order_id_required")
        try:
            values = quantize_order_values(
                quantity=Decimal(str(order.quantity)),
                price=Decimal(str(order.price)) if order.price is not None else None,
                side=_order_side(order),
                order_type=order_type,
                spec=spec,
            )
        except ValueError as error:
            raise ExchangeError(f"rithmic_order_validation_failed: {error}") from error
        order.quantity = values.quantity
        order.price = values.price

    def _native_bracket_plan(
        self,
        orders: list[Order],
        *,
        persist: bool,
    ) -> NativeBracketPlan[Order]:
        return build_native_bracket_plan(
            orders,
            validate_order=self.validate_order,
            get_instrument_spec=self.get_instrument_spec,
            order_side=_order_side,
            persist=persist,
        )

    def modify_protection(self, order: Order, *, trigger_price: Decimal) -> bool:
        request = build_native_protection_request(
            order,
            trigger_price,
            get_instrument_spec=self.get_instrument_spec,
        )
        try:
            with self._client_lock:
                return bool(
                    self._require_client().modify_protection(
                        request.basket_id,
                        self._route_exchanges[request.product_id],
                        to_rithmic_symbol(request.product_id),
                        request.quantity,
                        request.leg_type,
                        request.price,
                    )
                )
        except RuntimeError as error:
            raise _map_runtime_error(
                "rithmic_protection_modify_failed", error
            ) from error

    def cancel_order(
        self,
        order_id: str,
        product_id: str,
        *,
        order_type: str | None = None,
    ) -> bool:
        self.get_instrument_spec(product_id)
        try:
            with self._client_lock:
                return bool(self._require_client().cancel(str(order_id)))
        except RuntimeError as error:
            raise _map_runtime_error("rithmic_order_cancel_failed", error) from error

    def cancel_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: str | None = None,
    ) -> bool:
        snapshot = self.get_order_by_client_id(
            client_order_id,
            product_id,
            order_type=order_type,
        )
        if snapshot is None:
            return False
        if snapshot.status in {"filled", "cancelled", "rejected"}:
            return False
        return self.cancel_order(
            snapshot.exchange_order_id,
            product_id,
            order_type=order_type,
        )

    def cancel_terminal_state_delivered_by_order_events(self) -> bool:
        return True

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: str | None = None,
    ) -> ExchangeOrderSnapshot | None:
        self.get_instrument_spec(product_id)
        try:
            with self._client_lock:
                remote = self._require_client().lookup(
                    str(client_order_id),
                    self._route_exchanges[product_id],
                    to_rithmic_symbol(product_id),
                )
        except RuntimeError as error:
            raise _map_runtime_error("rithmic_order_lookup_failed", error) from error
        if remote is None:
            return None
        return project_rithmic_order_snapshot(remote, account_id=self.account_id)

    def poll_order_event(self) -> ExchangeOrderEvent | None:
        try:
            with self._client_lock:
                event = self._require_client().poll_event()
        except RuntimeError as error:
            raise _map_runtime_error("rithmic_order_event_failed", error) from error
        if event is None:
            return None
        product_id, native_identity = resolve_rithmic_order_event_identity(
            event,
            account_id=self.account_id,
            products_by_native_identity=self._products_by_native_identity,
        )
        client_order_id = event.client_order_id
        original_basket_id = event.original_basket_id
        basket_id = str(event.basket_id)
        with self._client_lock:
            client_order_id = resolve_native_bracket_event_client_order_id(
                client_order_id=client_order_id,
                basket_id=basket_id,
                original_basket_id=original_basket_id,
                price_type=event.price_type,
                groups=self._native_brackets_by_parent,
                parent_ids=self._native_bracket_parent_client_order_ids,
            )
        return project_rithmic_order_event(
            event,
            product_id=product_id,
            client_order_id=client_order_id,
            native_identity=native_identity,
        )

    def get_instrument_spec(self, product_id: str) -> InstrumentSpec:
        try:
            return self._instrument_specs[product_id]
        except KeyError as error:
            raise ExchangeError(
                f"rithmic_instrument_not_configured: product_id={product_id}"
            ) from error

    def get_balance(self, asset: str) -> Decimal:
        raise ExchangeError("rithmic_live_balance_unavailable")

    def get_position(self, product_id: str) -> Position | None:
        self.get_instrument_spec(product_id)
        raise ExchangeError("rithmic_live_position_unavailable")

    @property
    def configured_product_ids(self) -> tuple[str, ...]:
        return tuple(self._instrument_specs)

    def exit_position(self, product_id: str) -> bool:
        """Ask Rithmic to exit the server-side position for one instrument."""
        self.get_instrument_spec(product_id)
        try:
            with self._client_lock:
                return bool(
                    self._require_client().exit_position(
                        self._route_exchanges[product_id],
                        to_rithmic_symbol(product_id),
                    )
                )
        except RuntimeError as error:
            raise _map_runtime_error(
                "rithmic_exit_position_failed",
                error,
            ) from error

    def exit_authoritative_position(
        self,
        product_id: str,
        *,
        account_id: str,
    ) -> bool:
        """Exit this adapter account's server-side position."""
        if self.account_id.strip() != account_id.strip():
            raise ExchangeError("authoritative_position_exit_account_mismatch")
        if not self.exit_position(product_id):
            raise ExchangeError("authoritative_position_exit_returned_false")
        return True

    def positions_from_ledger_snapshot(self, snapshot) -> list[Position]:
        """Convert one authoritative account snapshot into configured positions."""
        return project_rithmic_ledger_positions(
            snapshot,
            account_id=self.account_id,
            products_by_native_identity=self._products_by_native_identity,
        )

    def connection_generation(self) -> int:
        """Successful (re)connect count of the order session.

        A strictly higher value than a previously observed one means the order
        session reconnected in between; the engine uses this to trigger
        owned-order reconciliation after a mid-session disconnect.
        """
        with self._client_lock:
            return self._require_client().connection_generation()

    def _require_client(self):
        if self._client is None:
            raise NetworkError("rithmic_order_stream_not_started")
        return self._client


def _required_decimal(raw: dict, field: str, product_id: str) -> Decimal:
    value = _optional_decimal(raw, field, product_id)
    if value is None or value <= 0:
        raise ValueError(f"{field}_must_be_positive")
    return value


def _optional_decimal(raw: dict, field: str, product_id: str) -> Decimal | None:
    value = raw.get(field)
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"invalid_{field}: product_id={product_id}") from error
    if not parsed.is_finite():
        raise ValueError(f"invalid_{field}: product_id={product_id}")
    return parsed


def _order_side(order: Order) -> str:
    side = getattr(order.side, "value", order.side)
    normalized = str(side).lower()
    if normalized not in {"buy", "sell"}:
        raise ExchangeError(f"rithmic_order_side_unsupported: side={normalized}")
    return normalized


def _map_runtime_error(prefix: str, error: RuntimeError) -> ExchangeError:
    message = str(error)
    ambiguous_markers = (
        "ambiguous",
        "disconnected",
        "reconnecting",
        "timed out",
        "stopped",
    )
    if any(marker in message.lower() for marker in ambiguous_markers):
        return NetworkError(f"{prefix}: {message}")
    return ExchangeError(f"{prefix}: {message}")
