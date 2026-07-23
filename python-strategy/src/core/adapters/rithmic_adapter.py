import logging
import threading
from decimal import Decimal, InvalidOperation
from typing import Callable

from src.core.client_order_id import linked_client_order_id
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    IExchangeAdapter,
    NetworkError,
)
from src.core.models import Position
from src.core.orm_models import Order
from src.core.product_registry import (
    InstrumentSpec,
    instrument_spec_from_product,
    quantize_order_values,
    to_rithmic_symbol,
)


class RithmicUnmappedOrderEvent(ExchangeError):
    """Account-level order event whose instrument is not locally configured."""

    def __init__(self, *, account_id: str, exchange: str, symbol: str):
        self.account_id = account_id
        self.exchange = exchange
        self.symbol = symbol
        super().__init__(
            "unknown_rithmic_order_event_instrument: "
            f"account_id={account_id} exchange={exchange} symbol={symbol}"
        )


class RithmicExchangeAdapter(IExchangeAdapter):
    """Rithmic ORDER adapter with explicit startup after ledger recovery."""

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
        return any(
            str(getattr(order, "type", "")).lower()
            in {"stop_loss", "take_profit", "trailing_stop"}
            for order in orders
        )

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
        restored: dict[str, dict[str, str]] = {}
        for order in orders:
            payload = getattr(order, "intent_payload", None)
            if (
                not isinstance(payload, dict)
                or payload.get("placement_mode") != "attach-at-entry"
            ):
                continue
            parent_basket_id = payload.get("native_parent_basket_id")
            if not parent_basket_id:
                continue
            parent_client_order_id = payload.get("native_parent_client_order_id")
            leg_type = payload.get("native_leg_type")
            if not parent_client_order_id or leg_type not in {
                "stop_loss",
                "take_profit",
            }:
                raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
            group = restored.setdefault(
                str(parent_basket_id),
                {"entry": str(parent_client_order_id)},
            )
            if group["entry"] != str(parent_client_order_id):
                raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
            existing = group.get(str(leg_type))
            if existing is not None and existing != str(order.client_order_id):
                raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
            if not order.client_order_id:
                raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
            group[str(leg_type)] = str(order.client_order_id)

        for entry in orders:
            payload = getattr(entry, "intent_payload", None)
            native = payload.get("native_protection") if isinstance(payload, dict) else None
            if not isinstance(native, dict) or not entry.exchange_order_id:
                continue
            if not entry.client_order_id:
                raise ExchangeError("rithmic_native_bracket_parent_client_order_id_missing")
            legs = native.get("legs")
            if not isinstance(legs, dict) or not legs:
                raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
            group = {"entry": str(entry.client_order_id)}
            for leg_type in ("stop_loss", "take_profit"):
                leg = legs.get(leg_type)
                if leg is None:
                    continue
                client_order_id = leg.get("client_order_id") if isinstance(leg, dict) else None
                if not client_order_id:
                    raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
                group[leg_type] = str(client_order_id)
            existing = restored.get(str(entry.exchange_order_id))
            if existing is None:
                restored[str(entry.exchange_order_id)] = group
                continue
            for key, value in group.items():
                if key in existing and existing[key] != value:
                    raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
                existing[key] = value
        with self._client_lock:
            merged = {
                parent_basket_id: dict(group)
                for parent_basket_id, group in self._native_brackets_by_parent.items()
            }
            for parent_basket_id, group in restored.items():
                existing = merged.setdefault(parent_basket_id, {})
                for key, value in group.items():
                    if key in existing and existing[key] != value:
                        raise ExchangeError(
                            "rithmic_native_bracket_restore_metadata_conflict"
                        )
                    existing[key] = value
            self._native_brackets_by_parent = merged
            self._native_bracket_parent_client_order_ids.update(
                group["entry"] for group in restored.values()
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
    ) -> dict[str, object]:
        entries = [
            order
            for order in orders
            if str(getattr(order, "type", "")).lower() in {"market", "limit"}
        ]
        legs = [order for order in orders if order not in entries]
        if len(entries) != 1 or not legs:
            raise ExchangeError("rithmic_native_bracket_group_invalid")
        entry = entries[0]
        self.validate_order(entry)
        if Decimal(str(entry.quantity)) != Decimal("1"):
            raise ExchangeError("rithmic_native_bracket_single_contract_required")

        spec = self.get_instrument_spec(entry.product_id)
        if spec.price_tick is None or spec.price_tick <= 0:
            raise ExchangeError("rithmic_native_bracket_price_tick_required")
        reference_price = (
            Decimal(str(entry.price))
            if entry.price is not None
            else _finite_positive_decimal(
                getattr(entry, "min_notional_reference_price", None),
                "rithmic_native_bracket_reference_price_required",
            )
        )
        if reference_price % spec.price_tick != 0:
            raise ExchangeError("rithmic_native_bracket_reference_price_off_tick")

        by_type: dict[str, Order] = {}
        ticks: dict[str, int] = {}
        for leg in legs:
            leg_type = str(getattr(leg, "type", "")).lower()
            if leg_type not in {"stop_loss", "take_profit"}:
                raise ExchangeError(
                    f"rithmic_native_bracket_leg_unsupported: order_type={leg_type}"
                )
            if leg_type in by_type:
                raise ExchangeError(
                    f"rithmic_native_bracket_duplicate_leg: order_type={leg_type}"
                )
            if leg.product_id != entry.product_id:
                raise ExchangeError("rithmic_native_bracket_product_mismatch")
            if Decimal(str(leg.quantity)) != Decimal("1"):
                raise ExchangeError("rithmic_native_bracket_single_contract_required")
            entry_side = _order_side(entry)
            expected_side = "sell" if entry_side == "buy" else "buy"
            if _order_side(leg) != expected_side:
                raise ExchangeError("rithmic_native_bracket_close_side_mismatch")
            if not leg.client_order_id:
                raise ExchangeError("rithmic_native_bracket_leg_client_order_id_required")
            suffix = "sl" if leg_type == "stop_loss" else "tp"
            if str(leg.client_order_id) != linked_client_order_id(
                str(entry.client_order_id), suffix
            ):
                raise ExchangeError("rithmic_native_bracket_leg_client_order_id_mismatch")
            trigger = _finite_positive_decimal(
                getattr(leg, "trigger_price", None),
                f"rithmic_native_bracket_{leg_type}_price_required",
            )
            _validate_bracket_price_side(
                entry_side=entry_side,
                leg_type=leg_type,
                reference_price=reference_price,
                trigger_price=trigger,
            )
            distance_ticks = (trigger - reference_price).copy_abs() / spec.price_tick
            integral_ticks = distance_ticks.to_integral_value()
            if distance_ticks != integral_ticks:
                raise ExchangeError(
                    f"rithmic_native_bracket_{leg_type}_distance_off_tick"
                )
            if integral_ticks <= 0 or integral_ticks > 2_147_483_647:
                raise ExchangeError(
                    f"rithmic_native_bracket_{leg_type}_ticks_out_of_range"
                )
            by_type[leg_type] = leg
            ticks[leg_type] = int(integral_ticks)

        leg_client_order_ids = {
            leg_type: str(leg.client_order_id) for leg_type, leg in by_type.items()
        }
        bracket_type = (
            "target_and_stop_static"
            if len(by_type) == 2
            else (
                "stop_only_static"
                if "stop_loss" in by_type
                else "target_only_static"
            )
        )
        if persist:
            native = {
                "placement_mode": "attach-at-entry",
                "bracket_type": bracket_type,
                "reference_price": str(reference_price),
                "price_tick": str(spec.price_tick),
                "legs": {
                    leg_type: {
                        "order_id": str(leg.id),
                        "client_order_id": str(leg.client_order_id),
                        "requested_price": str(leg.trigger_price),
                        "ticks": str(ticks[leg_type]),
                    }
                    for leg_type, leg in by_type.items()
                },
            }
            entry_payload = dict(getattr(entry, "intent_payload", None) or {})
            entry_payload["native_protection"] = native
            entry.intent_payload = entry_payload
            for leg_type, leg in by_type.items():
                leg_payload = dict(getattr(leg, "intent_payload", None) or {})
                leg_payload.update(
                    {
                        "placement_mode": "attach-at-entry",
                        "native_leg_type": leg_type,
                        "native_bracket_type": bracket_type,
                        "reference_price": str(reference_price),
                        "requested_price": str(leg.trigger_price),
                        "price_tick": str(spec.price_tick),
                        "ticks": str(ticks[leg_type]),
                        "entry_side": _order_side(entry),
                        "native_parent_client_order_id": str(entry.client_order_id),
                    }
                )
                leg.intent_payload = leg_payload
        return {
            "entry": entry,
            "stop_ticks": ticks.get("stop_loss"),
            "target_ticks": ticks.get("take_profit"),
            "leg_client_order_ids": leg_client_order_ids,
        }

    def modify_protection(self, order: Order, *, trigger_price: Decimal) -> bool:
        payload = getattr(order, "intent_payload", None)
        if (
            not isinstance(payload, dict)
            or payload.get("placement_mode") != "attach-at-entry"
        ):
            raise ExchangeError("rithmic_native_protection_identity_required")
        leg_type = str(getattr(order, "type", "")).lower()
        if leg_type not in {"stop_loss", "take_profit"}:
            raise ExchangeError("rithmic_native_protection_leg_unsupported")
        if not order.exchange_order_id:
            raise ExchangeError("rithmic_native_protection_basket_id_required")
        if Decimal(str(order.quantity)) != Decimal("1"):
            raise ExchangeError("rithmic_native_bracket_single_contract_required")
        spec = self.get_instrument_spec(order.product_id)
        if spec.price_tick is None or spec.price_tick <= 0:
            raise ExchangeError("rithmic_native_bracket_price_tick_required")
        price = _finite_positive_decimal(
            trigger_price,
            "rithmic_native_protection_price_required",
        )
        if price % spec.price_tick != 0:
            raise ExchangeError("rithmic_native_protection_price_off_tick")
        reference_price = _finite_positive_decimal(
            payload.get("actual_entry_fill_price") or payload.get("reference_price"),
            "rithmic_native_protection_reference_price_required",
        )
        _validate_bracket_price_side(
            entry_side=str(payload.get("entry_side") or "").lower(),
            leg_type=leg_type,
            reference_price=reference_price,
            trigger_price=price,
        )
        try:
            with self._client_lock:
                return bool(
                    self._require_client().modify_protection(
                        str(order.exchange_order_id),
                        self._route_exchanges[order.product_id],
                        to_rithmic_symbol(order.product_id),
                        str(order.quantity),
                        leg_type,
                        str(price),
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
        quantity = Decimal(str(remote.quantity))
        filled_quantity = _event_decimal(remote.filled_quantity) or Decimal("0")
        status = _normalize_snapshot_status(
            str(remote.status),
            filled_quantity,
            quantity,
            notification_type=getattr(remote, "notification_type", None),
        )
        return ExchangeOrderSnapshot(
            client_order_id=str(remote.client_order_id),
            exchange_order_id=str(remote.basket_id),
            status=status,
            filled_quantity=filled_quantity,
            average_price=_event_decimal(remote.average_fill_price),
            raw={
                "basket_id": str(remote.basket_id),
                "exchange_order_id": remote.exchange_order_id,
                "quantity": str(remote.quantity),
                "account_id": self.account_id,
            },
        )

    def poll_order_event(self) -> ExchangeOrderEvent | None:
        try:
            with self._client_lock:
                event = self._require_client().poll_event()
        except RuntimeError as error:
            raise _map_runtime_error("rithmic_order_event_failed", error) from error
        if event is None:
            return None
        identity = (str(event.exchange).upper(), str(event.symbol).upper())
        product_id = self._products_by_native_identity.get(identity)
        if product_id is None:
            raise RithmicUnmappedOrderEvent(
                account_id=self.account_id,
                exchange=identity[0],
                symbol=identity[1],
            )
        client_order_id = event.client_order_id
        original_basket_id = event.original_basket_id
        basket_id = str(event.basket_id)
        with self._client_lock:
            if original_basket_id:
                group = self._native_brackets_by_parent.get(str(original_basket_id))
                if group is None:
                    client_order_id = None
                else:
                    if client_order_id not in {None, group["entry"]}:
                        raise ExchangeError(
                            "rithmic_native_bracket_child_client_id_mismatch"
                        )
                    leg_type = _bracket_leg_type(event.price_type)
                    client_order_id = group.get(leg_type) if leg_type else None
            elif basket_id in self._native_brackets_by_parent:
                group = self._native_brackets_by_parent[basket_id]
                if client_order_id not in {None, group["entry"]}:
                    raise ExchangeError(
                        "rithmic_native_bracket_parent_client_id_mismatch"
                    )
                client_order_id = group["entry"]
            elif client_order_id in self._native_bracket_parent_client_order_ids:
                # Native children can repeat the parent's user_tag while omitting
                # original_basket_id. Do not let that ambiguous tag claim the entry;
                # the event applier may still resolve an already-known child basket.
                client_order_id = None
        return ExchangeOrderEvent(
            status=str(event.status),
            product_id=product_id,
            client_order_id=client_order_id,
            exchange_order_id=basket_id,
            cumulative_filled_quantity=_event_decimal(
                event.cumulative_filled_quantity
            ),
            cumulative_average_price=_event_decimal(event.cumulative_average_price),
            last_fill_quantity=_event_decimal(event.last_fill_quantity),
            last_fill_price=_event_decimal(event.last_fill_price),
            event_timestamp=event.timestamp_ms,
            raw={
                "basket_id": str(event.basket_id),
                "native_parent_client_order_id": event.client_order_id,
                "original_basket_id": event.original_basket_id,
                "linked_basket_ids": event.linked_basket_ids,
                "exchange_order_id": event.exchange_order_id,
                "account_id": event.account_id,
                "exchange": identity[0],
                "symbol": identity[1],
                "price": event.price,
                "trigger_price": event.trigger_price,
                "price_type": event.price_type,
                "bracket_type": event.bracket_type,
                "notification_type": getattr(event, "notification_type", None),
            },
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


def _finite_positive_decimal(value, error_code: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ExchangeError(error_code) from error
    if not parsed.is_finite() or parsed <= 0:
        raise ExchangeError(error_code)
    return parsed


def _validate_bracket_price_side(
    *,
    entry_side: str,
    leg_type: str,
    reference_price: Decimal,
    trigger_price: Decimal,
) -> None:
    valid = {
        ("buy", "stop_loss"): trigger_price < reference_price,
        ("buy", "take_profit"): trigger_price > reference_price,
        ("sell", "stop_loss"): trigger_price > reference_price,
        ("sell", "take_profit"): trigger_price < reference_price,
    }.get((entry_side, leg_type), False)
    if not valid:
        raise ExchangeError(f"rithmic_native_bracket_{leg_type}_wrong_side")


def _bracket_leg_type(price_type: str | None) -> str | None:
    normalized = str(price_type or "").lower()
    if normalized in {"stop_market", "stop_limit"}:
        return "stop_loss"
    if normalized in {"limit", "market_if_touched", "limit_if_touched"}:
        return "take_profit"
    return None


def _order_side(order: Order) -> str:
    side = getattr(order.side, "value", order.side)
    normalized = str(side).lower()
    if normalized not in {"buy", "sell"}:
        raise ExchangeError(f"rithmic_order_side_unsupported: side={normalized}")
    return normalized


def _event_decimal(value) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _normalize_snapshot_status(
    status: str,
    filled_quantity: Decimal,
    quantity: Decimal,
    *,
    notification_type: str | None = None,
) -> str:
    normalized = status.strip().lower().replace("-", "_").replace(" ", "_")
    notification = str(notification_type or "").strip().upper()
    if quantity <= 0 or filled_quantity < 0 or filled_quantity > quantity:
        raise ExchangeError("invalid_rithmic_order_snapshot_quantities")
    if notification == "CANCEL":
        if filled_quantity == quantity:
            raise ExchangeError("invalid_rithmic_cancel_snapshot_quantities")
        return "cancelled"
    if notification == "REJECT":
        if filled_quantity == quantity:
            raise ExchangeError("invalid_rithmic_reject_snapshot_quantities")
        return "rejected"
    if normalized in {"open", "open_pending", "new", "submitted", "accepted"}:
        return "partially_filled" if filled_quantity > 0 else "open"
    if normalized in {"partial", "partially_filled", "partiallyfilled"}:
        if Decimal("0") < filled_quantity < quantity:
            return "partially_filled"
    elif normalized in {"complete", "completed", "filled"}:
        if filled_quantity == quantity:
            return "filled"
    elif normalized in {"cancel", "canceled", "cancelled"}:
        return "cancelled"
    elif normalized in {"reject", "rejected", "failed", "expired"}:
        return "rejected"
    raise ExchangeError(
        f"unsupported_rithmic_order_snapshot_status: status={normalized}"
    )


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
