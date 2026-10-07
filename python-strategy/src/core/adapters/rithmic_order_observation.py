from dataclasses import replace
from decimal import Decimal
from typing import Mapping

from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
)
from src.core.adapters.rithmic_order_status import (
    _normalize_status,
    _normalize_snapshot_status as _normalize_snapshot_status,
    _status_decimal as _status_decimal,
)
from src.core.interfaces import IOrderRepository
from src.core.models import OrderStatus


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


def project_rithmic_order_snapshot(
    remote: object,
    *,
    account_id: str,
) -> ExchangeOrderSnapshot:
    quantity = Decimal(str(getattr(remote, "quantity")))
    filled_quantity = _event_decimal(getattr(remote, "filled_quantity"))
    status = _normalize_snapshot_status(
        str(getattr(remote, "status")),
        filled_quantity,
        quantity,
        notification_type=getattr(remote, "notification_type", None),
        unfilled_quantity=_status_decimal(getattr(remote, "unfilled_quantity", None)),
    )
    basket_id = getattr(remote, "basket_id")
    if type(basket_id) is not str or not str.strip(basket_id):
        raise ExchangeError("rithmic_order_snapshot_basket_id_required")
    return ExchangeOrderSnapshot(
        client_order_id=str(getattr(remote, "client_order_id")),
        exchange_order_id=basket_id,
        status=status,
        filled_quantity=filled_quantity,
        average_price=_event_decimal(getattr(remote, "average_fill_price")),
        raw={
            "basket_id": basket_id,
            "exchange_order_id": getattr(remote, "exchange_order_id"),
            "quantity": str(getattr(remote, "quantity")),
            "account_id": account_id,
        },
    )


def resolve_rithmic_order_event_identity(
    event: object,
    *,
    account_id: str,
    products_by_native_identity: Mapping[tuple[str, str], str],
) -> tuple[str, tuple[str, str]]:
    native_identity = (
        str(getattr(event, "exchange")).upper(),
        str(getattr(event, "symbol")).upper(),
    )
    product_id = products_by_native_identity.get(native_identity)
    if product_id is None:
        raise RithmicUnmappedOrderEvent(
            account_id=account_id,
            exchange=native_identity[0],
            symbol=native_identity[1],
        )
    return product_id, native_identity


def project_rithmic_order_event(
    event: object,
    *,
    product_id: str,
    client_order_id: str | None,
    native_identity: tuple[str, str],
) -> ExchangeOrderEvent:
    return ExchangeOrderEvent(
        status=str(getattr(event, "status")),
        product_id=product_id,
        client_order_id=client_order_id,
        exchange_order_id=str(getattr(event, "basket_id")),
        cumulative_filled_quantity=_event_decimal(
            getattr(event, "cumulative_filled_quantity")
        ),
        cumulative_average_price=_event_decimal(
            getattr(event, "cumulative_average_price")
        ),
        last_fill_quantity=_event_decimal(getattr(event, "last_fill_quantity")),
        last_fill_price=_event_decimal(getattr(event, "last_fill_price")),
        event_timestamp=getattr(event, "timestamp_ms"),
        raw={
            "basket_id": str(getattr(event, "basket_id")),
            "native_parent_client_order_id": getattr(event, "client_order_id"),
            "original_basket_id": getattr(event, "original_basket_id"),
            "linked_basket_ids": getattr(event, "linked_basket_ids"),
            "exchange_order_id": getattr(event, "exchange_order_id"),
            "account_id": getattr(event, "account_id"),
            "exchange": native_identity[0],
            "symbol": native_identity[1],
            "price": getattr(event, "price"),
            "trigger_price": getattr(event, "trigger_price"),
            "price_type": getattr(event, "price_type"),
            "bracket_type": getattr(event, "bracket_type"),
            "notification_type": getattr(event, "notification_type", None),
            "raw_status": getattr(event, "raw_status", None),
            "quantity": getattr(event, "quantity", None),
            "unfilled_quantity": getattr(event, "unfilled_quantity", None),
            "transaction_type": getattr(event, "transaction_type", None),
        },
    )


def _resolve_sparse_live_order_event(
    repository: IOrderRepository,
    event: ExchangeOrderEvent,
    *,
    account_profile: str,
    account_id: str,
) -> ExchangeOrderEvent | None:
    """Resolve a native sparse-quantity carrier from durable acknowledged state."""
    if event.status != "quantity_unresolved":
        return event
    raw = event.raw or {}
    basket_id = event.exchange_order_id
    if (
        not isinstance(basket_id, str)
        or not basket_id
        or raw.get("quantity") is not None
        or raw.get("account_id") != account_id
    ):
        return None
    order = repository.get_order_by_exchange_order_id(
        basket_id,
        exchange_id="RITHMIC",
        product_id=event.product_id,
    )
    if order is None:
        return None
    if (
        str(order.exchange_id).upper() != "RITHMIC"
        or order.product_id != event.product_id
        or order.exchange_order_id != basket_id
        or order.account_profile != account_profile
        or order.account_id != account_id
        or not isinstance(order.client_order_id, str)
        or not order.client_order_id
        or (
            event.client_order_id is not None
            and event.client_order_id != order.client_order_id
        )
    ):
        return None

    raw_transaction_type = raw.get("transaction_type")
    expected_side = {
        "BUY": "buy",
        "SELL": "sell",
        "SHORT_SELL": "sell",
    }.get(str(raw_transaction_type or "").upper())
    local_side = str(getattr(order.side, "value", order.side)).lower()
    if expected_side is None or local_side != expected_side:
        return None

    quantity = _status_decimal(order.quantity)
    cumulative = event.cumulative_filled_quantity
    local_filled = _status_decimal(order.filled_quantity)
    if (
        quantity is None
        or not quantity.is_finite()
        or quantity <= 0
        or (
            local_filled is not None
            and (
                not local_filled.is_finite()
                or local_filled < 0
                or local_filled > quantity
            )
        )
    ):
        return None

    status = str(getattr(order.status, "value", order.status)).upper()
    acknowledged = (
        status
        in {
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
            OrderStatus.FILLED.value,
            OrderStatus.CANCELLED.value,
            "CLOSED",
        }
        or order.acked_at is not None
        or (local_filled is not None and Decimal("0") < local_filled <= quantity)
    )
    if not acknowledged:
        return None

    normalized_status = _normalize_status(
        str(raw.get("raw_status") or ""),
        cumulative,
        quantity,
        notification_type=str(raw.get("notification_type") or ""),
        unfilled_quantity=_status_decimal(raw.get("unfilled_quantity")),
        allow_live_implicit_progress=True,
    )
    if normalized_status is None:
        return None
    return replace(
        event,
        status=normalized_status,
        client_order_id=order.client_order_id,
    )


def _event_decimal(value: object) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None
