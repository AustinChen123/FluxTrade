from decimal import Decimal
from typing import Mapping

from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
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


def project_rithmic_order_snapshot(
    remote: object,
    *,
    account_id: str,
) -> ExchangeOrderSnapshot:
    quantity = Decimal(str(getattr(remote, "quantity")))
    filled_quantity = _event_decimal(getattr(remote, "filled_quantity")) or Decimal("0")
    status = _normalize_snapshot_status(
        str(getattr(remote, "status")),
        filled_quantity,
        quantity,
        notification_type=getattr(remote, "notification_type", None),
    )
    return ExchangeOrderSnapshot(
        client_order_id=str(getattr(remote, "client_order_id")),
        exchange_order_id=str(getattr(remote, "basket_id")),
        status=status,
        filled_quantity=filled_quantity,
        average_price=_event_decimal(getattr(remote, "average_fill_price")),
        raw={
            "basket_id": str(getattr(remote, "basket_id")),
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
        },
    )


def _event_decimal(value: object) -> Decimal | None:
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
