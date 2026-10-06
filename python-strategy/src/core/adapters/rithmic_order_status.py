from decimal import Decimal

from src.core.interfaces.exchange import ExchangeError


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


def _normalize_status(
    status: str,
    cumulative_quantity: Decimal,
    order_quantity: Decimal,
    *,
    notification_type: str | None = None,
) -> str | None:
    normalized = (status or "").strip().lower()
    notification = str(notification_type or "").strip().upper()
    if notification == "CANCEL":
        return "cancelled" if cumulative_quantity < order_quantity else None
    if notification == "REJECT":
        return "failed" if cumulative_quantity < order_quantity else None
    if normalized in {"open", "new", "submitted", "accepted", "modified"}:
        if cumulative_quantity >= order_quantity:
            return None
        return "partially_filled" if cumulative_quantity > 0 else "open"
    if normalized in {"partial", "partially_filled", "partiallyfilled"}:
        return (
            "partially_filled"
            if Decimal("0") < cumulative_quantity < order_quantity
            else None
        )
    if normalized in {"filled", "closed"}:
        return "filled" if cumulative_quantity == order_quantity else None
    if normalized in {"canceled", "cancelled"}:
        return "cancelled"
    if normalized in {"rejected", "expired", "failed"}:
        return "failed"
    if normalized == "complete":
        return "filled" if cumulative_quantity == order_quantity else None
    return None


def rithmic_order_may_be_working(remote: object) -> bool:
    notification = str(getattr(remote, "notification_type", None) or "").strip().upper()
    quantity = _decimal(getattr(remote, "quantity", None))
    filled = _decimal(getattr(remote, "filled_quantity", None))
    status = str(getattr(remote, "status", None) or "").strip().lower()
    if notification == "COMPLETE" and status in {"complete", "completed"}:
        return False
    if notification in {"CANCEL", "REJECT"} or status in {
        "cancel",
        "canceled",
        "cancelled",
        "reject",
        "rejected",
        "expired",
        "failed",
    }:
        return not (quantity > 0 and filled < quantity)
    if quantity > 0 and filled == quantity:
        return not (
            notification == "FILL"
            or status in {"complete", "completed", "filled", "closed"}
        )
    return True


def _decimal(value) -> Decimal:
    return Decimal(str(value or "0"))
