from dataclasses import dataclass
from decimal import Decimal

from src.core.interfaces.exchange import ExchangeError


@dataclass(frozen=True)
class _StatusInterpretation:
    economic_status: str | None
    terminal_order: bool | None


def _classify_status(
    status: str | None,
    filled_quantity: Decimal,
    quantity: Decimal,
    *,
    notification_type: str | None = None,
    unfilled_quantity: Decimal | None = None,
) -> _StatusInterpretation:
    """Classify only supported status/quantity evidence; unknown stays unresolved."""
    notification = str(notification_type or "").strip().upper()
    if not _valid_quantity_evidence(filled_quantity, quantity, unfilled_quantity):
        return _StatusInterpretation(None, None)
    if (
        notification == "FILL"
        and unfilled_quantity is not None
        and filled_quantity + unfilled_quantity != quantity
    ):
        return _StatusInterpretation(None, None)

    normalized = _normalize_status_text(status)
    fully_filled = filled_quantity == quantity
    incomplete = filled_quantity < quantity

    # ExchangeOrderNotification FILL carries cumulative progress; its raw status
    # is not authoritative. The native decoder uses the same quantity boundary.
    if notification == "FILL":
        if filled_quantity <= 0:
            return _StatusInterpretation(None, None)
        return _StatusInterpretation(
            "filled" if fully_filled else "partially_filled", fully_filled
        )

    # The official sample treats COMPLETE as order completion, not proof of a
    # full fill. Preserve terminal evidence without inventing economic progress.
    if notification == "COMPLETE":
        if normalized in _CANCEL_ALIASES and incomplete:
            return _StatusInterpretation("cancelled", True)
        if normalized in _REJECT_ALIASES and incomplete:
            return _StatusInterpretation("failed", True)
        if normalized in _FULL_ALIASES and fully_filled:
            return _StatusInterpretation("filled", True)
        return _StatusInterpretation(None, True)

    if notification in {"CANCEL", "REJECT"}:
        if not incomplete:
            return _StatusInterpretation(None, None)
        return _StatusInterpretation(
            "cancelled" if notification == "CANCEL" else "failed", True
        )

    if normalized in _OPEN_ALIASES:
        if fully_filled:
            return _StatusInterpretation(None, None)
        return _StatusInterpretation(
            "partially_filled" if filled_quantity > 0 else "open", False
        )
    if normalized in _PARTIAL_ALIASES:
        if filled_quantity <= 0 or fully_filled:
            return _StatusInterpretation(None, None)
        return _StatusInterpretation("partially_filled", False)
    if normalized in _FULL_ALIASES:
        if not fully_filled:
            return _StatusInterpretation(None, None)
        return _StatusInterpretation("filled", True)
    if normalized in _CANCEL_ALIASES:
        return (
            _StatusInterpretation("cancelled", True)
            if incomplete
            else _StatusInterpretation(None, None)
        )
    if normalized in _REJECT_ALIASES:
        return (
            _StatusInterpretation("failed", True)
            if incomplete
            else _StatusInterpretation(None, None)
        )
    return _StatusInterpretation(None, None)


_OPEN_ALIASES = {"open", "open_pending", "new", "submitted", "accepted", "modified"}
_PARTIAL_ALIASES = {"partial", "partially_filled", "partiallyfilled"}
_FULL_ALIASES = {"complete", "completed", "filled", "closed"}
_CANCEL_ALIASES = {"cancel", "canceled", "cancelled"}
_REJECT_ALIASES = {"reject", "rejected", "failed", "expired"}


def _valid_quantity_evidence(
    filled_quantity: Decimal,
    quantity: Decimal,
    unfilled_quantity: Decimal | None,
) -> bool:
    if not quantity.is_finite() or quantity <= 0:
        return False
    if (
        not filled_quantity.is_finite()
        or not Decimal("0") <= filled_quantity <= quantity
    ):
        return False
    if unfilled_quantity is None:
        return True
    if (
        not unfilled_quantity.is_finite()
        or not Decimal("0") <= unfilled_quantity <= quantity
    ):
        return False
    return True


def _normalize_status_text(status: str | None) -> str:
    return "_".join((status or "").strip().lower().replace("-", " ").split())


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
