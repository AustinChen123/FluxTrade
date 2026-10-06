from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from src.core.interfaces.exchange import ExchangeError


@dataclass(frozen=True)
class _StatusInterpretation:
    economic_status: str | None
    terminal_order: bool | None


def _classify_status(
    status: str | None,
    filled_quantity: Decimal | None,
    quantity: Decimal | None,
    *,
    notification_type: str | None = None,
    unfilled_quantity: Decimal | None = None,
) -> _StatusInterpretation:
    """Classify only supported status/quantity evidence; unknown stays unresolved."""
    notification = str(notification_type or "").strip().upper()
    if not _valid_quantity_evidence(filled_quantity, quantity, unfilled_quantity):
        return _StatusInterpretation(None, None)
    if filled_quantity is None:
        return (
            _StatusInterpretation(None, True)
            if notification == "COMPLETE"
            else _StatusInterpretation(None, None)
        )
    assert filled_quantity is not None and quantity is not None
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
    filled_quantity: Decimal | None,
    quantity: Decimal | None,
    unfilled_quantity: Decimal | None,
) -> bool:
    if quantity is None:
        return False
    if not quantity.is_finite() or quantity <= 0:
        return False
    if filled_quantity is not None and (
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
    filled_quantity: Decimal | None,
    quantity: Decimal,
    *,
    notification_type: str | None = None,
    unfilled_quantity: Decimal | None = None,
) -> str:
    notification = str(notification_type or "").strip().upper()
    normalized = _normalize_status_text(status)
    if not _valid_quantity_evidence(filled_quantity, quantity, unfilled_quantity):
        raise ExchangeError("invalid_rithmic_order_snapshot_quantities")
    if notification in {"CANCEL", "REJECT"} and filled_quantity == quantity:
        error_code = (
            "invalid_rithmic_cancel_snapshot_quantities"
            if notification == "CANCEL"
            else "invalid_rithmic_reject_snapshot_quantities"
        )
        raise ExchangeError(error_code)
    interpretation = _classify_status(
        status,
        filled_quantity,
        quantity,
        notification_type=notification,
        unfilled_quantity=unfilled_quantity,
    )
    if interpretation.economic_status is None:
        raise ExchangeError(
            f"unsupported_rithmic_order_snapshot_status: status={normalized}"
        )
    if interpretation.economic_status == "failed":
        return "rejected"
    return interpretation.economic_status


def _normalize_status(
    status: str,
    cumulative_quantity: Decimal | None,
    order_quantity: Decimal,
    *,
    notification_type: str | None = None,
    unfilled_quantity: Decimal | None = None,
) -> str | None:
    return _classify_status(
        status,
        cumulative_quantity,
        order_quantity,
        notification_type=notification_type,
        unfilled_quantity=unfilled_quantity,
    ).economic_status


def rithmic_order_may_be_working(remote: object) -> bool:
    notification = str(getattr(remote, "notification_type", None) or "").strip().upper()
    interpretation = _classify_status(
        str(getattr(remote, "status", None) or ""),
        _status_decimal(getattr(remote, "filled_quantity", None)),
        _status_decimal(getattr(remote, "quantity", None)),
        notification_type=notification,
        unfilled_quantity=_status_decimal(getattr(remote, "unfilled_quantity", None)),
    )
    return interpretation.terminal_order is not True


def _status_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("NaN")
