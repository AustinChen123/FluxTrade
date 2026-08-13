"""Verified net-reduction replay identity and persistence."""

from decimal import Decimal

from src.core.interfaces import IOrderRepository
from src.core.models import OrderSide, OrderStatus, Signal
from src.core.orm_models import Order


def validated_order_payload(
    signal: Signal,
    order: Order,
    *,
    expected_side: OrderSide | None,
) -> dict:
    payload = order.intent_payload if isinstance(order.intent_payload, dict) else {}
    signal_payload = payload.get("signal")
    quantity = Decimal(str(order.quantity))
    filled_quantity = Decimal(str(order.filled_quantity or Decimal("0")))
    if (
        expected_side is None
        or str(order.strategy_id) != signal.strategy_id
        or str(order.product_id) != signal.product_id
        or str(order.type) != "market"
        or str(getattr(order.side, "value", order.side)).lower() != expected_side.value
        or payload.get("source") != "authoritative_net_reduction"
        or not isinstance(signal_payload, dict)
        or signal_payload.get("type") != signal.type.value
        or str(order.status) != OrderStatus.FILLED.value
        or not quantity.is_finite()
        or quantity <= 0
        or filled_quantity != quantity
    ):
        raise RuntimeError("authoritative_exit_replay_identity_mismatch")
    return payload


def completed_replay(
    signal: Signal,
    existing_order: Order | None,
    *,
    expected_side: OrderSide | None,
) -> bool:
    if existing_order is None:
        return False

    payload = validated_order_payload(
        signal,
        existing_order,
        expected_side=expected_side,
    )
    verification = payload.get("authoritative_verification")
    if (
        not isinstance(verification, dict)
        or verification.get("status") != "verified_portfolio_reduction"
        or verification.get("strategy_id") != signal.strategy_id
        or verification.get("product_id") != signal.product_id
    ):
        raise RuntimeError("authoritative_exit_replay_verification_missing")
    return True


def validate_remaining_remote_quantity(remaining_remote_quantity: Decimal) -> None:
    if not remaining_remote_quantity.is_finite() or remaining_remote_quantity < 0:
        raise ValueError("verified_net_reduction_remaining_quantity_invalid")


def record_verification(
    repository: IOrderRepository,
    signal: Signal,
    order: Order,
    *,
    client_order_id: str,
    expected_side: OrderSide | None,
    remaining_remote_quantity: Decimal,
) -> None:
    validate_remaining_remote_quantity(remaining_remote_quantity)
    if str(order.client_order_id) != client_order_id:
        raise RuntimeError("verified_net_reduction_order_identity_mismatch")

    payload = dict(
        validated_order_payload(
            signal,
            order,
            expected_side=expected_side,
        )
    )
    payload["authoritative_verification"] = {
        "status": "verified_portfolio_reduction",
        "strategy_id": signal.strategy_id,
        "product_id": signal.product_id,
        "remaining_remote_quantity": str(remaining_remote_quantity),
    }
    setattr(order, "intent_payload", payload)
    repository.update_order(order)
