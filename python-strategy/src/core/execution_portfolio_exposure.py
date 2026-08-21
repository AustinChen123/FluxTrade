"""Atomic portfolio exposure projection for coordinated entry decisions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any, ContextManager

from src.core.models import OrderSide, OrderStatus, PositionSide
from src.core.portfolio_runtime import PortfolioExposureSnapshot


def project_portfolio_exposure(
    *,
    position_loader: Callable[[str, str], object | None] | None,
    order_repository: Any,
    order_event_lock: ContextManager[object],
    strategy_ids: tuple[str, ...],
    product_id: str,
    requested_intents: Mapping[str, str],
) -> PortfolioExposureSnapshot:
    """Read positions and working entries under one order-event fence."""
    if position_loader is None:
        raise RuntimeError("portfolio_position_loader_missing")

    active_statuses = {
        OrderStatus.NEW.value,
        OrderStatus.SUBMITTED_UNCONFIRMED.value,
        OrderStatus.SUBMITTED.value,
        OrderStatus.PARTIALLY_FILLED.value,
    }
    owners = set(strategy_ids)
    if len(owners) != len(strategy_ids):
        raise ValueError("portfolio_exposure_strategy_ids_must_be_unique")
    if set(requested_intents.values()) - owners:
        raise ValueError("portfolio_exposure_intent_owner_unknown")

    with order_event_lock:
        existing_client_order_ids: set[str] = set()
        for client_order_id, expected_strategy_id in requested_intents.items():
            existing_order = order_repository.get_order_by_client_order_id(
                client_order_id
            )
            if existing_order is None:
                continue
            if (
                str(existing_order.strategy_id) != expected_strategy_id
                or str(existing_order.product_id) != product_id
            ):
                raise RuntimeError(
                    "portfolio_replay_intent_identity_mismatch:"
                    f"client_order_id={client_order_id}"
                )
            existing_client_order_ids.add(client_order_id)

        quantities: dict[str, Decimal] = {}
        for strategy_id in strategy_ids:
            position = position_loader(strategy_id, product_id)
            if position is None:
                quantities[strategy_id] = Decimal("0")
                continue
            quantity = Decimal(str(getattr(position, "quantity")))
            side = str(
                getattr(
                    getattr(position, "side"),
                    "value",
                    getattr(position, "side"),
                )
            ).upper()
            if not quantity.is_finite() or quantity <= 0:
                raise RuntimeError(f"portfolio_position_invalid:{strategy_id}")
            if side == PositionSide.LONG.value:
                quantities[strategy_id] = quantity
            elif side == PositionSide.SHORT.value:
                quantities[strategy_id] = -quantity
            else:
                raise RuntimeError(f"portfolio_position_side_invalid:{strategy_id}")

        for order in order_repository.list_orders_by_statuses(active_statuses):
            strategy_id = str(order.strategy_id)
            if strategy_id not in owners or str(order.product_id) != product_id:
                continue
            payload = (
                order.intent_payload if isinstance(order.intent_payload, dict) else {}
            )
            order_payload = payload.get("order")
            if not isinstance(order_payload, dict):
                order_payload = {}
            if (
                payload.get("pending_entry_order_id")
                or payload.get("reduce_only") is True
                or order_payload.get("reduce_only") is True
            ):
                continue

            quantity = Decimal(str(order.quantity))
            filled_quantity = Decimal(str(order.filled_quantity or Decimal("0")))
            remaining = quantity - filled_quantity
            if (
                not quantity.is_finite()
                or quantity <= 0
                or not filled_quantity.is_finite()
                or filled_quantity < 0
                or remaining < 0
            ):
                raise RuntimeError(
                    f"portfolio_pending_entry_quantity_invalid:order_id={order.id}"
                )
            if remaining == 0:
                continue
            side = str(getattr(order.side, "value", order.side)).lower()
            if side == OrderSide.BUY.value:
                signed = remaining
            elif side == OrderSide.SELL.value:
                signed = -remaining
            else:
                raise RuntimeError(
                    f"portfolio_pending_entry_side_invalid:order_id={order.id}"
                )
            current = quantities[strategy_id]
            if current * signed < 0:
                raise RuntimeError(
                    f"portfolio_pending_entry_crosses_sleeve_position:{strategy_id}"
                )
            quantities[strategy_id] = current + signed

        return PortfolioExposureSnapshot(
            quantities=quantities,
            existing_client_order_ids=frozenset(existing_client_order_ids),
        )
