"""Plan safe cancellation targets from owned records and one ledger snapshot."""

from typing import Any, Iterable

from src.core.adapters.rithmic_order_observation import (
    project_rithmic_order_snapshot,
)
from src.core.adapters.rithmic_order_status import rithmic_order_may_be_working
from src.core.models import OrderStatus


def _plan_rithmic_cancellation_targets(
    active_order_records: Iterable[Any],
    snapshot: object,
    *,
    strategy_id: str,
    product_id: str,
    profile: str,
    account_id: str,
) -> tuple[list[tuple[Any, Any]], list[Any]]:
    active_orders: list[Any] = []
    for order_record in active_order_records:
        order: Any = order_record
        if (
            order.strategy_id == strategy_id
            and order.product_id == product_id
            and str(getattr(order, "exchange_id", "RITHMIC")).casefold() == "rithmic"
            and getattr(order, "account_profile", profile) == profile
            and getattr(order, "account_id", account_id) == account_id
        ):
            active_orders.append(order)
    cancellation_targets: list[tuple[Any, Any]] = []
    new_orders: list[Any] = []
    for order in active_orders:
        if order.status == OrderStatus.NEW.value:
            new_orders.append(order)
            continue
        basket_id = str(getattr(order, "exchange_order_id", None) or "").strip()
        if not basket_id:
            raise RuntimeError(
                f"rithmic_strategy_exit_cancel_identity_missing:order_id={order.id}"
            )
        remote_matches = [
            remote
            for remote in getattr(snapshot, "orders", ())
            if str(getattr(remote, "basket_id", "")) == basket_id
        ]
        if len(remote_matches) != 1:
            raise RuntimeError(
                f"rithmic_strategy_exit_cancel_identity_ambiguous:order_id={order.id}"
            )
        remote = remote_matches[0]
        projected = project_rithmic_order_snapshot(
            remote,
            account_id=account_id,
        )
        if projected.exchange_order_id != basket_id:
            raise RuntimeError(
                f"rithmic_strategy_exit_cancel_identity_mismatch:order_id={order.id}"
            )
        if not rithmic_order_may_be_working(remote):
            continue
        cancellation_targets.append((order, remote))
    return cancellation_targets, new_orders
