from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Iterable

from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.product_registry import to_rithmic_symbol


@dataclass(frozen=True)
class RithmicRecoveryItem:
    order: object
    classification: str
    event: ExchangeOrderEvent | None = None
    reason: str | None = None
    unresolved: bool = False
    verification_blocked: bool = False


def load_rithmic_recovery_snapshot(
    profile: str,
    account_id: str | None,
    orders: Iterable[object],
    now_seconds: int,
    loader: Callable | None = None,
):
    if loader is None:
        from fluxtrade_core import rithmic_ledger_snapshot

        loader = rithmic_ledger_snapshot

    orders = list(orders)
    basket_ids = {
        str(order.exchange_order_id) for order in orders if order.exchange_order_id
    }
    basket_ids.update(
        str(parent_basket_id)
        for order in orders
        if isinstance(getattr(order, "intent_payload", None), dict)
        and (parent_basket_id := order.intent_payload.get("native_parent_basket_id"))
    )
    basket_ids = sorted(basket_ids)
    if not basket_ids:
        return loader(profile, account_id)
    start_seconds = max(0, min(int(order.timestamp) for order in orders) // 1000 - 1)
    return loader(
        profile,
        account_id,
        recovery_basket_ids=basket_ids,
        fill_start_index=start_seconds,
        fill_finish_index=now_seconds + 1,
    )


def build_rithmic_recovery_plan(
    orders: Iterable[object],
    snapshot,
) -> tuple[list[RithmicRecoveryItem], list[dict[str, str | None]]]:
    orders = list(orders)
    local_by_id = {str(order.id): order for order in orders}
    working_by_basket, duplicate_working = _unique_index(snapshot.orders, "basket_id")
    working_by_client, duplicate_clients = _unique_index(
        (
            order
            for order in snapshot.orders
            if order.client_order_id
            and not getattr(order, "original_basket_id", None)
        ),
        "client_order_id",
    )
    _, duplicate_local_baskets = _unique_index(
        (order for order in orders if order.exchange_order_id),
        "exchange_order_id",
    )
    _, duplicate_local_clients = _unique_index(
        (order for order in orders if order.client_order_id),
        "client_order_id",
    )
    history_by_basket = _group(snapshot.order_history, "basket_id")
    fills_by_basket = _group(snapshot.fills, "basket_id")
    local_baskets = {str(order.exchange_order_id) for order in orders if order.exchange_order_id}
    local_clients = {str(order.client_order_id) for order in orders if order.client_order_id}
    expected_native_children = _expected_native_children(orders, local_by_id)
    native_local_counts: dict[tuple[str, str], int] = {}
    for order in orders:
        parent = _native_parent_basket(order, local_by_id)
        if parent and str(order.type) in {"stop_loss", "take_profit"}:
            key = (parent, str(order.type))
            native_local_counts[key] = native_local_counts.get(key, 0) + 1
    remote_native_counts: dict[tuple[str, str], int] = {}
    for remote in snapshot.orders:
        key = _remote_native_key(remote)
        if key is not None:
            remote_native_counts[key] = remote_native_counts.get(key, 0) + 1
    external = [
        {
            "basket_id": remote.basket_id,
            "client_order_id": remote.client_order_id,
            "status": remote.status,
        }
        for remote in snapshot.orders
        if _remote_may_be_working(remote)
        if not _is_expected_native_child(
            remote,
            expected_native_children,
            remote_native_counts,
        )
        and (
            (
                remote.basket_id not in local_baskets
                and remote.client_order_id not in local_clients
            )
            or getattr(remote, "original_basket_id", None)
        )
    ]

    results = []
    for order in orders:
        basket_id = str(order.exchange_order_id) if order.exchange_order_id else None
        client_order_id = str(order.client_order_id) if order.client_order_id else None
        native_parent = _native_parent_basket(order, local_by_id)
        if (
            native_parent is not None
            and native_local_counts.get((native_parent, str(order.type)), 0) > 1
        ):
            results.append(_blocked(order, "duplicate_local_native_bracket_leg"))
            continue
        if basket_id in duplicate_local_baskets or client_order_id in duplicate_local_clients:
            results.append(_blocked(order, "duplicate_local_identity"))
            continue
        if basket_id in duplicate_working or client_order_id in duplicate_clients:
            results.append(_blocked(order, "duplicate_remote_identity"))
            continue

        by_basket = working_by_basket.get(basket_id) if basket_id else None
        by_client = (
            working_by_client.get(client_order_id)
            if client_order_id and native_parent is None
            else None
        )
        if native_parent is not None and by_basket is None:
            candidates = [
                remote
                for remote in snapshot.orders
                if getattr(remote, "original_basket_id", None) == native_parent
                and _remote_leg_type(remote) == str(order.type)
            ]
            if len(candidates) > 1:
                results.append(_blocked(order, "duplicate_remote_native_bracket_leg"))
                continue
            if candidates:
                by_basket = candidates[0]
        if (
            by_basket is not None
            and by_client is not None
            and str(by_basket.basket_id) != str(by_client.basket_id)
        ):
            results.append(_blocked(order, "conflicting_remote_identity"))
            continue
        remote = by_basket or by_client
        if remote is not None and basket_id and remote.basket_id != basket_id:
            results.append(_blocked(order, "working_basket_id_mismatch"))
            continue
        if remote is None and basket_id:
            remote, reason = _latest_history(history_by_basket.get(basket_id, []))
            if reason is not None:
                results.append(_blocked(order, reason))
                continue
        if remote is not None and native_parent is not None:
            if str(getattr(remote, "original_basket_id", None) or "") != native_parent:
                results.append(_blocked(order, "native_parent_basket_id_mismatch"))
                continue
            if _remote_leg_type(remote) != str(order.type):
                results.append(_blocked(order, "native_bracket_leg_mismatch"))
                continue

        remote_basket_id = remote.basket_id if remote is not None else basket_id
        fills, reason = _deduplicate_fills(fills_by_basket.get(remote_basket_id, []))
        if reason is not None:
            results.append(_blocked(order, reason))
            continue
        results.append(_classify_order(order, remote, fills))
    return results, external


def compare_rithmic_positions(
    orders: Iterable[object],
    local_positions: Iterable[object],
    remote_positions: Iterable[object],
) -> list[dict[str, str]]:
    local_positions = list(local_positions)
    remote_positions = list(remote_positions)
    products = {
        str(order.product_id): _product_symbol(str(order.product_id))
        for order in orders
    }
    for position in local_positions:
        product_id = str(position.product_id)
        if product_id.upper().startswith("RITHMIC:"):
            products[product_id] = _product_symbol(product_id)
    local_by_product = {product_id: Decimal("0") for product_id in products}
    for position in local_positions:
        product_id = str(position.product_id)
        if product_id not in local_by_product:
            continue
        quantity = _decimal(position.quantity)
        side = getattr(position.side, "value", position.side)
        local_by_product[product_id] += (
            -quantity if str(side).upper() == "SHORT" else quantity
        )

    remote_by_symbol: dict[str, Decimal] = {}
    for position in remote_positions:
        symbol = str(position.symbol).upper()
        remote_by_symbol[symbol] = remote_by_symbol.get(symbol, Decimal("0")) + _decimal(
            position.net_quantity
        )

    drifts = [
        {
            "product_id": product_id,
            "local_quantity": str(local_by_product[product_id]),
            "remote_quantity": str(remote_by_symbol.get(symbol, Decimal("0"))),
        }
        for product_id, symbol in sorted(products.items())
        if local_by_product[product_id] != remote_by_symbol.get(symbol, Decimal("0"))
    ]
    known_symbols = set(products.values())
    drifts.extend(
        {
            "product_id": f"RITHMIC:{symbol}",
            "local_quantity": "0",
            "remote_quantity": str(quantity),
        }
        for symbol, quantity in sorted(remote_by_symbol.items())
        if symbol not in known_symbols and quantity != 0
    )
    return drifts


def _classify_order(order, remote, fills: list[object]) -> RithmicRecoveryItem:
    if remote is not None:
        allowed_client_ids = {str(order.client_order_id)}
        intent_payload = getattr(order, "intent_payload", None)
        if isinstance(intent_payload, dict):
            parent_client_id = intent_payload.get("native_parent_client_order_id")
            if parent_client_id:
                allowed_client_ids.add(str(parent_client_id))
        if remote.client_order_id and str(remote.client_order_id) not in allowed_client_ids:
            return _blocked(order, "client_order_id_mismatch")
        if not _symbol_matches_product(order.product_id, remote.symbol):
            return _blocked(order, "product_symbol_mismatch")
        if _decimal(remote.quantity) != Decimal(str(order.quantity)):
            return _blocked(order, "order_quantity_mismatch", unresolved=True)
        remote_side = str(remote.transaction_type).lower()
        if remote_side == "short_sell":
            remote_side = "sell"
        if remote_side != str(order.side).lower():
            return _blocked(order, "order_side_mismatch", unresolved=True)

    for fill in fills:
        if not _symbol_matches_product(order.product_id, fill.symbol):
            return _blocked(order, "fill_product_symbol_mismatch")
        fill_side = _normalize_transaction_type(fill.transaction_type)
        if fill_side is None:
            return _blocked(order, "unknown_fill_transaction_type")
        if fill_side != str(order.side).lower():
            return _blocked(order, "fill_side_mismatch")
        if (
            remote is not None
            and remote.exchange_order_id
            and fill.exchange_order_id
            and remote.exchange_order_id != fill.exchange_order_id
        ):
            return _blocked(order, "order_and_fill_exchange_order_id_mismatch")

    fill_quantity, fill_average = _aggregate_fills(fills)
    remote_quantity = _decimal(remote.filled_quantity) if remote is not None else None
    remote_average = _decimal(remote.average_fill_price) if remote is not None else None
    if remote_quantity is not None and fills and remote_quantity != fill_quantity:
        return _blocked(order, "order_and_fill_history_quantity_mismatch", unresolved=True)
    if remote_average is not None and fills and remote_average != fill_average:
        return _blocked(order, "order_and_fill_history_average_mismatch", unresolved=True)
    cumulative_quantity = remote_quantity if remote_quantity is not None else fill_quantity
    cumulative_average = remote_average if remote_average is not None else fill_average
    order_quantity = Decimal(str(order.quantity))
    local_filled = _decimal(getattr(order, "filled_quantity", None))
    if cumulative_quantity > order_quantity:
        return _blocked(order, "remote_fill_exceeds_order_quantity")
    if local_filled > cumulative_quantity:
        return _blocked(order, "local_fill_exceeds_remote")
    if cumulative_quantity > 0 and cumulative_average is None:
        return _blocked(order, "missing_authoritative_fill_average")

    if remote is None:
        if not fills:
            return _blocked(order, "no_authoritative_remote_evidence")
        if cumulative_quantity == order_quantity:
            status = "filled"
            unresolved = False
        else:
            status = "partially_filled"
            unresolved = True
    else:
        status = _normalize_status(
            remote.status,
            cumulative_quantity,
            order_quantity,
            notification_type=getattr(remote, "notification_type", None),
        )
        if status is None:
            return _blocked(order, "unknown_rithmic_order_status")
        unresolved = False

    event = ExchangeOrderEvent(
        status=status,
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id=(
            remote.basket_id if remote is not None else str(order.exchange_order_id)
        ),
        cumulative_filled_quantity=cumulative_quantity,
        cumulative_average_price=cumulative_average,
        event_timestamp=(
            remote.timestamp_ms
            if remote is not None
            else max((fill.timestamp_ms for fill in fills), default=None)
        ),
        raw=(
            {
                "original_basket_id": getattr(remote, "original_basket_id", None),
                "price": getattr(remote, "price", None),
                "trigger_price": getattr(remote, "trigger_price", None),
                "price_type": getattr(remote, "price_type", None),
                "bracket_type": getattr(remote, "bracket_type", None),
                "notification_type": "recovery_snapshot",
            }
            if remote is not None
            else {}
        ),
    )
    if unresolved:
        classification = "matched" if _event_matches_local(order, event) else "repaired_partial"
    else:
        classification = "matched" if _event_matches_local(order, event) else "repaired"

    return RithmicRecoveryItem(
        order=order,
        classification=classification,
        event=event,
        reason=("terminal_state_unknown_after_partial_fill" if unresolved else None),
        unresolved=unresolved,
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


def _normalize_transaction_type(value: str) -> str | None:
    normalized = (value or "").strip().upper()
    if normalized == "BUY":
        return "buy"
    if normalized in {"SELL", "SS"}:
        return "sell"
    return None


def _remote_may_be_working(remote) -> bool:
    notification = str(getattr(remote, "notification_type", None) or "").strip().upper()
    quantity = _decimal(getattr(remote, "quantity", None))
    filled = _decimal(getattr(remote, "filled_quantity", None))
    status = str(getattr(remote, "status", None) or "").strip().lower()
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


def _event_matches_local(order, event: ExchangeOrderEvent) -> bool:
    expected_status = {
        "open": "SUBMITTED",
        "partially_filled": "PARTIALLY_FILLED",
        "filled": "FILLED",
        "cancelled": "CANCELLED",
        "failed": "FAILED",
    }.get(event.status)
    if expected_status is None or str(getattr(order, "status", "")) != expected_status:
        return False
    if event.exchange_order_id != str(getattr(order, "exchange_order_id", "") or ""):
        return False
    if _decimal(getattr(order, "filled_quantity", None)) != (
        event.cumulative_filled_quantity or Decimal("0")
    ):
        return False
    if (event.cumulative_filled_quantity or Decimal("0")) == 0:
        return True
    return _decimal(getattr(order, "filled_price", None)) == (
        event.cumulative_average_price or Decimal("0")
    )


def _latest_history(history: list[object]) -> tuple[object | None, str | None]:
    if not history:
        return None, None
    if len(history) == 1:
        return history[0], None
    if any(item.timestamp_ms is None for item in history):
        return None, "ambiguous_order_history_ordering"
    latest_timestamp = max(item.timestamp_ms for item in history)
    latest = [item for item in history if item.timestamp_ms == latest_timestamp]
    if len(latest) != 1:
        return None, "ambiguous_order_history_ordering"
    return latest[0], None


def _deduplicate_fills(fills: list[object]) -> tuple[list[object], str | None]:
    unique = {}
    for fill in fills:
        previous = unique.get(fill.fill_id)
        fingerprint = (
            fill.basket_id,
            fill.exchange_order_id,
            fill.symbol,
            fill.transaction_type,
            fill.fill_quantity,
            fill.fill_price,
            fill.timestamp_ms,
        )
        if previous is not None and previous != fingerprint:
            return [], "conflicting_fill_id"
        unique[fill.fill_id] = fingerprint
    return [fill for fill in fills if unique.pop(fill.fill_id, None) is not None], None


def _aggregate_fills(fills: list[object]) -> tuple[Decimal, Decimal | None]:
    quantity = sum((_decimal(fill.fill_quantity) for fill in fills), Decimal("0"))
    if quantity == 0:
        return quantity, None
    notional = sum(
        (_decimal(fill.fill_quantity) * _decimal(fill.fill_price) for fill in fills),
        Decimal("0"),
    )
    return quantity, notional / quantity


def _unique_index(items: Iterable[object], field: str) -> tuple[dict[str, object], set[str]]:
    index = {}
    duplicates = set()
    for item in items:
        key = str(getattr(item, field))
        if key in index:
            duplicates.add(key)
        else:
            index[key] = item
    return index, duplicates


def _group(items: Iterable[object], field: str) -> dict[str, list[object]]:
    grouped = {}
    for item in items:
        grouped.setdefault(str(getattr(item, field)), []).append(item)
    return grouped


def _symbol_matches_product(product_id: str, symbol: str) -> bool:
    try:
        return _product_symbol(product_id) == symbol.upper()
    except (ValueError, IndexError):
        return False


def _product_symbol(product_id: str) -> str:
    return to_rithmic_symbol(product_id)


def _native_protection(order) -> dict | None:
    payload = getattr(order, "intent_payload", None)
    native = payload.get("native_protection") if isinstance(payload, dict) else None
    return native if isinstance(native, dict) else None


def _native_parent_basket(order, local_by_id: dict[str, object]) -> str | None:
    payload = getattr(order, "intent_payload", None)
    if not isinstance(payload, dict) or payload.get("placement_mode") != "attach-at-entry":
        return None
    persisted_basket_id = payload.get("native_parent_basket_id")
    if persisted_basket_id:
        return str(persisted_basket_id)
    parent_id = payload.get("pending_entry_order_id")
    parent = local_by_id.get(str(parent_id)) if parent_id is not None else None
    basket_id = getattr(parent, "exchange_order_id", None) if parent is not None else None
    return str(basket_id) if basket_id else None


def _expected_native_children(
    orders: Iterable[object],
    local_by_id: dict[str, object],
) -> dict[tuple[str, str], set[str]]:
    expected: dict[tuple[str, str], set[str]] = {}
    for order in orders:
        parent_basket = _native_parent_basket(order, local_by_id)
        if parent_basket and str(order.type) in {"stop_loss", "take_profit"}:
            payload = getattr(order, "intent_payload", None) or {}
            expected[(parent_basket, str(order.type))] = {
                str(value)
                for value in (
                    getattr(order, "client_order_id", None),
                    payload.get("native_parent_client_order_id"),
                )
                if value
            }
        native = _native_protection(order)
        if native is None or not order.exchange_order_id:
            continue
        for leg_type in (native.get("legs") or {}):
            if leg_type in {"stop_loss", "take_profit"}:
                expected[(str(order.exchange_order_id), leg_type)] = {
                    str(order.client_order_id)
                }
    return expected


def _remote_native_key(remote) -> tuple[str, str] | None:
    parent = getattr(remote, "original_basket_id", None)
    leg_type = _remote_leg_type(remote)
    return (str(parent), leg_type) if parent and leg_type else None


def _is_expected_native_child(
    remote,
    expected: dict[tuple[str, str], set[str]],
    remote_counts: dict[tuple[str, str], int],
) -> bool:
    key = _remote_native_key(remote)
    if key is None or key not in expected or remote_counts.get(key) != 1:
        return False
    client_order_id = getattr(remote, "client_order_id", None)
    return not client_order_id or str(client_order_id) in expected[key]


def _remote_leg_type(remote) -> str | None:
    price_type = str(getattr(remote, "price_type", "") or "").lower()
    if price_type in {"stop_market", "stop_limit"}:
        return "stop_loss"
    if price_type in {"limit", "market_if_touched", "limit_if_touched"}:
        return "take_profit"
    return None


def _decimal(value) -> Decimal:
    return Decimal(str(value or "0"))


def _blocked(order, reason: str, *, unresolved: bool = True) -> RithmicRecoveryItem:
    return RithmicRecoveryItem(
        order=order,
        classification="unresolved",
        reason=reason,
        unresolved=unresolved,
        verification_blocked=True,
    )
