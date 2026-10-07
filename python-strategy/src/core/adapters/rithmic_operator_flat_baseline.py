"""Private SQL journal for operator-approved Rithmic flat baselines."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event
from src.core.models import Position
from src.core.orm_models import Order, SystemEvent, Trade


_EVENT_TYPE = "ops"
_EVENT_SUBTYPE = "rithmic_operator_flat_baseline"
_ECONOMICS_REASON = "provider_exit_fill_unattributed"
_CUT_ORDER_FIELDS = (
    "id",
    "exchange_id",
    "account_profile",
    "account_id",
    "strategy_id",
    "product_id",
    "client_order_id",
    "exchange_order_id",
    "type",
    "side",
    "quantity",
    "status",
    "filled_quantity",
    "filled_price",
    "placement_mode",
    "pending_entry_order_id",
    "native_parent_client_order_id",
    "native_parent_basket_id",
    "native_leg_type",
)
_CUT_TRADE_FIELDS = (
    "id",
    "order_id",
    "exchange_trade_id",
    "product_id",
    "side",
    "price",
    "quantity",
    "fee",
    "fee_asset",
)
_ENVELOPE_FIELDS = frozenset(
    {
        "version",
        "operation_id",
        "actor",
        "profile",
        "account_id",
        "product_ids",
        "observed_at_ms",
        "prior_positions",
        "target",
        "cut_sha256",
        "economics_unresolved",
        "economics_reason",
    }
)
_POSITION_FIELDS = frozenset(
    {"strategy_id", "product_id", "side", "quantity", "entry_price"}
)
_CUT_HASH = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class _BaselineRecord:
    event_id: int
    payload: dict[str, object]


@dataclass(frozen=True)
class _BaselineRead:
    record: _BaselineRecord | None
    reason: str


SessionFactory = Callable[[], Session]


def compute_rithmic_operator_flat_cut(
    session: Session,
    *,
    profile: str,
    account_id: str,
    product_ids: Iterable[str],
) -> str:
    """Hash every scoped durable order and joined trade at this SQL snapshot."""
    normalized_profile, normalized_account, products = _scope(
        profile, account_id, product_ids
    )
    order_rows = session.scalars(
        select(Order).where(
            func.lower(Order.exchange_id) == "rithmic",
            Order.account_profile == normalized_profile,
            Order.account_id == normalized_account,
            Order.product_id.in_(products),
        )
    ).all()
    orders = sorted(
        (_order_cut_row(order) for order in order_rows),
        key=lambda row: cast(str, row["id"]),
    )

    trade_rows = session.execute(
        select(Trade, Order.product_id)
        .join(Order, Trade.order_id == Order.id)
        .where(
            func.lower(Order.exchange_id) == "rithmic",
            Order.account_profile == normalized_profile,
            Order.account_id == normalized_account,
            Order.product_id.in_(products),
        )
    ).all()
    trades = []
    for trade, order_product_id in trade_rows:
        if trade.product_id != order_product_id:
            raise ValueError("rithmic_operator_flat_baseline_trade_product_mismatch")
        trades.append(_trade_cut_row(trade))
    trades.sort(key=lambda row: row["id"])

    encoded = json.dumps(
        {"orders": orders, "trades": trades},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def write_rithmic_operator_flat_baseline(
    session_factory: SessionFactory,
    *,
    operation_id: str,
    actor: str,
    profile: str,
    account_id: str,
    product_ids: Iterable[str],
    observed_at_ms: int,
    prior_positions: Sequence[Position],
) -> _BaselineRecord:
    """Commit a V1 baseline, reusing an exact operation replay idempotently."""
    operation = _required_text(operation_id, "operation_id")
    operator = _required_text(actor, "actor")
    normalized_profile, normalized_account, products = _scope(
        profile, account_id, product_ids
    )
    if type(observed_at_ms) is not int or observed_at_ms < 0:
        raise ValueError("rithmic_operator_flat_baseline_observation_invalid")
    prior = _prior_position_rows(prior_positions, products)

    with session_factory() as session:
        cut = compute_rithmic_operator_flat_cut(
            session,
            profile=normalized_profile,
            account_id=normalized_account,
            product_ids=products,
        )
        existing_events = session.scalars(
            select(SystemEvent)
            .where(
                SystemEvent.event_type == _EVENT_TYPE,
                SystemEvent.event_subtype == _EVENT_SUBTYPE,
            )
            .order_by(SystemEvent.id)
        ).all()
        matching_replays: list[_BaselineRecord] = []
        for event in existing_events:
            payload = getattr(event, "payload")
            if (
                not isinstance(payload, dict)
                or payload.get("operation_id") != operation
            ):
                continue
            existing = _parse_payload(payload)
            if existing is None:
                raise ValueError("rithmic_operator_flat_baseline_operation_conflict")
            if (
                existing["profile"] == normalized_profile
                and existing["account_id"] == normalized_account
                and existing["product_ids"] == list(products)
                and existing["cut_sha256"] == cut
                and existing["target"] == "FLAT"
            ):
                matching_replays.append(_BaselineRecord(_event_id(event), existing))
                continue
            raise ValueError("rithmic_operator_flat_baseline_operation_conflict")
        if matching_replays:
            return matching_replays[0]

        payload: dict[str, object] = {
            "version": 1,
            "operation_id": operation,
            "actor": operator,
            "profile": normalized_profile,
            "account_id": normalized_account,
            "product_ids": list(products),
            "observed_at_ms": observed_at_ms,
            "prior_positions": prior,
            "target": "FLAT",
            "cut_sha256": cut,
            "economics_unresolved": True,
            "economics_reason": _ECONOMICS_REASON,
        }
        event = write_system_event(
            session,
            event_type=_EVENT_TYPE,
            event_subtype=_EVENT_SUBTYPE,
            payload=payload,
        )
        session.commit()
        session.refresh(event)
        validated = _parse_payload(getattr(event, "payload"))
        if validated is None:
            raise RuntimeError("rithmic_operator_flat_baseline_write_invalid")
        return _BaselineRecord(_event_id(event), validated)


def read_latest_rithmic_operator_flat_baseline(
    session_factory: SessionFactory,
    *,
    profile: str,
    account_id: str,
    product_ids: Iterable[str],
) -> _BaselineRead:
    """Read the latest exact-scope V1 event only while its durable cut matches."""
    normalized_profile, normalized_account, products = _scope(
        profile, account_id, product_ids
    )
    with session_factory() as session:
        events = session.scalars(
            select(SystemEvent)
            .where(
                SystemEvent.event_type == _EVENT_TYPE,
                SystemEvent.event_subtype == _EVENT_SUBTYPE,
            )
            .order_by(SystemEvent.id.desc())
        ).all()
        current_cut = compute_rithmic_operator_flat_cut(
            session,
            profile=normalized_profile,
            account_id=normalized_account,
            product_ids=products,
        )
    expected_products = list(products)
    for event in events:
        payload = getattr(event, "payload")
        if not _payload_matches_scope(
            payload,
            normalized_profile,
            normalized_account,
            expected_products,
        ):
            continue
        parsed = _parse_payload(payload)
        if parsed is None:
            return _BaselineRead(None, "scope_or_version_inapplicable")
        if parsed["cut_sha256"] != current_cut:
            return _BaselineRead(None, "durable_cut_changed")
        return _BaselineRead(
            _BaselineRecord(_event_id(event), parsed), "operator_flat_baseline"
        )
    return _BaselineRead(None, "no_baseline")


def _scope(
    profile: str,
    account_id: str,
    product_ids: Iterable[str],
) -> tuple[str, str, tuple[str, ...]]:
    normalized_profile = _required_text(profile, "profile").strip()
    normalized_account = _required_text(account_id, "account_id").strip()
    if isinstance(product_ids, (str, bytes)):
        raise ValueError("rithmic_operator_flat_baseline_product_scope_invalid")
    raw_products = tuple(product_ids)
    if any(
        not isinstance(product, str) or not product or product.strip() != product
        for product in raw_products
    ):
        raise ValueError("rithmic_operator_flat_baseline_product_scope_invalid")
    products = tuple(sorted(set(raw_products)))
    if not products:
        raise ValueError("rithmic_operator_flat_baseline_product_scope_invalid")
    if not normalized_profile or not normalized_account:
        raise ValueError("rithmic_operator_flat_baseline_scope_invalid")
    return normalized_profile, normalized_account, products


def _order_cut_row(order: Order) -> dict[str, object]:
    intent = order.intent_payload
    if intent is not None and not isinstance(intent, dict):
        raise ValueError("rithmic_operator_flat_baseline_order_intent_invalid")
    row: dict[str, object] = {}
    for field in _CUT_ORDER_FIELDS:
        if field in {
            "placement_mode",
            "pending_entry_order_id",
            "native_parent_client_order_id",
            "native_parent_basket_id",
            "native_leg_type",
        }:
            value = intent.get(field) if intent is not None else None
            row[field] = _optional_text(value, field)
        elif field == "exchange_id":
            row[field] = _required_text(getattr(order, field), field).lower()
        elif field in {"quantity", "filled_quantity", "filled_price"}:
            value = getattr(order, field)
            row[field] = None if value is None else _decimal_text(value, field)
        elif field in {"client_order_id", "exchange_order_id"}:
            row[field] = _optional_text(getattr(order, field), field)
        else:
            row[field] = _required_text(getattr(order, field), field)
    return row


def _trade_cut_row(trade: Trade) -> dict[str, object]:
    row: dict[str, object] = {}
    for field in _CUT_TRADE_FIELDS:
        value = getattr(trade, field)
        if field in {"price", "quantity", "fee"}:
            row[field] = None if value is None else _decimal_text(value, field)
        elif field in {"exchange_trade_id", "fee_asset"}:
            row[field] = _optional_text(value, field)
        else:
            row[field] = _required_text(value, field)
    return row


def _prior_position_rows(
    positions: Sequence[Position],
    product_ids: tuple[str, ...],
) -> list[dict[str, str]]:
    rows = []
    for position in positions:
        side = str(getattr(position.side, "value", position.side))
        if side not in {"LONG", "SHORT"}:
            raise ValueError("rithmic_operator_flat_baseline_position_invalid")
        quantity = _decimal_text(position.quantity, "position.quantity")
        entry_price = _decimal_text(position.entry_price, "position.entry_price")
        if Decimal(quantity) <= 0:
            raise ValueError("rithmic_operator_flat_baseline_position_invalid")
        product_id = _required_text(position.product_id, "product_id")
        if product_id not in product_ids:
            raise ValueError("rithmic_operator_flat_baseline_position_out_of_scope")
        rows.append(
            {
                "strategy_id": _required_text(position.strategy_id, "strategy_id"),
                "product_id": product_id,
                "side": side,
                "quantity": quantity,
                "entry_price": entry_price,
            }
        )
    rows.sort(key=lambda row: (row["strategy_id"], row["product_id"]))
    if len({(row["strategy_id"], row["product_id"]) for row in rows}) != len(rows):
        raise ValueError("rithmic_operator_flat_baseline_position_duplicate")
    return rows


def _parse_payload(payload: object) -> dict[str, object] | None:
    if not isinstance(payload, dict) or set(payload) != _ENVELOPE_FIELDS:
        return None
    if type(payload.get("version")) is not int or payload["version"] != 1:
        return None
    for field in ("operation_id", "actor", "profile", "account_id", "target"):
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            return None
    if payload["target"] != "FLAT":
        return None
    if type(payload.get("observed_at_ms")) is not int or payload["observed_at_ms"] < 0:
        return None
    products = payload.get("product_ids")
    if (
        not isinstance(products, list)
        or not products
        or any(
            not isinstance(value, str) or not value or value.strip() != value
            for value in products
        )
        or products != sorted(set(products))
    ):
        return None
    prior = payload.get("prior_positions")
    if not isinstance(prior, list):
        return None
    prior_keys = []
    for position in prior:
        if not isinstance(position, dict) or set(position) != _POSITION_FIELDS:
            return None
        if any(
            not isinstance(position.get(field), str) or not position[field]
            for field in ("strategy_id", "product_id")
        ):
            return None
        if not isinstance(position.get("side"), str) or position["side"] not in {
            "LONG",
            "SHORT",
        }:
            return None
        for field in ("quantity", "entry_price"):
            text_value = position.get(field)
            if not isinstance(text_value, str) or not _is_canonical_decimal(text_value):
                return None
        if Decimal(position["quantity"]) <= 0:
            return None
        if position["product_id"] not in products:
            return None
        prior_keys.append((position["strategy_id"], position["product_id"]))
    if prior_keys != sorted(set(prior_keys)):
        return None
    cut = payload.get("cut_sha256")
    if not isinstance(cut, str) or _CUT_HASH.fullmatch(cut) is None:
        return None
    if payload.get("economics_unresolved") is not True:
        return None
    if payload.get("economics_reason") != _ECONOMICS_REASON:
        return None
    return dict(payload)


def _payload_matches_scope(
    payload: object,
    profile: str,
    account_id: str,
    product_ids: list[str],
) -> bool:
    return (
        isinstance(payload, Mapping)
        and payload.get("profile") == profile
        and payload.get("account_id") == account_id
        and payload.get("product_ids") == product_ids
    )


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"rithmic_operator_flat_baseline_{field}_invalid")
    return value


def _event_id(event: SystemEvent) -> int:
    return int(getattr(event, "id"))


def _optional_text(value: object, field: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise ValueError(f"rithmic_operator_flat_baseline_{field}_invalid")
    return value


def _decimal_text(value: object, field: str) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"rithmic_operator_flat_baseline_{field}_invalid")
    if value.is_zero():
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _is_canonical_decimal(value: str) -> bool:
    try:
        parsed = Decimal(value)
    except Exception:
        return False
    if not parsed.is_finite():
        return False
    return _decimal_text(parsed, "payload") == value
