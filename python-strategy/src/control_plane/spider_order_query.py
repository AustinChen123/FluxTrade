"""Read-only, server-scoped local Spider order and execution inspection."""

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any
from urllib.parse import parse_qsl, unquote_to_bytes

from sqlalchemy import and_, func, select, text
from sqlalchemy.orm import Session

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.models import OrderStatus
from src.core.decimal_math import canonical_decimal_text
from src.core.orm_models import Order, Trade
from src.core.runtime_environment import validate_environment_identity
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeOperation,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
)
from src.core.spider_management.financial_delivery import (
    _projection_matches_install,
    prove_spider_order_wire_binding,
)

SPIDER_ORDERS_PATH = "/api/v1/spider/orders"
_ORDER_SCHEMA = "spider_local_orders_v1"
_TRADE_SCHEMA = "spider_local_trades_v1"
_VERSION_DOMAIN = "SPIDER_LOCAL_ORDER_VIEW_V1"
_ORDERING = "ORDER_CREATED_DESC_LOCAL_ID_ASC_V1"
_TRADE_ORDERING = "TRADE_TIME_ASC_LOCAL_ID_ASC_V1"
_MAX_BASE_ORDERS = 5000
_MAX_LINKED_TRADES = 50000
_QUERY_KEYS = frozenset(
    {
        "strategy_id",
        "category",
        "local_order_id",
        "order_created_from_ms",
        "order_created_to_ms",
        "limit",
        "offset",
        "view_version",
    }
)
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_CATEGORIES = frozenset({"ALL", "CURRENT", "PENDING", "HISTORY"})
_KNOWN_STATUSES = frozenset(item.value for item in OrderStatus)


class _Invalid(ValueError):
    pass


class _Evidence(ValueError):
    pass


class _NotFound(Exception):
    def __init__(self, code: str) -> None:
        if code not in {"spider_scope_not_found", "spider_order_not_found"}:
            raise ValueError("invalid local query not-found code")
        self.code = code


class _TooLarge(ValueError):
    pass


def _opaque(value: str, maximum: int, *, local: bool = False) -> bool:
    return (
        1 <= len(value) <= maximum
        and "\x00" not in value
        and not any(ord(char) < 32 or ord(char) == 127 for char in value)
        and (not local or not any(char in "/\\" for char in value))
    )


def _integer(value: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
        raise _Invalid
    try:
        number = int(value)
    except ValueError:
        raise _Invalid from None
    if number < minimum or maximum is not None and number > maximum:
        raise _Invalid
    return number


def _time_ms(value: str) -> int:
    if not re.fullmatch(r"-?(?:0|[1-9][0-9]*)", value):
        raise _Invalid
    try:
        number = int(value)
    except ValueError:
        raise _Invalid from None
    if not -62135596800000 <= number <= 253402300799999:
        raise _Invalid
    return number


def _parse_query(raw_query: str, body: str | bytes | None) -> dict[str, Any]:
    if body is not None:
        raise _Invalid
    if re.search(r"%(?![0-9a-fA-F]{2})", raw_query):
        raise _Invalid
    try:
        pairs = parse_qsl(
            raw_query,
            keep_blank_values=True,
            strict_parsing=True,
            encoding="utf-8",
            errors="strict",
            max_num_fields=8,
        )
    except (ValueError, UnicodeError):
        raise _Invalid from None
    if any(key not in _QUERY_KEYS for key, _ in pairs) or len(
        {k for k, _ in pairs}
    ) != len(pairs):
        raise _Invalid
    values = dict(pairs)
    strategy = values.get("strategy_id")
    category = values.get("category", "ALL")
    local_id = values.get("local_order_id")
    if strategy is None or not _opaque(strategy, 128) or category not in _CATEGORIES:
        raise _Invalid
    if local_id is not None and not _opaque(local_id, 36, local=True):
        raise _Invalid
    start, end = values.get("order_created_from_ms"), values.get("order_created_to_ms")
    if (start is None) != (end is None):
        raise _Invalid
    start_ms = end_ms = None
    if start is not None and end is not None:
        start_ms = _time_ms(start)
        end_ms = _time_ms(end)
        if start_ms >= end_ms:
            raise _Invalid
    limit = _integer(values.get("limit", "20"), minimum=1, maximum=100)
    offset = _integer(values.get("offset", "0"))
    version = values.get("view_version")
    if version is not None and not _HEX64.fullmatch(version):
        raise _Invalid
    if offset and version is None:
        raise _Invalid
    return {
        "strategy_id": strategy,
        "category": category,
        "local_order_id": local_id,
        "order_created_from_ms": start_ms,
        "order_created_to_ms": end_ms,
        "limit": limit,
        "offset": offset,
        "view_version": version,
    }


def _decode_route_id(encoded: str) -> str:
    try:
        raw = unquote_to_bytes(encoded)
        value = raw.decode("utf-8", errors="strict")
    except (UnicodeError, ValueError):
        raise _Invalid from None
    if re.search(r"%(?![0-9a-fA-F]{2})", encoded) or not _opaque(value, 36, local=True):
        raise _Invalid
    return value


def classify_presentation(action_state: str, status: str) -> str:
    """Single presentation-only classifier; it grants no mutation authority."""
    if (
        action_state == "SEND_UNKNOWN"
        or status == "SUBMITTED_UNCONFIRMED"
        or status not in _KNOWN_STATUSES
    ):
        return "PENDING"
    if action_state in {"REJECTED", "TERMINAL"} or status in {
        "FILLED",
        "CANCELLED",
        "FAILED",
        "LIQUIDATED",
    }:
        return "HISTORY"
    if action_state == "UNSENT" or status == "NEW":
        return "PENDING"
    return "CURRENT"


def _decimal(value: Decimal | None, *, nullable: bool) -> str | None:
    if value is None:
        if nullable:
            return None
        raise _Evidence
    if type(value) is not Decimal or not value.is_finite():
        raise _Evidence
    return canonical_decimal_text(value)


def _date(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise _Evidence
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _view_version(
    scope: dict[str, str],
    filters: dict[str, Any],
    orders: list[dict[str, Any]],
    trades: list[dict[str, Any]],
) -> str:
    canonical = {
        "domain": _VERSION_DOMAIN,
        "orders_schema_version": _ORDER_SCHEMA,
        "trades_schema_version": _TRADE_SCHEMA,
        "orders_ordering_id": _ORDERING,
        "trades_ordering_id": _TRADE_ORDERING,
        "scope": scope,
        "filters": filters,
        "orders": orders,
        "trades": trades,
    }
    return sha256(canonical_bytes(canonical)).hexdigest()


def _wire_proof(
    scope: SpiderRuntimeScope,
    action: SpiderRuntimeAction,
    binding: SpiderRuntimeBinding,
    order: Order,
) -> tuple[str, str | None, str]:
    try:
        proof = prove_spider_order_wire_binding(
            scope.environment,
            scope.execution_scope_id,
            scope.strategy_id,
            action,
            binding,
            order,
        )
        intent = order.intent_payload
        source = decode_canonical(action.original_payload)
        if (
            proof is None
            or type(intent) is not dict
            or type(source) is not dict
            or action.payload_sha256 != sha256(action.original_payload).hexdigest()
            or source.get("instId") != binding.source_product_id
            or source.get("clOrdId") != binding.source_client_order_id
            or source.get("side") != order.side
            or source.get("ordType") != order.type
            or Decimal(source.get("sz", "NaN")) != order.quantity
            or Decimal(source.get("px", "NaN")) != order.price
        ):
            raise _Evidence
        return proof
    except (KeyError, TypeError, ValueError, ArithmeticError):
        raise _Evidence from None


class SpiderOrderQueryService:
    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        *,
        environment: str,
        execution_scope_id: str,
    ) -> None:
        self._sessions = sessions
        self._environment = validate_environment_identity(environment)
        if not _opaque(execution_scope_id, 128):
            raise ValueError("invalid configured Spider scope")
        self._execution_scope_id = execution_scope_id

    def orders(
        self, raw_query: str, body: str | bytes | None = None
    ) -> tuple[int, dict[str, Any]]:
        return self._query(raw_query, body, None)

    def trades(
        self, local_order_id: str, raw_query: str, body: str | bytes | None = None
    ) -> tuple[int, dict[str, Any]]:
        try:
            decoded_id = _decode_route_id(local_order_id)
            query = _parse_query(raw_query, body)
            if (
                query["local_order_id"] is not None
                and query["local_order_id"] != decoded_id
            ):
                raise _Invalid
        except _Invalid:
            return 422, {"error": "validation_error"}
        return self._query_values(query, decoded_id)

    def _query(
        self, raw_query: str, body: str | bytes | None, route_id: str | None
    ) -> tuple[int, dict[str, Any]]:
        try:
            query = _parse_query(raw_query, body)
        except _Invalid:
            return 422, {"error": "validation_error"}
        return self._query_values(query, route_id)

    def _query_values(
        self, query: dict[str, Any], route_id: str | None
    ) -> tuple[int, dict[str, Any]]:
        try:
            return 200, self._read(query, route_id)
        except _NotFound as exc:
            return 404, {"error": exc.code}
        except _TooLarge:
            return 413, {"error": "spider_order_view_too_large"}
        except _Invalid:
            return 422, {"error": "validation_error"}
        except _VersionChanged:
            return 409, {"error": "spider_order_view_changed"}
        except _Evidence:
            return 503, {"error": "spider_order_evidence_unavailable"}
        except Exception:
            # Exception text and row/account data intentionally never leave this boundary.
            return 503, {"error": "spider_order_query_unavailable"}

    def _read(self, query: dict[str, Any], route_id: str | None) -> dict[str, Any]:
        strategy = query["strategy_id"]
        scope_key = (self._environment, self._execution_scope_id, strategy)
        with self._sessions() as session:
            with session.begin():
                session.execute(
                    text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                )
                session.execute(text("SET LOCAL statement_timeout = '5000ms'"))
                observed = session.execute(
                    text("SELECT transaction_timestamp()")
                ).scalar_one()
                scope = session.execute(
                    select(SpiderRuntimeScope).where(
                        SpiderRuntimeScope.environment == scope_key[0],
                        SpiderRuntimeScope.execution_scope_id == scope_key[1],
                        SpiderRuntimeScope.strategy_id == scope_key[2],
                    )
                ).scalar_one_or_none()
                if scope is None:
                    raise _NotFound("spider_scope_not_found")
                plans = (
                    session.execute(
                        select(SpiderRuntimePlan).where(
                            SpiderRuntimePlan.environment == scope_key[0],
                            SpiderRuntimePlan.execution_scope_id == scope_key[1],
                            SpiderRuntimePlan.strategy_id == scope_key[2],
                        )
                    )
                    .scalars()
                    .all()
                )
                plan_by_id = {plan.plan_id: plan for plan in plans}
                current = plan_by_id.get(scope.initial_plan_id)
                if (
                    current is None
                    or current.operation_id != scope.current_operation_id
                    or current.generation != scope.generation
                    or current.configuration_sha256 != scope.configuration_sha256
                ):
                    raise _Evidence
                current_manifest = _manifest(current, scope)
                bootstrap = current_manifest.get("bootstrap_basis")
                if type(bootstrap) is not dict:
                    raise _Evidence
                current_account = bootstrap.get("runtime_selection")
                if type(current_account) is not dict:
                    raise _Evidence
                if (
                    current_account.get("environment") != scope.environment
                    or current_account.get("execution_scope_id")
                    != scope.execution_scope_id
                    or current_account.get("strategy_id") != scope.strategy_id
                    or current_account.get("configuration_sha256")
                    != scope.configuration_sha256
                ):
                    raise _Evidence
                row_filters = [
                    SpiderRuntimeAction.environment == scope_key[0],
                    SpiderRuntimeAction.execution_scope_id == scope_key[1],
                    SpiderRuntimeAction.strategy_id == scope_key[2],
                ]
                if query["order_created_from_ms"] is not None:
                    row_filters.extend(
                        [
                            Order.timestamp >= query["order_created_from_ms"],
                            Order.timestamp < query["order_created_to_ms"],
                        ]
                    )
                if query["local_order_id"] is not None:
                    row_filters.append(Order.id == query["local_order_id"])
                rows = session.execute(
                    select(
                        SpiderRuntimeBinding,
                        SpiderRuntimeAction,
                        Order,
                        SpiderRuntimePlan,
                        SpiderRuntimeOperation,
                    )
                    .select_from(SpiderRuntimeAction)
                    .outerjoin(
                        SpiderRuntimeBinding,
                        and_(
                            SpiderRuntimeBinding.action_id
                            == SpiderRuntimeAction.action_id,
                            SpiderRuntimeBinding.environment == scope_key[0],
                            SpiderRuntimeBinding.execution_scope_id == scope_key[1],
                            SpiderRuntimeBinding.strategy_id == scope_key[2],
                        ),
                    )
                    .join(
                        Order,
                        Order.id
                        == func.coalesce(
                            SpiderRuntimeBinding.local_order_id,
                            SpiderRuntimeAction.local_order_id,
                        ),
                    )
                    .join(
                        SpiderRuntimePlan,
                        SpiderRuntimePlan.plan_id == SpiderRuntimeAction.plan_id,
                    )
                    .join(
                        SpiderRuntimeOperation,
                        SpiderRuntimeOperation.operation_id
                        == SpiderRuntimePlan.operation_id,
                    )
                    .where(*row_filters)
                    .order_by(Order.timestamp.desc(), Order.id.collate("C").asc())
                    .limit(_MAX_BASE_ORDERS + 1)
                ).all()
                if len(rows) > _MAX_BASE_ORDERS:
                    raise _TooLarge
                if query["local_order_id"] is not None and not rows:
                    raise _NotFound("spider_order_not_found")
                orders: list[dict[str, Any]] = []
                all_trades: list[dict[str, Any]] = []
                for binding, action, order, plan, operation in rows:
                    if binding is None or action.local_order_id != order.id:
                        raise _Evidence
                    try:
                        _check_plan(
                            plan, operation, action, binding, scope, current_account
                        )
                    except (ValueError, TypeError, ArithmeticError, KeyError):
                        raise _Evidence from None
                    before = {
                        name: getattr(order, name)
                        for name in (
                            "id",
                            "strategy_id",
                            "product_id",
                            "exchange_id",
                            "account_profile",
                            "account_id",
                            "type",
                            "side",
                            "price",
                            "trigger_price",
                            "quantity",
                            "client_order_id",
                        )
                    }
                    try:
                        projection_ok = _projection_matches_install(
                            order, binding, action, before, current_account
                        )
                    except (ValueError, TypeError, ArithmeticError, KeyError):
                        raise _Evidence from None
                    if not projection_ok:
                        raise _Evidence
                    source_client, _local_client, wire_client = _wire_proof(
                        scope, action, binding, order
                    )
                    trade_count = session.execute(
                        select(func.count())
                        .select_from(Trade)
                        .where(Trade.order_id == order.id)
                    ).scalar_one()
                    if type(trade_count) is not int or trade_count < 0:
                        raise _Evidence
                    if len(all_trades) + trade_count > _MAX_LINKED_TRADES:
                        raise _TooLarge
                    order_trades = (
                        session.execute(
                            select(Trade)
                            .where(Trade.order_id == order.id)
                            .order_by(
                                Trade.timestamp.asc(), Trade.id.collate("C").asc()
                            )
                        )
                        .scalars()
                        .all()
                    )
                    row_trades = [_trade_dto(trade, order) for trade in order_trades]
                    all_trades.extend(row_trades)
                    category = classify_presentation(action.outcome_state, order.status)
                    orders.append(
                        _order_dto(
                            order,
                            binding,
                            action,
                            plan,
                            source_client,
                            wire_client,
                            category,
                            len(row_trades),
                        )
                    )
                if len(all_trades) > _MAX_LINKED_TRADES:
                    raise _TooLarge
                all_trades.sort(
                    key=lambda row: (row["timestamp_ms"], row["local_trade_id"])
                )
                filters = {
                    "category": query["category"],
                    "local_order_id": query["local_order_id"],
                    "order_created_from_ms": query["order_created_from_ms"],
                    "order_created_to_ms": query["order_created_to_ms"],
                }
                scope_dto = {
                    "environment": scope.environment,
                    "execution_scope_id": scope.execution_scope_id,
                    "strategy_id": strategy,
                }
                version = _view_version(scope_dto, filters, orders, all_trades)
                if (
                    query["view_version"] is not None
                    and query["view_version"] != version
                ):
                    raise _VersionChanged
                selected = [
                    item
                    for item in orders
                    if query["category"] == "ALL"
                    or item["category"] == query["category"]
                ]
                if route_id is not None and route_id not in {
                    item["local_order_id"] for item in selected
                }:
                    raise _NotFound("spider_order_not_found")
                offset, limit = query["offset"], query["limit"]
                if route_id is None and offset > len(selected):
                    raise _Invalid
                page = selected[offset : offset + limit] if route_id is None else []
                next_offset = (
                    offset + len(page)
                    if route_id is None and offset + len(page) < len(selected)
                    else None
                )
                base = {
                    "schema_version": _ORDER_SCHEMA
                    if route_id is None
                    else _TRADE_SCHEMA,
                    "scope": scope_dto,
                    "source": "SQL_LOCAL_FACTS",
                    "provider_reconciliation": "NOT_ASSERTED",
                    "observed_at": _date(observed),
                    "filters": filters,
                    "view_version": version,
                    "limit": limit,
                    "offset": offset,
                    "total_count": len(selected),
                    "next_offset": next_offset,
                }
                if route_id is None:
                    return {**base, "orders": page}
                trades = [
                    item for item in all_trades if item["local_order_id"] == route_id
                ]
                if query["offset"] > len(trades):
                    raise _Invalid
                trade_page = trades[offset : offset + limit]
                base["total_count"] = len(trades)
                base["next_offset"] = (
                    offset + len(trade_page)
                    if offset + len(trade_page) < len(trades)
                    else None
                )
                return {**base, "local_order_id": route_id, "trades": trade_page}


class _VersionChanged(Exception):
    pass


def _manifest(plan: SpiderRuntimePlan, scope: SpiderRuntimeScope) -> dict[str, Any]:
    if plan.manifest_sha256 != sha256(plan.manifest_payload).hexdigest() or (
        plan.environment,
        plan.execution_scope_id,
        plan.strategy_id,
    ) != (scope.environment, scope.execution_scope_id, scope.strategy_id):
        raise _Evidence
    try:
        value = decode_canonical(plan.manifest_payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise _Evidence from None
    if type(value) is not dict:
        raise _Evidence
    bootstrap = value.get("bootstrap_basis")
    initial = value.get("initial_manifest")
    source_map = value.get("source_product_map")
    if (
        type(bootstrap) is not dict
        or type(initial) is not dict
        or type(source_map) is not dict
        or value.get("domain") != "SPIDER_INITIAL_MANIFEST_V1"
        or value.get("operation_id") != plan.operation_id
        or value.get("generation") != plan.generation
        or value.get("configuration_sha256") != plan.configuration_sha256
        or value.get("bootstrap_digest")
        != sha256(canonical_bytes(bootstrap)).hexdigest()
        or initial.get("bootstrap_basis_sha256") != value.get("bootstrap_digest")
        or initial.get("product_map") != source_map
        or initial.get("product_map_sha256")
        != sha256(canonical_bytes(source_map)).hexdigest()
        or bootstrap.get("product_map_sha256")
        != sha256(canonical_bytes(source_map)).hexdigest()
        or plan.original_build_checkpoint_sha256
        != sha256(plan.original_build_checkpoint).hexdigest()
    ):
        raise _Evidence
    return value


def _check_plan(
    plan: SpiderRuntimePlan,
    operation: SpiderRuntimeOperation,
    action: SpiderRuntimeAction,
    binding: SpiderRuntimeBinding,
    scope: SpiderRuntimeScope,
    current_account: dict[str, object],
) -> None:
    manifest = _manifest(plan, scope)
    if (
        plan.generation != action.generation
        or plan.configuration_sha256 != action.configuration_sha256
        or (operation.environment, operation.execution_scope_id, operation.strategy_id)
        != (scope.environment, scope.execution_scope_id, scope.strategy_id)
        or operation.operation_id != plan.operation_id
        or action.plan_id != plan.plan_id
        or manifest.get("domain") != "SPIDER_INITIAL_MANIFEST_V1"
        or manifest.get("operation_id") != plan.operation_id
        or manifest.get("generation") != plan.generation
        or manifest.get("configuration_sha256") != plan.configuration_sha256
    ):
        raise _Evidence
    bootstrap = manifest.get("bootstrap_basis")
    initial = manifest.get("initial_manifest")
    source_map = manifest.get("source_product_map")
    if (
        type(bootstrap) is not dict
        or type(initial) is not dict
        or type(source_map) is not dict
        or manifest.get("bootstrap_digest")
        != sha256(canonical_bytes(bootstrap)).hexdigest()
        or initial.get("bootstrap_basis_sha256") != manifest.get("bootstrap_digest")
        or initial.get("product_map") != source_map
        or initial.get("product_map_sha256")
        != sha256(canonical_bytes(source_map)).hexdigest()
        or bootstrap.get("product_map_sha256")
        != sha256(canonical_bytes(source_map)).hexdigest()
        or binding.product_map_sha256 != sha256(canonical_bytes(source_map)).hexdigest()
        or plan.original_build_checkpoint_sha256
        != sha256(plan.original_build_checkpoint).hexdigest()
    ):
        raise _Evidence
    entries = source_map.get("entries")
    if type(entries) is not list:
        raise _Evidence
    mapped = [
        entry
        for entry in entries
        if type(entry) is dict
        and entry.get("source_product_id") == binding.source_product_id
    ]
    if (
        len(mapped) != 1
        or type(mapped[0].get("instrument_spec")) is not dict
        or mapped[0]["instrument_spec"].get("product_id")
        != binding.canonical_product_id
    ):
        raise _Evidence
    selection = bootstrap.get("runtime_selection")
    if (
        type(selection) is not dict
        or selection.get("account_profile") != current_account.get("account_profile")
        or selection.get("account_id") != current_account.get("account_id")
    ):
        raise _Evidence


def _order_dto(
    order: Order,
    binding: SpiderRuntimeBinding,
    action: SpiderRuntimeAction,
    plan: SpiderRuntimePlan,
    source: str,
    wire: str,
    category: str,
    trade_count: int,
) -> dict[str, Any]:
    status = order.status
    return {
        "local_order_id": order.id,
        "client_order_id": order.client_order_id,
        "source_client_order_id": source,
        "provider_wire_client_order_id": wire,
        "provider_order_id": order.exchange_order_id,
        "provider_order_id_status": "RECORDED"
        if order.exchange_order_id is not None
        else "NOT_RECORDED",
        "strategy_id": order.strategy_id,
        "account_profile": order.account_profile,
        "account_id": order.account_id,
        "exchange_id": order.exchange_id,
        "source_product_id": binding.source_product_id,
        "product_id": order.product_id,
        "role": action.role,
        "plan_id": plan.plan_id,
        "action_id": action.action_id,
        "operation_id": plan.operation_id,
        "generation": _count(action.generation),
        "configuration_sha256": action.configuration_sha256,
        "transmission_state": action.outcome_state,
        "order_status": status,
        "category": category,
        "order_status_recognized": status in _KNOWN_STATUSES,
        "order_type": order.type,
        "side": order.side,
        "quantity": _decimal(order.quantity, nullable=False),
        "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
        "price": _decimal(order.price, nullable=True),
        "trigger_price": _decimal(order.trigger_price, nullable=True),
        "filled_quantity": _decimal(order.filled_quantity, nullable=True),
        "filled_price": _decimal(order.filled_price, nullable=True),
        "created_timestamp_ms": _timestamp(order.timestamp),
        "submitted_at": _date(order.submitted_at),
        "acked_at": _date(order.acked_at),
        "last_reconciled_at": _date(order.last_reconciled_at),
        "trade_count": _count(trade_count),
    }


def _trade_dto(trade: Trade, order: Order) -> dict[str, Any]:
    if (
        trade.order_id != order.id
        or trade.product_id != order.product_id
        or trade.side != order.side
    ):
        raise _Evidence
    fee_complete = trade.fee is not None and trade.fee_asset is not None
    return {
        "local_trade_id": trade.id,
        "local_order_id": trade.order_id,
        "provider_trade_id": trade.exchange_trade_id,
        "provider_trade_id_status": "RECORDED"
        if trade.exchange_trade_id is not None
        else "NOT_RECORDED",
        "product_id": trade.product_id,
        "side": trade.side,
        "price": _decimal(trade.price, nullable=False),
        "quantity": _decimal(trade.quantity, nullable=False),
        "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
        "fee": _decimal(trade.fee, nullable=True),
        "fee_asset": trade.fee_asset,
        "fee_evidence": "RECORDED" if fee_complete else "INCOMPLETE",
        "timestamp_ms": _timestamp(trade.timestamp),
    }


def _count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise _Evidence
    return value


def _timestamp(value: object) -> int:
    if type(value) is not int or not -62135596800000 <= value <= 253402300799999:
        raise _Evidence
    return value
