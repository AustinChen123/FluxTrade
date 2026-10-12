"""Private TX A receipts for existing Spider-owned Order and Trade writes."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.decimal_math import canonical_decimal_text
from src.core.orm_models import Order, Trade
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimeOperation,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)

_PAYLOAD_KEYS = {
    "schema_version", "scope", "kind", "binding", "trade_id",
    "transition_ordinal", "before", "after", "binding_resolution",
    "reason_code",
}
_PROJECTION_KEYS = {
    "status", "filled_quantity", "filled_price", "exchange_order_id"
}
_MAX_SEQUENCE = 2**63 - 1


def _digest(data: bytes) -> str:
    return sha256(data).hexdigest()


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else canonical_decimal_text(value)


def _projection(values: Mapping[str, object]) -> dict[str, object]:
    filled_quantity = values["filled_quantity"]
    filled_price = values["filled_price"]
    return {
        "status": values["status"],
        "filled_quantity": _decimal(cast(Decimal | None, filled_quantity)),
        "filled_price": _decimal(cast(Decimal | None, filled_price)),
        "exchange_order_id": values["exchange_order_id"],
    }


def _order_projection(order: Order) -> dict[str, object]:
    return _projection({
        "status": order.status,
        "filled_quantity": order.filled_quantity,
        "filled_price": order.filled_price,
        "exchange_order_id": order.exchange_order_id,
    })


def _same_projection(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    return (
        set(left) == _PROJECTION_KEYS
        and set(right) == _PROJECTION_KEYS
        and all(left[key] == right[key] for key in _PROJECTION_KEYS)
    )


def _scope(binding: SpiderRuntimeBinding) -> dict[str, str]:
    return {
        "environment": binding.environment,
        "execution_scope_id": binding.execution_scope_id,
        "strategy_id": binding.strategy_id,
    }


def _binding_value(binding: SpiderRuntimeBinding, action: SpiderRuntimeAction) -> dict[str, object]:
    return {
        "action_id": binding.action_id,
        "local_order_id": binding.local_order_id,
        "source_product_id": binding.source_product_id,
        "canonical_product_id": binding.canonical_product_id,
        "source_client_order_id": binding.source_client_order_id,
        "generation": action.generation,
        "configuration_sha256": action.configuration_sha256,
    }


def stage_financial_receipt(
    session: Session,
    order: Order,
    trade: Trade | None,
    owned_scopes: Mapping[str, tuple[str, str]],
) -> bool:
    """Stage one receipt with the repository's existing Order/Trade transaction."""
    if not session.in_transaction() or not session.is_active:
        raise RuntimeError("spider_financial_participant_requires_active_transaction")
    with session.no_autoflush:
        binding = session.execute(
            select(SpiderRuntimeBinding.__table__).where(
                SpiderRuntimeBinding.local_order_id == order.id
            )
        ).mappings().one_or_none()
        if binding is None:
            if order.strategy_id in owned_scopes:
                logging.getLogger(__name__).warning(
                    "Spider-scoped financial write has no durable binding: %s",
                    order.strategy_id,
                )
            return False
        environment = cast(str, binding["environment"])
        execution_scope_id = cast(str, binding["execution_scope_id"])
        strategy_id = cast(str, binding["strategy_id"])
        if owned_scopes.get(strategy_id) != (environment, execution_scope_id):
            return False
        bound = session.execute(
            select(SpiderRuntimeBinding).where(
                SpiderRuntimeBinding.action_id == binding["action_id"],
                SpiderRuntimeBinding.environment == environment,
                SpiderRuntimeBinding.execution_scope_id == execution_scope_id,
                SpiderRuntimeBinding.strategy_id == strategy_id,
            )
        ).scalar_one_or_none()
        if bound is None:
            raise RuntimeError("spider_financial_binding_unavailable")
        scope_row = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == environment,
                SpiderRuntimeScope.execution_scope_id == execution_scope_id,
                SpiderRuntimeScope.strategy_id == strategy_id,
            ).with_for_update()
        ).scalar_one_or_none()
        if scope_row is None:
            return False
        action = session.execute(
            select(SpiderRuntimeAction).where(
                SpiderRuntimeAction.action_id == bound.action_id,
                SpiderRuntimeAction.environment == environment,
                SpiderRuntimeAction.execution_scope_id == execution_scope_id,
                SpiderRuntimeAction.strategy_id == strategy_id,
            )
        ).scalar_one_or_none()
        if action is None:
            raise RuntimeError("spider_financial_action_unavailable")
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == scope_row.initial_plan_id,
                SpiderRuntimePlan.operation_id == scope_row.current_operation_id,
            )
        ).scalar_one_or_none()
        operation = session.execute(
            select(SpiderRuntimeOperation).where(
                SpiderRuntimeOperation.operation_id == scope_row.current_operation_id,
                SpiderRuntimeOperation.environment == environment,
                SpiderRuntimeOperation.execution_scope_id == execution_scope_id,
                SpiderRuntimeOperation.strategy_id == strategy_id,
            )
        ).scalar_one_or_none()
        if plan is None or operation is None or operation.phase != "INCOMPLETE":
            raise RuntimeError("spider_financial_runtime_unavailable")
        manifest = decode_canonical(plan.manifest_payload)
        if type(manifest) is not dict:
            raise RuntimeError("spider_financial_manifest_unavailable")
        bootstrap = cast(dict[str, object], manifest.get("bootstrap_basis"))
        selection = bootstrap.get("runtime_selection")
        if type(selection) is not dict:
            raise RuntimeError("spider_financial_selection_unavailable")

        # Lock Order after its owning runtime row. Scalar selection bypasses a
        # possibly dirty identity-map object, while no_autoflush preserves the
        # caller's intended write for the later repository flush.
        before_row = session.execute(
            select(
                Order.id,
                Order.strategy_id,
                Order.product_id,
                Order.exchange_id,
                Order.account_profile,
                Order.account_id,
                Order.type,
                Order.side,
                Order.price,
                Order.trigger_price,
                Order.quantity,
                Order.status,
                Order.filled_quantity,
                Order.filled_price,
                Order.client_order_id,
                Order.intent_payload,
                Order.exchange_order_id,
            ).where(Order.id == bound.local_order_id).with_for_update()
        ).mappings().one_or_none()
        if before_row is None:
            raise RuntimeError("spider_financial_order_unavailable")
        before_values = cast(Mapping[str, object], before_row)
        before = _projection(before_values)
        after = _order_projection(order)
        binding_value = _binding_value(bound, action)
        reason: str | None = None
        if not _projection_matches_install(
            order, bound, action, before_values, cast(dict[str, object], selection)
        ):
            reason = "BINDING_PROJECTION_MISMATCH"
        elif before["exchange_order_id"] is not None and (
            after["exchange_order_id"] != before["exchange_order_id"]
        ):
            reason = "BINDING_PROVIDER_ID_CONFLICT"

        if trade is None:
            if _same_projection(before, after):
                return False
            transition_ordinal = _next_transition_ordinal(
                session, bound, scope_row.last_received_sequence
            )
            identity = {
                "scope": _scope(bound),
                "local_order_id": bound.local_order_id,
                "transition_ordinal": transition_ordinal,
            }
            kind = "ORDER_STATE"
            trade_id = None
            delivery_id = _digest(b"SPIDER_ORDER_STATE_V1\0" + canonical_bytes(identity))
        else:
            if trade.order_id != order.id or not trade.id:
                raise RuntimeError("spider_financial_trade_order_mismatch")
            transition_ordinal = None
            kind = "FILL"
            trade_id = trade.id
            identity = {"scope": _scope(bound), "trade_id": trade_id}
            delivery_id = _digest(b"SPIDER_FINANCIAL_TRADE_V1\0" + canonical_bytes(identity))
            existing = session.execute(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.environment == environment,
                    SpiderRuntimeDelivery.execution_scope_id == execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == strategy_id,
                    SpiderRuntimeDelivery.delivery_id == delivery_id,
                )
            ).scalar_one_or_none()
            if existing is not None:
                if (
                    existing.trade_id != trade.id
                    or existing.action_id != bound.action_id
                    or existing.local_order_id != order.id
                    or not _saved_trade_matches(session, trade)
                ):
                    raise RuntimeError("spider_financial_trade_identity_conflict")
                if (
                    reason is not None
                    or not _same_projection(before, after)
                    or order.intent_payload != before_values["intent_payload"]
                ):
                    raise RuntimeError("spider_financial_trade_replay_order_conflict")
                if trade in session.new:
                    session.expunge(trade)
                return False
            # The caller has already attached this original Trade under
            # no_autoflush. Flush it only after the runtime and Order locks and
            # scalar prewrite proof, so the receipt's FK is satisfied without
            # taking a Trade->Order key lock ahead of the approved lock order.
            session.flush([trade])

        payload = {
            "schema_version": "SPIDER_FINANCIAL_RECEIPT_V1",
            "scope": _scope(bound),
            "kind": kind,
            "binding": binding_value,
            "trade_id": trade_id,
            "transition_ordinal": transition_ordinal,
            "before": before,
            "after": after,
            "binding_resolution": "UNRESOLVED" if reason else "BOUND",
            "reason_code": reason,
        }
        payload_bytes = canonical_bytes(payload)
        sequence = scope_row.last_received_sequence + 1
        if type(sequence) is not int or sequence < 1 or sequence > _MAX_SEQUENCE:
            raise RuntimeError("spider_financial_sequence_exhausted")
        scope_row.last_received_sequence = sequence
        delivery = SpiderRuntimeDelivery(
            environment=environment,
            execution_scope_id=execution_scope_id,
            strategy_id=strategy_id,
            sequence=sequence,
            delivery_id=delivery_id,
            content_sha256=_digest(payload_bytes),
            kind=kind,
            local_order_id=order.id,
            trade_id=trade_id,
            action_id=bound.action_id,
            generation=action.generation,
            configuration_sha256=action.configuration_sha256,
            role="GRID",
            source_payload=payload_bytes,
            visible_at=session.execute(
                select(func.date_trunc("milliseconds", func.clock_timestamp()))
            ).scalar_one(),
            elapsed_ms=None,
            disposition="PENDING",
            suppression_reason=None,
        )
        session.add(delivery)
        tape_sequence = scope_row.last_tape_sequence + 1
        if type(tape_sequence) is not int or tape_sequence < 1 or tape_sequence > _MAX_SEQUENCE:
            raise RuntimeError("spider_financial_tape_sequence_exhausted")
        scope_row.last_tape_sequence = tape_sequence
        tape = SpiderRuntimeTapeIndex(
            environment=environment,
            execution_scope_id=execution_scope_id,
            strategy_id=strategy_id,
            ordinal=tape_sequence,
            kind="DELIVERY",
            delivery_sequence=sequence,
            transmission_group_id=None,
        )
        session.add(tape)
        # Make only the just-staged management proof visible to the same-TX
        # projection queries; the repository still owns the Order write/flush.
        session.flush([scope_row, delivery, tape])
        return True


def validate_tape_index(session: Session, runtime: SpiderRuntimeScope) -> bool:
    """Prove the shared tape is contiguous and references every saved item once."""
    if (
        type(runtime.last_received_sequence) is not int
        or type(runtime.last_tape_sequence) is not int
        or runtime.last_received_sequence < 0
        or runtime.last_tape_sequence < runtime.last_received_sequence
        or runtime.last_tape_sequence > _MAX_SEQUENCE
    ):
        return False
    scope_filters = (
        SpiderRuntimeDelivery.environment == runtime.environment,
        SpiderRuntimeDelivery.execution_scope_id == runtime.execution_scope_id,
        SpiderRuntimeDelivery.strategy_id == runtime.strategy_id,
    )
    deliveries = session.execute(
        select(SpiderRuntimeDelivery.sequence).where(*scope_filters).order_by(
            SpiderRuntimeDelivery.sequence
        )
    ).scalars().all()
    if deliveries != list(range(1, runtime.last_received_sequence + 1)):
        return False
    transmission_table = SpiderRuntimeTransmission.__table__
    transmission_rows = session.execute(
        select(transmission_table.c.transmission_group_id,
               transmission_table.c.request_payload,
               transmission_table.c.request_sha256)
        .where(
            transmission_table.c.environment == runtime.environment,
            transmission_table.c.execution_scope_id == runtime.execution_scope_id,
            transmission_table.c.strategy_id == runtime.strategy_id,
        )
    ).mappings().all()
    transmission_ids: set[str] = set()
    for row in transmission_rows:
        group_id = row["transmission_group_id"]
        payload = row["request_payload"]
        if (
            type(group_id) is not str
            or type(payload) is not bytes
            or _digest(payload) != row["request_sha256"]
            or group_id in transmission_ids
        ):
            return False
        transmission_ids.add(group_id)
    tape_table = SpiderRuntimeTapeIndex.__table__
    tape_rows = session.execute(
        select(tape_table).where(
            tape_table.c.environment == runtime.environment,
            tape_table.c.execution_scope_id == runtime.execution_scope_id,
            tape_table.c.strategy_id == runtime.strategy_id,
        ).order_by(tape_table.c.ordinal)
    ).mappings().all()
    if len(tape_rows) != runtime.last_tape_sequence:
        return False
    seen_deliveries: set[int] = set()
    seen_transmissions: set[str] = set()
    next_delivery_sequence = 1
    for ordinal, row in enumerate(tape_rows, 1):
        if row["ordinal"] != ordinal:
            return False
        if row["kind"] == "DELIVERY":
            sequence = row["delivery_sequence"]
            if (
                type(sequence) is not int
                or row["transmission_group_id"] is not None
                or not 1 <= sequence <= runtime.last_received_sequence
                or sequence in seen_deliveries
                or sequence != next_delivery_sequence
            ):
                return False
            seen_deliveries.add(sequence)
            next_delivery_sequence += 1
        elif row["kind"] == "SEND_MARKER":
            group_id = row["transmission_group_id"]
            if (
                row["delivery_sequence"] is not None
                or type(group_id) is not str
                or group_id not in transmission_ids
                or group_id in seen_transmissions
            ):
                return False
            seen_transmissions.add(group_id)
        else:
            return False
    return (
        seen_deliveries == set(range(1, runtime.last_received_sequence + 1))
        and seen_transmissions == transmission_ids
    )


def _projection_matches_install(
    order: Order,
    binding: SpiderRuntimeBinding,
    action: SpiderRuntimeAction,
    before_row: Mapping[str, object],
    selection: Mapping[str, object],
) -> bool:
    intent = order.intent_payload
    original = decode_canonical(action.original_payload)
    if type(intent) is not dict or type(original) is not dict:
        return False
    payload = cast(dict[str, object], original)
    return (
        order.id == binding.local_order_id
        and action.local_order_id == binding.local_order_id
        and order.strategy_id == binding.strategy_id
        and order.product_id == binding.canonical_product_id
        and order.account_profile == selection.get("account_profile")
        and order.account_id == selection.get("account_id")
        and order.type == "limit"
        and order.exchange_id == binding.canonical_product_id.split(":", 1)[0]
        and order.side == payload.get("side")
        and order.price == Decimal(cast(str, payload.get("px")))
        and order.trigger_price is None
        and order.quantity == Decimal(cast(str, payload.get("sz")))
        and order.client_order_id == intent.get("local_client_order_id")
        and intent.get("source_client_order_id") == binding.source_client_order_id
        and intent.get("action_id") == action.action_id
        and before_row["strategy_id"] == binding.strategy_id
        and before_row["product_id"] == binding.canonical_product_id
        and before_row["exchange_id"] == binding.canonical_product_id.split(":", 1)[0]
        and before_row["account_profile"] == selection.get("account_profile")
        and before_row["account_id"] == selection.get("account_id")
        and before_row["type"] == "limit"
        and before_row["side"] == payload.get("side")
        and before_row["price"] == Decimal(cast(str, payload.get("px")))
        and before_row["trigger_price"] is None
        and before_row["quantity"] == Decimal(cast(str, payload.get("sz")))
        and before_row["client_order_id"] == intent.get("local_client_order_id")
        and order.product_id == binding.canonical_product_id
        and action.product_id == binding.source_product_id
        and payload.get("clOrdId") == binding.source_client_order_id
        and before_row["id"] == binding.local_order_id
    )


def prove_spider_order_wire_binding(
    environment: str,
    execution_scope_id: str,
    strategy_id: str,
    action: SpiderRuntimeAction,
    binding: SpiderRuntimeBinding,
    order: Order,
) -> tuple[str, str | None, str] | None:
    """Verify the saved canonical provider wire binding without lifecycle gates.

    This is intentionally narrower than INSTALL replay and transmission
    validation: local-fact readers may verify identity but cannot authorize a
    send, restore runtime policy, or infer provider acceptance.
    """
    intent = order.intent_payload
    if type(intent) is not dict:
        return None
    wire_hex = intent.get("provider_binding_bytes_hex")
    if type(wire_hex) is not str or len(wire_hex) % 2:
        return None
    try:
        raw = bytes.fromhex(wire_hex)
        saved = decode_canonical(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    expected = {
        "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
        "environment": environment,
        "execution_scope_id": execution_scope_id,
        "strategy_id": strategy_id,
        "action_id": action.action_id,
        "product_id": binding.canonical_product_id,
    }
    wire_id = "sp" + sha256(b"SPIDER_OKX_CLIENT_BINDING_V1\0" + raw).hexdigest()[:30]
    local_client_id = intent.get("local_client_order_id")
    source_client_id = intent.get("source_client_order_id")
    if type(source_client_id) is not str or (
        local_client_id is not None and type(local_client_id) is not str
    ):
        return None
    if (
        saved != expected
        or intent.get("action_id") != action.action_id
        or intent.get("provider_binding_sha256") != _digest(raw)
        or intent.get("provider_wire_client_order_id") != wire_id
        or source_client_id != binding.source_client_order_id
        or local_client_id != order.client_order_id
        or binding.action_id != action.action_id
        or binding.local_order_id != order.id
        or order.strategy_id != strategy_id
        or order.product_id != binding.canonical_product_id
        or binding.source_product_id != action.product_id
    ):
        return None
    return source_client_id, local_client_id, wire_id


def _next_transition_ordinal(
    session: Session,
    binding: SpiderRuntimeBinding,
    last_received_sequence: int,
) -> int:
    rows = session.execute(
        select(SpiderRuntimeDelivery.source_payload).where(
            SpiderRuntimeDelivery.environment == binding.environment,
            SpiderRuntimeDelivery.execution_scope_id == binding.execution_scope_id,
            SpiderRuntimeDelivery.strategy_id == binding.strategy_id,
            SpiderRuntimeDelivery.local_order_id == binding.local_order_id,
            SpiderRuntimeDelivery.kind == "ORDER_STATE",
            SpiderRuntimeDelivery.sequence <= last_received_sequence,
        ).order_by(SpiderRuntimeDelivery.sequence.desc())
    ).scalars()
    latest = next(iter(rows), None)
    if latest is None:
        return 1
    payload = decode_canonical(bytes(latest))
    if type(payload) is not dict or type(payload.get("transition_ordinal")) is not int:
        raise RuntimeError("spider_financial_transition_history_invalid")
    ordinal = cast(int, payload["transition_ordinal"]) + 1
    if ordinal > _MAX_SEQUENCE:
        raise RuntimeError("spider_financial_transition_exhausted")
    return ordinal


def _saved_trade_matches(session: Session, trade: Trade) -> bool:
    row = session.execute(
        select(
            Trade.order_id,
            Trade.exchange_trade_id,
            Trade.product_id,
            Trade.side,
            Trade.price,
            Trade.quantity,
            Trade.fee,
            Trade.fee_asset,
            Trade.timestamp,
        ).where(Trade.id == trade.id)
    ).mappings().one_or_none()
    if row is None:
        return False
    return all(
        row[key] == getattr(trade, key)
        for key in (
            "order_id", "exchange_trade_id", "product_id", "side", "price",
            "quantity", "fee", "fee_asset", "timestamp",
        )
    )


def validate_order_receipt_chain(
    session: Session,
    *,
    scope: Mapping[str, str],
    action: SpiderRuntimeAction,
    binding: SpiderRuntimeBinding,
    order_id: str,
    current_projection: Mapping[str, object],
    through_sequence: int | None = None,
) -> bool:
    """Verify receipt bytes, identity, Trade links, and the complete Order chain."""
    scope_value = dict(scope)
    binding_value = _binding_value(binding, action)
    immutable_order = session.execute(
        select(Order.product_id, Order.side, Order.quantity).where(Order.id == order_id)
    ).one_or_none()
    if immutable_order is None or action.operation != "PLACE":
        return False
    source_action = decode_canonical(bytes(action.original_payload))
    if type(source_action) is not dict:
        return False
    source_terms = cast(dict[str, object], source_action)
    try:
        original_quantity = Decimal(cast(str, source_terms["sz"]))
        if (
            not original_quantity.is_finite()
            or original_quantity <= 0
            or immutable_order.quantity != original_quantity
            or immutable_order.product_id != binding.canonical_product_id
            or immutable_order.side != source_terms["side"]
        ):
            return False
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return False
    cursor: dict[str, object] = {
        "status": "NEW",
        "filled_quantity": "0",
        "filled_price": "0",
        "exchange_order_id": None,
    }
    ordinal = 0
    previous_sequence = 0
    proved_fill_quantity = Decimal(0)
    delivery_filters = [
            SpiderRuntimeDelivery.environment == scope["environment"],
            SpiderRuntimeDelivery.execution_scope_id == scope["execution_scope_id"],
            SpiderRuntimeDelivery.strategy_id == scope["strategy_id"],
            SpiderRuntimeDelivery.local_order_id == order_id,
    ]
    if through_sequence is not None:
        if type(through_sequence) is not int or through_sequence <= 0:
            return False
        delivery_filters.append(SpiderRuntimeDelivery.sequence <= through_sequence)
    rows = session.execute(
        select(SpiderRuntimeDelivery).where(*delivery_filters).order_by(
            SpiderRuntimeDelivery.sequence
        )
    ).scalars()
    for row in rows:
        if row.sequence <= previous_sequence:
            return False
        previous_sequence = row.sequence
        raw = bytes(row.source_payload)
        if _digest(raw) != row.content_sha256:
            return False
        payload = decode_canonical(raw)
        if type(payload) is not dict or set(payload) != _PAYLOAD_KEYS:
            return False
        record = cast(dict[str, object], payload)
        if (
            record["schema_version"] != "SPIDER_FINANCIAL_RECEIPT_V1"
            or record["scope"] != scope_value
            or record["binding"] != binding_value
            or record["binding_resolution"] != "BOUND"
            or record["reason_code"] is not None
            or row.action_id != action.action_id
            or row.generation != action.generation
            or row.configuration_sha256 != action.configuration_sha256
            or row.role != "GRID"
            or not _same_projection(cast(dict[str, object], record["before"]), cursor)
        ):
            return False
        after = record["after"]
        if type(after) is not dict or set(cast(dict[str, object], after)) != _PROJECTION_KEYS:
            return False
        if row.kind == "ORDER_STATE":
            if (
                record["kind"] != "ORDER_STATE"
                or record["trade_id"] is not None
                or row.trade_id is not None
                or type(record["transition_ordinal"]) is not int
                or record["transition_ordinal"] != ordinal + 1
            ):
                return False
            identity = {
                "scope": scope_value,
                "local_order_id": order_id,
                "transition_ordinal": record["transition_ordinal"],
            }
            ordinal += 1
            domain = b"SPIDER_ORDER_STATE_V1\0"
            before = cast(dict[str, object], record["before"])
            after_values = cast(dict[str, object], after)
            try:
                before_quantity = Decimal(cast(str, before["filled_quantity"]))
                after_quantity = Decimal(cast(str, after_values["filled_quantity"]))
            except (InvalidOperation, TypeError, ValueError):
                return False
            if (
                after_quantity < before_quantity
                or after_quantity > original_quantity
                or (after_quantity > before_quantity and proved_fill_quantity != after_quantity)
            ):
                return False
        elif row.kind == "FILL":
            if (
                record["kind"] != "FILL"
                or record["trade_id"] != row.trade_id
                or row.trade_id is None
                or record["transition_ordinal"] is not None
                or not _trade_exists_for_order(session, row.trade_id, order_id)
            ):
                return False
            before = cast(dict[str, object], record["before"])
            after_values = cast(dict[str, object], after)
            try:
                before_quantity = Decimal(cast(str, before["filled_quantity"]))
                after_quantity = Decimal(cast(str, after_values["filled_quantity"]))
            except (InvalidOperation, TypeError, ValueError):
                return False
            trade_values = session.execute(
                select(Trade.order_id, Trade.product_id, Trade.side, Trade.price, Trade.quantity)
                .where(Trade.id == row.trade_id)
            ).one_or_none()
            if trade_values is None:
                return False
            trade_order_id, trade_product, trade_side, trade_price, trade_quantity = trade_values
            delta = after_quantity - before_quantity
            if (
                trade_order_id != order_id
                or trade_product != immutable_order.product_id
                or trade_side != immutable_order.side
                or not isinstance(trade_price, Decimal)
                or not trade_price.is_finite()
                or trade_price <= 0
                or not isinstance(trade_quantity, Decimal)
                or not trade_quantity.is_finite()
                or trade_quantity <= 0
                or delta <= 0
                or trade_quantity != delta
            ):
                return False
            proved_fill_quantity += trade_quantity
            if proved_fill_quantity != after_quantity or after_quantity > original_quantity:
                return False
            identity = {"scope": scope_value, "trade_id": row.trade_id}
            domain = b"SPIDER_FINANCIAL_TRADE_V1\0"
        else:
            return False
        if row.delivery_id != _digest(domain + canonical_bytes(identity)):
            return False
        cursor = cast(dict[str, object], after)
    return _same_projection(cursor, current_projection)


def _trade_exists_for_order(session: Session, trade_id: str, order_id: str) -> bool:
    return session.execute(
        select(Trade.id).where(Trade.id == trade_id, Trade.order_id == order_id)
    ).scalar_one_or_none() == trade_id


__all__ = ["stage_financial_receipt", "validate_order_receipt_chain"]
