"""Private run-local proportional gross-response proposal and episode facts."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_HALF_EVEN, Decimal as D, localcontext
from copy import deepcopy
from typing import Any

from src.core.backtest.spider_gross_execution_input import (
    SpiderGrossExecutionInput,
    validate_spider_gross_execution_input,
)


POLICY_ID = "PROPORTIONAL_PAID_GROSS_TARGET_V1"
TRIGGER_CAP = D("2")
TARGET_CAP = D("1.5")
ALLOCATION = "GROSS_PROPORTIONAL_V1"
QUANTITY_ROUNDING = "CEIL_STEP_NO_MINIMUM_UPLIFT_V1"
_CHECKPOINTS = frozenset((
    "START_READINESS", "SOURCE_GROUP", "MARKET_STEP", "DELIVERY",
))


def _decimal(value: object) -> D:
    if type(value) is D:
        result = value
    elif type(value) is str:
        result = D(value)
    else:
        raise ValueError("INVALID_SCHEMA")
    if not result.is_finite():
        raise ValueError("INVALID_SCHEMA")
    return result


def _decimal_text(value: D) -> str:
    if not value.is_finite():
        raise ValueError("INVALID_SCHEMA")
    if value == 0:
        return "0"
    normalized = value.normalize()
    text = format(normalized, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def gross_response_contract_hash(request: SpiderGrossExecutionInput) -> str:
    return validate_spider_gross_execution_input(request)


def gross_obligations_clear(
    endpoint: Any, composition: Any, native_state: dict[str, Any],
) -> bool:
    """Single pure predicate for currently unresolved gross-response obligations."""
    if any(action["role"] in ("GROSS_CANCEL", "GROSS_REDUCTION")
           and action["classification"] == "PENDING" for action in endpoint.actions):
        return False
    if any(endpoint.delivery_roles.get(delivery_id) in ("GRID", "GROSS_REDUCTION", "GROSS_CANCEL")
           for delivery_id in endpoint.pending_delivery_ids):
        return False
    if any(
        delivery_id in endpoint.pending_delivery_ids
        and action.get("role") == "GROSS_CANCEL"
        for delivery_id, action in endpoint.cancel_ack_action_by_delivery.items()
    ):
        return False
    if any(
        endpoint.roles_by_order.get(order_id) == "GROSS_CANCEL"
        or any(
            action["role"] == "GROSS_CANCEL" and action["order_id"] == order_id
            and action["classification"] == "COMMITTED"
            for action in endpoint.actions
        )
        for order_id in composition._paid_cancel_pending_orders
    ):
        return False
    if any(dep["role"] in ("GRID", "GROSS_REDUCTION") and dep["state"] == "PENDING"
           for dep in composition._historical_send_dependencies.values()):
        return False
    orders = native_state.get("orders")
    if not isinstance(orders, list):
        raise ValueError("INVALID_SCHEMA")
    if any(isinstance(order, dict) and order.get("cancel_pending") is True
           for order in orders):
        return False
    return not any(
        isinstance(order, dict)
        and endpoint.roles_by_order.get(order.get("order_id"))
        in ("GROSS_CANCEL", "GROSS_REDUCTION")
        for order in orders
    )


@dataclass(frozen=True)
class GrossResponseProposal:
    state: str
    reason: str | None
    native_digest: str
    account_version: int
    valuation_context_id: str
    effective_equity: D
    gross: D
    estimated_fee_rate: D | None
    estimated_reduction_gross: D | None
    estimated_fee: D | None
    estimated_equity_after: D | None
    rows: tuple[dict[str, Any], ...]
    proposal_context: dict[str, Any] = field(default_factory=dict)

    def detached(self) -> dict[str, Any]:
        return dict(
            policy_id=POLICY_ID,
            parameters=dict(
                trigger_cap=_decimal_text(TRIGGER_CAP),
                target_cap=_decimal_text(TARGET_CAP),
                allocation=ALLOCATION,
                quantity_rounding=QUANTITY_ROUNDING,
            ),
            state=self.state,
            reason=self.reason,
            native_state_digest=self.native_digest,
            account_version=self.account_version,
            valuation_context_id=self.valuation_context_id,
            estimated_fee_rate=(
                _decimal_text(self.estimated_fee_rate)
                if self.estimated_fee_rate is not None else None
            ),
            estimated_reduction_gross=(
                _decimal_text(self.estimated_reduction_gross)
                if self.estimated_reduction_gross is not None else None
            ),
            estimated_fee=(_decimal_text(self.estimated_fee) if self.estimated_fee is not None else None),
            estimated_equity_after=(
                _decimal_text(self.estimated_equity_after)
                if self.estimated_equity_after is not None else None
            ),
            rows=[dict(row) for row in self.rows],
            proposal_context=deepcopy(self.proposal_context),
        )


def propose_reductions(
    native_state: dict[str, Any], *, strategy_id: str, ordered_products: tuple[str, ...],
    proposal_context: dict[str, Any] | None = None,
) -> GrossResponseProposal:
    """Build a complete current-mark proposal without changing native or host facts."""
    account_version = native_state.get("account_version")
    valuation_context_id = native_state.get("valuation_context_id")
    native_digest = native_state.get("owner_state_digest")
    if (type(account_version) is not int or type(valuation_context_id) is not str
            or type(native_digest) is not str):
        raise ValueError("INVALID_SCHEMA")
    gross_projection = native_state.get("gross_admission")
    products = native_state.get("products")
    if not isinstance(gross_projection, dict) or not isinstance(products, list):
        raise ValueError("INVALID_SCHEMA")
    equity = _decimal(gross_projection.get("effective_equity"))
    gross = _decimal(gross_projection.get("gross"))
    if (type(ordered_products) is not tuple or len(set(ordered_products)) != len(ordered_products)
            or set(ordered_products) != {row.get("product_id") for row in products if isinstance(row, dict)}):
        raise ValueError("INVALID_SCHEMA")
    empty: dict[str, Any] = dict(
        native_digest=native_digest, account_version=account_version,
        valuation_context_id=valuation_context_id, effective_equity=equity, gross=gross,
        proposal_context=deepcopy(proposal_context) if proposal_context is not None else {},
    )
    if equity <= 0 or gross <= 0:
        return GrossResponseProposal(
            state="INCOMPLETE", reason="NON_POSITIVE_GROSS_OR_EQUITY",
            estimated_fee_rate=None, estimated_reduction_gross=None,
            estimated_fee=None, estimated_equity_after=None, rows=(), **empty,
        )

    try:
        with localcontext() as context:
            context.prec = 60
            context.rounding = ROUND_HALF_EVEN
            weighted_fee_numerator = D(0)
            checked_products: list[dict[str, Any]] = []
            seen: set[str] = set()
            gross_sum = D(0)
            for product in products:
                if not isinstance(product, dict):
                    raise ValueError("INVALID_SCHEMA")
                product_id = product.get("product_id")
                if type(product_id) is not str or product_id in seen:
                    raise ValueError("INVALID_SCHEMA")
                seen.add(product_id)
                product_gross = _decimal(product.get("gross"))
                if product_gross < 0:
                    raise ValueError("INVALID_SCHEMA")
                gross_sum += product_gross
                if product_gross == 0:
                    continue
                fee_rate = _decimal(product.get("taker_fee_rate"))
                mark = _decimal(product.get("mark"))
                contract_value = _decimal(product.get("contract_value"))
                multiplier = _decimal(product.get("multiplier"))
                quantity_step = _decimal(product.get("quantity_step"))
                signed_contracts = _decimal(product.get("signed_contracts"))
                capacity = _decimal(product.get("closing_capacity"))
                lots = product.get("lots")
                if (fee_rate < 0 or mark <= 0 or contract_value <= 0 or multiplier <= 0
                        or quantity_step <= 0 or capacity < 0 or not isinstance(lots, list)):
                    raise ValueError("INVALID_SCHEMA")
                lot_quantity = D(0)
                authorized = True
                for lot in lots:
                    if not isinstance(lot, dict):
                        raise ValueError("INVALID_SCHEMA")
                    lot_owner = lot.get("strategy_id")
                    lot_contracts = _decimal(lot.get("contracts"))
                    if lot_contracts <= 0:
                        raise ValueError("INVALID_SCHEMA")
                    lot_quantity += lot_contracts
                    authorized = authorized and lot_owner == strategy_id
                position_quantity = abs(signed_contracts)
                if lot_quantity != position_quantity:
                    raise ValueError("INVALID_SCHEMA")
                weighted_fee_numerator += product_gross * fee_rate
                checked_products.append(dict(
                    product_id=product_id,
                    gross=product_gross,
                    fee_rate=fee_rate,
                    mark=mark,
                    contract_value=contract_value,
                    multiplier=multiplier,
                    quantity_step=quantity_step,
                    signed_contracts=signed_contracts,
                    capacity=capacity,
                    position_quantity=position_quantity,
                    authorized=authorized,
                ))
            if gross_sum != gross:
                raise ValueError("INVALID_SCHEMA")
            if tuple(product["product_id"] for product in products) != tuple(sorted(seen)):
                raise ValueError("INVALID_SCHEMA")
            product_rank = {product_id: index for index, product_id in enumerate(ordered_products)}
            checked_products.sort(key=lambda product: product_rank[product["product_id"]])
            if gross / equity <= TARGET_CAP:
                return GrossResponseProposal(
                    state="RECOVERED", reason=None, estimated_fee_rate=None,
                    estimated_reduction_gross=D(0), estimated_fee=D(0),
                    estimated_equity_after=equity, rows=(), **empty,
                )
            if any(not product["authorized"] for product in checked_products):
                return GrossResponseProposal(
                    state="INCOMPLETE", reason="AUTHORIZATION_SCOPE_INSUFFICIENT",
                    estimated_fee_rate=None, estimated_reduction_gross=None,
                    estimated_fee=None, estimated_equity_after=None, rows=(), **empty,
                )
            fee_rate = weighted_fee_numerator / gross
            denominator = D(1) - TARGET_CAP * fee_rate
            reduction_gross = (gross - TARGET_CAP * equity) / denominator
            if denominator <= 0 or reduction_gross <= 0 or reduction_gross > gross:
                return GrossResponseProposal(
                    state="INCOMPLETE", reason="ESTIMATE_UNAVAILABLE",
                    estimated_fee_rate=fee_rate, estimated_reduction_gross=None,
                    estimated_fee=None, estimated_equity_after=None, rows=(), **empty,
                )
            proposed_rows: list[dict[str, Any]] = []
            estimated_reduction = D(0)
            estimated_fee = D(0)
            for product in checked_products:
                product_gross = product["gross"]
                raw_quantity = (
                    product["position_quantity"] * reduction_gross / gross
                )
                if raw_quantity == 0:
                    continue
                step = product["quantity_step"]
                quantity = (raw_quantity / step).to_integral_value(rounding=ROUND_CEILING) * step
                rounding_excess = quantity - raw_quantity
                capacity = product["capacity"]
                reason = None
                if quantity > product["position_quantity"]:
                    reason = "ROUNDING_CAPACITY_UNREACHABLE"
                elif quantity > capacity:
                    reason = "WAIT_CAPACITY"
                mark_value = product["mark"] * product["contract_value"] * product["multiplier"]
                row_gross = quantity * mark_value
                row_fee = row_gross * product["fee_rate"]
                estimated_reduction += row_gross
                estimated_fee += row_fee
                proposed_rows.append(dict(
                    product_id=product["product_id"],
                    side="SHORT" if product["signed_contracts"] > 0 else "LONG",
                    gross=_decimal_text(product_gross),
                    raw_quantity=_decimal_text(raw_quantity),
                    quantity=_decimal_text(quantity),
                    rounding_excess=_decimal_text(rounding_excess),
                    capacity=_decimal_text(capacity),
                    reason=reason,
                ))
            rows_blocked = any(row["reason"] is not None for row in proposed_rows)
            if estimated_reduction > gross or equity - estimated_fee <= 0:
                return GrossResponseProposal(
                    state="INCOMPLETE", reason="ESTIMATE_UNAVAILABLE",
                    estimated_fee_rate=fee_rate, estimated_reduction_gross=None,
                    estimated_fee=None, estimated_equity_after=None, rows=(), **empty,
                )
            return GrossResponseProposal(
                state="WAIT_CAPACITY" if rows_blocked else "PROPOSED",
                reason=("WAIT_CAPACITY" if rows_blocked else None),
                estimated_fee_rate=fee_rate,
                estimated_reduction_gross=estimated_reduction,
                estimated_fee=estimated_fee,
                estimated_equity_after=equity - estimated_fee,
                rows=tuple(proposed_rows),
                **empty,
            )
    except (ArithmeticError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc) == "INVALID_SCHEMA":
            raise
        return GrossResponseProposal(
            state="INCOMPLETE", reason="ESTIMATE_UNAVAILABLE",
            estimated_fee_rate=None, estimated_reduction_gross=None,
            estimated_fee=None, estimated_equity_after=None, rows=(), **empty,
        )


@dataclass
class GrossResponseEpisode:
    contract_hash: str
    strategy_id: str
    ordered_products: tuple[str, ...]
    state: str = "IDLE"
    reason: str | None = None
    episode_sequence: int = 0
    observations: list[dict[str, Any]] = field(default_factory=list)
    rounds: list[dict[str, Any]] = field(default_factory=list)
    last_native_digest: str | None = None
    blocked_products: set[str] = field(default_factory=set)
    attempted_products: set[str] = field(default_factory=set)
    rejected_products: set[str] = field(default_factory=set)
    current_round_sequence: int | None = None
    current_round_action_ids: list[str] = field(default_factory=list)
    active: bool = False
    last_observed_identity: tuple[Any, ...] | None = None
    last_round_native_digest: str | None = None

    def checkpoint(self, endpoint: Any, composition: Any, cause: str) -> None:
        if cause not in _CHECKPOINTS:
            raise ValueError("INVALID_SCHEMA")
        native_state = composition._codec.gross_response_projection()
        if not isinstance(native_state, dict) or native_state.get("schema_version") != "spider_gross_response_native_state_v1":
            raise ValueError("INVALID_SCHEMA")
        identity = (
            cause,
            composition._current_time,
            composition._last_popped,
            native_state.get("account_version"),
            native_state.get("owner_state_digest"),
        )
        if identity == self.last_observed_identity:
            return
        self.last_observed_identity = identity
        before = self.state
        proposal_context = self._proposal_context(endpoint, composition)
        lifecycle = native_state.get("lifecycle")
        gate = native_state.get("gate")
        reason: str | None = None
        action_ids: list[str] = []
        proposal_evidence: dict[str, Any] | None = None
        admission = native_state.get("gross_admission")
        if not isinstance(admission, dict):
            raise ValueError("INVALID_SCHEMA")
        equity = _decimal(admission.get("effective_equity"))
        gross = _decimal(admission.get("gross"))
        ready = native_state.get("current_admission_state_ready") is True
        policy_end = endpoint.policy_end_ms * 16 + 6
        policy_closed = composition._current_time >= policy_end

        if lifecycle in ("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"):
            self.active = False
            self.state = lifecycle
            reason = "NATIVE_TERMINAL"
        elif isinstance(gate, dict) and gate.get("status") == "FAILED":
            self.active = False
            self.state = "FAILED"
            reason = "NATIVE_FAILURE"
        elif not self.active:
            if equity > 0 and gross > TRIGGER_CAP * equity:
                self.active = True
                self.episode_sequence += 1
                self.rejected_products.clear()
                self.current_round_sequence = None
                self.current_round_action_ids = []
                self.state = "RESTRICTED"
                self.blocked_products.update(self.ordered_products)
                reason = "GROSS_ABOVE_TRIGGER"
        if self.active and lifecycle not in ("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"):
            if equity <= 0:
                self.state, reason = "INCOMPLETE", "NON_POSITIVE_EQUITY"
            elif (gross <= TARGET_CAP * equity and ready
                  and self._obligations_clear(endpoint, composition, native_state)):
                self.active = False
                self.state = "RECOVERED"
                self.blocked_products.clear()
                reason = None
            elif (gross <= TARGET_CAP * equity and not ready
                  and self._obligations_clear(endpoint, composition, native_state)
                  and not self._unverified_orders_to_cancel(native_state)):
                self.state, reason = "RESTRICTED", "WAIT_ADMISSION_READINESS"
            elif policy_closed:
                # Once the ordinary paid drain begins, preserve the live episode
                # and its evidence but never create another response action.
                self.state = "RESTRICTED"
                reason = "WAIT_OBLIGATIONS" if not self._obligations_clear(
                    endpoint, composition, native_state,
                ) else "POLICY_END"
            elif self.rejected_products:
                # A rejected row remains part of this proportional episode. Do
                # not silently redistribute its share into a later round.
                self.state, reason = "INCOMPLETE", "ACTION_REJECTED"
            else:
                clear = self._obligations_clear(endpoint, composition, native_state)
                if clear:
                    self._cancel_unverified(endpoint, composition, native_state, action_ids)
                if action_ids:
                    self.state, reason = "RESTRICTED", "CANCEL_PENDING"
                elif clear:
                    proposal = propose_reductions(
                        native_state,
                        strategy_id=self.strategy_id,
                        ordered_products=self.ordered_products,
                        proposal_context=proposal_context,
                    )
                    proposal_evidence = proposal.detached()
                    reason = proposal.reason
                    if proposal.state == "INCOMPLETE":
                        self.state = "INCOMPLETE"
                    elif proposal.state == "WAIT_CAPACITY":
                        self.state = "RESTRICTED"
                    elif proposal.state == "RECOVERED":
                        self.state, reason = "RESTRICTED", "WAIT_ADMISSION_READINESS"
                    else:
                        if (self.last_round_native_digest is not None
                                and native_state.get("owner_state_digest") == self.last_round_native_digest):
                            self.state, reason = "RESTRICTED", "WAIT_OBLIGATIONS"
                        else:
                            self.attempted_products.clear()
                            rows = list(proposal.rows)
                            if not rows:
                                self.state, reason = "INCOMPLETE", "ACTION_REJECTED"
                            else:
                                admit = []
                                for row in rows:
                                    if row["reason"] is not None:
                                        self.state, reason = "RESTRICTED", row["reason"]
                                        admit = []
                                        break
                                    quantity = D(row["quantity"])
                                    if quantity <= 0:
                                        continue
                                    signed_quantity = (
                                        quantity if row["side"] == "SHORT" else -quantity
                                    )
                                    item = endpoint._exit_group(
                                        composition, row["product_id"], signed_quantity,
                                        role="GROSS_REDUCTION",
                                    )
                                    action = endpoint.action_by_group[item["group"]["group_id"]]
                                    action_ids.append(action["action_id"])
                                    self.attempted_products.add(row["product_id"])
                                    admit.append((item, None))
                                if admit:
                                    admitted = composition._admit_set(admit)
                                    if any(row["classification"] != "PENDING" for row in admitted):
                                        self.state, reason = "FAILED", "SCHEDULER_ADMISSION_FAILED"
                                    else:
                                        self.state, reason = "RESTRICTED", "REDUCTION_PENDING"
                                        self.rounds.append(dict(
                                            sequence=len(self.rounds) + 1,
                                            observed_at=composition._current_time,
                                            proposal=proposal.detached(),
                                            action_ids=list(action_ids),
                                        ))
                                        self.current_round_sequence = len(self.rounds)
                                        self.current_round_action_ids = list(action_ids)
                                        self.last_round_native_digest = native_state.get("owner_state_digest")
                else:
                    self.state, reason = "RESTRICTED", "WAIT_OBLIGATIONS"

        self.reason = reason
        self.observations.append(dict(
            cause=cause,
            observed_at=composition._current_time,
            scheduler_key=(
                dict(visible_at=composition._last_popped[0],
                     schedule_sequence=composition._last_popped[2],
                     stable_id=composition._last_popped[3])
                if composition._last_popped is not None else None
            ),
            native_state=native_state,
            before_state=before,
            after_state=self.state,
            reason=reason,
            action_ids=action_ids,
            proposal=proposal_evidence,
            proposal_context=proposal_context,
        ))

    @staticmethod
    def _proposal_context(endpoint: Any, composition: Any) -> dict[str, Any]:
        """Detach source/cache and all known operation facts at this scheduler cut."""
        cache_rows = [
            dict(
                key=list(record["key"]),
                snapshot=record["item"]["snapshot"].detached(),
                result=deepcopy(record["result"]),
            )
            for record in composition._records.values()
            if record["item"].get("kind") == "HISTORICAL_MARKET_CACHE"
            and record["key"] <= (composition._last_popped or record["key"])
            and record["result"].get("classification") != "PENDING"
        ]
        pending_deliveries = []
        for key, record in sorted(composition._records.items()):
            delivery = record["item"].get("delivery")
            if (record["item"].get("kind") != "DELIVERY"
                    or not isinstance(delivery, dict)
                    or delivery.get("delivery_id") not in endpoint.pending_delivery_ids):
                continue
            delivery_id = delivery["delivery_id"]
            ack_action = endpoint.cancel_ack_action_by_delivery.get(delivery_id)
            pending_deliveries.append(dict(
                key=list(record["key"]),
                delivery=deepcopy(delivery),
                result=deepcopy(record["result"]),
                role=endpoint.delivery_roles.get(delivery_id),
                cancel_action_id=ack_action.get("action_id") if ack_action is not None else None,
            ))
        cancel_maps = {
            name: {
                key: value.get("action_id") if isinstance(value, dict) else None
                for key, value in sorted(mapping.items())
            }
            for name, mapping in (
                ("requests", endpoint.cancel_action_by_request),
                ("effects", endpoint.cancel_action_by_effect),
                ("acks", endpoint.cancel_ack_action_by_delivery),
            )
        }
        episode = endpoint.gross_response
        gross_progress = None
        if episode is not None:
            gross_progress = dict(
                episode_sequence=episode.episode_sequence,
                state=episode.state,
                active=episode.active,
                rejected_products=[
                    product_id for product_id in episode.ordered_products
                    if product_id in episode.rejected_products
                ],
                current_round_sequence=episode.current_round_sequence,
                current_round_action_ids=list(episode.current_round_action_ids),
            )
        return deepcopy(dict(
            source_cache=dict(
                policy_rows=composition._policy.rows,
                policy_markets=composition._policy.markets,
                policy_last_earn_ms=composition._policy.last_earn_ms,
                cache_records=cache_rows,
            ),
            operations=dict(
                actions=endpoint.actions,
                product_operations=endpoint.product_operations,
                pending_delivery_ids=sorted(endpoint.pending_delivery_ids),
                pending_deliveries=pending_deliveries,
                delivery_roles=endpoint.delivery_roles,
                cancel_action_ids=cancel_maps,
                send_dependencies=composition._historical_send_dependencies,
                cancel_effects=composition._historical_cancel_effects,
                cancel_pending_order_ids=sorted(composition._paid_cancel_pending_orders),
                gross_response=gross_progress,
            ),
            scheduler_key=(
                dict(
                    visible_at=composition._last_popped[0],
                    queue_class=composition._last_popped[1],
                    schedule_sequence=composition._last_popped[2],
                    stable_id=composition._last_popped[3],
                ) if composition._last_popped is not None else None
            ),
        ))

    @staticmethod
    def _obligations_clear(
        endpoint: Any, composition: Any, native_state: dict[str, Any],
    ) -> bool:
        return gross_obligations_clear(endpoint, composition, native_state)

    @staticmethod
    def gross_drain_obligations_clear(endpoint: Any, composition: Any) -> bool:
        """Wait for this episode's real native reductions before whole-run EXIT."""
        gross_roles = ("GROSS_CANCEL", "GROSS_REDUCTION")
        if any(action["role"] in gross_roles and action["classification"] == "PENDING"
               for action in endpoint.actions):
            return False
        if any(dependency.get("role") == "GROSS_REDUCTION"
               and dependency.get("state") == "PENDING"
               for dependency in composition._historical_send_dependencies.values()):
            return False
        if any(endpoint.delivery_roles.get(delivery_id) in gross_roles
               for delivery_id in endpoint.pending_delivery_ids):
            return False
        gross_cancel_targets = {
            spec.get("target_order_id")
            for spec in composition._historical_cancel_effects.values()
            if spec.get("paid_action_role") == "GROSS_CANCEL"
        }
        if composition._paid_cancel_pending_orders.intersection(gross_cancel_targets):
            return False
        if any(endpoint.roles_by_order.get(order_id) in gross_roles
               for order_id in composition._paid_cancel_pending_orders):
            return False
        return not any(
            order.get("order_id") in endpoint.roles_by_order
            and endpoint.roles_by_order[order["order_id"]] in gross_roles
            for order in composition._codec.historical_working_orders()
        )

    def _cancel_unverified(
        self, endpoint: Any, composition: Any, native_state: dict[str, Any],
        action_ids: list[str],
    ) -> None:
        items = []
        for order in self._unverified_orders_to_cancel(native_state):
            action = endpoint._cancel_group(composition, order, role="GROSS_CANCEL")
            group_id = action["group"]["group_id"]
            action_row = endpoint.action_by_group[group_id]
            action_ids.append(action_row["action_id"])
            items.append((action, None))
        if items:
            queued = composition._admit_set(items)
            if any(row["classification"] != "PENDING" for row in queued):
                self.state, self.reason = "FAILED", "SCHEDULER_ADMISSION_FAILED"

    def _unverified_orders_to_cancel(self, native_state: dict[str, Any]) -> list[dict[str, Any]]:
        products = native_state.get("products")
        orders = native_state.get("orders")
        if not isinstance(products, list) or not isinstance(orders, list):
            raise ValueError("INVALID_SCHEMA")
        authorized = {
            row["product_id"] for row in products
            if isinstance(row, dict)
            and isinstance(row.get("lots"), list)
            and all(isinstance(lot, dict) and lot.get("strategy_id") == self.strategy_id
                    for lot in row["lots"])
        }
        product_rank = {product_id: index for index, product_id in enumerate(self.ordered_products)}
        return [
            order for order in sorted(
                orders,
                key=lambda row: (
                    product_rank.get(row.get("product_id"), len(product_rank)),
                    row.get("order_id", ""),
                ),
            )
            if isinstance(order, dict)
            and order.get("strategy_id") == self.strategy_id
            and order.get("product_id") in authorized
            and order.get("verified_reducing") is not True
            and order.get("cancel_pending") is not True
        ]

    @classmethod
    def admit(cls, request: SpiderGrossExecutionInput) -> "GrossResponseEpisode":
        contract_hash = gross_response_contract_hash(request)
        scope = request.authorization_scope
        return cls(contract_hash, scope.strategy_id, scope.ordered_products)

    def observation(
        self, *, settlement_complete: bool = False, terminal_failure: bool = False,
    ) -> dict[str, Any]:
        reported_state = _observation_state(
            self.state, self.active,
            settlement_complete=settlement_complete,
            terminal_failure=terminal_failure,
        )
        return dict(
            schema_version="spider_gross_response_observation_v1",
            run_contract_sha256=self.contract_hash,
            policy_id=POLICY_ID,
            observations=[dict(row) for row in self.observations],
            episodes=[dict(row) for row in self.rounds],
            current_state=reported_state,
            reason=self.reason,
        )


def _observation_state(
    state: str, active: bool, *, settlement_complete: bool = False,
    terminal_failure: bool = False,
) -> str:
    """Map captured gross progress to its existing readonly endpoint state."""
    reported_state = state
    if terminal_failure:
        reported_state = "FAILED"
    elif settlement_complete and active and reported_state not in (
        "FAILED", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "RECOVERED",
    ):
        reported_state = "INCOMPLETE"
    return reported_state
