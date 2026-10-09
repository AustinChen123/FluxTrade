"""Run-local paid-endpoint action identity and exact result projection."""

from __future__ import annotations

from dataclasses import dataclass, field
from copy import deepcopy
from hashlib import sha256
from typing import Any
from collections.abc import Mapping, Sequence

from decimal import Decimal as D

from src.core.backtest.spider_execution_input import (
    SpiderExecutionInput,
    validate_spider_execution_input,
)
from src.core.backtest.spider_run_artifacts import _decimal_text, canonical_bytes
from src.core.backtest import spider_policy_protocol as policy_protocol


_REASONS = (
    "PENDING_SEND", "CANCEL_PENDING", "EXIT_PENDING", "RESIDUAL_POSITION",
    "EXIT_REJECTED", "NOTIFICATION_PENDING", "POLL_PENDING", "NATIVE_TERMINAL",
    "EXECUTION_FAILURE",
)


def _financial_decimal(value: object) -> D:
    if type(value) is D:
        result = value
    elif type(value) is str:
        result = D(_decimal_text(value))
    else:
        raise ValueError("INVALID_ARTIFACT")
    if not result.is_finite():
        raise ValueError("INVALID_ARTIFACT")
    return result


def _classify_paid_endpoint_result(
    request_identity: Mapping[str, Any],
    owner_evidence: dict[str, Any] | None,
    actions: Sequence[Mapping[str, Any]],
    scheduler_observation: Mapping[str, Any],
    paid_observation: Mapping[str, Any],
    failure: dict[str, Any] | None,
) -> dict[str, Any]:
    """Purely project admitted run facts into the frozen paid result DTO."""
    current_evidence_complete = paid_observation["current_evidence_complete"] is True
    positions = deepcopy(_snapshot_rows(owner_evidence, "positions")) if current_evidence_complete else None
    working_orders = deepcopy(_snapshot_rows(owner_evidence, "open_orders")) if current_evidence_complete else None
    pending_deliveries = [deepcopy(row["key"]) for row in paid_observation["pending_paid_deliveries"]]
    polls = [
        dict(poll_id=row["poll_id"], continuation_id=row["continuation_id"],
             status=row["status"], awaiting=row["awaiting"])
        for row in scheduler_observation["polls"]
        if row["status"] == "IN_PROGRESS" and row["awaiting"] is not None
    ]
    detached_actions = [deepcopy(dict(action)) for action in actions]
    pending_actions = [row["action_id"] for row in detached_actions
                       if row["classification"] == "PENDING"]
    pending_sends = [row["group_id"] for row in paid_observation["send_dependencies"]
                     if row["state"] == "PENDING"]
    pending_cancels = set(paid_observation["pending_cancel_order_ids"])
    roles_by_order = {row["order_id"]: row["role"] for row in paid_observation["order_roles"]}
    working_exit_orders: set[str] = set()
    for row in working_orders or []:
        if isinstance(row, dict):
            order_id = row.get("order_id")
            if isinstance(order_id, str) and roles_by_order.get(order_id) == "EXIT":
                working_exit_orders.add(order_id)
    pending_actions.extend(
        row["action_id"] for row in detached_actions
        if row["role"] == "EXIT" and row["order_id"] in working_exit_orders
        and row["action_id"] not in pending_actions
    )
    reasons = _reason_codes(
        actions=detached_actions,
        pending_deliveries=pending_deliveries,
        pending_polls=polls,
        positions=positions,
        orders=working_orders,
        scheduler_terminal=scheduler_observation["gate"] == "FAILED",
        execution_failure=failure is not None,
        pending_sends=pending_sends,
        pending_cancels=pending_cancels,
        native_terminal=paid_observation["native_terminal"],
        working_exit_orders=working_exit_orders,
    )
    inspection_state = (owner_evidence or {}).get("inspection")
    liquidated = ((inspection_state or {}).get("lifecycle") in
                  ("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"))
    if failure is not None:
        status = "FAILED"
    elif liquidated:
        status = (inspection_state or {})["lifecycle"]
    elif _paid_flat(positions, working_orders, pending_actions, pending_deliveries, polls,
                    pending_cancels=pending_cancels, pending_sends=pending_sends):
        status = "PAID_FLAT"
    else:
        status = "INCOMPLETE"
    if status == "PAID_FLAT":
        reasons = []
    return dict(
        schema_version="spider_paid_endpoint_result_v1",
        run_contract_sha256=request_identity["run_contract_sha256"],
        policy_end_ms=request_identity["policy_end_ms"],
        settlement_end_ms=request_identity["settlement_end_ms"],
        generation=request_identity["generation"],
        status=status,
        owner_evidence=deepcopy(owner_evidence),
        actions=detached_actions,
        pending_obligations=dict(
            positions=positions,
            working_orders=working_orders,
            pending_actions=pending_actions,
            pending_deliveries=sorted(pending_deliveries, key=_key_sort),
            pending_polls=sorted(polls, key=lambda row: (row["poll_id"], row["continuation_id"])),
        ),
        reasons=reasons,
        failure=deepcopy(failure) if status == "FAILED" else None,
    )


@dataclass
class PaidEndpoint:
    """Private, nondurable operation facts; all financial state remains native."""

    request: SpiderExecutionInput
    contract_hash: str
    actions: list[dict[str, Any]] = field(default_factory=list)
    action_by_group: dict[str, dict[str, Any]] = field(default_factory=dict)
    roles_by_order: dict[str, str] = field(default_factory=dict)
    pending_delivery_ids: set[str] = field(default_factory=set)
    delivery_roles: dict[str, str] = field(default_factory=dict)
    cancel_action_by_request: dict[str, dict[str, Any]] = field(default_factory=dict)
    cancel_action_by_effect: dict[str, dict[str, Any]] = field(default_factory=dict)
    cancel_ack_action_by_delivery: dict[str, dict[str, Any]] = field(default_factory=dict)
    rejected_exit_products: set[str] = field(default_factory=set)
    drain_started: bool = False
    exit_products: set[str] = field(default_factory=set)
    last_owner_evidence: dict[str, Any] | None = None
    last_cutoff: int | None = None

    @classmethod
    def admit(cls, request: SpiderExecutionInput) -> "PaidEndpoint":
        return cls(request, validate_spider_execution_input(request))

    @property
    def policy_end_ms(self) -> int:
        return self.request.policy_end_ms

    @property
    def settlement_end_ms(self) -> int:
        return self.request.historical_input.range_end_ms

    @property
    def generation(self) -> int:
        return self.request.generation

    def source_enabled(self, visible_at: int) -> bool:
        return type(visible_at) is int and visible_at < self.policy_end_ms * 16 + 6

    def new_action(
        self, *, role: str, product_id: str, effective_at: int,
        parent_action_id: str | None = None, order_id: str | None = None,
    ) -> dict[str, Any]:
        if role not in ("DRAIN_CANCEL", "EXIT"):
            raise ValueError("INVALID_SCHEMA")
        sequence = len(self.actions)
        identity = sha256(canonical_bytes({
            "run_contract_sha256": self.contract_hash,
            "action_sequence": sequence,
            "role": role,
        })).hexdigest()
        action = dict(
            action_id="PAID_ACTION_" + identity,
            parent_action_id=parent_action_id,
            role=role,
            product_id=product_id,
            order_id=order_id,
            group_id=None,
            effective_at=effective_at,
            classification="PENDING",
            result=None,
        )
        self.actions.append(action)
        return action

    def bind_group(self, action: dict[str, Any], group_id: str) -> None:
        if action["group_id"] is not None or group_id in self.action_by_group:
            raise ValueError("INVALID_SCHEMA")
        action["group_id"] = group_id
        self.action_by_group[group_id] = action

    def start_drain(self, composition: Any) -> None:
        if self.drain_started:
            raise ValueError("INVALID_SCHEMA")
        self.drain_started = True
        if composition._paid_native_terminal is not None:
            return
        if self.has_unbound_cancel():
            return
        # A send accepted after the cutoff remains a native obligation. Wait for
        # its original effective event and binding rather than dropping it.
        if any(dep["state"] == "PENDING"
               for dep in composition._historical_send_dependencies.values()):
            return
        orders = composition._codec.historical_working_orders()
        items = [(self._cancel_group(composition, order), None) for order in orders
                 if self._grid_order(composition, order)
                 and order["order_id"] not in composition._paid_cancel_attempted_orders]
        if items:
            composition._admit_set(items)

    def progress(self, composition: Any, cause: str) -> None:
        if (not self.drain_started or composition._paid_native_terminal is not None
                or cause not in ("SOURCE_GROUP", "DELIVERY", "HISTORICAL_MARKET_STEP", "PAID_ENDPOINT_DRAIN")):
            return
        if self.has_unbound_cancel():
            return
        if any(dep["state"] == "PENDING"
               for dep in composition._historical_send_dependencies.values()):
            return
        orders = composition._codec.historical_working_orders()
        if not composition._paid_cancel_pending_orders and not self.pending_delivery_ids:
            cancel_items = []
            for order in orders:
                if order["order_id"] not in self.roles_by_order:
                    raise ValueError("DEPENDENCY_BINDING_CONFLICT")
                if (self._grid_order(composition, order)
                        and order["order_id"] not in composition._paid_cancel_attempted_orders):
                    cancel_items.append((self._cancel_group(composition, order), None))
            if cancel_items:
                composition._admit_set(cancel_items)
                return
        if composition._paid_cancel_pending_orders or self.pending_delivery_ids:
            return
        request = composition._paid_snapshot_request(
            "POSITIONS", composition._current_time,
            f"PAID_PROGRESS_{self.contract_hash[:16]}_{composition._last_popped[2]}_{composition._last_popped[3]}",
        )
        fact = composition._codec.capture_snapshot(request)
        payload = fact["immutable_payload"]
        if not isinstance(payload, dict) or payload.get("outcome") != "SUCCESS":
            raise ValueError("POSITION_SNAPSHOT_FAILED")
        rows = payload.get("rows")
        if not isinstance(rows, list):
            raise ValueError("INVALID_SCHEMA")
        exits_working = {order["product_id"] for order in orders
                         if self.roles_by_order.get(order["order_id"]) == "EXIT"}
        exit_items = []
        for position in rows:
            if not isinstance(position, dict):
                raise ValueError("INVALID_SCHEMA")
            quantity, product_id = position["position_contracts"], position["product_id"]
            if type(quantity) is not D or not quantity.is_finite():
                raise ValueError("INVALID_SCHEMA")
            if quantity == 0 or product_id in exits_working or product_id in self.rejected_exit_products:
                continue
            exit_items.append((self._exit_group(composition, product_id, quantity), None))
        if exit_items:
            composition._admit_set(exit_items)

    def has_unbound_cancel(self) -> bool:
        return any(action["role"] == "DRAIN_CANCEL" and action["order_id"] is None
                   and action["classification"] == "PENDING" for action in self.actions)

    def _grid_order(self, composition: Any, order: Any) -> bool:
        role = self.roles_by_order.get(order["order_id"])
        if role not in ("GRID", "EXIT"):
            raise ValueError("DEPENDENCY_BINDING_CONFLICT")
        return role == "GRID"

    def _cancel_group(self, composition: Any, order: Any) -> dict[str, Any]:
        contract = composition._historical_order_contract
        if contract is None or order["order_id"] not in self.roles_by_order:
            raise ValueError("DEPENDENCY_BINDING_CONFLICT")
        order_id = order["order_id"]
        parent_raw = composition._current_time // 16
        acceptance_raw = parent_raw + contract["order_accept_delay_ms"]
        sequence = composition._historical_order_sequence
        parent_id = f"PAID_DRAIN_{self.contract_hash[:16]}_{sequence}"
        request_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "CANCEL_REQUEST_EVENT", 0)
        group_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "CANCEL_REQUEST_GROUP", 0)
        action = self.new_action(role="DRAIN_CANCEL", product_id=order["product_id"],
                                 order_id=order_id, effective_at=acceptance_raw * 16 + 3)
        self.bind_group(action, group_id)
        self.cancel_action_by_request[group_id] = action
        composition._paid_cancel_attempted_orders.add(order_id)
        composition._paid_cancel_pending_orders.add(order_id)
        member = dict(kind="CANCEL_REQUEST", stamp=dict(
            event_id=request_id, effective_at=acceptance_raw * 16 + 3,
            source_sequence=sequence, causal_parent_ids=[],
            ordering_contract_id="HISTORICAL_ORDER_V1", scenario_ordinal=40,
        ), payload=dict(targets=[dict(target_order_id=order_id, reason="EXPLICIT_SCENARIO")]))
        group = dict(schema_version="scenario_group_v1", group_id=group_id,
                     account_key=deepcopy(composition._account), ordering_contract_id="HISTORICAL_ORDER_V1",
                     group_effective_at=acceptance_raw * 16 + 3, declared_member_count=1, members=[member])
        policy_protocol._plan_group(group, composition._configured_product_ids)
        effect_event_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "CANCEL_EFFECT_EVENT", 0)
        effect_group_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "CANCEL_EFFECT_GROUP", 0)
        composition._historical_cancel_effects[group_id] = dict(
            parent_raw=parent_raw, request_event_id=request_id,
            effect_event_id=effect_event_id, effect_group_id=effect_group_id,
            target_order_id=order_id, product_id=order["product_id"],
            strategy_id=contract["strategy_identity"], reason="EXPLICIT_SCENARIO",
            paid_action_id=action["action_id"],
        )
        composition._historical_order_sequence += 1
        return dict(kind="SOURCE_GROUP", schedule_sequence=sequence, group=group)

    def _exit_group(self, composition: Any, product_id: str, quantity: D) -> dict[str, Any]:
        contract = composition._historical_order_contract
        if contract is None or quantity == 0:
            raise ValueError("INVALID_SCHEMA")
        parent_raw = composition._current_time // 16
        accepted_raw = parent_raw + contract["order_accept_delay_ms"]
        sequence = composition._historical_order_sequence
        parent_id = f"PAID_EXIT_{self.contract_hash[:16]}_{sequence}"
        event_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "ORDER_EVENT", 0)
        group_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "ORDER_GROUP", 0)
        client_id = f"PAID-EXIT-{self.contract_hash[:16]}-{sequence}"
        intent_id = policy_protocol._historical_child_id(self.contract_hash, parent_id, "ORDER_INTENT", 0)
        action = self.new_action(role="EXIT", product_id=product_id,
                                 effective_at=accepted_raw * 16 + 3)
        self.bind_group(action, group_id)
        composition._paid_role_by_client[client_id] = "EXIT"
        dependency = dict(client_order_id=client_id, product_id=product_id,
                          strategy_id=contract["strategy_identity"], group_id=group_id,
                          event_id=event_id, state="PENDING", binding=None, role="EXIT")
        if client_id in composition._historical_send_by_client:
            raise ValueError("DUPLICATE_SEND_DEPENDENCY")
        composition._historical_send_dependencies[group_id] = dependency
        composition._historical_send_by_client[client_id] = dependency
        member = dict(kind="INTENT", stamp=dict(
            event_id=event_id, effective_at=accepted_raw * 16 + 3,
            source_sequence=sequence, causal_parent_ids=[],
            ordering_contract_id="HISTORICAL_ORDER_V1", scenario_ordinal=60,
        ), payload=dict(
            intent_id=intent_id, client_order_id=client_id,
            config_id=contract["config_id"], product_id=product_id,
            strategy_id=contract["strategy_identity"],
            side="SHORT" if quantity > 0 else "LONG", order_type="MARKET",
            quantity_contracts=quantity.copy_abs(), limit_price=None,
            reduce_only=True, requested_at=parent_raw,
        ))
        group = dict(schema_version="scenario_group_v1", group_id=group_id,
                     account_key=deepcopy(composition._account), ordering_contract_id="HISTORICAL_ORDER_V1",
                     group_effective_at=accepted_raw * 16 + 3, declared_member_count=1, members=[member])
        policy_protocol._plan_group(group, composition._configured_product_ids)
        composition._historical_order_sequence += 1
        return dict(kind="SOURCE_GROUP", schedule_sequence=sequence, group=group)

    def record_group(self, group_id: str, result: Mapping[str, Any]) -> None:
        action = self.action_by_group.get(group_id)
        if action is None:
            return
        classification = result.get("classification")
        if classification not in ("COMMITTED", "REJECTED", "FAULT"):
            raise ValueError("INVALID_SCHEMA")
        action["classification"] = classification
        action["result"] = result
        if action["role"] == "EXIT" and classification == "REJECTED":
            self.rejected_exit_products.add(action["product_id"])

    def _paid_observation(
        self, composition: Any, scheduler_observation: Mapping[str, Any],
        *, current_evidence_complete: bool, failure: dict[str, Any] | None,
    ) -> dict[str, Any]:
        pending_source_groups = []
        pending_paid_deliveries = []
        for key in scheduler_observation["pending_keys"]:
            queue_class = key["queue_class"]
            if queue_class not in _QUEUE_LABELS:
                raise ValueError("INVALID_SCHEMA")
            queue_index = _QUEUE_LABELS.index(queue_class)
            record = composition._records.get((queue_index, key["stable_id"]))
            if queue_class == "SOURCE_GROUP":
                if record is not None and record["item"].get("kind") == "SOURCE_GROUP":
                    pending_source_groups.append(dict(
                        key=deepcopy(key), group=deepcopy(record["item"]["group"]),
                    ))
            elif queue_class == "DELIVERY" and record is not None:
                delivery = record["item"].get("delivery")
                if not isinstance(delivery, dict):
                    continue
                delivery_id = delivery["delivery_id"]
                if delivery_id in self.pending_delivery_ids:
                    cancel_action = self.cancel_ack_action_by_delivery.get(delivery_id)
                    pending_paid_deliveries.append(dict(
                        key=deepcopy(key), delivery=deepcopy(delivery),
                        role=self.delivery_roles.get(delivery_id),
                        cancel_action_id=cancel_action["action_id"] if cancel_action is not None else None,
                    ))
        send_dependencies = []
        for dependency in sorted(
            composition._historical_send_dependencies.values(), key=lambda row: row["group_id"],
        ):
            binding = dependency.get("binding")
            send_dependencies.append(dict(
                client_order_id=dependency["client_order_id"],
                product_id=dependency["product_id"],
                strategy_id=dependency["strategy_id"],
                group_id=dependency["group_id"],
                event_id=dependency["event_id"],
                state=dependency["state"],
                binding=(dict(
                    client_order_id=binding["client_order_id"],
                    order_id=binding["order_id"],
                    product_id=binding["product_id"],
                    strategy_id=binding["strategy_id"],
                ) if isinstance(binding, dict) else None),
                role=dependency["role"],
            ))
        terminal_fact = None
        last_popped = scheduler_observation["last_popped"]
        terminal = scheduler_observation["terminal"]
        if failure is not None and last_popped is not None and terminal is not None:
            queue_class = last_popped["queue_class"]
            if queue_class not in _QUEUE_LABELS:
                raise ValueError("INVALID_SCHEMA")
            queue_index = _QUEUE_LABELS.index(queue_class)
            record = composition._records.get((queue_index, last_popped["stable_id"]))
            kind = record["item"].get("kind") if record is not None else None
            record_terminal = any(
                row["key"] == last_popped
                and row["kind"] == kind
                and row["stable_id"] == last_popped["stable_id"]
                and row["classification"] == "TERMINAL"
                for row in scheduler_observation["records"]
            )
            terminal_matches = (
                terminal["kind"] == kind
                and terminal["stable_id"] == last_popped["stable_id"]
                and terminal["classification"] == "TERMINAL"
            )
            if kind in ("SOURCE_GROUP", "DELIVERY") and record_terminal and terminal_matches:
                item = record["item"]
                result = record.get("result")
                delivery = item.get("delivery") if kind == "DELIVERY" else None
                delivery_id = delivery.get("delivery_id") if isinstance(delivery, dict) else None
                cancel_action = (self.cancel_ack_action_by_delivery.get(delivery_id)
                                 if isinstance(delivery_id, str) else None)
                group_result = (result.get("group_result") if isinstance(result, dict) else None)
                if group_result is None:
                    group_result = terminal.get("group_result")
                terminal_fact = dict(
                    key=deepcopy(last_popped), kind=kind,
                    source_group=deepcopy(item.get("group")) if kind == "SOURCE_GROUP" else None,
                    delivery=deepcopy(delivery),
                    group_result=deepcopy(group_result),
                    failure=deepcopy(failure["primary"]),
                    role=self.delivery_roles.get(delivery_id) if delivery_id is not None else None,
                    cancel_action_id=(cancel_action["action_id"] if cancel_action is not None else None),
                )
        return dict(
            schema_version="spider_paid_observation_v1",
            run_contract_sha256=self.contract_hash,
            observed_at=scheduler_observation["current_time"],
            current_evidence_complete=current_evidence_complete,
            native_terminal=composition._paid_native_terminal,
            send_dependencies=send_dependencies,
            order_roles=[dict(order_id=order_id, role=role)
                         for order_id, role in sorted(self.roles_by_order.items())],
            pending_cancel_order_ids=sorted(composition._paid_cancel_pending_orders),
            pending_source_groups=pending_source_groups,
            pending_paid_deliveries=pending_paid_deliveries,
            terminal_fact=terminal_fact,
        )

    def result(self, composition: Any) -> dict[str, Any]:
        cutoff = (composition._current_time if composition._terminal is not None
                  else self.settlement_end_ms * 16 + 6)
        failure: dict[str, Any] | None = None
        evidence_error: dict[str, Any] | None = None
        inspection_error: dict[str, Any] | None = None
        inspection: dict[str, Any] | None = None
        primary = composition._terminal
        if primary is not None:
            failure = dict(
                primary=dict(
                    kind=primary.get("kind", "SCHEDULER"),
                    reason=primary.get("reason", "EXECUTION_FAILURE"),
                    native_failure=primary.get("native_failure"),
                ),
                evidence_error=None,
                inspection=None,
                inspection_error=None,
                observed_at=composition._current_time,
            )
            evidence_error = None
            inspection_error = None
            inspection = None
        owner_evidence = None
        current_evidence_complete = False
        try:
            prefix = "PAID_FINAL_" + self.contract_hash[:16]
            owner_evidence = composition.capture_owner_evidence(
                cutoff,
                composition._paid_snapshot_request("TRADING", cutoff, prefix),
                composition._paid_snapshot_request("POSITIONS", cutoff, prefix),
                composition._paid_snapshot_request("OPEN_ORDERS", cutoff, prefix),
            )["owner_evidence"]
            self.last_owner_evidence = owner_evidence
            self.last_cutoff = cutoff
            current_evidence_complete = True
        except Exception as exc:
            evidence_error = _error(exc, composition)
        if primary is not None or evidence_error is not None:
            try:
                inspection = composition._paid_inspection()
            except Exception as exc:
                inspection_error = _error(exc, composition)
            if primary is None:
                primary = dict(kind="OWNER_EVIDENCE", reason="EVIDENCE_FAILED", native_failure=None)
            failure = dict(primary=_normalize_error(primary), evidence_error=evidence_error,
                           inspection=inspection, inspection_error=inspection_error,
                           observed_at=composition._current_time)
            owner_evidence = self.last_owner_evidence
            cutoff = self.last_cutoff if owner_evidence is not None else None
        scheduler_observation = composition._public_scheduler_observation()
        paid_observation = self._paid_observation(
            composition, scheduler_observation,
            current_evidence_complete=current_evidence_complete, failure=failure,
        )
        return _classify_paid_endpoint_result(
            request_identity=dict(
                run_contract_sha256=self.contract_hash,
                policy_end_ms=self.policy_end_ms,
                settlement_end_ms=self.settlement_end_ms,
                generation=self.generation,
            ),
            owner_evidence=owner_evidence,
            actions=self.actions,
            scheduler_observation=scheduler_observation,
            paid_observation=paid_observation,
            failure=failure,
        )


def _error(error: Exception, composition: Any) -> dict[str, Any]:
    native = composition._paid_error_payload(error)
    if native is not None:
        return dict(kind=native["kind"], reason=native["reason"], native_failure=native)
    reason = str(error.args[0]) if error.args else type(error).__name__
    return dict(kind=type(error).__name__, reason=reason, native_failure=None)


def _normalize_error(error: dict[str, Any]) -> dict[str, Any]:
    return dict(kind=error.get("kind", "SCHEDULER"),
                reason=error.get("reason", "EXECUTION_FAILURE"),
                native_failure=error.get("native_failure"))


def _snapshot_rows(owner: dict[str, Any] | None, key: str) -> list[dict[str, Any]] | None:
    if owner is None:
        return None
    fact = owner.get(key + "_fact")
    if not isinstance(fact, dict):
        return None
    payload = fact.get("immutable_payload")
    if not isinstance(payload, dict) or payload.get("outcome") != "SUCCESS":
        return None
    rows = payload.get("rows", [])
    return rows if isinstance(rows, list) else None


_QUEUE_LABELS = (
    "SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
    "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER", "PAID_ENDPOINT_DRAIN",
)


def _key_sort(row: dict[str, Any]) -> tuple[Any, ...]:
    return row["visible_at"], row["queue_class"], row["schedule_sequence"], row["stable_id"]


def _reason_codes(**state: Any) -> list[str]:
    reasons: set[str] = set()
    actions = state["actions"]
    for action in actions:
        if action["classification"] == "PENDING":
            reasons.add("CANCEL_PENDING" if action["role"] == "DRAIN_CANCEL" else "EXIT_PENDING")
        elif action["role"] == "EXIT" and action["classification"] == "REJECTED":
            reasons.add("EXIT_REJECTED")
    if any(action["classification"] == "PENDING" and action["role"] == "EXIT" for action in actions):
        reasons.add("EXIT_PENDING")
    if state["working_exit_orders"]:
        reasons.add("EXIT_PENDING")
    if state["pending_sends"]:
        reasons.add("PENDING_SEND")
    if state["pending_cancels"]:
        reasons.add("CANCEL_PENDING")
    if state["pending_deliveries"]:
        reasons.add("NOTIFICATION_PENDING")
    if state["pending_polls"]:
        reasons.add("POLL_PENDING")
    if state["positions"] is None or state["orders"] is None:
        if state["execution_failure"]:
            reasons.add("EXECUTION_FAILURE")
    else:
        if any(_financial_decimal(row.get("position_contracts")) != 0 for row in state["positions"]):
            reasons.add("RESIDUAL_POSITION")
    if state["scheduler_terminal"]:
        reasons.add("EXECUTION_FAILURE")
    if state["native_terminal"] is not None:
        reasons.add("NATIVE_TERMINAL")
    return [code for code in _REASONS if code in reasons]


def _paid_flat(positions: object, orders: object, actions: list[object],
               deliveries: Sequence[object], polls: Sequence[object], *,
               pending_cancels: set[str], pending_sends: list[str]) -> bool:
    if not isinstance(positions, list) or not isinstance(orders, list):
        return False
    if any(_financial_decimal(row.get("position_contracts")) != 0
           for row in positions if isinstance(row, dict)):
        return False
    if orders or deliveries or polls or pending_cancels or pending_sends:
        return False
    return not actions
