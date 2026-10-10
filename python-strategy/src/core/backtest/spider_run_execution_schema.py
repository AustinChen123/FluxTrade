"""Closed structural schemas for the paid Spider research artifact family.

This module validates serialized shape and local identity links only. It does
not establish artifact-family consistency, run admission, or financial truth.
"""

from __future__ import annotations

import re
from typing import cast

from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_run_native_schema as native
from src.core.backtest.spider_execution_input import (
    decode_spider_execution_input,
    validate_spider_execution_input,
)
from src.core.backtest.spider_floating_exit_input import (
    decode_spider_floating_exit_input,
    validate_spider_floating_exit_input,
)
from src.core.backtest.spider_gross_execution_input import (
    decode_spider_gross_execution_input,
    validate_spider_gross_execution_input,
)
from src.core.backtest import spider_gross_artifact_schema as gross_schema
from src.core.backtest.spider_run_artifacts import (
    ConfigurationContext,
    ExecutionContext,
    FloatingExitContext,
    GrossExecutionContext,
    HistoricalContext,
    _HASH_NAMES,
    _P3_COVERAGE_KINDS,
    _attempt,
    _boundary,
    decode_canonical,
    _enum,
    _integer,
    _list,
    _object,
    _require,
    _text,
    configuration_context,
    decode_jsonl,
    encode_jsonl,
    execution_context as validate_execution_context,
    floating_exit_context as validate_floating_exit_context,
    gross_execution_context as validate_gross_execution_context,
    historical_context,
    _status,
)

_SCHEMA = {
    "attempt.json": "spider_execution_attempt_v1",
    "status.json": "spider_execution_status_v1",
    "journal.jsonl": "spider_execution_journal_record_v1",
    "execution_input.json": "spider_execution_input_v1",
    "paid_endpoint.json": "spider_paid_endpoint_result_v1",
    "report.jsonl": "spider_execution_report_v1",
    "reconciliation.json": "spider_execution_reconciliation_v1",
    "completion.json": "spider_execution_completion_v1",
}
_FAMILY_FILES = tuple(_SCHEMA)
_RESULT_STATUSES = ("PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED")
_ROLES = ("GRID", "EXIT")
_ACTIONS = ("DRAIN_CANCEL", "EXIT")
_ACTION_STATES = ("PENDING", "COMMITTED", "REJECTED", "FAULT")
_REASONS = (
    "PENDING_SEND", "CANCEL_PENDING", "EXIT_PENDING", "RESIDUAL_POSITION",
    "EXIT_REJECTED", "NOTIFICATION_PENDING", "POLL_PENDING", "NATIVE_TERMINAL",
    "EXECUTION_FAILURE",
)
_GROSS_REASONS = (*_REASONS, "GROSS_RESPONSE_PENDING")
_CHECKS = (
    "INPUT_IDENTITY", "JOURNAL_FRONTIER", "OWNER_SCOPE_AND_CHAIN", "SNAPSHOT_DIGESTS",
    "ACTION_BINDINGS", "ENDPOINT_CLASSIFICATION", "REPORT_PROJECTION",
)
_COMPLETION_ARTIFACTS = (
    ("attempt.json", "spider_execution_attempt_v1", 1),
    ("status.json", "spider_execution_status_v1", 1),
    ("journal.jsonl", "spider_execution_journal_record_v1", None),
    ("execution_input.json", "spider_execution_input_v1", 1),
    ("paid_endpoint.json", "spider_paid_endpoint_result_v1", 1),
    ("report.jsonl", "spider_execution_report_v1", 1),
    ("reconciliation.json", "spider_execution_reconciliation_v1", 1),
)
_FLOATING_SCHEMA = {
    "attempt.json": "spider_floating_exit_attempt_v1",
    "status.json": "spider_floating_exit_status_v1",
    "journal.jsonl": "spider_floating_exit_journal_record_v1",
    "execution_input.json": "spider_floating_exit_input_v1",
    "paid_endpoint.json": "spider_paid_endpoint_result_v1",
    "report.jsonl": "spider_floating_exit_report_v1",
    "reconciliation.json": "spider_floating_exit_reconciliation_v1",
    "completion.json": "spider_floating_exit_completion_v1",
}
_FLOATING_COMPLETION_ARTIFACTS = tuple(
    (name, schema, None if name == "journal.jsonl" else 1)
    for name, schema in _FLOATING_SCHEMA.items() if name != "completion.json"
)
_FLOATING_CONTRACT = "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1"
_FLOATING_TERMINAL = "PAID_CLOSE_RESEARCH_V1"
_GROSS_CONTRACT = "SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1"
_GROSS_SCHEMA = {
    "attempt.json": "spider_gross_execution_attempt_v1",
    "status.json": "spider_gross_execution_status_v1",
    "journal.jsonl": "spider_gross_execution_journal_record_v1",
    "execution_input.json": "spider_gross_execution_input_v1",
    "paid_endpoint.json": "spider_gross_paid_endpoint_result_v1",
    "report.jsonl": "spider_gross_execution_report_v1",
    "reconciliation.json": "spider_gross_execution_reconciliation_v1",
    "completion.json": "spider_gross_execution_completion_v1",
}
_GROSS_COMPLETION_ARTIFACTS = tuple(
    (name, schema, None if name == "journal.jsonl" else 1)
    for name, schema in _GROSS_SCHEMA.items() if name != "completion.json"
)


def _context_pair(row: dict[str, object]) -> tuple[ConfigurationContext, HistoricalContext]:
    context = configuration_context(row["configuration_context"])
    historical = historical_context(row["historical_context"])
    return context, historical


def execution_context_from_input(raw: bytes) -> dict[str, object]:
    """Validate original wrapper bytes and derive the exact persisted context."""
    value = decode_spider_execution_input(raw)
    contract_hash = validate_spider_execution_input(value)
    return dict(
        schema_version="spider_execution_context_v1",
        policy_id=value.policy_id,
        endpoint_policy_id=value.endpoint_policy_id,
        execution_input_sha256=contract_hash,
        policy_end_ms=value.policy_end_ms,
        settlement_end_ms=value.historical_input.range_end_ms,
        generation=value.generation,
    )


def floating_exit_context_from_input(raw: bytes) -> dict[str, object]:
    value = decode_spider_floating_exit_input(raw)
    candidate_hash = validate_spider_floating_exit_input(value)
    base_hash = validate_spider_execution_input(value.execution_input)
    run = value.execution_input
    return dict(
        schema_version="spider_floating_exit_context_v1",
        policy_id=value.policy_id,
        decision_policy_id=value.decision_policy_id,
        model_id=value.model_id,
        observation_projection_id="CHAT_BAYES_CLOSED15M_SOURCE14_V1",
        source_study_sha256="cc5bd45253f8d1f999c4437c944e5bf066fd8c61c30fbae64f0b68f47fb2da6c",
        endpoint_policy_id=run.endpoint_policy_id,
        execution_input_sha256=candidate_hash,
        base_execution_input_sha256=base_hash,
        policy_end_ms=run.policy_end_ms,
        settlement_end_ms=run.historical_input.range_end_ms,
        generation=run.generation,
    )


def gross_execution_context_from_input(raw: bytes) -> dict[str, object]:
    """Validate wrapper bytes and derive its full closed gross context."""
    value = decode_spider_gross_execution_input(raw)
    gross_hash = validate_spider_gross_execution_input(value)
    base_hash = validate_spider_execution_input(value.execution_input)
    run = value.execution_input
    return dict(
        schema_version="spider_gross_execution_context_v1",
        policy_id=value.policy_id,
        native_profile_id=value.native_profile_id,
        admission_policy_id=value.admission_policy_id,
        response_policy_id=value.response_policy_id,
        response_parameters=dict(
            trigger_cap=str(value.response_parameters.trigger_cap),
            target_cap=str(value.response_parameters.target_cap),
            allocation=value.response_parameters.allocation,
            quantity_rounding=value.response_parameters.quantity_rounding,
        ),
        authorization_scope=dict(
            kind="RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
            strategy_id=value.authorization_scope.strategy_id,
            ordered_products=list(value.authorization_scope.ordered_products),
        ),
        model_id=None,
        endpoint_policy_id=run.endpoint_policy_id,
        execution_input_sha256=gross_hash,
        base_execution_input_sha256=base_hash,
        policy_end_ms=run.policy_end_ms,
        settlement_end_ms=run.historical_input.range_end_ms,
        generation=run.generation,
    )


def _required_context(value: object | None) -> ExecutionContext | FloatingExitContext | GrossExecutionContext:
    _require(value is not None)
    if type(value) is GrossExecutionContext or (
        type(value) is dict and value.get("schema_version") == "spider_gross_execution_context_v1"
    ):
        return validate_gross_execution_context(value)
    if type(value) is FloatingExitContext or (
        type(value) is dict
        and cast(dict[str, object], value).get("schema_version") == "spider_floating_exit_context_v1"
    ):
        return validate_floating_exit_context(value)
    return validate_execution_context(value)


def _embedded_execution(row: dict[str, object], expected: ExecutionContext | FloatingExitContext | GrossExecutionContext) -> None:
    embedded = (_required_context(row["execution_context"]))
    _require(embedded == expected)


def _paid_attempt(value: object, expected: ExecutionContext | FloatingExitContext | None,
                  *, floating_exit: bool = False) -> None:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    schema = _FLOATING_SCHEMA if floating_exit else _SCHEMA
    _require(row.get("schema_version") == schema["attempt.json"])
    _text(row.get("run_id"), r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    _attempt(row, paid_execution=not floating_exit, floating_exit=floating_exit)
    embedded = (validate_floating_exit_context(row["execution_context"]) if floating_exit
                else validate_execution_context(row["execution_context"]))
    if expected is not None:
        _require(embedded == expected)
    _require(row["run_contract_id"] == (_FLOATING_CONTRACT if floating_exit else "SPIDER_PAID_RESEARCH_RUN_V1"))
    _require(row["registration_state"] == "VALIDATED" and row["registration_failure"] is None)
    _require(row["scenario_plan_sha256"] == embedded.execution_input_sha256)
    _require(row["terminal_policy"] == _FLOATING_TERMINAL if floating_exit
             else row["terminal_policy"] == "PAID_CLOSE_RESEARCH_V1")


def _gross_attempt(value: object, expected: GrossExecutionContext | None) -> None:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    _attempt(row, gross_execution=True)
    _require(row["schema_version"] == _GROSS_SCHEMA["attempt.json"])
    _text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    embedded = validate_gross_execution_context(row["execution_context"])
    _require(expected is None or embedded == expected)
    _require(row["run_contract_id"] == _GROSS_CONTRACT
             and row["requested_scenario_selector"] == _GROSS_CONTRACT
             and row["scenario_plan_id"] == _GROSS_CONTRACT
             and row["scenario_plan_sha256"] == embedded.execution_input_sha256
             and row["profile_id"] == embedded.native_profile_id
             and row["terminal_policy"] == "PAID_CLOSE_RESEARCH_V1")


def _paid_status(value: object, expected: ExecutionContext | FloatingExitContext | None,
                 *, floating_exit: bool = False) -> None:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    schema = _FLOATING_SCHEMA if floating_exit else _SCHEMA
    _require(row.get("schema_version") == schema["status.json"])
    _text(row.get("run_id"), r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    _status(row, paid_execution=not floating_exit, floating_exit=floating_exit)
    embedded = (validate_floating_exit_context(row["execution_context"]) if floating_exit
                else validate_execution_context(row["execution_context"]))
    if expected is not None:
        _require(embedded == expected)
    _require("configuration_context" in row and "historical_context" in row)
    configuration_context(row["configuration_context"])
    historical_context(row["historical_context"])


def _gross_status(value: object, expected: GrossExecutionContext | None) -> None:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    _status(row, gross_execution=True)
    _require(row["schema_version"] == _GROSS_SCHEMA["status.json"])
    _text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    embedded = validate_gross_execution_context(row["execution_context"])
    _require(expected is None or embedded == expected)
    _require("configuration_context" in row and "historical_context" in row)
    configuration_context(row["configuration_context"])
    historical_context(row["historical_context"])


def _paid_journal(value: object, execution: ExecutionContext | FloatingExitContext | GrossExecutionContext,
                  context: ConfigurationContext, historical: HistoricalContext) -> None:
    rows = _list(value)
    envelope.journal(rows, execution=execution)
    for item in rows:
        row = cast(dict[str, object], item)
        schema = (_GROSS_SCHEMA if isinstance(execution, GrossExecutionContext)
                  else _FLOATING_SCHEMA if isinstance(execution, FloatingExitContext) else _SCHEMA)
        _require(row["schema_version"] == schema["journal.jsonl"])
        _embedded_execution(row, execution)
        _require("configuration_context" in row and "historical_context" in row)
        _require(configuration_context(row["configuration_context"]) == context)
        _require(historical_context(row["historical_context"]) == historical)


def _key(value: object) -> tuple[int, int, int, str]:
    return envelope.scheduler_key(value, historical=True, paid_execution=True)


def _ordered(values: list[tuple]) -> None:
    _require(values == sorted(set(values)))


def _failure(value: object, *, context: ConfigurationContext | None = None) -> None:
    row = _object(value, "kind reason native_failure")
    _text(row["kind"], r"[A-Za-z][A-Za-z0-9_]{0,63}")
    _text(row["reason"])
    if row["native_failure"] is not None:
        failure = _object(row["native_failure"], "kind reason")
        _text(failure["kind"], r"[A-Z][A-Z0-9_]{0,63}")
        _text(failure["reason"])


def _paid_failure(value: object, *, context: ConfigurationContext, gross_profile: bool = False) -> None:
    row = _object(value, "primary evidence_error inspection inspection_error observed_at")
    _failure(row["primary"], context=context)
    for key in ("evidence_error", "inspection_error"):
        if row[key] is not None:
            _failure(row[key], context=context)
    if row["inspection"] is not None:
        native.inspection(row["inspection"], context=context, gross_profile=gross_profile)
    _integer(row["observed_at"])


def _actions(value: object, *, context: ConfigurationContext, gross_execution: bool = False) -> None:
    identities: list[str] = []
    for item in _list(value):
        row = _object(item, "action_id parent_action_id role product_id order_id group_id effective_at classification result")
        action_id = _text(row["action_id"])
        identities.append(action_id)
        if row["parent_action_id"] is not None:
            _text(row["parent_action_id"])
        allowed_roles = _ACTIONS + (("GROSS_CANCEL", "GROSS_REDUCTION") if gross_execution else ())
        role = _enum(row["role"], allowed_roles)
        _enum(row["product_id"], context.products)
        if row["order_id"] is not None:
            _text(row["order_id"])
        if row["group_id"] is not None:
            _text(row["group_id"])
        _integer(row["effective_at"])
        classification = _enum(row["classification"], _ACTION_STATES)
        if classification == "PENDING":
            _require(row["result"] is None)
        else:
            native.group_result(row["result"])
            result = cast(dict[str, object], row["result"])
            _require(result["classification"] == classification)
        _require(role != "DRAIN_CANCEL" or row["order_id"] is not None)
    _require(len(identities) == len(set(identities)))


def _position_rows(value: object, *, context: ConfigurationContext) -> None:
    identities: list[str] = []
    for item in _list(value):
        native.position(item, context=context)
        product = cast(dict[str, object], item)["product_id"]
        identities.append(cast(str, product))
    _require(identities == sorted(set(identities)))


def _open_order_rows(value: object, *, context: ConfigurationContext) -> None:
    identities: list[tuple[str, str]] = []
    for item in _list(value):
        native.open_order(item, context=context)
        row = cast(dict[str, object], item)
        identities.append((_text(row["product_id"]), _text(row["order_id"])))
    _require(identities == sorted(set(identities)))


def _delivery_key(value: object) -> tuple[int, int, int, str]:
    return _key(value)


def _paid_observation(value: object, execution: ExecutionContext | FloatingExitContext | GrossExecutionContext, context: ConfigurationContext) -> None:
    row = _object(
        value,
        "schema_version run_contract_sha256 observed_at current_evidence_complete native_terminal send_dependencies order_roles pending_cancel_order_ids pending_source_groups pending_paid_deliveries terminal_fact",
    )
    _enum(row["schema_version"], ["spider_gross_paid_observation_v1" if isinstance(execution, GrossExecutionContext)
                                   else "spider_paid_observation_v1"])
    _require(_text(row["run_contract_sha256"], r"[0-9a-f]{64}") == execution.execution_input_sha256)
    _integer(row["observed_at"])
    _require(type(row["current_evidence_complete"]) is bool)
    if row["native_terminal"] is not None:
        _enum(row["native_terminal"], ["LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"])
    dependencies: list[str] = []
    clients: set[str] = set()
    orders: set[str] = set()
    observation_roles = _ROLES + (("GROSS_CANCEL", "GROSS_REDUCTION")
                                  if isinstance(execution, GrossExecutionContext) else ())
    for item in _list(row["send_dependencies"]):
        dep = _object(item, "client_order_id product_id strategy_id group_id event_id state binding role")
        client = _text(dep["client_order_id"])
        clients.add(client)
        group = _text(dep["group_id"])
        dependencies.append(group)
        _enum(dep["product_id"], context.products)
        _text(dep["strategy_id"])
        _text(dep["event_id"])
        _enum(dep["state"], ["PENDING", "COMMITTED", "REJECTED"])
        send_roles = _ROLES + (("GROSS_CANCEL", "GROSS_REDUCTION") if isinstance(execution, GrossExecutionContext) else ())
        _enum(dep["role"], send_roles)
        binding = dep["binding"]
        if dep["state"] == "COMMITTED":
            bind = _object(binding, "client_order_id order_id product_id strategy_id")
            _require(bind["client_order_id"] == client and bind["product_id"] == dep["product_id"]
                     and bind["strategy_id"] == dep["strategy_id"])
            order_id = _text(bind["order_id"])
            _require(order_id not in orders)
            orders.add(order_id)
        else:
            _require(binding is None)
    _require(dependencies == sorted(set(dependencies)) and len(clients) == len(dependencies))
    role_rows: list[tuple[str, str]] = []
    for item in _list(row["order_roles"]):
        role = _object(item, "order_id role")
        delivery_roles = _ROLES + (("GROSS_CANCEL", "GROSS_REDUCTION") if isinstance(execution, GrossExecutionContext) else ())
        role_rows.append((_text(role["order_id"]), _enum(role["role"], delivery_roles)))
    _require(role_rows == sorted(set(role_rows)))
    cancel_ids = [_text(item) for item in _list(row["pending_cancel_order_ids"])]
    _require(cancel_ids == sorted(set(cancel_ids)))
    source_keys: list[tuple] = []
    for item in _list(row["pending_source_groups"]):
        pending = _object(item, "key group")
        key = _key(pending["key"])
        _require(key[1] == 0)
        source_keys.append(key)
        native.group(pending["group"], context=context, historical=True)
    _ordered(source_keys)
    delivery_keys: list[tuple] = []
    for item in _list(row["pending_paid_deliveries"]):
        pending = _object(item, "key delivery role cancel_action_id")
        key = _delivery_key(pending["key"])
        _require(key[1] == 2)
        delivery_keys.append(key)
        native.delivery(pending["delivery"], context=context)
        if pending["role"] is not None:
            _enum(pending["role"], observation_roles)
        if pending["cancel_action_id"] is not None:
            _text(pending["cancel_action_id"])
            delivery = cast(dict[str, object], pending["delivery"])
            _require(delivery["payload_kind"] == "TRANSPORT_ACK")
        _require(pending["role"] is not None or pending["cancel_action_id"] is not None)
    _ordered(delivery_keys)
    if row["terminal_fact"] is not None:
        terminal = _object(row["terminal_fact"], "key kind source_group delivery group_result failure role cancel_action_id")
        key = _key(terminal["key"])
        kind = _enum(terminal["kind"], ["SOURCE_GROUP", "DELIVERY"])
        _require(key[1] == (0 if kind == "SOURCE_GROUP" else 2))
        if kind == "SOURCE_GROUP":
            native.group(terminal["source_group"], context=context, historical=True)
            _require(terminal["delivery"] is None)
            _require(terminal["role"] is None and terminal["cancel_action_id"] is None)
        else:
            native.delivery(terminal["delivery"], context=context)
            _require(terminal["source_group"] is None)
        if terminal["group_result"] is not None:
            native.group_result(terminal["group_result"])
        _failure(terminal["failure"], context=context)
        if terminal["role"] is not None:
            _enum(terminal["role"], observation_roles)
        if terminal["cancel_action_id"] is not None:
            _text(terminal["cancel_action_id"])
        if terminal["cancel_action_id"] is not None:
            _text(terminal["cancel_action_id"])
            delivery = cast(dict[str, object], terminal["delivery"])
            _require(delivery["payload_kind"] == "TRANSPORT_ACK")


def _paid_endpoint(value: object, execution: ExecutionContext | FloatingExitContext | GrossExecutionContext, context: ConfigurationContext) -> None:
    row = _object(value, "schema_version run_contract_sha256 policy_end_ms settlement_end_ms generation status owner_evidence actions pending_obligations reasons failure")
    artifact_schema = (_GROSS_SCHEMA if isinstance(execution, GrossExecutionContext) else
                       _FLOATING_SCHEMA if isinstance(execution, FloatingExitContext) else _SCHEMA)
    _enum(row["schema_version"], [artifact_schema["paid_endpoint.json"]])
    _require(_text(row["run_contract_sha256"], r"[0-9a-f]{64}") == execution.execution_input_sha256)
    _require(_integer(row["policy_end_ms"]) == execution.policy_end_ms)
    _require(_integer(row["settlement_end_ms"]) == execution.settlement_end_ms)
    _require(_integer(row["generation"]) == execution.generation)
    status = _enum(row["status"], _RESULT_STATUSES)
    owner = row["owner_evidence"]
    if owner is not None:
        envelope.owner_evidence(owner, context=context,
                                gross_profile=isinstance(execution, GrossExecutionContext))
    else:
        _require(status == "FAILED")
    _actions(row["actions"], context=context,
             gross_execution=isinstance(execution, GrossExecutionContext))
    obligations = _object(row["pending_obligations"], "positions working_orders pending_actions pending_deliveries pending_polls")
    if obligations["positions"] is not None:
        _position_rows(obligations["positions"], context=context)
    if obligations["working_orders"] is not None:
        _open_order_rows(obligations["working_orders"], context=context)
    if owner is None:
        _require(obligations["positions"] is None and obligations["working_orders"] is None)
    elif status != "FAILED":
        _require(type(obligations["positions"]) is list and type(obligations["working_orders"]) is list)
    action_ids = [_text(item) for item in _list(obligations["pending_actions"])]
    _require(len(action_ids) == len(set(action_ids)))
    known_action_ids = {
        cast(dict[str, object], action)["action_id"] for action in _list(row["actions"])
    }
    _require(set(action_ids) <= known_action_ids)
    delivery_keys = [_key(item) for item in _list(obligations["pending_deliveries"])]
    _ordered(delivery_keys)
    polls = [envelope.poll(item) for item in _list(obligations["pending_polls"])]
    _ordered(polls)
    _require(len({item[0] for item in polls}) == len(polls) == len({item[1] for item in polls}))
    reason_codes = _GROSS_REASONS if isinstance(execution, GrossExecutionContext) else _REASONS
    reasons = [_enum(item, reason_codes) for item in _list(row["reasons"])]
    _require(reasons == sorted(set(reasons), key=reason_codes.index))
    if status == "FAILED":
        _paid_failure(row["failure"], context=context,
                      gross_profile=isinstance(execution, GrossExecutionContext))
    else:
        _require(row["failure"] is None and owner is not None)


def _scheduler(value: object) -> None:
    envelope.scheduler_observation(value, historical=True, paid_execution=True)


def _floating_observation(value: object, execution: FloatingExitContext,
                          products: tuple[str, ...]) -> None:
    row = _object(value, "schema_version run_contract_sha256 decision_batches product_exits")
    _enum(row["schema_version"], ["spider_floating_exit_observation_v1"])
    _require(_text(row["run_contract_sha256"], r"[0-9a-f]{64}") == execution.execution_input_sha256)
    _list(row["decision_batches"])
    exits = _list(row["product_exits"])
    identities = []
    for item in exits:
        progress = _object(item, "product_id reason requested_at status completed_at")
        identities.append(_enum(progress["product_id"], products))
        _enum(progress["reason"], ["TP", "RISK"])
        _integer(progress["requested_at"])
        _enum(progress["status"], [
            "EXITING", "EXITED", "INCOMPLETE", "FAILED",
            "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT",
        ])
        if progress["completed_at"] is not None:
            _integer(progress["completed_at"])
    _require(identities == [product for product in products if product in identities])
    _require(len(identities) == len(set(identities)))


def _report(value: object, execution: ExecutionContext | FloatingExitContext,
            context: ConfigurationContext, *, floating_exit: bool = False) -> None:
    required = "schema_version run_id initial_owner_evidence scheduler_observation paid_observation paid_endpoint source_evidence_refs"
    if floating_exit:
        required += " floating_observation"
    row = _object(value, required)
    schema = _FLOATING_SCHEMA if floating_exit else _SCHEMA
    _enum(row["schema_version"], [schema["report.jsonl"]])
    _text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    if row["initial_owner_evidence"] is not None:
        envelope.owner_evidence(row["initial_owner_evidence"], context=context)
    _scheduler(row["scheduler_observation"])
    _paid_observation(row["paid_observation"], execution, context)
    _paid_endpoint(row["paid_endpoint"], execution, context)
    if floating_exit:
        _require(isinstance(execution, FloatingExitContext))
        if row["floating_observation"] is None:
            _require(row["initial_owner_evidence"] is None
                     and cast(dict[str, object], row["paid_endpoint"])["status"] == "FAILED")
        else:
            _floating_observation(
                row["floating_observation"], cast(FloatingExitContext, execution), context.products,
            )
    _require(row["initial_owner_evidence"] is not None
             or cast(dict[str, object], row["paid_endpoint"])["status"] == "FAILED")
    refs = [_text(item, r"journal:[1-9][0-9]*") for item in _list(row["source_evidence_refs"])]
    _require(refs == sorted(set(refs), key=lambda item: int(item.split(":", 1)[1])))


def _refs(value: object, journal_rows: int | None = None) -> None:
    refs = [_text(item) for item in _list(value)]
    for ref in refs:
        if ref.startswith("journal:"):
            match = re.fullmatch(r"journal:([1-9][0-9]*)", ref)
            _require(match is not None and (journal_rows is None or int(match.group(1)) <= journal_rows))
        else:
            target, marker, pointer = ref.partition("#")
            _require(bool(marker) and target in ({"artifact:" + name for name in _FAMILY_FILES}
                                                  | {"artifact:" + name for name in _FLOATING_SCHEMA}
                                                  | {"artifact:completion.json"}))
            _require(not pointer or pointer.startswith("/"))
            _require(re.search(r"~(?![01])", pointer) is None)
    _require(len(refs) == len(set(refs)))


def _reconciliation(value: object, run_id: str, journal_rows: int | None,
                    *, floating_exit: bool = False) -> None:
    row = _object(value, "schema_version run_id result evidence_scope checks")
    schema = _FLOATING_SCHEMA if floating_exit else _SCHEMA
    _enum(row["schema_version"], [schema["reconciliation.json"]])
    _require(_text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}") == run_id)
    _enum(row["result"], ["OK", "FAILED"])
    _enum(row["evidence_scope"], ["COMPLETE_RUN", "FAILED_PREFIX"])
    checks = _list(row["checks"])
    _require(len(checks) == len(_CHECKS))
    for item, name in zip(checks, _CHECKS, strict=True):
        check = _object(item, "name result evidence_refs")
        _enum(check["name"], [name])
        _enum(check["result"], ["OK", "FAILED"])
        _refs(check["evidence_refs"], journal_rows)


def _completion(value: object, execution: ExecutionContext | FloatingExitContext,
                run_id: str, *, floating_exit: bool = False) -> None:
    row = _object(
        value,
        "schema_version run_id state terminal_reason input_contract_hashes planned_coverage processed_boundary persisted_boundary endpoint_state_digest reconciliation_digest artifacts configuration_context historical_context execution_context endpoint_status evidence_scope",
    )
    schema = _FLOATING_SCHEMA if floating_exit else _SCHEMA
    completion_artifacts = _FLOATING_COMPLETION_ARTIFACTS if floating_exit else _COMPLETION_ARTIFACTS
    _enum(row["schema_version"], [schema["completion.json"]])
    _require(_text(row["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}") == run_id)
    _enum(row["state"], ["COMPLETE"])
    _enum(row["terminal_reason"], [_FLOATING_TERMINAL if floating_exit else "PAID_CLOSE_RESEARCH_V1"])
    _text(row["endpoint_state_digest"], r"[0-9a-f]{64}")
    _text(row["reconciliation_digest"], r"[0-9a-f]{64}")
    for name in ("processed_boundary", "persisted_boundary"):
        _boundary(row[name])
    hashes = _list(row["input_contract_hashes"])
    _require(len(hashes) == len(_HASH_NAMES))
    for item, name in zip(hashes, _HASH_NAMES, strict=True):
        entry = _object(item, "name sha256")
        _enum(entry["name"], [name])
        _text(entry["sha256"], r"[0-9a-f]{64}")
    coverage: list[tuple[int, str]] = []
    seen: set[str] = set()
    for ordinal, item in enumerate(_list(row["planned_coverage"]), 1):
        entry = _object(item, "ordinal barrier_id record_kind")
        _require(_integer(entry["ordinal"]) == ordinal)
        barrier = _text(entry["barrier_id"])
        _require(barrier not in seen)
        seen.add(barrier)
        _enum(entry["record_kind"], [*_P3_COVERAGE_KINDS, "PAID_ENDPOINT_DRAIN"])
        coverage.append((ordinal, barrier))
    _require(bool(coverage))
    configuration, historical = _context_pair(row)
    _require(configuration is not None and historical is not None)
    _embedded_execution(row, execution)
    _enum(row["endpoint_status"], _RESULT_STATUSES)
    _enum(row["evidence_scope"], ["COMPLETE_RUN", "FAILED_PREFIX"])
    artifacts = _list(row["artifacts"])
    _require(len(artifacts) == len(completion_artifacts))
    for item, (path, artifact_schema, fixed_rows) in zip(artifacts, completion_artifacts, strict=True):
        artifact = _object(item, "path schema_version sha256 byte_count row_count")
        _enum(artifact["path"], [path])
        _enum(artifact["schema_version"], [artifact_schema])
        _text(artifact["sha256"], r"[0-9a-f]{64}")
        _integer(artifact["byte_count"])
        rows = _integer(artifact["row_count"])
        if fixed_rows is not None:
            _require(rows == fixed_rows)


def validate_execution_artifact(
    filename: str,
    raw: bytes,
    *,
    execution_context: object | None = None,
    configuration: object | None = None,
    historical: object | None = None,
    journal_rows: int | None = None,
) -> object:
    """Validate one closed paid-family serialized artifact, never resolve refs.

    `execution_context` is required for all artifacts except execution_input.json
    and is validated before it can authorize the native market hash.
    """
    gross_execution = (
        execution_context is not None
        and (type(execution_context) is GrossExecutionContext
             or (type(execution_context) is dict
                 and execution_context.get("schema_version") == "spider_gross_execution_context_v1"))
    )
    if filename == "execution_input.json" and execution_context is None:
        decoded_input = decode_canonical(raw) if type(raw) is bytes else None
        gross_execution = (type(decoded_input) is dict
                           and cast(dict[str, object], decoded_input).get("schema_version")
                           == "spider_gross_execution_input_v1")
    floating_exit = (
        execution_context is not None
        and (
            type(execution_context) is FloatingExitContext
            or (type(execution_context) is dict
                and execution_context.get("schema_version") == "spider_floating_exit_context_v1")
        )
    )
    family_schema = (_GROSS_SCHEMA if gross_execution else
                     _FLOATING_SCHEMA if floating_exit else _SCHEMA)
    _enum(filename, tuple(family_schema) if gross_execution or floating_exit else _FAMILY_FILES)
    _require(type(raw) is bytes)
    if filename == "execution_input.json":
        if gross_execution:
            value = decode_spider_gross_execution_input(raw)
            expected = validate_gross_execution_context(execution_context) if execution_context is not None else None
            expected_context = gross_execution_context_from_input(raw)
            _require(expected is None or expected == validate_gross_execution_context(expected_context))
        elif floating_exit:
            value = decode_spider_floating_exit_input(raw)
            expected = validate_floating_exit_context(execution_context) if execution_context is not None else None
            expected_context = floating_exit_context_from_input(raw)
            _require(expected is None or expected._asdict() == expected_context)
        else:
            value = decode_spider_execution_input(raw)
            expected_hash = validate_spider_execution_input(value)
            if execution_context is not None:
                expected = validate_execution_context(execution_context)
                _require(expected.execution_input_sha256 == expected_hash
                         and expected.policy_end_ms == value.policy_end_ms
                         and expected.settlement_end_ms == value.historical_input.range_end_ms
                         and expected.generation == value.generation
                         and expected.policy_id == value.policy_id
                         and expected.endpoint_policy_id == value.endpoint_policy_id)
        return value
    expected = _required_context(execution_context)
    if filename in ("attempt.json", "status.json", "paid_endpoint.json", "reconciliation.json", "completion.json"):
        rows = decode_jsonl(raw)
        _require(len(rows) == 1)
        value: object = rows[0]
    elif filename in ("journal.jsonl", "report.jsonl"):
        value = decode_jsonl(raw)
    else:
        raise ValueError("INVALID_ARTIFACT")
    if filename == "attempt.json" and gross_execution:
        _gross_attempt(value, cast(GrossExecutionContext, expected))
    elif filename == "status.json" and gross_execution:
        _gross_status(value, cast(GrossExecutionContext, expected))
    elif filename == "attempt.json":
        _paid_attempt(value, cast(ExecutionContext | FloatingExitContext, expected), floating_exit=floating_exit)
    elif filename == "status.json":
        _paid_status(value, cast(ExecutionContext | FloatingExitContext, expected), floating_exit=floating_exit)
    elif filename == "journal.jsonl":
        _require(configuration is not None and historical is not None)
        _paid_journal(value, expected, configuration_context(configuration), historical_context(historical))
    elif filename == "paid_endpoint.json":
        _require(configuration is not None)
        _paid_endpoint(value, expected, configuration_context(configuration))
    elif filename == "report.jsonl":
        rows = _list(value)
        _require(len(rows) == 1)
        _require(configuration is not None)
        if gross_execution:
            gross_schema.execution_report(
                rows[0], execution=cast(GrossExecutionContext, expected),
                configuration=configuration_context(configuration),
            )
            report_row = cast(dict[str, object], rows[0])
            _paid_observation(report_row["paid_observation"], expected,
                              configuration_context(configuration))
            _paid_endpoint(report_row["paid_endpoint"], expected,
                           configuration_context(configuration))
        else:
            _report(rows[0], cast(ExecutionContext | FloatingExitContext, expected),
                    configuration_context(configuration), floating_exit=floating_exit)
    elif filename == "reconciliation.json":
        row = cast(dict[str, object], value)
        if gross_execution:
            gross_schema.reconciliation(
                row, cast(str, row.get("run_id")), journal_rows=journal_rows,
                validate_refs=_refs,
            )
        else:
            _reconciliation(row, cast(str, row.get("run_id")), journal_rows, floating_exit=floating_exit)
    else:
        row = cast(dict[str, object], value)
        if gross_execution:
            gross_schema.completion(row, execution=cast(GrossExecutionContext, expected),
                                    run_id=cast(str, row.get("run_id")),
                                    artifact_schemas=_GROSS_COMPLETION_ARTIFACTS)
        else:
            _completion(row, cast(ExecutionContext | FloatingExitContext, expected),
                        cast(str, row.get("run_id")), floating_exit=floating_exit)
    return value


def validate_gross_paid_endpoint(value: object, execution: object, context: object) -> None:
    _paid_endpoint(value, validate_gross_execution_context(execution), configuration_context(context))


def validate_gross_report(value: object, execution: object, context: object) -> None:
    validate_execution_artifact(
        "report.jsonl", encode_jsonl([value]), execution_context=execution,
        configuration=context,
    )


def canonical_gross_paid_bytes(value: object, execution: object, context: object) -> bytes:
    raw = encode_jsonl([cast(object, value)])
    row = decode_jsonl(raw)[0]
    validate_gross_paid_endpoint(row, execution, context)
    return raw


def validate_paid_endpoint(value: object, execution: object, context: object) -> None:
    """Validate the frozen 11-field result DTO with explicit paid context."""
    _paid_endpoint(value, validate_execution_context(execution), configuration_context(context))


def validate_report(value: object, execution: object, context: object) -> None:
    """Validate the single public paid report record shape."""
    expected = validate_execution_context(execution)
    _report(value, expected, configuration_context(context))


def canonical_paid_bytes(value: object, execution: object, context: object) -> bytes:
    """Return canonical paid result JSONL bytes after structural validation."""
    raw = encode_jsonl([cast(object, value)])
    decoded = decode_jsonl(raw)
    validate_paid_endpoint(decoded[0], execution, context)
    return raw
