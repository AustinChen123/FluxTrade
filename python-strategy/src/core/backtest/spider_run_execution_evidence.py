"""Pure consistency predicate for the closed paid Spider artifact family.

This layer links already-serialized artifacts. It never constructs a native
owner, dispatches replay, accesses the filesystem, or computes financial state.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from hashlib import sha256
from typing import Any, cast

from src.core.backtest.spider_policy_protocol import (
    HistoricalDeliveryKind as _HistoricalDeliveryKind,
    HistoricalSequenceMode as _HistoricalSequenceMode,
    historical_delivery_sequence as _historical_delivery_sequence,
)
from src.core.backtest.spider_execution_input import (
    BASE_POLICY_SOURCE_SHA256,
    SpiderExecutionInput,
    decode_spider_execution_input,
    validate_spider_execution_input,
)
from src.core.backtest.spider_floating_exit_input import (
    SpiderFloatingExitInput,
    decode_spider_floating_exit_input,
    validate_spider_floating_exit_input,
)
from src.core.backtest.spider_gross_execution_input import (
    decode_spider_gross_execution_input,
    validate_spider_gross_execution_input,
)
from src.core.backtest.spider_floating_exit_policy import decide_floating_exits
from src.core.backtest.spider_bayes_replay import prepare_closed15m_source14
from src.core.backtest.spider_historical_input import (
    historical_context_for_input,
    historical_market_step_clock,
    historical_planned_coverage,
)
from src.core.backtest.spider_paid_endpoint import _classify_paid_endpoint_result
from src.core.backtest.spider_run_artifacts import (
    _HASH_NAMES,
    ConfigurationContext,
    ExecutionContext,
    FloatingExitContext,
    GrossExecutionContext,
    canonical_bytes,
    canonical_decimal_text,
    configuration_context,
    decode_canonical,
    decode_jsonl,
    execution_context,
    floating_exit_context,
    gross_execution_context,
    historical_context,
)
from src.core.backtest.spider_run_execution_schema import (
    _COMPLETION_ARTIFACTS,
    _GROSS_COMPLETION_ARTIFACTS,
    _FLOATING_COMPLETION_ARTIFACTS,
    _SCHEMA,
    _FLOATING_SCHEMA,
    _GROSS_SCHEMA,
    _CHECKS,
    execution_context_from_input,
    floating_exit_context_from_input,
    gross_execution_context_from_input,
    validate_execution_artifact,
)
from src.core.backtest.spider_run_evidence import (
    _historical_polls_endpoint_mode_compatible,
    _historical_snapshot_matches,
)

_CHECK_REFS: dict[str, list[str]] = {
    "INPUT_IDENTITY": ["artifact:attempt.json#/input_contract_hashes", "artifact:execution_input.json#"],
    "JOURNAL_FRONTIER": ["artifact:attempt.json#/planned_coverage", "artifact:status.json#/persisted_boundary", "artifact:journal.jsonl#"],
    "OWNER_SCOPE_AND_CHAIN": ["artifact:report.jsonl#/initial_owner_evidence", "artifact:paid_endpoint.json#/owner_evidence", "artifact:journal.jsonl#"],
    "SNAPSHOT_DIGESTS": ["artifact:report.jsonl#/initial_owner_evidence", "artifact:paid_endpoint.json#/owner_evidence"],
    "ACTION_BINDINGS": ["artifact:paid_endpoint.json#/actions", "artifact:report.jsonl#/paid_observation", "artifact:journal.jsonl#"],
    "ENDPOINT_CLASSIFICATION": ["artifact:paid_endpoint.json#", "artifact:report.jsonl#/paid_observation", "artifact:report.jsonl#/scheduler_observation"],
    "REPORT_PROJECTION": ["artifact:report.jsonl#", "artifact:paid_endpoint.json#"],
}
_CLASS_ORDER = {
    "SOURCE_GROUP": 0, "SNAPSHOT_CAPTURE": 1, "DELIVERY": 2,
    "HISTORICAL_MARKET_STEP": 3, "HISTORICAL_MARKET_CACHE": 4,
    "HISTORICAL_TIMER": 5, "PAID_ENDPOINT_DRAIN": 6,
}


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("INVALID_ARTIFACT")


def _decode_file(artifacts: Mapping[str, bytes], filename: str) -> object:
    raw = artifacts[filename]
    if filename == "execution_input.json":
        value = decode_canonical(raw)
        _require(type(value) is dict)
        if cast(dict[str, object], value).get("schema_version") == "spider_floating_exit_input_v1":
            return decode_spider_floating_exit_input(raw)
        if cast(dict[str, object], value).get("schema_version") == "spider_gross_execution_input_v1":
            return decode_spider_gross_execution_input(raw).execution_input
        return decode_spider_execution_input(raw)
    rows = decode_jsonl(raw)
    if filename.endswith(".jsonl"):
        return rows
    _require(len(rows) == 1)
    return rows[0]


def _key(value: object) -> tuple[int, int, int, str]:
    row = cast(dict[str, object], value)
    return (cast(int, row["visible_at"]), _CLASS_ORDER[cast(str, row["queue_class"])],
            cast(int, row["schedule_sequence"]), cast(str, row["stable_id"]))


def _expected_coverage(request: SpiderExecutionInput, contract_hash: str) -> list[dict[str, object]]:
    run = request.historical_input
    rows = historical_planned_coverage(run)
    scheduled: list[tuple[tuple[int, int, int, str], str, str]] = []
    step_seq = cache_seq = 0
    for entry in rows:
        barrier = cast(str, entry["barrier_id"])
        kind = cast(str, entry["record_kind"])
        if kind == "HISTORICAL_TIMER":
            continue
        if kind == "HISTORICAL_MARKET_STEP":
            bar_open, step = (int(part) for part in barrier.removeprefix("P3_MARKET_").split("_"))
            _, effective = historical_market_step_clock(bar_open, step)
            key = (effective, 3, step_seq, barrier)
            step_seq += 1
        else:
            close = int(barrier.removeprefix("MARKET_CLOSE_"))
            key = (close * 16 + 6, 4, cache_seq, barrier)
            cache_seq += 1
        scheduled.append((key, barrier, kind))
    timer_seq = 0
    for raw_at in range(run.range_start_ms + run.first_timer_ms - run.range_start_ms,
                        request.policy_end_ms, run.timer_period_ms):
        barrier = f"PAID_{contract_hash[:16]}_TIMER_{raw_at}"
        scheduled.append(((raw_at * 16 + 9, 5, timer_seq, barrier), barrier, "HISTORICAL_TIMER"))
        timer_seq += 1
    drain = f"PAID_DRAIN_{contract_hash[:16]}"
    scheduled.append(((request.policy_end_ms * 16 + 7, 6, 0, drain), drain, "PAID_ENDPOINT_DRAIN"))
    scheduled.sort()
    return [dict(ordinal=index, barrier_id=barrier, record_kind=kind)
            for index, (_, barrier, kind) in enumerate(scheduled, 1)]


def _coverage_keys(coverage: list[dict[str, object]], request: SpiderExecutionInput) -> dict[tuple[str, str], tuple[int, int, int, str]]:
    step_seq = cache_seq = timer_seq = 0
    keys = {}
    for row in coverage:
        kind, barrier = cast(str, row["record_kind"]), cast(str, row["barrier_id"])
        if kind == "HISTORICAL_MARKET_STEP":
            bar_open, step = (int(part) for part in barrier.removeprefix("P3_MARKET_").split("_"))
            _, effective = historical_market_step_clock(bar_open, step)
            keys[(kind, barrier)] = (effective, 3, step_seq, barrier)
            step_seq += 1
        elif kind == "HISTORICAL_MARKET_CACHE":
            close = int(barrier.removeprefix("MARKET_CLOSE_"))
            keys[(kind, barrier)] = (close * 16 + 6, 4, cache_seq, barrier)
            cache_seq += 1
        elif kind == "HISTORICAL_TIMER":
            raw_at = int(barrier.rsplit("_", 1)[1])
            keys[(kind, barrier)] = (raw_at * 16 + 9, 5, timer_seq, barrier)
            timer_seq += 1
        else:
            keys[(kind, barrier)] = (request.policy_end_ms * 16 + 7, 6, 0, barrier)
    return keys


def _same(left: object, right: object) -> bool:
    try:
        return canonical_bytes(left) == canonical_bytes(right)
    except (TypeError, ValueError, UnicodeError):
        return False


def _group_has_intent(group: object, dependency: dict[str, Any]) -> bool:
    if type(group) is not dict or group.get("group_id") != dependency["group_id"]:
        return False
    members = group.get("members")
    if type(members) is not list:
        return False
    matching = [member for member in members if type(member) is dict
                and member.get("kind") == "INTENT"
                and type(member.get("payload")) is dict
                and member["payload"].get("client_order_id") == dependency["client_order_id"]]
    return len(matching) == 1 and all(
        matching[0]["payload"].get(field) == dependency[field]
        for field in ("product_id", "strategy_id")
    )


def _paid_delivery_origins(journal: list[dict[str, Any]], request: SpiderExecutionInput,
                          roles: dict[str, str], actions_by_group: dict[str, dict[str, Any]],
                          mode: _HistoricalSequenceMode) -> list[dict[str, Any]]:
    """Project native fill/cancel facts into the existing ordered delivery keys."""
    sequence = 0
    origins = []
    cumulative_by_order: dict[str, Decimal] = {}
    for row in journal:
        if row["record_kind"] == "HISTORICAL_MARKET_RESULT":
            result = row["payload"]["result"]
            visible_at = (result["raw_time_ms"] + request.historical_input.order_notice_delay_ms) * 16 + 6
            for product in result["products"]:
                for fill in product["fills"]:
                    cumulative = (cumulative_by_order.get(fill["order_id"], Decimal(0))
                                  + Decimal(fill["quantity_contracts"]))
                    cumulative_by_order[fill["order_id"]] = cumulative
                    role = roles.get(fill["order_id"])
                    origins.append(dict(
                        visible_at=visible_at,
                        schedule_sequence=_historical_delivery_sequence(
                            mode, _HistoricalDeliveryKind.NATIVE, sequence,
                        ),
                        payload_kind="EXECUTION_FACT", source_fact_id=fill["source_event_id"],
                        occurrence_index=fill["occurrence_index"], order_id=fill["order_id"],
                        product_id=product["product_id"], price=fill["price"],
                        quantity=cumulative, effective_at=result["effective_at"],
                        role=role, cancel_action_id=None, paid_obligation=role is not None,
                    ))
                    sequence += 1
        elif row["record_kind"] == "SOURCE_GROUP_RESULT":
            group = row["payload"]["request"]
            result = row["payload"]["result"]
            members = group["members"]
            if (len(members) == 1 and members[0]["kind"] == "CANCEL_EFFECT"
                    and result["classification"] == "COMMITTED"):
                action = actions_by_group.get(group["group_id"])
                stamp = members[0]["stamp"]
                visible_at = (stamp["effective_at"] // 16
                              + request.historical_input.cancel_ack_delay_ms) * 16 + 6
                origins.append(dict(
                    visible_at=visible_at,
                    schedule_sequence=_historical_delivery_sequence(
                        mode, _HistoricalDeliveryKind.NATIVE, sequence,
                    ),
                    payload_kind="TRANSPORT_ACK", source_fact_id=stamp["event_id"],
                    occurrence_index=0, order_id=action["order_id"] if action is not None else None,
                    role=None, cancel_action_id=action["action_id"] if action is not None else None,
                    paid_obligation=action is not None,
                ))
                sequence += 1
    return origins


def _input_check(run_id: str, attempt: dict[str, Any], status: dict[str, Any],
                 report: dict[str, Any], endpoint: dict[str, Any],
                 request: SpiderExecutionInput,
                 context: ExecutionContext | FloatingExitContext | GrossExecutionContext,
                 configured: ConfigurationContext, history: object,
                 *, contract_hash: str | None = None,
                 floating_exit: bool = False, gross_execution: bool = False,
                 execution_context_wire: dict[str, object] | None = None) -> bool:
    run = request.historical_input
    wrapper_hash = contract_hash or validate_spider_execution_input(request)
    try:
        config = decode_canonical(run.configuration_bytes)
    except ValueError:
        return False
    account = dict(venue="SPIDER_HISTORICAL_RESEARCH", environment="RESEARCH_ONLY", account=run.account_key)
    hashes = attempt["input_contract_hashes"]
    return (
        run.run_id == run_id == attempt["run_id"] == status["run_id"] == report["run_id"]
        and attempt["run_contract_id"] == (
            "SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1" if gross_execution else
            "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1" if floating_exit else
            "SPIDER_PAID_RESEARCH_RUN_V1"
        )
        and attempt["registration_state"] == "VALIDATED"
        and attempt["scenario_plan_sha256"] == wrapper_hash == context.execution_input_sha256
        and attempt["policy_source_sha256"] == BASE_POLICY_SOURCE_SHA256
        and cast(dict[str, Any], config)["config_id"] == configured.config_id
        and attempt["account_key"] == account
        and attempt["profile_id"] == (
            "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1" if gross_execution
            else "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1"
        )
        and configured.products == run.ordered_products
        and attempt["configuration_context"] == configured._asdict() | {"products": list(configured.products)}
        and attempt["historical_context"] == history
        and status["configuration_context"] == attempt["configuration_context"]
        and status["historical_context"] == attempt["historical_context"]
        and historical_context_for_input(run) == history
        and [row["name"] for row in hashes] == list(_HASH_NAMES)
        and [row["sha256"] for row in hashes] == [
            wrapper_hash, attempt["program_sha256"], attempt["native_artifact_sha256"],
            attempt["policy_source_sha256"],
        ]
        and all(len(row["sha256"]) == 64 for row in hashes)
        and endpoint["run_contract_sha256"] == wrapper_hash
        and report["paid_endpoint"] == endpoint
        and attempt["execution_context"] == (
            execution_context_wire if gross_execution else context._asdict()
        )
        and status["execution_context"] == (
            execution_context_wire if gross_execution else context._asdict()
        )
    )


def _journal_check(run_id: str, attempt: dict[str, Any], status: dict[str, Any],
                   journal: list[dict[str, Any]], report: dict[str, Any],
                   request: SpiderExecutionInput, contract_hash: str,
                   endpoint: dict[str, Any]) -> bool:
    coverage = _expected_coverage(request, contract_hash)
    boundary = (dict(ordinal=len(journal), journal_seq=len(journal), barrier_id=journal[-1]["barrier_id"])
                if journal else None)
    seqs = [row["journal_seq"] for row in journal]
    barriers = [row["barrier_id"] for row in journal]
    keys = [_key(row["scheduler_key"]) for row in journal]
    refs = [f"journal:{index}" for index in range(1, len(journal) + 1)]
    scheduler = report["scheduler_observation"]
    records = scheduler["records"]
    expected_records = {
        (cast(str, row["record_kind"]), cast(str, row["barrier_id"])): row for row in coverage
    }
    observed_records = {
        (cast(str, row["kind"]), cast(str, row["stable_id"])): row for row in records
    }
    terminal_key = scheduler["last_popped"] if scheduler["gate"] == "FAILED" else None
    planned_keys = _coverage_keys(coverage, request)
    initial_cut = request.historical_input.range_start_ms * 16
    failure = endpoint["failure"]
    observation = report["paid_observation"]
    initial_capture_failure = (
        not journal
        and status["processed_boundary"] is None and status["persisted_boundary"] is None
        and endpoint["status"] == "FAILED" and endpoint["owner_evidence"] is None
        and report["initial_owner_evidence"] is None and report["source_evidence_refs"] == []
        and failure is not None and failure["primary"]["kind"] == "OWNER_EVIDENCE"
        and failure["primary"]["reason"] == "EVIDENCE_FAILED" and failure["evidence_error"] is not None
        and failure["observed_at"] == initial_cut
        and observation["current_evidence_complete"] is False
        and observation["observed_at"] == initial_cut
        and not endpoint["actions"]
        and not observation["send_dependencies"] and not observation["pending_cancel_order_ids"]
        and not observation["pending_source_groups"] and not observation["pending_paid_deliveries"]
        and observation["terminal_fact"] is None
        and scheduler["current_time"] == initial_cut and scheduler["gate"] == "RUNNING"
        and scheduler["last_popped"] is None and scheduler["terminal"] is None
        and not scheduler["polls"] and not scheduler["callback_actions"]
    )
    if initial_capture_failure:
        # At construction, the first market step, snapshots, timers and paid
        # drain are queued; later market steps are causally enqueued by earlier
        # steps and therefore are not yet observable at this zero-event prefix.
        market_steps = [key for key in expected_records if key[0] == "HISTORICAL_MARKET_STEP"]
        initial_identities = {
            key for key in expected_records
            if key[0] in ("HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER", "PAID_ENDPOINT_DRAIN")
        }
        if market_steps:
            initial_identities.add(min(market_steps, key=lambda key: planned_keys[key]))
        actual_identities = [(row["kind"], row["stable_id"]) for row in records]
        initial_records_match = len(actual_identities) == len(set(actual_identities)) \
            and set(actual_identities) == initial_identities
        for row in records:
            identity = row["kind"], row["stable_id"]
            expected_key = planned_keys.get(identity)
            initial_records_match = (initial_records_match and expected_key is not None
                                     and row["classification"] == "PENDING"
                                     and _same(row["key"], dict(
                                         visible_at=expected_key[0],
                                         queue_class=("HISTORICAL_MARKET_STEP" if expected_key[1] == 3
                                                      else "HISTORICAL_MARKET_CACHE" if expected_key[1] == 4
                                                      else "HISTORICAL_TIMER" if expected_key[1] == 5
                                                      else "PAID_ENDPOINT_DRAIN"),
                                         schedule_sequence=expected_key[2], stable_id=expected_key[3],
                                     )))
        expected_pending = sorted(
            (planned_keys[identity] for identity in initial_identities),
        )
        pending_matches = [_key(key) for key in scheduler["pending_keys"]] == expected_pending
        plan_matches = initial_records_match and pending_matches
    else:
        plan_matches = True
        for key in expected_records:
            observed = observed_records.get(key)
            expected_key = planned_keys[key]
            if observed is None:
                plan_matches = plan_matches and endpoint["status"] == "FAILED" and terminal_key is not None \
                    and expected_key > _key(terminal_key)
            elif endpoint["status"] == "FAILED" and terminal_key is not None:
                if _key(observed["key"]) < _key(terminal_key):
                    plan_matches = plan_matches and observed["classification"] == "SUCCESS"
                elif observed["key"] == terminal_key:
                    plan_matches = plan_matches and observed["classification"] == "TERMINAL"
                else:
                    plan_matches = plan_matches and observed["classification"] == "PENDING"
            else:
                plan_matches = plan_matches and observed["classification"] == "SUCCESS"
    records_by_key = {canonical_bytes(row["key"]): row for row in records}
    journal_observed = all(canonical_bytes(row["scheduler_key"]) in records_by_key for row in journal)
    return (
        attempt["planned_coverage"] == coverage
        and plan_matches and journal_observed
        and seqs == list(range(1, len(journal) + 1))
        and len(barriers) == len(set(barriers))
        and keys == sorted(keys)
        and all(row["run_id"] == run_id for row in journal)
        and status["persisted_boundary"] == boundary == status["processed_boundary"]
        and report["source_evidence_refs"] == refs
    )


def _owner_checks(attempt: dict[str, Any], initial: object, endpoint: dict[str, Any],
                  journal: list[dict[str, Any]], configured: ConfigurationContext,
                  request: SpiderExecutionInput, report: dict[str, Any]) -> tuple[bool, bool]:
    account = attempt["account_key"]
    owners: list[dict[str, Any]] = []
    if type(initial) is dict:
        owners.append(cast(dict[str, Any], initial))
    final = endpoint["owner_evidence"]
    if type(final) is dict:
        owners.append(cast(dict[str, Any], final))
    scope_ok = bool(owners) or endpoint["status"] == "FAILED"
    snapshots_ok = True
    for owner in owners:
        inspection = owner["inspection"]
        snapshots_ok = snapshots_ok and inspection["account_key"] == account
        snapshots_ok = snapshots_ok and inspection["profile_id"] == attempt["profile_id"]
        snapshots_ok = snapshots_ok and inspection["config_id"] == configured.config_id
        snapshots_ok = snapshots_ok and _historical_snapshot_matches(
            owner, owner["cutoff"], account, inspection["account_version"],
        )
    if type(initial) is dict:
        scope_ok = scope_ok and initial["cutoff"] == request.historical_input.range_start_ms * 16
    paid_observation = report["paid_observation"]
    if type(final) is dict:
        if endpoint["status"] != "FAILED":
            scope_ok = scope_ok and final["cutoff"] == request.historical_input.range_end_ms * 16 + 6
        elif paid_observation["current_evidence_complete"]:
            scope_ok = scope_ok and final["cutoff"] == paid_observation["observed_at"]
        else:
            scope_ok = scope_ok and final["cutoff"] <= paid_observation["observed_at"]
    chain_digest = (owners[0]["inspection"]["owner_state_digest"] if owners else None)
    chain_version = (owners[0]["inspection"]["account_version"] if owners else None)
    for row in journal:
        payload = row["payload"]
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            result = payload["result"]
            before = payload.get("owner_evidence_before")
            after = payload.get("owner_evidence_after")
            if type(before) is not dict or type(after) is not dict:
                scope_ok = False
                snapshots_ok = False
                continue
            group_at = payload["request"]["group_effective_at"]
            before_inspection, after_inspection = before["inspection"], after["inspection"]
            for captured in (before, after):
                inspection = captured["inspection"]
                snapshots_ok = snapshots_ok and captured["cutoff"] == group_at
                snapshots_ok = snapshots_ok and inspection["account_key"] == account
                snapshots_ok = snapshots_ok and inspection["profile_id"] == attempt["profile_id"]
                snapshots_ok = snapshots_ok and inspection["config_id"] == configured.config_id
                snapshots_ok = snapshots_ok and _historical_snapshot_matches(
                    captured, group_at, account, inspection["account_version"],
                )
            if chain_digest is not None:
                scope_ok = scope_ok and before_inspection["owner_state_digest"] == chain_digest
                scope_ok = scope_ok and before_inspection["account_version"] == chain_version
            scope_ok = scope_ok and result["account_version_before"] == before_inspection["account_version"]
            scope_ok = scope_ok and result["account_version_after"] == after_inspection["account_version"]
            scope_ok = scope_ok and row["account_version_before"] == result["account_version_before"]
            scope_ok = scope_ok and row["account_version_after"] == result["account_version_after"]
            if result["owner_state_digest"] is not None:
                scope_ok = scope_ok and result["owner_state_digest"] == after_inspection["owner_state_digest"]
            if result["classification"] == "FAULT":
                scope_ok = scope_ok and result["account_version_before"] == result["account_version_after"]
            chain_digest, chain_version = after_inspection["owner_state_digest"], after_inspection["account_version"]
        elif row["record_kind"] == "HISTORICAL_MARKET_RESULT":
            before, after = payload["owner_inspection_before"], payload["owner_inspection_after"]
            scope_ok = scope_ok and before["account_key"] == account and after["account_key"] == account
            if chain_digest is not None:
                scope_ok = scope_ok and before["owner_state_digest"] == chain_digest
                scope_ok = scope_ok and before["account_version"] == chain_version
                scope_ok = scope_ok and row["account_version_before"] == chain_version
            scope_ok = scope_ok and row["account_version_after"] == after["account_version"]
            scope_ok = scope_ok and after == payload["result"]["owner_evidence"]
            chain_digest, chain_version = after["owner_state_digest"], after["account_version"]
    if type(final) is dict and chain_digest is not None:
        scope_ok = scope_ok and final["inspection"]["owner_state_digest"] == chain_digest
        scope_ok = scope_ok and final["inspection"]["account_version"] == chain_version
    return scope_ok, snapshots_ok


def _action_bindings(attempt: dict[str, Any], journal: list[dict[str, Any]],
                     report: dict[str, Any], endpoint: dict[str, Any],
                     request: SpiderExecutionInput, *, contract_hash: str | None = None,
                     floating_exit: bool = False, gross_execution: bool = False) -> bool:
    scheduler = report["scheduler_observation"]
    cutoff = scheduler["current_time"]
    paid_prefix = f"PAID_{(contract_hash or validate_spider_execution_input(request))[:16]}"
    allow_terminal = endpoint["status"] == "FAILED" and scheduler["gate"] == "FAILED"
    return any(
        _historical_polls_endpoint_mode_compatible(scheduler, cutoff, mode, paid_prefix, allow_terminal)
        and _action_bindings_for_mode(
            attempt, journal, report, endpoint, request, mode,
            contract_hash=contract_hash or validate_spider_execution_input(request),
            floating_exit=floating_exit, gross_execution=gross_execution,
        )
        for mode in _HistoricalSequenceMode
    )


def _action_bindings_for_mode(attempt: dict[str, Any], journal: list[dict[str, Any]],
                              report: dict[str, Any], endpoint: dict[str, Any],
                              request: SpiderExecutionInput,
                              mode: _HistoricalSequenceMode, *, contract_hash: str,
                              floating_exit: bool, gross_execution: bool = False) -> bool:
    observation = report["paid_observation"]
    scheduler = report["scheduler_observation"]
    actions = endpoint["actions"]
    if observation["observed_at"] != scheduler["current_time"]:
        return False
    final_owner = endpoint["owner_evidence"]
    if type(final_owner) is dict:
        lifecycle = final_owner["inspection"]["lifecycle"]
        terminal_status = lifecycle if lifecycle in ("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT") else None
        if observation["native_terminal"] != terminal_status:
            return False
    groups = {}
    for row in journal:
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            groups[row["payload"]["request"]["group_id"]] = row
    pending_groups = {item["group"]["group_id"]: (item["group"], item["key"])
                      for item in observation["pending_source_groups"]}
    terminal_observation = observation["terminal_fact"]
    terminal_group = (terminal_observation["source_group"]
                      if terminal_observation is not None else None)
    action_by_id = {row["action_id"]: row for row in actions}
    actions_by_group = {row["group_id"]: row for row in actions if row["group_id"] is not None}
    if len(actions_by_group) != len(actions):
        return False
    initial = report["initial_owner_evidence"]

    def source_group_for(action: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
        group_id = action["group_id"]
        row = groups.get(group_id)
        if row is not None:
            return row["payload"]["request"], row["scheduler_key"]
        if group_id in pending_groups:
            return pending_groups[group_id]
        if terminal_group is not None and terminal_group.get("group_id") == group_id:
            return terminal_group, terminal_observation["key"]
        return None

    action_admission_keys = []
    for action in actions:
        group_id = action["group_id"]
        if group_id is None:
            return False
        row = groups.get(group_id)
        source = source_group_for(action)
        if source is None:
            return False
        group, key = source
        if (group["group_id"] != group_id or group["account_key"] != attempt["account_key"]
                or group["ordering_contract_id"] != "HISTORICAL_ORDER_V1"
                or group["group_effective_at"] != action["effective_at"]
                or key["queue_class"] != "SOURCE_GROUP" or key["stable_id"] != group_id
                or key["visible_at"] != action["effective_at"]):
            return False
        action_admission_keys.append(_key(key))
        members = group["members"]
        if len(members) != 1:
            return False
        member = members[0]
        stamp = member["stamp"]
        if stamp["effective_at"] != action["effective_at"] or stamp["ordering_contract_id"] != "HISTORICAL_ORDER_V1":
            return False
        if row is None:
            if action["classification"] != "PENDING":
                return False
        else:
            result = row["payload"]["result"]
            if action["classification"] != result["classification"] or not _same(action["result"], result):
                return False
        if action["role"] == "EXIT" or (gross_execution and action["role"] == "GROSS_REDUCTION"):
            payload = member["payload"]
            if (member["kind"] != "INTENT" or payload["product_id"] != action["product_id"]
                    or payload["strategy_id"] != request.historical_input.strategy_identity
                    or payload["reduce_only"] is not True or action["parent_action_id"] is not None):
                return False
            if action["order_id"] is not None:
                dependency = next((item for item in observation["send_dependencies"]
                                   if item["group_id"] == group_id), None)
                if (dependency is None or dependency["state"] != "COMMITTED"
                        or dependency["binding"] is None
                        or dependency["binding"]["order_id"] != action["order_id"]):
                    return False
        elif (action["role"] == "DRAIN_CANCEL"
              or (gross_execution and action["role"] == "GROSS_CANCEL")) and member["kind"] == "CANCEL_REQUEST":
            targets = member["payload"]["targets"]
            if len(targets) != 1 or action["parent_action_id"] is not None:
                return False
            target_order_id = targets[0]["target_order_id"]
            if action["order_id"] not in (None, target_order_id):
                return False
        elif (action["role"] == "DRAIN_CANCEL"
              or (gross_execution and action["role"] == "GROSS_CANCEL")) and member["kind"] == "CANCEL_EFFECT":
            effects = member["payload"]["effects"]
            if len(effects) != 1 or action["order_id"] != effects[0]["target_order_id"]:
                return False
            parent = action_by_id.get(action["parent_action_id"])
            if (parent is None or parent["role"] != action["role"]
                    or parent["parent_action_id"] is not None
                    or parent["effective_at"] > action["effective_at"]):
                return False
            parent_source = source_group_for(parent)
            if parent_source is None:
                return False
            parent_group = parent_source[0]
            parent_members = parent_group["members"]
            if (len(parent_members) != 1 or parent_members[0]["kind"] != "CANCEL_REQUEST"
                    or parent_members[0]["payload"]["targets"][0]["target_order_id"] != action["order_id"]
                    or stamp["causal_parent_ids"] != [parent_members[0]["stamp"]["event_id"]]):
                return False
        else:
            return False
    if floating_exit or gross_execution:
        for sequence, action in enumerate(actions):
            expected_id = "PAID_ACTION_" + sha256(canonical_bytes({
                "run_contract_sha256": contract_hash,
                "action_sequence": sequence,
                "role": action["role"],
            })).hexdigest()
            if action["action_id"] != expected_id:
                return False
    if gross_execution:
        # Actions retain source-group registration order; future cancel effects
        # may be registered before requests effective sooner. Legacy paid ordering
        # remains unchanged; financial journal order is verified independently.
        action_schedule_sequences = [key[2] for key in action_admission_keys]
        if action_schedule_sequences != sorted(action_schedule_sequences):
            return False
    elif not floating_exit and action_admission_keys != sorted(action_admission_keys):
        return False
    seed = decode_canonical(request.historical_input.initial_account_state)
    seed_orders = cast(dict[str, Any], seed)["orders"]
    role_rows = {row["order_id"]: row["role"] for row in observation["order_roles"]}
    expected_roles = {row["order_id"]: "GRID" for row in seed_orders}
    product_by_order = {row["order_id"]: row["product_id"] for row in seed_orders}
    for dependency in observation["send_dependencies"]:
        if dependency["state"] == "COMMITTED":
            order_id = dependency["binding"]["order_id"]
            expected_roles[order_id] = dependency["role"]
            product_by_order[order_id] = dependency["product_id"]
    if role_rows != expected_roles:
        return False
    if len(role_rows) != len(observation["order_roles"]):
        return False
    owners_to_scan = [initial, endpoint["owner_evidence"]]
    owners_to_scan.extend(
        row["payload"].get("owner_evidence_after") for row in journal
        if row["record_kind"] == "SOURCE_GROUP_RESULT"
    )
    for owner in owners_to_scan:
        if type(owner) is not dict:
            continue
        native_rows = owner["open_orders_fact"]["immutable_payload"]["rows"]
        for native_order in native_rows:
            previous = product_by_order.get(native_order["order_id"])
            if previous is not None and previous != native_order["product_id"]:
                return False
            product_by_order[native_order["order_id"]] = native_order["product_id"]
    for action in actions:
        if action["role"] == "DRAIN_CANCEL" or (
            gross_execution and action["role"] == "GROSS_CANCEL"
        ):
            source = source_group_for(action)
            if source is None:
                return False
            member = source[0]["members"][0]
            target_id = (member["payload"]["targets"][0]["target_order_id"]
                         if member["kind"] == "CANCEL_REQUEST"
                         else member["payload"]["effects"][0]["target_order_id"])
            if role_rows.get(target_id) != "GRID" or product_by_order.get(target_id) != action["product_id"]:
                return False
    for dependency in observation["send_dependencies"]:
        row = groups.get(dependency["group_id"])
        if dependency["state"] == "PENDING":
            pending_groups = [item["group"] for item in observation["pending_source_groups"]
                              if item["group"].get("group_id") == dependency["group_id"]]
            terminal = observation["terminal_fact"]
            terminal_group = terminal["source_group"] if terminal is not None else None
            group = pending_groups[0] if len(pending_groups) == 1 else terminal_group
            if group is None or not _group_has_intent(group, dependency):
                return False
            continue
        if row is None:
            terminal = observation["terminal_fact"]
            terminal_group = terminal["source_group"] if terminal is not None else None
            if (dependency["state"] == "COMMITTED" and terminal_group is not None
                    and terminal_group.get("group_id") == dependency["group_id"]
                    and _group_has_intent(terminal_group, dependency)
                    and terminal["group_result"] is not None
                    and terminal["group_result"]["classification"] == "COMMITTED"):
                continue
            return False
        payload = row["payload"]
        intent = next((member for member in payload["request"]["members"]
                       if member["kind"] == "INTENT"
                       and member["payload"]["client_order_id"] == dependency["client_order_id"]), None)
        if intent is None or not _group_has_intent(
            {"group_id": dependency["group_id"], "members": payload["request"]["members"]}, dependency,
        ):
            return False
        if dependency["state"] == "REJECTED":
            if row["payload"]["result"]["classification"] != "REJECTED":
                return False
            continue
        if dependency["state"] != "COMMITTED":
            return False
        binding = dependency["binding"]
        owner_after = payload.get("owner_evidence_after")
        if intent is None or owner_after is None:
            return False
        native_orders = owner_after["open_orders_fact"]["immutable_payload"]["rows"]
        if not any(order["order_id"] == binding["order_id"]
                   and order["client_order_id"] == dependency["client_order_id"]
                   and order["product_id"] == dependency["product_id"]
                   and intent["payload"]["strategy_id"] == dependency["strategy_id"]
                   for order in native_orders):
            return False
        if role_rows.get(binding["order_id"]) != dependency["role"]:
            return False
    pending_source_keys = {canonical_bytes(item["key"]) for item in observation["pending_source_groups"]}
    scheduler_source_keys = {canonical_bytes(key) for key in scheduler["pending_keys"]
                              if key["queue_class"] == "SOURCE_GROUP"}
    if pending_source_keys != scheduler_source_keys:
        return False
    pending_delivery = {canonical_bytes(item["key"]) for item in observation["pending_paid_deliveries"]}
    endpoint_delivery = {canonical_bytes(item) for item in endpoint["pending_obligations"]["pending_deliveries"]}
    if pending_delivery != endpoint_delivery:
        return False
    live_deliveries = {canonical_bytes(key) for key in scheduler["pending_keys"] if key["queue_class"] == "DELIVERY"}
    if not pending_delivery <= live_deliveries:
        return False
    # Rebuild paid delivery obligations from native fills and committed cancel
    # effects, then bind each one to the scheduler's actual stable key and its
    # pending, consumed, or terminal public outcome. Observation arrays alone
    # cannot make an omitted notice look settled.
    expected_deliveries = _paid_delivery_origins(journal, request, role_rows, actions_by_group, mode)
    scheduler_deliveries = [row for row in scheduler["records"] if row["kind"] == "DELIVERY"]
    observation_deliveries = observation["pending_paid_deliveries"]
    matched_pending: set[bytes] = set()
    callbacks_by_key: dict[bytes, list[dict[str, Any]]] = {}
    for row in journal:
        if row["record_kind"] == "CALLBACK_RESULT":
            callbacks_by_key.setdefault(canonical_bytes(row["scheduler_key"]), []).append(row)
    terminal_delivery = (observation["terminal_fact"]
                         if observation["terminal_fact"] is not None
                         and observation["terminal_fact"]["kind"] == "DELIVERY" else None)
    for origin in expected_deliveries:
        scheduled = [row for row in scheduler_deliveries
                     if row["key"]["visible_at"] == origin["visible_at"]
                     and row["key"]["schedule_sequence"] == origin["schedule_sequence"]]
        if len(scheduled) != 1:
            return False
        record = scheduled[0]
        key = record["key"]
        if key["queue_class"] != "DELIVERY" or record["stable_id"] != key["stable_id"]:
            return False
        key_bytes = canonical_bytes(key)
        if not origin["paid_obligation"]:
            pending_key = any(canonical_bytes(item) == key_bytes for item in scheduler["pending_keys"])
            callbacks = callbacks_by_key.get(key_bytes, [])
            is_terminal = terminal_delivery is not None and terminal_delivery["key"] == key
            if record["classification"] == "PENDING":
                if not pending_key:
                    return False
            elif record["classification"] == "SUCCESS":
                if pending_key or len(callbacks) != 1 or callbacks[0]["payload"]["outcome"] not in (
                    "SUCCESS", "CONSUMER_DEDUPLICATED",
                ):
                    return False
            elif record["classification"] == "TERMINAL":
                if not is_terminal:
                    return False
            else:
                return False
            continue
        pending = [item for item in observation_deliveries
                   if canonical_bytes(item["key"]) == key_bytes]
        callbacks = callbacks_by_key.get(key_bytes, [])
        is_terminal = terminal_delivery is not None and terminal_delivery["key"] == key
        failed_callbacks = [row for row in callbacks
                            if row["payload"]["outcome"] == "CALLBACK_FAILED"]
        settled_callbacks = [row for row in callbacks if row not in failed_callbacks]
        if len(pending) + len(settled_callbacks) + int(is_terminal) != 1:
            return False
        if pending:
            item = pending[0]
            delivery = item["delivery"]
            if (record["classification"] != "PENDING"
                    or delivery["delivery_id"] != key["stable_id"]
                    or delivery["payload_kind"] != origin["payload_kind"]
                    or delivery["source_fact_id"] != origin["source_fact_id"]
                    or delivery["occurrence_index"] != origin["occurrence_index"]
                    or (origin["payload_kind"] == "EXECUTION_FACT"
                        and (delivery["immutable_payload"]["order_id"] != origin["order_id"]
                             or delivery["immutable_payload"]["product_id"] != origin["product_id"]
                             or delivery["immutable_payload"]["fill_price"] != origin["price"]
                             or Decimal(delivery["immutable_payload"]["cumulative_filled_size_contracts"])
                             != origin["quantity"]
                             or delivery["immutable_payload"]["execution_effective_at"] != origin["effective_at"]))
                    or item["role"] != origin["role"]
                    or item["cancel_action_id"] != origin["cancel_action_id"]):
                return False
            matched_pending.add(key_bytes)
        elif settled_callbacks:
            callback = settled_callbacks[0]["payload"]
            if (record["classification"] != "SUCCESS"
                    or callback["delivery_id"] != key["stable_id"]
                    or callback["outcome"] not in ("SUCCESS", "CONSUMER_DEDUPLICATED")):
                return False
        elif is_terminal:
            terminal = cast(dict[str, Any], terminal_delivery)
            delivery = terminal["delivery"]
            if (record["classification"] != "TERMINAL"
                    or delivery is None or delivery["delivery_id"] != key["stable_id"]
                    or delivery["payload_kind"] != origin["payload_kind"]
                    or delivery["source_fact_id"] != origin["source_fact_id"]
                    or delivery["occurrence_index"] != origin["occurrence_index"]
                    or terminal["role"] != origin["role"]
                    or terminal["cancel_action_id"] != origin["cancel_action_id"]):
                return False
        else:
            return False
    if matched_pending != pending_delivery:
        return False
    expected_polls = [dict(poll_id=row["poll_id"], continuation_id=row["continuation_id"],
                           status=row["status"], awaiting=row["awaiting"])
                      for row in scheduler["polls"] if row["status"] == "IN_PROGRESS" and row["awaiting"] is not None]
    if endpoint["pending_obligations"]["pending_polls"] != expected_polls:
        return False
    pending_action_ids = set(endpoint["pending_obligations"]["pending_actions"])
    expected_pending_actions = {row["action_id"] for row in actions if row["classification"] == "PENDING"}
    working = endpoint["pending_obligations"]["working_orders"]
    if working is not None:
        live_paid_orders = {
            row["order_id"] for row in working
            if role_rows.get(row["order_id"]) == "EXIT"
        }
        expected_pending_actions.update(
            row["action_id"] for row in actions
            if row["role"] == "EXIT" and row["order_id"] in live_paid_orders
        )
    if pending_action_ids != expected_pending_actions:
        return False
    by_action = {row["action_id"]: row for row in actions}
    pending_ack_action_ids = {
        item["cancel_action_id"] for item in observation["pending_paid_deliveries"]
        if item["delivery"]["payload_kind"] == "TRANSPORT_ACK"
        and item["cancel_action_id"] in by_action
        and by_action[item["cancel_action_id"]]["role"] in (
            ("DRAIN_CANCEL", "GROSS_CANCEL") if gross_execution else ("DRAIN_CANCEL",)
        )
        and by_action[item["cancel_action_id"]]["classification"] == "COMMITTED"
        and by_action[item["cancel_action_id"]]["order_id"] is not None
    }
    for order_id in observation["pending_cancel_order_ids"]:
        if role_rows.get(order_id) != "GRID":
            return False
        if not any(row["role"] in (
                    ("DRAIN_CANCEL", "GROSS_CANCEL") if gross_execution else ("DRAIN_CANCEL",)
                ) and row["order_id"] == order_id
                   and (row["classification"] == "PENDING" or row["action_id"] in pending_action_ids
                        or row["action_id"] in pending_ack_action_ids)
                   for row in actions):
            terminal = observation["terminal_fact"]
            if terminal is None or terminal["cancel_action_id"] not in by_action:
                return False
            cancel_action = by_action[terminal["cancel_action_id"]]
            if (cancel_action["role"] not in (
                    ("DRAIN_CANCEL", "GROSS_CANCEL") if gross_execution else ("DRAIN_CANCEL",)
                ) or cancel_action["order_id"] != order_id):
                return False
    terminal = observation["terminal_fact"]
    scheduler_terminal = scheduler["terminal"]
    if terminal is not None:
        if scheduler["gate"] != "FAILED" or scheduler_terminal is None:
            return False
        if terminal["key"] != scheduler["last_popped"]:
            return False
        if (scheduler_terminal["stable_id"] != terminal["key"]["stable_id"]
                or scheduler_terminal["kind"] != terminal["kind"]
                or scheduler_terminal["classification"] != "TERMINAL"):
            return False
        record = next((item for item in scheduler["records"]
                       if item["key"] == terminal["key"] and item["kind"] == terminal["kind"]
                       and item["stable_id"] == terminal["key"]["stable_id"]), None)
        if record is None or record["classification"] != "TERMINAL":
            return False
        if endpoint["failure"] is None or not _same(terminal["failure"], endpoint["failure"]["primary"]):
            return False
        terminal_rows = [row for row in journal if row["scheduler_key"] == terminal["key"]]
        if terminal["kind"] == "DELIVERY":
            if (terminal["delivery"] is None
                    or terminal["delivery"]["delivery_id"] != terminal["key"]["stable_id"]):
                return False
            callback = [row for row in terminal_rows if row["record_kind"] == "CALLBACK_RESULT"]
            delivery = terminal["delivery"]
            snapshot_kinds = {
                "EARN_SNAPSHOT": "EARN", "TRADING_SNAPSHOT": "TRADING",
                "POSITION_SNAPSHOT": "POSITIONS", "OPEN_ORDER_SNAPSHOT": "OPEN_ORDERS",
            }
            if delivery["payload_kind"] in snapshot_kinds:
                if terminal["role"] is not None or terminal["cancel_action_id"] is not None:
                    return False
                facts = [row["payload"] for row in journal
                         if row["record_kind"] == "SNAPSHOT_FACT"
                         and row["payload"]["purpose"] == "POLL"
                         and row["payload"]["fact"]["reference"] == dict(
                             namespace="SNAPSHOT", fact_id=delivery["source_fact_id"],
                         )]
                if len(facts) != 1:
                    return False
                snapshot = facts[0]
                snapshot_request = snapshot["request"]
                fact = snapshot["fact"]
                expected_kind = snapshot_kinds[delivery["payload_kind"]]
                if (delivery["source_namespace"] != "SNAPSHOT"
                        or delivery["account_key"] != attempt["account_key"]
                        or snapshot_request["account_key"] != attempt["account_key"]
                        or snapshot_request["snapshot_id"] != delivery["source_fact_id"]
                        or snapshot_request["snapshot_kind"] != expected_kind
                        or fact["snapshot_kind"] != expected_kind
                        or snapshot_request["captured_at"] != fact["snapshot_as_of"]
                        or delivery["snapshot_as_of"] != fact["snapshot_as_of"]
                        or delivery.get("snapshot_version") != fact.get("captured_account_version")
                        or delivery.get("continuation_id") != snapshot_request.get("continuation_id")
                        or fact.get("continuation_id") != snapshot_request.get("continuation_id")
                        or not _same(delivery["immutable_payload"], fact["immutable_payload"])):
                    return False
                if callback and (len(callback) != 1
                                 or callback[0]["payload"]["delivery_id"] != delivery["delivery_id"]
                                 or callback[0]["payload"]["outcome"] != "CALLBACK_FAILED"
                                 or callback[0]["payload"]["failure"]["reason"] != terminal["failure"]["reason"]):
                    return False
            elif (len(callback) != 1
                  or callback[0]["payload"]["delivery_id"] != delivery["delivery_id"]
                  or callback[0]["payload"]["outcome"] != "CALLBACK_FAILED"
                  or callback[0]["payload"]["failure"]["reason"] != terminal["failure"]["reason"]):
                return False
            if delivery["payload_kind"] == "TRANSPORT_ACK":
                action = by_action.get(terminal["cancel_action_id"])
                if (action is None or action["role"] not in (
                        ("DRAIN_CANCEL", "GROSS_CANCEL") if gross_execution else ("DRAIN_CANCEL",)
                    ) or action["order_id"] is None):
                    return False
                if not any(
                    row["record_kind"] == "SOURCE_GROUP_RESULT"
                    and any(member["kind"] == "CANCEL_EFFECT"
                            and member["stamp"]["event_id"] == delivery["source_fact_id"]
                            and member["payload"]["effects"][0]["target_order_id"] == action["order_id"]
                            for member in row["payload"]["request"]["members"])
                    and row["payload"]["result"]["classification"] == "COMMITTED"
                    for row in journal
                ):
                    return False
            elif delivery["payload_kind"] == "EXECUTION_FACT":
                fills = [fill for row in journal if row["record_kind"] == "HISTORICAL_MARKET_RESULT"
                         for product in row["payload"]["result"]["products"] for fill in product["fills"]
                         if fill["source_event_id"] == delivery["source_fact_id"]
                         and fill["occurrence_index"] == delivery["occurrence_index"]]
                if (len(fills) != 1 or terminal["role"] != role_rows.get(fills[0]["order_id"])):
                    return False
            elif delivery["payload_kind"] in snapshot_kinds:
                pass
            else:
                return False
        else:
            groups_at_terminal = [row for row in terminal_rows if row["record_kind"] == "SOURCE_GROUP_RESULT"]
            if groups_at_terminal:
                if (len(groups_at_terminal) != 1
                        or groups_at_terminal[0]["payload"]["request"] != terminal["source_group"]):
                    return False
                if terminal["group_result"] is not None and not _same(
                    groups_at_terminal[0]["payload"]["result"], terminal["group_result"],
                ):
                    return False
        if terminal["group_result"] is not None:
            matches = [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"
                       and row["payload"]["request"]["group_id"] == terminal["group_result"]["group_id"]]
            if matches and not _same(matches[-1]["payload"]["result"], terminal["group_result"]):
                return False
    elif scheduler_terminal is not None and scheduler_terminal["kind"] in ("SOURCE_GROUP", "DELIVERY"):
        return False
    return True


def _report_projection(report: dict[str, Any], endpoint: dict[str, Any],
                       journal: list[dict[str, Any]], *,
                       floating_request: object | None = None,
                       floating_hash: str | None = None) -> bool:
    expected_refs = [f"journal:{index}" for index in range(1, len(journal) + 1)]
    if report["paid_endpoint"] != endpoint or report["source_evidence_refs"] != expected_refs:
        return False
    if floating_hash is not None:
        observation = report.get("floating_observation")
        if observation is None:
            if type(floating_request) is not SpiderFloatingExitInput:
                return False
            return (
                endpoint["status"] == "FAILED"
                and endpoint["owner_evidence"] is None
                and report["initial_owner_evidence"] is None
                and not journal
            )
        if (type(observation) is not dict
                or observation.get("schema_version") != "spider_floating_exit_observation_v1"
                or observation.get("run_contract_sha256") != floating_hash):
            return False
        if floating_request is None or not _floating_batches_match(
            observation, floating_request, report, journal, endpoint,
        ):
            return False
    return True


def _batch_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError("INVALID_ARTIFACT")
    parsed = Decimal(value)
    if not parsed.is_finite() or canonical_decimal_text(parsed) != value:
        raise ValueError("INVALID_ARTIFACT")
    return parsed


def _batch_wire_value(value: object) -> object:
    if isinstance(value, Decimal):
        return canonical_decimal_text(value)
    if type(value) is dict:
        return {key: _batch_wire_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_batch_wire_value(item) for item in value]
    return value


def _floating_batches_match(observation: dict[str, Any], request: object,
                            report: dict[str, Any], journal: list[dict[str, Any]],
                            endpoint: dict[str, Any]) -> bool:
    """Check captured policy batches against their source sample and saved native facts."""
    try:
        from dataclasses import asdict

        from src.core.backtest.spider_floating_exit_input import SpiderFloatingExitInput

        if type(request) is not SpiderFloatingExitInput:
            return False
        run = request.execution_input.historical_input
        projection = prepare_closed15m_source14(run)
        samples = {sample.cutoff_ms: sample for sample in projection.samples}
        policy_cache = decode_canonical(run.initial_policy_cache)
        if type(policy_cache) is not dict:
            return False
        cache_rows = policy_cache.get("rows")
        if type(cache_rows) is not list:
            return False
        cache_active = {
            row.get("product_id"): row.get("active") == "true"
            for row in cache_rows if type(row) is dict
        }
        if set(cache_active) != set(run.ordered_products):
            return False
        requested_products: set[str] = set()
        terminal_lifecycles = {"LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"}
        prior_terminal_keys = []
        initial = report["initial_owner_evidence"]
        if type(initial) is dict and initial["inspection"]["lifecycle"] in terminal_lifecycles:
            prior_terminal_keys.append((run.range_start_ms * 16, -1, -1, ""))
        for row in journal:
            payload = row["payload"]
            lifecycle = None
            if row["record_kind"] == "HISTORICAL_MARKET_RESULT":
                lifecycle = payload["owner_inspection_after"]["lifecycle"]
            elif row["record_kind"] == "SOURCE_GROUP_RESULT":
                evidence = payload.get("owner_evidence_after")
                if type(evidence) is dict:
                    lifecycle = evidence["inspection"]["lifecycle"]
            if lifecycle in terminal_lifecycles:
                prior_terminal_keys.append(_key(row["scheduler_key"]))

        scheduler = report["scheduler_observation"]
        scheduler_records = scheduler["records"]
        terminal_cache: tuple[int, tuple[int, int, int, str]] | None = None
        terminal = scheduler.get("terminal")
        if (endpoint["status"] == "FAILED" and scheduler["gate"] == "FAILED"
                and type(terminal) is dict and terminal.get("kind") == "HISTORICAL_MARKET_CACHE"):
            matches = [record for record in scheduler_records
                       if record["kind"] == "HISTORICAL_MARKET_CACHE"
                       and record["stable_id"] == terminal.get("stable_id")
                       and record["classification"] == "TERMINAL"]
            if len(matches) == 1:
                terminal_record = matches[0]
                terminal_cache = (
                    int(terminal_record["stable_id"].removeprefix("MARKET_CLOSE_")),
                    _key(terminal_record["key"]),
                )
        expected = []
        for record in scheduler_records:
            if record["kind"] != "HISTORICAL_MARKET_CACHE" or record["classification"] != "SUCCESS":
                continue
            key = _key(record["key"])
            cutoff = int(record["stable_id"].removeprefix("MARKET_CLOSE_"))
            if (cutoff not in samples
                    or not (run.range_start_ms < cutoff < request.execution_input.policy_end_ms)):
                continue
            sample = samples[cutoff]
            if key[0] != sample.visible_at:
                continue
            if any(terminal_key < key for terminal_key in prior_terminal_keys):
                continue
            expected.append((cutoff, key))
        batches = observation["decision_batches"]
        if type(batches) is not list:
            return False
        if (terminal_cache is not None and batches and type(batches[-1]) is dict
                and batches[-1].get("cutoff_ms") == terminal_cache[0]
                and batches[-1].get("visible_at") == terminal_cache[1][0]):
            cutoff, key = terminal_cache
            if (cutoff in samples
                    and run.range_start_ms < cutoff < request.execution_input.policy_end_ms
                    and key[0] == samples[cutoff].visible_at
                    and not any(prior_key < key for prior_key in prior_terminal_keys)):
                expected.append(terminal_cache)
        if len(batches) != len(expected):
            return False

        for batch, (cutoff, key) in zip(batches, expected, strict=True):
            if type(batch) is not dict:
                return False
            sample = samples.get(cutoff)
            if sample is None:
                return False
            if (batch.get("policy_id") != request.decision_policy_id
                    or batch.get("model_id") != request.model_id
                    or batch.get("observation_projection_id") != projection.projection_id
                    or batch.get("cutoff_ms") != cutoff
                    or batch.get("visible_at") != sample.visible_at
                    or batch.get("observed_at") != key[0]
                    or batch.get("observation") != canonical_decimal_text(sample.observation)):
                return False
            if type(batch.get("observed_at")) is not int:
                return False
            account_identity = batch.get("account_identity")
            if type(account_identity) is not list or len(account_identity) != 4:
                return False
            account = dict(venue=account_identity[0], environment=account_identity[1],
                           account=account_identity[2])
            if account_identity[3] is not None:
                account["subaccount"] = account_identity[3]
            rows = batch.get("rows")
            posterior = batch.get("posterior")
            if type(rows) is not list or len(rows) != len(run.ordered_products):
                return False
            if type(posterior) is not list:
                return False
            active = bool(policy_cache["running"] and not policy_cache["paused"])
            for callback in journal:
                if (callback["record_kind"] != "CALLBACK_RESULT"
                        or _key(callback["scheduler_key"]) >= key):
                    continue
                events = callback["payload"].get("policy_events")
                if type(events) is not list:
                    return False
                if any(type(event) is dict and event.get("kind") == "alert"
                       and event.get("reason") == "daily_stop" for event in events):
                    active = False
                    break
            expected_active = tuple(
                product for product in run.ordered_products
                if active and cache_active[product] and product not in requested_products
            )
            cost_rows = []
            active_products = []
            for product_id, row in zip(run.ordered_products, rows, strict=True):
                if type(row) is not dict or row.get("product_id") != product_id:
                    return False
                if type(row.get("active")) is not bool or row["active"] != (product_id in expected_active):
                    return False
                if row["active"]:
                    active_products.append(product_id)
                cost_rows.append({
                    "product_id": product_id,
                    "signed_base_quantity": _batch_decimal(row.get("signed_base_quantity")),
                    "average_open_price": _batch_decimal(row.get("average_open_price")),
                    "estimated_exit_fee_rate": _batch_decimal(row.get("estimated_exit_fee_rate")),
                })
            cost = dict(
                schema_version="spider_source_cost_projection_v1",
                model_id="SOURCE_WEIGHTED_OPEN_COST_V1", account_key=account,
                account_version=batch.get("account_version"),
                valuation_context_id=batch.get("valuation_context_id"),
                lifecycle=batch.get("lifecycle"),
                seed_fee_provenance="MODELED_CONFIGURED_TAKER_RATE_V1", rows=cost_rows,
            )
            expected_batch = decide_floating_exits(
                request.decision_policy_id, sample, cost, active_products, account,
                cast(int, batch["observed_at"]),
            )
            expected_value = asdict(expected_batch)
            expected_value["account_identity"] = list(expected_batch.account_identity)
            expected_value["posterior"] = list(expected_batch.posterior)
            expected_value["rows"] = [dict(asdict(row)) for row in expected_batch.rows]
            expected_value["rows"] = [
                {**row, "product_id": row["product_id"]}
                for row in expected_value["rows"]
            ]
            if not _same(_batch_wire_value(expected_value), batch):
                return False
            requested_products.update(
                row["product_id"] for row in expected_value["rows"]
                if row["active"] and row["reason"] is not None
            )
            market_rows = [row for row in journal
                           if row["record_kind"] == "HISTORICAL_MARKET_RESULT"
                           and _key(row["scheduler_key"]) < key]
            if not market_rows:
                return False
            market = market_rows[-1]["payload"]
            raw_time = market["result"]["raw_time_ms"]
            market_inspection = market["owner_inspection_after"]
            if (market_inspection["account_version"] != batch["account_version"]
                    or market_inspection["account_key"] != account
                    or market_inspection["valuation_context_id"] != batch["valuation_context_id"]
                    or market_inspection["lifecycle"] != batch["lifecycle"]
                    or raw_time != cutoff or raw_time * 16 + 6 != batch["visible_at"]):
                return False

        decision_origins: dict[str, tuple[str, int]] = {}
        batch_products: list[list[str]] = []
        for batch in batches:
            current_products = []
            for row in batch["rows"]:
                if row["active"] and row["reason"] is not None:
                    decision_origins.setdefault(
                        row["product_id"], (row["reason"], batch["observed_at"]),
                    )
                    current_products.append(row["product_id"])
            batch_products.append(current_products)
        exits = observation["product_exits"]
        scheduler_times = {_key(record["key"])[0] for record in scheduler_records}
        if type(exits) is not list:
            return False
        exit_products = [row.get("product_id") for row in exits]
        terminal_batch_present = (
            terminal_cache is not None and batches and type(batches[-1]) is dict
            and batches[-1].get("cutoff_ms") == terminal_cache[0]
            and batches[-1].get("visible_at") == terminal_cache[1][0]
        )
        expected_set = set(decision_origins)
        if terminal_batch_present:
            prior_products = {
                product for products in batch_products[:-1] for product in products
            }
            last_products = batch_products[-1]
            if not prior_products.issubset(exit_products):
                return False
            persisted_last_products = [
                product for product in exit_products if product not in prior_products
            ]
            if persisted_last_products != last_products[:len(persisted_last_products)]:
                return False
            expected_set = prior_products | set(persisted_last_products)
        expected_products = [product for product in run.ordered_products if product in expected_set]
        if exit_products != expected_products:
            return False
        final_owner = endpoint["owner_evidence"]
        for row in exits:
            product_id = row["product_id"]
            reason, requested_at = decision_origins[product_id]
            if row.get("reason") != reason or row.get("requested_at") != requested_at:
                return False
            status = row.get("status")
            completed_at = row.get("completed_at")
            if (completed_at is not None
                    and (completed_at < requested_at or completed_at > scheduler["current_time"]
                         or completed_at not in scheduler_times)):
                return False
            if status == "EXITED":
                if (type(final_owner) is not dict or completed_at is None
                        or completed_at > scheduler["current_time"]
                        or endpoint["status"] == "FAILED"):
                    return False
                positions = final_owner["positions_fact"]["immutable_payload"].get("rows")
                orders = final_owner["open_orders_fact"]["immutable_payload"].get("rows")
                if (type(positions) is not list or type(orders) is not list
                        or any(position.get("product_id") == product_id for position in positions)
                        or any(order.get("product_id") == product_id for order in orders)):
                    return False
            elif status in ("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"):
                if (type(final_owner) is not dict
                        or final_owner["inspection"]["lifecycle"] != status
                        or (completed_at is not None
                            and completed_at > scheduler["current_time"])):
                    return False
            elif status == "FAILED":
                if endpoint["status"] != "FAILED":
                    return False
            elif status == "INCOMPLETE":
                if endpoint["status"] not in ("INCOMPLETE", "FAILED"):
                    return False
            elif status == "EXITING":
                if completed_at is not None or endpoint["status"] == "PAID_FLAT":
                    return False
            else:
                return False
        return True
    except (ArithmeticError, IndexError, KeyError, TypeError, ValueError):
        return False


def _completion_check(completion: dict[str, Any] | None,
                      context: ExecutionContext | FloatingExitContext | GrossExecutionContext,
                      run_id: str, endpoint: dict[str, Any], reconciliation: dict[str, Any],
                      status: dict[str, Any], attempt: dict[str, Any],
                      artifacts: Mapping[str, bytes], *, floating_exit: bool = False,
                      gross_execution: bool = False,
                      execution_context_wire: dict[str, object] | None = None) -> bool:
    if completion is None:
        return True
    if completion["run_id"] != run_id or completion["endpoint_status"] != endpoint["status"]:
        return False
    if (reconciliation["result"] != "OK" or completion["state"] != "COMPLETE"
            or completion["processed_boundary"] != status["processed_boundary"]
            or completion["persisted_boundary"] != status["persisted_boundary"]
            or completion["input_contract_hashes"] != attempt["input_contract_hashes"]
            or completion["planned_coverage"] != attempt["planned_coverage"]
            or completion["configuration_context"] != attempt["configuration_context"]
            or completion["historical_context"] != attempt["historical_context"]
            or completion["evidence_scope"] != reconciliation["evidence_scope"]):
        return False
    if completion["endpoint_state_digest"] != sha256(canonical_bytes(endpoint)).hexdigest():
        return False
    if completion["reconciliation_digest"] != sha256(canonical_bytes(reconciliation)).hexdigest():
        return False
    metadata = completion["artifacts"]
    expected_names = [row[0] for row in (
        _GROSS_COMPLETION_ARTIFACTS if gross_execution else
        _FLOATING_COMPLETION_ARTIFACTS if floating_exit else _COMPLETION_ARTIFACTS
    )]
    if [row["path"] for row in metadata] != expected_names:
        return False
    for row in metadata:
        raw = artifacts[row["path"]]
        row_count = (1 if row["path"] == "execution_input.json" else len(decode_jsonl(raw)))
        if row["sha256"] != sha256(raw).hexdigest() or row["byte_count"] != len(raw) or row["row_count"] != row_count:
            return False
        schema = _GROSS_SCHEMA if gross_execution else _FLOATING_SCHEMA if floating_exit else _SCHEMA
        if row["schema_version"] != schema[row["path"]]:
            return False
    return completion["execution_context"] == (
        execution_context_wire if gross_execution else context._asdict()
    )


def build_execution_reconciliation(run_id: str, artifacts: Mapping[str, bytes]) -> dict[str, Any]:
    """Rebuild the one paid consistency result from canonical persisted bytes.

    Required inputs are the seven reportable artifacts excluding reconciliation
    and completion. Supplying either latter file verifies it against the same
    rebuilt result; omitting them yields the candidate reconciliation for the
    writer's existing finalization sequence.
    """
    required = {"attempt.json", "status.json", "journal.jsonl", "execution_input.json",
                "paid_endpoint.json", "report.jsonl"}
    _require(required <= set(artifacts) <= required | {"reconciliation.json", "completion.json"})
    _require("completion.json" not in artifacts or "reconciliation.json" in artifacts)
    raw_input = artifacts["execution_input.json"]
    encoded_input = decode_canonical(raw_input)
    _require(type(encoded_input) is dict)
    input_schema = cast(dict[str, object], encoded_input).get("schema_version")
    gross_execution = input_schema == "spider_gross_execution_input_v1"
    floating_exit = input_schema == "spider_floating_exit_input_v1"
    execution_context_wire: dict[str, object] | None = None
    gross_request = None
    floating_request = None
    if gross_execution:
        gross_request = decode_spider_gross_execution_input(raw_input)
        request = gross_request.execution_input
        wrapper_hash = validate_spider_gross_execution_input(gross_request)
        execution_context_wire = gross_execution_context_from_input(raw_input)
        context = gross_execution_context(execution_context_wire)
        artifact_schema = _GROSS_SCHEMA
    elif floating_exit:
        floating_request = decode_spider_floating_exit_input(raw_input)
        wrapper_hash = validate_spider_floating_exit_input(floating_request)
        request = floating_request.execution_input
        context = floating_exit_context(floating_exit_context_from_input(raw_input))
        artifact_schema = _FLOATING_SCHEMA
    else:
        request = decode_spider_execution_input(raw_input)
        wrapper_hash = validate_spider_execution_input(request)
        context = execution_context(execution_context_from_input(raw_input))
        artifact_schema = _SCHEMA
    configured_obj = decode_canonical(request.historical_input.configuration_bytes)
    config = cast(dict[str, Any], configured_obj)
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1", config_id=config["config_id"],
        configuration_sha256=request.historical_input.configuration_sha256,
        products=list(request.historical_input.ordered_products),
    ))
    history = historical_context(historical_context_for_input(request.historical_input))
    values = {name: _decode_file(artifacts, name) for name in required - {"journal.jsonl", "execution_input.json"}}
    journal = cast(list[dict[str, Any]], _decode_file(artifacts, "journal.jsonl"))
    attempt = cast(dict[str, Any], values["attempt.json"])
    status = cast(dict[str, Any], values["status.json"])
    endpoint = cast(dict[str, Any], values["paid_endpoint.json"])
    report_rows = cast(list[dict[str, Any]], values["report.jsonl"])
    if len(report_rows) != 1:
        raise ValueError("INVALID_ARTIFACT")
    report = report_rows[0]
    validate_execution_artifact("attempt.json", artifacts["attempt.json"], execution_context=context)
    validate_execution_artifact("status.json", artifacts["status.json"], execution_context=context)
    validate_execution_artifact("journal.jsonl", artifacts["journal.jsonl"], execution_context=context,
                                configuration=configured, historical=history)
    validate_execution_artifact("paid_endpoint.json", artifacts["paid_endpoint.json"], execution_context=context,
                                configuration=configured)
    validate_execution_artifact("report.jsonl", artifacts["report.jsonl"], execution_context=context,
                                configuration=configured)
    if "completion.json" in artifacts:
        validate_execution_artifact("completion.json", artifacts["completion.json"], execution_context=context)
    initial = report["initial_owner_evidence"]
    owner_ok, snapshots_ok = _owner_checks(attempt, initial, endpoint, journal, configured, request, report)
    identity_ok = _input_check(
        run_id, attempt, status, report, endpoint, request, context, configured,
        history._asdict(), contract_hash=wrapper_hash, floating_exit=floating_exit,
        gross_execution=gross_execution, execution_context_wire=execution_context_wire,
    )
    frontier_ok = _journal_check(run_id, attempt, status, journal, report, request, wrapper_hash, endpoint)
    action_ok = _action_bindings(
        attempt, journal, report, endpoint, request, contract_hash=wrapper_hash,
        floating_exit=floating_exit, gross_execution=gross_execution,
    )
    request_identity = dict(run_contract_sha256=context.execution_input_sha256,
                            policy_end_ms=context.policy_end_ms, settlement_end_ms=context.settlement_end_ms,
                            generation=context.generation)
    rebuilt_endpoint = _classify_paid_endpoint_result(
        request_identity, endpoint["owner_evidence"], endpoint["actions"],
        report["scheduler_observation"], report["paid_observation"], endpoint["failure"],
    )
    if gross_execution:
        rebuilt_endpoint["schema_version"] = "spider_gross_paid_endpoint_result_v1"
    endpoint_ok = _same(rebuilt_endpoint, endpoint)
    if gross_execution:
        from src.core.backtest.spider_gross_execution_evidence import gross_report_projection

        assert gross_request is not None
        report_ok = (
            _report_projection(report, endpoint, journal)
            and gross_report_projection(report, endpoint, gross_request, wrapper_hash, journal)
        )
    else:
        report_ok = _report_projection(
            report, endpoint, journal,
            floating_request=floating_request,
            floating_hash=wrapper_hash if floating_exit else None,
        )
    checks = [dict(name=name, result="OK" if ok else "FAILED", evidence_refs=_CHECK_REFS[name])
              for name, ok in zip(_CHECKS, (identity_ok, frontier_ok, owner_ok, snapshots_ok,
                                           action_ok, endpoint_ok, report_ok), strict=True)]
    result = "OK" if all(row["result"] == "OK" for row in checks) else "FAILED"
    scope = "FAILED_PREFIX" if endpoint["status"] == "FAILED" or status["state"] == "FAILED" else "COMPLETE_RUN"
    reconciliation = dict(schema_version=artifact_schema["reconciliation.json"], run_id=run_id,
                          result=result, evidence_scope=scope, checks=checks)
    if "reconciliation.json" in artifacts:
        supplied = cast(dict[str, Any], _decode_file(artifacts, "reconciliation.json"))
        validate_execution_artifact("reconciliation.json", artifacts["reconciliation.json"],
                                    execution_context=context, journal_rows=len(journal))
        _require(_same(supplied, reconciliation))
    if "completion.json" in artifacts:
        completion = cast(dict[str, Any], _decode_file(artifacts, "completion.json"))
        _require(_completion_check(completion, context, run_id, endpoint, reconciliation,
                                   status, attempt, artifacts, floating_exit=floating_exit,
                                   gross_execution=gross_execution,
                                   execution_context_wire=execution_context_wire))
    return reconciliation
