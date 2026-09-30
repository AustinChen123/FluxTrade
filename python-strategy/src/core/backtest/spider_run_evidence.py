"""The single pure reconciliation projection/predicate owner; never admission."""

from copy import deepcopy as _copy
from hashlib import sha256 as _sha256
from struct import pack as _pack
from typing import Any as _Any
from typing import cast as _cast

from src.core.backtest.spider_run_artifacts import (
    ConfigurationContext as _ConfigurationContext,
    _P3_RUN_CONTRACT,
    _artifact_contexts,
    _configuration_context,
    _historical_context,
    canonical_bytes as _bytes,
)
from src.core.backtest.spider_run_completion_schema import report as _validate_report
from src.core.backtest.spider_run_envelope_schema import (
    endpoint as _validate_endpoint, journal_record as _validate_journal_record,
)
from src.core.backtest.spider_run_reconciliation_schema import _CHECKS, reconciliation as _validate
from src.core.backtest.spider_configured_scale_input import _configured_scale_projection_oracle
from src.core.backtest.spider_scenario_plans import plan_bundle as _plan_bundle

_INITIAL = "artifact:endpoint.json#/initial_owner_evidence"
_FINAL = "artifact:endpoint.json#/final_owner_evidence"
_P1_RUN_CONTRACT = "SPIDER_SYNTHETIC_P1_RUN_V1"
_P2_RUN_CONTRACT = "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1"
_P2_SELECTOR = "SPIDER_P2_CONFIGURED_SCALE_V1"
_P2_PLAN_SHA256 = "8235c952a5d199825a2a77842b03f49e213ef707c0f30b24d8bf623fde53c2b3"


class ReconciliationProjectionError(ValueError):
    """Required evidence cannot be represented by the frozen eleven shapes."""

    reason = "ENDPOINT_RECONCILIATION_FAILED"

    def __init__(self) -> None:
        super().__init__(self.reason)


def _historical_snapshot_matches(owner: dict[str, _Any], cutoff: int,
                                 account_key: dict[str, _Any], account_version: int) -> bool:
    """Bind the final owner snapshots to their frozen request and native payload digests."""
    encoded = bytearray()

    def integer(value: int) -> None:
        encoded.extend(_pack(">q", value))

    def text(value: str) -> None:
        raw = value.encode("utf-8")
        integer(len(raw))
        encoded.extend(raw)

    def optional_text(value: str | None) -> None:
        encoded.append(1 if value is not None else 0)
        if value is not None:
            text(value)

    def decimal(value: str) -> None:
        text(value)

    def request_digest_for(request: dict[str, _Any]) -> str:
        encoded.clear()
        text("SCENARIO_SNAPSHOT_REQUEST_V1")
        text(request["schema_version"])
        account = request["account_key"]
        for field in ("venue", "environment", "account"):
            text(account[field])
        optional_text(account.get("subaccount"))
        text(request["snapshot_id"])
        text(request["snapshot_kind"])
        text(request["capture_mode"])
        optional_text(request.get("fixture_key"))
        integer(request["captured_at"])
        optional_text(request.get("continuation_id"))
        return _sha256(encoded).hexdigest()

    def digest_for(kind: str, request: dict[str, _Any], fact: dict[str, _Any]) -> str:
        encoded.clear()
        text("SCENARIO_SNAPSHOT_PAYLOAD_V1")
        text("SNAPSHOT")
        text(request["snapshot_id"])
        encoded.extend(bytes.fromhex(fact["request_digest"]))
        text(kind)
        version = fact["captured_account_version"]
        encoded.append(1 if version is not None else 0)
        if version is not None:
            integer(version)
        integer(fact["snapshot_as_of"])
        optional_text(fact.get("continuation_id"))
        payload = fact["immutable_payload"]
        text({"TRADING": "TRADING_SNAPSHOT", "POSITIONS": "POSITION_SNAPSHOT",
              "OPEN_ORDERS": "OPEN_ORDER_SNAPSHOT"}[kind])
        text("SUCCESS")
        if kind == "TRADING":
            decimal(payload["equity"])
            decimal(payload["available_equity"])
        else:
            rows = payload["rows"]
            integer(len(rows))
            for row in rows:
                if kind == "POSITIONS":
                    text(row["product_id"])
                    text(row["margin_mode"])
                    decimal(row["position_contracts"])
                    optional_text(row.get("last_price"))
                    optional_text(row.get("notional_usd"))
                else:
                    for field in ("order_id", "client_order_id", "product_id", "state", "side"):
                        text(row[field])
                    for field in ("limit_price", "original_size_contracts", "cumulative_filled_size_contracts"):
                        decimal(row[field])
                    integer(row["created_at"])
        return _sha256(encoded).hexdigest()

    try:
        if owner["cutoff"] != cutoff or owner["inspection"]["account_key"] != account_key:
            return False
        version = owner["inspection"]["account_version"]
        if version != account_version:
            return False
        for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS"):
            name = kind.lower()
            request, fact = owner[name + "_request"], owner[name + "_fact"]
            if (request["account_key"] != account_key or request["snapshot_id"] != fact["reference"]["fact_id"]
                    or request["captured_at"] != cutoff or fact["snapshot_kind"] != kind
                    or fact["snapshot_as_of"] != cutoff or fact["captured_account_version"] != account_version
                    or request_digest_for(request) != fact["request_digest"]
                    or digest_for(kind, request, fact) != fact["payload_digest"]):
                return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def _historical_pending_matches(observation: dict[str, _Any], cutoff: int) -> bool:
    pending = observation["pending_keys"]
    records = [row["key"] for row in observation["records"] if row["classification"] == "PENDING"]
    if len(pending) != len(records) or any(key["visible_at"] <= cutoff for key in pending):
        return False
    by_identity: dict[tuple[str, str], dict[str, _Any]] = {}
    for key in pending:
        identity = key["queue_class"], key["stable_id"]
        if identity in by_identity:
            return False
        by_identity[identity] = key
    seen: set[tuple[str, str]] = set()
    for key in records:
        identity = key["queue_class"], key["stable_id"]
        if identity in seen or identity not in by_identity or key != by_identity[identity]:
            return False
        seen.add(identity)
    return seen == set(by_identity)


def _historical_polls_complete(observation: dict[str, _Any]) -> bool:
    return all(poll["status"] == "COMPLETED" and poll["awaiting"] is None
               for poll in observation["polls"])


def _context_chain(attempt: dict[str, _Any], *artifacts: object) -> _ConfigurationContext | None:
    """Return the attempt's context only when every supplied artifact agrees."""
    contract = attempt["run_contract_id"]
    if contract not in (_P1_RUN_CONTRACT, _P2_RUN_CONTRACT, _P3_RUN_CONTRACT):
        raise ReconciliationProjectionError()
    expected = _artifact_contexts(attempt)
    configuration, historical = expected
    if (contract == _P1_RUN_CONTRACT and expected != (None, None)
            or contract == _P2_RUN_CONTRACT and (configuration is None or historical is not None)
            or contract == _P3_RUN_CONTRACT and (configuration is None or historical is None)):
        raise ReconciliationProjectionError()
    for artifact in artifacts:
        rows = artifact if type(artifact) is list else [artifact]
        for row in rows:
            if type(row) is not dict:
                raise ReconciliationProjectionError()
            supplied = _artifact_contexts(row)
            if supplied != expected:
                raise ReconciliationProjectionError()
    return configuration


def _required(row: dict[str, _Any], key: str) -> _Any:
    if key not in row or row[key] is None:
        raise ReconciliationProjectionError()
    return row[key]


def _equal(left: object, right: object) -> bool:
    return _bytes(left) == _bytes(right)


def _coverage(journal: list[dict[str, _Any]]) -> list[dict[str, _Any]]:
    return [dict(ordinal=row["journal_seq"], barrier_id=row["barrier_id"], record_kind=row["record_kind"]) for row in journal]


def _sites(journal: list[dict[str, _Any]], endpoint: dict[str, _Any]) -> list[tuple]:
    initial = endpoint["initial_owner_evidence"]
    sites = [("INITIAL", _INITIAL, initial, initial["inspection"]["owner_state_digest"])]
    for index, row in enumerate(journal):
        if row["record_kind"] != "SOURCE_GROUP_RESULT":
            continue
        payload = row["payload"]
        for phase in ("before", "after"):
            owner = payload["owner_evidence_" + phase]
            first = payload["result"]["owner_state_digest"] if phase == "after" else owner["inspection"]["owner_state_digest"]
            sites.append((row["barrier_id"] + ":" + phase.upper(),
                          f"artifact:journal.jsonl#/{index}/payload/owner_evidence_{phase}", owner, first))
    final = endpoint["final_owner_evidence"]
    sites.append(("FINAL", _FINAL, final, final["inspection"]["owner_state_digest"]))
    return sites


def _digests(sites: list[tuple]) -> list[dict[str, _Any]]:
    return [dict(barrier_id=stage, expected_owner_sha256=first, observed_owner_sha256=owner["inspection"]["owner_state_digest"])
            for stage, _, owner, first in sites]


def _actions(journal: list[dict[str, _Any]], actions: list[dict[str, _Any]]) -> list[dict[str, _Any]]:
    projected = []
    for action in actions:
        matches = [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"
                   and row["payload"]["result"]["group_id"] == action["group_id"]
                   and _equal(row["payload"]["result"], action["group_result"])] if action["status"] == "SUBMITTED" else []
        projected.append(dict(delivery_id=action["delivery_id"], event_index=action["event_index"], action_index=action["action_index"],
                              group_id=action["group_id"], status=action["status"], cancel_effect_ref=None,
                              group_result_ref=f"journal:{matches[0]['journal_seq']}" if len(matches) == 1 else None))
    return projected


def _terminal(policy: str, endpoint: dict[str, _Any]) -> dict[str, _Any]:
    observation = endpoint["scheduler_observation"]
    inspection = endpoint["final_owner_evidence"]["inspection"]
    return dict(terminal_policy=policy, terminal_reason=endpoint["terminal_reason"], scheduler_gate=observation["gate"],
                scheduler_terminal=observation["terminal"], owner_gate=inspection["gate"], owner_lifecycle=inspection["lifecycle"],
                remaining_planned_barriers=endpoint["remaining_planned_barriers"])


def _report(rows: list[dict[str, _Any]]) -> dict[str, _Any]:
    return dict(report_rows=rows, report_sha256=_sha256(_bytes(rows)).hexdigest())


def _historical_report(run_id: str, attempt: dict[str, _Any], journal: list[dict[str, _Any]],
                       endpoint: dict[str, _Any], context: _ConfigurationContext,
                       historical: _Any) -> list[dict[str, _Any]]:
    final = endpoint["final_owner_evidence"]
    inspection = final["inspection"]
    trading = final["trading_fact"]["immutable_payload"]
    positions = _required(final["positions_fact"]["immutable_payload"], "rows")
    orders = _required(final["open_orders_fact"]["immutable_payload"], "rows")
    rows = []
    for product in context.products:
        position = next((row for row in positions if row["product_id"] == product), None)
        relevant_refs: list[str] = []
        fills: list[str] = []
        for row in journal:
            payload = row["payload"]
            related = False
            if row["record_kind"] == "HISTORICAL_MARKET_RESULT":
                result_product = next((item for item in payload["result"]["products"]
                                       if item["product_id"] == product), None)
                if result_product is not None:
                    related = True
                    fills.extend("SOURCE:" + fill["source_event_id"] for fill in result_product["fills"])
            elif row["record_kind"] == "SOURCE_GROUP_RESULT":
                related = any(member.get("payload", {}).get("product_id") == product
                              for member in payload["request"]["members"])
            if related:
                relevant_refs.append(f"journal:{row['journal_seq']}")
        relevant_refs.append(_FINAL)
        rows.append(dict(
            schema_version="spider_product_report_v1", run_id=run_id, product_id=product,
            terminal_reason=attempt["terminal_policy"],
            position_contracts=_required(position, "position_contracts") if position is not None else "0",
            mark_price=_required(position, "last_price") if position is not None else None,
            notional_usd=_required(position, "notional_usd") if position is not None else "0",
            open_orders=[row for row in orders if row["product_id"] == product],
            committed_execution_refs=fills, source_evidence_refs=relevant_refs,
            account_cash=_required(inspection, "cash"),
            account_equity=_required(trading, "equity"),
            account_available_equity=_required(trading, "available_equity"),
            account_gross_realized=_required(inspection, "gross_realized"),
            account_total_fees=_required(inspection, "total_fees"),
            configuration_context=_configuration_context(context),
            historical_context=_historical_context(historical),
        ))
    return rows


def _historical_endpoint(run_id: str, attempt: dict[str, _Any], status: dict[str, _Any],
                         journal: list[dict[str, _Any]], initial: dict[str, _Any],
                         final: dict[str, _Any], observation: dict[str, _Any]) -> dict[str, _Any]:
    context, historical = _artifact_contexts(attempt)
    if context is None or historical is None or attempt.get("terminal_policy") != "MTM_PRESERVE_OPEN_V1":
        raise ReconciliationProjectionError()
    _context_chain(attempt, status, journal)
    cutoff = _required(observation, "current_time")
    persisted = _required(status, "persisted_boundary")
    processed = _required(status, "processed_boundary")
    if persisted is None or persisted != processed or type(cutoff) is not int:
        raise ReconciliationProjectionError()
    coverage = attempt["planned_coverage"]
    planned = {(item["record_kind"], item["barrier_id"]): item for item in coverage}
    seen: set[tuple[str, str]] = set()
    planned_records = [record for record in observation["records"]
                       if record["kind"] in ("HISTORICAL_MARKET_STEP", "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER")]
    for record in planned_records:
        identity = record["kind"], record["stable_id"]
        if identity not in planned or identity in seen or record["classification"] != "SUCCESS":
            raise ReconciliationProjectionError()
        if record["key"]["visible_at"] > cutoff:
            raise ReconciliationProjectionError()
        seen.add(identity)
    if seen != set(planned):
        raise ReconciliationProjectionError()
    if observation.get("gate") != "RUNNING" or observation.get("terminal") is not None:
        raise ReconciliationProjectionError()
    if not _historical_pending_matches(observation, cutoff) or not _historical_polls_complete(observation):
        raise ReconciliationProjectionError()
    pending = observation["pending_keys"]
    for action in observation["callback_actions"]:
        if action["status"] == "UNSUBMITTED" and not any(
                key["queue_class"] == "DELIVERY" and key["stable_id"] == action["delivery_id"]
                for key in pending):
            raise ReconciliationProjectionError()
    if len(journal) != persisted["journal_seq"] or any(
            row["journal_seq"] != index or row["run_id"] != run_id
            for index, row in enumerate(journal, 1)):
        raise ReconciliationProjectionError()
    for row in journal:
        _validate_journal_record(row)
    final_inspection = initial["inspection"]
    for row in journal:
        payload = row["payload"]
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            final_inspection = payload["owner_evidence_after"]["inspection"]
        elif row["record_kind"] == "HISTORICAL_MARKET_RESULT":
            final_inspection = payload["owner_inspection_after"]
    if final["inspection"] != final_inspection or not _historical_snapshot_matches(
            final, cutoff, attempt["account_key"], final_inspection["account_version"]):
        raise ReconciliationProjectionError()
    endpoint = _copy(dict(
        schema_version="spider_endpoint_v1", run_id=run_id, terminal_reason="MTM_PRESERVE_OPEN_V1",
        cutoff=dict(scheduler_time=cutoff, persisted_boundary=persisted),
        initial_owner_evidence=initial, final_owner_evidence=final,
        scheduler_observation=observation, remaining_planned_barriers=[],
        configuration_context=_configuration_context(context),
        historical_context=_historical_context(historical),
    ))
    _validate_endpoint(endpoint)
    report = _historical_report(run_id, attempt, journal, endpoint, context, historical)
    _validate_report(report, context=context, historical_context=historical)
    return _copy(dict(endpoint=endpoint, report=report,
                      reconciliation=build_reconciliation(run_id, attempt, status, journal, endpoint, report)))


def _historical_reconciliation(run_id: str, attempt: dict[str, _Any], status: dict[str, _Any],
                               journal: list[dict[str, _Any]], endpoint: dict[str, _Any],
                               report: list[dict[str, _Any]], context: _ConfigurationContext,
                               historical: _Any) -> dict[str, _Any]:
    planned = attempt["planned_coverage"]
    observation = endpoint["scheduler_observation"]
    cutoff = endpoint["cutoff"]["scheduler_time"]
    queue_class_order = {name: index for index, name in enumerate((
        "SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
        "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER",
    ))}
    market_records = [row for row in observation["records"]
                      if row["kind"] in ("HISTORICAL_MARKET_STEP", "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER")]
    market_records.sort(key=lambda row: (
        row["key"]["visible_at"], queue_class_order[row["key"]["queue_class"]],
        row["key"]["schedule_sequence"], row["key"]["stable_id"],
    ))
    observed_coverage = [dict(ordinal=index, barrier_id=row["stable_id"], record_kind=row["kind"])
                         for index, row in enumerate(market_records, 1)]
    coverage_ok = (_equal(planned, observed_coverage) and len(market_records) == len(planned)
                   and all(row["classification"] == "SUCCESS" and row["key"]["visible_at"] <= cutoff
                           for row in market_records)
                   and endpoint["remaining_planned_barriers"] == [])
    boundary = (dict(ordinal=journal[-1]["journal_seq"], barrier_id=journal[-1]["barrier_id"],
                     journal_seq=journal[-1]["journal_seq"]) if journal else None)
    processed, persisted = status["processed_boundary"], status["persisted_boundary"]
    frontier_ok = (boundary is not None and processed == persisted == boundary
                   and endpoint["cutoff"]["persisted_boundary"] == persisted)
    observed_barriers = [dict(ordinal=row["journal_seq"], barrier_id=row["barrier_id"],
                              record_kind=row["record_kind"], journal_seq=row["journal_seq"])
                         for row in journal]
    expected_barriers = [dict(ordinal=index, barrier_id=row["barrier_id"],
                              record_kind=row["record_kind"], journal_seq=index)
                         for index, row in enumerate(journal, 1)]
    contiguous = (all(row["journal_seq"] == index for index, row in enumerate(journal, 1))
                  and len({row["barrier_id"] for row in journal}) == len(journal))
    initial, final = endpoint["initial_owner_evidence"], endpoint["final_owner_evidence"]
    sites: list[tuple[str, str, dict[str, _Any]]] = [("INITIAL", _INITIAL, initial)]
    for index, row in enumerate(journal):
        payload = row["payload"]
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            sites.extend((
                (row["barrier_id"] + ":BEFORE", f"artifact:journal.jsonl#/{index}/payload/owner_evidence_before", payload["owner_evidence_before"]),
                (row["barrier_id"] + ":AFTER", f"artifact:journal.jsonl#/{index}/payload/owner_evidence_after", payload["owner_evidence_after"]),
            ))
        elif row["record_kind"] == "HISTORICAL_MARKET_RESULT":
            sites.extend((
                (row["barrier_id"] + ":BEFORE", f"artifact:journal.jsonl#/{index}/payload/owner_inspection_before",
                 dict(inspection=payload["owner_inspection_before"])),
                (row["barrier_id"] + ":AFTER", f"artifact:journal.jsonl#/{index}/payload/owner_inspection_after",
                 dict(inspection=payload["owner_inspection_after"])),
            ))
    sites.append(("FINAL", _FINAL, final))
    account = attempt["account_key"]
    expected_identity = [dict(evidence_ref=ref, account_key=account, profile_id=attempt["profile_id"], config_id=context.config_id)
                         for _, ref, _ in sites]
    observed_identity = [dict(evidence_ref=ref, account_key=owner["inspection"]["account_key"],
                              profile_id=owner["inspection"]["profile_id"], config_id=owner["inspection"]["config_id"])
                         for _, ref, owner in sites]
    identity_ok = all(owner["inspection"]["account_key"] == account
                      and owner["inspection"]["profile_id"] == attempt["profile_id"]
                      and owner["inspection"]["config_id"] == context.config_id
                      for _, _, owner in sites)
    expected_digests: list[dict[str, _Any]] = []
    observed_digests: list[dict[str, _Any]] = []
    initial_digest = initial["inspection"]["owner_state_digest"]
    expected_digests.append(dict(barrier_id="INITIAL", expected_owner_sha256=initial_digest,
                                 observed_owner_sha256=initial_digest))
    observed_digests.append(dict(barrier_id="INITIAL", expected_owner_sha256=initial_digest,
                                 observed_owner_sha256=initial_digest))
    previous_digest = initial_digest
    previous_version = initial["inspection"]["account_version"]
    chain_ok = True
    for row in journal:
        if row["record_kind"] not in ("SOURCE_GROUP_RESULT", "HISTORICAL_MARKET_RESULT"):
            continue
        payload = row["payload"]
        before = (payload["owner_evidence_before"] if row["record_kind"] == "SOURCE_GROUP_RESULT"
                  else payload["owner_inspection_before"])
        after = (payload["owner_evidence_after"] if row["record_kind"] == "SOURCE_GROUP_RESULT"
                 else payload["owner_inspection_after"])
        before_inspection = before["inspection"] if row["record_kind"] == "SOURCE_GROUP_RESULT" else before
        after_inspection = after["inspection"] if row["record_kind"] == "SOURCE_GROUP_RESULT" else after
        before_digest = before_inspection["owner_state_digest"]
        after_digest = after_inspection["owner_state_digest"]
        expected_after = (payload["result"]["owner_state_digest"] if row["record_kind"] == "SOURCE_GROUP_RESULT"
                          else payload["result"]["owner_evidence"]["owner_state_digest"])
        before_stage, after_stage = row["barrier_id"] + ":BEFORE", row["barrier_id"] + ":AFTER"
        expected_digests.extend((
            dict(barrier_id=before_stage, expected_owner_sha256=previous_digest, observed_owner_sha256=previous_digest),
            dict(barrier_id=after_stage, expected_owner_sha256=expected_after, observed_owner_sha256=expected_after),
        ))
        observed_digests.extend((
            dict(barrier_id=before_stage, expected_owner_sha256=previous_digest, observed_owner_sha256=before_digest),
            dict(barrier_id=after_stage, expected_owner_sha256=expected_after, observed_owner_sha256=after_digest),
        ))
        chain_ok = chain_ok and before_digest == previous_digest
        chain_ok = chain_ok and row["account_version_before"] == previous_version == before_inspection["account_version"]
        chain_ok = chain_ok and row["account_version_after"] == after_inspection["account_version"]
        chain_ok = chain_ok and after_digest == expected_after
        previous_digest, previous_version = after_digest, after_inspection["account_version"]
    final_digest = final["inspection"]["owner_state_digest"]
    expected_digests.append(dict(barrier_id="FINAL", expected_owner_sha256=previous_digest,
                                 observed_owner_sha256=previous_digest))
    observed_digests.append(dict(barrier_id="FINAL", expected_owner_sha256=previous_digest,
                                 observed_owner_sha256=final_digest))
    chain_ok = chain_ok and final_digest == previous_digest
    chain_ok = chain_ok and final["inspection"]["account_version"] == previous_version
    expected_final_inspection = initial["inspection"]
    for row in journal:
        payload = row["payload"]
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            expected_final_inspection = payload["owner_evidence_after"]["inspection"]
        elif row["record_kind"] == "HISTORICAL_MARKET_RESULT":
            expected_final_inspection = payload["owner_inspection_after"]
    chain_ok = chain_ok and final["inspection"] == expected_final_inspection
    version = final["inspection"]["account_version"]
    observed_versions = dict(inspection_account_version=version, endpoint_cutoff=cutoff)
    version_ok = endpoint["cutoff"]["persisted_boundary"] == persisted
    version_ok = version_ok and _historical_snapshot_matches(
        final, cutoff, attempt["account_key"], version,
    )
    for kind in ("trading", "positions", "open_orders"):
        fact = final[kind + "_fact"]
        observed_versions[kind] = dict(captured_account_version=fact["captured_account_version"],
                                       snapshot_as_of=fact["snapshot_as_of"])
        version_ok = version_ok and fact["captured_account_version"] == version and fact["snapshot_as_of"] == cutoff
    pending = observation["pending_keys"]
    queue_ok = (_historical_pending_matches(observation, cutoff)
                and endpoint["remaining_planned_barriers"] == [])
    polls = observation["polls"]
    polls_ok = _historical_polls_complete(observation)
    actions = _actions(journal, observation["callback_actions"])
    actions_ok = all(action["status"] != "UNSUBMITTED" or any(
        key["queue_class"] == "DELIVERY" and key["stable_id"] == action["delivery_id"] for key in pending
    ) for action in observation["callback_actions"])
    expected_queue = dict(pending_keys=pending, remaining_planned_barriers=[])
    observed_queue = dict(pending_keys=pending, remaining_planned_barriers=endpoint["remaining_planned_barriers"])
    terminal_expected = dict(terminal_policy="MTM_PRESERVE_OPEN_V1", terminal_reason="MTM_PRESERVE_OPEN_V1",
                             scheduler_gate="RUNNING", scheduler_terminal=None,
                             owner_gate=final["inspection"]["gate"], owner_lifecycle=final["inspection"]["lifecycle"],
                             remaining_planned_barriers=[])
    terminal_observed = _terminal(attempt["terminal_policy"], endpoint)
    expected_report = _historical_report(run_id, attempt, journal, endpoint, context, historical)
    expected = [planned, dict(processed_boundary=boundary, persisted_boundary=boundary,
                              last_planned=planned[-1]), expected_barriers, expected_identity,
                expected_digests, observed_versions, expected_queue, polls, actions,
                terminal_expected, _report(expected_report)]
    observed = [observed_coverage, dict(processed_boundary=processed, persisted_boundary=persisted,
                                       last_planned=planned[-1]), observed_barriers, observed_identity,
                observed_digests, observed_versions, observed_queue, polls, actions,
                terminal_observed, _report(report)]
    extras = [coverage_ok, frontier_ok, contiguous, identity_ok, chain_ok, version_ok, queue_ok,
              polls_ok, actions_ok,
              observation["gate"] == "RUNNING" and observation["terminal"] is None
              and endpoint["terminal_reason"] == attempt["terminal_policy"] == "MTM_PRESERVE_OPEN_V1"
              and final["inspection"]["gate"] == "RUNNING",
              _equal(expected_report, report)]
    refs = [
        ["artifact:attempt.json#/planned_coverage", "artifact:endpoint.json#/scheduler_observation/records"],
        ["artifact:status.json#/processed_boundary", "artifact:status.json#/persisted_boundary", *[f"journal:{row['journal_seq']}" for row in journal]],
        [f"journal:{row['journal_seq']}" for row in journal],
        [_INITIAL, *[f"artifact:journal.jsonl#/{index}/payload" for index, row in enumerate(journal)
                     if row["record_kind"] in ("SOURCE_GROUP_RESULT", "HISTORICAL_MARKET_RESULT")], _FINAL],
        [_INITIAL, *[f"artifact:journal.jsonl#/{index}/payload" for index, row in enumerate(journal)
                     if row["record_kind"] in ("SOURCE_GROUP_RESULT", "HISTORICAL_MARKET_RESULT")], _FINAL],
        [_FINAL],
        ["artifact:endpoint.json#/scheduler_observation/pending_keys", "artifact:endpoint.json#/remaining_planned_barriers"],
        ["artifact:endpoint.json#/scheduler_observation/polls"],
        ["artifact:endpoint.json#/scheduler_observation/callback_actions", *[f"journal:{row['journal_seq']}" for row in journal]],
        ["artifact:attempt.json#/terminal_policy", "artifact:endpoint.json#/terminal_reason", "artifact:endpoint.json#/cutoff",
         "artifact:endpoint.json#/scheduler_observation", _FINAL],
        [_FINAL, *[f"artifact:report.jsonl#/{index}" for index in range(len(report))]],
    ]
    checks = [dict(name=name, expected=wanted, observed=actual, evidence_refs=evidence,
                   result="OK" if valid and _equal(wanted, actual) else "FAILED")
              for name, wanted, actual, evidence, valid in zip(_CHECKS, expected, observed, refs, extras, strict=True)]
    result = _copy(dict(schema_version="spider_reconciliation_v1", run_id=run_id,
                        result="OK" if all(row["result"] == "OK" for row in checks) else "FAILED",
                        checks=checks, configuration_context=_configuration_context(context),
                        historical_context=_historical_context(historical)))
    _validate(result)
    return result


def _configured_order_matches(orders: list[dict[str, _Any]], oracle_rows: list[dict[str, _Any]]) -> bool:
    expected = [row for row in oracle_rows if row["client_order_id"] is not None]
    if len(orders) != len(expected):
        return False
    by_client = {row["client_order_id"]: row for row in orders}
    if len(by_client) != len(orders):
        return False
    return all(
        (actual := by_client.get(item["client_order_id"])) is not None
        and actual["product_id"] == item["product_id"]
        and actual["side"] == "buy"
        and actual["state"] == "live"
        and actual["created_at"] == item["created_at"]
        and actual["limit_price"] == "100"
        and actual["original_size_contracts"] == "1"
        and actual["cumulative_filled_size_contracts"] == "0"
        and type(actual["order_id"]) is str
        and bool(actual["order_id"])
        for item in expected
    )


def _configured_report(run_id: str, terminal: str, context: _ConfigurationContext,
                       final: dict[str, _Any], oracle: dict[str, _Any], *, expected: bool = False) -> list[dict[str, _Any]]:
    inspection = final["inspection"]
    trading = final["trading_fact"]["immutable_payload"]
    positions = _required(final["positions_fact"]["immutable_payload"], "rows")
    orders = _required(final["open_orders_fact"]["immutable_payload"], "rows")
    rows = []
    for template in oracle["products"]:
        product = template["product_id"]
        position = next((row for row in positions if row["product_id"] == product), None)
        refs = [f"journal:{ordinal}" for ordinal in template["evidence_ordinals"]]
        refs.append(_FINAL)
        rows.append(dict(
            schema_version="spider_product_report_v1", run_id=run_id, product_id=product,
            terminal_reason=terminal,
            position_contracts=template["position_contracts"] if expected else _required(position, "position_contracts") if position is not None else "0",
            mark_price=template["mark_price"] if expected else _required(position, "last_price") if position is not None else None,
            notional_usd=template["notional_usd"] if expected else _required(position, "notional_usd") if position is not None else "0",
            open_orders=[row for row in orders if row["product_id"] == product],
            committed_execution_refs=template["committed_execution_refs"],
            source_evidence_refs=refs,
            account_cash=oracle["cash"] if expected else _required(inspection, "cash"),
            account_equity=oracle["equity"] if expected else _required(trading, "equity"),
            account_available_equity=oracle["available_equity"] if expected else _required(trading, "available_equity"),
            account_gross_realized=oracle["gross_realized"] if expected else _required(inspection, "gross_realized"),
            account_total_fees=oracle["total_fees"] if expected else _required(inspection, "total_fees"),
            configuration_context=_configuration_context(context),
        ))
    return rows


def _configured_digest_vectors(journal: list[dict[str, _Any]], endpoint: dict[str, _Any]) -> tuple[list[dict[str, _Any]], list[dict[str, _Any]]]:
    initial = endpoint["initial_owner_evidence"]
    expected, observed = [], []
    previous = initial["inspection"]["owner_state_digest"]
    expected.append(dict(barrier_id="INITIAL", expected_owner_sha256=previous, observed_owner_sha256=previous))
    observed.append(dict(barrier_id="INITIAL", expected_owner_sha256=previous, observed_owner_sha256=previous))
    for row in journal:
        if row["record_kind"] != "SOURCE_GROUP_RESULT":
            continue
        payload = row["payload"]
        before = payload["owner_evidence_before"]["inspection"]["owner_state_digest"]
        after = payload["owner_evidence_after"]["inspection"]["owner_state_digest"]
        result_digest = payload["result"]["owner_state_digest"]
        for phase, wanted, actual in (("BEFORE", previous, before), ("AFTER", result_digest, after)):
            stage = row["barrier_id"] + ":" + phase
            expected.append(dict(barrier_id=stage, expected_owner_sha256=wanted, observed_owner_sha256=wanted))
            observed.append(dict(barrier_id=stage, expected_owner_sha256=wanted, observed_owner_sha256=actual))
        previous = after
    final = endpoint["final_owner_evidence"]["inspection"]["owner_state_digest"]
    expected.append(dict(barrier_id="FINAL", expected_owner_sha256=previous, observed_owner_sha256=previous))
    observed.append(dict(barrier_id="FINAL", expected_owner_sha256=previous, observed_owner_sha256=final))
    return expected, observed


def _configured_reconciliation(run_id: str, attempt: dict[str, _Any], status: dict[str, _Any],
                               journal: list[dict[str, _Any]], endpoint: dict[str, _Any],
                               report: list[dict[str, _Any]], frozen: dict[str, _Any],
                               context: _ConfigurationContext) -> dict[str, _Any]:
    plan = frozen
    oracle = _configured_scale_projection_oracle()
    barriers = plan["planned_barriers"]
    if len(journal) != len(barriers) or len(report) != len(plan["products"]):
        raise ReconciliationProjectionError()
    coverage = [dict(ordinal=row["ordinal"], barrier_id=row["barrier_id"], record_kind=row["record_kind"]) for row in barriers]
    observed_coverage = _coverage(journal)
    last = barriers[-1]
    boundary = dict(ordinal=last["ordinal"], barrier_id=last["barrier_id"], journal_seq=last["ordinal"])
    processed = _required(status, "processed_boundary")
    persisted = _required(status, "persisted_boundary")
    observed_barriers = [dict(ordinal=row["journal_seq"], barrier_id=row["barrier_id"],
                              record_kind=row["record_kind"], journal_seq=row["journal_seq"]) for row in journal]
    expected_barriers = [dict(ordinal=row["ordinal"], barrier_id=row["barrier_id"],
                              record_kind=row["record_kind"], journal_seq=row["ordinal"]) for row in barriers]
    barrier_metadata_matches = all(
        row["journal_seq"] == expected["ordinal"]
        and row["barrier_id"] == expected["barrier_id"]
        and row["record_kind"] == expected["record_kind"]
        and row["scheduler_key"] == expected["scheduler_key"]
        and row["causal_parent_ids"] == expected["causal_parent_ids"]
        for row, expected in zip(journal, barriers, strict=True)
    )
    initial = endpoint["initial_owner_evidence"]
    final = endpoint["final_owner_evidence"]
    sites = [("INITIAL", _INITIAL, initial)]
    for index, row in enumerate(journal):
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            payload = row["payload"]
            sites.extend((
                (row["barrier_id"] + ":BEFORE", f"artifact:journal.jsonl#/{index}/payload/owner_evidence_before", payload["owner_evidence_before"]),
                (row["barrier_id"] + ":AFTER", f"artifact:journal.jsonl#/{index}/payload/owner_evidence_after", payload["owner_evidence_after"]),
            ))
    sites.append(("FINAL", _FINAL, final))
    observed_identity = [dict(evidence_ref=ref, **{key: owner["inspection"][key] for key in ("account_key", "profile_id", "config_id")})
                         for _, ref, owner in sites]
    expected_identity = [dict(evidence_ref=ref, account_key=plan["account_key"], profile_id=plan["native_profile"], config_id=context.config_id)
                         for _, ref, _ in sites]
    expected_digests, observed_digests = _configured_digest_vectors(journal, endpoint)
    observation = endpoint["scheduler_observation"]
    source_rows = [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    expected_records = []
    for step in plan["recipe"]:
        if step["kind"] == "SOURCE_GROUP":
            group = step["group"]
            expected_records.append(dict(kind="SOURCE_GROUP", stable_id=group["group_id"], classification="SUCCESS",
                key=dict(visible_at=step["at"], queue_class="SOURCE_GROUP", schedule_sequence=step["schedule_sequence"], stable_id=group["group_id"])))
    delivery_ids = [row["delivery_id"] for row in plan["callback_plans"]]
    delivery_count = sum(step["kind"] == "DELIVERY" for step in plan["recipe"])
    expected_delivery_ids = delivery_ids[:delivery_count]
    delivery_index = 0
    for step in plan["recipe"]:
        if step["kind"] == "DELIVERY":
            delivery_id = expected_delivery_ids[delivery_index]
            delivery_index += 1
            expected_records.append(dict(kind="DELIVERY", stable_id=delivery_id, classification="SUCCESS",
                key=dict(visible_at=step["projection"]["visible_at"], queue_class="DELIVERY",
                         schedule_sequence=step["projection"]["schedule_sequence"], stable_id=delivery_id)))
    snapshot_sequence = 0
    for step in plan["recipe"]:
        if step["kind"] != "POLL_BEGIN":
            continue
        for name in ("trading", "positions", "open_orders"):
            request = step["plan"][name]["snapshot_request"]
            expected_records.append(dict(kind="SNAPSHOT_CAPTURE", stable_id=request["snapshot_id"], classification="SUCCESS",
                key=dict(visible_at=request["captured_at"], queue_class="SNAPSHOT_CAPTURE", schedule_sequence=snapshot_sequence,
                         stable_id=request["snapshot_id"])))
            delivery_id = delivery_ids[delivery_count + snapshot_sequence]
            expected_records.append(dict(kind="DELIVERY", stable_id=delivery_id, classification="SUCCESS",
                key=dict(visible_at=request["captured_at"], queue_class="DELIVERY", schedule_sequence=snapshot_sequence,
                         stable_id=delivery_id)))
            snapshot_sequence += 1
    queue_order = {"SOURCE_GROUP": 0, "SNAPSHOT_CAPTURE": 1, "DELIVERY": 2}
    expected_records.sort(key=lambda row: (queue_order[row["key"]["queue_class"]], row["key"]["stable_id"]))
    observed_records = observation["records"]
    records_match = len(observed_records) == len(expected_records) and _equal(expected_records, observed_records)
    source_exec_expected = []
    for step in plan["recipe"]:
        if step["kind"] != "SOURCE_GROUP":
            continue
        group = step["group"]
        for member in group["members"]:
            if member["kind"] == "EXECUTION":
                event_id = member["stamp"]["event_id"]
                source_exec_expected.append(dict(group_id=group["group_id"], event_id=event_id, kind="EXECUTION",
                    product_id=member["payload"]["product_id"], committed_references=[dict(namespace="SOURCE", fact_id=event_id)]))
    source_exec_observed = []
    for row in source_rows:
        request, result = row["payload"]["request"], row["payload"]["result"]
        for member in request["members"]:
            if member["kind"] == "EXECUTION":
                source_exec_observed.append(dict(group_id=request["group_id"], event_id=member["stamp"]["event_id"],
                    kind=member["kind"], product_id=member["payload"].get("product_id"),
                    committed_references=result["committed_references"]))
    execution_sources_match = _equal(source_exec_expected, source_exec_observed)
    snapshots_match = attempt["account_key"] == plan["account_key"]
    for _, _, owner in sites:
        snapshots_match = snapshots_match and owner["inspection"]["account_key"] == plan["account_key"]
        for kind in ("trading", "positions", "open_orders"):
            request, fact = owner[kind + "_request"], owner[kind + "_fact"]
            snapshots_match = snapshots_match and request["account_key"] == plan["account_key"]
            snapshots_match = snapshots_match and fact["reference"] == dict(namespace="SNAPSHOT", fact_id=request["snapshot_id"])
            snapshots_match = snapshots_match and fact["snapshot_kind"] == request["snapshot_kind"]
            snapshots_match = snapshots_match and fact["snapshot_as_of"] == request["captured_at"]
    previous_version = initial["inspection"]["account_version"]
    source_versions_match = True
    for row in source_rows:
        payload = row["payload"]
        result = payload["result"]
        before = row["account_version_before"]
        after = row["account_version_after"]
        owner_before = payload["owner_evidence_before"]["inspection"]["account_version"]
        owner_after = payload["owner_evidence_after"]["inspection"]["account_version"]
        source_versions_match = source_versions_match and before == previous_version
        source_versions_match = source_versions_match and result["account_version_before"] == before
        source_versions_match = source_versions_match and result["account_version_after"] == after
        source_versions_match = source_versions_match and owner_before == before and owner_after == after
        previous_version = after
    source_versions_match = source_versions_match and previous_version == final["inspection"]["account_version"]
    cutoff = plan["final_cutoff"]
    expected_versions = dict(inspection_account_version=oracle["account_version"], endpoint_cutoff=cutoff,
                             **{kind: dict(captured_account_version=oracle["account_version"], snapshot_as_of=cutoff)
                                for kind in ("trading", "positions", "open_orders")})
    observed_versions = dict(inspection_account_version=final["inspection"]["account_version"],
                             endpoint_cutoff=endpoint["cutoff"]["scheduler_time"],
                             **{kind: dict(captured_account_version=final[kind + "_fact"]["captured_account_version"],
                                           snapshot_as_of=final[kind + "_fact"]["snapshot_as_of"])
                                for kind in ("trading", "positions", "open_orders")})
    poll_ids = [step["plan"]["trading"]["snapshot_request"]["continuation_id"]
                for step in plan["recipe"] if step["kind"] == "POLL_BEGIN"]
    expected_polls = [dict(poll_id=poll_id, continuation_id=poll_id, status="COMPLETED", awaiting=None) for poll_id in poll_ids]
    expected_queue = dict(pending_keys=[], remaining_planned_barriers=[])
    observed_queue = dict(pending_keys=observation["pending_keys"], remaining_planned_barriers=endpoint["remaining_planned_barriers"])
    expected_terminal = dict(terminal_policy=plan["terminal_policy"], terminal_reason=plan["terminal_policy"],
                             scheduler_gate="RUNNING", scheduler_terminal=None, owner_gate="RUNNING",
                             owner_lifecycle="RISK_STABLE", remaining_planned_barriers=[])
    observed_terminal = _terminal(attempt["terminal_policy"], endpoint)
    expected_report = _configured_report(run_id, plan["terminal_policy"], context, final, oracle, expected=True)
    committed_by_event = {
        member["stamp"]["event_id"]: row["payload"]["result"]["committed_references"]
        for row in source_rows for member in row["payload"]["request"]["members"]
    }
    committed_exist = all(
        {"namespace": "SOURCE", "fact_id": ref.removeprefix("SOURCE:")} in committed_by_event.get(ref.removeprefix("SOURCE:"), [])
        for item in oracle["products"] for ref in item["committed_execution_refs"]
    )
    expected_position_rows = [dict(product_id=item["product_id"], position_contracts=item["position_contracts"],
                                   last_price=item["mark_price"], notional_usd=item["notional_usd"])
                              for item in oracle["products"] if item["mark_price"] is not None]
    actual_positions = final["positions_fact"]["immutable_payload"]["rows"]
    projected_positions = [{key: row[key] for key in ("product_id", "position_contracts", "last_price", "notional_usd")}
                           for row in actual_positions]
    order_rows = final["open_orders_fact"]["immutable_payload"]["rows"]
    expected = [coverage, dict(processed_boundary=boundary, persisted_boundary=boundary, last_planned=coverage[-1]),
                expected_barriers, expected_identity, expected_digests, expected_versions, expected_queue, expected_polls, [],
                expected_terminal, _report(expected_report)]
    observed = [observed_coverage, dict(processed_boundary=processed, persisted_boundary=persisted,
                                        last_planned=attempt["planned_coverage"][-1]),
                observed_barriers, observed_identity, observed_digests, observed_versions, observed_queue, observation["polls"],
                _actions(journal, observation["callback_actions"]), observed_terminal, _report(report)]
    final_inspection = final["inspection"]
    actual_trading = final["trading_fact"]["immutable_payload"]
    oracle_matches = (
        final_inspection["account_version"] == oracle["account_version"]
        and final_inspection["cash"] == oracle["cash"]
        and final_inspection["gross_realized"] == oracle["gross_realized"]
        and final_inspection["total_fees"] == oracle["total_fees"]
        and actual_trading["equity"] == oracle["equity"]
        and actual_trading["available_equity"] == oracle["available_equity"]
        and projected_positions == expected_position_rows
        and _configured_order_matches(order_rows, oracle["products"])
        and committed_exist
    )
    extras = [
        _equal(observed_coverage, attempt["planned_coverage"]) and _equal(observed_coverage, coverage),
        endpoint["cutoff"]["persisted_boundary"] == boundary,
        barrier_metadata_matches,
        all(owner["inspection"]["account_key"] == plan["account_key"]
            and owner["inspection"]["profile_id"] == plan["native_profile"]
            and owner["inspection"]["config_id"] == context.config_id for _, _, owner in sites) and snapshots_match,
        _equal(expected_digests, observed_digests),
        source_versions_match,
        True,
        records_match,
        True,
        (initial["cutoff"] == plan["initial_cutoff"]
         and final["cutoff"] == cutoff
         and observation["current_time"] == cutoff
         and observation["last_popped"] == barriers[-1]["scheduler_key"]),
        oracle_matches and execution_sources_match and _equal(expected_report, report),
    ]
    journal_refs = [f"journal:{row['journal_seq']}" for row in journal]
    source_refs = [ref for ref, row in zip(journal_refs, journal, strict=True) if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    delivery_refs = [ref for ref, row in zip(journal_refs, journal, strict=True)
                     if row["record_kind"] in ("DELIVERY_ATTEMPT", "CALLBACK_RESULT")]
    refs = [["artifact:attempt.json#/planned_coverage", *journal_refs],
            ["artifact:status.json#/processed_boundary", "artifact:status.json#/persisted_boundary", "artifact:attempt.json#/planned_coverage"],
            journal_refs, [_INITIAL, *source_refs, _FINAL], [_INITIAL, *source_refs, _FINAL], [_FINAL],
            ["artifact:endpoint.json#/scheduler_observation/pending_keys", "artifact:endpoint.json#/remaining_planned_barriers"],
            ["artifact:endpoint.json#/scheduler_observation/polls"],
            ["artifact:endpoint.json#/scheduler_observation/callback_actions", *delivery_refs],
            ["artifact:attempt.json#/terminal_policy", "artifact:endpoint.json#/terminal_reason", "artifact:endpoint.json#/cutoff",
             "artifact:endpoint.json#/scheduler_observation", _FINAL],
            [_FINAL, *[f"artifact:report.jsonl#/{index}" for index in range(len(report))]]]
    checks = [dict(name=name, expected=wanted, observed=actual, evidence_refs=references,
                   result="OK" if additional and _equal(wanted, actual) else "FAILED")
              for name, wanted, actual, references, additional in zip(_CHECKS, expected, observed, refs, extras, strict=True)]
    result = _copy(dict(schema_version="spider_reconciliation_v1", run_id=run_id,
                        result="OK" if all(row["result"] == "OK" for row in checks) else "FAILED", checks=checks,
                        configuration_context=_configuration_context(context)))
    _validate(result)
    return result


def build_endpoint_artifacts(run_id: str, attempt: dict[str, _Any], status: dict[str, _Any],
                             journal: list[dict[str, _Any]], initial_owner_evidence: dict[str, _Any],
                             final_owner_evidence: dict[str, _Any], scheduler_observation: dict[str, _Any]) -> dict[str, _Any]:
    """Project actual endpoint evidence and reuse the sole reconciliation predicates."""
    if type(run_id) is not str or any(row.get("run_id") != run_id for row in [attempt, status, *journal]):
        raise ReconciliationProjectionError()
    if attempt.get("run_contract_id") == _P3_RUN_CONTRACT:
        return _historical_endpoint(run_id, attempt, status, journal, initial_owner_evidence,
                                    final_owner_evidence, scheduler_observation)
    try:
        frozen = _cast(dict[str, _Any], _plan_bundle(attempt.get("scenario_plan_id")))
    except ValueError as error:
        if error.args == ("UNSUPPORTED_CONFIGURATION",):
            raise ReconciliationProjectionError() from error
        raise
    configured = frozen.get("schema_version") == "spider_scenario_plan_v2"
    if configured and (attempt.get("scenario_plan_id") != _P2_SELECTOR
                       or _sha256(_bytes(frozen)).hexdigest() != _P2_PLAN_SHA256):
        raise ReconciliationProjectionError()
    context = _context_chain(attempt, status, journal)
    if configured != (context is not None):
        raise ReconciliationProjectionError()
    if configured and (_cast(_ConfigurationContext, context).config_id != frozen["configuration"]["config_id"]
                       or _cast(_ConfigurationContext, context).configuration_sha256 != frozen["configuration_sha256"]
                       or list(_cast(_ConfigurationContext, context).products) != frozen["products"]
                       or attempt.get("scenario_plan_sha256") != _P2_PLAN_SHA256
                       or attempt.get("run_contract_id") != _P2_RUN_CONTRACT):
        raise ReconciliationProjectionError()
    persisted = _required(status, "persisted_boundary")
    terminal = _required(attempt, "terminal_policy")
    endpoint = _copy(dict(schema_version="spider_endpoint_v1", run_id=run_id, terminal_reason=terminal,
                          cutoff=dict(scheduler_time=_required(scheduler_observation, "current_time"), persisted_boundary=persisted),
                          initial_owner_evidence=initial_owner_evidence, final_owner_evidence=final_owner_evidence,
                          scheduler_observation=scheduler_observation,
                          remaining_planned_barriers=attempt["planned_coverage"][persisted["ordinal"]:]))
    if context is not None:
        endpoint["configuration_context"] = _configuration_context(context)
    final = final_owner_evidence
    inspection = final["inspection"]
    trading = final["trading_fact"]["immutable_payload"]
    positions = _required(final["positions_fact"]["immutable_payload"], "rows")
    orders = _required(final["open_orders_fact"]["immutable_payload"], "rows")
    if configured:
        oracle = _configured_scale_projection_oracle()
        report = _configured_report(run_id, terminal, _cast(_ConfigurationContext, context), final, oracle)
    else:
        report = []
        for template in frozen["report"]:
            product = template["product_id"]
            position = next((row for row in positions if row["product_id"] == product), None)
            row = dict(schema_version="spider_product_report_v1", run_id=run_id, product_id=product, terminal_reason=terminal,
                               position_contracts=_required(position, "position_contracts") if position is not None else "0",
                               mark_price=_required(position, "last_price") if position is not None else None,
                               notional_usd=_required(position, "notional_usd") if position is not None else "0",
                               open_orders=[row for row in orders if row["product_id"] == product],
                               committed_execution_refs=template["committed_execution_refs"], source_evidence_refs=template["source_evidence_refs"],
                               account_cash=_required(inspection, "cash"), account_equity=_required(trading, "equity"),
                               account_available_equity=_required(trading, "available_equity"), account_gross_realized=_required(inspection, "gross_realized"),
                               account_total_fees=_required(inspection, "total_fees"))
            if context is not None:
                row["configuration_context"] = _configuration_context(context)
            report.append(row)
    _validate_endpoint(endpoint)
    _validate_report(report, context=context)
    return _copy(dict(endpoint=endpoint, report=report,
                      reconciliation=build_reconciliation(run_id, attempt, status, journal, endpoint, report)))


def build_reconciliation(run_id: str, attempt: dict[str, _Any], status: dict[str, _Any],
                         journal: list[dict[str, _Any]], endpoint: dict[str, _Any], report: list[dict[str, _Any]]) -> dict[str, _Any]:
    """Construct all checks from validated detached artifacts, ignoring no content."""
    if type(run_id) is not str or any(row.get("run_id") != run_id for row in [attempt, status, *journal, endpoint, *report]):
        raise ReconciliationProjectionError()
    if attempt.get("run_contract_id") == _P3_RUN_CONTRACT:
        context, historical = _artifact_contexts(attempt)
        if context is None or historical is None:
            raise ReconciliationProjectionError()
        _context_chain(attempt, status, journal, endpoint, report)
        _validate_endpoint(endpoint)
        _validate_report(report, context=context, historical_context=historical)
        for row in journal:
            _validate_journal_record(row)
        return _historical_reconciliation(run_id, attempt, status, journal, endpoint, report,
                                          context, historical)
    try:
        frozen = _cast(dict[str, _Any], _plan_bundle(attempt.get("scenario_plan_id")))
    except ValueError as error:
        if error.args == ("UNSUPPORTED_CONFIGURATION",):
            raise ReconciliationProjectionError() from error
        raise
    if frozen.get("schema_version") == "spider_scenario_plan_v2":
        if (attempt.get("scenario_plan_id") != _P2_SELECTOR
                or _sha256(_bytes(frozen)).hexdigest() != _P2_PLAN_SHA256):
            raise ReconciliationProjectionError()
        context = _context_chain(attempt, status, journal, endpoint, report)
        if context is None:
            raise ReconciliationProjectionError()
        if (context.config_id != frozen["configuration"]["config_id"]
                or context.configuration_sha256 != frozen["configuration_sha256"]
                or list(context.products) != frozen["products"]
                or attempt.get("scenario_plan_sha256") != _P2_PLAN_SHA256
                or attempt.get("run_contract_id") != _P2_RUN_CONTRACT):
            raise ReconciliationProjectionError()
        return _configured_reconciliation(run_id, attempt, status, journal, endpoint, report, frozen, context)
    context = _context_chain(attempt, status, journal, endpoint, report)
    plan, frozen_journal, frozen_endpoint = frozen["plan"], frozen["journal"], frozen["endpoint"]
    if not journal or not attempt["planned_coverage"]:
        raise ReconciliationProjectionError()
    processed = _required(status, "processed_boundary")
    persisted = _required(status, "persisted_boundary")
    final = endpoint["final_owner_evidence"]
    version = _required(final["inspection"], "account_version")
    cutoff = _required(endpoint["cutoff"], "scheduler_time")
    observed_versions = dict(inspection_account_version=version, endpoint_cutoff=cutoff)
    expected_versions = dict(inspection_account_version=version, endpoint_cutoff=cutoff)
    for kind in ("trading", "positions", "open_orders"):
        fact = final[kind + "_fact"]
        observed_versions[kind] = dict(captured_account_version=_required(fact, "captured_account_version"), snapshot_as_of=_required(fact, "snapshot_as_of"))
        expected_versions[kind] = dict(captured_account_version=version, snapshot_as_of=cutoff)
    expected_report = frozen["report"]
    for row in expected_report:
        row["run_id"] = run_id
        if context is not None:
            row["configuration_context"] = _configuration_context(context)
    _validate_report(expected_report, context=context)
    coverage = plan["planned_coverage"]
    observed_coverage = _coverage(journal)
    last = coverage[-1]
    boundary = dict(ordinal=last["ordinal"], barrier_id=last["barrier_id"], journal_seq=last["ordinal"])
    actual_sites, frozen_sites = _sites(journal, endpoint), _sites(frozen_journal, frozen_endpoint)
    if any(owner["inspection"]["config_id"] != "scenario-v1" for _, _, owner, _ in actual_sites):
        raise ReconciliationProjectionError()
    observed_identity = [dict(evidence_ref=ref, **{key: owner["inspection"][key] for key in ("account_key", "profile_id", "config_id")})
                         for _, ref, owner, _ in actual_sites]
    expected_identity = [dict(evidence_ref=ref, account_key=plan["account_key"], profile_id=plan["native_profile"], config_id="scenario-v1")
                         for _, ref, _, _ in actual_sites]
    observation = endpoint["scheduler_observation"]
    source = [row["payload"] for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    frozen_source = [row["payload"] for row in frozen_journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    dc = [row["payload"] for row in journal if row["record_kind"] in ("DELIVERY_ATTEMPT", "CALLBACK_RESULT")]
    frozen_dc = [row["payload"] for row in frozen_journal if row["record_kind"] in ("DELIVERY_ATTEMPT", "CALLBACK_RESULT")]
    envelopes = [{key: value for key, value in row.items() if key not in ("payload", "run_id")} for row in journal]
    frozen_envelopes = [{key: value for key, value in row.items() if key != "payload"} for row in frozen_journal]
    expected = [coverage, dict(processed_boundary=boundary, persisted_boundary=boundary, last_planned=last),
                [dict(row, journal_seq=row["ordinal"]) for row in coverage], expected_identity, _digests(frozen_sites), expected_versions,
                dict(pending_keys=[], remaining_planned_barriers=[]), frozen_endpoint["scheduler_observation"]["polls"],
                _actions(frozen_journal, frozen_endpoint["scheduler_observation"]["callback_actions"]),
                _terminal(plan["terminal_policy"], frozen_endpoint), _report(expected_report)]
    observed = [observed_coverage, dict(processed_boundary=processed, persisted_boundary=persisted, last_planned=attempt["planned_coverage"][-1]),
                [dict(row, journal_seq=row["ordinal"]) for row in observed_coverage], observed_identity, _digests(actual_sites), observed_versions,
                dict(pending_keys=observation["pending_keys"], remaining_planned_barriers=endpoint["remaining_planned_barriers"]), observation["polls"],
                _actions(journal, observation["callback_actions"]), _terminal(attempt["terminal_policy"], endpoint), _report(report)]
    extra = [
        _equal(observed_coverage, attempt["planned_coverage"]), True, _equal(envelopes, frozen_envelopes),
        _equal(attempt["account_key"], plan["account_key"]) and attempt["profile_id"] == plan["native_profile"],
        _equal(source, frozen_source) and _equal(endpoint["initial_owner_evidence"], frozen_endpoint["initial_owner_evidence"])
        and _equal(final, frozen_endpoint["final_owner_evidence"]), True, True, True, _equal(dc, frozen_dc),
        _equal(endpoint["cutoff"], frozen_endpoint["cutoff"]) and _equal(observation, frozen_endpoint["scheduler_observation"]), True,
    ]
    journal_refs = [f"journal:{row['journal_seq']}" for row in journal]
    source_refs = [ref for ref, row in zip(journal_refs, journal, strict=True) if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    dc_refs = [ref for ref, row in zip(journal_refs, journal, strict=True) if row["record_kind"] in ("DELIVERY_ATTEMPT", "CALLBACK_RESULT")]
    refs = [["artifact:attempt.json#/planned_coverage", *journal_refs],
            ["artifact:status.json#/processed_boundary", "artifact:status.json#/persisted_boundary", "artifact:attempt.json#/planned_coverage"],
            journal_refs, [_INITIAL, *source_refs, _FINAL], [_INITIAL, *source_refs, _FINAL], [_FINAL],
            ["artifact:endpoint.json#/scheduler_observation/pending_keys", "artifact:endpoint.json#/remaining_planned_barriers"],
            ["artifact:endpoint.json#/scheduler_observation/polls"], ["artifact:endpoint.json#/scheduler_observation/callback_actions", *dc_refs],
            ["artifact:attempt.json#/terminal_policy", "artifact:endpoint.json#/terminal_reason", "artifact:endpoint.json#/cutoff", "artifact:endpoint.json#/scheduler_observation", _FINAL],
            [_FINAL, *[f"artifact:report.jsonl#/{index}" for index in range(len(report))]]]
    checks = [dict(name=name, expected=wanted, observed=actual, evidence_refs=references, result="OK" if additional and _equal(wanted, actual) else "FAILED")
              for name, wanted, actual, references, additional in zip(_CHECKS, expected, observed, refs, extra, strict=True)]
    result: dict[str, _Any] = _copy(dict(schema_version="spider_reconciliation_v1", run_id=run_id, result="OK" if all(row["result"] == "OK" for row in checks) else "FAILED", checks=checks))
    if context is not None:
        result["configuration_context"] = _configuration_context(context)
    _validate(result)
    return result
