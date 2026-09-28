"""The single pure reconciliation projection/predicate owner; never admission."""

from copy import deepcopy as _copy
from hashlib import sha256 as _sha256
from typing import Any as _Any
from typing import cast as _cast

from src.core.backtest.spider_run_artifacts import canonical_bytes as _bytes
from src.core.backtest.spider_run_completion_schema import report as _validate_report
from src.core.backtest.spider_run_reconciliation_schema import _CHECKS, reconciliation as _validate
from src.core.backtest.spider_scenario_plans import plan_bundle as _plan_bundle

_INITIAL = "artifact:endpoint.json#/initial_owner_evidence"
_FINAL = "artifact:endpoint.json#/final_owner_evidence"


class ReconciliationProjectionError(ValueError):
    """Required evidence cannot be represented by the frozen eleven shapes."""

    reason = "ENDPOINT_RECONCILIATION_FAILED"

    def __init__(self) -> None:
        super().__init__(self.reason)


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


def build_reconciliation(run_id: str, attempt: dict[str, _Any], status: dict[str, _Any],
                         journal: list[dict[str, _Any]], endpoint: dict[str, _Any], report: list[dict[str, _Any]]) -> dict[str, _Any]:
    """Construct all checks from validated detached artifacts, ignoring no content."""
    if type(run_id) is not str or any(row.get("run_id") != run_id for row in [attempt, status, *journal, endpoint, *report]):
        raise ReconciliationProjectionError()
    try:
        frozen = _cast(dict[str, _Any], _plan_bundle(attempt.get("scenario_plan_id")))
    except ValueError as error:
        if error.args == ("UNSUPPORTED_CONFIGURATION",):
            raise ReconciliationProjectionError() from error
        raise
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
    _validate_report(expected_report)
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
    result = _copy(dict(schema_version="spider_reconciliation_v1", run_id=run_id, result="OK" if all(row["result"] == "OK" for row in checks) else "FAILED", checks=checks))
    _validate(result)
    return result
