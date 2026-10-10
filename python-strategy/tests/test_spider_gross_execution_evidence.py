"""Actual-native gross evidence crosses the shared sealed report builder."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_execution_evidence as evidence
from src.core.backtest.spider_gross_execution_input import encode_spider_gross_execution_input
from src.core.backtest.spider_gross_observation import project_gross_observation
from src.core.backtest.spider_gross_response import GrossResponseEpisode
from src.core.backtest.spider_floating_exit_input import encode_spider_floating_exit_input
from src.core.backtest.spider_run_artifacts import (
    _HASH_NAMES, canonical_bytes, configuration_context, decode_canonical,
    decode_jsonl, encode_jsonl, floating_exit_context, historical_context,
)
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_store import SpiderRunStore
from src.core.backtest.spider_run_execution_schema import (
    canonical_gross_paid_bytes, canonical_paid_bytes, gross_execution_context_from_input,
    floating_exit_context_from_input, _paid_endpoint,
)
from src.core.backtest.spider_historical_input import historical_context_for_input
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_gross_response_episode import (
    _initial_account_state, _policy_cache, _with_first_market_price, _with_later_mark,
    _with_limited_settlement_volume, _with_minimums,
)
from test_spider_floating_exit_host import (
    FIXED_REMAINING_COST_TP_V1, _candidate, _seeded_candidate_run,
)
from test_spider_gross_delivery import _dispatch_to_reduction_notice
from test_spider_historical_input import _run_with_configuration
from test_spider_run_execution_evidence import _bundle as _paid_bundle

H = "a" * 64


def _capture(composition: _ReplayComposition, tag: str, cutoff: int | None = None) -> dict[str, Any]:
    cutoff = composition._current_time if cutoff is None else cutoff
    assert cutoff is not None
    requests = [composition._paid_snapshot_request(kind, cutoff, tag)
                for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS")]
    return composition.capture_owner_evidence(cutoff, *requests)["owner_evidence"]


def _actual_gross_bundle(
    request: Any = None, *, fail_callback: bool = False, fail_source: bool = False,
) -> tuple[dict[str, bytes], dict[str, Any]]:
    request = request or _over_cap_gross_input(policy_end_offset_ms=30_000)
    history_input = request.execution_input.historical_input
    run_id = history_input.run_id
    composition = _ReplayComposition._from_gross_response_run(request)
    events: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    captures: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    counter = 0
    native_codec = composition._codec

    class CaptureOwnerChain:
        def __getattr__(self, name: str) -> Any:
            return getattr(native_codec, name)

        def apply_gross_response_group(self, group: dict[str, Any], *, restricted: bool) -> Any:
            nonlocal counter
            counter += 1
            cutoff = group["group_effective_at"]
            before = _capture(composition, f"GROSS-EVIDENCE-BEFORE-{counter}", cutoff)
            if fail_source:
                from src.core.backtest.synthetic_scenario_replay import _ScheduleError
                raise _ScheduleError("EXECUTION_FAILURE")
            result = native_codec.apply_gross_response_group(cast(Any, group), restricted=restricted)
            after = _capture(composition, f"GROSS-EVIDENCE-AFTER-{counter}", cutoff)
            captures[group["group_id"]] = before, after
            return result

    cast(Any, composition)._codec = CaptureOwnerChain()
    composition._evidence_callback = lambda kind, key, payload: events.append((kind, key, payload))
    initial = _capture(composition, "GROSS-EVIDENCE-INITIAL")
    if fail_callback:
        notice = _dispatch_to_reduction_notice(composition, composition._paid_endpoint)
        delivery_id = notice["item"]["delivery"]["delivery_id"]
        original_capture = cast(Any, composition)._capture_callback

        def fail_notice(candidate: Any) -> Any:
            if candidate["delivery_id"] == delivery_id:
                from src.core.backtest.synthetic_scenario_replay import _CallbackPrefix
                prefix = original_capture(candidate)
                return _CallbackPrefix(prefix.events, RuntimeError("injected gross callback failure"))
            return original_capture(candidate)

        cast(Any, composition)._capture_callback = fail_notice
        dispatch = composition._dispatch_due(notice["key"][0])
        assert dispatch["classification"] == "TERMINAL"
    else:
        dispatch = composition._dispatch_due(history_input.range_end_ms * 16 + 6)
        assert dispatch["classification"] == ("TERMINAL" if fail_source else "SUCCESS"), (
            dispatch, composition._terminal,
        )
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    paid_private = endpoint.result(composition)
    paid_endpoint = deepcopy(paid_private)
    paid_endpoint["schema_version"] = "spider_gross_paid_endpoint_result_v1"
    paid_endpoint.pop("gross_response")
    scheduler = composition._public_scheduler_observation()
    paid_observation = endpoint._paid_observation(
        composition, scheduler,
        current_evidence_complete=paid_private["failure"] is None
        or paid_private["failure"]["evidence_error"] is None,
        failure=paid_private["failure"],
    )
    paid_observation["schema_version"] = "spider_gross_paid_observation_v1"
    final_facts = {
        "operations": GrossResponseEpisode._proposal_context(endpoint, composition)["operations"],
        "native_state": native_codec.gross_response_projection(),
        "current_time": composition._current_time,
        "last_popped": composition._last_popped,
    }
    gross_report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection=paid_private["gross_response"],
        settlement_key={"visible_at": composition._current_time},
        final_facts=final_facts,
        include_cut_evidence=True,
    )
    assert gross_report is not None

    raw_input = encode_spider_gross_execution_input(request)
    context = gross_execution_context_from_input(raw_input)
    config = cast(dict[str, Any], decode_canonical(history_input.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1", config_id=config["config_id"],
        configuration_sha256=history_input.configuration_sha256,
        products=list(history_input.ordered_products),
    ))
    history = historical_context(historical_context_for_input(history_input))
    rows = []
    for kind, key, payload in events:
        if kind == "DELIVERY_ATTEMPT":
            continue
        payload = deepcopy(payload)
        if kind == "SOURCE_GROUP_RESULT":
            group_id = payload["request"]["group_id"]
            if group_id in captures:
                payload["owner_evidence_before"], payload["owner_evidence_after"] = captures[group_id]
            result = payload["result"]
            before, after, effective = (
                result["account_version_before"], result["account_version_after"],
                payload["request"]["group_effective_at"],
            )
        elif kind == "HISTORICAL_MARKET_RESULT":
            before = payload["owner_inspection_before"]["account_version"]
            after = payload["owner_inspection_after"]["account_version"]
            effective = payload["result"]["effective_at"]
        elif kind == "SNAPSHOT_FACT":
            before = after = None
            effective = payload["request"]["captured_at"]
        else:
            before = after = None
            effective = key["visible_at"]
        sequence = len(rows) + 1
        rows.append(dict(
            schema_version="spider_gross_execution_journal_record_v1", run_id=run_id,
            journal_seq=sequence, barrier_id=f"{kind}:{key['stable_id']}:{sequence}",
            record_kind=kind, scheduler_key=key, causal_parent_ids=[],
            effective_at=effective, visible_at=key["visible_at"],
            account_version_before=before, account_version_after=after, payload=payload,
            configuration_context=configured._asdict() | {"products": list(configured.products)},
            historical_context=history._asdict(), execution_context=context,
        ))
    rows = decode_jsonl(encode_jsonl(cast(list[object], rows)))
    wrapper_hash = evidence.validate_spider_gross_execution_input(request)
    attempt = dict(
        schema_version="spider_gross_execution_attempt_v1", run_id=run_id,
        run_contract_id="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1", registration_state="VALIDATED",
        requested_scenario_selector="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1", registration_failure=None,
        input_contract_hashes=[dict(name=name, sha256=(
            wrapper_hash if index == 0 else history_input.policy_source_sha256 if index == 3 else H
        )) for index, name in enumerate(_HASH_NAMES)],
        planned_coverage=evidence._expected_coverage(request.execution_input, wrapper_hash),
        profile_id="SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1",
        account_key=dict(venue="SPIDER_HISTORICAL_RESEARCH", environment="RESEARCH_ONLY",
                         account=history_input.account_key),
        scenario_plan_id="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1", scenario_plan_sha256=wrapper_hash,
        program_sha256=H, native_artifact_sha256=H,
        policy_source_sha256=history_input.policy_source_sha256,
        ordering_contract_id="HISTORICAL_ORDER_V1", cost_contract_id="SPIDER_HISTORICAL_RESEARCH_COSTS_V1",
        funding_exclusion="HISTORICAL_FUNDING_DISABLED", terminal_policy="PAID_CLOSE_RESEARCH_V1",
        artifact_encoding="artifact_encoding_v1",
        configuration_context=configured._asdict() | {"products": list(configured.products)},
        historical_context=history._asdict(), execution_context=context,
    )
    frontier = dict(ordinal=len(rows), journal_seq=len(rows), barrier_id=rows[-1]["barrier_id"])
    primary = paid_private["failure"]["primary"] if paid_private["status"] == "FAILED" else None
    failure_reasons = {
        "OWNER_EVIDENCE": "UNEXPECTED_EXCEPTION", "SCHEDULER": "SCHEDULER_FAILED",
        "SOURCE_GROUP": "SCHEDULER_FAILED",
        "CALLBACK": "CALLBACK_FAILED", "DELIVERY": "CALLBACK_FAILED",
        "NATIVE": "NATIVE_FAULT", "PERSISTENCE": "PERSISTENCE_FAILED",
    }
    status_failure_reason = failure_reasons.get(primary["kind"], primary["reason"]) if primary else None
    status_primary = (
        None if primary is None or primary["kind"] == "OWNER_EVIDENCE" else {
            "kind": ("CALLBACK" if primary["kind"] == "DELIVERY" else
                     "SCHEDULER" if primary["kind"] == "SOURCE_GROUP" else primary["kind"]),
            "reason": primary["reason"],
        }
    )
    status = dict(
        schema_version="spider_gross_execution_status_v1", run_id=run_id,
        state="FAILED" if paid_private["status"] == "FAILED" else "COMPLETE",
        processed_boundary=frontier, persisted_boundary=frontier,
        failure_reason=status_failure_reason, primary_failure=status_primary,
        configuration_context=attempt["configuration_context"],
        historical_context=attempt["historical_context"], execution_context=context,
    )
    report = dict(
        schema_version="spider_gross_execution_report_v1", run_id=run_id,
        initial_owner_evidence=initial, scheduler_observation=scheduler,
        paid_observation=paid_observation, paid_endpoint=paid_endpoint,
        source_evidence_refs=[f"journal:{index}" for index in range(1, len(rows) + 1)],
        gross_observation=gross_report,
    )
    artifacts = {
        "attempt.json": encode_jsonl([attempt]), "status.json": encode_jsonl([status]),
        "journal.jsonl": encode_jsonl(cast(list[object], rows)), "execution_input.json": raw_input,
        "paid_endpoint.json": canonical_gross_paid_bytes(paid_endpoint, context, configured),
        "report.jsonl": encode_jsonl(cast(list[object], [report])),
    }
    return artifacts, dict(run_id=run_id, request=request, endpoint=paid_endpoint,
                           report=report, journal=rows, attempt=attempt, raw_input=raw_input,
                           composition=composition)


def _native_terminal_request(cash: str) -> Any:
    request = _over_cap_gross_input()
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    positions = [dict(
        product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
        lots=[dict(seed_execution_id="E3B2-TERMINAL-SEED", seed_sequence=0,
                   strategy_id=history.strategy_identity, quantity_contracts="1",
                   entry_price="100")],
    )]
    configuration.update(cash=cash, positions=positions, orders=[])
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(cash=cash, positions=positions, orders=[]),
        initial_policy_cache=_policy_cache(running=False),
    )
    request = replace(request, execution_input=replace(execution, historical_input=history))
    return _with_first_market_price(request, "A-USDT-SWAP", "1")


def _store_and_admit(tmp_path: Any, artifacts: dict[str, bytes], facts: dict[str, Any]) -> dict[str, Any]:
    store = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    assert store.register(facts["attempt"], gross_execution_input=facts["raw_input"])
    for row in facts["journal"]:
        boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"],
                        barrier_id=row["barrier_id"])
        store.mark_processed(boundary, record_kind=row["record_kind"])
        store.append_journal(row)
    reconciliation = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert reconciliation["result"] == "OK", [
        (
            [row["name"] for row in reconciliation["checks"] if row["result"] != "OK"],
            facts["report"]["gross_observation"]["current_state"],
            facts["report"]["gross_observation"]["reason"],
            facts["report"]["gross_observation"]["episodes"][-1]["state"]
            if facts["report"]["gross_observation"]["episodes"] else None,
            facts["report"]["gross_observation"]["episodes"][-1]["reason"]
            if facts["report"]["gross_observation"]["episodes"] else None,
            facts["report"]["gross_observation"]["final_obligation_state"][
                "operation_state"]["gross_response"],
        )
    ]
    assert store.finalize(facts["endpoint"], reconciliation, [facts["report"]]) == "COMPLETE_PUBLISHED"
    admitted = admit_spider_run(tmp_path / facts["run_id"])
    assert admitted["decision"] == "ACCEPT"
    return admitted


def _actual_ownerless_failure_bundle() -> tuple[dict[str, bytes], dict[str, Any]]:
    request = _over_cap_gross_input()
    template, facts = _actual_gross_bundle(request)
    composition = _ReplayComposition._from_gross_response_run(request)

    def fail_initial_capture(*_args: Any) -> Any:
        raise RuntimeError("injected gross initial owner capture failure")

    cast(Any, composition).capture_owner_evidence = fail_initial_capture
    endpoint_owner = composition._paid_endpoint
    assert endpoint_owner is not None
    paid_private = endpoint_owner.result(composition)
    assert paid_private["status"] == "FAILED" and paid_private["owner_evidence"] is None
    scheduler = composition._public_scheduler_observation()
    paid_observation = endpoint_owner._paid_observation(
        composition, scheduler, current_evidence_complete=False, failure=paid_private["failure"],
    )
    paid_observation["schema_version"] = "spider_gross_paid_observation_v1"
    paid_endpoint = deepcopy(paid_private)
    paid_endpoint["schema_version"] = "spider_gross_paid_endpoint_result_v1"
    paid_endpoint.pop("gross_response")
    report = dict(
        schema_version="spider_gross_execution_report_v1", run_id=facts["run_id"],
        initial_owner_evidence=None, scheduler_observation=scheduler,
        paid_observation=paid_observation, paid_endpoint=paid_endpoint,
        source_evidence_refs=[], gross_observation=None,
    )
    attempt = facts["attempt"]
    failure = paid_private["failure"]["primary"]
    failure_reason = (
        "UNEXPECTED_EXCEPTION" if failure["kind"] == "OWNER_EVIDENCE" else failure["reason"]
    )
    status = dict(
        schema_version="spider_gross_execution_status_v1", run_id=facts["run_id"],
        state="FAILED", processed_boundary=None, persisted_boundary=None,
        failure_reason=failure_reason, primary_failure=None,
        configuration_context=attempt["configuration_context"],
        historical_context=attempt["historical_context"],
        execution_context=attempt["execution_context"],
    )
    raw = facts["raw_input"]
    context = gross_execution_context_from_input(raw)
    configuration = configuration_context(attempt["configuration_context"])
    artifacts = {
        "attempt.json": encode_jsonl([attempt]), "status.json": encode_jsonl([status]),
        "journal.jsonl": b"", "execution_input.json": raw,
        "paid_endpoint.json": canonical_gross_paid_bytes(paid_endpoint, context, configuration),
        "report.jsonl": encode_jsonl([report]),
    }
    return artifacts, facts | dict(
        endpoint=paid_endpoint, report=report, journal=[], composition=composition,
        raw_input=raw,
    )


def test_actual_native_gross_bundle_rebuilds_all_shared_checks_with_v2_cut_facts():
    artifacts, facts = _actual_gross_bundle()
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = [row["name"] for row in result["checks"] if row["result"] != "OK"]
    assert failed == []
    assert result["result"] == "OK"
    assert result["evidence_scope"] == "COMPLETE_RUN"

    from src.core.backtest.spider_gross_artifact_schema import gross_observation
    gross_observation(decode_canonical(canonical_bytes(facts["report"]["gross_observation"])),
                      expected_hash=cast(str, facts["attempt"]["execution_context"]["execution_input_sha256"]))
    positive_ratio_null = deepcopy(facts["report"]["gross_observation"])
    positive_native = positive_ratio_null["final_obligation_state"]["native_state"]
    assert Decimal(positive_native["gross_admission"]["effective_equity"]) > 0
    positive_native["gross_admission"]["gross_ratio"] = None
    with pytest.raises(ValueError):
        gross_observation(positive_ratio_null)
    for equity, ratio, accepted in (
        ("0", None, True), ("-1", None, True),
        ("0", {"numerator": "0", "denominator": "1"}, False),
    ):
        boundary = deepcopy(facts["report"]["gross_observation"])
        boundary_native = boundary["final_obligation_state"]["native_state"]
        boundary_native["gross_admission"]["effective_equity"] = equity
        boundary_native["gross_admission"]["gross_ratio"] = ratio
        if accepted:
            gross_observation(decode_canonical(canonical_bytes(boundary)))
        else:
            with pytest.raises(ValueError):
                gross_observation(decode_canonical(canonical_bytes(boundary)))
    pending_cut = next(
        row for row in facts["report"]["gross_observation"]["observations"]
        if row["operation_state"] is not None
        and row["operation_state"]["pending_delivery_ids"]
        and row["key"] is not None
        and any(row["key"] == journal_row["scheduler_key"] for journal_row in facts["journal"])
    )
    malformed_roots = []
    missing_final = deepcopy(facts["report"]["gross_observation"])
    missing_final.pop("final_obligation_state")
    malformed_roots.append(missing_final)
    extra_final = deepcopy(facts["report"]["gross_observation"])
    extra_final["unexpected"] = None
    malformed_roots.append(extra_final)
    missing_operation = deepcopy(facts["report"]["gross_observation"])
    missing_operation["observations"][0].pop("operation_state")
    malformed_roots.append(missing_operation)
    extra_operation = deepcopy(facts["report"]["gross_observation"])
    selected_operation = next(
        row for row in extra_operation["observations"] if row["operation_state"] is not None
    )
    selected_operation["operation_state"]["unexpected"] = None
    malformed_roots.append(extra_operation)
    for malformed in malformed_roots:
        with pytest.raises(ValueError):
            gross_observation(decode_canonical(canonical_bytes(malformed)))
    false_completion = deepcopy(facts["report"])
    false_completion["gross_observation"]["episodes"][0]["rounds"][0][
        "completion_key"
    ] = pending_cut["key"]
    changed = deepcopy(artifacts)
    changed["report.jsonl"] = encode_jsonl(cast(list[object], [false_completion]))
    rejected = evidence.build_execution_reconciliation(facts["run_id"], changed)
    assert rejected["result"] == "FAILED"
    assert next(row for row in rejected["checks"] if row["name"] == "REPORT_PROJECTION")[
        "result"
    ] == "FAILED"

    def assert_projection_rejects(mutated: dict[str, Any]) -> None:
        changed_artifacts = deepcopy(artifacts)
        changed_artifacts["report.jsonl"] = encode_jsonl(cast(list[object], [mutated]))
        check = evidence.build_execution_reconciliation(facts["run_id"], changed_artifacts)
        assert next(row for row in check["checks"] if row["name"] == "REPORT_PROJECTION")[
            "result"
        ] == "FAILED"

    omitted_pending_snapshot = deepcopy(facts["report"])
    omitted_cut = next(
        row for row in omitted_pending_snapshot["gross_observation"]["observations"]
        if row["operation_state"] is not None
        and row["operation_state"]["pending_delivery_ids"]
        and row["episode_sequence"] is not None
    )
    cut_episode = next(
        episode for episode in omitted_pending_snapshot["gross_observation"]["episodes"]
        if episode["sequence"] == omitted_cut["episode_sequence"]
    )
    cut_round = next(round_row for round_row in cut_episode["rounds"]
                     if round_row["action_ids"])
    cut_round["completion_key"] = omitted_cut["key"]
    omitted_pending_operation = omitted_cut["operation_state"]
    omitted_pending_operation["pending_delivery_ids"] = []
    omitted_pending_operation["pending_deliveries"] = []
    assert_projection_rejects(omitted_pending_snapshot)

    false_current = deepcopy(facts["report"])
    false_current["gross_observation"]["current_state"] = "RECOVERED"
    assert_projection_rejects(false_current)
    false_current["gross_observation"]["current_state"] = "IDLE"
    assert_projection_rejects(false_current)

    stale_final_time = deepcopy(facts["report"])
    stale_final_time["gross_observation"]["final_obligation_state"]["observed_at"] += 1
    assert_projection_rejects(stale_final_time)

    mixed_final_version = deepcopy(facts["report"])
    mixed_final_version["gross_observation"]["final_obligation_state"][
        "native_state"
    ]["account_version"] += 1
    assert_projection_rejects(mixed_final_version)

    unanchored_observation = deepcopy(facts["report"])
    non_proposal = next(
        row for row in unanchored_observation["gross_observation"]["observations"]
        if not any(round_row["proposal_key"] == row["key"]
                   for episode in unanchored_observation["gross_observation"]["episodes"]
                   for round_row in episode["rounds"])
    )
    non_proposal["native_state"]["owner_state_digest"] = "0" * 64
    assert_projection_rejects(unanchored_observation)

    omitted_pending_id = deepcopy(facts["report"])
    pending_observation = next(
        row for row in omitted_pending_id["gross_observation"]["observations"]
        if row["operation_state"] is not None
        and row["operation_state"]["pending_delivery_ids"]
    )
    pending_observation["operation_state"]["pending_delivery_ids"] = []
    assert_projection_rejects(omitted_pending_id)

    changed = deepcopy(artifacts)
    changed_report = deepcopy(facts["report"])
    changed_observation = changed_report["gross_observation"]
    changed_peak = dict(changed_observation["peak_ratio"])
    changed_peak["numerator"] += Decimal(1)
    changed_observation["peak_ratio"] = changed_peak
    changed["report.jsonl"] = encode_jsonl([changed_report])
    rejected = evidence.build_execution_reconciliation(facts["run_id"], changed)
    assert rejected["result"] == "FAILED"


def test_actual_flat_observation_rows_cannot_be_deleted():
    artifacts, facts = _actual_gross_bundle(_native_terminal_request("99.003"))
    assert facts["report"]["gross_observation"]["current_state"] == "LIQUIDATED_FLAT"
    assert facts["report"]["gross_observation"]["observations"]
    forged = deepcopy(facts["report"])
    forged["gross_observation"]["observations"] = []
    changed = deepcopy(artifacts)
    changed["report.jsonl"] = encode_jsonl([forged])
    result = evidence.build_execution_reconciliation(facts["run_id"], changed)
    assert next(row for row in result["checks"] if row["name"] == "REPORT_PROJECTION")[
        "result"
    ] == "FAILED"


def test_actual_native_recovery_rebreach_bundle_rebuilds_v2_barriers():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000, policy_end_offset_ms=150_000,
    )
    request = _with_later_mark(_with_minimums(request, {"B-USDT-SWAP": "0.8"}), "37.5")
    artifacts, facts = _actual_gross_bundle(request)
    observation = facts["report"]["gross_observation"]
    assert len(observation["episodes"]) == 2
    assert observation["episodes"][0]["recovered_key"] is not None
    assert observation["observations"]
    assert all(row["operation_state"] is None or type(row["operation_state"]) is dict
               for row in observation["observations"])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["result"] == "OK"
    false_recovery = deepcopy(facts["report"])
    first = false_recovery["gross_observation"]["episodes"][0]
    non_recovery = next(
        row for row in false_recovery["gross_observation"]["observations"]
        if row["episode_sequence"] == first["sequence"]
        and row["after_state"] != "RECOVERED"
        and row["key"] is not None
    )
    first["recovered_key"] = non_recovery["key"]
    mutated_artifacts = deepcopy(artifacts)
    mutated_artifacts["report.jsonl"] = encode_jsonl(cast(list[object], [false_recovery]))
    rejected = evidence.build_execution_reconciliation(facts["run_id"], mutated_artifacts)
    assert next(row for row in rejected["checks"] if row["name"] == "REPORT_PROJECTION")[
        "result"
    ] == "FAILED"
    forged_round_completion = deepcopy(facts["report"])
    second = forged_round_completion["gross_observation"]["episodes"][1]
    second["rounds"][0]["completion_key"] = second["rounds"][0]["proposal_key"]
    mutated_artifacts = deepcopy(artifacts)
    mutated_artifacts["report.jsonl"] = encode_jsonl(cast(list[object], [forged_round_completion]))
    rejected = evidence.build_execution_reconciliation(facts["run_id"], mutated_artifacts)
    assert next(row for row in rejected["checks"] if row["name"] == "REPORT_PROJECTION")[
        "result"
    ] == "FAILED"


def test_actual_native_gross_cancel_effect_bundle_rebuilds_and_admits(tmp_path: Any):
    request = _over_cap_gross_input(with_working_order=True, policy_end_offset_ms=30_000)
    artifacts, facts = _actual_gross_bundle(request)
    assert any(
        row["record_kind"] == "SOURCE_GROUP_RESULT"
        and row["payload"]["request"]["members"][0]["kind"] == "CANCEL_EFFECT"
        for row in facts["journal"]
    )
    _store_and_admit(tmp_path, artifacts, facts)


def test_pending_execution_roles_are_anchored_at_the_same_completion_cut():
    artifacts, facts = _actual_gross_bundle()
    baseline = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert baseline["result"] == "OK", [
        (row["name"], row["result"]) for row in baseline["checks"]
    ]
    report = facts["report"]
    cut = next(
        row for row in report["gross_observation"]["observations"]
        if row["key"] is not None
        and row["key"]["queue_class"] == "HISTORICAL_MARKET_STEP"
        and row["key"]["visible_at"] == 1_383_040_008
        and row["key"]["stable_id"] == "P3_MARKET_86400000_2"
        and row["operation_state"] is not None
        and len(row["operation_state"]["pending_deliveries"]) == 2
    )
    assert all(
        item["delivery"]["payload_kind"] == "EXECUTION_FACT"
        for item in cut["operation_state"]["pending_deliveries"]
    )

    def assert_cut_rejected(mutate: Any) -> None:
        forged = deepcopy(report)
        forged_cut = next(
            row for row in forged["gross_observation"]["observations"]
            if row["key"] == cut["key"]
        )
        forged["gross_observation"]["episodes"][0]["rounds"][0][
            "completion_key"
        ] = cut["key"]
        mutate(forged_cut["operation_state"])
        changed = deepcopy(artifacts)
        changed["report.jsonl"] = encode_jsonl(cast(list[object], [forged]))
        result = evidence.build_execution_reconciliation(facts["run_id"], changed)
        assert result["result"] == "FAILED"

    assert_cut_rejected(lambda operations: operations["delivery_roles"].clear())

    def retag_sibling(operations: dict[str, Any]) -> None:
        first, second = operations["pending_delivery_ids"]
        operations["delivery_roles"][first] = "GROSS_CANCEL"
        operations["delivery_roles"][second] = "GROSS_REDUCTION"

    assert_cut_rejected(retag_sibling)

    def drop_item_role(operations: dict[str, Any]) -> None:
        operations["pending_deliveries"][0]["role"] = None

    assert_cut_rejected(drop_item_role)


def test_pending_gross_cancel_ack_is_anchored_to_its_cut_receipt():
    request = _over_cap_gross_input(with_working_order=True, policy_end_offset_ms=30_000)
    artifacts, facts = _actual_gross_bundle(request)
    baseline = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert baseline["result"] == "OK", [
        (row["name"], row["result"]) for row in baseline["checks"]
    ]
    report = facts["report"]
    cut = next(
        row for row in report["gross_observation"]["observations"]
        if row["key"] is not None
        and row["operation_state"] is not None
        and any(item["delivery"]["payload_kind"] == "TRANSPORT_ACK"
                for item in row["operation_state"]["pending_deliveries"])
    )
    assert any(
        action["role"] == "GROSS_CANCEL"
        and action["classification"] == "COMMITTED"
        for action in cut["operation_state"]["actions"]
    )

    def assert_cut_rejected(mutate: Any) -> None:
        forged = deepcopy(report)
        forged_cut = next(
            row for row in forged["gross_observation"]["observations"]
            if row["key"] == cut["key"]
        )
        mutate(forged_cut["operation_state"])
        changed = deepcopy(artifacts)
        changed["report.jsonl"] = encode_jsonl(cast(list[object], [forged]))
        result = evidence.build_execution_reconciliation(facts["run_id"], changed)
        assert result["result"] == "FAILED"

    def pending_ack(operations: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        item = next(
            item for item in operations["pending_deliveries"]
            if item["delivery"]["payload_kind"] == "TRANSPORT_ACK"
        )
        return item["delivery"]["delivery_id"], item

    def remove_ack_map(operations: dict[str, Any]) -> None:
        delivery_id, _ = pending_ack(operations)
        operations["cancel_action_ids"]["acks"].pop(delivery_id)

    def retag_ack_map(operations: dict[str, Any]) -> None:
        delivery_id, item = pending_ack(operations)
        assert item["cancel_action_id"] is not None
        operations["cancel_action_ids"]["acks"][delivery_id] = "wrong-cancel-action"

    def drop_item_cancel_action(operations: dict[str, Any]) -> None:
        _, item = pending_ack(operations)
        item["cancel_action_id"] = None

    def drop_cut_action(operations: dict[str, Any]) -> None:
        _, item = pending_ack(operations)
        action_id = item["cancel_action_id"]
        operations["actions"] = [
            action for action in operations["actions"]
            if action["action_id"] != action_id
        ]

    assert_cut_rejected(remove_ack_map)
    assert_cut_rejected(retag_ack_map)
    assert_cut_rejected(drop_item_cancel_action)
    assert_cut_rejected(drop_cut_action)


def test_actual_native_partial_bundle_keeps_unknown_completion_under_v2():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000, policy_end_offset_ms=150_000,
    )
    request = _with_limited_settlement_volume(request, "0.1")
    artifacts, facts = _actual_gross_bundle(request)
    observation = facts["report"]["gross_observation"]
    rounds = [round_row for episode in observation["episodes"]
              for round_row in episode["rounds"]]
    assert rounds and rounds[0]["completion_key"] is None
    assert observation["schema_version"] == "spider_gross_response_observation_v2"
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["result"] == "OK"


def test_actual_native_callback_failure_preserves_terminal_callback_map(tmp_path: Any):
    artifacts, facts = _actual_gross_bundle(fail_callback=True)
    observation = facts["report"]["gross_observation"]
    assert facts["endpoint"]["status"] == "FAILED"
    assert facts["endpoint"]["failure"]["primary"]["reason"] == "CALLBACK_FAILED"
    from src.core.backtest.spider_gross_artifact_schema import _audit_event, gross_observation
    decoded = cast(dict[str, Any], decode_canonical(canonical_bytes(observation)))
    gross_observation(decoded)
    terminal_result = next(
        delivery["result"]
        for item in observation["final_obligation_state"]["operation_state"]["pending_deliveries"]
        for delivery in [item]
        if item["result"]["classification"] == "TERMINAL"
    )
    assert type(terminal_result["callbacks"]) is dict
    invalid_values = []
    list_callbacks = deepcopy(decoded)
    list_final = cast(dict[str, Any], list_callbacks["final_obligation_state"])
    list_operations = cast(dict[str, Any], list_final["operation_state"])
    deliveries = cast(list[dict[str, Any]], list_operations["pending_deliveries"])
    result = next(
        cast(dict[str, Any], item["result"])
        for item in deliveries
        if item["result"]["classification"] == "TERMINAL"
    )
    result["callbacks"] = []
    invalid_values.append(list_callbacks)
    bad_key = deepcopy(decoded)
    key_final = cast(dict[str, Any], bad_key["final_obligation_state"])
    key_operations = cast(dict[str, Any], key_final["operation_state"])
    deliveries = cast(list[dict[str, Any]], key_operations["pending_deliveries"])
    result = next(
        cast(dict[str, Any], item["result"])
        for item in deliveries
        if item["result"]["classification"] == "TERMINAL"
    )
    result["callbacks"]["not-a-delivery-id"] = []
    invalid_values.append(bad_key)
    bad_event = deepcopy(decoded)
    event_final = cast(dict[str, Any], bad_event["final_obligation_state"])
    event_operations = cast(dict[str, Any], event_final["operation_state"])
    deliveries = cast(list[dict[str, Any]], event_operations["pending_deliveries"])
    result = next(
        cast(dict[str, Any], item["result"])
        for item in deliveries
        if item["result"]["classification"] == "TERMINAL"
    )
    key = next(key for key, events in result["callbacks"].items() if events)
    result["callbacks"][key][0]["event"]["unexpected"] = True
    invalid_values.append(bad_event)
    for malformed in invalid_values:
        with pytest.raises(ValueError):
            gross_observation(malformed)
    _audit_event({
        "event": {
            "at_ms": 1, "kind": "send", "operation": "send", "route": "REST",
            "orders": [{
                "instId": "A-USDT-SWAP", "instIdCode": 1, "tdMode": "cross",
                "clOrdId": "gross-test", "tag": "gross-test", "side": "sell",
                "ordType": "market", "px": "", "sz": "1",
            }],
        },
        "actions": [{"group_id": None, "status": "UNSUBMITTED"}],
        "disposition": "NONE",
    })
    with pytest.raises(ValueError):
        _audit_event({
            "event": {"at_ms": 1, "kind": "request_market"},
            "actions": [{"group_id": None, "status": "UNSUBMITTED", "unexpected": True}],
        })

    reconciliation = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert reconciliation["result"] == "OK"
    assert reconciliation["evidence_scope"] == "FAILED_PREFIX"
    assert len(observation["final_obligation_state"]["operation_state"][
        "pending_delivery_ids"
    ]) == 2
    omitted_final_ids = deepcopy(facts["report"])
    omitted_final_operation = omitted_final_ids["gross_observation"][
        "final_obligation_state"
    ]["operation_state"]
    assert len(omitted_final_operation["pending_delivery_ids"]) == 2
    omitted_final_operation["pending_delivery_ids"] = []
    omitted_final_ids["gross_observation"]["episodes"][-1][
        "remaining_obligations"
    ] = []
    changed_artifacts = deepcopy(artifacts)
    changed_artifacts["report.jsonl"] = encode_jsonl(
        cast(list[object], [omitted_final_ids]),
    )
    rejected = evidence.build_execution_reconciliation(facts["run_id"], changed_artifacts)
    assert rejected["result"] == "FAILED"
    assert next(row for row in rejected["checks"] if row["name"] == "REPORT_PROJECTION")[
        "result"
    ] == "FAILED"
    store = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    assert store.register(facts["attempt"], gross_execution_input=facts["raw_input"])
    for row in facts["journal"]:
        boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"],
                        barrier_id=row["barrier_id"])
        store.mark_processed(boundary, record_kind=row["record_kind"])
        store.append_journal(row)
    assert store.finalize(
        facts["endpoint"], reconciliation, [facts["report"]],
    ) == "COMPLETE_PUBLISHED"
    admitted = admit_spider_run(tmp_path / facts["run_id"])
    assert admitted["decision"] == "ACCEPT"
    assert admitted["artifacts"]["completion.json"]["endpoint_status"] == "FAILED"
    assert admitted["artifacts"]["completion.json"]["evidence_scope"] == "FAILED_PREFIX"


def test_actual_native_source_group_failure_is_stored_and_admitted(tmp_path: Any):
    artifacts, facts = _actual_gross_bundle(fail_source=True)
    terminal = facts["report"]["scheduler_observation"]["terminal"]
    assert terminal["kind"] == "SOURCE_GROUP"
    assert terminal["classification"] == "TERMINAL"
    assert facts["endpoint"]["status"] == "FAILED"
    assert "GROSS_RESPONSE_PENDING" in facts["endpoint"]["reasons"]
    context = gross_execution_context_from_input(facts["raw_input"])
    configuration = configuration_context(facts["attempt"]["configuration_context"])
    for mutate in (
        lambda reasons: reasons.append("GROSS_RESPONSE_PENDING"),
        lambda reasons: reasons.append("UNKNOWN_REASON"),
        lambda reasons: reasons.reverse(),
    ):
        changed_endpoint = deepcopy(facts["endpoint"])
        mutate(changed_endpoint["reasons"])
        with pytest.raises(ValueError):
            canonical_gross_paid_bytes(changed_endpoint, context, configuration)

    floating_run = _seeded_candidate_run(minutes=17)
    floating_request = _candidate(
        floating_run, FIXED_REMAINING_COST_TP_V1,
        cutoff=floating_run.range_start_ms + 15 * 60_000,
    )
    floating_context = floating_exit_context(floating_exit_context_from_input(
        encode_spider_floating_exit_input(floating_request),
    ))
    # Shape-only dispatch check: validate the existing closed DTO against a
    # valid candidate context after normalizing context-dependent fields; this
    # is not a replay or admission claim.
    paid_artifacts, paid_facts = _paid_bundle()
    old_paid = decode_jsonl(paid_artifacts["paid_endpoint.json"])[0]
    old_paid["reasons"] = [*cast(list[str], old_paid["reasons"]), "GROSS_RESPONSE_PENDING"]
    with pytest.raises(ValueError):
        canonical_paid_bytes(old_paid, paid_facts["context"], paid_facts["configured"])
    floating_shape = decode_jsonl(paid_artifacts["paid_endpoint.json"])[0]
    floating_context = floating_exit_context(floating_context._replace(
        policy_end_ms=floating_shape["policy_end_ms"],
        settlement_end_ms=floating_shape["settlement_end_ms"],
        generation=floating_shape["generation"],
    ))
    floating_shape["run_contract_sha256"] = floating_context.execution_input_sha256
    _paid_endpoint(floating_shape, floating_context, paid_facts["configured"])
    floating_shape["reasons"] = [*cast(list[str], floating_shape["reasons"]),
                                  "GROSS_RESPONSE_PENDING"]
    with pytest.raises(ValueError):
        _paid_endpoint(floating_shape, floating_context, paid_facts["configured"])
    reconciliation = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert reconciliation["result"] == "OK", [
        (row["name"], row["result"]) for row in reconciliation["checks"]
    ]
    assert reconciliation["evidence_scope"] == "FAILED_PREFIX"
    store = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    assert store.register(facts["attempt"], gross_execution_input=facts["raw_input"])
    for row in facts["journal"]:
        boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"],
                        barrier_id=row["barrier_id"])
        store.mark_processed(boundary, record_kind=row["record_kind"])
        store.append_journal(row)
    assert store.finalize(
        facts["endpoint"], reconciliation, [facts["report"]],
    ) == "COMPLETE_PUBLISHED"
    admitted = admit_spider_run(tmp_path / facts["run_id"])
    assert admitted["decision"] == "ACCEPT"
    assert admitted["artifacts"]["completion.json"]["endpoint_status"] == "FAILED"
    assert admitted["artifacts"]["completion.json"]["evidence_scope"] == "FAILED_PREFIX"


def test_actual_native_gross_store_failure_prefix_is_diagnosed_fail_closed(tmp_path: Any):
    artifacts, facts = _actual_gross_bundle()
    store = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    assert store.register(facts["attempt"], gross_execution_input=facts["raw_input"])
    first = facts["journal"][0]
    boundary = dict(ordinal=first["journal_seq"], journal_seq=first["journal_seq"],
                    barrier_id=first["barrier_id"])
    store.mark_processed(boundary, record_kind=first["record_kind"])
    store.publish_failure(
        "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="INJECTED_WRITE_FAILURE"),
    )
    diagnostic = admit_spider_run(tmp_path / facts["run_id"])
    assert diagnostic["decision"] == "REJECT"
    assert diagnostic["reason"] == "INCOMPLETE_PERSISTENCE"


def test_actual_gross_initial_owner_failure_is_null_observation_prefix(tmp_path: Any):
    artifacts, facts = _actual_ownerless_failure_bundle()
    report = facts["report"]
    assert report["paid_endpoint"]["status"] == "FAILED"
    assert report["paid_endpoint"]["owner_evidence"] is None
    assert report["initial_owner_evidence"] is None
    assert report["gross_observation"] is None
    reconciliation = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert reconciliation["result"] == "OK"
    assert reconciliation["evidence_scope"] == "FAILED_PREFIX"
    store = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    assert store.register(facts["attempt"], gross_execution_input=facts["raw_input"])
    assert store.finalize(
        report["paid_endpoint"], reconciliation, [report],
    ) == "COMPLETE_PUBLISHED"
    admitted = admit_spider_run(tmp_path / facts["run_id"])
    assert admitted["decision"] == "ACCEPT"
    assert admitted["artifacts"]["completion.json"]["endpoint_status"] == "FAILED"
    assert admitted["artifacts"]["completion.json"]["evidence_scope"] == "FAILED_PREFIX"


def test_actual_native_gross_store_finalize_and_anchored_admission_accept(tmp_path: Any):
    artifacts, facts = _actual_gross_bundle()
    admitted = _store_and_admit(tmp_path, artifacts, facts)
    assert set(admitted["artifacts"]) == {
        "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
        "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
    }


@pytest.mark.parametrize(("cash", "expected"), [
    ("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT"),
])
def test_actual_native_economic_terminal_bundle_is_stored_and_admitted(
    tmp_path: Any, cash: str, expected: str,
):
    artifacts, facts = _actual_gross_bundle(_native_terminal_request(cash))
    assert facts["report"]["gross_observation"]["current_state"] == expected
    owner = facts["endpoint"]["owner_evidence"]["inspection"]
    assert owner["lifecycle"] == expected
    if expected == "LIQUIDATED_INSOLVENT":
        terminal_gross = facts["report"]["gross_observation"]
        native_state = terminal_gross["final_obligation_state"]["native_state"]
        admission = native_state["gross_admission"]
        assert Decimal(admission["effective_equity"]) <= 0
        assert admission["gross_ratio"] is None
        from src.core.backtest.spider_gross_artifact_schema import gross_observation
        gross_observation(decode_canonical(canonical_bytes(terminal_gross)))
        forged_ratio = deepcopy(terminal_gross)
        forged_ratio["final_obligation_state"]["native_state"]["gross_admission"][
            "gross_ratio"] = {"numerator": "0", "denominator": "1"}
        with pytest.raises(ValueError):
            gross_observation(forged_ratio)
    admitted = _store_and_admit(tmp_path, artifacts, facts)
    assert admitted["artifacts"]["journal.jsonl"] == facts["journal"]
    assert admitted["artifacts"]["report.jsonl"][0]["gross_observation"][
        "final_obligation_state"]["native_state"]["gross_admission"]["effective_equity"] == (
            facts["report"]["gross_observation"]["final_obligation_state"][
                "native_state"]["gross_admission"]["effective_equity"]
        )
