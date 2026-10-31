"""Closed paid artifact-format tests; structural acceptance is not admission."""

from copy import deepcopy
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_run_execution_schema as schema
from src.core.backtest import spider_run_native_schema as native
from src.core.backtest.spider_execution_input import encode_spider_execution_input
from src.core.backtest.spider_historical_input import (
    encode_historical_run_input,
    historical_context_for_input,
    validate_historical_input,
)
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes,
    configuration_context,
    decode_canonical,
    decode_jsonl,
    encode_jsonl,
)
from src.core.backtest.spider_run_envelope_schema import scheduler_observation
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition, _ScheduleError
from test_spider_execution_input import _input

H = "a" * 64
RUN = "paid-run"


def _contexts(request=None) -> tuple[Any, Any, Any, Any, Any]:
    request = _input() if request is None else request
    run = request.historical_input
    raw = encode_spider_execution_input(request)
    context = schema.execution_context_from_input(raw)
    config = cast(dict[str, object], decode_canonical(run.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1",
        config_id=config["config_id"],
        configuration_sha256=run.configuration_sha256,
        products=list(run.ordered_products),
    ))
    history = historical_context_for_input(run)
    return request, raw, context, configured, history


def _attempt(request, execution, configured, history):
    run = request.historical_input
    input_hash = execution["execution_input_sha256"]
    return dict(
        schema_version="spider_execution_attempt_v1", run_id=RUN,
        run_contract_id="SPIDER_PAID_RESEARCH_RUN_V1", registration_state="VALIDATED",
        requested_scenario_selector="SPIDER_PAID_RESEARCH_RUN_V1", registration_failure=None,
        input_contract_hashes=[dict(name=name, sha256=H) for name in
                               ("SCENARIO_PLAN", "PROGRAM", "NATIVE_ARTIFACT", "POLICY_SOURCE_MANIFEST")],
        planned_coverage=[dict(ordinal=1, barrier_id="market-0", record_kind="HISTORICAL_MARKET_STEP")],
        profile_id="SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        account_key=dict(venue="SPIDER_HISTORICAL_RESEARCH", environment="RESEARCH_ONLY", account=run.account_key),
        scenario_plan_id="SPIDER_PAID_RESEARCH_RUN_V1", scenario_plan_sha256=input_hash,
        program_sha256=H, native_artifact_sha256=H, policy_source_sha256=H,
        ordering_contract_id="HISTORICAL_ORDER_V1",
        cost_contract_id="SPIDER_HISTORICAL_RESEARCH_COSTS_V1",
        funding_exclusion="HISTORICAL_FUNDING_DISABLED",
        terminal_policy="PAID_CLOSE_RESEARCH_V1", artifact_encoding="artifact_encoding_v1",
        configuration_context=configured._asdict() | {"products": list(configured.products)},
        historical_context=history,
        execution_context=execution,
    )


def _native_paid_result(request):
    composition = _ReplayComposition._from_execution_run(request)
    result = composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)
    assert result["classification"] == "SUCCESS", (result, composition._terminal)
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    paid = endpoint.result(composition)
    return composition, endpoint, paid


def test_execution_input_and_context_are_original_canonical_semantics():
    request, raw, context, _, _ = _contexts()
    assert raw.endswith(b"}") and not raw.endswith(b"\n")
    assert schema.validate_execution_artifact("execution_input.json", raw) == request
    assert context["execution_input_sha256"] == schema.execution_context_from_input(raw)["execution_input_sha256"]
    assert context["settlement_end_ms"] == request.historical_input.range_end_ms
    assert context["execution_input_sha256"] != validate_historical_input(request.historical_input)
    wrong_context = dict(context, execution_input_sha256=H)
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("execution_input.json", raw, execution_context=wrong_context)
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("historical_input.json", raw)
    with pytest.raises(ValueError):
        schema.execution_context_from_input(raw + b"\n")


@pytest.mark.parametrize("mutation", ["missing", "extra", "schema", "policy", "cutoff", "generation", "hash"])
def test_execution_context_is_exact_and_strict(mutation):
    _, _, context, _, _ = _contexts()
    row = deepcopy(context)
    if mutation == "missing":
        del row["execution_input_sha256"]
    elif mutation == "extra":
        row["run_id"] = RUN
    elif mutation == "schema":
        row["schema_version"] = "spider_historical_context_v1"
    elif mutation == "policy":
        row["policy_id"] = "SPIDER_GRID_ORIGINAL_V1"
    elif mutation == "cutoff":
        row["policy_end_ms"] = True
    elif mutation == "generation":
        row["generation"] = 1
    else:
        row["execution_input_sha256"] = "A" * 64
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("journal.jsonl", b"", execution_context=row,
                                           configuration={}, historical={})


def test_attempt_and_status_closed_paid_family_and_no_p3_alias():
    request, _, execution, configured, history = _contexts()
    attempt = _attempt(request, execution, configured, history)
    raw_attempt = encode_jsonl([attempt])
    assert schema.validate_execution_artifact(
        "attempt.json", raw_attempt, execution_context=execution,
    ) == attempt
    for change in (lambda row: row.pop("execution_context"),
                   lambda row: row.update(schema_version="spider_attempt_v1"),
                   lambda row: row.update(scenario_plan_sha256=history["historical_input_sha256"]),
                   lambda row: row.update(terminal_policy="MTM_PRESERVE_OPEN_V1"),
                   lambda row: row.update(extra=True)):
        changed = deepcopy(attempt)
        change(changed)
        with pytest.raises(ValueError):
            schema.validate_execution_artifact("attempt.json", encode_jsonl([changed]), execution_context=execution)

    status = dict(
        schema_version="spider_execution_status_v1", run_id=RUN, state="RUNNING",
        processed_boundary=None, persisted_boundary=None, failure_reason=None, primary_failure=None,
        configuration_context=attempt["configuration_context"], historical_context=history,
        execution_context=execution,
    )
    assert schema.validate_execution_artifact("status.json", encode_jsonl([status]), execution_context=execution)
    for mutation in (lambda row: row.pop("execution_context"),
                     lambda row: row.update(schema_version="spider_status_v1"),
                     lambda row: row.update(state="UNKNOWN"),
                     lambda row: row.update(unexpected=None)):
        changed = deepcopy(status)
        mutation(changed)
        with pytest.raises(ValueError):
            schema.validate_execution_artifact("status.json", encode_jsonl([changed]), execution_context=execution)


@pytest.mark.parametrize("bad_run_id", [None, 123, {}, "", "..", "outside/id"])
def test_paid_attempt_and_status_reject_invalid_run_ids(bad_run_id):
    request, _, execution, configured, history = _contexts()
    attempt = _attempt(request, execution, configured, history)
    status = dict(
        schema_version="spider_execution_status_v1", run_id=RUN, state="RUNNING",
        processed_boundary=None, persisted_boundary=None, failure_reason=None, primary_failure=None,
        configuration_context=attempt["configuration_context"], historical_context=history,
        execution_context=execution,
    )
    for filename, artifact in (("attempt.json", attempt), ("status.json", status)):
        changed = deepcopy(artifact)
        changed["run_id"] = bad_run_id
        with pytest.raises(ValueError):
            schema.validate_execution_artifact(filename, encode_jsonl([changed]), execution_context=execution)


@pytest.mark.parametrize("run_id", ["A", "A" + "x" * 63])
def test_paid_attempt_and_status_keep_original_run_id_boundaries(run_id):
    request, _, execution, configured, history = _contexts()
    attempt = _attempt(request, execution, configured, history)
    status = dict(
        schema_version="spider_execution_status_v1", run_id=RUN, state="RUNNING",
        processed_boundary=None, persisted_boundary=None, failure_reason=None, primary_failure=None,
        configuration_context=attempt["configuration_context"], historical_context=history,
        execution_context=execution,
    )
    for filename, artifact in (("attempt.json", attempt), ("status.json", status)):
        changed = deepcopy(artifact)
        changed["run_id"] = run_id
        assert schema.validate_execution_artifact(
            filename, encode_jsonl([changed]), execution_context=execution,
        )


def test_paid_result_roundtrips_native_flat_and_all_five_status_tags_as_closed_shape():
    request, _, execution, configured, _ = _contexts()
    _, _, result = _native_paid_result(request)
    result = decode_jsonl(schema.canonical_paid_bytes(result, execution, configured))[0]
    original = deepcopy(result)
    schema.validate_paid_endpoint(result, execution, configured)
    assert result == original
    encoded = schema.canonical_paid_bytes(result, execution, configured)
    assert decode_jsonl(encoded) == [result]
    assert schema.validate_execution_artifact(
        "paid_endpoint.json", encoded, execution_context=execution,
        configuration=configured,
    ) == result
    for status in ("PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"):
        # Schema validation is deliberately structural; economic status truth is
        # compared by the later shared evidence predicate, not duplicated here.
        changed = deepcopy(result)
        changed["status"] = status
        schema.validate_paid_endpoint(changed, execution, configured)

    failed = deepcopy(result)
    failed.update(
        status="FAILED", owner_evidence=None,
        pending_obligations=dict(positions=None, working_orders=None, pending_actions=[],
                                 pending_deliveries=[], pending_polls=[]),
        failure=dict(primary=dict(kind="SCHEDULER", reason="stopped", native_failure=None),
                     evidence_error=None, inspection=None, inspection_error=None, observed_at=1),
    )
    schema.validate_paid_endpoint(failed, execution, configured)
    for mutation in (lambda row: row.update(status="FUTURE"),
                     lambda row: row.update(failure=None),
                     lambda row: row.update(extra=1),
                     lambda row: row.update(run_contract_sha256=H)):
        changed = deepcopy(failed)
        mutation(changed)
        with pytest.raises(ValueError):
            schema.validate_paid_endpoint(changed, execution, configured)


@pytest.mark.parametrize("capture_fails", [False, True])
def test_failed_native_endpoint_keeps_valid_evidence_or_explicit_null(
    capture_fails: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, _, execution, configured, _ = _contexts()
    composition = _ReplayComposition._from_execution_run(request)
    if capture_fails:
        def fail_capture(*_args):
            raise RuntimeError("injected evidence capture failure")
        monkeypatch.setattr(composition, "capture_owner_evidence", fail_capture)
    else:
        composition._stop_scheduler(_ScheduleError("native scheduler failed"), "POLL", "schema-failure")
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    result = endpoint.result(composition)
    assert result["status"] == "FAILED"
    if capture_fails:
        assert result["owner_evidence"] is None
        assert result["pending_obligations"]["positions"] is None
        assert result["pending_obligations"]["working_orders"] is None
        assert result["failure"]["evidence_error"] is not None
    else:
        assert result["owner_evidence"]["cutoff"] == composition._current_time
        assert result["failure"]["evidence_error"] is None
    encoded = schema.canonical_paid_bytes(result, execution, configured)
    schema.validate_execution_artifact(
        "paid_endpoint.json", encoded, execution_context=execution,
        configuration=configured,
    )


def test_paid_scheduler_class_is_only_admitted_by_explicit_paid_opt_in():
    observation = dict(
        current_time=100, last_popped=None, gate="RUNNING", terminal=None,
        pending_keys=[dict(visible_at=101, queue_class="PAID_ENDPOINT_DRAIN", schedule_sequence=0, stable_id="drain")],
        records=[dict(key=dict(visible_at=101, queue_class="PAID_ENDPOINT_DRAIN", schedule_sequence=0,
                               stable_id="drain"), kind="PAID_ENDPOINT_DRAIN", stable_id="drain",
                     classification="PENDING")],
        polls=[], callback_actions=[],
    )
    scheduler_observation(observation, historical=True, paid_execution=True)
    with pytest.raises(ValueError):
        scheduler_observation(observation, historical=True)


def test_public_report_uses_only_closed_paid_observation_and_scheduler_shapes():
    request, _, execution, configured, _ = _contexts()
    composition, endpoint, paid = _native_paid_result(request)
    paid = decode_jsonl(schema.canonical_paid_bytes(paid, execution, configured))[0]
    scheduler = composition._public_scheduler_observation()
    observation = endpoint._paid_observation(
        composition, scheduler, current_evidence_complete=True,
        failure=cast(dict[str, Any] | None, paid["failure"]),
    )
    row = dict(
        schema_version="spider_execution_report_v1", run_id=RUN,
        initial_owner_evidence=paid["owner_evidence"], scheduler_observation=scheduler,
        paid_observation=observation, paid_endpoint=paid,
        source_evidence_refs=["journal:1", "journal:2"],
    )
    schema.validate_report(row, execution, configured)
    assert schema.validate_execution_artifact(
        "report.jsonl", encode_jsonl([row]), execution_context=execution,
        configuration=configured,
    ) == [row]
    for change in (lambda value: value.update(schema_version="spider_product_report_v1"),
                   lambda value: value.update(extra=None),
                   lambda value: value.update(source_evidence_refs=["journal:2", "journal:1"]),
                   lambda value: value.update(initial_owner_evidence=None)):
        changed = deepcopy(row)
        change(changed)
        with pytest.raises(ValueError):
            schema.validate_report(changed, execution, configured)


def test_actual_native_market_payload_uses_paid_hash_and_p3_default_stays_base_hash():
    request, _, execution, _, _ = _contexts()

    def market_evidence(paid: bool) -> tuple[dict[str, Any], dict[str, Any]]:
        events = []
        composition = (_ReplayComposition._from_execution_run(request) if paid
                       else _ReplayComposition._from_historical_run(request.historical_input))
        composition._evidence_callback = lambda kind, key, payload: events.append((kind, key, payload))
        dispatch = composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)
        assert dispatch["classification"] == "SUCCESS", (dispatch, composition._terminal)
        _, key, payload = next(event for event in events if event[0] == "HISTORICAL_MARKET_RESULT")
        return cast(dict[str, Any], decode_canonical(canonical_bytes(payload))), key

    paid_payload, paid_key = market_evidence(True)
    configured = _contexts()[3]
    history = historical_context_for_input(request.historical_input)
    assert paid_payload["request"]["run_contract_hash"] == execution["execution_input_sha256"]
    native.historical_market_result(
        paid_payload,
        context=configuration_context(configured),
        historical_context=history,
        expected_run_contract_hash=execution["execution_input_sha256"],
    )
    with pytest.raises(ValueError):
        native.historical_market_result(
            paid_payload, context=configuration_context(configured),
            historical_context=history,
        )

    paid_result = cast(dict[str, Any], paid_payload["result"])
    paid_before = cast(dict[str, Any], paid_payload["owner_inspection_before"])
    paid_after = cast(dict[str, Any], paid_payload["owner_inspection_after"])
    paid_record = dict(
        schema_version="spider_execution_journal_record_v1", run_id=RUN, journal_seq=1,
        barrier_id="paid-market", record_kind="HISTORICAL_MARKET_RESULT", scheduler_key=paid_key,
        causal_parent_ids=[], effective_at=paid_result["effective_at"], visible_at=paid_key["visible_at"],
        account_version_before=paid_before["account_version"], account_version_after=paid_after["account_version"],
        payload=paid_payload, configuration_context=configured._asdict() | {"products": list(configured.products)},
        historical_context=history, execution_context=execution,
    )
    assert schema.validate_execution_artifact(
        "journal.jsonl", encode_jsonl([paid_record]), execution_context=execution,
        configuration=configured, historical=history,
    ) == [paid_record]

    base_payload, base_key = market_evidence(False)
    assert base_payload["request"]["run_contract_hash"] == validate_historical_input(request.historical_input)
    native.historical_market_result(
        base_payload, context=configuration_context(configured), historical_context=history,
    )
    with pytest.raises(ValueError):
        native.historical_market_result(
            base_payload, context=configuration_context(configured), historical_context=history,
            expected_run_contract_hash=execution["execution_input_sha256"],
        )
    base_result = cast(dict[str, Any], base_payload["result"])
    base_before = cast(dict[str, Any], base_payload["owner_inspection_before"])
    base_after = cast(dict[str, Any], base_payload["owner_inspection_after"])
    base_record = dict(
        schema_version="spider_journal_record_v1", run_id=RUN, journal_seq=1,
        barrier_id="base-market", record_kind="HISTORICAL_MARKET_RESULT", scheduler_key=base_key,
        causal_parent_ids=[], effective_at=base_result["effective_at"], visible_at=base_key["visible_at"],
        account_version_before=base_before["account_version"], account_version_after=base_after["account_version"],
        payload=base_payload, configuration_context=configured._asdict() | {"products": list(configured.products)},
        historical_context=history,
    )
    envelope.journal_record(base_record)


def test_paid_journal_seam_requires_valid_contexts_and_exact_context_row():
    _, _, execution, configured, history = _contexts()
    row = dict(
        schema_version="spider_execution_journal_record_v1", run_id=RUN, journal_seq=1,
        barrier_id="barrier", record_kind="CALLBACK_RESULT",
        scheduler_key=dict(visible_at=1, queue_class="DELIVERY", schedule_sequence=0, stable_id="d"),
        causal_parent_ids=[], effective_at=1, visible_at=1,
        account_version_before=None, account_version_after=None,
        payload=dict(delivery_id="d", outcome="SUCCESS", policy_events=[], actions=[], failure=None),
        configuration_context=configured._asdict() | {"products": list(configured.products)},
        historical_context=history, execution_context=execution,
    )
    envelope.journal_record(row, execution=execution)
    assert schema.validate_execution_artifact(
        "journal.jsonl", encode_jsonl([row]), execution_context=execution,
        configuration=configured, historical=history,
    ) == [row]
    for changed in (dict(row, execution_context=dict(execution, generation=1)),
                    {key: value for key, value in row.items() if key != "configuration_context"},
                    {key: value for key, value in row.items() if key != "historical_context"}):
        with pytest.raises(ValueError):
            envelope.journal_record(changed, execution=execution)
    with pytest.raises(ValueError):
        envelope.journal_record(row, execution={"schema_version": "bad"})


def test_new_reconciliation_completion_shapes_are_closed_and_family_specific():
    request, _, execution, configured, history = _contexts()
    attempt = _attempt(request, execution, configured, history)
    checks = [dict(name=name, result="OK", evidence_refs=[]) for name in schema._CHECKS]
    reconciliation = dict(schema_version="spider_execution_reconciliation_v1", run_id=RUN,
                          result="OK", evidence_scope="COMPLETE_RUN", checks=checks)
    raw_reconciliation = encode_jsonl([reconciliation])
    assert schema.validate_execution_artifact("reconciliation.json", raw_reconciliation,
                                              execution_context=execution) == reconciliation
    completion = dict(
        schema_version="spider_execution_completion_v1", run_id=RUN, state="COMPLETE",
        terminal_reason="PAID_CLOSE_RESEARCH_V1",
        input_contract_hashes=attempt["input_contract_hashes"],
        planned_coverage=attempt["planned_coverage"],
        processed_boundary=dict(ordinal=1, barrier_id="market-0", journal_seq=1),
        persisted_boundary=dict(ordinal=1, barrier_id="market-0", journal_seq=1),
        endpoint_state_digest=H, reconciliation_digest=H,
        configuration_context=attempt["configuration_context"], historical_context=history,
        execution_context=execution, endpoint_status="INCOMPLETE", evidence_scope="FAILED_PREFIX",
        artifacts=[dict(path=path, schema_version=version, sha256=H, byte_count=0, row_count=1 if count is None else count)
                   for path, version, count in schema._COMPLETION_ARTIFACTS],
    )
    assert schema.validate_execution_artifact(
        "completion.json", encode_jsonl([completion]), execution_context=execution,
    ) == completion
    zero_journal = deepcopy(completion)
    zero_journal["processed_boundary"] = None
    zero_journal["persisted_boundary"] = None
    cast(list[dict[str, Any]], zero_journal["artifacts"])[2]["row_count"] = 0
    assert schema.validate_execution_artifact(
        "completion.json", encode_jsonl([zero_journal]), execution_context=execution,
    ) == zero_journal
    for name in ("processed_boundary", "persisted_boundary"):
        malformed_frontier = deepcopy(zero_journal)
        malformed_frontier[name] = {"ordinal": 1}
        with pytest.raises(ValueError):
            schema.validate_execution_artifact(
                "completion.json", encode_jsonl([malformed_frontier]), execution_context=execution,
            )
    bad = deepcopy(completion)
    cast(dict[str, Any], bad)["artifacts"].reverse()
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("completion.json", encode_jsonl([bad]), execution_context=execution)


def test_artifact_dispatch_rejects_family_substitution_and_preserves_original_input_hashes():
    request, raw, execution, configured, history = _contexts()
    original_p3_hash = validate_historical_input(request.historical_input)
    assert history["historical_input_sha256"] == original_p3_hash
    for filename in ("endpoint.json", "historical_input.json", "unknown.json"):
        with pytest.raises(ValueError):
            schema.validate_execution_artifact(filename, raw)
    decoded_wrapper = cast(dict[str, object], decode_canonical(raw))
    decoded_base = decode_canonical(encode_historical_run_input(request.historical_input))
    assert decoded_wrapper["historical_input"] == decoded_base
    assert execution["execution_input_sha256"] != original_p3_hash
    assert configured.products == request.historical_input.ordered_products


def test_paid_jsonl_rejects_duplicate_keys_and_numeric_float_literals():
    request, _, execution, configured, history = _contexts()
    status = dict(
        schema_version="spider_execution_status_v1", run_id=RUN, state="RUNNING",
        processed_boundary=None, persisted_boundary=None, failure_reason=None, primary_failure=None,
        configuration_context=_attempt(request, execution, configured, history)["configuration_context"],
        historical_context=history, execution_context=execution,
    )
    raw = encode_jsonl([status])
    token = b'"policy_end_ms":'
    start = raw.index(token) + len(token)
    end = raw.index(b",", start)
    value = raw[start:end]
    duplicate = raw[:end] + b',"policy_end_ms":' + value + raw[end:]
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("status.json", duplicate, execution_context=execution)
    float_literal = raw[:end] + b".0" + raw[end:]
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("status.json", float_literal, execution_context=execution)
