"""Closed gross-family schema checks remain shape-only and family-specific."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_execution_schema as schema
from src.core.backtest import spider_run_native_schema as native
from src.core.backtest import spider_gross_artifact_schema as gross_schema
from src.core.backtest.spider_historical_input import historical_context_for_input
from src.core.backtest.spider_gross_observation import project_gross_observation
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes, configuration_context, decode_canonical, encode_jsonl,
    gross_execution_context,
)
from src.core.backtest.spider_gross_execution_input import encode_spider_gross_execution_input
from src.core.backtest.synthetic_scenario_replay import _CallbackPrefix, _ReplayComposition
from test_spider_gross_observation import (
    _dispatch_to_end, _with_later_mark, _with_limited_settlement_volume, _with_minimums,
)
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_gross_delivery import _dispatch_to_reduction_notice
from test_spider_run_execution_schema import _attempt as _paid_attempt


def _context():
    request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    raw = encode_spider_gross_execution_input(request)
    return request, raw, schema.gross_execution_context_from_input(raw)


def test_gross_context_is_recomputed_exact_and_old_context_stays_closed():
    request, raw, context = _context()
    assert schema.validate_execution_artifact(
        "execution_input.json", raw,
        execution_context=gross_execution_context(context),
    ) == request
    assert schema.validate_execution_artifact("execution_input.json", raw) == request
    assert context["execution_input_sha256"] != context["base_execution_input_sha256"]
    authorization_scope = cast(dict[str, object], context["authorization_scope"])
    assert authorization_scope["ordered_products"] == list(
        request.authorization_scope.ordered_products,
    )
    for changed in (
        {key: value for key, value in context.items() if key != "native_profile_id"},
        dict(context, model_id="unexpected"),
        dict(context, extra=True),
    ):
        with pytest.raises(ValueError):
            gross_execution_context(changed)
    for changed in (
        dict(context, schema_version="spider_execution_context_v1"),
        dict(context, policy_id="SPIDER_HOST_ORIGINAL_PAID_V1"),
    ):
        with pytest.raises(ValueError):
            schema.validate_execution_artifact("execution_input.json", raw,
                                               execution_context=changed)
    with pytest.raises(ValueError):
        schema.validate_execution_artifact("report.jsonl", b"", execution_context={
            "schema_version": "spider_execution_context_v1",
            "policy_id": "SPIDER_HOST_ORIGINAL_PAID_V1",
            "endpoint_policy_id": "PAID_CLOSE_RESEARCH_V1",
            "execution_input_sha256": "0" * 64,
            "policy_end_ms": 1, "settlement_end_ms": 2, "generation": 0,
        })


def test_actual_gross_native_projection_has_closed_gross_report_shape():
    request, raw, context = _context()
    history = request.execution_input.historical_input
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    assert composition._dispatch_due(history.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    configured_raw = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1",
        config_id=configured_raw["config_id"],
        configuration_sha256=history.configuration_sha256,
        products=list(history.ordered_products),
    ))
    paid = endpoint.result(composition)
    paid["schema_version"] = "spider_gross_paid_endpoint_result_v1"
    paid.pop("gross_response")  # E3b's paid DTO transport strips the private result field.
    schema.validate_gross_paid_endpoint(
        decode_canonical(canonical_bytes(paid)), context, configured,
    )

    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection={"current_state": endpoint.gross_response.state,
                             "reason": endpoint.gross_response.reason},
    )
    assert report is not None
    from src.core.backtest.spider_gross_artifact_schema import gross_observation
    serialized_report = decode_canonical(canonical_bytes(report))
    gross_observation(serialized_report, expected_hash=cast(str, context["execution_input_sha256"]))
    root = cast(dict[str, Any], serialized_report)
    with pytest.raises(ValueError):
        gross_observation({key: value for key, value in root.items() if key != "reason"})
    with pytest.raises(ValueError):
        gross_observation(dict(root, schema_version="spider_execution_report_v1"))
    with pytest.raises(ValueError):
        gross_observation(dict(root, unexpected=None))

    # These are structural acceptance examples only. They preserve the exact
    # nullable/no-action, partial, rejection and terminal shapes without
    # claiming that those economic outcomes occurred in this replay.
    example_round = deepcopy(root)
    round_row = example_round["episodes"][0]["rounds"][0]
    round_row.update(sequence=None, action_ids=[], completion_key=None,
                     reason="WAIT_CAPACITY")
    round_row["proposal"].update(
        state="WAIT_CAPACITY", estimated_fee_rate=None,
        estimated_reduction_gross=None, estimated_fee=None,
        estimated_equity_after=None,
    )
    gross_observation(example_round)
    partial_round = deepcopy(root)
    partial = partial_round["episodes"][0]["rounds"][0]
    partial["action_ids"] = partial["action_ids"][:1]
    partial["completion_key"] = None
    partial["reason"] = "PARTIAL_EXECUTION"
    gross_observation(partial_round)
    rejected_round = deepcopy(root)
    rejected = rejected_round["episodes"][0]["rounds"][0]
    rejected["reason"] = "ACTION_REJECTED"
    rejected["completion_key"] = None
    gross_observation(rejected_round)
    terminal_report = deepcopy(root)
    terminal_report["current_state"] = "LIQUIDATED_INSOLVENT"
    terminal_report["reason"] = "NATIVE_TERMINAL"
    gross_observation(terminal_report)

    scheduler = composition._public_scheduler_observation()
    paid_observation = endpoint._paid_observation(
        composition, scheduler, current_evidence_complete=True, failure=None,
    )
    paid_observation["schema_version"] = "spider_gross_paid_observation_v1"
    report_row = dict(
        schema_version="spider_gross_execution_report_v1", run_id="gross-run",
        initial_owner_evidence=paid["owner_evidence"], scheduler_observation=scheduler,
        paid_observation=paid_observation, paid_endpoint=paid,
        source_evidence_refs=["journal:1"], gross_observation=report,
    )
    schema.validate_execution_artifact(
        "report.jsonl", encode_jsonl([report_row]), execution_context=context,
        configuration=configured,
    )
    schema.validate_gross_report(
        decode_canonical(canonical_bytes(report_row)), context, configured,
    )
    malformed_paid: dict[str, Any] = deepcopy(report_row)
    malformed_paid["paid_observation"]["unexpected"] = True
    with pytest.raises(ValueError):
        schema.validate_gross_report(
            decode_canonical(canonical_bytes(malformed_paid)), context, configured,
        )
    nonfailed_null: dict[str, Any] = deepcopy(report_row)
    nonfailed_null["gross_observation"] = None
    with pytest.raises(ValueError):
        schema.validate_gross_report(
            decode_canonical(canonical_bytes(nonfailed_null)), context, configured,
        )

    attempt: dict[str, Any] = _paid_attempt(request.execution_input, context, configured,
                                             historical_context_for_input(history))
    attempt.update(schema_version="spider_gross_execution_attempt_v1",
                   run_contract_id="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
                   requested_scenario_selector="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
                   scenario_plan_id="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
                   profile_id=cast(str, context["native_profile_id"]))
    status = dict(schema_version="spider_gross_execution_status_v1", run_id="gross-run",
                  state="RUNNING", processed_boundary=None, persisted_boundary=None,
                  failure_reason=None, primary_failure=None,
                  configuration_context=attempt["configuration_context"],
                  historical_context=historical_context_for_input(history),
                  execution_context=context)
    checks = [dict(name=name, result="OK", evidence_refs=[]) for name in (
        "INPUT_IDENTITY", "JOURNAL_FRONTIER", "OWNER_SCOPE_AND_CHAIN",
        "SNAPSHOT_DIGESTS", "ACTION_BINDINGS", "ENDPOINT_CLASSIFICATION",
        "REPORT_PROJECTION",
    )]
    reconciliation = dict(schema_version="spider_gross_execution_reconciliation_v1",
                          run_id="gross-run", result="OK", evidence_scope="COMPLETE_RUN",
                          checks=checks)
    completion = dict(
        schema_version="spider_gross_execution_completion_v1", run_id="gross-run",
        state="COMPLETE", terminal_reason="PAID_CLOSE_RESEARCH_V1",
        input_contract_hashes=attempt["input_contract_hashes"],
        planned_coverage=[dict(ordinal=1, barrier_id="market-0",
                               record_kind="HISTORICAL_MARKET_STEP")],
        processed_boundary=dict(ordinal=1, barrier_id="market-0", journal_seq=1),
        persisted_boundary=dict(ordinal=1, barrier_id="market-0", journal_seq=1),
        endpoint_state_digest="a" * 64, reconciliation_digest="b" * 64,
        artifacts=[dict(path=path, schema_version=version, sha256="c" * 64,
                        byte_count=0, row_count=1 if count is None else count)
                   for path, version, count in schema._GROSS_COMPLETION_ARTIFACTS],
        configuration_context=attempt["configuration_context"],
        historical_context=historical_context_for_input(history), execution_context=context,
        endpoint_status="FAILED", evidence_scope="FAILED_PREFIX",
    )
    for filename, value in (
        ("execution_input.json", raw),
        ("attempt.json", encode_jsonl([attempt])),
        ("status.json", encode_jsonl([status])),
        ("journal.jsonl", encode_jsonl([])),
        ("paid_endpoint.json", encode_jsonl([paid])),
        ("report.jsonl", encode_jsonl([report_row])),
        ("reconciliation.json", encode_jsonl([reconciliation])),
        ("completion.json", encode_jsonl([completion])),
    ):
        schema.validate_execution_artifact(
            filename, value, execution_context=context,
            configuration=configured, historical=historical_context_for_input(history),
            journal_rows=0,
        )
    malformed = deepcopy(report)
    malformed["episodes"][0]["rounds"][0]["proposal"]["extra"] = True
    with pytest.raises(ValueError):
        gross_observation(decode_canonical(canonical_bytes(malformed)))
    for target, value in (
        ("source_state", {"unexpected": True}),
        ("operation_state", {"unexpected": True}),
    ):
        malformed_context = deepcopy(report)
        malformed_context["episodes"][0]["rounds"][0]["proposal"]["proposal_context"][target] = value
        with pytest.raises(ValueError):
            gross_observation(decode_canonical(canonical_bytes(malformed_context)))
    malformed_order = deepcopy(report)
    malformed_order["episodes"][0]["trigger_state"]["orders"] = [{"unexpected": True}]
    with pytest.raises(ValueError):
        gross_observation(decode_canonical(canonical_bytes(malformed_order)))
    with pytest.raises(ValueError):
        gross_observation(report, expected_hash="0" * 64)


def test_gross_native_market_and_source_control_are_explicitly_family_scoped():
    request, _, context = _context()
    history = request.execution_input.historical_input
    composition = _ReplayComposition._from_gross_response_run(request)
    events = []
    composition._evidence_callback = lambda kind, key, payload: events.append((kind, key, payload))
    assert composition._dispatch_due(history.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    configuration_row = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1",
        config_id=configuration_row["config_id"],
        configuration_sha256=history.configuration_sha256,
        products=list(history.ordered_products),
    ))
    historical = historical_context_for_input(history)
    market_payload = next(
        payload for kind, _, payload in events if kind == "HISTORICAL_MARKET_RESULT"
    )
    market = decode_canonical(canonical_bytes(market_payload))
    market = cast(dict[str, Any], market)
    native.historical_market_result(
        market, context=configured, historical_context=historical,
        expected_run_contract_hash=cast(str, context["execution_input_sha256"]), gross_profile=True,
    )
    with pytest.raises(ValueError):
        native.historical_market_result(
            market, context=configured, historical_context=historical,
            expected_run_contract_hash=cast(str, context["execution_input_sha256"]),
        )
    control = next(payload["gross_group_control"] for kind, _, payload in events
                   if kind == "SOURCE_GROUP_RESULT" and "gross_group_control" in payload)
    gross_schema.gross_group_control(control)
    with pytest.raises(ValueError):
        gross_schema.gross_group_control(dict(control, unknown=True))

    assert composition._paid_endpoint is not None
    paid = composition._paid_endpoint.result(composition)
    owner = cast(dict[str, Any], decode_canonical(canonical_bytes(paid["owner_evidence"])))
    selected = []
    for kind in ("SOURCE_GROUP_RESULT", "HISTORICAL_MARKET_RESULT", "SNAPSHOT_FACT", "CALLBACK_RESULT"):
        key, payload = next((key, payload) for actual, key, payload in events if actual == kind)
        payload = cast(dict[str, Any], payload)
        if kind == "SOURCE_GROUP_RESULT":
            payload = dict(payload, owner_evidence_before=owner, owner_evidence_after=owner)
        selected.append((kind, key, payload))
    journal = []
    for sequence, (kind, key, payload) in enumerate(selected, 1):
        result = payload.get("result") if type(payload) is dict else None
        before = result.get("account_version_before") if type(result) is dict else None
        after = result.get("account_version_after") if type(result) is dict else None
        if kind == "HISTORICAL_MARKET_RESULT":
            before = payload["owner_inspection_before"]["account_version"]
            after = payload["owner_inspection_after"]["account_version"]
        effective_at = (result.get("effective_at", key["visible_at"])
                        if type(result) is dict else key["visible_at"])
        journal.append(dict(
            schema_version="spider_gross_execution_journal_record_v1", run_id="gross-run",
            journal_seq=sequence, barrier_id=key["stable_id"], record_kind=kind,
            scheduler_key=key, causal_parent_ids=[], effective_at=effective_at,
            visible_at=key["visible_at"], account_version_before=before,
            account_version_after=after, payload=payload,
            configuration_context=configured._asdict() | {"products": list(configured.products)},
            historical_context=historical, execution_context=context,
        ))
    schema.validate_execution_artifact(
        "journal.jsonl", encode_jsonl(journal), execution_context=context,
        configuration=configured, historical=historical,
    )


def test_gross_attempt_status_dispatch_accepts_complete_rows_and_rejects_mutations():
    request, raw, context = _context()
    history_input = request.execution_input.historical_input
    config_raw = cast(dict[str, Any], decode_canonical(history_input.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1",
        config_id=config_raw["config_id"],
        configuration_sha256=history_input.configuration_sha256,
        products=list(history_input.ordered_products),
    ))
    history = historical_context_for_input(history_input)
    attempt: dict[str, Any] = _paid_attempt(request.execution_input, context, configured, history)
    attempt.update(
        schema_version="spider_gross_execution_attempt_v1",
        run_contract_id="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
        requested_scenario_selector="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
        scenario_plan_id="SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
        profile_id=cast(str, context["native_profile_id"]),
    )
    assert schema.validate_execution_artifact(
        "attempt.json", encode_jsonl([attempt]), execution_context=context,
    ) == attempt
    status = dict(
        schema_version="spider_gross_execution_status_v1", run_id=attempt["run_id"],
        state="RUNNING", processed_boundary=None, persisted_boundary=None,
        failure_reason=None, primary_failure=None,
        configuration_context=attempt["configuration_context"],
        historical_context=history, execution_context=context,
    )
    assert schema.validate_execution_artifact(
        "status.json", encode_jsonl([status]), execution_context=context,
    ) == status
    for filename, row in (("attempt.json", attempt), ("status.json", status)):
        for mutation in ("missing", "extra", "mixed"):
            changed: dict[str, Any] = deepcopy(row)
            if mutation == "missing":
                changed.pop("run_id")
            elif mutation == "extra":
                changed["unexpected"] = True
            else:
                changed["schema_version"] = "spider_execution_" + filename.split(".")[0] + "_v1"
            with pytest.raises(ValueError):
                schema.validate_execution_artifact(
                    filename, encode_jsonl([changed]), execution_context=context,
                )


def test_ownerless_failed_nullable_report_is_validated_by_public_dispatchers():
    request, _, context = _context()
    history = request.execution_input.historical_input
    config_raw = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1", config_id=config_raw["config_id"],
        configuration_sha256=history.configuration_sha256, products=list(history.ordered_products),
    ))

    def report_for(composition: Any, *, ownerless: bool) -> dict[str, Any]:
        endpoint = composition._paid_endpoint
        assert endpoint is not None
        paid = endpoint.result(composition)
        assert paid["status"] == "FAILED"
        paid["schema_version"] = "spider_gross_paid_endpoint_result_v1"
        paid.pop("gross_response")
        scheduler = composition._public_scheduler_observation()
        primary = composition._terminal or {
            "kind": "OWNER_EVIDENCE", "reason": "EVIDENCE_FAILED", "native_failure": None,
        }
        failure = dict(
            primary=dict(kind=primary.get("kind", "SCHEDULER"),
                         reason=primary.get("reason", "EXECUTION_FAILURE"),
                         native_failure=primary.get("native_failure")),
            evidence_error=None, inspection=None, inspection_error=None,
            observed_at=scheduler["current_time"],
        )
        paid_observation = endpoint._paid_observation(
            composition, scheduler, current_evidence_complete=paid["owner_evidence"] is not None,
            failure=failure,
        )
        paid_observation["schema_version"] = "spider_gross_paid_observation_v1"
        assert (paid["owner_evidence"] is None) is ownerless
        return dict(
            schema_version="spider_gross_execution_report_v1", run_id="gross-run",
            initial_owner_evidence=paid["owner_evidence"], scheduler_observation=scheduler,
            paid_observation=paid_observation, paid_endpoint=paid,
            source_evidence_refs=[], gross_observation=None,
        )

    ownerless_composition = _ReplayComposition._from_gross_response_run(request)
    assert ownerless_composition._dispatch_due(history.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    ownerless_endpoint = ownerless_composition._paid_endpoint
    assert ownerless_endpoint is not None
    ownerless_composition.capture_owner_evidence = lambda *args, **kwargs: (_ for _ in ()).throw(
        ValueError("INJECTED_OWNER_EVIDENCE_FAILURE"),
    )
    ownerless = report_for(ownerless_composition, ownerless=True)
    raw = encode_jsonl([ownerless])
    schema.validate_execution_artifact(
        "report.jsonl", raw, execution_context=context, configuration=configured,
    )
    schema.validate_gross_report(
        decode_canonical(canonical_bytes(ownerless)), context, configured,
    )

    ownerful_composition = _ReplayComposition._from_gross_response_run(request)
    notice = _dispatch_to_reduction_notice(ownerful_composition, ownerful_composition._paid_endpoint)
    delivery_id = notice["item"]["delivery"]["delivery_id"]
    original_capture = ownerful_composition._capture_callback

    def fail_target_notice(delivery: Any) -> Any:
        if delivery["delivery_id"] == delivery_id:
            return _CallbackPrefix((), RuntimeError("INJECTED_CALLBACK_FAILURE"))
        return original_capture(delivery)

    ownerful_composition._capture_callback = fail_target_notice
    outcome = ownerful_composition._dispatch_due(notice["key"][0])
    assert outcome["classification"] == "TERMINAL"
    ownerful = report_for(ownerful_composition, ownerless=False)
    with pytest.raises(ValueError):
        schema.validate_execution_artifact(
            "report.jsonl", encode_jsonl([ownerful]), execution_context=context,
            configuration=configured,
        )


def test_gross_reconciliation_uses_bounded_shared_reference_validation():
    _, _, context = _context()
    checks = [dict(name=name, result="OK", evidence_refs=[]) for name in (
        "INPUT_IDENTITY", "JOURNAL_FRONTIER", "OWNER_SCOPE_AND_CHAIN",
        "SNAPSHOT_DIGESTS", "ACTION_BINDINGS", "ENDPOINT_CLASSIFICATION",
        "REPORT_PROJECTION",
    )]
    row = dict(schema_version="spider_gross_execution_reconciliation_v1",
               run_id="gross-run", result="OK", evidence_scope="COMPLETE_RUN", checks=checks)
    assert schema.validate_execution_artifact(
        "reconciliation.json", encode_jsonl([row]), execution_context=context,
        journal_rows=2,
    ) == row
    for refs in (["journal:0"], ["journal:3"], ["journal:1", "journal:1"],
                 ["not-a-reference"]):
        changed: dict[str, Any] = deepcopy(row)
        changed["checks"][0]["evidence_refs"] = refs
        with pytest.raises(ValueError):
            schema.validate_execution_artifact(
                "reconciliation.json", encode_jsonl([changed]), execution_context=context,
                journal_rows=2,
            )


def _validate_actual_report_projection(request: Any, endpoint: Any, *, endpoint_projection: Any = None) -> None:
    assert endpoint.gross_response is not None
    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection=(endpoint_projection if endpoint_projection is not None
                             else endpoint.gross_response.observation()),
    )
    assert report is not None
    gross_schema.gross_observation(
        decode_canonical(canonical_bytes(report)),
        expected_hash=cast(str, schema.gross_execution_context_from_input(
            encode_spider_gross_execution_input(request),
        )["execution_input_sha256"]),
    )


def test_actual_producer_intermediate_and_terminal_reports_match_closed_shapes():
    pending_request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    pending_composition = _ReplayComposition._from_gross_response_run(pending_request)
    pending_endpoint = pending_composition._paid_endpoint
    assert pending_endpoint is not None and pending_endpoint.gross_response is not None
    _dispatch_to_reduction_notice(pending_composition, pending_endpoint)
    _validate_actual_report_projection(pending_request, pending_endpoint)

    partial_request = _with_limited_settlement_volume(
        _over_cap_gross_input(policy_end_offset_ms=150_000, settlement_extension_ms=180_000),
        "0.1",
    )
    _, partial_endpoint = _dispatch_to_end(partial_request)
    _validate_actual_report_projection(partial_request, partial_endpoint)


def test_actual_producer_nested_context_leaf_shapes_reject_mutations():
    request = _over_cap_gross_input(
        with_working_order=True, policy_end_offset_ms=150_000,
        settlement_extension_ms=180_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    while composition._queue:
        composition._dispatch_due(composition._queue[0][0])

    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection=endpoint.gross_response.observation(),
    )
    assert report is not None
    frozen = decode_canonical(canonical_bytes(report))
    gross_schema.gross_observation(frozen)
    root = cast(dict[str, Any], frozen)

    selected: tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]] | None = None
    for episode in root["episodes"]:
        for round_row in episode["rounds"]:
            proposal_context = round_row["proposal"].get("proposal_context")
            if type(proposal_context) is not dict:
                continue
            source = proposal_context.get("source_state")
            operations = proposal_context.get("operation_state")
            if type(source) is not dict or type(operations) is not dict:
                continue
            rows = source.get("policy_rows")
            markets = source.get("policy_markets")
            actions = operations.get("actions")
            dependencies = operations.get("send_dependencies")
            if not (rows and type(markets) is dict and actions and type(dependencies) is dict):
                continue
            action = next((item for item in actions
                           if type(item) is dict and type(item.get("result")) is dict), None)
            dependency = next((item for item in dependencies.values()
                               if type(item) is dict and item.get("state") == "COMMITTED"
                               and type(item.get("binding")) is dict), None)
            if action is not None and dependency is not None:
                selected = (rows[0], next(iter(markets.values())), action["result"],
                            dependency["binding"])
                break
        if selected is not None:
            break
    assert selected is not None
    row, market, action_result, binding = selected

    def reject_leaf_mutations(leaf: dict[str, Any], required_field: str) -> None:
        assert required_field in leaf
        for mutation in ("missing", "extra", "wrong_type", "replacement"):
            changed = deepcopy(root)
            target = None
            for episode in changed["episodes"]:
                for round_row in episode["rounds"]:
                    context = round_row["proposal"].get("proposal_context")
                    if type(context) is not dict:
                        continue
                    source = context.get("source_state")
                    operations = context.get("operation_state")
                    if kind == "policy_row" and type(source) is dict and source.get("policy_rows"):
                        target = source["policy_rows"][0]
                    elif kind == "policy_market" and type(source) is dict and source.get("policy_markets"):
                        target = next(iter(source["policy_markets"].values()))
                    elif kind == "action_result" and type(operations) is dict:
                        target = next((item["result"] for item in operations.get("actions", [])
                                       if type(item) is dict and type(item.get("result")) is dict), None)
                    elif kind == "binding" and type(operations) is dict:
                        target = next((item["binding"] for item in
                                       operations.get("send_dependencies", {}).values()
                                       if type(item) is dict and item.get("state") == "COMMITTED"
                                       and type(item.get("binding")) is dict), None)
                    if target is not None:
                        break
                if target is not None:
                    break
            assert type(target) is dict
            if mutation == "missing":
                target.pop(required_field)
            elif mutation == "extra":
                target["unexpected_leaf"] = True
            elif mutation == "wrong_type":
                target[required_field] = []
            else:
                target.clear()
                target["unexpected"] = True
            with pytest.raises(ValueError):
                gross_schema.gross_observation(changed)

    for kind, leaf, required_field in (
        ("policy_row", row, "product_id"),
        ("policy_market", market, "state"),
        ("action_result", action_result, next(iter(action_result))),
        ("binding", binding, "client_order_id"),
    ):
        reject_leaf_mutations(leaf, required_field)

    recovery_request = _with_minimums(
        _with_later_mark(
            _over_cap_gross_input(policy_end_offset_ms=150_000, settlement_extension_ms=180_000),
            "37.5",
        ),
        {"B-USDT-SWAP": "0.8"},
    )
    _, recovery_endpoint = _dispatch_to_end(recovery_request)
    _validate_actual_report_projection(recovery_request, recovery_endpoint)

    cancel_request = _over_cap_gross_input(
        with_working_order=True, policy_end_offset_ms=150_000,
        settlement_extension_ms=180_000,
    )
    cancel_composition = _ReplayComposition._from_gross_response_run(cancel_request)
    cancel_endpoint = cancel_composition._paid_endpoint
    assert cancel_endpoint is not None and cancel_endpoint.gross_response is not None
    captured_effects = False
    for _ in range(200):
        assert cancel_composition._queue
        cancel_composition._dispatch_due(cancel_composition._queue[0][0])
        facts = cancel_endpoint.gross_response.observations
        if any(row["proposal_context"]["operations"]["cancel_effects"]
               for row in facts if row.get("proposal_context") is not None):
            captured_effects = True
            _validate_actual_report_projection(cancel_request, cancel_endpoint)
            break
    assert captured_effects
    for _ in range(200):
        if cancel_endpoint.cancel_ack_action_by_delivery:
            break
        assert cancel_composition._queue
        cancel_composition._dispatch_due(cancel_composition._queue[0][0])
    assert cancel_endpoint.cancel_ack_action_by_delivery
    _validate_actual_report_projection(cancel_request, cancel_endpoint)

    partial_request = _with_limited_settlement_volume(
        _over_cap_gross_input(settlement_extension_ms=180_000, policy_end_offset_ms=150_000),
        "0.1",
    )
    partial_composition, partial_endpoint = _dispatch_to_end(partial_request)
    _validate_actual_report_projection(
        partial_request, partial_endpoint, endpoint_projection=partial_endpoint.result(partial_composition),
    )

    recovery_request = _over_cap_gross_input(
        settlement_extension_ms=180_000, policy_end_offset_ms=150_000,
    )
    recovery_request = _with_minimums(recovery_request, {"B-USDT-SWAP": "0.8"})
    recovery_request = _with_later_mark(recovery_request, "37.5")
    recovery_composition, recovery_endpoint = _dispatch_to_end(recovery_request)
    _validate_actual_report_projection(
        recovery_request, recovery_endpoint,
        endpoint_projection=recovery_endpoint.result(recovery_composition),
    )

    full_request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    full_composition, full_endpoint = _dispatch_to_end(full_request)
    _validate_actual_report_projection(
        full_request, full_endpoint, endpoint_projection=full_endpoint.result(full_composition),
    )
