"""Actual-native causal tests for the paid cross-artifact consistency owner."""

from copy import deepcopy
import ast
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_execution_evidence as evidence
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes, configuration_context, decode_canonical, decode_jsonl, encode_jsonl, historical_context,
)
from src.core.backtest.spider_execution_input import encode_spider_execution_input, validate_spider_execution_input
from src.core.backtest.spider_historical_input import historical_context_for_input
from src.core.backtest.spider_run_execution_schema import (
    canonical_paid_bytes,
    execution_context_from_input,
)
from src.core.backtest.spider_run_artifacts import _HASH_NAMES
from src.core.backtest import synthetic_scenario_replay as replay_module
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_execution_input import _input
from test_spider_historical_input import (
    _initial_account_state, _p2_configuration, _policy_cache, _rehashed_run,
    _run_with_configuration, _valid_run,
)
from test_spider_paid_endpoint_native import _seeded_two_product_run

H = "a" * 64


def _capture(composition: _ReplayComposition, tag: str, cutoff: int | None = None) -> dict[str, Any]:
    cutoff = composition._current_time if cutoff is None else cutoff
    assert cutoff is not None
    requests = [composition._paid_snapshot_request(kind, cutoff, tag)
                for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS")]
    return composition.capture_owner_evidence(cutoff, *requests)["owner_evidence"]


def _bundle(request=None, *, failure_mode: str | None = None, monkeypatch=None) -> tuple[dict[str, bytes], dict[str, Any]]:
    request = _seeded_two_product_run() if request is None else request
    run_id = request.historical_input.run_id
    composition = _ReplayComposition._from_execution_run(request)
    events: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    captures: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    counter = 0
    fail_current_capture = [False]
    native_codec = composition._codec

    class CaptureOwnerChain:
        def __getattr__(self, name):
            if fail_current_capture[0] and name == "capture_snapshot":
                return lambda *_args: (_ for _ in ()).throw(RuntimeError("injected evidence capture failure"))
            if failure_mode == "binding_lookup" and name == "historical_order_binding":
                return lambda *_args: None
            return getattr(native_codec, name)

        def apply_group(self, group):
            nonlocal counter
            counter += 1
            cutoff = group["group_effective_at"]
            before = _capture(composition, f"EVIDENCE-BEFORE-{counter}", cutoff)
            result = native_codec.apply_group(group)
            after = _capture(composition, f"EVIDENCE-AFTER-{counter}", cutoff)
            captures[group["group_id"]] = before, after
            return result

    cast(Any, composition)._codec = CaptureOwnerChain()
    composition._evidence_callback = lambda kind, key, payload: events.append((kind, key, payload))
    initial = _capture(composition, "EVIDENCE-INITIAL")
    if failure_mode == "source_fault":
        assert composition._dispatch_due(request.policy_end_ms * 16 + 7)["classification"] == "SUCCESS"
        pending = next(record for record in composition._records.values()
                       if record["item"].get("kind") == "SOURCE_GROUP"
                       and record["item"]["group"]["members"][0]["kind"] == "INTENT")
        pending["item"]["group"]["members"][0]["payload"]["config_id"] = "INVALID-CONFIG"
    if failure_mode == "callback_ack":
        from src.core.backtest.synthetic_scenario_replay import _CallbackPrefix

        assert monkeypatch is not None
        original_capture_callback = composition._capture_callback

        def fail_ack(delivery):
            if delivery["payload_kind"] == "TRANSPORT_ACK":
                return _CallbackPrefix((), RuntimeError("injected paid ACK callback failure"))
            return original_capture_callback(delivery)

        monkeypatch.setattr(composition, "_capture_callback", fail_ack)
    if failure_mode == "earn_snapshot_poll":
        from src.core.backtest.synthetic_scenario_replay import _ScheduleError

        assert monkeypatch is not None

        def fail_earn_poll(_delivery):
            raise _ScheduleError("POLL_OCCURRENCE_MISMATCH")

        monkeypatch.setattr(composition, "_resume_poll", fail_earn_poll)
    dispatch = composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)
    expected_dispatch = "TERMINAL" if failure_mode in {
        "source_fault", "binding_lookup", "callback_ack", "earn_snapshot_poll",
    } else "SUCCESS"
    assert dispatch["classification"] == expected_dispatch, (dispatch, composition._terminal)
    if failure_mode == "capture_failure":
        fail_current_capture[0] = True
    endpoint_owner = composition._paid_endpoint
    assert endpoint_owner is not None
    paid = endpoint_owner.result(composition)
    scheduler = composition._public_scheduler_observation()
    paid_observation = endpoint_owner._paid_observation(
        composition, scheduler,
        current_evidence_complete=paid["failure"] is None or paid["failure"]["evidence_error"] is None,
        failure=paid["failure"],
    )
    wrapper = encode_spider_execution_input(request)
    context = execution_context_from_input(wrapper)
    config = cast(dict[str, Any], decode_canonical(request.historical_input.configuration_bytes))
    configured = configuration_context(dict(
        schema_version="spider_configuration_context_v1", config_id=config["config_id"],
        configuration_sha256=request.historical_input.configuration_sha256,
        products=list(request.historical_input.ordered_products),
    ))
    history = historical_context(historical_context_for_input(request.historical_input))
    rows = []
    for kind, key, payload in events:
        if kind == "DELIVERY_ATTEMPT":
            continue
        index = len(rows) + 1
        payload = deepcopy(payload)
        if kind == "SOURCE_GROUP_RESULT":
            group_id = payload["request"]["group_id"]
            if group_id in captures:
                payload["owner_evidence_before"], payload["owner_evidence_after"] = captures[group_id]
            before = payload["result"]["account_version_before"]
            after = payload["result"]["account_version_after"]
            effective = payload["request"]["group_effective_at"]
        elif kind == "HISTORICAL_MARKET_RESULT":
            before = payload["owner_inspection_before"]["account_version"]
            after = payload["owner_inspection_after"]["account_version"]
            effective = payload["result"]["effective_at"]
        elif kind == "SNAPSHOT_FACT":
            before = after = None
            effective = payload["request"]["captured_at"]
        elif kind == "DELIVERY_ATTEMPT":
            before = after = None
            effective = payload["delivery"]["visible_at"]
        else:
            before = after = None
            effective = key["visible_at"]
        rows.append(dict(
            schema_version="spider_execution_journal_record_v1", run_id=run_id,
            journal_seq=index, barrier_id=f"{kind}:{key['stable_id']}:{index}",
            record_kind=kind, scheduler_key=key, causal_parent_ids=[],
            effective_at=effective, visible_at=key["visible_at"],
            account_version_before=before, account_version_after=after,
            payload=payload,
            configuration_context=configured._asdict() | {"products": list(configured.products)},
            historical_context=history._asdict(), execution_context=context,
        ))
    wrapper_hash = validate_spider_execution_input(request)
    attempt = dict(
        schema_version="spider_execution_attempt_v1", run_id=run_id,
        run_contract_id="SPIDER_PAID_RESEARCH_RUN_V1", registration_state="VALIDATED",
        requested_scenario_selector="SPIDER_PAID_RESEARCH_RUN_V1", registration_failure=None,
        input_contract_hashes=[dict(
            name=name,
            sha256=(wrapper_hash if index == 0 else request.historical_input.policy_source_sha256
                    if index == 3 else H),
        )
                               for index, name in enumerate(_HASH_NAMES)],
        planned_coverage=evidence._expected_coverage(request, wrapper_hash),
        profile_id="SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        account_key=dict(venue="SPIDER_HISTORICAL_RESEARCH", environment="RESEARCH_ONLY",
                         account=request.historical_input.account_key),
        scenario_plan_id="SPIDER_PAID_RESEARCH_RUN_V1", scenario_plan_sha256=wrapper_hash,
        program_sha256=H, native_artifact_sha256=H, policy_source_sha256=request.historical_input.policy_source_sha256,
        ordering_contract_id="HISTORICAL_ORDER_V1", cost_contract_id="SPIDER_HISTORICAL_RESEARCH_COSTS_V1",
        funding_exclusion="HISTORICAL_FUNDING_DISABLED", terminal_policy="PAID_CLOSE_RESEARCH_V1",
        artifact_encoding="artifact_encoding_v1",
        configuration_context=configured._asdict() | {"products": list(configured.products)},
        historical_context=history._asdict(), execution_context=context,
    )
    frontier = (dict(ordinal=len(rows), journal_seq=len(rows), barrier_id=rows[-1]["barrier_id"])
                if rows else None)
    status = dict(
        schema_version="spider_execution_status_v1", run_id=run_id,
        # Store finalization succeeded for this bundle. A FAILED endpoint is
        # still a durable, reconciled FAILED_PREFIX rather than a store fault.
        state="COMPLETE",
        processed_boundary=frontier, persisted_boundary=frontier,
        failure_reason=None,
        primary_failure=None,
        configuration_context=attempt["configuration_context"],
        historical_context=attempt["historical_context"], execution_context=context,
    )
    report = dict(
        schema_version="spider_execution_report_v1", run_id=run_id,
        initial_owner_evidence=initial, scheduler_observation=scheduler,
        paid_observation=paid_observation, paid_endpoint=paid,
        source_evidence_refs=[f"journal:{index}" for index in range(1, len(rows) + 1)],
    )
    artifacts = {
        "attempt.json": encode_jsonl([attempt]), "status.json": encode_jsonl([status]),
        "journal.jsonl": encode_jsonl(rows), "execution_input.json": wrapper,
        "paid_endpoint.json": canonical_paid_bytes(paid, context, configured),
        "report.jsonl": encode_jsonl([report]),
    }
    return artifacts, dict(run_id=run_id, request=request, paid=paid, journal=rows, report=report,
                           attempt=attempt, status=status, configured=configured, context=context)


def test_actual_native_paid_flat_bundle_reconciles_and_rebuilds_identically():
    artifacts, facts = _bundle()
    assert facts["paid"]["status"] == "PAID_FLAT"
    run_id = facts["run_id"]
    first = evidence.build_execution_reconciliation(run_id, artifacts)
    assert first["result"] == "OK", [(row["name"], row["result"]) for row in first["checks"]]
    assert first["evidence_scope"] == "COMPLETE_RUN"
    assert [row["name"] for row in first["checks"]] == list(evidence._CHECKS)
    second = evidence.build_execution_reconciliation(run_id, artifacts)
    assert canonical_bytes(first) == canonical_bytes(second)
    artifacts["reconciliation.json"] = encode_jsonl([first])
    assert canonical_bytes(evidence.build_execution_reconciliation(run_id, artifacts)) == canonical_bytes(first)


def test_paid_legacy_shared_delivery_sequences_remain_readable(monkeypatch):
    with monkeypatch.context() as producer_patch:
        producer_patch.setattr(
            replay_module.policy_protocol,
            "historical_delivery_sequence",
            lambda _mode, _kind, counter: counter,
        )
        artifacts, facts = _bundle()
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert facts["paid"]["status"] == "PAID_FLAT"
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]


@pytest.mark.parametrize("with_working_order", [False, True])
def test_paid_mixed_poll_and_native_delivery_modes_are_rejected(with_working_order):
    request = _seeded_two_product_run(with_working_order=with_working_order)
    request = replace(request, policy_end_ms=request.policy_end_ms + 30_000)
    artifacts, facts = _bundle(request)
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    journal = cast(list[dict[str, Any]], decode_jsonl(artifacts["journal.jsonl"]))
    scheduler = report["scheduler_observation"]
    if with_working_order:
        roles = {row["order_id"]: row["role"] for row in report["paid_observation"]["order_roles"]}
        actions_by_group = {row["group_id"]: row for row in facts["paid"]["actions"]
                            if row["group_id"] is not None}
        origins = evidence._paid_delivery_origins(
            journal, facts["request"], roles, actions_by_group,
            evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
        )
        ack_origins = [origin for origin in origins if origin["payload_kind"] == "TRANSPORT_ACK"]
        assert ack_origins
        for origin in ack_origins:
            deliveries = [row for row in scheduler["records"] if row["kind"] == "DELIVERY"
                          and row["key"]["visible_at"] == origin["visible_at"]
                          and row["key"]["schedule_sequence"] == origin["schedule_sequence"]]
            assert len(deliveries) == 1 and deliveries[0]["classification"] == "SUCCESS"
    delivery = None
    capture_sequence = 0
    for poll in scheduler["polls"]:
        poll_prefix, _, poll_counter = poll["poll_id"].partition("_POLL_")
        timer = next(row for row in scheduler["records"]
                     if row["kind"] == "HISTORICAL_TIMER"
                     and row["stable_id"] == f"{poll_prefix}_TIMER_{poll_counter}")
        poll_time = int(poll_counter)
        has_earn = any(row["kind"] == "SNAPSHOT_CAPTURE"
                       and row["stable_id"] == f"{poll['poll_id']}_EARN" for row in scheduler["records"])
        stage = "EARN" if has_earn else "TRADING"
        capture = next((row for row in scheduler["records"]
                        if row["kind"] == "SNAPSHOT_CAPTURE"
                        and row["stable_id"] == f"{poll['poll_id']}_{stage}"
                        and row["classification"] == "SUCCESS"), None)
        if capture is None:
            continue
        capture_sequence = timer["key"]["schedule_sequence"] * 4
        if capture_sequence == 0:
            continue
        delivery_time = (poll_time + 5) * 16 + 6
        delivery = next((row for row in scheduler["records"]
                         if row["kind"] == "DELIVERY"
                         and row["key"]["visible_at"] == delivery_time
                         and row["key"]["schedule_sequence"] == 2 * capture_sequence), None)
        if delivery is not None and delivery["classification"] == "SUCCESS":
            break
        delivery = None
    assert delivery is not None
    old_key = deepcopy(delivery["key"])
    delivery["key"]["schedule_sequence"] = capture_sequence
    for row in journal:
        if row["scheduler_key"] == old_key:
            row["scheduler_key"]["schedule_sequence"] = capture_sequence
    artifacts["report.jsonl"] = encode_jsonl([report])
    artifacts["journal.jsonl"] = encode_jsonl(cast(list[object], journal))

    paid_prefix = f"PAID_{validate_spider_execution_input(facts['request'])[:16]}"
    modes = [evidence._historical_polls_endpoint_mode_compatible(
        scheduler, scheduler["current_time"], mode, paid_prefix,
    ) for mode in evidence._HistoricalSequenceMode]
    assert modes == [False, False]
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed


def test_paid_legacy_poll_with_native_grid_ack_mode_is_rejected(monkeypatch):
    request = _seeded_two_product_run(with_working_order=True)
    request = replace(request, policy_end_ms=request.policy_end_ms + 30_000)
    native_sequence = replay_module.policy_protocol.historical_delivery_sequence
    with monkeypatch.context() as producer_patch:
        producer_patch.setattr(
            replay_module.policy_protocol,
            "historical_delivery_sequence",
            lambda mode, kind, counter: counter if kind is evidence._HistoricalDeliveryKind.POLL
            else native_sequence(mode, kind, counter),
        )
        artifacts, facts = _bundle(request)

    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    scheduler = report["scheduler_observation"]
    assert scheduler["polls"]
    assert evidence._historical_polls_endpoint_mode_compatible(
        scheduler, scheduler["current_time"], evidence._HistoricalSequenceMode.LEGACY_SHARED_SEQUENCE_V1,
        f"PAID_{validate_spider_execution_input(request)[:16]}",
    )
    assert not evidence._historical_polls_endpoint_mode_compatible(
        scheduler, scheduler["current_time"], evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
        f"PAID_{validate_spider_execution_input(request)[:16]}",
    )
    roles = {row["order_id"]: row["role"] for row in report["paid_observation"]["order_roles"]}
    actions_by_group = {row["group_id"]: row for row in facts["paid"]["actions"]
                        if row["group_id"] is not None}
    disjoint_origins = evidence._paid_delivery_origins(
        facts["journal"], request, roles, actions_by_group,
        evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
    )
    ack_origins = [origin for origin in disjoint_origins if origin["payload_kind"] == "TRANSPORT_ACK"]
    assert ack_origins
    for origin in ack_origins:
        deliveries = [row for row in scheduler["records"] if row["kind"] == "DELIVERY"
                      and row["key"]["visible_at"] == origin["visible_at"]
                      and row["key"]["schedule_sequence"] == origin["schedule_sequence"]]
        assert len(deliveries) == 1 and deliveries[0]["classification"] == "SUCCESS"
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed


@pytest.mark.parametrize("offset_ms", [30_000, 59_000])
def test_actual_native_incomplete_exit_and_notice_prefixes_reconcile(offset_ms):
    request = _seeded_two_product_run()
    request = replace(request, policy_end_ms=request.policy_end_ms + offset_ms)
    artifacts, facts = _bundle(request)
    assert facts["paid"]["status"] == "INCOMPLETE"
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]
    assert result["evidence_scope"] == "COMPLETE_RUN"


def test_native_pending_notices_cannot_be_erased_from_both_paid_projections():
    request = _seeded_two_product_run()
    request = replace(request, policy_end_ms=request.policy_end_ms + 30_000)
    artifacts, facts = _bundle(request)
    assert facts["paid"]["status"] == "INCOMPLETE"
    assert len(facts["paid"]["pending_obligations"]["pending_deliveries"]) == 2
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    report["paid_observation"]["pending_paid_deliveries"] = []
    report["paid_endpoint"]["pending_obligations"]["pending_deliveries"] = []
    artifacts["paid_endpoint.json"] = canonical_paid_bytes(
        report["paid_endpoint"], facts["context"], facts["configured"],
    )
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed

    for field, value in (("order_id", "NOT-A-NATIVE-ORDER"),
                         ("cumulative_filled_size_contracts", D("999"))):
        artifacts, facts = _bundle(request)
        report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
        delivery = report["paid_observation"]["pending_paid_deliveries"][0]["delivery"]
        delivery["immutable_payload"][field] = value
        delivery["payload_digest"] = sha256(canonical_bytes(delivery["immutable_payload"])).hexdigest()
        artifacts["report.jsonl"] = encode_jsonl([report])
        result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
        failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
        assert "ACTION_BINDINGS" in failed


def test_native_pending_cancel_ack_is_an_independent_paid_obligation():
    run = _seeded_two_product_run(with_working_order=True)
    request = replace(run, policy_end_ms=run.historical_input.range_end_ms - 5_002)
    artifacts, facts = _bundle(request)
    assert facts["paid"]["status"] == "INCOMPLETE"
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    acknowledgements = [item for item in report["paid_observation"]["pending_paid_deliveries"]
                        if item["delivery"]["payload_kind"] == "TRANSPORT_ACK"]
    assert len(acknowledgements) == 1
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]

    ack_key = acknowledgements[0]["key"]
    report["paid_observation"]["pending_paid_deliveries"] = [
        item for item in report["paid_observation"]["pending_paid_deliveries"]
        if item["key"] != ack_key
    ]
    endpoint = cast(dict[str, Any], decode_jsonl(artifacts["paid_endpoint.json"])[0])
    endpoint["pending_obligations"]["pending_deliveries"] = [
        key for key in endpoint["pending_obligations"]["pending_deliveries"] if key != ack_key
    ]
    report["paid_endpoint"] = endpoint
    artifacts["paid_endpoint.json"] = canonical_paid_bytes(
        endpoint, facts["context"], facts["configured"],
    )
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed

    artifacts, facts = _bundle(request)
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    report["paid_observation"]["pending_paid_deliveries"][0]["cancel_action_id"] = "UNBOUND-ACTION"
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed


def _rewrite_jsonl(raw: bytes, transform) -> bytes:
    rows = decode_jsonl(raw)
    assert len(rows) == 1
    return encode_jsonl([transform(deepcopy(rows[0]))])


def _sync_endpoint(artifacts: dict[str, bytes], facts: dict[str, Any], endpoint: dict[str, Any]) -> None:
    artifacts["paid_endpoint.json"] = canonical_paid_bytes(
        endpoint, facts["context"], facts["configured"],
    )
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    report["paid_endpoint"] = endpoint
    artifacts["report.jsonl"] = encode_jsonl([report])


@pytest.mark.parametrize(
    ("filename", "transform", "expected_check"),
    [
        ("attempt.json", lambda row: row | {"run_id": "different-run"}, "INPUT_IDENTITY"),
        ("attempt.json", lambda row: row | {
            "input_contract_hashes": [
                *row["input_contract_hashes"][:1],
                row["input_contract_hashes"][1] | {"sha256": "c" * 64},
                *row["input_contract_hashes"][2:],
            ],
        }, "INPUT_IDENTITY"),
        ("status.json", lambda row: row | {"processed_boundary": None, "persisted_boundary": None},
         "JOURNAL_FRONTIER"),
        ("report.jsonl", lambda row: row | {
            "initial_owner_evidence": row["initial_owner_evidence"] | {
                "cutoff": row["initial_owner_evidence"]["cutoff"] + 1,
            },
        }, "OWNER_SCOPE_AND_CHAIN"),
        (
            "paid_endpoint.json",
            lambda row: row | {"status": "INCOMPLETE"},
            "ENDPOINT_CLASSIFICATION",
        ),
    ],
)
def test_semantic_mutations_fail_their_reconciliation_check(filename, transform, expected_check):
    artifacts, facts = _bundle()
    if filename == "report.jsonl":
        artifacts[filename] = _rewrite_jsonl(artifacts[filename], transform)
    else:
        artifacts[filename] = _rewrite_jsonl(artifacts[filename], transform)
        if filename == "paid_endpoint.json":
            endpoint = cast(dict[str, Any], decode_jsonl(artifacts[filename])[0])
            artifacts[filename] = canonical_paid_bytes(endpoint, facts["context"], facts["configured"])
            report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
            report["paid_endpoint"] = endpoint
            artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert expected_check in failed


def test_report_projection_and_snapshot_digest_mutations_are_not_self_authenticated():
    artifacts, facts = _bundle()
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    report["source_evidence_refs"] = report["source_evidence_refs"][:-1]
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "JOURNAL_FRONTIER" in failed
    assert "REPORT_PROJECTION" in failed

    artifacts, facts = _bundle()
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    report["initial_owner_evidence"]["positions_fact"]["payload_digest"] = "b" * 64
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "SNAPSHOT_DIGESTS" in failed


@pytest.mark.parametrize(
    ("which", "expected_check"),
    [("after", "SNAPSHOT_DIGESTS"), ("before", "OWNER_SCOPE_AND_CHAIN")],
)
def test_source_group_owner_captures_are_validated_not_merely_chained(which, expected_check):
    artifacts, facts = _bundle()
    rows = cast(list[dict[str, Any]], decode_jsonl(artifacts["journal.jsonl"]))
    target = next(row for row in rows if row["record_kind"] == "SOURCE_GROUP_RESULT")
    owner = cast(dict[str, Any], target["payload"][f"owner_evidence_{which}"])
    if which == "after":
        owner["open_orders_fact"]["payload_digest"] = "b" * 64
    else:
        owner["inspection"]["owner_state_digest"] = "b" * 64
    artifacts["journal.jsonl"] = encode_jsonl(cast(list[object], rows))
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert expected_check in failed


def test_action_binding_mutation_fails_against_authoritative_native_journal():
    artifacts, facts = _bundle()
    endpoint = cast(dict[str, Any], decode_jsonl(artifacts["paid_endpoint.json"])[0])
    endpoint["actions"][0]["group_id"] = "unobserved-native-group"
    artifacts["paid_endpoint.json"] = canonical_paid_bytes(
        endpoint, facts["context"], facts["configured"],
    )
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    report["paid_endpoint"] = endpoint
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed


@pytest.mark.parametrize("field,value", [("order_id", "NOT-A-NATIVE-ORDER"),
                                         ("parent_action_id", "NONEXISTENT")])
def test_paid_action_identity_and_parent_must_match_native_request(field, value):
    artifacts, facts = _bundle()
    endpoint = cast(dict[str, Any], decode_jsonl(artifacts["paid_endpoint.json"])[0])
    action = next(row for row in endpoint["actions"] if row["role"] == "EXIT")
    action[field] = value
    _sync_endpoint(artifacts, facts, endpoint)
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed


def test_paid_actions_must_remain_in_native_admission_order():
    artifacts, facts = _bundle()
    endpoint = cast(dict[str, Any], decode_jsonl(artifacts["paid_endpoint.json"])[0])
    endpoint["actions"] = list(reversed(endpoint["actions"]))
    _sync_endpoint(artifacts, facts, endpoint)
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "ACTION_BINDINGS" in failed


@pytest.mark.parametrize("failure_mode", ["source_fault", "binding_lookup", "callback_ack", "capture_failure"])
def test_actual_native_failed_prefixes_keep_terminal_facts_and_reconcile(failure_mode, monkeypatch):
    request = _seeded_two_product_run(with_working_order=failure_mode == "callback_ack")
    artifacts, facts = _bundle(request, failure_mode=failure_mode, monkeypatch=monkeypatch)
    assert facts["paid"]["status"] == "FAILED"
    if failure_mode == "capture_failure":
        assert facts["paid"]["failure"]["primary"]["kind"] == "OWNER_EVIDENCE"
        assert facts["paid"]["failure"]["evidence_error"]["reason"] == "injected evidence capture failure"
        assert facts["paid"]["failure"]["inspection_error"] is None
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["evidence_scope"] == "FAILED_PREFIX"
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]


def _initial_capture_failure_bundle(monkeypatch):
    request = _seeded_two_product_run()
    template, facts = _bundle(request)
    composition = _ReplayComposition._from_execution_run(request)

    def fail_initial_capture(*_args):
        raise RuntimeError("injected initial owner capture failure")

    monkeypatch.setattr(composition, "capture_owner_evidence", fail_initial_capture)
    endpoint_owner = composition._paid_endpoint
    assert endpoint_owner is not None
    endpoint = endpoint_owner.result(composition)
    scheduler = composition._public_scheduler_observation()
    paid_observation = endpoint_owner._paid_observation(
        composition, scheduler, current_evidence_complete=False, failure=endpoint["failure"],
    )
    assert endpoint["status"] == "FAILED" and endpoint["owner_evidence"] is None
    assert endpoint["failure"]["primary"]["kind"] == "OWNER_EVIDENCE"
    assert endpoint["failure"]["evidence_error"] is not None
    assert scheduler["current_time"] == request.historical_input.range_start_ms * 16
    assert scheduler["last_popped"] is scheduler["terminal"] is None
    attempt = cast(dict[str, Any], decode_jsonl(template["attempt.json"])[0])
    status = cast(dict[str, Any], decode_jsonl(template["status.json"])[0])
    status.update(state="RUNNING", processed_boundary=None, persisted_boundary=None,
                  failure_reason=None, primary_failure=None)
    report = dict(
        schema_version="spider_execution_report_v1", run_id=facts["run_id"],
        initial_owner_evidence=None, scheduler_observation=scheduler,
        paid_observation=paid_observation, paid_endpoint=endpoint, source_evidence_refs=[],
    )
    artifacts = {
        "attempt.json": encode_jsonl([attempt]), "status.json": encode_jsonl([status]),
        "journal.jsonl": b"", "execution_input.json": template["execution_input.json"],
        "paid_endpoint.json": canonical_paid_bytes(endpoint, facts["context"], facts["configured"]),
        "report.jsonl": encode_jsonl([report]),
    }
    return artifacts, facts, endpoint


def test_actual_initial_capture_failure_is_a_truthful_zero_journal_prefix(monkeypatch):
    artifacts, facts, endpoint = _initial_capture_failure_bundle(monkeypatch)
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert endpoint["status"] == "FAILED"
    assert result["evidence_scope"] == "FAILED_PREFIX"
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]
    assert artifacts["journal.jsonl"] == b""
    status = decode_jsonl(artifacts["status.json"])[0]
    assert status["processed_boundary"] is status["persisted_boundary"] is None


@pytest.mark.parametrize("mutation", ["progress", "wrong_cut", "missing_fault", "erase_pending"])
def test_zero_journal_prefix_rejects_fabricated_initial_evidence(mutation, monkeypatch):
    artifacts, facts, endpoint = _initial_capture_failure_bundle(monkeypatch)
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    if mutation == "progress":
        report["scheduler_observation"]["records"][0]["classification"] = "SUCCESS"
    elif mutation == "wrong_cut":
        report["scheduler_observation"]["current_time"] += 16
        report["paid_observation"]["observed_at"] += 16
        endpoint["failure"]["observed_at"] += 16
        _sync_endpoint(artifacts, facts, endpoint)
        report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
        report["scheduler_observation"]["current_time"] += 16
        report["paid_observation"]["observed_at"] += 16
    elif mutation == "missing_fault":
        endpoint["failure"]["primary"]["kind"] = "SCHEDULER"
        _sync_endpoint(artifacts, facts, endpoint)
        report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    else:
        report["scheduler_observation"]["records"].pop()
        report["scheduler_observation"]["pending_keys"].pop()
    artifacts["report.jsonl"] = encode_jsonl([report])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
    assert "JOURNAL_FRONTIER" in failed


def test_actual_native_earn_snapshot_terminal_without_callback_result_reconciles(monkeypatch):
    run = _valid_run()
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 30_000)
    artifacts, facts = _bundle(request, failure_mode="earn_snapshot_poll", monkeypatch=monkeypatch)
    assert facts["paid"]["status"] == "FAILED"
    report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
    terminal = report["paid_observation"]["terminal_fact"]
    assert terminal is not None
    assert terminal["kind"] == "DELIVERY"
    assert terminal["delivery"]["payload_kind"] == "EARN_SNAPSHOT"
    assert terminal["role"] is None and terminal["cancel_action_id"] is None
    assert not any(row["record_kind"] == "CALLBACK_RESULT"
                   and row["scheduler_key"] == terminal["key"] for row in facts["journal"])
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]

    mutations = (
        ("reference", lambda delivery: delivery.__setitem__("source_fact_id", "UNLINKED-SNAPSHOT")),
        ("continuation", lambda delivery: delivery.__setitem__("continuation_id", "UNLINKED-CONTINUATION")),
        ("payload", lambda delivery: delivery["immutable_payload"].__setitem__("earn", "999")),
    )
    for _name, mutate in mutations:
        artifacts, facts = _bundle(request, failure_mode="earn_snapshot_poll", monkeypatch=monkeypatch)
        report = cast(dict[str, Any], decode_jsonl(artifacts["report.jsonl"])[0])
        mutate(report["paid_observation"]["terminal_fact"]["delivery"])
        artifacts["report.jsonl"] = encode_jsonl([report])
        result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
        failed = {row["name"] for row in result["checks"] if row["result"] == "FAILED"}
        assert "ACTION_BINDINGS" in failed, _name


def _liquidation_request(cash: str):
    run = _valid_run()
    config = _p2_configuration()
    config["cash"] = cash
    config["positions"] = [dict(
        product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
        lots=[dict(seed_execution_id="EVIDENCE-LIQUIDATION", seed_sequence=0,
                   strategy_id=run.strategy_identity, quantity_contracts="1", entry_price="100")],
    )]
    run = _run_with_configuration(run, config)
    run = replace(run, initial_account_state=_initial_account_state(
        cash=cash, positions=config["positions"],
    ), initial_policy_cache=_policy_cache(running=False))
    trade = tuple(replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
                  if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
                  for row in run.trade_bars)
    marks = tuple(replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
                  if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
                  for row in run.mark_bars)
    return replace(_input(_rehashed_run(run, trade_rows=trade, mark_rows=marks)),
                   policy_end_ms=run.range_start_ms + 5_000)


@pytest.mark.parametrize("cash,status", [("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT")])
def test_actual_native_liquidation_results_reconcile_without_paid_exit(cash, status):
    artifacts, facts = _bundle(_liquidation_request(cash))
    assert facts["paid"]["status"] == status
    result = evidence.build_execution_reconciliation(facts["run_id"], artifacts)
    assert result["result"] == "OK", [(row["name"], row["result"]) for row in result["checks"]]


def _forbidden_imports(tree: ast.AST) -> set[str]:
    forbidden_modules = {
        "synthetic_scenario_replay", "spider_run_store", "spider_scenario_run",
        "spider_formal_runner", "pathlib", "os", "sqlite3", "ccxt", "requests",
        "httpx", "aiohttp", "fluxtrade_core",
    }
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.update(f"{node.module}.{alias.name}" for alias in node.names)
    return {name for name in found if any(part in forbidden_modules for part in name.split("."))}


def test_paid_evidence_owner_forbids_replay_store_and_io_dependencies():
    source = Path(evidence.__file__).read_text(encoding="utf-8")
    assert _forbidden_imports(ast.parse(source)) == set()
    probe = ast.parse(
        "from src.core.backtest import synthetic_scenario_replay, spider_run_store"
    )
    assert _forbidden_imports(probe) == {
        "src.core.backtest.synthetic_scenario_replay",
        "src.core.backtest.spider_run_store",
    }
