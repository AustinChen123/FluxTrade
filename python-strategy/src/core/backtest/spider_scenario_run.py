"""Bounded single-run orchestration over the existing durability and runtime owners."""

from copy import deepcopy as _copy
from decimal import Decimal as _Decimal
from hashlib import sha256 as _sha256
import logging as _logging
from pathlib import Path as _Path
from typing import Any as _Any, cast as _cast

from src.core.backtest import spider_policy_protocol as _protocol, spider_scenario_plans as _plans, synthetic_scenario_codec as _wire
from src.core.backtest.spider_run_artifacts import _HASHES, _HASH_NAMES, _IDENTITIES, _configuration_context as _context_payload, _object, _require, _text, canonical_bytes as _bytes, canonical_decimal_text as _decimal_text, decode_canonical as _decode, encode_jsonl as _jsonl, validate_artifact as _validate
from src.core.backtest.spider_run_envelope_schema import endpoint as _endpoint, journal as _journal
from src.core.backtest.spider_run_evidence import ReconciliationProjectionError as _ProjectionError, build_endpoint_artifacts as _build
from src.core.backtest.spider_run_admission import admit_spider_run as _admit
from src.core.backtest.spider_run_store import SpiderRunStore as _Store, SpiderRunStoreError as _StoreError
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition, ReplayPersistenceError as _PersistenceError, _ScheduleError
from src.core.backtest.spider_paid_endpoint import _classify_paid_endpoint_result as _classify_paid, _error as _paid_error
from src.core.backtest.spider_run_execution_schema import canonical_paid_bytes as _paid_bytes, execution_context_from_input as _execution_context_from_input, validate_execution_artifact as _validate_execution_artifact
from src.core.backtest.spider_run_execution_schema import floating_exit_context_from_input as _floating_context_from_input
from src.core.backtest.spider_run_execution_evidence import build_execution_reconciliation as _build_execution_reconciliation, _expected_coverage as _paid_coverage
from src.core.backtest.spider_execution_input import decode_spider_execution_input as _decode_execution_input, validate_spider_execution_input as _validate_execution_input
from src.core.backtest.spider_execution_input import encode_spider_execution_input as _encode_execution_input
from src.core.backtest.spider_floating_exit_input import (
    decode_spider_floating_exit_input as _decode_floating_input,
    validate_spider_floating_exit_input as _validate_floating_input,
)
from src.core.backtest.spider_historical_input import (
    SCHEMA_ID as _P3_SELECTOR,
    HistoricalInputError as _HistoricalInputError,
    HistoricalRunInput as _HistoricalRunInput,
    decode_historical_run_input as _decode_historical_input,
    decode_p2_configuration as _decode_p2_configuration,
    historical_context_for_input as _historical_context_for_input,
    historical_planned_coverage as _historical_planned_coverage,
    validate_historical_input as _validate_historical_input,
    encode_historical_run_input as _encode_historical_input,
)
from src.core.backtest.spider_run_artifacts import (
    configuration_context as _configuration_context,
    historical_context as _historical_context,
)

_ROOT = _Path(__file__).resolve().parents[4]
_PROGRAM = tuple("python-strategy/src/core/backtest/" + name + ".py" for name in (
    "synthetic_scenario_codec", "synthetic_scenario_replay", "synthetic_policy_dispatch", "spider_policy", "spider_policy_protocol", "spider_run_admission",
    "spider_run_artifacts", "spider_run_completion_schema", "spider_run_envelope_schema", "spider_run_evidence",
    "spider_run_native_schema", "spider_run_reconciliation_schema", "spider_run_store", "spider_scenario_plan_liquidation",
    "spider_scenario_plan_o03", "spider_scenario_plan_scheduled", "spider_scenario_plans", "spider_scenario_run",
    "spider_execution_input", "spider_paid_endpoint"))
_POLICY = "eb6ab34d8685fb59e286f5ffda8af24cbecf3e5ab2c729c585ccdca797cac336"
_P3_POLICY_SOURCE = "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec"
_P2_SELECTOR = "SPIDER_P2_CONFIGURED_SCALE_V1"
_P2_PLAN_SHA256 = "8235c952a5d199825a2a77842b03f49e213ef707c0f30b24d8bf623fde53c2b3"
_P2_CONFIG_SHA256 = "807054044bdd182274509535ecf8bbc4598f00b6a92b598c6228129a6ff11b70"
_P2_RUN_CONTRACT = "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1"
_P3_RUN_CONTRACT = "SPIDER_HISTORICAL_RESEARCH_RUN_V1"
_P3_PROGRAM = (*_PROGRAM, "python-strategy/src/core/backtest/spider_historical_input.py",
               "python-strategy/src/core/backtest/spider_historical_replay.py")
_PAID_SELECTOR = "SPIDER_PAID_RESEARCH_RUN_V1"
_PAID_RUN_CONTRACT = _PAID_SELECTOR
_PAID_PROGRAM = (*_P3_PROGRAM,
                 "python-strategy/src/core/backtest/spider_run_execution_schema.py",
                 "python-strategy/src/core/backtest/spider_run_execution_evidence.py",
                 "python-strategy/src/core/backtest/spider_formal_runner.py",
                 "python-strategy/src/core/backtest_runner.py",
                 "python-strategy/src/core/research_backtest_runner.py",
                 "python-strategy/examples/run_spider_scenario_replay.py")
_FLOATING_SELECTOR = "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1"
_FLOATING_PROGRAM = tuple(dict.fromkeys((*_PAID_PROGRAM, *(
    "python-strategy/src/core/backtest/" + name + ".py" for name in (
        "spider_bayes_model", "spider_bayes_replay", "spider_floating_exit_input",
        "spider_floating_exit_host", "spider_floating_exit_policy",
    )
))))
_LOGGER = _logging.getLogger(__name__)


def _paid_durability_unknown(error: Exception) -> bool:
    if not isinstance(error, _StoreError):
        return False
    if error.reason == "PUBLICATION_DURABILITY_UNKNOWN":
        return True
    primary = error.primary_failure
    return (
        error.reason == "PERSISTENCE_FAILED"
        and isinstance(primary, dict)
        and primary.get("kind") == "PERSISTENCE"
        and primary.get("reason") == "PUBLICATION_DURABILITY_UNKNOWN"
    )


def _normalized(value):
    return _cast(_Any, _decode(_bytes(value)))


def _empty(delivery):
    return dict(delivery_id=delivery["delivery_id"], expected_policy_events=[], financial_items=[], market_requests=[])


def _configuration(selector, run_id, select):
    if selector == _P2_SELECTOR:
        plan = _cast(dict[str, _Any], select(selector))
        _object(plan, "schema_version scenario_plan_id native_profile account_key terminal_policy initial_cutoff final_cutoff configuration configuration_sha256 products recipe callback_plans planned_barriers")
        _require(plan["schema_version"] == "spider_scenario_plan_v2" and plan["scenario_plan_id"] == selector)
        _require(_sha256(_bytes(plan)).hexdigest() == _P2_PLAN_SHA256)
        configuration = plan["configuration"]
        _require(_sha256(_bytes(configuration)).hexdigest() == plan["configuration_sha256"] == _P2_CONFIG_SHA256)
        _require(plan["products"] == [product["product_id"] for product in configuration["products"]])
        _require(plan["native_profile"] == "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1" and plan["initial_cutoff"] == 500 and plan["final_cutoff"] == 100024)
        context = _context_payload(dict(schema_version="spider_configuration_context_v1", config_id=configuration["config_id"],
                                         configuration_sha256=plan["configuration_sha256"], products=plan["products"]))
        result = {"plan": plan, "configuration_context": context}
        program = _sha256()
        for name in sorted((*_PROGRAM, "python-strategy/src/core/backtest/spider_configured_scale_input.py")):
            raw = name.encode()
            program.update(len(raw).to_bytes(8, "big") + raw + _sha256((_ROOT / name).read_bytes()).digest())
        native = _sha256(_Path(_wire.loaded_native_artifact_path()).read_bytes()).hexdigest()
        policy = _POLICY
        return result, [_P2_PLAN_SHA256, program.hexdigest(), native, policy]
    bundle = _cast(dict[str, _Any], select(selector))
    _object(bundle, "plan plan_sha256 journal endpoint report", "native_identity_vectors" if selector == _plans.PLAN_IDS[2] else "")
    plan = bundle["plan"]
    _object(plan, "schema_version scenario_plan_id native_profile account_key initial_cutoff final_cutoff terminal_policy source_groups deliveries planned_coverage")
    _require(plan["schema_version"] == "spider_scenario_plan_v1" and plan["scenario_plan_id"] == selector)
    _require(_sha256(_bytes(plan)).hexdigest() == bundle["plan_sha256"])
    rows = bundle["journal"]
    for row in [*rows, bundle["endpoint"]]:
        row["run_id"] = run_id
    _journal(rows)
    _endpoint(bundle["endpoint"])
    sources = [row for row in rows if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    expected_kinds = ["SOURCE_GROUP_RESULT"] * len(sources) + (["DELIVERY_ATTEMPT", "CALLBACK_RESULT"] if selector == _plans.CLI_PLAN_IDS[0] else [])
    _require([row["record_kind"] for row in rows] == expected_kinds and bool(sources))
    _require(plan["planned_coverage"] == [dict(ordinal=row["journal_seq"], barrier_id=row["barrier_id"], record_kind=row["record_kind"]) for row in rows])
    _require(plan["source_groups"] == [dict(schedule_sequence=row["scheduler_key"]["schedule_sequence"], group_id=row["payload"]["request"]["group_id"],
                                         group_sha256=row["payload"]["result"]["group_digest"]) for row in sources])
    deliveries = [row["payload"]["delivery"] for row in rows if row["record_kind"] == "DELIVERY_ATTEMPT"]
    _require(plan["deliveries"] == [dict(schedule_sequence=d["schedule_sequence"], delivery_id=d["delivery_id"],
                                       emission_plan_sha256=_protocol._emission_plan_digest(_empty(d))) for d in deliveries])
    _require(plan["initial_cutoff"] == bundle["endpoint"]["initial_owner_evidence"]["cutoff"]
             and plan["final_cutoff"] == bundle["endpoint"]["final_owner_evidence"]["cutoff"])
    program = _sha256()
    for name in sorted(_PROGRAM):
        raw = name.encode()
        program.update(len(raw).to_bytes(8, "big") + raw + _sha256((_ROOT / name).read_bytes()).digest())
    native = _sha256(_Path(_wire.loaded_native_artifact_path()).read_bytes()).hexdigest()
    policy = _POLICY
    return bundle, [bundle["plan_sha256"], program.hexdigest(), native, policy]


def _historical_configuration(raw: bytes, run_id: str):
    if type(raw) is not bytes:
        raise _HistoricalInputError("historical input bytes are required")
    run = _decode_historical_input(raw)
    if run.run_id != run_id:
        raise _HistoricalInputError("historical run id does not match invocation")
    semantic_hash = _validate_historical_input(run)
    policy = _P3_POLICY_SOURCE
    if policy != _P3_POLICY_SOURCE or policy != run.policy_source_sha256:
        raise _HistoricalInputError("current policy source does not match historical input")
    configuration = _decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products,
    )
    context = _configuration_context(dict(
        schema_version="spider_configuration_context_v1",
        config_id=configuration["config_id"],
        configuration_sha256=run.configuration_sha256,
        products=list(run.ordered_products),
    ))._asdict()
    context["products"] = list(context["products"])
    history = _historical_context(_historical_context_for_input(run))._asdict()
    plan = dict(
        schema_version="spider_historical_research_run_v1",
        scenario_plan_id=_P3_SELECTOR,
        native_profile="SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        account_key=dict(venue="SPIDER_HISTORICAL_RESEARCH", environment="RESEARCH_ONLY",
                         account=run.account_key),
        terminal_policy="MTM_PRESERVE_OPEN_V1",
        planned_coverage=_historical_planned_coverage(run),
    )
    bundle = dict(plan=plan, configuration_context=context, historical_context=history)
    program = _sha256()
    for name in sorted(_P3_PROGRAM):
        encoded = name.encode()
        program.update(len(encoded).to_bytes(8, "big") + encoded + _sha256((_ROOT / name).read_bytes()).digest())
    native = _sha256(_Path(_wire.loaded_native_artifact_path()).read_bytes()).hexdigest()
    return run, bundle, [semantic_hash, program.hexdigest(), native, policy]


def _paid_configuration(raw: bytes, run_id: str):
    request = _decode_execution_input(raw)
    if request.historical_input.run_id != run_id:
        raise _HistoricalInputError("execution run id does not match invocation")
    wrapper_hash = _validate_execution_input(request)
    run_raw = _encode_historical_input(request.historical_input)
    run, bundle, base_hashes = _historical_configuration(run_raw, run_id)
    program = _sha256()
    if len(_PAID_PROGRAM) != len(set(_PAID_PROGRAM)):
        raise RuntimeError("DUPLICATE_PROGRAM_SOURCE")
    for name in sorted(_PAID_PROGRAM):
        encoded = name.encode()
        program.update(len(encoded).to_bytes(8, "big") + encoded + _sha256((_ROOT / name).read_bytes()).digest())
    hashes = [wrapper_hash, program.hexdigest(), base_hashes[2], base_hashes[3]]
    attempt = _attempt(run_id, _P3_SELECTOR, bundle, base_hashes, _P3_RUN_CONTRACT)
    execution = _execution_context_from_input(raw)
    attempt.update(
        **dict(zip(_HASHES, hashes, strict=True)),
        input_contract_hashes=[dict(name=name, sha256=value) for name, value in zip(_HASH_NAMES, hashes, strict=True)],
        schema_version="spider_execution_attempt_v1",
        run_contract_id=_PAID_RUN_CONTRACT,
        requested_scenario_selector=_PAID_SELECTOR,
        scenario_plan_id=_PAID_SELECTOR,
        terminal_policy="PAID_CLOSE_RESEARCH_V1",
        execution_context=execution,
        planned_coverage=_paid_coverage(request, wrapper_hash),
    )
    _validate_execution_artifact("attempt.json", _bytes(attempt) + b"\n", execution_context=execution)
    return request, bundle, attempt


def _floating_configuration(raw: bytes, run_id: str):
    request = _decode_floating_input(raw)
    if request.execution_input.historical_input.run_id != run_id:
        raise _HistoricalInputError("floating-exit run id does not match invocation")
    contract_hash = _validate_floating_input(request)
    base_raw = _encode_execution_input(request.execution_input)
    _, bundle, attempt = _paid_configuration(base_raw, run_id)
    program = _sha256()
    if len(_FLOATING_PROGRAM) != len(set(_FLOATING_PROGRAM)):
        raise RuntimeError("DUPLICATE_PROGRAM_SOURCE")
    for name in sorted(_FLOATING_PROGRAM):
        encoded = name.encode()
        program.update(len(encoded).to_bytes(8, "big") + encoded + _sha256((_ROOT / name).read_bytes()).digest())
    hashes = [contract_hash, program.hexdigest(), attempt["native_artifact_sha256"], _P3_POLICY_SOURCE]
    execution = _floating_context_from_input(raw)
    attempt.update(
        **dict(zip(_HASHES, hashes, strict=True)),
        input_contract_hashes=[dict(name=name, sha256=value) for name, value in zip(_HASH_NAMES, hashes, strict=True)],
        schema_version="spider_floating_exit_attempt_v1",
        run_contract_id=_FLOATING_SELECTOR,
        requested_scenario_selector=_FLOATING_SELECTOR,
        scenario_plan_id=_FLOATING_SELECTOR,
        terminal_policy="PAID_CLOSE_RESEARCH_V1",
        execution_context=execution,
        planned_coverage=_paid_coverage(request.execution_input, contract_hash),
    )
    _validate_execution_artifact("attempt.json", _bytes(attempt) + b"\n", execution_context=execution)
    return request, bundle, attempt


def _attempt(run_id, selector, bundle=None, hashes=(), rejected_contract="SPIDER_SYNTHETIC_P1_RUN_V1"):
    row = dict(schema_version="spider_attempt_v1", run_id=run_id, run_contract_id=rejected_contract,
               registration_state="REJECTED", requested_scenario_selector=selector, registration_failure="UNSUPPORTED_CONFIGURATION",
               input_contract_hashes=[], planned_coverage=[], **dict.fromkeys(_IDENTITIES))
    if bundle is not None:
        plan = bundle["plan"]
        configured = plan["schema_version"] == "spider_scenario_plan_v2"
        historical = plan["schema_version"] == "spider_historical_research_run_v1"
        row.update(registration_state="VALIDATED", registration_failure=None, profile_id=plan["native_profile"], account_key=plan["account_key"],
                   scenario_plan_id=selector,
                   ordering_contract_id="HISTORICAL_ORDER_V1" if historical else "S_order_v1",
                   cost_contract_id="SPIDER_HISTORICAL_RESEARCH_COSTS_V1" if historical else "SPIDER_SYNTHETIC_COSTS_V1",
                   funding_exclusion=("HISTORICAL_FUNDING_DISABLED" if historical else
                                      "SYNTHETIC_P2_NO_FUNDING_INPUT_OR_CLAIM" if configured else
                                      "SYNTHETIC_P1_NO_FUNDING_INPUT_OR_CLAIM"),
                   terminal_policy=plan["terminal_policy"],
                   planned_coverage=([dict(ordinal=item["ordinal"], barrier_id=item["barrier_id"], record_kind=item["record_kind"]) for item in plan["planned_barriers"]]
                                     if configured else plan["planned_coverage"]), artifact_encoding="artifact_encoding_v1",
                   **dict(zip(_HASHES, hashes, strict=True)))
        if configured:
            row["run_contract_id"] = _P2_RUN_CONTRACT
            row["configuration_context"] = bundle["configuration_context"]
        elif historical:
            row["run_contract_id"] = _P3_RUN_CONTRACT
            row["configuration_context"] = bundle["configuration_context"]
            row["historical_context"] = bundle["historical_context"]
        row["input_contract_hashes"] = [dict(name=name, sha256=value) for name, value in zip(_HASH_NAMES, hashes, strict=True)]
    _validate(row)
    return row


def _primary(kind, payload):
    if kind == "SOURCE_GROUP_RESULT" and payload["result"]["classification"] == "FAULT":
        return dict(kind="NATIVE", reason=payload["result"]["failure"])
    failure = payload.get("failure") if kind == "CALLBACK_RESULT" else None
    return dict(kind="SCHEDULER" if failure["kind"] == "PLAN" else failure["kind"], reason=failure["reason"]) if failure else None


def _terminal(result, closed):
    if closed:
        return "PERSISTENCE_FAILED", None
    if "native_failure" in result or result.get("group_result", {}).get("classification") == "FAULT":
        return "NATIVE_FAULT", dict(kind="NATIVE", reason=result["reason"])
    callback = result["reason"] == "CALLBACK_FAILED"
    return ("CALLBACK_FAILED" if callback else "SCHEDULER_FAILED"), dict(kind="CALLBACK" if callback else "SCHEDULER", reason=result["reason"])


def _owner_requests(account, cutoff, prefix):
    return [dict(schema_version="snapshot_request_v1", account_key=account,
                 snapshot_id=f"{prefix}-{kind.replace('_', '-')}", snapshot_kind=kind,
                 capture_mode="OWNER_CURRENT", captured_at=cutoff)
            for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS")]


def _configured_effective_times(plan):
    source, snapshots = {}, {}
    for step in plan["recipe"]:
        if step["kind"] == "SOURCE_GROUP":
            stamp = step["group"]["members"][0]["stamp"]
            source[stamp["event_id"]] = stamp["effective_at"]
        elif step["kind"] == "POLL_BEGIN":
            for name in ("trading", "positions", "open_orders"):
                request = step["plan"][name]["snapshot_request"]
                snapshots[request["snapshot_id"]] = request["captured_at"]
    return source, snapshots


def _execute_configured(store, bundle, attempt):
    plan = bundle["plan"]
    barriers = plan["planned_barriers"]
    context = attempt["configuration_context"]
    callback_plans = {row["delivery_id"]: row for row in plan["callback_plans"]}
    source_times, snapshot_times = _configured_effective_times(plan)
    journal, previous, closed = [], None, False

    def capture(cutoff, prefix):
        return _normalized(owner.capture_owner_evidence(cutoff, *_owner_requests(plan["account_key"], cutoff, prefix)))

    def barrier(kind, key, observed):
        nonlocal previous, closed
        template = barriers[len(journal)]
        if kind != template["record_kind"] or key != template["scheduler_key"]:
            raise RuntimeError("UNEXPECTED_BARRIER")
        ordinal = template["ordinal"]
        store.mark_processed(dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=template["barrier_id"]))
        payload = _normalized(observed)
        before = after = None
        if kind == "SOURCE_GROUP_RESULT":
            before = previous
            group_at = payload["request"]["group_effective_at"]
            try:
                after = capture(group_at, f"SPIDER-P2-SOURCE-{ordinal}")["owner_evidence"]
            except Exception as error:
                if _wire._native_failure(error) is None:
                    raise
                store.publish_failure("PERSISTENCE_FAILED", _primary(kind, payload) or dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED"))
                closed = True
                raise _PersistenceError() from error
            payload.update(owner_evidence_before=before, owner_evidence_after=after)
        if kind == "SOURCE_GROUP_RESULT":
            effective = payload["request"]["group_effective_at"]
            before_version = payload["result"]["account_version_before"]
            after_version = payload["result"]["account_version_after"]
        elif kind == "SNAPSHOT_FACT":
            effective = payload["request"]["captured_at"]
            before_version = after_version = None
        elif kind == "DELIVERY_ATTEMPT":
            delivery = payload["delivery"]
            effective = (source_times[delivery["source_fact_id"]] if delivery["source_namespace"] == "SOURCE"
                         else snapshot_times[delivery["source_fact_id"]])
            before_version = after_version = None
        else:
            effective = key["visible_at"]
            before_version = after_version = None
        row = dict(schema_version="spider_journal_record_v1", run_id=attempt["run_id"], journal_seq=ordinal,
                   barrier_id=template["barrier_id"], record_kind=kind, scheduler_key=key,
                   causal_parent_ids=template["causal_parent_ids"], effective_at=effective,
                   visible_at=key["visible_at"], account_version_before=before_version,
                   account_version_after=after_version, payload=payload, configuration_context=context)
        try:
            store.append_journal(row)
        except _StoreError as error:
            store.publish_failure("PERSISTENCE_FAILED", _primary(kind, payload) or error.primary_failure)
            closed = True
            raise _PersistenceError() from error
        journal.append(row)
        if kind == "SOURCE_GROUP_RESULT":
            previous = after

    owner = _ReplayComposition(plan["native_profile"], plan["account_key"], callback_plans,
                               evidence_callback=barrier, configuration=plan["configuration"])
    initial = capture(plan["initial_cutoff"], "SPIDER-P2-INITIAL")["owner_evidence"]
    previous = initial
    for step in plan["recipe"]:
        if step["kind"] == "SOURCE_GROUP":
            group = _wire._decode(_bytes(step["group"]).decode())
            result = owner._enqueue(dict(kind="SOURCE_GROUP", schedule_sequence=step["schedule_sequence"], group=group))
            if result["classification"] != "TERMINAL":
                result = owner._dispatch_due(step["at"])
        elif step["kind"] == "DELIVERY":
            delivery = owner._codec.build_delivery(_cast(_wire.Projection, step["projection"]))
            result = owner._enqueue(dict(kind="DELIVERY", delivery=delivery), callback_plans[delivery["delivery_id"]])
        else:
            result = owner._begin_poll(step["plan"])
        if result["classification"] == "TERMINAL":
            reason, primary = _terminal(result, closed)
            if not closed:
                store.publish_failure(reason, primary)
            return reason
    result = owner._dispatch_due(plan["final_cutoff"])
    if result["classification"] == "TERMINAL":
        reason, primary = _terminal(result, closed)
        if not closed:
            store.publish_failure(reason, primary)
        return reason
    _require(len(journal) == len(barriers))
    final = capture(plan["final_cutoff"], "SPIDER-P2-FINAL")
    last = attempt["planned_coverage"][-1]
    boundary = dict(ordinal=last["ordinal"], journal_seq=last["ordinal"], barrier_id=last["barrier_id"])
    status = dict(schema_version="spider_status_v1", run_id=attempt["run_id"], state="RUNNING",
                  processed_boundary=boundary, persisted_boundary=_copy(boundary), failure_reason=None, primary_failure=None,
                  configuration_context=context)
    artifacts = _build(attempt["run_id"], attempt, status, journal, initial, final["owner_evidence"], final["scheduler_observation"])
    if artifacts["reconciliation"]["result"] != "OK":
        raise _ProjectionError()
    if store.finalize(**artifacts) != "COMPLETE_PUBLISHED":
        raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
    admitted = _admit(store._path)
    return None if admitted["decision"] == "ACCEPT" else admitted["reason"]


def _execute(store, bundle, attempt):
    if bundle["plan"]["schema_version"] == "spider_scenario_plan_v2":
        return _execute_configured(store, bundle, attempt)
    rows, plan = bundle["journal"], bundle["plan"]
    journal, previous, closed = [], None, False
    def capture(template):
        return owner.capture_owner_evidence(template["cutoff"], *[template[kind + "_request"] for kind in ("trading", "positions", "open_orders")])
    def barrier(kind, key, payload):
        nonlocal previous, closed
        template = rows[len(journal)]
        if kind != template["record_kind"] or key != template["scheduler_key"]:
            raise RuntimeError("UNEXPECTED_BARRIER")
        upstream = _primary(kind, payload)
        store.mark_processed(dict(ordinal=template["journal_seq"], journal_seq=template["journal_seq"], barrier_id=template["barrier_id"]))
        after = previous
        try:
            if kind == "SOURCE_GROUP_RESULT":
                after = capture(template["payload"]["owner_evidence_after"])["owner_evidence"]
                payload = dict(payload, owner_evidence_before=previous, owner_evidence_after=after)
        except Exception as error:
            if _wire._native_failure(error) is None:
                raise
            store.publish_failure("PERSISTENCE_FAILED", upstream or dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED"))
            closed = True
            raise _PersistenceError() from error
        row = _normalized(dict(template, payload=payload))
        try:
            store.append_journal(row)
        except _StoreError as error:
            store.publish_failure("PERSISTENCE_FAILED", upstream or error.primary_failure)
            closed = True
            raise _PersistenceError() from error
        journal.append(row)
        previous = after
    owner = _ReplayComposition(plan["native_profile"], plan["account_key"], evidence_callback=barrier)
    initial = capture(bundle["endpoint"]["initial_owner_evidence"])["owner_evidence"]
    previous = initial
    while len(journal) < len(rows):
        row = rows[len(journal)]
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            group = _wire._decode(_bytes(row["payload"]["request"]).decode())
            result = owner._enqueue(dict(kind="SOURCE_GROUP", group=group, schedule_sequence=row["scheduler_key"]["schedule_sequence"]))
        else:
            frozen = row["payload"]["delivery"]
            delivery = owner._codec.build_delivery(_cast(_wire.Projection, dict(schema_version="delivery_projection_v1",
                reference=dict(namespace=frozen["source_namespace"], fact_id=frozen["source_fact_id"]),
                **{key: frozen[key] for key in ("payload_kind", "occurrence_index", "schedule_sequence", "visible_at")})))
            result = owner._enqueue(dict(kind="DELIVERY", delivery=delivery), _empty(delivery))
        if result["classification"] != "TERMINAL":
            result = owner._dispatch_due(row["visible_at"])
        if result["classification"] == "TERMINAL":
            reason, primary = _terminal(result, closed)
            if not closed:
                store.publish_failure(reason, primary)
            return reason
        if len(journal) < row["journal_seq"]:
            raise RuntimeError("UNOBSERVED_BARRIER")
    _require(len(journal) == len(plan["planned_coverage"]))
    result = owner._dispatch_due(plan["final_cutoff"])
    if result["classification"] == "TERMINAL":
        reason, primary = _terminal(result, closed)
        store.publish_failure(reason, primary)
        return reason
    final = _normalized(capture(bundle["endpoint"]["final_owner_evidence"]))
    last = plan["planned_coverage"][-1]
    boundary = dict(ordinal=last["ordinal"], journal_seq=last["ordinal"], barrier_id=last["barrier_id"])
    status = dict(schema_version="spider_status_v1", run_id=attempt["run_id"], state="RUNNING", processed_boundary=boundary,
                  persisted_boundary=_copy(boundary), failure_reason=None, primary_failure=None)
    artifacts = _build(attempt["run_id"], attempt, status, journal, _normalized(initial), final["owner_evidence"], final["scheduler_observation"])
    if store.finalize(**artifacts) != "COMPLETE_PUBLISHED":
        raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
    admitted = _admit(store._path)
    return None if admitted["decision"] == "ACCEPT" else admitted["reason"]


def _execute_historical(store, attempt, run: _HistoricalRunInput):
    journal = []
    previous_owner = None
    closed = False
    composition = _ReplayComposition._from_historical_run(run)
    configuration = attempt["configuration_context"]
    historical = attempt["historical_context"]

    def capture(cutoff, prefix):
        return _normalized(composition.capture_owner_evidence(
            cutoff, *_owner_requests(composition._account, cutoff, prefix),
        ))

    def persist(kind, key, observed):
        nonlocal previous_owner, closed
        if kind == "DELIVERY_ATTEMPT":
            return
        ordinal = len(journal) + 1
        boundary = dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=key["stable_id"])
        try:
            store.mark_processed(boundary, record_kind=kind)
            payload = _normalized(observed)
            before_version = after_version = None
            if kind == "SOURCE_GROUP_RESULT":
                if previous_owner is None:
                    raise RuntimeError("MISSING_INITIAL_OWNER_EVIDENCE")
                payload["owner_evidence_before"] = previous_owner
                group_at = payload["request"]["group_effective_at"]
                after = capture(group_at, f"GROUP-{ordinal}")["owner_evidence"]
                payload["owner_evidence_after"] = after
                before_version = payload["result"]["account_version_before"]
                after_version = payload["result"]["account_version_after"]
                previous_owner = after
            elif kind == "HISTORICAL_MARKET_RESULT":
                before_version = payload["owner_inspection_before"]["account_version"]
                after_version = payload["owner_inspection_after"]["account_version"]
                previous_owner = capture(payload["result"]["effective_at"], f"MARKET-{ordinal}")["owner_evidence"]
            if kind in ("SOURCE_GROUP_RESULT", "SNAPSHOT_FACT"):
                effective = payload["request"].get("group_effective_at", payload["request"].get("captured_at"))
            else:
                effective = key["visible_at"]
            row = dict(
                schema_version="spider_journal_record_v1", run_id=run.run_id,
                journal_seq=ordinal, barrier_id=key["stable_id"], record_kind=kind,
                scheduler_key=key, causal_parent_ids=[], effective_at=effective,
                visible_at=key["visible_at"], account_version_before=before_version,
                account_version_after=after_version, payload=payload,
                configuration_context=configuration, historical_context=historical,
            )
            store.append_journal(row)
        except Exception as error:
            primary = (error.primary_failure if isinstance(error, _StoreError)
                       else dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED"))
            if isinstance(error, _StoreError) and error.reason == "PUBLICATION_DURABILITY_UNKNOWN":
                closed = True
                raise error
            try:
                store.publish_failure("PERSISTENCE_FAILED", primary)
            finally:
                closed = True
            raise _PersistenceError() from error
        journal.append(row)

    composition._evidence_callback = persist
    start = run.range_start_ms * 16
    initial = capture(start, "INITIAL")["owner_evidence"]
    previous_owner = initial
    cutoff = run.range_end_ms * 16 + 6
    result = composition._dispatch_due(cutoff)
    if result["classification"] != "SUCCESS":
        reason, primary = _terminal(result, closed)
        if not closed:
            store.publish_failure(reason, primary)
        return reason
    final_capture = capture(cutoff, "FINAL")
    status = store._status("RUNNING")
    artifacts = _build(
        run.run_id, attempt, status, journal, initial, final_capture["owner_evidence"],
        final_capture["scheduler_observation"],
    )
    if store.finalize(**artifacts) != "COMPLETE_PUBLISHED":
        raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
    admitted = _admit(store._path)
    return None if admitted["decision"] == "ACCEPT" else admitted["reason"]


def _floating_report_observation(composition, request):
    host = composition._floating_exit_host
    endpoint = composition._paid_endpoint
    if host is None or endpoint is None:
        raise RuntimeError("MISSING_FLOATING_EXIT_HOST")
    host_observation = host.observation()
    product_exits = []
    for product_id in request.execution_input.historical_input.ordered_products:
        if product_id not in endpoint.product_operations:
            continue
        progress = endpoint.product_exit_progress(composition, product_id)
        product_exits.append({
            "product_id": progress["product_id"], "reason": progress["reason"],
            "requested_at": progress["requested_at"], "status": progress["status"],
            "completed_at": progress["completed_at"],
        })
    def wire_value(value):
        if isinstance(value, _Decimal):
            return _decimal_text(value)
        if type(value) is dict:
            return {key: wire_value(item) for key, item in value.items()}
        if type(value) in (list, tuple):
            return [wire_value(item) for item in value]
        return value

    return _normalized(dict(
        schema_version="spider_floating_exit_observation_v1",
        run_contract_sha256=host.contract_hash,
        decision_batches=wire_value(host_observation["batches"]),
        product_exits=product_exits,
    ))


def _execute_paid(store, request, attempt, execution_input: bytes, result_state: dict[str, _Any],
                  *, floating_exit: bool = False):
    composition = (_ReplayComposition._from_floating_exit_run(request) if floating_exit
                   else _ReplayComposition._from_execution_run(request))
    run_request = request.execution_input if floating_exit else request
    endpoint = composition._paid_endpoint
    if endpoint is None:
        raise RuntimeError("MISSING_PAID_ENDPOINT")
    execution = attempt["execution_context"]
    configuration = attempt["configuration_context"]
    historical = attempt["historical_context"]
    journal: list[dict[str, _Any]] = []
    source_captures: dict[str, tuple[dict[str, _Any] | None, dict[str, _Any] | None, Exception | None]] = {}
    source_capture_error: list[Exception] = []
    persistence_error: list[_StoreError] = []

    def capture(cutoff: int, prefix: str) -> dict[str, _Any]:
        requests = [composition._paid_snapshot_request(kind, cutoff, prefix)
                    for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS")]
        return _normalized(composition.capture_owner_evidence(cutoff, *requests))

    initial_capture: dict[str, _Any] | None = None
    initial_error: Exception | None = None
    start_cutoff = run_request.historical_input.range_start_ms * 16
    try:
        initial_capture = capture(start_cutoff, "PAID_INITIAL_" + endpoint.contract_hash[:16])
    except Exception as error:
        initial_error = error

    class RecordingCodec:
        def __init__(self, codec):
            self._inner = codec
            self._sequence = 0

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def apply_group(self, group):
            group_id = group["group_id"]
            cutoff = group["group_effective_at"]
            sequence = self._sequence
            self._sequence += 1
            try:
                before = capture(cutoff, f"PAID_SOURCE_{sequence}_BEFORE") ["owner_evidence"]
            except Exception as error:
                source_capture_error.append(error)
                source_captures[group_id] = (None, None, error)
                raise _ScheduleError("SOURCE_EVIDENCE_CAPTURE_FAILED") from error
            try:
                result = self._inner.apply_group(group)
            except Exception:
                raise
            try:
                after = capture(cutoff, f"PAID_SOURCE_{sequence}_AFTER")["owner_evidence"]
                source_captures[group_id] = (before, after, None)
            except Exception as error:
                source_capture_error.append(error)
                source_captures[group_id] = (before, None, error)
            return result

    _cast(_Any, composition)._codec = RecordingCodec(composition._codec)

    def persist(kind, key, observed):
        if kind == "DELIVERY_ATTEMPT":
            return
        payload = _normalized(observed)
        before_version = after_version = None
        if kind == "SOURCE_GROUP_RESULT":
            group_id = payload["request"]["group_id"]
            captured = source_captures.pop(group_id, None)
            if captured is None or captured[1] is None or captured[2] is not None:
                primary = dict(kind="PERSISTENCE", reason="SOURCE_EVIDENCE_CAPTURE_FAILED")
                if store._state == "RUNNING":
                    try:
                        store.publish_failure("PERSISTENCE_FAILED", primary)
                    except _StoreError as error:
                        persistence_error.append(error)
                if captured is not None and captured[2] is not None:
                    source_capture_error.append(captured[2])
                raise _PersistenceError("SOURCE_EVIDENCE_CAPTURE_FAILED")
            payload["owner_evidence_before"], payload["owner_evidence_after"] = captured[0], captured[1]
            effective = payload["request"]["group_effective_at"]
            before_version = payload["result"]["account_version_before"]
            after_version = payload["result"]["account_version_after"]
        elif kind == "SNAPSHOT_FACT":
            effective = payload["request"]["captured_at"]
        elif kind == "HISTORICAL_MARKET_RESULT":
            effective = payload["result"]["effective_at"]
            before_version = payload["owner_inspection_before"]["account_version"]
            after_version = payload["owner_inspection_after"]["account_version"]
        else:
            effective = key["visible_at"]
        ordinal = len(journal) + 1
        row = dict(
            schema_version=("spider_floating_exit_journal_record_v1" if floating_exit
                            else "spider_execution_journal_record_v1"), run_id=attempt["run_id"],
            journal_seq=ordinal, barrier_id=key["stable_id"], record_kind=kind,
            scheduler_key=key, causal_parent_ids=[], effective_at=effective,
            visible_at=key["visible_at"], account_version_before=before_version,
            account_version_after=after_version, payload=payload,
            configuration_context=configuration, historical_context=historical,
            execution_context=execution,
        )
        try:
            store.mark_processed(dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=key["stable_id"]),
                                 record_kind=kind)
            store.append_journal(row)
        except _StoreError as error:
            persistence_error.append(error)
            if not _paid_durability_unknown(error) and store._state == "RUNNING":
                try:
                    store.publish_failure("PERSISTENCE_FAILED", error.primary_failure
                                          or dict(kind="PERSISTENCE", reason=error.reason))
                except _StoreError as newer:
                    persistence_error[-1] = newer
            raise _PersistenceError() from error
        journal.append(row)

    composition._evidence_callback = persist
    if initial_error is None:
        composition._dispatch_due(run_request.historical_input.range_end_ms * 16 + 6)
        if persistence_error and _paid_durability_unknown(persistence_error[-1]):
            raise _StoreError("PUBLICATION_DURABILITY_UNKNOWN", "PERSISTENCE_FAILED")
        paid_result = endpoint.result(composition)
    else:
        scheduler = composition._public_scheduler_observation()
        try:
            inspection = composition._paid_inspection()
            inspection_error = None
        except Exception as error:
            inspection = None
            inspection_error = _paid_error(error, composition)
        failure = dict(
            primary=dict(kind="OWNER_EVIDENCE", reason="EVIDENCE_FAILED", native_failure=None),
            evidence_error=_paid_error(initial_error, composition),
            inspection=inspection, inspection_error=inspection_error,
            observed_at=start_cutoff,
        )
        paid_observation = endpoint._paid_observation(
            composition, scheduler, current_evidence_complete=False, failure=failure,
        )
        paid_result = _classify_paid(
            request_identity=dict(
                run_contract_sha256=endpoint.contract_hash,
                policy_end_ms=endpoint.policy_end_ms,
                settlement_end_ms=endpoint.settlement_end_ms,
                generation=endpoint.generation,
            ),
            owner_evidence=None, actions=endpoint.actions,
            scheduler_observation=scheduler, paid_observation=paid_observation,
            failure=failure,
        )
    result_state["paid_endpoint"] = paid_result
    result_state["endpoint_status"] = paid_result["status"]
    if source_capture_error:
        scheduler = composition._public_scheduler_observation()
        failure = _copy(paid_result["failure"])
        if failure is None:
            raise RuntimeError("MISSING_SOURCE_CAPTURE_FAILURE")
        failure["primary"] = _paid_error(source_capture_error[0], composition)
        paid_observation = endpoint._paid_observation(
            composition, scheduler,
            current_evidence_complete=(failure["evidence_error"] is None),
            failure=failure,
        )
        paid_result = _classify_paid(
            request_identity=dict(
                run_contract_sha256=endpoint.contract_hash,
                policy_end_ms=endpoint.policy_end_ms,
                settlement_end_ms=endpoint.settlement_end_ms,
                generation=endpoint.generation,
            ),
            owner_evidence=paid_result["owner_evidence"], actions=endpoint.actions,
            scheduler_observation=scheduler, paid_observation=paid_observation,
            failure=failure,
        )
        result_state["paid_endpoint"] = paid_result
        result_state["endpoint_status"] = paid_result["status"]
    if source_capture_error and not persistence_error and store._state == "RUNNING":
        try:
            store.publish_failure("PERSISTENCE_FAILED", dict(
                kind="PERSISTENCE", reason="SOURCE_EVIDENCE_CAPTURE_FAILED",
            ))
        except _StoreError as error:
            persistence_error.append(error)
    if persistence_error:
        error = persistence_error[-1]
        if _paid_durability_unknown(error):
            raise _StoreError("PUBLICATION_DURABILITY_UNKNOWN", "PERSISTENCE_FAILED") from error
        if store._state in ("RUNNING", "FAILED"):
            store.publish_paid_diagnostic(paid_result)
        return dict(outcome="FAILED", reason=error.requested_failure_reason or error.reason)
    if source_capture_error and store._state == "FAILED":
        store.publish_paid_diagnostic(paid_result)
        return dict(outcome="FAILED", reason="PERSISTENCE_FAILED")

    scheduler = composition._public_scheduler_observation()
    failed = paid_result["failure"]
    paid_observation = endpoint._paid_observation(
        composition, scheduler,
        current_evidence_complete=(failed is None or failed["evidence_error"] is None),
        failure=failed,
    )
    report = [dict(
        schema_version=("spider_floating_exit_report_v1" if floating_exit
                        else "spider_execution_report_v1"), run_id=attempt["run_id"],
        initial_owner_evidence=(initial_capture["owner_evidence"] if initial_capture is not None else None),
        scheduler_observation=scheduler, paid_observation=paid_observation,
        paid_endpoint=paid_result,
        source_evidence_refs=[f"journal:{row['journal_seq']}" for row in journal],
    )]
    if floating_exit:
        report[0]["floating_observation"] = _floating_report_observation(composition, request)
    raw_files = {
        name: (store._path / name).read_bytes()
        for name in ("attempt.json", "status.json", "journal.jsonl", "execution_input.json")
    }
    raw_files["paid_endpoint.json"] = (
        _bytes(paid_result) + b"\n" if floating_exit
        else _paid_bytes(paid_result, execution, configuration)
    )
    raw_files["report.jsonl"] = _jsonl(_cast(list[object], report))
    reconciliation = _build_execution_reconciliation(attempt["run_id"], raw_files)
    if reconciliation["result"] != "OK":
        store.publish_paid_diagnostic(paid_result)
        store.publish_failure("ENDPOINT_RECONCILIATION_FAILED", None)
        return dict(outcome="FAILED", reason="ENDPOINT_RECONCILIATION_FAILED")
    if store.finalize(paid_result, reconciliation, report) != "COMPLETE_PUBLISHED":
        raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
    admitted = _admit(store._path)
    if admitted["decision"] != "ACCEPT":
        return dict(outcome="FAILED", reason=admitted["reason"])
    return dict(outcome="ADMITTED", reason=(failed["primary"]["reason"] if failed is not None else None))


def _invoke(
    output_root, run_id, selector, select, historical_input: bytes | None = None,
    execution_input: bytes | None = None, floating_exit_input: bytes | None = None,
):
    try:
        _text(output_root)
        _text(run_id, r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
        _text(selector, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
        _require("\0" not in output_root)
        output_root.encode("utf-8")
    except (ValueError, UnicodeError):
        raise ValueError("INVALID_INVOCATION") from None
    store = None
    floating_invocation = selector == _FLOATING_SELECTOR or floating_exit_input is not None
    paid_invocation = selector == _PAID_SELECTOR or execution_input is not None
    if floating_invocation and paid_invocation:
        return dict(run_id=run_id, outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION",
                    endpoint_status=None)
    paid_state: dict[str, _Any] = {}

    def response(outcome, reason):
        value = dict(run_id=run_id, outcome=outcome, reason=reason)
        if paid_invocation or floating_invocation:
            value["endpoint_status"] = (paid_state.get("paid_endpoint") or {}).get("status")
        return value

    def failed(error):
        if (paid_invocation or floating_invocation) and _paid_durability_unknown(error):
            return response("DURABILITY_UNKNOWN", "PUBLICATION_DURABILITY_UNKNOWN")
        if ((paid_invocation or floating_invocation) and store is not None and paid_state.get("paid_endpoint") is not None
                and error.reason != "PUBLICATION_DURABILITY_UNKNOWN"
                and store._state in ("RUNNING", "FAILED")
                and not (store._path / "paid_endpoint.json").exists()):
            try:
                store.publish_paid_diagnostic(paid_state["paid_endpoint"])
            except _StoreError as newer:
                error = newer
        if store is not None and store._state == "RUNNING" and error.requested_failure_reason is None:
            try:
                store.publish_failure(error.reason, error.primary_failure)
            except _StoreError as newer:
                error = newer
        return response("DURABILITY_UNKNOWN" if error.reason == "PUBLICATION_DURABILITY_UNKNOWN" else "FAILED",
                        error.reason)
    historical_run = None
    historical_attempt = None
    paid_request = None
    paid_attempt = None
    floating_request = None
    floating_attempt = None
    if floating_invocation:
        if (selector != _FLOATING_SELECTOR or historical_input is not None
                or execution_input is not None or type(floating_exit_input) is not bytes):
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
        try:
            floating_request, _, floating_attempt = _floating_configuration(floating_exit_input, run_id)
        except (OSError, ValueError, TypeError, KeyError, IndexError):
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
    if paid_invocation:
        if (selector != _PAID_SELECTOR or historical_input is not None
                or type(execution_input) is not bytes):
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
        try:
            paid_request, _, paid_attempt = _paid_configuration(execution_input, run_id)
        except (OSError, ValueError, TypeError, KeyError, IndexError):
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
    if selector == _P3_SELECTOR or historical_input is not None:
        if selector != _P3_SELECTOR or type(historical_input) is not bytes:
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
        try:
            historical_run, bundle, hashes = _historical_configuration(historical_input, run_id)
            historical_attempt = _attempt(run_id, selector, bundle, hashes, _P3_RUN_CONTRACT)
        except (OSError, ValueError, TypeError, KeyError):
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
    try:
        store = _Store.create(output_root, run_id)
        if paid_request is not None:
            assert paid_attempt is not None and execution_input is not None
            if store.register(paid_attempt, execution_input=execution_input) != "NATIVE_CONSTRUCTION_ALLOWED":
                raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
            result = _execute_paid(store, paid_request, paid_attempt, execution_input, paid_state)
            return response(result["outcome"], result["reason"])
        if floating_request is not None:
            assert floating_attempt is not None and floating_exit_input is not None
            if store.register(floating_attempt, floating_exit_input=floating_exit_input) != "NATIVE_CONSTRUCTION_ALLOWED":
                raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
            result = _execute_paid(
                store, floating_request, floating_attempt, floating_exit_input,
                paid_state, floating_exit=True,
            )
            return response(result["outcome"], result["reason"])
        if historical_run is not None:
            assert historical_attempt is not None
            if store.register(historical_attempt, historical_input=historical_input) != "NATIVE_CONSTRUCTION_ALLOWED":
                raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
            reason = _execute_historical(store, historical_attempt, historical_run)
            return response("ADMITTED" if reason is None else "FAILED", reason)
        try:
            bundle, hashes = _configuration(selector, run_id, select)
            attempt = _attempt(run_id, selector, bundle, hashes)
        except (OSError, ValueError) as error:
            if isinstance(error, ValueError) and error.args not in (("INVALID_ARTIFACT",), ("UNSUPPORTED_CONFIGURATION",), ("POLICY_EMISSION_MISMATCH",)):
                raise
            store.register(_attempt(run_id, selector, rejected_contract=_P2_RUN_CONTRACT if selector == _P2_SELECTOR else "SPIDER_SYNTHETIC_P1_RUN_V1"))
            return response("REJECTED", "UNSUPPORTED_CONFIGURATION")
        if store.register(attempt) != "NATIVE_CONSTRUCTION_ALLOWED":
            raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
        reason = _execute(store, bundle, attempt)
        return response("ADMITTED" if reason is None else "FAILED", reason)
    except _StoreError as error:
        return failed(error)
    except Exception as error:
        native = _wire._native_failure(error)
        reason = "NATIVE_FAULT" if native else "ENDPOINT_RECONCILIATION_FAILED" if isinstance(error, _ProjectionError) else "UNEXPECTED_EXCEPTION"
        if ((paid_invocation or floating_invocation) and store is not None and paid_state.get("paid_endpoint") is not None
                and store._state in ("RUNNING", "FAILED")
                and not (store._path / "paid_endpoint.json").exists()):
            try:
                store.publish_paid_diagnostic(paid_state["paid_endpoint"])
            except _StoreError as newer:
                return failed(newer)
        if store is not None and store._state == "RUNNING":
            try:
                store.publish_failure(reason, dict(kind="NATIVE", reason=native["reason"]) if native else None)
            except _StoreError as newer:
                return failed(newer)
        if floating_invocation:
            if native is None and not isinstance(error, _ProjectionError):
                _LOGGER.exception("floating-exit research invocation failed")
            return response("FAILED", reason)
        if native or isinstance(error, _ProjectionError):
            return response("FAILED", reason)
        raise


def run_spider_scenario(
    output_root: str, run_id: str, scenario_selector: str,
    historical_input: bytes | None = None, *, execution_input: bytes | None = None,
    floating_exit_input: bytes | None = None,
) -> dict[str, _Any]:
    """Run only a CLI-approved fixed recipe and admit its exact persisted output."""
    return _invoke(output_root, run_id, scenario_selector, _plans.cli_plan_bundle,
                   historical_input, execution_input, floating_exit_input)


def _run_o03(output_root: str, run_id: str, scenario_selector: str) -> dict[str, _Any]:
    if type(scenario_selector) is not str or scenario_selector != _plans.PLAN_IDS[2]:
        raise ValueError("UNSUPPORTED_CONFIGURATION")
    return _invoke(output_root, run_id, scenario_selector, _plans.plan_bundle)
