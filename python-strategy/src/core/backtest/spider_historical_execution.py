"""Durable event-driven historical scenario execution."""

from typing import Any as _Any, cast as _cast

from src.core.backtest.spider_run_artifacts import canonical_bytes as _bytes, decode_canonical as _decode
from src.core.backtest.synthetic_scenario_replay import (
    _ReplayComposition,
    ReplayPersistenceError as _PersistenceError,
)
from src.core.backtest.spider_historical_input import HistoricalRunInput as _HistoricalRunInput
from src.core.backtest.spider_run_errors import classify_historical_failure as _classify_failure
from src.core.backtest.spider_historical_durable_ports import DurableHistoricalOutputAdapter as _DurableOutput


def _normalized(value):
    return _cast(_Any, _decode(_bytes(value)))


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


def _execute_historical(store, attempt, run: _HistoricalRunInput):
    journal = []
    output = _DurableOutput(store, attempt, run, journal)
    previous_owner = None
    closed = False
    ordinal = 0
    last_boundary = None
    last_recorded_ordinal = None
    composition = _ReplayComposition._from_historical_run(run)
    configuration = attempt["configuration_context"]
    historical = attempt["historical_context"]

    def capture(cutoff, prefix, *, include_scheduler=False):
        requests = _owner_requests(composition._account, cutoff, prefix)
        if include_scheduler:
            captured = composition.capture_owner_evidence(cutoff, *requests)
        else:
            captured = dict(owner_evidence=composition._capture_owner_evidence_core(cutoff, *requests))
        return _normalized(captured)

    def persist(kind, key, observed):
        nonlocal previous_owner, closed, ordinal, last_boundary, last_recorded_ordinal
        if kind == "DELIVERY_ATTEMPT":
            return
        ordinal += 1
        boundary = dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=key["stable_id"])
        last_boundary = boundary
        try:
            output.checkpoint(boundary, kind)
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
            output.persist(row)
        except Exception as error:
            decision = _classify_failure(error, "CALLBACK_PERSIST", None, closed)
            if decision.propagate_original:
                closed = decision.closed
                raise error
            try:
                store.publish_failure(decision.reason, decision.primary_failure)
            finally:
                closed = decision.closed
            raise _PersistenceError() from error
        output.retain(row)
        last_recorded_ordinal = ordinal

    composition._evidence_callback = persist
    start = run.range_start_ms * 16
    initial = capture(start, "INITIAL")["owner_evidence"]
    previous_owner = initial
    cutoff = run.range_end_ms * 16 + 6
    result = composition._dispatch_due(cutoff)
    if result["classification"] != "SUCCESS":
        reason, primary = _terminal(result, closed)
        if not closed:
            output.fail(result["classification"], reason, primary, last_boundary, last_recorded_ordinal)
        return reason
    final_capture = capture(cutoff, "FINAL", include_scheduler=True)
    return output.finish(
        initial, final_capture["owner_evidence"], final_capture["scheduler_observation"],
        attempt["terminal_policy"],
        dict(configuration_context=configuration, historical_context=historical),
    )
