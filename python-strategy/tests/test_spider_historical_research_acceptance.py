"""Full-fixture parity and acceptance oracles for the research entry."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
import os
from pathlib import Path
import shutil
from typing import Any, cast

import pytest

from src.core.backtest import spider_historical_durable_ports as durable_ports
from src.core.backtest import spider_historical_input as historical_input
from src.core.backtest import spider_historical_research_entry as research_entry
from src.core.backtest import spider_historical_research_output as research_output
from src.core.backtest import spider_run_admission as admission
from src.core.backtest import spider_run_evidence as evidence
from src.core.backtest import spider_run_store as store
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes,
    decode_canonical,
    decode_jsonl,
    encode_jsonl,
)
from tools.spider_run_compare import compare
from test_synthetic_scenario_native import _native_error_type
from test_spider_scenario_run import frozen_h03_input, install_frozen_h03_order

_S0 = Path(
    os.environ.get(
        "SPIDER_THROUGHPUT_S0", "/Volumes/Dev/Scratch/agents/spider_throughput/S0"
    )
)
_FIXTURES = (
    ("fin", _S0 / "fin-a" / "fin", "ADMITTED", None),
    ("h360", _S0 / "h360" / "h360", "FAILED", "ARTIFACT_MISMATCH"),
)


def _fixture_input(run_id: str) -> tuple[Path, bytes]:
    root = next(root for name, root, _outcome, _reason in _FIXTURES if name == run_id)
    path = root / "historical_input.json"
    if not path.is_file():
        pytest.skip(f"S0 fixture missing: {path}")
    raw = path.read_bytes()
    assert historical_input.decode_historical_run_input(raw).run_id == run_id
    return root, raw


def _listing(root: Path) -> tuple[tuple[str, bool], ...]:
    return tuple(
        sorted((str(path.relative_to(root)), path.is_dir()) for path in root.rglob("*"))
    )


def _new_audit() -> dict[str, Any]:
    return {
        "journal": sha256(),
        "fills": sha256(),
        "groups": sha256(),
        "events": sha256(),
        "rows": 0,
        "fills_count": 0,
        "groups_count": 0,
        "barriers": {},
    }


def _audit_row(audit: dict[str, Any], row: dict[str, Any]) -> None:
    encoded = canonical_bytes(row) + b"\n"
    audit["journal"].update(encoded)
    audit["rows"] += 1
    audit["barriers"][row["journal_seq"]] = row["barrier_id"]
    audit["events"].update(
        canonical_bytes([row["journal_seq"], row["record_kind"], row["barrier_id"]])
        + b"\n"
    )
    if row["record_kind"] == "SOURCE_GROUP_RESULT":
        audit["groups"].update(encoded)
        audit["groups_count"] += 1
    if row["record_kind"] == "HISTORICAL_MARKET_RESULT":
        for product in row["payload"]["result"]["products"]:
            for fill in product["fills"]:
                audit["fills"].update(canonical_bytes(fill) + b"\n")
                audit["fills_count"] += 1


def _install_observer(monkeypatch: pytest.MonkeyPatch):
    mode = ["durable"]
    audits = {name: _new_audit() for name in ("durable", "research")}
    captures: dict[str, list[dict[str, Any]]] = {name: [] for name in audits}
    rust_snapshots: dict[str, list[bytes]] = {name: [] for name in audits}
    boundary: dict[str, dict[str, Any] | None] = {name: None for name in audits}
    final_capture_depth = [0]

    def capture_trace(cutoff, requests, *, scheduler: bool) -> None:
        ids = [request["snapshot_id"] for request in requests]
        current_boundary = boundary[mode[0]]
        captures[mode[0]].append(
            {
                "cutoff": cutoff,
                "prefix": ids[0].rsplit("-", 1)[0],
                "kinds": [request["snapshot_kind"] for request in requests],
                "snapshot_ids": ids,
                "request_bytes": [canonical_bytes(request) for request in requests],
                "event_key": current_boundary["event_key"]
                if current_boundary is not None
                else None,
                "scheduler": scheduler,
            }
        )

    original_core = scenario._ReplayComposition._capture_owner_evidence_core
    original_final = scenario._ReplayComposition.capture_owner_evidence
    original_snapshot = wire.ScenarioCodec.capture_snapshot
    original_d_checkpoint = durable_ports.DurableHistoricalOutputAdapter.checkpoint
    original_r_checkpoint = research_output.ResearchEvidenceSink.checkpoint
    original_d_retain = durable_ports.DurableHistoricalOutputAdapter.retain
    original_r_retain = research_output.ResearchEvidenceSink.retain

    def core(self, cutoff, *requests):
        if final_capture_depth[0] == 0:
            capture_trace(cutoff, requests, scheduler=False)
        return original_core(self, cutoff, *requests)

    def final(self, cutoff, *requests):
        capture_trace(cutoff, requests, scheduler=True)
        final_capture_depth[0] += 1
        try:
            return original_final(self, cutoff, *requests)
        finally:
            final_capture_depth[0] -= 1

    def snapshot(self, request):
        result = original_snapshot(self, request)
        rust_snapshots[mode[0]].append(canonical_bytes(request))
        return result

    def checkpoint(original):
        def wrapped(self, value, kind):
            result = original(self, value, kind)
            boundary[mode[0]] = {
                "ordinal": value["ordinal"],
                "kind": kind,
                "event_key": value["barrier_id"],
            }
            return result

        return wrapped

    def retain(original):
        def wrapped(self, row):
            _audit_row(audits[mode[0]], row)
            return original(self, row)

        return wrapped

    monkeypatch.setattr(
        scenario._ReplayComposition, "_capture_owner_evidence_core", core
    )
    monkeypatch.setattr(scenario._ReplayComposition, "capture_owner_evidence", final)
    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", snapshot)
    monkeypatch.setattr(
        durable_ports.DurableHistoricalOutputAdapter,
        "checkpoint",
        checkpoint(original_d_checkpoint),
    )
    monkeypatch.setattr(
        research_output.ResearchEvidenceSink,
        "checkpoint",
        checkpoint(original_r_checkpoint),
    )
    monkeypatch.setattr(
        durable_ports.DurableHistoricalOutputAdapter,
        "retain",
        retain(original_d_retain),
    )
    monkeypatch.setattr(
        research_output.ResearchEvidenceSink, "retain", retain(original_r_retain)
    )
    return mode, audits, captures, rust_snapshots


@pytest.mark.parametrize("run_id", ["fin", "h360"])
def test_full_fixture_durable_research_parity_and_independent_recompute(
    tmp_path, monkeypatch, run_id
):
    # This compares persisted S0 legacy-v1 bundles against their frozen bytes.
    monkeypatch.setattr(scenario, "_P3_ATTEMPT_ARTIFACT_ENCODING", "artifact_encoding_v1")
    baseline, raw = _fixture_input(run_id)
    run = historical_input.decode_historical_run_input(raw)
    expected_outcome = next(row[2] for row in _FIXTURES if row[0] == run_id)
    expected_reason = next(row[3] for row in _FIXTURES if row[0] == run_id)
    mode, audits, captures, rust_snapshots = _install_observer(monkeypatch)
    output_root = tmp_path / "durable"
    output_root.mkdir()
    durable = scenario.run_spider_scenario(
        str(output_root), run.run_id, scenario._P3_SELECTOR, raw
    )
    durable_dir = output_root / run.run_id

    assert durable == {
        "run_id": run.run_id,
        "outcome": expected_outcome,
        "reason": expected_reason,
    }
    independent_admit = admission.admit_spider_run
    independent = admission.admit_spider_run(durable_dir)
    assert independent["decision"] == ("ACCEPT" if run_id == "fin" else "REJECT")
    assert independent.get("reason") == expected_reason
    assert compare(durable_dir, baseline)["equal"] is True

    endpoint = cast(
        dict[str, Any],
        decode_canonical((durable_dir / "endpoint.json").read_bytes().rstrip(b"\n")),
    )
    report_raw = (durable_dir / "report.jsonl").read_bytes()
    journal_raw = (durable_dir / "journal.jsonl").read_bytes()
    cwd = tmp_path / "research-cwd"
    home = tmp_path / "research-home"
    cwd.mkdir()
    home.mkdir()
    before = _listing(tmp_path)
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("HOME", str(home))

    def deny(*_args, **_kwargs):
        raise AssertionError("research entry reached durable I/O or admission")

    for name in (
        "create",
        "register",
        "mark_processed",
        "append_journal",
        "publish_failure",
        "finalize",
    ):
        monkeypatch.setattr(store.SpiderRunStore, name, deny)
    monkeypatch.setattr(admission, "admit_spider_run", deny)
    monkeypatch.setattr(evidence, "build_endpoint_artifacts", deny)
    monkeypatch.setattr(durable_ports, "build_endpoint_artifacts", deny)
    mode[0] = "research"
    result = research_entry.run_historical_research(raw)

    assert isinstance(result, research_output.ResearchRunResult)
    assert (result.research_classification, result.admission_state) == (
        "RESEARCH_ONLY",
        "NOT_ADMITTED",
    )
    assert _listing(tmp_path) == before
    assert not (output_root / "research" / run.run_id).exists()
    assert audits["durable"]["rows"] == audits["research"]["rows"]
    assert audits["durable"]["journal"].digest() == sha256(journal_raw).digest()
    assert audits["research"]["journal"].digest() == sha256(journal_raw).digest()
    assert audits["durable"]["events"].digest() == audits["research"]["events"].digest()
    assert audits["durable"]["groups_count"] == audits["research"]["groups_count"]
    assert audits["durable"]["groups"].digest() == audits["research"]["groups"].digest()
    assert audits["durable"]["fills_count"] == audits["research"]["fills_count"]
    assert audits["durable"]["fills"].digest() == audits["research"]["fills"].digest()
    if run_id == "h360":
        assert audits["research"]["fills_count"] > 0
    assert captures["durable"] == captures["research"]
    assert rust_snapshots["durable"] == rust_snapshots["research"]
    expected_owner_requests = [
        raw_request
        for capture in captures["durable"]
        for raw_request in capture["request_bytes"]
    ]
    snapshot_index = 0
    for raw_request in rust_snapshots["durable"]:
        if (
            snapshot_index < len(expected_owner_requests)
            and raw_request == expected_owner_requests[snapshot_index]
        ):
            snapshot_index += 1
    assert snapshot_index == len(expected_owner_requests)
    for trace in captures["research"]:
        prefix = trace["prefix"]
        if prefix in ("INITIAL", "FINAL"):
            continue
        ordinal = int(prefix.split("-", 1)[1])
        assert trace["event_key"] == audits["research"]["barriers"][ordinal]

    research_report_raw = encode_jsonl(cast(list[object], list(result.report_rows)))
    assert research_report_raw == report_raw
    assert canonical_bytes(result.final_owner_evidence) == canonical_bytes(
        endpoint["final_owner_evidence"]
    )
    assert canonical_bytes(result.final_scheduler_observation) == canonical_bytes(
        endpoint["scheduler_observation"]
    )
    assert (
        result.final_owner_evidence["inspection"]["owner_state_digest"]
        == (endpoint["final_owner_evidence"]["inspection"]["owner_state_digest"])
    )
    assert sha256(research_report_raw).hexdigest() == sha256(report_raw).hexdigest()
    if run_id == "fin":
        tampered = tmp_path / "tampered"
        shutil.copytree(durable_dir, tampered)
        rows = decode_jsonl((tampered / "report.jsonl").read_bytes())
        rows[0]["account_equity"] = "999999999"
        report_mutation = encode_jsonl(cast(list[object], rows))
        (tampered / "report.jsonl").write_bytes(report_mutation)
        manifest = cast(
            dict[str, Any],
            decode_canonical((tampered / "completion.json").read_bytes().rstrip(b"\n")),
        )
        report_entry = next(
            item for item in manifest["artifacts"] if item["path"] == "report.jsonl"
        )
        report_entry["sha256"] = sha256(report_mutation).hexdigest()
        report_entry["byte_count"] = len(report_mutation)
        (tampered / "completion.json").write_bytes(canonical_bytes(manifest) + b"\n")
        recomputed = independent_admit(tampered)
        assert recomputed["decision"] == "REJECT"
        assert recomputed["reason"] == "ENDPOINT_RECONCILIATION_FAILED"


def _run_failure_pair(tmp_path, monkeypatch, failure: str):
    value, raw = frozen_h03_input(f"c7_{failure}")
    install_frozen_h03_order(monkeypatch)
    mode = ["durable"]
    seen: dict[str, list[Any]] = {"durable": [], "research": []}
    durable_decisions: dict[str, Any] = {}
    durable_checkpoints: list[dict[str, Any]] = []
    durable_retained_ordinals: list[int] = []
    original_market = wire.ScenarioCodec.historical_market_step
    original_capture = scenario._ReplayComposition._capture_owner_evidence_core
    original_checkpoint = durable_ports.DurableHistoricalOutputAdapter.checkpoint
    original_d_retain = durable_ports.DurableHistoricalOutputAdapter.retain
    original_r_retain = research_output.ResearchEvidenceSink.retain
    original_fail = durable_ports.DurableHistoricalOutputAdapter.fail
    original_outer_classifier = scenario._classify_historical_failure
    target_ms = value.range_start_ms

    def market(self, node):
        key = (node["bar_open_ms"], node["step_index"])
        seen[mode[0]].append(("market", key))
        if failure == "forced_event" and key == (target_ms, 0):
            raise _native_error_type("ScenarioReplayInvariantError")("C7_FORCED_FAILURE")
        return original_market(self, node)

    def capture(self, cutoff, *requests):
        ids = [request["snapshot_id"] for request in requests]
        seen[mode[0]].append(("capture", cutoff, tuple(ids)))
        if failure == "capture_fault" and ids[0].startswith("MARKET-"):
            raise _native_error_type("ScenarioReplayInvariantError")("C7_CAPTURE_FAILURE")
        return original_capture(self, cutoff, *requests)

    def retain(original):
        def wrapped(self, row):
            fills = []
            if row["record_kind"] == "HISTORICAL_MARKET_RESULT":
                fills = [
                    fill
                    for product in row["payload"]["result"]["products"]
                    for fill in product["fills"]
                ]
            fill_digest = sha256(
                b"".join(canonical_bytes(fill) + b"\n" for fill in fills)
            ).hexdigest()
            seen[mode[0]].append(
                (
                    "retain",
                    row["journal_seq"],
                    row["barrier_id"],
                    len(fills),
                    fill_digest,
                )
            )
            if failure == "retain_fault" and row["journal_seq"] == 1:
                raise _native_error_type("ScenarioReplayInvariantError")("C7_RETAIN_FAILURE")
            result = original(self, row)
            if mode[0] == "durable":
                durable_retained_ordinals.append(row["journal_seq"])
            return result

        return wrapped

    def checkpoint(self, boundary, kind):
        result = original_checkpoint(self, boundary, kind)
        if mode[0] == "durable":
            durable_checkpoints.append(
                {
                    "ordinal": boundary["ordinal"],
                    "kind": kind,
                    "event_key": boundary["barrier_id"],
                }
            )
        return result

    def fail(
        self, classification, reason, primary, last_boundary, last_recorded_ordinal
    ):
        if mode[0] == "durable":
            durable_decisions["sink"] = {
                "failure_classification": classification,
                "reason": reason,
                "primary": primary,
                "last_processed_boundary": last_boundary,
                "last_fully_recorded_row_ordinal": last_recorded_ordinal,
            }
        return original_fail(
            self, classification, reason, primary, last_boundary, last_recorded_ordinal
        )

    def outer_classifier(*args, **kwargs):
        result = original_outer_classifier(*args, **kwargs)
        if mode[0] == "durable":
            durable_decisions["outer"] = result
        return result

    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", market)
    monkeypatch.setattr(
        scenario._ReplayComposition, "_capture_owner_evidence_core", capture
    )
    monkeypatch.setattr(
        durable_ports.DurableHistoricalOutputAdapter,
        "checkpoint",
        checkpoint,
    )
    monkeypatch.setattr(
        durable_ports.DurableHistoricalOutputAdapter,
        "retain",
        retain(original_d_retain),
    )
    monkeypatch.setattr(
        research_output.ResearchEvidenceSink, "retain", retain(original_r_retain)
    )
    monkeypatch.setattr(durable_ports.DurableHistoricalOutputAdapter, "fail", fail)
    monkeypatch.setattr(scenario, "_classify_historical_failure", outer_classifier)

    durable_path = tmp_path / "durable"
    durable_path.mkdir()
    try:
        durable = scenario.run_spider_scenario(
            str(durable_path), value.run_id, scenario._P3_SELECTOR, raw
        )
    except BaseException as error:
        durable = error
    mode[0] = "research"
    try:
        research = research_entry.run_historical_research(raw)
    except BaseException as error:
        research = error
    return (
        value,
        durable,
        research,
        seen,
        durable_path / value.run_id,
        durable_decisions,
        durable_checkpoints,
        durable_retained_ordinals,
    )


def _assert_failure_dto_matches_durable(research, expected: dict[str, Any]) -> None:
    assert isinstance(research, research_output.ResearchFailureResult)
    assert research.outcome == expected["outcome"]
    assert research.failure_classification == expected["failure_classification"]
    assert research.reason == expected["reason"]
    assert research.primary == expected["primary"]
    assert research.last_processed_boundary == expected["last_processed_boundary"]
    assert (
        research.last_fully_recorded_row_ordinal
        == expected["last_fully_recorded_row_ordinal"]
    )


@pytest.mark.parametrize("failure", ["forced_event", "capture_fault", "retain_fault"])
def test_forced_capture_and_retain_failures_match_both_modes(
    tmp_path, monkeypatch, failure
):
    (
        value,
        durable,
        research,
        seen,
        durable_dir,
        durable_decisions,
        durable_checkpoints,
        durable_retained_ordinals,
    ) = _run_failure_pair(tmp_path, monkeypatch, failure)
    assert seen["durable"] == seen["research"], (durable, research)
    assert not isinstance(durable, dict) or durable["outcome"] != "ADMITTED"
    assert not isinstance(research, research_output.ResearchRunResult)
    if isinstance(durable, BaseException):
        assert isinstance(research, BaseException)
        assert type(research) is type(durable)
        assert research.args == durable.args
    else:
        assert isinstance(research, research_output.ResearchFailureResult)
        status = cast(
            dict[str, Any],
            decode_canonical((durable_dir / "status.json").read_bytes().rstrip(b"\n")),
        )
        decision = durable_decisions.get("sink")
        if decision is None:
            outer = durable_decisions["outer"]
            decision = {
                "failure_classification": outer.classification,
                "reason": status["failure_reason"],
                "primary": status["primary_failure"],
                "last_processed_boundary": None,
            }
        expected = {
            "outcome": durable["outcome"],
            "failure_classification": decision["failure_classification"],
            "reason": status["failure_reason"],
            "primary": status["primary_failure"],
            "last_processed_boundary": durable_checkpoints[-1]
            if durable_checkpoints
            else None,
            "last_fully_recorded_row_ordinal": durable_retained_ordinals[-1]
            if durable_retained_ordinals
            else None,
        }
        assert durable["reason"] == expected["reason"]
        assert research.admission_state == "NOT_ADMITTED"
        _assert_failure_dto_matches_durable(research, expected)

        mutations = [
            ("failure_classification", "MUTATED_CLASS"),
            ("outcome", "MUTATED_OUTCOME"),
            ("last_fully_recorded_row_ordinal", 99999),
        ]
        if expected["last_processed_boundary"] is not None:
            mutations.append(("last_processed_boundary", None))
        for field, mutated_value in mutations:
            with pytest.raises(AssertionError):
                _assert_failure_dto_matches_durable(
                    replace(research, **{field: mutated_value}), expected
                )
    if durable_dir.is_dir():
        assert not (durable_dir / "completion.json").exists()
        status_path = durable_dir / "status.json"
        if status_path.is_file() and isinstance(
            research, research_output.ResearchFailureResult
        ):
            status = cast(
                dict[str, Any],
                decode_canonical(status_path.read_bytes().rstrip(b"\n")),
            )
            if status["failure_reason"] is not None:
                assert status["failure_reason"] == research.reason
            if status["primary_failure"] is not None:
                assert status["primary_failure"] == research.primary
    if failure == "forced_event":
        assert seen["durable"][-1] == ("market", (value.range_start_ms, 0))
    if failure == "capture_fault":
        assert seen["durable"][-1][0] == "capture"
        assert seen["durable"][-1][2][0].startswith("MARKET-")
    if failure == "retain_fault":
        for observed in seen.values():
            retains = [item for item in observed if item[0] == "retain"]
            assert len(retains) == 1 and retains[0][1] == 1
            assert observed[-1][0] == "retain"
