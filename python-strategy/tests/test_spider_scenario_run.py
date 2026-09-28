"""Bounded orchestration proofs across the real store, scheduler and native owner."""

import ast
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.core.backtest import spider_scenario_run as run, synthetic_scenario_codec as wire
from src.core.backtest import spider_run_store as storage
from src.core.backtest.spider_run_artifacts import decode_jsonl
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_scenario_plans import PLAN_IDS, plan_bundle


def read(root, run_id, name):
    return cast(list[dict[str, Any]], decode_jsonl((root / run_id / name).read_bytes()))


def invoke(root, index=0, name="r1"):
    return (run._run_o03 if index == 2 else run.run_spider_scenario)(str(root), name, PLAN_IDS[index])


@pytest.mark.parametrize("index", range(3))
def test_real_runs_registration_order_exact_artifacts_and_one_admission(tmp_path, index, monkeypatch):
    constructed, admitted = [], []
    original, admission = run._ReplayComposition, run._admit
    def owner(*args, **kwargs):
        assert read(tmp_path, "r1", "status.json")[0]["state"] == "RUNNING"
        assert (tmp_path / "r1/journal.jsonl").read_bytes() == b""
        constructed.append(args)
        return original(*args, **kwargs)
    def admit(path):
        assert (path / "completion.json").exists()
        admitted.append(path)
        return admission(path)
    monkeypatch.setattr(run, "_ReplayComposition", owner)
    monkeypatch.setattr(run, "_admit", admit)
    result = invoke(tmp_path, index)
    assert result == dict(run_id="r1", outcome="ADMITTED", reason=None)
    assert len(constructed) == len(admitted) == 1
    frozen = cast(dict[str, Any], plan_bundle(PLAN_IDS[index]))
    for name in ("journal", "endpoint", "report"):
        expected = frozen[name] if name != "endpoint" else [frozen[name]]
        for row in expected:
            row["run_id"] = "r1"
        assert read(tmp_path, "r1", name + (".json" if name == "endpoint" else ".jsonl")) == expected
    attempt = read(tmp_path, "r1", "attempt.json")[0]
    assert attempt["native_artifact_sha256"] == sha256(Path(wire.loaded_native_artifact_path()).read_bytes()).hexdigest()
    records = [(name.encode(), sha256((run._ROOT / name).read_bytes()).digest()) for name in sorted(run._PROGRAM)]
    assert len(records) == len(set(run._PROGRAM)) == 17
    assert attempt["program_sha256"] == sha256(b"".join(len(name).to_bytes(8, "big") + name + digest for name, digest in records)).hexdigest()


@pytest.mark.parametrize("field,value", [(0, ""), (0, "bad\0root"), (0, None), (1, "../bad"), (1, True), (2, "bad selector"), (2, 1)])
def test_invalid_transport_never_creates(tmp_path, field, value):
    args = [str(tmp_path), "r1", PLAN_IDS[0]]
    args[field] = value
    with pytest.raises(ValueError, match="^INVALID_INVOCATION$"):
        run.run_spider_scenario(*args)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("selector", ["unknown", PLAN_IDS[2]])
def test_rejected_selector_constructs_no_owner(tmp_path, monkeypatch, selector):
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("no native owner"))
    assert run.run_spider_scenario(str(tmp_path), "r1", selector)["outcome"] == "REJECTED"
    attempt = read(tmp_path, "r1", "attempt.json")[0]
    assert attempt["registration_state"] == "REJECTED" and attempt["planned_coverage"] == []
    assert all(attempt[key] is None for key in run._IDENTITIES)
    assert read(tmp_path, "r1", "status.json")[0]["failure_reason"] == "UNSUPPORTED_CONFIGURATION"
    assert not (tmp_path / "r1/journal.jsonl").exists()


@pytest.mark.parametrize("fault", ["manifest", "program", "native", "hash", "recipe"])
def test_preowner_provenance_and_recipe_fail_closed(tmp_path, monkeypatch, fault):
    original_read, select = Path.read_bytes, run._plans.cli_plan_bundle
    def bytes_(path):
        if fault == "manifest" and path.name == "source_manifest.json":
            return b"wrong"
        if fault == "program" and path.name == "spider_policy.py":
            raise FileNotFoundError()
        return original_read(path)
    def bundle(selector):
        result = cast(dict[str, Any], select(selector))
        if fault == "hash":
            result["plan_sha256"] = "f" * 64
        if fault == "recipe":
            result["journal"] = []
        return result
    monkeypatch.setattr(Path, "read_bytes", bytes_)
    monkeypatch.setattr(run._plans, "cli_plan_bundle", bundle)
    if fault == "native":
        monkeypatch.setattr(wire, "loaded_native_artifact_path", lambda: "/no-such-spider-native")
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("no owner"))
    assert invoke(tmp_path) == dict(run_id="r1", outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION")


def test_f01_keeps_native_three_processed_three_persisted_two_and_new_run_clean(tmp_path, monkeypatch):
    append, apply = storage.SpiderRunStore.append_journal, wire.ScenarioCodec.apply_group
    observed = []
    def group(owner, request):
        result = apply(owner, request)
        observed.append((request["group_id"], owner.inspect_state()))
        return result
    def fail(store, row):
        if row["journal_seq"] == 3:
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        append(store, row)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", group)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail)
    assert invoke(tmp_path)["reason"] == "PERSISTENCE_FAILED"
    assert [name for name, _ in observed] == ["MIN-X1", "reject", "context"]
    assert observed[-1][1]["account_version"] == 2
    status = read(tmp_path, "r1", "status.json")[0]
    assert (status["processed_boundary"]["ordinal"], status["persisted_boundary"]["ordinal"]) == (3, 2)
    assert len(read(tmp_path, "r1", "journal.jsonl")) == 2
    assert admit_spider_run(tmp_path / "r1")["reason"] == "INCOMPLETE_PERSISTENCE"
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", append)
    assert invoke(tmp_path, name="r2")["outcome"] == "ADMITTED"
    assert len(read(tmp_path, "r1", "journal.jsonl")) == 2
    assert invoke(tmp_path, name="r2")["reason"] == "PUBLICATION_FAILED"


def test_callback_partial_prefix_persists_once_and_never_retries(tmp_path, monkeypatch):
    calls = []
    def callback(owner, delivery):
        calls.append(delivery["delivery_id"])
        owner._policy.emit("alert", reason="total_limit")
        raise RuntimeError("private callback detail")
    monkeypatch.setattr(run._ReplayComposition, "_apply_payload", callback)
    assert invoke(tmp_path)["reason"] == "CALLBACK_FAILED"
    rows = read(tmp_path, "r1", "journal.jsonl")
    assert len(rows) == 5 and len(calls) == 1
    assert rows[-1]["payload"]["policy_events"][0]["kind"] == "alert"
    assert rows[-1]["payload"]["outcome"] == "CALLBACK_FAILED"
    assert read(tmp_path, "r1", "status.json")[0]["primary_failure"] == dict(kind="CALLBACK", reason="CALLBACK_FAILED")
    assert not (tmp_path / "r1/completion.json").exists()


@pytest.mark.parametrize("phase", ["constructor", "initial", "after", "final"])
def test_native_failures_at_each_capture_boundary(tmp_path, monkeypatch, phase):
    codec = wire.ScenarioCodec("SYNTHETIC_MIN_CASH_V1", cast(wire.Account, dict(venue="okx-scenario", environment="test", account="A")))
    with pytest.raises(ValueError) as rejected:
        codec.capture_snapshot(cast(wire.SnapshotRequest, {}))
    error = rejected.value
    capture = run._ReplayComposition.capture_owner_evidence
    def fail(owner, cutoff, *requests):
        if cutoff == {"initial": 500, "after": 501, "final": 504}.get(phase):
            raise error
        return capture(owner, cutoff, *requests)
    def constructor(*args, **kwargs):
        raise error
    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", fail)
    if phase == "constructor":
        monkeypatch.setattr(run, "_ReplayComposition", constructor)
    assert invoke(tmp_path)["reason"] == ("PERSISTENCE_FAILED" if phase == "after" else "NATIVE_FAULT")
    status = read(tmp_path, "r1", "status.json")[0]
    if phase == "after":
        assert status["processed_boundary"]["ordinal"] == 1 and status["persisted_boundary"] is None
        assert status["primary_failure"] == dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED")


@pytest.mark.parametrize("kind,failure,expected", [("SOURCE_GROUP_RESULT", dict(failure="BROKEN"), "NATIVE"),
    ("CALLBACK_RESULT", dict(kind="CALLBACK", reason="CALLBACK_FAILED"), "CALLBACK"),
    ("CALLBACK_RESULT", dict(kind="PLAN", reason="POLICY_EMISSION_MISMATCH"), "SCHEDULER"),
    ("CALLBACK_RESULT", dict(kind="NATIVE", reason="BROKEN"), "NATIVE")])
def test_upstream_failure_precedence_over_append_failure(tmp_path, monkeypatch, kind, failure, expected):
    original = run._ReplayComposition._evidence
    def evidence(owner, actual_kind, key, payload):
        if actual_kind == kind:
            payload = deepcopy(payload)
            if kind == "SOURCE_GROUP_RESULT":
                payload["result"].update(classification="FAULT", **failure)
            else:
                payload["failure"] = failure
        return original(owner, actual_kind, key, payload)
    append = storage.SpiderRunStore.append_journal
    def fail(store, row):
        if row["record_kind"] == kind:
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        append(store, row)
    monkeypatch.setattr(run._ReplayComposition, "_evidence", evidence)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail)
    assert invoke(tmp_path)["reason"] == "PERSISTENCE_FAILED"
    primary = read(tmp_path, "r1", "status.json")[0]["primary_failure"]
    assert primary == dict(kind=expected, reason=failure.get("reason", "BROKEN"))


@pytest.mark.parametrize("publication", [False, True])
def test_unknown_after_error_propagates_original_unless_failure_publication_fails(tmp_path, monkeypatch, publication):
    error = RuntimeError("private programming detail")
    capture = run._ReplayComposition.capture_owner_evidence
    def fail(owner, cutoff, *requests):
        if cutoff == 501:
            raise error
        return capture(owner, cutoff, *requests)
    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", fail)
    if publication:
        def publish(*args):
            raise storage.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN", "UNEXPECTED_EXCEPTION")
        monkeypatch.setattr(storage.SpiderRunStore, "publish_failure", publish)
        assert invoke(tmp_path) == dict(run_id="r1", outcome="DURABILITY_UNKNOWN", reason="PUBLICATION_DURABILITY_UNKNOWN")
    else:
        with pytest.raises(RuntimeError) as caught:
            invoke(tmp_path)
        assert caught.value is error
        status = read(tmp_path, "r1", "status.json")[0]
        assert status["failure_reason"] == "UNEXPECTED_EXCEPTION" and status["primary_failure"] is None
        assert status["processed_boundary"]["ordinal"] == 1


@pytest.mark.parametrize("outcome", ["reject", "frozen_error", "finalize_error"])
def test_completion_admission_and_publication_mapping(tmp_path, monkeypatch, outcome):
    finalize = storage.SpiderRunStore.finalize
    def fail(store, **artifacts):
        if outcome == "frozen_error":
            finalize(store, **artifacts)
        raise storage.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN" if outcome == "frozen_error" else "PUBLICATION_FAILED")
    if outcome == "reject":
        monkeypatch.setattr(run, "_admit", lambda _: dict(decision="REJECT", reason="ARTIFACT_MISMATCH"))
    else:
        monkeypatch.setattr(storage.SpiderRunStore, "finalize", fail)
    result = invoke(tmp_path)
    assert result["reason"] == {"reject": "ARTIFACT_MISMATCH", "frozen_error": "PUBLICATION_DURABILITY_UNKNOWN", "finalize_error": "PUBLICATION_FAILED"}[outcome]
    assert read(tmp_path, "r1", "status.json")[0]["state"] == ("FAILED" if outcome == "finalize_error" else "COMPLETE")


@pytest.mark.parametrize("direct,child,valid", [("/x/core.so", None, True), ("/x/__init__.py", "/x/child.so", True),
    ("/x/__init__.py", None, False), (None, "/x/child.so", False), ("/x/wrapper.py", "/x/child.so", False),
    ("/x/__init__.py", 1, False), ("/x/__init__.py", "/x/child.py", False), (1, None, False)])
def test_codec_loaded_binary_path_only(direct, child, valid, monkeypatch):
    monkeypatch.setattr(wire, "_native", SimpleNamespace(__file__=direct, fluxtrade_core=SimpleNamespace(__file__=child)))
    if valid:
        assert wire.loaded_native_artifact_path() == (direct if direct.endswith(".so") else child)
    else:
        with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
            wire.loaded_native_artifact_path()


def test_surface_private_o03_and_no_native_access():
    assert {name for name in vars(run) if not name.startswith("_")} == {"run_spider_scenario"}
    with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
        run._run_o03("unused", "r1", PLAN_IDS[0])
    tree = ast.parse(Path(run.__file__).read_text())
    assert not any(isinstance(node, ast.Attribute) and node.attr == "_native" for node in ast.walk(tree))
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))


@pytest.mark.parametrize("result,reason,kind", [
    (dict(reason="NATIVE_DETAIL", native_failure=dict(reason="NATIVE_DETAIL")), "NATIVE_FAULT", "NATIVE"),
    (dict(reason="FAULT_DETAIL", group_result=dict(classification="FAULT")), "NATIVE_FAULT", "NATIVE"),
    (dict(reason="CALLBACK_FAILED"), "CALLBACK_FAILED", "CALLBACK"),
    (dict(reason="INVALID_SCHEMA"), "SCHEDULER_FAILED", "SCHEDULER"),
])
def test_terminal_mapping_has_no_poison_inference(tmp_path, monkeypatch, result, reason, kind):
    monkeypatch.setattr(run._ReplayComposition, "_dispatch_due", lambda *_: dict(classification="TERMINAL", **result))
    assert invoke(tmp_path)["reason"] == reason
    status = read(tmp_path, "r1", "status.json")[0]
    assert status["primary_failure"] == dict(kind=kind, reason=result["reason"])


@pytest.mark.parametrize("operation", ["create", "register"])
def test_registration_failure_prevents_owner(tmp_path, monkeypatch, operation):
    def fail(*args):
        raise storage.SpiderRunStoreError("PUBLICATION_FAILED")
    monkeypatch.setattr(storage.SpiderRunStore, operation, fail)
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("no owner"))
    assert invoke(tmp_path)["reason"] == "PUBLICATION_FAILED"


@pytest.mark.parametrize("upstream", [False, True])
def test_after_capture_failure_primary_and_failed_publication_precedence(tmp_path, monkeypatch, upstream):
    owner = wire.ScenarioCodec("SYNTHETIC_MIN_CASH_V1", cast(wire.Account, dict(venue="okx-scenario", environment="test", account="A")))
    with pytest.raises(ValueError) as rejected:
        owner.capture_snapshot(cast(wire.SnapshotRequest, {}))
    capture, emit = run._ReplayComposition.capture_owner_evidence, run._ReplayComposition._evidence
    def evidence(owner, kind, key, payload):
        if upstream and kind == "SOURCE_GROUP_RESULT":
            payload["result"].update(classification="FAULT", failure="ORIGINAL_NATIVE_FAULT")
        return emit(owner, kind, key, payload)
    def fail(owner, cutoff, *requests):
        if cutoff == 501:
            raise rejected.value
        return capture(owner, cutoff, *requests)
    published = []
    def publication(store, reason, primary):
        published.append((reason, primary))
        raise storage.SpiderRunStoreError("PUBLICATION_FAILED", reason, primary)
    monkeypatch.setattr(run._ReplayComposition, "_evidence", evidence)
    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", fail)
    monkeypatch.setattr(storage.SpiderRunStore, "publish_failure", publication)
    assert invoke(tmp_path)["reason"] == "PUBLICATION_FAILED"
    primary = dict(kind="NATIVE", reason="ORIGINAL_NATIVE_FAULT") if upstream else dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED")
    assert published == [("PERSISTENCE_FAILED", primary)]


def test_projection_failure_is_bounded_but_missing_callback_is_not_retried(tmp_path, monkeypatch):
    def project(*args):
        raise run._ProjectionError()
    monkeypatch.setattr(run, "_build", project)
    assert invoke(tmp_path)["reason"] == "ENDPOINT_RECONCILIATION_FAILED"
    monkeypatch.setattr(run._ReplayComposition, "_dispatch_due", lambda *_: dict(classification="SUCCESS"))
    with pytest.raises(RuntimeError, match="^UNOBSERVED_BARRIER$"):
        invoke(tmp_path, name="r2")
    assert read(tmp_path, "r2", "status.json")[0]["failure_reason"] == "UNEXPECTED_EXCEPTION"
