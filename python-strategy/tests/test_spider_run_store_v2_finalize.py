"""P3 v2 finalize lifecycle tests over real durable files."""

import inspect
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_projection_size as projection_size
from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from test_spider_run_store_v2_read import p3_v2_finalize_fixture


@pytest.fixture(scope="module")
def real_p3_artifacts(tmp_path_factory):
    from test_spider_scenario_run import historical_input as make_historical_input
    from src.core.backtest.spider_scenario_run import run_spider_scenario

    root = tmp_path_factory.mktemp("j6-real-p3-finalize")
    run, raw = make_historical_input("j6-real-p3-finalize")
    assert run_spider_scenario(str(root), run.run_id, "SPIDER_HISTORICAL_RESEARCH_RUN_V1", raw) == {
        "run_id": run.run_id, "outcome": "ADMITTED", "reason": None,
    }
    return {
        name: (root / run.run_id / name).read_bytes()
        for name in ("attempt.json", "status.json", "journal.jsonl", "endpoint.json",
                     "reconciliation.json", "report.jsonl", "historical_input.json")
    }


def test_v2_finalize_uses_projection_but_completion_records_wire_bytes(tmp_path, real_p3_artifacts):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    assert store.finalize(values[3], recon, values[4]) == "COMPLETE_PUBLISHED"
    complete = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    entries = cast(list[dict[str, Any]], complete["artifacts"])
    journal_entry = next(item for item in entries if item["path"] == "journal.jsonl")
    assert journal_entry == dict(
        path="journal.jsonl", schema_version="spider_journal_record_v2",
        sha256=sha256(raw).hexdigest(), byte_count=len(raw), row_count=len(values[2]),
    )
    for entry in entries:
        artifact_raw = (store._path / entry["path"]).read_bytes()
        assert entry["sha256"] == sha256(artifact_raw).hexdigest()
        assert entry["byte_count"] == len(artifact_raw)
        assert entry["row_count"] == (artifact_raw.count(b"\n") if entry["path"].endswith(".jsonl") else 1)
    assert (store._path / "journal.jsonl").read_bytes() == raw
    for name in ("endpoint.json", "reconciliation.json", "report.jsonl"):
        assert (store._path / name).read_bytes() == real_p3_artifacts[name]
    assert decode_jsonl((store._path / "endpoint.json").read_bytes()) == [values[3]]
    assert decode_jsonl((store._path / "report.jsonl").read_bytes()) == values[4]
    assert store._state == "FROZEN"


def test_v2_finalize_drops_each_journal_projection_and_preserves_partial_publication(tmp_path,
                                                                                     real_p3_artifacts,
                                                                                     monkeypatch):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    original_publish, original_write = module.SpiderRunStore._publish, module._write
    drop_checks = []

    def observe_publish(self, name, content):
        frame = inspect.currentframe()
        assert frame is not None and frame.f_back is not None
        frame = frame.f_back
        local_values = frame.f_locals
        if name == "endpoint.json":
            drop_checks.append("current_journal" not in local_values)
        if name == "status.json" and b'"COMPLETE"' in content:
            drop_checks.append("journal" not in local_values and local_values["reread"][2][0] is None)
        return original_publish(self, name, content)

    def fail_completion_write(path, content, append=False):
        if Path(path).name == ".completion.json.tmp":
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original_write(path, content, append)

    monkeypatch.setattr(module.SpiderRunStore, "_publish", observe_publish)
    monkeypatch.setattr(module, "_write", fail_completion_write)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.finalize(values[3], recon, values[4])
    assert caught.value.reason == "ARTIFACT_WRITE_FAILED"
    assert drop_checks == [True, True]
    assert (store._path / "journal.jsonl").read_bytes() == raw
    assert b'"state":"COMPLETE"' in (store._path / "status.json").read_bytes()
    assert all((store._path / name).exists() for name in (
        "endpoint.json", "reconciliation.json", "report.jsonl",
    ))
    assert not (store._path / "completion.json").exists()
    assert not (store._path / ".completion.json.tmp").exists()
    assert store._state == "RUNNING"
    before = {p.name: p.read_bytes() for p in store._path.iterdir() if p.is_file()}
    with pytest.raises(module.SpiderRunStoreError) as retry:
        store.finalize(values[3], recon, values[4])
    assert retry.value.reason == "PUBLICATION_FAILED"
    assert not (store._path / "completion.json").exists()
    assert store._state == "RUNNING"
    assert (store._path / "journal.jsonl").read_bytes() == raw
    assert (store._path / "status.json").read_bytes() == before["status.json"]


@pytest.mark.parametrize("failure", ["metadata", "missing_metadata", "mixed", "partial", "projection",
                                     "attempt", "foreign_run_id"])
def test_v2_finalize_read_failures_leave_real_files_and_running_status(tmp_path, real_p3_artifacts,
                                                                       monkeypatch, failure):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal_path = store._path / "journal.jsonl"
    attempt_path = store._path / "attempt.json"
    if failure == "metadata":
        cast(dict[str, object], store._p3_v2_journal_metadata)["sha256"] = "f" * 64
    elif failure == "missing_metadata":
        store._p3_v2_journal_metadata = None
    elif failure == "foreign_run_id":
        rows = decode_jsonl(raw)
        rows[0]["run_id"] = "foreign-run"
        bad = b"".join(canonical_bytes(row) + b"\n" for row in rows)
        journal_path.write_bytes(bad)
        store._p3_v2_journal_metadata = dict(
            schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
            byte_count=len(bad), row_count=len(rows),
        )
    elif failure == "mixed":
        rows = decode_jsonl(raw)
        rows[0]["schema_version"] = "spider_journal_record_v1"
        bad = b"".join(canonical_bytes(row) + b"\n" for row in rows)
        journal_path.write_bytes(bad)
        store._p3_v2_journal_metadata = dict(
            schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
            byte_count=len(bad), row_count=len(rows),
        )
    elif failure == "partial":
        bad = raw[:-1]
        journal_path.write_bytes(bad)
        store._p3_v2_journal_metadata = dict(
            schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
            byte_count=len(bad), row_count=bad.count(b"\n"),
        )
    elif failure == "projection":
        monkeypatch.setattr(projection_size, "P3_V2_MATERIAL_CEILING", 1)
    else:
        attempt = deepcopy(store._attempt)
        attempt["planned_coverage"][0]["barrier_id"] = "different-valid-coverage-id"
        attempt_path.write_bytes(canonical_bytes(attempt) + b"\n")
    journal_before = journal_path.read_bytes()
    status_before = (store._path / "status.json").read_bytes()
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.finalize(values[3], recon, values[4])
    assert caught.value.reason == ("ARTIFACT_LIMIT_EXCEEDED" if failure == "projection"
                                   else "ENDPOINT_RECONCILIATION_FAILED")
    assert journal_path.read_bytes() == journal_before
    assert (store._path / "status.json").read_bytes() == status_before
    assert not any((store._path / name).exists() for name in (
        "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json",
    ))
    assert store._state == "RUNNING"
