# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false
"""Process-death boundaries around real P3-v2 journal commits."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_artifacts import canonical_bytes
from spider_run_store_v2_fixtures import empty_v2_writer, p3_v2_finalize_fixture


_CHILD = r'''
import json, os, sys
from pathlib import Path
from src.core.backtest import spider_run_store as module
data = json.load(sys.stdin)
store = module.SpiderRunStore(Path(data["path"]), data["run_id"])
store._attempt = data["attempt"]
store._state = "RUNNING"
store._p3_v2_write_enabled = True
if not store._writes_p3_v2_journal(): os._exit(85)
row = data["row"]
boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"])
store.mark_processed(boundary, record_kind=row["record_kind"])
point = data["point"]
real_write = module._write
real_publish = store._publish
def write(path, raw, append=False):
    if Path(path).name == "journal.jsonl":
        if json.loads(raw)["schema_version"] != "spider_journal_record_v2": os._exit(86)
        if point == "before_journal_fsync": os._exit(81)
        real_write(path, raw, append)
        if point == "after_journal_fsync": os._exit(82)
        return
    real_write(path, raw, append)
def publish(name, raw):
    if name == "status.json":
        if point == "before_status_publish": os._exit(83)
        real_publish(name, raw)
        if point == "after_status_publish": os._exit(84)
        return
    real_publish(name, raw)
module._write = write
store._publish = publish
store.append_journal(row)
os._exit(0)
'''


@pytest.mark.parametrize(
    "point,exit_code,journal_complete,status_persisted",
    [
        ("before_journal_fsync", 81, False, False),
        ("after_journal_fsync", 82, True, False),
        ("before_status_publish", 83, True, False),
        ("after_status_publish", 84, True, True),
    ],
)
def test_v2_crash_restart_at_each_append_commit_boundary(
    tmp_path, real_p3_artifacts, point, exit_code, journal_complete, status_persisted,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    store._processed = store._persisted = None
    store._processed_kind = None
    initial_status = canonical_bytes(store._status("RUNNING")) + b"\n"
    store._publish("status.json", initial_status)
    row = values[2][0]
    child_input = json.dumps(dict(
        path=str(store._path), run_id=store._run_id, attempt=store._attempt,
        row=row, point=point,
    ))
    environment = dict(os.environ)
    environment["TMPDIR"] = "/Volumes/Dev/Scratch/agents"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    process = subprocess.run(
        [sys.executable, "-c", _CHILD], input=child_input, text=True,
        cwd=Path(__file__).parents[1], env=environment, check=False, timeout=30,
    )
    assert process.returncode == exit_code

    journal_bytes = journal.read_bytes()
    status_bytes = (store._path / "status.json").read_bytes()
    if journal_complete:
        assert journal_bytes.endswith(b"\n") and journal_bytes.count(b"\n") == 1
        record = json.loads(journal_bytes)
        assert record["schema_version"] == "spider_journal_record_v2"
    else:
        assert journal_bytes == b""
    assert b'"persisted_boundary":' in status_bytes
    assert b'"state":"RUNNING"' in status_bytes
    if status_persisted:
        assert status_bytes != initial_status
    else:
        assert b'"persisted_boundary":{"barrier_id":' not in status_bytes
    assert (b'"persisted_boundary":{"barrier_id":' in status_bytes) is status_persisted
    before_restart_call = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    restarted = module.SpiderRunStore(store._path, store._run_id)
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        restarted.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id="restart"), record_kind=row["record_kind"])
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before_restart_call
