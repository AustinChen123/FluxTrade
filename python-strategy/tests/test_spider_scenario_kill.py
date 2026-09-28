"""Actual SIGKILL at acknowledged child-process barriers; no production hooks."""

import json
import multiprocessing
import os
from pathlib import Path
import signal
from typing import Any, cast

import pytest

from src.core.backtest import spider_scenario_run as run, synthetic_scenario_codec as wire
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import decode_jsonl
from src.core.backtest.spider_scenario_plans import CLI_PLAN_IDS


def child(root, window, pipe):
    def reached(**evidence):
        pipe.send(dict(window=window, **evidence))
        signal.pause()
        raise AssertionError("barrier must end only by SIGKILL")
    register, apply = run._Store.register, wire.ScenarioCodec.apply_group
    append, publish = run._Store.append_journal, run._Store._publish
    def registered(self, attempt):
        result = register(self, attempt)
        if window == "running":
            reached(token=result)
        return result
    def applied(self, request):
        result = apply(self, request)
        if window == "native" and request["group_id"] == "context":
            reached(account_version=self.inspect_state()["account_version"])
        return result
    def appended(self, row):
        append(self, row)
        if window == "callback" and row["record_kind"] == "CALLBACK_RESULT":
            reached(journal_seq=row["journal_seq"])
    def published(self, name, raw):
        publish(self, name, raw)
        if window == "status_complete" and name == "status.json" and json.loads(raw)["state"] == "COMPLETE":
            reached(state="COMPLETE")
        if window == "completion" and name == "completion.json":
            reached(state=self._state)
    run._Store.register, wire.ScenarioCodec.apply_group = registered, applied
    run._Store.append_journal, run._Store._publish = appended, published
    run.run_spider_scenario(root, "killed", CLI_PLAN_IDS[0])
    raise AssertionError("named barrier was not reached")


@pytest.mark.parametrize("window,rows,state,evidence", [("running", 0, "RUNNING", dict(token="NATIVE_CONSTRUCTION_ALLOWED")),
    ("native", 2, "RUNNING", dict(account_version=2)), ("callback", 5, "RUNNING", dict(journal_seq=5)),
    ("status_complete", 5, "COMPLETE", dict(state="COMPLETE")), ("completion", 5, "COMPLETE", dict(state="FROZEN"))])
def test_sigkill_only_after_control_pipe_ack(tmp_path, window, rows, state, evidence):
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(target=child, args=(str(tmp_path), window, send))
    try:
        process.start()
        send.close()
        assert receive.poll(20), "child did not acknowledge the named barrier"
        assert receive.recv() == dict(window=window, **evidence)
        assert process.pid is not None
        os.kill(process.pid, signal.SIGKILL)
        process.join(20)
        assert process.exitcode == -signal.SIGKILL
    finally:
        if process.is_alive():
            process.kill()
            process.join(20)
        receive.close()
        send.close()
        process.close()
    root = Path(tmp_path) / "killed"
    status = cast(dict[str, Any], decode_jsonl((root / "status.json").read_bytes())[0])
    assert status["state"] == state and len(decode_jsonl((root / "journal.jsonl").read_bytes())) == rows
    assert status["persisted_boundary"]["ordinal"] == rows if rows else status["persisted_boundary"] is None
    admitted = admit_spider_run(root)
    if window == "completion":
        assert admitted["decision"] == "ACCEPT"
    else:
        assert admitted["reason"] == "MISSING_COMPLETE_MANIFEST"
        assert not (root / "completion.json").exists()
