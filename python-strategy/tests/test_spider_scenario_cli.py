"""CLI transport and GT-06 evidence from real frozen runs, never profit fixtures."""

from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, cast

import pytest

from examples import run_spider_scenario_replay as cli
from src.core.backtest import spider_scenario_run as run
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from src.core.backtest.spider_scenario_plans import CLI_PLAN_IDS, PLAN_IDS
from test_spider_run_admission import write_bundle

ROOT = Path(__file__).parents[1]
CLI = ROOT / "examples/run_spider_scenario_replay.py"


class InvocationSubclass(ValueError):
    pass


def args(root, name="r1", selector=CLI_PLAN_IDS[0]):
    return ["--output-root", str(root), "--run-id", name, "--scenario-selector", selector]


def command(root, name="r1", selector=CLI_PLAN_IDS[0]):
    return subprocess.run([sys.executable, str(CLI), *args(root, name, selector)], cwd=root,
        env=dict(os.environ, PYTHONPATH=str(ROOT)), capture_output=True, text=True, encoding="utf-8", timeout=20)


@pytest.mark.parametrize("outcome,reason,code", [("ADMITTED", None, 0), ("REJECTED", "UNSUPPORTED_CONFIGURATION", 2),
    ("FAILED", "UNSUPPORTED_CONFIGURATION", 3), ("FAILED", "NATIVE_FAULT", 3),
    ("DURABILITY_UNKNOWN", "PUBLICATION_DURABILITY_UNKNOWN", 3)])
def test_result_transport_unchanged_one_call(tmp_path, capsys, monkeypatch, outcome, reason, code):
    result = dict(run_id="r1", outcome=outcome, reason=reason)
    calls = []
    def execute(*values):
        calls.append(values)
        return result
    monkeypatch.setattr(run, "run_spider_scenario", execute)
    assert cli.main(args(tmp_path)) == code
    captured = capsys.readouterr()
    assert captured.out == json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n" and captured.err == ""
    assert calls == [(str(tmp_path), "r1", CLI_PLAN_IDS[0])]


@pytest.mark.parametrize("error,code,stderr", [(ValueError("INVALID_INVOCATION"), 2, "INVALID_INVOCATION\n"),
    (InvocationSubclass("INVALID_INVOCATION"), 3, "INTERNAL_FAILURE\n"),
    (ValueError("private"), 3, "INTERNAL_FAILURE\n"), (RuntimeError("private"), 3, "INTERNAL_FAILURE\n")])
def test_propagated_exceptions_are_bounded(tmp_path, capsys, monkeypatch, error, code, stderr):
    def fail(*_):
        raise error
    monkeypatch.setattr(run, "run_spider_scenario", fail)
    assert cli.main(args(tmp_path)) == code
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == stderr


@pytest.mark.parametrize("argv,code", [([], 2), (args("unused") + ["--unknown"], 2),
    (["--output", "unused", "--run-id", "r1", "--scenario-selector", CLI_PLAN_IDS[0]], 2), (["--help"], 0)])
def test_argparse_stops_before_orchestration(argv, code, capsys, monkeypatch):
    monkeypatch.setattr(run, "run_spider_scenario", lambda *_: pytest.fail("must not execute"))
    with pytest.raises(SystemExit) as caught:
        cli.main(argv)
    assert caught.value.code == code
    captured = capsys.readouterr()
    assert (bool(captured.out), bool(captured.err)) == (code == 0, code != 0)


def test_repeated_option_is_last_value(tmp_path, monkeypatch, capsys):
    calls = []
    def execute(*values):
        calls.append(values)
        return dict(run_id=values[1], outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION")
    monkeypatch.setattr(run, "run_spider_scenario", execute)
    assert cli.main(args(tmp_path) + ["--output-root", "last-root", "--run-id", "last", "--scenario-selector", "unknown"]) == 2
    assert calls == [("last-root", "last", "unknown")]
    assert json.loads(capsys.readouterr().out)["run_id"] == "last"


@pytest.mark.parametrize("selector", ["unknown", PLAN_IDS[2]])
def test_subprocess_unsupported_selector_is_durable_rejected(tmp_path, selector):
    result = command(tmp_path, selector=selector)
    assert result.returncode == 2 and result.stderr == ""
    assert json.loads(result.stdout) == dict(run_id="r1", outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION")
    attempt = decode_jsonl((tmp_path / "r1/attempt.json").read_bytes())[0]
    assert attempt["requested_scenario_selector"] == selector and attempt["registration_state"] == "REJECTED"
    assert decode_jsonl((tmp_path / "r1/status.json").read_bytes())[0]["state"] == "FAILED"
    assert not (tmp_path / "r1/journal.jsonl").exists()


def test_actual_invalid_invocation_emits_no_json_or_artifacts(tmp_path):
    result = command(tmp_path, "../invalid")
    assert (result.returncode, result.stdout, result.stderr) == (2, "", "INVALID_INVOCATION\n")
    assert list(tmp_path.iterdir()) == []


def normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: {"first": "normalized", "second": "normalized"}[item] if key == "run_id" else normalize(item) for key, item in value.items()}
    return [normalize(item) for item in value] if isinstance(value, list) else value


@pytest.mark.parametrize("selector", CLI_PLAN_IDS)
def test_actual_losing_admission_and_two_run_rehashed_determinism(tmp_path, selector):
    normalized = []
    for name in ("first", "second"):
        result = command(tmp_path, name, selector)
        assert (result.returncode, result.stderr) == (0, "")
        assert result.stdout == json.dumps(dict(run_id=name, outcome="ADMITTED", reason=None), sort_keys=True, separators=(",", ":")) + "\n"
        admitted = admit_spider_run(tmp_path / name)
        assert admitted["decision"] == "ACCEPT"
        values = cast(dict[str, Any], admitted["artifacts"])
        endpoint = values["endpoint.json"]
        assert Decimal(endpoint["final_owner_evidence"]["inspection"]["cash"]) < Decimal(endpoint["initial_owner_evidence"]["inspection"]["cash"])
        for metadata in values["completion.json"]["artifacts"]:
            raw = (tmp_path / name / metadata["path"]).read_bytes()
            assert (sha256(raw).hexdigest(), len(raw), len(decode_jsonl(raw))) == (metadata["sha256"], metadata["byte_count"], metadata["row_count"])
        values = normalize(deepcopy(values))
        report_check = values["reconciliation.json"]["checks"][-1]
        for side in ("expected", "observed"):
            report_check[side]["report_sha256"] = sha256(canonical_bytes(report_check[side]["report_rows"])).hexdigest()
        directory = tmp_path / (name + "-normalized")
        directory.mkdir()
        write_bundle(directory, values)
        assert admit_spider_run(directory)["decision"] == "ACCEPT"
        normalized.append({path.name: path.read_bytes() for path in directory.iterdir()})
    assert normalized[0] == normalized[1]


@pytest.mark.parametrize("mutation,reason", [("missing", "MISSING_COMPLETE_MANIFEST"), ("truncated", "ARTIFACT_MISMATCH"),
    ("mixed", "RUN_ID_MISMATCH"), ("open", "ENDPOINT_RECONCILIATION_FAILED")])
def test_gt06_tamper_real_cli_bundle(tmp_path, mutation, reason):
    assert command(tmp_path).returncode == 0
    root = tmp_path / "r1"
    values = cast(dict[str, Any], admit_spider_run(root)["artifacts"])
    if mutation == "missing":
        (root / "completion.json").unlink()
    elif mutation == "truncated":
        report = root / "report.jsonl"
        report.write_bytes(report.read_bytes()[:-1])
    else:
        if mutation == "mixed":
            values["report.jsonl"][0]["run_id"] = "other"
        else:
            values["endpoint.json"]["final_owner_evidence"]["inspection"]["lifecycle"] = "AWAITING_CANCEL_EFFECTIVE"
        write_bundle(root, values)
    assert admit_spider_run(root)["reason"] == reason


def test_gt06_real_third_append_failure(tmp_path, monkeypatch, capsys):
    original = run._Store.append_journal
    def append(store, row):
        if row["journal_seq"] == 3:
            raise run._StoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        original(store, row)
    monkeypatch.setattr(run._Store, "append_journal", append)
    assert cli.main(args(tmp_path)) == 3
    assert json.loads(capsys.readouterr().out)["reason"] == "PERSISTENCE_FAILED"
    assert admit_spider_run(tmp_path / "r1")["reason"] == "INCOMPLETE_PERSISTENCE"
