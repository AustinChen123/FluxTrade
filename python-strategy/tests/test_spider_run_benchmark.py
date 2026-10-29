"""Subprocess safety tests for the Spider benchmark harness."""

from __future__ import annotations

from pathlib import Path
import sys

from tools.spider_run_benchmark import run


def _input(tmp_path: Path) -> Path:
    path = tmp_path / "input.json"
    path.write_bytes(b"test-only")
    return path


def test_benchmark_drains_large_stdout_and_stderr(tmp_path: Path) -> None:
    child = (
        "import json,sys;"
        "sys.stdout.write('x'*200000+'\\n');"
        "sys.stdout.write(json.dumps({'outcome':'FAILED','reason':'synthetic'})+'\\n');"
        "sys.stderr.write('e'*200000)"
    )
    result = run(
        _input(tmp_path), tmp_path / "runs", "large-output",
        command=[sys.executable, "-c", child],
    )
    assert result["process_return_code"] == 0
    assert result["outcome"] == {"outcome": "FAILED", "reason": "synthetic"}
    assert len(result["stderr"]) == 200000
    assert result["admission"] == {"decision": "UNAVAILABLE", "reason": "RUN_DIRECTORY_MISSING"}


def test_benchmark_records_nonzero_exit(tmp_path: Path) -> None:
    child = "import json,sys;print(json.dumps({'outcome':'PROCESS_ERROR','reason':'synthetic'}));sys.exit(7)"
    result = run(
        _input(tmp_path), tmp_path / "runs", "nonzero",
        command=[sys.executable, "-c", child],
    )
    assert result["process_return_code"] == 7
    assert result["return_code"] == 7
    assert result["outcome"]["reason"] == "synthetic"


def test_benchmark_reports_invalid_json_stdout(tmp_path: Path) -> None:
    result = run(
        _input(tmp_path), tmp_path / "runs", "invalid-json",
        command=[sys.executable, "-c", "print('not-json')"],
    )
    assert result["process_return_code"] == 0
    assert result["outcome"]["outcome"] == "PROCESS_ERROR"
    assert "not-json" in result["outcome"]["reason"]
