"""Subprocess safety tests for the Spider benchmark harness."""

from __future__ import annotations

import gc
import os
from pathlib import Path
import signal
import sys
import time
import warnings

import pytest

import tools.spider_run_benchmark as benchmark
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


def test_benchmark_failure_closes_pipes_without_resource_warning(tmp_path: Path) -> None:
    child = "import json,sys;print(json.dumps({'outcome':'PROCESS_ERROR','reason':'synthetic'}));sys.exit(7)"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        result = run(
            _input(tmp_path), tmp_path / "runs", "failed-child-cleanup",
            command=[sys.executable, "-c", child],
        )
        gc.collect()

    assert result["process_return_code"] == 7
    assert not [warning for warning in caught if issubclass(warning.category, ResourceWarning)]


def test_benchmark_reports_invalid_json_stdout(tmp_path: Path) -> None:
    result = run(
        _input(tmp_path), tmp_path / "runs", "invalid-json",
        command=[sys.executable, "-c", "print('not-json')"],
    )
    assert result["process_return_code"] == 0
    assert result["outcome"]["outcome"] == "PROCESS_ERROR"
    assert "not-json" in result["outcome"]["reason"]


def _track_processes(monkeypatch: pytest.MonkeyPatch, *, missing_pipes: set[str] | None = None):
    processes = []
    real_popen = benchmark.subprocess.Popen

    def tracked_popen(command, **kwargs):
        if missing_pipes:
            for pipe_name in missing_pipes:
                kwargs[pipe_name] = benchmark.subprocess.DEVNULL
        process = real_popen(command, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(benchmark.subprocess, "Popen", tracked_popen)
    return processes


@pytest.mark.parametrize(
    ("failure", "expected_error"),
    [
        ("thread_setup", RuntimeError),
        ("thread_start", RuntimeError),
        ("wait4", OSError),
        ("status_conversion", ValueError),
    ],
)
def test_exceptional_exit_reaps_child_and_closes_pipes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str, expected_error: type[Exception],
) -> None:
    processes = _track_processes(monkeypatch)
    child = [sys.executable, "-c", "import time;time.sleep(30)"]
    if failure == "status_conversion":
        child = [sys.executable, "-c", "pass"]

    if failure == "thread_setup":
        def fail_thread_setup(*_args, **_kwargs):
            raise RuntimeError("injected_thread_setup")

        monkeypatch.setattr(benchmark, "Thread", fail_thread_setup)
    elif failure == "thread_start":
        original_start = benchmark.Thread.start
        starts = 0

        def fail_second_start(reader):
            nonlocal starts
            starts += 1
            if starts == 2:
                raise RuntimeError("injected_thread_start")
            original_start(reader)

        monkeypatch.setattr(benchmark.Thread, "start", fail_second_start)
    elif failure == "wait4":
        def fail_wait4(*_args, **_kwargs):
            raise OSError("injected_wait4")

        monkeypatch.setattr(benchmark.os, "wait4", fail_wait4)
    else:
        def fail_status_conversion(_status: int) -> int:
            raise ValueError("injected_status_conversion")

        monkeypatch.setattr(benchmark.os, "waitstatus_to_exitcode", fail_status_conversion)

    with pytest.raises(expected_error, match="injected_"):
        run(_input(tmp_path), tmp_path / "runs", failure, command=child)

    assert len(processes) == 1
    process = processes[0]
    assert process.returncode is not None
    assert process.stdout is not None and process.stdout.closed
    assert process.stderr is not None and process.stderr.closed


@pytest.mark.parametrize("missing", [{"stdout"}, {"stderr"}, {"stdout", "stderr"}])
def test_pipe_guard_reaps_child_and_closes_every_available_pipe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: set[str],
) -> None:
    processes = _track_processes(monkeypatch, missing_pipes=missing)
    child = [sys.executable, "-c", "import time;time.sleep(30)"]

    with pytest.raises(RuntimeError, match="SUBPROCESS_PIPE_UNAVAILABLE"):
        run(_input(tmp_path), tmp_path / "runs", "pipe-guard", command=child)

    process = processes[0]
    assert process.returncode is not None
    for stream in (process.stdout, process.stderr):
        assert stream is None or stream.closed


def _process_is_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def test_benchmark_kills_inherited_pipe_writer_and_closes_both_pipes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    processes = _track_processes(monkeypatch)
    pid_path = tmp_path / "descendant.pid"
    code = (
        "import json,os,time;"
        "pid=os.fork();"
        "(time.sleep(60),os._exit(0)) if pid==0 else None;"
        f"open({str(pid_path)!r},'w').write(str(pid));"
        "print(json.dumps({'outcome':'FAILED','reason':'synthetic','descendant_pid':pid}),flush=True)"
    )
    monkeypatch.setattr(benchmark, "_READER_JOIN_TIMEOUT_SECONDS", 0.5)
    descendant_pid: int | None = None
    try:
        result = run(
            _input(tmp_path), tmp_path / "runs", "inherited-writer",
            command=[sys.executable, "-c", code],
        )
        descendant_pid = int(pid_path.read_text())
        assert result["process_return_code"] == 0
        assert result["outcome"]["descendant_pid"] == descendant_pid
        assert result["wall_seconds"] >= 0
        assert result["child_peak_rss_mb"] >= 0
        assert processes[0].returncode is not None
        assert processes[0].stdout is not None and processes[0].stdout.closed
        assert processes[0].stderr is not None and processes[0].stderr.closed
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not _process_is_gone(descendant_pid):
            time.sleep(0.02)
        assert _process_is_gone(descendant_pid)
    finally:
        if descendant_pid is None and pid_path.exists():
            descendant_pid = int(pid_path.read_text())
        if descendant_pid is not None and not _process_is_gone(descendant_pid):
            try:
                os.kill(descendant_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
