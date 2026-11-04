"""Measure one Spider historical CLI run and inventory every persisted artifact.

Example::

    uv run --no-sync python -m tools.spider_run_benchmark \
        --input /path/input.json --output-root /path/runs --run-id fin \
        --output /path/metrics.json

The --profile mode is separate and its wall time is not comparable with a normal run.
The run function accepts command for fast subprocess boundary tests; production calls
use the historical replay CLI by default.
"""

from __future__ import annotations

import argparse
import json
import os
from hashlib import sha256
from pathlib import Path
import pstats
import signal
import subprocess
import sys
import time
from threading import Thread
from typing import Any, IO, cast, Sequence

from src.core.backtest.spider_run_admission import _LIMIT, _P3_JOURNAL_LIMIT, admit_spider_run

_ARTIFACT_NAMES = {
    "attempt.json", "status.json", "journal.jsonl", "endpoint.json", "reconciliation.json",
    "report.jsonl", "historical_input.json", "completion.json",
}
_PROFILE_FUNCTIONS = {
    "_execute_historical", "capture", "persist", "capture_owner_evidence", "_public_scheduler_observation",
    "_normalized", "apply_group", "historical_market_step", "inspect_state", "capture_snapshot",
}
_READER_JOIN_TIMEOUT_SECONDS = 30


def _cleanup_process(
    process: subprocess.Popen[str],
    started_readers: list[tuple[Thread, IO[str]]],
    pipe_wrappers: list[IO[str] | None],
) -> tuple[list[Exception], bool]:
    errors: list[Exception] = []

    try:
        if process.poll() is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
    except Exception as error:
        errors.append(error)
    finally:
        try:
            process.wait()
        except Exception as error:
            errors.append(error)

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except Exception as error:
        errors.append(error)

    live_streams: set[int] = set()
    for reader, stream in started_readers:
        try:
            reader.join(timeout=_READER_JOIN_TIMEOUT_SECONDS)
        except Exception as error:
            errors.append(error)
        try:
            if reader.is_alive():
                live_streams.add(id(stream))
        except Exception as error:
            errors.append(error)

    pipe_drain_timeout = bool(live_streams)
    if pipe_drain_timeout:
        errors.append(RuntimeError("BENCHMARK_PIPE_DRAIN_TIMEOUT"))

    for stream in pipe_wrappers:
        if stream is not None and id(stream) not in live_streams:
            try:
                stream.close()
            except Exception as error:
                errors.append(error)

    return errors, pipe_drain_timeout


def _limits() -> dict[str, int]:
    return {name: _P3_JOURNAL_LIMIT if name == "journal.jsonl" else _LIMIT for name in _ARTIFACT_NAMES}


def _profile_summary(path: Path) -> list[dict[str, Any]]:
    stats = pstats.Stats(str(path))
    items = []
    for (filename, line, function), (primitive, calls, total, cumulative, _callers) in cast(Any, stats).stats.items():
        if function not in _PROFILE_FUNCTIONS:
            continue
        items.append({
            "function": function,
            "filename": filename,
            "line": line,
            "primitive_calls": primitive,
            "calls": calls,
            "total_seconds": total,
            "cumulative_seconds": cumulative,
            "cumulative_seconds_per_call": cumulative / calls if calls else None,
        })
    return sorted(items, key=lambda item: item["cumulative_seconds_per_call"] or 0, reverse=True)


def run(
    input_path: Path,
    output_root: Path,
    run_id: str,
    *,
    profile: bool = False,
    command: Sequence[str] | None = None,
) -> dict[str, Any]:
    input_raw = input_path.read_bytes()
    output_root.mkdir(parents=True, exist_ok=True)
    profile_path = output_root / f"{run_id}.prof"
    child_command = list(command) if command is not None else [
        sys.executable, "-m", "examples.run_spider_scenario_replay",
        "--output-root", str(output_root), "--run-id", run_id,
        "--scenario-selector", "SPIDER_HISTORICAL_RESEARCH_RUN_V1",
        "--historical-input", str(input_path),
    ]
    if profile:
        child_command = [sys.executable, "-m", "cProfile", "-o", str(profile_path), *child_command[1:]]
    start = time.perf_counter()
    captured: dict[str, list[str]] = {"stdout": [], "stderr": []}
    started_readers: list[tuple[Thread, IO[str]]] = []
    pipe_wrappers: list[IO[str] | None] = [None, None]
    primary_error: BaseException | None = None
    process = subprocess.Popen(
        child_command, cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace",
        start_new_session=True,
    )
    try:
        pipe_wrappers[0] = process.stdout
        pipe_wrappers[1] = process.stderr
        if process.stdout is None or process.stderr is None:
            raise RuntimeError("SUBPROCESS_PIPE_UNAVAILABLE")

        def drain(stream: IO[str], destination: list[str]) -> None:
            destination.append(stream.read())

        readers = (
            (Thread(target=drain, args=(process.stdout, captured["stdout"]), daemon=True), process.stdout),
            (Thread(target=drain, args=(process.stderr, captured["stderr"]), daemon=True), process.stderr),
        )
        for reader, stream in readers:
            reader.start()
            started_readers.append((reader, stream))
        _pid, wait_status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(wait_status)
    except BaseException as error:
        primary_error = error
        raise
    finally:
        cleanup_errors, pipe_drain_timeout = _cleanup_process(process, started_readers, pipe_wrappers)
        if primary_error is not None:
            for cleanup_error in cleanup_errors:
                primary_error.add_note(f"benchmark cleanup error: {cleanup_error!r}")
        elif pipe_drain_timeout:
            raise RuntimeError("BENCHMARK_PIPE_DRAIN_TIMEOUT")
        elif cleanup_errors:
            raise cleanup_errors[0]
    wall = time.perf_counter() - start
    stdout = "".join(captured["stdout"])
    stderr = "".join(captured["stderr"])
    try:
        outcome = json.loads(stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        outcome = {"outcome": "PROCESS_ERROR", "reason": stderr.strip() or stdout.strip() or "INVALID_JSON_STDOUT"}
    run_dir = output_root / run_id
    artifact_records: dict[str, Any] = {}
    if run_dir.is_dir():
        for path in sorted(item for item in run_dir.rglob("*") if item.is_file()):
            relative = path.relative_to(run_dir).as_posix()
            raw = path.read_bytes()
            limit = _limits().get(path.name)
            artifact_records[relative] = {
                "bytes": len(raw),
                "limit_bytes": limit,
                "headroom_bytes": limit - len(raw) if limit is not None else None,
                "headroom_fraction": (limit - len(raw)) / limit if limit is not None else None,
                "sha256": sha256(raw).hexdigest(),
            }
    journal_path = run_dir / "journal.jsonl"
    journal_rows = sum(1 for _ in journal_path.open("rb")) if journal_path.exists() else 0
    if run_dir.is_dir():
        try:
            admission_raw = admit_spider_run(run_dir)
            admission = {"decision": admission_raw["decision"], "reason": admission_raw.get("reason")}
        except (OSError, ValueError) as error:
            admission = {"decision": "ERROR", "reason": str(error)}
    else:
        admission = {"decision": "UNAVAILABLE", "reason": "RUN_DIRECTORY_MISSING"}
    # The CLI result reports outcome/reason; admission gives the persisted classification independently.
    input_minutes = None
    try:
        from src.core.backtest.spider_historical_input import decode_historical_run_input
        historical = decode_historical_run_input(input_raw)
        input_minutes = (historical.range_end_ms - historical.range_start_ms) / 60_000
    except Exception:
        pass
    peak_rss = usage.ru_maxrss / (1024 * 1024 if sys.platform == "darwin" else 1024)
    outcome_name = outcome.get("outcome")
    cli_codes = {"ADMITTED": 0, "REJECTED": 2, "FAILED": 3, "DURABILITY_UNKNOWN": 3}
    cli_return_code = cli_codes.get(outcome_name, process.returncode) if isinstance(outcome_name, str) else process.returncode
    result = {
        "run_id": run_id,
        "input_sha256": sha256(input_raw).hexdigest(),
        "horizon_minutes": input_minutes,
        "wall_seconds": wall,
        "seconds_per_sim_minute": wall / input_minutes if input_minutes else None,
        "child_peak_rss_mb": peak_rss,
        "return_code": cli_return_code,
        "process_return_code": process.returncode,
        "wrapper_return_code": process.returncode,
        "outcome": outcome,
        "admission": admission,
        "stderr": stderr.strip(),
        "journal_rows": journal_rows,
        "artifacts": artifact_records,
        "profiled": profile,
        "profile_path": str(profile_path) if profile else None,
        "profile_functions": _profile_summary(profile_path) if profile and profile_path.exists() else [],
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--profile", action="store_true", help="run the CLI under cProfile (timing is not comparable)")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.input, args.output_root, args.run_id, profile=args.profile)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
