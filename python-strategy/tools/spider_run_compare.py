"""Compare P3 runs while masking only documented provenance-derived values.

Expected differences and source locations:

| JSON path | Cause | Source |
| --- | --- | --- |
| attempt.json#/program_sha256 | source manifest | spider_scenario_run.py:64-65,203-208 |
| attempt.json#/native_artifact_sha256 | loaded native binary | spider_scenario_run.py:207-208 |
| attempt.json#/input_contract_hashes/1/sha256 | PROGRAM copy | spider_run_artifacts.py:17; spider_scenario_run.py:319-330 |
| attempt.json#/input_contract_hashes/2/sha256 | NATIVE_ARTIFACT copy | spider_run_artifacts.py:17; spider_scenario_run.py:319-330 |
| completion.json#/input_contract_hashes/1/sha256 | PROGRAM copy | spider_run_store.py:681-684; spider_run_completion_schema.py:145-150 |
| completion.json#/input_contract_hashes/2/sha256 | NATIVE_ARTIFACT copy | spider_run_store.py:681-684; spider_run_completion_schema.py:145-150 |
| completion.json#/artifacts/0/sha256 | attempt.json digest | spider_run_store.py:586-611,677-684; spider_run_completion_schema.py:15-20,158-160 |

Every other artifact field remains significant. Journal rows retain their original order.
Use python -m tools.spider_run_compare RUN_A RUN_B.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.core.backtest.spider_run_completion_schema import _P3_ARTIFACTS

# These paths correspond to the PROGRAM and NATIVE_ARTIFACT entries of input_contract_hashes.
_REQUIRED_ARTIFACTS = {name for name, _schema in _P3_ARTIFACTS} | {"completion.json"}
_EXPECTED_DIFFERENCES = {
    "attempt.json": {
        ("program_sha256",),
        ("native_artifact_sha256",),
        ("input_contract_hashes", "1", "sha256"),
        ("input_contract_hashes", "2", "sha256"),
    },
    "completion.json": {
        ("input_contract_hashes", "1", "sha256"),
        ("input_contract_hashes", "2", "sha256"),
        ("artifacts", "0", "sha256"),
    },
}


def _load_artifact(path: Path) -> Any:
    raw = path.read_bytes()
    if path.name.endswith(".jsonl"):
        return [json.loads(line) for line in raw.splitlines()]
    return json.loads(raw)


def _canonical(value: Any, path: str, differences: set[tuple[str, ...]], parts: tuple[str, ...] = ()) -> Any:
    if parts in differences:
        return "<EXPECTED_PROVENANCE_DIFFERENCE>"
    if isinstance(value, dict):
        return {key: _canonical(item, path, differences, parts + (key,)) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_canonical(item, path, differences, parts + (str(index),)) for index, item in enumerate(value)]
    return value


def _has_path(value: Any, parts: tuple[str, ...]) -> bool:
    current = value
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdecimal() and int(part) < len(current):
            current = current[int(part)]
        else:
            return False
    return True


def _first_difference(left: Any, right: Any, path: str = "$") -> str | None:
    if type(left) is not type(right):
        return f"{path}: type {type(left).__name__} != {type(right).__name__}"
    if isinstance(left, dict):
        left_keys, right_keys = set(left), set(right)
        if left_keys != right_keys:
            missing = sorted(left_keys - right_keys)
            extra = sorted(right_keys - left_keys)
            return f"{path}: keys differ; missing from right={missing}, missing from left={extra}"
        for key in sorted(left):
            difference = _first_difference(left[key], right[key], f"{path}/{key}")
            if difference:
                return difference
    elif isinstance(left, list):
        if len(left) != len(right):
            return f"{path}: list length {len(left)} != {len(right)}"
        for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
            difference = _first_difference(left_item, right_item, f"{path}/{index}")
            if difference:
                return difference
    elif left != right:
        return f"{path}: {left!r} != {right!r}"
    return None


def compare(left_dir: Path, right_dir: Path) -> dict[str, Any]:
    left_files = {path.relative_to(left_dir).as_posix() for path in left_dir.rglob("*") if path.is_file()}
    right_files = {path.relative_to(right_dir).as_posix() for path in right_dir.rglob("*") if path.is_file()}
    if left_files != right_files:
        return {"equal": False, "difference": f"artifact set differs: left-only={sorted(left_files-right_files)}, right-only={sorted(right_files-left_files)}"}
    if left_files != _REQUIRED_ARTIFACTS:
        return {"equal": False, "difference": f"expected P3 artifact set {sorted(_REQUIRED_ARTIFACTS)}, found {sorted(left_files)}"}
    for name in sorted(left_files):
        left_path = left_dir / name
        right_path = right_dir / name
        if not name.endswith((".json", ".jsonl")):
            return {"equal": False, "difference": f"unsupported artifact file: {name}"}
        left = _load_artifact(left_path)
        right = _load_artifact(right_path)
        allowlist = _EXPECTED_DIFFERENCES.get(name, set())
        for parts in allowlist:
            if not _has_path(left, parts) or not _has_path(right, parts):
                pointer = "/".join(parts)
                return {"equal": False, "difference": f"{name}$/{pointer}: allowlisted provenance field missing"}
        canonical_left = _canonical(left, name, allowlist)
        canonical_right = _canonical(right, name, allowlist)
        difference = _first_difference(canonical_left, canonical_right, f"{name}$")
        if difference:
            return {"equal": False, "difference": difference}
    return {"equal": True, "difference": None, "artifacts_compared": sorted(left_files)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    args = parser.parse_args()
    result = compare(args.left, args.right)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
