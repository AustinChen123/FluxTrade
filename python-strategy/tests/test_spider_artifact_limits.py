"""Golden policy and v1 classification matrix for Spider artifact limits."""

import ast
from pathlib import Path

from src.core.backtest.spider_artifact_limits import (
    DEFAULT_LIMIT,
    GROSS_REPORT_LIMIT,
    LEG32,
    PAID_JOURNAL_LIMIT,
    POLICY_TABLE,
)


def test_policy_table_matches_design_t13():
    assert (DEFAULT_LIMIT, LEG32, PAID_JOURNAL_LIMIT, GROSS_REPORT_LIMIT) == (
        16_777_216, 33_554_432, 33_554_432, 33_554_432,
    )
    assert POLICY_TABLE == {
        ("P3", "v1", "journal.jsonl"): LEG32,
        ("P3", "v1", "other"): DEFAULT_LIMIT,
        ("P3", "v2", "journal.jsonl"): None,
        ("P3", "v2", "reconciliation.json"): None,
        ("P3", "v2", "endpoint.json"): None,
        ("P3", "v2", "other"): DEFAULT_LIMIT,
        ("paid_execution", "v1", "journal.jsonl"): LEG32,
        ("paid_floating", "v1", "journal.jsonl"): LEG32,
        ("paid_gross", "v1", "journal.jsonl"): LEG32,
        ("paid_gross", "v1", "report.jsonl"): LEG32,
        ("paid_execution", "v1", "other"): DEFAULT_LIMIT,
        ("paid_floating", "v1", "other"): DEFAULT_LIMIT,
        ("paid_gross", "v1", "other"): DEFAULT_LIMIT,
        ("P1", "v1", "other"): DEFAULT_LIMIT,
        ("P2", "v1", "other"): DEFAULT_LIMIT,
    }


def test_limit_literals_live_only_in_policy_module():
    source_root = Path(__file__).parents[1] / "src" / "core" / "backtest"
    forbidden = {DEFAULT_LIMIT, LEG32}
    for path in source_root.glob("*.py"):
        if path.name == "spider_artifact_limits.py":
            continue
        tree = ast.parse(path.read_text())
        assert not [
            node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and type(node.value) is int and node.value in forbidden
        ], path.name
