"""Golden policy and v1 classification matrix for Spider artifact limits."""

import ast
from pathlib import Path

from src.core.backtest import spider_artifact_limits as limits


def test_policy_table_matches_design_t13():
    assert (
        limits.DEFAULT_LIMIT,
        limits.LEG32,
        limits.PAID_JOURNAL_LIMIT,
        limits.GROSS_REPORT_LIMIT,
    ) == (
        16_777_216,
        33_554_432,
        33_554_432,
        33_554_432,
    )
    assert limits.POLICY_TABLE == {
        ("P3", "v1", "journal.jsonl"): limits.LEG32,
        ("P3", "v1", "other"): limits.DEFAULT_LIMIT,
        ("P3", "v2", "journal.jsonl"): None,
        ("P3", "v2", "reconciliation.json"): None,
        ("P3", "v2", "endpoint.json"): None,
        ("P3", "v2", "other"): limits.DEFAULT_LIMIT,
        ("paid_execution", "v1", "journal.jsonl"): limits.LEG32,
        ("paid_floating", "v1", "journal.jsonl"): limits.LEG32,
        ("paid_gross", "v1", "journal.jsonl"): limits.LEG32,
        ("paid_gross", "v1", "report.jsonl"): limits.LEG32,
        ("paid_execution", "v1", "other"): limits.DEFAULT_LIMIT,
        ("paid_floating", "v1", "other"): limits.DEFAULT_LIMIT,
        ("paid_gross", "v1", "other"): limits.DEFAULT_LIMIT,
        ("P1", "v1", "other"): limits.DEFAULT_LIMIT,
        ("P2", "v1", "other"): limits.DEFAULT_LIMIT,
    }


def test_limit_literals_live_only_in_policy_module():
    source_root = Path(__file__).parents[1] / "src" / "core" / "backtest"
    forbidden = {limits.DEFAULT_LIMIT, limits.LEG32}
    for path in source_root.glob("*.py"):
        if path.name == "spider_artifact_limits.py":
            continue
        tree = ast.parse(path.read_text())
        assert not [
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and type(node.value) is int
            and node.value in forbidden
        ], path.name


def test_p3_v2_policy_constants_are_golden():
    assert limits.P3_V2_COVERAGE_KINDS == (
        "HISTORICAL_MARKET_STEP",
        "HISTORICAL_MARKET_CACHE",
        "HISTORICAL_TIMER",
    )
    assert limits.P3_V2_RECORD_KINDS == {
        "HISTORICAL_MARKET_RESULT",
        "SNAPSHOT_FACT",
        "CALLBACK_RESULT",
        "SOURCE_GROUP_RESULT",
    }
    assert limits.P3_V2_COVERAGE_BASIS == {
        "HISTORICAL_MARKET_RESULT": ("HISTORICAL_MARKET_STEP",),
        "SNAPSHOT_FACT": limits.P3_V2_COVERAGE_KINDS,
        "CALLBACK_RESULT": limits.P3_V2_COVERAGE_KINDS,
        "SOURCE_GROUP_RESULT": None,
    }
    assert limits.P3_V2_MULTIPLIERS == {
        "HISTORICAL_MARKET_RESULT": (11, 10),
        "SNAPSHOT_FACT": (12, 5),
        "CALLBACK_RESULT": (27, 10),
    }
    assert limits.P3_V2_ROW_CAPS == {
        "HISTORICAL_MARKET_RESULT": 5933,
        "SNAPSHOT_FACT": 1572,
        "CALLBACK_RESULT": 1544,
    }
    assert (
        limits.P3_V2_DEFS_PER_MARKET,
        limits.P3_V2_DEFCAP,
        limits.P3_V2_ROW_MAX,
        limits.P3_V2_ROW_MIN,
    ) == (5, 425, 1_048_576, 1591)
    assert (limits.P3_V2_HEADER_BYTES, limits.P3_V2_MAX_DEFINITION_BYTES) == (0, 4096)
    assert (
        limits.P3_V2_WIRE_CEILING,
        limits.P3_V2_MATERIAL_CEILING,
        limits.P3_V2_EXP_MAX,
        limits.P3_V2_JOURNAL_CAP,
    ) == (536_870_912, 536_870_912, 6, 89_478_485)
    assert (
        limits.P3_V2_RECONCILIATION_FLOOR,
        limits.P3_V2_X2_COEFFICIENT,
        limits.P3_V2_E2_COEFFICIENT,
    ) == (limits.DEFAULT_LIMIT, (19_048_383, 6119), (10_790_164, 6119))


def test_p3_v2_closed_record_kinds_match_common_schema():
    from src.core.backtest import spider_run_envelope_schema as schema

    schema_kinds = set(schema._KINDS) | {schema._P3_KIND}
    assert schema_kinds - {"DELIVERY_ATTEMPT"} == limits.P3_V2_RECORD_KINDS
