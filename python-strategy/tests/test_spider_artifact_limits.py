"""Golden policy and v1 classification matrix for Spider artifact limits."""

import ast
from pathlib import Path

import pytest

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


# Captured with the v2 D5.2 closed slots and D5.3 canonical bytes. H01-H10
# came from the real research executor; F-IN/H60/H360 from stored replay rows.
P3_V2_FIXTURES = (
    ("F-IN", (287, 96, 24), (459303, 1177725, 616274, 346894), 80, 30894),
    ("H01", (23, 8, 2), (38460, 95284, 65035, 279066), 17, 6385),
    ("H02", (4, 2, 0), (10786, 15720, 9063, 0), 9, 2252),
    ("H03", (20, 7, 1), (30976, 78652, 39553, 0), 0, 0),
    ("H04", (20, 7, 1), (33379, 81822, 42231, 0), 7, 1828),
    ("H05", (4, 2, 0), (9395, 15004, 7869, 0), 2, 503),
    ("H06-A", (1, 1, 0), (4431, 5480, 5612, 0), 0, 0),
    ("H06-B", (1, 1, 0), (4431, 5363, 4292, 0), 0, 0),
    ("H08", (11, 4, 1), (18302, 46125, 23968, 0), 2, 491),
    ("H09", (11, 4, 1), (18823, 45525, 24109, 0), 4, 962),
    ("H10", (0, 1, 0), (5284, 0, 0, 0), 2, 530),
    ("H60", (719, 240, 60), (1194812, 3004779, 1985534, 9492559), 482, 184479),
    ("H360", (4319, 1440, 360), (6891086, 18364493, 9853802, 15081552), 724, 276840),
)


def test_p3_v2_fixture_calibration_admits_all_executable_scopes():
    kinds = (
        "HISTORICAL_MARKET_RESULT",
        "SNAPSHOT_FACT",
        "CALLBACK_RESULT",
        "SOURCE_GROUP_RESULT",
    )
    for name, (timers, market, cache), actual, definitions, def_bytes in P3_V2_FIXTURES:
        budget = limits.p3_v2_scope_budget(
            dict(zip(limits.P3_V2_COVERAGE_KINDS, (market, cache, timers), strict=True))
        )
        total = timers + market + cache
        expected_rows = {
            "HISTORICAL_MARKET_RESULT": (11 * market + 9) // 10,
            "SNAPSHOT_FACT": (12 * total + 4) // 5,
            "CALLBACK_RESULT": (27 * total + 9) // 10,
        }
        expected_budgets = {
            "HISTORICAL_MARKET_RESULT": expected_rows["HISTORICAL_MARKET_RESULT"]
            * 5933,
            "SNAPSHOT_FACT": expected_rows["SNAPSHOT_FACT"] * 1572,
            "CALLBACK_RESULT": expected_rows["CALLBACK_RESULT"] * 1544,
        }
        assert budget["row_counts"] == expected_rows, name
        assert budget["kind_budgets"] == expected_budgets, name
        actual_by_kind = dict(zip(kinds, actual, strict=True))
        assert (
            not budget["refused"]
            and budget["U_wire"] == budget["J2"] == limits.P3_V2_JOURNAL_CAP
        ), name
        assert budget["L_wire"] <= limits.P3_V2_JOURNAL_CAP, name
        assert all(actual_by_kind[k] <= budget["kind_budgets"][k] for k in kinds[:3]), (
            name
        )
        assert actual_by_kind["SOURCE_GROUP_RESULT"] <= budget["B_SG"], name
        assert (
            definitions <= budget["table_max"]
            and def_bytes <= budget["table_bytes_max"]
        ), name
        assert def_bytes <= definitions * limits.P3_V2_DEFCAP, name


def test_p3_v2_bounds_are_inclusive_and_refuse_only_above_limit():
    assert (
        limits.p3_v2_scope_refused(limits.P3_V2_JOURNAL_CAP, limits.P3_V2_JOURNAL_CAP)
        is False
    )
    assert limits.p3_v2_scope_refused(limits.P3_V2_JOURNAL_CAP + 1, 0) is True
    assert limits.p3_v2_scope_refused(0, limits.P3_V2_JOURNAL_CAP + 1) is True
    one = limits.p3_v2_scope_budget({"HISTORICAL_MARKET_STEP": 1})
    assert one["row_counts"] == {
        "HISTORICAL_MARKET_RESULT": 2,
        "SNAPSHOT_FACT": 3,
        "CALLBACK_RESULT": 3,
    }


def test_p3_v2_h360_derived_budget_matches_fixed_arithmetic():
    budget = limits.p3_v2_scope_budget(
        {
            "HISTORICAL_TIMER": 4319,
            "HISTORICAL_MARKET_STEP": 1440,
            "HISTORICAL_MARKET_CACHE": 360,
        }
    )
    assert budget["row_counts"] == {
        "HISTORICAL_MARKET_RESULT": 1584,
        "SNAPSHOT_FACT": 14686,
        "CALLBACK_RESULT": 16522,
    }
    assert (budget["table_max"], budget["table_bytes_max"]) == (7200, 3_060_000)
    assert (budget["structural_bytes"], budget["B_SG"], budget["L_wire"]) == (
        61_054_232,
        28_424_253,
        2_291_040,
    )
    assert (budget["U_wire"], budget["J2"], budget["X2"], budget["E2"]) == (
        89_478_485,
        89_478_485,
        19_048_383,
        limits.DEFAULT_LIMIT,
    )


def test_p3_v2_coverage_rejects_empty_malformed_and_unknown_inputs():
    for value in (
        {},
        {"HISTORICAL_TIMER": 0},
        {"UNKNOWN": 1},
        {"HISTORICAL_TIMER": True},
        {"HISTORICAL_TIMER": -1},
        {"HISTORICAL_TIMER": 1.0},
    ):
        try:
            limits.p3_v2_scope_budget(value)
        except ValueError as exc:
            assert exc.args == ("INVALID_ARTIFACT",)
        else:
            raise AssertionError(value)
    for structural, minimum in ((True, 0), (-1, 0), (0, -1), (0.0, 0)):
        try:
            limits.p3_v2_scope_refused(structural, minimum)
        except ValueError as exc:
            assert exc.args == ("INVALID_ARTIFACT",)
        else:
            raise AssertionError((structural, minimum))


def test_registration_preflight_d2_5_exact_limits_and_refusal_twins():
    from dataclasses import replace
    from src.core.backtest.spider_historical_input import encode_historical_run_input
    from src.core.backtest import spider_scenario_run
    from test_spider_historical_input import _valid_run

    run = replace(_valid_run(), run_id="preflight-test",
                  policy_source_sha256=spider_scenario_run._P3_POLICY_SOURCE)
    raw = encode_historical_run_input(run)
    _, bundle, hashes = spider_scenario_run._historical_configuration(raw, run.run_id)
    attempt = spider_scenario_run._attempt(
        run.run_id, spider_scenario_run._P3_SELECTOR, bundle, hashes,
        spider_scenario_run._P3_RUN_CONTRACT,
    )
    attempt["artifact_encoding"] = "artifact_encoding_v2"
    original = limits.P3_V2_HISTORICAL_INPUT_LIMIT
    try:
        limits.P3_V2_HISTORICAL_INPUT_LIMIT = len(raw)
        limits.p3_v2_registration_preflight(attempt, raw)
        limits.P3_V2_HISTORICAL_INPUT_LIMIT = len(raw) - 1
        with pytest.raises(ValueError) as caught:
            limits.p3_v2_registration_preflight(attempt, raw)
        assert caught.value.args == ("ARTIFACT_LIMIT_EXCEEDED",)
    finally:
        limits.P3_V2_HISTORICAL_INPUT_LIMIT = original


def test_registration_preflight_refuses_structural_scope_and_accepts_legal_twin():
    from dataclasses import replace
    from src.core.backtest.spider_historical_input import encode_historical_run_input, historical_planned_coverage
    from test_spider_historical_input import _valid_run

    run = replace(_valid_run(), run_id="preflight-test")
    raw = encode_historical_run_input(run)
    attempt = {"run_contract_id": "SPIDER_HISTORICAL_RESEARCH_RUN_V1", "artifact_encoding": "artifact_encoding_v2",
               "planned_coverage": historical_planned_coverage(run)}
    limits.p3_v2_registration_preflight(attempt, raw)
    assert attempt["planned_coverage"]
    limits.p3_v2_registration_preflight(attempt, raw)


def test_registration_preflight_rejects_when_budget_classifier_marks_scope_refused(monkeypatch):
    from dataclasses import replace
    from src.core.backtest.spider_historical_input import encode_historical_run_input, historical_planned_coverage
    from test_spider_historical_input import _valid_run

    run = replace(_valid_run(), run_id="preflight-test")
    attempt = {
        "run_contract_id": "SPIDER_HISTORICAL_RESEARCH_RUN_V1",
        "artifact_encoding": "artifact_encoding_v2",
        "planned_coverage": historical_planned_coverage(run),
    }
    monkeypatch.setattr(limits, "p3_v2_scope_budget", lambda _: {"refused": True})
    try:
        limits.p3_v2_registration_preflight(attempt, encode_historical_run_input(run))
    except ValueError as exc:
        assert exc.args == ("ARTIFACT_LIMIT_EXCEEDED",)
    else:
        raise AssertionError("scope refusal was not applied")


@pytest.mark.parametrize(
    ("field", "cap"),
    (("X2", limits.P3_V2_OTHER_ARTIFACT_LIMITS["X2"]),
     ("E2", limits.P3_V2_OTHER_ARTIFACT_LIMITS["E2"])),
)
def test_registration_preflight_checks_each_other_artifact_cap_inclusively(
    monkeypatch, field, cap,
):
    from dataclasses import replace
    from src.core.backtest.spider_historical_input import encode_historical_run_input, historical_planned_coverage
    from test_spider_historical_input import _valid_run

    run = replace(_valid_run(), run_id="preflight-artifact-cap")
    attempt = {
        "run_contract_id": "SPIDER_HISTORICAL_RESEARCH_RUN_V1",
        "artifact_encoding": "artifact_encoding_v2",
        "planned_coverage": historical_planned_coverage(run),
    }
    raw = encode_historical_run_input(run)
    budget = {"refused": False, "X2": 0, "E2": 0}
    budget[field] = cap
    monkeypatch.setattr(limits, "p3_v2_scope_budget", lambda _: budget)
    limits.p3_v2_registration_preflight(attempt, raw)

    budget[field] = cap + 1
    with pytest.raises(ValueError) as caught:
        limits.p3_v2_registration_preflight(attempt, raw)
    assert caught.value.args == ("ARTIFACT_LIMIT_EXCEEDED",)


def test_reviewer_h360_x2_input_is_accepted():
    from collections import Counter
    from dataclasses import replace
    from pathlib import Path
    from src.core.backtest import spider_scenario_run
    from src.core.backtest.spider_historical_input import (
        decode_historical_run_input, encode_historical_run_input, historical_planned_coverage,
    )
    from src.core.backtest.spider_run_artifacts import canonical_bytes

    source = Path("/Volumes/Dev/Scratch/agents/spider_throughput/S4/x/s3-3-after/h360_durable/runs/h360/historical_input.json")
    run = decode_historical_run_input(source.read_bytes())
    run = replace(run, policy_source_sha256=spider_scenario_run._P3_POLICY_SOURCE)
    raw = encode_historical_run_input(run)
    _, bundle, hashes = spider_scenario_run._historical_configuration(raw, run.run_id)
    attempt = spider_scenario_run._attempt(
        run.run_id, spider_scenario_run._P3_SELECTOR, bundle, hashes,
        spider_scenario_run._P3_RUN_CONTRACT,
    )
    attempt["artifact_encoding"] = "artifact_encoding_v2"
    counts = Counter(row["record_kind"] for row in historical_planned_coverage(run))
    budget = limits.p3_v2_scope_budget(counts)
    assert budget["X2"] == 19_048_383
    assert budget["structural_bytes"] == 61_054_232 and not budget["refused"]
    assert len(raw) == 1_858_827 and len(canonical_bytes(attempt)) + 65_536 == 622_244
    limits.p3_v2_registration_preflight(attempt, raw)


def test_p3_v2_uses_python_integers_and_scope_budgets_are_monotone():
    huge = 10**100
    result = limits.p3_v2_scope_budget(
        {"HISTORICAL_TIMER": huge, "HISTORICAL_MARKET_STEP": huge}
    )
    assert result["refused"] is True
    small = limits.p3_v2_scope_budget(
        {
            "HISTORICAL_TIMER": 719,
            "HISTORICAL_MARKET_STEP": 240,
            "HISTORICAL_MARKET_CACHE": 60,
        }
    )
    large = limits.p3_v2_scope_budget(
        {
            "HISTORICAL_TIMER": 720,
            "HISTORICAL_MARKET_STEP": 240,
            "HISTORICAL_MARKET_CACHE": 60,
        }
    )
    assert large["structural_bytes"] >= small["structural_bytes"]
    assert large["X2"] >= small["X2"] and large["E2"] >= small["E2"]
    assert large["B_SG"] <= small["B_SG"]
    scaled = limits.p3_v2_scope_budget({"HISTORICAL_TIMER": 10_000})
    assert (scaled["X2"], scaled["E2"]) == (31_129_896, 17_633_869)
