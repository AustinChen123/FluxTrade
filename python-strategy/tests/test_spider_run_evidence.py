"""Pure reconciliation oracle, representable failures and projection errors."""

import ast
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_artifacts as artifacts
from src.core.backtest import spider_policy_protocol as sequence_protocol
from src.core.backtest import spider_run_completion_schema as completion
from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_run_evidence as evidence
from src.core.backtest.spider_run_reconciliation_schema import reconciliation
from src.core.backtest.spider_scenario_plans import PLAN_IDS, plan_bundle
from src.core.backtest.spider_historical_input import historical_planned_coverage
from test_spider_run_artifacts import attempt as attempt_fixture, historical_context
from test_spider_historical_oracle import _oracle_run
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition

RUN = "evidence-test-1"
NAMES = ["PLANNED_COVERAGE_COMPLETE", "PROCESSED_EQUALS_PERSISTED", "JOURNAL_CONTIGUOUS", "OWNER_IDENTITY_MATCH", "OWNER_DIGEST_MATCH",
         "ENDPOINT_SNAPSHOT_VERSION_MATCH", "NO_UNRESOLVED_QUEUE", "NO_UNRESOLVED_POLL", "NO_UNSUBMITTED_CALLBACK_ACTION", "TERMINAL_POLICY_MATCH", "REPORT_PROJECTION_MATCH"]
INITIAL = "artifact:endpoint.json#/initial_owner_evidence"
FINAL = "artifact:endpoint.json#/final_owner_evidence"


def fixture(index=0):
    bundle = cast(dict[str, Any], plan_bundle(PLAN_IDS[index]))
    plan = bundle["plan"]
    attempt = attempt_fixture()
    attempt.update(run_id=RUN, requested_scenario_selector=PLAN_IDS[index], scenario_plan_id=PLAN_IDS[index], profile_id=plan["native_profile"],
                   account_key=plan["account_key"], scenario_plan_sha256=bundle["plan_sha256"], planned_coverage=plan["planned_coverage"], terminal_policy=plan["terminal_policy"])
    for entry, key in zip(cast(list[dict], attempt["input_contract_hashes"]), ["scenario_plan_sha256", "program_sha256", "native_artifact_sha256", "policy_source_sha256"], strict=True):
        entry["sha256"] = attempt[key]
    last = plan["planned_coverage"][-1]
    boundary = dict(ordinal=last["ordinal"], journal_seq=last["ordinal"], barrier_id=last["barrier_id"])
    status = dict(schema_version="spider_status_v1", run_id=RUN, state="COMPLETE", processed_boundary=boundary, persisted_boundary=deepcopy(boundary), failure_reason=None, primary_failure=None)
    for row in [*bundle["journal"], bundle["endpoint"], *bundle["report"]]:
        row["run_id"] = RUN
    return [attempt, status, bundle["journal"], bundle["endpoint"], bundle["report"]]


@pytest.fixture(scope="module")
def configured_artifacts(tmp_path_factory):
    from src.core.backtest.spider_scenario_run import _invoke

    root = tmp_path_factory.mktemp("configured-evidence")
    result = _invoke(str(root), "configured", "SPIDER_P2_CONFIGURED_SCALE_V1", plan_bundle)
    assert result == dict(run_id="configured", outcome="ADMITTED", reason=None)
    directory = root / "configured"
    return [
        artifacts.decode_jsonl((directory / name).read_bytes())[0]
        if name.endswith(".json")
        else artifacts.decode_jsonl((directory / name).read_bytes())
        for name in ("attempt.json", "status.json", "journal.jsonl", "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json")
    ]


def validate_inputs(values):
    artifacts.validate_artifact(values[0])
    artifacts.validate_artifact(values[1])
    envelope.journal(values[2])
    envelope.endpoint(values[3])
    completion.report(values[4])


def h03_endpoint_observation(offset=100_001, *, legacy=False):
    _, run = _oracle_run("H03")
    composition = _ReplayComposition._from_historical_run(run)
    cutoff = (run.range_start_ms + offset) * 16 + 6
    old_sequence = pytest.MonkeyPatch() if legacy else None
    if old_sequence is not None:
        old_sequence.setattr(
            sequence_protocol, "historical_delivery_sequence",
            lambda _mode, _kind, counter: counter,
        )
    try:
        assert composition._dispatch_due(cutoff)["classification"] == "SUCCESS"
    finally:
        if old_sequence is not None:
            old_sequence.undo()
    requests = [
        dict(
            schema_version="snapshot_request_v1",
            account_key=composition._account,
            snapshot_id=f"H03_ENDPOINT_{kind}",
            snapshot_kind=kind,
            capture_mode="OWNER_CURRENT",
            captured_at=cutoff,
        )
        for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS")
    ]
    observation = composition.capture_owner_evidence(cutoff, *requests)["scheduler_observation"]
    return historical_planned_coverage(run), observation, cutoff


def test_h03_in_progress_poll_is_allowed_only_for_its_exact_future_continuation():
    coverage, observation, cutoff = h03_endpoint_observation()
    assert evidence._historical_polls_endpoint_compatible(observation, cutoff)
    assert evidence._historical_coverage_records_compatible(coverage, observation, cutoff)

    poll = next(row for row in observation["polls"] if row["status"] == "IN_PROGRESS")
    assert poll["awaiting"] == "TRADING"
    capture_id = f"{poll['poll_id']}_{poll['awaiting']}"
    capture = next(row for row in observation["records"] if row["stable_id"] == capture_id)
    raw_at = int(poll["poll_id"].removeprefix("P3_POLL_"))
    delivery_sequence = sequence_protocol.historical_delivery_sequence(
        sequence_protocol.HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
        sequence_protocol.HistoricalDeliveryKind.POLL,
        capture["key"]["schedule_sequence"],
    )
    has_earn = any(row["stable_id"] == f"{poll['poll_id']}_EARN" for row in observation["records"])
    delivery_offset = 10 if has_earn else 5
    continuation = next(
        row for row in observation["records"]
        if row["kind"] == "DELIVERY" and row["classification"] == "PENDING"
        and row["key"]["schedule_sequence"] == delivery_sequence
        and row["key"]["visible_at"] == (raw_at + delivery_offset) * 16 + 6
    )

    missing = deepcopy(observation)
    missing["pending_keys"] = [key for key in missing["pending_keys"]
                               if key != continuation["key"]]
    missing["records"] = [record for record in missing["records"] if record["key"] != continuation["key"]]
    assert not evidence._historical_polls_endpoint_compatible(missing, cutoff)

    mismatched = deepcopy(observation)
    next(row for row in mismatched["polls"] if row["poll_id"] == poll["poll_id"])["continuation_id"] += "-other"
    assert not evidence._historical_polls_endpoint_compatible(mismatched, cutoff)

    bad_capture_sequence = deepcopy(observation)
    next(row for row in bad_capture_sequence["records"] if row["stable_id"] == capture_id)["key"]["schedule_sequence"] += 1
    assert not evidence._historical_polls_endpoint_compatible(bad_capture_sequence, cutoff)

    bad_capture_time = deepcopy(observation)
    next(row for row in bad_capture_time["records"] if row["stable_id"] == capture_id)["key"]["visible_at"] += 1
    assert not evidence._historical_polls_endpoint_compatible(bad_capture_time, cutoff)

    bad_capture_identity = deepcopy(observation)
    next(row for row in bad_capture_identity["records"] if row["stable_id"] == capture_id)["stable_id"] += "-other"
    assert not evidence._historical_polls_endpoint_compatible(bad_capture_identity, cutoff)

    bad_delivery_sequence = deepcopy(observation)
    for key in bad_delivery_sequence["pending_keys"]:
        if key["stable_id"] == continuation["stable_id"]:
            key["schedule_sequence"] += 1
    for row in bad_delivery_sequence["records"]:
        if row["stable_id"] == continuation["stable_id"]:
            row["key"]["schedule_sequence"] += 1
    assert not evidence._historical_polls_endpoint_compatible(bad_delivery_sequence, cutoff)

    bad_delivery_time = deepcopy(observation)
    for key in bad_delivery_time["pending_keys"]:
        if key["stable_id"] == continuation["stable_id"]:
            key["visible_at"] += 1
    for row in bad_delivery_time["records"]:
        if row["stable_id"] == continuation["stable_id"]:
            row["key"]["visible_at"] += 1
    assert not evidence._historical_polls_endpoint_compatible(bad_delivery_time, cutoff)

    bad_delivery_identity = deepcopy(observation)
    next(row for row in bad_delivery_identity["records"]
         if row["stable_id"] == continuation["stable_id"])["key"]["stable_id"] += "-other"
    assert not evidence._historical_polls_endpoint_compatible(bad_delivery_identity, cutoff)

    due = deepcopy(observation)
    due_key = next(key for key in due["pending_keys"] if key["stable_id"] == continuation["stable_id"])
    due_key["visible_at"] = cutoff
    next(record for record in due["records"] if record["stable_id"] == continuation["stable_id"])["key"]["visible_at"] = cutoff
    assert not evidence._historical_polls_endpoint_compatible(due, cutoff)

    market_coverage, market_observation, market_cutoff = h03_endpoint_observation(100_000)
    assert evidence._historical_coverage_records_compatible(
        market_coverage, market_observation, market_cutoff,
    )
    future_market = next(
        record for record in market_observation["records"]
        if record["kind"] == "HISTORICAL_MARKET_STEP" and record["classification"] == "PENDING"
    )
    missing_market = deepcopy(market_observation)
    missing_market["pending_keys"] = [key for key in missing_market["pending_keys"]
                                     if key != future_market["key"]]
    missing_market["records"] = [record for record in missing_market["records"]
                                 if record["key"] != future_market["key"]]
    assert not evidence._historical_coverage_records_compatible(
        market_coverage, missing_market, market_cutoff,
    )

    mismatched_market = deepcopy(market_observation)
    changed_key = next(key for key in mismatched_market["pending_keys"]
                       if key["stable_id"] == future_market["stable_id"])
    changed_key["schedule_sequence"] += 1
    assert not evidence._historical_coverage_records_compatible(
        market_coverage, mismatched_market, market_cutoff,
    )

    due_market = deepcopy(market_observation)
    due_key = next(key for key in due_market["pending_keys"]
                   if key["stable_id"] == future_market["stable_id"])
    due_key["visible_at"] = market_cutoff
    next(record for record in due_market["records"]
         if record["stable_id"] == future_market["stable_id"])["key"]["visible_at"] = market_cutoff
    assert not evidence._historical_coverage_records_compatible(
        market_coverage, due_market, market_cutoff,
    )


@pytest.mark.parametrize("offset", [100_001, 100_005, 100_006, 100_010, 100_011, 100_015])
def test_h03_poll_endpoint_stage_boundary_matrix(offset):
    _, observation, cutoff = h03_endpoint_observation(offset)
    assert evidence._historical_polls_endpoint_compatible(observation, cutoff)


def test_h03_earn_if_due_uses_frozen_earn_snapshot_stage():
    _, observation, cutoff = h03_endpoint_observation(5_001)
    poll = next(row for row in observation["polls"] if row["status"] == "IN_PROGRESS")
    assert poll["awaiting"] == "EARN_IF_DUE"
    assert any(
        row["stable_id"] == f"{poll['poll_id']}_EARN"
        for row in observation["records"]
    )
    assert evidence._historical_polls_endpoint_compatible(observation, cutoff)


def test_h03_legacy_shared_delivery_sequences_remain_readable():
    _, observation, cutoff = h03_endpoint_observation(100_001, legacy=True)
    assert evidence._historical_polls_endpoint_compatible(observation, cutoff)


def test_h03_mixed_poll_delivery_sequence_modes_are_rejected():
    _, observation, cutoff = h03_endpoint_observation(100_001, legacy=True)
    poll = next(row for row in observation["polls"] if row["status"] == "IN_PROGRESS")
    capture = next(row for row in observation["records"]
                   if row["stable_id"] == f"{poll['poll_id']}_{poll['awaiting']}")
    raw_at = int(poll["poll_id"].removeprefix("P3_POLL_"))
    has_earn = any(row["stable_id"] == f"{poll['poll_id']}_EARN" for row in observation["records"])
    offset = 10 if has_earn else 5
    expected_at = (raw_at + offset) * 16 + 6
    delivery = next(row for row in observation["records"]
                    if row["kind"] == "DELIVERY" and row["key"]["visible_at"] == expected_at)
    capture_sequence = capture["key"]["schedule_sequence"]
    delivery["key"]["schedule_sequence"] = sequence_protocol.historical_delivery_sequence(
        sequence_protocol.HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
        sequence_protocol.HistoricalDeliveryKind.POLL,
        capture_sequence,
    )
    for key in observation["pending_keys"]:
        if key["stable_id"] == delivery["stable_id"]:
            key["schedule_sequence"] = delivery["key"]["schedule_sequence"]
    assert not evidence._historical_polls_endpoint_compatible(observation, cutoff)


def _assert_poll_checker_differential(observation, cutoff, mode, paid_prefix=None,
                                      allow_terminal_delivery=False):
    args = (observation, cutoff, mode, paid_prefix, allow_terminal_delivery)
    try:
        expected = evidence._historical_polls_endpoint_mode_compatible_original(*args)
    except Exception as error:
        expected = (type(error), error.args)
    try:
        actual = evidence._historical_polls_endpoint_mode_compatible(*args)
    except Exception as error:
        actual = (type(error), error.args)
    assert actual == expected


def test_indexed_poll_checker_matches_original_for_delivery_multimap_collisions():
    _, observation, cutoff = h03_endpoint_observation(100_001)
    mode = evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1
    _assert_poll_checker_differential(observation, cutoff, mode)
    poll = next(row for row in observation["polls"] if row["status"] == "COMPLETED")
    raw_at = int(poll["poll_id"].partition("_POLL_")[2])
    timer = next(row for row in observation["records"]
                 if row["kind"] == "HISTORICAL_TIMER"
                 and row["stable_id"] == f"P3_TIMER_{raw_at}")
    has_earn = any(row["kind"] == "SNAPSHOT_CAPTURE"
                   and row["stable_id"] == f"{poll['poll_id']}_EARN"
                   for row in observation["records"])
    layout = (("EARN", 0, 1, 5), ("TRADING", 1, 6, 10),
              ("POSITIONS", 2, 11, 15), ("OPEN_ORDERS", 3, 16, 20)) if has_earn else (
                  ("TRADING", 0, 1, 5), ("POSITIONS", 1, 6, 10), ("OPEN_ORDERS", 2, 11, 15))
    target = None
    for name, stage_index, _, delivery_offset in layout:
        capture = next(row for row in observation["records"]
                       if row["kind"] == "SNAPSHOT_CAPTURE"
                       and row["stable_id"] == f"{poll['poll_id']}_{name}")
        sequence = timer["key"]["schedule_sequence"] * 4 + stage_index
        delivery_sequence = sequence_protocol.historical_delivery_sequence(
            mode, sequence_protocol.HistoricalDeliveryKind.POLL, sequence,
        )
        delivery_at = (raw_at + delivery_offset) * 16 + 6
        target = next((row for row in observation["records"]
                       if row["kind"] == "DELIVERY"
                       and row["key"]["schedule_sequence"] == delivery_sequence
                       and row["key"]["visible_at"] == delivery_at), None)
        if capture["classification"] == "SUCCESS" and target is not None:
            break
        target = None
    assert target is not None
    # Same schedule/time, distinct stable identity must retain cardinality=2.
    duplicate_identity = deepcopy(target)
    duplicate_identity["key"]["stable_id"] += "-other"
    observation["records"].append(duplicate_identity)
    _assert_poll_checker_differential(observation, cutoff, mode)
    observation["records"].pop()
    # Same stable identity at another schedule/time must not enter the bucket.
    duplicate_time = deepcopy(target)
    duplicate_time["key"]["schedule_sequence"] += 100_000
    duplicate_time["key"]["visible_at"] += 1
    observation["records"].append(duplicate_time)
    _assert_poll_checker_differential(observation, cutoff, mode)


@pytest.mark.parametrize("mode", list(evidence._HistoricalSequenceMode))
def test_indexed_poll_checker_differential_invalid_status_missing_timer_and_legal_timer(mode):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    _assert_poll_checker_differential(observation, cutoff, mode)
    invalid = deepcopy(observation)
    invalid["polls"][0]["status"] = "INVALID"
    invalid["records"] = [{}]
    _assert_poll_checker_differential(invalid, cutoff, mode)
    assert evidence._historical_polls_endpoint_compatible(invalid, cutoff) is False
    missing = deepcopy(observation)
    poll = missing["polls"][-1]
    poll["status"] = "IN_PROGRESS"
    timer_id = f"P3_TIMER_{int(poll['poll_id'].partition('_POLL_')[2])}"
    missing["records"] = [row for row in missing["records"] if row.get("stable_id") != timer_id]
    _assert_poll_checker_differential(missing, cutoff, mode)


def test_indexed_poll_checker_unsafe_relevant_row_uses_whole_call_fallback(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    observation["records"].append({"kind": "HISTORICAL_TIMER", "stable_id": [],
                                    "classification": "SUCCESS", "key": {}})
    expected = evidence._historical_polls_endpoint_mode_compatible_original(
        observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
    )
    called = 0
    original = evidence._historical_polls_endpoint_mode_compatible_original

    def spy(*args):
        nonlocal called
        called += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    actual = evidence._historical_polls_endpoint_mode_compatible(
        observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
    )
    assert actual == expected
    assert called == 1


def test_indexed_poll_checker_subclass_sources_fall_back_before_hash_or_mapping_reads(monkeypatch):
    class UnhashableStr(str):
        __hash__ = None  # pyright: ignore[reportAssignmentType]

    class DictSubclass(dict):
        def __contains__(self, key):
            raise AssertionError("subclass containment must not be called by fast path")

    _, observation, cutoff = h03_endpoint_observation(100_001)
    observation["polls"] = []
    observation["records"] = [{
        "kind": "HISTORICAL_TIMER", "stable_id": UnhashableStr("timer"),
        "classification": "SUCCESS", "key": {"schedule_sequence": 0},
    }]
    original = evidence._historical_polls_endpoint_mode_compatible_original
    cases = [observation]
    mapping_case = deepcopy(observation)
    mapping_case["records"] = [DictSubclass({"kind": "OTHER"})]
    cases.append(mapping_case)
    for case in cases:
        try:
            expected = original(case, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1)
        except Exception as error:
            expected = (type(error), error.args)
        calls = 0

        def spy(*args):
            nonlocal calls
            calls += 1
            return original(*args)

        monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", original)
        monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
        try:
            actual = evidence._historical_polls_endpoint_mode_compatible(
                case, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
            )
        except Exception as error:
            actual = (type(error), error.args)
        assert actual == expected
        assert calls == 1
        monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", original)


def test_indexed_poll_checker_stateful_classification_subclass_falls_back(monkeypatch):
    class Classification(str):
        def __eq__(self, other):
            self.calls = getattr(self, "calls", 0) + 1
            if self.calls > 1:
                raise RuntimeError("classification compared twice")
            return super().__eq__(other)

    def make_observation():
        _, observation, cutoff = h03_endpoint_observation(100_001)
        observation["polls"] = []
        observation["pending_keys"] = []
        observation["records"] = [{
            "kind": "HISTORICAL_TIMER", "stable_id": "x",
            "classification": Classification("SUCCESS"),
            "key": {"schedule_sequence": 0},
        }]
        return observation, cutoff

    original = evidence._historical_polls_endpoint_mode_compatible_original
    observation, cutoff = make_observation()
    args = (observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1)
    try:
        expected = original(*args)
    except Exception as error:
        expected = (type(error), error.args)
    calls = 0

    def spy(*spy_args):
        nonlocal calls
        calls += 1
        return original(*spy_args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    observation, cutoff = make_observation()
    args = (observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1)
    try:
        actual = evidence._historical_polls_endpoint_mode_compatible(*args)
    except Exception as error:
        actual = (type(error), error.args)
    assert actual == expected is True
    assert calls == 1


def test_indexed_poll_checker_nested_non_json_payload_falls_back(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    observation["records"][0]["payload"] = {"nested": ([object()],)}
    original = evidence._historical_polls_endpoint_mode_compatible_original
    calls = 0

    def spy(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    args = (observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1)
    actual = evidence._historical_polls_endpoint_mode_compatible(*args)
    expected = original(*args)
    assert actual == expected
    assert calls == 1


def test_indexed_poll_checker_tuple_float_payload_stays_on_fast_path(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    observation["records"][0]["payload"] = ("allowed", [1.25, None, True])
    original = evidence._historical_polls_endpoint_mode_compatible_original
    calls = 0

    def spy(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    args = (observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1)
    expected = original(*args)
    actual = evidence._historical_polls_endpoint_mode_compatible(*args)
    assert actual == expected
    assert calls == 0




def test_indexed_poll_checker_contract_a_fallback_parity_and_input_purity(monkeypatch):
    _, base, cutoff = h03_endpoint_observation(100_001)
    mode = evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1
    invalid_poll_id = {
        "records": [], "pending_keys": [],
        "polls": [{"status": "COMPLETED", "awaiting": None, "poll_id": 7, "continuation_id": "x"}],
    }
    paused = deepcopy(invalid_poll_id)
    paused["polls"][0].update(status="PAUSED")
    completed_keyerror = deepcopy(invalid_poll_id)
    completed_keyerror["polls"][0].update(status="COMPLETED")
    completed_keyerror["records"] = [{"kind": "OTHER"}]
    huge_id = deepcopy(invalid_poll_id)
    huge_id["polls"][0].update(poll_id="P3_POLL_" + "1" * 5000)
    invalid_integer_id = {
        "records": [], "pending_keys": [],
        "polls": [{"status": "INVALID", "awaiting": None, "poll_id": 0, "continuation_id": "x"}],
    }
    invalid_huge_id = deepcopy(invalid_integer_id)
    invalid_huge_id["polls"][0].update(poll_id="P3_POLL_" + "1" * 5000)
    negative_sequence = deepcopy(base)
    poll = next(row for row in negative_sequence["polls"] if row["status"] == "COMPLETED")
    poll_id = poll["poll_id"]
    raw_at = int(poll_id.partition("_POLL_")[2])
    timer = next(row for row in negative_sequence["records"]
                 if row["kind"] == "HISTORICAL_TIMER" and row["stable_id"] == f"P3_TIMER_{raw_at}")
    timer["key"]["schedule_sequence"] = -1
    capture = next(row for row in negative_sequence["records"]
                   if row["kind"] == "SNAPSHOT_CAPTURE" and row["stable_id"] == f"{poll_id}_TRADING")
    capture["key"]["schedule_sequence"] = 0
    helper_exception = deepcopy(base)
    helper_poll = next(row for row in helper_exception["polls"] if row["status"] == "COMPLETED")
    helper_poll_id = helper_poll["poll_id"]
    helper_raw_at = int(helper_poll_id.partition("_POLL_")[2])
    helper_timer = next(row for row in helper_exception["records"]
                        if row["kind"] == "HISTORICAL_TIMER"
                        and row["stable_id"] == f"P3_TIMER_{helper_raw_at}")
    helper_timer["key"]["schedule_sequence"] = -1
    helper_capture = next(row for row in helper_exception["records"]
                          if row["kind"] == "SNAPSHOT_CAPTURE"
                          and row["stable_id"] == f"{helper_poll_id}_TRADING")
    helper_capture["key"]["schedule_sequence"] = -4
    cases = [
        (invalid_poll_id, None), (paused, "PAID_x"),
        (completed_keyerror, None), (huge_id, None),
        (invalid_integer_id, None), (invalid_huge_id, None),
        (negative_sequence, None), (helper_exception, None),
    ]

    actual_original = evidence._historical_polls_endpoint_mode_compatible_original
    calls = 0

    def fallback_spy(*args):
        nonlocal calls
        calls += 1
        return actual_original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", fallback_spy)
    for observation, paid_prefix in cases:
        pristine = deepcopy(observation)
        args = (observation, cutoff, mode, paid_prefix)
        try:
            expected = actual_original(*args)
        except Exception as error:
            expected = (type(error), error.args)
        before = calls
        try:
            actual = evidence._historical_polls_endpoint_mode_compatible(*args)
        except Exception as error:
            actual = (type(error), error.args)
        assert actual == expected
        if observation in (invalid_integer_id, invalid_huge_id):
            assert actual is False
        if isinstance(expected, tuple) and len(expected) == 2 and isinstance(expected[0], type):
            assert calls == before + 1
        else:
            assert calls in (before, before + 1)
        assert observation == pristine

    _, normal, normal_cutoff = h03_endpoint_observation(100_001)
    pristine = deepcopy(normal)
    before = calls
    expected = actual_original(normal, normal_cutoff, mode)
    actual = evidence._historical_polls_endpoint_mode_compatible(normal, normal_cutoff, mode)
    assert actual == expected
    assert calls == before
    assert normal == pristine


def test_indexed_poll_checker_real_sequence_negative_schedule_falls_back(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    poll = next(row for row in observation["polls"] if row["status"] == "COMPLETED")
    raw_at = int(poll["poll_id"].partition("_POLL_")[2])
    timer = next(row for row in observation["records"]
                 if row["kind"] == "HISTORICAL_TIMER" and row["stable_id"] == f"P3_TIMER_{raw_at}")
    timer["key"]["schedule_sequence"] = -1
    capture = next(row for row in observation["records"]
                   if row["kind"] == "SNAPSHOT_CAPTURE" and row["stable_id"] == f"{poll['poll_id']}_TRADING")
    capture["key"]["schedule_sequence"] = 0
    mode = evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1
    original = evidence._historical_polls_endpoint_mode_compatible_original
    calls = 0

    def spy(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    expected = original(observation, cutoff, mode)
    actual = evidence._historical_polls_endpoint_mode_compatible(observation, cutoff, mode)
    assert actual == expected is False
    assert calls == 0


def test_indexed_poll_checker_metaclass_equality_is_never_called_by_guard(monkeypatch):
    class Meta(type):
        def __eq__(cls, other):
            raise RuntimeError("metaclass equality called")

    class Unknown(metaclass=Meta):
        pass

    _, observation, cutoff = h03_endpoint_observation(100_001)
    observation["records"][0]["payload"] = {"deep": (Unknown(),)}
    mode = evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1
    original = evidence._historical_polls_endpoint_mode_compatible_original
    try:
        expected = original(observation, cutoff, mode)
    except Exception as error:
        expected = (type(error), error.args)
    calls = 0

    def fallback_spy(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", fallback_spy)
    try:
        actual = evidence._historical_polls_endpoint_mode_compatible(observation, cutoff, mode)
    except Exception as error:
        actual = (type(error), error.args)
    assert actual == expected
    assert calls == 1


def test_indexed_poll_checker_unhashable_delivery_query_key_falls_back(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    monkeypatch.setattr(evidence, "_historical_delivery_sequence", lambda *_args: [])
    expected = evidence._historical_polls_endpoint_mode_compatible_original(
        observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
    )
    called = 0
    original = evidence._historical_polls_endpoint_mode_compatible_original

    def spy(*args):
        nonlocal called
        called += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    actual = evidence._historical_polls_endpoint_mode_compatible(
        observation, cutoff, evidence._HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
    )
    assert actual == expected
    assert called == 1


@pytest.mark.parametrize("pending_capture", [False, True])
def test_indexed_poll_checker_empty_delivery_source_uses_empty_result(pending_capture, monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    if pending_capture:
        poll = next(row for row in observation["polls"] if row["status"] == "IN_PROGRESS")
    else:
        poll = next(row for row in observation["polls"] if row["status"] == "COMPLETED")
    timer_id = f"P3_TIMER_{int(poll['poll_id'].partition('_POLL_')[2])}"
    observation["polls"] = [poll]
    observation["records"] = [
        row for row in observation["records"]
        if row.get("stable_id") == timer_id
        or (row.get("kind") == "SNAPSHOT_CAPTURE" and
            type(row.get("stable_id")) is str and row["stable_id"].startswith(poll["poll_id"] + "_"))
    ]
    observation["pending_keys"] = [
        deepcopy(row["key"]) for row in observation["records"]
        if row.get("classification") == "PENDING"
    ]
    monkeypatch.setattr(evidence, "_historical_delivery_sequence", lambda *_args: [])
    for mode in evidence._HistoricalSequenceMode:
        _assert_poll_checker_differential(observation, cutoff, mode)


def test_missing_capture_stable_id_without_timer_returns_false_and_falls_back(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    poll = observation["polls"][-1]
    poll["status"] = "IN_PROGRESS"
    observation["polls"] = [poll]
    raw_at = int(poll["poll_id"].partition("_POLL_")[2])
    timer_id = f"P3_TIMER_{raw_at}"
    missing_capture_id = f"{poll['poll_id']}_{poll['awaiting']}"
    observation["records"] = [
        row for row in observation["records"]
        if row.get("stable_id") != timer_id
    ]
    capture = next(row for row in observation["records"]
                   if row["kind"] == "SNAPSHOT_CAPTURE" and row["stable_id"] == missing_capture_id)
    del capture["stable_id"]
    original = evidence._historical_polls_endpoint_mode_compatible_original
    calls = 0

    def spy(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_polls_endpoint_mode_compatible_original", spy)
    for mode in evidence._HistoricalSequenceMode:
        _assert_poll_checker_differential(observation, cutoff, mode)
        assert evidence._historical_polls_endpoint_mode_compatible(observation, cutoff, mode) is False
    assert calls == len(evidence._HistoricalSequenceMode) * 3


def test_invalid_poll_status_short_circuits_malformed_records_differential(monkeypatch):
    _, observation, cutoff = h03_endpoint_observation(100_001)
    observation["polls"][0]["status"] = "INVALID"
    observation["records"] = [{}]
    actual = evidence._historical_polls_endpoint_compatible(observation, cutoff)
    monkeypatch.setattr(
        evidence, "_historical_polls_endpoint_mode_compatible",
        evidence._historical_polls_endpoint_mode_compatible_original,
    )
    expected = evidence._historical_polls_endpoint_compatible(observation, cutoff)
    assert actual is expected is False


def expected_checks(values):
    attempt, status, journal, endpoint, report = values
    coverage = attempt["planned_coverage"]
    identities, digests = [], []
    sites = [("INITIAL", INITIAL, endpoint["initial_owner_evidence"])]
    for i, row in enumerate(journal):
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            for phase in ["before", "after"]:
                sites.append((row["barrier_id"] + ":" + phase.upper(), f"artifact:journal.jsonl#/{i}/payload/owner_evidence_{phase}", row["payload"]["owner_evidence_" + phase]))
    sites.append(("FINAL", FINAL, endpoint["final_owner_evidence"]))
    for stage, ref, owner in sites:
        inspection = owner["inspection"]
        identities.append(dict(evidence_ref=ref, account_key=attempt["account_key"], profile_id=attempt["profile_id"], config_id="scenario-v1"))
        digests.append(dict(barrier_id=stage, expected_owner_sha256=inspection["owner_state_digest"], observed_owner_sha256=inspection["owner_state_digest"]))
    final = endpoint["final_owner_evidence"]["inspection"]
    version = final["account_version"]
    cutoff = endpoint["cutoff"]["scheduler_time"]
    shapes = [coverage, dict(processed_boundary=status["processed_boundary"], persisted_boundary=status["persisted_boundary"], last_planned=coverage[-1]),
              [dict(row, journal_seq=row["ordinal"]) for row in coverage], identities, digests,
              dict(inspection_account_version=version, endpoint_cutoff=cutoff, **{key: dict(captured_account_version=version, snapshot_as_of=cutoff) for key in ["trading", "positions", "open_orders"]}),
              dict(pending_keys=[], remaining_planned_barriers=[]), [], [],
              dict(terminal_policy=attempt["terminal_policy"], terminal_reason=attempt["terminal_policy"], scheduler_gate="RUNNING", scheduler_terminal=None,
                   owner_gate="RUNNING", owner_lifecycle=final["lifecycle"], remaining_planned_barriers=[]),
              dict(report_rows=report, report_sha256=sha256(artifacts.canonical_bytes(report)).hexdigest())]
    jrefs = [f"journal:{row['journal_seq']}" for row in journal]
    srefs = [f"journal:{row['journal_seq']}" for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    dc = [f"journal:{row['journal_seq']}" for row in journal if row["record_kind"] != "SOURCE_GROUP_RESULT"]
    refs = [["artifact:attempt.json#/planned_coverage", *jrefs], ["artifact:status.json#/processed_boundary", "artifact:status.json#/persisted_boundary", "artifact:attempt.json#/planned_coverage"],
            jrefs, [INITIAL, *srefs, FINAL], [INITIAL, *srefs, FINAL], [FINAL],
            ["artifact:endpoint.json#/scheduler_observation/pending_keys", "artifact:endpoint.json#/remaining_planned_barriers"], ["artifact:endpoint.json#/scheduler_observation/polls"],
            ["artifact:endpoint.json#/scheduler_observation/callback_actions", *dc],
            ["artifact:attempt.json#/terminal_policy", "artifact:endpoint.json#/terminal_reason", "artifact:endpoint.json#/cutoff", "artifact:endpoint.json#/scheduler_observation", FINAL],
            [FINAL, *[f"artifact:report.jsonl#/{i}" for i in range(len(report))]]]
    return [dict(name=name, expected=deepcopy(shape), observed=deepcopy(shape), result="OK", evidence_refs=ref) for name, shape, ref in zip(NAMES, shapes, refs, strict=True)]


@pytest.mark.parametrize("index", range(3))
def test_exact_all_eleven_rows_references_and_immutability(index):
    values = fixture(index)
    validate_inputs(values)
    before = deepcopy(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result == dict(schema_version="spider_reconciliation_v1", run_id=RUN, result="OK", checks=expected_checks(values))
    reconciliation(result)
    assert values == before
    result["checks"][0]["observed"].clear()
    assert values == before


def at(value: Any, path: tuple) -> Any:
    for key in path:
        value = value[key]
    return value


@pytest.mark.parametrize("index", range(3))
def test_unrepresentable_config_at_every_identity_site(index):
    values = fixture(index)
    sites = [(3, "initial_owner_evidence"), (3, "final_owner_evidence")]
    sites += [(2, i, "payload", phase) for i, row in enumerate(values[2])
              if row["record_kind"] == "SOURCE_GROUP_RESULT"
              for phase in ("owner_evidence_before", "owner_evidence_after")]
    for site in sites:
        mutated = deepcopy(values)
        at(mutated, site)["inspection"]["config_id"] = "other-config"
        validate_inputs(mutated)
        before = deepcopy(mutated)
        with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$") as error:
            evidence.build_reconciliation(RUN, *mutated)
        assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
        assert mutated == before


@pytest.mark.parametrize("index", range(3))
@pytest.mark.parametrize("field", ["account_key", "profile_id"])
def test_attempt_identity_mismatch_is_representable_failure(index, field):
    values = fixture(index)
    if field == "account_key":
        values[0][field]["account"] = "other-account"
    else:
        values[0][field] = "SYNTHETIC_P1_LIQUIDATION_V1" if index == 0 else "SYNTHETIC_MIN_CASH_V1"
    validate_inputs(values)
    before = deepcopy(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["checks"][3]["result"] == "FAILED"
    assert result["result"] == "FAILED"
    reconciliation(result)
    assert values == before


MUTATIONS = [
    ((0, "planned_coverage", 0, "barrier_id"), "SOURCE_GROUP:other", {1}),
    ((2, 0, "barrier_id"), "SOURCE_GROUP:other", {1, 3, 5}),
    ((2, 0, "effective_at"), 999, {3}),
    ((2, 0, "visible_at"), 999, {3}),
    ((2, 0, "account_version_after"), 99, {3}),
    ((2, 0, "scheduler_key", "schedule_sequence"), 99, {3}),
    ((2, 0, "causal_parent_ids"), ["changed"], {3}),
    ((2, 0, "payload", "request", "account_key", "account"), "B", {5}),
    ((2, 0, "payload", "request", "members", 0, "payload", "price"), "1", {5}),
    ((2, 0, "payload", "result", "group_id"), "changed", {5}),
    ((2, 0, "payload", "result", "owner_state_digest"), "f" * 64, {5}),
    ((2, 0, "payload", "owner_evidence_before", "inspection", "cash"), "1", {5}),
    ((2, 0, "payload", "owner_evidence_after", "inspection", "positions_digest"), "f" * 64, {5}),
    ((3, "initial_owner_evidence", "inspection", "account_key", "account"), "B", {4, 5}),
    ((3, "final_owner_evidence", "trading_request", "captured_at"), 999, {5}),
    ((3, "final_owner_evidence", "positions_fact", "captured_account_version"), 99, {5, 6}),
    ((3, "final_owner_evidence", "open_orders_fact", "snapshot_as_of"), 999, {5, 6}),
    ((3, "final_owner_evidence", "trading_fact", "immutable_payload", "equity"), "1", {5}),
    ((3, "final_owner_evidence", "trading_fact", "request_digest"), "f" * 64, {5}),
    ((2, 3, "payload", "delivery", "immutable_payload", "fill_price"), "1", {9}),
    ((2, 3, "payload", "delivery", "account_key", "account"), "B", {9}),
    ((2, 3, "payload", "emission_plan_digest"), "f" * 64, {9}),
    ((2, 4, "payload", "policy_events"), [dict(at_ms=504, kind="alert", reason="total_limit")], {9}),
    ((2, 4, "payload", "outcome"), "CALLBACK_FAILED", {9}),
    ((3, "scheduler_observation", "pending_keys"), [dict(visible_at=999, queue_class="DELIVERY", schedule_sequence=0, stable_id="new")], {7, 10}),
    ((3, "scheduler_observation", "polls"), [dict(poll_id="p", continuation_id="c", status="PAUSED", awaiting=None)], {8, 10}),
    ((3, "scheduler_observation", "records", 0, "classification"), "PENDING", {10}),
    ((3, "scheduler_observation", "last_popped", "visible_at"), 999, {10}),
    ((3, "cutoff", "scheduler_time"), 999, {6, 10}),
    ((3, "terminal_reason"), "O03_NON_ATOMIC_COMPLETE", {10}),
    ((4, 0, "account_cash"), "1", {11}),
    ((4, 0, "source_evidence_refs"), ["journal:2"], {11}),
    ((4, 0, "committed_execution_refs"), [], {11}),
    ((1, "processed_boundary", "ordinal"), 4, {2}),
]


@pytest.mark.parametrize("path,replacement,failed", MUTATIONS)
def test_rehashed_outer_artifacts_do_not_hide_mutations(path, replacement, failed):
    values = fixture()
    parent = at(values, path[:-1])
    parent[path[-1]] = replacement
    validate_inputs(values)
    before = deepcopy(values)
    outer_hashes = [sha256(artifacts.canonical_bytes(value)).hexdigest() for value in values]
    assert all(len(digest) == 64 for digest in outer_hashes)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["result"] == "FAILED"
    assert {i + 1 for i, check in enumerate(result["checks"]) if check["result"] == "FAILED"} == failed
    reconciliation(result)
    assert values == before


@pytest.mark.parametrize("index", range(3))
@pytest.mark.parametrize("phase", ["owner_evidence_before", "owner_evidence_after"])
@pytest.mark.parametrize("path,replacement", [
    (("inspection", "account_version"), 99),
    (("inspection", "owner_state_digest"), "f" * 64),
    (("trading_request", "snapshot_id"), "other"),
    (("positions_request", "captured_at"), 999),
    (("open_orders_request", "account_key", "account"), "other"),
    (("trading_fact", "immutable_payload", "available_equity"), "1"),
    (("positions_fact", "payload_digest"), "f" * 64),
    (("open_orders_fact", "request_digest"), "f" * 64),
])
def test_all_protocols_full_source_owner_vectors(index, phase, path, replacement):
    values = fixture(index)
    owner = values[2][0]["payload"][phase]
    at(owner, path[:-1])[path[-1]] = replacement
    validate_inputs(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["checks"][4]["result"] == "FAILED"
    reconciliation(result)


def test_o03_cancel_target_order_and_rejection_with_retained_digests():
    for mutation in ["targets", "rejection"]:
        values = fixture(2)
        payload = values[2][2]["payload"]
        if mutation == "targets":
            payload["request"]["members"][0]["payload"]["targets"].reverse()
        else:
            payload["result"]["rejections"][0]["reason"] = "CAPACITY_EXCEEDED"
        validate_inputs(values)
        assert evidence.build_reconciliation(RUN, *values)["checks"][4]["result"] == "FAILED"


@pytest.mark.parametrize("matches", [0, 1, 2])
def test_callback_matching_complete_result_unique_whole_row(matches):
    values = fixture()
    result = deepcopy(values[2][0]["payload"]["result"])
    if matches == 0:
        result["result_digest"] = "f" * 64
    if matches == 2:
        duplicate = deepcopy(values[2][0])
        duplicate.update(journal_seq=6, barrier_id="SOURCE_GROUP:duplicate")
        values[2].append(duplicate)
    values[3]["scheduler_observation"]["callback_actions"] = [dict(delivery_id="d", event_index=0, action_index=0,
        group_id=result["group_id"], status="SUBMITTED", group_result=result, native_failure=None)]
    validate_inputs(values)
    actual = evidence.build_reconciliation(RUN, *values)
    action = actual["checks"][8]["observed"][0]
    assert action["group_result_ref"] == ("journal:1" if matches == 1 else None)
    assert action["cancel_effect_ref"] is None
    assert actual["checks"][8]["result"] == "FAILED"
    reconciliation(actual)


def test_actions_index_matches_ordered_scan_and_uses_fast_path(monkeypatch):
    first = dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=4,
                 payload=dict(result=dict(group_id="g", value=1)))
    second = dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=7,
                  payload=dict(result=dict(group_id="g", value=2)))
    actions = [dict(delivery_id="d1", event_index=0, action_index=0, group_id="g", status="SUBMITTED",
                    group_result=dict(group_id="g", value=2)),
               dict(delivery_id="d2", event_index=1, action_index=1, group_id="g", status="SUBMITTED",
                    group_result=dict(group_id="g", value=1))]
    fallbacks = 0
    original = evidence._actions_original

    def count_fallback(journal, observed):
        nonlocal fallbacks
        fallbacks += 1
        return original(journal, observed)

    monkeypatch.setattr(evidence, "_actions_original", count_fallback)
    assert evidence._actions([first, second], actions) == original([first, second], actions)
    assert [row["group_result_ref"] for row in evidence._actions([first, second], actions)] == ["journal:7", "journal:4"]
    assert fallbacks == 0


@pytest.mark.parametrize("journal,actions", [
    ([dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=1, payload=dict(result=dict(group_id="g", value=1)))],
     [dict(delivery_id="d", event_index=0, action_index=0, group_id=[], status="SUBMITTED", group_result={})]),
    ([], [dict(delivery_id="d", event_index=0, action_index=0, status="SUBMITTED")]),
    ([{}], []),
])
def test_actions_index_malformed_inputs_match_verbatim_oracle(journal, actions):
    def outcome(call):
        try:
            return ("return", call())
        except Exception as error:
            return ("raise", type(error), error.args)

    assert outcome(lambda: evidence._actions(journal, actions)) == outcome(
        lambda: evidence._actions_original(journal, actions))


def test_actions_exception_fallback_discards_partial_index(monkeypatch):
    journal = [dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=1,
                    payload=dict(result=dict(group_id="g", value="x")))]
    actions = [dict(delivery_id="d", event_index=0, action_index=0, group_id="g", status="SUBMITTED",
                    group_result=dict(group_id="g", value="x"))]
    expected = evidence._actions_original(journal, actions)
    original_equal = evidence._equal
    calls = 0

    def fail_once(left, right):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("fast-path-only")
        return original_equal(left, right)

    monkeypatch.setattr(evidence, "_equal", fail_once)
    assert evidence._actions(journal, actions) == expected
    assert calls == 2


def test_actions_missing_group_result_precedes_later_malformed_source_result(monkeypatch):
    journal = [dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=1,
                    payload=dict(result=dict(group_id="g", value="x"))),
               dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=2)]
    actions = [dict(delivery_id="d", event_index=0, action_index=0, group_id="g", status="SUBMITTED")]
    original = evidence._actions_original
    fallback_calls = 0

    def fallback(journal_rows, observed):
        nonlocal fallback_calls
        fallback_calls += 1
        return original(journal_rows, observed)

    monkeypatch.setattr(evidence, "_actions_original", fallback)
    with pytest.raises(KeyError) as error:
        evidence._actions(journal, actions)
    assert error.value.args == ("group_result",)
    assert fallback_calls == 1


def test_deferred_action_index_differential_duplicates_and_fallback(monkeypatch):
    key = dict(queue_class="DELIVERY", stable_id="d", schedule_sequence=1, visible_at=6)
    callback = dict(record_kind="CALLBACK_RESULT", scheduler_key=key, payload=dict(
        outcome="SUCCESS", delivery_id="d",
        actions=[dict(event_index=0, action_index=0, status="UNSUBMITTED",
        group_id=None, group_result=None, native_failure=None)]))
    observation = dict(records=[dict(kind="DELIVERY", stable_id="d", classification="SUCCESS", key=key)],
                       callback_actions=[dict(delivery_id="d", event_index=0, action_index=0, group_id=None,
                       status="UNSUBMITTED", group_result=None, effective_at=19)], pending_keys=[])
    original = evidence._historical_deferred_actions_compatible_original
    fallbacks = 0

    def count_fallback(journal, observed, cutoff):
        nonlocal fallbacks
        fallbacks += 1
        return original(journal, observed, cutoff)

    def outcome(call):
        try:
            return ("return", call())
        except Exception as error:
            return ("raise", type(error), error.args)

    monkeypatch.setattr(evidence, "_historical_deferred_actions_compatible_original", count_fallback)
    assert evidence._historical_deferred_actions_compatible([callback], observation, 6) is True
    assert fallbacks == 0
    duplicated = dict(observation, records=[*observation["records"], observation["records"][0]])
    assert evidence._historical_deferred_actions_compatible([callback], duplicated, 6) == original([callback], duplicated, 6)
    assert fallbacks == 0
    malformed = dict(observation, records=[{}])
    assert outcome(lambda: evidence._historical_deferred_actions_compatible([callback], malformed, 6)) == outcome(
        lambda: original([callback], malformed, 6))
    assert fallbacks == 1


def test_action_fast_paths_leave_inputs_unchanged_on_success_and_fallback():
    journal = [dict(record_kind="SOURCE_GROUP_RESULT", journal_seq=1,
                    payload=dict(result=dict(group_id="g", value="x")))]
    actions = [dict(delivery_id="d", event_index=0, action_index=0, group_id="g", status="SUBMITTED",
                    group_result=dict(group_id="g", value="x"))]
    before = deepcopy((journal, actions))
    evidence._actions(journal, actions)
    assert (journal, actions) == before

    key = dict(queue_class="DELIVERY", stable_id="d", schedule_sequence=1, visible_at=6)
    callback = dict(record_kind="CALLBACK_RESULT", scheduler_key=key, payload=dict(
        outcome="SUCCESS", delivery_id="d", actions=[dict(event_index=0, action_index=0,
        status="UNSUBMITTED", group_id=None, group_result=None, native_failure=None)]))
    observation = dict(records=[dict(kind="DELIVERY", stable_id="d", classification="SUCCESS", key=key)],
                       callback_actions=[dict(delivery_id="d", event_index=0, action_index=0, group_id=None,
                       status="UNSUBMITTED", group_result=None, effective_at=19)], pending_keys=[])
    before = deepcopy(([callback], observation))
    evidence._historical_deferred_actions_compatible([callback], observation, 6)
    assert ([callback], observation) == before
    malformed_observation = dict(observation, records=[{}])
    before = deepcopy(([callback], malformed_observation))
    with pytest.raises(KeyError, match="kind"):
        evidence._historical_deferred_actions_compatible([callback], malformed_observation, 6)
    assert ([callback], malformed_observation) == before


def test_actions_payload_dict_subclass_falls_back_without_fast_path_access(monkeypatch):
    class Payload(dict):
        touched = False

        def __getitem__(self, key):
            type(self).touched = True
            raise AssertionError("fast path touched custom payload")

    payload = Payload(result={"group_id": "g"})
    journal = [{"record_kind": "SOURCE_GROUP_RESULT", "payload": payload}]
    actions = []
    original = evidence._actions_original
    calls = []

    def fallback(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(evidence, "_actions_original", fallback)
    before = dict(payload)
    def outcome(call):
        try:
            return ("value", call())
        except Exception as exc:
            return ("exception", type(exc), exc.args)
    assert outcome(lambda: evidence._actions(journal, actions)) == outcome(lambda: original(journal, actions))
    assert calls == [(journal, actions)]
    assert Payload.touched is False
    assert dict(payload) == before


def test_deferred_payload_dict_subclass_falls_back_without_fast_path_access(monkeypatch):
    class Payload(dict):
        touched = False

        def __getitem__(self, key):
            type(self).touched = True
            raise AssertionError("fast path touched custom payload")

    payload = Payload(result={"group_id": "g"})
    journal = [{"record_kind": "SOURCE_GROUP_RESULT", "payload": payload}]
    observation = {"records": [], "callback_actions": [], "pending_keys": []}
    original = evidence._historical_deferred_actions_compatible_original
    calls = []

    def fallback(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(evidence, "_historical_deferred_actions_compatible_original", fallback)
    before = dict(payload)
    def outcome(call):
        try:
            return ("value", call())
        except Exception as exc:
            return ("exception", type(exc), exc.args)
    assert outcome(lambda: evidence._historical_deferred_actions_compatible(journal, observation, 0)) == outcome(
        lambda: original(journal, observation, 0)
    )
    assert calls == [(journal, observation, 0)]
    assert Payload.touched is False
    assert dict(payload) == before


@pytest.mark.parametrize("status", ["SUBMITTED", "UNSUBMITTED"])
def test_unresolved_nullable_callback_is_failed_not_projection_error(status):
    values = fixture()
    values[3]["scheduler_observation"]["callback_actions"] = [dict(delivery_id="d", event_index=1, action_index=2,
        group_id=None, status=status, group_result=None, native_failure=None)]
    validate_inputs(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["checks"][8]["result"] == "FAILED"
    assert result["checks"][8]["observed"][0]["group_result_ref"] is None


@pytest.mark.parametrize("path", [(1, "processed_boundary"), (1, "persisted_boundary"),
                                  (3, "final_owner_evidence", "trading_fact", "captured_account_version"),
                                  (3, "final_owner_evidence", "positions_fact", "captured_account_version"),
                                  (3, "final_owner_evidence", "open_orders_fact", "captured_account_version")])
def test_missing_projection_has_dedicated_error(path):
    values = fixture()
    if len(path) == 2:
        values[1][path[-1]] = None
    else:
        del at(values, path[:-1])[path[-1]]
    validate_inputs(values)
    before = deepcopy(values)
    with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$") as error:
        evidence.build_reconciliation(RUN, *values)
    assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert values == before


@pytest.mark.parametrize("location", [(0,), (1,), (2, 0), (2, 4), (3,), (4, 0), (4, 1)])
def test_every_run_identity_checked_before_selection(location, monkeypatch):
    values = fixture()
    at(values, location)["run_id"] = "different"
    def forbidden(_):
        raise AssertionError("selector must not run")
    monkeypatch.setattr(evidence, "_plan_bundle", forbidden)
    with pytest.raises(evidence.ReconciliationProjectionError):
        evidence.build_reconciliation(RUN, *values)


@pytest.mark.parametrize("selector", [None, 1, True, "unknown", [PLAN_IDS[0]]])
def test_invalid_plan_resolution(selector):
    values = fixture()
    values[0]["scenario_plan_id"] = selector
    with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$"):
        evidence.build_reconciliation(RUN, *values)


@pytest.mark.parametrize("builder", ["endpoint", "reconciliation"])
def test_selected_v2_rejects_malformed_context_before_projection(builder):
    attempt: dict[str, Any] = {"run_id": RUN, "scenario_plan_id": "SPIDER_P2_CONFIGURED_SCALE_V1",
                               "run_contract_id": "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1",
                               "configuration_context": {"malformed": True}}
    status: dict[str, Any] = {"run_id": RUN}
    if builder == "endpoint":
        with pytest.raises(ValueError, match="^INVALID_ARTIFACT$"):
            evidence.build_endpoint_artifacts(RUN, attempt, status, [], {}, {}, {})
    else:
        endpoint: dict[str, Any] = {"run_id": RUN}
        with pytest.raises(ValueError, match="^INVALID_ARTIFACT$"):
            evidence.build_reconciliation(RUN, attempt, status, [], endpoint, [])


@pytest.mark.parametrize("check", range(11))
def test_configured_oracle_rejects_independent_mutation_at_every_check(configured_artifacts, check):
    source = deepcopy(configured_artifacts)
    values = [source[0], source[1], source[2], source[3], source[5]]
    attempt, status, journal, endpoint, report = values
    if check == 0:
        attempt["planned_coverage"][0]["barrier_id"] = "SOURCE_GROUP:changed"
    elif check == 1:
        status["processed_boundary"]["ordinal"] = 39
    elif check == 2:
        journal[0]["scheduler_key"]["stable_id"] = "changed"
    elif check == 3:
        journal[0]["payload"]["owner_evidence_before"]["inspection"]["account_key"]["account"] = "OTHER"
    elif check == 4:
        journal[0]["payload"]["result"]["owner_state_digest"] = "f" * 64
    elif check == 5:
        endpoint["final_owner_evidence"]["trading_fact"]["captured_account_version"] = 16
    elif check == 6:
        endpoint["scheduler_observation"]["pending_keys"] = [journal[0]["scheduler_key"]]
    elif check == 7:
        endpoint["scheduler_observation"]["polls"][0]["status"] = "PAUSED"
    elif check == 8:
        endpoint["scheduler_observation"]["callback_actions"] = [dict(
            delivery_id="unsubmitted", event_index=0, action_index=0, group_id=None,
            status="UNSUBMITTED", group_result_ref=None, cancel_effect_ref=None)]
    elif check == 9:
        endpoint["terminal_reason"] = "O03_NON_ATOMIC_COMPLETE"
    else:
        report[0]["account_cash"] = "120"
    result = evidence.build_reconciliation("configured", *values)
    assert result["checks"][check]["result"] == "FAILED"
    assert result["result"] == "FAILED"


@pytest.mark.parametrize("products", [
    ["ETH-USDT-SWAP", "BTC-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP",
     "ARB-USDT-SWAP", "OP-USDT-SWAP", "NEAR-USDT-SWAP", "APT-USDT-SWAP", "SUI-USDT-SWAP", "ADA-USDT-SWAP"],
    ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "CFG-OTHER", "BNB-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP",
     "ARB-USDT-SWAP", "OP-USDT-SWAP", "NEAR-USDT-SWAP", "APT-USDT-SWAP", "SUI-USDT-SWAP", "ADA-USDT-SWAP"],
])
def test_configured_context_must_match_selected_ordered_products(configured_artifacts, products):
    values = deepcopy(configured_artifacts)
    embedded = values[0]["configuration_context"]
    embedded["products"] = products
    for artifact in [values[1], values[2], values[3], values[4], values[5], values[6]]:
        rows = artifact if type(artifact) is list else [artifact]
        for row in rows:
            row["configuration_context"] = deepcopy(embedded)
    by_product = {row["product_id"]: row for row in values[5]}
    reordered = []
    for product in products:
        if product in by_product:
            reordered.append(by_product[product])
        else:
            row = deepcopy(by_product["ETH-USDT-SWAP"])
            row.update(product_id=product, source_evidence_refs=["journal:3", "artifact:endpoint.json#/final_owner_evidence"])
            reordered.append(row)
    values[5][:] = reordered
    completion.report(values[5], context=artifacts.configuration_context(embedded))
    with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$"):
        evidence.build_reconciliation("configured", values[0], values[1], values[2], values[3], values[5])


@pytest.mark.parametrize("site", ["initial", "first-source-before", "middle-source-after", "last-source-after", "final"])
@pytest.mark.parametrize("mutation", ["request-account", "fact-reference"])
def test_configured_identity_binds_every_snapshot_request_fact(configured_artifacts, site, mutation):
    values = deepcopy(configured_artifacts)
    attempt, status, journal, endpoint, _, report, _ = values
    if site == "initial":
        owner = endpoint["initial_owner_evidence"]
    elif site == "final":
        owner = endpoint["final_owner_evidence"]
    else:
        sources = [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
        position, phase = {"first-source-before": (0, "before"), "middle-source-after": (len(sources) // 2, "after"),
                           "last-source-after": (len(sources) - 1, "after")}[site]
        owner = sources[position]["payload"]["owner_evidence_" + phase]
    if mutation == "request-account":
        owner["positions_request"]["account_key"]["account"] = "OTHER"
    else:
        owner["positions_fact"]["reference"]["fact_id"] = "OTHER-FACT"
    result = evidence.build_reconciliation("configured", attempt, status, journal, endpoint, report)
    assert result["checks"][3]["name"] == "OWNER_IDENTITY_MATCH"
    assert result["checks"][3]["result"] == "FAILED"


def test_configured_scheduler_records_require_exact_scheduled_success(configured_artifacts):
    values = deepcopy(configured_artifacts)
    attempt, status, journal, endpoint, _, report, _ = values
    endpoint["scheduler_observation"]["records"][0]["classification"] = "PENDING"
    result = evidence.build_reconciliation("configured", attempt, status, journal, endpoint, report)
    assert result["checks"][7]["name"] == "NO_UNRESOLVED_POLL"
    assert result["checks"][7]["result"] == "FAILED"


@pytest.mark.parametrize("event_id", ["event-514", "event-518", "event-519"])
@pytest.mark.parametrize("mutation", ["kind", "product", "event", "extra-reference", "wrong-reference"])
def test_configured_execution_source_recipe_and_committed_reference_vector(configured_artifacts, event_id, mutation):
    values = deepcopy(configured_artifacts)
    attempt, status, journal, endpoint, _, report, _ = values
    row = next(row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"
               and row["payload"]["request"]["members"][0]["kind"] == "EXECUTION"
               and row["payload"]["request"]["members"][0]["stamp"]["event_id"] == event_id)
    member = row["payload"]["request"]["members"][0]
    if mutation == "kind":
        member["kind"] = "INTENT"
    elif mutation == "product":
        member["payload"]["product_id"] = "DOGE-USDT-SWAP" if event_id == "event-514" else "BTC-USDT-SWAP"
    elif mutation == "event":
        member["stamp"]["event_id"] = "event-514" if event_id == "event-519" else "event-519"
    elif mutation == "extra-reference":
        row["payload"]["result"]["committed_references"].append(dict(namespace="SOURCE", fact_id="unowned"))
    else:
        other_event = "event-518" if event_id == "event-519" else "event-519"
        row["payload"]["result"]["committed_references"] = [dict(namespace="SOURCE", fact_id=other_event)]
    result = evidence.build_reconciliation("configured", attempt, status, journal, endpoint, report)
    assert result["checks"][10]["name"] == "REPORT_PROJECTION_MATCH"
    assert result["checks"][10]["result"] == "FAILED"


@pytest.mark.parametrize("mutation", ["journal-before", "journal-after", "result-before", "result-after", "owner-before", "owner-after"])
def test_configured_source_result_versions_bind_to_journal_owner_chain(configured_artifacts, mutation):
    values = deepcopy(configured_artifacts)
    attempt, status, journal, endpoint, _, report, _ = values
    row = next(row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT")
    if mutation == "journal-before":
        row["account_version_before"] = 999
    elif mutation == "journal-after":
        row["account_version_after"] = 999
    elif mutation == "result-before":
        row["payload"]["result"]["account_version_before"] = 999
    elif mutation == "result-after":
        row["payload"]["result"]["account_version_after"] = 999
    elif mutation == "owner-before":
        row["payload"]["owner_evidence_before"]["inspection"]["account_version"] = 999
    else:
        row["payload"]["owner_evidence_after"]["inspection"]["account_version"] = 999
    result = evidence.build_reconciliation("configured", attempt, status, journal, endpoint, report)
    assert result["checks"][5]["name"] == "ENDPOINT_SNAPSHOT_VERSION_MATCH"
    assert result["checks"][5]["result"] == "FAILED"


@pytest.mark.parametrize("artifact_index", [2, 5])
def test_configured_representable_length_mismatch_is_projection_error(configured_artifacts, artifact_index):
    values = deepcopy(configured_artifacts)
    attempt, status, journal, endpoint, _, report, _ = values
    changed = journal[:-1] if artifact_index == 2 else report[:-1]
    with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$"):
        evidence.build_reconciliation("configured", attempt, status, changed if artifact_index == 2 else journal,
                                      endpoint, changed if artifact_index == 5 else report)


def test_admission_accept_to_reject_on_schema_valid_attempt_account_mutation(tmp_path):
    from src.core.backtest.spider_run_admission import admit_spider_run
    from src.core.backtest.spider_scenario_run import _invoke

    assert _invoke(str(tmp_path), "identity", "SPIDER_P2_CONFIGURED_SCALE_V1", plan_bundle) == dict(
        run_id="identity", outcome="ADMITTED", reason=None)
    directory = tmp_path / "identity"
    assert admit_spider_run(directory)["decision"] == "ACCEPT"
    attempt_path, manifest_path = directory / "attempt.json", directory / "completion.json"
    attempt = cast(dict[str, Any], artifacts.decode_jsonl(attempt_path.read_bytes())[0])
    attempt["account_key"]["account"] = "OTHER"
    attempt_raw = artifacts.canonical_bytes(attempt) + b"\n"
    attempt_path.write_bytes(attempt_raw)
    manifest = cast(dict[str, Any], artifacts.decode_jsonl(manifest_path.read_bytes())[0])
    entry = next(row for row in cast(list[dict[str, Any]], manifest["artifacts"]) if row["path"] == "attempt.json")
    entry.update(sha256=sha256(attempt_raw).hexdigest(), byte_count=len(attempt_raw))
    manifest_path.write_bytes(artifacts.canonical_bytes(manifest) + b"\n")
    rejected = admit_spider_run(directory)
    assert rejected["decision"] == "REJECT"
    assert rejected["reason"] == "ENDPOINT_RECONCILIATION_FAILED"


@pytest.mark.parametrize("site", [
    (0, None), (1, None), (2, 0), (2, 19), (2, 39), (3, None), (4, None),
    (5, 0), (5, 5), (5, 11), (6, None),
])
@pytest.mark.parametrize("field", ["configuration_sha256", "products"])
def test_configured_context_mutation_at_every_artifact_position(configured_artifacts, site, field):
    values = deepcopy(configured_artifacts)
    attempt, status, journal, endpoint, reconciliation_row, report, completion_row = values
    supplied = [status, journal, endpoint, reconciliation_row, report, completion_row]
    artifact_index, row_index = site
    target = attempt if artifact_index == 0 else supplied[artifact_index - 1]
    if row_index is not None:
        target = target[row_index]
    if field == "configuration_sha256":
        target["configuration_context"][field] = "f" * 64
    else:
        target["configuration_context"][field].reverse()
    with pytest.raises(evidence.ReconciliationProjectionError):
        evidence._context_chain(attempt, *supplied)


def test_unrelated_programming_value_error_propagates(monkeypatch):
    def broken(_):
        raise ValueError("programming defect")
    monkeypatch.setattr(evidence, "_plan_bundle", broken)
    with pytest.raises(ValueError, match="^programming defect$") as error:
        evidence.build_reconciliation(RUN, *fixture())
    assert type(error.value) is ValueError


def test_context_chain_uses_attempt_mode_and_exact_ordered_context():
    context = artifacts.ConfigurationContext(
        "spider_configuration_context_v1", "configured-v1", "a" * 64,
        ("CFG-FIRST", "CFG-MIDDLE", "CFG-LAST"),
    )
    attempt = {"run_contract_id": "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1",
               "configuration_context": context}
    embedded = dict(schema_version=context.schema_version, config_id=context.config_id,
                    configuration_sha256=context.configuration_sha256, products=list(context.products))
    chain = [
        {"configuration_context": deepcopy(embedded)},
        [{"configuration_context": deepcopy(embedded)} for _ in range(3)],
        {"configuration_context": deepcopy(embedded)},
        {"configuration_context": deepcopy(embedded)},
        [{"configuration_context": deepcopy(embedded)} for _ in range(3)],
        {"configuration_context": deepcopy(embedded)},
    ]
    assert evidence._context_chain(attempt, *chain) == context
    for artifact_index, row_index in [(0, None), (1, 0), (1, 1), (1, 2), (2, None),
                                      (3, None), (4, 0), (4, 1), (4, 2), (5, None)]:
        supplied = deepcopy(chain)
        target = supplied[artifact_index] if row_index is None else supplied[artifact_index][row_index]
        del target["configuration_context"]
        with pytest.raises(evidence.ReconciliationProjectionError):
            evidence._context_chain(attempt, *supplied)
    positions = [(0, None), (1, 0), (1, 1), (1, 2), (2, None),
                 (3, None), (4, 0), (4, 1), (4, 2), (5, None)]
    for artifact_index, row_index in positions:
        for field, value in [("configuration_sha256", "b" * 64),
                             ("products", ["CFG-MIDDLE", "CFG-FIRST", "CFG-LAST"])]:
            supplied = deepcopy(chain)
            target = supplied[artifact_index] if row_index is None else supplied[artifact_index][row_index]
            target["configuration_context"][field] = value
            with pytest.raises(evidence.ReconciliationProjectionError):
                evidence._context_chain(attempt, *supplied)
        p1_chain = deepcopy(chain)
        for p1_index, p1_row in positions:
            p1_value = p1_chain[p1_index] if p1_row is None else p1_chain[p1_index][p1_row]
            p1_value.pop("configuration_context")
        p1_target = p1_chain[artifact_index] if row_index is None else p1_chain[artifact_index][row_index]
        p1_target["configuration_context"] = deepcopy(embedded)
        with pytest.raises(evidence.ReconciliationProjectionError):
            evidence._context_chain(
                {"run_contract_id": "SPIDER_SYNTHETIC_P1_RUN_V1"}, *p1_chain
            )


def test_context_chain_requires_exact_historical_pair_on_every_artifact_and_row():
    config_value = {
        "schema_version": "spider_configuration_context_v1", "config_id": "configured-v1",
        "configuration_sha256": "a" * 64, "products": ["CFG-FIRST", "CFG-MIDDLE", "CFG-LAST"],
    }
    historical_value = historical_context()
    config = artifacts.configuration_context(config_value)
    artifacts.historical_context(historical_value)
    attempt = {
        "run_contract_id": "SPIDER_HISTORICAL_RESEARCH_RUN_V1",
        "configuration_context": config_value, "historical_context": historical_value,
    }
    chain = [
        {"configuration_context": deepcopy(config_value), "historical_context": deepcopy(historical_value)},
        [{"configuration_context": deepcopy(config_value), "historical_context": deepcopy(historical_value)} for _ in range(2)],
    ]
    assert evidence._context_chain(attempt, *chain) == config
    for artifact_index, row_index in ((0, None), (1, 0), (1, 1)):
        for field, replacement in (
            ("historical_context", None),
            ("configuration_context", None),
            ("historical_context", {**historical_value, "path_pair_sha256": "f" * 64}),
        ):
            changed = deepcopy(chain)
            target = changed[artifact_index] if row_index is None else changed[artifact_index][row_index]
            if replacement is None:
                target.pop(field)
            else:
                target[field] = replacement
            with pytest.raises(ValueError):
                evidence._context_chain(attempt, *changed)


def test_no_reconciliation_authority_and_public_import_boundary():
    assert {name for name in vars(evidence) if not name.startswith("_")} == {"ReconciliationProjectionError", "build_reconciliation", "build_endpoint_artifacts"}
    with pytest.raises(TypeError):
        cast(Any, evidence.build_reconciliation)(RUN, *fixture(), reconciliation={"result": "OK"})
    tree = ast.parse(Path(evidence.__file__).read_text())
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    assert {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)} <= {
            "copy", "enum", "hashlib", "struct", "typing", "src.core.backtest.spider_run_artifacts", "src.core.backtest.spider_run_completion_schema",
        "src.core.backtest.spider_run_reconciliation_schema", "src.core.backtest.spider_scenario_plans", "src.core.backtest.spider_run_envelope_schema",
        "src.core.backtest.spider_configured_scale_input", "src.core.backtest.spider_policy_protocol"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))
