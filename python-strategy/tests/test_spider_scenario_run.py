"""Bounded orchestration proofs across the real store, scheduler and native owner."""

import ast
import multiprocessing
import os
import signal
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.core.backtest import spider_scenario_run as run, synthetic_scenario_codec as wire
from src.core.backtest import spider_configured_scale_input as scale
from src.core.backtest import spider_policy as spider_policy
from src.core.backtest import spider_run_store as storage
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_scenario_plans import PLAN_IDS, plan_bundle
from src.core.backtest.spider_historical_input import encode_historical_run_input, historical_planned_coverage
from test_spider_historical_input import _rehashed_run, _valid_run
from test_spider_historical_oracle import _case_answer, _enqueue_policy_order, _oracle_run

CONFIGURED_GROUP_VERSIONS = (
    ("G-501", 1), ("G-502", 2), ("G-503", 3), ("G-504", 4),
    ("G-505", 5), ("G-506", 6), ("G-507", 7), ("G-508", 8),
    ("G-509", 9), ("G-510", 10), ("G-511", 11), ("G-512", 12),
    ("G-513", 12), ("G-514", 13), ("G-515", 14), ("G-516", 15),
    ("G-518", 16), ("G-519", 17),
)


def read(root, run_id, name):
    return cast(list[dict[str, Any]], decode_jsonl((root / run_id / name).read_bytes()))


def invoke(root, index=0, name="r1"):
    return (run._run_o03 if index == 2 else run.run_spider_scenario)(str(root), name, PLAN_IDS[index])


def historical_input(name="p3-run", *, partial_fill=False):
    value = _valid_run()
    if partial_fill:
        trade_rows = tuple(
            replace(row, high=Decimal("103"), low=Decimal("98"), volume=Decimal("0.1"))
            if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == value.range_start_ms else row
            for row in value.trade_bars
        )
        value = _rehashed_run(value, trade_rows=trade_rows)
    value = replace(value, run_id=name, policy_source_sha256=run._P3_POLICY_SOURCE)
    return value, encode_historical_run_input(value)


def frozen_h03_input(run_id):
    source, value = _oracle_run("H03")
    answer = _case_answer("H03")
    assert source["policy"].endswith(run._P3_POLICY_SOURCE)
    assert answer["S1"] == "E+40000_crossing_before_accept,no_fill"
    assert answer["S2"] == "acceptE+40001,no_backfill"
    assert answer["S3"].startswith("E+100000_fillLONG1@95,fee0.095,cash999.905")
    assert answer["endpoint"].startswith("E+100001,before_noticeE+100002")
    value = replace(value, run_id=run_id)
    return value, encode_historical_run_input(value)


def queue_frozen_h03_order(composition, value):
    start = value.range_start_ms
    return _enqueue_policy_order(
        composition,
        start + 40_000,
        "send",
        dict(
            instId="A-USDT-SWAP",
            side="buy",
            ordType="limit",
            clOrdId="H03-ORDER-1",
            sz="1",
            px="95",
        ),
    )


def frozen_h03_factory(original):
    def with_frozen_order(historical_run):
        composition = original(historical_run)
        queue_frozen_h03_order(composition, historical_run)
        return composition

    return with_frozen_order


def install_frozen_h03_order(monkeypatch):
    monkeypatch.setattr(
        run._ReplayComposition,
        "_from_historical_run",
        staticmethod(frozen_h03_factory(run._ReplayComposition._from_historical_run)),
    )


def _kill_child_after_filled_market_marker(output_root, run_id, raw, connection):
    original_append = storage.SpiderRunStore.append_journal
    original_factory = run._ReplayComposition._from_historical_run
    run._ReplayComposition._from_historical_run = staticmethod(frozen_h03_factory(original_factory))
    def block_before_append(store, row):
        fills = [fill for product in row.get("payload", {}).get("result", {}).get("products", [])
                 for fill in product.get("fills", [])]
        if row["record_kind"] == "HISTORICAL_MARKET_RESULT" and fills:
            connection.send(("MARKED", row["barrier_id"], row["journal_seq"], len(fills)))
            connection.recv()
        return original_append(store, row)
    storage.SpiderRunStore.append_journal = block_before_append
    try:
        result = run.run_spider_scenario(output_root, run_id, run._P3_SELECTOR, raw)
        connection.send(("RETURNED", result))
    finally:
        storage.SpiderRunStore.append_journal = original_append
        run._ReplayComposition._from_historical_run = staticmethod(original_factory)
        connection.close()


@pytest.mark.parametrize("index", range(3))
def test_real_runs_registration_order_exact_artifacts_and_one_admission(tmp_path, index, monkeypatch):
    constructed, admitted = [], []
    original, admission = run._ReplayComposition, run._admit
    def owner(*args, **kwargs):
        assert read(tmp_path, "r1", "status.json")[0]["state"] == "RUNNING"
        assert (tmp_path / "r1/journal.jsonl").read_bytes() == b""
        constructed.append(args)
        return original(*args, **kwargs)
    def admit(path):
        assert (path / "completion.json").exists()
        admitted.append(path)
        return admission(path)
    monkeypatch.setattr(run, "_ReplayComposition", owner)
    monkeypatch.setattr(run, "_admit", admit)
    result = invoke(tmp_path, index)
    assert result == dict(run_id="r1", outcome="ADMITTED", reason=None)
    assert len(constructed) == len(admitted) == 1
    frozen = cast(dict[str, Any], plan_bundle(PLAN_IDS[index]))
    for name in ("journal", "endpoint", "report"):
        expected = frozen[name] if name != "endpoint" else [frozen[name]]
        for row in expected:
            row["run_id"] = "r1"
        assert read(tmp_path, "r1", name + (".json" if name == "endpoint" else ".jsonl")) == expected
    attempt = read(tmp_path, "r1", "attempt.json")[0]
    assert attempt["native_artifact_sha256"] == sha256(Path(wire.loaded_native_artifact_path()).read_bytes()).hexdigest()
    records = [(name.encode(), sha256((run._ROOT / name).read_bytes()).digest()) for name in sorted(run._PROGRAM)]
    assert len(records) == len(set(run._PROGRAM)) == 17
    assert attempt["program_sha256"] == sha256(b"".join(len(name).to_bytes(8, "big") + name + digest for name, digest in records)).hexdigest()


@pytest.mark.parametrize("field,value", [(0, ""), (0, "bad\0root"), (0, None), (1, "../bad"), (1, True), (2, "bad selector"), (2, 1)])
def test_invalid_transport_never_creates(tmp_path, field, value):
    args = [str(tmp_path), "r1", PLAN_IDS[0]]
    args[field] = value
    with pytest.raises(ValueError, match="^INVALID_INVOCATION$"):
        run.run_spider_scenario(*args)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("partial_fill", [False, True])
def test_public_historical_run_uses_existing_store_and_admission(tmp_path, partial_fill):
    value, raw = historical_input("historical-fill" if partial_fill else "historical-flat",
                                  partial_fill=partial_fill)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="ADMITTED", reason=None)
    admitted = admit_spider_run(tmp_path / value.run_id)
    assert admitted["decision"] == "ACCEPT"
    artifacts = admitted["artifacts"]
    assert artifacts["historical_input.json"]["run_id"] == value.run_id
    assert artifacts["attempt.json"]["run_contract_id"] == run._P3_RUN_CONTRACT
    assert artifacts["completion.json"]["state"] == "COMPLETE"
    market_rows = [row for row in artifacts["journal.jsonl"] if row["record_kind"] == "HISTORICAL_MARKET_RESULT"]
    fills = [fill for row in market_rows for product in row["payload"]["result"]["products"]
             for fill in product["fills"]]
    assert bool(fills) is partial_fill
    assert artifacts["report.jsonl"]
    assert (tmp_path / value.run_id / "historical_input.json").read_bytes() == raw


@pytest.mark.parametrize("variant,expected_margin,expected_available,expected_actions", [
    ("A", Decimal("1200"), Decimal("8800"), "global"),
    ("B", Decimal("900"), Decimal("9100"), "individual"),
    ("hold_boundary", Decimal("800"), Decimal("9200"), "none"),
    ("elapsed_boundary", Decimal("900"), Decimal("9100"), "none"),
])
def test_h06_frozen_policy_limits_run_through_public_admission(
    tmp_path, monkeypatch, variant, expected_margin, expected_available, expected_actions,
):
    source, value = _oracle_run("H06", h06_variant=variant)
    answer = _case_answer("H06")
    assert source["config"] == (
        "P3_ORACLE_CONFIG_BASE_V1:sha256:"
        "e4198c375c41c8c3e9b0a6d8aa7f836f4f5545e8f2b5abc712851c3b556009dd"
    )
    assert answer["check"] == "E+5020_after_failed_OPEN_ORDERS,cache_stale,owner_unchanged,childrenE+5021_UNSUBMITTED"
    assert all(
        tuple(str(getattr(spec, field)) for field in
              ("contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity"))
        == ("1", "1", "1", "1", "1")
        for spec in (*value.spec_before, *value.spec_after)
    )
    coverage = historical_planned_coverage(value)
    assert [row["barrier_id"] for row in coverage if row["record_kind"] == "HISTORICAL_MARKET_STEP"] == [
        f"P3_MARKET_{value.range_start_ms}_0",
    ]
    assert not any(row["barrier_id"] in {
        f"P3_MARKET_{value.range_start_ms}_1",
        f"P3_MARKET_{value.range_start_ms}_2",
        f"P3_MARKET_{value.range_start_ms}_3",
        f"MARKET_CLOSE_{value.range_start_ms + 60_000}",
    } for row in coverage)

    compare_calls = []
    original_compare = spider_policy.Policy.compare_reply
    monkeypatch.setattr(
        spider_policy.Policy, "compare_reply",
        lambda policy: (compare_calls.append(True), original_compare(policy))[1],
    )
    raw = encode_historical_run_input(replace(value, run_id=f"h06-{variant}"))
    value = replace(value, run_id=f"h06-{variant}")
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="ADMITTED", reason=None)
    admitted = admit_spider_run(tmp_path / value.run_id)
    assert admitted["decision"] == "ACCEPT"
    artifacts = admitted["artifacts"]
    endpoint = artifacts["endpoint.json"]
    report = artifacts["report.jsonl"]
    journal = artifacts["journal.jsonl"]
    assert artifacts["reconciliation.json"]["result"] == "OK"
    assert not compare_calls
    assert not [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    assert not [row for row in journal if row["record_kind"] == "HISTORICAL_MARKET_RESULT"
                and any(product["fills"] for product in row["payload"]["result"]["products"])]

    initial = endpoint["initial_owner_evidence"]
    final = endpoint["final_owner_evidence"]
    observation_metadata = {"account_version", "owner_state_digest", "valuation_context_id"}
    assert {key: value for key, value in initial["inspection"].items()
            if key not in observation_metadata} == {
                key: value for key, value in final["inspection"].items()
                if key not in observation_metadata
            }
    assert initial["inspection"]["account_version"] == 0
    assert final["inspection"]["account_version"] == 1
    assert Decimal(final["inspection"]["cash"]) == Decimal("10000")
    assert Decimal(final["inspection"]["total_fees"]) == Decimal("0")
    assert Decimal(final["inspection"]["gross_realized"]) == Decimal("0")
    trading = final["trading_fact"]["immutable_payload"]
    assert Decimal(trading["equity"]) == Decimal("10000")
    assert Decimal(trading["available_equity"]) == expected_available
    assert Decimal("10000") - Decimal(trading["available_equity"]) == expected_margin
    assert final["open_orders_fact"]["immutable_payload"]["rows"] == []
    assert all(row["open_orders"] == [] for row in report)
    assert all(Decimal(row["account_cash"]) == Decimal("10000")
               and Decimal(row["account_equity"]) == Decimal("10000")
               and Decimal(row["account_available_equity"]) == expected_available
               and Decimal(row["account_total_fees"]) == Decimal("0")
               and Decimal(row["account_gross_realized"]) == Decimal("0")
               for row in report)
    assert endpoint["scheduler_observation"]["polls"] == [dict(
        poll_id=f"P3_POLL_{value.range_start_ms + 5_000}",
        continuation_id=f"P3_CONT_{value.range_start_ms + 5_000}",
        status="COMPLETED", awaiting=None,
    )]
    assert any(row["record_kind"] == "SNAPSHOT_FACT"
               and row["payload"]["request"]["snapshot_kind"] == "OPEN_ORDERS"
               and row["payload"]["fact"]["immutable_payload"] ==
               dict(outcome="FAILURE", reason="SYNTHETIC_FAILURE") for row in journal)

    callbacks = [row["payload"] for row in journal if row["record_kind"] == "CALLBACK_RESULT"]
    callback = next(row for row in callbacks if row["policy_events"])
    events = callback["policy_events"]
    assert all(event["at_ms"] == value.range_start_ms + 5_020 for event in events)
    summary = []
    for event in events:
        if event["kind"] in ("send", "cancel"):
            summary.append((event["kind"], tuple(
                (order.get("instId"), order.get("side"), order.get("ordType"),
                 order.get("px"), order.get("sz"), order.get("ordId"))
                for order in event["orders"]
            )))
        elif event["kind"] == "alert":
            summary.append(("alert", event["reason"]))
        else:
            summary.append((event["kind"],))
    if expected_actions == "global":
        assert summary == [
            ("check_websocket",), ("request_market",),
            ("send", (("A-USDT-SWAP", "sell", "market", "", "55", None),
                      ("B-USDT-SWAP", "sell", "market", "", "15", None))),
            ("cancel", (("A-USDT-SWAP", None, None, None, None, "H06-GRID-A-1"),
                        ("A-USDT-SWAP", None, None, None, None, "H06-GRID-A-2"))),
            ("send", (("A-USDT-SWAP", "buy", "limit", "50", "46", None),
                      ("A-USDT-SWAP", "sell", "limit", "200", "35", None))),
            ("cancel", (("B-USDT-SWAP", None, None, None, None, "H06-GRID-B-1"),
                        ("B-USDT-SWAP", None, None, None, None, "H06-GRID-B-2"))),
            ("send", (("B-USDT-SWAP", "buy", "limit", "50", "46", None),
                      ("B-USDT-SWAP", "sell", "limit", "200", "35", None))),
            ("alert", "total_limit"),
        ]
    elif expected_actions == "individual":
        assert summary == [
            ("check_websocket",), ("request_market",),
            ("send", (("A-USDT-SWAP", "buy", "market", "", "115", None),)),
            ("cancel", (("A-USDT-SWAP", None, None, None, None, "H06-GRID-A-1"),
                        ("A-USDT-SWAP", None, None, None, None, "H06-GRID-A-2"))),
            ("send", (("A-USDT-SWAP", "buy", "limit", "50", "46", None),
                      ("A-USDT-SWAP", "sell", "limit", "200", "35", None))),
            ("alert", "individual_limit"),
        ]
    else:
        assert summary == [("check_websocket",)]

    deferred = endpoint["scheduler_observation"]["callback_actions"]
    if expected_actions == "none":
        assert deferred == []
    else:
        assert len(deferred) == (10 if expected_actions == "global" else 5)
        assert all(action["status"] == "UNSUBMITTED" and action["group_id"] is None
                   and action["effective_at"] == (value.range_start_ms + 5_021) * 16 + 3
                   for action in deferred)
        assert artifacts["reconciliation.json"]["checks"][8]["expected"] == [
            {"delivery_id": deferred_action["delivery_id"], "event_index": deferred_action["event_index"],
             "action_index": deferred_action["action_index"], "group_id": None, "status": "UNSUBMITTED",
             "cancel_effect_ref": None, "group_result_ref": None,
             "effective_at": (value.range_start_ms + 5_021) * 16 + 3}
            for deferred_action in deferred
        ]


@pytest.mark.parametrize("mutation", [
    "missing_effective_at", "wrong_effective_at", "due_effective_at", "parent_failed",
    "delete_one_observation_action", "delete_all_observation_actions", "mark_deferred_submitted",
])
def test_h06_endpoint_reconciliation_rejects_invalid_deferred_action_evidence(tmp_path, mutation):
    _, value = _oracle_run("H06", h06_variant="A")
    value = replace(value, run_id="h06-deferred-mutation")
    raw = encode_historical_run_input(value)
    assert run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw) == dict(
        run_id=value.run_id, outcome="ADMITTED", reason=None,
    )
    admitted = admit_spider_run(tmp_path / value.run_id)
    artifacts = admitted["artifacts"]
    endpoint = deepcopy(artifacts["endpoint.json"])
    observation_actions = endpoint["scheduler_observation"]["callback_actions"]
    action = observation_actions[0]
    if mutation == "missing_effective_at":
        del action["effective_at"]
    elif mutation == "wrong_effective_at":
        action["effective_at"] += 1
    elif mutation == "due_effective_at":
        action["effective_at"] = endpoint["cutoff"]["scheduler_time"]
    elif mutation == "parent_failed":
        parent = next(row for row in endpoint["scheduler_observation"]["records"]
                      if row["kind"] == "DELIVERY" and row["stable_id"] == action["delivery_id"])
        parent["classification"] = "TERMINAL"
    elif mutation == "delete_one_observation_action":
        del observation_actions[0]
    elif mutation == "delete_all_observation_actions":
        observation_actions.clear()
    else:
        action["status"] = "SUBMITTED"
        action.pop("effective_at")
    from src.core.backtest.spider_run_evidence import build_reconciliation
    reconciliation = build_reconciliation(
        value.run_id, artifacts["attempt.json"], artifacts["status.json"],
        artifacts["journal.jsonl"], endpoint, artifacts["report.jsonl"],
    )
    assert reconciliation["result"] == "FAILED"
    assert reconciliation["checks"][8]["result"] == "FAILED"


@pytest.mark.parametrize("case", ["missing", "noncanonical", "run_id", "selector", "policy_source", "p1_bytes"])
def test_historical_input_rejections_precede_store_and_owner(tmp_path, monkeypatch, case):
    value, raw = historical_input("historical-invalid")
    selector, supplied = run._P3_SELECTOR, raw
    if case == "missing":
        supplied = None
    elif case == "noncanonical":
        supplied = raw + b"\n"
    elif case == "run_id":
        other, supplied = historical_input("other-run")
        assert other.run_id != value.run_id
    elif case == "selector":
        selector = PLAN_IDS[0]
    elif case == "policy_source":
        mismatched = replace(_valid_run(), run_id=value.run_id)
        supplied = encode_historical_run_input(mismatched)
    else:
        selector = PLAN_IDS[0]
    store_calls = []
    monkeypatch.setattr(run._Store, "create", lambda *args, **kwargs: store_calls.append(args))
    monkeypatch.setattr(run, "_ReplayComposition", SimpleNamespace(
        _from_historical_run=lambda *_: pytest.fail("historical owner constructed before admission"),
    ))
    result = run.run_spider_scenario(str(tmp_path), value.run_id, selector, supplied)
    assert result == dict(run_id=value.run_id, outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION")
    assert not store_calls and list(tmp_path.iterdir()) == []


def test_historical_persistence_failure_is_terminal_and_unadmitted(tmp_path, monkeypatch):
    value, raw = historical_input("historical-write-failure")
    calls = []
    def fail_append(self, row):
        calls.append(row)
        raise storage.SpiderRunStoreError(
            "ARTIFACT_WRITE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"),
        )
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail_append)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="FAILED", reason="PERSISTENCE_FAILED")
    assert len(calls) == 1
    assert read(tmp_path, value.run_id, "status.json")[0]["failure_reason"] == "PERSISTENCE_FAILED"
    assert not (tmp_path / value.run_id / "completion.json").exists()


def test_historical_processed_marker_publication_failure_stops_before_append(tmp_path, monkeypatch):
    value, raw = historical_input("h11-marker-publication-failure")
    original = storage.SpiderRunStore._publish
    failures = []

    def fail_first_processed_status(store, name, data):
        if name == "status.json" and store._processed is not None and not failures:
            failures.append(store._processed.copy())
            raise storage.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original(store, name, data)

    monkeypatch.setattr(storage.SpiderRunStore, "_publish", fail_first_processed_status)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="FAILED", reason="PERSISTENCE_FAILED")
    directory = tmp_path / value.run_id
    status = read(tmp_path, value.run_id, "status.json")[0]
    journal = read(tmp_path, value.run_id, "journal.jsonl")
    assert len(failures) == 1
    assert status["state"] == "FAILED" and status["failure_reason"] == "PERSISTENCE_FAILED"
    assert status["processed_boundary"] == failures[0]
    assert status["persisted_boundary"] is None and journal == []
    assert not (directory / "completion.json").exists()
    assert admit_spider_run(directory)["reason"] == "INCOMPLETE_PERSISTENCE"


@pytest.mark.parametrize("capture_site", ["SOURCE_GROUP_RESULT", "HISTORICAL_MARKET_RESULT"])
def test_historical_post_mutation_capture_failure_preserves_unpersisted_frontier(
    tmp_path, monkeypatch, capture_site,
):
    value, raw = historical_input(f"capture-failure-{capture_site.lower()}",
                                  partial_fill=capture_site == "HISTORICAL_MARKET_RESULT")
    original_capture = run._ReplayComposition.capture_owner_evidence
    original_group = wire.ScenarioCodec.apply_group
    original_market = wire.ScenarioCodec.historical_market_step
    mutation = []
    fail_market_capture = [False]
    failed_captures = []

    def capture(owner, cutoff, *requests):
        snapshot_id = requests[0]["snapshot_id"]
        matches = (capture_site == "SOURCE_GROUP_RESULT" and snapshot_id.startswith("GROUP-")
                   or capture_site == "HISTORICAL_MARKET_RESULT" and snapshot_id.startswith("MARKET-")
                   and fail_market_capture[0])
        if matches:
            failed_captures.append(snapshot_id)
            raise RuntimeError("injected post-mutation owner capture failure")
        return original_capture(owner, cutoff, *requests)

    def apply_group(codec, request):
        before = codec.inspect_state()["account_version"]
        result = original_group(codec, request)
        if result["classification"] == "COMMITTED":
            mutation.append(("SOURCE_GROUP_RESULT", before, result["account_version_after"]))
        return result

    def market_step(codec, node):
        before = codec.inspect_state()["account_version"]
        result = original_market(codec, node)
        fills = sum(len(product["fills"]) for product in result["products"])
        after = result["owner_evidence"]["account_version"]
        if fills and after > before:
            mutation.append(("HISTORICAL_MARKET_RESULT", before, after, fills))
            fail_market_capture[0] = True
        return result

    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", capture)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", apply_group)
    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", market_step)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="FAILED", reason="PERSISTENCE_FAILED")
    changed = next(row for row in mutation if row[0] == capture_site)
    assert changed[2] > changed[1]
    if capture_site == "HISTORICAL_MARKET_RESULT":
        assert changed[3] > 0
    assert len(failed_captures) == 1

    directory = tmp_path / value.run_id
    status = read(tmp_path, value.run_id, "status.json")[0]
    journal = read(tmp_path, value.run_id, "journal.jsonl")
    processed, persisted = status["processed_boundary"], status["persisted_boundary"]
    assert status["state"] == "FAILED" and status["failure_reason"] == "PERSISTENCE_FAILED"
    assert processed is not None and processed["ordinal"] == len(journal) + 1
    assert (persisted["ordinal"] if persisted is not None else 0) == len(journal)
    assert not any(row["barrier_id"] == processed["barrier_id"] for row in journal)
    if capture_site == "HISTORICAL_MARKET_RESULT":
        assert processed["barrier_id"].startswith("P3_MARKET_")
    assert not (directory / "completion.json").exists()
    assert admit_spider_run(directory)["reason"] == "INCOMPLETE_PERSISTENCE"


def test_historical_sigkill_after_durable_market_marker_then_clean_new_run(tmp_path, monkeypatch):
    failed_run, failed_input = frozen_h03_input("h11-killed-run")
    baseline_run = replace(failed_run, run_id="h11-baseline-run")
    baseline_input = encode_historical_run_input(baseline_run)
    install_frozen_h03_order(monkeypatch)
    baseline_result = run.run_spider_scenario(
        str(tmp_path), baseline_run.run_id, run._P3_SELECTOR, baseline_input,
    )
    assert baseline_result == dict(run_id=baseline_run.run_id, outcome="ADMITTED", reason=None)
    baseline_admission = admit_spider_run(tmp_path / baseline_run.run_id)
    assert baseline_admission["decision"] == "ACCEPT"

    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_kill_child_after_filled_market_marker,
        args=(str(tmp_path), failed_run.run_id, failed_input, child),
    )
    process.start()
    child.close()
    try:
        assert parent.poll(60), "child never reached post-marker, pre-journal append window"
        message = parent.recv()
        assert message[0] == "MARKED", message
        barrier_id, ordinal, fills = message[1:]
        assert fills > 0

        failed_directory = tmp_path / failed_run.run_id
        status = read(tmp_path, failed_run.run_id, "status.json")[0]
        journal = read(tmp_path, failed_run.run_id, "journal.jsonl")
        processed, persisted = status["processed_boundary"], status["persisted_boundary"]
        assert status["state"] == "RUNNING"
        assert processed == dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=barrier_id)
        assert processed["ordinal"] == len(journal) + 1
        assert (persisted["ordinal"] if persisted is not None else 0) == len(journal)
        assert not any(row["barrier_id"] == barrier_id for row in journal)
        assert not (failed_directory / "completion.json").exists()

        os.kill(process.pid, signal.SIGKILL)
        process.join(10)
        assert process.exitcode == -signal.SIGKILL
        assert admit_spider_run(failed_directory) == dict(
            decision="REJECT", reason="INCOMPLETE_PERSISTENCE",
            evidence=("completion.json", "status.json", "journal.jsonl"),
        )
        killed_bytes = {path.name: path.read_bytes() for path in failed_directory.iterdir() if path.is_file()}

        clean_run = replace(failed_run, run_id="h11-clean-run")
        clean_input = encode_historical_run_input(clean_run)
        assert run._historical_configuration(failed_input, failed_run.run_id)[2][0] == run._historical_configuration(
            clean_input, clean_run.run_id,
        )[2][0]
        clean_result = run.run_spider_scenario(str(tmp_path), clean_run.run_id, run._P3_SELECTOR, clean_input)
        assert clean_result == dict(run_id=clean_run.run_id, outcome="ADMITTED", reason=None)
        clean_admission = admit_spider_run(tmp_path / clean_run.run_id)
        assert clean_admission["decision"] == "ACCEPT"
        failed_after = {path.name: path.read_bytes() for path in failed_directory.iterdir() if path.is_file()}
        assert failed_after == killed_bytes

        failed_attempt = read(tmp_path, failed_run.run_id, "attempt.json")[0]
        clean_attempt = read(tmp_path, clean_run.run_id, "attempt.json")[0]
        assert failed_attempt["historical_context"] == clean_attempt["historical_context"]
        assert failed_attempt["input_contract_hashes"] == clean_attempt["input_contract_hashes"]

        def normalize(value):
            if isinstance(value, dict):
                return {key: "<run-id>" if key == "run_id" else normalize(item)
                        for key, item in value.items()}
            if isinstance(value, list):
                return [normalize(item) for item in value]
            return value

        for name in ("journal.jsonl", "endpoint.json", "report.jsonl"):
            assert normalize(baseline_admission["artifacts"][name]) == normalize(
                clean_admission["artifacts"][name],
            )
        baseline_attempt = baseline_admission["artifacts"]["attempt.json"]
        assert baseline_attempt["historical_context"] == clean_attempt["historical_context"]
        assert baseline_attempt["input_contract_hashes"] == clean_attempt["input_contract_hashes"]
    finally:
        if process.is_alive():
            os.kill(process.pid, signal.SIGKILL)
            process.join(10)
        parent.close()


def test_historical_filled_market_append_failure_keeps_native_mutation_and_unpersisted_marker(
    tmp_path, monkeypatch,
):
    value, raw = frozen_h03_input("h11-append-failure")
    install_frozen_h03_order(monkeypatch)
    original = storage.SpiderRunStore.append_journal
    original_market = wire.ScenarioCodec.historical_market_step
    failed_rows = []
    mutated_owners = []

    def record_filled_market(codec, node):
        result = original_market(codec, node)
        if any(product["fills"] for product in result["products"]):
            mutated_owners.append((codec, deepcopy(result["owner_evidence"])))
        return result

    def fail_filled_market_append(store, row):
        fills = [fill for product in row.get("payload", {}).get("result", {}).get("products", [])
                 for fill in product.get("fills", [])]
        if row["record_kind"] == "HISTORICAL_MARKET_RESULT" and fills:
            failed_rows.append(row)
            raise storage.SpiderRunStoreError(
                "ARTIFACT_WRITE_FAILED", "PERSISTENCE_FAILED",
                dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"),
            )
        return original(store, row)

    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail_filled_market_append)
    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", record_filled_market)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="FAILED", reason="PERSISTENCE_FAILED")
    assert len(failed_rows) == 1
    row = failed_rows[0]
    assert any(product["fills"] for product in row["payload"]["result"]["products"])
    assert row["payload"]["owner_inspection_after"]["account_version"] > row["payload"]["owner_inspection_before"]["account_version"]
    assert mutated_owners and mutated_owners[-1][0].inspect_state() == mutated_owners[-1][1]
    directory = tmp_path / value.run_id
    status = read(tmp_path, value.run_id, "status.json")[0]
    journal = read(tmp_path, value.run_id, "journal.jsonl")
    assert status["state"] == "FAILED" and status["failure_reason"] == "PERSISTENCE_FAILED"
    assert status["processed_boundary"]["barrier_id"] == row["barrier_id"]
    assert status["processed_boundary"]["ordinal"] == len(journal) + 1
    assert (status["persisted_boundary"]["ordinal"] if status["persisted_boundary"] else 0) == len(journal)
    assert not any(item["barrier_id"] == row["barrier_id"] for item in journal)
    assert not (directory / "completion.json").exists()
    assert admit_spider_run(directory)["reason"] == "INCOMPLETE_PERSISTENCE"


def test_historical_endpoint_mutation_is_rejected_by_existing_admission(tmp_path, monkeypatch):
    value, raw = historical_input("historical-endpoint-mutation")
    original = run._admit
    def tamper(path):
        endpoint_path = path / "endpoint.json"
        endpoint = decode_jsonl(endpoint_path.read_bytes())[0]
        endpoint["terminal_reason"] = "SCHEDULED_MTM"
        endpoint_path.write_bytes(canonical_bytes(endpoint) + b"\n")
        return original(path)
    monkeypatch.setattr(run, "_admit", tamper)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="FAILED", reason="ARTIFACT_MISMATCH")


def test_historical_completed_bundle_rejects_one_byte_truncated_endpoint(tmp_path, monkeypatch):
    value, raw = frozen_h03_input("h11-truncated-endpoint")
    install_frozen_h03_order(monkeypatch)
    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=value.run_id, outcome="ADMITTED", reason=None)
    endpoint = tmp_path / value.run_id / "endpoint.json"
    original = endpoint.read_bytes()
    endpoint.write_bytes(original[:-1])
    rejected = admit_spider_run(tmp_path / value.run_id)
    assert rejected["decision"] == "REJECT"
    assert rejected["reason"] in ("ARTIFACT_MISMATCH", "UNSUPPORTED_SCHEMA", "INVALID_MANIFEST")


@pytest.mark.parametrize("selector", ["unknown", PLAN_IDS[2]])
def test_rejected_selector_constructs_no_owner(tmp_path, monkeypatch, selector):
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("no native owner"))
    assert run.run_spider_scenario(str(tmp_path), "r1", selector)["outcome"] == "REJECTED"
    attempt = read(tmp_path, "r1", "attempt.json")[0]
    assert attempt["registration_state"] == "REJECTED" and attempt["planned_coverage"] == []
    assert all(attempt[key] is None for key in run._IDENTITIES)
    assert read(tmp_path, "r1", "status.json")[0]["failure_reason"] == "UNSUPPORTED_CONFIGURATION"
    assert not (tmp_path / "r1/journal.jsonl").exists()


@pytest.mark.parametrize("fault", ["manifest", "program", "native", "hash", "recipe"])
def test_preowner_provenance_and_recipe_fail_closed(tmp_path, monkeypatch, fault):
    original_read, select = Path.read_bytes, run._plans.cli_plan_bundle
    def bytes_(path):
        if fault == "manifest" and path.name == "source_manifest.json":
            return b"wrong"
        if fault == "program" and path.name == "spider_policy.py":
            raise FileNotFoundError()
        return original_read(path)
    def bundle(selector):
        result = cast(dict[str, Any], select(selector))
        if fault == "hash":
            result["plan_sha256"] = "f" * 64
        if fault == "recipe":
            result["journal"] = []
        return result
    monkeypatch.setattr(Path, "read_bytes", bytes_)
    monkeypatch.setattr(run._plans, "cli_plan_bundle", bundle)
    if fault == "native":
        monkeypatch.setattr(wire, "loaded_native_artifact_path", lambda: "/no-such-spider-native")
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("no owner"))
    assert invoke(tmp_path) == dict(run_id="r1", outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION")


def test_f01_keeps_native_three_processed_three_persisted_two_and_new_run_clean(tmp_path, monkeypatch):
    append, apply = storage.SpiderRunStore.append_journal, wire.ScenarioCodec.apply_group
    observed = []
    def group(owner, request):
        result = apply(owner, request)
        observed.append((request["group_id"], owner.inspect_state()))
        return result
    def fail(store, row):
        if row["journal_seq"] == 3:
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        append(store, row)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", group)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail)
    assert invoke(tmp_path)["reason"] == "PERSISTENCE_FAILED"
    assert [name for name, _ in observed] == ["MIN-X1", "reject", "context"]
    assert observed[-1][1]["account_version"] == 2
    status = read(tmp_path, "r1", "status.json")[0]
    assert (status["processed_boundary"]["ordinal"], status["persisted_boundary"]["ordinal"]) == (3, 2)
    assert len(read(tmp_path, "r1", "journal.jsonl")) == 2
    assert admit_spider_run(tmp_path / "r1")["reason"] == "INCOMPLETE_PERSISTENCE"
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", append)
    assert invoke(tmp_path, name="r2")["outcome"] == "ADMITTED"
    assert len(read(tmp_path, "r1", "journal.jsonl")) == 2
    assert invoke(tmp_path, name="r2")["reason"] == "PUBLICATION_FAILED"


def test_callback_partial_prefix_persists_once_and_never_retries(tmp_path, monkeypatch):
    calls = []
    def callback(owner, delivery):
        calls.append(delivery["delivery_id"])
        owner._policy.emit("alert", reason="total_limit")
        raise RuntimeError("private callback detail")
    monkeypatch.setattr(run._ReplayComposition, "_apply_payload", callback)
    assert invoke(tmp_path)["reason"] == "CALLBACK_FAILED"
    rows = read(tmp_path, "r1", "journal.jsonl")
    assert len(rows) == 5 and len(calls) == 1
    assert rows[-1]["payload"]["policy_events"][0]["kind"] == "alert"
    assert rows[-1]["payload"]["outcome"] == "CALLBACK_FAILED"
    assert read(tmp_path, "r1", "status.json")[0]["primary_failure"] == dict(kind="CALLBACK", reason="CALLBACK_FAILED")
    assert not (tmp_path / "r1/completion.json").exists()


@pytest.mark.parametrize("phase", ["constructor", "initial", "after", "final"])
def test_native_failures_at_each_capture_boundary(tmp_path, monkeypatch, phase):
    codec = wire.ScenarioCodec("SYNTHETIC_MIN_CASH_V1", cast(wire.Account, dict(venue="okx-scenario", environment="test", account="A")))
    with pytest.raises(ValueError) as rejected:
        codec.capture_snapshot(cast(wire.SnapshotRequest, {}))
    error = rejected.value
    capture = run._ReplayComposition.capture_owner_evidence
    def fail(owner, cutoff, *requests):
        if cutoff == {"initial": 500, "after": 501, "final": 504}.get(phase):
            raise error
        return capture(owner, cutoff, *requests)
    def constructor(*args, **kwargs):
        raise error
    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", fail)
    if phase == "constructor":
        monkeypatch.setattr(run, "_ReplayComposition", constructor)
    assert invoke(tmp_path)["reason"] == ("PERSISTENCE_FAILED" if phase == "after" else "NATIVE_FAULT")
    status = read(tmp_path, "r1", "status.json")[0]
    if phase == "after":
        assert status["processed_boundary"]["ordinal"] == 1 and status["persisted_boundary"] is None
        assert status["primary_failure"] == dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED")


@pytest.mark.parametrize("kind,failure,expected", [("SOURCE_GROUP_RESULT", dict(failure="BROKEN"), "NATIVE"),
    ("CALLBACK_RESULT", dict(kind="CALLBACK", reason="CALLBACK_FAILED"), "CALLBACK"),
    ("CALLBACK_RESULT", dict(kind="PLAN", reason="POLICY_EMISSION_MISMATCH"), "SCHEDULER"),
    ("CALLBACK_RESULT", dict(kind="NATIVE", reason="BROKEN"), "NATIVE")])
def test_upstream_failure_precedence_over_append_failure(tmp_path, monkeypatch, kind, failure, expected):
    original = run._ReplayComposition._evidence
    def evidence(owner, actual_kind, key, payload):
        if actual_kind == kind:
            payload = deepcopy(payload)
            if kind == "SOURCE_GROUP_RESULT":
                payload["result"].update(classification="FAULT", **failure)
            else:
                payload["failure"] = failure
        return original(owner, actual_kind, key, payload)
    append = storage.SpiderRunStore.append_journal
    def fail(store, row):
        if row["record_kind"] == kind:
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        append(store, row)
    monkeypatch.setattr(run._ReplayComposition, "_evidence", evidence)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail)
    assert invoke(tmp_path)["reason"] == "PERSISTENCE_FAILED"
    primary = read(tmp_path, "r1", "status.json")[0]["primary_failure"]
    assert primary == dict(kind=expected, reason=failure.get("reason", "BROKEN"))


@pytest.mark.parametrize("publication", [False, True])
def test_unknown_after_error_propagates_original_unless_failure_publication_fails(tmp_path, monkeypatch, publication):
    error = RuntimeError("private programming detail")
    capture = run._ReplayComposition.capture_owner_evidence
    def fail(owner, cutoff, *requests):
        if cutoff == 501:
            raise error
        return capture(owner, cutoff, *requests)
    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", fail)
    if publication:
        def publish(*args):
            raise storage.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN", "UNEXPECTED_EXCEPTION")
        monkeypatch.setattr(storage.SpiderRunStore, "publish_failure", publish)
        assert invoke(tmp_path) == dict(run_id="r1", outcome="DURABILITY_UNKNOWN", reason="PUBLICATION_DURABILITY_UNKNOWN")
    else:
        with pytest.raises(RuntimeError) as caught:
            invoke(tmp_path)
        assert caught.value is error
        status = read(tmp_path, "r1", "status.json")[0]
        assert status["failure_reason"] == "UNEXPECTED_EXCEPTION" and status["primary_failure"] is None
        assert status["processed_boundary"]["ordinal"] == 1


@pytest.mark.parametrize("outcome", ["reject", "frozen_error", "finalize_error"])
def test_completion_admission_and_publication_mapping(tmp_path, monkeypatch, outcome):
    finalize = storage.SpiderRunStore.finalize
    def fail(store, **artifacts):
        if outcome == "frozen_error":
            finalize(store, **artifacts)
        raise storage.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN" if outcome == "frozen_error" else "PUBLICATION_FAILED")
    if outcome == "reject":
        monkeypatch.setattr(run, "_admit", lambda _: dict(decision="REJECT", reason="ARTIFACT_MISMATCH"))
    else:
        monkeypatch.setattr(storage.SpiderRunStore, "finalize", fail)
    result = invoke(tmp_path)
    assert result["reason"] == {"reject": "ARTIFACT_MISMATCH", "frozen_error": "PUBLICATION_DURABILITY_UNKNOWN", "finalize_error": "PUBLICATION_FAILED"}[outcome]
    assert read(tmp_path, "r1", "status.json")[0]["state"] == ("FAILED" if outcome == "finalize_error" else "COMPLETE")


@pytest.mark.parametrize("direct,child,valid", [("/x/core.so", None, True), ("/x/__init__.py", "/x/child.so", True),
    ("/x/__init__.py", None, False), (None, "/x/child.so", False), ("/x/wrapper.py", "/x/child.so", False),
    ("/x/__init__.py", 1, False), ("/x/__init__.py", "/x/child.py", False), (1, None, False)])
def test_codec_loaded_binary_path_only(direct, child, valid, monkeypatch):
    monkeypatch.setattr(wire, "_native", SimpleNamespace(__file__=direct, fluxtrade_core=SimpleNamespace(__file__=child)))
    if valid:
        assert wire.loaded_native_artifact_path() == (direct if direct.endswith(".so") else child)
    else:
        with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
            wire.loaded_native_artifact_path()


def test_surface_private_o03_and_no_native_access():
    assert {name for name in vars(run) if not name.startswith("_")} == {"run_spider_scenario"}
    with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
        run._run_o03("unused", "r1", PLAN_IDS[0])
    tree = ast.parse(Path(run.__file__).read_text())
    assert not any(isinstance(node, ast.Attribute) and node.attr == "_native" for node in ast.walk(tree))
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))


@pytest.mark.parametrize("result,reason,kind", [
    (dict(reason="NATIVE_DETAIL", native_failure=dict(reason="NATIVE_DETAIL")), "NATIVE_FAULT", "NATIVE"),
    (dict(reason="FAULT_DETAIL", group_result=dict(classification="FAULT")), "NATIVE_FAULT", "NATIVE"),
    (dict(reason="CALLBACK_FAILED"), "CALLBACK_FAILED", "CALLBACK"),
    (dict(reason="INVALID_SCHEMA"), "SCHEDULER_FAILED", "SCHEDULER"),
])
def test_terminal_mapping_has_no_poison_inference(tmp_path, monkeypatch, result, reason, kind):
    monkeypatch.setattr(run._ReplayComposition, "_dispatch_due", lambda *_: dict(classification="TERMINAL", **result))
    assert invoke(tmp_path)["reason"] == reason
    status = read(tmp_path, "r1", "status.json")[0]
    assert status["primary_failure"] == dict(kind=kind, reason=result["reason"])


@pytest.mark.parametrize("operation", ["create", "register"])
def test_registration_failure_prevents_owner(tmp_path, monkeypatch, operation):
    def fail(*args):
        raise storage.SpiderRunStoreError("PUBLICATION_FAILED")
    monkeypatch.setattr(storage.SpiderRunStore, operation, fail)
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("no owner"))
    assert invoke(tmp_path)["reason"] == "PUBLICATION_FAILED"


@pytest.mark.parametrize("upstream", [False, True])
def test_after_capture_failure_primary_and_failed_publication_precedence(tmp_path, monkeypatch, upstream):
    owner = wire.ScenarioCodec("SYNTHETIC_MIN_CASH_V1", cast(wire.Account, dict(venue="okx-scenario", environment="test", account="A")))
    with pytest.raises(ValueError) as rejected:
        owner.capture_snapshot(cast(wire.SnapshotRequest, {}))
    capture, emit = run._ReplayComposition.capture_owner_evidence, run._ReplayComposition._evidence
    def evidence(owner, kind, key, payload):
        if upstream and kind == "SOURCE_GROUP_RESULT":
            payload["result"].update(classification="FAULT", failure="ORIGINAL_NATIVE_FAULT")
        return emit(owner, kind, key, payload)
    def fail(owner, cutoff, *requests):
        if cutoff == 501:
            raise rejected.value
        return capture(owner, cutoff, *requests)
    published = []
    def publication(store, reason, primary):
        published.append((reason, primary))
        raise storage.SpiderRunStoreError("PUBLICATION_FAILED", reason, primary)
    monkeypatch.setattr(run._ReplayComposition, "_evidence", evidence)
    monkeypatch.setattr(run._ReplayComposition, "capture_owner_evidence", fail)
    monkeypatch.setattr(storage.SpiderRunStore, "publish_failure", publication)
    assert invoke(tmp_path)["reason"] == "PUBLICATION_FAILED"
    primary = dict(kind="NATIVE", reason="ORIGINAL_NATIVE_FAULT") if upstream else dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED")
    assert published == [("PERSISTENCE_FAILED", primary)]


def test_projection_failure_is_bounded_but_missing_callback_is_not_retried(tmp_path, monkeypatch):
    def project(*args):
        raise run._ProjectionError()
    monkeypatch.setattr(run, "_build", project)
    assert invoke(tmp_path)["reason"] == "ENDPOINT_RECONCILIATION_FAILED"
    monkeypatch.setattr(run._ReplayComposition, "_dispatch_due", lambda *_: dict(classification="SUCCESS"))
    with pytest.raises(RuntimeError, match="^UNOBSERVED_BARRIER$"):
        invoke(tmp_path, name="r2")
    assert read(tmp_path, "r2", "status.json")[0]["failure_reason"] == "UNEXPECTED_EXCEPTION"


def _invoke_configured(root, run_id="p2", select=None):
    return run._invoke(str(root), run_id, "SPIDER_P2_CONFIGURED_SCALE_V1", select or run._plans.plan_bundle)


def test_configured_f01_append_failure_after_second_execution_has_exact_frontier(tmp_path, monkeypatch):
    attempted, append_attempts, finalize_calls, group_versions = [], [], [], []
    original_append = storage.SpiderRunStore.append_journal
    original_apply = wire.ScenarioCodec.apply_group
    def apply(codec, request):
        result = original_apply(codec, request)
        group_versions.append((request["group_id"], result["account_version_after"]))
        return result
    def fail_at_twenty(store, row):
        append_attempts.append(row["journal_seq"])
        if row["journal_seq"] == 20:
            attempted.append(deepcopy(row))
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED",
                                              dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        original_append(store, row)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail_at_twenty)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", apply)
    monkeypatch.setattr(storage.SpiderRunStore, "finalize", lambda *_args, **_kwargs: finalize_calls.append(True))
    monkeypatch.setattr(run, "_admit", lambda *_: pytest.fail("admission must not follow persistence failure"))

    result = _invoke_configured(tmp_path, "f01")
    assert result == dict(run_id="f01", outcome="FAILED", reason="PERSISTENCE_FAILED")
    rows = read(tmp_path, "f01", "journal.jsonl")
    status = read(tmp_path, "f01", "status.json")[0]
    assert [row["journal_seq"] for row in rows] == list(range(1, 20))
    plan = scale._configured_scale_plan_input()
    assert [(row["barrier_id"], row["record_kind"]) for row in rows] == [
        (row["barrier_id"], row["record_kind"]) for row in plan["planned_barriers"][:19]]
    assert append_attempts == list(range(1, 21))
    assert tuple(group_versions) == CONFIGURED_GROUP_VERSIONS
    assert len(attempted) == 1 and attempted[0]["journal_seq"] == 20
    assert attempted[0]["barrier_id"] == "SOURCE_GROUP:G-519"
    assert attempted[0]["payload"]["result"]["account_version_after"] == 17
    assert attempted[0]["payload"]["owner_evidence_after"]["inspection"]["account_version"] == 17
    assert (status["state"], status["failure_reason"], status["primary_failure"]) == (
        "FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
    assert (status["processed_boundary"]["ordinal"], status["persisted_boundary"]["ordinal"]) == (20, 19)
    assert not any((tmp_path / "f01" / name).exists() for name in (
        "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json"))
    assert finalize_calls == []
    directory = tmp_path / "f01"
    before = {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()}
    assert admit_spider_run(directory)["reason"] == "INCOMPLETE_PERSISTENCE"
    assert {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()} == before


def test_configured_f02_callback_prefix_persists_once_without_retry_or_resume(tmp_path, monkeypatch):
    calls, finalize_calls, owners, group_versions = [], [], [], []
    original_apply = run._ReplayComposition._apply_payload
    original_finalize, original_admit = storage.SpiderRunStore.finalize, run._admit
    original_group = wire.ScenarioCodec.apply_group
    def apply_group(codec, request):
        result = original_group(codec, request)
        group_versions.append((request["group_id"], result["account_version_after"]))
        return result
    def fail_after_prefix(owner, delivery):
        owners.append(owner)
        calls.append((delivery["delivery_id"], owner._codec.inspect_state()["account_version"]))
        owner._policy.emit("alert", reason="total_limit")
        raise RuntimeError("private callback detail")
    monkeypatch.setattr(run._ReplayComposition, "_apply_payload", fail_after_prefix)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", apply_group)
    monkeypatch.setattr(storage.SpiderRunStore, "finalize", lambda *_args, **_kwargs: finalize_calls.append(True))
    monkeypatch.setattr(run, "_admit", lambda *_: pytest.fail("admission must not follow callback failure"))

    result = _invoke_configured(tmp_path, "f02")
    rows = read(tmp_path, "f02", "journal.jsonl")
    status = read(tmp_path, "f02", "status.json")[0]
    assert result == dict(run_id="f02", outcome="FAILED", reason="CALLBACK_FAILED")
    plan = scale._configured_scale_plan_input()
    assert calls == [(plan["callback_plans"][0]["delivery_id"], 15)] and len(rows) == 18
    assert tuple(group_versions) == CONFIGURED_GROUP_VERSIONS[:16]
    assert len(owners) == 1 and owners[0]._codec.inspect_state()["account_version"] == 15
    assert [(row["journal_seq"], row["barrier_id"], row["record_kind"]) for row in rows] == [
        (row["ordinal"], row["barrier_id"], row["record_kind"]) for row in plan["planned_barriers"][:18]]
    callback = rows[-1]
    assert callback["record_kind"] == "CALLBACK_RESULT" and callback["barrier_id"].startswith("CALLBACK_RESULT:")
    assert callback["payload"] == dict(
        actions=[], delivery_id=plan["callback_plans"][0]["delivery_id"],
        failure=dict(kind="CALLBACK", reason="CALLBACK_FAILED"), outcome="CALLBACK_FAILED",
        policy_events=[dict(at_ms=100000, kind="alert", reason="total_limit")])
    assert (status["state"], status["failure_reason"], status["primary_failure"]) == (
        "FAILED", "CALLBACK_FAILED", dict(kind="CALLBACK", reason="CALLBACK_FAILED"))
    assert (status["processed_boundary"]["ordinal"], status["persisted_boundary"]["ordinal"]) == (18, 18)
    assert not any((tmp_path / "f02" / name).exists() for name in (
        "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json"))
    assert finalize_calls == []
    directory = tmp_path / "f02"
    before = {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()}
    assert admit_spider_run(directory)["reason"] == "MISSING_COMPLETE_MANIFEST"
    assert {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()} == before

    monkeypatch.setattr(run._ReplayComposition, "_apply_payload", original_apply)
    monkeypatch.setattr(storage.SpiderRunStore, "finalize", original_finalize)
    monkeypatch.setattr(run, "_admit", original_admit)
    assert run.run_spider_scenario(str(tmp_path), "clean-after-f02", "SPIDER_P2_CONFIGURED_SCALE_V1") == dict(
        run_id="clean-after-f02", outcome="ADMITTED", reason=None)


def test_configured_runner_projects_finalizes_and_admits_exact_forty_barriers(tmp_path, monkeypatch):
    owners, captures = [], []
    finalizes, admissions = [], []
    policy_calls = dict(orders=0, order_filled=0, raise_leverage=0, check_risk=0)
    original = run._ReplayComposition
    original_finalize = storage.SpiderRunStore.finalize
    original_admit = run._admit
    def finalize(store, **artifacts):
        finalizes.append(deepcopy(artifacts))
        return original_finalize(store, **artifacts)
    def admit(path):
        before = {item.name: item.read_bytes() for item in path.iterdir() if item.is_file()}
        result = original_admit(path)
        after = {item.name: item.read_bytes() for item in path.iterdir() if item.is_file()}
        admissions.append((result, before, after))
        return result
    monkeypatch.setattr(storage.SpiderRunStore, "finalize", finalize)
    monkeypatch.setattr(run, "_admit", admit)
    def observe(*args, **kwargs):
        attempt = read(tmp_path, "p2", "attempt.json")[0]
        assert read(tmp_path, "p2", "status.json")[0]["state"] == "RUNNING"
        assert (tmp_path / "p2/journal.jsonl").read_bytes() == b""
        assert attempt["registration_state"] == "VALIDATED"
        assert attempt["configuration_context"]["configuration_sha256"] == "807054044bdd182274509535ecf8bbc4598f00b6a92b598c6228129a6ff11b70"
        assert args[2] == {row["delivery_id"]: row for row in scale._configured_scale_plan_input()["callback_plans"]}
        assert kwargs["configuration"] == scale._configured_scale_plan_input()["configuration"]
        owner = original(*args, **kwargs)
        for name in policy_calls:
            method = getattr(owner._policy, name)
            def counted(*call_args, _method=method, _name=name, **call_kwargs):
                policy_calls[_name] += 1
                return _method(*call_args, **call_kwargs)
            setattr(owner._policy, name, counted)
        capture = owner.capture_owner_evidence
        def capture_and_record(cutoff, trading_request, positions_request, open_orders_request):
            requests = (trading_request, positions_request, open_orders_request)
            result = capture(cutoff, *requests)
            captures.append((cutoff, deepcopy(requests), run._normalized(result)))
            return result
        owner.capture_owner_evidence = capture_and_record
        owners.append(owner)
        return owner
    monkeypatch.setattr(run, "_ReplayComposition", observe)
    result = _invoke_configured(tmp_path)
    assert result == dict(run_id="p2", outcome="ADMITTED", reason=None)
    plan = scale._configured_scale_plan_input()
    attempt = read(tmp_path, "p2", "attempt.json")[0]
    status = read(tmp_path, "p2", "status.json")[0]
    rows = read(tmp_path, "p2", "journal.jsonl")
    assert attempt["run_contract_id"] == "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1"
    assert attempt["scenario_plan_sha256"] == "8235c952a5d199825a2a77842b03f49e213ef707c0f30b24d8bf623fde53c2b3"
    assert attempt["configuration_context"] == dict(schema_version="spider_configuration_context_v1",
        config_id=plan["configuration"]["config_id"], configuration_sha256=plan["configuration_sha256"], products=plan["products"])
    assert attempt["planned_coverage"] == [dict(ordinal=row["ordinal"], barrier_id=row["barrier_id"], record_kind=row["record_kind"]) for row in plan["planned_barriers"]]
    assert len(rows) == 40 and [row["journal_seq"] for row in rows] == list(range(1, 41))
    assert [(row["barrier_id"], row["record_kind"], row["scheduler_key"], row["causal_parent_ids"]) for row in rows] == [
        (row["barrier_id"], row["record_kind"], row["scheduler_key"], row["causal_parent_ids"]) for row in plan["planned_barriers"]]
    assert all(row["configuration_context"] == attempt["configuration_context"] for row in rows)
    source_ordinals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20]
    source_cutoffs = [501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 518, 519]
    kinds = [("TRADING", "TRADING"), ("POSITIONS", "POSITIONS"), ("OPEN_ORDERS", "OPEN-ORDERS")]
    expected_captures = [(500, [f"SPIDER-P2-INITIAL-{suffix}" for _, suffix in kinds])]
    expected_captures.extend((cutoff, [f"SPIDER-P2-SOURCE-{ordinal}-{suffix}" for _, suffix in kinds])
                             for ordinal, cutoff in zip(source_ordinals, source_cutoffs, strict=True))
    expected_captures.append((100024, [f"SPIDER-P2-FINAL-{suffix}" for _, suffix in kinds]))
    assert [(cutoff, [request["snapshot_id"] for request in requests]) for cutoff, requests, _ in captures] == expected_captures
    assert all([request["snapshot_kind"] for request in requests] == [kind for kind, _ in kinds] for _, requests, _ in captures)
    all_capture_ids = [request["snapshot_id"] for _, requests, _ in captures for request in requests]
    assert len(all_capture_ids) == len(set(all_capture_ids)) == 60
    source_rows = [row for row in rows if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    assert len(source_rows) == 18
    for index, row in enumerate(source_rows):
        evidence = row["payload"]
        assert evidence["owner_evidence_before"] == captures[index][2]["owner_evidence"]
        assert evidence["owner_evidence_after"] == captures[index + 1][2]["owner_evidence"]
    rejected = rows[12]["payload"]["result"]
    assert (rejected["classification"], rejected["rejections"], rejected["account_version_before"], rejected["account_version_after"]) == (
        "REJECTED", [{"event_id": "event-513", "reason": "INSUFFICIENT_SHARED_EQUITY"}], 12, 12)
    assert [(rows[index]["payload"]["result"]["account_version_before"],
             rows[index]["payload"]["result"]["account_version_after"]) for index in (13, 14, 15, 18, 19)] == [
        (12, 13), (13, 14), (14, 15), (15, 16), (16, 17)]
    assert [row["effective_at"] for row in rows[20:29:3]] == [100010, 100012, 100014]
    assert [row["effective_at"] for row in rows[31:40:3]] == [100020, 100022, 100024]
    assert (status["state"], status["processed_boundary"]["ordinal"], status["persisted_boundary"]["ordinal"], status["failure_reason"]) == ("COMPLETE", 40, 40, None)
    assert all((tmp_path / "p2" / name).exists() for name in ("endpoint.json", "reconciliation.json", "report.jsonl", "completion.json"))
    endpoint = read(tmp_path, "p2", "endpoint.json")[0]
    reconciliation = read(tmp_path, "p2", "reconciliation.json")[0]
    report = read(tmp_path, "p2", "report.jsonl")
    completion = read(tmp_path, "p2", "completion.json")[0]
    assert endpoint["cutoff"] == dict(scheduler_time=100024, persisted_boundary=status["persisted_boundary"])
    assert endpoint["remaining_planned_barriers"] == []
    assert endpoint["scheduler_observation"]["gate"] == "RUNNING"
    assert endpoint["scheduler_observation"]["terminal"] is None
    assert endpoint["scheduler_observation"]["pending_keys"] == []
    assert endpoint["scheduler_observation"]["callback_actions"] == []
    assert endpoint["scheduler_observation"]["last_popped"] == plan["planned_barriers"][-1]["scheduler_key"]
    assert [(poll["poll_id"], poll["status"]) for poll in endpoint["scheduler_observation"]["polls"]] == [("Q1", "COMPLETED"), ("Q2", "COMPLETED")]
    oracle = scale._configured_scale_projection_oracle()
    assert scale._PROJECTION_ORACLE_SHA256 == "7154053ff5f233362ecf0bf6f64bf9757bdc76807b748f6e0b063280031641a3"
    fills = [(Decimal("0.5"), Decimal("100"))] * 3
    fees = sum((quantity * price * Decimal("0.001") for quantity, price in fills), Decimal("0"))
    cash = Decimal("121.2") - fees
    unrealized = Decimal("0.5") * (Decimal("100") - Decimal("100")) + Decimal("1") * (Decimal("100") - Decimal("100"))
    assert (fees, cash, cash + unrealized, Decimal(oracle["gross_realized"])) == (
        Decimal(oracle["total_fees"]), Decimal(oracle["cash"]), Decimal(oracle["equity"]), Decimal("0"))
    positions = [row for row in report if Decimal(row["position_contracts"]) != 0]
    live_orders = [order for row in report for order in row["open_orders"]]
    position_margin = sum((Decimal(row["notional_usd"]) / Decimal("10") for row in positions), Decimal("0"))
    order_margin = sum((Decimal(order["limit_price"]) * Decimal(order["original_size_contracts"]) / Decimal("10")
                        for order in live_orders), Decimal("0"))
    order_fee_holds = sum((Decimal(order["limit_price"]) * Decimal(order["original_size_contracts"]) * Decimal("0.001")
                           for order in live_orders), Decimal("0"))
    independently_available = cash + unrealized - position_margin - order_margin - order_fee_holds
    assert (len(positions), len(live_orders), position_margin, order_margin, order_fee_holds, independently_available) == (
        2, 10, Decimal("15"), Decimal("100"), Decimal("1"), Decimal("5.05"))
    assert independently_available == Decimal(report[0]["account_available_equity"])
    assert [row["product_id"] for row in report] == plan["products"]
    assert all(row["configuration_context"] == attempt["configuration_context"] for row in report)
    assert all((row["account_cash"], row["account_equity"], row["account_available_equity"],
                row["account_gross_realized"], row["account_total_fees"]) ==
               (oracle["cash"], oracle["equity"], oracle["available_equity"], oracle["gross_realized"], oracle["total_fees"])
               for row in report)
    assert [(row["product_id"], row["position_contracts"], row["mark_price"], row["notional_usd"], row["committed_execution_refs"])
            for row in report] == [(row["product_id"], row["position_contracts"], row["mark_price"], row["notional_usd"], row["committed_execution_refs"])
                                   for row in oracle["products"]]
    source_by_event = {member["stamp"]["event_id"]: row["payload"]["result"]["committed_references"]
                       for row in rows if row["record_kind"] == "SOURCE_GROUP_RESULT"
                       for member in row["payload"]["request"]["members"]}
    assert all({"namespace": "SOURCE", "fact_id": reference.removeprefix("SOURCE:")} in
               source_by_event.get(reference.removeprefix("SOURCE:"), [])
               for item in oracle["products"] for reference in item["committed_execution_refs"])
    assert all(row["source_evidence_refs"] == [*[f"journal:{ordinal}" for ordinal in oracle_row["evidence_ordinals"]],
                                                "artifact:endpoint.json#/final_owner_evidence"]
               for row, oracle_row in zip(report, oracle["products"], strict=True))
    for row, oracle_row in zip(report, oracle["products"], strict=True):
        assert len(row["open_orders"]) == (0 if oracle_row["client_order_id"] is None else 1)
        if oracle_row["client_order_id"] is not None:
            order = row["open_orders"][0]
            assert (order["client_order_id"], order["product_id"], order["side"], order["state"], order["limit_price"],
                    order["original_size_contracts"], order["cumulative_filled_size_contracts"]) == (
                oracle_row["client_order_id"], oracle_row["product_id"], "buy", "live", "100", "1", "0")
    assert reconciliation["result"] == "OK" and all(check["result"] == "OK" for check in reconciliation["checks"])
    assert len(reconciliation["checks"]) == 11
    assert completion["state"] == "COMPLETE" and completion["configuration_context"] == attempt["configuration_context"]
    assert len(finalizes) == len(admissions) == 1
    assert finalizes[0]["reconciliation"]["result"] == "OK"
    assert admissions[0][0]["decision"] == "ACCEPT" and admissions[0][1] == admissions[0][2]
    assert len(owners) == 1
    assert len(run._PROGRAM) == 17
    records = [(name.encode(), sha256((run._ROOT / name).read_bytes()).digest())
               for name in sorted((*run._PROGRAM, "python-strategy/src/core/backtest/spider_configured_scale_input.py"))]
    expected_program = sha256(b"".join(len(name).to_bytes(8, "big") + name + digest for name, digest in records)).hexdigest()
    assert attempt["program_sha256"] == expected_program
    native = owners[0]._codec.inspect_state()
    assert (native["account_version"], native["cash"], native["total_fees"], native["gross_realized"], native["lifecycle"]) == (17, Decimal("121.05"), Decimal("0.15"), Decimal("0"), "RISK_STABLE")
    assert owners[0]._polls["Q1"].observation.status == owners[0]._polls["Q2"].observation.status == "COMPLETED"
    assert policy_calls == dict(orders=1, order_filled=0, raise_leverage=2, check_risk=2)
    account = cast(wire.Account, plan["account_key"])
    trading = owners[0]._codec.capture_snapshot(cast(wire.SnapshotRequest, dict(
        schema_version="snapshot_request_v1", account_key=account, snapshot_id="P2-FINAL-READONLY-TRADING",
        snapshot_kind="TRADING", capture_mode="OWNER_CURRENT", captured_at=519)))
    positions = owners[0]._codec.capture_snapshot(cast(wire.SnapshotRequest, dict(
        schema_version="snapshot_request_v1", account_key=account, snapshot_id="P2-FINAL-READONLY-POSITIONS",
        snapshot_kind="POSITIONS", capture_mode="OWNER_CURRENT", captured_at=519)))
    assert trading["immutable_payload"]["equity"] == Decimal("121.05")
    assert trading["immutable_payload"]["available_equity"] == Decimal("5.05")
    assert [(row["product_id"], row["position_contracts"], row["notional_usd"])
            for row in positions["immutable_payload"]["rows"]] == [
        ("BTC-USDT-SWAP", Decimal("0.5"), Decimal("50")),
        ("DOGE-USDT-SWAP", Decimal("1"), Decimal("100"))]


def test_configured_journal_is_run_id_independent_and_rejected_input_never_constructs(tmp_path, monkeypatch):
    first = _invoke_configured(tmp_path, "p2a")
    second = _invoke_configured(tmp_path, "p2b")
    assert first == dict(run_id="p2a", outcome="ADMITTED", reason=None)
    assert second == dict(run_id="p2b", outcome="ADMITTED", reason=None)
    def normalized(value, path=()):
        if isinstance(value, dict):
            return {key: normalized(item, (*path, key)) for key, item in value.items()
                    if key not in {"run_id", "report_sha256", "endpoint_state_digest", "reconciliation_digest"}
                    and not (key == "sha256" and "artifacts" in path)}
        if isinstance(value, list):
            return [normalized(item, path) for item in value]
        return value
    for artifact in ("journal.jsonl", "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json"):
        left = read(tmp_path, "p2a", artifact)
        right = read(tmp_path, "p2b", artifact)
        assert normalized(left) == normalized(right)
    original = run._plans.plan_bundle
    def changed(selector):
        plan = cast(dict[str, Any], original(selector))
        plan["configuration"]["products"][0]["product_id"] = "CFG-OTHER"
        plan["configuration_sha256"] = sha256(run._bytes(plan["configuration"])).hexdigest()
        return plan
    monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("owner must not construct"))
    assert _invoke_configured(tmp_path, "bad", changed)["reason"] == "UNSUPPORTED_CONFIGURATION"
    rejected = read(tmp_path, "bad", "attempt.json")[0]
    assert rejected["run_contract_id"] == "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1"
    assert rejected["registration_state"] == "REJECTED" and "configuration_context" not in rejected


def test_configured_failed_projection_never_finalizes_or_admits(tmp_path, monkeypatch):
    finalize_calls, admission_calls = [], []
    original_build = run._build
    def failed_projection(*args, **kwargs):
        projected = original_build(*args, **kwargs)
        projected["reconciliation"]["checks"][0]["result"] = "FAILED"
        projected["reconciliation"]["result"] = "FAILED"
        return projected
    monkeypatch.setattr(run, "_build", failed_projection)
    monkeypatch.setattr(storage.SpiderRunStore, "finalize", lambda *args, **kwargs: finalize_calls.append(args))
    monkeypatch.setattr(run, "_admit", lambda *_: admission_calls.append(True))
    assert _invoke_configured(tmp_path) == dict(
        run_id="p2", outcome="FAILED", reason="ENDPOINT_RECONCILIATION_FAILED")
    status = read(tmp_path, "p2", "status.json")[0]
    assert status["state"] == "FAILED" and status["failure_reason"] == "ENDPOINT_RECONCILIATION_FAILED"
    assert status["processed_boundary"]["ordinal"] == status["persisted_boundary"]["ordinal"] == 40
    assert finalize_calls == admission_calls == []
    assert not any((tmp_path / "p2" / name).exists() for name in (
        "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json"))


def test_configured_recipe_kind_and_configuration_mutations_reject_before_owner(tmp_path, monkeypatch):
    original = run._plans.plan_bundle
    for mutation in ("recipe", "configuration"):
        root = tmp_path / mutation
        root.mkdir()
        def changed(selector, mutation=mutation):
            plan = cast(dict[str, Any], original(selector))
            if mutation == "recipe":
                plan["recipe"][0]["kind"] = "DELIVERY"
            else:
                plan["configuration"]["products"][0]["product_id"] = "CFG-OTHER"
                plan["configuration_sha256"] = sha256(run._bytes(plan["configuration"])).hexdigest()
            return plan
        monkeypatch.setattr(run, "_ReplayComposition", lambda *a, **k: pytest.fail("owner must not construct"))
        assert _invoke_configured(root, mutation, changed)["reason"] == "UNSUPPORTED_CONFIGURATION"
        attempt = read(root, mutation, "attempt.json")[0]
        assert attempt["run_contract_id"] == "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1"
        assert attempt["registration_state"] == "REJECTED" and "configuration_context" not in attempt


def test_configured_barrier_mismatch_stops_before_processing_or_append(tmp_path, monkeypatch):
    original = run._ReplayComposition
    def mismatch(*args, **kwargs):
        callback = kwargs["evidence_callback"]
        def altered(kind, key, payload):
            if kind == "SOURCE_GROUP_RESULT":
                key["visible_at"] += 1
            return callback(kind, key, payload)
        kwargs["evidence_callback"] = altered
        return original(*args, **kwargs)
    monkeypatch.setattr(run, "_ReplayComposition", mismatch)
    with pytest.raises(RuntimeError, match="^UNEXPECTED_BARRIER$"):
        _invoke_configured(tmp_path)
    status = read(tmp_path, "p2", "status.json")[0]
    assert status["failure_reason"] == "UNEXPECTED_EXCEPTION"
    assert status["processed_boundary"] is status["persisted_boundary"] is None
    assert read(tmp_path, "p2", "journal.jsonl") == []


def test_configured_context_drop_and_journal_write_failure_stop_at_exact_frontier(tmp_path, monkeypatch):
    append = storage.SpiderRunStore.append_journal
    def drop_context(store, row):
        row = deepcopy(row)
        row.pop("configuration_context")
        return append(store, row)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", drop_context)
    with pytest.raises(ValueError, match="^INVALID_ARTIFACT$"):
        _invoke_configured(tmp_path, "context")
    status = read(tmp_path, "context", "status.json")[0]
    assert status["failure_reason"] == "UNEXPECTED_EXCEPTION"
    assert status["processed_boundary"]["ordinal"] == 1 and status["persisted_boundary"] is None
    assert read(tmp_path, "context", "journal.jsonl") == []
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", append)
    def fail_at_twenty(store, row):
        if row["journal_seq"] == 20:
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        append(store, row)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail_at_twenty)
    assert _invoke_configured(tmp_path, "persist")["reason"] == "PERSISTENCE_FAILED"
    failed = read(tmp_path, "persist", "status.json")[0]
    assert (failed["processed_boundary"]["ordinal"], failed["persisted_boundary"]["ordinal"]) == (20, 19)
    assert len(read(tmp_path, "persist", "journal.jsonl")) == 19


def test_configured_native_terminal_preserves_existing_failure_mapping(tmp_path, monkeypatch):
    codec = wire.ScenarioCodec("SYNTHETIC_MIN_CASH_V1", cast(wire.Account, dict(venue="okx-scenario", environment="test", account="A")))
    with pytest.raises(ValueError) as rejected:
        codec.capture_snapshot(cast(wire.SnapshotRequest, {}))
    native = rejected.value
    def fail(*args, **kwargs):
        raise native
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", fail)
    assert _invoke_configured(tmp_path, "native")["reason"] == "NATIVE_FAULT"
    status = read(tmp_path, "native", "status.json")[0]
    assert status["primary_failure"] == dict(kind="NATIVE", reason="INVALID_SCHEMA")
    assert status["processed_boundary"] is status["persisted_boundary"] is None
    assert read(tmp_path, "native", "journal.jsonl") == []


@pytest.mark.parametrize("ordinal", [21, 22, 23, 40])
def test_configured_final_dispatch_persistence_fault_publishes_once_without_retry(tmp_path, monkeypatch, ordinal):
    append, publish = storage.SpiderRunStore.append_journal, storage.SpiderRunStore.publish_failure
    attempts, failures = [], []
    def fail_at_barrier(store, row):
        attempts.append(row["journal_seq"])
        if row["journal_seq"] == ordinal:
            raise storage.SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED",
                                               dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))
        append(store, row)
    def record_failure(store, reason, primary):
        failures.append((reason, primary))
        return publish(store, reason, primary)
    monkeypatch.setattr(storage.SpiderRunStore, "append_journal", fail_at_barrier)
    monkeypatch.setattr(storage.SpiderRunStore, "publish_failure", record_failure)
    assert _invoke_configured(tmp_path, f"fault-{ordinal}") == dict(
        run_id=f"fault-{ordinal}", outcome="FAILED", reason="PERSISTENCE_FAILED")
    status = read(tmp_path, f"fault-{ordinal}", "status.json")[0]
    journal = read(tmp_path, f"fault-{ordinal}", "journal.jsonl")
    assert (status["processed_boundary"]["ordinal"], status["persisted_boundary"]["ordinal"]) == (ordinal, ordinal - 1)
    assert len(journal) == ordinal - 1 and len(attempts) == ordinal
    assert attempts.count(ordinal) == 1
    assert failures == [("PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED"))]
