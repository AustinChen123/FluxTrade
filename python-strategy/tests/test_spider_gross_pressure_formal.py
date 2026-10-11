"""Finite multi-product gross and original-GRID pressure through formal runners."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
import heapq
import json
from pathlib import Path
import os
from types import SimpleNamespace
from typing import Any, cast

import pytest

from examples import run_spider_scenario_replay as cli
from src.core import backtest_runner as full
from src.core import research_backtest_runner as research
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest.spider_gross_execution_input import (
    ADMISSION_POLICY_ID,
    NATIVE_PROFILE_ID,
    POLICY_ID,
    RESPONSE_POLICY_ID,
    SCHEMA_VERSION,
    GrossAuthorizationScope,
    GrossResponseParameters,
    SpiderGrossExecutionInput,
    encode_spider_gross_execution_input,
)
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_policy_protocol import HistoricalSequenceMode
from src.core.backtest.spider_run_execution_evidence import _action_bindings_for_mode
from src.core.backtest import spider_run_admission as admission
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.backtest.spider_historical_input import (
    BAR_DURATION_MS,
    HistoricalBar,
    HistoricalRunInput,
    InstrumentSpecEvidence,
    SourceManifest,
    validate_historical_input,
)
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from test_spider_execution_formal_runner import _expected_projection
from test_spider_execution_input import _input
from test_spider_historical_input import (
    _initial_account_state,
    _p2_configuration,
    _policy_cache,
    _rehashed_run,
    _rows,
    _run_with_configuration,
    _valid_run,
)
from test_spider_historical_replay import _extend_one_minute


_MODELS = (
    "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
    "OHLC4_OPEN_LOW_HIGH_CLOSE_V1",
)
_PRODUCTS = ("A-USDT-SWAP", "B-USDT-SWAP", "C-USDT-SWAP")
_ARTIFACTS = (
    "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
    "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
)


def _manifest(
    template: SourceManifest,
    rows: tuple[HistoricalBar, ...],
    products: tuple[str, ...],
    *,
    trade: bool,
) -> SourceManifest:
    order = {product: index for index, product in enumerate(products)}
    ordered = sorted(rows, key=lambda row: (order[row.product_id], row.source_sequence))
    digest = sha256(b"".join(bytes.fromhex(row.source_row_hash) for row in ordered)).hexdigest()
    return replace(
        template,
        canonical_rows_sha256=digest,
        product_mapping=tuple((product, product) for product in products),
        units="CONTRACTS" if trade else "PRICE",
        source_row_count=len(rows),
        normalized_row_count=len(rows),
        first_source_timestamp_ms=min(row.bar_open_ms for row in rows),
        last_source_timestamp_ms=max(row.bar_open_ms for row in rows),
    )


def _bar(
    product: str,
    at: int,
    sequence: int,
    *,
    trade: bool,
    ohlc: tuple[str, str, str, str],
    volume: str = "4",
) -> HistoricalBar:
    row = HistoricalBar(
        product,
        at,
        Decimal(ohlc[0]),
        Decimal(ohlc[1]),
        Decimal(ohlc[2]),
        Decimal(ohlc[3]),
        1,
        sequence,
        Decimal(volume) if trade else None,
    )
    return replace(row, source_row_hash=sha256(row.canonical_row(trade=trade)).hexdigest())


def _gross_request(history: HistoricalRunInput, policy_end_ms: int) -> SpiderGrossExecutionInput:
    execution = replace(_input(history), policy_end_ms=policy_end_ms)
    return SpiderGrossExecutionInput(
        SCHEMA_VERSION,
        POLICY_ID,
        NATIVE_PROFILE_ID,
        ADMISSION_POLICY_ID,
        RESPONSE_POLICY_ID,
        GrossResponseParameters(),
        GrossAuthorizationScope(history.strategy_identity, history.ordered_products),
        None,
        execution,
    )


def _three_product_shock(model_id: str, run_id: str) -> SpiderGrossExecutionInput:
    base = _valid_run()
    start, end, warmup = base.range_start_ms, base.range_start_ms + BAR_DURATION_MS, base.warmup_start_ms
    configuration = _p2_configuration(_PRODUCTS)
    configuration["cash"] = "75"
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = end
    sides = ("LONG", "SHORT", "LONG")
    configuration["positions"] = [
        {
            "product_id": product,
            "side": side,
            "quantity_contracts": "1",
            "lots": [{
                "seed_execution_id": f"G3-SEED-{product}",
                "seed_sequence": index,
                "strategy_id": base.strategy_identity,
                "quantity_contracts": "1",
                "entry_price": "100",
            }],
        }
        for index, (product, side) in enumerate(zip(_PRODUCTS, sides, strict=True))
    ]
    configured = _run_with_configuration(base, configuration)
    trade_rows: list[HistoricalBar] = []
    mark_rows: list[HistoricalBar] = []
    shock = ("100", "120", "80", "100")
    for product in _PRODUCTS:
        trades = list(_rows(product, warmup, trade=True))
        marks = list(_rows(product, warmup, trade=False))
        trades[-1] = _bar(
            product,
            start,
            len(trades) - 1,
            trade=True,
            ohlc=("100", "100", "100", "100") if product == _PRODUCTS[1] else shock,
        )
        trade_rows.extend(trades)
        mark_rows.extend(marks)
    trade_rows.sort(key=lambda row: (row.bar_open_ms, _PRODUCTS.index(row.product_id)))
    mark_rows.sort(key=lambda row: (row.bar_open_ms, _PRODUCTS.index(row.product_id)))
    specs = tuple(
        InstrumentSpecEvidence(
            product,
            start,
            Decimal("1"),
            Decimal("1"),
            Decimal("0.01"),
            Decimal("0.001"),
            Decimal("0.001"),
            "0" * 64,
        )
        for product in _PRODUCTS
    )
    history = replace(
        configured,
        run_id=run_id,
        ordered_products=_PRODUCTS,
        model_id=model_id,
        range_end_ms=end,
        trade_bars=tuple(trade_rows),
        mark_bars=tuple(mark_rows),
        trade_manifest=_manifest(base.trade_manifest, tuple(trade_rows), _PRODUCTS, trade=True),
        mark_manifest=_manifest(base.mark_manifest, tuple(mark_rows), _PRODUCTS, trade=False),
        spec_before=specs,
        spec_after=tuple(replace(spec, effective_at_ms=end) for spec in specs),
        initial_policy_cache=_policy_cache(_PRODUCTS, running=False),
        initial_account_state=_initial_account_state(
            cash="75", positions=configuration["positions"], orders=[],
        ),
    )
    validate_historical_input(history)
    return _gross_request(history, start + 59_000)


def _grid_replenishment(model_id: str, run_id: str) -> SpiderGrossExecutionInput:
    history = _extend_one_minute(_valid_run())
    history = replace(history, run_id=run_id, model_id=model_id)
    start = history.range_start_ms
    trades = tuple(
        _bar(
            row.product_id,
            row.bar_open_ms,
            row.source_sequence,
            trade=True,
            ohlc=("101", "101.5", "100.5", "101"),
            volume="0.4",
        )
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == start else
        _bar(
            row.product_id,
            row.bar_open_ms,
            row.source_sequence,
            trade=True,
            ohlc=("100", "103", "98", "100"),
        )
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == start + BAR_DURATION_MS else row
        for row in history.trade_bars
    )
    history = _rehashed_run(history, trade_rows=trades)
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = history.range_end_ms
    history = _run_with_configuration(history, configuration)
    cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    cache["running"] = True
    for row in cache["rows"]:
        row["active"] = "true" if row["product_id"] == "A-USDT-SWAP" else "false"
    history = replace(history, initial_policy_cache=canonical_bytes(cache))
    validate_historical_input(history)
    return _gross_request(history, start + 119_000)


def _admitted_artifacts(root, run_id: str) -> dict[str, Any]:
    return cast(dict[str, Any], admit_spider_run(root / run_id)["artifacts"])


def _projection_bytes(artifacts: dict[str, Any]) -> bytes:
    expected = _expected_projection(cast(dict[str, object], artifacts))
    expected["schema_version"] = "spider_gross_execution_projection_v1"
    return canonical_bytes(expected)


def _accepted_run(root, run_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    admitted = admit_spider_run(root / run_id)
    artifacts = cast(dict[str, Any], admitted["artifacts"])
    return artifacts, cast(dict[str, Any], admitted)


def _sample_gross_dispatch(composition: Any, peak: dict[str, int]) -> None:
    """Read pressure counts without invoking a result projection or native capture."""
    endpoint = composition._paid_endpoint
    if endpoint is None:
        raise AssertionError("gross endpoint missing at dispatch boundary")
    if any(action["role"] == "EXIT" for action in endpoint.actions):
        raise AssertionError("GGRID fixture unexpectedly has an EXIT-role action")
    pending_actions = [
        action for action in endpoint.actions if action["classification"] == "PENDING"
    ]
    peak["working"] = max(peak["working"], len(composition._codec.historical_working_orders()))
    peak["queue"] = max(peak["queue"], len(composition._queue))
    peak["pending_actions"] = max(peak["pending_actions"], len(pending_actions))
    peak["pending_deliveries"] = max(
        peak["pending_deliveries"], len(endpoint.pending_delivery_ids),
    )


def test_gross_dispatch_sampler_only_reads_existing_boundary_state(monkeypatch):
    forbidden_calls = {"result": 0, "capture_snapshot": 0, "heappop": 0}

    class Endpoint:
        actions = [
            {"role": "GROSS_REDUCTION", "classification": "PENDING"},
            {"role": "DRAIN_CANCEL", "classification": "COMMITTED"},
            {"role": "DRAIN_CANCEL", "classification": "PENDING"},
        ]
        pending_delivery_ids = {"delivery-a", "delivery-b", "delivery-c"}
        last_owner_evidence = {"sentinel": "owner"}
        last_cutoff = 123
        gross_response = SimpleNamespace(state="IDLE", active=False)

        def result(self, *_args, **_kwargs):
            forbidden_calls["result"] += 1
            raise AssertionError("sampler must not call PaidEndpoint.result")

    class Codec:
        def historical_working_orders(self):
            return ["working-a", "working-b"]

        def capture_snapshot(self, *_args, **_kwargs):
            forbidden_calls["capture_snapshot"] += 1
            raise AssertionError("sampler must not capture native snapshots")

    composition = SimpleNamespace(
        _paid_endpoint=Endpoint(), _codec=Codec(), _queue=["q1", "q2", "q3", "q4"],
        _current_time=456, _last_popped=(455, 2, 17, "prior"),
        _records={"record": {"classification": "SUCCESS"}}, _audit={"audit": []},
    )
    before = (
        deepcopy(composition._queue), deepcopy(composition._paid_endpoint.actions),
        composition._codec.historical_working_orders(),
        set(composition._paid_endpoint.pending_delivery_ids),
        deepcopy(composition._paid_endpoint.last_owner_evidence),
        composition._paid_endpoint.last_cutoff, composition._current_time,
        composition._last_popped, deepcopy(composition._records), deepcopy(composition._audit),
        composition._paid_endpoint.gross_response.state,
        composition._paid_endpoint.gross_response.active,
    )
    original_heappop = heapq.heappop

    def forbidden_heappop(_queue):
        forbidden_calls["heappop"] += 1
        raise AssertionError("sampler must not pop scheduler items")

    monkeypatch.setattr(heapq, "heappop", forbidden_heappop)
    peak = {"working": 0, "queue": 0, "pending_actions": 0, "pending_deliveries": 0}
    _sample_gross_dispatch(composition, peak)
    after = (
        deepcopy(composition._queue), deepcopy(composition._paid_endpoint.actions),
        composition._codec.historical_working_orders(),
        set(composition._paid_endpoint.pending_delivery_ids),
        deepcopy(composition._paid_endpoint.last_owner_evidence),
        composition._paid_endpoint.last_cutoff, composition._current_time,
        composition._last_popped, deepcopy(composition._records), deepcopy(composition._audit),
        composition._paid_endpoint.gross_response.state,
        composition._paid_endpoint.gross_response.active,
    )
    assert peak == {"working": 2, "queue": 4, "pending_actions": 2, "pending_deliveries": 3}
    assert after == before
    assert forbidden_calls == {"result": 0, "capture_snapshot": 0, "heappop": 0}
    assert heapq.heappop is forbidden_heappop
    assert original_heappop is not forbidden_heappop


def _observe_gross_dispatch(monkeypatch) -> dict[str, Any]:
    original_factory = _ReplayComposition._from_gross_response_run
    captured: dict[str, Any] = {"composition": None, "peak": {
        "working": 0, "queue": 0, "pending_actions": 0, "pending_deliveries": 0,
    }}

    def sample(composition):
        _sample_gross_dispatch(composition, captured["peak"])

    def factory(cls, request):
        composition = original_factory(request)
        captured["composition"] = composition
        queue = composition._queue
        original_heappop = heapq.heappop

        def observed_heappop(candidate_queue):
            if (
                candidate_queue is queue and candidate_queue
                and composition._last_popped is not None
                and candidate_queue[0][0] != composition._last_popped[0]
            ):
                sample(composition)
            return original_heappop(candidate_queue)

        monkeypatch.setattr(heapq, "heappop", observed_heappop)
        original_dispatch = composition._dispatch_due

        def observed_dispatch(until):
            result = original_dispatch(until)
            sample(composition)
            return result

        composition._dispatch_due = observed_dispatch
        return composition

    monkeypatch.setattr(_ReplayComposition, "_from_gross_response_run", classmethod(factory))
    return captured


def _assert_g3_native(artifacts: dict[str, Any], model_id: str) -> None:
    endpoint = cast(dict[str, Any], artifacts["paid_endpoint.json"])
    report = cast(dict[str, Any], cast(list[Any], artifacts["report.jsonl"])[0])
    gross = cast(dict[str, Any], report["gross_observation"])
    expected = {
        _MODELS[0]: {
            "gross_state": "INCOMPLETE",
            "rounds": 2,
            "actions": 6,
            "fees": "0.16272244",
            "cash": "49.63451756",
            "realized": "-25.20276",
            "price": {"A-USDT-SWAP": "79.92", "B-USDT-SWAP": "100.1", "C-USDT-SWAP": "79.92"},
            "residual": "0.127",
        },
        _MODELS[1]: {
            "gross_state": "RECOVERED",
            "rounds": 1,
            "actions": 3,
            "fees": "0.21275236",
            "cash": "99.61440764",
            "realized": "24.82716",
            "price": {"A-USDT-SWAP": "119.88", "B-USDT-SWAP": "100.1", "C-USDT-SWAP": "119.88"},
            "residual": "0.374",
        },
    }[model_id]
    attempt = cast(dict[str, Any], artifacts["attempt.json"])
    assert attempt["profile_id"] == NATIVE_PROFILE_ID
    native_path = Path(scenario._wire.loaded_native_artifact_path())
    assert attempt["native_artifact_sha256"] == sha256(native_path.read_bytes()).hexdigest()
    scope = attempt["execution_context"]["authorization_scope"]
    assert scope["ordered_products"] == list(_PRODUCTS)
    wrapper = cast(dict[str, Any], artifacts["execution_input.json"])
    nested_history = wrapper["execution_input"]["historical_input"]
    assert nested_history["model_id"] == model_id
    assert nested_history["ordered_products"] == list(_PRODUCTS)
    assert endpoint["status"] == "INCOMPLETE"
    assert gross["current_state"] == expected["gross_state"]
    episodes = cast(list[dict[str, Any]], gross["episodes"])
    assert len(episodes) == 1
    assert [len(episode["rounds"]) for episode in episodes] == [expected["rounds"]]
    initial_native = cast(dict[str, Any], gross["observations"][0]["native_state"])
    initial_admission = initial_native["gross_admission"]
    assert initial_native["profile_id"] == NATIVE_PROFILE_ID
    assert initial_native["account_version"] == 0
    assert initial_admission["gross"] == "300"
    assert initial_admission["effective_equity"] == "75"
    assert initial_admission["net"] == "100"
    assert [row["product_id"] for row in initial_native["products"]] == list(_PRODUCTS)
    assert [row["signed_contracts"] for row in initial_native["products"]] == ["1", "-1", "1"]
    actions = cast(list[dict[str, Any]], endpoint["actions"])
    reductions = [row for row in actions if row["role"] == "GROSS_REDUCTION"]
    assert len(reductions) == expected["actions"]
    assert all(row["classification"] == "COMMITTED" for row in reductions)
    source_groups = {
        row["payload"]["request"]["group_id"]: row
        for row in cast(list[dict[str, Any]], artifacts["journal.jsonl"])
        if row["record_kind"] == "SOURCE_GROUP_RESULT"
    }
    paid_observation = cast(dict[str, Any], report["paid_observation"])
    dependencies = {
        row["group_id"]: row for row in paid_observation["send_dependencies"]
    }
    for action in reductions:
        receipt = source_groups[action["group_id"]]
        members = receipt["payload"]["request"]["members"]
        assert len(members) == 1 and members[0]["kind"] == "INTENT"
        intent = members[0]["payload"]
        assert intent["reduce_only"] is True
        assert intent["product_id"] == action["product_id"]
        assert intent["side"] == ("SHORT" if action["product_id"] != "B-USDT-SWAP" else "LONG")
        dependency = dependencies[action["group_id"]]
        assert dependency["state"] == "COMMITTED"
        assert dependency["client_order_id"] == intent["client_order_id"]
        binding = dependency["binding"]
        assert binding["client_order_id"] == intent["client_order_id"]
        assert binding["order_id"] == action["order_id"]
        assert binding["product_id"] == intent["product_id"]
        assert binding["strategy_id"] == intent["strategy_id"]
        committed_refs = receipt["payload"]["result"]["committed_references"]
        assert {row["fact_id"] for row in committed_refs} == {dependency["event_id"]}
        assert receipt["payload"]["result"]["classification"] == "COMMITTED"
    order_ids = {row["order_id"] for row in reductions if row["order_id"] is not None}
    fills = [
        (product["product_id"], fill)
        for record in cast(list[dict[str, Any]], artifacts["journal.jsonl"])
        if record["record_kind"] == "HISTORICAL_MARKET_RESULT"
        for product in record["payload"]["result"]["products"]
        for fill in product["fills"]
        if fill["order_id"] in order_ids
    ]
    assert len(fills) == 3
    assert {product for product, _ in fills} == set(_PRODUCTS)
    assert len({fill["order_id"] for _, fill in fills}) == 3
    assert len({fill["execution_id"] for _, fill in fills}) == 3
    assert {product: (fill["quantity_contracts"], fill["price"]) for product, fill in fills} == {
        product: ("0.626", price) for product, price in expected["price"].items()
    }
    state = cast(dict[str, Any], endpoint["owner_evidence"]["inspection"])
    assert state["total_fees"] == expected["fees"]
    assert state["cash"] == expected["cash"]
    assert state["gross_realized"] == expected["realized"]
    final_obligation = cast(dict[str, Any], gross["final_obligation_state"])
    final_native = cast(dict[str, Any], final_obligation["native_state"])
    assert final_native["profile_id"] == NATIVE_PROFILE_ID
    native_orders = cast(list[dict[str, Any]], final_native["orders"])
    assert len(native_orders) == 3
    assert {row["product_id"]: row["remaining_contracts"] for row in native_orders} == {
        product: expected["residual"] for product in _PRODUCTS
    }
    assert all(row["status"] == "OPEN" and row["reduce_only"] is True for row in native_orders)
    assert state["lifecycle"] not in {"LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"}


@pytest.mark.parametrize("model_id", _MODELS)
def test_three_product_gross_shock_matches_all_public_formal_entrances(tmp_path, capsys, model_id):
    suffix = "hl" if model_id == _MODELS[0] else "lh"
    run_ids = (
        f"g3-cli-{suffix}",
        f"g3-research-distinct-{suffix}-run",
        f"g3-full-distinct-length-{suffix}-run",
        f"g3-full-repeat-distinct-{suffix}",
    )
    assert len({len(run_id) for run_id in run_ids}) == len(run_ids)
    raw_by_run = {
        run_id: encode_spider_gross_execution_input(_three_product_shock(model_id, run_id))
        for run_id in run_ids
    }
    input_path = tmp_path / f"{suffix}-gross-input.json"
    input_path.write_bytes(raw_by_run[run_ids[0]])
    assert cli.main([
        "--output-root", str(tmp_path), "--run-id", run_ids[0],
        "--scenario-selector", scenario._GROSS_SELECTOR,
        "--gross-execution-input", str(input_path),
    ]) == 1
    cli_output = capsys.readouterr()
    assert cli_output.err == ""
    assert json.loads(cli_output.out) == {
        "run_id": run_ids[0], "outcome": "ADMITTED", "reason": None,
        "endpoint_status": "INCOMPLETE",
    }
    results = [
        research.ResearchBacktestRunner.run_spider_gross_execution(
            output_root=str(tmp_path), run_id=run_ids[1], gross_execution_input=raw_by_run[run_ids[1]],
        ),
        full.BacktestRunner.run_spider_gross_execution(
            output_root=str(tmp_path), run_id=run_ids[2], gross_execution_input=raw_by_run[run_ids[2]],
        ),
        full.BacktestRunner.run_spider_gross_execution(
            output_root=str(tmp_path), run_id=run_ids[3], gross_execution_input=raw_by_run[run_ids[3]],
        ),
    ]
    assert [result.runner_kind for result in results] == ["research", "full", "full"]
    assert all(result.outcome == "ADMITTED" and result.admission == "ACCEPT" for result in results)
    assert all(result.endpoint_status == "INCOMPLETE" for result in results)
    expected_projection: bytes | None = None
    for run_id, result in zip(run_ids, [None, *results], strict=True):
        artifacts, _ = _accepted_run(tmp_path, run_id)
        _assert_g3_native(artifacts, model_id)
        projection = _projection_bytes(artifacts)
        if expected_projection is None:
            expected_projection = projection
        assert projection == expected_projection
        if result is not None:
            assert result.projection is not None
            assert result.projection.canonical_json == projection


def _assert_grid_native(
    artifacts: dict[str, Any], model_id: str, dispatch_capture: dict[str, Any],
) -> None:
    endpoint = cast(dict[str, Any], artifacts["paid_endpoint.json"])
    report = cast(dict[str, Any], cast(list[Any], artifacts["report.jsonl"])[0])
    gross = cast(dict[str, Any], report["gross_observation"])
    assert endpoint["status"] == "INCOMPLETE"
    assert gross["current_state"] == "IDLE"
    expected_price, expected_fee, expected_cash, expected_realized = (
        ("101.5", "0.0198485", "1000.1271515", "0.147")
        if model_id == _MODELS[0] else
        ("101", "0.019799", "1000.078201", "0.098")
    )
    journal = cast(list[dict[str, Any]], artifacts["journal.jsonl"])
    market = [row for row in journal if row["record_kind"] == "HISTORICAL_MARKET_RESULT"]
    fills = [
        (product["product_id"], fill)
        for record in market
        for product in record["payload"]["result"]["products"]
        for fill in product["fills"]
    ]
    a_fills = [(product, fill) for product, fill in fills if product == "A-USDT-SWAP"]
    assert [(fill["quantity_contracts"], fill["price"]) for _, fill in a_fills] == [
        ("0.099", expected_price), ("0.098", "100"),
    ]
    account = cast(dict[str, Any], endpoint["owner_evidence"]["inspection"])
    assert account["total_fees"] == expected_fee
    assert account["cash"] == expected_cash
    assert account["gross_realized"] == expected_realized
    source_records = [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    groups = [row["payload"]["request"] for row in source_records]
    pending_groups = report["paid_observation"]["pending_source_groups"]
    all_groups = [*groups, *(row["group"] for row in pending_groups)]
    assert len(groups) == 145 and len(all_groups) == 147
    kinds = Counter(member["kind"] for group in all_groups for member in group["members"])
    assert kinds == Counter(INTENT=52, CANCEL_REQUEST=59, CANCEL_EFFECT=36)
    outcomes = Counter(row["payload"]["result"]["classification"] for row in source_records)
    assert outcomes == Counter(COMMITTED=107, REJECTED=38)
    assert len(pending_groups) == 2
    assert all(row["group"]["members"][0]["kind"] == "CANCEL_EFFECT" for row in pending_groups)
    assert all(row["group"]["group_id"] not in {group["group_id"] for group in groups}
               for row in pending_groups)
    scheduler = report["scheduler_observation"]
    assert len(scheduler["records"]) == 359
    assert len(scheduler["callback_actions"]) == 109
    assert len(scheduler["pending_keys"]) == 2
    callback_rows = [row for row in journal if row["record_kind"] == "CALLBACK_RESULT"]
    assert len(callback_rows) == 107
    assert sum(row["payload"]["outcome"] == "SUCCESS" for row in callback_rows) == 107
    all_group_by_id = {group["group_id"]: group for group in all_groups}
    callback_groups = {
        action["group_id"] for action in scheduler["callback_actions"]
        if action["group_id"] is not None
    }
    assert callback_groups <= set(all_group_by_id)
    assert {row["key"]["stable_id"] for row in pending_groups} <= {
        key["stable_id"] for key in scheduler["pending_keys"]
    }
    actions_by_delivery: dict[str, list[dict[str, Any]]] = {}
    for action in scheduler["callback_actions"]:
        actions_by_delivery.setdefault(action["delivery_id"], []).append(action)
    child_batches = [rows for rows in actions_by_delivery.values() if len(rows) in (3, 5)]
    assert sorted(len(rows) for rows in child_batches) == [3, 5]
    for rows in child_batches:
        kinds_for_batch = Counter(
            all_group_by_id[row["group_id"]]["members"][0]["kind"] for row in rows
        )
        expected_batch = Counter(CANCEL_REQUEST=1, INTENT=2) if len(rows) == 3 else Counter(
            CANCEL_REQUEST=3, INTENT=2,
        )
        assert kinds_for_batch == expected_batch
    composition = dispatch_capture["composition"]
    assert composition is not None
    assert dispatch_capture["peak"] == {
        "working": 9, "queue": 34, "pending_actions": 13, "pending_deliveries": 4,
    }
    delivery_records = [
        record for record in composition._records.values()
        if record["item"].get("kind") == "DELIVERY"
    ]
    delivery_kinds = Counter(record["item"]["delivery"]["payload_kind"] for record in delivery_records)
    assert delivery_kinds["TRANSPORT_ACK"] == 34
    assert delivery_kinds["EXECUTION_FACT"] == 2
    ack_ids = {record["item"]["delivery"]["delivery_id"] for record in delivery_records
               if record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"}
    endpoint_owner = composition._paid_endpoint
    assert endpoint_owner is not None
    assert ack_ids == composition._p3_cancel_ack_deliveries
    assert endpoint_owner.cancel_ack_action_by_delivery == {}
    ack_scheduler_records = {
        record["stable_id"]: record for record in scheduler["records"]
        if record["kind"] == "DELIVERY"
    }
    assert set(ack_scheduler_records) >= ack_ids
    actions_by_id = {action["action_id"]: action for action in endpoint_owner.actions}
    source_records_by_group = {
        record["payload"]["request"]["group_id"]: record
        for record in journal if record["record_kind"] == "SOURCE_GROUP_RESULT"
    }
    effects_by_event: dict[str, list[dict[str, Any]]] = {}
    for record in source_records_by_group.values():
        for member in record["payload"]["request"]["members"]:
            if member["kind"] == "CANCEL_EFFECT":
                effects_by_event.setdefault(member["stamp"]["event_id"], []).append(record)
    effect_actions = endpoint_owner.cancel_action_by_effect
    effect_groups_by_classification = {
        classification: {
            group_id for group_id, action in effect_actions.items()
            if action["classification"] == classification
        }
        for classification in ("COMMITTED", "PENDING")
    }
    pending_effect_groups = {row["group"]["group_id"] for row in pending_groups}
    assert len(effect_actions) == 36
    assert {key: len(group_ids) for key, group_ids in effect_groups_by_classification.items()} == {
        "COMMITTED": 34, "PENDING": 2,
    }
    assert effect_groups_by_classification["COMMITTED"].isdisjoint(
        effect_groups_by_classification["PENDING"],
    )
    assert effect_groups_by_classification["PENDING"] == pending_effect_groups
    linked_effect_groups: set[str] = set()
    for record in delivery_records:
        delivery = record["item"]["delivery"]
        if delivery["payload_kind"] != "TRANSPORT_ACK":
            continue
        delivery_id = delivery["delivery_id"]
        assert ack_scheduler_records[delivery_id]["classification"] == "SUCCESS"
        matching_effects = effects_by_event[delivery["source_fact_id"]]
        assert len(matching_effects) == 1
        effect_record = matching_effects[0]
        group = effect_record["payload"]["request"]
        group_id = group["group_id"]
        assert group_id not in linked_effect_groups
        linked_effect_groups.add(group_id)
        assert effect_record["payload"]["result"]["classification"] == "COMMITTED"
        member = group["members"][0]
        assert member["kind"] == "CANCEL_EFFECT"
        committed_refs = effect_record["payload"]["result"]["committed_references"]
        assert {row["fact_id"] for row in committed_refs} == {member["stamp"]["event_id"]}
        effect = effect_actions[group_id]
        assert effect["role"] == "DRAIN_CANCEL"
        assert effect["group_id"] == group_id
        assert effect["order_id"] == member["payload"]["effects"][0]["target_order_id"]
        assert actions_by_id[effect["action_id"]] is effect
        parent_actions = [
            action for action in endpoint_owner.cancel_action_by_request.values()
            if action["action_id"] == effect["parent_action_id"]
        ]
        assert len(parent_actions) == 1
        assert parent_actions[0]["role"] == "DRAIN_CANCEL"
        assert parent_actions[0]["product_id"] == effect["product_id"]
        request_record = source_records_by_group[parent_actions[0]["group_id"]]
        request_member = request_record["payload"]["request"]["members"][0]
        assert request_member["kind"] == "CANCEL_REQUEST"
        assert request_record["payload"]["result"]["classification"] == "COMMITTED"
        assert request_member["payload"]["targets"][0]["target_order_id"] == effect["order_id"]
    assert linked_effect_groups == effect_groups_by_classification["COMMITTED"]
    execution_records = [record for record in delivery_records
                         if record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"]
    assert len(execution_records) == 2
    linked_children: list[list[dict[str, Any]]] = []
    for record in execution_records:
        delivery = record["item"]["delivery"]
        assert delivery["immutable_payload"]["product_id"] == "A-USDT-SWAP"
        audits = composition._audit[delivery["delivery_id"]]
        child_ids = [action["group_id"] for event in audits for action in event.get("actions", [])]
        assert child_ids and all(group_id in all_group_by_id for group_id in child_ids)
        linked_children.append([all_group_by_id[group_id] for group_id in child_ids])
    child_shapes = [
        Counter(group["members"][0]["kind"] for group in groups_for_event)
        for groups_for_event in linked_children
    ]
    assert sorted((row["INTENT"], row["CANCEL_REQUEST"]) for row in child_shapes) == [
        (2, 1), (2, 3),
    ]
    pending = endpoint["pending_obligations"]
    assert len(pending["pending_actions"]) == 2
    assert len(pending["pending_deliveries"]) == 0
    open_orders_fact = endpoint["owner_evidence"]["open_orders_fact"]
    assert open_orders_fact["immutable_payload"]["outcome"] == "SUCCESS"
    open_order_rows = open_orders_fact["immutable_payload"]["rows"]
    assert len(open_order_rows) == 2
    assert open_order_rows == pending["working_orders"]
    assert {order["side"] for order in open_order_rows} == {"sell"}
    assert {
        Decimal(order["original_size_contracts"])
        - Decimal(order["cumulative_filled_size_contracts"])
        for order in open_order_rows
    } == {Decimal("0.099"), Decimal("0.098")}

    attempt = cast(dict[str, Any], artifacts["attempt.json"])
    request = _grid_replenishment(model_id, attempt["run_id"]).execution_input
    contract_hash = attempt["execution_context"]["execution_input_sha256"]
    mode = HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1

    def action_binding_ok(candidate_journal, candidate_endpoint) -> bool:
        return _action_bindings_for_mode(
            attempt, candidate_journal, report, candidate_endpoint, request, mode,
            contract_hash=contract_hash, floating_exit=False, gross_execution=True,
        )

    assert action_binding_ok(journal, endpoint)
    reordered_endpoint = deepcopy(endpoint)
    reordered_endpoint["actions"].reverse()
    assert not action_binding_ok(journal, reordered_endpoint)
    wrong_group_journal = deepcopy(journal)
    first_action_group = endpoint["actions"][0]["group_id"]
    source_row = next(
        row for row in wrong_group_journal
        if row["record_kind"] == "SOURCE_GROUP_RESULT"
        and row["payload"]["request"]["group_id"] == first_action_group
    )
    source_row["scheduler_key"]["stable_id"] = "wrong-source-group"
    assert not action_binding_ok(wrong_group_journal, endpoint)
    wrong_parent_endpoint = deepcopy(endpoint)
    cancel_child = next(
        action for action in wrong_parent_endpoint["actions"]
        if action["role"] == "DRAIN_CANCEL" and action["parent_action_id"] is not None
    )
    cancel_child["parent_action_id"] = "missing-causal-parent"
    assert not action_binding_ok(journal, wrong_parent_endpoint)


@pytest.mark.parametrize("model_id", _MODELS)
def test_original_grid_replenishment_pressure_is_retained_in_gross_formal_bundle(
    tmp_path, model_id, monkeypatch,
):
    run_id = "ggrid-high-low-formal" if model_id == _MODELS[0] else "ggrid-low-high-formal"
    raw = encode_spider_gross_execution_input(_grid_replenishment(model_id, run_id))
    dispatch_capture = _observe_gross_dispatch(monkeypatch)
    original_capture = admission._capture
    capture_limits: dict[str, int] = {}

    def observe_capture(directory, name, **kwargs):
        capture_limits[name] = kwargs.get("limit", admission._LIMIT)
        return original_capture(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", observe_capture)
    result = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_id, gross_execution_input=raw,
    )
    assert capture_limits["report.jsonl"] == admission._GROSS_REPORT_LIMIT
    assert capture_limits["report.jsonl"] == 33_554_432
    assert capture_limits["journal.jsonl"] == 33_554_432
    assert all(
        limit == admission._LIMIT
        for name, limit in capture_limits.items()
        if name not in ("journal.jsonl", "report.jsonl")
    )
    assert result.outcome == "ADMITTED" and result.admission == "ACCEPT"
    assert result.endpoint_status == "INCOMPLETE"
    assert result.projection is not None
    artifacts, _ = _accepted_run(tmp_path, run_id)
    assert result.projection.canonical_json == _projection_bytes(artifacts)
    _assert_grid_native(artifacts, model_id, dispatch_capture)


def test_gross_report_capture_bound_is_inclusive_and_excess_rejected(tmp_path):
    limit = admission._GROSS_REPORT_LIMIT
    file_path = tmp_path / "report.jsonl"
    file_path.write_bytes(b"x" * limit)
    directory = os.open(tmp_path, os.O_RDONLY)
    try:
        captured = admission._capture(directory, file_path.name, limit=limit)
        assert len(captured) == limit
        assert captured[:1] == b"x"
        file_path.write_bytes(b"x" * (limit + 1))
        with pytest.raises(admission._Oversize):
            admission._capture(directory, file_path.name, limit=limit)
    finally:
        os.close(directory)


def test_legacy_paid_report_capture_limit_remains_16_mib(tmp_path, monkeypatch):
    from test_spider_scenario_execution_run import _request

    request, raw = _request("legacy-paid-failed-prefix")
    original_snapshot = wire.ScenarioCodec.capture_snapshot
    failed = [False]

    def fail_initial(codec, snapshot_request):
        if not failed[0]:
            failed[0] = True
            raise RuntimeError("bounded initial capture fault")
        return original_snapshot(codec, snapshot_request)

    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", fail_initial)
    original_capture = admission._capture
    capture_limits: dict[str, int] = {}

    def observe_capture(directory, name, **kwargs):
        capture_limits[name] = kwargs.get("limit", admission._LIMIT)
        return original_capture(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", observe_capture)
    result = scenario.run_spider_scenario(
        str(tmp_path), request.historical_input.run_id, scenario._PAID_SELECTOR,
        execution_input=raw,
    )
    assert failed[0]
    assert result["outcome"] == "ADMITTED" and result["endpoint_status"] == "FAILED"
    admitted = admission.admit_spider_run(tmp_path / request.historical_input.run_id)
    assert admitted["decision"] == "ACCEPT"
    assert capture_limits["report.jsonl"] == 16_777_216
    assert capture_limits["journal.jsonl"] == 33_554_432
    assert all(
        limit == (33_554_432 if name == "journal.jsonl" else 16_777_216)
        for name, limit in capture_limits.items()
    )


def test_legacy_floating_report_capture_limit_remains_16_mib(tmp_path, monkeypatch):
    from test_spider_floating_exit_host import _candidate, _seeded_candidate_run
    from src.core.backtest.spider_floating_exit_input import encode_spider_floating_exit_input
    from src.core.backtest.synthetic_scenario_replay import _ReplayComposition

    run = _seeded_candidate_run()
    request = _candidate(run)
    raw = encode_spider_floating_exit_input(request)
    original_factory = _ReplayComposition._from_floating_exit_run.__func__

    def fail_initial_capture(cls, candidate_request):
        composition = original_factory(cls, candidate_request)

        def fail_capture(*_args, **_kwargs):
            raise RuntimeError("bounded initial capture fault")

        composition.capture_owner_evidence = fail_capture
        return composition

    monkeypatch.setattr(
        _ReplayComposition, "_from_floating_exit_run", classmethod(fail_initial_capture),
    )
    original_capture = admission._capture
    capture_limits: dict[str, int] = {}

    def observe_capture(directory, name, **kwargs):
        capture_limits[name] = kwargs.get("limit", admission._LIMIT)
        return original_capture(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", observe_capture)
    result = scenario.run_spider_scenario(
        str(tmp_path), run.run_id, "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1",
        floating_exit_input=raw,
    )
    assert result["outcome"] == "ADMITTED" and result["endpoint_status"] == "FAILED"
    admitted = admission.admit_spider_run(tmp_path / run.run_id)
    assert admitted["decision"] == "ACCEPT"
    assert capture_limits["report.jsonl"] == 16_777_216
    assert capture_limits["journal.jsonl"] == 33_554_432
    assert all(
        limit == (33_554_432 if name == "journal.jsonl" else 16_777_216)
        for name, limit in capture_limits.items()
    )
