from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
import json
import os
import re
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_historical_input import (
    HistoricalInputError, HistoricalRunInput, admit_before_construction, decode_p2_configuration,
    encode_historical_run_input, historical_context_for_input,
    historical_planned_coverage, validate_historical_input,
)
from src.core.backtest.spider_historical_replay import _ClosedMarketSnapshot
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical, decode_jsonl
from src.core.backtest.spider_run_envelope_schema import journal_record
from src.core.backtest.spider_run_store import SpiderRunStore, SpiderRunStoreError
from src.core.backtest.spider_run_evidence import build_endpoint_artifacts, build_reconciliation
from src.core.backtest.synthetic_scenario_replay import ReplayPersistenceError, _ReplayComposition
from test_spider_historical_input import (
    ANSWERS,
    ANSWERS_SHA256,
    INPUTS,
    INPUTS_SHA256,
    _initial_account_state,
    _manifest,
    _p2_configuration,
    _policy_cache,
    _rehashed_run,
    _run_with_configuration,
    _valid_run,
)
from test_spider_run_artifacts import historical_attempt


_P3_ORACLE_EPOCH_MS = 1_790_640_000_000


def _frozen_h01_input() -> dict[str, str]:
    raw = INPUTS.read_bytes()
    assert sha256(raw).hexdigest() == INPUTS_SHA256
    line = next(row for row in raw.decode("utf-8").splitlines() if row.startswith("H01|"))
    fields: dict[str, str] = {}
    for segment in line.split("|")[1:]:
        key, separator, value = segment.partition("=")
        if not separator:
            match = re.fullmatch(r"([a-z_]+)([0-9]+)", segment)
            assert match is not None
            key, value = match.groups()
        assert key not in fields
        fields[key] = value
    assert fields["case"] == "H01" and fields["base"] == "BASE_V1"
    return fields


def _h01_run(record: dict[str, str]) -> HistoricalRunInput:
    """Materialize only the frozen H01 input over admitted historical DTOs."""
    base = _valid_run()
    start = _P3_ORACLE_EPOCH_MS
    products = tuple(record["products"].split(","))
    assert products == base.ordered_products
    policy_identity, policy_hash = record["policy"].split(":sha256:")
    policy_id, policy_version = policy_identity.split("@")
    config_id = record["config"].split(":sha256:")[0]
    delta = start - base.range_start_ms
    shifted_trade = tuple(replace(row, bar_open_ms=row.bar_open_ms + delta) for row in base.trade_bars)
    shifted_marks = tuple(replace(row, bar_open_ms=row.bar_open_ms + delta) for row in base.mark_bars)
    trade_manifest = _manifest("P3_ORACLE_TRADE_V1", shifted_trade, trade=True)
    mark_manifest = _manifest("P3_ORACLE_MARK_V1", shifted_marks, trade=False)
    run = replace(
        base,
        run_id="h01-transport-only",
        account_key=record["account"],
        policy_id=policy_id,
        policy_version=policy_version,
        policy_source_sha256=policy_hash,
        strategy_identity=policy_id,
        range_start_ms=start,
        range_end_ms=start + 60_000,
        warmup_start_ms=start - 86_400_000,
        first_timer_ms=start + 5_000,
        trade_bars=shifted_trade,
        mark_bars=shifted_marks,
        trade_manifest=trade_manifest,
        mark_manifest=mark_manifest,
    )
    run = _extend_one_minute(run)

    # The input record is the only source of the changed market path.
    bars: dict[tuple[str, int], tuple[D, D, D, D, D]] = {}
    product = None
    product_ids = dict(zip(("A", "B"), products, strict=True))
    for segment in record["bars"].split(";"):
        if ":" in segment:
            label, segment = segment.split(":", 1)
            product = product_ids[label]
        assert product is not None
        offset, values = segment.split("=", 1)
        raw_open = start + (0 if offset == "E" else int(offset.removeprefix("E+")))
        bars[(product, raw_open)] = tuple(D(value) for value in values.split("/"))  # type: ignore[assignment]

    def apply_bars(rows, *, trade: bool):
        updated = []
        for row in rows:
            values = bars.get((row.product_id, row.bar_open_ms))
            if values is None:
                updated.append(row)
                continue
            opening, high, low, close, volume = values
            updated.append(replace(row, open=opening, high=high, low=low, close=close,
                                  volume=volume if trade else None))
        return tuple(updated)

    trade_rows = apply_bars(run.trade_bars, trade=True)
    mark_rows = apply_bars(run.mark_bars, trade=False)
    run = _rehashed_run(run, trade_rows=trade_rows, mark_rows=mark_rows)

    configuration = _p2_configuration()
    configuration["config_id"] = config_id
    configuration["cash"] = "1000"
    for product in configuration["products"]:
        product["taker_fee_rate"] = "0.001"
        product["liquidation_fee_rate"] = "0.00602"
        product["specs"][0].update(
            contract_value="1", multiplier="1", price_tick="1",
            quantity_step="1", minimum_quantity="1",
        )
        product["tiers"][0]["rows"][0].update(
            maximum_contracts="1000", mmr="0.005", imr="0.1", max_leverage="10",
        )
        product["marks"][0].update(valid_to=start + 120_000, mark="100")
    run = _run_with_configuration(run, configuration)
    run = replace(
        run,
        range_end_ms=start + 120_000,
        spec_before=tuple(replace(spec, effective_at_ms=start, price_tick=D("1"),
                                  quantity_step=D("1"), minimum_quantity=D("1"))
                          for spec in run.spec_before),
        spec_after=tuple(replace(spec, effective_at_ms=start + 120_000, price_tick=D("1"),
                                 quantity_step=D("1"), minimum_quantity=D("1"))
                         for spec in run.spec_after),
        initial_policy_cache=_policy_cache(
            running=True, paused=False, online=True, ws_open=False, order_id=1,
            capital={"total": "1000", "usdt": "1000", "avail": "1000", "earn": "0", "position": "0"},
            rows=[
                {"product_id": product, "name": product, "active": "true", "leverage": "1",
                 "歩差": "1", "單數": "1", "hold上限": "0.8", "hold下限": "-0.8", "hold": "0"}
                for product in run.ordered_products
            ],
            markets=[
                {"product_id": product, "price": "100", "ctVal": "1", "lotSz": "1", "minSz": "1",
                 "increment": "1", "ratioHL": "0", "state": "live", "instIdCode": code}
                for product, code in zip(run.ordered_products, (1, 2), strict=True)
            ],
            replies={}, orders={}, positions={}, last_filled_price={},
        ),
        initial_account_state=_initial_account_state(cash="1000", orders=[], positions=[]),
    )
    assert validate_historical_input(run)
    return run


def _frozen_h01_answer() -> dict[str, str]:
    raw = ANSWERS.read_bytes()
    assert sha256(raw).hexdigest() == ANSWERS_SHA256
    line = next(row for row in raw.decode("utf-8").splitlines() if row.startswith("H01|"))
    fields: dict[str, str] = {}
    for segment in line.split("|")[1:]:
        key, separator, value = segment.partition("=")
        assert separator and key not in fields
        fields[key] = value
    assert fields["endpoint"].startswith("E+60002")
    return fields


def _snapshot_event(composition: _ReplayComposition, index: int = 0) -> dict[str, object]:
    keys = sorted(key for key in composition._records if key[0] == 4)
    return composition._records[keys[index]]["item"]


def _node(run: HistoricalRunInput, open_ms: int, step: int) -> wire.HistoricalNode:
    trade = {(row.product_id, row.bar_open_ms): row for row in run.trade_bars}
    marks = {(row.product_id, row.bar_open_ms): row for row in run.mark_bars}
    bars = []
    for product in run.ordered_products:
        t, m = trade[(product, open_ms)], marks[(product, open_ms)]
        bars.append({
            "product_id": product,
            "trade": {"open": t.open, "high": t.high, "low": t.low, "close": t.close,
                      "volume_contracts": cast(D, t.volume), "confirmed": True,
                      "source_row_hash": t.source_row_hash},
            "mark": {"open": m.open, "high": m.high, "low": m.low, "close": m.close,
                     "confirmed": True, "source_row_hash": m.source_row_hash},
        })
    return {
        "schema_version": "historical_node_v1",
        "model_id": run.model_id,
        "model_version": "1",
        "run_contract_hash": validate_historical_input(run),
        "bar_open_ms": open_ms,
        "bar_duration_ms": 60_000,
        "step_index": step,
        "market_slippage_bps": run.market_slippage_bps,
        "bars": bars,
        "working_orders": [],
    }


def _composition(run: HistoricalRunInput, evidence_callback=None) -> _ReplayComposition:
    composition = _ReplayComposition._from_historical_run(run)
    composition._evidence_callback = evidence_callback
    return composition


def _count_native_session_constructions(monkeypatch) -> list[bool]:
    calls: list[bool] = []
    original = wire._new_native_session

    def count(profile: wire.Profile, account_json: str, configuration_json: str | None = None):
        calls.append(True)
        return original(profile, account_json, configuration_json)

    monkeypatch.setattr(wire, "_new_native_session", count)
    return calls


def _historical_snapshot_requests(composition, cutoff, prefix):
    return [dict(schema_version="snapshot_request_v1", account_key=composition._account,
                 snapshot_id=f"{prefix}-{kind}", snapshot_kind=kind,
                 capture_mode="OWNER_CURRENT", captured_at=cutoff)
            for kind in ("TRADING", "POSITIONS", "OPEN_ORDERS")]


def _capture_historical_owner(composition, cutoff, prefix):
    return decode_canonical(canonical_bytes(composition.capture_owner_evidence(
        cutoff, *_historical_snapshot_requests(composition, cutoff, prefix),
    )))


def _historical_endpoint_case(tmp_path, *, partial_fill, return_store=False):
    run = _valid_run()
    if partial_fill:
        trade_rows = tuple(
            replace(row, high=D("103"), low=D("98"), volume=D("0.1"))
            if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
            for row in run.trade_bars
        )
        run = _rehashed_run(run, trade_rows=trade_rows)
    run = replace(run, run_id="p3-partial-endpoint" if partial_fill else "p3-no-fill-endpoint")
    raw_input = encode_historical_run_input(run)
    configuration = decode_p2_configuration(run.configuration_bytes, run.configuration_sha256, run.ordered_products)
    config_context = dict(schema_version="spider_configuration_context_v1", config_id=configuration["config_id"],
                          configuration_sha256=run.configuration_sha256, products=list(run.ordered_products))
    history_context = historical_context_for_input(run)
    attempt = historical_attempt()
    attempt.update(run_id=run.run_id,
                   account_key={"venue": "SPIDER_HISTORICAL_RESEARCH", "environment": "RESEARCH_ONLY",
                                "account": run.account_key},
                   scenario_plan_sha256=history_context["historical_input_sha256"],
                   policy_source_sha256=run.policy_source_sha256,
                   configuration_context=config_context, historical_context=history_context,
                   planned_coverage=historical_planned_coverage(run))
    for item, key in zip(attempt["input_contract_hashes"],
                         ("scenario_plan_sha256", "program_sha256", "native_artifact_sha256", "policy_source_sha256"),
                         strict=True):
        item["sha256"] = attempt[key]
    store = SpiderRunStore.create(str(tmp_path), run.run_id)
    store.register(attempt, historical_input=raw_input)
    composition = None
    previous_owner = None

    def persist(kind, key, payload):
        nonlocal previous_owner
        if kind == "DELIVERY_ATTEMPT":
            return
        ordinal = len(decode_jsonl((store._path / "journal.jsonl").read_bytes())) + 1
        payload = decode_canonical(canonical_bytes(payload))
        before_version = after_version = None
        if kind == "SOURCE_GROUP_RESULT":
            if previous_owner is None:
                raise AssertionError("source group precedes initial owner evidence")
            payload["owner_evidence_before"] = previous_owner
            capture_at = payload["request"]["group_effective_at"]
            after = _capture_historical_owner(composition, capture_at, f"GROUP-{ordinal}")["owner_evidence"]
            payload["owner_evidence_after"] = after
            before_version = payload["result"]["account_version_before"]
            after_version = payload["result"]["account_version_after"]
            previous_owner = after
        elif kind == "HISTORICAL_MARKET_RESULT":
            before_version = payload["owner_inspection_before"]["account_version"]
            after_version = payload["owner_inspection_after"]["account_version"]
            capture_at = payload["result"]["effective_at"]
            previous_owner = _capture_historical_owner(composition, capture_at, f"MARKET-{ordinal}")["owner_evidence"]
        if kind in ("SOURCE_GROUP_RESULT", "SNAPSHOT_FACT"):
            effective = payload["request"].get("group_effective_at", payload["request"].get("captured_at"))
        else:
            effective = key["visible_at"]
        row = dict(schema_version="spider_journal_record_v1", run_id=run.run_id, journal_seq=ordinal,
                   barrier_id=key["stable_id"], record_kind=kind, scheduler_key=key, causal_parent_ids=[],
                   effective_at=effective, visible_at=key["visible_at"], account_version_before=before_version,
                   account_version_after=after_version, payload=payload,
                   configuration_context=config_context, historical_context=history_context)
        store.mark_processed(dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=key["stable_id"]), record_kind=kind)
        store.append_journal(row)

    composition = _composition(run, persist)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms
    initial = _capture_historical_owner(composition, start * 16, "INITIAL")["owner_evidence"]
    previous_owner = initial
    cutoff = run.range_end_ms * 16 + 6
    result = composition._dispatch_due(cutoff)
    assert result["classification"] == "SUCCESS"
    final_capture = _capture_historical_owner(composition, cutoff, "FINAL")
    journal = decode_jsonl((store._path / "journal.jsonl").read_bytes())
    status = store._status("RUNNING")
    artifacts = build_endpoint_artifacts(run.run_id, attempt, status, journal, initial,
                                         final_capture["owner_evidence"], final_capture["scheduler_observation"])
    result = (run, attempt, status, journal, artifacts, composition)
    return (*result, store) if return_store else result


def _extend_one_minute(run: HistoricalRunInput) -> HistoricalRunInput:
    next_open = run.range_end_ms
    trade, marks = [], []
    for product in run.ordered_products:
        for source, target in ((run.trade_bars, trade), (run.mark_bars, marks)):
            prior = next(row for row in reversed(source) if row.product_id == product)
            target.append(replace(prior, bar_open_ms=next_open, source_sequence=prior.source_sequence + 1))
    run = _rehashed_run(run, trade_rows=(*run.trade_bars, *trade), mark_rows=(*run.mark_bars, *marks))
    return replace(
        run,
        range_end_ms=next_open + 60_000,
        trade_manifest=replace(run.trade_manifest, source_row_count=len(run.trade_bars),
                               normalized_row_count=len(run.trade_bars), last_source_timestamp_ms=next_open),
        mark_manifest=replace(run.mark_manifest, source_row_count=len(run.mark_bars),
                              normalized_row_count=len(run.mark_bars), last_source_timestamp_ms=next_open),
        spec_after=tuple(replace(spec, effective_at_ms=next_open + 60_000) for spec in run.spec_after),
    )


def test_historical_bootstrap_restores_policy_cache_and_keeps_detached_order_position_evidence():
    run = _valid_run()
    cache = _policy_cache(
        running=False,
        paused=True,
        online=False,
        ws_open=True,
        order_id=9,
        capital={"total": "1200", "usdt": "1100", "avail": "900", "earn": "100", "position": "25"},
        rows=[
            {"product_id": "A-USDT-SWAP", "name": "A-USDT-SWAP", "active": "false", "leverage": "3",
             "歩差": "0.02", "單數": "4", "hold上限": "0.8", "hold下限": "-0.8", "hold": "0.25"},
            {"product_id": "B-USDT-SWAP", "name": "B-USDT-SWAP", "active": "true", "leverage": "2",
             "歩差": "0.03", "單數": "5", "hold上限": "0.9", "hold下限": "-0.9", "hold": "-0.5"},
        ],
        markets=[
            {"product_id": "A-USDT-SWAP", "price": "77", "ctVal": "1", "lotSz": "1", "minSz": "1",
             "increment": "1", "ratioHL": "0.2", "state": "live", "instIdCode": 1},
            {"product_id": "B-USDT-SWAP", "price": "88", "ctVal": "1", "lotSz": "1", "minSz": "1",
             "increment": "1", "ratioHL": "0.3", "state": "live", "instIdCode": 2},
        ],
        replies={"A-USDT-SWAP": {"reply": "kept"}},
        orders={"A-USDT-SWAP": {"order_id": "evidence-only"}},
        positions={"B-USDT-SWAP": {"quantity": "evidence-only"}},
        last_filled_price={"A-USDT-SWAP": {"buy": "1.25", "sell": "0"}},
    )
    run = replace(run, initial_policy_cache=cache)
    composition = _composition(run)
    policy = composition._policy
    assert (policy.running, policy.paused, policy.online, policy.ws_open, policy.order_id) == (False, True, False, True, 9)
    assert [row["name"] for row in policy.rows] == list(run.ordered_products)
    assert policy.rows[0]["active"] == "false" and policy.rows[1]["hold"] == "-0.5"
    assert list(policy.markets) == list(run.ordered_products)
    assert policy.markets["A-USDT-SWAP"]["price"] == "77"
    assert policy.capital["total"] == "1200" and policy.replies == {"A-USDT-SWAP": {"reply": "kept"}}
    assert policy.last_filled_price == {"A-USDT-SWAP": {"buy": "1.25", "sell": "0"}}
    assert policy.parameters["defaultN"] == D("1")
    assert composition._historical_initial_cache_evidence == {
        "orders": {"A-USDT-SWAP": {"order_id": "evidence-only"}},
        "positions": {"B-USDT-SWAP": {"quantity": "evidence-only"}},
    }
    assert composition._historical_initial_cache_evidence["orders"] is not cast(dict, decode_cache(run))["orders"]


def decode_cache(run: HistoricalRunInput) -> dict[str, object]:
    from src.core.backtest.spider_run_artifacts import decode_canonical
    return cast(dict[str, object], decode_canonical(run.initial_policy_cache))


def test_historical_account_identity_uses_frozen_research_namespace_only():
    first = _composition(_valid_run())
    second = _composition(replace(_valid_run(), account_key="another-account"))
    expected = {"venue": "SPIDER_HISTORICAL_RESEARCH", "environment": "RESEARCH_ONLY", "account": "account"}
    assert first._codec.inspect_state()["account_key"] == first._account == expected
    assert second._codec.inspect_state()["account_key"] == second._account == {**expected, "account": "another-account"}
    assert first._codec.inspect_state()["account_key"] != second._codec.inspect_state()["account_key"]


def test_invalid_historical_run_constructs_neither_composition_nor_native_owner(monkeypatch):
    composition_calls = []
    owner_calls = _count_native_session_constructions(monkeypatch)
    original_init = _ReplayComposition.__init__

    def count_composition(self, *args, **kwargs):
        composition_calls.append(True)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(_ReplayComposition, "__init__", count_composition)
    with pytest.raises(HistoricalInputError):
        _composition(replace(_valid_run(), policy_id=""))
    assert composition_calls == [] and owner_calls == []


def _assert_rejected_before_owner(run: HistoricalRunInput, monkeypatch, *, message: str | None = None) -> None:
    composition_calls = []
    owner_calls = _count_native_session_constructions(monkeypatch)
    original_init = _ReplayComposition.__init__

    def count_composition(self, *args, **kwargs):
        composition_calls.append(True)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(_ReplayComposition, "__init__", count_composition)
    with pytest.raises(HistoricalInputError, match=message):
        _composition(run)
    assert composition_calls == [] and owner_calls == []


@pytest.mark.parametrize("case", [
    "no_active", "multiple_active", "ends_before_last", "ends_at_last",
    "second_spec_overlaps_during_run",
    "contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity",
])
def test_configured_spec_semantics_reject_before_composition_or_native_owner(case, monkeypatch):
    run = _valid_run()
    if case in {"ends_before_last", "ends_at_last"}:
        run = _extend_one_minute(run)
    configuration = _p2_configuration()
    first = configuration["products"][0]["specs"][0]
    start, end = run.range_start_ms, run.range_end_ms
    last_open = end - run.bar_duration_ms
    evidence_product = 1 if case in {"multiplier", "quantity_step"} else 0
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = end
    if case == "no_active":
        first["valid_from"] = start + 1
    elif case == "multiple_active":
        configuration["products"][0]["specs"].append({**first, "version": "spec-v2"})
    elif case == "ends_before_last":
        first["valid_to"] = last_open - 1
    elif case == "ends_at_last":
        first["valid_to"] = last_open
    elif case == "second_spec_overlaps_during_run":
        run = _extend_one_minute(run)
        start, end = run.range_start_ms, run.range_end_ms
        last_open = end - run.bar_duration_ms
        configuration["products"][0]["specs"].append({
            **first, "version": "spec-v2", "valid_from": start + run.bar_duration_ms,
        })
    else:
        product = configuration["products"][evidence_product]
        product["specs"][0][case] = "2"
    run = _run_with_configuration(run, configuration)
    message = ("configured product spec differs from run evidence"
               if case in {"contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity"}
               else None)
    _assert_rejected_before_owner(run, monkeypatch, message=message)


def test_configured_spec_coherent_baseline_is_accepted():
    composition = _composition(_valid_run())
    assert composition._policy.markets["A-USDT-SWAP"]["increment"] == "0.01"
    assert composition._policy.markets["A-USDT-SWAP"]["lotSz"] == "0.001"


def test_configured_spec_exact_run_end_boundary_is_valid_and_uses_run_cache(monkeypatch):
    run = _valid_run()
    configuration = _p2_configuration()
    for product in configuration["products"]:
        first = product["specs"][0]
        first["valid_to"] = run.range_end_ms
        product["specs"].append({**first, "version": "spec-v2", "valid_from": run.range_end_ms,
                                 "valid_to": None})
        product["marks"][0]["valid_to"] = 200_000_000
    run = _run_with_configuration(run, configuration)
    # Historical timers now perform real Policy polling; keep this cache test
    # isolated from intentional grid-order acceptance.
    run = replace(run, initial_policy_cache=_policy_cache(running=False))
    owner_calls = _count_native_session_constructions(monkeypatch)
    composition = _composition(run)
    assert len(owner_calls) == 1
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    final_close = composition._records[(3, f"P3_MARKET_{run.range_start_ms}_3")]
    assert final_close["result"]["classification"] == "SUCCESS"
    assert composition._records[(4, f"MARKET_CLOSE_{run.range_end_ms}")]["result"]["classification"] == "SUCCESS"
    for market in composition._policy.markets.values():
        assert (market["ctVal"], market["lotSz"], market["minSz"], market["increment"]) == (
            D("1"), D("0.001"), D("0.001"), D("0.01"))


def test_h07_real_spec_crossing_remains_scenario_only_before_owner_or_store():
    raw_inputs = INPUTS.read_bytes()
    assert sha256(raw_inputs).hexdigest() == INPUTS_SHA256
    h07 = next(line for line in raw_inputs.decode().splitlines() if line.startswith("H07|"))
    fields = dict(part.split("=", 1) for part in h07.split("|")[1:] if "=" in part)
    assert fields["config"].endswith("0915384c69cc5d2e2b16c59a024a436c19d79302c02b634f2894184a5786bc78")
    assert fields["scenario_only"] == "true"

    run = _extend_one_minute(_valid_run())
    start, boundary, end = run.range_start_ms, run.range_start_ms + 60_000, run.range_end_ms
    configuration = _p2_configuration()
    for index, product in enumerate(configuration["products"]):
        spec = product["specs"][0]
        spec.update(version="H07-SPEC-V1", valid_from=0, valid_to=None,
                    contract_value="1", multiplier="1", price_tick="1",
                    quantity_step="1", minimum_quantity="1")
        if index == 0:
            spec["valid_to"] = boundary
            product["specs"].append({**spec, "version": "H07-SPEC-V2", "valid_from": boundary,
                                     "valid_to": None, "price_tick": "5"})
        product["marks"][0]["valid_to"] = end
    run = _run_with_configuration(run, configuration)
    run = replace(
        run,
        spec_before=tuple(replace(spec, effective_at_ms=start, price_tick=D("1"),
                                  quantity_step=D("1"), minimum_quantity=D("1"))
                          for spec in run.spec_before),
        spec_after=tuple(replace(spec, effective_at_ms=end,
                                 price_tick=D("5") if spec.product_id == "A-USDT-SWAP" else D("1"),
                                 quantity_step=D("1"), minimum_quantity=D("1"))
                         for spec in run.spec_after),
    )

    calls = []
    with pytest.raises(HistoricalInputError, match="spec/tier changed across run"):
        admit_before_construction(
            run, lambda digest: calls.append(("owner", digest)),
            lambda digest: calls.append(("store", digest)),
        )
    assert calls == []


def test_market_cache_uses_exact_1440_closed_bars_and_ignores_future_until_its_close():
    base = _extend_one_minute(_valid_run())
    outside_open = base.warmup_start_ms
    included_open = base.range_start_ms - 86_340_000
    altered = []
    for row in base.trade_bars:
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == outside_open:
            altered.append(replace(row, high=D("1000"), low=D("1")))
        elif row.product_id == "A-USDT-SWAP" and row.bar_open_ms == included_open:
            altered.append(replace(row, high=D("200"), low=D("100")))
        else:
            altered.append(row)
    common = _rehashed_run(base, trade_rows=tuple(altered))
    future_rows = list(common.trade_bars)
    future_index = next(i for i, row in enumerate(future_rows)
                        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == common.range_start_ms + 60_000)
    future_rows[future_index] = replace(future_rows[future_index], high=D("300"), low=D("90"), close=D("150"))
    different_future = _rehashed_run(common, trade_rows=tuple(future_rows))
    left, right = _composition(common), _composition(different_future)
    for composition in (left, right):
        for row in composition._policy.rows:
            row["active"] = "false"
    close_ms = common.range_start_ms + 60_000
    due = close_ms * 16 + 6
    assert left._policy.markets["A-USDT-SWAP"]["price"] == "100"
    assert left._dispatch_due(due - 1)["classification"] == "SUCCESS"
    assert left._policy.markets["A-USDT-SWAP"]["price"] == "100"
    assert left._dispatch_due(due)["classification"] == "SUCCESS"
    assert right._dispatch_due(due)["classification"] == "SUCCESS"
    assert left._policy.markets == right._policy.markets
    assert left._policy.events == right._policy.events
    assert [(event["at_ms"] - common.range_start_ms, event["kind"])
            for event in left._policy.events] == [
                (5_000, "request_earn"), (5_020, "check_websocket"),
                *((offset + 15, "check_websocket") for offset in range(10_000, 60_000, 5_000)),
            ]
    snapshot_left = left._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]
    snapshot_right = right._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]
    assert snapshot_left == snapshot_right
    rows = snapshot_left["markets"]
    assert [row["product_id"] for row in rows] == ["A-USDT-SWAP", "B-USDT-SWAP"]
    assert [(row["ctVal"], row["lotSz"], row["minSz"], row["increment"]) for row in rows] == [
        (D("1"), D("0.001"), D("0.001"), D("0.01")),
        (D("1"), D("0.001"), D("0.001"), D("0.01"))]
    assert rows[0]["ratioHL"] == D("100")
    assert rows[1]["ratioHL"] == D("0")
    assert rows[0]["price"] == D("100")
    later = common.range_end_ms * 16 + 6
    assert left._dispatch_due(later)["classification"] == "SUCCESS"
    assert right._dispatch_due(later)["classification"] == "SUCCESS"
    assert left._policy.markets["A-USDT-SWAP"]["price"] == D("100")
    assert right._policy.markets["A-USDT-SWAP"]["price"] == D("150")
    assert right._policy.markets["A-USDT-SWAP"]["ratioHL"] > left._policy.markets["A-USDT-SWAP"]["ratioHL"]


def test_market_cache_duplicates_conflicts_and_out_of_order_are_terminal_before_policy_mutation():
    composition = _composition(_valid_run())
    item = _snapshot_event(composition)
    before = deepcopy(composition._policy.markets)
    assert composition._enqueue(item)["classification"] == "PENDING"
    assert composition._enqueue(deepcopy(item))["classification"] == "PENDING"
    changed = deepcopy(item)
    snapshot = cast(_ClosedMarketSnapshot, changed["snapshot"])
    changed["snapshot"] = replace(snapshot, markets=(replace(snapshot.markets[0], price=D("99")), *snapshot.markets[1:]))
    conflict = composition._enqueue(changed)
    assert conflict["classification"] == "TERMINAL" and conflict["reason"] == "INVALID_SCHEMA"
    assert composition._policy.markets == before


def test_step3_cache_and_next_open_dispatch_in_frozen_phase_order(monkeypatch):
    run = _extend_one_minute(_valid_run())
    config = _p2_configuration()
    for product in config["products"]:
        product["marks"][0]["valid_to"] = 200_000_000
    run = _run_with_configuration(run, config)
    run = replace(run, initial_policy_cache=_policy_cache(running=False))
    composition = _composition(run)
    calls = []
    original_step = wire.ScenarioCodec.historical_market_step
    original_market = composition._policy.apply_market

    def tracked_step(codec, node):
        calls.append(("STEP", node["step_index"]))
        return original_step(codec, node)

    def tracked_market(markets):
        calls.append(("CACHE", tuple(markets)))
        original_market(markets)

    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", tracked_step)
    monkeypatch.setattr(composition._policy, "apply_market", tracked_market)
    close_ms = run.range_start_ms + 60_000
    assert composition._dispatch_due(close_ms * 16 + 7)["classification"] == "SUCCESS"
    assert [entry[0] for entry in calls] == ["STEP", "STEP", "STEP", "STEP", "CACHE", "STEP"]
    assert [entry[1] for entry in calls if entry[0] == "STEP"] == [0, 1, 2, 3, 0]
    assert composition._policy.events
    assert all(run.range_start_ms < event["at_ms"] < close_ms
               and (event["at_ms"] - run.range_start_ms) % 5_000 == 0
               for event in composition._policy.events)
    assert composition._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]["close_ms"] == close_ms


def test_historical_poll_timers_use_raw_policy_and_effective_scheduler_clocks():
    run = _valid_run()
    composition = _composition(run)
    policy = composition._policy
    assert (policy.now_ms, policy.shared_ms, policy.last_earn_ms, policy.last_market_ms,
            policy.reset_ms, policy.last_capital) == (
        run.range_start_ms, run.range_start_ms - 12_000, run.range_start_ms - 61_000, 0, {}, None)
    timer_records = [record for record in composition._records.values()
                     if record["item"]["kind"] == "HISTORICAL_TIMER"]
    assert [row["raw_time_ms"] for row in timer_records] == list(
        range(run.range_start_ms + 5_000, run.range_end_ms, 5_000))
    first_raw = run.range_start_ms + 5_000
    assert composition._dispatch_due(first_raw * 16 + 9)["classification"] == "SUCCESS"
    first = composition._records[(5, f"P3_TIMER_{first_raw}")]["result"]
    assert first["events"] == ({"at_ms": first_raw, "kind": "request_earn"},)
    assert policy.now_ms == first_raw
    assert composition._current_time == first_raw * 16 + 9
    assert len(composition._continuations) == 1

    earn = composition._records[(1, f"P3_POLL_{first_raw}_EARN")]["item"]
    request = earn["request"]
    assert request["capture_mode"] == "FROZEN_POLL_FIXTURE"
    assert request["fixture_key"] == "P3_POLL_EARN_ZERO"
    assert request["captured_at"] == (first_raw + 1) * 16 + 5
    assert earn["delivery_projection"]["visible_at"] == (first_raw + 5) * 16 + 6
    assert composition._records[(1, request["snapshot_id"])]["key"][0] == request["captured_at"]
    first_poll = composition._polls[f"P3_POLL_{first_raw}"]
    assert [(stage["snapshot_request"]["captured_at"], stage["delivery_projection"]["visible_at"])
            for stage in first_poll.stages] == [
        ((first_raw + 1) * 16 + 5, (first_raw + 5) * 16 + 6),
        ((first_raw + 6) * 16 + 5, (first_raw + 10) * 16 + 6),
        ((first_raw + 11) * 16 + 5, (first_raw + 15) * 16 + 6),
        ((first_raw + 16) * 16 + 5, (first_raw + 20) * 16 + 6),
    ]
    assert composition._dispatch_due(request["captured_at"])["classification"] == "SUCCESS"
    earn_delivery = next(record["item"]["delivery"] for record in composition._records.values()
                         if record["item"]["kind"] == "DELIVERY"
                         and record["item"]["delivery"]["continuation_id"] == f"P3_CONT_{first_raw}"
                         and record["item"]["delivery"]["payload_kind"] == "EARN_SNAPSHOT")
    assert earn_delivery["immutable_payload"] == {"outcome": "SUCCESS", "earn": D(0)}
    assert earn_delivery["snapshot_as_of"] == request["captured_at"]
    delivery_key = next(record["key"][0] for record in composition._records.values()
                        if record["item"]["kind"] == "DELIVERY"
                        and record["item"]["delivery"]["delivery_id"] == earn_delivery["delivery_id"])
    assert delivery_key == earn["delivery_projection"]["visible_at"]

    second_raw = first_raw + 5_000
    assert composition._dispatch_due(second_raw * 16 + 9)["classification"] == "SUCCESS"
    second_poll = composition._polls[f"P3_POLL_{second_raw}"]
    assert [stage["snapshot_request"]["snapshot_kind"] for stage in second_poll.stages] == [
        "TRADING", "POSITIONS", "OPEN_ORDERS"]
    assert [(stage["snapshot_request"]["captured_at"], stage["delivery_projection"]["visible_at"])
            for stage in second_poll.stages] == [
        ((second_raw + 1) * 16 + 5, (second_raw + 5) * 16 + 6),
        ((second_raw + 6) * 16 + 5, (second_raw + 10) * 16 + 6),
        ((second_raw + 11) * 16 + 5, (second_raw + 15) * 16 + 6),
    ]
    assert [stage["snapshot_request"]["capture_mode"] for stage in second_poll.stages] == [
        "OWNER_CURRENT", "OWNER_CURRENT", "OWNER_CURRENT"]
    assert policy.now_ms == second_raw


def test_historical_timer_phase_follows_same_time_market_cache_and_excludes_endpoint():
    run = _extend_one_minute(_valid_run())
    composition = _composition(run)
    same_time = run.range_start_ms + 60_000
    assert composition._dispatch_due(same_time * 16 + 9)["classification"] == "SUCCESS"
    assert composition._records[(4, f"MARKET_CLOSE_{same_time}")]["result"]["classification"] == "SUCCESS"
    timer = composition._records[(5, f"P3_TIMER_{same_time}")]
    assert timer["result"]["classification"] == "SUCCESS"
    assert composition._last_popped == (same_time * 16 + 9, 5, 11, f"P3_TIMER_{same_time}")
    assert len(composition._continuations) == 12

    endpoint = _composition(_valid_run())
    end = run.range_start_ms + 60_000
    assert all(record["item"]["raw_at"] != end for record in endpoint._records.values()
               if record["item"]["kind"] == "HISTORICAL_TIMER")


def test_historical_poll_sends_become_native_groups_only_after_acceptance_tick():
    run = _extend_one_minute(_valid_run())
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    parent_raw = run.range_start_ms + 5_020
    assert composition._dispatch_due(parent_raw * 16 + 6)["classification"] == "SUCCESS"
    actions = [action for events in composition._audit.values() for event in events
               for action in event.get("actions", [])]
    assert actions and all(action["status"] == "UNSUBMITTED" for action in actions)
    groups = [record for record in composition._records.values()
              if record["item"]["kind"] == "SOURCE_GROUP"
              and record["item"]["group"]["ordering_contract_id"] == "HISTORICAL_ORDER_V1"]
    assert len(groups) == 4
    assert len({record["key"][0] for record in groups}) == 1
    accepted_at = (parent_raw + run.order_accept_delay_ms) * 16 + 3
    assert {record["key"][0] for record in groups} == {accepted_at}
    assert [record["key"][2] for record in groups] == list(range(4))
    assert [record["item"]["group"]["members"][0]["payload"]["client_order_id"]
            for record in groups] == [
        order["clOrdId"] for event in composition._policy.events
        if event["kind"] == "send" for order in event["orders"]
    ]
    assert all(record["result"]["classification"] == "PENDING" for record in groups)
    assert all(action["status"] == "UNSUBMITTED" for action in actions)
    orders_before_accept = composition._codec.inspect_state()["orders_digest"]
    assert composition._dispatch_due(accepted_at - 1)["classification"] == "SUCCESS"
    assert composition._codec.inspect_state()["orders_digest"] == orders_before_accept
    assert composition._dispatch_due(accepted_at)["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED" for record in groups)
    assert all(action["status"] == "SUBMITTED" for action in actions if action["group_id"] is not None)
    assert composition._codec.inspect_state()["orders_digest"] != orders_before_accept
    assert all(record["key"][3] in {action["group_id"] for action in actions} for record in groups)


def test_frozen_h01_runs_policy_poll_native_fill_notice_and_endpoint_oracle():
    source = _frozen_h01_input()
    run = _h01_run(source)
    composition = _composition(run)
    start = run.range_start_ms
    endpoint = start + 60_002

    # The market opens at E before the first poll can create any owner orders.
    first_open = next(record for record in composition._records.values()
                      if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                      and record["item"]["node"]["bar_open_ms"] == start
                      and record["item"]["node"]["step_index"] == 0)
    assert composition._dispatch_due(start * 16 + 7)["classification"] == "SUCCESS"
    first_result = first_open["result"]["historical_result"]
    assert all(not product["fills"] for product in first_result["products"])
    assert composition._codec.historical_working_orders() == []

    # The frozen input's initial ordered poll is visible at E+5020.
    poll_visible = start + 5_020
    assert composition._dispatch_due(poll_visible * 16 + 6)["classification"] == "SUCCESS"
    sends = [event for event in composition._policy.events
             if event["at_ms"] == poll_visible and event["kind"] == "send"]
    actual_source_orders = [
        (order["instId"], "LONG" if order["side"] == "buy" else "SHORT",
         D(order["sz"]), D(order["px"]))
        for event in sends for order in event["orders"]
    ]
    assert [event["orders"][0]["instId"] for event in sends] == list(run.ordered_products)

    groups = [record for record in composition._records.values()
              if record["item"]["kind"] == "SOURCE_GROUP"
              and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"]
    assert len(groups) == 4
    accepted_at = (poll_visible + run.order_accept_delay_ms) * 16 + 3
    assert {record["key"][0] for record in groups} == {accepted_at}
    assert all(record["result"]["classification"] == "PENDING" for record in groups)
    assert composition._codec.historical_working_orders() == []
    assert composition._dispatch_due(accepted_at - 1)["classification"] == "SUCCESS"
    assert composition._codec.historical_working_orders() == []
    assert composition._dispatch_due(accepted_at)["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED" for record in groups)
    accepted_orders = composition._codec.historical_working_orders()
    assert [(order["product_id"], order["side"], order["limit_price"],
             order["remaining_quantity_contracts"]) for order in accepted_orders] == [
        ("A-USDT-SWAP", "LONG", D("50"), D("1")),
        ("A-USDT-SWAP", "SHORT", D("200"), D("1")),
        ("B-USDT-SWAP", "LONG", D("50"), D("1")),
        ("B-USDT-SWAP", "SHORT", D("200"), D("1")),
    ]
    accept_snapshot = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="H01_ACCEPT_TRADING", snapshot_kind="TRADING",
        capture_mode="OWNER_CURRENT", captured_at=accepted_at,
    ))["immutable_payload"]

    # Every intervening completed poll sees the same four native orders and emits no repair.
    for offset in range(10_000, 60_000, 5_000):
        visible = start + offset + 20
        assert composition._dispatch_due(visible * 16 + 6)["classification"] == "SUCCESS"
        if offset < 60_000:
            assert composition._polls[f"P3_POLL_{start + offset}"].observation.status == "COMPLETED"
            assert len(composition._codec.historical_working_orders()) == 4
    assert len([record for record in composition._records.values()
                if record["item"]["kind"] == "SOURCE_GROUP"
                and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"]) == 4

    # The next bar opens at 45. Only A LONG fills; execution notice is separate and delayed.
    fill_time = start + 60_000
    assert composition._dispatch_due(fill_time * 16 + 9)["classification"] == "SUCCESS"
    market_steps = [record for record in composition._records.values()
                    if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"]
    fill_result = next(record["result"]["historical_result"] for record in market_steps
                       if record["item"]["node"]["bar_open_ms"] == fill_time
                       and record["item"]["node"]["step_index"] == 0)
    fills = [fill for product in fill_result["products"] for fill in product["fills"]]
    assert len(fills) == 1
    fill = fills[0]
    selected = next(order for order in accepted_orders if order["order_id"] == fill["order_id"])
    assert (selected["product_id"], selected["side"], selected["limit_price"],
            fill["quantity_contracts"], fill["price"]) == (
        "A-USDT-SWAP", "LONG", D("50"), D("1"), D("45"))
    after_fill = composition._codec.inspect_state()
    assert (after_fill["cash"], after_fill["gross_realized"], after_fill["total_fees"]) == (
        D("999.955"), D("0"), D("0.045"))

    notice_visible = (fill_time + run.order_notice_delay_ms) * 16 + 6
    notice = next(record for record in composition._records.values()
                  if record["item"]["kind"] == "DELIVERY"
                  and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
                  and record["item"]["delivery"]["source_fact_id"] == fill["source_event_id"])
    assert notice["key"][0] == notice_visible
    assert notice["item"]["delivery"]["immutable_payload"]["limit_price"] == D("50")
    assert notice["item"]["delivery"]["immutable_payload"]["fill_price"] == D("45")
    assert composition._dispatch_due(notice_visible)["classification"] == "SUCCESS"
    after_notice = composition._codec.inspect_state()
    for field in ("account_version", "cash", "gross_realized", "total_fees",
                  "positions_digest", "orders_digest"):
        assert after_notice[field] == after_fill[field]
    derived_actions = [action for events in composition._audit.values() for event in events
                       for action in event.get("actions", [])
                       if action.get("group_id") is not None
                       and action["group_id"] not in {record["item"]["group"]["group_id"] for record in groups}]
    assert len(derived_actions) == 3
    assert all(action["status"] == "UNSUBMITTED" for action in derived_actions)
    child_groups = [record for record in composition._records.values()
                    if record["item"]["kind"] == "SOURCE_GROUP"
                    and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"
                    and record["item"]["group"]["members"][0]["stamp"]["source_sequence"] >= 4]
    child_groups.sort(key=lambda record: record["item"]["group"]["members"][0]["stamp"]["source_sequence"])
    assert len(child_groups) == 3
    assert all(record["key"][0] == (endpoint + 1) * 16 + 3 for record in child_groups)
    assert all(record["result"]["classification"] == "PENDING" for record in child_groups)
    notice_audit = composition._audit[notice["item"]["delivery"]["delivery_id"]]
    notice_actions = [entry["event"] for entry in notice_audit
                      if entry["event"]["kind"] in ("cancel", "send")]
    assert [entry["kind"] for entry in notice_actions] == ["cancel", "send"]
    initial_a_short = next(order["order_id"] for order in accepted_orders
                           if order["product_id"] == "A-USDT-SWAP" and order["side"] == "SHORT")
    assert [row["ordId"] for row in notice_actions[0]["orders"]] == [initial_a_short]
    assert [row["instId"] for row in notice_actions[0]["orders"]] == ["A-USDT-SWAP"]

    # At the frozen H01 observation endpoint, only the original A-short and B grid remain.
    assert composition._dispatch_due(endpoint * 16 + 6)["classification"] == "SUCCESS"
    pending_poll = composition._polls[f"P3_POLL_{fill_time}"]
    assert pending_poll.observation.status == "IN_PROGRESS"
    endpoint_inspection = composition._codec.inspect_state()
    assert (endpoint_inspection["cash"], endpoint_inspection["gross_realized"],
            endpoint_inspection["total_fees"]) == (D("999.955"), D("0"), D("0.045"))
    trading = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="H01_ENDPOINT_TRADING", snapshot_kind="TRADING",
        capture_mode="OWNER_CURRENT", captured_at=endpoint * 16 + 6,
    ))["immutable_payload"]
    positions = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="H01_ENDPOINT_POSITIONS", snapshot_kind="POSITIONS",
        capture_mode="OWNER_CURRENT", captured_at=endpoint * 16 + 6,
    ))["immutable_payload"]["rows"]
    open_orders = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="H01_ENDPOINT_ORDERS", snapshot_kind="OPEN_ORDERS",
        capture_mode="OWNER_CURRENT", captured_at=endpoint * 16 + 6,
    ))["immutable_payload"]["rows"]
    assert trading["equity"] == D("999.955")
    assert [(row["product_id"], row["position_contracts"], row["last_price"], row["notional_usd"])
            for row in positions] == [("A-USDT-SWAP", D("1"), D("45"), D("45"))]
    actual_open = [(row["product_id"], "LONG" if row["side"] == "buy" else "SHORT",
                    row["original_size_contracts"], row["limit_price"])
                   for row in open_orders]
    assert set(actual_open) == {
        ("A-USDT-SWAP", "SHORT", D("1"), D("200")),
        ("B-USDT-SWAP", "LONG", D("1"), D("50")),
        ("B-USDT-SWAP", "SHORT", D("1"), D("200")),
    }

    # Read the answer record only after runtime behavior is complete; it is never input to replay.
    answer = _frozen_h01_answer()
    expected_source = []
    for product_row in answer["source_orders"].split(";"):
        label, orders = product_row.split(":", 1)
        product = dict(zip(("A", "B"), run.ordered_products, strict=True))[label]
        for encoded in orders.split(","):
            match = re.fullmatch(r"(LONG|SHORT)([0-9.]+)@([0-9.]+)", encoded)
            assert match is not None
            expected_source.append((product, match.group(1), D(match.group(2)), D(match.group(3))))
    assert actual_source_orders == expected_source
    open_match = re.fullmatch(
        r"E\+\d+:(A_LONG)([0-9.]+)@([0-9.]+)_gaps_fills([0-9.]+)@([0-9.]+),"
        r"fee([0-9.]+),cash/equity([0-9.]+)", answer["open"])
    assert open_match is not None
    assert (selected["side"], fill["quantity_contracts"], fill["price"]) == (
        "LONG", D(open_match.group(4)), D(open_match.group(5)))
    expected_accept = int(re.search(r"E\+(\d+)", answer["accepts"]).group(1))
    assert accepted_at == (start + expected_accept) * 16 + 3
    expected_equity = D(open_match.group(7))
    expected_fee = D(open_match.group(6))
    assert (after_fill["cash"], after_fill["total_fees"], trading["equity"]) == (
        expected_equity, expected_fee, expected_equity)
    notice_expected = answer["notice"].split(":", 1)[1].split(";")
    assert notice_expected == ["cancel_A_SHORT", "LONG3@25", "SHORT2@100"]
    replacement_orders = [order for event in notice_actions if event["kind"] == "send"
                          for order in event["orders"]]
    assert [("LONG" if order["side"] == "buy" else "SHORT", D(order["sz"]), D(order["px"]))
            for order in replacement_orders] == [
        ("LONG", D("3"), D("25")), ("SHORT", D("2"), D("100")),
    ]
    assert answer["endpoint"].startswith("E+60002,")
    assert "children>=E+60003_UNSUBMITTED" in answer["endpoint"]
    assert len(derived_actions) == len(notice_expected)
    accepted_used = next(value for key, value in answer.items() if key.startswith("accepted_used"))
    accepted_match = re.fullmatch(
        r"exposure([0-9.]+)\+long_loss([0-9.]+)\+all_fee_holds([0-9.]+),available([0-9.]+)",
        accepted_used,
    )
    assert accepted_match is not None
    expected_available = D(accepted_match.group(4))
    expected_used = sum((D(accepted_match.group(index)) for index in (1, 2, 3)), D(0))
    assert expected_used == D("20.5")
    assert accept_snapshot["equity"] - accept_snapshot["available_equity"] == expected_used
    assert accept_snapshot["available_equity"] == expected_available


def test_seed_limit_projection_is_ranked_and_fills_only_after_seed_effective_at():
    run = _valid_run()
    start = run.range_start_ms
    config = json.loads(run.configuration_bytes)
    config["seed_effective_at"] = start
    # Reverse configuration order intentionally: native seed ordering is the
    # owner_order_id lexical rank, not input-array order.
    seed_orders = [
        {
            "intent_id": "SEED-INTENT-2",
            "order_id": "SEED-ORDER-2",
            "client_order_id": "SEED-CLIENT-2",
            "strategy_id": "seed-policy",
            "product_id": "A-USDT-SWAP",
            "side": "LONG",
            "limit_price": "101",
            "reduce_only": False,
            "original_quantity_contracts": "0.5",
            "filled_quantity_contracts": "0",
            "canceled_quantity_contracts": "0",
            "remaining_quantity_contracts": "0.5",
            "status": "OPEN",
        },
        {
            "intent_id": "SEED-INTENT-1",
            "order_id": "SEED-ORDER-1",
            "client_order_id": "SEED-CLIENT-1",
            "strategy_id": "seed-policy",
            "product_id": "A-USDT-SWAP",
            "side": "LONG",
            "limit_price": "101",
            "reduce_only": False,
            "original_quantity_contracts": "0.5",
            "filled_quantity_contracts": "0",
            "canceled_quantity_contracts": "0",
            "remaining_quantity_contracts": "0.5",
            "status": "OPEN",
        },
    ]
    config["orders"] = seed_orders
    run = _run_with_configuration(run, config)
    run = replace(
        run,
        initial_account_state=_initial_account_state(orders=seed_orders),
        initial_policy_cache=_policy_cache(running=False),
    )
    trade_rows = tuple(
        replace(row, high=D("120"), low=D("80"), close=D("110"), volume=D("1"))
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == start
        else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    assert validate_historical_input(run)

    composition = _composition(run)
    projection = composition._codec.historical_working_orders()
    assert [
        (
            order["order_id"],
            order["product_id"],
            order["side"],
            order["limit_price"],
            order["remaining_quantity_contracts"],
            order["accepted_at"],
            order["accepted_source_sequence"],
            order["order_kind"],
        )
        for order in projection
    ] == [
        (
            "SEED-ORDER-1",
            "A-USDT-SWAP",
            "LONG",
            D("101"),
            D("0.5"),
            start * 16,
            1,
            "LIMIT",
        ),
        (
            "SEED-ORDER-2",
            "A-USDT-SWAP",
            "LONG",
            D("101"),
            D("0.5"),
            start * 16,
            2,
            "LIMIT",
        ),
    ]

    seed_effective_at = start * 16
    market_steps = [
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
    ]
    assert min(record["key"][0] for record in market_steps) > seed_effective_at

    # The first queued market segment is phase 7, after seed phase 0. Both
    # orders have the same trigger; one segment lot proves rank 1 is first.
    assert composition._dispatch_due(start * 16 + 7)["classification"] == "SUCCESS"
    first = next(
        record["result"]["historical_result"]
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        and record["item"]["node"]["bar_open_ms"] == start
        and record["item"]["node"]["step_index"] == 0
    )
    fills = [fill for product in first["products"] for fill in product["fills"]]
    assert [
        (fill["order_id"], fill["quantity_contracts"], fill["price"])
        for fill in fills
    ] == [("SEED-ORDER-1", D("0.25"), D("100"))]
    after_partial = composition._codec.historical_working_orders()
    assert [
        (order["order_id"], order["remaining_quantity_contracts"],
         order["accepted_source_sequence"], order["status"])
        for order in after_partial
    ] == [
        ("SEED-ORDER-1", D("0.25"), 1, "PARTIALLY_FILLED"),
        ("SEED-ORDER-2", D("0.5"), 2, "OPEN"),
    ]


def test_historical_chain_uses_frozen_owner_orders_and_commits_one_partial_fill(tmp_path):
    run = _valid_run()
    trade_rows = tuple(
        replace(row, high=D("103"), low=D("98"), volume=D("0.1"))
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    raw_input = encode_historical_run_input(run)
    configuration = decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products
    )
    config_context = dict(
        schema_version="spider_configuration_context_v1",
        config_id=configuration["config_id"],
        configuration_sha256=run.configuration_sha256,
        products=list(run.ordered_products),
    )
    history_context = historical_context_for_input(run)
    attempt = historical_attempt()
    attempt.update(
        run_id=run.run_id,
        account_key={"venue": "SPIDER_HISTORICAL_RESEARCH", "environment": "RESEARCH_ONLY",
                     "account": run.account_key},
        scenario_plan_sha256=history_context["historical_input_sha256"],
        policy_source_sha256=run.policy_source_sha256,
        configuration_context=config_context,
        historical_context=history_context,
        planned_coverage=historical_planned_coverage(run),
    )
    for item, key in zip(
        attempt["input_contract_hashes"],
        ("scenario_plan_sha256", "program_sha256", "native_artifact_sha256", "policy_source_sha256"),
        strict=True,
    ):
        item["sha256"] = attempt[key]
    store = SpiderRunStore.create(str(tmp_path), run.run_id)
    store.register(attempt, historical_input=raw_input)
    evidence = []
    existing_notices_at_evidence = []
    composition = None

    def persist(kind, key, payload):
        if kind == "HISTORICAL_MARKET_RESULT":
            ordinal = len(decode_jsonl((store._path / "journal.jsonl").read_bytes())) + 1
            detached_payload = decode_canonical(canonical_bytes(payload))
            row = dict(
                schema_version="spider_journal_record_v1",
                run_id=run.run_id,
                journal_seq=ordinal,
                barrier_id=key["stable_id"],
                record_kind=kind,
                scheduler_key=key,
                causal_parent_ids=[],
                effective_at=payload["result"]["effective_at"],
                visible_at=key["visible_at"],
                account_version_before=payload["owner_inspection_before"]["account_version"],
                account_version_after=payload["owner_inspection_after"]["account_version"],
                payload=detached_payload,
                configuration_context=config_context,
                historical_context=history_context,
            )
            store.mark_processed(
                dict(ordinal=ordinal, journal_seq=ordinal, barrier_id=key["stable_id"]),
                record_kind=kind,
            )
            store.append_journal(row)
            source_ids = {
                fill["source_event_id"]
                for product in payload["result"]["products"]
                for fill in product["fills"]
            }
            existing_notices_at_evidence.append(any(
                record["item"].get("kind") == "DELIVERY"
                and record["item"]["delivery"].get("source_fact_id") in source_ids
                for record in composition._records.values()
            ))
        evidence.append((kind, key, payload))

    composition = _composition(run, persist)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms

    steps = [record for record in composition._records.values()
             if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"]
    assert len(steps) == 1
    first_key = start * 16 + 7
    assert steps[0]["key"][0] == first_key
    assert composition._dispatch_due(first_key)["classification"] == "SUCCESS"
    first_result = steps[0]["result"]["historical_result"]
    assert all(not product["fills"] for product in first_result["products"])
    assert steps[0]["result"]["working_orders_snapshot"] == []

    parent_visible = (start + 5_020) * 16 + 6
    assert composition._dispatch_due(parent_visible)["classification"] == "SUCCESS"
    historical_groups = [record for record in composition._records.values()
                         if record["item"]["kind"] == "SOURCE_GROUP"
                         and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"]
    assert len(historical_groups) == 4
    assert all(record["result"]["classification"] == "PENDING" for record in historical_groups)
    assert composition._dispatch_due(historical_groups[0]["key"][0]) ["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED"
               for record in historical_groups)

    step1_key = (start + 20_000) * 16 + 8
    step1 = next(record for record in composition._records.values()
                 if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                 and record["item"]["node"]["step_index"] == 1)
    assert step1["key"][0] == step1_key
    assert step1["item"]["node"]["working_orders"] == []
    orders_before_step1 = composition._codec.inspect_state()["orders_digest"]
    step1_dispatch = composition._dispatch_due(step1_key)
    assert step1_dispatch["classification"] == "SUCCESS", step1_dispatch
    step1_result = step1["result"]["historical_result"]
    assert all(not product["fills"] for product in step1_result["products"])
    assert step1["result"]["working_orders_snapshot"] == []
    assert step1_result["owner_evidence"]["orders_digest"] == orders_before_step1

    step2_key = (start + 40_000) * 16 + 8
    step2 = next(record for record in composition._records.values()
                 if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                 and record["item"]["node"]["step_index"] == 2)
    frozen = step2["item"]["node"]["working_orders"]
    assert len(frozen) == 4
    assert [row["accepted_source_sequence"] for row in frozen] == [0, 1, 2, 3]
    assert composition._dispatch_due(step2_key)["classification"] == "SUCCESS"
    step2_result = step2["result"]["historical_result"]
    fills = [fill for product in step2_result["products"] for fill in product["fills"]]
    assert len(fills) == 1
    fill = fills[0]
    assert fill["quantity_contracts"] == D("0.025")
    selected = next(row for row in frozen if row["order_id"] == fill["order_id"])
    assert selected["side"] == "SHORT" and selected["limit_price"] == D("101")
    assert fill["price"] == D("103")
    assert len(fill["execution_id"]) == 64
    assert fill["source_event_id"]
    assert fill["occurrence_index"] == 0
    step2_evidence = [item for kind, key, item in evidence
                      if kind == "HISTORICAL_MARKET_RESULT" and key["stable_id"] == step2["item"]["stable_id"]]
    assert len(step2_evidence) == 1
    captured = step2_evidence[0]
    assert captured["request"] == step2["item"]["node"]
    assert captured["working_orders_snapshot"] == frozen
    assert captured["result"] == step2_result
    assert captured["owner_inspection_after"] == step2_result["owner_evidence"]
    assert existing_notices_at_evidence[-1] is False
    configuration = decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products
    )
    row = decode_canonical(canonical_bytes(dict(
        schema_version="spider_journal_record_v1",
        run_id=run.run_id,
        journal_seq=1,
        barrier_id=step2["item"]["stable_id"],
        record_kind="HISTORICAL_MARKET_RESULT",
        scheduler_key=evidence[-1][1],
        causal_parent_ids=[],
        effective_at=step2_result["effective_at"],
        visible_at=evidence[-1][1]["visible_at"],
        account_version_before=captured["owner_inspection_before"]["account_version"],
        account_version_after=captured["owner_inspection_after"]["account_version"],
        payload=captured,
        configuration_context=dict(
            schema_version="spider_configuration_context_v1",
            config_id=configuration["config_id"],
            configuration_sha256=run.configuration_sha256,
            products=list(run.ordered_products),
        ),
        historical_context=historical_context_for_input(run),
    )))
    journal_record(row)
    captured["result"]["products"][0]["fills"].clear()
    assert step2["result"]["historical_result"] == step2_result
    persisted = decode_jsonl((store._path / "journal.jsonl").read_bytes())
    persisted_step2 = next(row for row in persisted if row["barrier_id"] == step2["item"]["stable_id"])
    assert persisted_step2["record_kind"] == "HISTORICAL_MARKET_RESULT"
    assert persisted_step2["payload"]["result"]["products"][0]["fills"]
    assert sum(row["barrier_id"] == step2["item"]["stable_id"] for row in persisted) == 1
    invalid_rows = []
    changed = deepcopy(persisted_step2)
    changed["payload"]["request"]["step_index"] = 1
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["payload"]["request"]["model_id"] = "OHLC4_OPEN_LOW_HIGH_CLOSE_V1"
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["payload"]["request"]["bars"][0]["trade"]["open"] = "0"
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["payload"]["working_orders_snapshot"].clear()
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["payload"]["account_key"]["account"] = "different-account"
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["payload"]["result"]["products"][0]["fills"][0]["quantity_contracts"] = "-1"
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["payload"]["result"]["products"][0]["fills"][0]["order_id"] = "missing-order"
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    del changed["historical_context"]
    invalid_rows.append(changed)
    for field in ("effective_at", "visible_at"):
        changed = deepcopy(persisted_step2)
        changed[field] += 1
        invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["scheduler_key"]["visible_at"] += 1
    invalid_rows.append(changed)
    changed = deepcopy(persisted_step2)
    changed["effective_at"] += 1
    changed["visible_at"] += 1
    changed["scheduler_key"]["visible_at"] += 1
    invalid_rows.append(changed)
    for field in ("account_version_before", "account_version_after"):
        changed = deepcopy(persisted_step2)
        changed[field] += 1
        invalid_rows.append(changed)
    for invalid_row in invalid_rows:
        with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
            journal_record(invalid_row)
    guarded_root = tmp_path / "account_guard"
    guarded_root.mkdir()
    guarded_store = SpiderRunStore.create(str(guarded_root), run.run_id)
    guarded_store.register(attempt, historical_input=raw_input)
    foreign_account_row = deepcopy(persisted[0])
    foreign_account = deepcopy(foreign_account_row["payload"]["account_key"])
    foreign_account["account"] = "other-account"
    foreign_account_row["payload"]["account_key"] = foreign_account
    for key in ("owner_inspection_before", "owner_inspection_after"):
        foreign_account_row["payload"][key]["account_key"] = deepcopy(foreign_account)
    foreign_account_row["payload"]["result"]["owner_evidence"]["account_key"] = deepcopy(foreign_account)
    guarded_store.mark_processed(
        dict(ordinal=1, journal_seq=1, barrier_id=foreign_account_row["barrier_id"]),
        record_kind="HISTORICAL_MARKET_RESULT",
    )
    journal_path = guarded_store._path / "journal.jsonl"
    status_path = guarded_store._path / "status.json"
    before = journal_path.read_bytes(), status_path.read_bytes()
    with pytest.raises(SpiderRunStoreError, match="ENDPOINT_RECONCILIATION_FAILED"):
        guarded_store.append_journal(foreign_account_row)
    assert (journal_path.read_bytes(), status_path.read_bytes()) == before
    for index, field in enumerate(("effective_at", "visible_at", "scheduler_visible_at",
                                   "account_version_before", "account_version_after")):
        root = tmp_path / f"result_guard_{index}"
        root.mkdir()
        result_store = SpiderRunStore.create(str(root), run.run_id)
        result_store.register(attempt, historical_input=raw_input)
        invalid_result = deepcopy(persisted[0])
        if field == "scheduler_visible_at":
            invalid_result["scheduler_key"]["visible_at"] += 1
        else:
            invalid_result[field] += 1
        result_store.mark_processed(
            dict(ordinal=1, journal_seq=1, barrier_id=invalid_result["barrier_id"]),
            record_kind="HISTORICAL_MARKET_RESULT",
        )
        result_journal = result_store._path / "journal.jsonl"
        result_status = result_store._path / "status.json"
        unchanged = result_journal.read_bytes(), result_status.read_bytes()
        with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
            result_store.append_journal(invalid_result)
        assert (result_journal.read_bytes(), result_status.read_bytes()) == unchanged
    combined_root = tmp_path / "combined_result_guard"
    combined_root.mkdir()
    combined_store = SpiderRunStore.create(str(combined_root), run.run_id)
    combined_store.register(attempt, historical_input=raw_input)
    combined_invalid = deepcopy(persisted[0])
    combined_invalid["effective_at"] += 1
    combined_invalid["visible_at"] += 1
    combined_invalid["scheduler_key"]["visible_at"] += 1
    combined_store.mark_processed(
        dict(ordinal=1, journal_seq=1, barrier_id=combined_invalid["barrier_id"]),
        record_kind="HISTORICAL_MARKET_RESULT",
    )
    combined_journal = combined_store._path / "journal.jsonl"
    combined_status = combined_store._path / "status.json"
    combined_files = combined_journal.read_bytes(), combined_status.read_bytes()
    combined_frontier = deepcopy(combined_store._processed), combined_store._processed_kind
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        combined_store.append_journal(combined_invalid)
    assert (combined_journal.read_bytes(), combined_status.read_bytes()) == combined_files
    assert (combined_store._processed, combined_store._processed_kind) == combined_frontier
    notice = next(record for record in composition._records.values()
                  if record["item"]["kind"] == "DELIVERY"
                  and record["item"]["delivery"]["source_fact_id"] == fill["source_event_id"])
    assert notice["key"][0] == (start + 40_002) * 16 + 6
    assert notice["item"]["delivery"]["immutable_payload"]["state"] == "partially_filled"
    owner_before_notice = composition._codec.inspect_state()
    assert composition._dispatch_due(notice["key"][0])["classification"] == "SUCCESS"
    notice_events = composition._audit[notice["item"]["delivery"]["delivery_id"]]
    assert not any(event["event"]["kind"] in ("send", "cancel") for event in notice_events)
    owner_after_notice = composition._codec.inspect_state()
    for field in ("account_version", "cash", "total_fees", "positions_digest", "orders_digest"):
        assert owner_after_notice[field] == owner_before_notice[field]
    policy_events_after_notice = deepcopy(composition._policy.events)
    delivered_notice = notice["item"]["delivery"]
    assert composition._codec.build_delivery(dict(
        schema_version="delivery_projection_v1",
        reference=dict(namespace=delivered_notice["source_namespace"], fact_id=delivered_notice["source_fact_id"]),
        payload_kind=delivered_notice["payload_kind"],
        occurrence_index=delivered_notice["occurrence_index"],
        schedule_sequence=delivered_notice["schedule_sequence"],
        visible_at=delivered_notice["visible_at"],
    )) == delivered_notice
    duplicate = composition._enqueue(dict(kind="DELIVERY", delivery=delivered_notice))
    assert duplicate["classification"] == "SUCCESS"
    assert composition._policy.events == policy_events_after_notice
    owner_after = step2_result["owner_evidence"]
    assert owner_after["account_version"] > first_result["owner_evidence"]["account_version"]
    assert owner_after["orders_digest"] != first_result["owner_evidence"]["orders_digest"]
    assert owner_after["total_fees"] > first_result["owner_evidence"]["total_fees"]
    assert owner_after["cash"] < first_result["owner_evidence"]["cash"]
    assert owner_after["positions_digest"] != first_result["owner_evidence"]["positions_digest"]

    step3 = next(record for record in composition._records.values()
                 if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                 and record["item"]["node"]["step_index"] == 3)
    partial = next(row for row in step3["item"]["node"]["working_orders"]
                   if row["order_id"] == fill["order_id"])
    assert partial["order_version"] == 2
    assert partial["status"] == "PARTIALLY_FILLED"
    assert partial["remaining_quantity_contracts"] == frozen[
        next(index for index, row in enumerate(frozen) if row["order_id"] == fill["order_id"])
    ]["remaining_quantity_contracts"] - fill["quantity_contracts"]
    positions = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="P3C5B_POSITIONS_AFTER_FILL", snapshot_kind="POSITIONS",
        capture_mode="OWNER_CURRENT", captured_at=step2_key,
    ))["immutable_payload"]["rows"]
    a_position = next(row for row in positions if row["product_id"] == "A-USDT-SWAP")
    assert a_position["position_contracts"] == -fill["quantity_contracts"]
    step3_key = run.range_end_ms * 16 + 4
    assert composition._dispatch_due(step3_key)["classification"] == "SUCCESS"
    assert step3["result"]["classification"] == "SUCCESS"


def test_historical_market_evidence_failure_stops_before_derived_events():
    run = _valid_run()
    calls = []

    def fail_persistence(kind, key, payload):
        calls.append((kind, key, payload))
        raise ReplayPersistenceError()

    composition = _composition(run, fail_persistence)
    start = run.range_start_ms
    initial = next(record for record in composition._records.values()
                   if record["item"]["kind"] == "HISTORICAL_MARKET_STEP")
    terminal = composition._dispatch_due(start * 16 + 7)
    assert terminal["classification"] == "TERMINAL"
    assert terminal["reason"] == "PERSISTENCE_FAILED"
    assert len(calls) == 1 and calls[0][0] == "HISTORICAL_MARKET_RESULT"
    steps = [record for record in composition._records.values()
             if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"]
    assert steps == [initial]
    assert not any(record["item"]["kind"] == "DELIVERY"
                   for record in composition._records.values())


@pytest.mark.parametrize("partial_fill", [False, True])
def test_historical_endpoint_and_reconciliation_project_authoritative_market_evidence(tmp_path, partial_fill):
    run, attempt, status, journal, artifacts, composition = _historical_endpoint_case(
        tmp_path, partial_fill=partial_fill,
    )
    endpoint, report, reconciliation = artifacts["endpoint"], artifacts["report"], artifacts["reconciliation"]
    assert endpoint["terminal_reason"] == "MTM_PRESERVE_OPEN_V1"
    assert endpoint["cutoff"]["scheduler_time"] == endpoint["scheduler_observation"]["current_time"]
    assert endpoint["remaining_planned_barriers"] == []
    assert reconciliation["result"] == "OK", reconciliation
    assert build_reconciliation(run.run_id, attempt, status, journal, endpoint, report) == reconciliation
    market_rows = [row for row in journal if row["record_kind"] == "HISTORICAL_MARKET_RESULT"]
    fills = [fill for row in market_rows for item in row["payload"]["result"]["products"] for fill in item["fills"]]
    assert bool(fills) is partial_fill
    if partial_fill:
        fill = fills[0]
        product_report = next(row for row in report if row["product_id"] == "A-USDT-SWAP")
        assert f"SOURCE:{fill['source_event_id']}" in product_report["committed_execution_refs"]
        assert any(ref.startswith("journal:") for ref in product_report["source_evidence_refs"])
    assert all(row["historical_context"] == attempt["historical_context"] for row in report)


@pytest.mark.parametrize("partial_fill", [False, True])
def test_historical_store_finalize_and_admission_accept_real_native_bundle(tmp_path, partial_fill):
    run, attempt, status, journal, artifacts, _, store = _historical_endpoint_case(
        tmp_path, partial_fill=partial_fill, return_store=True,
    )
    assert store.finalize(artifacts["endpoint"], artifacts["reconciliation"], artifacts["report"]) == "COMPLETE_PUBLISHED"
    raw_input = (store._path / "historical_input.json").read_bytes()
    assert raw_input == encode_historical_run_input(run)
    manifest = decode_canonical((store._path / "completion.json").read_bytes().rstrip(b"\n"))
    input_metadata = manifest["artifacts"][-1]
    assert input_metadata == dict(
        path="historical_input.json", schema_version="spider_historical_research_run_v1",
        sha256=sha256(raw_input).hexdigest(), byte_count=len(raw_input), row_count=1,
    )
    admitted = admit_spider_run(store._path)
    assert admitted["decision"] == "ACCEPT", admitted
    assert admitted["artifacts"]["historical_input.json"]["run_id"] == run.run_id
    assert admitted["artifacts"]["journal.jsonl"] == journal
    assert status["state"] == "RUNNING"


@pytest.mark.parametrize("mutation", [
    "missing_input", "truncated_input", "changed_input", "symlink_input", "hardlink_input",
    "context_mismatch", "hash_mismatch", "incomplete_frontier", "reconciliation_mismatch",
])
def test_historical_admission_rejects_mutated_finalized_bundle(tmp_path, mutation):
    run, _, _, _, artifacts, _, store = _historical_endpoint_case(
        tmp_path, partial_fill=True, return_store=True,
    )
    store.finalize(artifacts["endpoint"], artifacts["reconciliation"], artifacts["report"])
    path = store._path
    completion_path = path / "completion.json"
    manifest = decode_canonical(completion_path.read_bytes().rstrip(b"\n"))

    def write_object(name, value):
        raw = canonical_bytes(value) + b"\n"
        (path / name).write_bytes(raw)
        entry = next(row for row in manifest["artifacts"] if row["path"] == name)
        entry.update(sha256=sha256(raw).hexdigest(), byte_count=len(raw),
                     row_count=len(value) if name.endswith(".jsonl") else 1)

    historical_path = path / "historical_input.json"
    if mutation == "missing_input":
        historical_path.unlink()
    elif mutation == "truncated_input":
        historical_path.write_bytes(historical_path.read_bytes()[:24])
    elif mutation == "changed_input":
        changed = decode_canonical(historical_path.read_bytes())
        changed["run_id"] = "other-run"
        historical_path.write_bytes(canonical_bytes(changed))
    elif mutation in ("symlink_input", "hardlink_input"):
        historical_path.unlink()
        if mutation == "symlink_input":
            historical_path.symlink_to(path / "attempt.json")
        else:
            os.link(path / "attempt.json", historical_path)
    elif mutation == "context_mismatch":
        attempt_path = path / "attempt.json"
        attempt = decode_canonical(attempt_path.read_bytes().rstrip(b"\n"))
        attempt["historical_context"]["source_sha256"] = "f" * 64
        write_object("attempt.json", attempt)
    elif mutation == "hash_mismatch":
        next(row for row in manifest["artifacts"] if row["path"] == "historical_input.json")["sha256"] = "f" * 64
    elif mutation == "incomplete_frontier":
        status_path = path / "status.json"
        status = decode_canonical(status_path.read_bytes().rstrip(b"\n"))
        status["state"] = "RUNNING"
        write_object("status.json", status)
    else:
        reconciliation_path = path / "reconciliation.json"
        reconciliation = decode_canonical(reconciliation_path.read_bytes().rstrip(b"\n"))
        reconciliation["result"] = "FAILED"
        write_object("reconciliation.json", reconciliation)
        manifest["reconciliation_digest"] = sha256(canonical_bytes(reconciliation)).hexdigest()

    completion_path.write_bytes(canonical_bytes(manifest) + b"\n")
    assert admit_spider_run(path)["decision"] == "REJECT"


@pytest.mark.parametrize("encoded_value", [[], None, "not-an-object"])
def test_historical_admission_rejects_canonical_non_object_input(tmp_path, encoded_value):
    _, _, _, _, artifacts, _, store = _historical_endpoint_case(
        tmp_path, partial_fill=False, return_store=True,
    )
    store.finalize(artifacts["endpoint"], artifacts["reconciliation"], artifacts["report"])
    raw = canonical_bytes(encoded_value)
    (store._path / "historical_input.json").write_bytes(raw)
    manifest_path = store._path / "completion.json"
    manifest = decode_canonical(manifest_path.read_bytes().rstrip(b"\n"))
    metadata = next(row for row in manifest["artifacts"] if row["path"] == "historical_input.json")
    metadata.update(sha256=sha256(raw).hexdigest(), byte_count=len(raw), row_count=1)
    manifest_path.write_bytes(canonical_bytes(manifest) + b"\n")
    assert admit_spider_run(store._path)["decision"] == "REJECT"


def test_historical_endpoint_rejects_missing_duplicate_and_past_due_coverage(tmp_path):
    run, attempt, status, journal, artifacts, _ = _historical_endpoint_case(tmp_path, partial_fill=False)
    endpoint = artifacts["endpoint"]
    for mutation in ("missing", "duplicate", "past_due", "unrecorded_future"):
        observation = deepcopy(endpoint["scheduler_observation"])
        planned = next(row for row in observation["records"] if row["kind"] == "HISTORICAL_MARKET_STEP")
        if mutation == "missing":
            observation["records"].remove(planned)
        elif mutation == "duplicate":
            observation["records"].append(deepcopy(planned))
            observation["records"].sort(key=lambda row: (
                ["SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
                 "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER"].index(row["kind"]), row["stable_id"],
            ))
        else:
            cutoff = endpoint["cutoff"]["scheduler_time"]
            visible_at = cutoff if mutation == "past_due" else cutoff + 1
            test_key = dict(visible_at=visible_at, queue_class="DELIVERY", schedule_sequence=999_999,
                            stable_id=f"{mutation}-test")
            observation["pending_keys"].append(test_key)
            observation["pending_keys"].sort(key=lambda key: (
                key["visible_at"], ["SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
                                    "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER"].index(key["queue_class"]),
                key["schedule_sequence"], key["stable_id"],
            ))
            if mutation == "past_due":
                observation["records"].append(dict(key=test_key, kind="DELIVERY", stable_id="past-due-test",
                                                   classification="PENDING"))
                observation["records"].sort(key=lambda row: (
                    ["SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
                     "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER"].index(row["kind"]), row["stable_id"],
                ))
        with pytest.raises(ValueError):
            build_endpoint_artifacts(run.run_id, attempt, status, journal,
                                     endpoint["initial_owner_evidence"], endpoint["final_owner_evidence"], observation)


def test_historical_endpoint_binds_final_owner_and_snapshot_payload_digests(tmp_path):
    run, attempt, status, journal, artifacts, _ = _historical_endpoint_case(tmp_path, partial_fill=False)
    endpoint = artifacts["endpoint"]
    for mutation in ("cash", "equity", "position", "open_order"):
        final = deepcopy(endpoint["final_owner_evidence"])
        if mutation == "cash":
            final["inspection"]["cash"] = "999"
        elif mutation == "equity":
            final["trading_fact"]["immutable_payload"]["equity"] = "999"
        elif mutation == "position":
            final["positions_fact"]["immutable_payload"]["rows"].append(dict(
                product_id="A-USDT-SWAP", margin_mode="cross", position_contracts="1",
                last_price="100", notional_usd="100",
            ))
        else:
            final["open_orders_fact"]["immutable_payload"]["rows"][0]["original_size_contracts"] = "0.097"
        with pytest.raises(ValueError):
            build_endpoint_artifacts(run.run_id, attempt, status, journal,
                                     endpoint["initial_owner_evidence"], final,
                                     endpoint["scheduler_observation"])
        changed_endpoint = deepcopy(endpoint)
        changed_endpoint["final_owner_evidence"] = final
        result = build_reconciliation(run.run_id, attempt, status, journal, changed_endpoint, artifacts["report"])
        assert result["result"] == "FAILED"
        assert result["checks"][4 if mutation == "cash" else 5]["result"] == "FAILED"


def test_historical_endpoint_rejects_native_cross_account_snapshot_transplant(tmp_path):
    run, attempt, status, journal, artifacts, _ = _historical_endpoint_case(tmp_path, partial_fill=False)
    foreign = replace(_valid_run(), account_key="another-account")
    config = _p2_configuration()
    config["cash"] = "2000"
    foreign = _run_with_configuration(foreign, config)
    foreign = replace(
        foreign,
        initial_account_state=_initial_account_state(cash="2000", orders=[], positions=[]),
        initial_policy_cache=_policy_cache(capital={
            "total": "2000", "usdt": "2000", "avail": "2000", "earn": "0", "position": "0",
        }),
    )
    assert validate_historical_input(foreign)
    foreign_composition = _composition(foreign)
    foreign_composition._policy.rows[1]["active"] = "false"
    cutoff = foreign.range_end_ms * 16 + 6
    assert foreign_composition._dispatch_due(cutoff)["classification"] == "SUCCESS"
    owner = _capture_historical_owner(foreign_composition, cutoff, "FINAL")["owner_evidence"]
    original = artifacts["endpoint"]["final_owner_evidence"]["trading_fact"]
    transplanted = owner["trading_fact"]
    assert original["reference"] == transplanted["reference"]
    assert original["captured_account_version"] == transplanted["captured_account_version"]
    assert original["snapshot_as_of"] == transplanted["snapshot_as_of"]
    assert original["request_digest"] != transplanted["request_digest"]
    assert original["immutable_payload"]["equity"] != transplanted["immutable_payload"]["equity"]

    final = deepcopy(artifacts["endpoint"]["final_owner_evidence"])
    final["trading_fact"] = transplanted
    with pytest.raises(ValueError):
        build_endpoint_artifacts(run.run_id, attempt, status, journal,
                                 artifacts["endpoint"]["initial_owner_evidence"], final,
                                 artifacts["endpoint"]["scheduler_observation"])


def test_historical_endpoint_rejects_pending_key_mismatch_and_incomplete_polls(tmp_path):
    run, attempt, status, journal, artifacts, _ = _historical_endpoint_case(tmp_path, partial_fill=False)
    endpoint = artifacts["endpoint"]
    cutoff = endpoint["cutoff"]["scheduler_time"]
    key = dict(visible_at=cutoff + 1, queue_class="DELIVERY", schedule_sequence=999_999,
               stable_id="future-delivery-test")
    for mutation in ("duplicate_identity", "visible_at_mismatch", "sequence_mismatch"):
        observation = deepcopy(endpoint["scheduler_observation"])
        observation["pending_keys"] = [key]
        record_key = deepcopy(key)
        if mutation == "duplicate_identity":
            duplicate = deepcopy(key)
            duplicate["schedule_sequence"] += 1
            observation["pending_keys"].append(duplicate)
        elif mutation == "visible_at_mismatch":
            record_key["visible_at"] += 1
        else:
            record_key["schedule_sequence"] += 1
        observation["records"].append(dict(key=record_key, kind="DELIVERY", stable_id=key["stable_id"],
                                           classification="PENDING"))
        observation["pending_keys"].sort(key=lambda row: (row["visible_at"], row["queue_class"],
                                                           row["schedule_sequence"], row["stable_id"]))
        class_order = {name: index for index, name in enumerate((
            "SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
            "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER",
        ))}
        observation["records"].sort(key=lambda row: (class_order[row["kind"]], row["stable_id"]))
        with pytest.raises(ValueError):
            build_endpoint_artifacts(run.run_id, attempt, status, journal,
                                     endpoint["initial_owner_evidence"], endpoint["final_owner_evidence"],
                                     observation)
        changed_endpoint = deepcopy(endpoint)
        changed_endpoint["scheduler_observation"] = observation
        result = build_reconciliation(run.run_id, attempt, status, journal, changed_endpoint, artifacts["report"])
        assert result["checks"][6]["result"] == "FAILED"

    for mutation in ("orphan_poll", "unrelated_work", "failed_poll"):
        observation = deepcopy(endpoint["scheduler_observation"])
        poll = dict(poll_id="poll-test", continuation_id="continuation-test",
                    status="CALLBACK_FAILED" if mutation == "failed_poll" else "PAUSED",
                    awaiting="LOCAL_ACTIONS")
        observation["polls"] = [poll]
        if mutation == "unrelated_work":
            unrelated = dict(visible_at=cutoff + 1, queue_class="SNAPSHOT_CAPTURE",
                             schedule_sequence=999_999, stable_id="unrelated-snapshot")
            observation["pending_keys"] = [unrelated]
            observation["records"].append(dict(key=unrelated, kind="SNAPSHOT_CAPTURE",
                                               stable_id=unrelated["stable_id"], classification="PENDING"))
            class_order = {name: index for index, name in enumerate((
                "SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
                "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER",
            ))}
            observation["records"].sort(key=lambda row: (class_order[row["kind"]], row["stable_id"]))
        with pytest.raises(ValueError):
            build_endpoint_artifacts(run.run_id, attempt, status, journal,
                                     endpoint["initial_owner_evidence"], endpoint["final_owner_evidence"],
                                     observation)
        changed_endpoint = deepcopy(endpoint)
        changed_endpoint["scheduler_observation"] = observation
        result = build_reconciliation(run.run_id, attempt, status, journal, changed_endpoint, artifacts["report"])
        assert result["checks"][7]["result"] == "FAILED"


def test_historical_reconciliation_rejects_missing_fill_chain_and_report_reference(tmp_path):
    run, attempt, status, journal, artifacts, _ = _historical_endpoint_case(tmp_path, partial_fill=True)
    endpoint, report = artifacts["endpoint"], artifacts["report"]
    fill_row = next(row for row in journal if row["record_kind"] == "HISTORICAL_MARKET_RESULT"
                    and any(product["fills"] for product in row["payload"]["result"]["products"]))
    missing_fill = deepcopy(journal)
    missing_fill.remove(fill_row)
    for sequence, row in enumerate(missing_fill, 1):
        row["journal_seq"] = sequence
    frontier = dict(ordinal=len(missing_fill), journal_seq=len(missing_fill),
                    barrier_id=missing_fill[-1]["barrier_id"])
    changed_status = deepcopy(status)
    changed_status.update(processed_boundary=frontier, persisted_boundary=frontier)
    changed_endpoint = deepcopy(endpoint)
    changed_endpoint["cutoff"]["persisted_boundary"] = frontier
    missing_result = build_reconciliation(run.run_id, attempt, changed_status, missing_fill, changed_endpoint, report)
    assert missing_result["checks"][4]["result"] == "FAILED"

    changed_journal = deepcopy(journal)
    changed_fill = next(row for row in changed_journal if row["barrier_id"] == fill_row["barrier_id"])
    changed_fill["payload"]["owner_inspection_before"]["owner_state_digest"] = "0" * 64
    chain_result = build_reconciliation(run.run_id, attempt, status, changed_journal, endpoint, report)
    assert chain_result["checks"][4]["result"] == "FAILED"

    changed_report = deepcopy(report)
    product = next(row for row in changed_report if row["committed_execution_refs"])
    product["committed_execution_refs"].pop()
    report_result = build_reconciliation(run.run_id, attempt, status, journal, endpoint, changed_report)
    assert report_result["checks"][10]["result"] == "FAILED"


def test_historical_full_execution_notice_derives_native_cancel_and_replacements():
    run = _valid_run()
    trade_rows = tuple(
        replace(row, open=D("101"), high=D("101.5"), low=D("100.5"), close=D("101"), volume=D("0.4"))
        if row.product_id == "A-USDT-SWAP" else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms
    assert composition._dispatch_due((start + 40_002) * 16 + 6)["classification"] == "SUCCESS"

    full_notices = [record for record in composition._records.values()
                    if record["item"]["kind"] == "DELIVERY"
                    and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
                    and record["item"]["delivery"]["immutable_payload"]["state"] == "filled"
                    and record["result"]["classification"] == "SUCCESS"]
    assert full_notices
    notice = next(record for record in full_notices
                  if record["item"]["delivery"]["immutable_payload"]["limit_price"]
                  != record["item"]["delivery"]["immutable_payload"]["fill_price"])
    fact = notice["item"]["delivery"]["immutable_payload"]
    events = composition._audit[notice["item"]["delivery"]["delivery_id"]]
    sends = [event for event in events if event["event"]["kind"] == "send"]
    cancels = [event for event in events if event["event"]["kind"] == "cancel"]
    assert sends and cancels
    assert sum(bool(composition._audit[record["item"]["delivery"]["delivery_id"]])
               for record in full_notices) == 1
    replacement_prices = [order["px"] for event in sends for order in event["event"]["orders"]]
    assert fact["limit_price"] == D("101") and fact["fill_price"] == D("101.5")
    assert "100" in replacement_prices
    assert str(fact["fill_price"]) not in replacement_prices
    assert all(action["status"] == "UNSUBMITTED" and action["group_id"] is not None
               for event in (*sends, *cancels) for action in event["actions"])

    initial_groups = [record for record in composition._records.values()
                      if record["item"]["kind"] == "SOURCE_GROUP"
                      and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"]
    child_groups = [record for record in initial_groups
                    if record["item"]["group"]["members"][0]["stamp"]["source_sequence"] >= 4]
    child_groups.sort(key=lambda record: record["item"]["group"]["members"][0]["stamp"]["source_sequence"])
    assert [record["item"]["group"]["members"][0]["kind"] for record in child_groups] == [
        "CANCEL_REQUEST", "INTENT", "INTENT",
    ]
    assert [record["item"]["group"]["members"][0]["stamp"]["source_sequence"]
            for record in child_groups] == list(range(4, 7))
    assert all(record["key"][0] == (start + 40_003) * 16 + 3 for record in child_groups)
    assert composition._dispatch_due((start + 40_003) * 16 + 3)["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED"
               for record in child_groups)
    assert all(action["status"] == "SUBMITTED"
               and action["group_result"]["classification"] == "COMMITTED"
               for event in (*sends, *cancels) for action in event["actions"])

    requests = [record for record in child_groups
                if record["item"]["group"]["members"][0]["kind"] == "CANCEL_REQUEST"]
    effects = [record for record in composition._records.values()
               if record["item"]["kind"] == "SOURCE_GROUP"
               and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"]
    assert len(requests) == len(effects) == 1
    for request in requests:
        request_member = request["item"]["group"]["members"][0]
        request_event = request_member["stamp"]["event_id"]
        target = request_member["payload"]["targets"][0]["target_order_id"]
        matching_effect = next(record for record in effects
                               if record["item"]["group"]["members"][0]["payload"]["effects"][0]["detecting_event_id"]
                               == request_event)
        effect_member = matching_effect["item"]["group"]["members"][0]
        assert matching_effect["key"][0] == (start + 45_003) * 16 + 2
        assert effect_member["stamp"]["causal_parent_ids"] == [request_event]
        assert effect_member["payload"]["effects"] == [dict(
            detecting_event_id=request_event, target_order_id=target, reason="EXPLICIT_SCENARIO")
        ]
        assert target in {order["order_id"] for order in composition._codec.historical_working_orders()}
    assert composition._dispatch_due((start + 45_003) * 16 + 2)["classification"] == "SUCCESS"
    ack_visible = (start + 45_005) * 16 + 6
    ack_records = [record for record in composition._records.values()
                   if record["item"]["kind"] == "DELIVERY"
                   and record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"]
    assert len(ack_records) == 1
    ack_record = ack_records[0]
    ack_delivery = ack_record["item"]["delivery"]
    effect_event = effects[0]["item"]["group"]["members"][0]["stamp"]["event_id"]
    assert ack_record["key"][0] == ack_delivery["visible_at"] == ack_visible
    assert ack_delivery["source_fact_id"] == effect_event
    ack_payload = ack_delivery["immutable_payload"]
    assert set(ack_payload) == {"route", "operation", "client_order_id", "order_id", "code"}
    assert (ack_payload["route"], ack_payload["operation"], ack_payload["order_id"], ack_payload["code"]) == (
        "WS", "CANCEL", target, "0",
    )
    assert isinstance(ack_payload["client_order_id"], str) and ack_payload["client_order_id"]
    state_after_effect = deepcopy(composition._codec.inspect_state())
    policy_events_before_ack = deepcopy(composition._policy.events)
    ack_calls = []
    capture_callback = composition._capture_callback

    def count_ack_callback(delivery):
        if delivery["delivery_id"] == ack_delivery["delivery_id"]:
            ack_calls.append(delivery["delivery_id"])
        return capture_callback(delivery)

    composition._capture_callback = count_ack_callback
    assert composition._dispatch_due(ack_visible - 1)["classification"] == "SUCCESS"
    assert ack_record["result"]["classification"] == "PENDING"
    assert composition._dispatch_due(ack_visible)["classification"] == "SUCCESS"
    assert ack_record["result"]["classification"] == "SUCCESS"
    assert ack_record["result"]["events"] == []
    assert ack_calls == [ack_delivery["delivery_id"]]
    assert composition._codec.inspect_state() == state_after_effect
    assert composition._policy.events == policy_events_before_ack
    ack_audit_after_delivery = deepcopy(composition._audit[ack_delivery["delivery_id"]])
    assert composition._enqueue(dict(kind="DELIVERY", delivery=ack_delivery))["classification"] == "SUCCESS"
    assert composition._policy.events == policy_events_before_ack
    assert composition._codec.inspect_state() == state_after_effect
    assert composition._audit[ack_delivery["delivery_id"]] == ack_audit_after_delivery
    assert ack_calls == [ack_delivery["delivery_id"]]
    assert composition._dispatch_due((start + 45_025) * 16 + 6)["classification"] == "SUCCESS"
    requests = [record for record in composition._records.values()
                if record["item"]["kind"] == "SOURCE_GROUP"
                and record["item"]["group"]["members"][0]["kind"] == "CANCEL_REQUEST"]
    effects = [record for record in composition._records.values()
               if record["item"]["kind"] == "SOURCE_GROUP"
               and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"]
    target_requests = [record for record in requests
                       if record["item"]["group"]["members"][0]["payload"]["targets"][0]["target_order_id"]
                       == target]
    target_effects = [record for record in effects
                      if record["item"]["group"]["members"][0]["payload"]["effects"][0]["target_order_id"]
                      == target]
    assert sum(record["result"]["group_result"]["classification"] == "COMMITTED"
               for record in target_requests) == 1
    rejected_repeats = [record for record in target_requests
                        if record["result"]["group_result"]["classification"] == "REJECTED"]
    assert rejected_repeats
    assert all(record["result"]["group_result"]["rejections"][0]["reason"]
               == "CANCEL_REQUEST_TOO_LATE" for record in rejected_repeats)
    assert len(target_effects) == 1
    assert target_effects[0]["result"]["group_result"]["classification"] == "COMMITTED"
    assert target not in {order["order_id"] for order in composition._codec.historical_working_orders()}

    request_effects = {
        effect_record["item"]["group"]["members"][0]["payload"]["effects"][0]["target_order_id"]:
        effect_record
        for effect_record in effects
    }
    pending_repeats = []
    post_effect_repeats = []
    for request_record in requests:
        if request_record["result"]["group_result"]["classification"] != "REJECTED":
            continue
        request_target = request_record["item"]["group"]["members"][0]["payload"]["targets"][0]["target_order_id"]
        if request_target not in request_effects:
            continue
        effect_record = request_effects[request_target]
        (pending_repeats if request_record["key"] < effect_record["key"]
         else post_effect_repeats).append(request_record)
    assert pending_repeats and post_effect_repeats
    policy_events_after_full = deepcopy(composition._policy.events)
    delivered_notice = notice["item"]["delivery"]
    assert composition._codec.build_delivery(dict(
        schema_version="delivery_projection_v1",
        reference=dict(namespace=delivered_notice["source_namespace"], fact_id=delivered_notice["source_fact_id"]),
        payload_kind=delivered_notice["payload_kind"],
        occurrence_index=delivered_notice["occurrence_index"],
        schedule_sequence=delivered_notice["schedule_sequence"],
        visible_at=delivered_notice["visible_at"],
    )) == delivered_notice
    duplicate = composition._enqueue(dict(kind="DELIVERY", delivery=delivered_notice))
    assert duplicate["classification"] == "SUCCESS"
    assert composition._policy.events == policy_events_after_full
    assert composition._dispatch_due((start + 60_000) * 16 + 4)["classification"] == "SUCCESS"
    close_node = next(record for record in composition._records.values()
                      if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                      and record["item"]["node"]["bar_open_ms"] == start
                      and record["item"]["node"]["step_index"] == 3)
    assert close_node["result"]["classification"] == "SUCCESS"
    assert target in {order["order_id"] for order in close_node["item"]["node"]["working_orders"]}
    assert all(fill["order_id"] != target
               for product in close_node["result"]["historical_result"]["products"]
               for fill in product["fills"])


def test_historical_cancel_ack_after_endpoint_remains_pending_without_extending_run():
    base = _valid_run()
    start = base.range_start_ms
    trade_rows = tuple(
        replace(row, open=D("101"), high=D("101.5"), low=D("100.5"), close=D("101"), volume=D("0.4"))
        if row.product_id == "A-USDT-SWAP" else row
        for row in base.trade_bars
    )
    run = replace(_rehashed_run(base, trade_rows=trade_rows), range_end_ms=start + 45_004)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    ack_calls = []
    capture_callback = composition._capture_callback

    def count_ack_callback(delivery):
        if delivery["payload_kind"] == "TRANSPORT_ACK":
            ack_calls.append(delivery["delivery_id"])
        return capture_callback(delivery)

    composition._capture_callback = count_ack_callback
    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    effects = [record for record in composition._records.values()
               if record["item"]["kind"] == "SOURCE_GROUP"
               and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"
               and record["result"].get("group_result", {}).get("classification") == "COMMITTED"]
    acks = [record for record in composition._records.values()
            if record["item"]["kind"] == "DELIVERY"
            and record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"]
    assert len(effects) == len(acks) == 1
    ack = acks[0]
    assert ack["item"]["delivery"]["source_fact_id"] == effects[0]["item"]["group"]["members"][0]["stamp"]["event_id"]
    assert ack["key"][0] > endpoint == run.range_end_ms * 16 + 6
    assert ack["result"]["classification"] == "PENDING"
    assert ack_calls == []


def test_historical_execution_notice_after_endpoint_remains_pending():
    run = _valid_run()
    trade_rows = tuple(
        replace(row, high=D("103"), low=D("98"), volume=D("1"))
        if row.product_id == "A-USDT-SWAP" else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms
    assert composition._dispatch_due((start + 5_020) * 16 + 6)["classification"] == "SUCCESS"
    assert composition._dispatch_due((start + 5_021) * 16 + 3)["classification"] == "SUCCESS"
    for row in composition._policy.rows:
        row["active"] = "false"
    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    assert (3, f"P3_MARKET_{start}_3") in composition._records
    pending = [record for record in composition._records.values()
               if record["item"]["kind"] == "DELIVERY"
               and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
               and record["key"][0] > endpoint]
    assert pending
    assert all(record["result"]["classification"] == "PENDING" for record in pending)
    assert all(record["item"]["delivery"]["visible_at"] == run.range_end_ms * 16 + 38
               for record in pending)


def test_historical_chain_runs_final_close_without_endpoint_next_open():
    run = _valid_run()
    start, end = run.range_start_ms, run.range_end_ms

    def close_bar(rows, *, trade):
        return tuple(
            replace(row, open=D("100"), high=D("110"), low=D("90"), close=D("105"))
            if row.bar_open_ms == start else row
            for row in rows
        )

    run = _rehashed_run(
        run,
        trade_rows=close_bar(run.trade_bars, trade=True),
        mark_rows=close_bar(run.mark_bars, trade=False),
    )
    configuration = _p2_configuration()
    configuration["cash"] = "10000"
    configuration["positions"] = [
        dict(product_id=product, side="LONG", quantity_contracts=quantity,
             lots=[dict(seed_execution_id=f"FINAL-CLOSE-{product}", seed_sequence=sequence,
                        strategy_id="SPIDER_GRID_ORIGINAL_V1", quantity_contracts=quantity,
                        entry_price="100")])
        for sequence, (product, quantity) in enumerate(
            (("A-USDT-SWAP", "80"), ("B-USDT-SWAP", "40"))
        )
    ]
    run = _run_with_configuration(run, configuration)
    run = replace(
        run,
        initial_account_state=_initial_account_state(cash="10000", positions=configuration["positions"]),
        initial_policy_cache=_policy_cache(running=False),
    )
    composition = _composition(run)
    assert composition._dispatch_due(end * 16 + 6)["classification"] == "SUCCESS"
    steps = [record for record in composition._records.values()
             if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"]
    assert [(record["item"]["node"]["bar_open_ms"], record["item"]["node"]["step_index"])
            for record in steps] == [(run.range_start_ms, 0), (run.range_start_ms, 1),
                                     (run.range_start_ms, 2), (run.range_start_ms, 3)]
    final = steps[-1]
    assert final["key"][0] == end * 16 + 4
    assert final["result"]["classification"] == "SUCCESS"
    cache = composition._records[(4, f"MARKET_CLOSE_{end}")]
    assert cache["key"][0] == end * 16 + 6
    assert cache["result"]["classification"] == "SUCCESS"
    assert not any(record["item"]["node"]["bar_open_ms"] == end for record in steps)
    assert all(not product["fills"] for step in steps
               for product in step["result"]["historical_result"]["products"])
    trading = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="FINAL_CLOSE_TRADING", snapshot_kind="TRADING",
        capture_mode="OWNER_CURRENT", captured_at=end * 16 + 6,
    ))["immutable_payload"]
    positions = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="FINAL_CLOSE_POSITIONS", snapshot_kind="POSITIONS",
        capture_mode="OWNER_CURRENT", captured_at=end * 16 + 6,
    ))["immutable_payload"]["rows"]
    assert trading["equity"] == D("10600")
    assert [(row["product_id"], row["position_contracts"], row["last_price"])
            for row in positions] == [
                ("A-USDT-SWAP", D("80"), D("105")),
                ("B-USDT-SWAP", D("40"), D("105")),
            ]


def test_private_future_bar_does_not_change_policy_prefix_before_its_close():
    base = _extend_one_minute(_valid_run())
    config = _p2_configuration()
    for product in config["products"]:
        product["marks"][0]["valid_to"] = 200_000_000
    base = _run_with_configuration(base, config)
    changed_rows = tuple(
        replace(row, high=D("120"), low=D("80"), close=D("110"))
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == base.range_end_ms - 60_000 else row
        for row in base.trade_bars
    )
    changed = _rehashed_run(base, trade_rows=changed_rows)
    left, right = _composition(base), _composition(changed)
    before_future_close = (base.range_end_ms - 60_000) * 16 + 9
    assert left._dispatch_due(before_future_close)["classification"] == "SUCCESS"
    assert right._dispatch_due(before_future_close)["classification"] == "SUCCESS"
    assert left._policy.events == right._policy.events
    assert left._policy.markets == right._policy.markets
    assert all(event["at_ms"] < base.range_end_ms for event in left._policy.events)


def test_historical_derived_ids_bind_contract_parent_and_ordinal_not_run_id():
    from src.core.backtest.spider_policy_protocol import _historical_child_id

    digest = "a" * 64
    first = _historical_child_id(digest, "delivery-a", "ORDER_GROUP", 0)
    assert first == _historical_child_id(digest, "delivery-a", "ORDER_GROUP", 0)
    assert first != _historical_child_id("b" * 64, "delivery-a", "ORDER_GROUP", 0)
    assert first != _historical_child_id(digest, "delivery-b", "ORDER_GROUP", 0)
    assert first != _historical_child_id(digest, "delivery-a", "ORDER_GROUP", 1)
    with pytest.raises(ValueError):
        _historical_child_id("not-a-hash", "delivery-a", "ORDER_GROUP", 0)
    with pytest.raises(ValueError):
        _historical_child_id(digest, "delivery-a", "ORDER_GROUP", -1)


def test_historical_send_at_endpoint_stays_unsubmitted_without_native_group():
    run = _extend_one_minute(_valid_run())
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    parent_raw = run.range_start_ms + 5_020
    accepted_raw = parent_raw + run.order_accept_delay_ms
    composition._historical_order_contract["range_end_ms"] = accepted_raw
    assert composition._dispatch_due(parent_raw * 16 + 6)["classification"] == "SUCCESS"
    assert not any(record["item"]["kind"] == "SOURCE_GROUP"
                   and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"
                   for record in composition._records.values())
    actions = [action for events in composition._audit.values() for event in events
               for action in event.get("actions", [])]
    assert actions and all(action["status"] == "UNSUBMITTED" and action["group_id"] is None for action in actions)


def _complete_poll_through_native_deliveries(
    composition: _ReplayComposition, issued_at: int, *, before_final=None,
) -> None:
    poll_plan = composition._historical_poll_plan(issued_at, len(composition._polls))
    started = composition._start_poll(poll_plan, lambda *_: None)
    assert started.observation.status == "IN_PROGRESS"
    poll = composition._polls[poll_plan["poll_id"]]
    while poll.observation.status == "IN_PROGRESS":
        stage = poll.stages[poll.index]
        if poll.index == len(poll.stages) - 1 and before_final is not None:
            before_final()
        request = stage["snapshot_request"]
        fact = composition._codec.capture_snapshot(request)
        projection = deepcopy(stage["delivery_projection"])
        projection["reference"] = fact["reference"]
        delivery = composition._codec.build_delivery(projection)
        composition._resume_poll(delivery)
    assert poll.observation.status == "COMPLETED"


@pytest.mark.parametrize("issued_at,expected_day_ago", [
    (129_540_000, False),  # 11:59 UTC
    (129_600_000, True),   # 12:00 UTC
    (129_660_000, False),  # 12:01 UTC
])
def test_historical_poll_completion_updates_day_ago_only_in_utc_noon_minute(issued_at, expected_day_ago):
    composition = _composition(_valid_run())
    composition._historical_poll_range = (issued_at, issued_at + 1)
    _complete_poll_through_native_deliveries(composition, issued_at)
    assert ("day_ago" in composition._policy.capital) is expected_day_ago
    if expected_day_ago:
        assert composition._policy.capital["day_ago"] == composition._policy.capital["total"]


def test_historical_noon_rollover_refreshes_on_repeated_completed_five_second_ticks():
    composition = _composition(_valid_run())
    first_noon_tick = 129_600_005
    second_noon_tick = first_noon_tick + 5_000
    composition._historical_poll_range = (first_noon_tick, second_noon_tick + 1)
    _complete_poll_through_native_deliveries(composition, first_noon_tick)
    assert composition._policy.capital["day_ago"] == D("1000")

    _complete_poll_through_native_deliveries(
        composition, second_noon_tick,
        before_final=lambda: composition._policy.capital.__setitem__("total", D("1100")),
    )
    assert composition._policy.capital["day_ago"] == D("1100")


def test_historical_timer_preserves_poll_start_terminal_and_stops_queue(monkeypatch):
    composition = _composition(_valid_run())
    first_raw = composition._historical_poll_range[0] + 5_000
    original_plan = composition._historical_poll_plan

    def invalid_plan(raw_at, sequence):
        plan = original_plan(raw_at, sequence)
        # A due EARN delivery is visible after this capture; force trading capture
        # to precede that visible boundary so the real poll validator rejects it.
        plan["trading"]["snapshot_request"]["captured_at"] = raw_at
        return plan

    monkeypatch.setattr(composition, "_historical_poll_plan", invalid_plan)
    result = composition._dispatch_due(first_raw * 16 + 9)
    timer = composition._records[(5, f"P3_TIMER_{first_raw}")]
    assert result["classification"] == timer["result"]["classification"] == "TERMINAL"
    assert result["reason"] == timer["result"]["reason"] == "INVALID_SCHEMA"
    assert composition._terminal["classification"] == "TERMINAL"
    assert composition._last_popped == timer["key"]
    later_raw = first_raw + 5_000
    assert (later_raw * 16 + 9, 5, 1, f"P3_TIMER_{later_raw}") in composition._queue
    assert composition._records[(5, f"P3_TIMER_{later_raw}")]["result"]["classification"] == "PENDING"
