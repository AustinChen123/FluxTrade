from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import ast
import re

import pytest

from src.core.backtest import spider_configured_scale_input as scale
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_historical_input import (
    BAR_DURATION_MS,
    SCHEMA_ID,
    SCHEMA_VERSION,
    HistoricalBar,
    HistoricalInputError,
    HistoricalRunInput,
    InstrumentSpecEvidence,
    SourceManifest,
    admit_before_construction,
    decode_p2_configuration,
    validate_historical_input,
)


ROOT = Path(__file__).parents[1]
CONTRACTS = ROOT.parent / "docs/internal/spider_engineering_requirements_v1/p3"
INPUTS = CONTRACTS / "P3_ORACLE_INPUTS_V1.txt"
ANSWERS = CONTRACTS / "P3_ORACLE_ANSWERS_V1.txt"
HASH = "0" * 64
INPUTS_SHA256 = "f0ef34464e3ccefd4cc7ea21485698e3f6398a0f323354bb7ac41156e4c3dd51"
ANSWERS_SHA256 = "c9ac9a8235f5f30f00ce5ba581e363f43c2762ac0129de763f80b1e6dba3a5a6"
P2_CONFIGURATION_SHA256 = "807054044bdd182274509535ecf8bbc4598f00b6a92b598c6228129a6ff11b70"


def _rows(product: str, start: int, *, trade: bool) -> tuple[HistoricalBar, ...]:
    result = []
    for sequence, timestamp in enumerate(range(start, start + 1441 * BAR_DURATION_MS, BAR_DURATION_MS)):
        row = HistoricalBar(
            product, timestamp, Decimal("100"), Decimal("100"), Decimal("100"),
            Decimal("100"), 1, sequence, Decimal("4") if trade else None,
        )
        result.append(replace(row, source_row_hash=sha256(row.canonical_row(trade=trade)).hexdigest()))
    return tuple(result)


def _manifest(source: str, rows: tuple[HistoricalBar, ...], *, trade: bool) -> SourceManifest:
    ordinals = {"A-USDT-SWAP": 0, "B-USDT-SWAP": 1}
    ordered = sorted(rows, key=lambda row: (ordinals[row.product_id], row.source_sequence))
    digest = sha256(b"".join(bytes.fromhex(row.source_row_hash) for row in ordered)).hexdigest()
    return SourceManifest(
        source, "OKX_PUBLIC_V5",
        "/api/v5/market/history-candles" if trade else "/api/v5/market/history-mark-price-candles",
        (("bar", "1m"),), "2026-09-29T00:00:00Z", 200,
        HASH, digest, "V1", "OKX_HISTORY_CANDLES_V1",
        (("A-USDT-SWAP", "A-USDT-SWAP"), ("B-USDT-SWAP", "B-USDT-SWAP")),
        "CONTRACTS" if trade else "PRICE", len(rows), len(rows),
        min(row.bar_open_ms for row in rows), max(row.bar_open_ms for row in rows),
        "REJECT", "REJECT", "ARCHIVED_FIXTURE",
    )


def _p2_configuration(products=("A-USDT-SWAP", "B-USDT-SWAP")):
    return {
        "schema_version": "synthetic_multi_product_config_v1",
        "config_id": "P3A_TEST_CONFIG_V1",
        "seed_effective_at": 0,
        "cash": "1000",
        "leverage": "10",
        "products": [{
            "product_id": product,
            "instrument_code": ordinal,
            "taker_fee_rate": "0.001",
            "liquidation_fee_rate": "0.001",
            "specs": [{"version": "spec-v1", "valid_from": 0, "valid_to": None,
                       "contract_value": "1", "multiplier": "1", "price_tick": "0.01",
                       "quantity_step": "0.001", "minimum_quantity": "0.001"}],
            "tiers": [{"version": "tier-v1", "valid_from": 0, "valid_to": None,
                       "rows": [{"minimum_contracts": "0", "maximum_contracts": "100",
                                 "mmr": "0.005", "imr": "0.1", "max_leverage": "10"}]}],
            "marks": [{"valid_from": 0, "valid_to": 86_400_000, "mark": "100"}],
        } for ordinal, product in enumerate(products, start=1)],
        "positions": [],
        "orders": [],
    }


def _policy_cache_state_row(product):
    return {"product_id": product, "name": product, "active": "true", "leverage": "4",
            "歩差": "0.01", "單數": "2", "hold上限": "0.8", "hold下限": "-0.8", "hold": "0"}


def _policy_cache_state_market(product):
    return {"product_id": product, "price": "100", "ctVal": "1", "lotSz": "0.001",
            "minSz": "0.001", "increment": "0.01", "ratioHL": "0.1",
            "state": "live", "instIdCode": 1}


def _policy_cache(products=("A-USDT-SWAP", "B-USDT-SWAP"), **overrides):
    state = {
        "schema_version": "SPIDER_POLICY_CACHE_V1",
        "running": True,
        "paused": False,
        "online": True,
        "ws_open": False,
        "order_id": 1,
        "capital": {"total": "1000", "usdt": "1000", "avail": "1000", "earn": "0", "position": "0"},
        "rows": [_policy_cache_state_row(product) for product in products],
        "markets": [_policy_cache_state_market(product) for product in products],
        "replies": {},
        "orders": {},
        "positions": {},
        "last_filled_price": {},
    }
    state.update(overrides)
    return canonical_bytes(state)


def _initial_account_state(**overrides):
    state = {"cash": "1000", "orders": [], "positions": []}
    state.update(overrides)
    return canonical_bytes(state)


def _valid_run() -> HistoricalRunInput:
    start, end = 86_400_000, 86_460_000
    warmup = start - 86_400_000
    trade = tuple(sorted(_rows("A-USDT-SWAP", warmup, trade=True) + _rows("B-USDT-SWAP", warmup, trade=True),
                         key=lambda row: (row.bar_open_ms, 0 if row.product_id.startswith("A-") else 1)))
    mark = tuple(sorted(_rows("A-USDT-SWAP", warmup, trade=False) + _rows("B-USDT-SWAP", warmup, trade=False),
                        key=lambda row: (row.bar_open_ms, 0 if row.product_id.startswith("A-") else 1)))
    specs = tuple(InstrumentSpecEvidence(p, start, Decimal("1"), Decimal("1"), Decimal("0.01"),
                                         Decimal("0.001"), Decimal("0.001"), HASH)
                  for p in ("A-USDT-SWAP", "B-USDT-SWAP"))
    configuration = _p2_configuration()
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = end
    configuration_bytes = canonical_bytes(configuration)
    return HistoricalRunInput(
        SCHEMA_ID, SCHEMA_VERSION, "transport-id", "account", "SPIDER_GRID_ORIGINAL_V1", "1", HASH,
        "SPIDER_GRID_ORIGINAL_V1", (("defaultN", Decimal("1")),), configuration_bytes,
        sha256(configuration_bytes).hexdigest(),
        ("A-USDT-SWAP", "B-USDT-SWAP"),
        "OHLC4_OPEN_HIGH_LOW_CLOSE_V1", 1, start, end, BAR_DURATION_MS, warmup,
        _manifest("P3_ORACLE_TRADE_V1", trade, trade=True), _manifest("P3_ORACLE_MARK_V1", mark, trade=False),
        trade, mark, specs, tuple(replace(s, effective_at_ms=end) for s in specs),
        Decimal("10"), "MODELLING_ASSUMPTION", 5000, start + 5000, "UTC", 1, 5001, 2, 2,
        "POLL_ORDERED_V1", "MTM_PRESERVE_OPEN_V1",
        _policy_cache(), _initial_account_state(),
    )


def _rehashed_run(run, *, trade_rows=None, mark_rows=None):
    changed_trade = run.trade_bars if trade_rows is None else trade_rows
    changed_mark = run.mark_bars if mark_rows is None else mark_rows
    def rehash(rows, trade):
        return tuple(replace(row, source_row_hash=sha256(row.canonical_row(trade=trade)).hexdigest()) for row in rows)
    changed_trade = rehash(changed_trade, True)
    changed_mark = rehash(changed_mark, False)
    def update_manifest(manifest, rows):
        ordinal = {"A-USDT-SWAP": 0, "B-USDT-SWAP": 1}
        ordered = sorted(rows, key=lambda row: (ordinal[row.product_id], row.source_sequence))
        digest = sha256(b"".join(bytes.fromhex(row.source_row_hash) for row in ordered)).hexdigest()
        return replace(manifest, canonical_rows_sha256=digest)
    return replace(run, trade_bars=changed_trade, mark_bars=changed_mark,
                   trade_manifest=update_manifest(run.trade_manifest, changed_trade),
                   mark_manifest=update_manifest(run.mark_manifest, changed_mark))


def _run_with_configuration(run, configuration):
    raw = canonical_bytes(configuration)
    return replace(run, configuration_bytes=raw, configuration_sha256=sha256(raw).hexdigest())


def _run_with_bad_seed_reference(run):
    configuration = _p2_configuration()
    position = {"product_id": "UNKNOWN-USDT-SWAP", "side": "LONG", "quantity_contracts": "NaN", "lots": []}
    configuration["positions"] = [position]
    raw = canonical_bytes(configuration)
    seeded = replace(run, configuration_bytes=raw, configuration_sha256=sha256(raw).hexdigest(),
                     initial_account_state=_initial_account_state(positions=[position]))
    return seeded


def test_valid_input_hash_is_deterministic_and_excludes_transport_run_id():
    run = _valid_run()
    assert validate_historical_input(run) == validate_historical_input(run)
    assert validate_historical_input(replace(run, run_id="another-transport-id")) == validate_historical_input(run)
    original = validate_historical_input(run)
    changed_config = _p2_configuration()
    changed_config["config_id"] = "P3A_TEST_CONFIG_CHANGED_V1"
    changed_config["cash"] = "1100"
    changed_configuration = canonical_bytes(changed_config)
    changes = (
        replace(run, configuration_bytes=changed_configuration,
                configuration_sha256=sha256(changed_configuration).hexdigest(),
                initial_account_state=_initial_account_state(cash="1100")),
        replace(run, market_slippage_bps=Decimal("11")),
        replace(run, execution_fee_provenance="OKX_FEE@v1:sha256:" + HASH),
        replace(run, initial_policy_cache=_policy_cache(capital={"total": "1100", "usdt": "1000", "avail": "1000", "earn": "0", "position": "0"})),
    )
    assert all(validate_historical_input(change) != original for change in changes)


def test_existing_p2_configuration_codec_and_identity_are_reused():
    plan = scale._configured_scale_plan_input()
    configuration = plan["configuration"]
    configuration_bytes = canonical_bytes(configuration)
    assert plan["configuration_sha256"] == P2_CONFIGURATION_SHA256
    assert sha256(configuration_bytes).hexdigest() == P2_CONFIGURATION_SHA256
    products = tuple(product["product_id"] for product in configuration["products"])
    assert decode_p2_configuration(configuration_bytes, P2_CONFIGURATION_SHA256, products) == configuration


@pytest.mark.parametrize("mutation", [
    lambda r: replace(r, schema_version="future"),
    lambda r: replace(r, model_id="UNKNOWN"),
    lambda r: replace(r, model_version=2),
    lambda r: replace(r, policy_source_sha256="bad"),
    lambda r: replace(r, configuration_sha256=HASH),
    lambda r: replace(r, timer_period_ms=1000),
    lambda r: replace(r, poll_profile="UNSUPPORTED"),
    lambda r: replace(r, endpoint_policy_id="OTHER"),
    lambda r: replace(r, initial_policy_cache=()),
    lambda r: replace(r, initial_policy_cache=canonical_bytes({"schema_version": "SPIDER_POLICY_CACHE_V1"})),
    lambda r: replace(r, initial_policy_cache=_policy_cache(unrelated="extra")),
    lambda r: replace(r, initial_policy_cache=_policy_cache(markets=[dict(_policy_cache_state_market("A-USDT-SWAP"), price="NaN"), _policy_cache_state_market("B-USDT-SWAP")])),
    lambda r: replace(r, initial_policy_cache=_policy_cache(rows=[dict(_policy_cache_state_row("A-USDT-SWAP"), leverage="nonsense"), _policy_cache_state_row("B-USDT-SWAP")])),
    lambda r: replace(r, initial_policy_cache=_policy_cache(last_filled_price={"UNKNOWN-USDT-SWAP": {"buy": "0", "sell": "0"}})),
    lambda r: _run_with_bad_seed_reference(r),
    lambda r: replace(r, initial_policy_cache=_policy_cache(capital={"total": 1000, "usdt": "1000", "avail": "1000", "earn": "0", "position": "0"})),
    lambda r: replace(r, initial_account_state=canonical_bytes({"cash": "1000", "positions": []})),
    lambda r: replace(r, initial_account_state=_initial_account_state(unrelated="extra")),
    lambda r: replace(r, initial_account_state=canonical_bytes({"cash": 1000, "orders": [], "positions": []})),
    lambda r: replace(r, initial_account_state=_initial_account_state(cash="1100")),
    lambda r: _run_with_configuration(r, _p2_configuration(("C-USDT-SWAP", "D-USDT-SWAP"))),
    lambda r: replace(r, parameters=(("defaultN", Decimal("1")), ("defaultN", Decimal("2")))),
    lambda r: replace(r, trade_bars=r.trade_bars[:-1]),
    lambda r: _rehashed_run(r, trade_rows=(replace(r.trade_bars[0], confirm=0),) + r.trade_bars[1:]),
    lambda r: _rehashed_run(r, trade_rows=(replace(r.trade_bars[0], open=Decimal("0")),) + r.trade_bars[1:]),
    lambda r: _rehashed_run(r, trade_rows=(replace(r.trade_bars[0], volume=Decimal("-1")),) + r.trade_bars[1:]),
    lambda r: _rehashed_run(r, mark_rows=(replace(r.mark_bars[0], volume=Decimal("1")),) + r.mark_bars[1:]),
    lambda r: replace(r, trade_manifest=replace(r.trade_manifest, retrieved_at_utc="not-utc")),
    lambda r: replace(r, mark_manifest=replace(r.mark_manifest, provider="other")),
    lambda r: replace(r, spec_before=(replace(r.spec_before[0], price_tick=1.0), r.spec_before[1])),
])
def test_invalid_input_never_reaches_owner_or_store_construction(mutation):
    calls = []
    def owner(contract_hash):
        calls.append(("owner", contract_hash))
        return object()
    def store(contract_hash):
        calls.append(("store", contract_hash))
        return object()

    with pytest.raises(HistoricalInputError):
        admit_before_construction(mutation(_valid_run()), owner, store)
    assert calls == []


def test_admitted_input_reaches_each_construction_seam_once():
    calls = []
    digest, owner, store = admit_before_construction(
        _valid_run(), lambda h: calls.append(("owner", h)) or "owner",
        lambda h: calls.append(("store", h)) or "store",
    )
    assert len(digest) == 64
    assert owner == "owner" and store == "store"
    assert [kind for kind, _ in calls] == ["owner", "store"]
    assert all(value == digest for _, value in calls)


def test_frozen_oracle_files_have_exact_ordered_records_and_no_answers_in_input():
    input_bytes, answer_bytes = INPUTS.read_bytes(), ANSWERS.read_bytes()
    assert sha256(input_bytes).hexdigest() == INPUTS_SHA256
    assert sha256(answer_bytes).hexdigest() == ANSWERS_SHA256
    input_lines = input_bytes.decode("utf-8").splitlines()
    answer_lines = answer_bytes.decode("utf-8").splitlines()
    ids = [f"H{i:02d}" for i in range(1, 12)]
    def parse(lines):
        parsed = []
        for line in lines:
            case_id, *segments = line.split("|")
            assert case_id in ids and segments
            record = {}
            for index, segment in enumerate(segments):
                key, separator, value = segment.partition("=")
                if not separator:
                    shorthand = re.match(r"([A-Za-z_][A-Za-z0-9_]*?)(?=E[+-]|\d)", segment)
                    if shorthand is not None:
                        key = shorthand.group(1)
                        value = segment[len(key):]
                    else:
                        key, value = f"_bare_{index}", segment
                assert key.strip() and value.strip()
                assert key not in record
                record[key] = value
            parsed.append((case_id, record))
        return parsed
    inputs, answers = parse(input_lines), parse(answer_lines)
    assert [case_id for case_id, _ in inputs] == ids
    assert [case_id for case_id, _ in answers] == ids
    input_anchors = ({"case", "bars"}, {"case", "delivery_schedule"}, {"case", "accept", "bars"},
                     {"case", "seed_order"}, {"case", "attempts"}, {"case", "snapshot"},
                     {"case", "activation"}, {"case", "poll_range"}, {"case", "models"},
                     {"case", "tiers"}, {"case", "faults"})
    answer_anchors = ({"endpoint"}, {"endpoint"}, {"endpoint"}, {"endpoint"}, {"endpoint"},
                      {"check", "hold_boundary"}, {"endpoint"}, {"ordered", "noon"},
                      {"each_endpoint"}, {"endpoint"}, {"A", "B", "C"})
    for (case_id, input_record), (answer_id, answer_record), input_keys, answer_keys in zip(
        inputs, answers, input_anchors, answer_anchors, strict=True
    ):
        assert case_id == answer_id
        assert input_keys <= input_record.keys()
        assert answer_keys <= answer_record.keys()
        assert any(re.search(r"(?:E[+-]\d+|\d+(?:\.\d+)?)", value)
                   for value in (*input_record.values(), *answer_record.values()))
    forbidden_answer_markers = ("expected_execution", "policy_answer", "oracle_result")
    assert not any(marker in line.lower() for line in input_lines for marker in forbidden_answer_markers)


def test_input_module_architecture_forbids_provider_and_financial_owner_imports():
    source = (ROOT / "src/core/backtest/spider_historical_input.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden = ("ccxt", "requests", "httpx", "aiohttp", "pandas", "src.core.backtest.spider_scenario_run",
                 "urllib", "http.client",
                 "src.core.backtest.spider_run_store", "src.core.backtest.synthetic_scenario_codec",
                 "src.core.backtest.spider_scenario_account")
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert not any(name == blocked or name.startswith(blocked + ".") for name in imports for blocked in forbidden)
    assert "P3_ORACLE_ANSWERS_V1" not in source
