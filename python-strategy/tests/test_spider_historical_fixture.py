"""Self-contained tests for the historical fixture builder."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from gzip import open as gzip_open
from hashlib import sha256
import json
from pathlib import Path

import pytest

from tools.spider_historical_fixture import build_input
from src.core.backtest.spider_historical_input import (
    BAR_DURATION_MS,
    decode_historical_run_input,
    encode_historical_run_input,
    validate_historical_input,
)


_PRODUCTS = ("NEO-USDT-SWAP", "GMX-USDT-SWAP")
_START = 90 * 86_400_000


def _source_tree(root: Path, *, missing: Path | None = None) -> tuple[Path, Path, int, int]:
    candle_root = root / "candles"
    meta_root = root / "meta"
    candle_root.mkdir(parents=True)
    meta_root.mkdir()
    start = _START
    warmup = start - 86_400_000
    end = start + 3 * BAR_DURATION_MS
    metadata = {
        "data": [
            {"instId": product, "ctVal": "1", "tickSz": "0.01", "lotSz": "0.1", "minSz": "0.1"}
            for product in _PRODUCTS
        ]
    }
    (meta_root / "instruments_SWAP_20261029T071147Z.json").write_text(json.dumps(metadata))
    tiers = {
        product.replace("-SWAP", ""): [
            {"minSz": "0.1", "maxSz": "1000", "mmr": "0.01", "imr": "0.1", "maxLever": "10"}
        ]
        for product in _PRODUCTS
    }
    (meta_root / "position_tiers_cross_20261029T071147Z.json").write_text(json.dumps(tiers))

    first = warmup - BAR_DURATION_MS
    last = end
    for product in _PRODUCTS:
        for kind in ("trade", "mark"):
            path = candle_root / f"{product}_1m_{kind}.csv.gz"
            if path == missing:
                continue
            with gzip_open(path, "wt", encoding="utf-8", newline="") as stream:
                for sequence, timestamp in enumerate(range(first, last + BAR_DURATION_MS, BAR_DURATION_MS)):
                    price = str(100 + sequence)
                    stream.write(f"{timestamp},{price},{price},{price},{price},4\n")
    return candle_root, meta_root, start, end


def test_builder_is_deterministic_and_obeys_window_decimal_and_ordering(tmp_path: Path) -> None:
    candle_root, meta_root, start, end = _source_tree(tmp_path)
    raw = build_input(
        products=_PRODUCTS, horizon_minutes=3, start_ms=start, run_id="tiny-window",
        candle_root=candle_root, meta_root=meta_root,
    )
    repeated = build_input(
        products=_PRODUCTS, horizon_minutes=3, start_ms=start, run_id="tiny-window",
        candle_root=candle_root, meta_root=meta_root,
    )
    assert raw == repeated
    run = decode_historical_run_input(raw)
    assert validate_historical_input(run)
    assert (run.range_start_ms, run.range_end_ms, run.warmup_start_ms) == (
        start, end, start - 86_400_000,
    )
    expected_timestamps = list(range(run.warmup_start_ms, end, BAR_DURATION_MS))
    for rows in (run.trade_bars, run.mark_bars):
        assert [row.bar_open_ms for row in rows if row.product_id == _PRODUCTS[0]] == expected_timestamps
        assert [row.bar_open_ms for row in rows if row.product_id == _PRODUCTS[1]] == expected_timestamps
        assert [(row.bar_open_ms, _PRODUCTS.index(row.product_id)) for row in rows] == sorted(
            (row.bar_open_ms, _PRODUCTS.index(row.product_id)) for row in rows
        )
        assert all(isinstance(value, Decimal) for row in rows for value in (row.open, row.high, row.low, row.close))
    assert all(isinstance(row.volume, Decimal) for row in run.trade_bars)
    assert all(row.volume is None for row in run.mark_bars)
    all_source_times = [row.bar_open_ms for row in (*run.trade_bars, *run.mark_bars)]
    assert run.warmup_start_ms - BAR_DURATION_MS not in all_source_times
    assert run.warmup_start_ms in all_source_times
    assert end - BAR_DURATION_MS in all_source_times
    assert end not in all_source_times


def test_builder_reports_exact_missing_candle_after_valid_metadata(tmp_path: Path) -> None:
    expected = tmp_path / "candles" / f"{_PRODUCTS[1]}_1m_mark.csv.gz"
    candle_root, meta_root, start, _end = _source_tree(tmp_path, missing=expected)
    with pytest.raises(FileNotFoundError) as error:
        build_input(
            products=_PRODUCTS, horizon_minutes=3, start_ms=start, run_id="missing-candle",
            candle_root=candle_root, meta_root=meta_root,
        )
    assert str(error.value) == str(expected)


def test_fixture_builder_reproduces_pinned_first_oracle_input() -> None:
    from test_spider_formal_acceptance import _ANSWERS_SHA256, _INPUTS_SHA256, ANSWERS, INPUTS, _MATRIX
    from test_spider_historical_oracle import _oracle_run

    assert sha256(INPUTS.read_bytes()).hexdigest() == _INPUTS_SHA256
    assert sha256(ANSWERS.read_bytes()).hexdigest() == _ANSWERS_SHA256
    case, model_id, expected_hash, _ = _MATRIX[0]
    _, oracle = _oracle_run(case, model_id=model_id)
    oracle = replace(oracle, run_id="s0-oracle")
    raw = encode_historical_run_input(oracle)
    assert validate_historical_input(oracle) == expected_hash
    assert raw == encode_historical_run_input(oracle)
