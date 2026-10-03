from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from src.control_plane.full_backtest_request import (
    FullStrategyBacktestRequest,
    parse_backtest_request,
)
from src.control_plane.models import BacktestJobRequest
from test_full_backtest_request import _payload


@pytest.mark.parametrize("kind", [None, "csv_signal_backtest"])
def test_parser_keeps_legacy_csv_request(kind: str | None) -> None:
    payload: dict[str, object] = {
        "strategy_id": "s",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "candles_csv_path": "candles.csv",
        "signals_csv_path": "signals.csv",
        "start_time": 0,
        "end_time": 60_000,
    }
    if kind is not None:
        payload["kind"] = kind
    parsed = parse_backtest_request(payload)
    assert isinstance(parsed, BacktestJobRequest)
    assert parsed.kind == "csv_signal_backtest"
    assert parsed.candles_csv_path == "candles.csv"
    with pytest.raises(ValidationError):
        parse_backtest_request(payload | {"kind": "unknown"})


def test_parser_selects_full_request_after_json_roundtrip() -> None:
    payload = json.loads(json.dumps(_payload()))
    parsed = parse_backtest_request(payload)
    assert isinstance(parsed, FullStrategyBacktestRequest)
    assert parsed.kind == "full_strategy_backtest"
