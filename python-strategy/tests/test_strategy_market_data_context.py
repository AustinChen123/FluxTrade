from dataclasses import replace
from decimal import Decimal
import json
from typing import cast

import pytest

from src.core.backtest.run_evidence import (
    canonical_decision_snapshot,
    configuration_sha256,
)
from src.core.strategy_context import StrategyContext, RiskSnapshot
from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from test_profile_decision_context import item, DAY


def context():
    return StrategyContext(
        "strategy",
        "BINANCE:BTCUSDT-SPOT",
        1,
        Decimal(10),
        Decimal(10),
        Decimal(0),
        Decimal(0),
        Decimal(0),
        Decimal(0),
    )


def test_none_projection_preserves_legacy_hash():
    original = context()
    expected = dict(
        strategy_id="strategy",
        product_id="BINANCE:BTCUSDT-SPOT",
        timestamp=1,
        available_cash=Decimal(10),
        total_equity=Decimal(10),
        realized_pnl=Decimal(0),
        unrealized_pnl=Decimal(0),
        current_drawdown=Decimal(0),
        max_drawdown=Decimal(0),
        position=None,
        open_orders=(),
        latest_fills=(),
        latest_rejections=(),
        risk=RiskSnapshot(),
        capital=None,
    )
    snapshot = canonical_decision_snapshot(original)
    assert original.market_data is None
    assert snapshot == expected
    assert "market_data" not in snapshot
    assert configuration_sha256(snapshot) == configuration_sha256(expected)
    assert replace(original).market_data is None


@pytest.mark.parametrize(
    "bad",
    [True, (), {}, item(), type("Child", (StrategyMarketDataContext,), {})(0, ())],
)
def test_market_data_exact_type(bad):
    with pytest.raises(ValueError, match="^invalid strategy market data context$"):
        replace(context(), market_data=cast(StrategyMarketDataContext, bad))


def test_explicit_empty_and_replace_preserve_evidence():
    original = context()
    evidence = StrategyMarketDataContext(0, ())
    enriched = replace(original, market_data=evidence)
    snapshot = canonical_decision_snapshot(enriched)
    assert snapshot["market_data"] == json.loads(evidence.canonical_bytes)
    assert configuration_sha256(snapshot) != configuration_sha256(
        canonical_decision_snapshot(original)
    )
    assert replace(enriched, timestamp=99).market_data is evidence
    assert original.market_data is None and enriched.market_data is evidence
    snapshot["market_data"] = None
    assert canonical_decision_snapshot(enriched)["market_data"] == json.loads(
        evidence.canonical_bytes
    )


def test_nested_change_affects_snapshot_and_hash_without_context_time_restriction():
    first = item()
    second = replace(first, observed_at_ms=DAY + 3)
    a = replace(context(), market_data=StrategyMarketDataContext(DAY + 3, (first,)))
    b = replace(a, market_data=StrategyMarketDataContext(DAY + 3, (second,)))
    assert a.timestamp == 1  # Collection time need not equal the account context time.
    left, right = canonical_decision_snapshot(a), canonical_decision_snapshot(b)
    assert left != right
    assert configuration_sha256(left) != configuration_sha256(right)
    assert (
        left["market_data"] == json.loads(a.market_data.canonical_bytes)
        if a.market_data
        else False
    )
