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
from src.core.market_data.profiles.composite_types import CompositeProfile
from test_profile_decision_context import item, DAY


def test_merge_identity_propagates_without_changing_legacy_evidence(monkeypatch):
    # Identity propagation only; this does not implement or validate a v2 merge.
    decision = item()
    assert decision.profile is not None
    collection = StrategyMarketDataContext(DAY + 3, (decision,))
    legacy = context()
    explicit = replace(legacy, market_data=collection)
    legacy_projection = canonical_decision_snapshot(legacy)
    legacy_hash = configuration_sha256(legacy_projection)
    old_id = decision.profile.composite_id
    old_item, old_collection = decision.digest, collection.digest
    old_hash = configuration_sha256(canonical_decision_snapshot(explicit))
    monkeypatch.setattr(
        CompositeProfile,
        "merge_algorithm_version",
        property(lambda self: "aligned-sum-v2"),
    )
    assert decision.profile.composite_id != old_id
    payload = json.loads(decision.canonical_bytes)
    assert payload["profile"]["composite_id"] == decision.profile.composite_id
    assert payload["profile"]["merge_algorithm_version"] == "aligned-sum-v2"
    assert decision.digest != old_item
    assert collection.digest != old_collection
    assert configuration_sha256(canonical_decision_snapshot(explicit)) != old_hash
    assert canonical_decision_snapshot(legacy) == legacy_projection
    assert configuration_sha256(canonical_decision_snapshot(legacy)) == legacy_hash


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
