from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from src.core.backtest_result_owner import (
    BacktestResultProjection,
    BacktestResultRunIdentity,
    BacktestResultSummaryValues,
    ClosedTradeValues,
    EquitySampleValues,
    MonthlyReturnValues,
    PnlDistributionValues,
)
from src.core.product_registry import InstrumentSpec, MarketType


def test_result_value_types_are_named_ordered_immutable_fields():
    identity = BacktestResultRunIdentity(
        job_id="job-1",
        strategy_id="strategy-1",
        artifact_version="1",
        catalog_sha256="a" * 64,
        dataset_id="dataset-1",
        dataset_checksum_sha256="b" * 64,
        product_id="BINANCE:BTCUSDT-SPOT",
        timeframe="1m",
        currency="USDT",
        start=1,
        end=2,
        initial_balance=Decimal("10"),
        instrument=InstrumentSpec(
            "BINANCE:BTCUSDT-SPOT",
            "binance",
            "BTC/USDT",
            "BTC",
            "USDT",
            market_type=MarketType.SPOT,
        ),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        drawdown_limit=None,
        execution_timeframe=None,
    )
    summary = BacktestResultSummaryValues(*[Decimal("1")] * 8)
    equity = EquitySampleValues(0, 1, Decimal("10"), Decimal("0"))
    trade = ClosedTradeValues(
        0,
        1,
        2,
        "LONG",
        Decimal("1"),
        Decimal("10"),
        Decimal("11"),
        Decimal("0"),
        Decimal("1"),
    )
    monthly = MonthlyReturnValues("2026-01", Decimal("0.1"))
    bucket = PnlDistributionValues(0, None, Decimal("0"), 1)
    projection = BacktestResultProjection(
        identity=identity,
        completed_at=3,
        summary=summary,
        equity=(equity,),
        closed_trades=(trade,),
        monthly_returns=(monthly,),
        pnl_distribution=(bucket,),
        input_digest="a" * 64,
        result_digest="b" * 64,
        input_payload_json="{}",
        result_payload_json="{}",
    )

    assert summary._fields == (
        "initial_balance",
        "total_pnl",
        "net_pnl",
        "return_pct",
        "max_drawdown",
        "sharpe",
        "sortino",
        "calmar",
    )
    assert [row.sequence for row in projection.equity] == [0]
    assert projection.closed_trades == (trade,)
    assert projection.monthly_returns == (monthly,)
    assert projection.pnl_distribution == (bucket,)
    with pytest.raises(FrozenInstanceError):
        setattr(projection, "completed_at", 4)
