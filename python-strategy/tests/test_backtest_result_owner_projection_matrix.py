from datetime import UTC, datetime
import hashlib
from dataclasses import replace
from decimal import Decimal, localcontext

import pytest

from src.core.backtest_result_owner import (
    BacktestResultRunIdentity,
    _project_result,
)
from src.core.backtest_result_persistence import (
    ClosedTradeSnapshot,
    FullBacktestOutcome,
)
from src.core.models import PositionSide
from src.core.product_registry import InstrumentSpec, MarketType


def _identity(start: int = 0, end: int = 10, balance: Decimal = Decimal("1000")):
    return BacktestResultRunIdentity(
        "job",
        "strategy",
        "1",
        "a" * 64,
        "dataset",
        "b" * 64,
        "BINANCE:BTCUSDT-SPOT",
        "1m",
        "USDT",
        start,
        end,
        balance,
        InstrumentSpec(
            "BINANCE:BTCUSDT-SPOT",
            "binance",
            "BTC/USDT",
            "BTC",
            "USDT",
            market_type=MarketType.SPOT,
        ),
        Decimal("0"),
        Decimal("0"),
        None,
        None,
    )


def _outcome(
    trades: tuple[ClosedTradeSnapshot, ...] = (),
    equity: tuple[tuple[int, Decimal], ...] = (),
    *,
    total: Decimal = Decimal("7"),
    mtm: Decimal = Decimal("8"),
    balance: Decimal = Decimal("1000"),
) -> FullBacktestOutcome:
    return FullBacktestOutcome(
        balance,
        total,
        mtm,
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        equity,
        trades,
    )


def _trade(exit_time: int, pnl: str, side: PositionSide = PositionSide.LONG):
    return ClosedTradeSnapshot(
        exit_time - 1,
        exit_time,
        Decimal("10"),
        Decimal("11"),
        side,
        Decimal("1"),
        Decimal(pnl),
        Decimal("0.01"),
    )


def _utc_ms(value: datetime) -> int:
    return int(value.replace(tzinfo=UTC).timestamp() * 1000)


def test_projection_payload_and_rows_are_independent_exact_mapping():
    identity = _identity()
    outcome = _outcome(
        (
            _trade(2, "1", PositionSide.SHORT),
            _trade(3, "-2"),
            _trade(4, "0"),
        ),
        ((0, Decimal("1002")), (1, Decimal("999")), (2, Decimal("1001"))),
    )
    result = _project_result(identity, outcome, completed_at=5)

    assert result.summary.total_pnl == Decimal("7")
    assert result.summary.net_pnl == Decimal("8")
    assert result.summary.return_pct == Decimal("0.008")
    assert [tuple(row) for row in result.equity] == [
        (0, 0, Decimal("1002"), Decimal("0")),
        (1, 1, Decimal("999"), Decimal("3")),
        (2, 2, Decimal("1001"), Decimal("1")),
    ]
    assert [(row.sequence, row.side, row.pnl) for row in result.closed_trades] == [
        (0, "SHORT", Decimal("1")),
        (1, "LONG", Decimal("-2")),
        (2, "LONG", Decimal("0")),
    ]
    assert [(row.month, row.return_pct) for row in result.monthly_returns] == [
        ("1970-01", Decimal("-0.001")),
    ]
    assert [(row.lower, row.upper, row.count) for row in result.pnl_distribution] == [
        (None, Decimal("0"), 1),
        (Decimal("0"), None, 2),
    ]
    assert result.result_payload_json == (
        '{"closed_trades":[{"entry_price":"10","entry_time":1,'
        '"exit_price":"11","exit_time":2,"fee":"0.01","pnl":"1",'
        '"quantity":"1","sequence":0,"side":"SHORT"},{"entry_price":"10",'
        '"entry_time":2,"exit_price":"11","exit_time":3,"fee":"0.01",'
        '"pnl":"-2","quantity":"1","sequence":1,"side":"LONG"},'
        '{"entry_price":"10","entry_time":3,"exit_price":"11","exit_time":4,'
        '"fee":"0.01","pnl":"0","quantity":"1","sequence":2,"side":"LONG"}],'
        '"equity":[{"drawdown":"0","equity":"1002","sequence":0,"timestamp":0},'
        '{"drawdown":"3","equity":"999","sequence":1,"timestamp":1},'
        '{"drawdown":"1","equity":"1001","sequence":2,"timestamp":2}],'
        '"monthly_returns":[{"month":"1970-01","return_pct":"-0.001"}],'
        '"pnl_distribution":[{"count":1,"lower":null,"sequence":0,"upper":"0"},'
        '{"count":2,"lower":"0","sequence":1,"upper":null}],"summary":{'
        '"calmar":"0","initial_balance":"1000","max_drawdown":"0",'
        '"net_pnl":"8","return_pct":"0.008","sharpe":"0",'
        '"sortino":"0","total_pnl":"7"}}'
    )
    assert (
        result.result_digest
        == hashlib.sha256(result.result_payload_json.encode()).hexdigest()
    )
    expected_input = (
        '{"currency":"USDT","dataset":{"checksum_sha256":"'
        + "b" * 64
        + '","id":"dataset"},"drawdown_limit":null,"end":10,'
        '"execution_timeframe":null,"fees":{"maker":"0","taker":"0"},'
        '"initial_balance":"1000","instrument":{"base":"BTC",'
        '"capital_model":null,"capital_per_contract":null,"exchange":"binance",'
        '"fee_model":null,"market_type":"spot","min_notional":null,'
        '"min_quantity":null,"multiplier":null,"price_tick":null,'
        '"product_id":"BINANCE:BTCUSDT-SPOT","quantity_step":null,'
        '"quote":"USDT","session_calendar_id":null,"symbol":"BTC/USDT",'
        '"tick_value":null},"product_id":"BINANCE:BTCUSDT-SPOT","start":0,'
        '"subject":{"digest":"' + "a" * 64 + '","id":"strategy:1",'
        '"kind":"STRATEGY_ARTIFACT"},"timeframe":"1m"}'
    )
    assert result.input_payload_json == expected_input
    assert result.input_digest == hashlib.sha256(expected_input.encode()).hexdigest()


def test_identity_job_clock_and_economic_changes_have_distinct_digest_effects():
    identity = _identity()
    outcome = _outcome((_trade(2, "1"),))
    result = _project_result(identity, outcome, completed_at=5)
    dataset_changed = _project_result(
        replace(identity, dataset_id="another-dataset"), outcome, completed_at=5
    )
    operational_changed = _project_result(
        replace(identity, job_id="another-job"), outcome, completed_at=8
    )
    economics_changed = _project_result(
        identity, replace(outcome, total_pnl=Decimal("9")), completed_at=5
    )

    assert dataset_changed.input_digest != result.input_digest
    assert dataset_changed.result_digest == result.result_digest
    assert operational_changed.input_digest == result.input_digest
    assert operational_changed.result_digest == result.result_digest
    assert economics_changed.input_digest == result.input_digest
    assert economics_changed.result_digest != result.result_digest


def test_high_precision_equity_and_monthly_arithmetic_ignore_decimal_context():
    identity = _identity()
    outcome = _outcome(
        (_trade(3, "1.23456789"), _trade(4, "0.00000001")),
        (
            (1, Decimal("100000000000000000000000000000")),
            (2, Decimal("99999999999999999999999999999.123456789")),
        ),
    )
    with localcontext() as context:
        context.prec = 3
        low = _project_result(identity, outcome, completed_at=5)
    with localcontext() as context:
        context.prec = 60
        high = _project_result(identity, outcome, completed_at=5)

    assert low.equity[1].drawdown == high.equity[1].drawdown == Decimal("0.876543211")
    assert (
        low.monthly_returns[0].return_pct
        == high.monthly_returns[0].return_pct
        == Decimal("0.0012345679")
    )
    assert low.result_payload_json == high.result_payload_json
    assert low.result_digest == high.result_digest

    third = _identity(balance=Decimal("3"))
    thirds = _outcome(
        (_trade(3, "1.23456789"), _trade(4, "0.00000001")),
        balance=Decimal("3"),
    )
    with localcontext() as context:
        context.prec = 3
        ratio = _project_result(third, thirds, completed_at=5)
    assert ratio.monthly_returns[0].return_pct == Decimal(
        "0.4115226333333333333333333333"
    )


def test_months_use_utc_exit_time_and_sort_without_reordering_trades():
    january = _utc_ms(datetime(2026, 1, 31, 23, 59, 59, 999000))
    february = _utc_ms(datetime(2026, 2, 1, 0, 0, 0))
    identity = _identity(january - 10, february + 10)
    result = _project_result(
        identity,
        _outcome((_trade(february, "2"), _trade(january, "-1"))),
        completed_at=february,
    )

    assert [row.month for row in result.monthly_returns] == ["2026-01", "2026-02"]
    assert [row.pnl for row in result.closed_trades] == [Decimal("2"), Decimal("-1")]


def test_ratio_exact_termination_and_precision_are_context_independent():
    denominator = 2**100
    identity = _identity(balance=Decimal(denominator))
    outcome = _outcome(mtm=Decimal("1"), balance=Decimal(denominator))
    with localcontext() as ctx:
        ctx.prec = 3
        low = _project_result(identity, outcome, completed_at=0)
    with localcontext() as ctx:
        ctx.prec = 60
        high = _project_result(identity, outcome, completed_at=0)

    exact = Decimal((0, tuple(map(int, str(5**100))), -100))
    assert low.summary.return_pct == high.summary.return_pct == exact
    assert low.result_payload_json == high.result_payload_json
    assert low.result_digest == high.result_digest


def test_nonterminating_ratio_uses_28_significant_digits():
    identity = _identity(balance=Decimal("3"))
    outcome = _outcome(mtm=Decimal("1"), balance=Decimal("3"))

    result = _project_result(identity, outcome, completed_at=0)

    assert result.summary.return_pct == Decimal("0.3333333333333333333333333333")


@pytest.mark.parametrize(
    "samples,trades",
    [
        (((-1, Decimal("1")),), ()),
        (((11, Decimal("1")),), ()),
        (((1, Decimal("1")), (1, Decimal("2"))), ()),
        ((), (_trade(0, "1"),)),
        (
            (),
            (
                ClosedTradeSnapshot(
                    3,
                    2,
                    Decimal("1"),
                    Decimal("1"),
                    PositionSide.LONG,
                    Decimal("1"),
                    Decimal("0"),
                    Decimal("0"),
                ),
            ),
        ),
    ],
)
def test_projection_rejects_invalid_sample_or_trade_window(samples, trades):
    with pytest.raises(ValueError):
        _project_result(_identity(), _outcome(trades, samples), completed_at=5)
