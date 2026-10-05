from decimal import Decimal

from src.core.backtest_result_owner import BacktestResultRunIdentity, _project_result
from src.core.backtest_result_persistence import FullBacktestOutcome
from src.core.product_registry import InstrumentSpec


def _identity() -> BacktestResultRunIdentity:
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
        0,
        10,
        Decimal("1000"),
        InstrumentSpec("BINANCE:BTCUSDT-SPOT", "binance", "BTC/USDT", "BTC", "USDT"),
        Decimal("0"),
        Decimal("0"),
        None,
        None,
    )


def test_projector_returns_immutable_summary_and_canonical_empty_result():
    identity = _identity()
    outcome = FullBacktestOutcome(
        Decimal("1000"),
        Decimal("7"),
        Decimal("8"),
        Decimal("3"),
        Decimal("1.25"),
        Decimal("2.5"),
        Decimal("0.75"),
        (),
        (),
    )

    result = _project_result(identity, outcome, completed_at=5)

    assert tuple(result.summary) == tuple(
        map(Decimal, ("1000", "7", "8", "0.008", "3", "1.25", "2.5", "0.75"))
    )
    assert result.equity == result.closed_trades == ()
    assert result.monthly_returns == result.pnl_distribution == ()
    assert result.result_payload_json == (
        '{"closed_trades":[],"equity":[],"monthly_returns":[],'
        '"pnl_distribution":[],"summary":{"calmar":"0.75",'
        '"initial_balance":"1000","max_drawdown":"3","net_pnl":"8",'
        '"return_pct":"0.008","sharpe":"1.25","sortino":"2.5",'
        '"total_pnl":"7"}}'
    )
    assert len(result.input_digest) == len(result.result_digest) == 64
