from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.control_plane import backtest_jobs
from src.control_plane.backtest_jobs import BacktestJobExecutor, BacktestRunSpec
from src.core.analytics import _normalize_initial_balance, calculate_metrics
from src.core.backtest_runner import BacktestRunner, _write_markdown_report
from src.core.models import OrderSide, Trade


_EXACT_BALANCE = Decimal("10000.0000000000000000001")
_INVALID_BALANCE_MESSAGE = "initial_balance must be a positive finite decimal value"


def test_runner_default_initial_balance_is_decimal() -> None:
    runner = BacktestRunner(
        start_time=0,
        end_time=1,
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
    )

    assert type(runner.initial_balance) is Decimal
    assert runner.initial_balance == Decimal("10000")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(Decimal("10000.2500"), Decimal("10000.2500"), id="decimal"),
        pytest.param(10_000, Decimal("10000"), id="integer"),
        pytest.param(10000.25, Decimal("10000.25"), id="float"),
        pytest.param("1.000025E4", Decimal("10000.25"), id="string"),
    ],
)
def test_runner_owns_one_decimal_initial_balance(
    value: Decimal | int | float | str,
    expected: Decimal,
) -> None:
    runner = BacktestRunner(
        start_time=0,
        end_time=1,
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        initial_balance=value,
    )

    assert type(runner.initial_balance) is Decimal
    assert runner.initial_balance == expected
    if type(value) is Decimal:
        assert runner.initial_balance is value


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param(object(), id="object"),
        pytest.param("", id="blank"),
        pytest.param("not-a-number", id="non-numeric"),
        pytest.param(0, id="zero"),
        pytest.param(-1, id="negative"),
        pytest.param(Decimal("NaN"), id="decimal-nan"),
        pytest.param(Decimal("Infinity"), id="decimal-infinity"),
        pytest.param(float("nan"), id="float-nan"),
        pytest.param(float("inf"), id="float-infinity"),
    ],
)
def test_normalizer_rejects_invalid_initial_balance_before_owner_use(
    value: object,
) -> None:
    with pytest.raises(ValueError, match=_INVALID_BALANCE_MESSAGE):
        _normalize_initial_balance(value)


def _drawdown_trades() -> list[Trade]:
    return [
        Trade(
            id="entry",
            product_id="BINANCE:BTCUSDT-PERP",
            timestamp=1_000,
            side=OrderSide.BUY,
            price=Decimal("100"),
            quantity=Decimal("1"),
        ),
        Trade(
            id="exit",
            product_id="BINANCE:BTCUSDT-PERP",
            timestamp=86_401_000,
            side=OrderSide.SELL,
            price=Decimal("90"),
            quantity=Decimal("1"),
        ),
    ]


@pytest.mark.parametrize(
    "initial_balance",
    [Decimal("1000"), 1000, 1000.0, "1E3"],
)
def test_metrics_preserve_the_literal_baseline_for_supported_inputs(
    initial_balance: Decimal | int | float | str,
) -> None:
    expected = calculate_metrics(
        _drawdown_trades(),
        initial_balance=Decimal("1000"),
    )
    metrics = calculate_metrics(
        _drawdown_trades(),
        initial_balance=initial_balance,
    )

    assert metrics == expected
    assert metrics["total_pnl"] == Decimal("-10.00")
    assert metrics["max_drawdown"] == Decimal("-10.00")
    assert metrics["calmar_ratio"] == Decimal("-365.0000")
    assert metrics["closed_trade_count"] == 1


def test_metrics_preserve_high_precision_mark_to_market_balance() -> None:
    final_equity = _EXACT_BALANCE + Decimal("0.0000000000000000001")

    metrics = calculate_metrics(
        [],
        initial_balance=_EXACT_BALANCE,
        equity_samples=[
            (1, _EXACT_BALANCE),
            (2, final_equity),
        ],
    )

    assert metrics["mark_to_market_pnl"] == Decimal("0.0000000000000000001")
    assert metrics["max_drawdown"] == Decimal("0")


@pytest.mark.parametrize(
    "value",
    [0, -1, Decimal("NaN"), Decimal("Infinity"), "not-a-number"],
)
def test_metrics_reject_invalid_initial_balance(
    value: Decimal | int | str,
) -> None:
    with pytest.raises(ValueError, match=_INVALID_BALANCE_MESSAGE):
        calculate_metrics([], initial_balance=value)


def test_decimal_owner_preserves_legacy_report_bytes(tmp_path) -> None:
    metrics = {
        "total_pnl": Decimal("0"),
        "total_trades": 0,
        "win_rate": Decimal("0"),
        "profit_factor": Decimal("0"),
        "max_drawdown": Decimal("0"),
        "trade_sharpe": Decimal("0"),
        "avg_trade": Decimal("0"),
        "sortino_ratio": Decimal("0"),
        "calmar_ratio": Decimal("0"),
        "max_drawdown_days": Decimal("0"),
        "avg_hold_time_hours": Decimal("0"),
        "trade_frequency_per_day": Decimal("0"),
        "max_consecutive_wins": 0,
        "max_consecutive_win_amount": Decimal("0"),
        "max_consecutive_losses": 0,
        "max_consecutive_loss_amount": Decimal("0"),
        "gross_profit": Decimal("0"),
        "gross_loss": Decimal("0"),
    }
    float_report = tmp_path / "float.md"
    decimal_report = tmp_path / "decimal.md"
    common = {
        "metrics": metrics,
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "start_time": 0,
        "end_time": 1,
        "fee_config": {},
        "candle_count": 0,
    }

    _write_markdown_report(
        **common,
        initial_balance=10000.25,
        path=float_report,
    )
    _write_markdown_report(
        **common,
        initial_balance=Decimal("10000.25"),
        path=decimal_report,
    )

    assert decimal_report.read_bytes() == float_report.read_bytes()
    assert b"10,000.25" in decimal_report.read_bytes()


def test_control_plane_preserves_high_precision_decimal_for_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class RecordingRunner:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def add_strategy(self, strategy: object) -> None:
            captured["strategy"] = strategy

        def run(self) -> dict[str, object]:
            return {"total_pnl": Decimal("0")}

    strategy = object()
    monkeypatch.setattr(backtest_jobs, "BacktestRunner", RecordingRunner)
    monkeypatch.setattr(
        backtest_jobs,
        "CsvSignalStrategy",
        MagicMock(return_value=strategy),
    )
    request = BacktestRunSpec(
        strategy_id="strategy-1",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        signals_csv_path="unused.csv",
        start_time=0,
        end_time=1,
        initial_balance=_EXACT_BALANCE,
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        instrument=None,
        write_reports=False,
    )

    result = BacktestJobExecutor(run_inline=True).run_backtest_request(
        request,
        data_source=MagicMock(),
    )

    assert captured["initial_balance"] is request.initial_balance
    assert captured["strategy"] is strategy
    assert result == {"total_pnl": "0"}
