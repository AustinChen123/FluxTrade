"""Integration: CallableStrategy & CsvSignalStrategy through full backtest pipeline.

Requires: compiled fluxtrade_core.so
Pipeline: MemoryDataSource → BacktestRunner → SimulatedAdapter → Rust PyMatchingEngine → PnL
"""
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.data_sources.memory import MemoryDataSource
from src.core.data_sources.csv_source import CsvDataSource
from src.core.backtest_runner import BacktestRunner
from src.core.models import Signal, SignalType, Candlestick
from src.strategies.callable_strategy import CallableStrategy
from src.strategies.csv_signal_strategy import CsvSignalStrategy
from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series

try:
    import fluxtrade_core  # noqa: F401
    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]

INTERVAL_MS = 15 * 60 * 1000


def _simple_predict(candle: Candlestick):
    """Buy every 10th candle, sell every 20th."""
    idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
    if idx % 20 == 10:
        return Signal(
            strategy_id="will_be_overwritten",
            product_id=candle.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=Decimal("0.01"),
        )
    elif idx % 20 == 0 and idx > 0:
        return Signal(
            strategy_id="will_be_overwritten",
            product_id=candle.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.EXIT_LONG,
            quantity=Decimal("0.01"),
        )
    return None


def _predict_with_sl_tp(candle: Candlestick):
    """Buy with SL/TP on candle index 10, exit on index 50."""
    idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
    if idx == 10:
        return Signal(
            strategy_id="x",
            product_id=candle.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=Decimal("0.01"),
            stop_loss=candle.close - Decimal("500"),
            take_profit=candle.close + Decimal("1000"),
        )
    elif idx == 50:
        return Signal(
            strategy_id="x",
            product_id=candle.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.EXIT_LONG,
            quantity=Decimal("0.01"),
        )
    return None


def _run_backtest(strategy, candle_data, mock_session_local, data_source=None):
    mock_session = MagicMock()
    mock_session.query.return_value.filter_by.return_value.all.return_value = []
    mock_session_local.return_value = mock_session

    ds = data_source or MemoryDataSource(candle_data)
    runner = BacktestRunner(
        start_time=candle_data[0].timestamp,
        end_time=candle_data[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=Decimal("10000"),
        data_source=ds,
        fee_config={"maker": 0.0002, "taker": 0.0006},
        report_config={"csv_trades": False, "equity_curve": False,
                       "markdown_report": False, "journal": False},
    )
    runner.add_strategy(strategy)
    return runner.run()


def _stable_backtest_snapshot(result):
    return {
        "total_pnl": result["total_pnl"],
        "total_trades": result["total_trades"],
        "journal_count": result["journal_count"],
        "journal_tags": [entry["tag"] for entry in result["journal"]],
    }


def _assert_journal_invariants(result):
    assert result["journal_count"] == len(result["journal"])
    assert result["journal_count"] > 0

    timestamps = [entry["timestamp"] for entry in result["journal"]]
    assert timestamps == sorted(timestamps)

    for entry in result["journal"]:
        quantity = entry["data"].get("quantity")
        if quantity is not None:
            assert Decimal(str(quantity)) > 0


def _write_candle_csv(path, candle_data):
    lines = ["timestamp,open,high,low,close,volume"]
    for candle in candle_data:
        lines.append(
            f"{candle.timestamp},{candle.open},{candle.high},"
            f"{candle.low},{candle.close},{candle.volume}"
        )
    path.write_text("\n".join(lines) + "\n")


@pytest.fixture
def candle_data():
    return make_candle_series(count=200)


class TestCallableStrategyIntegration:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_callable_full_backtest(self, mock_sl, candle_data):
        """CallableStrategy should complete a full backtest with trading activity."""
        strat = CallableStrategy("callable_e2e", _simple_predict, PRODUCT_ID, TIMEFRAME)
        result = _run_backtest(strat, candle_data, mock_sl)

        assert result is not None
        assert result["journal_count"] > 0, "Callable should generate trades"
        assert isinstance(result["total_pnl"], Decimal)

    @patch("src.core.backtest_runner.SessionLocal")
    def test_callable_backtest_smoke_is_deterministic(self, mock_sl, candle_data):
        """Early replay smoke: deterministic input should produce stable outputs."""
        first = _run_backtest(
            CallableStrategy("callable_smoke", _simple_predict, PRODUCT_ID, TIMEFRAME),
            candle_data,
            mock_sl,
        )
        second = _run_backtest(
            CallableStrategy("callable_smoke", _simple_predict, PRODUCT_ID, TIMEFRAME),
            candle_data,
            mock_sl,
        )

        _assert_journal_invariants(first)
        _assert_journal_invariants(second)
        assert _stable_backtest_snapshot(first) == _stable_backtest_snapshot(second)

    @patch("src.core.backtest_runner.SessionLocal")
    def test_callable_backtest_smoke_accepts_candle_csv_source(
        self, mock_sl, candle_data, tmp_path
    ):
        """A tiny OHLCV CSV fixture should drive the same backtest pipeline."""
        csv_path = tmp_path / "candles.csv"
        _write_candle_csv(csv_path, candle_data)
        csv_source = CsvDataSource(str(csv_path), product_id=PRODUCT_ID, timeframe=TIMEFRAME)

        result = _run_backtest(
            CallableStrategy("callable_csv_smoke", _simple_predict, PRODUCT_ID, TIMEFRAME),
            candle_data,
            mock_sl,
            data_source=csv_source,
        )

        _assert_journal_invariants(result)
        assert isinstance(result["total_pnl"], Decimal)

    @patch("src.core.backtest_runner.SessionLocal")
    def test_callable_with_sl_tp(self, mock_sl, candle_data):
        """CallableStrategy returning SL/TP should be processed by Rust engine."""
        strat = CallableStrategy("callable_sltp", _predict_with_sl_tp, PRODUCT_ID, TIMEFRAME)
        result = _run_backtest(strat, candle_data, mock_sl)

        assert result is not None
        assert result["journal_count"] > 0, "SL/TP signals should generate activity"


class TestCsvSignalIntegration:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_csv_replay_matches_callable(self, mock_sl, candle_data, tmp_path):
        """CSV replay of callable signals should produce identical PnL."""
        lines = ["timestamp,type,price,stop_loss,take_profit,trailing_distance,quantity"]
        for candle in candle_data:
            sig = _simple_predict(candle)
            if sig is not None:
                lines.append(
                    f"{candle.timestamp},{sig.type.value},"
                    f"{sig.price or ''},{sig.stop_loss or ''},"
                    f"{sig.take_profit or ''},{sig.trailing_distance or ''},"
                    f"{sig.quantity or ''}"
                )

        csv_path = tmp_path / "replay.csv"
        csv_path.write_text("\n".join(lines) + "\n")

        callable_strat = CallableStrategy("cmp_callable", _simple_predict, PRODUCT_ID, TIMEFRAME)
        csv_strat = CsvSignalStrategy("cmp_csv", str(csv_path), PRODUCT_ID, TIMEFRAME)

        result_callable = _run_backtest(callable_strat, candle_data, mock_sl)
        result_csv = _run_backtest(csv_strat, candle_data, mock_sl)

        assert result_callable["journal_count"] > 0, \
            "Both strategies must produce trades for comparison to be meaningful"
        assert result_callable["total_pnl"] == result_csv["total_pnl"], \
            "CSV replay must match callable PnL exactly"
        assert result_callable["journal_count"] == result_csv["journal_count"]

    @patch("src.core.backtest_runner.SessionLocal")
    def test_csv_minimal_fields(self, mock_sl, candle_data, tmp_path):
        """CSV with only timestamp+type should work in full backtest."""
        ts = candle_data[10].timestamp
        ts_exit = candle_data[30].timestamp
        csv_path = tmp_path / "minimal.csv"
        csv_path.write_text(
            f"timestamp,type\n{ts},LONG\n{ts_exit},EXIT_LONG\n"
        )

        strat = CsvSignalStrategy("minimal_csv", str(csv_path), PRODUCT_ID, TIMEFRAME)
        result = _run_backtest(strat, candle_data, mock_sl)

        assert result is not None
        assert result["journal_count"] > 0
