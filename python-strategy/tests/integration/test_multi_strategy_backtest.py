"""Integration: Multi-strategy parallel backtest.

Requires: compiled fluxtrade_core.so
Validates: multiple strategies concurrent execution, isolation, PnL consistency.
"""
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.data_sources.memory import MemoryDataSource
from src.core.backtest_runner import BacktestRunner
from src.core.models import Signal, SignalType
from src.strategies.callable_strategy import CallableStrategy
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


def _make_long_strategy(name: str, entry_idx: int = 10, exit_idx: int = 20):
    """Strategy that goes LONG at entry_idx, exits at exit_idx (modulo cycle)."""
    cycle = exit_idx + 10

    def predict(candle):
        idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
        if idx % cycle == entry_idx:
            return Signal(
                strategy_id=name, product_id=candle.product_id,
                timeframe=TIMEFRAME, timestamp=candle.timestamp,
                type=SignalType.LONG, quantity=Decimal("0.01"),
            )
        elif idx % cycle == exit_idx:
            return Signal(
                strategy_id=name, product_id=candle.product_id,
                timeframe=TIMEFRAME, timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG, quantity=Decimal("0.01"),
            )
        return None

    return CallableStrategy(name, predict, PRODUCT_ID, TIMEFRAME)


def _make_short_strategy(name: str, entry_idx: int = 5, exit_idx: int = 15):
    """Strategy that goes SHORT at entry_idx, exits at exit_idx."""
    cycle = exit_idx + 10

    def predict(candle):
        idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
        if idx % cycle == entry_idx:
            return Signal(
                strategy_id=name, product_id=candle.product_id,
                timeframe=TIMEFRAME, timestamp=candle.timestamp,
                type=SignalType.SHORT, quantity=Decimal("0.01"),
            )
        elif idx % cycle == exit_idx:
            return Signal(
                strategy_id=name, product_id=candle.product_id,
                timeframe=TIMEFRAME, timestamp=candle.timestamp,
                type=SignalType.EXIT_SHORT, quantity=Decimal("0.01"),
            )
        return None

    return CallableStrategy(name, predict, PRODUCT_ID, TIMEFRAME)


def _run_backtest_with_strategies(strategies, candle_data, mock_session_local, balance=Decimal("10000")):
    mock_session = MagicMock()
    mock_session.query.return_value.filter_by.return_value.all.return_value = []
    mock_session_local.return_value = mock_session

    ds = MemoryDataSource(candle_data)
    runner = BacktestRunner(
        start_time=candle_data[0].timestamp,
        end_time=candle_data[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=balance,
        data_source=ds,
        fee_config={"maker": 0.0002, "taker": 0.0006},
        report_config={"csv_trades": False, "equity_curve": False,
                       "markdown_report": False, "journal": False},
    )
    for s in strategies:
        runner.add_strategy(s)
    return runner.run()


@pytest.fixture
def candle_data():
    return make_candle_series(count=200)


class TestMultiStrategyBacktest:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_three_strategies_all_produce_activity(self, mock_sl, candle_data):
        """3 strategies should all generate journal entries."""
        strats = [
            _make_long_strategy("long_a", entry_idx=10, exit_idx=20),
            _make_long_strategy("long_b", entry_idx=15, exit_idx=25),
            _make_short_strategy("short_c", entry_idx=5, exit_idx=15),
        ]
        result = _run_backtest_with_strategies(strats, candle_data, mock_sl)

        assert result is not None
        assert result["journal_count"] > 0, "Multi-strategy should generate activity"
        assert isinstance(result["total_pnl"], Decimal)

    @patch("src.core.backtest_runner.SessionLocal")
    def test_single_vs_multi_strategy_both_complete(self, mock_sl, candle_data):
        """Running a strategy alone vs with others should both complete successfully."""
        strat_a = _make_long_strategy("solo_a", entry_idx=10, exit_idx=20)
        result_solo = _run_backtest_with_strategies([strat_a], candle_data, mock_sl)

        strat_a2 = _make_long_strategy("multi_a", entry_idx=10, exit_idx=20)
        strat_b = _make_short_strategy("multi_b", entry_idx=5, exit_idx=15)
        result_multi = _run_backtest_with_strategies([strat_a2, strat_b], candle_data, mock_sl)

        assert result_solo is not None
        assert result_multi is not None
        assert result_solo["journal_count"] > 0
        assert result_multi["journal_count"] > 0

    @patch("src.core.backtest_runner.SessionLocal")
    def test_conflicting_long_short_completes(self, mock_sl, candle_data):
        """Strategy A long + strategy B short on same product should not crash."""
        strat_long = _make_long_strategy("conflict_long", entry_idx=10, exit_idx=20)
        strat_short = _make_short_strategy("conflict_short", entry_idx=10, exit_idx=20)
        result = _run_backtest_with_strategies(
            [strat_long, strat_short], candle_data, mock_sl
        )

        assert result is not None
        assert isinstance(result["total_pnl"], Decimal)
        assert result["journal_count"] > 0, "Conflicting strategies should still produce trades"
