"""Integration test: End-to-end backtest pipeline.

MemoryDataSource → BacktestRunner → SimulatedAdapter → Rust PyMatchingEngine → PnL.

Requires: compiled fluxtrade_core.so
"""
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.data_sources.memory import MemoryDataSource
from src.core.backtest_runner import BacktestRunner
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType
from src.core.strategy_context import StrategyContext
from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle, make_candle_series

# Skip if Rust .so is not available
try:
    import fluxtrade_core  # noqa: F401
    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]


# ---------------------------------------------------------------------------
# Test strategy that generates predictable signals
# ---------------------------------------------------------------------------
class AlwaysLongStrategy(BaseStrategy):
    """Opens a LONG on every 10th candle, exits on every 20th."""

    def __init__(self, strategy_id: str = "always-long", product_id: str = PRODUCT_ID):
        super().__init__(strategy_id, product_id)
        self._count = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            lookback_window=5,
        )

    def on_candle(
        self, candle: Candlestick, context: StrategyContext | None = None
    ) -> Signal:
        self._count += 1
        if self._count % 20 == 10:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                value=candle.close,
                quantity=Decimal("0.005"),
            )
        elif self._count % 20 == 0:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                value=candle.close,
                quantity=Decimal("0.005"),
            )
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
            value=candle.close,
        )


class OneShotLongStrategy(BaseStrategy):
    """Opens one long position and leaves it open."""

    def __init__(self, strategy_id: str = "one-shot-long", product_id: str = PRODUCT_ID):
        super().__init__(strategy_id, product_id)
        self._sent = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            lookback_window=1,
        )

    def on_candle(
        self, candle: Candlestick, context: StrategyContext | None = None
    ) -> Signal:
        if self._sent:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.NO_SIGNAL,
                value=candle.close,
            )
        self._sent = True
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            value=candle.close,
            quantity=Decimal("1"),
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def candle_data():
    """200 candles with mild uptrend."""
    return make_candle_series(count=200)


@pytest.fixture
def memory_source(candle_data):
    ds = MemoryDataSource()
    ds.add_candles(candle_data)
    return ds


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestBacktestE2E:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_runs_to_completion(self, mock_session_local, memory_source, candle_data):
        """BacktestRunner should complete without error and return result dict."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        start_ts = candle_data[0].timestamp
        end_ts = candle_data[-1].timestamp

        runner = BacktestRunner(
            start_time=start_ts,
            end_time=end_ts,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            data_source=memory_source,
            fee_config={"maker": 0.0002, "taker": 0.0006},
            report_config={"csv_trades": False, "equity_curve": False,
                           "markdown_report": False, "journal_export": False},
        )

        strategy = AlwaysLongStrategy()
        runner.add_strategy(strategy)
        result = runner.run()

        assert result is not None
        assert "total_pnl" in result
        assert "total_trades" in result

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_produces_journal_entries(self, mock_session_local, memory_source, candle_data):
        """With AlwaysLongStrategy, journal should capture signal activity.
        Note: total_trades comes from DB query (mocked → 0), so we verify
        journal events instead as proof of order execution.
        """
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        runner = BacktestRunner(
            start_time=candle_data[0].timestamp,
            end_time=candle_data[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            data_source=memory_source,
            fee_config={"maker": 0.0002, "taker": 0.0006},
            report_config={"csv_trades": False, "equity_curve": False,
                           "markdown_report": False, "journal_export": False},
        )

        strategy = AlwaysLongStrategy()
        runner.add_strategy(strategy)
        result = runner.run()

        # Journal captures execution events even with mock DB
        assert result is not None
        assert result["journal_count"] > 0

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_fees_reflected(self, mock_session_local, memory_source, candle_data):
        """Fees should exactly reduce PnL for identical LIMIT fill economics."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        def run_backtest(fee_config):
            runner = BacktestRunner(
                start_time=candle_data[0].timestamp,
                end_time=candle_data[-1].timestamp,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                data_source=memory_source,
                fee_config=fee_config,
                report_config={"csv_trades": False, "equity_curve": False,
                               "markdown_report": False, "journal_export": False},
            )
            runner.add_strategy(AlwaysLongStrategy())
            result = runner.run()
            assert result is not None
            return result

        maker_fee_rate = Decimal("0.001")
        result_no_fee = run_backtest({"maker": Decimal("0"), "taker": Decimal("0")})
        result_with_fee = run_backtest(
            {"maker": maker_fee_rate, "taker": Decimal("0.002")}
        )

        no_fee_fills = [
            entry["data"] for entry in result_no_fee["journal"]
            if "fill_type" in entry["data"]
        ]
        with_fee_fills = [
            entry["data"] for entry in result_with_fee["journal"]
            if "fill_type" in entry["data"]
        ]
        assert no_fee_fills

        def normalize(fills):
            return [
                (
                    fill["side"].value,
                    Decimal(fill["price"]),
                    Decimal(fill["quantity"]),
                    fill["fill_type"],
                )
                for fill in fills
            ]

        no_fee_economics = normalize(no_fee_fills)
        with_fee_economics = normalize(with_fee_fills)
        assert {side for side, _, _, _ in no_fee_economics} == {"buy", "sell"}
        assert all(fill_type == "LIMIT" for _, _, _, fill_type in no_fee_economics)
        assert no_fee_economics == with_fee_economics

        expected_fees = sum(
            (price * quantity * maker_fee_rate for _, price, quantity, _ in no_fee_economics),
            Decimal("0"),
        )
        reported_fees = sum(
            (Decimal(fill["fee"]) for fill in with_fee_fills), Decimal("0")
        )
        assert reported_fees == expected_fees
        assert (
            Decimal(result_no_fee["total_pnl"])
            - Decimal(result_with_fee["total_pnl"])
            == expected_fees
        )

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_journal_populated(self, mock_session_local, memory_source, candle_data):
        """Journal should capture events during backtest."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        runner = BacktestRunner(
            start_time=candle_data[0].timestamp,
            end_time=candle_data[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            data_source=memory_source,
            fee_config={"maker": 0.0002, "taker": 0.0006},
            report_config={"csv_trades": False, "equity_curve": False,
                           "markdown_report": False, "journal_export": False},
        )

        runner.add_strategy(AlwaysLongStrategy())
        result = runner.run()

        assert result is not None
        journal = result["journal"]
        assert result["journal_count"] == len(journal) == 39
        assert [entry["tag"] for entry in journal] == ["entry", "fill"] * 19 + ["entry"]
        expected_timestamps = []
        for signal_index in range(9, len(candle_data), 10):
            expected_timestamps.append(candle_data[signal_index].timestamp)
            fill_index = signal_index + 1
            if fill_index < len(candle_data):
                expected_timestamps.append(candle_data[fill_index].timestamp)
        assert [entry["timestamp"] for entry in journal] == expected_timestamps
        for journal_entry in journal:
            assert journal_entry["data"]["order_id"] == journal_entry["trade_id"]
        for entry, fill in zip(journal[::2], journal[1::2]):
            assert entry["trade_id"] == fill["trade_id"]

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_below_drawdown_threshold_runs_to_completion(
        self, mock_session_local, memory_source, candle_data
    ):
        """Backtest should complete while drawdown remains below the threshold."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        runner = BacktestRunner(
            start_time=candle_data[0].timestamp,
            end_time=candle_data[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            max_drawdown_limit=0.001,  # Very tight: stop at 0.1% drawdown
            data_source=memory_source,
            fee_config={"maker": 0.001, "taker": 0.002},
            report_config={"csv_trades": False, "equity_curve": False,
                           "markdown_report": False, "journal_export": False},
        )

        runner.add_strategy(AlwaysLongStrategy())
        result = runner.run()

        assert result is not None
        assert result["candle_count"] == 200
        max_drawdown = result["max_drawdown"]
        assert type(max_drawdown) is Decimal
        assert Decimal("0") <= max_drawdown < Decimal("10.0000")
        endpoint_state = result["endpoint_state"]
        assert endpoint_state.halted_early is False
        assert endpoint_state.final_mark == candle_data[-1].close
        assert endpoint_state.end_timestamp == candle_data[-1].timestamp

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_circuit_breaker_uses_open_position_drawdown(self, mock_session_local):
        """Backtest should stop on unrealized drawdown before a trade closes."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        base_ts = 1_700_000_000_000
        interval_ms = 15 * 60 * 1000
        candles = [
            make_candle(base_ts, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),
            make_candle(base_ts + interval_ms, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),
            make_candle(base_ts + 2 * interval_ms, Decimal("60"), Decimal("60"), Decimal("60"), Decimal("60")),
            make_candle(base_ts + 3 * interval_ms, Decimal("105"), Decimal("105"), Decimal("105"), Decimal("105")),
        ]
        data_source = MemoryDataSource(candles)
        runner = BacktestRunner(
            start_time=candles[0].timestamp,
            end_time=candles[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            max_drawdown_limit=0.004,
            data_source=data_source,
            report_config={"csv_trades": False, "equity_curve": False,
                           "markdown_report": False, "journal_export": False},
        )

        runner.add_strategy(OneShotLongStrategy())
        result = runner.run()

        assert result is not None
        assert result["candle_count"] == 3
        assert result["max_drawdown"] == Decimal("40")
        assert result["journal_count"] >= 1
        endpoint_state = result["endpoint_state"]
        assert endpoint_state.halted_early is True
        assert endpoint_state.final_mark == candles[2].close
        assert endpoint_state.end_timestamp == candles[2].timestamp

    @patch("src.core.backtest_runner.SessionLocal")
    def test_backtest_drawdown_aggregates_strategy_scoped_positions(
        self,
        mock_session_local,
    ):
        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        base_ts = 1_700_000_000_000
        interval_ms = 15 * 60 * 1000
        candles = [
            make_candle(
                base_ts + index * interval_ms,
                price,
                price,
                price,
                price,
            )
            for index, price in enumerate(
                (Decimal("100"), Decimal("100"), Decimal("60"))
            )
        ]
        runner = BacktestRunner(
            start_time=candles[0].timestamp,
            end_time=candles[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=10000.0,
            max_drawdown_limit=None,
            data_source=MemoryDataSource(candles),
            report_config={
                "csv_trades": False,
                "equity_curve": False,
                "markdown_report": False,
                "journal_export": False,
            },
        )
        runner.add_strategy(OneShotLongStrategy("long-a"))
        runner.add_strategy(OneShotLongStrategy("long-b"))

        result = runner.run()

        assert result is not None
        assert result["max_drawdown"] == Decimal("80.00")
