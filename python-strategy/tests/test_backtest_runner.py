"""
Tests for src/core/backtest_runner.py

Covers:
- Report export helpers (_write_csv_trades, _write_equity_curve, _write_journal, _write_markdown_report)
- Report config: skip when all disabled
- BacktestRunner initialization defaults
- _export_reports integration
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.core.backtest_runner import (
    _write_csv_trades,
    _write_equity_curve,
    _write_journal,
    _write_markdown_report,
    BacktestRunner,
)
from src.core.analytics import ClosedTrade
from src.core.models import Candlestick, PositionSide
from src.core.portfolio_runtime import PortfolioDefinition, PortfolioSleeve
from src.core.journal import StrategyJournal
from src.strategies.base import BaseStrategy, StrategyRequirements


# =============================================================================
# Helpers
# =============================================================================


def _make_closed_trade(**overrides) -> ClosedTrade:
    defaults = dict(
        entry_time=1704067200000,
        exit_time=1704067260000,
        side=PositionSide.LONG,
        entry_price=Decimal("42000.00"),
        exit_price=Decimal("42500.00"),
        quantity=Decimal("0.1"),
        pnl=Decimal("50.0"),
    )
    defaults.update(overrides)
    return ClosedTrade(**defaults)


# =============================================================================
# _write_csv_trades
# =============================================================================


class TestWriteCsvTrades:

    def test_creates_csv_file(self, tmp_path):
        """Should create a CSV file at the given path."""
        path = tmp_path / "trades.csv"
        trades = [_make_closed_trade()]

        _write_csv_trades(trades, path)

        assert path.exists()

    def test_csv_header(self, tmp_path):
        """CSV should have correct header row."""
        path = tmp_path / "trades.csv"
        _write_csv_trades([_make_closed_trade()], path)

        lines = path.read_text().strip().split("\n")
        assert lines[0] == "entry_time,exit_time,side,entry_price,exit_price,quantity,fee,pnl"

    def test_csv_data_row(self, tmp_path):
        """CSV should contain trade data."""
        path = tmp_path / "trades.csv"
        _write_csv_trades([_make_closed_trade()], path)

        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2  # header + 1 data row
        assert "42000" in lines[1]

    def test_csv_empty_trades(self, tmp_path):
        """Empty trade list should produce header-only CSV."""
        path = tmp_path / "trades.csv"
        _write_csv_trades([], path)

        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1  # header only


# =============================================================================
# _write_equity_curve
# =============================================================================


class TestWriteEquityCurve:

    def test_creates_file(self, tmp_path):
        """Should create equity curve CSV."""
        path = tmp_path / "equity.csv"
        _write_equity_curve([0.0, 100.0, 50.0], path)

        assert path.exists()

    def test_header_and_rows(self, tmp_path):
        """Should have bar,equity header and correct row count."""
        path = tmp_path / "equity.csv"
        curve = [0.0, 100.0, 50.0]
        _write_equity_curve(curve, path)

        lines = path.read_text().strip().split("\n")
        assert lines[0] == "bar,equity"
        assert len(lines) == 4  # header + 3 data rows


# =============================================================================
# _write_journal
# =============================================================================


class TestWriteJournal:

    def test_creates_jsonl_file(self, tmp_path):
        """Should write journal entries to JSONL file."""
        path = tmp_path / "journal.jsonl"
        journal = StrategyJournal("test_strat")
        journal.log("entry", {"side": "LONG"}, timestamp=1704067200000)

        _write_journal(journal, path)

        assert path.exists()
        content = path.read_text()
        assert "entry" in content
        assert "LONG" in content


# =============================================================================
# _write_markdown_report
# =============================================================================


class TestWriteMarkdownReport:

    def test_creates_markdown_file(self, tmp_path):
        """Should create a markdown report file."""
        path = tmp_path / "report.md"
        metrics = {
            "total_pnl": 500,
            "total_trades": 10,
            "win_rate": 0.6,
            "profit_factor": 1.5,
            "max_drawdown": -200,
            "trade_sharpe": 1.2,
            "avg_trade": 50,
            "sortino_ratio": 1.8,
            "calmar_ratio": 2.5,
            "max_drawdown_days": 3.0,
            "avg_hold_time_hours": 2.5,
            "trade_frequency_per_day": 1.5,
            "max_consecutive_wins": 5,
            "max_consecutive_win_amount": 250.0,
            "max_consecutive_losses": 3,
            "max_consecutive_loss_amount": -150.0,
            "gross_profit": 800.0,
            "gross_loss": -300.0,
        }
        _write_markdown_report(
            metrics,
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            initial_balance=10000.0,
            start_time=1704067200000,
            end_time=1704153600000,
            fee_config={"maker": 0.0002, "taker": 0.0006},
            candle_count=1440,
            path=path,
        )

        assert path.exists()
        content = path.read_text()
        assert "# Backtest Report" in content
        assert "BINANCE:BTCUSDT-PERP" in content
        assert "10,000.00" in content

    def test_monthly_returns_section(self, tmp_path):
        """Should include monthly returns table when present."""
        path = tmp_path / "report.md"
        metrics = {
            "total_pnl": 100, "total_trades": 5, "win_rate": 0.5,
            "profit_factor": 1.0, "max_drawdown": -50, "trade_sharpe": 0.5,
            "avg_trade": 20, "sortino_ratio": 0.5, "calmar_ratio": 0.5,
            "max_drawdown_days": 1.0, "avg_hold_time_hours": 1.0,
            "trade_frequency_per_day": 1.0, "max_consecutive_wins": 2,
            "max_consecutive_win_amount": 40.0, "max_consecutive_losses": 1,
            "max_consecutive_loss_amount": -20.0, "gross_profit": 60.0,
            "gross_loss": -40.0,
            "monthly_returns": {"2024-01": 50.0, "2024-02": 50.0},
        }
        _write_markdown_report(
            metrics, product_id="X:Y-PERP", timeframe="1d",
            initial_balance=10000.0, start_time=0, end_time=0,
            fee_config={}, candle_count=0, path=path,
        )

        content = path.read_text()
        assert "## Monthly Returns" in content
        assert "2024-01" in content


# =============================================================================
# BacktestRunner defaults
# =============================================================================


class TestBacktestRunnerInit:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_default_values(self, mock_session_local):
        """Should initialize with correct defaults."""
        mock_session_local.return_value = MagicMock()

        runner = BacktestRunner(
            start_time=1704067200000,
            end_time=1704153600000,
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
        )

        assert runner.initial_balance == 10000.0
        assert runner.max_drawdown_limit == 0.20
        assert runner.fee_config == {}
        assert runner.report_config["csv_trades"] is True

    @patch("src.core.backtest_runner.SessionLocal")
    def test_custom_fee_config(self, mock_session_local):
        """Should accept custom fee config."""
        mock_session_local.return_value = MagicMock()

        runner = BacktestRunner(
            start_time=0, end_time=0,
            product_id="X:Y-PERP", timeframe="1m",
            fee_config={"maker": 0.001, "taker": 0.002},
        )

        assert runner.fee_config == {"maker": 0.001, "taker": 0.002}

    @patch("src.core.backtest_runner.SessionLocal")
    def test_report_config_merge(self, mock_session_local):
        """Custom report config should merge with defaults."""
        mock_session_local.return_value = MagicMock()

        runner = BacktestRunner(
            start_time=0, end_time=0,
            product_id="X:Y-PERP", timeframe="1m",
            report_config={"csv_trades": False},
        )

        # Custom overrides default
        assert runner.report_config["csv_trades"] is False
        # Other defaults preserved
        assert runner.report_config["markdown_report"] is True

    @patch("src.core.backtest_runner.SessionLocal")
    def test_unknown_report_config_key_is_rejected(self, mock_session_local):
        """A typo must not silently leave a default report export enabled."""
        mock_session_local.return_value = MagicMock()

        with pytest.raises(
            ValueError,
            match="unknown report_config keys: journal",
        ):
            BacktestRunner(
                start_time=0,
                end_time=0,
                product_id="X:Y-PERP",
                timeframe="1m",
                report_config={"journal": False},
            )

    @patch("src.core.backtest_runner.SessionLocal")
    def test_add_portfolio_uses_parent_as_result_identity(
        self,
        mock_session_local,
    ):
        mock_session_local.return_value = MagicMock()

        class NoSignalStrategy(BaseStrategy):
            @property
            def requirements(self):
                return StrategyRequirements(self.product_id, "5m", 1)

            def on_candle(self, candle):
                return None

        definition = PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id="RITHMIC:MNQ_ROLL-PERP",
            sleeves=(
                PortfolioSleeve(
                    NoSignalStrategy(
                        "portfolio_v1.sleeve_a",
                        "RITHMIC:MNQ_ROLL-PERP",
                    )
                ),
            ),
            max_gross_quantity=Decimal("1"),
        )
        runner = BacktestRunner(
            start_time=0,
            end_time=0,
            product_id="RITHMIC:MNQ_ROLL-PERP",
            timeframe="5m",
        )

        runner.add_portfolio(definition)

        assert runner._primary_runtime_id == "portfolio_v1"
        assert runner._portfolios_buffer == [definition]
        assert [
            strategy.strategy_id for strategy in runner._strategies_buffer
        ] == ["portfolio_v1.sleeve_a"]

    @patch("src.core.backtest_runner.SessionLocal")
    def test_execution_timeframe_requires_finer_even_divisor(
        self,
        mock_session_local,
    ):
        mock_session_local.return_value = MagicMock()

        with pytest.raises(ValueError, match="evenly divide"):
            BacktestRunner(
                start_time=0,
                end_time=0,
                product_id="RITHMIC:MNQ_ROLL-PERP",
                timeframe="5m",
                execution_timeframe="3m",
            )


def test_split_backtest_routes_every_1m_fill_and_only_completed_5m_decisions():
    runner = BacktestRunner(
        start_time=1_700_000_000_000,
        end_time=1_700_000_300_000,
        product_id="RITHMIC:MNQ_ROLL-PERP",
        timeframe="5m",
        execution_timeframe="1m",
    )
    runner.engine = MagicMock()
    account = MagicMock()
    account.adapter = None
    account.get_balance.return_value = Decimal("10000")
    bucket_start = 1_704_067_200_000
    candles = [
        Candlestick(
            product_id="RITHMIC:MNQ_ROLL-PERP",
            timeframe="1m",
            timestamp=bucket_start + minute * 60_000,
            open=Decimal("20000"),
            high=Decimal("20001"),
            low=Decimal("19999"),
            close=Decimal("20000"),
            volume=Decimal("1"),
        )
        for minute in range(6)
    ]

    progress = runner._process_candles(
        candles,
        account,
        stop_drawdown_amount=None,
    )

    assert progress.candle_count == 6
    assert progress.final_mark == Decimal("20000")
    assert progress.end_timestamp == candles[-1].timestamp
    assert progress.halted_early is False
    assert runner.engine.on_backtest_market_data.call_count == 6
    decision_candles = [
        call.args[1]
        for call in runner.engine.on_backtest_market_data.call_args_list
        if call.args[1] is not None
    ]
    assert len(decision_candles) == 1
    assert decision_candles[0].timeframe == "5m"
    assert decision_candles[0].volume == Decimal("5")


def test_add_portfolio_rejects_runner_identity_mismatch():
    class OneMinuteStrategy(BaseStrategy):
        @property
        def requirements(self):
            return StrategyRequirements(self.product_id, "1m", 1)

        def on_candle(self, candle):
            return None

    strategy = OneMinuteStrategy("sleeve", "RITHMIC:MNQ_ROLL-PERP")
    definition = PortfolioDefinition(
        portfolio_id="portfolio_v1",
        product_id="RITHMIC:MNQ_ROLL-PERP",
        sleeves=(PortfolioSleeve(strategy),),
        max_gross_quantity=Decimal("1"),
    )
    runner = BacktestRunner(
        start_time=0,
        end_time=0,
        product_id="RITHMIC:MNQ_ROLL-PERP",
        timeframe="5m",
    )

    with pytest.raises(ValueError, match="product/timeframe"):
        runner.add_portfolio(definition)


# =============================================================================
# _export_reports
# =============================================================================


class TestExportReports:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_returns_none_when_all_disabled(self, mock_session_local):
        """Should return None when all report types are disabled."""
        mock_session_local.return_value = MagicMock()

        runner = BacktestRunner(
            start_time=0, end_time=0,
            product_id="X:Y-PERP", timeframe="1m",
            report_config={
                "csv_trades": False,
                "markdown_report": False,
                "equity_curve": False,
                "journal_export": False,
            },
        )

        metrics = {"total_pnl": 0, "closed_trades": []}
        journal = StrategyJournal("test")

        result = runner._export_reports(
            metrics,
            journal,
            candle_count=0,
            equity_samples=[],
        )
        assert result is None

    @patch("src.core.backtest_runner.SessionLocal")
    def test_creates_output_dir(self, mock_session_local, tmp_path):
        """Should create output directory if it doesn't exist."""
        mock_session_local.return_value = MagicMock()
        output_dir = tmp_path / "test_output"

        runner = BacktestRunner(
            start_time=1704067200000, end_time=1704153600000,
            product_id="BINANCE:BTCUSDT-PERP", timeframe="1m",
            report_config={
                "csv_trades": False,
                "markdown_report": True,
                "equity_curve": False,
                "journal_export": False,
                "output_dir": str(output_dir),
            },
        )

        metrics = {
            "total_pnl": 100, "total_trades": 5, "win_rate": 0.5,
            "profit_factor": 1.0, "max_drawdown": -50, "trade_sharpe": 0.5,
            "avg_trade": 20, "sortino_ratio": 0.5, "calmar_ratio": 0.5,
            "max_drawdown_days": 1.0, "avg_hold_time_hours": 1.0,
            "trade_frequency_per_day": 1.0, "max_consecutive_wins": 2,
            "max_consecutive_win_amount": 40.0, "max_consecutive_losses": 1,
            "max_consecutive_loss_amount": -20.0, "gross_profit": 60.0,
            "gross_loss": -40.0, "closed_trades": [],
        }
        journal = StrategyJournal("test")

        result = runner._export_reports(
            metrics,
            journal,
            candle_count=100,
            equity_samples=[
                (1, Decimal("10000")),
                (2, Decimal("9990")),
            ],
        )

        assert result is not None
        assert output_dir.exists()
