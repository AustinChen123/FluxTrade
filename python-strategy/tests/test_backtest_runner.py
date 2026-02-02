"""
Tests for src/core/backtest_runner.py

Covers:
- Report export helpers (_write_csv_trades, _write_equity_curve, _write_journal, _write_markdown_report)
- Report config: skip when all disabled
- BacktestRunner initialization defaults
- _export_reports integration
"""

from unittest.mock import MagicMock, patch

from src.core.backtest_runner import (
    _write_csv_trades,
    _write_equity_curve,
    _write_journal,
    _write_markdown_report,
    BacktestRunner,
)
from src.core.analytics import ClosedTrade
from src.core.journal import StrategyJournal


# =============================================================================
# Helpers
# =============================================================================


def _make_closed_trade(**overrides) -> ClosedTrade:
    defaults = dict(
        entry_time=1704067200000,
        exit_time=1704067260000,
        side="LONG",
        entry_price=42000.0,
        exit_price=42500.0,
        quantity=0.1,
        pnl=50.0,
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
        assert lines[0] == "entry_time,exit_time,side,entry_price,exit_price,quantity,pnl"

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

        result = runner._export_reports(metrics, journal, candle_count=0)
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

        result = runner._export_reports(metrics, journal, candle_count=100)

        assert result is not None
        assert output_dir.exists()
