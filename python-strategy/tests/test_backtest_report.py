"""Tests for backtest report output (Phase 6.4)."""

import csv
import json
from src.core.backtest_runner import (
    _write_csv_trades,
    _write_equity_curve,
    _write_journal,
    _write_markdown_report,
    DEFAULT_REPORT_CONFIG,
)
from src.core.analytics import ClosedTrade
from src.core.models import PositionSide
from src.core.journal import StrategyJournal


# ── Helpers ──────────────────────────────────────────────────────

def _sample_closed_trades() -> list[ClosedTrade]:
    return [
        ClosedTrade(
            entry_time=1_000_000,
            exit_time=2_000_000,
            entry_price=100.0,
            exit_price=110.0,
            side=PositionSide.LONG,
            quantity=0.5,
            pnl=5.0,
        ),
        ClosedTrade(
            entry_time=3_000_000,
            exit_time=4_000_000,
            entry_price=200.0,
            exit_price=190.0,
            side=PositionSide.SHORT,
            quantity=0.3,
            pnl=3.0,
        ),
    ]


def _sample_metrics() -> dict:
    return {
        "total_pnl": 8.0,
        "max_drawdown": -2.0,
        "trade_sharpe": 1.5,
        "win_rate": 0.75,
        "profit_factor": 3.0,
        "avg_trade": 4.0,
        "total_trades": 2,
        "sortino_ratio": 1.2,
        "calmar_ratio": 0.8,
        "monthly_returns": {"2024-01": 5.0, "2024-02": 3.0},
        "max_drawdown_days": 3.5,
        "trade_frequency_per_day": 1.5,
        "avg_hold_time_hours": 2.0,
        "max_consecutive_wins": 3,
        "max_consecutive_losses": 1,
        "max_consecutive_win_amount": 10.0,
        "max_consecutive_loss_amount": 2.0,
        "gross_profit": 10.0,
        "gross_loss": 2.0,
        "closed_trades": _sample_closed_trades(),
    }


# ── CSV trades ───────────────────────────────────────────────────

class TestWriteCsvTrades:
    def test_writes_header_and_rows(self, tmp_path):
        path = tmp_path / "trades.csv"
        _write_csv_trades(_sample_closed_trades(), path)

        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows[0] == [
            "entry_time", "exit_time", "side", "entry_price",
            "exit_price", "quantity", "pnl",
        ]
        assert len(rows) == 3  # header + 2 trades
        assert rows[1][2] == "LONG"
        assert rows[2][2] == "SHORT"

    def test_empty_trades(self, tmp_path):
        path = tmp_path / "trades.csv"
        _write_csv_trades([], path)

        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert len(rows) == 1  # header only


# ── Equity curve ─────────────────────────────────────────────────

class TestWriteEquityCurve:
    def test_writes_equity_data(self, tmp_path):
        path = tmp_path / "equity.csv"
        equity = [0.0, 5.0, 8.0, 6.0, 10.0]
        _write_equity_curve(equity, path)

        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows[0] == ["bar", "equity"]
        assert len(rows) == 6  # header + 5 points
        assert rows[1] == ["0", "0.00"]
        assert rows[4] == ["3", "6.00"]


# ── Journal export ───────────────────────────────────────────────

class TestWriteJournal:
    def test_writes_jsonl(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        journal = StrategyJournal("test_strat")
        journal.log("entry", {"side": "LONG"}, timestamp=1000)
        journal.log("sl_hit", {"price": 99}, timestamp=2000)

        _write_journal(journal, path)

        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2

        first = json.loads(lines[0])
        assert first["tag"] == "entry"
        assert first["strategy_id"] == "test_strat"

    def test_empty_journal(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        journal = StrategyJournal("test")
        _write_journal(journal, path)
        assert path.read_text().strip() == ""


# ── Markdown report ──────────────────────────────────────────────

class TestWriteMarkdownReport:
    def test_contains_all_sections(self, tmp_path):
        path = tmp_path / "report.md"
        _write_markdown_report(
            _sample_metrics(),
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="15m",
            initial_balance=10000.0,
            start_time=1000,
            end_time=2000,
            fee_config={"maker": 0.0002, "taker": 0.0006},
            candle_count=500,
            path=path,
        )

        content = path.read_text()
        assert "# Backtest Report" in content
        assert "## Configuration" in content
        assert "## Performance Summary" in content
        assert "## Monthly Returns" in content
        assert "BINANCE:BTCUSDT-PERP" in content
        assert "15m" in content
        assert "Sortino" in content
        assert "Calmar" in content
        assert "2024-01" in content

    def test_no_monthly_section_when_empty(self, tmp_path):
        path = tmp_path / "report.md"
        metrics = _sample_metrics()
        metrics["monthly_returns"] = {}
        _write_markdown_report(
            metrics,
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            initial_balance=10000.0,
            start_time=1000,
            end_time=2000,
            fee_config={},
            candle_count=100,
            path=path,
        )
        content = path.read_text()
        assert "Monthly Returns" not in content

    def test_no_fee_rows_when_empty(self, tmp_path):
        path = tmp_path / "report.md"
        _write_markdown_report(
            _sample_metrics(),
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            initial_balance=10000.0,
            start_time=1000,
            end_time=2000,
            fee_config={},
            candle_count=100,
            path=path,
        )
        content = path.read_text()
        assert "Maker Fee" not in content


# ── Default report config ────────────────────────────────────────

class TestDefaultReportConfig:
    def test_defaults_are_true(self):
        assert DEFAULT_REPORT_CONFIG["csv_trades"] is True
        assert DEFAULT_REPORT_CONFIG["markdown_report"] is True
        assert DEFAULT_REPORT_CONFIG["equity_curve"] is True
        assert DEFAULT_REPORT_CONFIG["journal_export"] is True
        assert DEFAULT_REPORT_CONFIG["output_dir"] == "backtest_output/"
