import csv
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict
from decimal import Decimal
from src.core.db import SessionLocal
from src.core.orm_models import Strategy as StrategyORM, BacktestResultSummary, BacktestTradeLog
from src.core.engine import StrategyEngine
from src.core.clock import BacktestClock
from src.strategies.base import BaseStrategy
from src.core.repositories import BacktestOrderRepository
from src.core.backtest.loader import get_candles_generator
from src.core.analytics import calculate_metrics, ClosedTrade
from src.core.interfaces.data_source import IDataSource
from src.core.adapters.simulated import SimulatedAdapter
from src.core.mocks.account_service import BacktestAccountService
from src.core.journal import StrategyJournal

logger = logging.getLogger(__name__)

DEFAULT_REPORT_CONFIG: Dict = {
    "csv_trades": True,
    "markdown_report": True,
    "equity_curve": True,
    "journal_export": True,
    "output_dir": "backtest_output/",
}


def _write_csv_trades(closed_trades: List[ClosedTrade], path: Path) -> None:
    """Write closed trades to CSV."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "entry_time", "exit_time", "side", "entry_price",
            "exit_price", "quantity", "pnl",
        ])
        for ct in closed_trades:
            writer.writerow([
                ct.entry_time, ct.exit_time, ct.side,
                f"{ct.entry_price:.6f}", f"{ct.exit_price:.6f}",
                f"{ct.quantity:.6f}", f"{ct.pnl:.2f}",
            ])


def _write_equity_curve(equity_curve: list, path: Path) -> None:
    """Write equity curve to CSV."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["bar", "equity"])
        for i, eq in enumerate(equity_curve):
            writer.writerow([i, f"{eq:.2f}"])


def _write_journal(journal: StrategyJournal, path: Path) -> None:
    """Write journal to JSONL file."""
    with open(path, "w") as f:
        f.write(journal.to_jsonl())
        f.write("\n")


def _write_markdown_report(
    metrics: Dict,
    *,
    product_id: str,
    timeframe: str,
    initial_balance: float,
    start_time: int,
    end_time: int,
    fee_config: Dict,
    candle_count: int,
    path: Path,
) -> None:
    """Write a markdown summary report."""
    lines: List[str] = []

    lines.append("# Backtest Report")
    lines.append("")
    lines.append("## Configuration")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|-----------|-------|")
    lines.append(f"| Product | {product_id} |")
    lines.append(f"| Timeframe | {timeframe} |")
    lines.append(f"| Initial Balance | {initial_balance:,.2f} |")
    lines.append(f"| Start | {start_time} |")
    lines.append(f"| End | {end_time} |")
    lines.append(f"| Candles | {candle_count} |")
    if fee_config:
        lines.append(f"| Maker Fee | {fee_config.get('maker', 0):.4%} |")
        lines.append(f"| Taker Fee | {fee_config.get('taker', 0):.4%} |")
    lines.append("")

    lines.append("## Performance Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total PnL | {metrics.get('total_pnl', 0)} |")
    lines.append(f"| Total Trades | {metrics.get('total_trades', 0)} |")
    lines.append(f"| Win Rate | {metrics.get('win_rate', 0):.2%} |")
    lines.append(f"| Profit Factor | {metrics.get('profit_factor', 0):.2f} |")
    lines.append(f"| Max Drawdown | {metrics.get('max_drawdown', 0)} |")
    lines.append(f"| Trade Sharpe | {metrics.get('trade_sharpe', 0):.2f} |")
    lines.append(f"| Avg Trade | {metrics.get('avg_trade', 0):.2f} |")
    lines.append(f"| Sortino Ratio | {metrics.get('sortino_ratio', 0):.4f} |")
    lines.append(f"| Calmar Ratio | {metrics.get('calmar_ratio', 0):.4f} |")
    lines.append(f"| Max Drawdown Days | {metrics.get('max_drawdown_days', 0):.1f} |")
    lines.append(f"| Avg Hold Time (h) | {metrics.get('avg_hold_time_hours', 0):.1f} |")
    lines.append(f"| Trade Freq (/day) | {metrics.get('trade_frequency_per_day', 0):.2f} |")
    lines.append(f"| Max Consec. Wins | {metrics.get('max_consecutive_wins', 0)} ({metrics.get('max_consecutive_win_amount', 0):.2f}) |")
    lines.append(f"| Max Consec. Losses | {metrics.get('max_consecutive_losses', 0)} ({metrics.get('max_consecutive_loss_amount', 0):.2f}) |")
    lines.append(f"| Gross Profit | {metrics.get('gross_profit', 0):.2f} |")
    lines.append(f"| Gross Loss | {metrics.get('gross_loss', 0):.2f} |")
    lines.append("")

    monthly = metrics.get("monthly_returns", {})
    if monthly:
        lines.append("## Monthly Returns")
        lines.append("")
        lines.append("| Month | PnL |")
        lines.append("|-------|-----|")
        for month, pnl in sorted(monthly.items()):
            lines.append(f"| {month} | {pnl:+.2f} |")
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


class BacktestRunner:
    def __init__(
        self,
        start_time: int,
        end_time: int,
        product_id: str,
        timeframe: str,
        initial_balance: float = 10000.0,
        max_drawdown_limit: float = 0.20,
        data_source: Optional[IDataSource] = None,
        fee_config: Optional[Dict[str, float]] = None,
        report_config: Optional[Dict] = None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        self.initial_balance = initial_balance
        self.max_drawdown_limit = max_drawdown_limit
        self.data_source = data_source
        self.fee_config = fee_config or {}
        self.report_config = {**DEFAULT_REPORT_CONFIG, **(report_config or {})}

        # Data session only needed when no external data_source
        self.data_session = None if data_source else SessionLocal()
        self.db_session = SessionLocal()

        self.clock = BacktestClock(start_time=start_time / 1000)
        self._strategies_buffer: List[BaseStrategy] = []
        self.engine = None

    def add_strategy(self, strategy: BaseStrategy):
        self._strategies_buffer.append(strategy)

    def _ensure_strategies_registered(self):
        """Register all added strategies in the DB to avoid FK constraints"""
        for strat in self._strategies_buffer:
            exists = self.db_session.query(StrategyORM).filter_by(id=strat.strategy_id).first()
            if not exists:
                logger.info("Registering missing strategy in DB: %s", strat.strategy_id)
                new_strat = StrategyORM(
                    id=strat.strategy_id,
                    name=f"Backtest: {strat.strategy_id}",
                    configuration_json="{}"
                )
                self.db_session.add(new_strat)
        self.db_session.commit()

    def _export_reports(
        self,
        metrics: Dict,
        journal: StrategyJournal,
        candle_count: int,
    ) -> Optional[str]:
        """Write report files to output_dir. Returns output directory path."""
        cfg = self.report_config
        if not any(cfg.get(k) for k in ("csv_trades", "markdown_report", "equity_curve", "journal_export")):
            return None

        output_dir = Path(cfg.get("output_dir", "backtest_output/"))
        output_dir.mkdir(parents=True, exist_ok=True)

        closed_trades: List[ClosedTrade] = metrics.get("closed_trades", [])

        if cfg.get("csv_trades") and closed_trades:
            _write_csv_trades(closed_trades, output_dir / "trades.csv")

        if cfg.get("equity_curve"):
            # Rebuild equity curve from closed trades
            running = Decimal("0")
            equity = [running]
            for ct in closed_trades:
                running += ct.pnl
                equity.append(running)
            _write_equity_curve(equity, output_dir / "equity_curve.csv")

        if cfg.get("journal_export") and len(journal) > 0:
            _write_journal(journal, output_dir / "journal.jsonl")

        if cfg.get("markdown_report"):
            _write_markdown_report(
                metrics,
                product_id=self.product_id,
                timeframe=self.timeframe,
                initial_balance=self.initial_balance,
                start_time=self.start_time,
                end_time=self.end_time,
                fee_config=self.fee_config,
                candle_count=candle_count,
                path=output_dir / "report.md",
            )

        return str(output_dir)

    def run(self):
        # 0. Registration Check
        self._ensure_strategies_registered()

        if not self._strategies_buffer:
            logger.warning("No strategies added. Exiting.")
            return

        # 1. Setup Backtest Session
        primary_strategy_id = self._strategies_buffer[0].strategy_id
        summary = BacktestResultSummary(
            strategy_id=primary_strategy_id,
            start_time=self.start_time,
            end_time=self.end_time,
            total_pnl=0,
            metrics_json="{}"
        )
        self.db_session.add(summary)
        self.db_session.commit()
        logger.info("Backtest Session Created: ID %s", summary.id)

        # 2. Create journal for structured event recording
        journal = StrategyJournal(primary_strategy_id)

        # 3. Create Rust-backed adapter with fee config
        adapter = SimulatedAdapter(
            initial_balance=Decimal(str(self.initial_balance)),
            maker_fee=Decimal(str(self.fee_config.get("maker", 0))),
            taker_fee=Decimal(str(self.fee_config.get("taker", 0))),
        )

        # 4. Setup repo (trade recording only) and account service
        repo = BacktestOrderRepository(self.db_session, summary.id)
        mock_account = BacktestAccountService(adapter=adapter)

        # 5. Setup Engine with pre-created adapter and journal
        self.engine = StrategyEngine(
            self.db_session,
            self.clock,
            order_repository=repo,
            account_service=mock_account,
            adapter=adapter,
            journal=journal,
        )

        # Inject journal and account service into strategies
        for strat in self._strategies_buffer:
            strat.journal = journal
            if hasattr(strat, 'risk_manager'):
                strat.risk_manager.account_service = mock_account
            self.engine.add_strategy(strat)

        logger.info("Starting Backtest for %s [%s - %s]", self.product_id, self.start_time, self.end_time)
        count = 0

        if self.data_source:
            candle_gen = self.data_source.get_candles(
                self.product_id,
                self.timeframe,
                self.start_time,
                self.end_time,
            )
        else:
            candle_gen = get_candles_generator(
                self.data_session,
                self.product_id,
                self.timeframe,
                self.start_time,
                self.end_time,
            )

        stop_threshold = Decimal(str(self.initial_balance)) * Decimal(str(1 - self.max_drawdown_limit))

        try:
            for candle in candle_gen:
                # Update Clock
                self.clock.set_time(candle.timestamp / 1000)

                # Process Candle
                self.engine.on_market_data(candle)

                # Check Circuit Breaker
                current_balance = mock_account.get_balance()
                if current_balance < stop_threshold:
                    logger.warning("STOPPING BACKTEST: Max Drawdown Reached! Balance: %s < %s", current_balance, stop_threshold)
                    break

                count += 1
                if count % 1000 == 0:
                    logger.info("Processed %d candles... Current Time: %s | Bal: %.2f", count, candle.timestamp, current_balance)
        finally:
            # Calculate Final PnL
            final_balance = mock_account.get_balance()
            total_pnl = final_balance - Decimal(str(self.initial_balance))

            summary.total_pnl = total_pnl

            # Metrics (with advanced calculations)
            trades = self.db_session.query(BacktestTradeLog).filter_by(session_id=summary.id).all()
            metrics = calculate_metrics(
                trades,
                initial_balance=self.initial_balance,
            )

            # Serialize metrics (exclude non-serializable closed_trades)
            metrics_for_json = {
                k: v for k, v in metrics.items() if k != "closed_trades"
            }
            summary.metrics_json = json.dumps(metrics_for_json, default=str)

            self.db_session.commit()

            # Export reports
            report_dir = self._export_reports(metrics, journal, candle_count=count)

            logger.info("Backtest Complete. Processed %d candles. Final PnL: %s", count, total_pnl)
            logger.info("Metrics: %s", metrics_for_json)
            if report_dir:
                logger.info("Reports written to: %s", report_dir)

            result = {
                "total_pnl": float(total_pnl),
                "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
                "win_rate": float(metrics.get("win_rate", 0.0)),
                "total_trades": int(metrics.get("total_trades", 0)),
                "trade_sharpe": float(metrics.get("trade_sharpe", 0.0)),
                "profit_factor": float(metrics.get("profit_factor", 0.0)),
                "sortino_ratio": float(metrics.get("sortino_ratio", 0.0)),
                "calmar_ratio": float(metrics.get("calmar_ratio", 0.0)),
                "avg_hold_time_hours": float(metrics.get("avg_hold_time_hours", 0.0)),
                "max_consecutive_wins": int(metrics.get("max_consecutive_wins", 0)),
                "max_consecutive_losses": int(metrics.get("max_consecutive_losses", 0)),
                "journal": journal.to_dicts(),
                "journal_count": len(journal),
                "report_dir": report_dir,
            }

            self.db_session.close()
            if self.data_session:
                self.data_session.close()

        return result
