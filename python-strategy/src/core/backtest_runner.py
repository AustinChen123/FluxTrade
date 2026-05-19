import csv
import json
import logging
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Callable, ContextManager, Iterable, List, Optional, Dict
from decimal import Decimal
from sqlalchemy.orm import Session
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


@contextmanager
def _sessionlocal_context():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


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
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
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
        self._db_session_factory = db_session_factory or _sessionlocal_context

        self.clock = BacktestClock(start_time=start_time / 1000)
        self._strategies_buffer: List[BaseStrategy] = []
        self.engine = None

    def add_strategy(self, strategy: BaseStrategy):
        self._strategies_buffer.append(strategy)

    def _ensure_strategies_registered(self, db_session: Session):
        """Register all added strategies in the DB to avoid FK constraints"""
        for strat in self._strategies_buffer:
            exists = db_session.query(StrategyORM).filter_by(id=strat.strategy_id).first()
            if not exists:
                logger.info("Registering missing strategy in DB: %s", strat.strategy_id)
                new_strat = StrategyORM(
                    id=strat.strategy_id,
                    name=f"Backtest: {strat.strategy_id}",
                    configuration_json="{}"
                )
                db_session.add(new_strat)
        db_session.commit()

    def _process_candles(
        self,
        candles: Iterable,
        mock_account: BacktestAccountService,
        stop_threshold: Decimal,
    ) -> int:
        count = 0
        for candle in candles:
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
                logger.info("Processed %d candles... Current Time: %s | Bal: %s", count, candle.timestamp, current_balance)
        return count

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
        with self._db_session_factory() as db_session:
            self._ensure_strategies_registered(db_session)

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
        with self._db_session_factory() as db_session:
            db_session.add(summary)
            db_session.commit()
            summary_id = summary.id
        logger.info("Backtest Session Created: ID %s", summary_id)

        # 2. Create journal for structured event recording
        journal = StrategyJournal(primary_strategy_id)

        # 3. Create Rust-backed adapter with fee config
        adapter = SimulatedAdapter(
            initial_balance=Decimal(str(self.initial_balance)),
            maker_fee=Decimal(str(self.fee_config.get("maker", 0))),
            taker_fee=Decimal(str(self.fee_config.get("taker", 0))),
        )

        # 4. Setup repo (trade recording only) and account service
        repo = BacktestOrderRepository(
            None,
            summary_id,
            db_session_factory=self._db_session_factory,
        )
        mock_account = BacktestAccountService(adapter=adapter)

        # 5. Setup Engine with pre-created adapter and journal
        self.engine = StrategyEngine(
            None,
            self.clock,
            order_repository=repo,
            account_service=mock_account,
            adapter=adapter,
            journal=journal,
            db_session_factory=self._db_session_factory,
        )

        # Inject journal and account service into strategies
        for strat in self._strategies_buffer:
            strat.journal = journal
            if hasattr(strat, 'risk_manager'):
                strat.risk_manager.account_service = mock_account
            self.engine.add_strategy(strat)

        logger.info("Starting Backtest for %s [%s - %s]", self.product_id, self.start_time, self.end_time)

        stop_threshold = Decimal(str(self.initial_balance)) * Decimal(str(1 - self.max_drawdown_limit))

        if self.data_source:
            candle_context = nullcontext(self.data_source.get_candles(
                self.product_id,
                self.timeframe,
                self.start_time,
                self.end_time,
            ))
        else:
            candle_context = self._db_session_factory()

        with candle_context as candle_source:
            if self.data_source:
                candle_gen = candle_source
            else:
                candle_gen = get_candles_generator(
                    candle_source,
                    self.product_id,
                    self.timeframe,
                    self.start_time,
                    self.end_time,
                )
            count = self._process_candles(candle_gen, mock_account, stop_threshold)

        # Calculate Final PnL
        final_balance = mock_account.get_balance()
        total_pnl = final_balance - Decimal(str(self.initial_balance))

        with self._db_session_factory() as db_session:
            summary = db_session.query(BacktestResultSummary).filter_by(id=summary_id).first()
            # Metrics (with advanced calculations)
            trades = db_session.query(BacktestTradeLog).filter_by(session_id=summary_id).all()
            metrics = calculate_metrics(
                trades,
                initial_balance=self.initial_balance,
            )

            # Per-strategy metrics
            per_strategy = self._compute_per_strategy_metrics(trades)

            # Serialize metrics (exclude non-serializable closed_trades)
            metrics_for_json = {
                k: v for k, v in metrics.items() if k != "closed_trades"
            }
            if per_strategy:
                metrics_for_json["per_strategy"] = {
                    sid: {k: v for k, v in m.items() if k != "closed_trades"}
                    for sid, m in per_strategy.items()
                }
            summary.metrics_json = json.dumps(metrics_for_json, default=str)
            summary.total_pnl = total_pnl

            db_session.commit()

        # Export reports
        report_dir = self._export_reports(metrics, journal, candle_count=count)

        logger.info("Backtest Complete. Processed %d candles. Final PnL: %s", count, total_pnl)
        logger.info("Metrics: %s", metrics_for_json)
        if report_dir:
            logger.info("Reports written to: %s", report_dir)

        result = {
            "total_pnl": total_pnl,
            "max_drawdown": metrics.get("max_drawdown", Decimal("0")),
            "win_rate": metrics.get("win_rate", Decimal("0")),
            "total_trades": int(metrics.get("total_trades", 0)),
            "trade_sharpe": metrics.get("trade_sharpe", Decimal("0")),
            "profit_factor": metrics.get("profit_factor", Decimal("0")),
            "sortino_ratio": metrics.get("sortino_ratio", Decimal("0")),
            "calmar_ratio": metrics.get("calmar_ratio", Decimal("0")),
            "avg_hold_time_hours": metrics.get("avg_hold_time_hours", Decimal("0")),
            "max_consecutive_wins": int(metrics.get("max_consecutive_wins", 0)),
            "max_consecutive_losses": int(metrics.get("max_consecutive_losses", 0)),
            "journal": journal.to_dicts(),
            "journal_count": len(journal),
            "report_dir": report_dir,
            "per_strategy": per_strategy,
        }

        return result

    def _compute_per_strategy_metrics(self, trades: list) -> Dict[str, Dict]:
        """Compute metrics per strategy by filtering trades by strategy_id."""
        strategy_ids = set()
        for t in trades:
            sid = getattr(t, "strategy_id", None)
            if sid:
                strategy_ids.add(sid)

        if len(strategy_ids) <= 1:
            return {}

        per_strategy: Dict[str, Dict] = {}
        for sid in strategy_ids:
            strategy_trades = [t for t in trades if getattr(t, "strategy_id", None) == sid]
            if strategy_trades:
                per_strategy[sid] = calculate_metrics(
                    strategy_trades,
                    initial_balance=self.initial_balance,
                )
        return per_strategy
