import csv
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import (
    Callable,
    ContextManager,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    cast,
)
from decimal import Decimal
from sqlalchemy.orm import Session
from fluxtrade_core import (
    CandleAggregator,
    Candlestick as RustCandlestick,
)
from src.core.db import SessionLocal
from src.core.orm_models import (
    Strategy as StrategyORM,
    BacktestResultSummary,
    BacktestTradeLog,
)
from src.core.engine import StrategyEngine
from src.core.clock import BacktestClock
from src.core.data_provider import timeframe_to_ms
from src.core.models import Candlestick, Signal
from src.strategies.base import BaseStrategy
from src.core.repositories import BacktestOrderRepository
from src.core.backtest.loader import get_candles_generator
from src.core.backtest.endpoint_state import build_replay_endpoint_state
from src.core.backtest.equity import PortfolioEquityCalculator
from src.core.backtest.external_funding import (
    ExternalFundingEvent,
    ExternalFundingTimeline,
    build_external_funding_timeline,
)
from src.core.backtest.flow_neutral_performance import (
    FlowNeutralPerformanceReport,
    FlowNeutralPerformanceTracker,
    return_metric_inputs,
)
from src.core.backtest.run_evidence import (
    BacktestDatasetEvidence,
    BacktestRunProvenance,
    build_backtest_run_provenance,
    canonical_decision_snapshot,
    canonical_fill_records,
    strategy_configuration_contract,
)
from src.core.analytics import (
    ClosedTrade,
    InitialBalanceInput,
    _normalize_initial_balance,
    annualized_sharpe_from_moments,
    calculate_metrics,
    utc_daily_return_metrics,
)
from src.core.interfaces.data_source import IDataSource
from src.core.portfolio_runtime import PortfolioDefinition
from src.core.adapters.simulated import (
    SimulatedAdapter,
    resolve_market_slippage_configuration,
)
from src.core.mocks.account_service import BacktestAccountService
from src.core.journal import StrategyJournal
from src.core.product_registry import (
    FeeModel,
    InstrumentSpec,
    MarketType,
    resolve_contract_multiplier,
    resolve_fee_model,
)
from src.core.strategy_context import StrategyContext
from src.core.market_data.profiles.context_enrichment import enrich_profile_context
from src.core.market_data.profiles.modeled_input import (
    ModeledProfileInput,
    completed_candle_decision_time_ms,
)
from src.core.signal_processor import (
    StrategyContextInvocationMode,
    strategy_context_invocation_mode,
)

logger = logging.getLogger(__name__)

DEFAULT_REPORT_CONFIG: Dict = {
    "csv_trades": True,
    "markdown_report": True,
    "equity_curve": True,
    "journal_export": True,
    "output_dir": "backtest_output/",
}


@dataclass(frozen=True, slots=True)
class _ReplayProgress:
    candle_count: int
    equity_samples: list[tuple[int, Decimal]]
    final_mark: Decimal | None
    end_timestamp: int | None
    halted_early: bool


class _BacktestRiskManager(Protocol):
    account_service: BacktestAccountService
    instrument_spec_resolver: Callable[[str], InstrumentSpec | None]


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
        writer.writerow(
            [
                "entry_time",
                "exit_time",
                "side",
                "entry_price",
                "exit_price",
                "quantity",
                "fee",
                "pnl",
            ]
        )
        for ct in closed_trades:
            writer.writerow(
                [
                    ct.entry_time,
                    ct.exit_time,
                    ct.side,
                    f"{ct.entry_price:.6f}",
                    f"{ct.exit_price:.6f}",
                    f"{ct.quantity:.6f}",
                    f"{ct.fee:.6f}",
                    f"{ct.pnl:.2f}",
                ]
            )


def _write_equity_curve(equity_curve: list, path: Path) -> None:
    """Write equity curve to CSV."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["bar", "equity"])
        for i, eq in enumerate(equity_curve):
            writer.writerow([i, f"{eq:.2f}"])


def _write_flow_neutral_curve(
    report: FlowNeutralPerformanceReport,
    path: Path,
) -> None:
    """Write timestamped raw equity and unitized NAV evidence."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["timestamp", "phase", "funding_event_id", "equity", "units", "nav"]
        )
        for sample in report.nav_samples:
            writer.writerow(
                [
                    sample.timestamp,
                    sample.phase,
                    sample.funding_event_id or "",
                    str(sample.equity),
                    str(sample.units),
                    str(sample.nav),
                ]
            )


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
    initial_balance: InitialBalanceInput,
    start_time: int,
    end_time: int,
    fee_config: Mapping[str, Decimal | float],
    fee_model: FeeModel = FeeModel.PERCENTAGE_NOTIONAL,
    candle_count: int,
    path: Path,
    provenance: BacktestRunProvenance | None = None,
    market_slippage_bps: Decimal = Decimal("0"),
) -> None:
    """Write a markdown summary report."""
    lines: List[str] = []

    lines.append("# Backtest Report")
    lines.append("")

    if provenance is not None:
        lines.append("## Reproducibility")
        lines.append("")
        lines.append("| Identity | Value |")
        lines.append("|----------|-------|")
        lines.append(
            f"| Dataset Identity Version | {provenance.dataset_identity_version} |"
        )
        lines.append(f"| Dataset SHA-256 | {provenance.dataset_sha256} |")
        lines.append(f"| Program Version | {provenance.program_version} |")
        lines.append(f"| Program SHA-256 | {provenance.program_sha256} |")
        lines.append(f"| Extension Version | {provenance.extension_version} |")
        lines.append(f"| Extension SHA-256 | {provenance.extension_sha256} |")
        lines.append(
            "| Configuration Identity Version | "
            f"{provenance.configuration_identity_version} |"
        )
        lines.append(f"| Configuration SHA-256 | {provenance.configuration_sha256} |")
        lines.append(
            "| Runner Configuration SHA-256 | "
            f"{provenance.runner_configuration_sha256} |"
        )
        lines.append(f"| Matching Model | {provenance.matching_model} |")
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
    lines.append(f"| Market Slippage (bps) | {market_slippage_bps} |")
    if fee_config:
        if fee_model == FeeModel.PER_CONTRACT:
            lines.append(f"| Maker Fee / Contract | {fee_config.get('maker', 0)} |")
            lines.append(f"| Taker Fee / Contract | {fee_config.get('taker', 0)} |")
        else:
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
    lines.append(
        f"| Trade Freq (/day) | {metrics.get('trade_frequency_per_day', 0):.2f} |"
    )
    lines.append(
        f"| Max Consec. Wins | {metrics.get('max_consecutive_wins', 0)} ({metrics.get('max_consecutive_win_amount', 0):.2f}) |"
    )
    lines.append(
        f"| Max Consec. Losses | {metrics.get('max_consecutive_losses', 0)} ({metrics.get('max_consecutive_loss_amount', 0):.2f}) |"
    )
    lines.append(f"| Gross Profit | {metrics.get('gross_profit', 0):.2f} |")
    lines.append(f"| Gross Loss | {metrics.get('gross_loss', 0):.2f} |")
    lines.append("")

    if "initial_capital" in metrics:
        lines.append("## Flow-Neutral Performance")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        for label, key in (
            ("Initial Capital", "initial_capital"),
            ("External Contributions", "external_contributions"),
            ("Total Contributed Capital", "total_contributed_capital"),
            ("Final Equity", "final_equity"),
            ("Net PnL", "net_pnl"),
            ("Ending Units", "ending_units"),
            ("Ending NAV", "ending_nav"),
            ("Time-Weighted Return", "time_weighted_return"),
            ("Annualized TWR", "annualized_time_weighted_return"),
            ("Unitized Max Drawdown", "unitized_max_drawdown"),
            ("Raw Max Drawdown", "raw_max_drawdown"),
            ("Annualization Seconds", "annualization_seconds"),
            ("Observed Duration (ms)", "duration_milliseconds"),
        ):
            lines.append(f"| {label} | {metrics.get(key)} |")
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
        initial_balance: InitialBalanceInput = Decimal("10000"),
        max_drawdown_limit: Optional[float] = 0.20,
        data_source: Optional[IDataSource] = None,
        fee_config: Mapping[str, Decimal | float] | None = None,
        report_config: Optional[Dict] = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        instrument_spec: InstrumentSpec | None = None,
        execution_timeframe: str | None = None,
        signal_batch_observer: Callable[[tuple[Signal, ...]], None] | None = None,
        spot_fee_asset: str = "quote",
        external_funding_events: Sequence[ExternalFundingEvent] = (),
        external_funding_account_id: str | None = None,
        market_slippage_bps: Decimal = Decimal("0"),
        modeled_profile_input: ModeledProfileInput | None = None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        self.execution_timeframe = execution_timeframe
        if execution_timeframe is not None:
            execution_ms = timeframe_to_ms(execution_timeframe)
            decision_ms = timeframe_to_ms(timeframe)
            if execution_ms >= decision_ms or decision_ms % execution_ms != 0:
                raise ValueError(
                    "execution_timeframe must evenly divide and be shorter "
                    "than the strategy timeframe"
                )
        self.initial_balance = _normalize_initial_balance(initial_balance)
        self.max_drawdown_limit = max_drawdown_limit
        self.data_source = data_source
        self.fee_config = fee_config or {}
        self.instrument_spec = instrument_spec
        self.market_slippage_bps, _ = resolve_market_slippage_configuration(
            market_slippage_bps,
            instrument_spec=instrument_spec,
        )
        unknown_report_keys = set(report_config or {}) - set(DEFAULT_REPORT_CONFIG)
        if unknown_report_keys:
            raise ValueError(
                "unknown report_config keys: "
                + ", ".join(sorted(str(key) for key in unknown_report_keys))
            )
        self.report_config = {**DEFAULT_REPORT_CONFIG, **(report_config or {})}
        self._db_session_factory = db_session_factory or _sessionlocal_context
        self.spot_fee_asset = spot_fee_asset
        self.external_funding_events = tuple(external_funding_events)
        self.external_funding_account_id = external_funding_account_id
        self.contract_multiplier = resolve_contract_multiplier(instrument_spec)
        self.fee_model = resolve_fee_model(instrument_spec)
        self.signal_batch_observer = signal_batch_observer
        if (
            modeled_profile_input is not None
            and type(modeled_profile_input) is not ModeledProfileInput
        ):
            raise TypeError("modeled_profile_input must be ModeledProfileInput")
        self._modeled_profile_input = modeled_profile_input

        self.clock = BacktestClock(start_time=start_time / 1000)
        self._strategies_buffer: List[BaseStrategy] = []
        self._portfolios_buffer: List[PortfolioDefinition] = []
        self._primary_runtime_id: str | None = None
        self.engine = None

    def add_strategy(self, strategy: BaseStrategy):
        self._validate_profile_admission((strategy,))
        if self._primary_runtime_id is None:
            self._primary_runtime_id = strategy.strategy_id
        self._strategies_buffer.append(strategy)

    def add_portfolio(self, definition: PortfolioDefinition) -> None:
        """Add a portfolio while retaining strategy-scoped fills and metrics."""
        self._validate_profile_admission(
            tuple(sleeve.strategy for sleeve in definition.sleeves)
        )
        decision_timeframe = definition.sleeves[0].strategy.requirements.timeframe
        if (
            definition.product_id != self.product_id
            or decision_timeframe != self.timeframe
        ):
            raise ValueError(
                "portfolio definition does not match backtest product/timeframe"
            )
        if self._primary_runtime_id is None:
            self._primary_runtime_id = definition.portfolio_id
        self._portfolios_buffer.append(definition)
        self._strategies_buffer.extend(sleeve.strategy for sleeve in definition.sleeves)

    def _validate_profile_admission(self, strategies: tuple[BaseStrategy, ...]) -> None:
        profile_strategies = tuple(
            strategy
            for strategy in strategies
            if strategy.requirements.profile_requirements
        )
        if not profile_strategies:
            return
        ids = ",".join(sorted(strategy.strategy_id for strategy in profile_strategies))
        if self._modeled_profile_input is None:
            raise RuntimeError(
                "profile_market_data_provider_required: runner=full strategy_id=" + ids
            )
        if any(
            strategy_context_invocation_mode(strategy)
            is StrategyContextInvocationMode.NONE
            for strategy in profile_strategies
        ):
            raise RuntimeError(
                "profile_strategy_context_required: runner=full strategy_id=" + ids
            )

    def _enrich_profile_decision_context(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
        context: StrategyContext,
    ) -> StrategyContext:
        requirements = strategy.requirements.profile_requirements
        if not requirements:
            return context
        modeled_input = self._modeled_profile_input
        if modeled_input is None:
            raise RuntimeError("profile modeled input missing after admission")
        decision_time_ms = completed_candle_decision_time_ms(
            candle.timestamp, candle.timeframe
        )
        market_data = modeled_input.resolve(
            requirements, decision_time_ms=decision_time_ms
        )
        return enrich_profile_context(
            context,
            requirements,
            market_data,
            decision_time_ms=decision_time_ms,
        )

    def _ensure_strategies_registered(self, db_session: Session):
        """Register all added strategies in the DB to avoid FK constraints"""
        runtime_ids = [strategy.strategy_id for strategy in self._strategies_buffer]
        runtime_ids.extend(
            portfolio.portfolio_id for portfolio in self._portfolios_buffer
        )
        for runtime_id in runtime_ids:
            exists = db_session.query(StrategyORM).filter_by(id=runtime_id).first()
            if not exists:
                logger.info("Registering missing strategy in DB: %s", runtime_id)
                new_strat = StrategyORM(
                    id=runtime_id,
                    name=f"Backtest: {runtime_id}",
                    configuration_json="{}",
                )
                db_session.add(new_strat)
        db_session.commit()

    def _process_candles(
        self,
        candles: Iterable,
        mock_account: BacktestAccountService,
        stop_drawdown_amount: Decimal | None,
        funding_timeline: ExternalFundingTimeline | None = None,
        performance_tracker: FlowNeutralPerformanceTracker | None = None,
        dataset_evidence: BacktestDatasetEvidence | None = None,
    ) -> _ReplayProgress:
        engine = cast(StrategyEngine, self.engine)
        count = 0
        peak_equity = self.initial_balance
        max_drawdown = Decimal("0")
        equity_samples: list[tuple[int, Decimal]] = []
        final_mark: Decimal | None = None
        end_timestamp: int | None = None
        halted_early = False
        recorded_funding_count = 0
        aggregator = (
            CandleAggregator() if self.execution_timeframe is not None else None
        )
        equity_calculator = (
            PortfolioEquityCalculator(
                adapter=mock_account.adapter,
                strategy_ids=[
                    strategy.strategy_id for strategy in self._strategies_buffer
                ],
                product_id=self.product_id,
                contract_multiplier=self.contract_multiplier,
            )
            if isinstance(mock_account.adapter, SimulatedAdapter)
            else None
        )
        for candle in candles:
            if dataset_evidence is not None:
                dataset_evidence.observe(candle)
            # Update Clock
            self.clock.set_time(candle.timestamp / 1000)

            # Fine-grained source candles drive matching first. Strategies only
            # see completed derived candles, matching the live Rust pipeline.
            if aggregator is None:
                engine.on_market_data(candle)
            else:
                if candle.timeframe != self.execution_timeframe:
                    raise ValueError(
                        "backtest execution candle timeframe mismatch: "
                        f"expected={self.execution_timeframe} "
                        f"actual={candle.timeframe}"
                    )
                completed = aggregator.add_candle(
                    RustCandlestick(
                        product_id=candle.product_id,
                        timeframe=candle.timeframe,
                        timestamp=candle.timestamp,
                        open=str(candle.open),
                        high=str(candle.high),
                        low=str(candle.low),
                        close=str(candle.close),
                        volume=str(candle.volume),
                    ),
                    self.timeframe,
                )
                decision_candle = (
                    None
                    if completed is None
                    else Candlestick(
                        product_id=completed.product_id,
                        timeframe=completed.timeframe,
                        timestamp=completed.timestamp,
                        open=Decimal(str(completed.open)),
                        high=Decimal(str(completed.high)),
                        low=Decimal(str(completed.low)),
                        close=Decimal(str(completed.close)),
                        volume=Decimal(str(completed.volume)),
                    )
                )
                engine.on_backtest_market_data(
                    candle,
                    decision_candle,
                )

            # Check Circuit Breaker
            if mock_account.adapter is None:
                current_equity = mock_account.get_balance()
            elif not isinstance(mock_account.adapter, SimulatedAdapter):
                raise RuntimeError(
                    "backtest portfolio equity requires SimulatedAdapter"
                )
            else:
                if equity_calculator is None:
                    raise RuntimeError(
                        "backtest portfolio equity calculator is unavailable"
                    )
                current_equity = equity_calculator.value(candle.close)
            if performance_tracker is not None:
                if funding_timeline is not None:
                    applications = funding_timeline.applications_since(
                        recorded_funding_count
                    )
                    for application in applications:
                        performance_tracker.record_funding(application)
                    recorded_funding_count += len(applications)
                performance_tracker.observe(
                    timestamp=candle.timestamp,
                    equity=current_equity,
                )
            equity_samples.append((candle.timestamp, current_equity))
            final_mark = candle.close
            end_timestamp = candle.timestamp
            peak_equity = max(peak_equity, current_equity)
            max_drawdown = max(max_drawdown, peak_equity - current_equity)
            count += 1
            flow_drawdown_reached = (
                performance_tracker is not None
                and self.max_drawdown_limit is not None
                and performance_tracker.unitized_max_drawdown
                >= Decimal(str(self.max_drawdown_limit))
            )
            raw_drawdown_reached = (
                performance_tracker is None
                and stop_drawdown_amount is not None
                and max_drawdown >= stop_drawdown_amount
            )
            if flow_drawdown_reached or raw_drawdown_reached:
                logger.warning(
                    "STOPPING BACKTEST: Max Drawdown Reached! raw=%s unitized=%s",
                    max_drawdown,
                    (
                        performance_tracker.unitized_max_drawdown
                        if performance_tracker is not None
                        else None
                    ),
                )
                halted_early = True
                break

            if count % 1000 == 0:
                logger.info(
                    "Processed %d candles... Current Time: %s | Equity: %s",
                    count,
                    candle.timestamp,
                    current_equity,
                )
        return _ReplayProgress(
            candle_count=count,
            equity_samples=equity_samples,
            final_mark=final_mark,
            end_timestamp=end_timestamp,
            halted_early=halted_early,
        )

    def _export_reports(
        self,
        metrics: Dict,
        journal: StrategyJournal,
        candle_count: int,
        equity_samples: list[tuple[int, Decimal]] | None = None,
        flow_neutral_performance: FlowNeutralPerformanceReport | None = None,
        provenance: BacktestRunProvenance | None = None,
    ) -> Optional[str]:
        """Write report files to output_dir. Returns output directory path."""
        cfg = self.report_config
        if not any(
            cfg.get(k)
            for k in ("csv_trades", "markdown_report", "equity_curve", "journal_export")
        ):
            return None

        output_dir = Path(cfg.get("output_dir", "backtest_output/"))
        output_dir.mkdir(parents=True, exist_ok=True)

        closed_trades: List[ClosedTrade] = metrics.get("closed_trades", [])

        if cfg.get("csv_trades") and closed_trades:
            _write_csv_trades(closed_trades, output_dir / "trades.csv")

        if cfg.get("equity_curve"):
            if equity_samples is None:
                raise ValueError("equity_samples are required for equity curve export")
            _write_equity_curve(
                [equity for _, equity in equity_samples],
                output_dir / "equity_curve.csv",
            )
            if flow_neutral_performance is not None:
                _write_flow_neutral_curve(
                    flow_neutral_performance,
                    output_dir / "flow_neutral_curve.csv",
                )

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
                fee_model=self.fee_model,
                candle_count=candle_count,
                provenance=provenance,
                market_slippage_bps=self.market_slippage_bps,
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
        primary_strategy_id = (
            self._primary_runtime_id or self._strategies_buffer[0].strategy_id
        )
        summary = BacktestResultSummary(
            strategy_id=primary_strategy_id,
            start_time=self.start_time,
            end_time=self.end_time,
            total_pnl=0,
            metrics_json="{}",
        )
        with self._db_session_factory() as db_session:
            db_session.add(summary)
            db_session.commit()
            summary_id = summary.id
        logger.info("Backtest Session Created: ID %s", summary_id)

        # 2. Create journal for structured event recording
        journal = StrategyJournal(primary_strategy_id)

        # 3. Create Rust-backed adapter with fee config
        funding_timeline = build_external_funding_timeline(
            self.external_funding_events,
            account_id=self.external_funding_account_id,
            quote_asset=(
                self.instrument_spec.quote
                if self.instrument_spec is not None
                and self.instrument_spec.market_type == MarketType.SPOT
                else None
            ),
            start_time=self.start_time,
        )
        adapter = SimulatedAdapter(
            initial_balance=self.initial_balance,
            maker_fee=Decimal(str(self.fee_config.get("maker", 0))),
            taker_fee=Decimal(str(self.fee_config.get("taker", 0))),
            instrument_spec=self.instrument_spec,
            spot_fee_asset=self.spot_fee_asset,
            external_funding_timeline=funding_timeline,
            market_slippage_bps=self.market_slippage_bps,
        )
        performance_tracker = (
            FlowNeutralPerformanceTracker(self.initial_balance)
            if adapter.is_cash_spot_settlement
            else None
        )
        context_peak_equity = {
            strategy.strategy_id: self.initial_balance
            for strategy in self._strategies_buffer
        }
        context_max_drawdown = {
            strategy.strategy_id: Decimal("0") for strategy in self._strategies_buffer
        }
        configuration_contract = {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "product_id": self.product_id,
            "timeframe": self.timeframe,
            "initial_balance": self.initial_balance,
            "max_drawdown_limit": self.max_drawdown_limit,
            "fee_config": self.fee_config,
            "market_slippage_bps": self.market_slippage_bps,
            "instrument_spec": self.instrument_spec,
            "spot_fee_asset": self.spot_fee_asset,
            "external_funding_events": self.external_funding_events,
            "external_funding_account_id": self.external_funding_account_id,
            "strategies": strategy_configuration_contract(self._strategies_buffer),
        }
        if self._modeled_profile_input is not None:
            configuration_contract["profile_availability_policy_id"] = (
                self._modeled_profile_input.availability_policy_id
            )
        runner_configuration_contract = {
            **configuration_contract,
            "runner_kind": "full",
            "execution_timeframe": self.execution_timeframe,
        }
        dataset_evidence = BacktestDatasetEvidence()
        decision_snapshots: list[dict[str, object]] = []

        def strategy_context_loader(
            strategy: BaseStrategy,
            candle: Candlestick,
            latest_fills: tuple[dict, ...],
        ) -> StrategyContext:
            strategy_id = strategy.strategy_id
            engine = cast(StrategyEngine, self.engine)
            context = adapter.get_strategy_context(
                strategy_id=strategy_id,
                product_id=candle.product_id,
                timestamp=candle.timestamp,
                initial_balance=self.initial_balance,
                mark_price=candle.close,
                peak_equity=context_peak_equity[strategy_id],
                max_drawdown=context_max_drawdown[strategy_id],
                latest_fills=list(latest_fills),
                latest_rejections=(
                    engine.execution_engine.pop_simulated_matching_rejections(
                        strategy_id
                    )
                ),
            )
            peak_equity = max(
                context_peak_equity[strategy_id],
                context.total_equity,
            )
            current_drawdown = max(
                peak_equity - context.total_equity,
                Decimal("0"),
            )
            max_drawdown = max(
                context_max_drawdown[strategy_id],
                current_drawdown,
            )
            context_peak_equity[strategy_id] = peak_equity
            context_max_drawdown[strategy_id] = max_drawdown
            if (
                context.current_drawdown == current_drawdown
                and context.max_drawdown == max_drawdown
            ):
                decision_context = context
            else:
                decision_context = replace(
                    context,
                    current_drawdown=current_drawdown,
                    max_drawdown=max_drawdown,
                )
            decision_context = self._enrich_profile_decision_context(
                strategy, candle, decision_context
            )
            decision_snapshots.append(canonical_decision_snapshot(decision_context))
            return decision_context

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
            signal_batch_observer=self.signal_batch_observer,
            is_backtest=True,
            strategy_context_loader=strategy_context_loader,
        )

        # Inject journal and account service into strategies
        portfolio_sleeve_ids = {
            sleeve.strategy.strategy_id
            for portfolio in self._portfolios_buffer
            for sleeve in portfolio.sleeves
        }
        for strat in self._strategies_buffer:
            strat.journal = journal
            if hasattr(strat, "risk_manager"):
                risk_manager = cast(
                    _BacktestRiskManager,
                    getattr(strat, "risk_manager"),
                )
                risk_manager.account_service = mock_account
                risk_manager.instrument_spec_resolver = self._resolve_instrument_spec
            if strat.strategy_id not in portfolio_sleeve_ids:
                self.engine.add_strategy(strat)
        for portfolio in self._portfolios_buffer:
            self.engine.add_portfolio(portfolio)

        logger.info(
            "Starting Backtest for %s [%s - %s]",
            self.product_id,
            self.start_time,
            self.end_time,
        )

        stop_drawdown_amount = (
            None
            if self.max_drawdown_limit is None
            else self.initial_balance * Decimal(str(self.max_drawdown_limit))
        )

        if self.data_source:
            progress = self._process_candles(
                self.data_source.get_candles(
                    self.product_id,
                    self.execution_timeframe or self.timeframe,
                    self.start_time,
                    self.end_time,
                ),
                mock_account,
                stop_drawdown_amount,
                funding_timeline,
                performance_tracker,
                dataset_evidence,
            )
        else:
            with self._db_session_factory() as db_session:
                candle_gen = get_candles_generator(
                    db_session,
                    self.product_id,
                    self.execution_timeframe or self.timeframe,
                    self.start_time,
                    self.end_time,
                )
                progress = self._process_candles(
                    candle_gen,
                    mock_account,
                    stop_drawdown_amount,
                    funding_timeline,
                    performance_tracker,
                    dataset_evidence,
                )

        endpoint_state = build_replay_endpoint_state(
            positions=adapter.get_all_positions(),
            working_orders=adapter.get_matching_open_orders(),
            final_mark=progress.final_mark,
            end_timestamp=progress.end_timestamp,
            halted_early=progress.halted_early,
        )
        flow_neutral_performance = (
            performance_tracker.report() if performance_tracker is not None else None
        )
        cash_spot_account_snapshot = (
            adapter.get_cash_spot_account_snapshot(progress.final_mark)
            if adapter.is_cash_spot_settlement and progress.final_mark is not None
            else None
        )

        # Calculate Final PnL
        final_balance = mock_account.get_balance()
        total_pnl = (
            flow_neutral_performance.net_pnl
            if flow_neutral_performance is not None
            and flow_neutral_performance.net_pnl is not None
            else (
                adapter.get_total_equity(progress.final_mark) - self.initial_balance
                if adapter.is_cash_spot_settlement and progress.final_mark is not None
                else final_balance - self.initial_balance
            )
        )

        with self._db_session_factory() as db_session:
            summary = cast(
                BacktestResultSummary,
                db_session.query(BacktestResultSummary)
                .filter_by(id=summary_id)
                .first(),
            )
            # Metrics (with advanced calculations)
            trades = (
                db_session.query(BacktestTradeLog)
                .filter_by(session_id=summary_id)
                .order_by(
                    BacktestTradeLog.timestamp,
                    BacktestTradeLog.fill_sequence,
                    BacktestTradeLog.id,
                )
                .all()
            )
            metrics = calculate_metrics(
                trades,
                initial_balance=self.initial_balance,
                contract_multiplier=self.contract_multiplier,
                equity_samples=progress.equity_samples,
                spot_base_asset=(
                    self.instrument_spec.base
                    if adapter.is_cash_spot_settlement
                    and self.instrument_spec is not None
                    else None
                ),
                spot_quote_asset=(
                    self.instrument_spec.quote
                    if adapter.is_cash_spot_settlement
                    and self.instrument_spec is not None
                    else None
                ),
            )
            fill_records = canonical_fill_records(trades)
            if flow_neutral_performance is not None:
                metrics.update(flow_neutral_performance.metric_fields())
                if flow_neutral_performance.net_pnl is not None:
                    metrics["total_pnl"] = flow_neutral_performance.net_pnl
                    metrics["mark_to_market_pnl"] = flow_neutral_performance.net_pnl

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

        provenance = build_backtest_run_provenance(
            runner_kind="full",
            dataset=dataset_evidence,
            configuration=configuration_contract,
            runner_configuration=runner_configuration_contract,
            program_components=(
                type(self),
                StrategyEngine,
                SimulatedAdapter,
                ExternalFundingTimeline,
                FlowNeutralPerformanceTracker,
                *(type(strategy) for strategy in self._strategies_buffer),
            ),
        )

        # Export reports
        report_dir = self._export_reports(
            metrics,
            journal,
            candle_count=progress.candle_count,
            equity_samples=progress.equity_samples,
            flow_neutral_performance=flow_neutral_performance,
            provenance=provenance,
        )

        logger.info(
            "Backtest Complete. Processed %d candles. Final PnL: %s",
            progress.candle_count,
            total_pnl,
        )
        logger.info("Metrics: %s", metrics_for_json)
        if report_dir:
            logger.info("Reports written to: %s", report_dir)

        result = {
            "total_pnl": total_pnl,
            "mark_to_market_pnl": metrics.get(
                "mark_to_market_pnl",
                total_pnl,
            ),
            "max_drawdown": metrics.get("max_drawdown", Decimal("0")),
            "win_rate": metrics.get("win_rate", Decimal("0")),
            "total_trades": int(metrics.get("total_trades", 0)),
            "closed_trade_count": int(metrics.get("closed_trade_count", 0)),
            "trade_sharpe": metrics.get("trade_sharpe", Decimal("0")),
            "trade_pnl_quality": metrics.get("trade_sharpe", Decimal("0")),
            "profit_factor": metrics.get("profit_factor", Decimal("0")),
            "sortino_ratio": metrics.get("sortino_ratio", Decimal("0")),
            "calmar_ratio": metrics.get("calmar_ratio", Decimal("0")),
            "max_drawdown_days": metrics.get("max_drawdown_days", Decimal("0")),
            "avg_hold_time_hours": metrics.get("avg_hold_time_hours", Decimal("0")),
            "max_consecutive_wins": int(metrics.get("max_consecutive_wins", 0)),
            "max_consecutive_losses": int(metrics.get("max_consecutive_losses", 0)),
            "monthly_returns": metrics.get("monthly_returns", {}),
            "closed_trades": metrics.get("closed_trades", []),
            "journal": journal.to_dicts(),
            "journal_count": len(journal),
            "candle_count": progress.candle_count,
            "report_dir": report_dir,
            "per_strategy": per_strategy,
            "endpoint_state": endpoint_state,
            "cash_spot_account_snapshot": cash_spot_account_snapshot,
            "flow_neutral_performance": flow_neutral_performance,
            "external_funding_applications": (
                funding_timeline.applications if funding_timeline is not None else ()
            ),
            "external_funding_checkpoint": (
                funding_timeline.checkpoint() if funding_timeline is not None else None
            ),
            "fill_records": fill_records,
            "decision_snapshots": tuple(decision_snapshots),
            "provenance": provenance,
        }
        if flow_neutral_performance is not None:
            result.update(flow_neutral_performance.metric_fields())
        (
            return_samples,
            return_initial,
            return_start_time,
            return_end_time,
        ) = return_metric_inputs(
            progress.equity_samples,
            self.initial_balance,
            self.start_time,
            self.end_time,
            flow_neutral_performance,
        )
        daily_return_metrics = utc_daily_return_metrics(
            return_samples,
            initial_balance=return_initial,
            start_time=return_start_time,
            end_time=return_end_time,
        )
        daily_return_moments: dict[str, Decimal | int] = {
            name: cast(Decimal | int, daily_return_metrics[name])
            for name in (
                "count",
                "sum",
                "sum_squares",
                "sum_cubes",
                "sum_fourth",
            )
        }
        result["daily_return_moments"] = daily_return_moments
        result["equity_sample_count"] = daily_return_metrics["equity_sample_count"]
        result["yearly_mark_to_market_returns"] = daily_return_metrics["yearly_returns"]
        result["yearly_time_weighted_returns"] = daily_return_metrics["yearly_returns"]
        result["annualized_sharpe"] = annualized_sharpe_from_moments(
            daily_return_moments
        )

        return result

    def _resolve_instrument_spec(self, product_id: str) -> InstrumentSpec | None:
        if (
            self.instrument_spec is None
            or self.instrument_spec.product_id != product_id
        ):
            return None
        return self.instrument_spec

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
            strategy_trades = [
                t for t in trades if getattr(t, "strategy_id", None) == sid
            ]
            if strategy_trades:
                strategy_metrics = calculate_metrics(
                    strategy_trades,
                    initial_balance=self.initial_balance,
                    contract_multiplier=self.contract_multiplier,
                    spot_base_asset=(
                        self.instrument_spec.base
                        if self.instrument_spec is not None
                        and self.instrument_spec.market_type == MarketType.SPOT
                        else None
                    ),
                    spot_quote_asset=(
                        self.instrument_spec.quote
                        if self.instrument_spec is not None
                        and self.instrument_spec.market_type == MarketType.SPOT
                        else None
                    ),
                )
                strategy_metrics["max_drawdown"] = abs(
                    strategy_metrics.get("max_drawdown", Decimal("0"))
                )
                per_strategy[sid] = strategy_metrics
        return per_strategy
