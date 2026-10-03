from __future__ import annotations

import hashlib
import inspect
import json
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import event, select
from sqlalchemy.orm import sessionmaker

from src.control_plane import backtest_jobs
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.jobs import JobStatus, SqliteJobStore
from src.core.backtest_result_owner import (
    BacktestResultPersistenceOwner,
    BacktestResultPersistenceReceipt,
    BacktestResultRunIdentity,
    _project_result,
)
from src.core.backtest_result_persistence import FullBacktestOutcome
from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
    BacktestResultSummary,
    BacktestTradeLog,
    ResearchCandlestick,
    ResearchDataset,
    Strategy,
)
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from src.core.strategy_loader import StrategyLoader
from src.strategies.base import BaseStrategy
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db
from test_full_backtest_resolution import _request as _request_template
from test_strategy_loader import _read_only_tree, _write_catalog

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_START = 1_704_067_200_000
_PRODUCT = "BINANCE:BTCUSDT-PERP"
_DATASET = "p1d3c-sealed-native"
_STRATEGY = "stable_strategy_v1"
_STRATEGY_SOURCE = """from decimal import Decimal

from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements


class CatalogSignalStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str) -> None:
        super().__init__(strategy_id, product_id)
        self._index = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle: Candlestick, context=None):
        self._index += 1
        signal_type = {1: SignalType.LONG, 4: SignalType.EXIT_LONG}.get(self._index)
        if signal_type is None:
            return None
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="1m",
            timestamp=candle.timestamp,
            type=signal_type,
            quantity=Decimal("1"),
        )
"""


def _write_native_catalog(root: Path) -> str:
    catalog = _write_catalog(root, id=_STRATEGY, **{"class": "CatalogSignalStrategy"})
    module = root / "strategy.py"
    module.write_text(_STRATEGY_SOURCE, encoding="utf-8")
    catalog["files"][module.name] = hashlib.sha256(module.read_bytes()).hexdigest()
    (root / StrategyLoader.CATALOG_NAME).write_text(
        json.dumps(catalog), encoding="utf-8"
    )
    return "1.0.0"


def _write_candles(path: Path) -> None:
    rows = (
        ("100.0000000000000000001", 101, 99, "100.0000000000000000001"),
        (100, 103, 99, 102),
        (102, 105, 101, 104),
        (104, 105, 102, 103),
        (103, 104, 100, 101),
        (101, 103, 100, 102),
        (102, 104, 101, 103),
        (103, 104, 101, 102),
    )
    lines = ["timestamp,open,high,low,close,volume"]
    lines.extend(
        f"{_START + index * 60_000},{opening},{high},{low},{close},1"
        for index, (opening, high, low, close) in enumerate(rows)
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_verified_catalog_sealed_pg_native_executor_owner_and_retry(
    fresh_pg_db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    session_factory = sessionmaker(bind=engine)
    store = SqliteJobStore(tmp_path / "jobs.sqlite3")
    catalog_root = tmp_path / "production_catalog"
    catalog_root.mkdir()
    artifact_version = _write_native_catalog(catalog_root)
    candles_csv = tmp_path / "sealed.csv"
    _write_candles(candles_csv)
    try:
        imported = ResearchDatasetImporter(session_factory).import_csv(
            candles_csv,
            ResearchDatasetSpec(
                dataset_id=_DATASET,
                product_id=_PRODUCT,
                timeframe="1m",
                source="p1d3c-native-integration",
                revision="v1",
            ),
        )
        with session_factory() as session:
            dataset = session.get(ResearchDataset, _DATASET)
            assert dataset is not None
            assert (dataset.lifecycle_state, dataset.quality_status) == (
                "sealed",
                "validated",
            )
            assert dataset.sealed_at is not None
            assert dataset.checksum_sha256 == imported.checksum_sha256
            assert dataset.start_time == _START
            assert dataset.end_time == _START + 420_000
            assert dataset.row_count == 8
            candle = session.scalars(
                select(ResearchCandlestick)
                .where(ResearchCandlestick.dataset_id == _DATASET)
                .order_by(ResearchCandlestick.timestamp)
            ).first()
            assert candle is not None
            assert (candle.open, candle.close, candle.volume) == (
                Decimal("100.0000000000000000001"),
                Decimal("100.0000000000000000001"),
                Decimal("1"),
            )
            assert isinstance(candle.close, Decimal)
            session.add(Strategy(id=_STRATEGY, name="Catalog Signal Strategy"))
            session.commit()

        with _read_only_tree(catalog_root):
            catalog = StrategyLoader.scan_production_sources(str(catalog_root))
            artifact = catalog[_STRATEGY]
            assert isinstance(artifact, type)
            loaded_version = getattr(artifact, "__fluxtrade_artifact_version__")
            loaded_catalog_sha = getattr(artifact, "__fluxtrade_catalog_sha256__")
            assert loaded_version == artifact_version
            assert len(loaded_catalog_sha) == 64

            loader_calls = 0

            def production_loader():
                nonlocal loader_calls
                loader_calls += 1
                return StrategyLoader.scan_production_sources(str(catalog_root))

            strategies: list[BaseStrategy] = []
            identity_outcomes: dict[
                str, tuple[BacktestResultRunIdentity, FullBacktestOutcome]
            ] = {}
            receipts: dict[str, BacktestResultPersistenceReceipt] = {}
            original_run = backtest_jobs.run_full_backtest
            original_persist = BacktestResultPersistenceOwner.persist

            def capture_run(resolved, *, session_factory):
                strategies.append(resolved.strategy)
                result = original_run(resolved, session_factory=session_factory)
                return result

            def capture_persist(owner, identity, outcome):
                current = store.get(identity.job_id)
                assert current is not None and current.status == JobStatus.RUNNING
                identity_outcomes[identity.job_id] = (identity, outcome)
                receipt = original_persist(owner, identity, outcome)
                receipts[identity.job_id] = receipt
                return receipt

            monkeypatch.setattr(backtest_jobs, "run_full_backtest", capture_run)
            monkeypatch.setattr(
                BacktestResultPersistenceOwner, "persist", capture_persist
            )
            executor = BacktestJobExecutor(
                store,
                db_session_factory=session_factory,
                run_inline=True,
                strategy_loader=production_loader,
            )
            template = _request_template()
            request = template.model_copy(
                update={
                    "dataset_id": _DATASET,
                    "strategy_id": _STRATEGY,
                    "artifact_version": artifact_version,
                    "initial_balance": Decimal("10000.0000000000000000000001"),
                    "instrument": template.instrument.model_copy(
                        update={
                            "quantity_step": Decimal("0.001"),
                            "price_tick": Decimal("0.01"),
                        }
                    ),
                }
            )
            from src.core.backtest_runner import BacktestRunner

            assert (
                Path(inspect.getsourcefile(BacktestRunner) or "").resolve()
                == (Path(__file__).parents[2] / "src/core/backtest_runner.py").resolve()
            )

            def fail_if_report_directory_created(*_args, **_kwargs):
                raise AssertionError("disabled reports must not touch the filesystem")

            monkeypatch.setattr(Path, "mkdir", fail_if_report_directory_created)

            def fail_closed_trade_insert(mapper, connection, target) -> None:
                raise RuntimeError("injected persistence failure")

            event.listen(BacktestClosedTrade, "before_insert", fail_closed_trade_insert)
            try:
                failed = executor.submit_backtest(request)
            finally:
                event.remove(
                    BacktestClosedTrade, "before_insert", fail_closed_trade_insert
                )

            assert failed.status == JobStatus.FAILED
            assert failed.error == "browser_result_persistence_failed"
            assert failed.result is None
            assert failed.request["dataset_id"] == _DATASET
            assert failed.request["initial_balance"] == "10000.0000000000000000000001"
            assert failed.id not in receipts
            with session_factory() as session:
                assert (
                    session.scalar(
                        select(sa.func.count())
                        .select_from(BacktestResultSummary)
                        .where(BacktestResultSummary.job_id == failed.id)
                    )
                    == 0
                )
                assert all(
                    session.scalar(select(sa.func.count()).select_from(table)) == 0
                    for table in (
                        BacktestEquitySample,
                        BacktestClosedTrade,
                        BacktestMonthlyReturn,
                        BacktestPnlDistribution,
                    )
                )
                audit_rows = session.scalar(
                    select(sa.func.count())
                    .select_from(BacktestTradeLog)
                    .where(BacktestTradeLog.strategy_id == _STRATEGY)
                )
                assert audit_rows is not None and audit_rows > 0

            retried = executor.retry_backtest(failed.id)
            assert retried.id != failed.id
            assert retried.status == JobStatus.SUCCEEDED
            assert retried.error is None
            assert retried.result is not None
            assert set(retried.result) == {"job_id", "input_digest", "result_digest"}
            assert retried.result["job_id"] == retried.id

            repeated = executor.submit_backtest(request)
            assert repeated.status == JobStatus.SUCCEEDED
            assert repeated.id != retried.id
            assert repeated.result is not None
            assert repeated.result["input_digest"] == retried.result["input_digest"]
            assert repeated.result["result_digest"] == retried.result["result_digest"]
            assert len(strategies) == 3
            assert len({id(strategy) for strategy in strategies}) == 3
            assert loader_calls == 3

            for job in (retried, repeated):
                identity, outcome = identity_outcomes[job.id]
                receipt = receipts[job.id]
                assert job.result == {
                    "job_id": receipt.job_id,
                    "input_digest": receipt.input_digest,
                    "result_digest": receipt.result_digest,
                }
                assert identity.dataset_checksum_sha256 == imported.checksum_sha256
                assert identity.catalog_sha256 == loaded_catalog_sha
                assert outcome.closed_trades
                assert any(trade.fee > 0 for trade in outcome.closed_trades)
                with session_factory() as session:
                    summary = session.scalars(
                        select(BacktestResultSummary).where(
                            BacktestResultSummary.job_id == job.id
                        )
                    ).one()
                    assert summary.completed_at is not None
                    projection = _project_result(
                        identity, outcome, completed_at=summary.completed_at
                    )
                    equity = session.scalars(
                        select(BacktestEquitySample)
                        .where(BacktestEquitySample.summary_id == summary.id)
                        .order_by(BacktestEquitySample.sequence)
                    ).all()
                    trades = session.scalars(
                        select(BacktestClosedTrade)
                        .where(BacktestClosedTrade.summary_id == summary.id)
                        .order_by(BacktestClosedTrade.sequence)
                    ).all()
                    monthly = session.scalars(
                        select(BacktestMonthlyReturn).where(
                            BacktestMonthlyReturn.summary_id == summary.id
                        )
                    ).all()
                    distribution = session.scalars(
                        select(BacktestPnlDistribution)
                        .where(BacktestPnlDistribution.summary_id == summary.id)
                        .order_by(BacktestPnlDistribution.sequence)
                    ).all()
                    audit_count = session.scalar(
                        select(sa.func.count())
                        .select_from(BacktestTradeLog)
                        .where(BacktestTradeLog.strategy_id == _STRATEGY)
                    )
                assert (
                    summary.initial_balance,
                    summary.total_pnl,
                    summary.net_pnl,
                    summary.return_pct,
                    summary.max_drawdown,
                    summary.sharpe,
                    summary.sortino,
                    summary.calmar,
                ) == tuple(projection.summary)
                assert summary.metrics_json is None
                assert (
                    summary.job_id,
                    summary.dataset_id,
                    summary.subject_kind,
                    summary.subject_id,
                    summary.input_digest,
                    summary.result_digest,
                    summary.product_id,
                    summary.timeframe,
                    summary.currency,
                    summary.start_time,
                    summary.end_time,
                ) == (
                    job.id,
                    _DATASET,
                    "STRATEGY_ARTIFACT",
                    f"{_STRATEGY}:{artifact_version}",
                    projection.input_digest,
                    projection.result_digest,
                    _PRODUCT,
                    "1m",
                    "USDT",
                    _START,
                    _START + 420_000,
                )
                assert [
                    (row.sequence, row.timestamp, row.equity, row.drawdown)
                    for row in equity
                ] == [tuple(row) for row in projection.equity]
                assert [
                    (
                        row.sequence,
                        row.entry_time,
                        row.exit_time,
                        row.side,
                        row.quantity,
                        row.entry_price,
                        row.exit_price,
                        row.fee,
                        row.pnl,
                    )
                    for row in trades
                ] == [tuple(row) for row in projection.closed_trades]
                assert [(row.month, row.return_pct) for row in monthly] == [
                    tuple(row) for row in projection.monthly_returns
                ]
                assert [
                    (row.sequence, row.lower, row.upper, row.count)
                    for row in distribution
                ] == [
                    (row.sequence, row.lower, row.upper, row.count)
                    for row in projection.pnl_distribution
                ]
                assert audit_count and len(trades) == len(outcome.closed_trades)
            assert executor.shutdown()
    finally:
        engine.dispose()
