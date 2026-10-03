from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import select
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
    ResearchCandlestick,
    ResearchDataset,
    Strategy,
)
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from src.core.strategy_loader import StrategyLoader
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


def test_verified_catalog_sealed_pg_native_executor_owner_success(
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
            session.add(Strategy(id=_STRATEGY, name="Catalog Signal Strategy"))
            session.commit()

        with _read_only_tree(catalog_root):
            loaded_catalog_sha = getattr(
                StrategyLoader.scan_production_sources(str(catalog_root))[_STRATEGY],
                "__fluxtrade_catalog_sha256__",
            )

            def production_loader():
                return StrategyLoader.scan_production_sources(str(catalog_root))

            identities: dict[str, BacktestResultRunIdentity] = {}
            outcomes: dict[str, FullBacktestOutcome] = {}
            receipts: dict[str, BacktestResultPersistenceReceipt] = {}
            run_outcomes: list[FullBacktestOutcome] = []
            original_run = backtest_jobs.run_full_backtest
            original_persist = BacktestResultPersistenceOwner.persist

            def capture_run(resolved, *, session_factory):
                outcome = original_run(resolved, session_factory=session_factory)
                run_outcomes.append(outcome)
                return outcome

            def capture_persist(owner, identity, outcome):
                current = store.get(identity.job_id)
                assert current is not None and current.status == JobStatus.RUNNING
                identities[identity.job_id] = identity
                receipts[identity.job_id] = original_persist(owner, identity, outcome)
                outcomes[identity.job_id] = outcome
                return receipts[identity.job_id]

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

            assert BacktestRunner.__module__ == "src.core.backtest_runner"

            def fail_if_report_directory_created(*_args, **_kwargs):
                raise AssertionError("disabled reports must not touch the filesystem")

            monkeypatch.setattr(Path, "mkdir", fail_if_report_directory_created)
            job = executor.submit_backtest(request)
            assert job.status == JobStatus.SUCCEEDED
            assert job.error is None and job.result is not None
            assert set(job.result) == {"job_id", "input_digest", "result_digest"}
            assert job.result["job_id"] == job.id

            identity = identities[job.id]
            outcome = outcomes[job.id]
            assert run_outcomes == [outcome]
            receipt = receipts[job.id]
            assert identity.dataset_checksum_sha256 == imported.checksum_sha256
            assert identity.catalog_sha256 == loaded_catalog_sha
            assert identity.dataset_id == _DATASET
            assert identity.artifact_version == artifact_version
            assert (identity.product_id, identity.timeframe) == (_PRODUCT, "1m")
            assert (identity.start, identity.end) == (_START, _START + 420_000)
            assert identity.initial_balance == Decimal("10000.0000000000000000000001")
            assert (identity.maker_fee, identity.taker_fee) == (
                Decimal("0.0002"),
                Decimal("0.0006"),
            )
            assert outcome.closed_trades and any(
                trade.fee > 0 for trade in outcome.closed_trades
            )
            assert job.result == {
                "job_id": receipt.job_id,
                "input_digest": receipt.input_digest,
                "result_digest": receipt.result_digest,
            }

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
                assert (
                    summary.dataset_id,
                    summary.input_digest,
                    summary.result_digest,
                ) == (
                    _DATASET,
                    projection.input_digest,
                    projection.result_digest,
                )
                counts = tuple(
                    session.scalar(
                        select(sa.func.count())
                        .select_from(model)
                        .where(model.summary_id == summary.id)
                    )
                    for model in (
                        BacktestEquitySample,
                        BacktestClosedTrade,
                        BacktestMonthlyReturn,
                        BacktestPnlDistribution,
                    )
                )
            assert counts == (
                len(projection.equity),
                len(projection.closed_trades),
                len(projection.monthly_returns),
                len(projection.pnl_distribution),
            )
            assert all(count is not None and count > 0 for count in counts)
            assert executor.shutdown()
    finally:
        engine.dispose()
