from __future__ import annotations

from decimal import Decimal
from typing import NoReturn

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from src.control_plane.evaluation_data import (
    RequestEvaluationDataSourceProvider,
    SealedDatasetBackendUnavailableError,
    SealedDatasetRejectedError,
)
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.models import (
    JobStatus,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_evaluation import ParameterSearchEvaluatorRegistry
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import (
    Base,
    Exchange,
    Product,
    ResearchCandlestick,
    ResearchDataset,
)
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec

PRODUCT = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "1m"
START = 1_704_067_200_000
DATASET = "preflight-btc-v1"


@pytest.fixture
def sealed_dataset(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'sealed-preflight.db'}")
    Base.metadata.create_all(
        engine,
        tables=[
            Exchange.__table__,
            Product.__table__,
            ResearchDataset.__table__,
            ResearchCandlestick.__table__,
        ],
    )
    sessions = sessionmaker(bind=engine)
    csv_path = tmp_path / "oscillating.csv"
    closes = [100, 99, 98, 105, 110, 95, 90, 108, 112, 90, 88, 110]
    rows = ["timestamp,open,high,low,close,volume"]
    for index, close in enumerate(closes):
        timestamp = START + index * 60_000
        rows.append(f"{timestamp},{close},{close + 1},{close - 1},{close},1")
    csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    ResearchDatasetImporter(session_factory=sessions).import_csv(
        csv_path,
        ResearchDatasetSpec(
            dataset_id=DATASET,
            product_id=PRODUCT,
            timeframe=TIMEFRAME,
            source="synthetic-test",
            revision="v1",
        ),
    )
    try:
        yield sessions, csv_path, START + (len(closes) - 1) * 60_000
    finally:
        engine.dispose()


def _request(
    *,
    dataset_id=DATASET,
    product_id=PRODUCT,
    timeframe=TIMEFRAME,
    start=START,
    end=None,
):
    return ParameterSearchJobRequest.model_validate(
        {
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross",
            "product_id": product_id,
            "timeframe": timeframe,
            "start_time": start,
            "end_time": START + 11 * 60_000 if end is None else end,
            "market_data": {"kind": "sealed_dataset", "dataset_id": dataset_id},
            "backtest": {
                "initial_balance": "1000",
                "maker_fee": "0.001",
                "taker_fee": "0.001",
            },
            "candidates": [
                {
                    "candidate_id": "small-windows",
                    "param_pack": {
                        "short_window": 2,
                        "long_window": 3,
                        "quantity": "1",
                    },
                }
            ],
        }
    )


class _UnusedEvaluator:
    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> NoReturn:
        raise AssertionError("preflight must not evaluate candidates")


class _LegacyEvaluator:
    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal("1"),
        )


@pytest.mark.parametrize(
    "case",
    [
        "absent",
        "unsealed",
        "unvalidated",
        "wrong_product",
        "wrong_timeframe",
        "outside",
        "reversed",
    ],
)
def test_registry_rejects_invalid_sealed_source_before_job_or_candle_load(
    sealed_dataset, monkeypatch, case
):
    sessions, _, end = sealed_dataset
    request = _request(end=end)
    if case == "absent":
        request = _request(dataset_id="missing", end=end)
    elif case in {"unsealed", "unvalidated"}:
        with sessions() as session:
            row = session.get(ResearchDataset, DATASET)
            assert row is not None
            if case == "unsealed":
                row.lifecycle_state = "importing"
                row.sealed_at = None
            else:
                session.connection().exec_driver_sql(
                    "PRAGMA ignore_check_constraints=ON"
                )
                row.quality_status = "pending"
            session.commit()
    elif case == "wrong_product":
        request = _request(product_id="BINANCE:ETHUSDT-PERP", end=end)
    elif case == "wrong_timeframe":
        request = _request(timeframe="5m", end=end)
    elif case == "outside":
        request = _request(start=START - 1, end=end)
    elif case == "reversed":
        request = request.model_copy(update={"start_time": end + 1})

    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_candles",
        lambda *args, **kwargs: pytest.fail("candles loaded before source admission"),
    )
    store = InMemoryJobStore()
    executor = ParameterSearchJobExecutor(
        ParameterSearchEvaluatorRegistry(
            {"golden_cross": _UnusedEvaluator()},
            data_source_provider=RequestEvaluationDataSourceProvider(
                session_factory=sessions
            ),
        ),
        store=store,
        run_inline=True,
    )

    with pytest.raises(SealedDatasetRejectedError):
        executor.submit_search(request)

    assert store.list() == []
    executor.shutdown()


def test_registry_rejects_sealed_request_without_explicit_source_provider():
    registry = ParameterSearchEvaluatorRegistry({"golden_cross": _UnusedEvaluator()})

    with pytest.raises(SealedDatasetRejectedError):
        registry.validate_request(_request())


def test_registry_classifies_catalog_backend_failure_without_detail(
    sealed_dataset, monkeypatch
):
    sessions, _, end = sealed_dataset

    def fail_catalog(*args, **kwargs):
        raise OperationalError(
            "SELECT private catalog", {}, RuntimeError("private detail")
        )

    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_available_range",
        fail_catalog,
    )
    store = InMemoryJobStore()
    executor = ParameterSearchJobExecutor(
        ParameterSearchEvaluatorRegistry(
            {"golden_cross": _UnusedEvaluator()},
            data_source_provider=RequestEvaluationDataSourceProvider(
                session_factory=sessions
            ),
        ),
        store=store,
        run_inline=True,
    )

    with pytest.raises(SealedDatasetBackendUnavailableError) as exc_info:
        executor.submit_search(_request(end=end))

    assert str(exc_info.value) == ""
    assert store.list() == []
    executor.shutdown()


def test_custom_evaluator_rejects_sealed_before_job_and_keeps_legacy_path():
    store = InMemoryJobStore()
    executor = ParameterSearchJobExecutor(
        _LegacyEvaluator(), store=store, run_inline=True
    )

    with pytest.raises(SealedDatasetRejectedError):
        executor.submit_search(_request())

    assert store.list() == []
    legacy = _request().model_copy(update={"market_data": None})
    job = executor.submit_search(legacy)
    assert job.status == JobStatus.SUCCEEDED
    assert len(store.list()) == 1
    executor.shutdown()
