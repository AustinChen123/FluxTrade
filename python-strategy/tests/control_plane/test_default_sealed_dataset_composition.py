from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.evaluation_data import RequestEvaluationDataSourceProvider
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.main import build_control_plane_app
from src.control_plane.models import ParameterSearchJobRequest
from src.control_plane.parameter_evaluation import (
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchEvaluatorRegistry,
)
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
DATASET = "default-composition-v1"


@pytest.fixture
def sealed_dataset(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'default-composition.db'}")
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
        yield sessions, START + (len(closes) - 1) * 60_000
    finally:
        engine.dispose()


def _request(end: int) -> ParameterSearchJobRequest:
    return ParameterSearchJobRequest.model_validate(
        {
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross",
            "product_id": PRODUCT,
            "timeframe": TIMEFRAME,
            "start_time": START,
            "end_time": end,
            "market_data": {"kind": "sealed_dataset", "dataset_id": DATASET},
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


def test_default_app_composes_one_sealed_provider_and_runs_native_golden_cross(
    sealed_dataset,
):
    sessions, end = sealed_dataset
    app = build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=sessions,
        job_store=InMemoryJobStore(),
        api_key="operator-key",
        readiness_probe=lambda: None,
        profile_query_service=MagicMock(),
    )
    try:
        request = _request(end)
        assert app.parameter_search_executor is not None
        registry = app.parameter_search_executor.evaluator
        assert isinstance(registry, ParameterSearchEvaluatorRegistry)
        assert isinstance(
            registry._data_source_provider,
            RequestEvaluationDataSourceProvider,
        )
        evaluator = registry._resolve(request)
        assert isinstance(evaluator, GoldenCrossResearchParameterEvaluator)
        assert evaluator._data_source_provider is registry._data_source_provider

        assert request.backtest is not None
        assert request.candidates is not None
        candidate = request.candidates[0]
        result = registry.evaluate(request, candidate)
        no_fee_request = request.model_copy(
            update={
                "backtest": request.backtest.model_copy(
                    update={"maker_fee": Decimal("0"), "taker_fee": Decimal("0")}
                )
            }
        )
        assert no_fee_request.candidates is not None
        no_fee_result = registry.evaluate(
            no_fee_request,
            no_fee_request.candidates[0],
        )
    finally:
        app.shutdown(timeout=1)

    assert result.metrics["closed_trade_count"] > 0
    assert result.score_total != 0
    assert result.score_total != no_fee_result.score_total
