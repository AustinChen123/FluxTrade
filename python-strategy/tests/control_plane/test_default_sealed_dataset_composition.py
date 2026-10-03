from __future__ import annotations

import json
import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.compiler import compiles
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
    EvolutionEpoch,
    GeneRecord,
    Product,
    ResearchCandlestick,
    ResearchDataset,
    Strategy,
)
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from src.core.data_sources.research_database import ResearchDatabaseDataSource

PRODUCT = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "1m"
START = 1_704_067_200_000
DATASET = "default-composition-v1"


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


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
            Strategy.__table__,
            EvolutionEpoch.__table__,
            GeneRecord.__table__,
        ],
    )
    sessions = sessionmaker(bind=engine)
    with sessions() as session:
        session.add(Strategy(id="golden_cross", name="Golden Cross"))
        session.commit()
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


def test_default_app_http_submit_persists_and_returns_native_sealed_result(
    sealed_dataset,
):
    sessions, end = sealed_dataset
    store = InMemoryJobStore()
    app = build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=sessions,
        job_store=store,
        api_key="operator-key",
        readiness_probe=lambda: None,
        profile_query_service=MagicMock(),
    )
    headers = {"Authorization": "Bearer operator-key"}
    try:
        submitted = app.handle(
            "POST",
            "/jobs/parameter-searches",
            body=_request(end).model_dump_json(exclude_none=True),
            headers=headers,
        )
        assert submitted.status_code == 202
        job_id = submitted.body["job"]["id"]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            job = store.get(job_id)
            assert job is not None
            if job.finished_at is not None:
                break
            time.sleep(0.01)

        retrieved = app.handle("GET", f"/jobs/{job_id}", headers=headers)
    finally:
        app.shutdown(timeout=2)

    assert retrieved.status_code == 200
    job = retrieved.body["job"]
    assert job["status"] == "SUCCEEDED"
    result = job["result"]
    evaluation = result["evaluations"][0]
    assert job["request"]["market_data"]["dataset_id"] == DATASET
    assert evaluation["metrics"]["closed_trade_count"] > 0
    assert Decimal(evaluation["score_total"]) != 0

    with sessions() as session:
        epoch = session.query(EvolutionEpoch).one()
        gene = session.query(GeneRecord).one()
    assert epoch.status == "completed"
    assert gene.epoch_id == result["epoch_id"]
    assert Decimal(gene.score_total) == Decimal(evaluation["score_total"])
    assert gene.score_breakdown == evaluation["metrics"]
    assert gene.score_breakdown["closed_trade_count"] > 0
    assert gene.score_breakdown["fill_records"]
    assert gene.score_breakdown["decision_snapshots"]
    assert gene.score_breakdown["provenance"]["dataset_sha256"]
    assert json.loads(json.dumps(gene.score_breakdown))["fill_records"]


@pytest.mark.parametrize(
    ("operation", "failure", "status", "error"),
    [
        ("submit", "rejected", 422, "sealed_dataset_rejected"),
        ("retry", "rejected", 422, "sealed_dataset_rejected"),
        ("submit", "backend", 503, "sealed_dataset_backend_unavailable"),
        ("retry", "backend", 503, "sealed_dataset_backend_unavailable"),
    ],
)
def test_default_app_sanitizes_sealed_source_failures_before_new_job(
    sealed_dataset, monkeypatch, operation, failure, status, error
):
    sessions, end = sealed_dataset
    store = InMemoryJobStore()
    app = build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=sessions,
        job_store=store,
        api_key="operator-key",
        readiness_probe=lambda: None,
        profile_query_service=MagicMock(),
    )
    payload = _request(end).model_dump(mode="json")
    if failure == "rejected":
        payload["market_data"]["dataset_id"] = "not-present"
    else:

        def fail_catalog(*args, **kwargs):
            raise OperationalError(
                "SELECT private catalog", {}, RuntimeError("private catalog detail")
            )

        monkeypatch.setattr(
            ResearchDatabaseDataSource,
            "get_available_range",
            fail_catalog,
        )
    request = ParameterSearchJobRequest.model_validate(payload)
    if operation == "retry":
        failed = store.create(kind="parameter_search", request=request)
        store.mark_failed(failed.id, "prior failure")

    path = (
        "/jobs/parameter-searches"
        if operation == "submit"
        else f"/jobs/{store.list()[0].id}/retry"
    )
    try:
        response = app.handle(
            "POST",
            path,
            body=json.dumps(payload) if operation == "submit" else "{}",
            headers={"Authorization": "Bearer operator-key"},
        )
    finally:
        app.shutdown(timeout=2)

    assert response.status_code == status
    assert response.body == {"error": error}
    assert len(store.list()) == (0 if operation == "submit" else 1)


def test_legacy_parameter_search_retry_value_error_keeps_existing_409(monkeypatch):
    store = InMemoryJobStore()
    app = build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=None,
        job_store=store,
        api_key="operator-key",
        readiness_probe=lambda: None,
        profile_query_service=MagicMock(),
    )
    assert app.parameter_search_executor is not None
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross",
            "product_id": PRODUCT,
            "timeframe": TIMEFRAME,
            "start_time": START,
            "end_time": START + 60_000,
            "backtest": {"candles_csv_path": "legacy.csv"},
            "candidates": [
                {
                    "candidate_id": "candidate",
                    "param_pack": {"short_window": 2, "long_window": 3},
                }
            ],
        }
    )
    job = store.create(kind="parameter_search", request=request)
    store.mark_failed(job.id, "previous failure")
    monkeypatch.setattr(
        app.parameter_search_executor,
        "retry_search",
        MagicMock(side_effect=ValueError("legacy retry failure")),
    )

    try:
        response = app.handle(
            "POST",
            f"/jobs/{job.id}/retry",
            headers={"Authorization": "Bearer operator-key"},
        )
    finally:
        app.shutdown(timeout=1)

    assert response.status_code == 409
    assert response.body == {
        "error": "job_action_rejected",
        "detail": "legacy retry failure",
    }
