import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.control_plane import (
    BacktestJobExecutor,
    ControlPlaneApp,
    CsvSignalBacktestParameterEvaluator,
    GeneControlService,
    GoldenCrossFastFitnessParameterEvaluator,
    GoldenCrossResearchParameterEvaluator,
    InMemoryJobStore,
    ParameterEvaluationResult,
    ParameterSearchJobExecutor,
    ParameterSearchEvaluatorRegistry,
    RedisStrategyCommandRouter,
    SqliteJobStore,
    StrategyControlService,
    StrategyStateQueryService,
    UnsupportedParameterSearchError,
)
from src.control_plane.models import (
    BacktestJobRequest,
    JobStatus,
    ParameterSearchJobRequest,
)
from src.core.product_registry import FeeModel
import src.control_plane.backtest_jobs as backtest_jobs
from src.core.command_router import CommandResult
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    EvolutionEpoch,
    Exchange,
    GeneRecord,
    SystemEvent,
    Product,
    SignalAudit,
    Strategy,
    StrategyState,
    StrategyStateTransition,
)
from src.core.precision import PrecisionCodec, PrecisionSpec

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "15m"


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


def _sqlite_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'control_plane_backtest.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.commit()
    return session_factory


def _sqlite_gene_registry_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'control_plane_gene_registry.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Strategy.__table__,
        SystemEvent.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Strategy(id="searchable", name="Searchable Strategy"))
        session.commit()
    return session_factory


def _sqlite_strategy_state_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'control_plane_strategy_state.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Strategy.__table__,
        StrategyState.__table__,
        StrategyStateTransition.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add_all(
            [
                Strategy(id="s1", name="Strategy 1"),
                Strategy(id="s2", name="Strategy 2"),
            ]
        )
        session.commit()
    return session_factory


def _sqlite_control_plane_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'control_plane_production_wiring.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Strategy.__table__,
        SystemEvent.__table__,
        StrategyState.__table__,
        StrategyStateTransition.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)
    return sessionmaker(bind=engine)


def _write_candles(path):
    rows = [
        (1_700_000_000_000, "50000", "50100", "49900", "50000", "100"),
        (1_700_000_900_000, "50100", "50200", "50000", "50100", "100"),
        (1_700_001_800_000, "50200", "50300", "50100", "50200", "100"),
        (1_700_002_700_000, "50300", "50400", "50200", "50300", "100"),
    ]
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        + "\n".join(",".join(map(str, row)) for row in rows)
        + "\n"
    )
    return rows


def _write_signals(path, timestamps):
    path.write_text(
        "timestamp,type,quantity\n"
        f"{timestamps[0]},LONG,0.01\n"
        f"{timestamps[2]},EXIT_LONG,0.01\n"
    )


def _write_research_candles(path):
    closes = ["100", "100", "100", "110", "120", "90", "80"]
    rows = []
    for index, close in enumerate(closes):
        timestamp = 1_700_000_000_000 + index * 900_000
        rows.append((timestamp, close, close, close, close, "100"))
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        + "\n".join(",".join(map(str, row)) for row in rows)
        + "\n"
    )
    return rows


def test_control_plane_rejects_invalid_backtest_payload():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle("POST", "/jobs/backtests", "{}")

    assert response.status_code == 422
    assert response.body["error"] == "validation_error"


def test_backtest_request_rejects_invalid_instrument_metadata():
    with pytest.raises(ValueError, match="multiplier must be positive"):
        BacktestJobRequest(
            strategy_id="invalid-instrument",
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            candles_csv_path="/tmp/candles.csv",
            signals_csv_path="/tmp/signals.csv",
            start_time=1,
            end_time=2,
            instrument={"multiplier": "0", "fee_model": "per_contract"},
        )


@pytest.mark.parametrize(
    ("instrument", "error"),
    [
        (None, "requires instrument configuration"),
        ({"quantity_step": "1"}, "requires price_tick"),
        ({"price_tick": "0.25"}, "requires quantity_step"),
    ],
)
def test_dated_future_backtest_request_requires_complete_rules(instrument, error):
    with pytest.raises(ValueError, match=error):
        BacktestJobRequest(
            strategy_id="mnq",
            product_id="RITHMIC:MNQ-202509",
            timeframe="1m",
            candles_csv_path="/tmp/candles.csv",
            signals_csv_path="/tmp/signals.csv",
            start_time=1,
            end_time=2,
            instrument=instrument,
        )


def test_dated_future_backtest_request_accepts_complete_rules():
    request = BacktestJobRequest(
        strategy_id="mnq",
        product_id="RITHMIC:MNQ-202509",
        timeframe="1m",
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
        instrument={"quantity_step": "1", "price_tick": "0.25"},
    )

    assert request.instrument.quantity_step == Decimal("1")
    assert request.instrument.price_tick == Decimal("0.25")

    with pytest.raises(ValueError, match="capital_per_contract must be positive"):
        BacktestJobRequest(
            strategy_id="invalid-capital",
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            candles_csv_path="/tmp/candles.csv",
            signals_csv_path="/tmp/signals.csv",
            start_time=1,
            end_time=2,
            instrument={
                "multiplier": "2",
                "capital_model": "per_contract",
            },
        )


def test_backtest_executor_propagates_instrument_spec(monkeypatch, tmp_path):
    captured = {}

    class RecordingRunner:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def add_strategy(self, strategy):
            pass

        def run(self):
            return {"total_pnl": Decimal("0")}

    monkeypatch.setattr(backtest_jobs, "BacktestRunner", RecordingRunner)
    signals_path = tmp_path / "signals.csv"
    signals_path.write_text("timestamp,type,quantity\n1,LONG,1\n")
    request = BacktestJobRequest(
        strategy_id="mnq",
        product_id=PRODUCT_ID,
        timeframe="1m",
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path=str(signals_path),
        start_time=1,
        end_time=2,
        instrument={
            "multiplier": "2",
            "quantity_step": "1",
            "price_tick": "0.25",
            "fee_model": "per_contract",
            "capital_model": "per_contract",
            "capital_per_contract": "2500",
        },
    )

    BacktestJobExecutor(run_inline=True).run_backtest_request(request)

    spec = captured["instrument_spec"]
    assert captured["max_drawdown_limit"] == 0.20
    assert spec.multiplier == Decimal("2")
    assert spec.fee_model == FeeModel.PER_CONTRACT
    assert spec.quantity_step == Decimal("1")
    assert spec.price_tick == Decimal("0.25")
    assert spec.capital_model.value == "per_contract"
    assert spec.capital_per_contract == Decimal("2500")


def test_control_plane_lists_submitted_jobs_without_framework():
    store = InMemoryJobStore()
    executor = BacktestJobExecutor(store=store, run_inline=False)
    request = BacktestJobRequest(
        strategy_id="queued",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    job = store.create(kind=request.kind, request=request)
    app = ControlPlaneApp(executor)

    list_response = app.handle("GET", "/jobs")
    get_response = app.handle("GET", f"/jobs/{job.id}")

    executor.shutdown(wait=False)
    assert list_response.status_code == 200
    assert list_response.body["jobs"][0]["id"] == job.id
    assert get_response.status_code == 200
    assert get_response.body["job"]["status"] == JobStatus.QUEUED.value


def test_control_plane_paginates_jobs_without_framework():
    store = InMemoryJobStore()
    executor = BacktestJobExecutor(store=store, run_inline=False)
    for index in range(3):
        request = BacktestJobRequest(
            strategy_id=f"queued-{index}",
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            candles_csv_path="/tmp/candles.csv",
            signals_csv_path="/tmp/signals.csv",
            start_time=1,
            end_time=2,
        )
        store.create(kind=request.kind, request=request)
    app = ControlPlaneApp(executor)

    response = app.handle("GET", "/jobs?limit=2&offset=1")

    executor.shutdown(wait=False)
    assert response.status_code == 200
    assert len(response.body["jobs"]) == 2
    assert response.body["total"] == 3
    assert response.body["limit"] == 2
    assert response.body["offset"] == 1


def test_control_plane_rejects_invalid_pagination():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle("GET", "/jobs?limit=0")

    assert response.status_code == 422
    assert response.body == {"error": "validation_error"}


def test_control_plane_api_key_auth_allows_health_without_credentials():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True), api_key="secret")

    response = app.handle("GET", "/health")

    assert response.status_code == 200
    assert response.body == {"status": "ok"}


def test_control_plane_reports_browser_session_endpoint_unavailable():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle("GET", "/api/v1/auth/session")

    assert response.status_code == 404
    assert response.body == {"error": "not_found"}


def test_control_plane_api_key_auth_rejects_missing_credentials():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True), api_key="secret")

    response = app.handle("GET", "/jobs")

    assert response.status_code == 401
    assert response.body == {"error": "unauthorized"}


def test_control_plane_api_key_auth_accepts_bearer_token():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True), api_key="secret")

    response = app.handle(
        "GET",
        "/jobs",
        headers={"Authorization": "Bearer secret"},
    )

    assert response.status_code == 200
    assert response.body["jobs"] == []


def test_control_plane_api_key_auth_accepts_x_api_key_case_insensitive():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True), api_key="secret")

    response = app.handle("GET", "/jobs", headers={"x-api-key": "secret"})

    assert response.status_code == 200
    assert response.body["jobs"] == []


def test_control_plane_rejects_empty_api_key_config():
    with pytest.raises(ValueError, match="api_key must be non-empty"):
        ControlPlaneApp(BacktestJobExecutor(run_inline=True), api_key="")


def test_sqlite_job_store_persists_job_state_across_instances(tmp_path):
    db_path = tmp_path / "jobs.db"
    request = BacktestJobRequest(
        strategy_id="durable",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    first_store = SqliteJobStore(db_path)

    created = first_store.create(kind=request.kind, request=request)
    first_store.mark_running(created.id)
    first_store.mark_succeeded(created.id, {"total_trades": 1, "total_pnl": "10.5"})
    second_store = SqliteJobStore(db_path)

    restored = second_store.get(created.id)
    listed = second_store.list()

    assert restored is not None
    assert restored.status == JobStatus.SUCCEEDED
    assert restored.result == {"total_trades": 1, "total_pnl": "10.5"}
    assert restored.started_at is not None
    assert restored.finished_at is not None
    assert listed[0].id == created.id


def test_backtest_executor_marks_persisted_active_jobs_interrupted_on_startup(tmp_path):
    db_path = tmp_path / "jobs.db"
    store = SqliteJobStore(db_path)
    request = BacktestJobRequest(
        strategy_id="recover",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    queued = store.create(kind=request.kind, request=request)
    running = store.create(kind=request.kind, request=request)
    succeeded = store.create(kind=request.kind, request=request)
    store.mark_running(running.id)
    store.mark_succeeded(succeeded.id, {"total_trades": 0})

    BacktestJobExecutor(
        store=SqliteJobStore(db_path),
        run_inline=True,
        recover_interrupted=True,
    )
    restored = SqliteJobStore(db_path)

    assert restored.get(queued.id).status == JobStatus.FAILED
    assert restored.get(running.id).status == JobStatus.FAILED
    assert restored.get(queued.id).error == "Job interrupted before control plane startup"
    assert restored.get(succeeded.id).status == JobStatus.SUCCEEDED


def test_control_plane_cancels_queued_backtest_job():
    store = InMemoryJobStore()
    executor = BacktestJobExecutor(store=store, run_inline=False)
    request = BacktestJobRequest(
        strategy_id="queued",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    job = store.create(kind=request.kind, request=request)
    app = ControlPlaneApp(executor)

    response = app.handle(
        "POST",
        f"/jobs/{job.id}/cancel",
        json.dumps({"reason": "no longer needed"}),
    )

    executor.shutdown(wait=False)
    assert response.status_code == 200
    assert response.body["job"]["status"] == JobStatus.CANCELLED.value
    assert response.body["job"]["error"] == "no longer needed"


def test_control_plane_rejects_running_job_cancellation():
    store = InMemoryJobStore()
    executor = BacktestJobExecutor(store=store, run_inline=False)
    request = BacktestJobRequest(
        strategy_id="running",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    job = store.create(kind=request.kind, request=request)
    store.mark_running(job.id)
    app = ControlPlaneApp(executor)

    response = app.handle("POST", f"/jobs/{job.id}/cancel")

    executor.shutdown(wait=False)
    assert response.status_code == 409
    assert response.body["error"] == "job_action_rejected"
    assert store.get(job.id).status == JobStatus.RUNNING


def test_backtest_executor_retries_cancelled_jobs():
    store = InMemoryJobStore()
    executor = BacktestJobExecutor(store=store, run_inline=False)
    request = BacktestJobRequest(
        strategy_id="retryable",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    original = store.create(kind=request.kind, request=request)
    store.mark_cancelled(original.id, "cancelled for retry")

    retry = executor.retry_backtest(original.id)

    executor.shutdown(wait=True)
    assert retry.id != original.id
    assert retry.status == JobStatus.QUEUED
    assert retry.request == original.request


class _FakeParameterEvaluator:
    def __init__(self) -> None:
        self.evaluated_candidate_ids = []

    def evaluate(self, request, candidate):
        self.evaluated_candidate_ids.append(candidate.candidate_id)
        score = Decimal(str(candidate.param_pack["score"]))
        drawdown = Decimal(str(candidate.param_pack.get("drawdown", "0")))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=drawdown,
            metrics={"seed": request.seed, "score": str(score)},
        )


class _WindowParameterEvaluator:
    def __init__(self) -> None:
        self.evaluated_candidate_ids = []

    def evaluate(self, request, candidate):
        self.evaluated_candidate_ids.append(candidate.candidate_id)
        score = Decimal(str(candidate.param_pack["short_window"]))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            metrics={"seed": request.seed, "score": str(score)},
        )


def test_control_plane_runs_parameter_search_job_with_injected_evaluator():
    store = InMemoryJobStore()
    evaluator = _FakeParameterEvaluator()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            evaluator,
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "searchable",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "seed": 7,
                "candidates": [
                    {"candidate_id": "a", "param_pack": {"score": "1.2"}},
                    {"candidate_id": "b", "param_pack": {"score": "2.5"}},
                ],
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["kind"] == "parameter_search"
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["best_candidate"]["candidate_id"] == "b"
    assert job["result"]["best_candidate"]["score_total"] == "2.5"
    assert evaluator.evaluated_candidate_ids == ["a", "b"]


def test_control_plane_rejects_duplicate_parameter_candidates():
    store = InMemoryJobStore()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            _FakeParameterEvaluator(),
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "searchable",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "candidates": [
                    {"candidate_id": "same", "param_pack": {"score": "1"}},
                    {"candidate_id": "same", "param_pack": {"score": "2"}},
                ],
            }
        ),
    )

    assert response.status_code == 422
    assert response.body["error"] == "validation_error"


def test_control_plane_reports_unavailable_parameter_search():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "searchable",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "candidates": [{"candidate_id": "a", "param_pack": {"score": "1"}}],
            }
        ),
    )

    assert response.status_code == 503
    assert response.body == {"error": "parameter_search_unavailable"}


def test_control_plane_runs_golden_cross_parameter_search_preset():
    store = InMemoryJobStore()
    evaluator = _WindowParameterEvaluator()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            evaluator,
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-search-presets/golden-cross",
        json.dumps(
            {
                "strategy_id": "golden_cross_easy",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "short_window": {"min": 5, "max": 10, "step": 5},
                "long_window": {"min": 20, "max": 20, "step": 5},
                "quantity": {"min": "0.01", "max": "0.01", "step": "0.01"},
                "candidate_sample_count": 2,
                "seed": 7,
                "backtest": {
                    "candles_csv_path": "data/BTCUSDT_15m.csv",
                    "initial_balance": "10000",
                    "maker_fee": "0",
                    "taker_fee": "0",
                },
                "research_runner": {
                    "capital_allocation": "1000",
                },
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["kind"] == "parameter_search"
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["strategy_id"] == "golden_cross_easy"
    assert job["result"]["seed"] == 7
    assert job["result"]["research_runner"] == {"capital_allocation": "1000"}
    assert job["result"]["resolved_candidates"] == [
        {
            "candidate_id": "generated_000001",
            "param_pack": {
                "short_window": 5,
                "long_window": 20,
                "quantity": "0.01",
            },
        },
        {
            "candidate_id": "generated_000002",
            "param_pack": {
                "short_window": 10,
                "long_window": 20,
                "quantity": "0.01",
            },
        },
    ]
    assert evaluator.evaluated_candidate_ids == [
        "generated_000001",
        "generated_000002",
    ]


def test_control_plane_reports_unavailable_parameter_search_preset():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle(
        "POST",
        "/jobs/parameter-search-presets/golden-cross",
        json.dumps(
            {
                "strategy_id": "golden_cross_easy",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "short_window": {"min": 5, "max": 10, "step": 5},
                "long_window": {"min": 20, "max": 20, "step": 5},
                "candidate_sample_count": 2,
            }
        ),
    )

    assert response.status_code == 503
    assert response.body == {"error": "parameter_search_unavailable"}


def test_control_plane_rejects_invalid_golden_cross_parameter_search_preset():
    store = InMemoryJobStore()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            _FakeParameterEvaluator(),
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-search-presets/golden-cross",
        json.dumps(
            {
                "strategy_id": "golden_cross_easy",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "short_window": {"min": 5, "max": 20, "step": 5},
                "long_window": {"min": 20, "max": 40, "step": 5},
                "candidate_sample_count": 2,
            }
        ),
    )

    assert response.status_code == 422
    assert response.body["error"] == "validation_error"


def test_parameter_search_records_evolution_epoch_and_gene_candidates(tmp_path):
    store = InMemoryJobStore()
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            _FakeParameterEvaluator(),
            store=store,
            run_inline=True,
            db_session_factory=session_factory,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "searchable",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1_700_000_000_000,
                "end_time": 1_700_001_800_000,
                "seed": 11,
                "candidates": [
                    {
                        "candidate_id": "a",
                        "param_pack": {"score": "1.2", "drawdown": "-0.40"},
                    },
                    {
                        "candidate_id": "b",
                        "param_pack": {"score": "2.5", "drawdown": "-0.10"},
                    },
                ],
            }
        ),
    )

    assert response.status_code == 200
    epoch_id = response.body["job"]["result"]["epoch_id"]
    assert epoch_id.startswith("epoch_")

    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, epoch_id)
        genes = (
            session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == epoch_id)
            .order_by(GeneRecord.score_total)
            .all()
        )

    assert epoch.strategy_id == "searchable"
    assert epoch.status == "completed"
    assert epoch.pop_size == 2
    assert epoch.best_score == Decimal("2.50000000")
    assert [gene.param_pack for gene in genes] == [
        {"score": "1.2", "drawdown": "-0.40"},
        {"score": "2.5", "drawdown": "-0.10"},
    ]
    assert [gene.max_drawdown for gene in genes] == [
        Decimal("0.40000000"),
        Decimal("0.10000000"),
    ]
    assert [gene.role for gene in genes] == ["challenger", "challenger"]


def test_control_plane_promotes_gene_and_retires_previous_champion(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    with session_factory() as session:
        epoch = EvolutionEpoch(
            id="epoch-promote",
            strategy_id="searchable",
            started_at=datetime(2026, 5, 20, tzinfo=UTC),
            pop_size=2,
            max_generations=1,
            generations_run=1,
            best_score=Decimal("2.5"),
            seed=1,
            config_json={},
            status="completed",
            eval_pair=PRODUCT_ID,
            eval_start_date=date(2026, 5, 20),
            eval_end_date=date(2026, 5, 20),
            eval_timeframe=TIMEFRAME,
        )
        champion = GeneRecord(
            strategy_id="searchable",
            role="champion",
            param_pack={"score": "1.2"},
            score_total=Decimal("1.2"),
            score_breakdown={},
            max_drawdown=Decimal("0.1"),
            generation_index=0,
            candidate_id="champion",
            epoch_id="epoch-promote",
        )
        challenger = GeneRecord(
            strategy_id="searchable",
            role="challenger",
            param_pack={"score": "2.5"},
            score_total=Decimal("2.5"),
            score_breakdown={},
            max_drawdown=Decimal("0.05"),
            generation_index=0,
            candidate_id="challenger",
            epoch_id="epoch-promote",
        )
        session.add(epoch)
        session.add_all([champion, challenger])
        session.commit()
        champion_id = champion.id
        challenger_id = challenger.id

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        gene_control=GeneControlService(session_factory),
    )

    response = app.handle(
        "POST",
        f"/genes/{challenger_id}/promote",
        json.dumps({"reason": "best search score", "actor": "operator"}),
    )

    assert response.status_code == 200
    assert response.body["gene"]["gene_id"] == challenger_id
    assert response.body["gene"]["role"] == "champion"
    assert response.body["gene"]["retired_gene_ids"] == [champion_id]

    with session_factory() as session:
        old_champion = session.get(GeneRecord, champion_id)
        new_champion = session.get(GeneRecord, challenger_id)
        events = session.query(SystemEvent).order_by(SystemEvent.id).all()

    assert old_champion.role == "retired"
    assert old_champion.retired_at is not None
    assert new_champion.role == "champion"
    assert new_champion.activated_at is not None
    assert [event.event_type for event in events] == ["gene_retire", "gene_promote"]
    assert events[-1].payload["reason"] == "best search score"


def test_control_plane_lists_and_gets_genes(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    with session_factory() as session:
        epoch = EvolutionEpoch(
            id="epoch-query",
            strategy_id="searchable",
            started_at=datetime(2026, 5, 20, tzinfo=UTC),
            pop_size=2,
            max_generations=2,
            generations_run=2,
            best_score=Decimal("2.5"),
            seed=1,
            config_json={},
            status="completed",
            eval_pair=PRODUCT_ID,
            eval_start_date=date(2026, 5, 20),
            eval_end_date=date(2026, 5, 20),
            eval_timeframe=TIMEFRAME,
        )
        champion = GeneRecord(
            strategy_id="searchable",
            role="champion",
            param_pack={"score": "2.5"},
            score_total=Decimal("2.5"),
            score_breakdown={"total_pnl": "2.5"},
            max_drawdown=Decimal("0.05"),
            generation_index=0,
            candidate_id="champion",
            epoch_id="epoch-query",
        )
        challenger = GeneRecord(
            strategy_id="searchable",
            role="challenger",
            param_pack={"score": "1.2"},
            score_total=Decimal("1.2"),
            score_breakdown={"total_pnl": "1.2"},
            max_drawdown=Decimal("0.10"),
            generation_index=0,
            candidate_id="challenger",
            epoch_id="epoch-query",
        )
        next_generation = GeneRecord(
            strategy_id="searchable",
            role="challenger",
            param_pack={"score": "3.1"},
            score_total=Decimal("3.1"),
            score_breakdown={"total_pnl": "3.1"},
            max_drawdown=Decimal("0.04"),
            generation_index=1,
            candidate_id="next-generation",
            epoch_id="epoch-query",
        )
        session.add(epoch)
        session.add_all([champion, challenger, next_generation])
        session.commit()
        champion_id = champion.id
        next_generation_id = next_generation.id

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        gene_control=GeneControlService(session_factory),
    )

    list_response = app.handle(
        "GET",
        "/genes?strategy_id=searchable&role=champion&limit=1&offset=0",
    )
    get_response = app.handle("GET", f"/genes/{champion_id}")
    generation_response = app.handle(
        "GET",
        "/genes?epoch_id=epoch-query&generation_index=1&limit=10000&offset=0",
    )
    summary_response = app.handle(
        "GET",
        "/evolution-epochs/epoch-query/generations",
    )

    assert list_response.status_code == 200
    assert [gene["id"] for gene in list_response.body["genes"]] == [champion_id]
    assert list_response.body["total"] == 1
    assert list_response.body["limit"] == 1
    assert list_response.body["offset"] == 0
    assert get_response.status_code == 200
    assert get_response.body["gene"]["id"] == champion_id
    assert get_response.body["gene"]["score_total"] == "2.50000000"
    assert get_response.body["gene"]["param_pack"] == {"score": "2.5"}
    assert [gene["id"] for gene in generation_response.body["genes"]] == [
        next_generation_id
    ]
    assert generation_response.body["total"] == 1
    assert generation_response.body["limit"] == 10_000
    assert summary_response.status_code == 200
    assert summary_response.body["generations"] == [
        {
            "generation_index": 0,
            "candidate_count": 2,
            "score_min": "1.20000000",
            "score_max": "2.50000000",
            "drawdown_min": "0.05000000",
            "drawdown_max": "0.10000000",
        },
        {
            "generation_index": 1,
            "candidate_count": 1,
            "score_min": "3.10000000",
            "score_max": "3.10000000",
            "drawdown_min": "0.04000000",
            "drawdown_max": "0.04000000",
        },
    ]


def test_control_plane_rejects_invalid_gene_generation_and_missing_epoch_summary(
    tmp_path,
):
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        gene_control=GeneControlService(
            _sqlite_gene_registry_session_factory(tmp_path)
        ),
    )

    invalid_generation = app.handle(
        "GET",
        "/genes?generation_index=-1",
    )
    missing_epoch = app.handle(
        "GET",
        "/evolution-epochs/missing/generations",
    )
    oversized_page = app.handle("GET", "/genes?limit=10001")

    assert invalid_generation.status_code == 422
    assert oversized_page.status_code == 422
    assert missing_epoch.status_code == 404
    assert missing_epoch.body == {"error": "epoch_not_found"}


def test_control_plane_lists_and_gets_evolution_epochs(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    with session_factory() as session:
        session.add(
            EvolutionEpoch(
                id="epoch-query",
                strategy_id="searchable",
                started_at=datetime(2026, 5, 20, tzinfo=UTC),
                finished_at=datetime(2026, 5, 20, 1, tzinfo=UTC),
                pop_size=2,
                max_generations=1,
                generations_run=1,
                best_score=Decimal("2.5"),
                seed=1,
                config_json={"objective": "maximize_score"},
                status="completed",
                eval_pair=PRODUCT_ID,
                eval_start_date=date(2026, 5, 20),
                eval_end_date=date(2026, 5, 20),
                eval_timeframe=TIMEFRAME,
            )
        )
        session.commit()

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        gene_control=GeneControlService(session_factory),
    )

    list_response = app.handle(
        "GET",
        "/evolution-epochs?strategy_id=searchable&limit=5&offset=0",
    )
    get_response = app.handle("GET", "/evolution-epochs/epoch-query")

    assert list_response.status_code == 200
    assert [epoch["id"] for epoch in list_response.body["epochs"]] == ["epoch-query"]
    assert list_response.body["total"] == 1
    assert get_response.status_code == 200
    assert get_response.body["epoch"]["best_score"] == "2.50000000"
    assert get_response.body["epoch"]["config_json"] == {"objective": "maximize_score"}
    assert get_response.body["epoch"]["eval_start_date"] == "2026-05-20"


def test_control_plane_lists_and_gets_system_events(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    with session_factory() as session:
        event = SystemEvent(
            event_type="gene_promote",
            related_strategy_id="searchable",
            related_gene_id=7,
            payload={"reason": "best search score"},
            created_at=datetime(2026, 5, 20, tzinfo=UTC),
        )
        other = SystemEvent(
            event_type="system_error",
            related_strategy_id="searchable",
            payload={"message": "ignored by filter"},
            created_at=datetime(2026, 5, 21, tzinfo=UTC),
        )
        session.add_all([event, other])
        session.commit()
        event_id = event.id

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        gene_control=GeneControlService(session_factory),
    )

    list_response = app.handle(
        "GET",
        "/system-events?event_type=gene_promote&strategy_id=searchable&related_gene_id=7&limit=1&offset=0",
    )
    get_response = app.handle("GET", f"/system-events/{event_id}")

    assert list_response.status_code == 200
    assert [event["id"] for event in list_response.body["events"]] == [event_id]
    assert list_response.body["total"] == 1
    assert get_response.status_code == 200
    assert get_response.body["event"]["event_type"] == "gene_promote"
    assert get_response.body["event"]["payload"] == {"reason": "best search score"}


def test_control_plane_rejects_invalid_system_event_gene_filter(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        gene_control=GeneControlService(session_factory),
    )

    response = app.handle("GET", "/system-events?related_gene_id=bad")

    assert response.status_code == 422
    assert response.body == {"error": "validation_error"}


def test_control_plane_lists_and_gets_strategy_states(tmp_path):
    session_factory = _sqlite_strategy_state_session_factory(tmp_path)
    with session_factory() as session:
        session.add_all(
            [
                StrategyState(
                    strategy_id="s1",
                    status="ACTIVE",
                    config_json='{"risk":"low"}',
                    performance_json='{"pnl":"12.3"}',
                    last_heartbeat=1_700_000_000_000,
                    uptime_start=1_699_999_000_000,
                    version=2,
                ),
                StrategyState(
                    strategy_id="s2",
                    status="STOPPED",
                    config_json="{}",
                    performance_json="{}",
                    stopped_at=datetime(2026, 5, 20, tzinfo=UTC),
                    version=1,
                ),
            ]
        )
        session.commit()

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        strategy_state_query=StrategyStateQueryService(session_factory),
    )

    list_response = app.handle("GET", "/strategy-states?status=ACTIVE&limit=5&offset=0")
    get_response = app.handle("GET", "/strategy-states/s1")

    assert list_response.status_code == 200
    assert list_response.body["total"] == 1
    assert [state["strategy_id"] for state in list_response.body["states"]] == ["s1"]
    assert get_response.status_code == 200
    assert get_response.body["state"]["status"] == "ACTIVE"
    assert get_response.body["state"]["config"] == {"risk": "low"}
    assert get_response.body["state"]["performance"] == {"pnl": "12.3"}
    assert get_response.body["state"]["version"] == 2


def test_control_plane_summarizes_strategy_states(tmp_path):
    session_factory = _sqlite_strategy_state_session_factory(tmp_path)
    with session_factory() as session:
        session.add_all(
            [
                StrategyState(
                    strategy_id="s1",
                    status="ACTIVE",
                    config_json="{}",
                    performance_json="{}",
                    last_heartbeat=1,
                    uptime_start=1,
                    version=1,
                ),
                StrategyState(
                    strategy_id="s2",
                    status="STOPPED",
                    config_json="{}",
                    performance_json="{}",
                    stopped_at=datetime(2026, 5, 20, tzinfo=UTC),
                    version=1,
                ),
            ]
        )
        session.commit()

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        strategy_state_query=StrategyStateQueryService(session_factory),
    )

    response = app.handle("GET", "/strategy-states/summary?stale_after_ms=1000")

    assert response.status_code == 200
    assert response.body["summary"]["total"] == 2
    assert response.body["summary"]["by_status"] == {"ACTIVE": 1, "STOPPED": 1}
    assert response.body["summary"]["stale_heartbeat_count"] == 1
    assert response.body["summary"]["stale_after_ms"] == 1000
    assert isinstance(response.body["summary"]["observed_at_ms"], int)


def test_control_plane_rejects_invalid_strategy_state_summary_threshold(tmp_path):
    session_factory = _sqlite_strategy_state_session_factory(tmp_path)
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        strategy_state_query=StrategyStateQueryService(session_factory),
    )

    response = app.handle("GET", "/strategy-states/summary?stale_after_ms=-1")

    assert response.status_code == 422
    assert response.body == {"error": "validation_error"}


def test_control_plane_lists_strategy_state_transitions(tmp_path):
    session_factory = _sqlite_strategy_state_session_factory(tmp_path)
    with session_factory() as session:
        session.add(
            StrategyState(
                strategy_id="s1",
                status="STOPPED",
                config_json="{}",
                performance_json="{}",
                stopped_at=datetime(2026, 5, 20, tzinfo=UTC),
                version=2,
            )
        )
        session.add_all(
            [
                StrategyStateTransition(
                    strategy_id="s1",
                    from_status="ACTIVE",
                    to_status="ERROR",
                    transitioned_at=datetime(2026, 5, 20, 1, tzinfo=UTC),
                    reason="risk breach",
                    actor="system",
                ),
                StrategyStateTransition(
                    strategy_id="s1",
                    from_status="ERROR",
                    to_status="STOPPED",
                    transitioned_at=datetime(2026, 5, 20, 2, tzinfo=UTC),
                    reason="operator stop",
                    actor="operator",
                ),
            ]
        )
        session.commit()

    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        strategy_state_query=StrategyStateQueryService(session_factory),
    )

    response = app.handle("GET", "/strategy-states/s1/transitions?limit=1&offset=0")

    assert response.status_code == 200
    assert response.body["total"] == 2
    assert response.body["limit"] == 1
    assert len(response.body["transitions"]) == 1
    assert response.body["transitions"][0]["to_status"] == "STOPPED"
    assert response.body["transitions"][0]["reason"] == "operator stop"


def test_control_plane_reports_unavailable_strategy_state_query():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle("GET", "/strategy-states")

    assert response.status_code == 503
    assert response.body == {"error": "strategy_state_query_unavailable"}


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_control_plane_runs_parameter_search_with_csv_signal_backtests(tmp_path):
    session_factory = _sqlite_session_factory(tmp_path)
    candle_rows = _write_candles(tmp_path / "candles.csv")
    conservative_signals = tmp_path / "conservative_signals.csv"
    aggressive_signals = tmp_path / "aggressive_signals.csv"
    conservative_signals.write_text(
        "timestamp,type,quantity\n"
        f"{candle_rows[1][0]},LONG,0.01\n"
        f"{candle_rows[2][0]},EXIT_LONG,0.01\n"
    )
    aggressive_signals.write_text(
        "timestamp,type,quantity\n"
        f"{candle_rows[0][0]},LONG,0.01\n"
        f"{candle_rows[2][0]},EXIT_LONG,0.01\n"
    )
    store = InMemoryJobStore()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            CsvSignalBacktestParameterEvaluator(db_session_factory=session_factory),
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "csv_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": candle_rows[0][0],
                "end_time": candle_rows[-1][0],
                "backtest": {
                    "candles_csv_path": str(tmp_path / "candles.csv"),
                    "initial_balance": "10000",
                    "maker_fee": "0",
                    "taker_fee": "0",
                },
                "candidates": [
                    {
                        "candidate_id": "conservative",
                        "param_pack": {"signals_csv_path": str(conservative_signals)},
                    },
                    {
                        "candidate_id": "aggressive",
                        "param_pack": {"signals_csv_path": str(aggressive_signals)},
                    },
                ],
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["best_candidate"]["candidate_id"] == "aggressive"
    assert Decimal(job["result"]["best_candidate"]["score_total"]) > Decimal("0")


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_csv_signal_walk_forward_fitness_has_required_metrics(tmp_path):
    session_factory = _sqlite_session_factory(tmp_path)
    candle_rows = _write_candles(tmp_path / "fitness_candles.csv")
    signals = tmp_path / "fitness_signals.csv"
    _write_signals(signals, [row[0] for row in candle_rows])
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "csv_fitness",
            "product_id": PRODUCT_ID,
            "timeframe": TIMEFRAME,
            "start_time": candle_rows[0][0],
            "end_time": candle_rows[-1][0],
            "backtest": {
                "candles_csv_path": str(tmp_path / "fitness_candles.csv"),
                "initial_balance": "10000",
            },
            "candidates": [
                {
                    "candidate_id": "baseline",
                    "param_pack": {"signals_csv_path": str(signals)},
                }
            ],
            "evaluation_set": {
                "datasets": [
                    {
                        "dataset_id": "first",
                        "product_id": PRODUCT_ID,
                        "timeframe": TIMEFRAME,
                        "start_time": candle_rows[0][0],
                        "end_time": candle_rows[1][0],
                    },
                    {
                        "dataset_id": "second",
                        "product_id": PRODUCT_ID,
                        "timeframe": TIMEFRAME,
                        "start_time": candle_rows[2][0],
                        "end_time": candle_rows[3][0],
                    },
                ]
            },
            "fitness": {},
        }
    )

    job = ParameterSearchJobExecutor(
        CsvSignalBacktestParameterEvaluator(
            db_session_factory=session_factory,
        ),
        run_inline=True,
    ).submit_search(request)

    assert job.status == JobStatus.SUCCEEDED
    assert (
        job.result["evaluations"][0]["metrics"]["fitness"]["metric_contract"][
            "version"
        ]
        == "walk_forward_fitness_v2"
    )


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_control_plane_runs_parameter_search_with_research_backtests(tmp_path):
    candle_rows = _write_research_candles(tmp_path / "research_candles.csv")
    store = InMemoryJobStore()
    evaluator = GoldenCrossResearchParameterEvaluator()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            evaluator,
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "golden_cross_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": candle_rows[0][0],
                "end_time": candle_rows[-1][0],
                "backtest": {
                    "candles_csv_path": str(tmp_path / "research_candles.csv"),
                    "initial_balance": "10000",
                    "maker_fee": "0",
                    "taker_fee": "0",
                },
                "candidates": [
                    {
                        "candidate_id": "active",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 3,
                            "quantity": "0.01",
                        },
                    },
                    {
                        "candidate_id": "slow",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 6,
                            "quantity": "0.01",
                        },
                    },
                ],
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["best_candidate"]["candidate_id"] == "slow"
    assert Decimal(job["result"]["best_candidate"]["score_total"]) == Decimal("0")
    evaluations = job["result"]["evaluations"]
    assert evaluations[0]["metrics"]["raw_trade_count"] == 2
    assert "raw_trades" not in evaluations[0]["metrics"]
    assert "closed_trades" not in evaluations[0]["metrics"]
    assert len(evaluator._candle_cache) == 1


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_control_plane_runs_research_parameter_search_with_capital_allocation(
    tmp_path,
):
    candle_rows = _write_research_candles(tmp_path / "capital_research_candles.csv")
    store = InMemoryJobStore()
    evaluator = GoldenCrossResearchParameterEvaluator()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            evaluator,
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "capital_golden_cross_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": candle_rows[0][0],
                "end_time": candle_rows[-1][0],
                "backtest": {
                    "candles_csv_path": str(tmp_path / "capital_research_candles.csv"),
                    "initial_balance": "10000",
                    "maker_fee": "0",
                    "taker_fee": "0",
                },
                "research_runner": {
                    "capital_allocation": "0.5",
                },
                "candidates": [
                    {
                        "candidate_id": "active",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 3,
                            "quantity": "0.01",
                        },
                    },
                ],
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["research_runner"] == {"capital_allocation": "0.5"}
    assert job["result"]["evaluations"][0]["metrics"]["raw_trade_count"] == 0
    assert len(evaluator._candle_cache) == 1


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_research_parameter_search_reuses_prepared_scaled_candles(tmp_path):
    candle_rows = _write_research_candles(tmp_path / "research_scaled_candles.csv")
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.001"),
        )
    )
    evaluator = GoldenCrossResearchParameterEvaluator(precision_codec=codec)
    store = InMemoryJobStore()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            evaluator,
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "golden_cross_scaled_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": candle_rows[0][0],
                "end_time": candle_rows[-1][0],
                "backtest": {
                    "candles_csv_path": str(tmp_path / "research_scaled_candles.csv"),
                    "initial_balance": "10000",
                    "maker_fee": "0",
                    "taker_fee": "0",
                },
                "candidates": [
                    {
                        "candidate_id": "active",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 3,
                            "quantity": "0.01",
                        },
                    },
                    {
                        "candidate_id": "slow",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 6,
                            "quantity": "0.01",
                        },
                    },
                ],
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["best_candidate"]["candidate_id"] == "slow"
    assert Decimal(job["result"]["best_candidate"]["score_total"]) == Decimal("0")
    assert len(evaluator._candle_cache) == 1
    assert len(evaluator._prepared_scaled_cache) == 1
    prepared = next(iter(evaluator._prepared_scaled_cache.values()))
    assert len(prepared) == len(candle_rows)


def test_control_plane_runs_parameter_search_with_fast_fitness(tmp_path):
    candle_rows = _write_research_candles(tmp_path / "fast_fitness_candles.csv")
    evaluator = GoldenCrossFastFitnessParameterEvaluator()
    store = InMemoryJobStore()
    app = ControlPlaneApp(
        BacktestJobExecutor(store=store, run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            evaluator,
            store=store,
            run_inline=True,
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_id": "golden_cross_fast_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": candle_rows[0][0],
                "end_time": candle_rows[-1][0],
                "backtest": {
                    "candles_csv_path": str(tmp_path / "fast_fitness_candles.csv"),
                    "initial_balance": "10000",
                    "maker_fee": "0",
                    "taker_fee": "0",
                },
                "candidates": [
                    {
                        "candidate_id": "active",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 3,
                            "quantity": "0.01",
                        },
                    },
                    {
                        "candidate_id": "slow",
                        "param_pack": {
                            "short_window": 1,
                            "long_window": 6,
                            "quantity": "0.01",
                        },
                    },
                ],
            }
        ),
    )

    assert response.status_code == 200
    job = response.body["job"]
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["best_candidate"]["candidate_id"] == "slow"
    evaluations = job["result"]["evaluations"]
    assert evaluations[0]["metrics"]["fitness_mode"] == "golden_cross_fast"
    assert evaluations[0]["metrics"]["raw_trade_count"] == 2
    assert len(evaluator._fitness_cache) == 1


class _FakeCommandRouter:
    def __init__(self) -> None:
        self.messages = []

    def handle(self, message):
        self.messages.append(message)
        command = message["command"]
        if command == "LIST":
            return CommandResult(
                True,
                "Listed active strategies",
                {"strategies": [{"strategy_id": "s1"}]},
            )
        if command == "HEALTH_CHECK":
            return CommandResult(True, "Health check complete", {"healthy": {"s1": True}})
        if command == "STOP":
            return CommandResult(True, "Stopped strategy s1")
        return CommandResult(False, f"Unknown command: {command}")


def test_control_plane_routes_strategy_status_and_commands():
    router = _FakeCommandRouter()
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        strategy_control=StrategyControlService(router),
    )

    list_response = app.handle("GET", "/strategies")
    health_response = app.handle("GET", "/strategies/health")
    command_response = app.handle(
        "POST",
        "/strategies/s1/commands",
        json.dumps({"command": "STOP", "reason": "operator pause"}),
    )

    assert list_response.status_code == 200
    assert list_response.body["result"]["data"]["strategies"] == [{"strategy_id": "s1"}]
    assert health_response.status_code == 200
    assert health_response.body["result"]["data"]["healthy"] == {"s1": True}
    assert command_response.status_code == 200
    assert router.messages[-1] == {
        "command": "STOP",
        "strategy_id": "s1",
        "params": {
            "strategy_id": "s1",
            "actor": "operator",
            "reason": "operator pause",
        },
    }


def test_redis_strategy_router_queries_state_and_publishes_commands(tmp_path):
    from unittest.mock import MagicMock

    session_factory = _sqlite_strategy_state_session_factory(tmp_path)
    with session_factory() as session:
        session.add(
            StrategyState(
                strategy_id="s1",
                status="ACTIVE",
                config_json="{}",
                performance_json="{}",
                last_heartbeat=1,
                uptime_start=1,
                version=1,
            )
        )
        session.commit()

    redis = MagicMock()
    redis.exists.return_value = 1
    redis.publish.return_value = 1
    router = RedisStrategyCommandRouter(
        redis,
        StrategyStateQueryService(session_factory),
    )

    listed = router.handle({"command": "LIST"})
    health = router.handle({"command": "HEALTH_CHECK"})
    submitted = router.handle(
        {
            "command": "STOP",
            "strategy_id": "s1",
            "params": {"strategy_id": "s1"},
        }
    )

    assert listed.success is True
    assert listed.data is not None
    assert [row["strategy_id"] for row in listed.data["strategies"]] == ["s1"]
    assert health.data == {"healthy": {"s1": True}}
    assert submitted.success is True
    assert submitted.accepted is True
    redis.publish.assert_called_once_with(
        "cmd:strategy:control",
        '{"command":"STOP","strategy_id":"s1","params":{"strategy_id":"s1"}}',
    )


def test_strategy_command_returns_503_without_engine_listener(tmp_path):
    from unittest.mock import MagicMock

    redis = MagicMock()
    redis.publish.return_value = 0
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        strategy_control=StrategyControlService(
            RedisStrategyCommandRouter(
                redis,
                StrategyStateQueryService(
                    _sqlite_strategy_state_session_factory(tmp_path)
                ),
            )
        ),
    )

    response = app.handle(
        "POST",
        "/strategies/s1/commands",
        json.dumps({"command": "STOP"}),
    )

    assert response.status_code == 503
    assert response.body == {
        "error": "strategy_control_unavailable",
        "detail": "Strategy engine listener unavailable",
    }


def test_control_plane_reports_unavailable_strategy_control():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle("GET", "/strategies")

    assert response.status_code == 503
    assert response.body == {"error": "strategy_control_unavailable"}


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_control_plane_runs_csv_signal_backtest_job(tmp_path):
    session_factory = _sqlite_session_factory(tmp_path)
    candle_rows = _write_candles(tmp_path / "candles.csv")
    _write_signals(tmp_path / "signals.csv", [row[0] for row in candle_rows])
    app = ControlPlaneApp(
        BacktestJobExecutor(
            db_session_factory=session_factory,
            run_inline=True,
        )
    )

    payload = {
        "strategy_id": "api_backtest",
        "product_id": PRODUCT_ID,
        "timeframe": TIMEFRAME,
        "candles_csv_path": str(tmp_path / "candles.csv"),
        "signals_csv_path": str(tmp_path / "signals.csv"),
        "start_time": candle_rows[0][0],
        "end_time": candle_rows[-1][0],
        "initial_balance": "10000",
        "maker_fee": "0",
        "taker_fee": "0",
    }

    response = app.handle("POST", "/jobs/backtests", json.dumps(payload))

    assert response.status_code == 200
    job = response.body["job"]
    assert job["status"] == JobStatus.SUCCEEDED.value
    assert job["result"]["total_trades"] == 1
    assert Decimal(job["result"]["total_pnl"]) > Decimal("0")

    with session_factory() as session:
        trade_count = session.scalar(
            select(func.count()).select_from(BacktestTradeLog)
        )
        audit_count = session.scalar(select(func.count()).select_from(SignalAudit))

    assert trade_count == 2
    assert audit_count == 2


# =============================================================================
# L6 kill-switch endpoint — POST /ops/kill-switch
# =============================================================================
#
# Matrix:
#   1. auth-fail: missing key  → 401
#   2. auth-fail: wrong key    → 401
#   3. happy-path: valid key   → 202 + publish to Redis with correct shape
#   4. reason forwarded        → reason appears in published payload params
#
# Tests 1 & 2 rely on the existing auth middleware (trivially green once the
# route exists and auth is enforced).  Tests 3 & 4 are RED until the stub is
# implemented.


def _kill_switch_app(*, api_key: str = "secret", redis_client=None) -> ControlPlaneApp:
    from unittest.mock import MagicMock

    return ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key=api_key,
        redis_client=redis_client or MagicMock(),
    )


def test_kill_switch_rejects_missing_api_key():
    app = _kill_switch_app()
    response = app.handle("POST", "/ops/kill-switch")
    assert response.status_code == 401
    assert response.body.get("error") == "unauthorized"


def test_kill_switch_rejects_wrong_api_key():
    app = _kill_switch_app()
    response = app.handle(
        "POST", "/ops/kill-switch", headers={"x-api-key": "wrong-key"}
    )
    assert response.status_code == 401
    assert response.body.get("error") == "unauthorized"


def test_kill_switch_happy_path_returns_202_and_publishes():
    from unittest.mock import MagicMock

    redis = MagicMock()
    app = _kill_switch_app(redis_client=redis)

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        headers={"x-api-key": "secret"},
    )

    assert response.status_code == 202
    assert response.body == {"status": "accepted"}

    redis.publish.assert_called_once()
    channel, raw_payload = redis.publish.call_args.args
    assert channel == "cmd:strategy:control"
    published = json.loads(raw_payload)
    assert published["command"] == "KILL_SWITCH"
    assert "params" in published


def test_kill_switch_reason_forwarded_in_publish():
    from unittest.mock import MagicMock

    redis = MagicMock()
    app = _kill_switch_app(redis_client=redis)

    app.handle(
        "POST",
        "/ops/kill-switch",
        body=json.dumps({"reason": "eod_risk_drill"}),
        headers={"x-api-key": "secret"},
    )

    _, raw_payload = redis.publish.call_args.args
    published = json.loads(raw_payload)
    assert published["params"].get("reason") == "eod_risk_drill"


def test_clear_kill_switch_returns_202_and_publishes_manual_release():
    from unittest.mock import MagicMock

    redis = MagicMock()
    redis.publish.return_value = 1
    app = _kill_switch_app(redis_client=redis)

    response = app.handle(
        "POST",
        "/ops/kill-switch/clear",
        headers={"x-api-key": "secret"},
    )

    assert response.status_code == 202
    _, raw_payload = redis.publish.call_args.args
    assert json.loads(raw_payload) == {
        "command": "CLEAR_KILL_SWITCH",
        "params": {"actor": "api_key"},
    }


@pytest.mark.parametrize("payload", [[], None, "reason", 1, True])
def test_kill_switch_rejects_non_object_json(payload):
    from unittest.mock import MagicMock

    redis = MagicMock()
    app = _kill_switch_app(redis_client=redis)

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        body=json.dumps(payload),
        headers={"x-api-key": "secret"},
    )

    assert response.status_code == 400
    assert response.body["error"] == "invalid_json"
    redis.publish.assert_not_called()


@pytest.mark.parametrize("reason", [[], {}, 1, True])
def test_kill_switch_rejects_non_string_reason(reason):
    from unittest.mock import MagicMock

    redis = MagicMock()
    app = _kill_switch_app(redis_client=redis)

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        body=json.dumps({"reason": reason}),
        headers={"x-api-key": "secret"},
    )

    assert response.status_code == 422
    assert response.body == {"error": "validation_error"}
    redis.publish.assert_not_called()


def test_kill_switch_publish_failure_returns_503():
    from unittest.mock import MagicMock

    redis = MagicMock()
    redis.publish.side_effect = RuntimeError("redis down")
    app = _kill_switch_app(redis_client=redis)

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        headers={"x-api-key": "secret"},
    )

    assert response.status_code == 503
    assert response.body["error"] == "redis_publish_failed"
    assert "redis down" in response.body["detail"]


def test_kill_switch_publish_without_subscriber_returns_503():
    from unittest.mock import MagicMock

    redis = MagicMock()
    redis.publish.return_value = 0
    app = _kill_switch_app(redis_client=redis)

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        headers={"x-api-key": "secret"},
    )

    assert response.status_code == 503
    assert response.body == {"error": "kill_switch_no_listener"}


def test_production_control_plane_wiring_has_no_missing_dependency_503(tmp_path):
    from unittest.mock import MagicMock

    from src.control_plane import main as control_plane_main

    redis = MagicMock()
    redis.exists.return_value = 1
    redis.publish.return_value = 1

    class AcceptingEvaluator:
        def evaluate(self, request, candidate):
            return ParameterEvaluationResult(
                candidate_id=candidate.candidate_id,
                score_total=Decimal("1"),
            )

    parameter_search_evaluator = ParameterSearchEvaluatorRegistry(
        {
            "test_strategy": AcceptingEvaluator(),
            "golden_cross": AcceptingEvaluator(),
        }
    )
    app = control_plane_main.build_control_plane_app(
        redis_client=redis,
        db_session_factory=_sqlite_control_plane_session_factory(tmp_path),
        job_store=InMemoryJobStore(),
        parameter_search_evaluator=parameter_search_evaluator,
    )

    generic_search_payload = json.dumps(
        {
            "strategy_type": "test_strategy",
            "strategy_id": "test_search",
            "product_id": PRODUCT_ID,
            "timeframe": TIMEFRAME,
            "start_time": 1,
            "end_time": 2,
            "candidates": [
                {
                    "candidate_id": "candidate",
                    "param_pack": {"period": 10},
                }
            ],
        }
    )
    golden_cross_preset_payload = json.dumps(
        {
            "product_id": PRODUCT_ID,
            "timeframe": TIMEFRAME,
            "start_time": 1,
            "end_time": 2,
            "short_window": {"min": 1, "max": 1},
            "long_window": {"min": 2, "max": 2},
            "candidate_sample_count": 1,
        }
    )
    responses = {
        "health": app.handle("GET", "/health"),
        "submit_backtest": app.handle("POST", "/jobs/backtests", "{}"),
        "submit_parameter_search": app.handle(
            "POST", "/jobs/parameter-searches", generic_search_payload
        ),
        "submit_parameter_preset": app.handle(
            "POST",
            "/jobs/parameter-search-presets/golden-cross",
            golden_cross_preset_payload,
        ),
        "list_jobs": app.handle("GET", "/jobs"),
        "get_job": app.handle("GET", "/jobs/missing"),
        "cancel_job": app.handle("POST", "/jobs/missing/cancel"),
        "retry_job": app.handle("POST", "/jobs/missing/retry"),
        "list_genes": app.handle("GET", "/genes"),
        "get_gene": app.handle("GET", "/genes/999"),
        "promote_gene": app.handle("POST", "/genes/999/promote"),
        "list_epochs": app.handle("GET", "/evolution-epochs"),
        "get_epoch": app.handle("GET", "/evolution-epochs/missing"),
        "list_system_events": app.handle("GET", "/system-events"),
        "get_system_event": app.handle("GET", "/system-events/999"),
        "list_strategies": app.handle("GET", "/strategies"),
        "strategy_health": app.handle("GET", "/strategies/health"),
        "list_strategy_states": app.handle("GET", "/strategy-states"),
        "strategy_state_summary": app.handle("GET", "/strategy-states/summary"),
        "get_strategy_state": app.handle("GET", "/strategy-states/missing"),
        "strategy_state_transitions": app.handle(
            "GET", "/strategy-states/missing/transitions"
        ),
        "submit_strategy_command": app.handle(
            "POST",
            "/strategies/s1/commands",
            json.dumps({"command": "STOP"}),
        ),
        "kill_switch": app.handle("POST", "/ops/kill-switch"),
        "clear_kill_switch": app.handle("POST", "/ops/kill-switch/clear"),
    }

    unexpected_503s = {
        name: response.body
        for name, response in responses.items()
        if response.status_code == 503
    }
    assert unexpected_503s == {}
    assert responses["submit_parameter_search"].status_code == 202
    assert responses["submit_parameter_preset"].status_code == 202
    assert responses["submit_strategy_command"].status_code == 202
    assert app.parameter_search_executor is not None
    assert app.gene_control is not None
    assert app.strategy_control is not None
    assert app.strategy_state_query is not None
    assert app.redis_client is redis
    assert app.backtest_executor.store is app.parameter_search_executor.store


def test_production_parameter_search_rejects_unregistered_strategy_before_job_creation(
    tmp_path,
):
    from unittest.mock import MagicMock

    from src.control_plane import main as control_plane_main

    store = InMemoryJobStore()
    app = control_plane_main.build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=_sqlite_control_plane_session_factory(tmp_path),
        job_store=store,
        parameter_search_evaluator=ParameterSearchEvaluatorRegistry(
            {"golden_cross": GoldenCrossResearchParameterEvaluator()}
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_type": "rsi",
                "strategy_id": "rsi_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "candidates": [
                    {
                        "candidate_id": "candidate",
                        "param_pack": {"rsi_period": 14},
                    }
                ],
            }
        ),
    )

    assert response.status_code == 422
    assert response.body == {
        "error": "parameter_search_rejected",
        "detail": "unsupported strategy_type: rsi",
    }
    assert store.list() == []


def test_production_control_plane_registers_default_parameter_evaluators(tmp_path):
    from unittest.mock import MagicMock

    from src.control_plane import main as control_plane_main

    app = control_plane_main.build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=_sqlite_control_plane_session_factory(tmp_path),
        job_store=InMemoryJobStore(),
    )
    registry = app.parameter_search_executor.evaluator
    assert isinstance(registry, ParameterSearchEvaluatorRegistry)
    csv_request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_type": "csv_signal",
            "strategy_id": "csv_search",
            "product_id": PRODUCT_ID,
            "timeframe": TIMEFRAME,
            "start_time": 1,
            "end_time": 2,
            "candidates": [
                {
                    "candidate_id": "candidate",
                    "param_pack": {"signals_csv_path": "/unused.csv"},
                }
            ],
        }
    )
    golden_cross_request = csv_request.model_copy(
        update={
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross_search",
        }
    )

    assert isinstance(
        registry._resolve(csv_request),
        CsvSignalBacktestParameterEvaluator,
    )
    assert isinstance(
        registry._resolve(golden_cross_request),
        GoldenCrossResearchParameterEvaluator,
    )


def test_production_parameter_search_rejects_unsupported_warmup_before_job_creation(
    tmp_path,
):
    from unittest.mock import MagicMock

    from src.control_plane import main as control_plane_main

    class NoWarmupEvaluator:
        def evaluate(self, request, candidate):
            return ParameterEvaluationResult(
                candidate_id=candidate.candidate_id,
                score_total=Decimal("1"),
            )

    store = InMemoryJobStore()
    app = control_plane_main.build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=_sqlite_control_plane_session_factory(tmp_path),
        job_store=store,
        parameter_search_evaluator=ParameterSearchEvaluatorRegistry(
            {"no_warmup": NoWarmupEvaluator()}
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_type": "no_warmup",
                "strategy_id": "walk_forward_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 20,
                "backtest": {"candles_csv_path": "/unused.csv"},
                "candidates": [
                    {
                        "candidate_id": "candidate",
                        "param_pack": {"period": 10},
                    }
                ],
                "evaluation_set": {
                    "datasets": [
                        {
                            "dataset_id": "fold",
                            "product_id": PRODUCT_ID,
                            "timeframe": TIMEFRAME,
                            "warmup_start_time": 1,
                            "start_time": 10,
                            "end_time": 20,
                        }
                    ]
                },
            }
        ),
    )

    assert response.status_code == 422
    assert response.body == {
        "error": "parameter_search_rejected",
        "detail": (
            "strategy_type does not support walk-forward warmup: no_warmup"
        ),
    }
    assert store.list() == []


def test_registry_delegates_request_validation_before_job_creation(tmp_path):
    from unittest.mock import MagicMock

    from src.control_plane import main as control_plane_main

    class RejectingEvaluator:
        def validate_request(self, request):
            raise UnsupportedParameterSearchError("strategy parameters rejected")

        def evaluate(self, request, candidate):
            raise AssertionError("rejected request must not be evaluated")

    store = InMemoryJobStore()
    app = control_plane_main.build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=_sqlite_control_plane_session_factory(tmp_path),
        job_store=store,
        parameter_search_evaluator=ParameterSearchEvaluatorRegistry(
            {"rejecting": RejectingEvaluator()}
        ),
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        json.dumps(
            {
                "strategy_type": "rejecting",
                "strategy_id": "rejected_search",
                "product_id": PRODUCT_ID,
                "timeframe": TIMEFRAME,
                "start_time": 1,
                "end_time": 2,
                "candidates": [
                    {
                        "candidate_id": "candidate",
                        "param_pack": {"period": 10},
                    }
                ],
            }
        ),
    )

    assert response.status_code == 422
    assert response.body == {
        "error": "parameter_search_rejected",
        "detail": "strategy parameters rejected",
    }
    assert store.list() == []


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/jobs/backtests", "[]"),
        ("/jobs/parameter-searches", '"not-an-object"'),
    ],
)
def test_control_plane_rejects_non_object_job_payloads(path, body):
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        parameter_search_executor=ParameterSearchJobExecutor(
            ParameterSearchEvaluatorRegistry(
                {"golden_cross": GoldenCrossResearchParameterEvaluator()}
            ),
            run_inline=True,
        ),
    )

    response = app.handle("POST", path, body)

    assert response.status_code == 400
    assert response.body["error"] == "invalid_json"


def test_control_plane_main_serves_production_app(monkeypatch):
    from src.control_plane import main as control_plane_main

    app = object()
    browser_auth = object()
    captured = {}
    monkeypatch.setenv("CONTROL_PLANE_STATIC_DIR", "/app/frontend")
    monkeypatch.setenv("CONTROL_PLANE_API_KEY", "api-key")
    monkeypatch.setattr(
        control_plane_main,
        "build_browser_session_auth_from_env",
        lambda: browser_auth,
    )

    monkeypatch.setattr(
        control_plane_main,
        "build_control_plane_app",
        lambda *, api_key, browser_auth: captured.update(
            {"api_key": api_key, "browser_auth": browser_auth}
        )
        or app,
    )
    monkeypatch.setattr(
        control_plane_main,
        "serve",
        lambda served_app, *, host, port, static_dir: captured.update(
            {
                "served_app": served_app,
                "host": host,
                "port": port,
                "static_dir": static_dir,
            }
        ),
    )

    control_plane_main.main()

    assert captured["served_app"] is app
    assert captured["api_key"] == "api-key"
    assert captured["browser_auth"] is browser_auth
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8080
    assert captured["static_dir"] == "/app/frontend"


def test_control_plane_main_disables_static_frontend_with_api_key_only(
    monkeypatch,
):
    from src.control_plane import main as control_plane_main

    app = object()
    captured = {}
    monkeypatch.setenv("CONTROL_PLANE_STATIC_DIR", "/app/frontend")
    monkeypatch.setenv("CONTROL_PLANE_API_KEY", "api-key")
    monkeypatch.setattr(
        control_plane_main,
        "build_control_plane_app",
        lambda *, api_key, browser_auth: captured.update(
            {"api_key": api_key, "browser_auth": browser_auth}
        )
        or app,
    )
    monkeypatch.setattr(
        control_plane_main,
        "serve",
        lambda served_app, *, host, port, static_dir: captured.update(
            {
                "served_app": served_app,
                "host": host,
                "port": port,
                "static_dir": static_dir,
            }
        ),
    )

    control_plane_main.main()

    assert captured["served_app"] is app
    assert captured["api_key"] == "api-key"
    assert captured["browser_auth"] is None
    assert captured["static_dir"] is None
