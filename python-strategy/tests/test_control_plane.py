import json
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
    InMemoryJobStore,
    ParameterEvaluationResult,
    ParameterSearchJobExecutor,
    SqliteJobStore,
    StrategyControlService,
)
from src.control_plane.models import BacktestJobRequest, JobStatus
from src.core.command_router import CommandResult
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Product,
    SignalAudit,
    Strategy,
)

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


def test_control_plane_rejects_invalid_backtest_payload():
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))

    response = app.handle("POST", "/jobs/backtests", "{}")

    assert response.status_code == 422
    assert response.body["error"] == "validation_error"


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
        "params": {"strategy_id": "s1", "reason": "operator pause"},
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
