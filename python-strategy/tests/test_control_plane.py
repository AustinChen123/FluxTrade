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
    InMemoryJobStore,
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
