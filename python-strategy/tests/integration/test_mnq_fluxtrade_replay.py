"""Deterministic MNQ replay through the production FluxTrade backtest path."""

from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.models import (
    BacktestInstrumentConfig,
    BacktestJobRequest,
    JobStatus,
)
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

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]

PRODUCT_ID = "RITHMIC:MNQ-202409"
FIXTURES = Path(__file__).parents[1] / "fixtures"


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


def _session_factory(tmp_path, request: pytest.FixtureRequest):
    engine = create_engine(f"sqlite:///{tmp_path / 'mnq_replay.db'}")
    request.addfinalizer(engine.dispose)
    for table in [
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ]:
        table.create(engine, checkfirst=True)

    factory = sessionmaker(bind=engine)
    with factory() as session:
        session.add(Exchange(id="RITHMIC", name="Rithmic"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="RITHMIC",
                base_asset="MNQ",
                quote_asset="USD",
            )
        )
        session.commit()
    return factory


@pytest.mark.smoke
def test_mnq_csv_replay_uses_futures_accounting_and_matcher(
    tmp_path, request: pytest.FixtureRequest
):
    session_factory = _session_factory(tmp_path, request)
    job_request = BacktestJobRequest(
        strategy_id="mnq_deterministic",
        product_id=PRODUCT_ID,
        timeframe="1m",
        candles_csv_path=str(FIXTURES / "mnq_1m_deterministic.csv"),
        signals_csv_path=str(FIXTURES / "mnq_signals_deterministic.csv"),
        start_time=1_720_656_000_000,
        end_time=1_720_656_300_000,
        initial_balance=Decimal("100000"),
        maker_fee=Decimal("0.50"),
        taker_fee=Decimal("0.50"),
        instrument=BacktestInstrumentConfig.model_validate(
            {
                "multiplier": "2",
                "quantity_step": "1",
                "price_tick": "0.25",
                "fee_model": "per_contract",
                "capital_model": "per_contract",
                "capital_per_contract": "2500",
            }
        ),
    )

    job = BacktestJobExecutor(
        db_session_factory=session_factory,
        run_inline=True,
    ).submit_backtest(job_request)

    assert job.status == JobStatus.SUCCEEDED
    assert job.result is not None
    assert job.result["candle_count"] == 6
    assert job.result["total_trades"] == 1
    assert Decimal(job.result["total_pnl"]) == Decimal("-2.00")

    with session_factory() as session:
        trades = list(
            session.scalars(
                select(BacktestTradeLog).order_by(BacktestTradeLog.timestamp)
            )
        )
        summary = session.scalars(select(BacktestResultSummary)).one()

    assert [trade.product_id for trade in trades] == [PRODUCT_ID, PRODUCT_ID]
    assert [Decimal(trade.quantity) for trade in trades] == [Decimal("1"), Decimal("1")]
    assert [Decimal(trade.price) for trade in trades] == [
        Decimal("20880.75"),
        Decimal("20880.25"),
    ]
    assert [trade.fee for trade in trades] == [
        Decimal("0.50"),
        Decimal("0.50"),
    ]
    assert summary.total_pnl is not None
    assert Decimal(summary.total_pnl) == Decimal("-2.00")
