"""Concurrency coverage for short-lived DB session factories."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from src.core.models import OrderSide
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Position,
    Product,
    Strategy,
    StrategyState,
)
from src.core.orm_models import Trade as ORMTrade
from src.core.repositories import BacktestOrderRepository, LiveOrderRepository


def _sqlite_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'session_lifecycle.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        Position.__table__,
        StrategyState.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ]:
        table.create(engine, checkfirst=True)
    return sessionmaker(bind=engine)


def test_concurrent_live_position_updates_use_independent_sessions(tmp_path):
    session_factory = _sqlite_session_factory(tmp_path)
    repo = LiveOrderRepository(db_session_factory=session_factory)

    def update_position(index: int) -> None:
        repo.update_position(
            strategy_id=f"strategy-{index}",
            product_id="BINANCE:BTCUSDT-PERP",
            side=OrderSide.BUY,
            fill_quantity=Decimal("0.1"),
            fill_price=Decimal("42000"),
            position_side="LONG",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(update_position, range(200)))

    with session_factory() as session:
        count = session.scalar(select(func.count()).select_from(Position))

    assert count == 200


def test_concurrent_backtest_trade_logs_use_independent_sessions(tmp_path):
    session_factory = _sqlite_session_factory(tmp_path)
    repo = BacktestOrderRepository(
        None,
        session_id=1,
        db_session_factory=session_factory,
    )

    def add_trade(index: int) -> None:
        repo.add_trade(
            ORMTrade(
                id=f"trade-{index}",
                order_id=f"order-{index}",
                exchange_trade_id=f"exchange-trade-{index}",
                product_id="BINANCE:BTCUSDT-PERP",
                side="buy",
                price=Decimal("42000"),
                quantity=Decimal("0.1"),
                fee=Decimal("0"),
                fee_asset="USDT",
                timestamp=1704067200000 + index,
            )
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(add_trade, range(200)))

    with session_factory() as session:
        count = session.scalar(select(func.count()).select_from(BacktestTradeLog))

    assert count == 200


def test_concurrent_engine_heartbeat_updates_use_independent_sessions(tmp_path, engine_factory):
    session_factory = _sqlite_session_factory(tmp_path)

    with session_factory() as session:
        session.add_all(
            StrategyState(
                strategy_id=f"strategy-{index}",
                status="ACTIVE",
                config_json="{}",
            )
            for index in range(200)
        )
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine._health_monitor.update_heartbeat = lambda strategy_id: None

    def record_heartbeat(index: int) -> None:
        engine._record_strategy_heartbeats([f"strategy-{index}"])

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(record_heartbeat, range(200)))

    with session_factory() as session:
        count = session.scalar(
            select(func.count())
            .select_from(StrategyState)
            .where(StrategyState.last_heartbeat.is_not(None))
        )

    engine.shutdown(timeout=0.1)

    assert count == 200
