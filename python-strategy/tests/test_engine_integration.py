"""Coordinator integration tests for StrategyEngine component wiring."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from src.core.models import Candlestick, Position, PositionSide, Signal, SignalType, StrategyStatus
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.orm_models import SignalAudit, Strategy, StrategyState, StrategyStateTransition
from src.core.risk_rules.existing_position_entry import ExistingPositionEntryRule
from src.strategies.base import BaseStrategy, StrategyRequirements


class EmittingStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str = "BINANCE:BTCUSDT-PERP"):
        super().__init__(strategy_id, product_id)
        self.candles_received: list[Candlestick] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 10)

    def on_candle(self, candle: Candlestick) -> Signal:
        self.candles_received.append(candle)
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            value=candle.close,
        )


class RestartAccountService:
    def __init__(self, position: Position | None = None):
        self.position = position

    def get_balance(self) -> Decimal:
        return Decimal("100000")

    def get_position(self, strategy_id: str, product_id: str):
        if self.position is None:
            return None
        if self.position.strategy_id != strategy_id or self.position.product_id != product_id:
            return None
        return self.position


def make_candle(
    product_id: str = "BINANCE:BTCUSDT-PERP",
    timeframe: str = "1m",
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=1704067200000,
        open=Decimal("42000"),
        high=Decimal("42500"),
        low=Decimal("41500"),
        close=Decimal("42200"),
        volume=Decimal("100"),
    )


def make_orm_candles(count: int = 10) -> list[ORMCandlestick]:
    return [
        ORMCandlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000 + i * 60_000,
            open=Decimal("42000"),
            high=Decimal("42500"),
            low=Decimal("41500"),
            close=Decimal("42200") + Decimal(i),
            volume=Decimal("100"),
        )
        for i in range(count)
    ]


def _sqlite_lifecycle_session_factory(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'engine_lifecycle.db'}")
    for table in [
        ORMCandlestick.__table__,
        Strategy.__table__,
        StrategyState.__table__,
    ]:
        table.create(engine, checkfirst=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE strategy_state_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_id VARCHAR NOT NULL,
                    from_status VARCHAR(32) NOT NULL,
                    to_status VARCHAR(32) NOT NULL,
                    transitioned_at DATETIME NOT NULL,
                    reason TEXT,
                    actor VARCHAR(64)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE signal_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp BIGINT NOT NULL,
                    strategy_id VARCHAR NOT NULL,
                    product_id VARCHAR NOT NULL,
                    signal_type VARCHAR NOT NULL,
                    risk_status VARCHAR NOT NULL,
                    risk_message TEXT,
                    order_id VARCHAR,
                    details_json TEXT,
                    client_order_id VARCHAR(128),
                    intent_payload TEXT,
                    outcome_payload TEXT,
                    signal_batch_id VARCHAR(64)
                )
                """
            )
        )
    return sessionmaker(bind=engine)


def test_full_lifecycle_routes_signal_through_wired_components(engine_factory, mock_db_session):
    engine = engine_factory()
    strategy = EmittingStrategy("s1")
    candle = make_candle()
    engine.add_strategy(strategy)
    engine.execution_engine.process_market_data = MagicMock()
    engine.execution_engine.execute_signal = MagicMock(return_value="order-1")
    engine.risk_manager.check_risk = MagicMock(return_value=(True, "ok"))

    engine.on_market_data(candle)
    engine.shutdown(timeout=0.1)

    assert strategy.candles_received == [candle]
    engine.execution_engine.process_market_data.assert_called_once_with(candle)
    engine.risk_manager.check_risk.assert_called_once()
    engine.execution_engine.execute_signal.assert_called_once()
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()
    assert engine.running is False


def test_command_router_lists_registered_strategies(engine_factory):
    engine = engine_factory()
    engine.add_strategy(EmittingStrategy("s1"))

    result = engine._command_router.handle({"command": "LIST"})

    assert result.success is True
    assert result.data == {
        "strategies": [
            {
                "strategy_id": "s1",
                "product_id": "BINANCE:BTCUSDT-PERP",
                "timeframe": "1m",
            }
        ]
    }


def test_health_monitoring_records_strategy_heartbeat(engine_factory):
    engine = engine_factory()
    engine.add_strategy(EmittingStrategy("s1"))
    mock_db = MagicMock()
    engine._db_session_factory = lambda: nullcontext(mock_db)

    engine._record_strategy_heartbeats(["s1"])

    assert engine._health_monitor.is_healthy("s1") is True
    assert engine._health_monitor.get_uptime("s1") >= 0.0
    mock_db.commit.assert_called_once()


def test_commands_update_lifecycle_state_with_real_state_manager(
    tmp_path,
    engine_factory,
):
    session_factory = _sqlite_lifecycle_session_factory(tmp_path)
    with session_factory() as session:
        session.add(Strategy(id="s1", name="Strategy 1"))
        session.add_all(make_orm_candles())
        session.add(
            StrategyState(
                strategy_id="s1",
                status=StrategyStatus.READY.value,
                config_json="{}",
                version=0,
            )
        )
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.loaded_classes["s1"] = EmittingStrategy

    engine._handle_command({"command": "START", "params": {"id": "s1"}})
    assert "s1" in engine.strategy_instances

    engine._handle_command({
        "command": "STOP",
        "params": {"id": "s1", "reason": "pause"},
    })
    assert "s1" not in engine.strategy_instances

    with session_factory() as session:
        state = session.get(StrategyState, "s1")
        state.status = StrategyStatus.ERROR.value
        state.last_error_message = "manual test error"
        state.entered_error_at = datetime.now(UTC)
        session.commit()

    engine._handle_command({
        "command": "FORCE_RECOVER",
        "params": {"strategy_id": "s1", "reason": "operator reset"},
    })
    assert "s1" in engine.strategy_instances
    engine.shutdown(timeout=0.1)

    with session_factory() as session:
        state = session.get(StrategyState, "s1")
        transitions = list(
            session.scalars(
                select(StrategyStateTransition).order_by(StrategyStateTransition.id)
            )
        )

    assert state.status == StrategyStatus.ACTIVE.value
    assert state.version == 3
    assert state.recovered_at is not None
    assert [
        (row.from_status, row.to_status, row.reason, row.actor)
        for row in transitions
    ] == [
        (StrategyStatus.READY.value, StrategyStatus.ACTIVE.value, None, "operator"),
        (StrategyStatus.ACTIVE.value, StrategyStatus.STOPPED.value, "pause", "operator"),
        (
            StrategyStatus.ERROR.value,
            StrategyStatus.ACTIVE.value,
            "operator reset",
            "operator",
        ),
    ]


def test_restart_restore_warms_and_blocks_duplicate_entry_with_real_session(
    tmp_path,
    engine_factory,
):
    session_factory = _sqlite_lifecycle_session_factory(tmp_path)
    with session_factory() as session:
        session.add(Strategy(id="s1", name="Strategy 1"))
        session.add_all(make_orm_candles())
        session.add(
            StrategyState(
                strategy_id="s1",
                status=StrategyStatus.ACTIVE.value,
                config_json="{}",
                version=0,
            )
        )
        session.commit()

    position = Position(
        strategy_id="s1",
        product_id="BINANCE:BTCUSDT-PERP",
        side=PositionSide.LONG,
        quantity=Decimal("0.01"),
        entry_price=Decimal("42200"),
        unrealized_pnl=Decimal("0"),
    )
    engine = engine_factory(
        db_session_factory=session_factory,
        account_service=RestartAccountService(position),
    )
    engine.risk_manager.existing_position_entry_rule = ExistingPositionEntryRule()
    engine.loaded_classes["s1"] = EmittingStrategy
    engine.execution_engine.execute_signal = MagicMock(return_value="order-1")

    engine._restore_active_strategies_on_startup()
    restored = engine.strategy_instances["s1"]
    engine.on_market_data(make_candle())

    assert len(restored.candles_received) == 11
    engine.execution_engine.execute_signal.assert_not_called()
    with session_factory() as session:
        audits = list(session.scalars(select(SignalAudit)))
    assert len(audits) == 1
    assert audits[0].risk_status == "REJECT"
    assert "existing_position_entry_duplicate" in audits[0].risk_message
