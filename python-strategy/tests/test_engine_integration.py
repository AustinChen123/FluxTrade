"""Coordinator integration tests for StrategyEngine component wiring."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from src.core.models import (
    Candlestick,
    OrderSide,
    Position,
    PositionSide,
    Signal,
    SignalType,
    StrategyStatus,
    Trade,
)
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.orm_models import (
    Exchange,
    MarketDataApplication,
    Product,
    SignalAudit,
    Strategy,
    StrategyState,
    StrategyStateTransition,
)
from src.core.runtime_environment import RuntimeEnvironment
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.strategies.golden_cross import GoldenCrossStrategy


class EmittingStrategy(BaseStrategy):
    __fluxtrade_readiness__ = "LIVE_APPROVED"

    def __init__(self, strategy_id: str, product_id: str = "BINANCE:BTCUSDT-PERP"):
        super().__init__(strategy_id, product_id)
        self.candles_received: list[Candlestick] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 10)

    def sync_position_state(self, position_side: str | None) -> bool:
        """Accept the position sync unconditionally — required for restart integration test."""
        return True

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(self.strategy_id, self.product_id)

    def replay_configuration(self) -> object:
        return ()

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
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
        if (
            self.position.strategy_id != strategy_id
            or self.position.product_id != product_id
        ):
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


@pytest.fixture
def sqlite_lifecycle_session_factory(
    tmp_path: Path,
) -> Iterator[sessionmaker[Session]]:
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
    try:
        yield sessionmaker(bind=engine)
    finally:
        engine.dispose()


@pytest.fixture
def sqlite_market_session_factory(
    tmp_path: Path,
) -> Iterator[sessionmaker[Session]]:
    engine = create_engine(f"sqlite:///{tmp_path / 'market_recovery.db'}")
    for table in [
        Exchange.__table__,
        Product.__table__,
        ORMCandlestick.__table__,
        MarketDataApplication.__table__,
    ]:
        table.create(engine, checkfirst=True)
    try:
        yield sessionmaker(bind=engine)
    finally:
        engine.dispose()


def test_full_lifecycle_routes_signal_through_wired_components(
    engine_factory, mock_db_session
):
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


def test_live_static_registration_rejects_missing_replay_contract(
    engine_factory,
):
    class UnrecoverableStrategy(BaseStrategy):
        __fluxtrade_readiness__ = "LIVE_APPROVED"

        @property
        def requirements(self) -> StrategyRequirements:
            return StrategyRequirements(self.product_id, "1m", 0)

        def on_candle(self, candle, context=None):
            return Signal(
                strategy_id=self.strategy_id,
                product_id=self.product_id,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp,
                type=SignalType.NO_SIGNAL,
            )

    engine = engine_factory()
    engine.runtime_environment = RuntimeEnvironment("live")

    with pytest.raises(
        NotImplementedError,
        match="pending-market replay configuration",
    ):
        engine.add_strategy(
            UnrecoverableStrategy("unrecoverable", "BINANCE:BTCUSDT-PERP")
        )

    assert engine.strategy_instances == {}
    assert engine.strategies == {}
    assert engine._registry.list_active() == []


def test_live_static_registration_rejects_replay_factory_returning_self(
    engine_factory,
):
    class SelfReturningStrategy(EmittingStrategy):
        def fresh_instance_for_replay(self) -> BaseStrategy:
            return self

    engine = engine_factory()
    engine.runtime_environment = RuntimeEnvironment("live")

    with pytest.raises(
        RuntimeError,
        match="did not return a distinct compatible instance",
    ):
        engine.add_strategy(
            SelfReturningStrategy("same-instance", "BINANCE:BTCUSDT-PERP")
        )

    assert engine.strategy_instances == {}
    assert engine.strategies == {}
    assert engine._registry.list_active() == []


def test_pending_replay_rebuilds_strategy_before_ambiguous_candle(
    engine_factory,
    sqlite_market_session_factory,
):
    session_factory = sqlite_market_session_factory
    rows = make_orm_candles(11)
    row_timestamps = [row.timestamp for row in rows]
    pending = Candlestick(
        product_id=rows[-1].product_id,
        timeframe=rows[-1].timeframe,
        timestamp=rows[-1].timestamp,
        open=rows[-1].open,
        high=rows[-1].high,
        low=rows[-1].low,
        close=rows[-1].close,
        volume=rows[-1].volume,
    )
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id="BINANCE:BTCUSDT-PERP",
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add_all(rows)
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.runtime_environment = RuntimeEnvironment("live")
    original = EmittingStrategy("s1")
    engine.add_strategy(original)
    engine.process_signal = MagicMock()

    engine.replay_pending_market_data(pending)

    replacement = engine.strategy_instances["s1"]
    assert replacement is not original
    assert [candle.timestamp for candle in replacement.candles_received] == [
        *row_timestamps[:-1],
        pending.timestamp,
    ]


def test_pending_trade_replay_fails_without_durable_state_boundary(engine_factory):
    engine = engine_factory()
    trade = Trade(
        id="trade-1",
        product_id="BINANCE:BTCUSDT-PERP",
        price=Decimal("42000"),
        quantity=Decimal("1"),
        side=OrderSide.BUY,
        timestamp=1704067200000,
    )

    with pytest.raises(
        RuntimeError,
        match="pending trade replay has no durable strategy-state boundary",
    ):
        engine.replay_pending_market_data(trade)


def test_pending_candle_replay_holds_engine_market_lock(engine_factory):
    engine = engine_factory()
    candle = make_candle()
    market_lock = MagicMock()
    engine._market_processing_lock = market_lock
    engine._pending_market_replay = MagicMock()

    def replay(data, *, apply_new):
        assert market_lock.__enter__.call_count == 1
        assert data is candle
        assert apply_new == engine._apply_unpersisted_candle

    engine._pending_market_replay.replay.side_effect = replay

    engine.replay_pending_market_data(candle)

    market_lock.__exit__.assert_called_once_with(None, None, None)
    engine._pending_market_replay.replay.assert_called_once()


def test_pending_replay_uses_strategy_recovery_factory(
    engine_factory,
    sqlite_market_session_factory,
):
    class ConfiguredStrategy(EmittingStrategy):
        def __init__(self, strategy_id: str, product_id: str, token: str):
            super().__init__(strategy_id, product_id)
            self.token = token

        def fresh_instance_for_replay(self):
            return type(self)(self.strategy_id, self.product_id, self.token)

        def replay_configuration(self):
            return (self.token,)

    session_factory = sqlite_market_session_factory
    rows = make_orm_candles(10)
    pending = make_candle()
    pending = pending.model_copy(update={"timestamp": rows[-1].timestamp + 60_000})
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=pending.product_id,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add_all(rows)
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.add_strategy(
        ConfiguredStrategy("configured", pending.product_id, "registered-formula")
    )
    engine.process_signal = MagicMock()

    engine.replay_pending_market_data(pending)

    replacement = engine.strategy_instances["configured"]
    assert replacement.token == "registered-formula"
    assert len(replacement.candles_received) == 11


def test_pending_replay_preserves_nondefault_golden_cross_configuration(
    engine_factory,
    sqlite_market_session_factory,
    monkeypatch,
):
    session_factory = sqlite_market_session_factory
    rows = make_orm_candles(4)
    pending = Candlestick(
        product_id=rows[-1].product_id,
        timeframe=rows[-1].timeframe,
        timestamp=rows[-1].timestamp,
        open=rows[-1].open,
        high=rows[-1].high,
        low=rows[-1].low,
        close=rows[-1].close,
        volume=rows[-1].volume,
    )
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=pending.product_id,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add_all(rows)
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.runtime_environment = RuntimeEnvironment("live")
    monkeypatch.setattr(
        GoldenCrossStrategy,
        "__fluxtrade_readiness__",
        "LIVE_APPROVED",
        raising=False,
    )
    original = GoldenCrossStrategy(
        "configured-golden-cross",
        pending.product_id,
        short_window=2,
        long_window=3,
        timeframe="1m",
        quantity=Decimal("2"),
    )
    engine.add_strategy(original)
    engine.process_signal = MagicMock()

    engine.replay_pending_market_data(pending)

    replacement = engine.strategy_instances["configured-golden-cross"]
    assert replacement is not original
    assert isinstance(replacement, GoldenCrossStrategy)
    assert replacement.replay_configuration() == (
        2,
        3,
        "1m",
        Decimal("2"),
    )


def test_live_candle_fence_holds_postgres_advisory_lock_around_application(
    engine_factory,
):
    engine = engine_factory()
    engine.runtime_environment = RuntimeEnvironment("live")
    db = MagicMock()
    db.get_bind.return_value.dialect.name = "postgresql"
    events: list[str] = []

    def execute(statement, _parameters):
        sql = str(statement)
        if "pg_advisory_unlock" in sql:
            events.append("unlock")
            result = MagicMock()
            result.scalar.return_value = True
            return result
        events.append("lock")
        return MagicMock()

    db.execute.side_effect = execute
    engine._db_session_factory = lambda: nullcontext(db)

    with engine._live_candle_application.application_fence(make_candle()):
        events.append("application")

    assert events == ["lock", "application", "unlock"]


def test_live_candle_fence_rejects_non_postgres_production_database(
    engine_factory,
):
    engine = engine_factory()
    engine.runtime_environment = RuntimeEnvironment("live")
    db = MagicMock()
    db.get_bind.return_value.dialect.name = "mysql"
    engine._db_session_factory = lambda: nullcontext(db)

    with pytest.raises(
        RuntimeError,
        match="requires PostgreSQL advisory locks",
    ):
        with engine._live_candle_application.application_fence(make_candle()):
            pytest.fail("application must remain fenced")


def test_live_candle_fence_times_out_without_entering_application(
    engine_factory,
):
    engine = engine_factory()
    engine.runtime_environment = RuntimeEnvironment("live")
    db = MagicMock()
    db.get_bind.return_value.dialect.name = "postgresql"
    result = MagicMock()
    result.scalar.return_value = False
    db.execute.return_value = result
    engine._db_session_factory = lambda: nullcontext(db)

    with patch(
        "src.core.live_candle_application.LIVE_CANDLE_FENCE_TIMEOUT_SECONDS",
        0,
    ):
        with pytest.raises(
            RuntimeError,
            match="timed out acquiring live candle application fence",
        ):
            with engine._live_candle_application.application_fence(make_candle()):
                pytest.fail("application must not run without the fence")


def test_live_candle_is_persisted_only_after_successful_application(
    engine_factory,
    sqlite_market_session_factory,
):
    session_factory = sqlite_market_session_factory
    engine = engine_factory(db_session_factory=session_factory)
    engine.runtime_environment = RuntimeEnvironment("live")
    engine.execution_engine.process_market_data = MagicMock()
    candle = make_candle()

    engine.on_market_data(candle)

    with session_factory() as session:
        persisted = session.get(
            ORMCandlestick,
            (candle.product_id, candle.timeframe, candle.timestamp),
        )
        assert persisted is not None
        assert type(persisted.close) is Decimal
        assert persisted.close == candle.close
        application = session.get(
            MarketDataApplication,
            (
                "live",
                candle.product_id,
                candle.timeframe,
                candle.timestamp,
            ),
        )
        assert application is not None
        assert Decimal(application.close) == candle.close

    engine._signal_processor.on_candle = MagicMock(
        side_effect=RuntimeError("strategy failed")
    )
    failed = candle.model_copy(update={"timestamp": candle.timestamp + 60_000})
    with pytest.raises(RuntimeError, match="strategy failed"):
        engine.on_market_data(failed)

    with session_factory() as session:
        assert (
            session.get(
                ORMCandlestick,
                (failed.product_id, failed.timeframe, failed.timestamp),
            )
            is None
        )
        assert (
            session.get(
                MarketDataApplication,
                (
                    "live",
                    failed.product_id,
                    failed.timeframe,
                    failed.timestamp,
                ),
            )
            is None
        )


def test_durable_receipt_rebuilds_state_without_duplicate_side_effects(
    engine_factory,
    sqlite_market_session_factory,
):
    session_factory = sqlite_market_session_factory
    rows = make_orm_candles(11)
    pending = Candlestick(
        product_id=rows[-1].product_id,
        timeframe=rows[-1].timeframe,
        timestamp=rows[-1].timestamp,
        open=rows[-1].open,
        high=rows[-1].high,
        low=rows[-1].low,
        close=rows[-1].close,
        volume=rows[-1].volume,
    )
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=pending.product_id,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add_all(rows)
        session.add(
            MarketDataApplication(
                environment="live",
                product_id=pending.product_id,
                timeframe=pending.timeframe,
                timestamp=pending.timestamp,
                open=pending.open,
                high=pending.high,
                low=pending.low,
                close=pending.close,
                volume=pending.volume,
            )
        )
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.runtime_environment = RuntimeEnvironment("live")
    original = EmittingStrategy("s1")
    engine.add_strategy(original)
    engine.execution_engine.process_market_data = MagicMock()
    engine._signal_processor.on_candle = MagicMock()

    engine.replay_pending_market_data(pending)
    engine.on_market_data(pending)

    replacement = engine.strategy_instances["s1"]
    assert replacement is not original
    assert replacement.candles_received[-1] == pending
    assert len(replacement.candles_received) == 10
    engine.execution_engine.process_market_data.assert_not_called()
    engine._signal_processor.on_candle.assert_not_called()


def test_unreceipted_older_candle_fails_closed_before_replay_or_callback(
    engine_factory,
    sqlite_market_session_factory,
):
    session_factory = sqlite_market_session_factory
    rows = make_orm_candles(11)
    latest = Candlestick(
        product_id=rows[-1].product_id,
        timeframe=rows[-1].timeframe,
        timestamp=rows[-1].timestamp,
        open=rows[-1].open,
        high=rows[-1].high,
        low=rows[-1].low,
        close=rows[-1].close,
        volume=rows[-1].volume,
    )
    older = Candlestick(
        product_id=rows[-2].product_id,
        timeframe=rows[-2].timeframe,
        timestamp=rows[-2].timestamp,
        open=rows[-2].open,
        high=rows[-2].high,
        low=rows[-2].low,
        close=rows[-2].close,
        volume=rows[-2].volume,
    )
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=latest.product_id,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add_all(rows)
        session.add(
            MarketDataApplication(
                environment="live",
                product_id=latest.product_id,
                timeframe=latest.timeframe,
                timestamp=latest.timestamp,
                open=latest.open,
                high=latest.high,
                low=latest.low,
                close=latest.close,
                volume=latest.volume,
            )
        )
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.runtime_environment = RuntimeEnvironment("live")
    engine.execution_engine.process_market_data = MagicMock()
    engine._signal_processor.on_candle = MagicMock()

    with pytest.raises(RuntimeError, match="live candle application is out of order"):
        engine.replay_pending_market_data(older)
    with pytest.raises(RuntimeError, match="live candle application is out of order"):
        engine.on_market_data(older)

    engine.execution_engine.process_market_data.assert_not_called()
    engine._signal_processor.on_candle.assert_not_called()


def test_conflicting_history_blocks_live_callback_before_side_effect(
    engine_factory,
    sqlite_market_session_factory,
):
    session_factory = sqlite_market_session_factory
    candle = make_candle()
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=candle.product_id,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add(
            ORMCandlestick(
                product_id=candle.product_id,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close + Decimal("1"),
                volume=candle.volume,
            )
        )
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.runtime_environment = RuntimeEnvironment("live")
    engine.execution_engine.process_market_data = MagicMock()
    engine._signal_processor.on_candle = MagicMock()

    with pytest.raises(
        RuntimeError,
        match="live candle conflicts with canonical history",
    ):
        engine.on_market_data(candle)

    engine.execution_engine.process_market_data.assert_not_called()
    engine._signal_processor.on_candle.assert_not_called()


def test_commands_update_lifecycle_state_with_real_state_manager(
    engine_factory,
    sqlite_lifecycle_session_factory,
):
    session_factory = sqlite_lifecycle_session_factory
    with session_factory() as session:
        session.add(Strategy(id="s1", name="Strategy 1"))
        session.add_all(make_orm_candles())
        session.add(
            StrategyState(
                strategy_id="s1",
                status=StrategyStatus.READY.value,
                config_json='{"product_id":"BINANCE:BTCUSDT-PERP"}',
                version=0,
            )
        )
        session.commit()

    engine = engine_factory(db_session_factory=session_factory)
    engine.loaded_classes["s1"] = EmittingStrategy

    engine._handle_command({"command": "START", "params": {"id": "s1"}})
    assert "s1" in engine.strategy_instances

    engine._handle_command(
        {
            "command": "STOP",
            "params": {"id": "s1", "reason": "pause"},
        }
    )
    assert "s1" not in engine.strategy_instances

    with session_factory() as session:
        state = session.get(StrategyState, "s1")
        assert state is not None
        state.status = StrategyStatus.ERROR.value
        state.last_error_message = "manual test error"
        state.entered_error_at = datetime.now(UTC)
        session.commit()

    engine._handle_command(
        {
            "command": "FORCE_RECOVER",
            "params": {"strategy_id": "s1", "reason": "operator reset"},
        }
    )
    assert "s1" in engine.strategy_instances
    engine.shutdown(timeout=0.1)

    with session_factory() as session:
        state = session.get(StrategyState, "s1")
        assert state is not None
        transitions = list(
            session.scalars(
                select(StrategyStateTransition).order_by(StrategyStateTransition.id)
            )
        )

    assert state.status == StrategyStatus.ACTIVE.value
    assert state.version == 3
    assert state.recovered_at is not None
    assert [
        (row.from_status, row.to_status, row.reason, row.actor) for row in transitions
    ] == [
        (StrategyStatus.READY.value, StrategyStatus.ACTIVE.value, None, "operator"),
        (
            StrategyStatus.ACTIVE.value,
            StrategyStatus.STOPPED.value,
            "pause",
            "operator",
        ),
        (
            StrategyStatus.ERROR.value,
            StrategyStatus.ACTIVE.value,
            "operator reset",
            "operator",
        ),
    ]


def test_restart_restore_warms_and_blocks_duplicate_entry_with_real_session(
    engine_factory,
    sqlite_lifecycle_session_factory,
):
    session_factory = sqlite_lifecycle_session_factory
    with session_factory() as session:
        session.add(Strategy(id="s1", name="Strategy 1"))
        session.add_all(make_orm_candles())
        session.add(
            StrategyState(
                strategy_id="s1",
                status=StrategyStatus.ACTIVE.value,
                config_json='{"product_id":"BINANCE:BTCUSDT-PERP"}',
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
    assert audits[0].risk_message is not None
    assert "existing_position_entry_duplicate" in audits[0].risk_message
