"""Live-like canonical outcome capture over the real application pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Callable
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

import src.core.adapters.simulated as simulated_module
import src.core.order_manager as order_manager_module
from src.core.adapters.simulated import SimulatedAdapter
from src.core.backtest.endpoint_state import EndpointPosition
from src.core.clock import BacktestClock
from src.core.consumer import (
    DataConsumer,
    FENCED_EPHEMERAL_GROUP_CLEANUP,
    FENCED_XACK,
    FENCED_XREADGROUP,
    RELEASE_OWNERSHIP_LEASE,
    RENEW_OWNERSHIP_LEASE,
)
from src.core.engine import StrategyEngine
from src.core.journal import StrategyJournal
from src.core.mocks.account_service import BacktestAccountService
from src.core.models import (
    Candlestick,
    PositionSide,
    Signal,
    SignalType,
    Trade as MarketTrade,
)
from src.core.orm_models import (
    Exchange,
    Order,
    Position,
    Product,
    SignalAudit,
    Strategy,
    SystemEvent,
    Trade,
)
from src.core.repositories import LiveOrderRepository
from src.core.runtime_environment import RuntimeEnvironment
from src.strategies.callable_strategy import CallableStrategy
from src.validation.backtest_capture import (
    BacktestOutcomeCaptureError,
    exact_decimal_subtract,
)
from src.validation.live_like_capture import (
    ConsumerFactory,
    LiveLikeOutcomeCapture,
    LiveLikeOutcomeCaptureError,
    SessionFactory,
)
from src.validation.trading_outcome import TradingOutcome

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]

PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "d0b3_live_like"
TIMEFRAME = "1m"
INITIAL_BALANCE = Decimal("12345678901234567890")
TIMESTAMPS = (
    1_800_000_000_000,
    1_800_000_060_000,
    1_800_000_120_000,
    1_800_000_180_000,
)


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_: object, compiler: object, **kw: object) -> str:
    return "JSON"


class CallbackBoom(RuntimeError):
    pass


class CleanupBoom(RuntimeError):
    pass


class _UuidSequence:
    def __init__(self, values: tuple[str, ...]) -> None:
        self._values = iter(UUID(value) for value in values)

    def __call__(self) -> UUID:
        return next(self._values)


class _DeterministicRedis:
    """Stateful Redis Stream transport; trading semantics stay in real owners."""

    def __init__(
        self,
        messages: tuple[tuple[str, dict[str, str]], ...],
        trace: list[tuple[str, object]],
    ) -> None:
        self.messages = list(messages)
        self.owner: str | None = None
        self.pending = 0
        self.acked: list[str] = []
        self.trace = trace
        self.registered_streams: set[str] = set()
        self.quarantined: set[str] = set()
        self.cleanup_calls = 0

    def set(self, _key: str, value: str, *, nx: bool, px: int) -> bool:
        assert nx is True and px > 0
        if self.owner is not None:
            return False
        self.owner = value
        return True

    def get(self, _key: str) -> str | None:
        return self.owner

    def eval(self, script: str, *args: object) -> object:
        if script == RENEW_OWNERSHIP_LEASE:
            return 1 if self.owner == args[2] else 0
        if script == RELEASE_OWNERSHIP_LEASE:
            if self.owner == args[2]:
                self.owner = None
                return 1
            return 0
        if script == FENCED_XREADGROUP:
            if self.owner != args[3]:
                return [0]
            if not self.messages:
                return [1]
            message_id, fields = self.messages.pop(0)
            self.pending += 1
            self.trace.append(("claim", message_id))
            flattened = [item for key, value in fields.items() for item in (key, value)]
            return [1, [[args[2], [[message_id, flattened]]]]]
        if script == FENCED_XACK:
            if self.owner != args[2]:
                return [0]
            message_id = str(args[5])
            self.pending -= 1
            self.acked.append(message_id)
            self.trace.append(("ack", message_id))
            return [1, 1]
        if script == FENCED_EPHEMERAL_GROUP_CLEANUP:
            if self.owner != args[-2]:
                return [0]
            self.cleanup_calls += 1
            return [1, self.pending, 4 if self.pending else 0]
        raise AssertionError("unexpected Redis script")

    def xgroup_create(
        self, stream: str, group: str, *, id: str, mkstream: bool
    ) -> bool:
        assert stream and group and id == "$" and mkstream is True
        return True

    def xpending(self, _stream: str, _group: str) -> dict[str, int]:
        return {"pending": self.pending}

    def scan_iter(self, *, match: str, _type: str):
        assert match == "stream:market:*" and _type == "stream"
        return iter(())

    def xinfo_groups(self, _stream: str) -> list[object]:
        return []

    def smembers(self, key: str) -> set[str]:
        return (
            self.quarantined if key.endswith(":quarantine") else self.registered_streams
        )

    def sadd(self, key: str, *values: str) -> int:
        target = (
            self.quarantined if key.endswith(":quarantine") else self.registered_streams
        )
        before = len(target)
        target.update(values)
        return len(target) - before

    def time(self) -> tuple[int, int]:
        return 1_800_000_200, 0

    def close(self) -> None:
        return None


class _FaultingConsumer:
    def __init__(
        self,
        inner: DataConsumer,
        *,
        fail_cleanup: bool,
        fail_stop: bool,
        before_projection: Callable[[], None] | None,
    ) -> None:
        self.inner = inner
        self.fail_cleanup = fail_cleanup
        self.fail_stop = fail_stop
        self.before_projection = before_projection

    def acquire_service_ownership(self) -> None:
        self.inner.acquire_service_ownership()

    def start(self) -> None:
        self.inner.start()

    def request_stop(self) -> None:
        self.inner.request_stop()

    def assert_no_unresolved_deliveries(self) -> None:
        self.inner.assert_no_unresolved_deliveries()
        if self.before_projection is not None:
            self.before_projection()

    def cleanup_consumer_group(self) -> None:
        if self.fail_cleanup:
            raise CleanupBoom("cleanup sentinel")
        self.inner.cleanup_consumer_group()

    def stop(self) -> None:
        self.inner.stop()
        if self.fail_stop:
            raise CleanupBoom("stop sentinel")


@dataclass
class _Runtime:
    capture: LiveLikeOutcomeCapture
    engine: StrategyEngine
    adapter: SimulatedAdapter
    clock: BacktestClock
    session_factory: SessionFactory
    consumer_factory: ConsumerFactory
    transport: _DeterministicRedis
    database_engine: Engine
    trace: list[tuple[str, object]]

    def run(self) -> TradingOutcome:
        return self.capture.run(
            engine=self.engine,
            adapter=self.adapter,
            clock=self.clock,
            session_factory=self.session_factory,
            channels=[f"stream:market:binance:btcusdt-perp:{TIMEFRAME}"],
            group_name="d0b3_live_like_capture",
            consumer_factory=self.consumer_factory,
        )

    def raw_ids(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        with self.session_factory() as session:
            order_ids = tuple(
                str(value)
                for value in session.scalars(select(Order.id).order_by(Order.timestamp))
            )
            trade_ids = tuple(
                str(value)
                for value in session.scalars(select(Trade.id).order_by(Trade.timestamp))
            )
        return order_ids, trade_ids

    def close(self) -> None:
        self.database_engine.dispose()


def _candle(timestamp: int, price: str) -> Candlestick:
    value = Decimal(price)
    return Candlestick(
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        timestamp=timestamp,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=Decimal("1"),
    )


def _payload(candle: Candlestick) -> dict[str, str]:
    return {"json": candle.model_dump_json()}


def _make_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    name: str,
    order_and_trade_ids: tuple[str, str, str, str],
    exchange_ids: tuple[str, str],
    fail_timestamp: int | None = None,
    fail_cleanup: bool = False,
    fail_stop: bool = False,
    project_nonflat: bool = False,
    final_price: str = "103",
) -> _Runtime:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-d0b3")
    database_engine = create_engine(
        f"sqlite:///{tmp_path / f'{name}.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in (
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        Order.__table__,
        Trade.__table__,
        Position.__table__,
        SignalAudit.__table__,
        SystemEvent.__table__,
    ):
        table.create(database_engine, checkfirst=True)
    factory = sessionmaker(bind=database_engine, expire_on_commit=False)
    with factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add(Strategy(id=STRATEGY_ID, name=STRATEGY_ID))
        session.commit()

    trace: list[tuple[str, object]] = []
    capture = LiveLikeOutcomeCapture(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT_ID,
        initial_balance=INITIAL_BALANCE,
        expected_deliveries=len(TIMESTAMPS),
        completion_timeout_seconds=2,
        shutdown_timeout_seconds=2,
    )
    original_completion_set = capture._wakeup.set

    def set_completion() -> None:
        trace.append(("completion", capture.processed_deliveries))
        original_completion_set()

    monkeypatch.setattr(capture._wakeup, "set", set_completion)
    prices = ("100", "101", "102", final_price)
    candles = tuple(
        _candle(timestamp, price)
        for timestamp, price in zip(TIMESTAMPS, prices, strict=True)
    )

    def predict(candle: Candlestick) -> Signal | None:
        trace.append(("callback", candle.timestamp))
        if candle.timestamp == fail_timestamp:
            raise CallbackBoom("callback sentinel")
        if candle.timestamp == TIMESTAMPS[0]:
            signal_type = SignalType.LONG
        elif candle.timestamp == TIMESTAMPS[2]:
            signal_type = SignalType.EXIT_LONG
        else:
            return None
        return Signal(
            strategy_id=STRATEGY_ID,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=signal_type,
            quantity=Decimal("1"),
        )

    adapter = SimulatedAdapter(
        initial_balance=INITIAL_BALANCE,
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0.001"),
    )
    engine_redis = MagicMock()
    engine_redis.close.return_value = None
    monkeypatch.setattr("src.core.engine.create_redis_client", lambda: engine_redis)
    clock = BacktestClock()
    engine = StrategyEngine(
        None,
        clock,
        order_repository=LiveOrderRepository(db_session_factory=factory),
        account_service=BacktestAccountService(adapter=adapter),
        adapter=adapter,
        journal=capture.journal,
        db_session_factory=factory,
        is_backtest=True,
        signal_batch_observer=capture.observe_signal_batch,
    )
    engine.add_strategy(CallableStrategy(STRATEGY_ID, predict, PRODUCT_ID, TIMEFRAME))
    transport = _DeterministicRedis(
        tuple((f"{candle.timestamp}-0", _payload(candle)) for candle in candles),
        trace,
    )
    monkeypatch.setattr(
        order_manager_module,
        "uuid",
        SimpleNamespace(uuid4=_UuidSequence(order_and_trade_ids)),
    )
    monkeypatch.setattr(
        simulated_module,
        "uuid",
        SimpleNamespace(uuid4=_UuidSequence(exchange_ids)),
    )
    monkeypatch.setattr(
        "src.core.consumer.create_redis_client",
        lambda: transport,
    )

    def consumer_factory(
        *,
        channels: list[str],
        on_message_callback: Callable[[Candlestick | MarketTrade], None],
        pending_replay_callback: (Callable[[Candlestick | MarketTrade], None] | None),
        runtime_environment: RuntimeEnvironment,
        group_name: str,
        ephemeral_group: bool,
    ) -> _FaultingConsumer:
        def before_projection() -> None:
            monkeypatch.setattr(
                adapter,
                "get_all_positions",
                lambda: (
                    EndpointPosition(
                        strategy_id=STRATEGY_ID,
                        product_id=PRODUCT_ID,
                        side=PositionSide.LONG,
                        quantity=Decimal("1"),
                        average_entry_price=Decimal("101"),
                    ),
                ),
            )

        return _FaultingConsumer(
            DataConsumer(
                channels=channels,
                on_message_callback=on_message_callback,
                pending_replay_callback=pending_replay_callback,
                runtime_environment=runtime_environment,
                group_name=group_name,
                ephemeral_group=ephemeral_group,
            ),
            fail_cleanup=fail_cleanup,
            fail_stop=fail_stop,
            before_projection=before_projection if project_nonflat else None,
        )

    return _Runtime(
        capture=capture,
        engine=engine,
        adapter=adapter,
        clock=clock,
        session_factory=factory,
        consumer_factory=consumer_factory,
        transport=transport,
        database_engine=database_engine,
        trace=trace,
    )


IDS_A = (
    "ffffffff-ffff-ffff-ffff-fffffffffff1",
    "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee1",
    "00000000-0000-0000-0000-000000000001",
    "11111111-1111-1111-1111-111111111111",
)
IDS_B = (
    "dddddddd-dddd-dddd-dddd-ddddddddddd1",
    "cccccccc-cccc-cccc-cccc-ccccccccccc1",
    "00000000-0000-0000-0000-000000000002",
    "22222222-2222-2222-2222-222222222222",
)
EXCHANGE_IDS = (
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1",
)


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    (
        (
            Decimal("1234567890123456789012345678.1"),
            Decimal("1234567890123456789012345678"),
            Decimal("0.1"),
        ),
        (
            Decimal("1234567890123456789012345678.1"),
            Decimal("0.1"),
            Decimal("1234567890123456789012345678"),
        ),
    ),
)
def test_exact_decimal_subtract_is_context_independent(
    left: Decimal,
    right: Decimal,
    expected: Decimal,
) -> None:
    with localcontext() as context:
        context.prec = 6
        low_precision = exact_decimal_subtract(left, right)
    with localcontext() as context:
        context.prec = 60
        high_precision = exact_decimal_subtract(left, right)

    assert low_precision == high_precision == expected


def test_actual_consumer_matcher_sqlite_path_builds_exact_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _make_runtime(
        tmp_path,
        monkeypatch,
        name="actual",
        order_and_trade_ids=IDS_A,
        exchange_ids=EXCHANGE_IDS,
    )
    try:
        outcome = runtime.run()
        order_ids, trade_ids = runtime.raw_ids()
    finally:
        runtime.close()

    assert type(outcome) is TradingOutcome
    assert tuple(signal.signal_type for signal in outcome.signals) == (
        "LONG",
        "NO_SIGNAL",
        "EXIT_LONG",
        "NO_SIGNAL",
    )
    assert len(outcome.order_observations) == 4
    assert len(outcome.fills) == 2
    assert len(outcome.journal) == 4
    assert tuple((fill.side, fill.price) for fill in outcome.fills) == (
        ("buy", Decimal("101")),
        ("sell", Decimal("103")),
    )
    assert not outcome.endpoint_state.positions
    assert not outcome.endpoint_state.working_orders
    assert outcome.endpoint_state.final_mark == Decimal("103")
    assert outcome.endpoint_state.end_timestamp == TIMESTAMPS[-1]
    assert tuple(fill.fee for fill in outcome.fills) == (
        Decimal("0.101"),
        Decimal("0.103"),
    )
    for index, expected_fee in zip(
        (1, 3),
        ("0.101", "0.103"),
        strict=True,
    ):
        data = outcome.journal[index].data_json
        assert type(data) is str
        assert f'"fee",["string","{expected_fee}"]' in data
    assert outcome.financial.fees == Decimal("0.204")
    assert outcome.financial.realized_pnl == Decimal("1.796")
    assert outcome.financial.equity == INITIAL_BALANCE + Decimal("1.796")
    assert order_ids == (IDS_A[0], IDS_A[2])
    assert trade_ids == (IDS_A[1], IDS_A[3])
    assert order_ids[0] > order_ids[1]
    assert trade_ids[0] > trade_ids[1]
    canonical = outcome.canonical_bytes()
    for raw_id in (*order_ids, *trade_ids):
        assert raw_id.encode() not in canonical
    assert runtime.transport.pending == 0
    assert runtime.transport.cleanup_calls == 1
    assert runtime.transport.acked == [f"{timestamp}-0" for timestamp in TIMESTAMPS]
    for timestamp in TIMESTAMPS:
        callback = ("callback", timestamp)
        ack = ("ack", f"{timestamp}-0")
        assert callback in runtime.trace and ack in runtime.trace
        assert runtime.trace.index(callback) < runtime.trace.index(ack)
    completion = ("completion", len(TIMESTAMPS))
    assert completion in runtime.trace
    assert runtime.trace.index(completion) < runtime.trace.index(
        ("ack", f"{TIMESTAMPS[-1]}-0")
    )


def test_raw_identity_and_decimal_context_do_not_change_canonical_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with localcontext() as context:
        context.prec = 6
        first = _make_runtime(
            tmp_path,
            monkeypatch,
            name="precision_low",
            order_and_trade_ids=IDS_A,
            exchange_ids=EXCHANGE_IDS,
        )
        try:
            first_outcome = first.run()
        finally:
            first.close()
    with localcontext() as context:
        context.prec = 60
        second = _make_runtime(
            tmp_path,
            monkeypatch,
            name="precision_high",
            order_and_trade_ids=IDS_B,
            exchange_ids=(EXCHANGE_IDS[1], EXCHANGE_IDS[0]),
        )
        try:
            second_outcome = second.run()
        finally:
            second.close()

    assert first_outcome.canonical_bytes() == second_outcome.canonical_bytes()
    assert first_outcome.sha256() == second_outcome.sha256()
    assert first_outcome.first_difference(second_outcome) is None


def test_money_path_change_changes_canonical_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = _make_runtime(
        tmp_path,
        monkeypatch,
        name="semantic_baseline",
        order_and_trade_ids=IDS_A,
        exchange_ids=EXCHANGE_IDS,
    )
    try:
        baseline_outcome = baseline.run()
    finally:
        baseline.close()
    changed = _make_runtime(
        tmp_path,
        monkeypatch,
        name="semantic_changed",
        order_and_trade_ids=IDS_B,
        exchange_ids=(EXCHANGE_IDS[1], EXCHANGE_IDS[0]),
        final_price="104",
    )
    try:
        changed_outcome = changed.run()
    finally:
        changed.close()

    assert baseline_outcome.sha256() != changed_outcome.sha256()
    difference = baseline_outcome.first_difference(changed_outcome)
    assert difference is not None
    assert difference.path in {
        "$.signals[3].value",
        "$.fills[1].price",
        "$.endpoint_state.final_mark",
        "$.financial.realized_pnl",
        "$.financial.equity",
        "$.journal[3].data_json.price",
    }


def test_nonflat_projection_fails_closed_after_safe_ephemeral_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _make_runtime(
        tmp_path,
        monkeypatch,
        name="nonflat_projection",
        order_and_trade_ids=IDS_A,
        exchange_ids=EXCHANGE_IDS,
        project_nonflat=True,
    )
    try:
        with pytest.raises(LiveLikeOutcomeCaptureError) as captured:
            runtime.run()
    finally:
        runtime.close()

    assert type(captured.value.__cause__) is BacktestOutcomeCaptureError
    assert type(captured.value.__cause__.__cause__) is ValueError
    assert "positions" in str(captured.value.__cause__.__cause__)
    assert runtime.transport.pending == 0
    assert runtime.transport.cleanup_calls == 1


@pytest.mark.parametrize("mismatch", ("adapter", "journal", "session"))
def test_capture_rejects_mixed_execution_owners_before_consumer_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    runtime = _make_runtime(
        tmp_path,
        monkeypatch,
        name=f"owner_{mismatch}",
        order_and_trade_ids=IDS_A,
        exchange_ids=EXCHANGE_IDS,
    )
    adapter = runtime.adapter
    session_factory = runtime.session_factory
    if mismatch == "adapter":
        adapter = SimulatedAdapter(initial_balance=INITIAL_BALANCE)
    elif mismatch == "journal":
        runtime.engine.execution_engine.journal = StrategyJournal(STRATEGY_ID)
    else:
        session_factory = sessionmaker(
            bind=runtime.database_engine,
            expire_on_commit=False,
        )
    try:
        with pytest.raises(LiveLikeOutcomeCaptureError) as captured:
            runtime.capture.run(
                engine=runtime.engine,
                adapter=adapter,
                clock=runtime.clock,
                session_factory=session_factory,
                channels=[f"stream:market:binance:btcusdt-perp:{TIMEFRAME}"],
                group_name="d0b3_owner_mismatch",
                consumer_factory=runtime.consumer_factory,
            )
    finally:
        runtime.close()

    assert captured.value.args == ("live-like outcome capture failed",)
    assert captured.value.__cause__ is None
    assert runtime.transport.cleanup_calls == 0


@pytest.mark.parametrize(
    ("callback_failure", "cleanup_failure"),
    ((False, False), (False, True), (True, False), (True, True)),
)
def test_primary_and_cleanup_failure_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    callback_failure: bool,
    cleanup_failure: bool,
) -> None:
    runtime = _make_runtime(
        tmp_path,
        monkeypatch,
        name=f"matrix_{callback_failure}_{cleanup_failure}",
        order_and_trade_ids=IDS_A,
        exchange_ids=EXCHANGE_IDS,
        fail_timestamp=TIMESTAMPS[1] if callback_failure else None,
        fail_cleanup=cleanup_failure and not callback_failure,
        fail_stop=cleanup_failure and callback_failure,
    )
    try:
        if not callback_failure and not cleanup_failure:
            assert type(runtime.run()) is TradingOutcome
            return
        with pytest.raises(LiveLikeOutcomeCaptureError) as captured:
            runtime.run()
    finally:
        runtime.close()

    cause = captured.value.__cause__
    assert captured.value.args == ("live-like outcome capture failed",)
    if callback_failure:
        assert type(cause) is CallbackBoom
        assert runtime.transport.pending == 1
        assert runtime.transport.cleanup_calls == 0
        if cleanup_failure:
            assert captured.value.__notes__ == [
                "secondary live-like cleanup failure: CleanupBoom"
            ]
    else:
        assert type(cause) is CleanupBoom
        assert runtime.transport.pending == 0
