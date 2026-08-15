from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, cast
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.core.adapters.simulated import SimulatedAdapter
from src.core.clock import BacktestClock
from src.core.execution import ExecutionEngine
from src.core.mocks.account_service import BacktestAccountService
from src.core.models import Candlestick, Signal, SignalType
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
from src.core.product_registry import (
    FeeModel,
    InstrumentSpec,
    is_dated_future_product_id,
    validate_product_id,
)
from src.core.repositories import LiveOrderRepository
from src.strategies.base import BaseStrategy
from src.validation.strategy_evidence import (
    StrategyEvidenceIdentity,
    require_verified_strategy_identity,
)

ET = ZoneInfo("America/New_York")
_RECOVERABLE_STATUSES = {
    "NEW",
    "SUBMITTED_UNCONFIRMED",
    "SUBMITTED",
    "PARTIALLY_FILLED",
}


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


@dataclass(frozen=True, slots=True)
class PaperScenarioReport:
    scenario: str
    driver: str
    strategy: StrategyEvidenceIdentity | None
    restart_unresolved_count: int
    restart_verification_blocked_count: int
    final_unresolved_count: int
    final_verification_blocked_count: int
    final_position_count: int
    final_working_order_count: int
    order_statuses: tuple[tuple[str, str], ...]
    orders: tuple["PaperOrderEvidence", ...]
    fills: tuple["PaperFillEvidence", ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PaperLifecycleReport:
    instrument: "PaperInstrumentEvidence"
    scenarios: tuple[PaperScenarioReport, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument": asdict(self.instrument),
            "scenarios": [scenario.to_dict() for scenario in self.scenarios],
        }


@dataclass(frozen=True, slots=True)
class PaperInstrumentEvidence:
    product_id: str
    quantity_step: str | None
    price_tick: str | None
    multiplier: str | None
    fee_model: str | None


@dataclass(frozen=True, slots=True)
class PaperOrderEvidence:
    order_id: str
    strategy_id: str
    client_order_id: str | None
    order_type: str
    side: str
    quantity: str
    trigger_price: str | None
    status: str
    filled_quantity: str
    filled_price: str | None
    pending_entry_order_id: str | None
    linked_order_id: str | None


@dataclass(frozen=True, slots=True)
class PaperFillEvidence:
    order_id: str
    strategy_id: str
    order_type: str
    side: str
    price: str
    quantity: str
    fee: str | None
    timestamp: int


def run_paper_lifecycle(
    workspace: Path,
    *,
    product_id: str,
    strategy_id: str,
    hard_flat_strategy_factory: Callable[[], BaseStrategy],
) -> PaperLifecycleReport:
    """Exercise protected entry, restart reconciliation, and hard-flat paths."""

    _validate_paper_product(product_id)
    workspace.mkdir(parents=True, exist_ok=True)
    hard_flat_strategy = hard_flat_strategy_factory()
    hard_flat_identity = require_verified_strategy_identity(hard_flat_strategy)
    reports = [
        _run_protection_scenario(
            workspace / f"{scenario}.db",
            scenario=scenario,
            product_id=product_id,
            strategy_id=strategy_id,
        )
        for scenario in ("stop_loss", "take_profit")
    ]
    reports.append(
        _run_hard_flat_scenario(
            workspace / "hard_flat.db",
            product_id=product_id,
            strategy_id=strategy_id,
            strategy=hard_flat_strategy,
            strategy_identity=hard_flat_identity,
        )
    )
    return PaperLifecycleReport(
        instrument=_instrument_evidence(product_id),
        scenarios=tuple(reports),
    )


def _run_protection_scenario(
    database_path: Path,
    *,
    scenario: str,
    product_id: str,
    strategy_id: str,
) -> PaperScenarioReport:
    session_factory = _session_factory(database_path, product_id, strategy_id)
    adapter = _adapter(product_id)
    clock = BacktestClock()
    engine = _execution_engine(session_factory, adapter, clock)
    entry_candle = _candle(product_id, 1_800_000_000_000)
    clock.set_time(entry_candle.timestamp / 1_000)
    entry_id = engine.execute_signal(
        _protected_entry(strategy_id, product_id, entry_candle.timestamp),
        entry_candle,
    )
    if entry_id is None:
        raise AssertionError(f"{scenario} paper entry was not submitted")

    fill_candle = _candle(product_id, entry_candle.timestamp + 300_000)
    clock.set_time(fill_candle.timestamp / 1_000)
    engine.process_market_data(fill_candle)
    _assert_position(adapter, product_id, strategy_id, "LONG")
    _assert_working_protection(adapter, product_id, strategy_id)

    restarted = _execution_engine(session_factory, adapter, clock)
    restart_reconcile = restarted.reconcile_recoverable_client_orders()
    _assert_reconciliation_resolved(
        restart_reconcile,
        context=f"{scenario} restart reconciliation",
    )

    trigger = (
        _candle(
            product_id,
            fill_candle.timestamp + 300_000,
            open_="20000",
            high="20001",
            low="19994",
            close="19995",
        )
        if scenario == "stop_loss"
        else _candle(
            product_id,
            fill_candle.timestamp + 300_000,
            open_="20000",
            high="20006",
            low="19999",
            close="20005",
        )
    )
    clock.set_time(trigger.timestamp / 1_000)
    restarted.process_market_data(trigger)
    return _finalize_report(
        session_factory,
        restarted,
        adapter,
        product_id,
        strategy_id,
        scenario,
        int(restart_reconcile["unresolved_count"]),
        int(restart_reconcile["verification_blocked_count"]),
        driver="synthetic_protected_entry",
        strategy_identity=None,
    )


def _run_hard_flat_scenario(
    database_path: Path,
    *,
    product_id: str,
    strategy_id: str,
    strategy: BaseStrategy,
    strategy_identity: StrategyEvidenceIdentity,
) -> PaperScenarioReport:
    if strategy.strategy_id != strategy_id or strategy.product_id != product_id:
        raise ValueError("hard-flat strategy identity does not match paper scenario")
    session_factory = _session_factory(database_path, product_id, strategy_id)
    adapter = _adapter(product_id)
    clock = BacktestClock()
    engine = _execution_engine(session_factory, adapter, clock)
    decision_bar = _et_candle(product_id, datetime(2026, 9, 8, 16, 35, tzinfo=ET))
    prior_bar = _et_candle(product_id, datetime(2026, 9, 8, 16, 30, tzinfo=ET))

    clock.set_time(prior_bar.timestamp / 1_000)
    entry_id = engine.execute_signal(
        _protected_entry(
            strategy_id,
            product_id,
            prior_bar.timestamp,
            stop_loss=Decimal("19900"),
            take_profit=Decimal("20100"),
        ),
        prior_bar,
    )
    if entry_id is None:
        raise AssertionError("hard-flat paper entry was not submitted")

    clock.set_time(decision_bar.timestamp / 1_000)
    engine.process_market_data(decision_bar)
    _assert_position(adapter, product_id, strategy_id, "LONG")
    if not strategy.sync_position_state("LONG"):
        raise AssertionError("hard-flat strategy rejected authoritative LONG state")
    exit_signal = strategy.on_candle(decision_bar)
    if not isinstance(exit_signal, Signal):
        raise TypeError("hard-flat strategy must return exactly one Signal")
    if exit_signal.type != SignalType.EXIT_LONG:
        raise AssertionError(
            "strategy did not emit EXIT_LONG at 16:40 ET decision time: "
            f"{exit_signal.type}"
        )
    exit_id = engine.execute_signal(exit_signal, decision_bar)
    if exit_id is None:
        raise AssertionError("hard-flat EXIT_LONG was not submitted")

    exit_fill = _et_candle(
        product_id,
        datetime(2026, 9, 8, 16, 40, tzinfo=ET),
    )
    clock.set_time(exit_fill.timestamp / 1_000)
    engine.process_market_data(exit_fill)
    return _finalize_report(
        session_factory,
        engine,
        adapter,
        product_id,
        strategy_id,
        "hard_flat_1640_et",
        0,
        0,
        driver="strategy",
        strategy_identity=strategy_identity,
    )


def _session_factory(
    database_path: Path,
    product_id: str,
    strategy_ids: str | tuple[str, ...],
):
    if database_path.exists():
        raise FileExistsError(
            f"paper evidence database already exists: {database_path}"
        )
    engine = create_engine(f"sqlite:///{database_path}")
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
        table.create(engine, checkfirst=True)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    venue, instrument = product_id.split(":", 1)
    root = instrument.split("-", 1)[0]
    with factory() as session:
        session.add(Exchange(id=venue, name=venue))
        session.add(
            Product(
                id=product_id,
                exchange_id=venue,
                base_asset=root,
                quote_asset="USD",
            )
        )
        if isinstance(strategy_ids, str):
            strategy_ids = (strategy_ids,)
        session.add_all(
            Strategy(id=strategy_id, name=strategy_id) for strategy_id in strategy_ids
        )
        session.commit()
    return factory


def _adapter(product_id: str) -> SimulatedAdapter:
    return SimulatedAdapter(
        initial_balance=Decimal("100000"),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        instrument_spec=_instrument_spec(product_id),
    )


def _instrument_spec(product_id: str) -> InstrumentSpec:
    instrument = product_id.split(":", 1)[1]
    root = instrument.split("-", 1)[0]
    return InstrumentSpec(
        product_id=product_id,
        exchange="rithmic",
        symbol=root,
        base=root,
        quote="USD",
        quantity_step=Decimal("1"),
        price_tick=Decimal("0.25"),
        multiplier=Decimal("2"),
        fee_model=FeeModel.PER_CONTRACT,
    )


def _validate_paper_product(product_id: str) -> None:
    validate_product_id(product_id)
    if not is_dated_future_product_id(product_id) or not product_id.startswith(
        "RITHMIC:MNQ-"
    ):
        raise ValueError(
            "paper lifecycle evidence currently supports only dated Rithmic "
            "MNQ contracts"
        )


def _instrument_evidence(product_id: str) -> PaperInstrumentEvidence:
    spec = _instrument_spec(product_id)
    return PaperInstrumentEvidence(
        product_id=spec.product_id,
        quantity_step=_decimal_text(spec.quantity_step),
        price_tick=_decimal_text(spec.price_tick),
        multiplier=_decimal_text(spec.multiplier),
        fee_model=spec.fee_model.value if spec.fee_model is not None else None,
    )


def _execution_engine(session_factory, adapter, clock) -> ExecutionEngine:
    repository = LiveOrderRepository(db_session_factory=session_factory)
    account_service = BacktestAccountService(adapter=adapter)
    return ExecutionEngine(
        None,
        clock,
        adapter,
        order_repository=repository,
        account_service=account_service,
        is_backtest=True,
        db_session_factory=session_factory,
        audit_external_orders=True,
    )


def _protected_entry(
    strategy_id: str,
    product_id: str,
    timestamp: int,
    *,
    quantity: Decimal = Decimal("1"),
    stop_loss: Decimal = Decimal("19995"),
    take_profit: Decimal = Decimal("20005"),
) -> Signal:
    return Signal(
        strategy_id=strategy_id,
        product_id=product_id,
        timeframe="5m",
        timestamp=timestamp,
        type=SignalType.LONG,
        quantity=quantity,
        stop_loss=stop_loss,
        take_profit=take_profit,
        metadata={"evidence": "paper_lifecycle"},
    )


def _candle(
    product_id: str,
    timestamp: int,
    *,
    open_: str = "20000",
    high: str = "20001",
    low: str = "19999",
    close: str = "20000",
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe="5m",
        timestamp=timestamp,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("100"),
    )


def _et_candle(product_id: str, local_start: datetime) -> Candlestick:
    timestamp = int(local_start.astimezone(UTC).timestamp() * 1_000)
    return _candle(product_id, timestamp)


def _assert_position(
    adapter: SimulatedAdapter,
    product_id: str,
    strategy_id: str,
    side: str,
    quantity: Decimal = Decimal("1"),
) -> None:
    position = adapter.get_position(product_id, strategy_id=strategy_id)
    if position is None or position.side.value != side or position.quantity != quantity:
        raise AssertionError(f"unexpected paper position: {position}")


def _assert_working_protection(
    adapter: SimulatedAdapter,
    product_id: str,
    strategy_id: str,
    quantity: Decimal = Decimal("1"),
) -> None:
    orders = adapter.get_open_orders(product_id, strategy_id)
    types = {order.type for order in orders}
    quantities = {Decimal(str(order.quantity)) for order in orders}
    if types != {"stop_loss", "take_profit"} or quantities != {quantity}:
        raise AssertionError(
            f"paper protection is incomplete: types={types} quantities={quantities}"
        )


def _finalize_report(
    session_factory,
    engine: ExecutionEngine,
    adapter: SimulatedAdapter,
    product_id: str,
    strategy_ids: str | tuple[str, ...],
    scenario: str,
    restart_unresolved_count: int,
    restart_verification_blocked_count: int,
    *,
    driver: str,
    strategy_identity: StrategyEvidenceIdentity | None,
) -> PaperScenarioReport:
    if isinstance(strategy_ids, str):
        strategy_ids = (strategy_ids,)
    final_reconcile = engine.reconcile_recoverable_client_orders()
    _assert_reconciliation_resolved(
        final_reconcile,
        context=f"{scenario} final reconciliation",
    )
    positions = tuple(
        adapter.get_position(product_id, strategy_id=strategy_id)
        for strategy_id in strategy_ids
    )
    working = tuple(
        order
        for strategy_id in strategy_ids
        for order in adapter.get_open_orders(product_id, strategy_id)
    )
    with session_factory() as session:
        orders = list(
            session.scalars(select(Order).order_by(Order.timestamp, Order.id))
        )
        trades = list(
            session.scalars(select(Trade).order_by(Trade.timestamp, Trade.id))
        )
    order_types = {str(order.id): str(order.type) for order in orders}
    order_strategy_ids = {str(order.id): str(order.strategy_id) for order in orders}
    order_evidence = tuple(_order_evidence(order) for order in orders)
    fill_evidence = tuple(
        PaperFillEvidence(
            order_id=str(trade.order_id),
            strategy_id=order_strategy_ids[str(trade.order_id)],
            order_type=order_types[str(trade.order_id)],
            side=str(trade.side),
            price=_required_decimal_text(trade.price),
            quantity=_required_decimal_text(trade.quantity),
            fee=_decimal_text(trade.fee),
            timestamp=int(trade.timestamp),
        )
        for trade in trades
    )
    report = PaperScenarioReport(
        scenario=scenario,
        driver=driver,
        strategy=strategy_identity,
        restart_unresolved_count=restart_unresolved_count,
        restart_verification_blocked_count=restart_verification_blocked_count,
        final_unresolved_count=int(final_reconcile["unresolved_count"]),
        final_verification_blocked_count=int(
            final_reconcile["verification_blocked_count"]
        ),
        final_position_count=sum(position is not None for position in positions),
        final_working_order_count=len(working),
        order_statuses=tuple((order.type, order.status) for order in orders),
        orders=order_evidence,
        fills=fill_evidence,
    )
    if (
        report.final_position_count
        or report.final_working_order_count
        or any(order.status in _RECOVERABLE_STATUSES for order in orders)
    ):
        raise AssertionError(
            f"paper lifecycle did not finish flat and resolved: {report}"
        )
    return report


def _assert_reconciliation_resolved(
    payload: dict,
    *,
    context: str,
) -> None:
    if (
        int(payload["unresolved_count"]) != 0
        or int(payload["verification_blocked_count"]) != 0
    ):
        raise AssertionError(f"{context} was not resolved and verified: {payload}")


def _order_evidence(order: Order) -> PaperOrderEvidence:
    intent = order.intent_payload or {}
    return PaperOrderEvidence(
        order_id=str(order.id),
        strategy_id=str(order.strategy_id),
        client_order_id=(
            None if order.client_order_id is None else str(order.client_order_id)
        ),
        order_type=str(order.type),
        side=str(order.side),
        quantity=_required_decimal_text(cast(Decimal, order.quantity)),
        trigger_price=_decimal_text(cast(Decimal | None, order.trigger_price)),
        status=str(order.status),
        filled_quantity=_required_decimal_text(
            cast(Decimal | None, order.filled_quantity) or Decimal("0")
        ),
        filled_price=_decimal_text(cast(Decimal | None, order.filled_price)),
        pending_entry_order_id=_optional_text(intent.get("pending_entry_order_id")),
        linked_order_id=_optional_text(intent.get("linked_order_id")),
    )


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _required_decimal_text(value: Decimal) -> str:
    text = _decimal_text(value)
    assert text is not None
    return text


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)
