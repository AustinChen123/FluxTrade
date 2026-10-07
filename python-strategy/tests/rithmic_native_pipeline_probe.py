"""Synthetic native decode/PyO3 regression bridge, not provider acceptance."""

from decimal import Decimal
from inspect import unwrap

import conftest
from test_execution import _rithmic_sparse_execution

from src.core.adapters.rithmic_order_observation import project_rithmic_order_event
from src.core.adapters.rithmic_runtime_composition import RithmicRuntimeBootstrap
from src.core.execution import ExecutionEngine
from src.core.models import OrderStatus
from src.core.orm_models import Position, Trade
from src.core.repositories import LiveOrderRepository
from src.core.runtime_capabilities import OrderAccountIdentity


def verify_native_pipeline(native_event: object, full_native_event: object) -> None:
    """Apply Rust-decoded partial/full fills and replay after SQL reload.

    Rust supplies the real PyO3 object; only the outbound adapter is mocked.
    Position projection uses the existing SQL test lane, not a Redis/live claim.
    """
    resources = unwrap(conftest.sqlite_order_session_factory)()
    factory = next(resources)
    try:
        clock = unwrap(conftest.mock_clock)()
        adapter = unwrap(conftest.mock_exchange_adapter)()
        engine, repository, order = _rithmic_sparse_execution(
            sqlite_order_session_factory=factory,
            mock_clock=clock,
            mock_exchange_adapter=adapter,
            order_factory=unwrap(conftest.order_factory)(),
        )
        event = project_rithmic_order_event(
            native_event,
            product_id="RITHMIC:NQ-202609",
            client_order_id=getattr(native_event, "client_order_id"),
            native_identity=("CME", "NQU6"),
        )
        assert event.status == "quantity_unresolved"
        assert event.raw is not None
        assert event.raw["quantity"] is None
        assert event.raw["raw_status"] == "OPEN"
        assert Decimal(str(event.raw["unfilled_quantity"])) == Decimal("1")
        first = engine.process_exchange_order_event(event)
        assert first["action"] == "applied"
        assert first["fill_quantity"] == Decimal("1")
        duplicate = engine.process_exchange_order_event(event)
        assert duplicate["action"] == "applied"
        assert duplicate["fill_quantity"] == Decimal("0")
        full = project_rithmic_order_event(
            full_native_event,
            product_id="RITHMIC:NQ-202609",
            client_order_id=getattr(full_native_event, "client_order_id"),
            native_identity=("CME", "NQU6"),
        )
        assert full.status == "filled"
        assert full.cumulative_filled_quantity == Decimal("2")
        assert full.raw is not None
        assert Decimal(str(full.raw["quantity"])) == Decimal("2")
        assert Decimal(str(full.raw["unfilled_quantity"])) == Decimal("0")
        final = engine.process_exchange_order_event(full)
        assert final["action"] == "applied"
        assert final["fill_quantity"] == Decimal("1")
        full_duplicate = engine.process_exchange_order_event(full)
        assert full_duplicate["action"] == "applied"
        assert full_duplicate["fill_quantity"] == Decimal("0")

        reloaded = LiveOrderRepository(
            db_session_factory=factory, account_profile="orders", account_id="ACCOUNT"
        )
        restarted = ExecutionEngine(
            db_session=None,
            clock=clock,
            adapter=adapter,
            order_repository=reloaded,
            db_session_factory=factory,
            is_backtest=True,
            order_event_processor=RithmicRuntimeBootstrap(
                OrderAccountIdentity("orders", "ACCOUNT"), True
            ).process_order_event,
        )
        replay = restarted.process_exchange_order_event(full)
        assert replay["action"] == "applied"
        assert replay["fill_quantity"] == Decimal("0")
        stored = reloaded.get_order(order.id)
        assert stored is not None
        assert stored.status == OrderStatus.FILLED.value
        assert stored.filled_quantity == Decimal("2")
        assert stored.filled_price == Decimal("20000.75")
        with factory() as session:
            trades = session.query(Trade).filter_by(order_id=order.id).all()
            assert [(trade.quantity, trade.price, trade.fee) for trade in trades] == [
                (Decimal("1"), Decimal("20000.25"), Decimal("0")),
                (Decimal("1"), Decimal("20001.25"), Decimal("0")),
            ]
            positions = (
                session.query(Position).filter_by(product_id=order.product_id).all()
            )
            assert [
                (position.quantity, position.entry_price) for position in positions
            ] == [(Decimal("2"), Decimal("20000.75"))]
        assert repository.get_order(order.id) is not None
        adapter.place_order.assert_not_called()
    finally:
        next(resources, None)
