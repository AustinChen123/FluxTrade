from unittest.mock import MagicMock

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter


def test_real_rithmic_runtime_composition_receives_same_engine_dispatcher(
    engine_factory,
):
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=MagicMock(),
    )
    engine = engine_factory(adapter=adapter, audit_external_orders=True)

    assert engine._venue_runtime.order_event_stream.notification_sink is (
        engine.notification_dispatcher
    )
    assert engine.execution_engine._order_event_applier.notification_sink is (
        engine.notification_dispatcher
    )
    assert engine._venue_runtime.order_event_stream.notification_sink is (
        engine.execution_engine.notification_sink
    )
    owned_reconciler = (
        engine.execution_engine._order_reconciler._owned_order_reconciler
    )
    assert owned_reconciler is not None
    assert (
        owned_reconciler.context.process_exchange_order_event
        == engine.execution_engine.process_exchange_order_event
    )
