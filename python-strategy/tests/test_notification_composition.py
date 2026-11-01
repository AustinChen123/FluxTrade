def test_real_strategy_engine_injects_one_unstarted_dispatcher_into_generic_path(
    engine_factory,
):
    from src.core.runtime_capabilities import NoopRuntimeCapabilities

    engine = engine_factory(
        runtime_bootstrap_factory=None,
        runtime_capabilities_factory=None,
    )

    assert engine.notification_dispatcher._state == "new"
    assert isinstance(engine._venue_runtime, NoopRuntimeCapabilities)
    assert engine.execution_engine.notification_sink is engine.notification_dispatcher
    assert (
        engine.execution_engine._order_event_applier.notification_sink
        is engine.notification_dispatcher
    )
    assert engine._generic_order_event_stream.notification_sink is engine.notification_dispatcher

