"""Actual main-to-owner MODEL time composition and registrar lifecycle checks."""

from __future__ import annotations

import os
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.core.spider_management.orm import SpiderRuntimeDelivery
from src.core.spider_management.runtime_owner import RuntimeScopeKey, SpiderRuntimeComposition
from test_spider_financial_delivery_pg import _install_owner
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_source_market_main_pg import _configure_main, _payload as _market_payload
from test_spider_source_time_pg import _UTC, _model_time, _observation_id, _runtime

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _time_payload(scope: Any, prepared: Any, factory: Any) -> bytes:
    return _model_time(
        scope, prepared, _runtime(factory, scope),
        observation_id=_observation_id("main-source-time-anchor"),
        event="ANCHOR", epoch_id="main-source-time-epoch",
        observed_utc_ms=_UTC, elapsed_ms=1_000,
    )


def test_main_registers_market_then_time_and_cleans_up_in_reverse_order(
    operation_pg: Engine, monkeypatch: Any,
) -> None:
    _owner, scope, _operation_id, _manager, prepared, _initial, confirmation, source = (
        _install_owner(operation_pg)
    )
    factory = sessionmaker(operation_pg)
    events: list[str] = []
    captured: dict[str, Any] = {}
    market = _market_payload(scope, prepared)

    def register_market(receiver: Any):
        events.append("market-register")
        captured["market_result"] = receiver(market)

        def cleanup() -> None:
            events.append("market-cleanup")

        return cleanup

    def register_time(receiver: Any):
        events.append("time-register")
        captured["time_result"] = receiver(_time_payload(scope, prepared, factory))

        def cleanup() -> None:
            events.append("time-cleanup")

        return cleanup

    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}, register_market, register_time
    )
    _configure_main(monkeypatch, operation_pg, composition, events, captured)

    assert events.index("engine-startup") < events.index("market-register")
    assert events.index("market-register") < events.index("time-register")
    assert events.index("time-register") < events.index("consumer-start")
    assert events.index("time-cleanup") < events.index("market-cleanup")
    assert events.index("market-cleanup") < events.index("engine-shutdown")
    assert "adapter-close" in events
    assert events.index("engine-shutdown") < events.index("consumer-stop")
    for name in ("market_result", "time_result"):
        result = captured[name]
        assert result.success and result.data is not None
        assert result.data["delivery_disposition"] == "CONSUMED"
    with factory() as session:
        deliveries = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind.in_(("SOURCE_MARKET", "SOURCE_TIME")),
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        assert [(row.sequence, row.kind, row.disposition) for row in deliveries] == [
            (1, "SOURCE_MARKET", "CONSUMED"),
            (2, "SOURCE_TIME", "CONSUMED"),
        ]


def test_main_attempts_both_source_cleanups_when_one_raises(
    operation_pg: Engine, monkeypatch: Any,
) -> None:
    _owner, scope, _operation_id, _manager, _prepared, _initial, confirmation, source = (
        _install_owner(operation_pg)
    )
    events: list[str] = []
    captured: dict[str, Any] = {}

    def register_market(_receiver: Any):
        def cleanup() -> None:
            events.append("market-cleanup")

        return cleanup

    def register_time(_receiver: Any):
        def cleanup() -> None:
            events.append("time-cleanup")
            raise RuntimeError("synthetic time cleanup failure")

        return cleanup

    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}, register_market, register_time
    )
    _configure_main(monkeypatch, operation_pg, composition, events, captured)
    assert events.count("time-cleanup") == events.count("market-cleanup") == 1
    assert events.index("time-cleanup") < events.index("market-cleanup")
    assert events.index("market-cleanup") < events.index("engine-shutdown")
    assert "adapter-close" in events
    assert events.index("engine-shutdown") < events.index("consumer-stop")


@pytest.mark.parametrize("failure", ("raises", "noncallable-cleanup"))
def test_main_cleans_market_when_second_registration_fails(
    operation_pg: Engine, monkeypatch: Any,
    failure: str,
) -> None:
    _owner, scope, _operation_id, _manager, _prepared, _initial, confirmation, source = (
        _install_owner(operation_pg)
    )
    events: list[str] = []
    captured: dict[str, Any] = {}

    def register_market(_receiver: Any):
        def cleanup() -> None:
            events.append("market-cleanup")

        return cleanup

    def register_time(_receiver: Any) -> Any:
        events.append("time-register")
        if failure == "raises":
            raise RuntimeError("synthetic time registration failure")
        return None

    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}, register_market, register_time
    )
    if failure == "raises":
        with pytest.raises(RuntimeError, match="time registration failure"):
            _configure_main(monkeypatch, operation_pg, composition, events, captured)
    else:
        with pytest.raises(TypeError, match="time source registrar"):
            _configure_main(monkeypatch, operation_pg, composition, events, captured)
    assert events.count("market-cleanup") == 1
    assert "time-cleanup" not in events
    assert "engine-shutdown" in events and "consumer-stop" in events
    assert "adapter-close" in events
    assert events.index("market-cleanup") < events.index("engine-shutdown")


def test_composition_rejects_noncallable_time_registrar() -> None:
    with pytest.raises(ValueError, match="time source registrar"):
        SpiderRuntimeComposition(
            (RuntimeScopeKey("test", "scope", "strategy"),), {},
            register_time_source=cast(Any, object()),
        )


def test_main_without_source_registrars_keeps_ordinary_startup(
    operation_pg: Engine, monkeypatch: Any,
) -> None:
    _owner, scope, _operation_id, _manager, _prepared, _initial, confirmation, source = (
        _install_owner(operation_pg)
    )
    events: list[str] = []
    captured: dict[str, Any] = {}
    composition = SpiderRuntimeComposition((scope,), {confirmation.request_id: source})
    _configure_main(monkeypatch, operation_pg, composition, events, captured)
    assert events.index("engine-startup") < events.index("consumer-start")
    assert events.index("consumer-start") < events.index("engine-shutdown")
    assert events.index("engine-shutdown") < events.index("consumer-stop")
    assert not any("-register" in event or "-cleanup" in event for event in events)
