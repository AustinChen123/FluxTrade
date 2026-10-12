"""Actual main/Engine composition delivery for SOURCE_MARKET."""

from __future__ import annotations

from decimal import Decimal
from hashlib import sha256
import os
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, Trade
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
)
from src.core.spider_management.runtime_owner import SpiderRuntimeComposition
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import _install_owner
from test_spider_operation_request_pg import operation_pg as _operation_pg

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
fresh_pg_db = _migration_fresh_pg_db
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _payload(scope: Any, prepared: Any) -> bytes:
    checkpoint = cast(dict[str, Any], decode_canonical(prepared.build_checkpoint_bytes))
    markets = []
    for row in cast(list[dict[str, Any]], checkpoint["markets"]):
        value = dict(cast(dict[str, Any], row["value"]))
        value["price"] = {"tag": "DECIMAL", "value": "123.45"}
        value["ratioHL"] = {"tag": "DECIMAL", "value": "0.625"}
        markets.append({"product_id": row["product_id"], "value": value})
    return canonical_bytes({
        "schema_version": "SPIDER_SOURCE_MARKET_V1",
        "scope": {
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
        },
        "effective_identity": {
            "generation": 1,
            "confirmation_request_id": prepared.runtime_selection[
                "confirmation_request_id"
            ],
            "configuration_sha256": prepared.runtime_selection[
                "configuration_sha256"
            ],
        },
        "snapshot_id": "a" * 64,
        "observed_at_ms": 1_800_000_000_000,
        "snapshot_as_of_ms": 1_800_000_000_000,
        "provenance": {
            "kind": "MODEL",
            "evidence_sha256": prepared.historical_input_artifact_sha256,
        },
        "markets": markets,
    })


def _snapshot(factory: Any, scope: Any) -> dict[str, Any]:
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == runtime.initial_plan_id
            )
        ).scalar_one()
        return {
            "checkpoint": bytes(runtime.source_checkpoint),
            "checkpoint_hash": runtime.source_checkpoint_sha256,
            "received": runtime.last_received_sequence,
            "consumed": runtime.last_consumed_sequence,
            "plan": (
                plan.plan_status,
                bytes(plan.original_build_checkpoint),
                plan.original_build_checkpoint_sha256,
                plan.manifest_sha256,
                bytes(plan.missing_members),
            ),
            "orders": tuple(tuple(getattr(row, column.name) for column in Order.__table__.columns)
                for row in session.scalars(
                select(Order).where(Order.strategy_id == scope.strategy_id)
                .order_by(Order.timestamp, Order.id)
            ).all()),
            "trades": tuple(tuple(getattr(row, column.name) for column in Trade.__table__.columns)
                for row in session.scalars(
                select(Trade).join(Order, Trade.order_id == Order.id)
                .where(Order.strategy_id == scope.strategy_id)
                .order_by(Trade.timestamp, Trade.id)
            ).all()),
            "actions": tuple(tuple(getattr(row, column.name) for column in SpiderRuntimeAction.__table__.columns)
                for row in session.scalars(
                select(SpiderRuntimeAction).where(
                    SpiderRuntimeAction.strategy_id == scope.strategy_id
                ).order_by(SpiderRuntimeAction.action_id)
            ).all()),
            "deliveries": tuple(tuple(getattr(row, column.name) for column in SpiderRuntimeDelivery.__table__.columns)
                for row in session.scalars(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id
                ).order_by(SpiderRuntimeDelivery.sequence)
            ).all()),
            "tape": tuple(tuple(getattr(row, column.name) for column in SpiderRuntimeTapeIndex.__table__.columns)
                for row in session.scalars(
                select(SpiderRuntimeTapeIndex).where(
                    SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
                ).order_by(SpiderRuntimeTapeIndex.ordinal)
            ).all()),
        }


def _configure_main(monkeypatch: Any, engine_db: Engine, composition: Any,
                    events: list[str], captured: dict[str, Any]) -> None:
    from src import main as main_module
    from src.core.engine import StrategyEngine

    factory = sessionmaker(engine_db)
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "test")
    monkeypatch.setattr(main_module, "SessionLocal", lambda: factory())
    monkeypatch.setattr(main_module, "_adapter_config_from_env", lambda: {"mode": "simulated"})
    monkeypatch.setattr(main_module, "_validate_runtime_config", lambda *_a, **_k: None)
    monkeypatch.setattr(main_module, "_strategy_artifact_loader_from_env", lambda: lambda: {})
    monkeypatch.setattr(main_module, "runtime_factories_for_config", lambda _c: (None, None))
    monkeypatch.setattr(main_module, "configure_metrics", lambda **_k: None)
    monkeypatch.setattr(main_module.signal, "signal", lambda *_a: None)

    class RedisProbe:
        def hgetall(self, _key: str) -> dict[object, object]:
            return {}

        def register_script(self, _script: str):
            return lambda *, args: None

        def publish(self, _channel: str, _message: str) -> int:
            return 1

        def close(self) -> None:
            pass

    monkeypatch.setattr("src.core.engine.create_redis_client", RedisProbe)
    monkeypatch.setattr("src.core.order_manager.create_redis_client", RedisProbe)

    class AdapterProbe:
        def supports_runtime_reconciliation(self) -> bool:
            return False

        def close(self) -> None:
            events.append("adapter-close")

    monkeypatch.setattr(main_module, "create_adapter", lambda *_a, **_k: AdapterProbe())

    class ConsumerProbe:
        def __init__(self, **_kwargs: Any):
            pass

        def acquire_service_ownership(self) -> None:
            pass

        def assert_service_ownership(self) -> None:
            pass

        def configure_callbacks(self, **_kwargs: Any) -> None:
            pass

        def start(self) -> None:
            events.append("consumer-start")
            callback = captured.get("on_consumer_start")
            if callback is not None:
                callback()

        def request_stop(self) -> None:
            pass

        def stop(self) -> None:
            events.append("consumer-stop")

    monkeypatch.setattr(main_module, "DataConsumer", ConsumerProbe)
    original_startup = StrategyEngine.startup
    original_shutdown = StrategyEngine.shutdown
    original_init = StrategyEngine.__init__
    original_owner_type = main_module.SpiderRuntimeOwner

    def capture_engine(self: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        captured["engine"] = self

    def capture_owner(*args: Any, **kwargs: Any):
        owner = original_owner_type(*args, **kwargs)
        captured["owner"] = owner
        return owner

    monkeypatch.setattr(main_module, "SpiderRuntimeOwner", capture_owner)
    monkeypatch.setattr(StrategyEngine, "__init__", capture_engine)

    def startup(self: Any) -> None:
        original_startup(self)
        events.append("engine-startup")

    def shutdown(self: Any, *, clean_exit: bool = False) -> None:
        events.append("engine-shutdown")
        original_shutdown(self, clean_exit=clean_exit)

    monkeypatch.setattr(StrategyEngine, "startup", startup)
    monkeypatch.setattr(StrategyEngine, "shutdown", shutdown)
    for method in (
        "_halt_for_kill_switch", "_start_command_listener", "_check_system_state",
        "_reconcile_startup_balance", "_start_strategy_state_subscriber_on_startup",
        "_start_exchange_order_event_stream", "_reconcile_recoverable_orders_on_startup",
        "_start_heartbeat", "scan_strategies",
    ):
        monkeypatch.setattr(StrategyEngine, method, lambda *_a, **_k: None)

    main_module.main(composition)


def test_main_real_engine_registers_composed_source_market_and_cleans_up(
    operation_pg: Engine, monkeypatch: Any
) -> None:
    _initial_owner, scope, _operation_id, _om, prepared, _initial, confirmation, source = (
        _install_owner(operation_pg)
    )
    factory = sessionmaker(operation_pg)
    before = _snapshot(factory, scope)
    payload = _payload(scope, prepared)
    events: list[str] = []
    captured: dict[str, Any] = {}

    def persist_financial_fill() -> None:
        engine = captured["engine"]
        with factory() as session:
            order_id = session.scalar(
                select(Order.id).where(Order.strategy_id == scope.strategy_id)
                .order_by(Order.timestamp, Order.id).limit(1)
            )
        assert order_id is not None
        order = engine.execution_engine.order_manager.repo.get_order(order_id)
        assert order is not None
        quantity = order.quantity / Decimal(2)
        execution_result = engine.execution_engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partially_filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                cumulative_filled_quantity=quantity,
                cumulative_average_price=cast(Decimal, order.price),
                last_fill_quantity=quantity,
                last_fill_price=cast(Decimal, order.price),
                fee=Decimal("0.01"),
                fee_asset="USDT",
                last_fill_exchange_trade_id="main-source-market-fill",
            )
        )
        assert execution_result["action"] == "applied"
        captured["financial_order_id"] = order_id
        captured["financial_order_values"] = (
            order.product_id,
            order.side,
            cast(Decimal, order.price),
            quantity,
        )

    def register(receiver: Any):
        events.append("registered")
        captured["receiver"] = receiver
        bad_doc = cast(dict[str, Any], decode_canonical(payload))
        bad_markets = cast(list[dict[str, Any]], bad_doc["markets"])
        bad_value = cast(dict[str, Any], bad_markets[0]["value"])
        bad_value["ratioHL"] = {"tag": "STRING", "value": "0.625"}
        rejected = receiver(canonical_bytes(bad_doc))
        captured["rejected"] = rejected
        captured["result"] = receiver(payload)
        captured["market_snapshot"] = _snapshot(factory, scope)

        def cleanup() -> None:
            events.append("source-cleanup")

        return cleanup

    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}, register
    )
    captured["on_consumer_start"] = persist_financial_fill
    _configure_main(monkeypatch, operation_pg, composition, events, captured)
    assert events.index("engine-startup") < events.index("registered")
    assert events.index("registered") < events.index("consumer-start")
    assert events.index("registered") < events.index("source-cleanup")
    assert events.index("source-cleanup") < events.index("engine-shutdown")
    assert events.index("engine-shutdown") < events.index("consumer-stop")
    assert events.count("registered") == events.count("source-cleanup") == 1
    assert callable(captured["receiver"])
    rejected = captured["rejected"]
    assert rejected.success is False and rejected.data is not None
    assert rejected.data["disposition"] == "REJECTED"
    result = captured["result"]
    assert result.success is True and result.data is not None
    assert result.data["delivery_sequence"] == 1
    assert result.data["delivery_disposition"] == "CONSUMED"
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        delivery = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence == 1,
            )
        ).scalar_one()
        tape = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id,
                SpiderRuntimeTapeIndex.ordinal == 1,
            )
        ).scalar_one()
        assert delivery.source_payload == payload
        assert delivery.content_sha256 == sha256(payload).hexdigest()
        assert delivery.kind == "SOURCE_MARKET" and delivery.disposition == "CONSUMED"
        assert delivery.local_order_id is None and delivery.action_id is None
        assert delivery.trade_id is None and delivery.role is None
        assert tape.kind == "DELIVERY" and tape.delivery_sequence == 1
        assert (runtime.last_received_sequence, runtime.last_consumed_sequence) == (2, 1)
    after = cast(dict[str, Any], captured["market_snapshot"])
    assert (after["received"], after["consumed"]) == (1, 1)
    before_checkpoint = cast(dict[str, Any], decode_canonical(before["checkpoint"]))
    after_checkpoint = cast(dict[str, Any], decode_canonical(after["checkpoint"]))
    expected = cast(dict[str, Any], decode_canonical(payload))["markets"]
    assert len(expected) == 2
    assert after_checkpoint["markets"] == expected
    assert {k: v for k, v in after_checkpoint.items() if k != "markets"} == {
        k: v for k, v in before_checkpoint.items() if k != "markets"
    }
    assert after["checkpoint_hash"] == sha256(after["checkpoint"]).hexdigest()
    assert after["plan"] == before["plan"]
    assert after["orders"] == before["orders"]
    assert after["trades"] == before["trades"]
    assert after["actions"] == before["actions"]
    assert after["deliveries"] == before["deliveries"] + after["deliveries"][-1:]
    assert after["tape"] == before["tape"] + after["tape"][-1:]
    readback = captured["owner"].readback(scope, _operation_id)
    assert readback is not None and readback["trading_ready"] is False
    with factory() as session:
        persisted_trade = session.execute(
            select(Trade).where(Trade.order_id == captured["financial_order_id"])
        ).scalar_one()
        financial_delivery = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.trade_id == persisted_trade.id,
            )
        ).scalar_one()
        final_runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        receipt_bytes = bytes(financial_delivery.source_payload)
        receipt = cast(dict[str, Any], decode_canonical(receipt_bytes))
        assert financial_delivery.kind == "FILL"
        assert financial_delivery.disposition == "PENDING"
        assert (final_runtime.last_received_sequence, final_runtime.last_consumed_sequence) == (2, 1)
        assert bytes(final_runtime.source_checkpoint) == after["checkpoint"]
        assert final_runtime.source_checkpoint_sha256 == after["checkpoint_hash"]
        assert persisted_trade.exchange_trade_id == "main-source-market-fill"
        assert (
            persisted_trade.order_id,
            persisted_trade.product_id,
            persisted_trade.side,
            persisted_trade.price,
            persisted_trade.quantity,
            persisted_trade.fee,
            persisted_trade.fee_asset,
        ) == (
            captured["financial_order_id"],
            *captured["financial_order_values"],
            Decimal("0.01"),
            "USDT",
        )
        assert receipt["schema_version"] == "SPIDER_FINANCIAL_RECEIPT_V1"
        assert receipt["kind"] == "FILL" and receipt["trade_id"] == persisted_trade.id
        assert receipt["binding"]["local_order_id"] == captured["financial_order_id"]
        assert canonical_bytes(receipt) == receipt_bytes
        assert financial_delivery.content_sha256 == sha256(receipt_bytes).hexdigest()
    final_readback = captured["owner"].readback(scope, _operation_id)
    assert final_readback is not None
    assert final_readback["trading_ready"] is False
    assert final_readback["checkpoint_sha256"] == after["checkpoint_hash"]


def test_main_market_registration_failure_uses_existing_cleanup(
    operation_pg: Engine, monkeypatch: Any
) -> None:
    _owner, scope, _op, _om, _prepared, _initial, confirmation, source = _install_owner(operation_pg)
    events: list[str] = []
    captured: dict[str, Any] = {}

    def fail(_receiver: Any):
        events.append("register-failed")
        raise RuntimeError("registrar failed")

    composition = SpiderRuntimeComposition((scope,), {confirmation.request_id: source}, fail)
    with pytest.raises(RuntimeError, match="registrar failed"):
        _configure_main(monkeypatch, operation_pg, composition, events, captured)
    assert events.index("engine-startup") < events.index("register-failed")
    assert "engine-shutdown" in events and "adapter-close" in events
    assert "consumer-stop" in events
    assert "source-cleanup" not in events


def test_main_market_cleanup_failure_does_not_skip_engine_or_consumer_cleanup(
    operation_pg: Engine, monkeypatch: Any
) -> None:
    from src import main as main_module

    _owner, scope, _op, _om, _prepared, _initial, confirmation, source = _install_owner(operation_pg)
    events: list[str] = []
    captured: dict[str, Any] = {}

    def register(_receiver: Any):
        def cleanup() -> None:
            events.append("source-cleanup")
            raise RuntimeError("cleanup failed")
        return cleanup

    composition = SpiderRuntimeComposition((scope,), {confirmation.request_id: source}, register)
    warning_calls: list[tuple[str, tuple[Any, ...]]] = []
    monkeypatch.setattr(
        main_module.logger,
        "warning",
        lambda message, *args: warning_calls.append((message, args)),
    )
    _configure_main(monkeypatch, operation_pg, composition, events, captured)
    assert events.index("source-cleanup") < events.index("engine-shutdown")
    assert events.index("engine-shutdown") < events.index("consumer-stop")
    assert warning_calls == [
        ("Spider market source cleanup failed: %s", ("RuntimeError",))
    ]
