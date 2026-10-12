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
from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, Trade
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
)
from src.core.spider_management.runtime_owner import SpiderRuntimeComposition
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
)
from test_spider_transmission_owner_pg import _normal_n3_run, _saved_group_request
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
                    events: list[str], captured: dict[str, Any],
                    adapter_config: dict[str, object] | None = None) -> None:
    from src import main as main_module
    from src.core.engine import StrategyEngine

    factory = sessionmaker(engine_db)
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "test")
    monkeypatch.setattr(main_module, "SessionLocal", lambda: factory())
    monkeypatch.setattr(
        main_module,
        "_adapter_config_from_env",
        lambda: adapter_config or {"mode": "simulated"},
    )
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
        assert (runtime.last_received_sequence, runtime.last_consumed_sequence) == (2, 2)
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
        assert financial_delivery.disposition == "CONSUMED"
        assert financial_delivery.decision_payload is not None
        assert financial_delivery.decision_sha256 == sha256(
            bytes(financial_delivery.decision_payload)
        ).hexdigest()
        normal_decision = cast(
            dict[str, Any], decode_canonical(bytes(financial_delivery.decision_payload))
        )
        assert (final_runtime.last_received_sequence, final_runtime.last_consumed_sequence) == (2, 2)
        assert final_runtime.source_checkpoint is not None
        assert final_runtime.source_checkpoint_sha256 == sha256(
            bytes(final_runtime.source_checkpoint)
        ).hexdigest()
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
    assert normal_decision["before_checkpoint_sha256"] == after["checkpoint_hash"]
    assert normal_decision["after_checkpoint_sha256"] == final_runtime.source_checkpoint_sha256
    assert final_readback["checkpoint_sha256"] == final_runtime.source_checkpoint_sha256


def test_main_execution_tx_a_to_normal_tx_b_uses_n3_source_and_cold_query_proof(
    operation_pg: Engine, monkeypatch: Any
) -> None:
    """Run the approved N3 fill/late-ACK path through the actual Engine entrypoint."""
    _setup_owner, scope, operation_id, _manager, prepared, _initial, confirmation, source = (
        _install_owner(operation_pg, admitted_run=_normal_n3_run())
    )
    factory = sessionmaker(operation_pg)
    payload = _payload(scope, prepared)
    events: list[str] = []
    captured: dict[str, Any] = {}

    def register(receiver: Any):
        captured["source_market_result"] = receiver(payload)
        return lambda: None

    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}, register
    )

    def actual_fill_before_rest_ack() -> None:
        engine = captured["engine"]
        owner = captured["owner"]
        source_group, request_payload, members = _saved_initial_place_request(
            operation_pg, scope
        )
        captured["source_group"] = source_group
        candidate = next(
            member for member in members
            if cast(dict[str, Any], member["request_payload"])["side"] == "buy"
            and cast(dict[str, Any], member["request_payload"])["px"] == "50"
        )
        captured["filled_action_id"] = candidate["action_id"]
        captured["filled_product_id"] = candidate["product_id"]
        order = engine.execution_engine.order_manager.repo.get_order(
            cast(str, candidate["local_order_id"])
        )
        assert order is not None and order.price == Decimal("50")
        assert order.quantity == Decimal("100")

        class FillThenAck:
            def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
                first = engine.execution_engine.process_exchange_order_event(
                    ExchangeOrderEvent(
                        status="partially_filled", product_id=order.product_id,
                        client_order_id=order.client_order_id,
                        exchange_order_id="main-normal-provider-order",
                        cumulative_filled_quantity=Decimal("60"),
                        cumulative_average_price=Decimal("48"),
                        last_fill_quantity=Decimal("60"), last_fill_price=Decimal("48"),
                        fee=Decimal("0.01"), fee_asset="USDT",
                        last_fill_exchange_trade_id="main-normal-60-at-48",
                    )
                )
                second = engine.execution_engine.process_exchange_order_event(
                    ExchangeOrderEvent(
                        status="filled", product_id=order.product_id,
                        client_order_id=order.client_order_id,
                        exchange_order_id="main-normal-provider-order",
                        cumulative_filled_quantity=Decimal("100"),
                        cumulative_average_price=Decimal("48.4"),
                        last_fill_quantity=Decimal("40"), last_fill_price=Decimal("49"),
                        fee=Decimal("0.02"), fee_asset="USDT",
                        last_fill_exchange_trade_id="main-normal-40-at-49",
                    )
                )
                assert first["action"] == second["action"] == "applied"
                return {
                    "code": "0",
                    "data": [
                        {
                            "clOrdId": member["wire_clOrdId"],
                            "ordId": (
                                "main-normal-provider-order"
                                if member["action_id"] == candidate["action_id"]
                                else f"main-provider-{i}"
                            ),
                            "sCode": "0",
                        }
                        for i, member in enumerate(members)
                    ],
                    "msg": "ACK arrives after complete financial chain",
                }

            def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
                raise AssertionError("TX B records CANCEL only; provider dispatch remains gated")

        result = owner.transmit_initial_place_group(
            scope, source_group, scope.execution_scope_id, request_payload,
            OkxSpiderBatchTransport(cast(Any, FillThenAck())),
        )
        assert result.disposition == "RECORDED"

        with factory() as session:
            plan_id = session.scalar(
                select(SpiderRuntimeScope.initial_plan_id).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                )
            )
            groups = list(dict.fromkeys(session.scalars(
                select(SpiderRuntimeAction.transmission_group)
                .where(
                    SpiderRuntimeAction.plan_id == plan_id,
                    SpiderRuntimeAction.operation == "PLACE",
                    SpiderRuntimeAction.trigger_delivery_id.is_(None),
                    SpiderRuntimeAction.transmission_group.is_not(None),
                )
                .order_by(SpiderRuntimeAction.candidate_ordinal)
            )))
            other_group = next(group for group in groups if group != source_group)
            other_aliases = set(session.scalars(
                select(SpiderRuntimeBinding.source_client_order_id)
                .join(SpiderRuntimeAction, SpiderRuntimeAction.action_id == SpiderRuntimeBinding.action_id)
                .where(
                    SpiderRuntimeAction.plan_id == plan_id,
                    SpiderRuntimeAction.transmission_group == other_group,
                )
            ))
        other_payload, _other_members = _saved_group_request(
            operation_pg, scope, cast(int, other_group)
        )

        class AckOnly:
            def privatePostTradeBatchOrders(
                self, orders: list[dict[str, object]]
            ) -> object:
                return {
                    "code": "0",
                    "data": [
                        {"clOrdId": row["clOrdId"], "ordId": f"main-b-{i}", "sCode": "0"}
                        for i, row in enumerate(orders)
                    ],
                    "msg": "second original product group acknowledged",
                }

        other_result = owner.transmit_initial_place_group(
            scope,
            cast(int, other_group),
            scope.execution_scope_id,
            other_payload,
            OkxSpiderBatchTransport(cast(Any, AckOnly())),
        )
        assert other_result.disposition == "RECORDED"
        captured["other_aliases"] = other_aliases
        captured["filled_order_id"] = order.id

    captured["on_consumer_start"] = actual_fill_before_rest_ack
    _configure_main(
        monkeypatch,
        operation_pg,
        composition,
        events,
        captured,
        adapter_config={
            "mode": "simulated",
            "account_profile": "okx",
            "account_id": "account-main",
        },
    )
    assert captured["source_market_result"].success is True
    assert captured["source_market_result"].data["delivery_disposition"] == "CONSUMED"

    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).scalar_one()
        rows = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind.in_(["FILL", "ORDER_STATE", "TRANSPORT_RESULT"]),
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        assert rows and all(row.disposition == "CONSUMED" for row in rows)
        decisions = [
            cast(dict[str, Any], decode_canonical(bytes(row.decision_payload)))
            for row in rows if row.decision_payload is not None
        ]
        full = [item for item in decisions if item["full_callback_key"] is not None]
        assert len(full) == 1
        late_ack = next(item for item in decisions if item["trigger_kind"] == "TRANSPORT_RESULT")
        late_ack_input = cast(dict[str, Any], late_ack["source_input"])
        assert late_ack_input["type"] == "REST_ACK"
        ack_aliases = {
            row["clOrdId"] for row in cast(list[dict[str, Any]], late_ack_input["entries"])
        }
        expected_live_aliases = set(session.scalars(
            select(SpiderRuntimeBinding.source_client_order_id)
            .join(SpiderRuntimeAction, SpiderRuntimeAction.action_id == SpiderRuntimeBinding.action_id)
            .where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.transmission_group == captured["source_group"],
                SpiderRuntimeAction.action_id != captured["filled_action_id"],
            )
        ))
        assert expected_live_aliases <= ack_aliases
        assert "0000015006" not in ack_aliases
        all_ack_aliases = {
            entry["clOrdId"]
            for item in decisions
            if item["trigger_kind"] == "TRANSPORT_RESULT"
            for entry in cast(
                list[dict[str, Any]],
                cast(dict[str, Any], item["source_input"])["entries"],
            )
        }
        assert cast(set[str], captured["other_aliases"]) <= all_ack_aliases
        source_order = cast(dict[str, Any], full[0]["source_input"]["order"])
        assert (source_order["px"], source_order["fillPx"], source_order["accFillSz"]) == (
            "50", "49", "100"
        )
        source_effects = [
            row for row in cast(list[dict[str, Any]], full[0]["events"])
            if row["kind"] in {"cancel", "send"}
        ]
        assert [row["kind"] for row in source_effects] == ["cancel", "send"]
        assert [row["ordId"] for row in source_effects[0]["orders"]] == ["0000065010"]
        assert [
            (row["px"], row["sz"], row["side"], row["clOrdId"])
            for row in source_effects[1]["orders"]
        ] == [
            ("6.25", "355.555", "buy", "0000115003"),
            ("100", "150", "sell", "0000125007"),
        ]
        assert runtime.last_received_sequence == runtime.last_consumed_sequence
        provider_ids = session.execute(
            select(Order.exchange_order_id).where(Order.id == captured["filled_order_id"])
        ).scalar_one()
        provider_trade_ids = set(session.scalars(
            select(Trade.exchange_trade_id).where(Trade.order_id == captured["filled_order_id"])
        ))
        assert provider_ids == "main-normal-provider-order"
        assert provider_trade_ids == {"main-normal-60-at-48", "main-normal-40-at-49"}

    from src.control_plane.spider_order_query import SpiderOrderQueryService

    service = SpiderOrderQueryService(
        factory, environment=scope.environment, execution_scope_id=scope.execution_scope_id
    )
    query_status, query_body = service.orders(f"strategy_id={scope.strategy_id}")
    assert query_status == 200, query_body
    queried_ids = {
        row["local_order_id"]
        for row in cast(list[dict[str, Any]], query_body["orders"])
    }
    with factory() as session:
        derived_ids = set(session.scalars(
            select(SpiderRuntimeAction.local_order_id).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.operation == "PLACE",
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
            )
        ))
        cancel = session.execute(
            select(SpiderRuntimeAction).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.operation == "CANCEL",
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
            )
        ).scalar_one()
        parent = session.get(SpiderRuntimeAction, cancel.parent_action_id)
        assert parent is not None and parent.operation == "PLACE"
        assert cancel.local_order_id == parent.local_order_id
        assert session.get(SpiderRuntimeBinding, cancel.action_id) is None
        cold_owner = captured["owner"]
    assert derived_ids and derived_ids <= queried_ids
    cold_readback = cold_owner.readback(scope, operation_id)
    assert cold_readback is not None and cold_readback["trading_ready"] is False


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
