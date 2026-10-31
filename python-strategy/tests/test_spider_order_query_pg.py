"""Real Control Plane + PostgreSQL + browser verification for local Spider facts."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
from threading import Thread
from time import sleep
from urllib.parse import quote
from urllib.request import Request, urlopen
from decimal import Decimal
from hashlib import sha256
from typing import cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event as sqlalchemy_event, select, update
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.main import build_control_plane_app
from src.control_plane.server import make_handler
from src.control_plane.spider_order_query import SpiderOrderQueryService
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, Trade
from src.core.decimal_math import canonical_decimal_text
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
)
from src.control_plane import spider_order_query as spider_order_query_module
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_financial_delivery_pg import _install_owner

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]

LOGIN = "spider-reader@example.invalid"


def _object_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AssertionError("expected object DTO")
    return cast(dict[str, object], value)


def _object_rows(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise AssertionError("expected DTO array")
    return [_object_dict(row) for row in value]


def _string_field(value: dict[str, object], key: str) -> str:
    field = value.get(key)
    if not isinstance(field, str):
        raise AssertionError(f"expected string DTO field: {key}")
    return field


def _facts_snapshot(engine, strategy_id: str):
    with Session(engine) as session:
        orders = tuple(
            session.execute(
                select(
                    Order.id,
                    Order.status,
                    Order.timestamp,
                    Order.filled_quantity,
                    Order.filled_price,
                    Order.exchange_order_id,
                    Order.last_reconciled_at,
                )
                .where(Order.strategy_id == strategy_id)
                .order_by(Order.id.collate("C"))
            ).all()
        )
        trades = tuple(
            session.execute(
                select(
                    Trade.id,
                    Trade.order_id,
                    Trade.exchange_trade_id,
                    Trade.product_id,
                    Trade.side,
                    Trade.price,
                    Trade.quantity,
                    Trade.fee,
                    Trade.fee_asset,
                    Trade.timestamp,
                )
                .join(Order, Order.id == Trade.order_id)
                .where(Order.strategy_id == strategy_id)
                .order_by(Trade.id.collate("C"))
            ).all()
        )
        return orders, trades


def _runtime_snapshot(engine, scope):
    with Session(engine) as session:
        scope_row = session.execute(
            select(
                SpiderRuntimeScope.initial_plan_id,
                SpiderRuntimeScope.current_operation_id,
                SpiderRuntimeScope.generation,
                SpiderRuntimeScope.configuration_sha256,
            ).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).one()
        actions = tuple(
            session.execute(
                select(
                    SpiderRuntimeAction.action_id,
                    SpiderRuntimeAction.outcome_state,
                    SpiderRuntimeAction.local_order_id,
                )
                .where(
                    SpiderRuntimeAction.environment == scope.environment,
                    SpiderRuntimeAction.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeAction.strategy_id == scope.strategy_id,
                )
                .order_by(SpiderRuntimeAction.action_id.collate("C"))
            ).all()
        )
        bindings = tuple(
            session.execute(
                select(
                    SpiderRuntimeBinding.action_id,
                    SpiderRuntimeBinding.local_order_id,
                    SpiderRuntimeBinding.source_product_id,
                )
                .where(
                    SpiderRuntimeBinding.environment == scope.environment,
                    SpiderRuntimeBinding.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeBinding.strategy_id == scope.strategy_id,
                )
                .order_by(SpiderRuntimeBinding.action_id.collate("C"))
            ).all()
        )
        return scope_row, actions, bindings


def _headers(origin: str | None = None) -> dict[str, str]:
    result = {
        "Tailscale-User-Login": LOGIN,
        "Tailscale-App-Capabilities": "{}",
    }
    if origin is not None:
        result["Origin"] = origin
    return result


def _query_service(engine, scope) -> SpiderOrderQueryService:
    return SpiderOrderQueryService(
        sessionmaker(engine),
        environment=scope.environment,
        execution_scope_id=scope.execution_scope_id,
    )


def _installed_query_scope(engine, monkeypatch):
    owner, scope, operation_id, order_manager, *_rest = _install_owner(engine)
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", scope.environment)
    return owner, scope, operation_id, order_manager


def _add_read_scope_only_generation_two(engine, scope) -> None:
    """Add one immutable current-generation fact while retaining gen-1 facts.

    This is a query fixture only; it deliberately does not exercise or emulate a
    runtime lifecycle transition.
    """
    with Session(engine) as session, session.begin():
        scope_row = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        source_plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == scope_row.initial_plan_id
            )
        ).scalar_one()
        source_action = session.execute(
            select(SpiderRuntimeAction)
            .where(
                SpiderRuntimeAction.plan_id == source_plan.plan_id,
                SpiderRuntimeAction.local_order_id.is_not(None),
            )
            .order_by(SpiderRuntimeAction.action_id.collate("C"))
            .limit(1)
        ).scalar_one()
        source_order = session.execute(
            select(Order).where(Order.id == source_action.local_order_id)
        ).scalar_one()
        source_binding = session.execute(
            select(SpiderRuntimeBinding).where(
                SpiderRuntimeBinding.action_id == source_action.action_id
            )
        ).scalar_one()
        plan_id = sha256(b"I04_READ_SCOPE_ONLY_GENERATION_TWO_PLAN").hexdigest()
        action_id = sha256(b"I04_READ_SCOPE_ONLY_GENERATION_TWO_ACTION").hexdigest()
        local_order_id = "query-generation-two-order"
        source_client = "query-generation-two-source"
        local_client = "query-generation-two-local"
        manifest = _object_dict(decode_canonical(source_plan.manifest_payload))
        manifest["generation"] = 2
        manifest_bytes = canonical_bytes(manifest)
        plan_values = {
            column.key: getattr(source_plan, column.key)
            for column in SpiderRuntimePlan.__table__.columns
        }
        plan_values.update(
            plan_id=plan_id,
            generation=2,
            manifest_payload=manifest_bytes,
            manifest_sha256=sha256(manifest_bytes).hexdigest(),
        )
        session.add(SpiderRuntimePlan(**plan_values))
        order_values = {
            column.key: getattr(source_order, column.key)
            for column in Order.__table__.columns
        }
        order_values.update(
            id=local_order_id,
            client_order_id=local_client,
            exchange_order_id=None,
        )
        payload = _object_dict(decode_canonical(source_action.original_payload))
        payload["clOrdId"] = source_client
        original_payload = canonical_bytes(payload)
        binding_bytes = canonical_bytes(
            {
                "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
                "action_id": action_id,
                "product_id": source_binding.canonical_product_id,
            }
        )
        intent = dict(order_values["intent_payload"])
        intent.update(
            action_id=action_id,
            source_client_order_id=source_client,
            local_client_order_id=local_client,
            provider_binding_bytes_hex=binding_bytes.hex(),
            provider_binding_sha256=sha256(binding_bytes).hexdigest(),
            provider_wire_client_order_id=(
                "sp"
                + sha256(b"SPIDER_OKX_CLIENT_BINDING_V1\0" + binding_bytes).hexdigest()[
                    :30
                ]
            ),
        )
        order_values["intent_payload"] = intent
        session.add(Order(**order_values))
        session.flush()
        action_values = {
            column.key: getattr(source_action, column.key)
            for column in SpiderRuntimeAction.__table__.columns
        }
        action_values.update(
            action_id=action_id,
            plan_id=plan_id,
            generation=2,
            local_order_id=local_order_id,
            original_payload=original_payload,
            payload_sha256=sha256(original_payload).hexdigest(),
        )
        session.add(SpiderRuntimeAction(**action_values))
        session.flush()
        binding_values = {
            column.key: getattr(source_binding, column.key)
            for column in SpiderRuntimeBinding.__table__.columns
        }
        binding_values.update(
            action_id=action_id,
            local_order_id=local_order_id,
            source_client_order_id=source_client,
        )
        session.add(SpiderRuntimeBinding(**binding_values))
        scope_row.initial_plan_id = plan_id
        scope_row.generation = 2
        session.flush()


def _get_json(origin: str, path: str, cookie: str) -> dict[str, object]:
    request = Request(
        f"{origin}{path}",
        headers={**_headers(origin), "Cookie": cookie},
        method="GET",
    )
    with urlopen(
        request, context=ssl._create_unverified_context(), timeout=10
    ) as response:
        assert response.status == 200
        assert response.headers.get("Cache-Control") == "no-store"
        return json.loads(response.read())


def test_actual_receipt_orders_and_trades_reach_browser_unchanged(
    operation_pg, mock_clock, mock_exchange_adapter, monkeypatch
):
    engine = operation_pg
    owner, scope, _operation_id, order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    session_factory = sessionmaker(engine)
    with Session(engine) as session:
        order = (
            session.execute(
                select(Order)
                .where(Order.strategy_id == scope.strategy_id)
                .order_by(Order.timestamp.desc(), Order.id.collate("C").asc())
            )
            .scalars()
            .first()
        )
    assert order is not None
    order = order_manager.repo.get_order(order.id)
    assert order is not None
    quantity = order.quantity
    price = order.price
    assert price is not None
    half = quantity / 2
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=session_factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    execution.process_exchange_order_event(
        ExchangeOrderEvent(
            status="partially_filled",
            product_id=order.product_id,
            client_order_id=order.client_order_id,
            cumulative_filled_quantity=half,
            cumulative_average_price=price,
            last_fill_quantity=half,
            last_fill_price=price,
            fee=-Decimal("0.01"),
            fee_asset="USDT",
            last_fill_exchange_trade_id="query-fixture-trade-1",
        )
    )
    execution.process_exchange_order_event(
        ExchangeOrderEvent(
            status="filled",
            product_id=order.product_id,
            client_order_id=order.client_order_id,
            exchange_order_id="query-fixture-order",
            cumulative_filled_quantity=quantity,
            cumulative_average_price=price,
            last_fill_quantity=quantity - half,
            last_fill_price=price,
            fee=Decimal("0"),
            fee_asset="USDT",
            last_fill_exchange_trade_id="query-fixture-trade-2",
        )
    )

    from http.server import ThreadingHTTPServer

    # Use the same configured-scope composition as the service entrypoint.
    auth = BrowserSessionAuth(
        allowed_origin="https://127.0.0.1",
        operator_capability="unused-read-only-capability",
        step_up_capability="unused-step-up-capability",
        owner_login=None,
    )
    redis = MagicMock()
    redis.get.return_value = b"LOCKDOWN"
    redis.pubsub_numsub.return_value = [("cmd:strategy:control", 0)]
    app = build_control_plane_app(
        redis_client=redis,
        db_session_factory=sessionmaker(engine),
        api_key=None,
        browser_auth=auth,
        spider_execution_scope_id=scope.execution_scope_id,
        readiness_probe=lambda: None,
    )
    # Bind an ephemeral loopback TLS listener and serve the already-built UI.
    frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    assert (frontend_dist / "index.html").is_file()
    with tempfile.TemporaryDirectory(prefix="spider-order-browser-") as tmp:
        cert = Path(tmp) / "cert.pem"
        key = Path(tmp) / "key.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-days",
                "1",
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(app, static_dir=frontend_dist)
        )
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        origin = f"https://127.0.0.1:{server.server_port}"
        auth.allowed_origin = origin
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            session_response = app.handle(
                "POST", "/api/v1/auth/session", headers=_headers(origin)
            )
            assert session_response.status_code == 201
            cookie = dict(session_response.headers)["Set-Cookie"].split(";", 1)[0]
            strategy = scope.strategy_id
            lockdown = _get_json(origin, "/ops/kill-switch", cookie)
            assert lockdown["state"] == "LOCKDOWN"
            assert lockdown["redis_state"] == "LOCKDOWN"
            assert lockdown["listener_available"] is False
            before = _facts_snapshot(engine, strategy)
            runtime_before = _runtime_snapshot(engine, scope)
            sql: list[str] = []

            def record_sql(_conn, _cursor, statement, _parameters, _context, _many):
                sql.append(statement.strip().lower())

            sqlalchemy_event.listen(engine, "before_cursor_execute", record_sql)
            try:
                orders = _get_json(
                    origin,
                    f"/api/v1/spider/orders?strategy_id={quote(strategy)}",
                    cookie,
                )
                assert orders["source"] == "SQL_LOCAL_FACTS"
                assert orders["provider_reconciliation"] == "NOT_ASSERTED"
                order_rows = _object_rows(orders.get("orders"))
                assert order_rows
                view_version = _string_field(orders, "view_version")
                selected = order_rows[0]
                assert selected["local_order_id"] == order.id
                trade_path = (
                    f"/api/v1/spider/orders/{quote(order.id, safe='')}/trades"
                    f"?strategy_id={quote(strategy)}&view_version={view_version}"
                )
                trades = _get_json(origin, trade_path, cookie)
                trade_rows = _object_rows(trades.get("trades"))
                assert len(trade_rows) == 2
                with Session(engine) as session:
                    expected_trades = (
                        session.execute(
                            select(Trade)
                            .where(Trade.order_id == order.id)
                            .order_by(
                                Trade.timestamp.asc(), Trade.id.collate("C").asc()
                            )
                        )
                        .scalars()
                        .all()
                    )
                expected_by_provider_id = {
                    trade.exchange_trade_id: {
                        "local_trade_id": trade.id,
                        "price": canonical_decimal_text(trade.price),
                        "quantity": canonical_decimal_text(trade.quantity),
                        "fee": None
                        if trade.fee is None
                        else canonical_decimal_text(trade.fee),
                        "fee_asset": trade.fee_asset,
                    }
                    for trade in expected_trades
                }
                assert [row["provider_trade_id"] for row in trade_rows] == [
                    trade.exchange_trade_id for trade in expected_trades
                ]
                assert len(expected_by_provider_id) == len(expected_trades)
                for row in trade_rows:
                    provider_trade_id = _string_field(row, "provider_trade_id")
                    expected = expected_by_provider_id[provider_trade_id]
                    assert row["local_trade_id"] == expected["local_trade_id"]
                    assert row["price"] == expected["price"]
                    assert row["quantity"] == expected["quantity"]
                    assert row["fee"] == expected["fee"]
                    assert row["fee_asset"] == expected["fee_asset"]
                assert _facts_snapshot(engine, strategy) == before
                assert _runtime_snapshot(engine, scope) == runtime_before
            finally:
                sqlalchemy_event.remove(engine, "before_cursor_execute", record_sql)
            assert any(
                "set transaction isolation level repeatable read, read only"
                in statement
                for statement in sql
            )
            assert any(
                "set local statement_timeout = '5000ms'" in statement
                for statement in sql
            )
            assert not any(
                statement.startswith(("insert ", "update ", "delete ", "alter "))
                for statement in sql
            )
            redis.publish.assert_not_called()
            expected = json.dumps(
                {"orders": orders, "trades": trades}, separators=(",", ":")
            )
            node = shutil.which("node")
            npm = shutil.which("npm")
            assert node is not None and npm is not None
            subprocess.run(
                [node, "--version"], check=True, capture_output=True, timeout=10
            )
            subprocess.run(
                [npm, "--version"], check=True, capture_output=True, timeout=10
            )
            child_env = {
                **os.environ,
                "SPIDER_ORDERS_REAL_ORIGIN": origin,
                "SPIDER_ORDERS_REAL_LOGIN": LOGIN,
                "SPIDER_ORDERS_REAL_EXPECTED_JSON": expected,
            }
            subprocess.run(
                [
                    npm,
                    "run",
                    "test:browser",
                    "--",
                    "--config",
                    "playwright.spider-orders.integration.config.ts",
                    "e2e/spider-orders-real.integration.ts",
                ],
                cwd=frontend_dist.parent,
                env=child_env,
                check=True,
                timeout=240,
            )
            monkeypatch.setattr(spider_order_query_module, "_MAX_LINKED_TRADES", 1)
            query_service = app.spider_order_query_service
            assert query_service is not None
            limited_status, limited = query_service.trades(
                order.id,
                f"strategy_id={quote(strategy)}&view_version={view_version}",
            )
            assert limited_status == 413
            assert limited == {"error": "spider_order_view_too_large"}
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            app.shutdown(timeout=1)


def test_scoped_pages_empty_unknown_and_read_only_bounds(operation_pg, monkeypatch):
    engine = operation_pg
    _owner, scope, _operation_id, _order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    service = _query_service(engine, scope)
    before = _facts_snapshot(engine, scope.strategy_id)
    status, first = service.orders(f"strategy_id={quote(scope.strategy_id)}&limit=1")
    assert status == 200
    assert first["orders"]
    assert first["next_offset"] == 1
    assert first["total_count"] > 1
    assert first["provider_reconciliation"] == "NOT_ASSERTED"
    assert first["source"] == "SQL_LOCAL_FACTS"
    empty_status, empty = service.orders(
        f"strategy_id={quote(scope.strategy_id)}"
        "&order_created_from_ms=0&order_created_to_ms=1"
    )
    assert empty_status == 200 and empty["total_count"] == 0 and empty["orders"] == []
    malformed_status, malformed = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&limit=01"
    )
    assert malformed_status == 422 and malformed == {"error": "validation_error"}
    trade_status, trade_empty = service.trades(
        quote(first["orders"][0]["local_order_id"], safe=""),
        f"strategy_id={quote(scope.strategy_id)}&view_version={first['view_version']}",
    )
    assert trade_status == 200
    assert trade_empty["trades"] == []

    second_status, second = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&limit=1&offset=1"
        f"&view_version={first['view_version']}"
    )
    assert second_status == 200
    assert second["view_version"] == first["view_version"]
    assert second["orders"][0]["local_order_id"] != first["orders"][0]["local_order_id"]
    at_end_status, at_end = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&offset={first['total_count']}"
        f"&view_version={first['view_version']}"
    )
    assert at_end_status == 200 and at_end["orders"] == []
    beyond_status, beyond = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&offset={first['total_count'] + 1}"
        f"&view_version={first['view_version']}"
    )
    assert beyond_status == 422 and beyond == {"error": "validation_error"}

    missing_status, missing = service.orders("strategy_id=missing-strategy")
    assert missing_status == 404 and missing == {"error": "spider_scope_not_found"}
    other_scope_status, other_scope = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&local_order_id=not-owned"
    )
    assert other_scope_status == 404 and other_scope == {
        "error": "spider_order_not_found"
    }
    category_status, category = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
    )
    assert category_status == 200
    assert category["view_version"] != first["view_version"]
    same_status, same = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    sleep(0.003)
    again_status, again = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert same_status == again_status == 200
    assert same["view_version"] == again["view_version"] == first["view_version"]
    assert same["observed_at"] != again["observed_at"]

    monkeypatch.setattr(spider_order_query_module, "_MAX_BASE_ORDERS", 1)
    bounded_status, bounded = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert bounded_status == 413
    assert bounded == {"error": "spider_order_view_too_large"}
    assert _facts_snapshot(engine, scope.strategy_id) == before


def test_queries_preserve_owned_prior_generation_rows(operation_pg, monkeypatch):
    engine = operation_pg
    _owner, scope, _operation_id, _order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    _add_read_scope_only_generation_two(engine, scope)
    status, response = _query_service(engine, scope).orders(
        f"strategy_id={quote(scope.strategy_id)}&limit=100"
    )
    assert status == 200
    assert {row["generation"] for row in response["orders"]} >= {1, 2}
    old_rows = [row for row in response["orders"] if row["generation"] == 1]
    new_rows = [row for row in response["orders"] if row["generation"] == 2]
    assert old_rows and new_rows
    assert response["scope"]["strategy_id"] == scope.strategy_id


@pytest.mark.parametrize(
    "tamper",
    [
        "binding_link",
        "missing_binding",
        "action_order_link",
        "different_account",
        "different_product",
        "only_wire_identity",
    ],
)
def test_corrupt_saved_link_or_wire_proof_fails_closed_without_query_writes(
    operation_pg, monkeypatch, tamper
):
    engine = operation_pg
    _owner, scope, _operation_id, _order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    service = _query_service(engine, scope)
    healthy_status, healthy = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert healthy_status == 200 and healthy["orders"]
    with Session(engine) as session:
        order = session.execute(
            select(Order).where(Order.strategy_id == scope.strategy_id).limit(1)
        ).scalar_one()
        order_id = order.id
        if tamper == "only_wire_identity":
            if not isinstance(order.intent_payload, dict):
                raise AssertionError("installed order is missing intent evidence")
            intent = dict(order.intent_payload)
            intent["provider_wire_client_order_id"] = "sp" + "0" * 30
            order.intent_payload = intent
        elif tamper == "different_account":
            order.account_id = "unauthorized-account"
        elif tamper == "different_product":
            binding = session.execute(
                select(SpiderRuntimeBinding).where(
                    SpiderRuntimeBinding.local_order_id == order_id
                )
            ).scalar_one()
            binding.canonical_product_id = "OKX:ETH-USDT-SWAP"
        elif tamper == "action_order_link":
            other_order_id = session.execute(
                select(Order.id)
                .where(Order.strategy_id == scope.strategy_id, Order.id != order_id)
                .order_by(Order.id.collate("C"))
                .limit(1)
            ).scalar_one()
            action = session.execute(
                select(SpiderRuntimeAction).where(
                    SpiderRuntimeAction.local_order_id == order_id
                )
            ).scalar_one()
            action.local_order_id = other_order_id
        elif tamper == "missing_binding":
            binding = session.execute(
                select(SpiderRuntimeBinding).where(
                    SpiderRuntimeBinding.local_order_id == order_id
                )
            ).scalar_one()
            session.delete(binding)
        else:
            binding = session.execute(
                select(SpiderRuntimeBinding).where(
                    SpiderRuntimeBinding.local_order_id == order_id
                )
            ).scalar_one()
            binding.source_product_id = "OKX:ETH-USDT-SWAP"
        session.commit()
    after_tamper = _facts_snapshot(engine, scope.strategy_id)
    status, response = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&local_order_id={quote(order_id)}"
    )
    assert status == 503
    assert response == {"error": "spider_order_evidence_unavailable"}
    assert _facts_snapshot(engine, scope.strategy_id) == after_tamper


def test_unfiltered_invalid_saved_order_timestamp_fails_closed(
    operation_pg, monkeypatch
):
    engine = operation_pg
    _owner, scope, _operation_id, _order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    service = _query_service(engine, scope)
    healthy_status, healthy = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert healthy_status == 200 and healthy["orders"]
    invalid_order_id = healthy["orders"][0]["local_order_id"]
    with Session(engine) as session, session.begin():
        session.execute(
            update(Order)
            .where(Order.id == invalid_order_id)
            .values(timestamp=253402300800000)
        )
    changed_facts = _facts_snapshot(engine, scope.strategy_id)
    unfiltered_status, unfiltered = service.orders(
        f"strategy_id={quote(scope.strategy_id)}"
    )
    assert unfiltered_status == 503
    assert unfiltered == {"error": "spider_order_evidence_unavailable"}
    range_status, range_view = service.orders(
        f"strategy_id={quote(scope.strategy_id)}"
        "&order_created_from_ms=0&order_created_to_ms=1"
    )
    assert range_status == 200 and range_view["orders"] == []
    assert _facts_snapshot(engine, scope.strategy_id) == changed_facts


def test_statement_timeout_returns_unavailable_and_closes_transaction(
    operation_pg, monkeypatch
):
    engine = operation_pg
    _owner, scope, _operation_id, _order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    service = _query_service(engine, scope)
    before_financial = _facts_snapshot(engine, scope.strategy_id)
    before_runtime = _runtime_snapshot(engine, scope)
    injected = False

    def time_out_scope_read(_conn, cursor, statement, _parameters, _context, _many):
        nonlocal injected
        if not injected and "from spider_runtime_scope" in statement.lower():
            injected = True
            cursor.execute("SELECT pg_sleep(6)")

    sqlalchemy_event.listen(engine, "before_cursor_execute", time_out_scope_read)
    try:
        status, response = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    finally:
        sqlalchemy_event.remove(engine, "before_cursor_execute", time_out_scope_read)
    assert injected
    assert status == 503
    assert response == {"error": "spider_order_query_unavailable"}
    assert _facts_snapshot(engine, scope.strategy_id) == before_financial
    assert _runtime_snapshot(engine, scope) == before_runtime
    # A fresh request after the cancelled READ ONLY transaction must still work.
    retry_status, retry = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert retry_status == 200 and retry["orders"]


def test_actual_execution_changes_hidden_content_and_null_evidence_conflicts(
    operation_pg, monkeypatch, mock_clock, mock_exchange_adapter
):
    engine = operation_pg
    owner, scope, _operation_id, order_manager = _installed_query_scope(
        engine, monkeypatch
    )
    service = _query_service(engine, scope)
    baseline_status, baseline = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
    )
    assert baseline_status == 200
    full_status, full = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert full_status == 200 and full["orders"]
    order = order_manager.repo.get_order(full["orders"][0]["local_order_id"])
    assert order is not None and order.price is not None
    assert all(item["local_order_id"] != order.id for item in baseline["orders"])
    quantity = order.quantity
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=sessionmaker(engine),
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    execution.process_exchange_order_event(
        ExchangeOrderEvent(
            status="filled",
            product_id=order.product_id,
            client_order_id=order.client_order_id,
            exchange_order_id="query-fixture-order-null-trade",
            cumulative_filled_quantity=quantity,
            cumulative_average_price=order.price,
            last_fill_quantity=quantity,
            last_fill_price=order.price,
            fee=None,
            fee_asset=None,
            last_fill_exchange_trade_id=None,
        )
    )
    after_execution_status, after_execution_hidden = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
    )
    assert after_execution_status == 200
    assert all(
        item["local_order_id"] != order.id for item in after_execution_hidden["orders"]
    )
    stale_page_status, stale_page = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
        f"&view_version={baseline['view_version']}"
    )
    assert stale_page_status == 409
    assert stale_page == {"error": "spider_order_view_changed"}
    stale_next_status, stale_next = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT&limit=1&offset=1"
        f"&view_version={baseline['view_version']}"
    )
    assert stale_next_status == 409
    assert stale_next == {"error": "spider_order_view_changed"}
    stale_trade_status, stale_trades = service.trades(
        quote(order.id, safe=""),
        f"strategy_id={quote(scope.strategy_id)}&view_version={full['view_version']}",
    )
    assert stale_trade_status == 409
    assert stale_trades == {"error": "spider_order_view_changed"}
    current_status, current = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert current_status == 200
    row = next(item for item in current["orders"] if item["local_order_id"] == order.id)
    trade_status, trade_page = service.trades(
        quote(order.id, safe=""),
        f"strategy_id={quote(scope.strategy_id)}&view_version={current['view_version']}",
    )
    assert trade_status == 200
    assert len(trade_page["trades"]) == 1
    saved_trade = trade_page["trades"][0]
    assert saved_trade["local_order_id"] == order.id
    assert saved_trade["provider_trade_id"] is None
    assert saved_trade["provider_trade_id_status"] == "NOT_RECORDED"
    assert saved_trade["price"] == canonical_decimal_text(order.price)
    assert saved_trade["quantity"] == canonical_decimal_text(quantity)
    # The normal ExecutionEngine event producer records absent fee input as
    # zero in the existing accounting path; this test preserves that behavior.
    assert saved_trade["fee"] == "0"
    assert saved_trade["fee_asset"] == "USDT"
    assert saved_trade["fee_evidence"] == "RECORDED"
    assert row["trade_count"] == 1

    # Nullable fee projection is a separate saved-DB read fixture, not a claim
    # that the normal execution producer emits unknown fees.
    with Session(engine) as session, session.begin():
        session.execute(
            update(Trade)
            .where(Trade.id == saved_trade["local_trade_id"])
            .values(fee=None, fee_asset=None)
        )
    stale_fee_status, stale_fee = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&view_version={current['view_version']}"
    )
    assert stale_fee_status == 409
    assert stale_fee == {"error": "spider_order_view_changed"}
    nullable_status, nullable_view = service.orders(
        f"strategy_id={quote(scope.strategy_id)}"
    )
    assert nullable_status == 200
    nullable_trades_status, nullable_trades = service.trades(
        quote(order.id, safe=""),
        f"strategy_id={quote(scope.strategy_id)}"
        f"&view_version={nullable_view['view_version']}",
    )
    assert nullable_trades_status == 200
    nullable_trade = nullable_trades["trades"][0]
    assert nullable_trade["fee"] is nullable_trade["fee_asset"] is None
    assert nullable_trade["fee_evidence"] == "INCOMPLETE"

    expected_version_status, expected_version_view = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
    )
    assert expected_version_status == 200
    expected_version = expected_version_view["view_version"]
    for fee, asset in (
        (Decimal("-0.01"), "USDT"),
        (Decimal("0"), "USDT"),
        (None, None),
    ):
        with Session(engine) as session, session.begin():
            session.execute(
                update(Trade)
                .where(Trade.id == saved_trade["local_trade_id"])
                .values(fee=fee, fee_asset=asset)
            )
        stale_status, stale = service.orders(
            f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
            f"&view_version={expected_version}"
        )
        assert stale_status == 409
        assert stale == {"error": "spider_order_view_changed"}
        refreshed_status, refreshed = service.orders(
            f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
        )
        assert refreshed_status == 200
        expected_version = refreshed["view_version"]

    with Session(engine) as session, session.begin():
        session.execute(
            update(Trade)
            .where(Trade.id == saved_trade["local_trade_id"])
            .values(exchange_trade_id="query-saved-provider-trade")
        )
    stale_id_status, stale_id = service.trades(
        quote(order.id, safe=""),
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
        f"&view_version={expected_version}",
    )
    assert stale_id_status == 409
    assert stale_id == {"error": "spider_order_view_changed"}
    refreshed_status, refreshed = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
    )
    assert refreshed_status == 200
    expected_version = refreshed["view_version"]

    with Session(engine) as session, session.begin():
        session.execute(
            update(Order).where(Order.id == order.id).values(status="UNRECOGNIZED")
        )
    stale_status, stale = service.orders(
        f"strategy_id={quote(scope.strategy_id)}&category=CURRENT"
        f"&view_version={expected_version}"
    )
    assert stale_status == 409
    assert stale == {"error": "spider_order_view_changed"}
    unknown_status, unknown = service.orders(f"strategy_id={quote(scope.strategy_id)}")
    assert unknown_status == 200
    unknown_order = next(
        item for item in unknown["orders"] if item["local_order_id"] == order.id
    )
    assert unknown_order["order_status"] == "UNRECOGNIZED"
    assert unknown_order["order_status_recognized"] is False
    assert unknown_order["category"] == "PENDING"
