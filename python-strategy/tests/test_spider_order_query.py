from __future__ import annotations

from hashlib import sha256
from unittest.mock import MagicMock

import pytest

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.spider_order_query import (
    _Invalid,
    _parse_query,
    _view_version,
    classify_presentation,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.models import OrderStatus
from src.core.orm_models import Order
from src.core.spider_management.financial_delivery import (
    prove_spider_order_wire_binding,
)
from src.core.spider_management.orm import SpiderRuntimeAction, SpiderRuntimeBinding


@pytest.mark.parametrize(
    ("action", "status", "expected"),
    [
        ("SEND_UNKNOWN", "FILLED", "PENDING"),
        ("ACCEPTED", "SUBMITTED_UNCONFIRMED", "PENDING"),
        ("ACCEPTED", "mystery", "PENDING"),
        ("TERMINAL", "NEW", "HISTORY"),
        ("ACCEPTED", "CANCELLED", "HISTORY"),
        ("UNSENT", "SUBMITTED", "PENDING"),
        ("ACCEPTED", "PARTIALLY_FILLED", "CURRENT"),
    ],
)
def test_presentation_category_is_closed_and_unknown_fails_pending(
    action, status, expected
):
    assert classify_presentation(action, status) == expected


@pytest.mark.parametrize(
    "status",
    [*(item.value for item in OrderStatus), "UNRECOGNIZED"],
)
@pytest.mark.parametrize(
    "action_state", ["SEND_UNKNOWN", "REJECTED", "TERMINAL", "UNSENT", "ACCEPTED"]
)
def test_classifier_full_core_status_and_saved_action_matrix(action_state, status):
    if action_state == "SEND_UNKNOWN" or status in {
        "SUBMITTED_UNCONFIRMED",
        "UNRECOGNIZED",
    }:
        expected = "PENDING"
    elif action_state in {"REJECTED", "TERMINAL"} or status in {
        "FILLED",
        "CANCELLED",
        "FAILED",
        "LIQUIDATED",
    }:
        expected = "HISTORY"
    elif action_state == "UNSENT" or status == "NEW":
        expected = "PENDING"
    else:
        expected = "CURRENT"
    assert classify_presentation(action_state, status) == expected


def test_order_query_accepts_normalized_bounds_and_rejects_ambiguous_inputs():
    parsed = _parse_query(
        "strategy_id=s1&order_created_from_ms=-62135596800000&order_created_to_ms=1&limit=2",
        None,
    )
    assert parsed["order_created_from_ms"] == -62135596800000
    assert parsed["order_created_to_ms"] == 1
    for query in (
        "strategy_id=s1&limit=01",
        "strategy_id=s1&strategy_id=s2",
        "strategy_id=s1&other=x",
        "strategy_id=s1&order_created_from_ms=1",
        "strategy_id=s1&offset=1",
        "strategy_id=s1&local_order_id=a%2Fb",
        "strategy_id=s1&view_version=" + "A" * 64,
        "strategy_id=s1&limit=1%ZZ",
    ):
        with pytest.raises(_Invalid):
            _parse_query(query, None)
    with pytest.raises(_Invalid):
        _parse_query("strategy_id=s1", b"")


def test_order_view_version_fixed_empty_oracle():
    scope = {
        "environment": "research",
        "execution_scope_id": "query-scope",
        "strategy_id": "query-strategy",
    }
    filters = {
        "category": "ALL",
        "local_order_id": None,
        "order_created_from_ms": None,
        "order_created_to_ms": None,
    }
    version = _view_version(scope, filters, [], [])
    expected_object = {
        "domain": "SPIDER_LOCAL_ORDER_VIEW_V1",
        "orders_schema_version": "spider_local_orders_v1",
        "trades_schema_version": "spider_local_trades_v1",
        "orders_ordering_id": "ORDER_CREATED_DESC_LOCAL_ID_ASC_V1",
        "trades_ordering_id": "TRADE_TIME_ASC_LOCAL_ID_ASC_V1",
        "scope": scope,
        "filters": filters,
        "orders": [],
        "trades": [],
    }
    assert version == sha256(canonical_bytes(expected_object)).hexdigest()
    assert version == "d750670ba9d10061fb095307f2be0a2a5a16aab4896012a2ce2c74d4da41e02e"


def test_order_view_version_fixed_multi_order_execution_oracle():
    scope = {
        "environment": "research",
        "execution_scope_id": "query-scope",
        "strategy_id": "query-strategy",
    }
    filters = {
        "category": "ALL",
        "local_order_id": None,
        "order_created_from_ms": None,
        "order_created_to_ms": None,
    }
    orders = [
        {
            "local_order_id": "o-new",
            "client_order_id": "c-new",
            "source_client_order_id": "s-new",
            "provider_wire_client_order_id": "w-new",
            "provider_order_id": None,
            "provider_order_id_status": "NOT_RECORDED",
            "strategy_id": "query-strategy",
            "account_profile": "okx",
            "account_id": "acct-1",
            "exchange_id": "OKX",
            "source_product_id": "BTC-USDT-SWAP",
            "product_id": "OKX:BTC-USDT-SWAP",
            "role": "GRID",
            "plan_id": "p-new",
            "action_id": "a-new",
            "operation_id": "op-new",
            "generation": 2,
            "configuration_sha256": "a" * 64,
            "transmission_state": "SEND_UNKNOWN",
            "order_status": "FILLED",
            "category": "PENDING",
            "order_status_recognized": True,
            "order_type": "limit",
            "side": "buy",
            "quantity": "1",
            "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
            "price": "100",
            "trigger_price": None,
            "filled_quantity": "1",
            "filled_price": "100",
            "created_timestamp_ms": 20,
            "submitted_at": None,
            "acked_at": None,
            "last_reconciled_at": None,
            "trade_count": 2,
        },
        {
            "local_order_id": "o-old",
            "client_order_id": "c-old",
            "source_client_order_id": "s-old",
            "provider_wire_client_order_id": "w-old",
            "provider_order_id": "provider-1",
            "provider_order_id_status": "RECORDED",
            "strategy_id": "query-strategy",
            "account_profile": "okx",
            "account_id": "acct-1",
            "exchange_id": "OKX",
            "source_product_id": "BTC-USDT-SWAP",
            "product_id": "OKX:BTC-USDT-SWAP",
            "role": "GRID",
            "plan_id": "p-old",
            "action_id": "a-old",
            "operation_id": "op-old",
            "generation": 1,
            "configuration_sha256": "b" * 64,
            "transmission_state": "ACCEPTED",
            "order_status": "PARTIALLY_FILLED",
            "category": "CURRENT",
            "order_status_recognized": True,
            "order_type": "limit",
            "side": "sell",
            "quantity": "2.5",
            "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
            "price": "105",
            "trigger_price": None,
            "filled_quantity": "0.5",
            "filled_price": "104.5",
            "created_timestamp_ms": 10,
            "submitted_at": "2026-01-01T00:00:00.000000Z",
            "acked_at": None,
            "last_reconciled_at": "2026-01-01T00:00:01.000000Z",
            "trade_count": 1,
        },
    ]
    trades = [
        {
            "local_trade_id": "t-early",
            "local_order_id": "o-old",
            "provider_trade_id": "pt-0",
            "provider_trade_id_status": "RECORDED",
            "product_id": "OKX:BTC-USDT-SWAP",
            "side": "sell",
            "price": "104.5",
            "quantity": "0.5",
            "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
            "fee": None,
            "fee_asset": "USDT",
            "fee_evidence": "INCOMPLETE",
            "timestamp_ms": 11,
        },
        {
            "local_trade_id": "t-mid",
            "local_order_id": "o-new",
            "provider_trade_id": None,
            "provider_trade_id_status": "NOT_RECORDED",
            "product_id": "OKX:BTC-USDT-SWAP",
            "side": "buy",
            "price": "99.5",
            "quantity": "0.25",
            "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
            "fee": "-0.001",
            "fee_asset": "USDT",
            "fee_evidence": "RECORDED",
            "timestamp_ms": 21,
        },
        {
            "local_trade_id": "t-late",
            "local_order_id": "o-new",
            "provider_trade_id": "pt-2",
            "provider_trade_id_status": "RECORDED",
            "product_id": "OKX:BTC-USDT-SWAP",
            "side": "buy",
            "price": "100",
            "quantity": "0.75",
            "quantity_unit": "LOCAL_INSTRUMENT_QUANTITY",
            "fee": "0",
            "fee_asset": "USDT",
            "fee_evidence": "RECORDED",
            "timestamp_ms": 22,
        },
    ]
    version = _view_version(scope, filters, orders, trades)
    assert version == "f070ceac70e79a4ab85e5cec70468fda60c95c81c5d0e6f9fe982409824576d0"


def test_wire_proof_is_shared_pure_scope_and_id_predicate():
    action = SpiderRuntimeAction(
        action_id="a" * 64,
        product_id="OKX:BTC-USDT-SWAP",
        original_payload=b"{}",
    )
    binding = SpiderRuntimeBinding(
        action_id=action.action_id,
        local_order_id="local-1",
        source_product_id=action.product_id,
        canonical_product_id="OKX:BTC-USDT-SWAP",
        source_client_order_id="src-1",
    )
    order = Order(
        id="local-1",
        strategy_id="s1",
        product_id=binding.canonical_product_id,
        client_order_id="local-client",
        intent_payload={},
    )
    raw = canonical_bytes(
        {
            "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
            "environment": "live",
            "execution_scope_id": "acct-main",
            "strategy_id": "s1",
            "action_id": action.action_id,
            "product_id": binding.canonical_product_id,
        }
    )
    wire_id = "sp" + sha256(b"SPIDER_OKX_CLIENT_BINDING_V1\0" + raw).hexdigest()[:30]
    assert isinstance(order.intent_payload, dict)
    order.intent_payload.update(
        {
            "provider_binding_bytes_hex": raw.hex(),
            "provider_binding_sha256": sha256(raw).hexdigest(),
            "provider_wire_client_order_id": wire_id,
            "action_id": action.action_id,
            "source_client_order_id": "src-1",
            "local_client_order_id": "local-client",
        }
    )
    assert prove_spider_order_wire_binding(
        "live", "acct-main", "s1", action, binding, order
    ) == (
        "src-1",
        "local-client",
        wire_id,
    )
    order.client_order_id = None
    order.intent_payload["local_client_order_id"] = None
    assert prove_spider_order_wire_binding(
        "live", "acct-main", "s1", action, binding, order
    ) == ("src-1", None, wire_id)
    binding.canonical_product_id = "OKX:ETH-USDT-SWAP"
    assert (
        prove_spider_order_wire_binding(
            "live", "acct-main", "s1", action, binding, order
        )
        is None
    )


def test_exact_readonly_routes_are_authenticated_and_no_store():
    service = MagicMock()
    service.orders.return_value = (200, {"orders": []})
    service.trades.return_value = (200, {"trades": []})
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="test-key",
        spider_order_query_service=service,
    )
    headers = {"X-API-Key": "test-key"}
    response = app.handle(
        "GET", "/api/v1/spider/orders?strategy_id=s1", headers=headers
    )
    assert response.status_code == 200
    assert dict(response.headers)["Cache-Control"] == "no-store"
    assert (
        app.handle(
            "POST", "/api/v1/spider/orders?strategy_id=s1", headers=headers
        ).status_code
        == 404
    )
    assert (
        app.handle(
            "GET", "/api/v1/spider/orders/abc/trades/", headers=headers
        ).status_code
        == 404
    )
    assert app.handle("GET", "/api/v1/spider/orders?strategy_id=s1").status_code == 401
    assert (
        dict(app.handle("GET", "/api/v1/spider/orders").headers)["Cache-Control"]
        == "no-store"
    )
