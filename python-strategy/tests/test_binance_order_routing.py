"""Tests for Binance conditional-order and client-ID routing policy."""

import ast
import inspect
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.core.adapters.ccxt_adapter as ccxt_adapter_module
import src.core.adapters.live_binance as live_binance_module
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.live_binance import LiveBinanceAdapter
from src.core.client_order_id import to_exchange_format
from src.core.interfaces.exchange import ExchangeError
from src.core.orm_models import Order


def _owner_functions():
    from src.core.adapters.binance_order_routing import (
        binance_conditional_order_mapping,
        binance_lookup_client_order_id_params,
        binance_submission_client_order_id_params,
        uses_binance_algo_order_endpoints,
    )

    return (
        binance_conditional_order_mapping,
        uses_binance_algo_order_endpoints,
        binance_submission_client_order_id_params,
        binance_lookup_client_order_id_params,
    )


def _adapter() -> LiveBinanceAdapter:
    adapter = object.__new__(LiveBinanceAdapter)
    adapter.exchange_id = "binance"
    adapter.logger = MagicMock()
    adapter.ws_connector = None
    return adapter


def _generic_adapter(exchange_id: str) -> CcxtExchangeAdapter:
    adapter = object.__new__(CcxtExchangeAdapter)
    adapter.exchange_id = exchange_id
    adapter.logger = MagicMock()
    return adapter


def test_live_binance_conditional_mapping_delegates_once(monkeypatch) -> None:
    owner = MagicMock(return_value=("OWNER", {"owner": True}))
    monkeypatch.setattr(
        live_binance_module,
        "binance_conditional_order_mapping",
        owner,
        raising=False,
    )
    order = MagicMock(spec=Order)
    order.type = "stop_loss"
    order.trigger_price = Decimal("41000")

    assert _adapter()._ccxt_order_type_and_params(order) == (
        "OWNER",
        {"owner": True},
    )
    owner.assert_called_once_with("binance", "stop_loss", Decimal("41000"))


def test_live_binance_client_id_params_delegate_once(monkeypatch) -> None:
    owner = MagicMock(return_value={"owner": True})
    monkeypatch.setattr(
        live_binance_module,
        "binance_lookup_client_order_id_params",
        owner,
        raising=False,
    )

    assert _adapter()._client_order_id_params("client-id", "stop_loss") == {
        "owner": True
    }
    owner.assert_called_once_with("binance", "stop_loss", "client-id")


def test_place_order_delegates_submission_client_id_policy(monkeypatch) -> None:
    adapter = _adapter()
    client = MagicMock()
    client.create_order.return_value = {"id": "exchange-order"}
    vars(adapter)["client"] = client
    monkeypatch.setattr(adapter, "_quantize_order", MagicMock())
    monkeypatch.setattr(
        adapter,
        "_ccxt_order_type_and_params",
        MagicMock(return_value=("market", {})),
    )
    policy = MagicMock(return_value={"ownerKey": "owner-value"})
    monkeypatch.setattr(
        live_binance_module,
        "binance_submission_client_order_id_params",
        policy,
    )
    order = MagicMock(spec=Order)
    order.product_id = "BINANCE:BTCUSDT-PERP"
    order.type = "market"
    order.side = "buy"
    order.quantity = Decimal("1")
    order.price = None
    order.client_order_id = "strategy-worker-entry-1700000000000000000"
    order.intent_payload = None

    assert adapter.place_order(order) == "exchange-order"

    exchange_id = to_exchange_format(order.client_order_id, "binance")
    policy.assert_called_once_with("binance", "market", exchange_id)
    client.create_order.assert_called_once_with(
        symbol="BTC/USDT:USDT",
        type="market",
        side="buy",
        amount="1",
        price=None,
        params={"ownerKey": "owner-value"},
    )


@pytest.mark.parametrize("uses_algo", [False, True])
def test_cancel_order_delegates_algo_namespace_policy(
    monkeypatch,
    uses_algo: bool,
) -> None:
    adapter = _adapter()
    client = MagicMock()
    vars(adapter)["client"] = client
    policy = MagicMock(return_value=uses_algo)
    monkeypatch.setattr(
        live_binance_module,
        "uses_binance_algo_order_endpoints",
        policy,
    )

    assert adapter.cancel_order(
        "exchange-order",
        "BINANCE:BTCUSDT-PERP",
        order_type="stop_loss",
    )

    policy.assert_called_once_with("binance", "stop_loss")
    expected_params = {"trigger": True} if uses_algo else None
    if expected_params is None:
        client.cancel_order.assert_called_once_with(
            "exchange-order",
            "BTC/USDT:USDT",
        )
    else:
        client.cancel_order.assert_called_once_with(
            "exchange-order",
            "BTC/USDT:USDT",
            params=expected_params,
        )


@pytest.mark.parametrize("public_method", ["place_order", "validate_order"])
def test_quantization_failure_precedes_policy(
    monkeypatch,
    public_method: str,
) -> None:
    adapter = _generic_adapter("bybit")
    client = MagicMock()
    vars(adapter)["client"] = client
    sentinel = RuntimeError("quantization sentinel")
    quantize = MagicMock(side_effect=sentinel)
    policy = MagicMock()
    monkeypatch.setattr(adapter, "_quantize_order", quantize)
    monkeypatch.setattr(adapter, "_ccxt_order_type_and_params", policy)
    order = SimpleNamespace(
        product_id="BYBIT:BTCUSDT-PERP",
        type="stop_loss",
        trigger_price=None,
    )

    with pytest.raises(RuntimeError) as exc_info:
        getattr(adapter, public_method)(order)

    assert exc_info.value is sentinel
    quantize.assert_called_once_with(order)
    policy.assert_not_called()
    client.create_order.assert_not_called()


@pytest.mark.parametrize("public_method", ["place_order", "validate_order"])
def test_policy_rejection_follows_successful_quantization(
    monkeypatch,
    public_method: str,
) -> None:
    adapter = _generic_adapter("bybit")
    client = MagicMock()
    vars(adapter)["client"] = client
    quantize = MagicMock()
    rejection = ExchangeError("conditional rejection")
    policy = MagicMock(side_effect=rejection)
    monkeypatch.setattr(adapter, "_quantize_order", quantize)
    monkeypatch.setattr(adapter, "_ccxt_order_type_and_params", policy)
    order = SimpleNamespace(
        product_id="BYBIT:BTCUSDT-PERP",
        type="stop_loss",
        trigger_price=None,
    )

    with pytest.raises(ExchangeError) as exc_info:
        getattr(adapter, public_method)(order)

    assert exc_info.value is rejection
    quantize.assert_called_once_with(order)
    policy.assert_called_once_with(order)
    client.create_order.assert_not_called()


@pytest.mark.parametrize("order_type", ["stop_loss", "take_profit"])
def test_non_binance_conditional_rejection_precedes_trigger_validation(
    order_type: str,
) -> None:
    mapping, _, _, _ = _owner_functions()

    with pytest.raises(
        ExchangeError,
        match="^conditional_order_mapping_unsupported: exchange=bybit$",
    ):
        mapping("bybit", order_type, None)


@pytest.mark.parametrize(
    ("order_type", "ccxt_type", "trigger_field"),
    [
        ("stop_loss", "STOP_MARKET", "stopLossPrice"),
        ("take_profit", "TAKE_PROFIT_MARKET", "takeProfitPrice"),
    ],
)
def test_binance_conditional_mapping(
    order_type: str,
    ccxt_type: str,
    trigger_field: str,
) -> None:
    mapping, _, _, _ = _owner_functions()

    assert mapping("binance", order_type, Decimal("41000.50")) == (
        ccxt_type,
        {trigger_field: "41000.50", "reduceOnly": True},
    )
    with pytest.raises(ExchangeError, match=rf"^{order_type}_requires_trigger_price$"):
        mapping("binance", order_type, None)


@pytest.mark.parametrize(
    "order_type",
    ["market", "limit", "trailing_stop", "unknown", "", None],
)
@pytest.mark.parametrize("exchange_id", ["binance", "bybit"])
def test_non_conditional_types_have_no_provider_mapping(
    exchange_id: str,
    order_type: str | None,
) -> None:
    mapping, _, _, _ = _owner_functions()

    assert mapping(exchange_id, order_type, Decimal("41000")) is None


@pytest.mark.parametrize("exchange_id", ["binance", "bybit"])
@pytest.mark.parametrize(
    "order_type",
    ["stop_loss", "take_profit", "market", "limit", None],
)
def test_algo_namespace_matrix(exchange_id: str, order_type: str | None) -> None:
    _, uses_algo, _, _ = _owner_functions()

    assert uses_algo(exchange_id, order_type) is (
        exchange_id == "binance" and order_type in {"stop_loss", "take_profit"}
    )


@pytest.mark.parametrize("exchange_id", ["binance", "bybit"])
@pytest.mark.parametrize("order_type", ["stop_loss", "market"])
def test_client_id_parameter_matrices(exchange_id: str, order_type: str) -> None:
    _, _, submission, lookup = _owner_functions()
    is_algo = exchange_id == "binance" and order_type == "stop_loss"

    expected_submission = None
    expected_lookup = None
    if exchange_id == "binance":
        expected_submission = {
            "clientAlgoId" if is_algo else "newClientOrderId": "client-id"
        }
        expected_lookup = (
            {"clientAlgoId": "client-id", "trigger": True}
            if is_algo
            else {"origClientOrderId": "client-id"}
        )

    assert submission(exchange_id, order_type, "client-id") == expected_submission
    assert lookup(exchange_id, order_type, "client-id") == expected_lookup


def test_owner_is_a_pure_policy_module() -> None:
    from src.core.adapters import binance_order_routing

    source = inspect.getsource(binance_order_routing)
    imports = {
        node.module if isinstance(node, ast.ImportFrom) else alias.name
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert imports == {"decimal", "src.core.interfaces.exchange"}


def test_shared_adapter_contains_no_binance_order_routing_literals() -> None:
    source = inspect.getsource(ccxt_adapter_module)

    assert "binance_order_routing" not in source
    assert "binance_user_stream" not in source

    for literal in (
        "clientAlgoId",
        "newClientOrderId",
        "origClientOrderId",
        "stopLossPrice",
        "takeProfitPrice",
    ):
        assert literal not in source
