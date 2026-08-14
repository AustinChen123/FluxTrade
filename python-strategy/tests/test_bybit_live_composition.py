"""Composition boundary tests for Bybit live adapters."""

import ast
import importlib
import inspect
from unittest.mock import MagicMock, patch

import ccxt
import pytest

import src.core.adapters as adapters
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.live_bybit import LiveBybitAdapter
from src.core.interfaces.exchange import ExchangeError


def _owner():
    return importlib.import_module("src.core.adapters.live_bybit")


def test_bybit_owner_constructs_exact_venue_adapter() -> None:
    owner = _owner()
    result = object()
    api_key = object()
    secret = object()
    testnet = object()
    extra_config = object()

    with patch.object(owner, "LiveBybitAdapter", return_value=result) as constructor:
        actual = owner.create_bybit_live_adapter(
            api_key=api_key,
            secret=secret,
            testnet=testnet,
            extra_config=extra_config,
        )

    assert actual is result
    constructor.assert_called_once_with(
        api_key=api_key,
        secret=secret,
        testnet=testnet,
        extra_config=extra_config,
    )


def test_bybit_owner_preserves_constructor_exception_identity() -> None:
    owner = _owner()
    failure = RuntimeError("bybit-constructor-sentinel")

    with (
        patch.object(owner, "LiveBybitAdapter", side_effect=failure),
        pytest.raises(RuntimeError) as raised,
    ):
        owner.create_bybit_live_adapter(
            api_key=None,
            secret=None,
            testnet=False,
            extra_config=None,
        )

    assert raised.value is failure


def test_bybit_owner_has_only_shared_ccxt_dependency() -> None:
    owner = _owner()
    tree = ast.parse(inspect.getsource(owner))
    imports = [
        ast.unparse(node)
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    assert imports == [
        "import hashlib",
        "import threading",
        "from typing import Any",
        "import ccxt",
        "from src.core.adapters.bybit_user_stream import BybitOrderEventStream",
        "from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter",
        "from src.core.client_order_id import parse_client_order_id",
        "from src.core.interfaces.exchange import ExchangeError, ExchangeOrderEvent, ExchangeOrderSnapshot",
        "from src.core.orm_models import Order",
        "from src.core.product_registry import to_ccxt_symbol",
    ]
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "create_bybit_live_adapter"
    )
    assert not any(
        isinstance(node, ast.Import | ast.ImportFrom) for node in ast.walk(function)
    )


def _adapter() -> LiveBybitAdapter:
    adapter = object.__new__(LiveBybitAdapter)
    adapter._client_order_aliases = {}
    adapter._client_order_alias_lock = __import__("threading").Lock()
    adapter.logger = MagicMock()
    return adapter


def test_live_adapter_delegates_order_event_lifecycle() -> None:
    adapter = _adapter()
    owner = MagicMock()
    event = object()
    owner.poll.return_value = event
    owner.close.return_value = False
    adapter._user_order_stream = owner

    adapter.start_order_event_stream()
    assert adapter.poll_order_event() is event
    adapter.close()

    owner.start.assert_called_once_with()
    owner.poll.assert_called_once_with()
    owner.close.assert_called_once_with()
    adapter.logger.warning.assert_called_once_with(
        "Bybit order event stream cleanup failed"
    )


def test_alias_is_stable_bounded_and_registered_before_parent_submission() -> None:
    adapter = _adapter()
    order = MagicMock()
    order.client_order_id = "strategy-market-LONG-1800000000000000000"

    def submit(_order):
        provider_id = next(iter(adapter._client_order_aliases))
        assert adapter._client_order_aliases[provider_id] == order.client_order_id
        return "exchange-order"

    with patch.object(CcxtExchangeAdapter, "place_order", side_effect=submit) as parent:
        assert adapter.place_order(order) == "exchange-order"

    parent.assert_called_once_with(order)
    provider_id = adapter._exchange_client_order_id(order.client_order_id)
    assert len(provider_id) == 36
    assert provider_id.replace("-", "").isalnum()
    assert adapter._canonical_client_order_id(provider_id) == order.client_order_id
    assert adapter._canonical_client_order_id("persisted-exchange-id") == (
        "persisted-exchange-id"
    )


def test_alias_collision_fails_before_parent_or_provider_io() -> None:
    adapter = _adapter()
    with patch.object(_owner(), "_bybit_client_order_id", return_value="ft-collision"):
        assert (
            adapter._exchange_client_order_id("alpha-market-LONG-1") == "ft-collision"
        )
        with pytest.raises(ExchangeError, match="^bybit_client_order_id_collision$"):
            adapter._exchange_client_order_id("beta-market-LONG-2")


def test_lookup_and_cancel_use_order_link_id_only() -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.return_value = {
        "result": {"list": [{"orderId": "provider-order"}]}
    }
    client.parse_order.return_value = {
        "id": "provider-order",
        "status": "open",
        "filled": "0",
    }
    adapter.client = client
    canonical = "strategy-market-LONG-1800000000000000000"

    snapshot = adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP")
    assert snapshot is not None
    assert snapshot.client_order_id == canonical
    assert adapter.cancel_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP") is True

    provider_id = adapter._exchange_client_order_id(canonical)
    request = {
        "category": "linear",
        "symbol": "BTCUSDT",
        "orderLinkId": provider_id,
    }
    client.privateGetV5OrderRealtime.assert_called_once_with(request)
    client.privatePostV5OrderCancel.assert_called_once_with(request)
    assert "orderId" not in request
    assert adapter.cancel_terminal_state_delivered_by_order_events() is True


@pytest.mark.parametrize(
    ("rows", "expected_none"),
    [([], True), ([{"orderId": "provider-order"}], False)],
)
def test_lookup_accepts_only_zero_or_one_order(rows, expected_none: bool) -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.return_value = {"result": {"list": rows}}
    client.privateGetV5OrderHistory.return_value = {"result": {"list": []}}
    client.parse_order.return_value = {
        "id": "provider-order",
        "status": "open",
        "filled": "0",
    }
    adapter.client = client

    result = adapter.get_order_by_client_id(
        "strategy-market-LONG-1800000000000000000",
        "BYBIT:BTCUSDT-PERP",
    )

    assert (result is None) is expected_none
    assert client.parse_order.call_count == (not expected_none)
    assert client.privateGetV5OrderHistory.call_count == expected_none


@pytest.mark.parametrize(
    "response",
    [
        None,
        {},
        {"result": None},
        {"result": {"list": {}}},
        {"result": {"list": [{}, {}]}},
        {"result": {"list": ["row"]}},
    ],
)
def test_malformed_or_multiple_lookup_rows_fail_closed(response) -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.return_value = response
    adapter.client = client

    with pytest.raises(ExchangeError, match="^bybit_client_order_lookup_failed$"):
        adapter.get_order_by_client_id(
            "strategy-market-LONG-1800000000000000000",
            "BYBIT:BTCUSDT-PERP",
        )


def test_lookup_and_cancel_provider_failures_keep_fixed_contracts() -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.side_effect = ccxt.NetworkError(
        "provider-secret-sentinel"
    )
    client.privatePostV5OrderCancel.side_effect = ccxt.NetworkError(
        "provider-secret-sentinel"
    )
    adapter.client = client
    canonical = "strategy-market-LONG-1800000000000000000"

    with pytest.raises(ExchangeError) as raised:
        adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP")
    assert str(raised.value) == "bybit_client_order_lookup_failed"
    assert adapter.cancel_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP") is False
    adapter.logger.error.assert_called_once_with(
        "Failed to cancel Bybit order by client identity"
    )


def test_order_not_found_is_nonfatal_for_lookup_and_cancel() -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.side_effect = ccxt.OrderNotFound("missing")
    client.privateGetV5OrderHistory.side_effect = ccxt.OrderNotFound("missing")
    client.privatePostV5OrderCancel.side_effect = ccxt.OrderNotFound("missing")
    adapter.client = client
    canonical = "strategy-market-LONG-1800000000000000000"

    assert adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP") is None
    assert adapter.cancel_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP") is False


def test_realtime_not_found_still_queries_history_with_identical_identity() -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.side_effect = ccxt.OrderNotFound("released")
    client.privateGetV5OrderHistory.return_value = {
        "result": {"list": [{"orderId": "closed-provider-order"}]}
    }
    client.parse_order.return_value = {
        "id": "closed-provider-order",
        "status": "closed",
        "filled": "1",
        "average": "101",
    }
    adapter.client = client
    canonical = "strategy-market-LONG-1800000000000000000"

    snapshot = adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP")

    assert snapshot is not None
    realtime_request = client.privateGetV5OrderRealtime.call_args.args[0]
    client.privateGetV5OrderHistory.assert_called_once_with(realtime_request)


def test_realtime_zero_falls_back_to_authoritative_order_history() -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.return_value = {"result": {"list": []}}
    client.privateGetV5OrderHistory.return_value = {
        "result": {"list": [{"orderId": "closed-provider-order"}]}
    }
    client.parse_order.return_value = {
        "id": "closed-provider-order",
        "status": "closed",
        "filled": "1",
        "average": "101",
    }
    adapter.client = client
    canonical = "strategy-market-LONG-1800000000000000000"

    snapshot = adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP")

    assert snapshot is not None
    assert snapshot.exchange_order_id == "closed-provider-order"
    request = client.privateGetV5OrderRealtime.call_args.args[0]
    client.privateGetV5OrderHistory.assert_called_once_with(request)


@pytest.mark.parametrize(
    "history_rows",
    [[], [{}, {}], {}, ["row"]],
)
def test_history_zero_or_invalid_rows_keep_strict_lookup_contract(history_rows) -> None:
    adapter = _adapter()
    client = MagicMock()
    client.market.return_value = {"id": "BTCUSDT"}
    client.privateGetV5OrderRealtime.return_value = {"result": {"list": []}}
    client.privateGetV5OrderHistory.return_value = {"result": {"list": history_rows}}
    adapter.client = client
    canonical = "strategy-market-LONG-1800000000000000000"

    if history_rows == []:
        assert adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP") is None
    else:
        with pytest.raises(ExchangeError, match="^bybit_client_order_lookup_failed$"):
            adapter.get_order_by_client_id(canonical, "BYBIT:BTCUSDT-PERP")


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"mode": "simulated"}, "simulated"),
        ({"mode": "live", "exchange": "rithmic"}, "rithmic"),
        (
            {
                "mode": "live",
                "exchange": "kraken",
                "api_key": "kraken-key",
                "secret": "kraken-secret",
                "testnet": False,
                "extra_config": {"recvWindow": 5_000},
            },
            "generic",
        ),
        (
            {
                "mode": "live",
                "exchange": "binance",
                "api_key": "binance-key",
                "secret": "binance-secret",
                "testnet": False,
                "enable_ws": True,
            },
            "binance",
        ),
        (
            {
                "mode": "live",
                "exchange": "backpack",
                "api_key": "backpack-key",
                "secret": "backpack-secret",
                "testnet": False,
            },
            "backpack",
        ),
        (
            {
                "mode": "live",
                "exchange": "bybit",
                "api_key": "bybit-key",
                "secret": "bybit-secret",
                "testnet": False,
                "extra_config": {"options": {"defaultType": "swap"}},
            },
            "bybit",
        ),
    ],
)
def test_generic_factory_routes_to_exact_construction_owner(config, expected) -> None:
    simulated = object()
    rithmic = MagicMock()
    generic = MagicMock()
    binance = MagicMock()
    backpack = MagicMock()
    bybit = MagicMock()

    with (
        patch.object(
            adapters, "create_simulated_adapter", return_value=simulated
        ) as simulated_cls,
        patch.object(adapters, "RithmicExchangeAdapter") as rithmic_cls,
        patch.object(
            adapters, "CcxtExchangeAdapter", return_value=generic
        ) as generic_cls,
        patch.object(
            adapters,
            "create_binance_live_adapter",
            return_value=binance,
        ) as binance_owner,
        patch.object(
            adapters,
            "create_backpack_live_adapter",
            return_value=backpack,
        ) as backpack_owner,
        patch.object(
            adapters,
            "create_bybit_live_adapter",
            return_value=bybit,
            create=True,
        ) as bybit_owner,
        patch.object(
            adapters.AccountInitializationConfig, "from_config", return_value=None
        ),
    ):
        rithmic_cls.from_config.return_value = rithmic
        actual = adapters.create_adapter(config)

    expected_result = {
        "simulated": simulated,
        "rithmic": rithmic,
        "generic": generic,
        "binance": binance,
        "backpack": backpack,
        "bybit": bybit,
    }[expected]
    assert actual is expected_result
    assert simulated_cls.call_count == (expected == "simulated")
    assert rithmic_cls.from_config.call_count == (expected == "rithmic")
    assert generic_cls.call_count == (expected == "generic")
    assert binance_owner.call_count == (expected == "binance")
    assert backpack_owner.call_count == (expected == "backpack")
    assert bybit_owner.call_count == (expected == "bybit")
    if expected == "bybit":
        bybit_owner.assert_called_once_with(
            api_key="bybit-key",
            secret="bybit-secret",
            testnet=False,
            extra_config=config["extra_config"],
        )


def test_generic_factory_preserves_bybit_lifecycle_order() -> None:
    trace: list[str] = []
    account_config = object()
    product_ids = ["BYBIT:BTCUSDT-PERP"]
    extra_config = {"options": {"defaultType": "swap"}}
    adapter = MagicMock()

    def parse(*_args, **_kwargs):
        trace.append("parse")
        return account_config

    def guard():
        trace.append("guard")

    def construct(**_kwargs):
        trace.append("construct")
        return adapter

    adapter.initialize_account.side_effect = lambda *_a, **_k: trace.append(
        "initialize"
    )
    adapter.warm_instrument_specs.side_effect = lambda *_a, **_k: trace.append("warm")

    with (
        patch.object(
            adapters.AccountInitializationConfig, "from_config", side_effect=parse
        ) as parser,
        patch.object(
            adapters,
            "create_bybit_live_adapter",
            side_effect=construct,
            create=True,
        ) as owner,
        patch.object(
            adapters,
            "CcxtExchangeAdapter",
            side_effect=AssertionError("generic constructor called"),
        ),
    ):
        actual = adapters.create_adapter(
            {
                "mode": "live",
                "exchange": "bybit",
                "api_key": "key",
                "secret": "secret",
                "testnet": False,
                "extra_config": extra_config,
                "instrument_product_ids": product_ids,
                "account_initialization": {"leverage": 2},
            },
            operation_guard=guard,
        )

    assert actual is adapter
    assert trace == ["parse", "guard", "construct", "guard", "initialize", "warm"]
    parser.assert_called_once_with(
        {"leverage": 2},
        default_product_ids=product_ids,
    )
    owner.assert_called_once_with(
        api_key="key",
        secret="secret",
        testnet=False,
        extra_config=extra_config,
    )
    adapter.initialize_account.assert_called_once_with(
        account_config,
        operation_guard=guard,
    )
    adapter.warm_instrument_specs.assert_called_once_with(
        product_ids,
        operation_guard=guard,
    )


@pytest.mark.parametrize("failure_stage", ["parse", "pre_guard", "construct"])
def test_bybit_early_failure_preserves_identity_and_stops(failure_stage) -> None:
    failure = RuntimeError(failure_stage)
    trace: list[str] = []
    adapter = MagicMock()

    def parse(*_args, **_kwargs):
        trace.append("parse")
        if failure_stage == "parse":
            raise failure
        return object()

    def guard():
        trace.append("guard")
        if failure_stage == "pre_guard":
            raise failure

    def construct(**_kwargs):
        trace.append("construct")
        if failure_stage == "construct":
            raise failure
        return adapter

    with (
        patch.object(
            adapters.AccountInitializationConfig, "from_config", side_effect=parse
        ),
        patch.object(
            adapters,
            "create_bybit_live_adapter",
            side_effect=construct,
            create=True,
        ),
        patch.object(
            adapters,
            "CcxtExchangeAdapter",
            side_effect=AssertionError("generic constructor called"),
        ),
        pytest.raises(RuntimeError) as raised,
    ):
        adapters.create_adapter(
            {"mode": "live", "exchange": "bybit"},
            operation_guard=guard,
        )

    assert raised.value is failure
    assert (
        trace
        == {
            "parse": ["parse"],
            "pre_guard": ["parse", "guard"],
            "construct": ["parse", "guard", "construct"],
        }[failure_stage]
    )
    adapter.initialize_account.assert_not_called()
    adapter.warm_instrument_specs.assert_not_called()
    adapter.close.assert_not_called()


@pytest.mark.parametrize("failure_stage", ["post_guard", "initialize", "warm"])
@pytest.mark.parametrize("close_mode", ["absent", "success", "failure"])
def test_bybit_downstream_failure_preserves_close_precedence(
    failure_stage, close_mode
) -> None:
    primary = RuntimeError(failure_stage)
    close_failure = RuntimeError("close")
    adapter = MagicMock()
    guard_calls = 0
    trace: list[str] = []

    def guard():
        nonlocal guard_calls
        guard_calls += 1
        trace.append("guard")
        if failure_stage == "post_guard" and guard_calls == 2:
            raise primary

    def initialize(*_args, **_kwargs):
        trace.append("initialize")
        if failure_stage == "initialize":
            raise primary

    def warm(*_args, **_kwargs):
        trace.append("warm")
        if failure_stage == "warm":
            raise primary

    def close():
        trace.append("close")
        if close_mode == "failure":
            raise close_failure

    adapter.initialize_account.side_effect = initialize
    adapter.warm_instrument_specs.side_effect = warm
    if close_mode == "absent":
        adapter.close = None
    else:
        adapter.close.side_effect = close

    def parse(*_args, **_kwargs):
        trace.append("parse")
        return object()

    def construct(**_kwargs):
        trace.append("construct")
        return adapter

    with (
        patch.object(
            adapters.AccountInitializationConfig, "from_config", side_effect=parse
        ),
        patch.object(
            adapters,
            "create_bybit_live_adapter",
            side_effect=construct,
            create=True,
        ),
        patch.object(
            adapters,
            "CcxtExchangeAdapter",
            side_effect=AssertionError("generic constructor called"),
        ),
        pytest.raises(RuntimeError) as raised,
    ):
        adapters.create_adapter(
            {"mode": "live", "exchange": "bybit"},
            operation_guard=guard,
        )

    assert raised.value is (close_failure if close_mode == "failure" else primary)
    if close_mode == "absent":
        assert adapter.close is None
    else:
        adapter.close.assert_called_once_with()
    if close_mode == "failure":
        assert close_failure.__context__ is primary
    expected = {
        "post_guard": ["parse", "guard", "construct", "guard"],
        "initialize": ["parse", "guard", "construct", "guard", "initialize"],
        "warm": [
            "parse",
            "guard",
            "construct",
            "guard",
            "initialize",
            "warm",
        ],
    }[failure_stage]
    if close_mode != "absent":
        expected = [*expected, "close"]
    assert trace == expected


def test_generic_factory_source_has_one_bybit_owner_entrypoint() -> None:
    tree = ast.parse(inspect.getsource(adapters.create_adapter))
    calls = [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]

    assert calls.count("create_bybit_live_adapter") == 1
