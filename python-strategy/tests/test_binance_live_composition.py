"""Composition boundary tests for Binance live adapters."""

import ast
import inspect
from unittest.mock import ANY, MagicMock, patch

import pytest

import src.core.adapters as adapters
from src.core.adapters import live_binance


@pytest.mark.parametrize("enable_ws", [True, "enabled"])
def test_binance_owner_routes_truthy_ws_to_live_adapter(enable_ws):
    result = object()
    guard = MagicMock()

    with (
        patch.object(live_binance, "LiveBinanceAdapter", return_value=result) as live,
        patch.object(live_binance, "CcxtExchangeAdapter") as generic,
    ):
        actual = live_binance.create_binance_live_adapter(
            api_key="key",
            secret="secret",
            testnet=False,
            enable_ws=enable_ws,
            extra_config={"options": {"defaultType": "future"}},
            operation_guard=guard,
        )

    assert actual is result
    live.assert_called_once_with(
        api_key="key",
        secret="secret",
        testnet=False,
        enable_ws=True,
        operation_guard=guard,
    )
    generic.assert_not_called()


@pytest.mark.parametrize("enable_ws", [None, False, 0, ""])
def test_binance_owner_routes_falsey_ws_to_generic_adapter(enable_ws):
    result = object()
    extra_config = {"recvWindow": 5_000}
    guard = MagicMock()

    with (
        patch.object(live_binance, "LiveBinanceAdapter") as live,
        patch.object(
            live_binance, "CcxtExchangeAdapter", return_value=result
        ) as generic,
    ):
        actual = live_binance.create_binance_live_adapter(
            api_key="key",
            secret="secret",
            testnet=False,
            enable_ws=enable_ws,
            extra_config=extra_config,
            operation_guard=guard,
        )

    assert actual is result
    generic.assert_called_once_with(
        exchange_id="binance",
        api_key="key",
        secret="secret",
        testnet=False,
        extra_config=extra_config,
    )
    live.assert_not_called()


@pytest.mark.parametrize("enable_ws", [False, True])
def test_binance_owner_preserves_selected_constructor_exception(enable_ws):
    failure = RuntimeError("constructor failed")
    live = MagicMock()
    generic = MagicMock()
    selected = live if enable_ws else generic
    selected.side_effect = failure

    with (
        patch.object(live_binance, "LiveBinanceAdapter", live),
        patch.object(live_binance, "CcxtExchangeAdapter", generic),
        pytest.raises(RuntimeError) as raised,
    ):
        live_binance.create_binance_live_adapter(
            api_key=None,
            secret=None,
            testnet=True,
            enable_ws=enable_ws,
            extra_config=None,
            operation_guard=None,
        )

    assert raised.value is failure
    if enable_ws:
        live.assert_called_once_with(
            api_key=None,
            secret=None,
            testnet=True,
            enable_ws=True,
            operation_guard=None,
        )
        generic.assert_not_called()
    else:
        generic.assert_called_once_with(
            exchange_id="binance",
            api_key=None,
            secret=None,
            testnet=True,
            extra_config=None,
        )
        live.assert_not_called()


def test_binance_owner_keeps_existing_module_dependency_boundary():
    tree = ast.parse(inspect.getsource(live_binance))
    imports = [
        ast.unparse(node)
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    assert imports == [
        "import logging",
        "import asyncio",
        "from collections.abc import Callable",
        "from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter",
        "from src.core.client_order_id import to_exchange_format",
        "from src.core.orm_models import Order",
        "from src.core.ws_connector import WebSocketOrderConnector",
    ]
    owner = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "create_binance_live_adapter"
    )
    assert not any(
        isinstance(node, ast.Import | ast.ImportFrom) for node in ast.walk(owner)
    )


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"mode": "simulated"}, "simulated"),
        ({"mode": "live", "exchange": "rithmic"}, "rithmic"),
        (
            {
                "mode": "live",
                "exchange": "bybit",
                "api_key": "bybit-key",
                "secret": "bybit-secret",
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
                "enable_ws": "enabled",
                "extra_config": {"recvWindow": 7_000},
            },
            "binance",
        ),
    ],
)
def test_generic_factory_routes_to_exact_venue_owner(config, expected):
    simulated = object()
    rithmic = MagicMock()
    generic = MagicMock()
    binance = MagicMock()

    with (
        patch.object(
            adapters, "SimulatedAdapter", return_value=simulated
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
            adapters.AccountInitializationConfig, "from_config", return_value=None
        ) as parser,
    ):
        rithmic_cls.from_config.return_value = rithmic
        actual = adapters.create_adapter(config)

    expected_result = {
        "simulated": simulated,
        "rithmic": rithmic,
        "generic": generic,
        "binance": binance,
    }[expected]
    assert actual is expected_result
    assert simulated_cls.call_count == (expected == "simulated")
    assert rithmic_cls.from_config.call_count == (expected == "rithmic")
    assert generic_cls.call_count == (expected == "generic")
    assert binance_owner.call_count == (expected == "binance")
    if expected in {"simulated", "rithmic"}:
        parser.assert_not_called()
    else:
        parser.assert_called_once_with(None, default_product_ids=[])
    if expected == "rithmic":
        rithmic_cls.from_config.assert_called_once_with(config)
    if expected == "generic":
        generic_cls.assert_called_once_with(
            exchange_id="bybit",
            api_key="bybit-key",
            secret="bybit-secret",
            testnet=False,
            extra_config=config["extra_config"],
        )
    if expected == "binance":
        binance_owner.assert_called_once_with(
            api_key="binance-key",
            secret="binance-secret",
            testnet=False,
            enable_ws="enabled",
            extra_config=config["extra_config"],
            operation_guard=ANY,
        )


def test_generic_factory_preserves_binance_lifecycle_order_and_arguments():
    trace = []
    account_config = object()
    product_ids = ["BINANCE:BTCUSDT-PERP"]
    extra_config = {"options": {"defaultType": "future"}}
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
            adapters, "create_binance_live_adapter", side_effect=construct
        ) as owner,
    ):
        actual = adapters.create_adapter(
            {
                "mode": "live",
                "exchange": "binance",
                "api_key": "key",
                "secret": "secret",
                "testnet": False,
                "enable_ws": True,
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
        enable_ws=True,
        extra_config=extra_config,
        operation_guard=guard,
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
def test_generic_factory_early_failure_preserves_identity_and_stops(failure_stage):
    failure = RuntimeError(failure_stage)
    trace = []
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
        patch.object(adapters, "create_binance_live_adapter", side_effect=construct),
        pytest.raises(RuntimeError) as raised,
    ):
        adapters.create_adapter(
            {"mode": "live", "exchange": "binance", "account_initialization": {}},
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
def test_generic_factory_downstream_failure_preserves_close_precedence(
    failure_stage,
    close_mode,
):
    primary = RuntimeError(failure_stage)
    close_failure = RuntimeError("close")
    adapter = MagicMock()
    guard_calls = 0
    trace = []

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
        patch.object(adapters, "create_binance_live_adapter", side_effect=construct),
        pytest.raises(RuntimeError) as raised,
    ):
        adapters.create_adapter(
            {"mode": "live", "exchange": "binance"},
            operation_guard=guard,
        )

    expected = close_failure if close_mode == "failure" else primary
    assert raised.value is expected
    if close_mode == "absent":
        assert adapter.close is None
    else:
        adapter.close.assert_called_once_with()
    if close_mode == "failure":
        assert close_failure.__context__ is primary
    lifecycle = {
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
        lifecycle = [*lifecycle, "close"]
    assert trace == lifecycle


def test_generic_factory_source_has_only_binance_owner_entrypoint():
    tree = ast.parse(inspect.getsource(adapters.create_adapter))
    calls = [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]

    assert calls.count("create_binance_live_adapter") == 1
    assert "LiveBinanceAdapter" not in calls
