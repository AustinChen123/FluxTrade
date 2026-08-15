"""Composition boundary tests for Backpack live adapters."""

import ast
import importlib
import inspect
from unittest.mock import MagicMock, patch

import pytest

import src.core.adapters as adapters
from src.core.adapters.live_backpack import LiveBackpackAdapter
from src.core.interfaces.exchange import ExchangeError


def _owner():
    return importlib.import_module("src.core.adapters.live_backpack")


def test_backpack_owner_constructs_exact_venue_adapter() -> None:
    owner = _owner()
    result = object()
    api_key = object()
    secret = object()
    testnet = object()
    extra_config = object()

    with patch.object(owner, "LiveBackpackAdapter", return_value=result) as constructor:
        actual = owner.create_backpack_live_adapter(
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


def test_backpack_owner_preserves_constructor_exception_identity() -> None:
    owner = _owner()
    failure = RuntimeError("backpack-constructor-sentinel")

    with (
        patch.object(owner, "LiveBackpackAdapter", side_effect=failure),
        pytest.raises(RuntimeError) as raised,
    ):
        owner.create_backpack_live_adapter(
            api_key=None,
            secret=None,
            testnet=False,
            extra_config=None,
        )

    assert raised.value is failure


def test_backpack_owner_has_only_shared_ccxt_dependency() -> None:
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
        "from src.core.adapters.backpack_user_stream import BackpackOrderEventStream",
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
        and node.name == "create_backpack_live_adapter"
    )
    assert not any(
        isinstance(node, ast.Import | ast.ImportFrom) for node in ast.walk(function)
    )


def test_live_adapter_delegates_order_event_lifecycle() -> None:
    adapter = object.__new__(LiveBackpackAdapter)
    owner = MagicMock()
    event = object()
    owner.poll.return_value = event
    owner.close.return_value = True
    adapter._user_order_stream = owner

    adapter.start_order_event_stream()
    assert adapter.poll_order_event() is event
    adapter.close()

    owner.start.assert_called_once_with()
    owner.poll.assert_called_once_with()
    owner.close.assert_called_once_with()


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
    ],
)
def test_generic_factory_routes_to_exact_construction_owner(config, expected) -> None:
    simulated = object()
    rithmic = MagicMock()
    generic = MagicMock()
    binance = MagicMock()

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
    }[expected]
    assert actual is expected_result
    assert simulated_cls.call_count == (expected == "simulated")
    assert rithmic_cls.from_config.call_count == (expected == "rithmic")
    assert generic_cls.call_count == (expected == "generic")
    assert binance_owner.call_count == (expected == "binance")


def test_generic_factory_rejects_unverifiable_backpack_before_construction() -> None:
    config = {
        "mode": "live",
        "exchange": "backpack",
        "api_key": "backpack-key",
        "secret": "backpack-secret",
        "testnet": False,
    }

    with (
        patch.object(adapters, "create_backpack_live_adapter") as construct,
        pytest.raises(
            ExchangeError,
            match="^backpack_account_identity_unverifiable$",
        ),
    ):
        adapters.create_adapter(config)

    construct.assert_not_called()
