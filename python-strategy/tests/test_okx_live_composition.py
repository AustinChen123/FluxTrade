"""Composition boundary tests for OKX live adapters."""

import ast
import importlib
import inspect
from unittest.mock import MagicMock, patch

import pytest

import src.core.adapters as adapters


def _owner():
    return importlib.import_module("src.core.adapters.live_okx")


@pytest.mark.parametrize(
    "inputs",
    [
        {
            "api_key": object(),
            "secret": object(),
            "testnet": object(),
            "extra_config": object(),
        },
        {
            "api_key": None,
            "secret": None,
            "testnet": False,
            "extra_config": None,
        },
    ],
)
def test_okx_owner_constructs_exact_generic_adapter(inputs) -> None:
    owner = _owner()
    result = object()

    with patch.object(owner, "CcxtExchangeAdapter", return_value=result) as constructor:
        actual = owner.create_okx_live_adapter(**inputs)

    assert actual is result
    constructor.assert_called_once_with(exchange_id="okx", **inputs)


def test_okx_owner_preserves_constructor_exception_identity() -> None:
    owner = _owner()
    failure = RuntimeError("okx-constructor-sentinel")

    with (
        patch.object(owner, "CcxtExchangeAdapter", side_effect=failure),
        pytest.raises(RuntimeError) as raised,
    ):
        owner.create_okx_live_adapter(
            api_key=None,
            secret=None,
            testnet=False,
            extra_config=None,
        )

    assert raised.value is failure


def test_okx_owner_has_only_shared_ccxt_dependency() -> None:
    owner = _owner()
    tree = ast.parse(inspect.getsource(owner))
    imports = [
        ast.unparse(node)
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    assert imports == [
        "from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter",
    ]
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "create_okx_live_adapter"
    )
    assert not any(
        isinstance(node, ast.Import | ast.ImportFrom) for node in ast.walk(function)
    )


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
        ({"mode": "live", "exchange": "binance"}, "binance"),
        ({"mode": "live", "exchange": "backpack"}, "backpack"),
        ({"mode": "live", "exchange": "bybit"}, "bybit"),
        (
            {
                "mode": "live",
                "exchange": "okx",
                "api_key": "okx-key",
                "secret": "okx-secret",
                "testnet": False,
                "extra_config": {"options": {"defaultType": "swap"}},
            },
            "okx",
        ),
    ],
)
def test_generic_factory_routes_to_exact_construction_owner(config, expected) -> None:
    results = {
        name: MagicMock(name=name)
        for name in (
            "simulated",
            "rithmic",
            "generic",
            "binance",
            "backpack",
            "bybit",
            "okx",
        )
    }

    with (
        patch.object(
            adapters,
            "create_simulated_adapter",
            return_value=results["simulated"],
        ) as simulated_cls,
        patch.object(adapters, "RithmicExchangeAdapter") as rithmic_cls,
        patch.object(
            adapters,
            "CcxtExchangeAdapter",
            return_value=results["generic"],
        ) as generic_cls,
        patch.object(
            adapters,
            "create_binance_live_adapter",
            return_value=results["binance"],
        ) as binance_owner,
        patch.object(
            adapters,
            "create_backpack_live_adapter",
            return_value=results["backpack"],
        ) as backpack_owner,
        patch.object(
            adapters,
            "create_bybit_live_adapter",
            return_value=results["bybit"],
        ) as bybit_owner,
        patch.object(
            adapters,
            "create_okx_live_adapter",
            return_value=results["okx"],
            create=True,
        ) as okx_owner,
        patch.object(
            adapters.AccountInitializationConfig,
            "from_config",
            return_value=None,
        ),
    ):
        rithmic_cls.from_config.return_value = results["rithmic"]
        actual = adapters.create_adapter(config)

    assert actual is results[expected]
    assert simulated_cls.call_count == (expected == "simulated")
    assert rithmic_cls.from_config.call_count == (expected == "rithmic")
    assert generic_cls.call_count == (expected == "generic")
    assert binance_owner.call_count == (expected == "binance")
    assert backpack_owner.call_count == (expected == "backpack")
    assert bybit_owner.call_count == (expected == "bybit")
    assert okx_owner.call_count == (expected == "okx")
    if expected == "okx":
        okx_owner.assert_called_once_with(
            api_key="okx-key",
            secret="okx-secret",
            testnet=False,
            extra_config=config["extra_config"],
        )


def test_generic_factory_preserves_okx_lifecycle_order() -> None:
    trace: list[str] = []
    account_config = object()
    product_ids = ["OKX:BTC-USDT-SWAP"]
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
            adapters.AccountInitializationConfig,
            "from_config",
            side_effect=parse,
        ) as parser,
        patch.object(
            adapters,
            "create_okx_live_adapter",
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
                "exchange": "okx",
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
    adapter.close.assert_not_called()


@pytest.mark.parametrize("failure_stage", ["parse", "pre_guard", "construct"])
def test_okx_early_failure_preserves_identity_and_stops(failure_stage) -> None:
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
            adapters.AccountInitializationConfig,
            "from_config",
            side_effect=parse,
        ),
        patch.object(
            adapters,
            "create_okx_live_adapter",
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
            {"mode": "live", "exchange": "okx"},
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
def test_okx_downstream_failure_preserves_close_precedence(
    failure_stage,
    close_mode,
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
            adapters.AccountInitializationConfig,
            "from_config",
            side_effect=parse,
        ),
        patch.object(
            adapters,
            "create_okx_live_adapter",
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
            {"mode": "live", "exchange": "okx"},
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


def test_generic_factory_source_has_one_okx_owner_entrypoint() -> None:
    tree = ast.parse(inspect.getsource(adapters.create_adapter))
    calls = [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]

    assert calls.count("create_okx_live_adapter") == 1
