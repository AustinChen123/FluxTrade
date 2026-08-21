"""Ownership tests for CCXT account initialization."""

import ast
import inspect
import logging
from typing import cast
from unittest.mock import MagicMock

import ccxt
import pytest

from src.core.adapters import ccxt_adapter as ccxt_adapter_module
from src.core.adapters import ccxt_account_initialization as owner_module
from src.core.adapters.ccxt_account_initialization import (
    AccountPositionMode,
    initialize_ccxt_account,
)
from src.core.adapters.ccxt_adapter import (
    AccountInitializationConfig,
    CcxtExchangeAdapter,
)
from src.core.interfaces.exchange import ExchangeError


_SUCCESS_TRACE = [
    "guard",
    "load_markets",
    "guard",
    "guard",
    "set_position_mode",
    "guard",
    "guard",
    "fetch_position_mode",
    "guard",
    "guard",
    "set_margin_mode",
    "guard",
    "guard",
    "set_leverage",
    "guard",
    "guard",
    "fetch_leverage",
    "guard",
    "guard",
    "fetch_margin_mode",
    "guard",
]


def _adapter() -> CcxtExchangeAdapter:
    adapter = CcxtExchangeAdapter.__new__(CcxtExchangeAdapter)
    client = MagicMock()
    client.fetch_position_mode.return_value = {"hedged": False}
    vars(adapter).update(
        exchange_id="binance",
        client=client,
        logger=MagicMock(),
    )
    return adapter


def test_adapter_delegates_account_initialization_exactly_once(monkeypatch) -> None:
    adapter = _adapter()
    config = AccountInitializationConfig(product_ids=("BINANCE:BTCUSDT-PERP",))
    guard = MagicMock()
    owner = MagicMock()
    monkeypatch.setattr(
        ccxt_adapter_module,
        "initialize_ccxt_account",
        owner,
        raising=False,
    )

    adapter.initialize_account(config, operation_guard=guard)

    owner.assert_called_once_with(
        exchange_id="binance",
        client=adapter.client,
        logger=adapter.logger,
        config=config,
        operation_guard=guard,
    )


def test_shared_adapter_contains_no_account_initialization_implementation() -> None:
    source = inspect.getsource(CcxtExchangeAdapter)

    for forbidden in (
        "_ensure_one_way_position_mode",
        "_set_margin_mode",
        "_set_leverage",
        "_verify_leverage",
        "_verify_margin_mode",
        "_fetch_leverage_value",
        "_fetch_margin_mode_value",
        "no need to change",
        "fetchpositionmode",
    ):
        assert forbidden not in source


@pytest.mark.parametrize("exchange_id", ["binance", "bybit", "okx", "kraken"])
def test_owner_has_no_venue_gate_and_preserves_complete_call_order(
    exchange_id: str,
) -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    logger = cast(logging.Logger, MagicMock())
    trace: list[str] = []
    client_mock.load_markets.side_effect = lambda: trace.append("load_markets")
    client_mock.set_position_mode.side_effect = lambda *_args: trace.append(
        "set_position_mode"
    )
    client_mock.fetch_position_mode.side_effect = lambda *_args: (
        trace.append("fetch_position_mode") or {"hedged": False}
    )
    client_mock.set_margin_mode.side_effect = lambda *_args: trace.append(
        "set_margin_mode"
    )
    client_mock.set_leverage.side_effect = lambda *_args: trace.append("set_leverage")
    client_mock.fetch_leverage.side_effect = lambda *_args: (
        trace.append("fetch_leverage") or {"leverage": "3"}
    )
    client_mock.fetch_margin_mode.side_effect = lambda *_args: (
        trace.append("fetch_margin_mode") or {"marginMode": "cross"}
    )

    initialize_ccxt_account(
        exchange_id=exchange_id,
        client=client,
        logger=logger,
        config=AccountInitializationConfig(
            product_ids=("BINANCE:BTCUSDT-PERP",),
            leverage=3,
            margin_mode="cross",
        ),
        operation_guard=lambda: trace.append("guard"),
    )

    assert trace == _SUCCESS_TRACE
    client_mock.set_position_mode.assert_called_once_with(False, "BTC/USDT:USDT")
    client_mock.set_margin_mode.assert_called_once_with(
        "cross",
        "BTC/USDT:USDT",
        {"leverage": "3"},
    )
    client_mock.set_leverage.assert_called_once_with(3, "BTC/USDT:USDT")


@pytest.mark.parametrize("stop_at_guard", range(1, 15))
def test_each_guard_position_stops_before_every_later_provider_call(
    stop_at_guard: int,
) -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    trace: list[str] = []
    client_mock.load_markets.side_effect = lambda: trace.append("load_markets")
    client_mock.set_position_mode.side_effect = lambda *_args: trace.append(
        "set_position_mode"
    )
    client_mock.fetch_position_mode.side_effect = lambda *_args: (
        trace.append("fetch_position_mode") or {"hedged": False}
    )
    client_mock.set_margin_mode.side_effect = lambda *_args: trace.append(
        "set_margin_mode"
    )
    client_mock.set_leverage.side_effect = lambda *_args: trace.append("set_leverage")
    client_mock.fetch_leverage.side_effect = lambda *_args: (
        trace.append("fetch_leverage") or {"leverage": "3"}
    )
    client_mock.fetch_margin_mode.side_effect = lambda *_args: (
        trace.append("fetch_margin_mode") or {"marginMode": "cross"}
    )
    sentinel = RuntimeError(f"guard {stop_at_guard}")
    guard_calls = 0

    def guard() -> None:
        nonlocal guard_calls
        guard_calls += 1
        trace.append("guard")
        if guard_calls == stop_at_guard:
            raise sentinel

    with pytest.raises(RuntimeError) as raised:
        initialize_ccxt_account(
            exchange_id="okx",
            client=client,
            logger=cast(logging.Logger, MagicMock()),
            config=AccountInitializationConfig(
                product_ids=("OKX:BTCUSDT-PERP",),
                leverage=3,
                margin_mode="cross",
            ),
            operation_guard=guard,
        )

    assert raised.value is sentinel
    guard_position = [
        index for index, event in enumerate(_SUCCESS_TRACE) if event == "guard"
    ][stop_at_guard - 1]
    assert trace == _SUCCESS_TRACE[: guard_position + 1]


@pytest.mark.parametrize(
    ("primary", "expected_trace", "fallback_called"),
    [
        (None, ["guard", "fetch_leverages", "guard"], True),
        (
            ccxt.ExchangeError("primary failed"),
            [
                "guard",
                "fetch_leverage",
                "guard",
                "guard",
                "fetch_leverages",
                "guard",
            ],
            True,
        ),
        (
            {},
            [
                "guard",
                "fetch_leverage",
                "guard",
                "guard",
                "fetch_leverages",
                "guard",
            ],
            True,
        ),
        ({"leverage": "3"}, ["guard", "fetch_leverage", "guard"], False),
    ],
)
def test_leverage_primary_and_fallback_matrix(
    primary: object,
    expected_trace: list[str],
    fallback_called: bool,
) -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    trace: list[str] = []
    if primary is None:
        client_mock.fetch_leverage = None
    else:

        def fetch_leverage(_symbol: str) -> object:
            trace.append("fetch_leverage")
            if isinstance(primary, BaseException):
                raise primary
            return primary

        client_mock.fetch_leverage.side_effect = fetch_leverage

    client_mock.fetch_leverages.side_effect = lambda _symbols: (
        trace.append("fetch_leverages") or {"BTC/USDT:USDT": {"leverage": "3"}}
    )

    assert (
        owner_module._fetch_leverage_value(
            "okx",
            client,
            "BTC/USDT:USDT",
            lambda: trace.append("guard"),
        )
        == 3
    )

    assert trace == expected_trace
    if fallback_called:
        client_mock.fetch_leverages.assert_called_once_with(["BTC/USDT:USDT"])
    else:
        client_mock.fetch_leverages.assert_not_called()


@pytest.mark.parametrize(
    ("primary", "expected_trace", "fallback_called"),
    [
        (None, ["guard", "fetch_leverage", "guard"], True),
        (
            ccxt.ExchangeError("primary failed"),
            [
                "guard",
                "fetch_margin_mode",
                "guard",
                "guard",
                "fetch_leverage",
                "guard",
            ],
            True,
        ),
        (
            {},
            [
                "guard",
                "fetch_margin_mode",
                "guard",
                "guard",
                "fetch_leverage",
                "guard",
            ],
            True,
        ),
        (
            {"marginMode": "isolated"},
            ["guard", "fetch_margin_mode", "guard"],
            False,
        ),
    ],
)
def test_margin_primary_and_fallback_matrix(
    primary: object,
    expected_trace: list[str],
    fallback_called: bool,
) -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    trace: list[str] = []
    if primary is None:
        client_mock.fetch_margin_mode = None
    else:

        def fetch_margin_mode(_symbol: str) -> object:
            trace.append("fetch_margin_mode")
            if isinstance(primary, BaseException):
                raise primary
            return primary

        client_mock.fetch_margin_mode.side_effect = fetch_margin_mode

    client_mock.fetch_leverage.side_effect = lambda _symbol: (
        trace.append("fetch_leverage") or {"marginType": "isolated"}
    )

    assert (
        owner_module._fetch_margin_mode_value(
            "kraken",
            client,
            "BTC/USDT:USDT",
            lambda: trace.append("guard"),
        )
        == "isolated"
    )

    assert trace == expected_trace
    if fallback_called:
        client_mock.fetch_leverage.assert_called_once_with("BTC/USDT:USDT")
    else:
        client_mock.fetch_leverage.assert_not_called()


@pytest.mark.parametrize("stop_at_guard", [1, 2])
def test_leverage_fallback_guard_stops_at_exact_boundary(
    stop_at_guard: int,
) -> None:
    client_mock = MagicMock()
    client_mock.fetch_leverage = None
    trace: list[str] = []
    client_mock.fetch_leverages.side_effect = lambda _symbols: (
        trace.append("fetch_leverages") or {"BTC/USDT:USDT": {"leverage": "3"}}
    )
    sentinel = RuntimeError(f"guard {stop_at_guard}")
    guard_calls = 0

    def guard() -> None:
        nonlocal guard_calls
        guard_calls += 1
        trace.append("guard")
        if guard_calls == stop_at_guard:
            raise sentinel

    with pytest.raises(RuntimeError) as raised:
        owner_module._fetch_leverage_value(
            "okx",
            cast(ccxt.Exchange, client_mock),
            "BTC/USDT:USDT",
            guard,
        )

    assert raised.value is sentinel
    assert trace == ["guard", "fetch_leverages", "guard"][: stop_at_guard * 2 - 1]


@pytest.mark.parametrize("stop_at_guard", [1, 2])
def test_margin_fallback_guard_stops_at_exact_boundary(
    stop_at_guard: int,
) -> None:
    client_mock = MagicMock()
    client_mock.fetch_margin_mode = None
    trace: list[str] = []
    client_mock.fetch_leverage.side_effect = lambda _symbol: (
        trace.append("fetch_leverage") or {"marginType": "isolated"}
    )
    sentinel = RuntimeError(f"guard {stop_at_guard}")
    guard_calls = 0

    def guard() -> None:
        nonlocal guard_calls
        guard_calls += 1
        trace.append("guard")
        if guard_calls == stop_at_guard:
            raise sentinel

    with pytest.raises(RuntimeError) as raised:
        owner_module._fetch_margin_mode_value(
            "kraken",
            cast(ccxt.Exchange, client_mock),
            "BTC/USDT:USDT",
            guard,
        )

    assert raised.value is sentinel
    assert trace == ["guard", "fetch_leverage", "guard"][: stop_at_guard * 2 - 1]


def test_accepted_set_with_unsupported_margin_verification_warns_once() -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    client_mock.fetch_position_mode.return_value = {"hedged": False}
    client_mock.fetch_margin_mode = None
    client_mock.fetch_leverage = None
    logger_mock = MagicMock()

    initialize_ccxt_account(
        exchange_id="bybit",
        client=client,
        logger=cast(logging.Logger, logger_mock),
        config=AccountInitializationConfig(
            product_ids=("BYBIT:BTCUSDT-PERP",),
            margin_mode="cross",
        ),
    )

    logger_mock.warning.assert_called_once_with(
        "Margin mode verification unsupported for %s on %s after accepted set_margin_mode",
        "BTC/USDT:USDT",
        "bybit",
    )


@pytest.mark.parametrize(
    "message",
    [
        "no need to change",
        "not modified",
        "-4059",
        "110025",
        "110026",
        "110043",
        "140025",
        "140026",
        "140043",
        "34036",
    ],
)
def test_no_change_classifier_exact_positive_ledger(message: str) -> None:
    assert owner_module._is_account_setting_no_change_error(
        ccxt.ExchangeError(f"provider: {message}")
    )


@pytest.mark.parametrize(
    "message",
    [
        "need to change",
        "modified",
        "-4058",
        "110024",
        "110027",
        "110042",
        "140024",
        "140027",
        "140042",
        "34035",
    ],
)
def test_no_change_classifier_adjacent_negative_ledger(message: str) -> None:
    assert not owner_module._is_account_setting_no_change_error(
        ccxt.ExchangeError(f"provider: {message}")
    )


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("fetchPositionMode() is not supported yet", True),
        ("fetchPositionMode() failed", False),
        ("operation is not supported yet", False),
    ],
)
def test_position_mode_unsupported_requires_both_substrings(
    message: str,
    expected: bool,
) -> None:
    assert (
        owner_module._is_position_mode_verification_unsupported(
            ccxt.ExchangeError(message)
        )
        is expected
    )


@pytest.mark.parametrize(
    "seam",
    [
        "load_markets",
        "set_position_mode",
        "fetch_position_mode",
        "set_margin_mode",
        "set_leverage",
        "fetch_leverage",
        "fetch_leverages",
        "fetch_margin_mode",
    ],
)
def test_generic_provider_exception_escapes_by_identity(seam: str) -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    client_mock.fetch_position_mode.return_value = {"hedged": False}
    client_mock.fetch_leverage.return_value = {"leverage": "3"}
    client_mock.fetch_margin_mode.return_value = {"marginMode": "cross"}
    sentinel = RuntimeError(f"{seam} sentinel")
    if seam == "fetch_leverages":
        client_mock.fetch_leverage.return_value = {}
    getattr(client_mock, seam).side_effect = sentinel
    logger_mock = MagicMock()

    with pytest.raises(RuntimeError) as raised:
        initialize_ccxt_account(
            exchange_id="okx",
            client=client,
            logger=cast(logging.Logger, logger_mock),
            config=AccountInitializationConfig(
                product_ids=("OKX:BTCUSDT-PERP",),
                leverage=3,
                margin_mode="cross",
            ),
        )

    assert raised.value is sentinel
    logger_mock.warning.assert_not_called()


@pytest.mark.parametrize(
    ("seam", "config", "expected_message"),
    [
        (
            "load_markets",
            AccountInitializationConfig(product_ids=("OKX:BTCUSDT-PERP",)),
            "account_initialization_load_markets_failed: provider failure",
        ),
        (
            "set_position_mode",
            AccountInitializationConfig(product_ids=("OKX:BTCUSDT-PERP",)),
            "account_position_mode_set_failed: symbol=BTC/USDT:USDT error=provider failure",
        ),
        (
            "fetch_position_mode",
            AccountInitializationConfig(product_ids=("OKX:BTCUSDT-PERP",)),
            "account_position_mode_verify_failed: symbol=BTC/USDT:USDT error=provider failure",
        ),
        (
            "set_margin_mode",
            AccountInitializationConfig(
                product_ids=("OKX:BTCUSDT-PERP",),
                margin_mode="cross",
            ),
            "account_margin_mode_set_failed: symbol=BTC/USDT:USDT error=provider failure",
        ),
        (
            "set_leverage",
            AccountInitializationConfig(
                product_ids=("OKX:BTCUSDT-PERP",),
                leverage=3,
            ),
            "account_leverage_set_failed: symbol=BTC/USDT:USDT error=provider failure",
        ),
    ],
)
def test_provider_base_error_wraps_with_exact_message_and_cause(
    seam: str,
    config: AccountInitializationConfig,
    expected_message: str,
) -> None:
    client_mock = MagicMock()
    client = cast(ccxt.Exchange, client_mock)
    client_mock.fetch_position_mode.return_value = {"hedged": False}
    client_mock.fetch_leverage.return_value = {"leverage": "3"}
    client_mock.fetch_margin_mode.return_value = {"marginMode": "cross"}
    cause = ccxt.ExchangeError("provider failure")
    getattr(client_mock, seam).side_effect = cause

    with pytest.raises(ExchangeError) as raised:
        initialize_ccxt_account(
            exchange_id="okx",
            client=client,
            logger=cast(logging.Logger, MagicMock()),
            config=config,
        )

    assert raised.value.args == (expected_message,)
    assert raised.value.__cause__ is cause


@pytest.mark.parametrize("outcome", ["absent", "base_error", "malformed"])
def test_leverage_fallback_exhaustion_is_exact_and_has_no_raw_cause(
    outcome: str,
) -> None:
    client_mock = MagicMock()
    if outcome == "absent":
        client_mock.fetch_leverage = None
        client_mock.fetch_leverages = None
    elif outcome == "base_error":
        client_mock.fetch_leverage.side_effect = ccxt.ExchangeError("primary raw")
        client_mock.fetch_leverages.side_effect = ccxt.ExchangeError("fallback raw")
    else:
        client_mock.fetch_leverage.return_value = {}
        client_mock.fetch_leverages.return_value = {}
    guard = MagicMock()

    with pytest.raises(ExchangeError) as raised:
        owner_module._fetch_leverage_value(
            "kraken",
            cast(ccxt.Exchange, client_mock),
            "BTC/USDT:USDT",
            guard,
        )

    assert raised.value.args == (
        "account_leverage_verification_unsupported: exchange=kraken",
    )
    assert raised.value.__cause__ is None
    if outcome == "absent":
        assert guard.call_count == 0
    else:
        assert guard.call_count == 4
        client_mock.fetch_leverage.assert_called_once_with("BTC/USDT:USDT")
        client_mock.fetch_leverages.assert_called_once_with(["BTC/USDT:USDT"])


@pytest.mark.parametrize("outcome", ["absent", "base_error", "malformed"])
def test_margin_fallback_exhaustion_is_exact_and_has_no_raw_cause(
    outcome: str,
) -> None:
    client_mock = MagicMock()
    if outcome == "absent":
        client_mock.fetch_margin_mode = None
        client_mock.fetch_leverage = None
    elif outcome == "base_error":
        client_mock.fetch_margin_mode.side_effect = ccxt.ExchangeError("primary raw")
        client_mock.fetch_leverage.side_effect = ccxt.ExchangeError("fallback raw")
    else:
        client_mock.fetch_margin_mode.return_value = {}
        client_mock.fetch_leverage.return_value = {}
    guard = MagicMock()

    with pytest.raises(ExchangeError) as raised:
        owner_module._fetch_margin_mode_value(
            "kraken",
            cast(ccxt.Exchange, client_mock),
            "BTC/USDT:USDT",
            guard,
        )

    assert raised.value.args == (
        "account_margin_mode_verification_unsupported: exchange=kraken",
    )
    assert raised.value.__cause__ is None
    if outcome == "absent":
        assert guard.call_count == 0
    else:
        assert guard.call_count == 4
        client_mock.fetch_margin_mode.assert_called_once_with("BTC/USDT:USDT")
        client_mock.fetch_leverage.assert_called_once_with("BTC/USDT:USDT")


def test_config_compound_invalid_precedence_is_position_then_leverage() -> None:
    with pytest.raises(ExchangeError) as raised:
        AccountInitializationConfig.from_config(
            {
                "product_ids": ["OKX:BTCUSDT-PERP"],
                "position_mode": "hedge",
                "leverage": "invalid",
                "margin_mode": "invalid",
            },
            default_product_ids=[],
        )

    assert raised.value.args == (
        "unsupported_account_position_mode: position_mode=hedge",
    )


@pytest.mark.parametrize("raw_config", [None, {}])
def test_falsey_config_is_disabled(raw_config: dict | None) -> None:
    assert (
        AccountInitializationConfig.from_config(
            raw_config,
            default_product_ids=[],
        )
        is None
    )


@pytest.mark.parametrize(
    ("raw_config", "defaults", "expected_products"),
    [
        (
            {"product_ids": ["OKX:BTCUSDT-PERP"]},
            ["KRAKEN:ETHUSDT-PERP"],
            ("OKX:BTCUSDT-PERP",),
        ),
        (
            {"instrument_product_ids": ["BYBIT:BTCUSDT-PERP"]},
            ["KRAKEN:ETHUSDT-PERP"],
            ("BYBIT:BTCUSDT-PERP",),
        ),
        (
            {"leverage": "3"},
            ["KRAKEN:ETHUSDT-PERP"],
            ("KRAKEN:ETHUSDT-PERP",),
        ),
    ],
)
def test_config_product_precedence(
    raw_config: dict,
    defaults: list[str],
    expected_products: tuple[str, ...],
) -> None:
    config = AccountInitializationConfig.from_config(
        raw_config,
        default_product_ids=defaults,
    )

    assert config is not None
    assert config.product_ids == expected_products


@pytest.mark.parametrize(
    ("raw_config", "expected_message"),
    [
        (
            {"product_ids": ["OKX:BTCUSDT-PERP"], "leverage": "bad"},
            "invalid_account_leverage: leverage=bad",
        ),
        (
            {"product_ids": ["OKX:BTCUSDT-PERP"], "leverage": 0},
            "invalid_account_leverage: leverage=0",
        ),
        (
            {"product_ids": ["OKX:BTCUSDT-PERP"], "margin_mode": "portfolio"},
            "invalid_account_margin_mode: margin_mode=portfolio",
        ),
    ],
)
def test_config_invalid_value_errors_are_exact(
    raw_config: dict,
    expected_message: str,
) -> None:
    with pytest.raises(ExchangeError) as raised:
        AccountInitializationConfig.from_config(
            raw_config,
            default_product_ids=[],
        )

    assert raised.value.args == (expected_message,)


def test_config_normalizes_leverage_and_margin_mode() -> None:
    config = AccountInitializationConfig.from_config(
        {
            "product_ids": ["OKX:BTCUSDT-PERP"],
            "leverage": "5",
            "margin_mode": "ISOLATED",
        },
        default_product_ids=[],
    )

    assert config == AccountInitializationConfig(
        product_ids=("OKX:BTCUSDT-PERP",),
        leverage=5,
        margin_mode="isolated",
    )


def test_public_reexports_preserve_exact_config_types() -> None:
    from src.core.adapters import AccountInitializationConfig as public_config
    from src.core.adapters.ccxt_adapter import (
        AccountPositionMode as adapter_position_mode,
    )

    assert public_config is AccountInitializationConfig
    assert adapter_position_mode is AccountPositionMode


def test_owner_dependency_allowlist() -> None:
    tree = ast.parse(inspect.getsource(owner_module))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)

    assert imports == {
        "ccxt",
        "collections.abc",
        "dataclasses",
        "enum",
        "logging",
        "src.core.interfaces.exchange",
        "src.core.product_registry",
    }
