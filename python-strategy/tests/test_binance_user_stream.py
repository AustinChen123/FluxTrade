"""Tests for the Binance-owned user-stream protocol boundary."""

import ast
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock

import ccxt
import pytest

import src.core.adapters.ccxt_adapter as ccxt_adapter_module
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeUserStreamUnsupported,
)


def _owner_functions():
    from src.core.adapters.binance_user_stream import (
        create_binance_user_stream_listen_key,
        keepalive_binance_user_stream,
    )

    return create_binance_user_stream_listen_key, keepalive_binance_user_stream


def _adapter(client: object) -> CcxtExchangeAdapter:
    adapter = object.__new__(CcxtExchangeAdapter)
    adapter.exchange_id = "binance"
    vars(adapter)["client"] = client
    return adapter


def test_shared_adapter_delegates_create_with_exact_identities(monkeypatch) -> None:
    client = object()
    owner = MagicMock(return_value="listen-key")
    monkeypatch.setattr(
        ccxt_adapter_module,
        "create_binance_user_stream_listen_key",
        owner,
        raising=False,
    )

    assert _adapter(client).create_user_stream_listen_key() == "listen-key"
    owner.assert_called_once_with("binance", client)


def test_shared_adapter_delegates_keepalive_with_exact_identities(monkeypatch) -> None:
    client = object()
    listen_key = "listen-key"
    owner = MagicMock()
    monkeypatch.setattr(
        ccxt_adapter_module,
        "keepalive_binance_user_stream",
        owner,
        raising=False,
    )

    assert _adapter(client).keepalive_user_stream(listen_key) is None
    owner.assert_called_once_with("binance", client, listen_key)


@pytest.mark.parametrize(
    ("owner_name", "adapter_method", "adapter_args", "owner_args"),
    [
        (
            "create_binance_user_stream_listen_key",
            "create_user_stream_listen_key",
            (),
            (),
        ),
        (
            "keepalive_binance_user_stream",
            "keepalive_user_stream",
            ("listen-key",),
            ("listen-key",),
        ),
    ],
)
def test_shared_adapter_preserves_owner_exception_identity(
    monkeypatch,
    owner_name: str,
    adapter_method: str,
    adapter_args: tuple[str, ...],
    owner_args: tuple[str, ...],
) -> None:
    client = object()
    sentinel = RuntimeError("owner sentinel")
    owner = MagicMock(side_effect=sentinel)
    monkeypatch.setattr(ccxt_adapter_module, owner_name, owner)

    with pytest.raises(RuntimeError) as exc_info:
        getattr(_adapter(client), adapter_method)(*adapter_args)

    assert exc_info.value is sentinel
    owner.assert_called_once_with("binance", client, *owner_args)


@pytest.mark.parametrize(
    ("exchange_id", "method_present"),
    [("binance", False), ("bybit", False), ("bybit", True)],
)
def test_create_rejects_unsupported_boundary_before_provider_call(
    exchange_id: str,
    method_present: bool,
) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(return_value={"listenKey": "listen-key"})
    client = SimpleNamespace()
    if method_present:
        client.fapiPrivatePostListenKey = provider

    with pytest.raises(
        ExchangeUserStreamUnsupported,
        match=rf"^user_stream_listen_key_unsupported: exchange={exchange_id}$",
    ):
        create(exchange_id, client)

    provider.assert_not_called()


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"listenKey": "listen-key"}, "listen-key"),
        ({"listenKey": 123}, "123"),
    ],
)
def test_create_returns_existing_string_projection(response, expected: str) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(return_value=response)

    assert (
        create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))
        == expected
    )
    provider.assert_called_once_with()


@pytest.mark.parametrize(
    "response",
    [{}, {"listenKey": ""}, {"listenKey": None}, "listen-key"],
)
def test_create_rejects_missing_or_malformed_listen_key(response) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(return_value=response)

    with pytest.raises(ExchangeError, match="^user_stream_listen_key_missing$"):
        create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))

    provider.assert_called_once_with()


@pytest.mark.parametrize(
    ("provider_error", "expected_message"),
    [
        (
            ccxt.ExchangeError("provider unavailable"),
            "user_stream_listen_key_create_failed: provider unavailable",
        ),
        (RuntimeError("generic sentinel"), None),
    ],
)
def test_create_preserves_provider_error_contract(
    provider_error: Exception,
    expected_message: str | None,
) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(side_effect=provider_error)

    if expected_message is None:
        with pytest.raises(RuntimeError) as exc_info:
            create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))
        assert exc_info.value is provider_error
    else:
        with pytest.raises(ExchangeError, match=f"^{expected_message}$") as exc_info:
            create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))
        assert exc_info.value.__cause__ is provider_error
    provider.assert_called_once_with()


@pytest.mark.parametrize("exchange_id", ["binance", "bybit"])
@pytest.mark.parametrize("method_present", [False, True])
@pytest.mark.parametrize("listen_key", ["", "listen-key"])
def test_keepalive_validation_precedence_matrix(
    exchange_id: str,
    method_present: bool,
    listen_key: str,
) -> None:
    _, keepalive = _owner_functions()
    provider = MagicMock()
    client = SimpleNamespace()
    if method_present:
        client.fapiPrivatePutListenKey = provider

    if exchange_id != "binance" or not method_present:
        with pytest.raises(
            ExchangeUserStreamUnsupported,
            match=rf"^user_stream_keepalive_unsupported: exchange={exchange_id}$",
        ):
            keepalive(exchange_id, client, listen_key)
        provider.assert_not_called()
    elif not listen_key:
        with pytest.raises(
            ExchangeError,
            match="^user_stream_keepalive_requires_listen_key$",
        ):
            keepalive(exchange_id, client, listen_key)
        provider.assert_not_called()
    else:
        assert keepalive(exchange_id, client, listen_key) is None
        provider.assert_called_once_with({"listenKey": listen_key})


@pytest.mark.parametrize(
    ("provider_error", "expected_message"),
    [
        (
            ccxt.ExchangeError("provider unavailable"),
            "user_stream_keepalive_failed: provider unavailable",
        ),
        (RuntimeError("generic sentinel"), None),
    ],
)
def test_keepalive_preserves_provider_error_contract(
    provider_error: Exception,
    expected_message: str | None,
) -> None:
    _, keepalive = _owner_functions()
    provider = MagicMock(side_effect=provider_error)
    client = SimpleNamespace(fapiPrivatePutListenKey=provider)

    if expected_message is None:
        with pytest.raises(RuntimeError) as exc_info:
            keepalive("binance", client, "listen-key")
        assert exc_info.value is provider_error
    else:
        with pytest.raises(ExchangeError, match=f"^{expected_message}$") as exc_info:
            keepalive("binance", client, "listen-key")
        assert exc_info.value.__cause__ is provider_error
    provider.assert_called_once_with({"listenKey": "listen-key"})


def test_owner_and_shared_adapter_keep_the_dependency_boundary() -> None:
    from src.core.adapters import binance_user_stream

    owner_source = inspect.getsource(binance_user_stream)
    owner_imports = {
        node.module if isinstance(node, ast.ImportFrom) else alias.name
        for node in ast.walk(ast.parse(owner_source))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    shared_create = inspect.getsource(CcxtExchangeAdapter.create_user_stream_listen_key)
    shared_keepalive = inspect.getsource(CcxtExchangeAdapter.keepalive_user_stream)

    assert owner_imports == {"typing", "ccxt", "src.core.interfaces.exchange"}
    assert "fapiPrivatePostListenKey" not in shared_create
    assert "fapiPrivatePutListenKey" not in shared_keepalive
    assert "listenKey" not in shared_create + shared_keepalive
