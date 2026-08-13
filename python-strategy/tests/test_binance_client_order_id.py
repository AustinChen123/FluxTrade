"""Tests for Binance-owned client-order-id formatting."""

import ast
import inspect

import pytest

from src.core import client_order_id as canonical_owner
from src.core.adapters.binance_client_order_id import (
    MAX_BINANCE_CLIENT_ORDER_ID_LENGTH,
    to_binance_client_order_id,
)
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.live_binance import LiveBinanceAdapter
from src.core.client_order_id import generate_client_order_id


CANONICAL_CLIENT_ORDER_ID = "strategy_1-worker_a-entry-1704067200000000000"


def test_binance_client_order_id_is_deterministic_and_within_limit() -> None:
    exchange_id = to_binance_client_order_id(CANONICAL_CLIENT_ORDER_ID)

    assert exchange_id == to_binance_client_order_id(CANONICAL_CLIENT_ORDER_ID)
    assert exchange_id == "strategy-2xeohcq2o0-cb6f4a088a4bb648"
    assert len(exchange_id) <= MAX_BINANCE_CLIENT_ORDER_ID_LENGTH
    assert exchange_id.startswith("strategy-")


def test_binance_client_order_id_avoids_collisions_for_many_strategies() -> None:
    exchange_ids = {
        to_binance_client_order_id(
            generate_client_order_id(
                f"strategy_{idx}",
                "worker_a",
                "entry",
                clock_ns=lambda idx=idx: 1704067200000000000 + idx,
            )
        )
        for idx in range(1000)
    }

    assert len(exchange_ids) == 1000


def test_generic_client_order_id_owner_has_no_venue_format_policy() -> None:
    tree = ast.parse(inspect.getsource(canonical_owner))

    assert not any(
        isinstance(node, ast.FunctionDef) and node.name == "to_exchange_format"
        for node in ast.walk(tree)
    )
    assert "binance" not in inspect.getsource(canonical_owner).lower()


def test_generic_adapter_validates_and_preserves_canonical_id() -> None:
    adapter = object.__new__(CcxtExchangeAdapter)
    formatter_source = inspect.getsource(CcxtExchangeAdapter._exchange_client_order_id)

    assert (
        adapter._exchange_client_order_id(CANONICAL_CLIENT_ORDER_ID)
        == CANONICAL_CLIENT_ORDER_ID
    )
    assert "exchange_id" not in formatter_source
    assert "binance" not in formatter_source.lower()
    with pytest.raises(ValueError, match="at least 4"):
        adapter._exchange_client_order_id("not-canonical")


def test_live_binance_adapter_owns_client_order_id_conversion() -> None:
    adapter = object.__new__(LiveBinanceAdapter)

    assert adapter._exchange_client_order_id(
        CANONICAL_CLIENT_ORDER_ID
    ) == to_binance_client_order_id(CANONICAL_CLIENT_ORDER_ID)
