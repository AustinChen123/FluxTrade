"""Tests for src/core/product_registry.py."""

import pytest

from src.core.product_registry import (
    to_ccxt_symbol,
    to_exchange_name,
    to_base_quote,
    to_stream_key,
    resolve_exchange,
    list_known_products,
)


class TestToCcxtSymbol:
    def test_known_binance_btc(self):
        assert to_ccxt_symbol("BINANCE:BTCUSDT-PERP") == "BTC/USDT:USDT"

    def test_known_bybit_eth(self):
        assert to_ccxt_symbol("BYBIT:ETHUSDT-PERP") == "ETH/USDT:USDT"

    def test_generic_parse_unknown_pair(self):
        assert to_ccxt_symbol("BINANCE:SOLUSDT-PERP") == "SOL/USDT:USDT"

    def test_generic_parse_usdc_quote(self):
        assert to_ccxt_symbol("BINANCE:BTCUSDC-PERP") == "BTC/USDC:USDC"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            to_ccxt_symbol("invalid")

    def test_no_perp_suffix_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            to_ccxt_symbol("BINANCE:BTCUSDT")


class TestToExchangeName:
    def test_binance(self):
        assert to_exchange_name("BINANCE:BTCUSDT-PERP") == "binance"

    def test_bybit(self):
        assert to_exchange_name("BYBIT:ETHUSDT-PERP") == "bybit"

    def test_generic_lowercase(self):
        assert to_exchange_name("OKX:BTCUSDT-PERP") == "okx"


class TestToBaseQuote:
    def test_btc_usdt(self):
        assert to_base_quote("BINANCE:BTCUSDT-PERP") == ("BTC", "USDT")

    def test_eth_usdt(self):
        assert to_base_quote("BYBIT:ETHUSDT-PERP") == ("ETH", "USDT")

    def test_generic_sol(self):
        assert to_base_quote("BINANCE:SOLUSDT-PERP") == ("SOL", "USDT")


class TestToStreamKey:
    def test_basic(self):
        assert to_stream_key("BINANCE:BTCUSDT-PERP", "15m") == "stream:market:binance:btcusdt:15m"

    def test_different_timeframe(self):
        assert to_stream_key("BYBIT:ETHUSDT-PERP", "1m") == "stream:market:bybit:ethusdt:1m"


class TestResolveExchange:
    def test_returns_tuple(self):
        exchange, symbol = resolve_exchange("BINANCE:BTCUSDT-PERP")
        assert exchange == "binance"
        assert symbol == "BTC/USDT:USDT"

    def test_generic_product(self):
        exchange, symbol = resolve_exchange("BINANCE:AVAXUSDT-PERP")
        assert exchange == "binance"
        assert symbol == "AVAX/USDT:USDT"


class TestListKnownProducts:
    def test_returns_list(self):
        products = list_known_products()
        assert isinstance(products, list)
        assert "BINANCE:BTCUSDT-PERP" in products

    def test_contains_multiple_exchanges(self):
        products = list_known_products()
        exchanges = {p.split(":")[0] for p in products}
        assert len(exchanges) >= 2
