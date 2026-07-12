"""Tests for src/core/product_registry.py."""

from decimal import Decimal

import pytest

from src.core.product_registry import (
    CapitalModel,
    InstrumentSpec,
    FeeModel,
    to_ccxt_symbol,
    to_exchange_name,
    to_base_quote,
    to_stream_key,
    resolve_exchange,
    list_known_products,
    resolve_contract_multiplier,
    resolve_fee_model,
    calculate_required_capital,
    calculate_notional_exposure,
    instrument_spec_from_product,
    validate_product_id,
)


def _spec(multiplier, fee_model=None):
    return InstrumentSpec(
        product_id="TEST:MNQ",
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=multiplier,
        fee_model=fee_model,
    )


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (None, Decimal("1")),
        (_spec(None), Decimal("1")),
        (_spec(Decimal("1")), Decimal("1")),
        (_spec(Decimal("2")), Decimal("2")),
    ],
)
def test_resolve_contract_multiplier(spec, expected):
    assert resolve_contract_multiplier(spec) == expected


@pytest.mark.parametrize("multiplier", [Decimal("0"), Decimal("-1")])
def test_resolve_contract_multiplier_rejects_non_positive(multiplier):
    with pytest.raises(ValueError, match="instrument multiplier must be positive"):
        resolve_contract_multiplier(_spec(multiplier))


@pytest.mark.parametrize(
    ("fee_model", "expected"),
    [
        (None, FeeModel.PERCENTAGE_NOTIONAL),
        (FeeModel.PERCENTAGE_NOTIONAL, FeeModel.PERCENTAGE_NOTIONAL),
        (FeeModel.PER_CONTRACT, FeeModel.PER_CONTRACT),
    ],
)
def test_resolve_fee_model(fee_model, expected):
    spec = None if fee_model is None else _spec(Decimal("1"), fee_model)
    assert resolve_fee_model(spec) == expected


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (None, Decimal("300")),
        (_spec(Decimal("2")), Decimal("600")),
        (
            InstrumentSpec(
                product_id="TEST:MNQ",
                exchange="test",
                symbol="MNQ",
                base="MNQ",
                quote="USD",
                multiplier=Decimal("2"),
                capital_model=CapitalModel.PER_CONTRACT,
                capital_per_contract=Decimal("2500"),
            ),
            Decimal("7500"),
        ),
    ],
)
def test_calculate_required_capital_modes(spec, expected):
    assert calculate_required_capital(Decimal("3"), Decimal("100"), spec) == expected


@pytest.mark.parametrize("capital_per_contract", [None, Decimal("0"), Decimal("-1")])
def test_per_contract_capital_requires_positive_amount(capital_per_contract):
    spec = InstrumentSpec(
        product_id="TEST:MNQ",
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        capital_model=CapitalModel.PER_CONTRACT,
        capital_per_contract=capital_per_contract,
    )
    with pytest.raises(ValueError, match="capital_per_contract must be positive"):
        calculate_required_capital(Decimal("1"), Decimal("100"), spec)


def test_notional_exposure_ignores_capital_and_fee_models():
    spec = InstrumentSpec(
        product_id="TEST:MNQ",
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
        fee_model=FeeModel.PER_CONTRACT,
        capital_model=CapitalModel.PER_CONTRACT,
        capital_per_contract=Decimal("2500"),
    )

    assert calculate_notional_exposure(Decimal("3"), Decimal("100"), spec) == Decimal("600")


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

    def test_dated_future_has_no_ccxt_mapping(self):
        with pytest.raises(ValueError, match="CCXT symbol mapping is unavailable"):
            to_ccxt_symbol("RITHMIC:MNQ-202509")


@pytest.mark.parametrize(
    "product_id",
    [
        "BINANCE:BTCUSDT-PERP",
        "BACKPACK:SOL_USDC-PERP",
        "RITHMIC:MNQ-202509",
        "CME:ES-202512",
    ],
)
def test_product_id_matrix_accepts_canonical_ids(product_id):
    assert validate_product_id(product_id) == product_id


@pytest.mark.parametrize(
    "product_id",
    [
        "RITHMIC:MNQ",
        "RITHMIC:MNQ-202500",
        "RITHMIC:MNQ-202513",
        "RITHMIC:MNQ-20259",
        "rithmic:MNQ-202509",
        "RITHMIC:mnq-202509",
        "RITHMIC::MNQ-202509",
    ],
)
def test_product_id_matrix_rejects_malformed_ids(product_id):
    with pytest.raises(ValueError, match="Cannot parse product_id"):
        validate_product_id(product_id)


def test_dated_future_registry_lookup_preserves_canonical_identity():
    spec = instrument_spec_from_product("RITHMIC:MNQ-202509", multiplier=Decimal("2"))

    assert spec.product_id == "RITHMIC:MNQ-202509"
    assert spec.exchange == "rithmic"
    assert spec.symbol == "MNQ-202509"
    assert spec.base == "MNQ"
    assert spec.quote == "USD"


@pytest.mark.parametrize(
    ("product_id", "expected_symbol"),
    [
        ("BINANCE:BTCUSDT-PERP", "BTC/USDT:USDT"),
        ("BINANCE:SOLUSDT-PERP", "SOL/USDT:USDT"),
        ("RITHMIC:MNQ-202509", "MNQ-202509"),
    ],
)
def test_instrument_spec_symbol_matrix(product_id, expected_symbol):
    assert instrument_spec_from_product(product_id).symbol == expected_symbol


def test_instrument_spec_rejects_unmapped_generic_perpetual():
    with pytest.raises(ValueError, match="Instrument symbol mapping is unavailable"):
        instrument_spec_from_product("YAHOO:BTC-PERP")


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

    def test_rithmic_dated_future(self):
        assert (
            to_stream_key("RITHMIC:MNQ-202509", "1m")
            == "stream:market:rithmic:mnq-202509:1m"
        )


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
