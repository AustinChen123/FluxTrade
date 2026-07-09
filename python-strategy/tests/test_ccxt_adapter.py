"""Tests for CcxtExchangeAdapter and adapter factory."""

from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.adapters import create_adapter
from src.core.adapters.ccxt_adapter import (
    AccountInitializationConfig,
    AccountPositionMode,
    CcxtExchangeAdapter,
)
from src.core.adapters.live_binance import LiveBinanceAdapter
from src.core.adapters.simulated import SimulatedAdapter
from src.core.client_order_id import to_exchange_format
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeUserStreamUnsupported,
    InsufficientFundsError,
    NetworkError,
)
from src.core.orm_models import Order
from src.core.product_registry import (
    PrecisionMode,
    instrument_spec_from_ccxt_market,
    instrument_spec_from_product,
    validate_min_notional,
)

CANONICAL_CLIENT_ORDER_ID = "strategy_1-worker_a-entry-1704067200000000000"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_order(**overrides) -> Order:
    defaults = {
        "id": 1,
        "strategy_id": "test-strat",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "side": "buy",
        "type": "market",
        "quantity": Decimal("0.01"),
        "price": None,
        "trigger_price": None,
        "status": "OPEN",
        "exchange_order_id": None,
        "client_order_id": None,
    }
    defaults.update(overrides)
    order = MagicMock(spec=Order)
    for k, v in defaults.items():
        setattr(order, k, v)
    return order


def _empty_btc_market() -> dict:
    return {"BTC/USDT:USDT": {}}


@pytest.fixture
def mock_ccxt_client():
    """A mock CCXT exchange client."""
    client = MagicMock()
    client.apiKey = "test-key"
    client.secret = "test-secret"
    client.load_markets.return_value = _empty_btc_market()
    return client


@pytest.fixture
def adapter(mock_ccxt_client):
    """CcxtExchangeAdapter with a mocked CCXT client injected."""
    with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
        mock_exchange_cls = MagicMock(return_value=mock_ccxt_client)
        mock_ccxt.binance = mock_exchange_cls
        setattr(mock_ccxt, "binance", mock_exchange_cls)
        a = CcxtExchangeAdapter(
            exchange_id="binance",
            api_key="test-key",
            secret="test-secret",
            testnet=True,
        )
    # Ensure client is our mock
    a.client = mock_ccxt_client
    return a


# ---------------------------------------------------------------------------
# CcxtExchangeAdapter
# ---------------------------------------------------------------------------


class TestCcxtAdapterInit:
    def test_invalid_exchange_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            CcxtExchangeAdapter(exchange_id="nonexistent_exchange_xyz")

    def test_valid_exchange_creates_adapter(self, adapter):
        assert adapter.exchange_id == "binance"


class TestPlaceOrder:
    def test_market_order(self, adapter, mock_ccxt_client):
        mock_ccxt_client.create_order.return_value = {"id": "EX-123"}
        order = _make_order()

        result = adapter.place_order(order)

        assert result == "EX-123"
        mock_ccxt_client.create_order.assert_called_once()
        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["symbol"] == "BTC/USDT:USDT"
        assert call_kwargs.kwargs["type"] == "market"
        assert call_kwargs.kwargs["side"] == "buy"

    def test_market_reduce_only_intent_passes_reduce_only_param(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.create_order.return_value = {"id": "EX-FLAT"}
        order = _make_order(
            side="sell",
            type="market",
            intent_payload={"reduce_only": True, "source": "kill_switch"},
        )

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["params"]["reduceOnly"] is True

    def test_limit_order_includes_gtc(self, adapter, mock_ccxt_client):
        mock_ccxt_client.create_order.return_value = {"id": "EX-456"}
        order = _make_order(type="limit", price=Decimal("50000"))

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["params"]["timeInForce"] == "GTC"
        assert call_kwargs.kwargs["price"] == "50000"

    @pytest.mark.parametrize(
        ("order_type", "trigger_param", "ccxt_type"),
        [
            ("stop_loss", "stopLossPrice", "STOP_MARKET"),
            ("take_profit", "takeProfitPrice", "TAKE_PROFIT_MARKET"),
        ],
    )
    def test_binance_protective_orders_use_futures_conditional_mapping(
        self,
        adapter,
        mock_ccxt_client,
        order_type,
        trigger_param,
        ccxt_type,
    ):
        mock_ccxt_client.create_order.return_value = {"id": "EX-COND"}
        order = _make_order(
            side="sell",
            type=order_type,
            quantity=Decimal("0.01"),
            trigger_price=Decimal("41000"),
            client_order_id=CANONICAL_CLIENT_ORDER_ID,
        )

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["type"] == ccxt_type
        assert call_kwargs.kwargs["price"] is None
        assert call_kwargs.kwargs["params"][trigger_param] == "41000"
        assert call_kwargs.kwargs["params"]["reduceOnly"] is True
        assert call_kwargs.kwargs["params"]["clientAlgoId"] == to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )

    def test_non_binance_protective_order_mapping_fails_closed(
        self, adapter, mock_ccxt_client
    ):
        adapter.exchange_id = "bybit"
        order = _make_order(
            product_id="BYBIT:BTCUSDT-PERP",
            side="sell",
            type="stop_loss",
            quantity=Decimal("0.01"),
            trigger_price=Decimal("41000"),
        )

        with pytest.raises(ExchangeError, match="conditional_order_mapping_unsupported"):
            adapter.place_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    def test_validate_order_rejects_unsupported_protective_mapping_before_submit(
        self, adapter, mock_ccxt_client
    ):
        adapter.exchange_id = "bybit"
        order = _make_order(
            product_id="BYBIT:BTCUSDT-PERP",
            side="sell",
            type="take_profit",
            quantity=Decimal("0.01"),
            trigger_price=Decimal("43000"),
        )

        with pytest.raises(ExchangeError, match="conditional_order_mapping_unsupported"):
            adapter.validate_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    @pytest.mark.parametrize("order_type", ["trailing_stop", "iceberg_stop"])
    def test_validate_order_rejects_unmapped_order_types_before_submit(
        self, adapter, mock_ccxt_client, order_type
    ):
        order = _make_order(
            side="sell",
            type=order_type,
            quantity=Decimal("0.01"),
            trigger_price=Decimal("41000"),
        )

        with pytest.raises(ExchangeError, match="mapping_unsupported"):
            adapter.validate_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    def test_fetches_instrument_spec_from_binance_filters(self, adapter, mock_ccxt_client):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "limits": {
                    "amount": {"min": "0.001"},
                    "cost": {"min": "5"},
                },
                "info": {
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.10",
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.001",
                            "stepSize": "0.001",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "10",
                        },
                    ],
                },
            },
        }

        spec = adapter.get_instrument_spec("BINANCE:BTCUSDT-PERP")

        assert spec.quantity_step == Decimal("0.001")
        assert spec.price_tick == Decimal("0.10")
        assert spec.min_quantity == Decimal("0.001")
        assert spec.min_notional == Decimal("10")
        assert spec.multiplier is None
        assert spec.tick_value is None
        assert spec.fee_model is None
        assert spec.session_calendar_id is None

    def test_fetches_instrument_spec_from_ccxt_precision(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.precisionMode = 2
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "precision": {
                    "amount": 3,
                    "price": 1,
                },
                "limits": {
                    "amount": {"min": "0.001"},
                    "cost": {"min": "5"},
                },
            },
        }

        spec = adapter.get_instrument_spec("BYBIT:BTCUSDT-PERP")

        assert spec.quantity_step == Decimal("0.001")
        assert spec.price_tick == Decimal("0.10")
        assert spec.min_quantity == Decimal("0.001")
        assert spec.min_notional == Decimal("5")

    def test_ccxt_precision_mode_tick_size_treats_integer_as_step(self):
        spec = instrument_spec_from_ccxt_market(
            "BYBIT:BTCUSDT-PERP",
            {
                "precision": {
                    "amount": 3,
                    "price": 1,
                },
            },
            precision_mode=PrecisionMode.TICK_SIZE,
        )

        assert spec.quantity_step == Decimal("3")
        assert spec.price_tick == Decimal("1")

    def test_ccxt_precision_mode_tick_size_accepts_fractional_step(self):
        spec = instrument_spec_from_ccxt_market(
            "BYBIT:BTCUSDT-PERP",
            {
                "precision": {
                    "amount": "0.001",
                    "price": "0.10",
                },
            },
            precision_mode=PrecisionMode.TICK_SIZE,
        )

        assert spec.quantity_step == Decimal("0.001")
        assert spec.price_tick == Decimal("0.10")

    def test_ccxt_precision_mode_significant_digits_uses_filters(self):
        spec = instrument_spec_from_ccxt_market(
            "BYBIT:BTCUSDT-PERP",
            {
                "precision": {
                    "amount": 3,
                    "price": 2,
                },
                "info": {
                    "lotSizeFilter": {"qtyStep": "0.001"},
                    "priceFilter": {"tickSize": "0.10"},
                },
            },
            precision_mode=PrecisionMode.SIGNIFICANT_DIGITS,
        )

        assert spec.quantity_step == Decimal("0.001")
        assert spec.price_tick == Decimal("0.10")

    def test_ccxt_precision_without_mode_is_ignored_with_warning(self, caplog):
        spec = instrument_spec_from_ccxt_market(
            "BYBIT:BTCUSDT-PERP",
            {
                "precision": {
                    "amount": 3,
                    "price": "0.10",
                },
            },
            precision_mode=None,
        )

        assert spec.quantity_step is None
        assert spec.price_tick is None
        assert "without supported precisionMode" in caplog.text

    @pytest.mark.parametrize(
        ("precision_mode", "value", "expected_step"),
        [
            (PrecisionMode.DECIMAL_PLACES, -1, Decimal("10")),
            (PrecisionMode.DECIMAL_PLACES, 0, Decimal("1")),
            (PrecisionMode.DECIMAL_PLACES, "0", Decimal("1")),
            (PrecisionMode.DECIMAL_PLACES, 3, Decimal("0.001")),
            (PrecisionMode.DECIMAL_PLACES, "0.001", None),
            (PrecisionMode.TICK_SIZE, -1, None),
            (PrecisionMode.TICK_SIZE, 0, None),
            (PrecisionMode.TICK_SIZE, "0", None),
            (PrecisionMode.TICK_SIZE, 3, Decimal("3")),
            (PrecisionMode.TICK_SIZE, "0.001", Decimal("0.001")),
            (PrecisionMode.SIGNIFICANT_DIGITS, -1, None),
            (PrecisionMode.SIGNIFICANT_DIGITS, 0, None),
            (PrecisionMode.SIGNIFICANT_DIGITS, "0", None),
            (PrecisionMode.SIGNIFICANT_DIGITS, 3, None),
            (PrecisionMode.SIGNIFICANT_DIGITS, "0.001", None),
        ],
    )
    def test_ccxt_precision_mode_value_matrix(
        self, precision_mode, value, expected_step
    ):
        spec = instrument_spec_from_ccxt_market(
            "BYBIT:BTCUSDT-PERP",
            {
                "precision": {
                    "amount": value,
                },
            },
            precision_mode=precision_mode,
        )

        assert spec.quantity_step == expected_step

    def test_fetches_instrument_spec_from_bybit_filters(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "lotSizeFilter": {
                        "qtyStep": "0.001",
                        "minOrderQty": "0.001",
                        "minNotionalValue": "5",
                    },
                    "priceFilter": {
                        "tickSize": "0.10",
                    },
                },
            },
        }

        spec = adapter.get_instrument_spec("BYBIT:BTCUSDT-PERP")

        assert spec.quantity_step == Decimal("0.001")
        assert spec.price_tick == Decimal("0.10")
        assert spec.min_quantity == Decimal("0.001")
        assert spec.min_notional == Decimal("5")

    def test_quantizes_order_from_non_binance_precision(self, adapter, mock_ccxt_client):
        mock_ccxt_client.precisionMode = 4
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "precision": {
                    "amount": "0.001",
                    "price": "0.10",
                },
                "limits": {
                    "cost": {"min": "10"},
                },
            },
        }
        mock_ccxt_client.create_order.return_value = {"id": "EX-BYBIT"}
        order = _make_order(
            product_id="BYBIT:BTCUSDT-PERP",
            type="limit",
            quantity=Decimal("0.0109"),
            price=Decimal("50123.456"),
        )

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert order.quantity == Decimal("0.010")
        assert order.price == Decimal("50123.40")
        assert call_kwargs.kwargs["amount"] == "0.010"
        assert call_kwargs.kwargs["price"] == "50123.40"

    def test_maps_ccxt_precision_mode_constants(self, adapter, mock_ccxt_client):
        mock_ccxt_client.precisionMode = 2
        assert adapter._precision_mode() == PrecisionMode.DECIMAL_PLACES
        mock_ccxt_client.precisionMode = 3
        assert adapter._precision_mode() == PrecisionMode.SIGNIFICANT_DIGITS
        mock_ccxt_client.precisionMode = 4
        assert adapter._precision_mode() == PrecisionMode.TICK_SIZE

    def test_missing_market_rules_fail_safe_without_caching(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "precision": {"amount": "0.001", "price": "0.10"},
            },
        }

        with pytest.raises(ExchangeError, match="market_not_found"):
            adapter.get_instrument_spec("BINANCE:ETHUSDT-PERP")
        assert "BINANCE:ETHUSDT-PERP" not in adapter._instrument_specs

        with pytest.raises(ExchangeError, match="market_not_found"):
            adapter.get_instrument_spec("BINANCE:ETHUSDT-PERP")
        assert mock_ccxt_client.load_markets.call_count == 2

    def test_warms_instrument_specs_for_known_products(self, adapter, mock_ccxt_client):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                    ],
                },
            },
        }
        mock_ccxt_client.create_order.return_value = {"id": "EX-456"}

        adapter.warm_instrument_specs(["BINANCE:BTCUSDT-PERP"])
        adapter.place_order(
            _make_order(
                type="limit",
                quantity=Decimal("0.0109"),
                price=Decimal("50123.456"),
            )
        )

        mock_ccxt_client.load_markets.assert_called_once()

    def test_quantizes_order_before_placing(self, adapter, mock_ccxt_client):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        mock_ccxt_client.create_order.return_value = {"id": "EX-456"}
        order = _make_order(
            type="limit",
            quantity=Decimal("0.0109"),
            price=Decimal("50123.456"),
        )

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert order.quantity == Decimal("0.010")
        assert order.price == Decimal("50123.40")
        assert call_kwargs.kwargs["amount"] == "0.010"
        assert call_kwargs.kwargs["price"] == "50123.40"

    def test_quantizes_sell_limit_price_up_before_placing(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        mock_ccxt_client.create_order.return_value = {"id": "EX-SELL"}
        order = _make_order(
            side="sell",
            type="limit",
            quantity=Decimal("0.0109"),
            price=Decimal("50123.456"),
        )

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert order.quantity == Decimal("0.010")
        assert order.price == Decimal("50123.50")
        assert call_kwargs.kwargs["amount"] == "0.010"
        assert call_kwargs.kwargs["price"] == "50123.50"

    @pytest.mark.parametrize(
        ("order_type", "side", "trigger_price", "expected_trigger_price"),
        [
            ("stop_loss", "sell", Decimal("49999.987"), Decimal("50000.00")),
            ("stop_loss", "buy", Decimal("49999.987"), Decimal("49999.90")),
            ("take_profit", "sell", Decimal("50123.456"), Decimal("50123.50")),
            ("take_profit", "buy", Decimal("50123.456"), Decimal("50123.40")),
        ],
    )
    def test_quantizes_protective_trigger_price_directionally_before_placing(
        self,
        adapter,
        mock_ccxt_client,
        order_type,
        side,
        trigger_price,
        expected_trigger_price,
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        mock_ccxt_client.create_order.return_value = {"id": "EX-SL"}
        order = _make_order(
            side=side,
            type=order_type,
            quantity=Decimal("0.0109"),
            price=None,
            trigger_price=trigger_price,
        )

        adapter.place_order(order)

        assert order.quantity == Decimal("0.010")
        assert order.trigger_price == expected_trigger_price
        mock_ccxt_client.create_order.assert_called_once()

    def test_rejects_unknown_off_tick_trigger_price_policy(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        order = _make_order(
            type="conditional",
            quantity=Decimal("0.010"),
            price=None,
            trigger_price=Decimal("49999.987"),
        )

        with pytest.raises(ExchangeError, match="trigger_price_off_tick"):
            adapter.place_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    def test_rejects_order_below_min_notional(self, adapter, mock_ccxt_client):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        order = _make_order(
            type="limit",
            quantity=Decimal("0.001"),
            price=Decimal("5000"),
        )

        with pytest.raises(ExchangeError, match="min_notional_not_met"):
            adapter.place_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    def test_rejects_market_order_when_min_notional_has_no_reference_price(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        order = _make_order(
            type="market",
            quantity=Decimal("0.001"),
            price=None,
        )

        with pytest.raises(ExchangeError, match="min_notional_unverifiable"):
            adapter.place_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    def test_checks_market_min_notional_with_reference_price(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        mock_ccxt_client.create_order.return_value = {"id": "EX-MARKET"}
        order = _make_order(
            type="market",
            quantity=Decimal("0.001"),
            price=None,
        )
        order.min_notional_reference_price = Decimal("12000")

        result = adapter.place_order(order)

        assert result == "EX-MARKET"
        mock_ccxt_client.create_order.assert_called_once()

    def test_rejects_market_min_notional_with_low_reference_price(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "info": {
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                },
            },
        }
        order = _make_order(
            type="market",
            quantity=Decimal("0.001"),
            price=None,
        )
        order.min_notional_reference_price = Decimal("5000")

        with pytest.raises(ExchangeError, match="min_notional_not_met"):
            adapter.place_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    @pytest.mark.parametrize(
        ("min_notional", "price", "reference_price", "raises"),
        [
            (None, None, None, None),
            (Decimal("10"), None, None, "min_notional_unverifiable"),
            (Decimal("10"), Decimal("5000"), None, "min_notional_not_met"),
            (Decimal("10"), Decimal("12000"), None, None),
            (Decimal("10"), None, Decimal("5000"), "min_notional_not_met"),
            (Decimal("10"), None, Decimal("12000"), None),
        ],
    )
    def test_min_notional_validation_matrix(
        self, min_notional, price, reference_price, raises
    ):
        spec = instrument_spec_from_product(
            "BINANCE:BTCUSDT-PERP",
            min_notional=min_notional,
        )

        if raises is None:
            validate_min_notional(
                quantity=Decimal("0.001"),
                price=price,
                reference_price=reference_price,
                spec=spec,
            )
        else:
            with pytest.raises(ValueError, match=raises):
                validate_min_notional(
                    quantity=Decimal("0.001"),
                    price=price,
                    reference_price=reference_price,
                    spec=spec,
                )

    def test_market_rule_load_error_raises_exchange_error(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib

        mock_ccxt_client.load_markets.side_effect = ccxt_lib.ExchangeError("rules unavailable")
        order = _make_order()

        with pytest.raises(ExchangeError, match="Failed to load market rules"):
            adapter.place_order(order)

        mock_ccxt_client.create_order.assert_not_called()

    def test_binance_order_passes_client_order_id(self, adapter, mock_ccxt_client):
        mock_ccxt_client.create_order.return_value = {"id": "EX-789"}
        order = _make_order(client_order_id=CANONICAL_CLIENT_ORDER_ID)

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["params"]["newClientOrderId"] == to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )

    def test_non_binance_order_passes_client_order_id(self, mock_ccxt_client):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_exchange_cls = MagicMock(return_value=mock_ccxt_client)
            mock_ccxt.bybit = mock_exchange_cls
            setattr(mock_ccxt, "bybit", mock_exchange_cls)
            adapter = CcxtExchangeAdapter(
                exchange_id="bybit",
                api_key="test-key",
                secret="test-secret",
            )
        adapter.client = mock_ccxt_client
        mock_ccxt_client.create_order.return_value = {"id": "EX-789"}
        order = _make_order(
            product_id="BYBIT:BTCUSDT-PERP",
            client_order_id=CANONICAL_CLIENT_ORDER_ID,
        )

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["params"]["clientOrderId"] == CANONICAL_CLIENT_ORDER_ID

    def test_insufficient_funds_raises(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.create_order.side_effect = ccxt_lib.InsufficientFunds("no money")
        order = _make_order()

        with pytest.raises(InsufficientFundsError):
            adapter.place_order(order)

    def test_network_error_raises(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.create_order.side_effect = ccxt_lib.NetworkError("timeout")
        order = _make_order()

        with pytest.raises(NetworkError):
            adapter.place_order(order)

    def test_generic_ccxt_error_raises_exchange_error(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.create_order.side_effect = ccxt_lib.ExchangeError("bad request")
        order = _make_order()

        with pytest.raises(ExchangeError):
            adapter.place_order(order)


class TestCancelOrder:
    def test_cancel_success(self, adapter, mock_ccxt_client):
        result = adapter.cancel_order("EX-123", "BINANCE:BTCUSDT-PERP")
        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with("EX-123", "BTC/USDT:USDT")

    def test_binance_cancel_by_client_order_id(self, adapter, mock_ccxt_client):
        result = adapter.cancel_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BINANCE:BTCUSDT-PERP",
        )

        exchange_client_order_id = to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )
        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with(
            exchange_client_order_id,
            "BTC/USDT:USDT",
            params={"origClientOrderId": exchange_client_order_id},
        )

    def test_non_binance_cancel_by_client_order_id(self, mock_ccxt_client):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_exchange_cls = MagicMock(return_value=mock_ccxt_client)
            mock_ccxt.bybit = mock_exchange_cls
            setattr(mock_ccxt, "bybit", mock_exchange_cls)
            adapter = CcxtExchangeAdapter(
                exchange_id="bybit",
                api_key="test-key",
                secret="test-secret",
            )
        adapter.client = mock_ccxt_client

        result = adapter.cancel_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BYBIT:BTCUSDT-PERP",
        )

        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with(
            CANONICAL_CLIENT_ORDER_ID,
            "BTC/USDT:USDT",
            params={"clientOrderId": CANONICAL_CLIENT_ORDER_ID},
        )

    def test_cancel_order_not_found(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.cancel_order.side_effect = ccxt_lib.OrderNotFound("not found")
        result = adapter.cancel_order("EX-999", "BINANCE:BTCUSDT-PERP")
        assert result is False

    def test_cancel_generic_error(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.cancel_order.side_effect = ccxt_lib.ExchangeError("fail")
        result = adapter.cancel_order("EX-999", "BINANCE:BTCUSDT-PERP")
        assert result is False


class TestConditionalOrderIdRouting:
    """Binance futures conditional (algo) orders live in a separate id
    namespace; ccxt only routes cancel/fetch to the algo endpoints when the
    trigger/conditional param is present (ccxt binance.py cancel_order /
    fetch_order). Regression for the OCO sibling staying live after cancel."""

    @pytest.mark.parametrize("order_type", ["stop_loss", "take_profit"])
    def test_cancel_conditional_order_passes_trigger_param(
        self, adapter, mock_ccxt_client, order_type
    ):
        result = adapter.cancel_order(
            "ALGO-123",
            "BINANCE:BTCUSDT-PERP",
            order_type=order_type,
        )

        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with(
            "ALGO-123",
            "BTC/USDT:USDT",
            params={"trigger": True},
        )

    @pytest.mark.parametrize("order_type", [None, "market", "limit"])
    def test_cancel_regular_order_does_not_pass_trigger_param(
        self, adapter, mock_ccxt_client, order_type
    ):
        result = adapter.cancel_order(
            "EX-123",
            "BINANCE:BTCUSDT-PERP",
            order_type=order_type,
        )

        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with("EX-123", "BTC/USDT:USDT")

    def test_cancel_conditional_by_client_id_uses_client_algo_id(
        self, adapter, mock_ccxt_client
    ):
        exchange_client_order_id = to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )

        result = adapter.cancel_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BINANCE:BTCUSDT-PERP",
            order_type="stop_loss",
        )

        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with(
            exchange_client_order_id,
            "BTC/USDT:USDT",
            params={"clientAlgoId": exchange_client_order_id, "trigger": True},
        )

    def test_fetch_conditional_by_client_id_uses_client_algo_id(
        self, adapter, mock_ccxt_client
    ):
        exchange_client_order_id = to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )
        mock_ccxt_client.fetch_order.return_value = {
            "id": "ALGO-123",
            "status": "open",
            "filled": "0",
        }

        snapshot = adapter.get_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BINANCE:BTCUSDT-PERP",
            order_type="take_profit",
        )

        assert snapshot is not None
        mock_ccxt_client.fetch_order.assert_called_once_with(
            exchange_client_order_id,
            "BTC/USDT:USDT",
            params={"clientAlgoId": exchange_client_order_id, "trigger": True},
        )

    def test_non_binance_conditional_cancel_does_not_use_binance_algo_params(
        self, mock_ccxt_client
    ):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_exchange_cls = MagicMock(return_value=mock_ccxt_client)
            setattr(mock_ccxt, "bybit", mock_exchange_cls)
            adapter = CcxtExchangeAdapter(
                exchange_id="bybit",
                api_key="test-key",
                secret="test-secret",
            )
        adapter.client = mock_ccxt_client

        result = adapter.cancel_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BYBIT:BTCUSDT-PERP",
            order_type="stop_loss",
        )

        assert result is True
        mock_ccxt_client.cancel_order.assert_called_once_with(
            CANONICAL_CLIENT_ORDER_ID,
            "BTC/USDT:USDT",
            params={"clientOrderId": CANONICAL_CLIENT_ORDER_ID},
        )


class TestGetOrderByClientId:
    def test_binance_fetches_order_with_exchange_safe_client_id(self, adapter, mock_ccxt_client):
        exchange_client_order_id = to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )
        mock_ccxt_client.fetch_order.return_value = {
            "id": "EX-123",
            "status": "open",
            "clientOrderId": exchange_client_order_id,
            "filled": "0.25",
            "average": "42000.5",
            "fee": {"cost": "0.12"},
        }

        snapshot = adapter.get_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BINANCE:BTCUSDT-PERP",
        )

        assert snapshot is not None
        assert snapshot.client_order_id == CANONICAL_CLIENT_ORDER_ID
        assert snapshot.exchange_order_id == "EX-123"
        assert snapshot.status == "open"
        assert snapshot.filled_quantity == Decimal("0.25")
        assert snapshot.average_price == Decimal("42000.5")
        assert snapshot.fee == Decimal("0.12")
        assert snapshot.raw["clientOrderId"] == exchange_client_order_id
        mock_ccxt_client.fetch_order.assert_called_once_with(
            exchange_client_order_id,
            "BTC/USDT:USDT",
            params={"origClientOrderId": exchange_client_order_id},
        )

    def test_non_binance_fetches_order_with_client_order_id(self, mock_ccxt_client):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_exchange_cls = MagicMock(return_value=mock_ccxt_client)
            mock_ccxt.bybit = mock_exchange_cls
            setattr(mock_ccxt, "bybit", mock_exchange_cls)
            adapter = CcxtExchangeAdapter(
                exchange_id="bybit",
                api_key="test-key",
                secret="test-secret",
            )
        adapter.client = mock_ccxt_client
        mock_ccxt_client.fetch_order.return_value = {
            "id": "EX-456",
            "status": "closed",
            "clientOrderId": CANONICAL_CLIENT_ORDER_ID,
        }

        snapshot = adapter.get_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BYBIT:BTCUSDT-PERP",
        )

        assert snapshot is not None
        assert snapshot.exchange_order_id == "EX-456"
        assert snapshot.status == "closed"
        mock_ccxt_client.fetch_order.assert_called_once_with(
            CANONICAL_CLIENT_ORDER_ID,
            "BTC/USDT:USDT",
            params={"clientOrderId": CANONICAL_CLIENT_ORDER_ID},
        )

    def test_fetch_order_does_not_use_limit_price_as_average(self, adapter, mock_ccxt_client):
        exchange_client_order_id = to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )
        mock_ccxt_client.fetch_order.return_value = {
            "id": "EX-123",
            "status": "open",
            "clientOrderId": exchange_client_order_id,
            "filled": "0.25",
            "average": None,
            "price": "43000",
        }

        snapshot = adapter.get_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BINANCE:BTCUSDT-PERP",
        )

        assert snapshot is not None
        assert snapshot.filled_quantity == Decimal("0.25")
        assert snapshot.average_price is None

    def test_fetch_order_derives_average_from_cost_when_available(self, adapter, mock_ccxt_client):
        exchange_client_order_id = to_exchange_format(
            CANONICAL_CLIENT_ORDER_ID,
            "binance",
        )
        mock_ccxt_client.fetch_order.return_value = {
            "id": "EX-123",
            "status": "open",
            "clientOrderId": exchange_client_order_id,
            "filled": "0.25",
            "average": None,
            "price": "43000",
            "cost": "10500",
        }

        snapshot = adapter.get_order_by_client_id(
            CANONICAL_CLIENT_ORDER_ID,
            "BINANCE:BTCUSDT-PERP",
        )

        assert snapshot is not None
        assert snapshot.filled_quantity == Decimal("0.25")
        assert snapshot.average_price == Decimal("42000")

    def test_fetch_order_not_found_returns_none(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib

        mock_ccxt_client.fetch_order.side_effect = ccxt_lib.OrderNotFound("not found")

        assert (
            adapter.get_order_by_client_id(
                CANONICAL_CLIENT_ORDER_ID,
                "BINANCE:BTCUSDT-PERP",
            )
            is None
        )

    def test_fetch_order_error_raises_exchange_error(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib

        mock_ccxt_client.fetch_order.side_effect = ccxt_lib.ExchangeError("fail")

        with pytest.raises(ExchangeError):
            adapter.get_order_by_client_id(
                CANONICAL_CLIENT_ORDER_ID,
                "BINANCE:BTCUSDT-PERP",
            )


class TestUserStreamListenKey:
    def test_binance_creates_user_stream_listen_key(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fapiPrivatePostListenKey.return_value = {
            "listenKey": "listen-key-1",
        }

        assert adapter.create_user_stream_listen_key() == "listen-key-1"

        mock_ccxt_client.fapiPrivatePostListenKey.assert_called_once_with()

    def test_binance_create_user_stream_requires_listen_key_in_response(
        self,
        adapter,
        mock_ccxt_client,
    ):
        mock_ccxt_client.fapiPrivatePostListenKey.return_value = {}

        with pytest.raises(ExchangeError, match="user_stream_listen_key_missing"):
            adapter.create_user_stream_listen_key()

    def test_binance_keepalive_user_stream(self, adapter, mock_ccxt_client):
        adapter.keepalive_user_stream("listen-key-1")

        mock_ccxt_client.fapiPrivatePutListenKey.assert_called_once_with(
            {"listenKey": "listen-key-1"}
        )

    def test_binance_keepalive_requires_listen_key(self, adapter):
        with pytest.raises(ExchangeError, match="requires_listen_key"):
            adapter.keepalive_user_stream("")

    def test_non_binance_user_stream_listen_key_is_unsupported(self, mock_ccxt_client):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_exchange_cls = MagicMock(return_value=mock_ccxt_client)
            mock_ccxt.bybit = mock_exchange_cls
            setattr(mock_ccxt, "bybit", mock_exchange_cls)
            adapter = CcxtExchangeAdapter(
                exchange_id="bybit",
                api_key="test-key",
                secret="test-secret",
            )
        adapter.client = mock_ccxt_client

        with pytest.raises(ExchangeUserStreamUnsupported):
            adapter.create_user_stream_listen_key()
        with pytest.raises(ExchangeUserStreamUnsupported):
            adapter.keepalive_user_stream("listen-key-1")


class TestGetBalance:
    def test_returns_decimal(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fetch_balance.return_value = {"free": {"USDT": 1234.56}}
        result = adapter.get_balance("USDT")
        assert result == Decimal("1234.56")

    def test_unknown_asset_returns_zero(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fetch_balance.return_value = {"free": {}}
        result = adapter.get_balance("ETH")
        assert result == Decimal("0")

    def test_fetch_error_raises(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.fetch_balance.side_effect = ccxt_lib.ExchangeError("fail")
        with pytest.raises(ExchangeError):
            adapter.get_balance("USDT")


class TestGetPosition:
    def test_long_position(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fetch_positions.return_value = [
            {
                "symbol": "BTC/USDT:USDT",
                "contracts": 0.5,
                "entryPrice": 65000,
                "unrealizedPnl": 100,
            }
        ]
        pos = adapter.get_position("BINANCE:BTCUSDT-PERP")
        assert pos is not None
        assert pos.side == "LONG"
        assert pos.quantity == Decimal("0.5")
        assert pos.entry_price == Decimal("65000")

    def test_short_position(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fetch_positions.return_value = [
            {
                "symbol": "BTC/USDT:USDT",
                "contracts": -0.3,
                "entryPrice": 70000,
                "unrealizedPnl": -50,
            }
        ]
        pos = adapter.get_position("BINANCE:BTCUSDT-PERP")
        assert pos is not None
        assert pos.side == "SHORT"
        assert pos.quantity == Decimal("0.3")

    def test_no_position_returns_none(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fetch_positions.return_value = [
            {"symbol": "BTC/USDT:USDT", "contracts": 0, "entryPrice": 0, "unrealizedPnl": 0}
        ]
        assert adapter.get_position("BINANCE:BTCUSDT-PERP") is None

    def test_wrong_symbol_returns_none(self, adapter, mock_ccxt_client):
        mock_ccxt_client.fetch_positions.return_value = [
            {"symbol": "ETH/USDT:USDT", "contracts": 1, "entryPrice": 3000, "unrealizedPnl": 0}
        ]
        assert adapter.get_position("BINANCE:BTCUSDT-PERP") is None

    def test_fetch_error_raises(self, adapter, mock_ccxt_client):
        import ccxt as ccxt_lib
        mock_ccxt_client.fetch_positions.side_effect = ccxt_lib.ExchangeError("fail")
        with pytest.raises(ExchangeError):
            adapter.get_position("BINANCE:BTCUSDT-PERP")

    def test_get_all_positions_returns_non_flat_exchange_positions(
        self, adapter, mock_ccxt_client
    ):
        mock_ccxt_client.fetch_positions.return_value = [
            {
                "symbol": "BTC/USDT:USDT",
                "contracts": 0.5,
                "side": "long",
                "entryPrice": 65000,
                "unrealizedPnl": 100,
            },
            {
                "symbol": "ETH/USDT:USDT",
                "contracts": 0,
                "side": "long",
                "entryPrice": 3000,
                "unrealizedPnl": 0,
            },
            {
                "symbol": "XRP/USDT:USDT",
                "contracts": 20,
                "side": "short",
                "entryPrice": 1,
                "unrealizedPnl": -2,
            },
        ]

        positions = adapter.get_all_positions()

        assert [(pos.product_id, pos.side, pos.quantity) for pos in positions] == [
            ("BINANCE:BTCUSDT-PERP", "LONG", Decimal("0.5")),
            ("BINANCE:XRPUSDT-PERP", "SHORT", Decimal("20")),
        ]


# ---------------------------------------------------------------------------
# create_adapter factory
# ---------------------------------------------------------------------------


class TestCreateAdapter:
    def test_simulated_default(self):
        a = create_adapter({})
        assert isinstance(a, SimulatedAdapter)

    def test_simulated_explicit(self):
        a = create_adapter({"mode": "simulated", "balance": 50000})
        assert isinstance(a, SimulatedAdapter)
        assert a.get_balance("USDT") == Decimal("50000")

    def test_live_creates_ccxt_adapter(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_cls = MagicMock()
            mock_cls.return_value = MagicMock()
            mock_ccxt.bybit = mock_cls
            setattr(mock_ccxt, "bybit", mock_cls)

            a = create_adapter({
                "mode": "live",
                "exchange": "bybit",
                "api_key": "k",
                "secret": "s",
                "testnet": True,
            })
            assert isinstance(a, CcxtExchangeAdapter)

    def test_live_adapter_warms_configured_instrument_specs(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_cls = MagicMock()
            client = MagicMock()
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.bybit = mock_cls
            setattr(mock_ccxt, "bybit", mock_cls)

            adapter = create_adapter({
                "mode": "live",
                "exchange": "bybit",
                "api_key": "k",
                "secret": "s",
                "instrument_product_ids": ["BYBIT:BTCUSDT-PERP"],
            })

        assert isinstance(adapter, CcxtExchangeAdapter)
        assert adapter.get_instrument_spec("BYBIT:BTCUSDT-PERP").quantity_step == Decimal("0.001")
        client.load_markets.assert_called_once()

    def test_live_adapter_initializes_account_before_warming_specs(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_ccxt.BaseError = Exception
            mock_cls = MagicMock()
            client = MagicMock()
            client.fetch_position_mode.side_effect = Exception(
                "bybit fetchPositionMode() is not supported yet"
            )
            client.fetch_leverage.return_value = {
                "longLeverage": 3,
                "shortLeverage": 3,
                "marginMode": "isolated",
            }
            client.fetch_margin_mode.return_value = {"marginMode": "isolated"}
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.bybit = mock_cls
            setattr(mock_ccxt, "bybit", mock_cls)

            adapter = create_adapter({
                "mode": "live",
                "exchange": "bybit",
                "api_key": "k",
                "secret": "s",
                "instrument_product_ids": ["BYBIT:BTCUSDT-PERP"],
                "account_initialization": {
                    "leverage": 3,
                    "margin_mode": "isolated",
                    "position_mode": "one_way",
                },
            })

        assert isinstance(adapter, CcxtExchangeAdapter)
        assert (
            client.mock_calls.index(call.load_markets())
            < client.mock_calls.index(call.set_position_mode(False, "BTC/USDT:USDT"))
        )
        client.set_position_mode.assert_called_once_with(False, "BTC/USDT:USDT")
        client.fetch_position_mode.assert_called_once_with("BTC/USDT:USDT")
        client.set_margin_mode.assert_called_once_with(
            "isolated",
            "BTC/USDT:USDT",
            {"leverage": "3"},
        )
        client.set_leverage.assert_called_once_with(3, "BTC/USDT:USDT")
        client.fetch_leverage.assert_called_once_with("BTC/USDT:USDT")
        client.fetch_margin_mode.assert_called_once_with("BTC/USDT:USDT")

    def test_bybit_account_initialization_allows_unsupported_position_mode_fetch(
        self,
    ):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_ccxt.BaseError = Exception
            mock_cls = MagicMock()
            client = MagicMock()
            client.fetch_position_mode.side_effect = Exception(
                "bybit fetchPositionMode() is not supported yet"
            )
            client.fetch_leverage.return_value = {
                "longLeverage": 3,
                "shortLeverage": 3,
                "marginMode": "cross",
            }
            client.fetch_margin_mode.return_value = {"marginMode": "cross"}
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.bybit = mock_cls
            setattr(mock_ccxt, "bybit", mock_cls)

            adapter = create_adapter({
                "mode": "live",
                "exchange": "bybit",
                "api_key": "k",
                "secret": "s",
                "instrument_product_ids": ["BYBIT:BTCUSDT-PERP"],
                "account_initialization": {
                    "leverage": 3,
                    "margin_mode": "cross",
                    "position_mode": "one_way",
                },
            })

        assert isinstance(adapter, CcxtExchangeAdapter)
        client.set_position_mode.assert_called_once_with(False, "BTC/USDT:USDT")
        client.fetch_position_mode.assert_called_once_with("BTC/USDT:USDT")
        client.set_margin_mode.assert_called_once_with(
            "cross",
            "BTC/USDT:USDT",
            {"leverage": "3"},
        )
        client.set_leverage.assert_called_once_with(3, "BTC/USDT:USDT")

    def test_bybit_account_initialization_allows_unsupported_margin_mode_verification(
        self,
    ):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_ccxt.BaseError = Exception
            mock_cls = MagicMock()
            client = MagicMock()
            client.fetch_position_mode.side_effect = Exception(
                "bybit fetchPositionMode() is not supported yet"
            )
            client.fetch_margin_mode.side_effect = Exception(
                "bybit fetchMarginMode() is not supported yet"
            )
            client.fetch_leverage.return_value = {
                "longLeverage": 3,
                "shortLeverage": 3,
                "marginMode": None,
            }
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.bybit = mock_cls
            setattr(mock_ccxt, "bybit", mock_cls)

            adapter = create_adapter({
                "mode": "live",
                "exchange": "bybit",
                "api_key": "k",
                "secret": "s",
                "instrument_product_ids": ["BYBIT:BTCUSDT-PERP"],
                "account_initialization": {
                    "leverage": 3,
                    "margin_mode": "cross",
                    "position_mode": "one_way",
                },
            })

        assert isinstance(adapter, CcxtExchangeAdapter)
        client.set_margin_mode.assert_called_once_with(
            "cross",
            "BTC/USDT:USDT",
            {"leverage": "3"},
        )
        client.fetch_margin_mode.assert_called_once_with("BTC/USDT:USDT")
        assert client.fetch_leverage.call_count == 2

    def test_live_adapter_rejects_when_position_mode_not_one_way(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_cls = MagicMock()
            client = MagicMock()
            client.fetch_position_mode.return_value = {"hedged": True}
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            with pytest.raises(ExchangeError, match="account_position_mode_not_one_way"):
                create_adapter({
                    "mode": "live",
                    "exchange": "binance",
                    "api_key": "k",
                    "secret": "s",
                    "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
                    "account_initialization": {
                        "leverage": 2,
                        "margin_mode": "cross",
                    },
                })

        client.set_margin_mode.assert_not_called()
        client.set_leverage.assert_not_called()

    def test_live_adapter_rejects_when_leverage_verification_fails(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_cls = MagicMock()
            client = MagicMock()
            client.fetch_position_mode.return_value = {"hedged": False}
            client.fetch_leverage.return_value = {
                "longLeverage": 1,
                "shortLeverage": 1,
            }
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            with pytest.raises(ExchangeError, match="account_leverage_not_configured"):
                create_adapter({
                    "mode": "live",
                    "exchange": "binance",
                    "api_key": "k",
                    "secret": "s",
                    "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
                    "account_initialization": {
                        "leverage": 2,
                    },
                })

        client.set_leverage.assert_called_once_with(2, "BTC/USDT:USDT")

    def test_account_initialization_config_requires_product_ids(self):
        with pytest.raises(ExchangeError, match="account_initialization_requires_products"):
            AccountInitializationConfig.from_config(
                {"leverage": 2},
                default_product_ids=[],
            )

    def test_account_initialization_config_rejects_hedge_mode(self):
        with pytest.raises(ExchangeError, match="unsupported_account_position_mode"):
            AccountInitializationConfig.from_config(
                {
                    "product_ids": ["BINANCE:BTCUSDT-PERP"],
                    "position_mode": "hedge",
                },
                default_product_ids=[],
            )

    def test_account_initialization_config_accepts_one_way(self):
        config = AccountInitializationConfig.from_config(
            {
                "product_ids": ["BINANCE:BTCUSDT-PERP"],
                "leverage": "5",
                "margin_mode": "cross",
                "position_mode": "one_way",
            },
            default_product_ids=[],
        )

        assert config is not None
        assert config.product_ids == ("BINANCE:BTCUSDT-PERP",)
        assert config.leverage == 5
        assert config.margin_mode == "cross"
        assert config.position_mode == AccountPositionMode.ONE_WAY

    def test_live_binance_with_ws(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector"):
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            a = create_adapter({
                "mode": "live",
                "exchange": "binance",
                "enable_ws": True,
                "api_key": "k",
                "secret": "s",
            })
            assert isinstance(a, LiveBinanceAdapter)


# ---------------------------------------------------------------------------
# LiveBinanceAdapter
# ---------------------------------------------------------------------------


class TestLiveBinanceWsInit:

    def test_ws_init_failure_falls_back_to_rest(self):
        """WS init failure should set ws_connector to None (REST fallback)."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector", side_effect=RuntimeError("ws fail")):
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            assert adapter.ws_connector is None

    def test_ws_disabled_no_connector(self):
        """When enable_ws=False, ws_connector should be None."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=False)
            assert adapter.ws_connector is None


class TestLiveBinanceWsOrderPath:

    def test_market_order_via_ws(self):
        """Market order should use WS fast path when connected."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector") as MockWS:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.load_markets.return_value = {
                "BTC/USDT:USDT": {
                    "info": {
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                },
            }
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            mock_ws_inst = MagicMock()
            mock_ws_inst.is_connected.return_value = True
            mock_ws_inst.place_order.return_value = True

            async def wait_for_ack(client_order_id):
                return MagicMock(exchange_order_id="WS-123")

            mock_ws_inst._wait_for_ack.side_effect = wait_for_ack
            MockWS.return_value = mock_ws_inst

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            order = _make_order(
                type="market",
                quantity=Decimal("0.0109"),
                client_order_id=CANONICAL_CLIENT_ORDER_ID,
            )
            result = adapter.place_order(order)

            exchange_client_order_id = to_exchange_format(
                CANONICAL_CLIENT_ORDER_ID,
                "binance",
            )
            assert result == "WS-123"
            mock_ws_inst.place_order.assert_called_once()
            assert (
                mock_ws_inst.place_order.call_args.kwargs["client_order_id"]
                == exchange_client_order_id
            )
            assert mock_ws_inst.place_order.call_args.kwargs["quantity"] == "0.010"
            mock_ws_inst._wait_for_ack.assert_called_once_with(exchange_client_order_id)

    def test_reduce_only_market_order_uses_rest_not_ws(self):
        """Reduce-only flatten orders must use the REST path that sends reduceOnly."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector") as MockWS:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.create_order.return_value = {"id": "REST-REDUCE"}
            client.load_markets.return_value = _empty_btc_market()
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            mock_ws_inst = MagicMock()
            mock_ws_inst.is_connected.return_value = True
            MockWS.return_value = mock_ws_inst

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            adapter.client = client
            order = _make_order(
                type="market",
                side="sell",
                intent_payload={"reduce_only": True, "source": "kill_switch"},
            )
            result = adapter.place_order(order)

            assert result == "REST-REDUCE"
            mock_ws_inst.place_order.assert_not_called()
            assert client.create_order.call_args.kwargs["params"]["reduceOnly"] is True

    def test_ws_ack_timeout_falls_back_to_rest(self):
        """WS ACK timeout should fall back to REST."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector") as MockWS:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.create_order.return_value = {"id": "REST-ACK-TIMEOUT"}
            client.load_markets.return_value = _empty_btc_market()
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            mock_ws_inst = MagicMock()
            mock_ws_inst.is_connected.return_value = True
            mock_ws_inst.place_order.return_value = True
            mock_ws_inst._wait_for_ack.side_effect = TimeoutError("ack timeout")
            MockWS.return_value = mock_ws_inst

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            adapter.client = client
            order = _make_order(type="market", client_order_id=CANONICAL_CLIENT_ORDER_ID)
            result = adapter.place_order(order)

            assert result == "REST-ACK-TIMEOUT"

    def test_ws_failure_falls_back_to_rest(self):
        """WS order failure should fall back to REST."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector") as MockWS:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.create_order.return_value = {"id": "REST-123"}
            client.load_markets.return_value = _empty_btc_market()
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            mock_ws_inst = MagicMock()
            mock_ws_inst.is_connected.return_value = True
            mock_ws_inst.place_order.side_effect = RuntimeError("ws order fail")
            MockWS.return_value = mock_ws_inst

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            adapter.client = client  # ensure REST uses our mock
            order = _make_order(type="market")
            result = adapter.place_order(order)

            assert result == "REST-123"

    def test_limit_order_uses_rest(self):
        """Limit orders should always use REST path (not WS)."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector") as MockWS:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.create_order.return_value = {"id": "REST-456"}
            client.load_markets.return_value = _empty_btc_market()
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            mock_ws_inst = MagicMock()
            mock_ws_inst.is_connected.return_value = True
            MockWS.return_value = mock_ws_inst

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            adapter.client = client
            order = _make_order(type="limit", price=Decimal("50000"))
            result = adapter.place_order(order)

            assert result == "REST-456"
            mock_ws_inst.place_order.assert_not_called()

    def test_ws_disconnected_uses_rest(self):
        """When WS is not connected, should fall back to REST."""
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector") as MockWS:
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
            client.create_order.return_value = {"id": "REST-789"}
            client.load_markets.return_value = _empty_btc_market()
            mock_cls.return_value = client
            mock_ccxt.binance = mock_cls
            setattr(mock_ccxt, "binance", mock_cls)

            mock_ws_inst = MagicMock()
            mock_ws_inst.is_connected.return_value = False
            MockWS.return_value = mock_ws_inst

            adapter = LiveBinanceAdapter(api_key="k", secret="s", enable_ws=True)
            adapter.client = client
            order = _make_order(type="market")
            result = adapter.place_order(order)

            assert result == "REST-789"
