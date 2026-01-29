"""Tests for CcxtExchangeAdapter and adapter factory."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.core.adapters import create_adapter
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.live_binance import LiveBinanceAdapter
from src.core.adapters.simulated import SimulatedAdapter
from src.core.interfaces.exchange import ExchangeError, InsufficientFundsError, NetworkError
from src.core.orm_models import Order


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
        "status": "OPEN",
        "exchange_order_id": None,
    }
    defaults.update(overrides)
    order = MagicMock(spec=Order)
    for k, v in defaults.items():
        setattr(order, k, v)
    return order


@pytest.fixture
def mock_ccxt_client():
    """A mock CCXT exchange client."""
    client = MagicMock()
    client.apiKey = "test-key"
    client.secret = "test-secret"
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

    def test_limit_order_includes_gtc(self, adapter, mock_ccxt_client):
        mock_ccxt_client.create_order.return_value = {"id": "EX-456"}
        order = _make_order(type="limit", price=Decimal("50000"))

        adapter.place_order(order)

        call_kwargs = mock_ccxt_client.create_order.call_args
        assert call_kwargs.kwargs["params"]["timeInForce"] == "GTC"
        assert call_kwargs.kwargs["price"] == 50000.0

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

    def test_live_binance_with_ws(self):
        with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt, \
             patch("src.core.adapters.live_binance.WebSocketOrderConnector"):
            mock_cls = MagicMock()
            client = MagicMock()
            client.apiKey = "k"
            client.secret = "s"
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
