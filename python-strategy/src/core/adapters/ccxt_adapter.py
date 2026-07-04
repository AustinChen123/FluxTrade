"""Universal CCXT exchange adapter.

Implements IExchangeAdapter for any exchange supported by CCXT.
Replaces the old ExchangeAdapter (exchange_adapter.py) which did NOT
implement IExchangeAdapter and only wrapped create_order.
"""

import logging
import os
from decimal import Decimal
from typing import Optional

import ccxt

from src.core.interfaces.exchange import (
    ExchangeOrderSnapshot,
    ExchangeError,
    IExchangeAdapter,
    InsufficientFundsError,
    NetworkError,
)
from src.core.client_order_id import to_exchange_format
from src.core.models import Position
from src.core.orm_models import Order
from src.core.product_registry import (
    InstrumentSpec,
    PrecisionMode,
    instrument_spec_from_ccxt_market,
    quantize_order_values,
    to_ccxt_symbol,
    validate_min_notional,
)

logger = logging.getLogger(__name__)


class CcxtExchangeAdapter(IExchangeAdapter):
    """Universal exchange adapter via CCXT.

    Supports any CCXT-compatible exchange (Binance, Bybit, Backpack, etc.)
    through a single implementation.
    """

    def __init__(
        self,
        exchange_id: str,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = False,
        extra_config: dict | None = None,
    ):
        self.exchange_id = exchange_id.lower()
        self.logger = logging.getLogger(f"CcxtAdapter.{self.exchange_id}")

        api_key = api_key or os.getenv("EXCHANGE_API_KEY")
        secret = secret or os.getenv("EXCHANGE_SECRET")

        if not hasattr(ccxt, self.exchange_id):
            raise ValueError(f"Exchange '{exchange_id}' not supported by CCXT")

        exchange_cls = getattr(ccxt, self.exchange_id)
        config = {
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if extra_config:
            config.update(extra_config)

        self.client: ccxt.Exchange = exchange_cls(config)

        if testnet:
            self.client.set_sandbox_mode(True)

        self._instrument_specs: dict[str, InstrumentSpec] = {}

        self.logger.info(
            "Connected to %s (%s)", self.exchange_id, "testnet" if testnet else "live"
        )

    # -- IExchangeAdapter ------------------------------------------------

    def place_order(self, order: Order) -> str:
        ccxt_symbol = to_ccxt_symbol(order.product_id)
        self._quantize_order(order)
        params: dict = {}
        if order.type and order.type.lower() == "limit":
            params["timeInForce"] = "GTC"
        client_order_id = getattr(order, "client_order_id", None)
        if client_order_id:
            exchange_client_order_id = to_exchange_format(client_order_id, self.exchange_id)
            if self.exchange_id == "binance":
                params["newClientOrderId"] = exchange_client_order_id
            else:
                params["clientOrderId"] = exchange_client_order_id

        try:
            self.logger.info(
                "Placing %s %s %s %s @ %s",
                order.type,
                order.side,
                order.quantity,
                ccxt_symbol,
                order.price or "market",
            )
            response = self.client.create_order(
                symbol=ccxt_symbol,
                type=order.type,
                side=order.side,
                amount=str(order.quantity),
                price=str(order.price) if order.price else None,
                params=params,
            )
            return str(response["id"])

        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(f"Insufficient funds: {e}") from e
        except ccxt.NetworkError as e:
            raise NetworkError(f"Network error: {e}") from e
        except ccxt.BaseError as e:
            raise ExchangeError(f"Order placement failed: {e}") from e

    def get_instrument_spec(self, product_id: str) -> InstrumentSpec:
        spec = self._instrument_specs.get(product_id)
        if spec is not None:
            return spec

        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            markets = self.client.load_markets()
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to load market rules for {product_id}: {e}") from e
        market = markets.get(ccxt_symbol) if isinstance(markets, dict) else None
        if market is None:
            raise ExchangeError(
                f"market_not_found: {ccxt_symbol} for {product_id}; "
                "refusing to build InstrumentSpec"
            )
        spec = instrument_spec_from_ccxt_market(
            product_id,
            market,
            precision_mode=self._precision_mode(),
        )
        missing_rules = []
        if spec.quantity_step is None:
            missing_rules.append("quantity_step")
        if spec.price_tick is None:
            missing_rules.append("price_tick")
        if missing_rules:
            self.logger.warning(
                "Instrument spec for %s is missing %s; order quantization may be incomplete",
                product_id,
                ", ".join(missing_rules),
            )
        self._instrument_specs[product_id] = spec
        return spec

    def _precision_mode(self) -> PrecisionMode | None:
        raw_mode = getattr(self.client, "precisionMode", None)
        constants = {
            getattr(ccxt, "DECIMAL_PLACES", 2): PrecisionMode.DECIMAL_PLACES,
            getattr(ccxt, "SIGNIFICANT_DIGITS", 3): PrecisionMode.SIGNIFICANT_DIGITS,
            getattr(ccxt, "TICK_SIZE", 4): PrecisionMode.TICK_SIZE,
            2: PrecisionMode.DECIMAL_PLACES,
            3: PrecisionMode.SIGNIFICANT_DIGITS,
            4: PrecisionMode.TICK_SIZE,
        }
        mode = constants.get(raw_mode)
        if mode is not None:
            return mode

        if isinstance(raw_mode, str):
            normalized = raw_mode.lower()
            return {
                "decimal_places": PrecisionMode.DECIMAL_PLACES,
                "significant_digits": PrecisionMode.SIGNIFICANT_DIGITS,
                "tick_size": PrecisionMode.TICK_SIZE,
            }.get(normalized)

        return None

    def warm_instrument_specs(self, product_ids: list[str]) -> None:
        """Fetch and cache instrument specs for known live products."""
        for product_id in product_ids:
            self.get_instrument_spec(product_id)

    def _quantize_order(self, order: Order) -> None:
        spec = self.get_instrument_spec(order.product_id)
        try:
            quantized = quantize_order_values(
                quantity=order.quantity,
                price=order.price,
                side=order.side,
                trigger_price=order.trigger_price,
                spec=spec,
            )
            notional_price = quantized.price or quantized.trigger_price
            validate_min_notional(
                quantity=quantized.quantity,
                price=notional_price,
                spec=spec,
            )
        except ValueError as e:
            raise ExchangeError(str(e)) from e
        if quantized.changed:
            self.logger.info(
                "Quantized order %s for %s: quantity %s -> %s, price %s -> %s",
                order.id,
                order.product_id,
                order.quantity,
                quantized.quantity,
                order.price,
                quantized.price,
            )
            order.quantity = quantized.quantity
            order.price = quantized.price
            order.trigger_price = quantized.trigger_price

    def cancel_order(self, order_id: str, product_id: str) -> bool:
        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            self.client.cancel_order(order_id, ccxt_symbol)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning("Order %s not found on exchange", order_id)
            return False
        except ccxt.BaseError as e:
            self.logger.error("Failed to cancel order %s: %s", order_id, e)
            return False

    def cancel_order_by_client_id(self, client_order_id: str, product_id: str) -> bool:
        ccxt_symbol = to_ccxt_symbol(product_id)
        exchange_client_order_id = to_exchange_format(client_order_id, self.exchange_id)
        params = (
            {"origClientOrderId": exchange_client_order_id}
            if self.exchange_id == "binance"
            else {"clientOrderId": exchange_client_order_id}
        )
        try:
            self.client.cancel_order(exchange_client_order_id, ccxt_symbol, params=params)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning(
                "Order with client_order_id %s not found on exchange",
                client_order_id,
            )
            return False
        except ccxt.BaseError as e:
            self.logger.error(
                "Failed to cancel order with client_order_id %s: %s",
                client_order_id,
                e,
            )
            return False

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
    ) -> Optional[ExchangeOrderSnapshot]:
        ccxt_symbol = to_ccxt_symbol(product_id)
        exchange_client_order_id = to_exchange_format(client_order_id, self.exchange_id)
        params = (
            {"origClientOrderId": exchange_client_order_id}
            if self.exchange_id == "binance"
            else {"clientOrderId": exchange_client_order_id}
        )
        try:
            response = self.client.fetch_order(
                exchange_client_order_id,
                ccxt_symbol,
                params=params,
            )
        except ccxt.OrderNotFound:
            self.logger.warning(
                "Order with client_order_id %s not found on exchange",
                client_order_id,
            )
            return None
        except ccxt.BaseError as e:
            raise ExchangeError(
                f"Failed to fetch order with client_order_id {client_order_id}: {e}"
            ) from e

        exchange_order_id = response.get("id")
        status = response.get("status") or "unknown"
        fee = response.get("fee") or {}
        fee_cost = fee.get("cost") if isinstance(fee, dict) else None
        filled_quantity = response.get("filled")
        average_price = response.get("average")
        cost = response.get("cost")
        if average_price is None and cost is not None and filled_quantity is not None:
            filled_decimal = Decimal(str(filled_quantity))
            if filled_decimal > 0:
                average_price = Decimal(str(cost)) / filled_decimal
        return ExchangeOrderSnapshot(
            client_order_id=client_order_id,
            exchange_order_id=str(exchange_order_id) if exchange_order_id is not None else None,
            status=str(status),
            filled_quantity=(
                Decimal(str(filled_quantity)) if filled_quantity is not None else None
            ),
            average_price=(
                average_price
                if isinstance(average_price, Decimal)
                else Decimal(str(average_price)) if average_price is not None else None
            ),
            fee=Decimal(str(fee_cost)) if fee_cost is not None else None,
            raw=response,
        )

    def get_balance(self, asset: str) -> Decimal:
        try:
            balance = self.client.fetch_balance()
            free = balance.get("free", {})
            return Decimal(str(free.get(asset, 0)))
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to fetch balance: {e}") from e

    def get_position(self, product_id: str) -> Optional[Position]:
        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            positions = self.client.fetch_positions([ccxt_symbol])
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to fetch position: {e}") from e

        for pos in positions:
            if pos.get("symbol") != ccxt_symbol:
                continue

            contracts = Decimal(str(pos.get("contracts", 0)))
            if contracts == 0:
                return None

            side = "LONG" if contracts > 0 else "SHORT"
            return Position(
                strategy_id="LIVE",
                product_id=product_id,
                side=side,
                quantity=Decimal(str(abs(contracts))),
                entry_price=Decimal(str(pos.get("entryPrice", 0))),
                unrealized_pnl=Decimal(str(pos.get("unrealizedPnl", 0))),
            )

        return None
