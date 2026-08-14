"""Universal CCXT exchange adapter.

Implements IExchangeAdapter for any exchange supported by CCXT.
Replaces the old ExchangeAdapter (exchange_adapter.py) which did NOT
implement IExchangeAdapter and only wrapped create_order.
"""

import logging
import os
from decimal import Decimal
from typing import Callable, Literal, Optional, Protocol, cast

import ccxt

from src.core.adapters.ccxt_account_initialization import (
    AccountInitializationConfig,
    AccountPositionMode as AccountPositionMode,
    initialize_ccxt_account,
)
from src.core.interfaces.exchange import (
    ExchangeOrderSnapshot,
    ExchangeError,
    ExchangeUserStreamUnsupported,
    IExchangeAdapter,
    InsufficientFundsError,
    NetworkError,
)
from src.core.client_order_id import parse_client_order_id
from src.core.models import Position, PositionSide
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


class _ExactCreateOrder(Protocol):
    """Runtime CCXT boundary omitted by its narrower generated annotation."""

    def __call__(
        self,
        *,
        symbol: str,
        type: str,
        side: Literal["buy", "sell"],
        amount: str,
        price: str | None,
        params: dict[str, object],
    ) -> dict[str, object]: ...


class CcxtExchangeAdapter(IExchangeAdapter):
    """Universal exchange adapter via CCXT.

    Supports provider-neutral CCXT transport and account operations. Concrete
    venue policy belongs to venue-owned subclasses or composition owners.
    """

    def supports_runtime_reconciliation(self) -> bool:
        return True

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

    def _exchange_client_order_id(self, client_order_id: str) -> str:
        parse_client_order_id(client_order_id)
        return client_order_id

    def place_order(self, order: Order) -> str:
        ccxt_symbol = to_ccxt_symbol(order.product_id)
        side = self._ccxt_order_side(order.side)
        self._quantize_order(order)
        order_type, params = self._ccxt_order_type_and_params(order)
        if order_type == "limit":
            params["timeInForce"] = "GTC"
        intent_payload = getattr(order, "intent_payload", None)
        if (
            isinstance(intent_payload, dict)
            and intent_payload.get("reduce_only") is True
        ):
            params["reduceOnly"] = True
        client_order_id = getattr(order, "client_order_id", None)
        if client_order_id:
            exchange_client_order_id = self._exchange_client_order_id(client_order_id)
            params.update(
                self._submission_client_order_id_params(
                    exchange_client_order_id,
                    getattr(order, "type", None),
                )
            )

        try:
            self.logger.info(
                "Placing %s %s %s %s @ %s",
                order.type,
                order.side,
                order.quantity,
                ccxt_symbol,
                order.price or "market",
            )
            create_order = cast(_ExactCreateOrder, self.client.create_order)
            response = create_order(
                symbol=ccxt_symbol,
                type=order_type,
                side=side,
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

    _SUPPORTED_PLAIN_ORDER_TYPES = frozenset({"market", "limit"})

    @staticmethod
    def _ccxt_order_side(side: str) -> Literal["buy", "sell"]:
        if side == "buy":
            return "buy"
        if side == "sell":
            return "sell"
        raise ExchangeError(f"order_side_mapping_unsupported: side={side}")

    def _submission_client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: Optional[str],
    ) -> dict:
        return {"clientOrderId": exchange_client_order_id}

    def _ccxt_order_type_and_params(self, order: Order) -> tuple[str, dict]:
        order_type = (getattr(order, "type", None) or "").lower()
        if order_type in {"stop_loss", "take_profit"}:
            raise ExchangeError(
                f"conditional_order_mapping_unsupported: exchange={self.exchange_id}"
            )
        if order_type == "trailing_stop":
            raise ExchangeError(
                f"trailing_stop_mapping_unsupported: exchange={self.exchange_id}"
            )
        if order_type in self._SUPPORTED_PLAIN_ORDER_TYPES:
            return order_type, {}
        raise ExchangeError(f"order_type_mapping_unsupported: order_type={order_type}")

    def validate_order(self, order: Order) -> None:
        """Validate and quantize an outbound order without placing it."""
        self._quantize_order(order)
        self._ccxt_order_type_and_params(order)

    def get_instrument_spec(self, product_id: str) -> InstrumentSpec:
        spec = self._instrument_specs.get(product_id)
        if spec is not None:
            return spec

        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            markets = self.client.load_markets()
        except ccxt.BaseError as e:
            raise ExchangeError(
                f"Failed to load market rules for {product_id}: {e}"
            ) from e
        market = markets.get(ccxt_symbol) if isinstance(markets, dict) else None
        if market is None:
            raise ExchangeError(
                f"market_not_found: {ccxt_symbol} for {product_id}; "
                "refusing to build InstrumentSpec"
            )
        try:
            spec = instrument_spec_from_ccxt_market(
                product_id,
                market,
                precision_mode=self._precision_mode(),
            )
        except ValueError as e:
            raise ExchangeError(f"invalid_contract_metadata: {product_id}: {e}") from e
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

    def warm_instrument_specs(
        self,
        product_ids: list[str],
        *,
        operation_guard: Callable[[], None] | None = None,
    ) -> None:
        """Fetch and cache instrument specs for known live products."""
        guard = operation_guard or (lambda: None)
        for product_id in product_ids:
            guard()
            self.get_instrument_spec(product_id)
            guard()

    def initialize_account(
        self,
        config: AccountInitializationConfig,
        *,
        operation_guard: Callable[[], None] | None = None,
    ) -> None:
        initialize_ccxt_account(
            exchange_id=self.exchange_id,
            client=self.client,
            logger=self.logger,
            config=config,
            operation_guard=operation_guard,
        )

    def _quantize_order(self, order: Order) -> None:
        spec = self.get_instrument_spec(order.product_id)
        intent_payload = getattr(order, "intent_payload", None)
        try:
            quantized = quantize_order_values(
                quantity=order.quantity,
                price=order.price,
                side=order.side,
                order_type=order.type,
                trigger_price=order.trigger_price,
                trailing_distance=getattr(order, "_trailing_distance", None),
                spec=spec,
            )
            notional_price = quantized.price or quantized.trigger_price
            reference_price = getattr(order, "min_notional_reference_price", None)
            if (
                spec.min_notional is not None
                and notional_price is None
                and reference_price is None
                and order.type.lower() == "market"
                and isinstance(intent_payload, dict)
                and intent_payload.get("reduce_only") is True
            ):
                reference_price = self._market_order_reference_price(order)
                order.min_notional_reference_price = reference_price
            validate_min_notional(
                quantity=quantized.quantity,
                price=notional_price,
                reference_price=reference_price,
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

    def _market_order_reference_price(self, order: Order) -> Decimal:
        ccxt_symbol = to_ccxt_symbol(order.product_id)
        try:
            ticker = self.client.fetch_ticker(ccxt_symbol)
        except ccxt.BaseError as exc:
            raise ExchangeError(f"market_reference_price_unavailable: {exc}") from exc

        side = order.side.lower()
        if side not in {"buy", "sell"}:
            raise ExchangeError(f"market_reference_side_unsupported: side={order.side}")
        value = ticker.get("ask" if side == "buy" else "bid") or ticker.get("last")
        if value is None or Decimal(str(value)) <= 0:
            raise ExchangeError(
                f"market_reference_price_unavailable: symbol={ccxt_symbol} side={side}"
            )
        return Decimal(str(value))

    def cancel_order(
        self,
        order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> bool:
        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            params = self._cancel_order_params(order_type)
            if params is not None:
                self.client.cancel_order(order_id, ccxt_symbol, params=params)
            else:
                self.client.cancel_order(order_id, ccxt_symbol)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning("Order %s not found on exchange", order_id)
            return False
        except ccxt.BaseError as e:
            self.logger.error("Failed to cancel order %s: %s", order_id, e)
            return False

    def _cancel_order_params(self, order_type: Optional[str]) -> dict | None:
        return None

    def cancel_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> bool:
        ccxt_symbol = to_ccxt_symbol(product_id)
        exchange_client_order_id = self._exchange_client_order_id(client_order_id)
        params = self._client_order_id_params(exchange_client_order_id, order_type)
        try:
            self.client.cancel_order(
                exchange_client_order_id, ccxt_symbol, params=params
            )
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

    def _client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: Optional[str],
    ) -> dict:
        return {"clientOrderId": exchange_client_order_id}

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> Optional[ExchangeOrderSnapshot]:
        ccxt_symbol = to_ccxt_symbol(product_id)
        exchange_client_order_id = self._exchange_client_order_id(client_order_id)
        params = self._client_order_id_params(exchange_client_order_id, order_type)
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

        return self._order_snapshot_from_response(client_order_id, response)

    def _order_snapshot_from_response(
        self,
        client_order_id: str,
        response: dict,
    ) -> ExchangeOrderSnapshot:
        """Project one locked-client order response without venue transport policy."""

        exchange_order_id = response.get("id")
        status = response.get("status") or "unknown"
        fee = response.get("fee") or {}
        fee_cost = fee.get("cost") if isinstance(fee, dict) else None
        fee_currency = fee.get("currency") if isinstance(fee, dict) else None
        filled_quantity = response.get("filled")
        average_price = response.get("average")
        cost = response.get("cost")
        if average_price is None and cost is not None and filled_quantity is not None:
            filled_decimal = Decimal(str(filled_quantity))
            if filled_decimal > 0:
                average_price = Decimal(str(cost)) / filled_decimal
        return ExchangeOrderSnapshot(
            client_order_id=client_order_id,
            exchange_order_id=str(exchange_order_id)
            if exchange_order_id is not None
            else None,
            status=str(status),
            filled_quantity=(
                Decimal(str(filled_quantity)) if filled_quantity is not None else None
            ),
            average_price=(
                average_price
                if isinstance(average_price, Decimal)
                else Decimal(str(average_price))
                if average_price is not None
                else None
            ),
            fee=Decimal(str(fee_cost)) if fee_cost is not None else None,
            fee_asset=(
                fee_currency
                if type(fee_currency) is str and fee_currency != ""
                else None
            ),
            raw=response,
        )

    def create_user_stream_listen_key(self) -> str:
        raise ExchangeUserStreamUnsupported(
            f"user_stream_listen_key_unsupported: exchange={self.exchange_id}"
        )

    def keepalive_user_stream(self, listen_key: str) -> None:
        raise ExchangeUserStreamUnsupported(
            f"user_stream_keepalive_unsupported: exchange={self.exchange_id}"
        )

    def get_balance(self, asset: str) -> Decimal:
        try:
            balance = self.client.fetch_balance()
            free = balance.get("free", {})
            return Decimal(str(free.get(asset, 0)))
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to fetch balance: {e}") from e

    def get_position(
        self,
        product_id: str,
        strategy_id: str | None = None,
    ) -> Optional[Position]:
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

            side_value = str(pos.get("side") or "").lower()
            side = (
                PositionSide.SHORT
                if side_value == "short" or contracts < 0
                else PositionSide.LONG
            )
            return Position(
                strategy_id="LIVE",
                product_id=product_id,
                side=side,
                quantity=Decimal(str(abs(contracts))),
                entry_price=Decimal(str(pos.get("entryPrice", 0))),
                unrealized_pnl=Decimal(str(pos.get("unrealizedPnl", 0))),
            )

        return None

    def get_all_positions(self) -> list[Position]:
        try:
            positions = self.client.fetch_positions()
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to fetch positions: {e}") from e

        result: list[Position] = []
        for raw_position in positions:
            position = self._position_from_ccxt(raw_position)
            if position is not None:
                result.append(position)
        return result

    def _position_from_ccxt(self, raw_position: dict) -> Optional[Position]:
        product_id = self._product_id_from_ccxt_symbol(raw_position.get("symbol"))
        if product_id is None:
            return None

        contracts = Decimal(str(raw_position.get("contracts") or 0))
        if contracts == 0:
            return None

        side_value = str(raw_position.get("side") or "").lower()
        side = (
            PositionSide.SHORT
            if side_value == "short" or contracts < 0
            else PositionSide.LONG
        )
        return Position(
            strategy_id="LIVE",
            product_id=product_id,
            side=side,
            quantity=abs(contracts),
            entry_price=Decimal(str(raw_position.get("entryPrice") or 0)),
            unrealized_pnl=Decimal(str(raw_position.get("unrealizedPnl") or 0)),
        )

    def _product_id_from_ccxt_symbol(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol or "/" not in symbol:
            return None
        pair = symbol.split(":", 1)[0]
        try:
            base, quote = pair.split("/", 1)
        except ValueError:
            return None
        return f"{self.exchange_id.upper()}:{base}{quote}-PERP"
