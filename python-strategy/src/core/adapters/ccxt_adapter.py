"""Universal CCXT exchange adapter.

Implements IExchangeAdapter for any exchange supported by CCXT.
Replaces the old ExchangeAdapter (exchange_adapter.py) which did NOT
implement IExchangeAdapter and only wrapped create_order.
"""

import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional

import ccxt

from src.core.interfaces.exchange import (
    ExchangeOrderSnapshot,
    ExchangeError,
    ExchangeUserStreamUnsupported,
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


class AccountPositionMode(str, Enum):
    ONE_WAY = "one_way"


@dataclass(frozen=True)
class AccountInitializationConfig:
    """Live account settings that must be applied before trading starts."""

    product_ids: tuple[str, ...]
    leverage: int | None = None
    margin_mode: str | None = None
    position_mode: AccountPositionMode = AccountPositionMode.ONE_WAY

    @classmethod
    def from_config(
        cls,
        raw_config: dict | None,
        *,
        default_product_ids: list[str],
    ) -> "AccountInitializationConfig | None":
        if not raw_config:
            return None

        product_ids = tuple(
            raw_config.get("product_ids")
            or raw_config.get("instrument_product_ids")
            or default_product_ids
        )
        if not product_ids:
            raise ExchangeError(
                "account_initialization_requires_products: "
                "configure account_initialization.product_ids or instrument_product_ids"
            )

        position_mode = raw_config.get("position_mode", AccountPositionMode.ONE_WAY.value)
        if position_mode != AccountPositionMode.ONE_WAY.value:
            raise ExchangeError(
                "unsupported_account_position_mode: "
                f"position_mode={position_mode}"
            )

        leverage = raw_config.get("leverage")
        if leverage is not None:
            try:
                leverage = int(leverage)
            except (TypeError, ValueError) as e:
                raise ExchangeError(
                    f"invalid_account_leverage: leverage={leverage}"
                ) from e
            if leverage < 1:
                raise ExchangeError(
                    f"invalid_account_leverage: leverage={leverage}"
                )

        margin_mode = raw_config.get("margin_mode")
        if margin_mode is not None:
            margin_mode = str(margin_mode).lower()
            if margin_mode not in {"cross", "isolated"}:
                raise ExchangeError(
                    f"invalid_account_margin_mode: margin_mode={margin_mode}"
                )

        return cls(
            product_ids=product_ids,
            leverage=leverage,
            margin_mode=margin_mode,
            position_mode=AccountPositionMode.ONE_WAY,
        )


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
        order_type, params = self._ccxt_order_type_and_params(order)
        if order_type == "limit":
            params["timeInForce"] = "GTC"
        intent_payload = getattr(order, "intent_payload", None)
        if isinstance(intent_payload, dict) and intent_payload.get("reduce_only") is True:
            params["reduceOnly"] = True
        client_order_id = getattr(order, "client_order_id", None)
        if client_order_id:
            exchange_client_order_id = to_exchange_format(client_order_id, self.exchange_id)
            if self._uses_algo_order_endpoints(order.type):
                params["clientAlgoId"] = exchange_client_order_id
            elif self.exchange_id == "binance":
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
                type=order_type,
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

    _BINANCE_ALGO_ORDER_TYPES = frozenset({"stop_loss", "take_profit"})
    _SUPPORTED_PLAIN_ORDER_TYPES = frozenset({"market", "limit"})

    def _uses_algo_order_endpoints(self, order_type: Optional[str]) -> bool:
        """Binance USDT-M conditional orders live in the algo order id namespace.

        ccxt only routes create/fetch/cancel to the futures algo endpoints when
        the conditional flag is present (ccxt 4.5 binance.py cancel_order /
        fetch_order: ``isConditional = safe_bool_n(['stop','trigger','conditional'])``).
        Without it the algoId/clientAlgoId is sent to the regular order endpoint
        and the order is reported as not found.
        """
        return (
            self.exchange_id == "binance"
            and (order_type or "").lower() in self._BINANCE_ALGO_ORDER_TYPES
        )

    def _ccxt_order_type_and_params(self, order: Order) -> tuple[str, dict]:
        order_type = (order.type or "").lower()
        if order_type in {"stop_loss", "take_profit"} and self.exchange_id != "binance":
            raise ExchangeError(
                f"conditional_order_mapping_unsupported: exchange={self.exchange_id}"
            )
        if order_type == "trailing_stop":
            raise ExchangeError(
                f"trailing_stop_mapping_unsupported: exchange={self.exchange_id}"
            )
        if order_type == "stop_loss":
            if order.trigger_price is None:
                raise ExchangeError("stop_loss_requires_trigger_price")
            return "STOP_MARKET", {
                "stopLossPrice": str(order.trigger_price),
                "reduceOnly": True,
            }
        if order_type == "take_profit":
            if order.trigger_price is None:
                raise ExchangeError("take_profit_requires_trigger_price")
            return "TAKE_PROFIT_MARKET", {
                "takeProfitPrice": str(order.trigger_price),
                "reduceOnly": True,
            }
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
            raise ExchangeError(f"Failed to load market rules for {product_id}: {e}") from e
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
        """Apply fail-safe account settings before live trading starts.

        CCXT source defines set_position_mode(hedged=False) as one-way mode for
        Binance and Bybit; fetch_position_mode() returns ``hedged`` for
        verification on those venues.
        """
        guard = operation_guard or (lambda: None)
        guard()
        try:
            self.client.load_markets()
        except ccxt.BaseError as e:
            raise ExchangeError(
                f"account_initialization_load_markets_failed: {e}"
            ) from e
        guard()

        for product_id in config.product_ids:
            symbol = to_ccxt_symbol(product_id)
            self._ensure_one_way_position_mode(symbol, guard)
            margin_mode_accepted = False
            if config.margin_mode is not None:
                margin_mode_accepted = self._set_margin_mode(
                    config.margin_mode,
                    symbol,
                    config,
                    guard,
                )
            if config.leverage is not None:
                self._set_leverage(config.leverage, symbol, guard)
                self._verify_leverage(config.leverage, symbol, guard)
            if config.margin_mode is not None:
                self._verify_margin_mode(
                    config.margin_mode,
                    symbol,
                    allow_unsupported=margin_mode_accepted,
                    operation_guard=guard,
                )

    def _ensure_one_way_position_mode(
        self,
        symbol: str,
        operation_guard: Callable[[], None],
    ) -> None:
        set_position_mode = getattr(self.client, "set_position_mode", None)
        if not callable(set_position_mode):
            raise ExchangeError(
                "account_position_mode_unsupported: "
                f"exchange={self.exchange_id}"
            )
        set_accepted = False
        operation_guard()
        try:
            set_position_mode(False, symbol)
            set_accepted = True
        except ccxt.BaseError as e:
            if not self._is_account_setting_no_change_error(e):
                raise ExchangeError(
                    f"account_position_mode_set_failed: symbol={symbol} error={e}"
                ) from e
            set_accepted = True
        operation_guard()

        fetch_position_mode = getattr(self.client, "fetch_position_mode", None)
        if not callable(fetch_position_mode):
            if set_accepted:
                return
            raise ExchangeError(
                "account_position_mode_verification_unsupported: "
                f"exchange={self.exchange_id}"
            )
        operation_guard()
        try:
            result = fetch_position_mode(symbol)
        except ccxt.BaseError as e:
            if set_accepted and self._is_position_mode_verification_unsupported(e):
                operation_guard()
                return
            raise ExchangeError(
                f"account_position_mode_verify_failed: symbol={symbol} error={e}"
            ) from e
        operation_guard()

        hedged = result.get("hedged") if isinstance(result, dict) else None
        if hedged is not False:
            raise ExchangeError(
                "account_position_mode_not_one_way: "
                f"symbol={symbol} hedged={hedged}"
            )

    def _set_margin_mode(
        self,
        margin_mode: str,
        symbol: str,
        config: AccountInitializationConfig,
        operation_guard: Callable[[], None],
    ) -> bool:
        set_margin_mode = getattr(self.client, "set_margin_mode", None)
        if not callable(set_margin_mode):
            raise ExchangeError(
                f"account_margin_mode_unsupported: exchange={self.exchange_id}"
            )
        params = {}
        if config.leverage is not None:
            params["leverage"] = str(config.leverage)
        operation_guard()
        try:
            set_margin_mode(margin_mode, symbol, params)
        except ccxt.BaseError as e:
            if not self._is_account_setting_no_change_error(e):
                raise ExchangeError(
                    f"account_margin_mode_set_failed: symbol={symbol} error={e}"
                ) from e
        operation_guard()
        return True

    def _set_leverage(
        self,
        leverage: int,
        symbol: str,
        operation_guard: Callable[[], None],
    ) -> None:
        set_leverage = getattr(self.client, "set_leverage", None)
        if not callable(set_leverage):
            raise ExchangeError(
                f"account_leverage_unsupported: exchange={self.exchange_id}"
            )
        operation_guard()
        try:
            set_leverage(leverage, symbol)
        except ccxt.BaseError as e:
            if not self._is_account_setting_no_change_error(e):
                raise ExchangeError(
                    f"account_leverage_set_failed: symbol={symbol} error={e}"
                ) from e
        operation_guard()

    def _verify_leverage(
        self,
        expected_leverage: int,
        symbol: str,
        operation_guard: Callable[[], None],
    ) -> None:
        leverage = self._fetch_leverage_value(symbol, operation_guard)
        if leverage != expected_leverage:
            raise ExchangeError(
                "account_leverage_not_configured: "
                f"symbol={symbol} expected={expected_leverage} actual={leverage}"
            )

    def _verify_margin_mode(
        self,
        expected_margin_mode: str,
        symbol: str,
        *,
        allow_unsupported: bool,
        operation_guard: Callable[[], None],
    ) -> None:
        try:
            margin_mode = self._fetch_margin_mode_value(
                symbol,
                operation_guard,
            )
        except ExchangeError as e:
            if allow_unsupported and str(e).startswith(
                "account_margin_mode_verification_unsupported"
            ):
                self.logger.warning(
                    "Margin mode verification unsupported for %s on %s after accepted set_margin_mode",
                    symbol,
                    self.exchange_id,
                )
                return
            raise
        if margin_mode != expected_margin_mode:
            raise ExchangeError(
                "account_margin_mode_not_configured: "
                f"symbol={symbol} expected={expected_margin_mode} actual={margin_mode}"
            )

    def _fetch_leverage_value(
        self,
        symbol: str,
        operation_guard: Callable[[], None],
    ) -> int | None:
        fetch_leverage = getattr(self.client, "fetch_leverage", None)
        if callable(fetch_leverage):
            operation_guard()
            try:
                leverage = self._leverage_value_from_result(fetch_leverage(symbol))
                if leverage is not None:
                    operation_guard()
                    return leverage
            except ccxt.BaseError:
                pass
            operation_guard()

        fetch_leverages = getattr(self.client, "fetch_leverages", None)
        if callable(fetch_leverages):
            operation_guard()
            try:
                leverages = fetch_leverages([symbol])
                result = leverages.get(symbol) if isinstance(leverages, dict) else None
                leverage = self._leverage_value_from_result(result)
                if leverage is not None:
                    operation_guard()
                    return leverage
            except ccxt.BaseError:
                pass
            operation_guard()

        raise ExchangeError(
            "account_leverage_verification_unsupported: "
            f"exchange={self.exchange_id}"
        )

    @staticmethod
    def _leverage_value_from_result(result) -> int | None:
        if not isinstance(result, dict):
            return None
        long_leverage = result.get("longLeverage")
        short_leverage = result.get("shortLeverage")
        if long_leverage is not None and short_leverage is not None:
            if int(long_leverage) == int(short_leverage):
                return int(long_leverage)
            return None
        leverage = result.get("leverage")
        return int(leverage) if leverage is not None else None

    def _fetch_margin_mode_value(
        self,
        symbol: str,
        operation_guard: Callable[[], None],
    ) -> str | None:
        fetch_margin_mode = getattr(self.client, "fetch_margin_mode", None)
        if callable(fetch_margin_mode):
            operation_guard()
            try:
                margin_mode = self._margin_mode_from_result(fetch_margin_mode(symbol))
                if margin_mode is not None:
                    operation_guard()
                    return margin_mode
            except ccxt.BaseError:
                pass
            operation_guard()

        fetch_leverage = getattr(self.client, "fetch_leverage", None)
        if callable(fetch_leverage):
            operation_guard()
            try:
                margin_mode = self._margin_mode_from_result(fetch_leverage(symbol))
                if margin_mode is not None:
                    operation_guard()
                    return margin_mode
            except ccxt.BaseError:
                pass
            operation_guard()

        raise ExchangeError(
            "account_margin_mode_verification_unsupported: "
            f"exchange={self.exchange_id}"
        )

    @staticmethod
    def _margin_mode_from_result(result) -> str | None:
        if not isinstance(result, dict):
            return None
        margin_mode = result.get("marginMode") or result.get("marginType")
        return str(margin_mode).lower() if margin_mode is not None else None

    @staticmethod
    def _is_account_setting_no_change_error(error: ccxt.BaseError) -> bool:
        message = str(error).lower()
        return (
            "no need to change" in message
            or "not modified" in message
            or "-4059" in message
            or "110025" in message
            or "110026" in message
            or "110043" in message
            or "140025" in message
            or "140026" in message
            or "140043" in message
            or "34036" in message
        )

    @staticmethod
    def _is_position_mode_verification_unsupported(error: ccxt.BaseError) -> bool:
        message = str(error).lower()
        return (
            "fetchpositionmode" in message
            and "not supported" in message
        )

    def _quantize_order(self, order: Order) -> None:
        spec = self.get_instrument_spec(order.product_id)
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
                and isinstance(getattr(order, "intent_payload", None), dict)
                and order.intent_payload.get("reduce_only") is True
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
            raise ExchangeError(
                f"market_reference_price_unavailable: {exc}"
            ) from exc

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
            if self._uses_algo_order_endpoints(order_type):
                self.client.cancel_order(order_id, ccxt_symbol, params={"trigger": True})
            else:
                self.client.cancel_order(order_id, ccxt_symbol)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning("Order %s not found on exchange", order_id)
            return False
        except ccxt.BaseError as e:
            self.logger.error("Failed to cancel order %s: %s", order_id, e)
            return False

    def cancel_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> bool:
        ccxt_symbol = to_ccxt_symbol(product_id)
        exchange_client_order_id = to_exchange_format(client_order_id, self.exchange_id)
        params = self._client_order_id_params(exchange_client_order_id, order_type)
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

    def _client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: Optional[str],
    ) -> dict:
        if self._uses_algo_order_endpoints(order_type):
            return {"clientAlgoId": exchange_client_order_id, "trigger": True}
        if self.exchange_id == "binance":
            return {"origClientOrderId": exchange_client_order_id}
        return {"clientOrderId": exchange_client_order_id}

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> Optional[ExchangeOrderSnapshot]:
        ccxt_symbol = to_ccxt_symbol(product_id)
        exchange_client_order_id = to_exchange_format(client_order_id, self.exchange_id)
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

    def create_user_stream_listen_key(self) -> str:
        """Create a Binance USD-M Futures user-data stream listen key.

        CCXT 4.5.34 exposes Binance USD-M Futures listen-key endpoints as
        ``fapiPrivatePostListenKey`` / ``fapiPrivatePutListenKey``.
        """
        if self.exchange_id != "binance" or not hasattr(
            self.client,
            "fapiPrivatePostListenKey",
        ):
            raise ExchangeUserStreamUnsupported(
                f"user_stream_listen_key_unsupported: exchange={self.exchange_id}"
            )
        try:
            response = self.client.fapiPrivatePostListenKey()
        except ccxt.BaseError as e:
            raise ExchangeError(f"user_stream_listen_key_create_failed: {e}") from e
        listen_key = response.get("listenKey") if isinstance(response, dict) else None
        if not listen_key:
            raise ExchangeError("user_stream_listen_key_missing")
        return str(listen_key)

    def keepalive_user_stream(self, listen_key: str) -> None:
        """Keep a Binance USD-M Futures user-data stream listen key alive."""
        if self.exchange_id != "binance" or not hasattr(
            self.client,
            "fapiPrivatePutListenKey",
        ):
            raise ExchangeUserStreamUnsupported(
                f"user_stream_keepalive_unsupported: exchange={self.exchange_id}"
            )
        if not listen_key:
            raise ExchangeError("user_stream_keepalive_requires_listen_key")
        try:
            self.client.fapiPrivatePutListenKey({"listenKey": listen_key})
        except ccxt.BaseError as e:
            raise ExchangeError(f"user_stream_keepalive_failed: {e}") from e

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
        side = "SHORT" if side_value == "short" or contracts < 0 else "LONG"
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
