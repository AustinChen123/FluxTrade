import logging
import uuid
import os
from collections.abc import Mapping
from decimal import Decimal
from fractions import Fraction
from typing import Optional
from redis.exceptions import RedisError
from src.core.orm_models import Order, Trade
from src.core.models import Signal, OrderSide, OrderStatus
from src.core.clock import Clock
from src.core.decimal_math import (
    canonical_decimal_text,
    decimal_from_fraction_significant,
    exact_decimal_add,
)
from src.core.interfaces import IOrderRepository
from src.core.jsonb_helpers import serialize_payload_with_decimals
from src.core.redis_factory import create_redis_client
from src.core.runtime_capabilities import OrderAccountIdentityResolver

logger = logging.getLogger(__name__)

_VALID_SIDES = {OrderSide.BUY, OrderSide.SELL, "buy", "sell"}
_VALID_ORDER_TYPES = {"market", "limit", "stop_loss", "take_profit", "trailing_stop"}


class PositionCachePersistenceError(RuntimeError):
    """The durable fill succeeded but its non-authoritative cache did not."""


class _PositionCacheSchemaError(ValueError):
    pass


class OrderManager:
    def __init__(
        self,
        repo: IOrderRepository,
        clock: Clock,
        is_backtest: Optional[bool] = None,
        *,
        order_account_identity_resolver: OrderAccountIdentityResolver | None = None,
    ):
        self.repo = repo
        self.clock = clock
        self.redis_client = None
        self.update_position_script = None

        # Detect Backtest Mode: explicit flag > repository type heuristic
        if is_backtest is not None:
            self.is_backtest = is_backtest
        else:
            self.is_backtest = "BacktestOrderRepository" in str(type(repo))

        if not self.is_backtest:
            self.redis_client = create_redis_client()
            # Load Lua Script
            lua_path = os.path.join(
                os.path.dirname(__file__), "../lua/update_position.lua"
            )
            try:
                with open(lua_path, "r") as f:
                    self.update_position_script = self.redis_client.register_script(
                        f.read()
                    )
            except Exception as e:
                logger.error("FATAL: Failed to load Lua script: %s", e)
                raise e
        else:
            logger.info("OrderManager: Initialized in Backtest Mode (Redis Disabled).")
        self._order_account_identity_resolver = order_account_identity_resolver

    def create_order(
        self,
        signal: Signal,
        side: OrderSide,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        trigger_price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        intent_payload: Optional[dict] = None,
    ) -> Order:
        if side.lower() not in _VALID_SIDES:
            raise ValueError(
                f"Invalid order side: {side!r}. Must be one of {_VALID_SIDES}"
            )
        if order_type.lower() not in _VALID_ORDER_TYPES:
            raise ValueError(
                f"Invalid order type: {order_type!r}. Must be one of {_VALID_ORDER_TYPES}"
            )
        exchange_id = signal.product_id.split(":")[0]
        account_profile = account_id = None
        if self._order_account_identity_resolver is not None:
            account_identity = self._order_account_identity_resolver(
                signal.product_id,
                is_backtest=self.is_backtest,
            )
            if account_identity is not None:
                account_profile = account_identity.account_profile
                account_id = account_identity.account_id
        order_id = str(uuid.uuid4())
        is_idempotent_order = client_order_id is not None

        new_order = Order(
            id=order_id,
            exchange_order_id=None if is_idempotent_order else f"sim_{order_id[:8]}",
            strategy_id=signal.strategy_id,
            product_id=signal.product_id,
            exchange_id=exchange_id,
            account_profile=account_profile,
            account_id=account_id,
            type=order_type,
            side=side,
            price=price,
            trigger_price=trigger_price,
            quantity=quantity,
            status=OrderStatus.NEW.value if is_idempotent_order else "open",
            timestamp=int(self.clock.now() * 1000),
            filled_quantity=Decimal("0"),
            filled_price=Decimal("0"),
            client_order_id=client_order_id,
            intent_payload=(
                serialize_payload_with_decimals(intent_payload)
                if intent_payload is not None
                else None
            ),
        )

        self.repo.add_order(new_order)
        logger.info(
            "Order created %s (%s %s %s %s)",
            new_order.id,
            side,
            order_type,
            quantity,
            signal.product_id,
        )
        return new_order

    def update_exchange_order_id(self, order: Order, exchange_order_id: str):
        self.repo.update_order_exchange_id(order, exchange_order_id)

    def mark_submitted_unconfirmed(self, order: Order) -> None:
        """Mark order as sent to exchange with ACK pending."""
        self._set_order_status(order, OrderStatus.SUBMITTED_UNCONFIRMED)

    def mark_submitted(
        self, order: Order, exchange_order_id: Optional[str] = None
    ) -> None:
        """Mark order as exchange-acknowledged."""
        order.status = OrderStatus.SUBMITTED.value
        if exchange_order_id is not None:
            self.repo.update_order_exchange_id(order, exchange_order_id)
        else:
            self.repo.update_order(order)

    def mark_cancelled(self, order: Order) -> None:
        """Mark order as cancelled."""
        self._set_order_status(order, OrderStatus.CANCELLED)

    def _set_order_status(self, order: Order, status: OrderStatus) -> None:
        order.status = status.value
        self.repo.update_order(order)

    def fail_order(self, order: Order, reason: str):
        """Marks an order as FAILED due to execution errors."""
        order.status = "failed"
        # We could verify if there's a specific field for error msg, but for now just status
        logger.error(
            "ORDER_FAILED: Order %s marked as FAILED. Reason: %s", order.id, reason
        )
        self.repo.update_order(order)

    def fill_order(
        self,
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee: Optional[Decimal] = None,
        fee_asset: Optional[str] = None,
    ):
        self._validated_fill(order.side, fill_price, fill_quantity)
        order.status = "closed"
        order.filled_quantity = fill_quantity
        order.filled_price = fill_price
        self._record_fill_trade(
            order,
            fill_price,
            fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
            log_label="Position Update",
        )

    def record_partial_fill(
        self,
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        cumulative_filled_quantity: Decimal,
        cumulative_average_price: Decimal,
        fee: Optional[Decimal] = None,
        fee_asset: Optional[str] = None,
    ) -> None:
        """Record a non-terminal exchange fill while keeping the order tracked."""
        self._validated_fill(order.side, fill_price, fill_quantity)
        order.status = OrderStatus.SUBMITTED.value
        order.filled_quantity = cumulative_filled_quantity
        order.filled_price = cumulative_average_price
        self._record_fill_trade(
            order,
            fill_price,
            fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
            log_label="Partial Position Update",
        )

    def record_fill_delta(
        self,
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        cumulative_filled_quantity: Decimal,
        cumulative_average_price: Decimal,
        terminal_status: OrderStatus | None = None,
        fee: Optional[Decimal] = None,
        fee_asset: Optional[str] = None,
    ) -> None:
        """Record a recovered exchange fill delta and set the desired order status."""
        self._validated_fill(order.side, fill_price, fill_quantity)
        order.status = (
            terminal_status.value
            if terminal_status is not None
            else OrderStatus.SUBMITTED.value
        )
        order.filled_quantity = cumulative_filled_quantity
        order.filled_price = cumulative_average_price
        self._record_fill_trade(
            order,
            fill_price,
            fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
            log_label="Recovered Position Update",
        )

    def _record_fill_trade(
        self,
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        *,
        fee: Optional[Decimal] = None,
        fee_asset: Optional[str] = None,
        log_label: str,
    ) -> None:
        side = self._validated_fill(order.side, fill_price, fill_quantity)
        current_time = int(self.clock.now() * 1000)
        trade_id = str(uuid.uuid4())

        new_trade = Trade(
            id=trade_id,
            order_id=order.id,
            exchange_trade_id=f"trd_{trade_id[:8]}",
            product_id=order.product_id,
            side=order.side,
            price=fill_price,
            quantity=fill_quantity,
            fee=fee if fee is not None else Decimal("0"),
            fee_asset=fee_asset or "USDT",
            timestamp=current_time,
        )
        self.repo.persist_fill(order, new_trade)

        if not self.is_backtest:
            try:
                position_quantity, entry_price = self._project_live_position(
                    strategy_id=order.strategy_id,
                    product_id=order.product_id,
                    side=side,
                    fill_quantity=fill_quantity,
                    fill_price=fill_price,
                )
                update_position_script = self.update_position_script
                if update_position_script is None:
                    raise _PositionCacheSchemaError(
                        "Redis update position script unavailable"
                    )
                update_position_script(
                    args=[
                        order.strategy_id,
                        order.product_id,
                        side,
                        str(fill_quantity),
                        str(fill_price),
                        str(current_time),
                        trade_id,
                        order.id,
                        canonical_decimal_text(position_quantity),
                        canonical_decimal_text(entry_price),
                    ]
                )
                logger.info(
                    "Redis: Atomic %s Successful (Trade %s)", log_label, trade_id
                )
            except (RedisError, _PositionCacheSchemaError):
                raise PositionCachePersistenceError(
                    "Live fill position cache projection failed"
                ) from None
        else:
            self.repo.update_position(
                strategy_id=order.strategy_id,
                product_id=order.product_id,
                side=order.side,
                fill_quantity=fill_quantity,
                fill_price=fill_price,
                position_side=order.side.upper(),
            )

    @staticmethod
    def _validated_fill(
        side: object,
        fill_price: Decimal,
        fill_quantity: Decimal,
    ) -> str:
        normalized_side = (
            side.value.upper() if isinstance(side, OrderSide) else str(side).upper()
        )
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("fill side must be BUY or SELL")
        for name, value in (
            ("fill_price", fill_price),
            ("fill_quantity", fill_quantity),
        ):
            if type(value) is not Decimal or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a finite positive exact Decimal")
        return normalized_side

    def _project_live_position(
        self,
        *,
        strategy_id: str,
        product_id: str,
        side: str,
        fill_quantity: Decimal,
        fill_price: Decimal,
    ) -> tuple[Decimal, Decimal]:
        redis_client = self.redis_client
        if redis_client is None:
            raise _PositionCacheSchemaError("Redis client unavailable")
        raw_position = redis_client.hgetall(
            f"state:position:{strategy_id}:{product_id}"
        )
        current_quantity, current_entry_price = self._cached_position(raw_position)
        signed_delta = fill_quantity if side == "BUY" else fill_quantity.copy_negate()
        new_quantity = exact_decimal_add(current_quantity, signed_delta)
        if current_quantity == 0:
            return new_quantity, fill_price
        if current_quantity.copy_sign(signed_delta) == current_quantity:
            total_notional = Fraction(abs(current_quantity)) * Fraction(
                current_entry_price
            ) + Fraction(fill_quantity) * Fraction(fill_price)
            total_quantity = Fraction(abs(current_quantity)) + Fraction(fill_quantity)
            return (
                new_quantity,
                decimal_from_fraction_significant(
                    total_notional / total_quantity,
                    precision=28,
                ),
            )
        if new_quantity == 0:
            return Decimal(0), Decimal(0)
        if current_quantity.copy_sign(new_quantity) == current_quantity:
            return new_quantity, current_entry_price
        return new_quantity, fill_price

    @staticmethod
    def _cached_position(raw_position: object) -> tuple[Decimal, Decimal]:
        if not isinstance(raw_position, Mapping):
            raise _PositionCacheSchemaError("Redis position must be a hash")
        if not raw_position:
            return Decimal(0), Decimal(0)

        def field(name: str) -> Decimal:
            raw = raw_position.get(name)
            if raw is None:
                raw = raw_position.get(name.encode())
            if isinstance(raw, bytes):
                try:
                    raw = raw.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise _PositionCacheSchemaError(
                        "Redis position field must be UTF-8 text"
                    ) from error
            if type(raw) is not str:
                raise _PositionCacheSchemaError(
                    "Redis position field must be exact text"
                )
            try:
                value = Decimal(raw)
            except Exception as error:
                raise _PositionCacheSchemaError(
                    "Redis position field must be Decimal text"
                ) from error
            if not value.is_finite():
                raise _PositionCacheSchemaError("Redis position field must be finite")
            return value

        quantity = field("quantity")
        entry_price = field("entry_price")
        if (quantity == 0 and entry_price != 0) or (quantity != 0 and entry_price <= 0):
            raise _PositionCacheSchemaError("Redis position fields are inconsistent")
        return quantity, entry_price
