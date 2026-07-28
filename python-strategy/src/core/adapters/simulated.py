import inspect
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Optional, List, Dict
from src.core.interfaces.exchange import ExchangeError, ExchangeOrderSnapshot, IExchangeAdapter
from src.core.orm_models import Order
from src.core.models import OrderSide, Position, Candlestick, PositionSide
from src.core.precision import PrecisionCodec
from src.core.product_registry import (
    InstrumentSpec,
    is_dated_future_product_id,
    quantize_order_values,
    resolve_contract_multiplier,
    resolve_fee_model,
)
from src.core.strategy_context import (
    CapitalSnapshot,
    FillSnapshot,
    OrderSnapshot,
    PositionSnapshot,
    RejectionSnapshot,
    RiskSnapshot,
    StrategyContext,
)

# Rust PyO3 matching engine
from fluxtrade_core import (
    PyMatchingEngine,
    Order as RustOrder,
    Candlestick as RustCandlestick,
)

try:
    from fluxtrade_core import ScaledCandlestick as RustScaledCandlestick
except ImportError:  # pragma: no cover - depends on local extension build
    RustScaledCandlestick = None

if TYPE_CHECKING:
    from src.core.capital_allocator import CapitalAllocator

# Detect if Rust engine supports strategy_id parameter
_RUST_HAS_STRATEGY_ID = "strategy_id" in str(inspect.signature(RustOrder))


class SimulatedAdapter(IExchangeAdapter):
    """Exchange adapter backed by Rust PyMatchingEngine for backtest.

    All balance, position, and order matching logic is delegated to the
    Rust engine.  This adapter converts between Python ORM/Pydantic types
    and the Rust types exposed via PyO3.
    """

    def __init__(
        self,
        initial_balance: Decimal = Decimal("100000"),
        maker_fee: Decimal = Decimal("0"),
        taker_fee: Decimal = Decimal("0"),
        precision_codec: PrecisionCodec | None = None,
        instrument_spec: InstrumentSpec | None = None,
    ):
        contract_multiplier = resolve_contract_multiplier(instrument_spec)
        fee_model = resolve_fee_model(instrument_spec)
        self._instrument_spec = instrument_spec
        self._engine = PyMatchingEngine(
            str(initial_balance),
            maker_fee=str(maker_fee),
            taker_fee=str(taker_fee),
            contract_multiplier=str(contract_multiplier),
            fee_model=fee_model.value,
        )
        self._contract_multiplier = contract_multiplier
        self._precision_codec = precision_codec
        if precision_codec is not None:
            if RustScaledCandlestick is None or not hasattr(self._engine, "on_scaled_candle"):
                raise RuntimeError("compiled Rust engine does not support scaled candle matching")
            self._engine.set_scaled_precision(
                str(precision_codec.spec.price_tick),
                str(precision_codec.spec.quantity_step),
            )
        # Map order ID → ORM Order so we can return it in fills
        self._order_map: Dict[str, Order] = {}
        self._rust_supports_strategy_id = _RUST_HAS_STRATEGY_ID

    @property
    def supports_strategy_positions(self) -> bool:
        """Whether Rust positions are isolated by strategy_id."""
        return self._rust_supports_strategy_id

    def get_instrument_spec(self, product_id: str) -> InstrumentSpec | None:
        if self._instrument_spec is None or self._instrument_spec.product_id != product_id:
            return None
        return self._instrument_spec

    # ── IExchangeAdapter interface ───────────────────────────────

    def place_order(self, order: Order) -> str:
        self.validate_order(order)
        exchange_id = f"SIM-{uuid.uuid4().hex[:8]}"

        rust_order = self._to_rust_order(order)
        self._engine.submit_order(rust_order)
        order.exchange_order_id = exchange_id
        self._order_map[order.id] = order

        return exchange_id

    def validate_order(self, order: Order) -> None:
        if self._instrument_spec is None:
            if is_dated_future_product_id(order.product_id):
                raise ExchangeError(
                    "instrument_spec_required_for_dated_future: "
                    f"product_id={order.product_id}"
                )
            return
        instrument_spec = self.get_instrument_spec(order.product_id)
        if instrument_spec is None:
            raise ExchangeError(
                "instrument_spec_product_mismatch: "
                f"configured={self._instrument_spec.product_id} "
                f"order={order.product_id}"
            )
        try:
            quantized = quantize_order_values(
                quantity=order.quantity,
                price=order.price,
                side=order.side,
                order_type=order.type,
                trigger_price=order.trigger_price,
                trailing_distance=getattr(order, "_trailing_distance", None),
                spec=instrument_spec,
            )
        except ValueError as exc:
            raise ExchangeError(str(exc)) from exc
        if quantized.changed:
            order.quantity = quantized.quantity
            order.price = quantized.price
            order.trigger_price = quantized.trigger_price

    def cancel_order(
        self,
        order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> bool:
        # order_type is only needed by venues with a separate conditional-order
        # id namespace; the simulated matcher has a single namespace.
        # order_id here is the exchange_order_id; we stored ORM id in Rust
        # Try to find the internal id for this exchange_order_id
        for oid, orm_order in self._order_map.items():
            if orm_order.exchange_order_id == order_id:
                cancelled = self._engine.cancel_order(oid)
                if cancelled:
                    del self._order_map[oid]
                return cancelled
        return False

    def cancel_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> bool:
        for oid, orm_order in self._order_map.items():
            if orm_order.client_order_id == client_order_id:
                cancelled = self._engine.cancel_order(oid)
                if cancelled:
                    del self._order_map[oid]
                return cancelled
        return False

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: Optional[str] = None,
    ) -> Optional[ExchangeOrderSnapshot]:
        for orm_order in self._order_map.values():
            if orm_order.client_order_id == client_order_id and orm_order.product_id == product_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=orm_order.exchange_order_id,
                    status=orm_order.status,
                    raw=None,
                )
        return None

    def get_balance(self, asset: str = "USDT") -> Decimal:
        return Decimal(self._engine.balance)

    def get_position(self, product_id: str, strategy_id: Optional[str] = None) -> Optional[Position]:
        rust_positions = self._engine.positions

        rust_pos = None
        if strategy_id:
            # Composite key: "strategy_id:product_id" (Rust engine uses this)
            composite_key = f"{strategy_id}:{product_id}"
            rust_pos = rust_positions.get(composite_key)
            # Fallback to product_id-only key for backward compat with older Rust engine
            if rust_pos is None:
                rust_pos = rust_positions.get(product_id)
        else:
            # No strategy_id specified: try product_id-only key first, then
            # scan composite keys ending with the product_id (return first match)
            rust_pos = rust_positions.get(product_id)
            if rust_pos is None:
                suffix = f":{product_id}"
                for key, pos in rust_positions.items():
                    if key.endswith(suffix):
                        rust_pos = pos
                        break

        if not rust_pos or rust_pos.side == "FLAT" or Decimal(rust_pos.quantity) <= 0:
            return None

        resolved_strategy_id = strategy_id or getattr(rust_pos, "strategy_id", "") or ""
        return Position(
            strategy_id=resolved_strategy_id,
            product_id=product_id,
            side=PositionSide(rust_pos.side),
            quantity=Decimal(rust_pos.quantity),
            entry_price=Decimal(rust_pos.entry_price),
            unrealized_pnl=Decimal(rust_pos.unrealized_pnl),
        )

    def get_open_orders(
        self,
        product_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
    ) -> List[Order]:
        """Return open ORM orders known to the simulated adapter."""
        orders = list(self._order_map.values())
        if product_id is not None:
            orders = [order for order in orders if order.product_id == product_id]
        if strategy_id is not None:
            orders = [order for order in orders if order.strategy_id == strategy_id]
        return orders

    def get_strategy_context(
        self,
        *,
        strategy_id: str,
        product_id: str,
        timestamp: int,
        initial_balance: Decimal,
        mark_price: Optional[Decimal] = None,
        peak_equity: Optional[Decimal] = None,
        max_drawdown: Decimal = Decimal("0"),
        latest_fills: Optional[List[Dict]] = None,
        latest_rejections: tuple[RejectionSnapshot, ...] = (),
        risk: RiskSnapshot = RiskSnapshot(),
        capital_allocator: Optional["CapitalAllocator"] = None,
    ) -> StrategyContext:
        """Build a read-only decision context from matcher-backed state."""
        cash = self.get_balance()
        position = self.get_position(product_id, strategy_id=strategy_id)
        position_snapshot = (
            _position_snapshot(position, mark_price, self._contract_multiplier)
            if position
            else None
        )
        unrealized_pnl = position_snapshot.unrealized_pnl if position_snapshot else Decimal("0")
        total_equity = cash + unrealized_pnl
        realized_pnl = total_equity - Decimal(str(initial_balance))
        if peak_equity is None or peak_equity <= 0:
            current_drawdown = Decimal("0")
        else:
            current_drawdown = max(peak_equity - total_equity, Decimal("0"))

        return StrategyContext(
            strategy_id=strategy_id,
            product_id=product_id,
            timestamp=timestamp,
            available_cash=cash,
            total_equity=total_equity,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            current_drawdown=current_drawdown,
            max_drawdown=max_drawdown,
            position=position_snapshot,
            open_orders=tuple(_order_snapshot(order) for order in self.get_open_orders(product_id, strategy_id)),
            latest_fills=tuple(
                _fill_snapshot(fill, timestamp) for fill in latest_fills or []
                if fill.get("order") is not None
            ),
            latest_rejections=latest_rejections,
            risk=risk,
            capital=_capital_snapshot(capital_allocator, strategy_id),
        )

    # ── Backtest simulation hook ─────────────────────────────────

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        """Process a candle through the Rust matching engine.

        Returns a list of fill dicts compatible with ExecutionEngine:
            {"order": ORM Order, "price": Decimal, "quantity": Decimal,
             "fee": Decimal, "fill_type": str}
        """
        rust_candle = self._to_rust_candle(candle)
        if self._precision_codec is not None:
            rust_fills = self._engine.on_scaled_candle(rust_candle)
        else:
            rust_fills = self._engine.on_candle(rust_candle)

        return self._fills_from_rust(rust_fills)

    def prepare_scaled_candle(self, candle: Candlestick):
        """Convert a Decimal candle to scaled units outside the replay hot loop."""
        return self._to_scaled_rust_candle(candle)

    def on_prepared_market_data(self, scaled_candle) -> List[Dict]:
        """Process a pre-encoded scaled candle through the Rust matching engine."""
        if self._precision_codec is None:
            raise RuntimeError("precision codec is required for prepared scaled candles")
        rust_fills = self._engine.on_scaled_candle(scaled_candle)
        return self._fills_from_rust(rust_fills)

    def _fills_from_rust(self, rust_fills) -> List[Dict]:
        fills: List[Dict] = []
        for rf in rust_fills:
            orm_order = self._order_map.pop(rf.order_id, None)
            if orm_order is None:
                continue
            fills.append({
                "order": orm_order,
                "price": Decimal(rf.price),
                "quantity": Decimal(rf.quantity),
                "fee": Decimal(rf.fee),
                "fill_type": rf.fill_type,
            })

        # Sync _order_map: remove orders cancelled by Rust (e.g. OCO)
        if fills:
            live_ids = {o.id for o in self._engine.open_orders}
            stale = [oid for oid in self._order_map if oid not in live_ids]
            cancelled_orders = [self._order_map[oid] for oid in stale]
            for oid in stale:
                del self._order_map[oid]
            if cancelled_orders:
                fills[-1]["cancelled_orders"] = cancelled_orders

        return fills

    # ── Conversion helpers ───────────────────────────────────────

    @staticmethod
    def _side_to_rust(side: str) -> str:
        """Convert buy/sell (OrderSide) to LONG/SHORT (PositionSide) for the Rust engine."""
        s = side.lower()
        if s == "buy":
            return PositionSide.LONG
        if s == "sell":
            return PositionSide.SHORT
        # Already LONG/SHORT
        return side.upper()

    @staticmethod
    def _order_type_to_rust(order_type: str) -> str:
        """Normalise order type string for Rust."""
        return order_type.upper().replace(" ", "_")

    def _to_rust_order(self, order: Order) -> RustOrder:
        side = self._side_to_rust(order.side)
        order_type = self._order_type_to_rust(order.type)

        # For conditional orders (SL/TP/Trailing), Rust expects 'side' to be the
        # position side being protected — not the trade direction.
        # ORM: side="sell" means "sell to close long" → Rust side="LONG"
        # ORM: side="buy" means "buy to close short" → Rust side="SHORT"
        if order_type in ("STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"):
            side = PositionSide.LONG if side == PositionSide.SHORT else PositionSide.SHORT

        trigger_price = None
        if order.trigger_price is not None:
            trigger_price = str(order.trigger_price)

        trailing_distance = None
        if hasattr(order, "_trailing_distance") and order._trailing_distance is not None:
            trailing_distance = str(order._trailing_distance)

        linked_order_id = None
        if hasattr(order, "_linked_order_id") and order._linked_order_id is not None:
            linked_order_id = str(order._linked_order_id)

        kwargs: dict = dict(
            id=str(order.id),
            product_id=order.product_id,
            side=side,
            order_type=order_type,
            price=str(order.price) if order.price else "0",
            quantity=str(order.quantity),
            timestamp=order.timestamp or 0,
            trigger_price=trigger_price,
            trailing_distance=trailing_distance,
            linked_order_id=linked_order_id,
        )
        # Pass strategy_id if the Rust engine supports it
        if self._rust_supports_strategy_id:
            kwargs["strategy_id"] = order.strategy_id or ""

        return RustOrder(**kwargs)

    def _to_rust_candle(self, candle: Candlestick) -> RustCandlestick:
        if self._precision_codec is not None:
            return self._to_scaled_rust_candle(candle)
        return RustCandlestick(
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            open=str(candle.open),
            high=str(candle.high),
            low=str(candle.low),
            close=str(candle.close),
            volume=str(candle.volume),
        )

    def _to_scaled_rust_candle(self, candle: Candlestick):
        if RustScaledCandlestick is None:
            raise RuntimeError("compiled Rust engine does not support scaled candles")
        codec = self._precision_codec
        if codec is None:
            raise RuntimeError("precision codec is not configured")
        return RustScaledCandlestick(
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            open_units=codec.encode_price(candle.open),
            high_units=codec.encode_price(candle.high),
            low_units=codec.encode_price(candle.low),
            close_units=codec.encode_price(candle.close),
            volume_units=codec.encode_quantity(candle.volume),
        )


def _position_snapshot(
    position: Position,
    mark_price: Optional[Decimal],
    contract_multiplier: Decimal = Decimal("1"),
) -> PositionSnapshot:
    notional = None
    unrealized_pnl = position.unrealized_pnl
    if mark_price is not None:
        notional = abs(position.quantity * mark_price * contract_multiplier)
        if position.side == PositionSide.LONG:
            unrealized_pnl = (
                (mark_price - position.entry_price)
                * position.quantity
                * contract_multiplier
            )
        elif position.side == PositionSide.SHORT:
            unrealized_pnl = (
                (position.entry_price - mark_price)
                * position.quantity
                * contract_multiplier
            )
    return PositionSnapshot(
        side=position.side,
        quantity=position.quantity,
        average_entry_price=position.entry_price,
        mark_price=mark_price,
        notional=notional,
        unrealized_pnl=unrealized_pnl,
    )


def _order_snapshot(order: Order) -> OrderSnapshot:
    return OrderSnapshot(
        id=order.id,
        product_id=order.product_id,
        side=OrderSide(order.side),
        order_type=order.type,
        quantity=Decimal(order.quantity),
        timestamp=order.timestamp,
        price=Decimal(order.price) if order.price is not None else None,
        status=order.status,
    )


def _fill_snapshot(fill: Dict, timestamp: int) -> FillSnapshot:
    order = fill["order"]
    return FillSnapshot(
        order_id=order.id,
        product_id=order.product_id,
        side=OrderSide(order.side),
        price=fill["price"],
        quantity=fill["quantity"],
        fee=fill.get("fee") or Decimal("0"),
        timestamp=timestamp,
    )


def _capital_snapshot(
    capital_allocator: Optional["CapitalAllocator"],
    strategy_id: str,
) -> CapitalSnapshot | None:
    if capital_allocator is None:
        return None
    return CapitalSnapshot(
        allocated=capital_allocator.get_allocation(strategy_id),
        used=capital_allocator.get_used(strategy_id),
        available=capital_allocator.get_available(strategy_id),
        unallocated=capital_allocator.get_unallocated(),
    )
