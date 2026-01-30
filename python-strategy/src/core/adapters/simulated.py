import uuid
from decimal import Decimal
from typing import Optional, List, Dict
from src.core.interfaces.exchange import IExchangeAdapter
from src.core.orm_models import Order
from src.core.models import Position, Candlestick

# Rust PyO3 matching engine
from fluxtrade_core import (
    PyMatchingEngine,
    Order as RustOrder,
    Candlestick as RustCandlestick,
)


class SimulatedAdapter(IExchangeAdapter):
    """Exchange adapter backed by Rust PyMatchingEngine for backtest.

    All balance, position, and order matching logic is delegated to the
    Rust engine.  This adapter converts between Python ORM/Pydantic types
    and the Rust types exposed via PyO3.
    """

    def __init__(
        self,
        initial_balance: Decimal = Decimal("100000"),
        maker_fee: float = 0.0,
        taker_fee: float = 0.0,
    ):
        self._engine = PyMatchingEngine(
            float(initial_balance),
            maker_fee=maker_fee,
            taker_fee=taker_fee,
        )
        # Map order ID → ORM Order so we can return it in fills
        self._order_map: Dict[str, Order] = {}

    # ── IExchangeAdapter interface ───────────────────────────────

    def place_order(self, order: Order) -> str:
        exchange_id = f"SIM-{uuid.uuid4().hex[:8]}"

        rust_order = self._to_rust_order(order)
        self._engine.submit_order(rust_order)
        order.exchange_order_id = exchange_id
        self._order_map[order.id] = order

        return exchange_id

    def cancel_order(self, order_id: str, product_id: str) -> bool:
        # order_id here is the exchange_order_id; we stored ORM id in Rust
        # Try to find the internal id for this exchange_order_id
        for oid, orm_order in self._order_map.items():
            if orm_order.exchange_order_id == order_id:
                cancelled = self._engine.cancel_order(oid)
                if cancelled:
                    del self._order_map[oid]
                return cancelled
        return False

    def get_balance(self, asset: str = "USDT") -> Decimal:
        return Decimal(str(self._engine.balance))

    def get_position(self, product_id: str) -> Optional[Position]:
        rust_positions = self._engine.positions
        rust_pos = rust_positions.get(product_id)
        if not rust_pos or rust_pos.side == "FLAT" or rust_pos.quantity < 1e-9:
            return None
        return Position(
            strategy_id="",
            product_id=product_id,
            side=rust_pos.side,
            quantity=Decimal(str(rust_pos.quantity)),
            entry_price=Decimal(str(rust_pos.entry_price)),
            unrealized_pnl=Decimal(str(rust_pos.unrealized_pnl)),
        )

    # ── Backtest simulation hook ─────────────────────────────────

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        """Process a candle through the Rust matching engine.

        Returns a list of fill dicts compatible with ExecutionEngine:
            {"order": ORM Order, "price": Decimal, "quantity": Decimal,
             "fee": Decimal, "fill_type": str}
        """
        rust_candle = self._to_rust_candle(candle)
        rust_fills = self._engine.on_candle(rust_candle)

        fills: List[Dict] = []
        for rf in rust_fills:
            orm_order = self._order_map.pop(rf.order_id, None)
            if orm_order is None:
                continue
            fills.append({
                "order": orm_order,
                "price": Decimal(str(rf.price)),
                "quantity": Decimal(str(rf.quantity)),
                "fee": Decimal(str(rf.fee)),
                "fill_type": rf.fill_type,
            })

        # Sync _order_map: remove orders cancelled by Rust (e.g. OCO)
        if fills:
            live_ids = {o.id for o in self._engine.open_orders}
            stale = [oid for oid in self._order_map if oid not in live_ids]
            for oid in stale:
                del self._order_map[oid]

        return fills

    # ── Conversion helpers ───────────────────────────────────────

    @staticmethod
    def _side_to_rust(side: str) -> str:
        """Convert buy/sell to LONG/SHORT for the Rust engine."""
        s = side.lower()
        if s == "buy":
            return "LONG"
        if s == "sell":
            return "SHORT"
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
            side = "LONG" if side == "SHORT" else "SHORT"

        trigger_price = None
        if order.trigger_price is not None:
            trigger_price = float(order.trigger_price)

        trailing_distance = None
        if hasattr(order, "_trailing_distance") and order._trailing_distance is not None:
            trailing_distance = float(order._trailing_distance)

        linked_order_id = None
        if hasattr(order, "_linked_order_id") and order._linked_order_id is not None:
            linked_order_id = str(order._linked_order_id)

        return RustOrder(
            id=str(order.id),
            product_id=order.product_id,
            side=side,
            order_type=order_type,
            price=float(order.price) if order.price else 0.0,
            quantity=float(order.quantity),
            timestamp=order.timestamp or 0,
            trigger_price=trigger_price,
            trailing_distance=trailing_distance,
            linked_order_id=linked_order_id,
        )

    @staticmethod
    def _to_rust_candle(candle: Candlestick) -> RustCandlestick:
        return RustCandlestick(
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            open=float(candle.open),
            high=float(candle.high),
            low=float(candle.low),
            close=float(candle.close),
            volume=float(candle.volume),
        )
