from __future__ import annotations

class Candlestick:
    product_id: str
    timeframe: str
    timestamp: int
    open: str
    high: str
    low: str
    close: str
    volume: str

    def __init__(
        self,
        product_id: str,
        timeframe: str,
        timestamp: int,
        open: str,
        high: str,
        low: str,
        close: str,
        volume: str,
    ) -> None: ...

class Trade:
    id: str
    product_id: str
    price: str
    quantity: str
    side: str
    timestamp: int

    def __init__(
        self,
        id: str,
        product_id: str,
        price: str,
        quantity: str,
        side: str,
        timestamp: int,
    ) -> None: ...

class Order:
    id: str
    product_id: str
    strategy_id: str
    side: str
    order_type: str
    price: str
    quantity: str
    timestamp: int
    trigger_price: str | None
    trailing_distance: str | None
    linked_order_id: str | None

    def __init__(
        self,
        id: str,
        product_id: str,
        side: str,
        order_type: str,
        price: str,
        quantity: str,
        timestamp: int,
        trigger_price: str | None = None,
        trailing_distance: str | None = None,
        linked_order_id: str | None = None,
        strategy_id: str = "",
    ) -> None: ...

class FillEvent:
    order_id: str
    product_id: str
    strategy_id: str
    price: str
    quantity: str
    fee: str
    timestamp: int
    fill_type: str

    def __init__(
        self,
        order_id: str,
        product_id: str,
        price: str,
        quantity: str,
        fee: str,
        timestamp: int,
        fill_type: str = "MARKET",
        strategy_id: str = "",
    ) -> None: ...

class Position:
    product_id: str
    strategy_id: str
    side: str
    quantity: str
    entry_price: str
    unrealized_pnl: str

    def __init__(
        self,
        product_id: str,
        side: str,
        quantity: str,
        entry_price: str,
        unrealized_pnl: str,
        strategy_id: str = "",
    ) -> None: ...

class ScaledCandlestick:
    product_id: str
    timeframe: str
    timestamp: int
    open_units: int
    high_units: int
    low_units: int
    close_units: int
    volume_units: int

    def __init__(
        self,
        product_id: str,
        timeframe: str,
        timestamp: int,
        open_units: int,
        high_units: int,
        low_units: int,
        close_units: int,
        volume_units: int,
    ) -> None: ...

class CandleAggregator:
    def __init__(self) -> None: ...
    def add_candle(
        self, candle: Candlestick, target_timeframe: str
    ) -> Candlestick | None: ...
    def reset_product(self, product_id: str) -> None: ...
    @staticmethod
    def can_aggregate(source_timeframe: str, target_timeframe: str) -> bool: ...

class PyMatchingEngine:
    balance: str
    positions: dict[str, Position]
    open_orders: list[Order]

    def __init__(
        self,
        initial_balance: str,
        maker_fee: str = "0",
        taker_fee: str = "0",
        contract_multiplier: str = "1",
        fee_model: str = "percentage_notional",
    ) -> None: ...
    def submit_order(self, order: Order) -> str: ...
    def get_positions(self) -> dict[str, Position]: ...
    def get_position(self, strategy_id: str, product_id: str) -> Position | None: ...
    def on_candle(self, candle: Candlestick) -> list[FillEvent]: ...
    def on_matching_tick(self, candle: Candlestick) -> list[FillEvent]: ...
    def set_scaled_precision(self, price_tick: str, volume_step: str) -> None: ...
    def on_scaled_candle(self, candle: ScaledCandlestick) -> list[FillEvent]: ...
    def cancel_order(self, order_id: str) -> bool: ...
