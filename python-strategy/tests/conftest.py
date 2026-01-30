"""
Shared pytest fixtures for FluxTrade Python Strategy tests.

Provides mock services, factory fixtures, and test adapters to enable
comprehensive testing without external dependencies (Redis, PostgreSQL, Exchange APIs).
"""

import pytest
import uuid
import time
from decimal import Decimal
from typing import Optional, Dict, List
from unittest.mock import Mock, MagicMock

# Models
from src.core.models import (
    Signal, SignalType, Candlestick, Position, Trade
)
from src.core.orm_models import Order, Trade as ORMTrade, Position as ORMPosition

# Interfaces
from src.core.interfaces import IOrderRepository, IExchangeAdapter

# Core modules
from src.core.risk_manager import AccountService
from src.core.clock import Clock


# =============================================================================
# Constants
# =============================================================================

DEFAULT_PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
DEFAULT_STRATEGY_ID = "test_strategy"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_BALANCE = Decimal("100000")


# =============================================================================
# Mock Services
# =============================================================================

class MockAccountService(AccountService):
    """Mock AccountService that doesn't require Redis."""

    def __init__(self, balance: Decimal = DEFAULT_BALANCE):
        self._balance = balance
        self._positions: Dict[str, Position] = {}

    def get_balance(self) -> Decimal:
        return self._balance

    def set_balance(self, balance: Decimal):
        """Test helper to set balance."""
        self._balance = balance

    def get_position(self, strategy_id: str, product_id: str) -> Optional[Position]:
        key = f"{strategy_id}:{product_id}"
        return self._positions.get(key)

    def set_position(self, position: Position):
        """Test helper to set a position."""
        key = f"{position.strategy_id}:{position.product_id}"
        self._positions[key] = position

    def clear_positions(self):
        """Test helper to clear all positions."""
        self._positions.clear()


class MockClock(Clock):
    """Mock clock for deterministic time in tests."""

    def __init__(self, initial_time: float = 1704067200.0):  # 2024-01-01 00:00:00 UTC
        self._current_time = initial_time

    def now(self) -> float:
        return self._current_time

    def advance(self, seconds: float):
        """Advance time by given seconds."""
        self._current_time += seconds

    def set_time(self, timestamp: float):
        """Set time to specific timestamp."""
        self._current_time = timestamp


class MockOrderRepository(IOrderRepository):
    """In-memory order repository for testing."""

    def __init__(self):
        self.orders: Dict[str, Order] = {}
        self.trades: List[ORMTrade] = []
        self.positions: Dict[str, ORMPosition] = {}

    def add_order(self, order: Order) -> None:
        self.orders[order.id] = order

    def update_order(self, order: Order) -> None:
        self.orders[order.id] = order

    def add_trade(self, trade: ORMTrade) -> None:
        self.trades.append(trade)

    def get_position(self, strategy_id: str, product_id: str, side: str = None) -> Optional[ORMPosition]:
        key = f"{strategy_id}:{product_id}"
        pos = self.positions.get(key)
        if pos and side and pos.side != side:
            return None
        return pos

    def update_position(self, strategy_id: str, product_id: str, side: str,
                       fill_quantity: Decimal, fill_price: Decimal, position_side: str = None) -> None:
        key = f"{strategy_id}:{product_id}"
        pos = self.positions.get(key)
        current_time = int(time.time() * 1000)

        if not pos:
            pos = ORMPosition(
                strategy_id=strategy_id,
                product_id=product_id,
                side=side.upper(),
                quantity=fill_quantity,
                entry_price=fill_price,
                unrealized_pnl=Decimal("0"),
                last_update_timestamp=current_time
            )
            self.positions[key] = pos
        else:
            # Simple update logic
            if side.lower() == 'buy':
                total_cost = (pos.quantity * pos.entry_price) + (fill_quantity * fill_price)
                total_qty = pos.quantity + fill_quantity
                if total_qty > 0:
                    pos.entry_price = total_cost / total_qty
                pos.quantity = total_qty
            else:
                pos.quantity = max(Decimal("0"), pos.quantity - fill_quantity)
            pos.last_update_timestamp = current_time

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id

    def clear(self):
        """Test helper to clear all data."""
        self.orders.clear()
        self.trades.clear()
        self.positions.clear()


class MockExchangeAdapter(IExchangeAdapter):
    """Mock exchange adapter for testing."""

    def __init__(self, initial_balance: Decimal = DEFAULT_BALANCE):
        self.balance = {"USDT": initial_balance}
        self.positions: Dict[str, Position] = {}
        self.open_orders: List[Order] = []
        self.filled_orders: List[Dict] = []
        self._next_fill_price: Optional[Decimal] = None
        self._should_fail: bool = False
        self._fail_reason: str = ""

    def place_order(self, order: Order) -> str:
        if self._should_fail:
            from src.core.interfaces import ExchangeError
            raise ExchangeError(self._fail_reason)

        exchange_id = f"MOCK-{uuid.uuid4().hex[:8]}"
        order.exchange_order_id = exchange_id
        self.open_orders.append(order)
        return exchange_id

    def cancel_order(self, order_id: str, product_id: str) -> bool:
        initial_len = len(self.open_orders)
        self.open_orders = [o for o in self.open_orders if o.exchange_order_id != order_id]
        return len(self.open_orders) < initial_len

    def get_balance(self, asset: str) -> Decimal:
        return self.balance.get(asset, Decimal("0"))

    def get_position(self, product_id: str) -> Optional[Position]:
        return self.positions.get(product_id)

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        fills = []
        remaining = []

        for order in self.open_orders:
            if order.product_id != candle.product_id:
                remaining.append(order)
                continue

            fill_price = self._next_fill_price or candle.close
            fills.append({
                "order": order,
                "price": fill_price,
                "quantity": order.quantity
            })

        self.open_orders = remaining
        self.filled_orders.extend(fills)
        return fills

    # Test helpers
    def set_next_fill_price(self, price: Decimal):
        self._next_fill_price = price

    def set_should_fail(self, should_fail: bool, reason: str = "Mock failure"):
        self._should_fail = should_fail
        self._fail_reason = reason

    def set_balance(self, asset: str, amount: Decimal):
        self.balance[asset] = amount

    def set_position(self, product_id: str, position: Position):
        self.positions[product_id] = position


# =============================================================================
# Fixture Factories
# =============================================================================

@pytest.fixture
def mock_account_service():
    """Provides a MockAccountService with default balance."""
    return MockAccountService(balance=DEFAULT_BALANCE)


@pytest.fixture
def mock_account_service_factory():
    """Factory to create MockAccountService with custom balance."""
    def _create(balance: Decimal = DEFAULT_BALANCE) -> MockAccountService:
        return MockAccountService(balance=balance)
    return _create


@pytest.fixture
def mock_clock():
    """Provides a MockClock starting at 2024-01-01."""
    return MockClock()


@pytest.fixture
def mock_order_repo():
    """Provides a MockOrderRepository."""
    return MockOrderRepository()


@pytest.fixture
def mock_exchange_adapter():
    """Provides a MockExchangeAdapter."""
    return MockExchangeAdapter()


@pytest.fixture
def mock_redis_client():
    """Provides a mock Redis client."""
    mock = MagicMock()
    mock.ping.return_value = True
    mock.hget.return_value = str(DEFAULT_BALANCE)
    mock.hgetall.return_value = {}
    return mock


@pytest.fixture
def mock_db_session():
    """Provides a mock SQLAlchemy session with rollback support."""
    session = MagicMock()
    session.query.return_value.filter_by.return_value.first.return_value = None
    session.query.return_value.with_for_update.return_value.filter_by.return_value.first.return_value = None
    return session


# =============================================================================
# Data Fixtures - Candlesticks
# =============================================================================

@pytest.fixture
def sample_candlestick():
    """Provides a sample Candlestick."""
    return Candlestick(
        product_id=DEFAULT_PRODUCT_ID,
        timeframe=DEFAULT_TIMEFRAME,
        timestamp=1704067200000,  # 2024-01-01 00:00:00 UTC
        open=Decimal("42000.00"),
        high=Decimal("42500.00"),
        low=Decimal("41500.00"),
        close=Decimal("42200.00"),
        volume=Decimal("1000.50")
    )


@pytest.fixture
def candlestick_factory():
    """Factory to create Candlesticks with custom parameters."""
    def _create(
        product_id: str = DEFAULT_PRODUCT_ID,
        timeframe: str = DEFAULT_TIMEFRAME,
        timestamp: int = 1704067200000,
        open: Decimal = Decimal("42000.00"),
        high: Decimal = Decimal("42500.00"),
        low: Decimal = Decimal("41500.00"),
        close: Decimal = Decimal("42200.00"),
        volume: Decimal = Decimal("1000.50")
    ) -> Candlestick:
        return Candlestick(
            product_id=product_id,
            timeframe=timeframe,
            timestamp=timestamp,
            open=open,
            high=high,
            low=low,
            close=close,
            volume=volume
        )
    return _create


@pytest.fixture
def candlestick_series_factory():
    """Factory to create a series of Candlesticks for backtesting."""
    def _create(
        count: int = 100,
        product_id: str = DEFAULT_PRODUCT_ID,
        timeframe: str = DEFAULT_TIMEFRAME,
        start_timestamp: int = 1704067200000,
        start_price: Decimal = Decimal("42000.00"),
        volatility: Decimal = Decimal("100.00")
    ) -> List[Candlestick]:
        import random
        candles = []
        price = float(start_price)

        for i in range(count):
            change = random.gauss(0, float(volatility))
            open_price = Decimal(str(round(price, 2)))
            close_price = Decimal(str(round(price + change, 2)))
            high = max(open_price, close_price) + Decimal(str(abs(random.gauss(0, float(volatility) / 2))))
            low = min(open_price, close_price) - Decimal(str(abs(random.gauss(0, float(volatility) / 2))))

            candles.append(Candlestick(
                product_id=product_id,
                timeframe=timeframe,
                timestamp=start_timestamp + i * 60000,
                open=open_price,
                high=high.quantize(Decimal("0.01")),
                low=low.quantize(Decimal("0.01")),
                close=close_price,
                volume=Decimal(str(round(random.uniform(100, 1000), 2)))
            ))
            price = float(close_price)

        return candles
    return _create


# =============================================================================
# Data Fixtures - Signals
# =============================================================================

@pytest.fixture
def sample_long_signal():
    """Provides a sample LONG signal."""
    return Signal(
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        timeframe=DEFAULT_TIMEFRAME,
        timestamp=1704067200000,
        type=SignalType.LONG,
        value=Decimal("42000.00"),
        quantity=Decimal("0.1"),
        price=Decimal("42000.00")
    )


@pytest.fixture
def sample_short_signal():
    """Provides a sample SHORT signal."""
    return Signal(
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        timeframe=DEFAULT_TIMEFRAME,
        timestamp=1704067200000,
        type=SignalType.SHORT,
        value=Decimal("42000.00"),
        quantity=Decimal("0.1"),
        price=Decimal("42000.00")
    )


@pytest.fixture
def sample_exit_long_signal():
    """Provides a sample EXIT_LONG signal."""
    return Signal(
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        timeframe=DEFAULT_TIMEFRAME,
        timestamp=1704067200000,
        type=SignalType.EXIT_LONG,
        value=Decimal("42500.00")
    )


@pytest.fixture
def sample_exit_short_signal():
    """Provides a sample EXIT_SHORT signal."""
    return Signal(
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        timeframe=DEFAULT_TIMEFRAME,
        timestamp=1704067200000,
        type=SignalType.EXIT_SHORT,
        value=Decimal("41500.00")
    )


@pytest.fixture
def signal_factory():
    """Factory to create Signals with custom parameters."""
    def _create(
        strategy_id: str = DEFAULT_STRATEGY_ID,
        product_id: str = DEFAULT_PRODUCT_ID,
        timeframe: str = DEFAULT_TIMEFRAME,
        timestamp: int = 1704067200000,
        signal_type: SignalType = SignalType.LONG,
        value: Decimal = Decimal("42000.00"),
        quantity: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
        stop_loss: Optional[Decimal] = None,
        take_profit: Optional[Decimal] = None,
        trailing_distance: Optional[Decimal] = None,
        metadata: Optional[dict] = None
    ) -> Signal:
        return Signal(
            strategy_id=strategy_id,
            product_id=product_id,
            timeframe=timeframe,
            timestamp=timestamp,
            type=signal_type,
            value=value,
            quantity=quantity,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_distance=trailing_distance,
            metadata=metadata
        )
    return _create


# =============================================================================
# Data Fixtures - Orders
# =============================================================================

@pytest.fixture
def sample_order():
    """Provides a sample Order."""
    return Order(
        id=str(uuid.uuid4()),
        exchange_order_id="",
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        exchange_id="BINANCE",
        type="market",
        side="buy",
        price=Decimal("42000.00"),
        quantity=Decimal("0.1"),
        status="open",
        timestamp=1704067200000,
        filled_quantity=Decimal("0"),
        filled_price=Decimal("0")
    )


@pytest.fixture
def order_factory():
    """Factory to create Orders with custom parameters."""
    def _create(
        order_id: str = None,
        exchange_order_id: str = "",
        strategy_id: str = DEFAULT_STRATEGY_ID,
        product_id: str = DEFAULT_PRODUCT_ID,
        exchange_id: str = "BINANCE",
        order_type: str = "market",
        side: str = "buy",
        price: Decimal = Decimal("42000.00"),
        quantity: Decimal = Decimal("0.1"),
        status: str = "open",
        timestamp: int = 1704067200000,
        filled_quantity: Decimal = Decimal("0"),
        filled_price: Decimal = Decimal("0"),
        trigger_price: Optional[Decimal] = None,
    ) -> Order:
        return Order(
            id=order_id or str(uuid.uuid4()),
            exchange_order_id=exchange_order_id,
            strategy_id=strategy_id,
            product_id=product_id,
            exchange_id=exchange_id,
            type=order_type,
            side=side,
            price=price,
            trigger_price=trigger_price,
            quantity=quantity,
            status=status,
            timestamp=timestamp,
            filled_quantity=filled_quantity,
            filled_price=filled_price
        )
    return _create


# =============================================================================
# Data Fixtures - Positions
# =============================================================================

@pytest.fixture
def sample_long_position():
    """Provides a sample LONG Position."""
    return Position(
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        side="LONG",
        quantity=Decimal("0.5"),
        entry_price=Decimal("42000.00"),
        unrealized_pnl=Decimal("0")
    )


@pytest.fixture
def sample_short_position():
    """Provides a sample SHORT Position."""
    return Position(
        strategy_id=DEFAULT_STRATEGY_ID,
        product_id=DEFAULT_PRODUCT_ID,
        side="SHORT",
        quantity=Decimal("0.5"),
        entry_price=Decimal("42000.00"),
        unrealized_pnl=Decimal("0")
    )


@pytest.fixture
def position_factory():
    """Factory to create Positions with custom parameters."""
    def _create(
        strategy_id: str = DEFAULT_STRATEGY_ID,
        product_id: str = DEFAULT_PRODUCT_ID,
        side: str = "LONG",
        quantity: Decimal = Decimal("0.5"),
        entry_price: Decimal = Decimal("42000.00"),
        unrealized_pnl: Decimal = Decimal("0")
    ) -> Position:
        return Position(
            strategy_id=strategy_id,
            product_id=product_id,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl
        )
    return _create


# =============================================================================
# Data Fixtures - Trades
# =============================================================================

@pytest.fixture
def sample_trade():
    """Provides a sample Trade."""
    return Trade(
        id=str(uuid.uuid4()),
        product_id=DEFAULT_PRODUCT_ID,
        price=Decimal("42000.00"),
        quantity=Decimal("0.1"),
        side="buy",
        timestamp=1704067200000
    )


@pytest.fixture
def trade_factory():
    """Factory to create Trades with custom parameters."""
    def _create(
        trade_id: str = None,
        product_id: str = DEFAULT_PRODUCT_ID,
        price: Decimal = Decimal("42000.00"),
        quantity: Decimal = Decimal("0.1"),
        side: str = "buy",
        timestamp: int = 1704067200000
    ) -> Trade:
        return Trade(
            id=trade_id or str(uuid.uuid4()),
            product_id=product_id,
            price=price,
            quantity=quantity,
            side=side,
            timestamp=timestamp
        )
    return _create


# =============================================================================
# Integration Fixtures
# =============================================================================

@pytest.fixture
def risk_manager(mock_account_service):
    """Provides a RiskManager with mock account service."""
    from src.core.risk_manager import RiskManager
    return RiskManager(mock_account_service)


@pytest.fixture
def order_manager(mock_order_repo, mock_clock):
    """Provides an OrderManager with mock repo and clock."""
    from src.core.order_manager import OrderManager
    return OrderManager(mock_order_repo, mock_clock)


@pytest.fixture
def simulated_adapter():
    """Provides a SimulatedAdapter backed by Rust PyMatchingEngine."""
    from src.core.adapters.simulated import SimulatedAdapter
    return SimulatedAdapter(
        initial_balance=DEFAULT_BALANCE,
        maker_fee=0.0002,
        taker_fee=0.0006,
    )


@pytest.fixture
def backtest_order_repo(mock_db_session):
    """Provides a BacktestOrderRepository."""
    from src.core.repositories import BacktestOrderRepository
    return BacktestOrderRepository(
        db_session=mock_db_session,
        session_id=1,
        initial_balance=DEFAULT_BALANCE
    )
