from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional
from src.core.orm_models import Order
from src.core.models import Candlestick, Position

class ExchangeError(Exception):
    """Base exception for all exchange related errors."""
    pass

class InsufficientFundsError(ExchangeError):
    """Raised when the account has insufficient funds for the order."""
    pass

class NetworkError(ExchangeError):
    """Raised when there is a network connectivity issue with the exchange."""
    pass

class ExchangeOrderLookupUnsupported(ExchangeError):
    """Raised when an adapter cannot query orders by client order ID."""
    pass

class ExchangeUserStreamUnsupported(ExchangeError):
    """Raised when an adapter cannot manage exchange user-data streams."""
    pass

@dataclass(frozen=True)
class ExchangeOrderSnapshot:
    """Adapter-neutral view of an exchange order used for recovery checks."""

    client_order_id: str
    exchange_order_id: Optional[str]
    status: str
    filled_quantity: Optional[Decimal] = None
    average_price: Optional[Decimal] = None
    fee: Optional[Decimal] = None
    raw: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class ExchangeOrderEvent:
    """Adapter-neutral live order event from user-data streams or polling."""

    status: str
    product_id: str
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    cumulative_filled_quantity: Optional[Decimal] = None
    cumulative_average_price: Optional[Decimal] = None
    last_fill_quantity: Optional[Decimal] = None
    last_fill_price: Optional[Decimal] = None
    fee: Optional[Decimal] = None
    fee_asset: Optional[str] = None
    event_timestamp: Optional[int] = None
    reason: Optional[str] = None
    raw: Optional[dict[str, Any]] = None


class IExchangeAdapter(ABC):
    """
    Interface for exchange adapters (Real and Simulated).
    Decouples execution logic from specific exchange implementations.
    """

    @abstractmethod
    def place_order(self, order: Order) -> str:
        """
        Places an order on the exchange.
        
        Args:
            order: The internal Order object (ORM model) containing all details.
            
        Returns:
            str: The exchange's order ID.
            
        Raises:
            ExchangeError: If the order fails.
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str, product_id: str) -> bool:
        """
        Cancels an existing order.
        
        Args:
            order_id: The exchange's order ID (not internal DB ID).
            product_id: The product/symbol identifier (e.g., BINANCE:BTCUSDT-PERP).
            
        Returns:
            bool: True if cancellation was successful, False otherwise.
        """
        pass

    def cancel_order_by_client_id(self, client_order_id: str, product_id: str) -> bool:
        """Cancel an existing order using the exchange client order ID.

        Adapters that do not have native client-order-id support can fall back
        to treating the value as an exchange order ID. Execution code should
        still keep the old exchange-id fallback while this capability is being
        rolled out across adapters.
        """
        return self.cancel_order(client_order_id, product_id)

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
    ) -> Optional[ExchangeOrderSnapshot]:
        """Return an exchange order snapshot by client order ID if supported."""
        raise ExchangeOrderLookupUnsupported(
            f"{type(self).__name__} does not support client order lookup"
        )

    def create_user_stream_listen_key(self) -> str:
        """Create a user-data stream listen key when supported."""
        raise ExchangeUserStreamUnsupported(
            f"{type(self).__name__} does not support user stream listen keys"
        )

    def keepalive_user_stream(self, listen_key: str) -> None:
        """Refresh an existing user-data stream listen key when supported."""
        raise ExchangeUserStreamUnsupported(
            f"{type(self).__name__} does not support user stream listen keys"
        )

    @abstractmethod
    def get_balance(self, asset: str) -> Decimal:
        """
        Retrieves the available balance for a specific asset.
        
        Args:
            asset: The asset symbol (e.g., USDT, BTC).
            
        Returns:
            Decimal: The available balance.
        """
        pass

    @abstractmethod
    def get_position(self, product_id: str) -> Optional[Position]:
        """
        Retrieves the current open position for a product.

        Args:
            product_id: The product/symbol identifier.

        Returns:
            Optional[Position]: The position details (Pydantic model) or None if no position.
        """
        pass

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        """
        Processes market data to check for simulated order fills.

        Override in simulated adapters to implement matching logic.
        Live adapters return empty list (exchange manages SL/TP).

        Args:
            candle: The latest candlestick data.

        Returns:
            List of fill dicts with keys: order, price, quantity, fee, fill_type.
        """
        return []
