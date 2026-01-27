from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional
from src.core.orm_models import Order
from src.core.models import Position

class ExchangeError(Exception):
    """Base exception for all exchange related errors."""
    pass

class InsufficientFundsError(ExchangeError):
    """Raised when the account has insufficient funds for the order."""
    pass

class NetworkError(ExchangeError):
    """Raised when there is a network connectivity issue with the exchange."""
    pass

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
