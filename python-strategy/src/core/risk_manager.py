from decimal import Decimal
from typing import Optional
from src.core.models import Signal, SignalType, Position

class AccountService:
    """Interface for accessing account data. Currently a Mock."""
    def get_balance(self) -> Decimal:
        # Mock: Return a positive balance by default
        return Decimal("10000.0")

    def get_position(self, strategy_id: str, product_id: str) -> Optional[Position]:
        # Mock: Return None (no position)
        return None

class RiskManager:
    def __init__(self, account_service: AccountService):
        self.account_service = account_service
        self.max_exposure_per_product = Decimal("50000.0") # USDT

    def check_risk(self, signal: Signal) -> bool:
        """
        Evaluates the signal against risk rules.
        Returns True if safe to proceed, False otherwise.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return True

        # Rule 1: Zero Balance Protection
        # Allow EXIT signals even if balance is zero (to close positions)
        is_entry = signal.type in [SignalType.LONG, SignalType.SHORT]
        balance = self.account_service.get_balance()
        
        if is_entry and balance <= 0:
            print(f"🛑 RISK REJECT: Account balance is {balance} (<= 0). Signal {signal.type} rejected.")
            return False

        # Rule 2: Max Exposure Check (Simplified)
        # In a real system, we'd calculate: current_exposure + new_order_value > max_exposure
        # Here we just check if we already have a huge position (mock logic)
        position = self.account_service.get_position(signal.strategy_id, signal.product_id)
        if position:
            current_exposure = position.quantity * position.entry_price
            if is_entry and current_exposure >= self.max_exposure_per_product:
                 print(f"🛑 RISK REJECT: Max exposure reached ({current_exposure} >= {self.max_exposure_per_product}).")
                 return False

        return True
