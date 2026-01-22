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

    def check_risk(self, signal: Signal) -> tuple[bool, str]:
        """
        Evaluates the signal against risk rules.
        Returns (True, "PASS") if safe to proceed, (False, reason) otherwise.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return True, "NO_SIGNAL"

        # Rule 1: Zero Balance Protection
        is_entry = signal.type in [SignalType.LONG, SignalType.SHORT]
        balance = self.account_service.get_balance()
        
        if is_entry and balance <= 0:
            msg = f"REJECT: Account balance is {balance} (<= 0)"
            print(f"🛑 RISK {msg}. Signal {signal.type} rejected.")
            return False, msg

        # Rule 2: Max Exposure Check
        position = self.account_service.get_position(signal.strategy_id, signal.product_id)
        if position:
            current_exposure = position.quantity * position.entry_price
            if is_entry and current_exposure >= self.max_exposure_per_product:
                 msg = f"REJECT: Max exposure reached ({current_exposure} >= {self.max_exposure_per_product})"
                 print(f"🛑 RISK {msg}.")
                 return False, msg

        return True, "PASS"
