import logging
from decimal import Decimal
from typing import Optional
from src.core.models import Signal, SignalType, Position
from src.core.redis_factory import create_redis_client

logger = logging.getLogger(__name__)

class AccountService:
    """Interface for accessing account data via Redis."""
    def __init__(self):
        try:
            self.redis = create_redis_client()
            self.redis.ping() # Check connection
        except Exception as e:
            logger.warning("AccountService: Redis connection failed: %s", e)
            self.redis = None

    def get_balance(self) -> Decimal:
        if not self.redis:
            return Decimal("0")
        
        # Assuming single account 'main' for now as per Lua script usage
        balance = self.redis.hget("state:balance:main", "free")
        return Decimal(balance) if balance else Decimal("0")

    def get_position(self, strategy_id: str, product_id: str) -> Optional[Position]:
        if not self.redis:
            return None

        key = f"state:position:{strategy_id}:{product_id}"
        data = self.redis.hgetall(key)
        
        if not data:
            return None

        qty_str = data.get("quantity", "0")
        qty = Decimal(qty_str)
        
        if qty == 0:
            return None

        # Determine Side & Abs Quantity
        if qty > 0:
            side = "LONG"
            abs_qty = qty
        else:
            side = "SHORT"
            abs_qty = abs(qty)

        # Entry Price (tracked by Lua or external updater)
        entry_price = Decimal(data.get("entry_price", "0"))
        
        # Unrealized PnL (Not strictly tracked in Redis Hash yet, placeholder)
        unrealized_pnl = Decimal("0")

        return Position(
            strategy_id=strategy_id,
            product_id=product_id,
            side=side,
            quantity=abs_qty,
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl
        )

class RiskManager:
    def __init__(self, account_service: AccountService):
        self.account_service = account_service
        self.max_exposure_per_product = Decimal("50000.0")

    def check_risk(self, signal: Signal, current_price: Optional[Decimal] = None) -> tuple[bool, str]:
        """
        Evaluates the signal against risk rules.
        Returns (True, "PASS") if safe to proceed, (False, reason) otherwise.

        Args:
            signal: The trading signal to evaluate.
            current_price: Current market price for exposure calculation.
                           Falls back to entry_price if not provided.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return True, "NO_SIGNAL"

        # Rule 1: Zero Balance Protection
        is_entry = signal.type in [SignalType.LONG, SignalType.SHORT]
        balance = self.account_service.get_balance()

        if is_entry and balance <= 0:
            msg = f"REJECT: Account balance is {balance} (<= 0)"
            logger.warning("RISK_REJECTED: %s signal_type=%s", msg, signal.type)
            return False, msg

        # Rule 2: Max Exposure Check (uses current market price)
        position = self.account_service.get_position(signal.strategy_id, signal.product_id)
        if position:
            price_for_exposure = current_price if current_price is not None else position.entry_price
            current_exposure = position.quantity * price_for_exposure
            if is_entry and current_exposure >= self.max_exposure_per_product:
                 msg = f"REJECT: Max exposure reached ({current_exposure} >= {self.max_exposure_per_product})"
                 logger.warning("RISK_REJECTED: %s", msg)
                 return False, msg

        return True, "PASS"

    def calculate_position_size(self, entry_price: Decimal, stop_loss_price: Decimal, risk_percent: float = 0.02) -> Decimal:
        """
        Calculates position size based on risk percentage and stop loss distance.
        Risk = |Entry - SL| * Size
        Size = Risk / |Entry - SL|
        """
        balance = self.account_service.get_balance()
        if balance <= 0:
            return Decimal("0")

        risk_amount = balance * Decimal(str(risk_percent))
        price_diff = abs(entry_price - stop_loss_price)

        if price_diff == 0:
            return Decimal("0")

        size = risk_amount / price_diff
        
        # Optional: Add rounding logic here based on exchange precision if known
        # For now, return raw Decimal
        return round(size, 4)
