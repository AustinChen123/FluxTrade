import random
from decimal import Decimal

class SlippageModel:
    def __init__(self, slip_pct: float = 0.001):
        self.slip_pct = slip_pct

    def calculate_slippage(self, price: Decimal) -> Decimal:
        """
        Apply random slippage to the price.
        Returns the new price.
        """
        # Convert slip_pct to Decimal
        deviation = price * Decimal(str(self.slip_pct))
        # Random factor between -1 and 1
        factor = Decimal(str(random.uniform(-1, 1)))
        slippage = factor * deviation
        return price + slippage
