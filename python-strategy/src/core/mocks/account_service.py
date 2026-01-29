from decimal import Decimal
from typing import Optional
from src.core.risk_manager import AccountService
from src.core.models import Position
from src.core.interfaces import IOrderRepository

class BacktestAccountService(AccountService):
    def __init__(self, repo: IOrderRepository, initial_balance: Decimal = Decimal("10000")):
        self.repo = repo
        # Ensure repo has balance attr (it should if it's BacktestOrderRepository)
        if hasattr(self.repo, 'balance'):
            self.repo.balance = initial_balance

    def get_balance(self, asset: str = "USDT") -> Decimal:
        if hasattr(self.repo, 'balance'):
            return self.repo.balance
        return Decimal("0")
    
    def update_balance(self, amount: Decimal):
        if hasattr(self.repo, 'balance'):
            self.repo.balance += amount

    def get_position(self, strategy_id: str, product_id: str) -> Optional[Position]:
        # Repo now handles netting and returns the single active position
        return self.repo.get_position(strategy_id, product_id)
