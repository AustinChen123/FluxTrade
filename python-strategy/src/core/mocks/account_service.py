from decimal import Decimal
from typing import Optional
from src.core.risk_manager import AccountService
from src.core.models import Position
from src.core.interfaces import IOrderRepository
from src.core.interfaces.exchange import IExchangeAdapter


class BacktestAccountService(AccountService):
    """Account service for backtest: reads balance/position from the adapter.

    When an ``adapter`` is provided (Rust-backed SimulatedAdapter), balance
    and position are read directly from it — the adapter is the single source
    of truth.  Falls back to repo-based tracking when no adapter is given.
    """

    def __init__(
        self,
        adapter: Optional[IExchangeAdapter] = None,
        repo: Optional[IOrderRepository] = None,
        initial_balance: Decimal = Decimal("10000"),
    ):
        self.adapter = adapter
        self.repo = repo
        if repo and not adapter:
            if hasattr(repo, "balance"):
                repo.balance = initial_balance

    def get_balance(self, asset: str = "USDT") -> Decimal:
        if self.adapter:
            return self.adapter.get_balance(asset)
        if self.repo and hasattr(self.repo, "balance"):
            return self.repo.balance
        return Decimal("0")

    def update_balance(self, amount: Decimal):
        if self.repo and hasattr(self.repo, "balance"):
            self.repo.balance += amount

    def get_position(self, strategy_id: str, product_id: str) -> Optional[Position]:
        if self.adapter:
            pos = self.adapter.get_position(product_id)
            if pos:
                pos.strategy_id = strategy_id
            return pos
        if self.repo:
            return self.repo.get_position(strategy_id, product_id)
        return None
