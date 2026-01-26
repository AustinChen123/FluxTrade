from decimal import Decimal
from src.core.models import Signal, SignalType
from src.core.risk_manager import RiskManager, AccountService

class MockAccountService(AccountService):
    def __init__(self, balance=10000.0):
        self.balance = Decimal(str(balance))
    
    def get_balance(self) -> Decimal:
        return self.balance
    
    def get_position(self, strategy_id, product_id):
        return None

def test_risk_rejection_on_zero_balance():
    # Setup: Balance = 0
    account_service = MockAccountService(balance=0.0)
    risk_manager = RiskManager(account_service)
    
    # Action: LONG signal
    signal = Signal(
        strategy_id="test",
        product_id="BTC",
        timeframe="1m",
        timestamp=1000,
        type=SignalType.LONG,
        value=Decimal("50000")
    )
    
    # Assert: Should be rejected
    assert risk_manager.check_risk(signal) is False

def test_risk_allow_exit_on_zero_balance():
    # Setup: Balance = 0
    account_service = MockAccountService(balance=0.0)
    risk_manager = RiskManager(account_service)
    
    # Action: EXIT signal
    signal = Signal(
        strategy_id="test",
        product_id="BTC",
        timeframe="1m",
        timestamp=1000,
        type=SignalType.EXIT_LONG,
        value=Decimal("50000")
    )
    
    # Assert: Should be accepted (to allow stop loss)
    assert risk_manager.check_risk(signal) is True
