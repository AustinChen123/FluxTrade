"""Exchange adapter implementations.

Factory function ``create_adapter`` provides config-driven instantiation.
"""

from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.live_binance import LiveBinanceAdapter
from src.core.adapters.simulated import SimulatedAdapter
from src.core.interfaces.exchange import IExchangeAdapter

__all__ = [
    "CcxtExchangeAdapter",
    "LiveBinanceAdapter",
    "SimulatedAdapter",
    "create_adapter",
]


def create_adapter(config: dict) -> IExchangeAdapter:
    """Create an exchange adapter from a configuration dict.

    Config keys:
        mode: "simulated" | "live"  (default: "simulated")
        exchange: CCXT exchange id  (required for live, default: "binance")
        api_key: API key            (optional, falls back to env)
        secret: API secret          (optional, falls back to env)
        testnet: bool               (default: True)
        balance: initial balance    (simulated only, default: 100000)
        enable_ws: bool             (live only, default: False)
        extra_config: dict          (extra CCXT config, optional)
    """
    from decimal import Decimal

    mode = config.get("mode", "simulated")

    if mode == "simulated":
        balance = Decimal(str(config.get("balance", 100000)))
        maker_fee = float(config.get("maker_fee", 0.0))
        taker_fee = float(config.get("taker_fee", 0.0))
        return SimulatedAdapter(
            initial_balance=balance,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
        )

    exchange_id = config.get("exchange", "binance")
    api_key = config.get("api_key")
    secret = config.get("secret")
    testnet = config.get("testnet", True)
    enable_ws = config.get("enable_ws", False)
    extra_config = config.get("extra_config")

    # Use Binance-specific adapter if WS requested and exchange is binance
    if exchange_id == "binance" and enable_ws:
        return LiveBinanceAdapter(
            api_key=api_key,
            secret=secret,
            testnet=testnet,
            enable_ws=True,
        )

    return CcxtExchangeAdapter(
        exchange_id=exchange_id,
        api_key=api_key,
        secret=secret,
        testnet=testnet,
        extra_config=extra_config,
    )
