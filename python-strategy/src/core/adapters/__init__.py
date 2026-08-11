"""Exchange adapter implementations.

Factory function ``create_adapter`` provides config-driven instantiation.
"""

from collections.abc import Callable

from src.core.adapters.ccxt_adapter import (
    AccountInitializationConfig,
    CcxtExchangeAdapter,
)
from src.core.adapters.live_binance import (
    LiveBinanceAdapter,
    create_binance_live_adapter,
)
from src.core.adapters.rithmic_adapter import (
    RithmicExchangeAdapter,
    RithmicUnmappedOrderEvent,
)
from src.core.adapters.simulated import SimulatedAdapter
from src.core.interfaces.exchange import IExchangeAdapter

__all__ = [
    "AccountInitializationConfig",
    "CcxtExchangeAdapter",
    "LiveBinanceAdapter",
    "RithmicExchangeAdapter",
    "RithmicUnmappedOrderEvent",
    "SimulatedAdapter",
    "create_adapter",
]


def create_adapter(
    config: dict,
    *,
    operation_guard: Callable[[], None] | None = None,
) -> IExchangeAdapter:
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
        account_initialization: dict (live account settings, optional)
    """
    from decimal import Decimal

    mode = config.get("mode", "simulated")

    if mode == "simulated":
        balance = Decimal(str(config.get("balance", 100000)))
        maker_fee = Decimal(str(config.get("maker_fee", 0)))
        taker_fee = Decimal(str(config.get("taker_fee", 0)))
        return SimulatedAdapter(
            initial_balance=balance,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
        )

    exchange_id = config.get("exchange", "binance")
    guard = operation_guard or (lambda: None)
    if str(exchange_id).lower() == "rithmic":
        guard()
        adapter = RithmicExchangeAdapter.from_config(config)
        try:
            guard()
        except Exception:
            adapter.close()
            raise
        return adapter
    api_key = config.get("api_key")
    secret = config.get("secret")
    testnet = config.get("testnet", True)
    enable_ws = config.get("enable_ws", False)
    extra_config = config.get("extra_config")
    instrument_product_ids = config.get("instrument_product_ids") or []
    account_initialization = AccountInitializationConfig.from_config(
        config.get("account_initialization"),
        default_product_ids=instrument_product_ids,
    )

    # Use Binance-specific adapter if WS requested and exchange is binance
    adapter = None
    try:
        guard()
        if exchange_id == "binance":
            adapter = create_binance_live_adapter(
                api_key=api_key,
                secret=secret,
                testnet=testnet,
                enable_ws=enable_ws,
                extra_config=extra_config,
                operation_guard=guard,
            )
        else:
            adapter = CcxtExchangeAdapter(
                exchange_id=exchange_id,
                api_key=api_key,
                secret=secret,
                testnet=testnet,
                extra_config=extra_config,
            )
        guard()
        if account_initialization is not None:
            adapter.initialize_account(
                account_initialization,
                operation_guard=guard,
            )
        adapter.warm_instrument_specs(
            instrument_product_ids,
            operation_guard=guard,
        )
        return adapter
    except Exception:
        close = getattr(adapter, "close", None)
        if callable(close):
            close()
        raise
