"""Exchange adapter implementations.

Factory function ``create_adapter`` provides config-driven instantiation.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from src.core.interfaces.exchange import IExchangeAdapter

if TYPE_CHECKING:
    from src.core.adapters.ccxt_account_initialization import (
        AccountInitializationConfig,
    )
    from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
    from src.core.adapters.live_binance import LiveBinanceAdapter
    from src.core.adapters.rithmic_adapter import (
        RithmicExchangeAdapter,
        RithmicUnmappedOrderEvent,
    )
    from src.core.adapters.simulated import SimulatedAdapter

__all__ = [
    "AccountInitializationConfig",
    "CcxtExchangeAdapter",
    "LiveBinanceAdapter",
    "RithmicExchangeAdapter",
    "RithmicUnmappedOrderEvent",
    "SimulatedAdapter",
    "create_adapter",
]


def __getattr__(name: str) -> Any:
    """Resolve compatibility exports without loading every venue provider."""
    if name == "AccountInitializationConfig":
        from src.core.adapters.ccxt_account_initialization import (
            AccountInitializationConfig as value,
        )
    elif name == "CcxtExchangeAdapter":
        from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter as value
    elif name == "LiveBinanceAdapter":
        from src.core.adapters import live_binance

        value = getattr(live_binance, name)
    elif name in {"RithmicExchangeAdapter", "RithmicUnmappedOrderEvent"}:
        from src.core.adapters import rithmic_adapter

        value = getattr(rithmic_adapter, name)
    elif name == "SimulatedAdapter":
        from src.core.adapters import simulated

        value = getattr(simulated, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def _adapter_dependency(name: str) -> Any:
    if name in globals():
        return globals()[name]
    return __getattr__(name)


def create_simulated_adapter(config: dict):
    from src.core.adapters.simulated import create_simulated_adapter as owner

    return owner(config)


def create_binance_live_adapter(**kwargs):
    from src.core.adapters.live_binance import create_binance_live_adapter as owner

    return owner(**kwargs)


def create_backpack_live_adapter(**kwargs):
    from src.core.adapters.live_backpack import create_backpack_live_adapter as owner

    return owner(**kwargs)


def create_bybit_live_adapter(**kwargs):
    from src.core.adapters.live_bybit import create_bybit_live_adapter as owner

    return owner(**kwargs)


def create_okx_live_adapter(**kwargs):
    from src.core.adapters.live_okx import create_okx_live_adapter as owner

    return owner(**kwargs)


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
    mode = config.get("mode", "simulated")

    if mode == "simulated":
        return create_simulated_adapter(config)

    exchange_id = config.get("exchange", "binance")
    guard = operation_guard or (lambda: None)
    if str(exchange_id).lower() == "rithmic":
        guard()
        adapter = _adapter_dependency("RithmicExchangeAdapter").from_config(config)
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
    account_initialization = _adapter_dependency(
        "AccountInitializationConfig"
    ).from_config(
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
        elif exchange_id == "backpack":
            adapter = create_backpack_live_adapter(
                api_key=api_key,
                secret=secret,
                testnet=testnet,
                extra_config=extra_config,
            )
        elif exchange_id == "bybit":
            adapter = create_bybit_live_adapter(
                api_key=api_key,
                secret=secret,
                testnet=testnet,
                extra_config=extra_config,
            )
        elif exchange_id == "okx":
            adapter = create_okx_live_adapter(
                api_key=api_key,
                secret=secret,
                testnet=testnet,
                extra_config=extra_config,
            )
        else:
            adapter = _adapter_dependency("CcxtExchangeAdapter")(
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
