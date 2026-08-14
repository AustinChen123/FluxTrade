"""Entrypoint-only adapter configuration and runtime capability composition."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from src.core.product_registry import to_base_quote
from src.core.runtime_capabilities import (
    RuntimeBootstrapFactory,
    RuntimeCapabilitiesFactory,
)


def build_live_adapter_config(
    *,
    exchange: str,
    product_ids: list[str],
    environ: Mapping[str, str],
    read_enable_ws: Callable[[], bool],
) -> dict[str, Any]:
    """Delegate one live adapter config to its existing venue owner."""
    balance_asset: str | None = None
    if exchange != "rithmic":
        quote_assets = {to_base_quote(product_id)[1] for product_id in product_ids}
        if len(quote_assets) != 1 or not all(quote_assets):
            raise ValueError("live CCXT products require one common balance asset")
        balance_asset = quote_assets.pop()
    if exchange == "backpack":
        from src.core.adapters.backpack_live_config import (
            build_backpack_live_adapter_config,
        )

        config = build_backpack_live_adapter_config(
            product_ids=product_ids,
            environ=environ,
        )
        config["balance_asset"] = balance_asset
        return config
    if exchange == "binance":
        from src.core.adapters.binance_live_config import (
            build_binance_live_adapter_config,
        )

        config = build_binance_live_adapter_config(
            product_ids=product_ids,
            environ=environ,
        )
        config["balance_asset"] = balance_asset
        return config
    if exchange == "bybit":
        from src.core.adapters.bybit_live_config import build_bybit_live_adapter_config

        config = build_bybit_live_adapter_config(
            product_ids=product_ids,
            environ=environ,
        )
        config["balance_asset"] = balance_asset
        return config
    if exchange == "okx":
        from src.core.adapters.okx_live_config import build_okx_live_adapter_config

        config = build_okx_live_adapter_config(
            product_ids=product_ids,
            environ=environ,
        )
        config["balance_asset"] = balance_asset
        return config

    account_initialization = {
        "product_ids": product_ids,
        "position_mode": environ.get("ACCOUNT_POSITION_MODE", "one_way"),
    }
    leverage = environ.get("ACCOUNT_LEVERAGE")
    if leverage:
        account_initialization["leverage"] = leverage
    margin_mode = environ.get("ACCOUNT_MARGIN_MODE")
    if margin_mode:
        account_initialization["margin_mode"] = margin_mode
    config = {
        "mode": "live",
        "exchange": exchange,
        "enable_ws": read_enable_ws(),
        "instrument_product_ids": product_ids,
        "account_initialization": account_initialization,
    }
    if exchange != "rithmic":
        from src.core.adapters.ccxt_live_credentials import (
            build_ccxt_live_credentials,
        )

        config.update(build_ccxt_live_credentials(environ))
        config["balance_asset"] = balance_asset
        return config

    from src.core.adapters.rithmic_live_config import (
        build_rithmic_live_adapter_config,
    )

    config.update(
        build_rithmic_live_adapter_config(
            product_ids=product_ids,
            environ=environ,
        )
    )
    return config


def validate_runtime_config(
    adapter_config: Mapping[str, object],
    *,
    audit_external_orders: bool,
) -> None:
    """Validate shared live safety before provider recovery identity."""
    if adapter_config.get("mode") == "live" and not audit_external_orders:
        raise ValueError(
            "live_adapter_requires_audit_external_orders: "
            "set AUDIT_EXTERNAL_ORDERS=true for live trading"
        )
    from src.core.adapters.rithmic_live_config import (
        validate_rithmic_recovery_identity,
    )

    validate_rithmic_recovery_identity(adapter_config)
    if (
        adapter_config.get("mode") == "live"
        and adapter_config.get("exchange") != "rithmic"
        and (
            type(adapter_config.get("balance_asset")) is not str
            or not adapter_config.get("balance_asset")
        )
    ):
        raise ValueError("live CCXT config requires one common balance asset")


def runtime_factories_for_config(
    adapter_config: Mapping[str, object],
) -> tuple[RuntimeBootstrapFactory | None, RuntimeCapabilitiesFactory | None]:
    """Return exact venue runtime factories for Engine composition."""
    if adapter_config.get("exchange") != "rithmic":
        return None, None
    from src.core.adapters.rithmic_runtime_composition import (
        build_rithmic_runtime_owners,
        prepare_rithmic_runtime_bootstrap,
    )

    return prepare_rithmic_runtime_bootstrap, build_rithmic_runtime_owners
