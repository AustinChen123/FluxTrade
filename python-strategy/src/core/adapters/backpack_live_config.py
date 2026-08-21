from collections.abc import Mapping
from typing import cast

from src.core.adapters.ccxt_live_credentials import build_ccxt_live_credentials


def _optional_flag(
    environ: Mapping[str, str], name: str, *, default: bool = False
) -> bool:
    value = environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def build_backpack_live_adapter_config(
    *, product_ids: list[str], environ: Mapping[str, str]
) -> dict[str, object]:
    credentials = build_ccxt_live_credentials(environ)
    if credentials["testnet"] is not False:
        raise ValueError("backpack_testnet_unsupported")
    if _optional_flag(environ, "EXCHANGE_ENABLE_WS"):
        raise ValueError("backpack_websocket_order_entry_unsupported")

    for name in (
        "ACCOUNT_POSITION_MODE",
        "ACCOUNT_LEVERAGE",
        "ACCOUNT_MARGIN_MODE",
    ):
        if (environ.get(name) or "").strip():
            raise ValueError(f"backpack_account_initialization_unsupported: {name}")

    return {
        "mode": "live",
        "exchange": "backpack",
        "api_key": cast(str, credentials["api_key"]),
        "secret": cast(str, credentials["secret"]),
        "testnet": False,
        "instrument_product_ids": product_ids,
    }
