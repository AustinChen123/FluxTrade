"""Binance-owned live environment configuration policy."""

from collections.abc import Mapping

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


def build_binance_live_adapter_config(
    *, product_ids: list[str], environ: Mapping[str, str]
) -> dict[str, object]:
    enable_ws = _optional_flag(environ, "EXCHANGE_ENABLE_WS")
    credentials = build_ccxt_live_credentials(environ)
    account_id = environ.get("BINANCE_ACCOUNT_ALIAS", "").strip()
    if not account_id:
        raise ValueError("BINANCE_ACCOUNT_ALIAS is required")
    account_initialization: dict[str, object] = {
        "product_ids": product_ids,
        "position_mode": environ.get("ACCOUNT_POSITION_MODE", "one_way"),
    }
    leverage = environ.get("ACCOUNT_LEVERAGE")
    if leverage:
        account_initialization["leverage"] = leverage
    margin_mode = environ.get("ACCOUNT_MARGIN_MODE")
    if margin_mode:
        account_initialization["margin_mode"] = margin_mode

    config: dict[str, object] = {
        "mode": "live",
        "exchange": "binance",
        "account_profile": (
            "ccxt:binance:testnet" if credentials["testnet"] else "ccxt:binance:live"
        ),
        "account_id": account_id,
        "enable_ws": enable_ws,
        "instrument_product_ids": product_ids,
        "account_initialization": account_initialization,
    }
    config.update(credentials)
    return config
