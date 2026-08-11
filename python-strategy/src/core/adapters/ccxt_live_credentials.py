from collections.abc import Mapping


def _required_value(environ: Mapping[str, str], name: str) -> str:
    value = (environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} must be set explicitly")
    return value


def _required_flag(environ: Mapping[str, str], name: str) -> bool:
    normalized = _required_value(environ, name).lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def build_ccxt_live_credentials(
    environ: Mapping[str, str],
) -> dict[str, str | bool]:
    return {
        "api_key": _required_value(environ, "EXCHANGE_API_KEY"),
        "secret": _required_value(environ, "EXCHANGE_SECRET"),
        "testnet": _required_flag(environ, "EXCHANGE_TESTNET"),
    }
