from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter


def create_okx_live_adapter(
    *,
    api_key: str | None = None,
    secret: str | None = None,
    testnet: bool = False,
    extra_config: dict | None = None,
) -> CcxtExchangeAdapter:
    """Construct the configured OKX adapter without owning its lifecycle."""
    return CcxtExchangeAdapter(
        exchange_id="okx",
        api_key=api_key,
        secret=secret,
        testnet=testnet,
        extra_config=extra_config,
    )
