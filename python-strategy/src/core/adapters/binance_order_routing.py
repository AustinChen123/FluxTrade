"""Pure Binance conditional-order and client-ID routing policy."""

from decimal import Decimal

from src.core.interfaces.exchange import ExchangeError


_ALGO_ORDER_TYPES = frozenset({"stop_loss", "take_profit"})


def uses_binance_algo_order_endpoints(
    exchange_id: str,
    order_type: str | None,
) -> bool:
    return exchange_id == "binance" and (order_type or "").lower() in _ALGO_ORDER_TYPES


def binance_conditional_order_mapping(
    exchange_id: str,
    order_type: str | None,
    trigger_price: Decimal | None,
) -> tuple[str, dict] | None:
    normalized = (order_type or "").lower()
    if normalized not in _ALGO_ORDER_TYPES:
        return None
    if exchange_id != "binance":
        raise ExchangeError(
            f"conditional_order_mapping_unsupported: exchange={exchange_id}"
        )
    if trigger_price is None:
        raise ExchangeError(f"{normalized}_requires_trigger_price")
    if normalized == "stop_loss":
        return "STOP_MARKET", {
            "stopLossPrice": str(trigger_price),
            "reduceOnly": True,
        }
    return "TAKE_PROFIT_MARKET", {
        "takeProfitPrice": str(trigger_price),
        "reduceOnly": True,
    }


def binance_submission_client_order_id_params(
    exchange_id: str,
    order_type: str | None,
    exchange_client_order_id: str,
) -> dict | None:
    if exchange_id != "binance":
        return None
    key = (
        "clientAlgoId"
        if uses_binance_algo_order_endpoints(exchange_id, order_type)
        else "newClientOrderId"
    )
    return {key: exchange_client_order_id}


def binance_lookup_client_order_id_params(
    exchange_id: str,
    order_type: str | None,
    exchange_client_order_id: str,
) -> dict | None:
    if exchange_id != "binance":
        return None
    if uses_binance_algo_order_endpoints(exchange_id, order_type):
        return {"clientAlgoId": exchange_client_order_id, "trigger": True}
    return {"origClientOrderId": exchange_client_order_id}
