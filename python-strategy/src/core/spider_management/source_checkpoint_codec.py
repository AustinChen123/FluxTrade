"""Private canonical encoder for the existing Spider source checkpoint V1."""

from decimal import Decimal
from typing import Any, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_policy import DEFAULTS


class ModelBootstrapUnavailable(ValueError):
    """The admitted model evidence cannot support this closed bootstrap."""


_CHECKPOINT_PARAMETER_KEYS = frozenset(DEFAULTS)
_ROW_KEYS = ("name", "active", "leverage", "歩差", "單數", "hold上限", "hold下限", "hold")
_MARKET_KEYS = (
    "price", "ctVal", "lotSz", "minSz", "increment", "ratioHL", "state", "instIdCode"
)
_CAPITAL_KEYS = frozenset({"earn", "usdt", "avail", "total", "position", "day_ago"})
_REPLY_KEYS = frozenset(
    {"instId", "side", "px", "sz", "clOrdId", "state", "accFillSz", "cTime", "sMsg"}
)


def _scalar(value: object) -> dict[str, object]:
    if value is None:
        return {"tag": "NULL"}
    if type(value) is str:
        return {"tag": "STRING", "value": value}
    if type(value) is Decimal:
        if not value.is_finite():
            raise ModelBootstrapUnavailable("model_source_unavailable")
        from src.core.decimal_math import canonical_decimal_text

        return {"tag": "DECIMAL", "value": canonical_decimal_text(value)}
    if type(value) is int and -(2**63) <= value < 2**63:
        return {"tag": "INT64", "value": value}
    if type(value) is bool:
        return {"tag": "BOOL", "value": value}
    raise ModelBootstrapUnavailable("model_source_unavailable")


def _object_values(value: object, allowed: frozenset[str]) -> list[dict[str, object]]:
    if type(value) is not dict:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    row = cast(dict[object, object], value)
    if any(type(key) is not str or key not in allowed for key in row):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    return [{"key": key, "value": _scalar(item)} for key, item in row.items()]


def encode_source_checkpoint(
    policy: Any,
    *,
    ordered_products: tuple[str, ...],
    policy_id: str,
    policy_version: str,
    policy_source_sha256: str,
    source_sha256: str,
    confirmation_request_id: str,
    configuration_sha256: str,
    generation: int = 1,
) -> bytes:
    """Encode one already-initialized Policy without changing source state."""
    rows: list[dict[str, object]] = []
    if type(policy.rows) is not list or len(policy.rows) != len(ordered_products):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    for product, source in zip(ordered_products, policy.rows, strict=True):
        if type(source) is not dict or source.get("name") != product:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        if set(cast(dict[object, object], source)) != set(_ROW_KEYS) | {"product_id"}:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        rows.append({key: _scalar(source[key]) for key in _ROW_KEYS})

    markets: list[dict[str, object]] = []
    if type(policy.markets) is not dict or tuple(policy.markets) != ordered_products:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    for product in ordered_products:
        market = policy.markets[product]
        if type(market) is not dict or set(cast(dict[object, object], market)) != set(_MARKET_KEYS):
            raise ModelBootstrapUnavailable("model_source_unavailable")
        markets.append({
            "product_id": product,
            "value": {key: _scalar(market[key]) for key in _MARKET_KEYS},
        })

    parameters = policy.parameters
    if type(parameters) is not dict or set(parameters) != _CHECKPOINT_PARAMETER_KEYS:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    encoded_parameters = [
        {"key": key, "value": _scalar(parameters[key])} for key in DEFAULTS
    ]
    capital = policy.capital
    if type(capital) is not dict or not set(capital).issubset(_CAPITAL_KEYS):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    encoded_capital = _object_values(capital, _CAPITAL_KEYS)

    replies = policy.replies
    if type(replies) is not dict:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    encoded_replies: list[dict[str, object]] = []
    for alias, reply in replies.items():
        if type(alias) is not str:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        encoded_replies.append({"alias": alias, "value": _object_values(reply, _REPLY_KEYS)})

    for flag in ("running", "paused", "online", "ws_open"):
        if type(getattr(policy, flag)) is not bool:
            raise ModelBootstrapUnavailable("model_source_unavailable")
    order_id = policy.order_id
    if type(order_id) is not int or not 0 < order_id < 2**63:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    for key in ("now_ms", "shared_ms", "last_market_ms", "last_earn_ms"):
        value = getattr(policy, key)
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ModelBootstrapUnavailable("model_source_unavailable")
    if policy.last_capital is not None and (
        type(policy.last_capital) is not Decimal or not policy.last_capital.is_finite()
    ):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    if type(policy.reset_ms) is not dict or any(
        type(key) is not str or type(value) is not int or not -(2**63) <= value < 2**63
        for key, value in policy.reset_ms.items()
    ):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    if type(policy.last_filled_price) is not dict:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    last_filled: list[dict[str, object]] = []
    for product, prices in policy.last_filled_price.items():
        if product not in ordered_products or type(prices) is not dict or set(prices) != {"buy", "sell"}:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        last_filled.append({
            "product_id": product,
            "buy": _scalar(prices["buy"]),
            "sell": _scalar(prices["sell"]),
        })

    value: dict[str, object] = {
        "schema_version": "SPIDER_SOURCE_CHECKPOINT_V1",
        "policy_identity": {
            "policy_id": policy_id,
            "policy_version": policy_version,
            "policy_source_sha256": policy_source_sha256,
        },
        "source_sha256": source_sha256,
        "effective_identity": {
            "generation": generation,
            "confirmation_request_id": confirmation_request_id,
            "configuration_sha256": configuration_sha256,
        },
        "rows": rows,
        "markets": markets,
        "parameters": encoded_parameters,
        "capital": encoded_capital,
        "replies": encoded_replies,
        "running": _scalar(policy.running),
        "paused": _scalar(policy.paused),
        "online": _scalar(policy.online),
        "ws_open": _scalar(policy.ws_open),
        "now_ms": policy.now_ms,
        "shared_ms": policy.shared_ms,
        "last_market_ms": policy.last_market_ms,
        "last_earn_ms": policy.last_earn_ms,
        "last_capital": _scalar(policy.last_capital),
        "reset_ms": [
            {"product_id": product, "value": timestamp}
            for product, timestamp in policy.reset_ms.items()
        ],
        "last_filled_price": last_filled,
        "order_id": order_id,
    }
    return canonical_bytes(value)
