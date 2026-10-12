"""Private canonical encoder for the existing Spider source checkpoint V1."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_policy import DEFAULTS, Policy


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
_MARKET_OBSERVATION_KEYS = frozenset(
    {
        "schema_version", "scope", "effective_identity", "snapshot_id",
        "observed_at_ms", "snapshot_as_of_ms", "provenance", "markets",
    }
)
_MARKET_SPEC_KEYS = ("ctVal", "lotSz", "minSz", "increment")


@dataclass(frozen=True, slots=True)
class SourceMarketObservation:
    """One strictly decoded, complete replacement of Policy.markets."""

    snapshot_id: str
    observed_at_ms: int
    snapshot_as_of_ms: int
    evidence_sha256: str
    markets: dict[str, dict[str, object]]


def source_market_envelope_identity(raw: bytes) -> tuple[dict[str, str], str] | None:
    """Extract only the closed routing identity before owner-scope validation."""
    from src.core.backtest.spider_run_artifacts import decode_canonical

    if type(raw) is not bytes or not 1 <= len(raw) <= 1_048_576:
        return None
    try:
        value = decode_canonical(raw)
        row = _closed_object(value, _MARKET_OBSERVATION_KEYS)
        scope = _closed_object(
            row["scope"], frozenset({"environment", "execution_scope_id", "strategy_id"})
        )
        snapshot_id = row["snapshot_id"]
        if (
            row["schema_version"] != "SPIDER_SOURCE_MARKET_V1"
            or canonical_bytes(value) != raw
            or any(type(item) is not str or not item for item in scope.values())
            or type(snapshot_id) is not str
            or len(snapshot_id) != 64
            or any(char not in "0123456789abcdef" for char in snapshot_id)
        ):
            return None
        return cast(dict[str, str], scope), cast(str, snapshot_id)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None


def decode_source_market_observation(
    raw: bytes,
    *,
    scope: dict[str, str],
    generation: int,
    confirmation_request_id: str,
    configuration_sha256: str,
    evidence_sha256: str,
    ordered_products: tuple[str, ...],
    build_checkpoint_bytes: bytes,
) -> SourceMarketObservation:
    """Validate one MODEL snapshot against the immutable INSTALL market basis."""
    from datetime import datetime, timezone

    from src.core.backtest.spider_run_artifacts import decode_canonical

    _require(
        type(raw) is bytes
        and 1 <= len(raw) <= 1_048_576
        and type(scope) is dict
        and set(scope) == {"environment", "execution_scope_id", "strategy_id"}
        and all(type(value) is str and bool(value) for value in scope.values())
        and type(generation) is int
        and 1 <= generation < 2**63
        and type(confirmation_request_id) is str
        and len(confirmation_request_id) == 64
        and all(char in "0123456789abcdef" for char in confirmation_request_id)
        and type(configuration_sha256) is str
        and len(configuration_sha256) == 64
        and all(char in "0123456789abcdef" for char in configuration_sha256)
        and type(evidence_sha256) is str
        and len(evidence_sha256) == 64
        and all(char in "0123456789abcdef" for char in evidence_sha256)
        and type(ordered_products) is tuple
        and bool(ordered_products)
        and all(type(product) is str and bool(product) for product in ordered_products)
        and len(set(ordered_products)) == len(ordered_products)
        and type(build_checkpoint_bytes) is bytes
    )
    try:
        value = decode_canonical(raw)
        observation = _closed_object(value, _MARKET_OBSERVATION_KEYS)
        _require(observation["schema_version"] == "SPIDER_SOURCE_MARKET_V1")
        _require(observation["scope"] == scope)
        effective_identity = _closed_object(
            observation["effective_identity"],
            frozenset({"generation", "confirmation_request_id", "configuration_sha256"}),
        )
        observed_generation = effective_identity["generation"]
        _require(
            type(observed_generation) is int
            and 1 <= cast(int, observed_generation) < 2**63
        )
        _require(
            effective_identity
            == {
                "generation": generation,
                "confirmation_request_id": confirmation_request_id,
                "configuration_sha256": configuration_sha256,
            }
        )
        snapshot_id = observation["snapshot_id"]
        _require(
            type(snapshot_id) is str
            and len(snapshot_id) == 64
            and all(char in "0123456789abcdef" for char in snapshot_id)
        )
        observed_at_ms = observation["observed_at_ms"]
        snapshot_as_of_ms = observation["snapshot_as_of_ms"]
        _require(
            type(observed_at_ms) is int
            and type(snapshot_as_of_ms) is int
            and snapshot_as_of_ms <= observed_at_ms
        )
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        def utc_millis(value: datetime) -> int:
            delta = value - epoch
            return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000

        minimum_ms = utc_millis(datetime(1, 1, 1, tzinfo=timezone.utc))
        maximum_ms = utc_millis(datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc))
        _require(
            minimum_ms
            <= cast(int, snapshot_as_of_ms)
            <= cast(int, observed_at_ms)
            <= maximum_ms
        )
        provenance = _closed_object(
            observation["provenance"], frozenset({"kind", "evidence_sha256"})
        )
        _require(
            provenance == {"kind": "MODEL", "evidence_sha256": evidence_sha256}
        )

        checkpoint = _closed_object(
            decode_canonical(build_checkpoint_bytes),
            frozenset(
                {
                    "schema_version", "policy_identity", "source_sha256",
                    "effective_identity", "rows", "markets", "parameters",
                    "capital", "replies", "running", "paused", "online",
                    "ws_open", "now_ms", "shared_ms", "last_market_ms",
                    "last_earn_ms", "last_capital", "reset_ms",
                    "last_filled_price", "order_id",
                }
            ),
        )
        baseline_value = checkpoint["markets"]
        supplied_value = observation["markets"]
        _require(
            type(baseline_value) is list
            and type(supplied_value) is list
            and len(cast(list[object], baseline_value)) == len(ordered_products)
            and len(cast(list[object], supplied_value)) == len(ordered_products)
        )
        markets: dict[str, dict[str, object]] = {}
        for product, baseline_item, supplied_item in zip(
            ordered_products,
            cast(list[object], baseline_value),
            cast(list[object], supplied_value),
            strict=True,
        ):
            baseline = _closed_object(baseline_item, frozenset({"product_id", "value"}))
            supplied = _closed_object(supplied_item, frozenset({"product_id", "value"}))
            _require(baseline["product_id"] == product == supplied["product_id"])
            baseline_fields = _closed_object(baseline["value"], frozenset(_MARKET_KEYS))
            supplied_fields = _closed_object(supplied["value"], frozenset(_MARKET_KEYS))
            decoded: dict[str, object] = {}
            for key in _MARKET_KEYS:
                encoded = supplied_fields[key]
                decoded[key] = _decode_scalar(encoded)
            for key in _MARKET_SPEC_KEYS + ("state", "instIdCode"):
                _require(supplied_fields[key] == baseline_fields[key])
            for key in _MARKET_SPEC_KEYS:
                _require(type(decoded[key]) is str)
                number = Decimal(cast(str, decoded[key]))
                _require(number.is_finite() and number > 0)
            _require(type(decoded["price"]) is Decimal and cast(Decimal, decoded["price"]).is_finite())
            _require(cast(Decimal, decoded["price"]) > 0)
            _require(type(decoded["ratioHL"]) is Decimal and cast(Decimal, decoded["ratioHL"]).is_finite())
            _require(cast(Decimal, decoded["ratioHL"]) >= 0)
            _require(decoded["state"] == "live")
            _require(type(decoded["instIdCode"]) is int and cast(int, decoded["instIdCode"]) >= 0)
            markets[product] = decoded
        _require(canonical_bytes(value) == raw)
        return SourceMarketObservation(
            snapshot_id=cast(str, snapshot_id),
            observed_at_ms=cast(int, observed_at_ms),
            snapshot_as_of_ms=cast(int, snapshot_as_of_ms),
            evidence_sha256=evidence_sha256,
            markets=markets,
        )
    except (ArithmeticError, TypeError, ValueError, UnicodeDecodeError):
        raise ValueError("INVALID_SOURCE_MARKET") from None


def replace_checkpoint_markets(
    checkpoint_bytes: bytes,
    *,
    ordered_products: tuple[str, ...],
    markets: dict[str, dict[str, object]],
) -> bytes:
    """Return canonical checkpoint bytes changing only its ordered market map."""
    from src.core.backtest.spider_run_artifacts import decode_canonical

    try:
        value = _closed_object(
            decode_canonical(checkpoint_bytes),
            frozenset(
                {
                    "schema_version", "policy_identity", "source_sha256",
                    "effective_identity", "rows", "markets", "parameters",
                    "capital", "replies", "running", "paused", "online",
                    "ws_open", "now_ms", "shared_ms", "last_market_ms",
                    "last_earn_ms", "last_capital", "reset_ms",
                    "last_filled_price", "order_id",
                }
            ),
        )
        _require(type(markets) is dict and tuple(markets) == ordered_products)
        replacement: list[dict[str, object]] = []
        for product in ordered_products:
            row = markets[product]
            _require(type(row) is dict and set(row) == set(_MARKET_KEYS))
            replacement.append(
                {
                    "product_id": product,
                    "value": {key: _scalar(row[key]) for key in _MARKET_KEYS},
                }
            )
        value["markets"] = replacement
        return canonical_bytes(value)
    except (ArithmeticError, TypeError, ValueError):
        raise ValueError("INVALID_SOURCE_CHECKPOINT") from None


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


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("INVALID_SOURCE_CHECKPOINT")


def _closed_object(value: object, keys: frozenset[str]) -> dict[str, object]:
    _require(type(value) is dict and set(cast(dict[object, object], value)) == keys)
    return cast(dict[str, object], value)


def _int64(value: object) -> int:
    _require(type(value) is int and -(2**63) <= cast(int, value) < 2**63)
    return cast(int, value)


def _decode_scalar(value: object) -> object:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    tag = row.get("tag")
    if tag == "NULL":
        _require(set(row) == {"tag"})
        return None
    if tag == "STRING":
        _require(set(row) == {"tag", "value"} and type(row["value"]) is str)
        return row["value"]
    if tag == "DECIMAL":
        text = row.get("value")
        _require(set(row) == {"tag", "value"} and type(text) is str)
        decimal_text = cast(str, text)
        # The encoder always emits fixed-point text; reject exponent forms before
        # rendering so a tiny payload cannot request an unbounded expansion.
        _require("e" not in decimal_text.lower())
        try:
            number = Decimal(decimal_text)
            from src.core.decimal_math import canonical_decimal_text

            canonical = canonical_decimal_text(number)
        except (InvalidOperation, ValueError):
            raise ValueError("INVALID_SOURCE_CHECKPOINT") from None
        _require(number.is_finite() and canonical == decimal_text)
        return number
    if tag == "INT64":
        _require(set(row) == {"tag", "value"})
        return _int64(row["value"])
    if tag == "BOOL":
        _require(set(row) == {"tag", "value"} and type(row["value"]) is bool)
        return row["value"]
    raise ValueError("INVALID_SOURCE_CHECKPOINT")


def _boolean_scalar(value: object) -> bool:
    decoded = _decode_scalar(value)
    _require(type(decoded) is bool)
    return cast(bool, decoded)


def _decode_values(value: object, allowed: frozenset[str]) -> dict[str, object]:
    _require(type(value) is list)
    result: dict[str, object] = {}
    for item in cast(list[object], value):
        pair = _closed_object(item, frozenset({"key", "value"}))
        key = pair["key"]
        _require(type(key) is str and key in allowed and key not in result)
        result[cast(str, key)] = _decode_scalar(pair["value"])
    return result


def restore_source_checkpoint(
    raw: bytes,
    *,
    ordered_products: tuple[str, ...],
    policy_id: str,
    policy_version: str,
    policy_source_sha256: str,
    source_sha256: str,
    confirmation_request_id: str,
    configuration_sha256: str,
    generation: int,
) -> Policy:
    """Reconstruct and byte-verify one saved V1 Policy without source activity."""
    from src.core.backtest.spider_run_artifacts import decode_canonical

    _require(
        type(raw) is bytes
        and type(ordered_products) is tuple
        and bool(ordered_products)
        and all(type(product) is str and bool(product) for product in ordered_products)
        and len(set(ordered_products)) == len(ordered_products)
        and type(policy_id) is str
        and bool(policy_id)
        and type(policy_version) is str
        and bool(policy_version)
        and all(
            type(value) is str
            and len(value) == 64
            and all(char in "0123456789abcdef" for char in value)
            for value in (
                policy_source_sha256,
                source_sha256,
                confirmation_request_id,
                configuration_sha256,
            )
        )
        and type(generation) is int
        and 1 <= generation < 2**63
    )
    try:
        value = decode_canonical(raw)
        envelope = _closed_object(
            value,
            frozenset(
                {
                    "schema_version", "policy_identity", "source_sha256",
                    "effective_identity", "rows", "markets", "parameters",
                    "capital", "replies", "running", "paused", "online",
                    "ws_open", "now_ms", "shared_ms", "last_market_ms",
                    "last_earn_ms", "last_capital", "reset_ms",
                    "last_filled_price", "order_id",
                }
            ),
        )
        _require(envelope["schema_version"] == "SPIDER_SOURCE_CHECKPOINT_V1")
        policy_identity = _closed_object(
            envelope["policy_identity"],
            frozenset({"policy_id", "policy_version", "policy_source_sha256"}),
        )
        effective_identity = _closed_object(
            envelope["effective_identity"],
            frozenset({"generation", "confirmation_request_id", "configuration_sha256"}),
        )
        _require(
            policy_identity
            == {
                "policy_id": policy_id,
                "policy_version": policy_version,
                "policy_source_sha256": policy_source_sha256,
            }
            and envelope["source_sha256"] == source_sha256
            and type(effective_identity["generation"]) is int
            and effective_identity
            == {
                "generation": generation,
                "confirmation_request_id": confirmation_request_id,
                "configuration_sha256": configuration_sha256,
            }
        )

        rows_value = envelope["rows"]
        _require(type(rows_value) is list and len(cast(list[object], rows_value)) == len(ordered_products))
        rows: list[dict[str, object]] = []
        for product, row_value in zip(ordered_products, cast(list[object], rows_value), strict=True):
            row = _closed_object(row_value, frozenset(_ROW_KEYS))
            decoded_row = {key: _decode_scalar(row[key]) for key in _ROW_KEYS}
            _require(decoded_row["name"] == product)
            decoded_row["product_id"] = product
            rows.append(decoded_row)

        markets_value = envelope["markets"]
        _require(
            type(markets_value) is list
            and len(cast(list[object], markets_value)) == len(ordered_products)
        )
        markets: dict[str, dict[str, object]] = {}
        for product, market_value in zip(
            ordered_products, cast(list[object], markets_value), strict=True
        ):
            market = _closed_object(market_value, frozenset({"product_id", "value"}))
            _require(market["product_id"] == product)
            values = _closed_object(market["value"], frozenset(_MARKET_KEYS))
            markets[product] = {key: _decode_scalar(values[key]) for key in _MARKET_KEYS}

        parameters_value = envelope["parameters"]
        _require(
            type(parameters_value) is list
            and len(cast(list[object], parameters_value)) == len(DEFAULTS)
        )
        parameters: dict[str, object] = {}
        for expected_key, pair_value in zip(DEFAULTS, cast(list[object], parameters_value), strict=True):
            pair = _closed_object(pair_value, frozenset({"key", "value"}))
            _require(pair["key"] == expected_key and expected_key not in parameters)
            parameters[expected_key] = _decode_scalar(pair["value"])

        capital = _decode_values(envelope["capital"], _CAPITAL_KEYS)
        replies_value = envelope["replies"]
        _require(type(replies_value) is list)
        replies: dict[str, dict[str, object]] = {}
        for reply_value in cast(list[object], replies_value):
            reply = _closed_object(reply_value, frozenset({"alias", "value"}))
            alias = reply["alias"]
            _require(type(alias) is str and alias not in replies)
            replies[cast(str, alias)] = _decode_values(reply["value"], _REPLY_KEYS)

        flags = {
            key: _boolean_scalar(envelope[key])
            for key in ("running", "paused", "online", "ws_open")
        }
        now_ms = _int64(envelope["now_ms"])
        shared_ms = _int64(envelope["shared_ms"])
        last_market_ms = _int64(envelope["last_market_ms"])
        last_earn_ms = _int64(envelope["last_earn_ms"])
        last_capital = _decode_scalar(envelope["last_capital"])
        _require(last_capital is None or type(last_capital) is Decimal)
        order_id = envelope["order_id"]
        _require(type(order_id) is int and 0 < cast(int, order_id) < 2**63)

        reset_value = envelope["reset_ms"]
        _require(type(reset_value) is list)
        reset_ms: dict[str, int] = {}
        for item in cast(list[object], reset_value):
            reset = _closed_object(item, frozenset({"product_id", "value"}))
            product = reset["product_id"]
            _require(type(product) is str and product in ordered_products and product not in reset_ms)
            reset_ms[cast(str, product)] = _int64(reset["value"])

        last_filled_value = envelope["last_filled_price"]
        _require(type(last_filled_value) is list)
        last_filled_price: dict[str, dict[str, object]] = {}
        for item in cast(list[object], last_filled_value):
            prices = _closed_object(item, frozenset({"product_id", "buy", "sell"}))
            product = prices["product_id"]
            _require(
                type(product) is str
                and product in ordered_products
                and product not in last_filled_price
            )
            buy = _decode_scalar(prices["buy"])
            sell = _decode_scalar(prices["sell"])
            _require(type(buy) is str and type(sell) is str)
            last_filled_price[cast(str, product)] = {
                "buy": buy,
                "sell": sell,
            }

        policy = Policy(rows, markets, parameters, now_ms=now_ms)
        policy.capital = capital
        policy.replies = replies
        policy.running = flags["running"]
        policy.paused = flags["paused"]
        policy.online = flags["online"]
        policy.ws_open = flags["ws_open"]
        policy.now_ms = now_ms
        policy.shared_ms = shared_ms
        policy.last_market_ms = last_market_ms
        policy.last_earn_ms = last_earn_ms
        policy.last_capital = cast(Decimal | None, last_capital)
        policy.reset_ms = reset_ms
        policy.last_filled_price = last_filled_price
        policy.order_id = cast(int, order_id)
        policy.events = []

        if encode_source_checkpoint(
            policy,
            ordered_products=ordered_products,
            policy_id=policy_id,
            policy_version=policy_version,
            policy_source_sha256=policy_source_sha256,
            source_sha256=source_sha256,
            confirmation_request_id=confirmation_request_id,
            configuration_sha256=configuration_sha256,
            generation=generation,
        ) != raw:
            raise ValueError("INVALID_SOURCE_CHECKPOINT")
        return policy
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError, IndexError):
        raise ValueError("INVALID_SOURCE_CHECKPOINT") from None
