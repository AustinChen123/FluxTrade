"""Immutable offline input DTOs and pre-owner admission for Spider P3.

This module deliberately has no network, provider, or financial-owner imports.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import json
import re
from typing import Any, Callable, TypeVar, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical


SCHEMA_ID = "SPIDER_HISTORICAL_RESEARCH_RUN_V1"
SCHEMA_VERSION = "spider_historical_research_run_v1"
BAR_DURATION_MS = 60_000
WARMUP_MS = 86_400_000
MATCHING_MODEL_IDS = frozenset(
    {"OHLC4_OPEN_HIGH_LOW_CLOSE_V1", "OHLC4_OPEN_LOW_HIGH_CLOSE_V1"}
)
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
_FEE_PROVENANCE = re.compile(r"[A-Z][A-Z0-9_]*@v[1-9][0-9]*:sha256:[0-9a-f]{64}\Z")
_POLL_PROFILES = frozenset({"POLL_ORDERED_V1", "POLL_REVERSED_VISIBILITY_V1"})


class HistoricalInputError(ValueError):
    """Input is not admissible for a historical Spider run."""


def _canonical_decimal(value: Decimal) -> str:
    if type(value) is not Decimal or not value.is_finite():
        raise HistoricalInputError("financial and numeric values must be finite Decimal")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"-0", ""} else text


def _canonical(value: object) -> bytes:
    def convert(item: object) -> object:
        if type(item) is Decimal:
            return _canonical_decimal(item)
        if type(item) in (str, int, bool) or item is None:
            return item
        if isinstance(item, (tuple, list)):
            return [convert(element) for element in item]
        if type(item) is bytes:
            return {"$canonical_bytes": item.hex()}
        if isinstance(item, frozenset):
            return sorted((convert(element) for element in item), key=str)
        if isinstance(item, dict):
            return {str(key): convert(element) for key, element in item.items()}
        raise HistoricalInputError("unsupported canonical value")

    return json.dumps(convert(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(value: object) -> str:
    return sha256(_canonical(value)).hexdigest()


def _parameter_projection(
    parameters: tuple[tuple[str, str | Decimal], ...],
) -> list[dict[str, str]]:
    """Preserve parameter value types in every canonical identity projection."""
    projected: list[dict[str, str]] = []
    for key, value in parameters:
        if type(value) is Decimal:
            projected.append({"key": key, "kind": "DECIMAL", "value": _canonical_decimal(value)})
        elif type(value) is str:
            projected.append({"key": key, "kind": "STRING", "value": value})
        else:
            raise HistoricalInputError("parameters must be Decimal or string")
    return projected


@dataclass(frozen=True, slots=True)
class HistoricalBar:
    product_id: str
    bar_open_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    confirm: int
    source_sequence: int
    volume: Decimal | None = None
    source_row_hash: str = ""

    def __post_init__(self) -> None:
        if type(self.product_id) is not str or not self.product_id:
            raise HistoricalInputError("invalid bar product")
        if type(self.source_row_hash) is not str:
            raise HistoricalInputError("invalid bar hash type")

    def canonical_row(self, *, trade: bool) -> bytes:
        fields: tuple[object, ...] = (
            self.product_id, self.bar_open_ms, self.open, self.high, self.low,
            self.close,
        )
        if trade:
            fields += (self.volume,)
        fields += (self.confirm, self.source_sequence)
        return "|".join(_canonical_decimal(value) if type(value) is Decimal else str(value) for value in fields).encode("utf-8")


@dataclass(frozen=True, slots=True)
class InstrumentSpecEvidence:
    product_id: str
    effective_at_ms: int
    contract_value: Decimal
    multiplier: Decimal
    price_tick: Decimal
    quantity_step: Decimal
    minimum_quantity: Decimal
    tier_hash: str


@dataclass(frozen=True, slots=True)
class SourceManifest:
    source_id: str
    provider: str
    endpoint: str
    request_parameters: tuple[tuple[str, str], ...]
    retrieved_at_utc: str
    http_status: int
    raw_sha256: str
    canonical_rows_sha256: str
    schema_version: str
    field_map_version: str
    product_mapping: tuple[tuple[str, str], ...]
    units: str
    source_row_count: int
    normalized_row_count: int
    first_source_timestamp_ms: int
    last_source_timestamp_ms: int
    duplicate_policy: str
    gap_policy: str
    evidence_label: str

    def __post_init__(self) -> None:
        if (type(self.request_parameters) is not tuple or type(self.product_mapping) is not tuple
                or any(type(pair) is not tuple or len(pair) != 2 for pair in self.request_parameters + self.product_mapping)):
            raise HistoricalInputError("manifest collections must be immutable tuples")


@dataclass(frozen=True, slots=True)
class HistoricalRunInput:
    schema_id: str
    schema_version: str
    run_id: str
    account_key: str
    policy_id: str
    policy_version: str
    policy_source_sha256: str
    strategy_identity: str
    parameters: tuple[tuple[str, str | Decimal], ...]
    configuration_bytes: bytes
    configuration_sha256: str
    ordered_products: tuple[str, ...]
    model_id: str
    model_version: int
    range_start_ms: int
    range_end_ms: int
    bar_duration_ms: int
    warmup_start_ms: int
    trade_manifest: SourceManifest
    mark_manifest: SourceManifest
    trade_bars: tuple[HistoricalBar, ...]
    mark_bars: tuple[HistoricalBar, ...]
    spec_before: tuple[InstrumentSpecEvidence, ...]
    spec_after: tuple[InstrumentSpecEvidence, ...]
    market_slippage_bps: Decimal
    execution_fee_provenance: str
    timer_period_ms: int
    first_timer_ms: int
    local_clock_zone: str
    order_accept_delay_ms: int
    cancel_effect_delay_ms: int
    order_notice_delay_ms: int
    cancel_ack_delay_ms: int
    poll_profile: str
    endpoint_policy_id: str
    initial_policy_cache: bytes
    initial_account_state: bytes
    funding_mode: str = "DISABLED"

    def __post_init__(self) -> None:
        collections = (self.parameters, self.ordered_products, self.trade_bars, self.mark_bars,
                       self.spec_before, self.spec_after)
        if any(type(value) is not tuple for value in collections):
            raise HistoricalInputError("run collections must be immutable tuples")
        if any(type(pair) is not tuple or len(pair) != 2 for pair in self.parameters):
            raise HistoricalInputError("parameters must be immutable key/value pairs")


def _valid_hash(value: str) -> bool:
    return type(value) is str and _HASH.fullmatch(value) is not None


def _decode_canonical(raw: bytes) -> object:
    if type(raw) is not bytes:
        raise HistoricalInputError("canonical state must be bytes")
    try:
        value = decode_canonical(raw)
    except (ValueError, UnicodeDecodeError) as error:
        raise HistoricalInputError("invalid canonical state") from error
    if canonical_bytes(value) != raw:
        raise HistoricalInputError("noncanonical state bytes")
    return value


def _decimal_string(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        return _canonical_decimal(Decimal(value)) == value
    except (ValueError, ArithmeticError):
        return False


_CONFIG_DECIMAL_FIELDS = frozenset({
    "cash", "leverage", "taker_fee_rate", "liquidation_fee_rate", "contract_value",
    "multiplier", "price_tick", "quantity_step", "minimum_quantity", "minimum_contracts",
    "maximum_contracts", "mmr", "imr", "max_leverage", "mark", "quantity_contracts",
    "entry_price", "limit_price", "original_quantity_contracts", "filled_quantity_contracts",
    "canceled_quantity_contracts", "remaining_quantity_contracts",
})


def _validate_decimal_leaves(value: object, product_ids: tuple[str, ...]) -> None:
    """Validate known P2 financial leaves and product references without modeling state."""
    if type(value) is dict:
        for key, nested in cast(dict[object, object], value).items():
            if key in _CONFIG_DECIMAL_FIELDS and not _decimal_string(nested):
                raise HistoricalInputError("invalid financial decimal leaf")
            if key == "product_id" and nested not in product_ids:
                raise HistoricalInputError("unknown configured product reference")
            _validate_decimal_leaves(nested, product_ids)
    elif type(value) is list:
        for nested in cast(list[object], value):
            _validate_decimal_leaves(nested, product_ids)


def _require_shape(value: object, required: set[str], label: str) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[str, object], value)) != required:
        raise HistoricalInputError(f"invalid {label} shape")
    return cast(dict[str, object], value)


def _validate_config_position(value: object, products: tuple[str, ...]) -> None:
    position = _require_shape(value, {"product_id", "side", "quantity_contracts", "lots"}, "P2 position")
    if (type(position["side"]) is not str or position["side"] not in {"LONG", "SHORT"}
            or type(position["lots"]) is not list):
        raise HistoricalInputError("invalid P2 position fields")
    for lot_value in cast(list[object], position["lots"]):
        lot = _require_shape(lot_value, {"seed_execution_id", "seed_sequence", "strategy_id", "quantity_contracts", "entry_price"}, "P2 lot")
        if (not _nonempty(lot["seed_execution_id"]) or not _nonempty(lot["strategy_id"])
                or type(lot["seed_sequence"]) is not int or lot["seed_sequence"] < 0):
            raise HistoricalInputError("invalid P2 lot identity")
    _validate_decimal_leaves(position, products)


def _validate_config_order(value: object, products: tuple[str, ...]) -> None:
    fields = {"intent_id", "order_id", "client_order_id", "strategy_id", "product_id", "side", "limit_price",
              "reduce_only", "original_quantity_contracts", "filled_quantity_contracts",
              "canceled_quantity_contracts", "remaining_quantity_contracts", "status"}
    order = _require_shape(value, fields, "P2 order")
    if (any(not _nonempty(order[key]) for key in ("intent_id", "order_id", "client_order_id", "strategy_id", "status"))
            or type(order["side"]) is not str or order["side"] not in {"LONG", "SHORT"}
            or type(order["reduce_only"]) is not bool):
        raise HistoricalInputError("invalid P2 order fields")
    _validate_decimal_leaves(order, products)


def decode_p2_configuration(
    configuration_bytes: bytes, configuration_sha256: str, ordered_products: tuple[str, ...]
) -> dict[str, object]:
    """Decode the existing P2 config codec and verify its frozen product identity."""
    config_value = _decode_canonical(configuration_bytes)
    if type(config_value) is not dict:
        raise HistoricalInputError("P2 configuration must be an object")
    config = cast(dict[str, object], config_value)
    required = {"schema_version", "config_id", "seed_effective_at", "cash", "leverage",
                "products", "positions", "orders"}
    if config.keys() != required or config["schema_version"] != "synthetic_multi_product_config_v1":
        raise HistoricalInputError("invalid P2 configuration schema")
    if (not _nonempty(config["config_id"]) or type(config["seed_effective_at"]) is not int
            or not _decimal_string(config["cash"]) or not _decimal_string(config["leverage"])
            or type(config["products"]) is not list or type(config["positions"]) is not list
            or type(config["orders"]) is not list):
        raise HistoricalInputError("invalid P2 configuration shape")
    products = cast(list[object], config["products"])
    product_ids = []
    for product in products:
        if type(product) is not dict or not _nonempty(cast(dict[str, object], product).get("product_id")):
            raise HistoricalInputError("invalid P2 product")
        product_ids.append(cast(str, cast(dict[str, object], product)["product_id"]))
    if (type(ordered_products) is not tuple or any(type(product) is not str for product in ordered_products)
            or tuple(product_ids) != ordered_products):
        raise HistoricalInputError("P2 product order mismatch")
    if len(set(product_ids)) != len(product_ids):
        raise HistoricalInputError("duplicate P2 products")
    product_tuple = tuple(product_ids)
    for position in cast(list[object], config["positions"]):
        _validate_config_position(position, product_tuple)
    for order in cast(list[object], config["orders"]):
        _validate_config_order(order, product_tuple)
    _validate_decimal_leaves(config, product_tuple)
    if sha256(configuration_bytes).hexdigest() != configuration_sha256:
        raise HistoricalInputError("P2 configuration content hash mismatch")
    return config


def _validate_initial_account_state(raw: bytes, config: dict[str, object]) -> None:
    value = _decode_canonical(raw)
    if type(value) is not dict or set(value) != {"cash", "orders", "positions"}:
        raise HistoricalInputError("invalid initial account state shape")
    state = cast(dict[str, object], value)
    if not _decimal_string(state["cash"]) or type(state["orders"]) is not list or type(state["positions"]) is not list:
        raise HistoricalInputError("invalid initial account state types")
    products = tuple(cast(str, cast(dict[str, object], item)["product_id"])
                     for item in cast(list[object], config["products"]))
    for position in cast(list[object], state["positions"]):
        _validate_config_position(position, products)
    for order in cast(list[object], state["orders"]):
        _validate_config_order(order, products)
    if any(state[key] != config[key] for key in ("cash", "orders", "positions")):
        raise HistoricalInputError("initial account state differs from P2 seed")


def _validate_initial_policy_cache(raw: bytes, ordered_products: tuple[str, ...]) -> None:
    value = _decode_canonical(raw)
    required = {"schema_version", "running", "paused", "online", "ws_open", "order_id",
                "capital", "rows", "markets", "replies", "orders", "positions", "last_filled_price"}
    if type(value) is not dict or set(value) != required:
        raise HistoricalInputError("invalid initial policy-cache shape")
    state = cast(dict[str, object], value)
    if state["schema_version"] != "SPIDER_POLICY_CACHE_V1":
        raise HistoricalInputError("unsupported policy-cache version")
    if any(type(state[key]) is not bool for key in ("running", "paused", "online", "ws_open")):
        raise HistoricalInputError("invalid policy runtime flags")
    if type(state["order_id"]) is not int or state["order_id"] < 0:
        raise HistoricalInputError("invalid policy order id")
    capital = state["capital"]
    if (type(capital) is not dict or set(cast(dict[str, object], capital)) !=
            {"total", "usdt", "avail", "earn", "position"}
            or not all(_decimal_string(value) for value in cast(dict[str, object], capital).values())):
        raise HistoricalInputError("invalid policy capital cache")
    for field in ("rows", "markets"):
        collection = state[field]
        if type(collection) is not list:
            raise HistoricalInputError("invalid policy product collection")
        product_ids: list[str] = []
        for entry in cast(list[object], collection):
            if type(entry) is not dict:
                raise HistoricalInputError("invalid policy product entry")
            item = cast(dict[str, object], entry)
            product_id = item.get("product_id")
            if not _nonempty(product_id):
                raise HistoricalInputError("missing policy product reference")
            if field == "rows":
                row = _require_shape(item, {"product_id", "name", "active", "leverage", "歩差", "單數", "hold上限", "hold下限", "hold"}, "policy row")
                if (row["name"] != product_id or type(row["active"]) is not str
                        or row["active"] not in {"true", "false"}):
                    raise HistoricalInputError("invalid policy row identity or active flag")
                for key in ("leverage", "歩差", "單數", "hold上限", "hold下限", "hold"):
                    if not _decimal_string(row[key]):
                        raise HistoricalInputError("invalid policy row decimal")
            else:
                market_required = {"product_id", "price", "ctVal", "lotSz", "minSz", "increment", "ratioHL", "state", "instIdCode"}
                market = _require_shape(item, market_required | ({"instId"} if "instId" in item else set()), "policy market")
                if (not all(_decimal_string(market[key]) for key in ("price", "ctVal", "lotSz", "minSz", "increment", "ratioHL"))
                        or not _nonempty(market["state"]) or type(market["instIdCode"]) is not int):
                    raise HistoricalInputError("invalid policy market fields")
                if any(not Decimal(cast(str, market[key])) > 0 for key in ("price", "ctVal", "lotSz", "minSz", "increment")):
                    raise HistoricalInputError("nonpositive policy market spec")
            product_ids.append(cast(str, product_id))
        if tuple(product_ids) != ordered_products:
            raise HistoricalInputError("policy product order mismatch")
    for field in ("replies", "orders", "positions", "last_filled_price"):
        if type(state[field]) is not dict:
            raise HistoricalInputError("invalid policy cache object")

    last_filled = cast(dict[str, object], state["last_filled_price"])
    for product, values in last_filled.items():
        if product not in ordered_products:
            raise HistoricalInputError("last-fill cache references unknown product")
        prices = _require_shape(values, {"buy", "sell"}, "last-fill prices")
        if not all(_decimal_string(price) for price in prices.values()):
            raise HistoricalInputError("invalid last-fill price")

    def check_references(item: object) -> None:
        if type(item) is dict:
            for key, nested in cast(dict[str, object], item).items():
                if key in {"product_id", "instId"} and nested not in ordered_products:
                    raise HistoricalInputError("policy cache references unknown product")
                check_references(nested)
        elif type(item) is list:
            for nested in cast(list[object], item):
                check_references(nested)
    check_references(state)


def validate_initial_states(
    configuration_bytes: bytes,
    configuration_sha256: str,
    ordered_products: tuple[str, ...],
    initial_account_state: bytes,
    initial_policy_cache: bytes,
) -> None:
    config = decode_p2_configuration(configuration_bytes, configuration_sha256, ordered_products)
    _validate_initial_account_state(initial_account_state, config)
    _validate_initial_policy_cache(initial_policy_cache, ordered_products)


def _valid_utc(value: str) -> bool:
    if type(value) is not str or _UTC.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def _nonempty(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _validate_manifest(
    manifest: SourceManifest,
    rows: tuple[HistoricalBar, ...],
    *,
    trade: bool,
    products: tuple[str, ...],
) -> None:
    if not isinstance(manifest, SourceManifest):
        raise HistoricalInputError("invalid source manifest")
    if not rows:
        raise HistoricalInputError("source manifest has no rows")
    text_fields = (
        manifest.source_id, manifest.provider, manifest.endpoint, manifest.schema_version,
        manifest.field_map_version, manifest.units, manifest.duplicate_policy,
        manifest.gap_policy, manifest.evidence_label,
    )
    if not all(_nonempty(value) for value in text_fields) or not _valid_utc(manifest.retrieved_at_utc):
        raise HistoricalInputError("invalid manifest provenance")
    if (manifest.provider != "OKX_PUBLIC_V5" or manifest.schema_version != "V1"
            or manifest.field_map_version != "OKX_HISTORY_CANDLES_V1"):
        raise HistoricalInputError("unsupported first-version manifest")
    expected_endpoint = (
        "/api/v5/market/history-candles" if trade
        else "/api/v5/market/history-mark-price-candles"
    )
    if manifest.endpoint != expected_endpoint:
        raise HistoricalInputError("unsupported source endpoint")
    if any(not _nonempty(key) or type(value) is not str for key, value in manifest.request_parameters):
        raise HistoricalInputError("invalid manifest request parameters")
    if not all(_valid_hash(value) for value in (manifest.raw_sha256, manifest.canonical_rows_sha256)):
        raise HistoricalInputError("invalid source hash")
    if (type(manifest.http_status) is not int or manifest.http_status != 200
            or type(manifest.source_row_count) is not int or type(manifest.normalized_row_count) is not int
            or manifest.source_row_count != len(rows) or manifest.normalized_row_count != len(rows)):
        raise HistoricalInputError("manifest row count or status mismatch")
    if manifest.duplicate_policy != "REJECT" or manifest.gap_policy != "REJECT":
        raise HistoricalInputError("unsupported duplicate or gap policy")
    row_hashes: list[tuple[int, int, bytes]] = []
    for row in rows:
        if type(row) is not HistoricalBar or row.product_id not in products:
            raise HistoricalInputError("invalid source row product or DTO")
        if type(row.bar_open_ms) is not int or type(row.source_sequence) is not int or row.source_sequence < 0:
            raise HistoricalInputError("invalid source row ordering fields")
        canonical = row.canonical_row(trade=trade)
        expected = sha256(canonical).hexdigest()
        if row.source_row_hash != expected:
            raise HistoricalInputError("source row hash mismatch")
        row_hashes.append((products.index(row.product_id), row.source_sequence, bytes.fromhex(expected)))
    if len({(ordinal, sequence) for ordinal, sequence, _ in row_hashes}) != len(row_hashes):
        raise HistoricalInputError("duplicate source sequence")
    if (type(manifest.first_source_timestamp_ms) is not int
            or type(manifest.last_source_timestamp_ms) is not int
            or manifest.first_source_timestamp_ms != min(row.bar_open_ms for row in rows)
            or manifest.last_source_timestamp_ms != max(row.bar_open_ms for row in rows)):
        raise HistoricalInputError("manifest timestamp range mismatch")
    ordered_hashes = (digest for _, _, digest in sorted(row_hashes, key=lambda item: item[:2]))
    if sha256(b"".join(ordered_hashes)).hexdigest() != manifest.canonical_rows_sha256:
        raise HistoricalInputError("canonical manifest hash mismatch")


def validate_historical_input(run: HistoricalRunInput) -> str:
    """Validate and hash the whole immutable input without constructing owners."""
    if not isinstance(run, HistoricalRunInput):
        raise HistoricalInputError("invalid run DTO")
    if (type(run.schema_id) is not str or type(run.schema_version) is not str
            or (run.schema_id, run.schema_version) != (SCHEMA_ID, SCHEMA_VERSION)):
        raise HistoricalInputError("unsupported schema")
    identities = (run.run_id, run.account_key, run.policy_id, run.policy_version,
                  run.strategy_identity, run.model_id)
    if not all(_nonempty(value) for value in identities):
        raise HistoricalInputError("missing run identity")
    if (type(run.model_id) is not str or run.model_id not in MATCHING_MODEL_IDS
            or type(run.model_version) is not int or run.model_version != 1):
        raise HistoricalInputError("unsupported matching model")
    if (run.funding_mode != "DISABLED" or type(run.bar_duration_ms) is not int
            or run.bar_duration_ms != BAR_DURATION_MS):
        raise HistoricalInputError("unsupported units or funding mode")
    if (type(run.ordered_products) is not tuple or not run.ordered_products
            or any(type(product) is not str or not product for product in run.ordered_products)
            or len(set(run.ordered_products)) != len(run.ordered_products)):
        raise HistoricalInputError("invalid product order")
    if (any(type(value) is not int for value in (run.range_start_ms, run.range_end_ms, run.warmup_start_ms))
            or run.range_start_ms >= run.range_end_ms
            or run.warmup_start_ms != run.range_start_ms - WARMUP_MS):
        raise HistoricalInputError("invalid run or warmup range")
    if run.warmup_start_ms < 0 or run.range_end_ms >= 1 << 59:
        raise HistoricalInputError("timestamp outside supported range")
    if not _valid_hash(run.policy_source_sha256) or not _valid_hash(run.configuration_sha256):
        raise HistoricalInputError("invalid policy/configuration hash")
    validate_initial_states(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products,
        run.initial_account_state, run.initial_policy_cache,
    )
    parameter_keys = [key for key, _ in run.parameters]
    if (not run.parameters or any(type(key) is not str or not key.strip() for key in parameter_keys)
            or parameter_keys != sorted(set(parameter_keys))):
        raise HistoricalInputError("invalid or duplicate strategy parameters")
    for _, value in run.parameters:
        if type(value) is Decimal:
            _canonical_decimal(value)
        elif type(value) is not str:
            raise HistoricalInputError("parameters must be Decimal or string")
    _canonical_decimal(run.market_slippage_bps)
    if run.market_slippage_bps < 0:
        raise HistoricalInputError("negative market slippage")
    if not _nonempty(run.execution_fee_provenance):
        raise HistoricalInputError("missing execution-fee provenance")
    if (run.execution_fee_provenance != "MODELLING_ASSUMPTION"
            and _FEE_PROVENANCE.fullmatch(run.execution_fee_provenance) is None):
        raise HistoricalInputError("invalid execution-fee provenance")
    if (type(run.timer_period_ms) is not int or run.timer_period_ms != 5000
            or type(run.first_timer_ms) is not int or run.first_timer_ms != run.range_start_ms + 5000
            or run.local_clock_zone != "UTC"):
        raise HistoricalInputError("unsupported timer or clock contract")
    fixed_delays = (run.order_accept_delay_ms, run.cancel_effect_delay_ms,
                    run.order_notice_delay_ms, run.cancel_ack_delay_ms)
    if any(type(value) is not int for value in fixed_delays) or fixed_delays != (1, 5001, 2, 2):
        raise HistoricalInputError("unsupported first-version delays")
    if (type(run.poll_profile) is not str or run.poll_profile not in _POLL_PROFILES
            or run.endpoint_policy_id != "MTM_PRESERVE_OPEN_V1"):
        raise HistoricalInputError("unsupported poll or endpoint policy")
    expected_mapping = tuple((product, product) for product in run.ordered_products)
    for manifest, rows, trade in (
        (run.trade_manifest, run.trade_bars, True),
        (run.mark_manifest, run.mark_bars, False),
    ):
        _validate_manifest(manifest, rows, trade=trade, products=run.ordered_products)
        expected_source = "P3_ORACLE_TRADE_V1" if trade else "P3_ORACLE_MARK_V1"
        expected_units = "CONTRACTS" if trade else "PRICE"
        if (manifest.source_id != expected_source or manifest.product_mapping != expected_mapping
                or manifest.units != expected_units):
            raise HistoricalInputError("product mapping or unit mismatch")
        if manifest.first_source_timestamp_ms > manifest.last_source_timestamp_ms:
            raise HistoricalInputError("invalid manifest timestamp range")

    expected_times = tuple(range(run.warmup_start_ms, run.range_end_ms, BAR_DURATION_MS))
    for rows in (run.trade_bars, run.mark_bars):
        keys: list[tuple[int, int]] = []
        for row in rows:
            if (type(row) is not HistoricalBar or row.product_id not in run.ordered_products
                    or type(row.confirm) is not int or row.confirm != 1):
                raise HistoricalInputError("unknown product or non-closed bar")
            if (type(row.bar_open_ms) is not int or type(row.source_sequence) is not int
                    or row.bar_open_ms % BAR_DURATION_MS or row.source_sequence < 0):
                raise HistoricalInputError("misaligned bar or invalid source sequence")
            for value in (row.open, row.high, row.low, row.close):
                _canonical_decimal(value)
                if value <= 0:
                    raise HistoricalInputError("OHLC prices must be positive")
            if rows is run.trade_bars:
                if type(row.volume) is not Decimal:
                    raise HistoricalInputError("trade volume must be Decimal")
                _canonical_decimal(row.volume)
                if row.volume < 0:
                    raise HistoricalInputError("negative trade volume")
            elif row.volume is not None:
                raise HistoricalInputError("mark volume must be absent")
            if row.high < max(row.open, row.close, row.low) or row.low > min(row.open, row.close, row.high):
                raise HistoricalInputError("invalid OHLC range")
            keys.append((row.bar_open_ms, run.ordered_products.index(row.product_id)))
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise HistoricalInputError("duplicate, out-of-order, or conflicting rows")
        by_product = {product: tuple(row.bar_open_ms for row in rows if row.product_id == product) for product in run.ordered_products}
        if any(by_product[product] != expected_times for product in run.ordered_products):
            raise HistoricalInputError("warmup/run coverage gap")

    if (type(run.spec_before) is not tuple or type(run.spec_after) is not tuple
            or any(type(item) is not InstrumentSpecEvidence for item in run.spec_before + run.spec_after)
            or tuple(item.product_id for item in run.spec_before) != run.ordered_products or tuple(
        item.product_id for item in run.spec_after
    ) != run.ordered_products):
        raise HistoricalInputError("missing product spec evidence")
    for before, after in zip(run.spec_before, run.spec_after, strict=True):
        if (type(before.effective_at_ms) is not int or type(after.effective_at_ms) is not int
                or before.effective_at_ms > run.range_start_ms or after.effective_at_ms < run.range_end_ms):
            raise HistoricalInputError("spec observations do not bracket run")
        if not _nonempty(before.product_id) or not _nonempty(after.product_id):
            raise HistoricalInputError("missing spec product identity")
        if (before.contract_value, before.multiplier, before.price_tick, before.quantity_step,
                before.minimum_quantity, before.tier_hash) != (
                after.contract_value, after.multiplier, after.price_tick, after.quantity_step,
                after.minimum_quantity, after.tier_hash):
            raise HistoricalInputError("spec/tier changed across run")
        for value in (before.contract_value, before.multiplier, before.price_tick,
                      before.quantity_step, before.minimum_quantity):
            _canonical_decimal(value)
            if value <= 0:
                raise HistoricalInputError("invalid product specification")
        if not _valid_hash(before.tier_hash) or not _valid_hash(after.tier_hash):
            raise HistoricalInputError("invalid tier evidence hash")
    configuration = decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products
    )
    for configured_product, evidence in zip(
        cast(list[dict[str, object]], configuration["products"]), run.spec_after, strict=True
    ):
        specs = cast(list[object], configured_product["specs"])
        endpoint_specs = [cast(dict[str, object], spec) for spec in specs
                          if type(spec) is dict
                          and cast(dict[str, object], spec).get("valid_from") == run.range_end_ms]
        if endpoint_specs:
            endpoint = endpoint_specs[0]
            if any(not _decimal_string(endpoint.get(field)) or
                Decimal(cast(str, endpoint[field])) != expected
                for field, expected in (
                    ("contract_value", evidence.contract_value),
                    ("multiplier", evidence.multiplier),
                    ("price_tick", evidence.price_tick),
                    ("quantity_step", evidence.quantity_step),
                    ("minimum_quantity", evidence.minimum_quantity),
                )
            ):
                raise HistoricalInputError("endpoint configured spec differs from run evidence")
    semantic_input = asdict(run)
    semantic_input.pop("run_id")
    semantic_input["parameters"] = _parameter_projection(run.parameters)
    return _digest(semantic_input)


_RUN_INPUT_KEYS = frozenset({
    "schema_id", "schema_version", "run_id", "account_key", "policy_id", "policy_version",
    "policy_source_sha256", "strategy_identity", "parameters", "configuration_bytes",
    "configuration_sha256", "ordered_products", "model_id", "model_version", "range_start_ms",
    "range_end_ms", "bar_duration_ms", "warmup_start_ms", "trade_manifest", "mark_manifest",
    "trade_bars", "mark_bars", "spec_before", "spec_after", "market_slippage_bps",
    "execution_fee_provenance", "timer_period_ms", "first_timer_ms", "local_clock_zone",
    "order_accept_delay_ms", "cancel_effect_delay_ms", "order_notice_delay_ms", "cancel_ack_delay_ms",
    "poll_profile", "endpoint_policy_id", "initial_policy_cache", "initial_account_state", "funding_mode",
})
_MANIFEST_KEYS = frozenset({
    "source_id", "provider", "endpoint", "request_parameters", "retrieved_at_utc", "http_status",
    "raw_sha256", "canonical_rows_sha256", "schema_version", "field_map_version", "product_mapping",
    "units", "source_row_count", "normalized_row_count", "first_source_timestamp_ms",
    "last_source_timestamp_ms", "duplicate_policy", "gap_policy", "evidence_label",
})
_BAR_KEYS = frozenset({
    "product_id", "bar_open_ms", "open", "high", "low", "close", "confirm", "source_sequence",
    "volume", "source_row_hash",
})
_SPEC_KEYS = frozenset({
    "product_id", "effective_at_ms", "contract_value", "multiplier", "price_tick", "quantity_step",
    "minimum_quantity", "tier_hash",
})


def _codec_object(value: object, keys: frozenset[str], label: str) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != keys:
        raise HistoricalInputError(f"invalid encoded {label} shape")
    return cast(dict[str, object], value)


def _codec_text(value: object, label: str) -> str:
    if type(value) is not str:
        raise HistoricalInputError(f"invalid encoded {label}")
    return cast(str, value)


def _codec_decimal(value: object, label: str) -> Decimal:
    text = _codec_text(value, label)
    try:
        decimal = Decimal(text)
    except (ValueError, ArithmeticError) as error:
        raise HistoricalInputError(f"invalid encoded {label}") from error
    if _canonical_decimal(decimal) != text:
        raise HistoricalInputError(f"noncanonical encoded {label}")
    return decimal


def _codec_bytes(value: object, label: str) -> bytes:
    text = _codec_text(value, label)
    if re.fullmatch(r"(?:[0-9a-f]{2})*", text) is None:
        raise HistoricalInputError(f"invalid encoded {label} hex")
    return bytes.fromhex(text)


def _codec_pairs(value: object, label: str) -> tuple[tuple[str, str], ...]:
    if type(value) is not list:
        raise HistoricalInputError(f"invalid encoded {label} pairs")
    pairs = []
    for item in cast(list[object], value):
        if type(item) is not list or len(cast(list[object], item)) != 2:
            raise HistoricalInputError(f"invalid encoded {label} pair")
        pair = cast(list[object], item)
        pairs.append((_codec_text(pair[0], label), _codec_text(pair[1], label)))
    return tuple(pairs)


def _manifest_value(manifest: SourceManifest) -> dict[str, object]:
    if type(manifest) is not SourceManifest:
        raise HistoricalInputError("invalid source manifest DTO")
    value = {key: getattr(manifest, key) for key in _MANIFEST_KEYS}
    value["request_parameters"] = [list(pair) for pair in manifest.request_parameters]
    value["product_mapping"] = [list(pair) for pair in manifest.product_mapping]
    return value


def _decode_manifest(value: object) -> SourceManifest:
    row = _codec_object(value, _MANIFEST_KEYS, "source manifest")
    decoded = dict(row)
    decoded["request_parameters"] = _codec_pairs(row["request_parameters"], "request_parameters")
    decoded["product_mapping"] = _codec_pairs(row["product_mapping"], "product_mapping")
    return SourceManifest(**cast(Any, decoded))


def _bar_value(bar: HistoricalBar) -> dict[str, object]:
    if type(bar) is not HistoricalBar:
        raise HistoricalInputError("invalid historical bar DTO")
    return {
        "product_id": bar.product_id, "bar_open_ms": bar.bar_open_ms, "open": _canonical_decimal(bar.open),
        "high": _canonical_decimal(bar.high), "low": _canonical_decimal(bar.low),
        "close": _canonical_decimal(bar.close), "confirm": bar.confirm,
        "source_sequence": bar.source_sequence,
        "volume": None if bar.volume is None else _canonical_decimal(bar.volume),
        "source_row_hash": bar.source_row_hash,
    }


def _decode_bar(value: object) -> HistoricalBar:
    row = _codec_object(value, _BAR_KEYS, "historical bar")
    decoded = dict(row)
    for field in ("open", "high", "low", "close"):
        decoded[field] = _codec_decimal(row[field], "bar " + field)
    if row["volume"] is not None:
        decoded["volume"] = _codec_decimal(row["volume"], "bar volume")
    return HistoricalBar(**cast(Any, decoded))


def _spec_value(spec: InstrumentSpecEvidence) -> dict[str, object]:
    if type(spec) is not InstrumentSpecEvidence:
        raise HistoricalInputError("invalid instrument spec DTO")
    return {
        "product_id": spec.product_id, "effective_at_ms": spec.effective_at_ms,
        "contract_value": _canonical_decimal(spec.contract_value), "multiplier": _canonical_decimal(spec.multiplier),
        "price_tick": _canonical_decimal(spec.price_tick), "quantity_step": _canonical_decimal(spec.quantity_step),
        "minimum_quantity": _canonical_decimal(spec.minimum_quantity), "tier_hash": spec.tier_hash,
    }


def _decode_spec(value: object) -> InstrumentSpecEvidence:
    row = _codec_object(value, _SPEC_KEYS, "instrument spec")
    decoded = dict(row)
    for field in ("contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity"):
        decoded[field] = _codec_decimal(row[field], "spec " + field)
    return InstrumentSpecEvidence(**cast(Any, decoded))


def encode_historical_run_input(run: HistoricalRunInput) -> bytes:
    """Encode the exact immutable historical input as canonical JSON bytes."""
    if type(run) is not HistoricalRunInput:
        raise HistoricalInputError("invalid run DTO")
    validate_historical_input(run)
    value: dict[str, object] = {key: getattr(run, key) for key in _RUN_INPUT_KEYS}
    value["parameters"] = _parameter_projection(run.parameters)
    for field in ("configuration_bytes", "initial_policy_cache", "initial_account_state"):
        value[field] = getattr(run, field).hex()
    value["ordered_products"] = list(run.ordered_products)
    value["trade_manifest"] = _manifest_value(run.trade_manifest)
    value["mark_manifest"] = _manifest_value(run.mark_manifest)
    value["trade_bars"] = [_bar_value(row) for row in run.trade_bars]
    value["mark_bars"] = [_bar_value(row) for row in run.mark_bars]
    value["spec_before"] = [_spec_value(row) for row in run.spec_before]
    value["spec_after"] = [_spec_value(row) for row in run.spec_after]
    value["market_slippage_bps"] = _canonical_decimal(run.market_slippage_bps)
    return _canonical(value)


def decode_historical_run_input(raw: bytes) -> HistoricalRunInput:
    """Decode canonical historical input bytes and revalidate before returning."""
    value = _decode_canonical(raw)
    row = _codec_object(value, _RUN_INPUT_KEYS, "historical run")
    raw_parameters = row["parameters"]
    if type(raw_parameters) is not list:
        raise HistoricalInputError("invalid encoded strategy parameters")
    parameters = []
    for item in cast(list[object], raw_parameters):
        parameter = _codec_object(item, frozenset({"key", "kind", "value"}), "strategy parameter")
        key = _codec_text(parameter["key"], "parameter key")
        kind = _codec_text(parameter["kind"], "parameter kind")
        if kind == "DECIMAL":
            parameter_value: str | Decimal = _codec_decimal(parameter["value"], "parameter value")
        elif kind == "STRING":
            parameter_value = _codec_text(parameter["value"], "parameter value")
        else:
            raise HistoricalInputError("unsupported encoded parameter kind")
        parameters.append((key, parameter_value))
    products = row["ordered_products"]
    if type(products) is not list:
        raise HistoricalInputError("invalid encoded ordered products")
    bars: dict[str, tuple[HistoricalBar, ...]] = {}
    specs: dict[str, tuple[InstrumentSpecEvidence, ...]] = {}
    for name in ("trade_bars", "mark_bars"):
        collection = row[name]
        if type(collection) is not list:
            raise HistoricalInputError(f"invalid encoded {name}")
        bars[name] = tuple(_decode_bar(item) for item in cast(list[object], collection))
    for name in ("spec_before", "spec_after"):
        collection = row[name]
        if type(collection) is not list:
            raise HistoricalInputError(f"invalid encoded {name}")
        specs[name] = tuple(_decode_spec(item) for item in cast(list[object], collection))
    decoded: dict[str, object] = dict(row)
    decoded.update({
        "parameters": tuple(parameters),
        "configuration_bytes": _codec_bytes(row["configuration_bytes"], "configuration bytes"),
        "ordered_products": tuple(_codec_text(product, "ordered product") for product in cast(list[object], products)),
        "trade_manifest": _decode_manifest(row["trade_manifest"]),
        "mark_manifest": _decode_manifest(row["mark_manifest"]),
        "trade_bars": bars["trade_bars"], "mark_bars": bars["mark_bars"],
        "spec_before": specs["spec_before"], "spec_after": specs["spec_after"],
        "market_slippage_bps": _codec_decimal(row["market_slippage_bps"], "market slippage"),
        "initial_policy_cache": _codec_bytes(row["initial_policy_cache"], "initial policy cache"),
        "initial_account_state": _codec_bytes(row["initial_account_state"], "initial account state"),
    })
    run = HistoricalRunInput(**cast(Any, decoded))
    validate_historical_input(run)
    if encode_historical_run_input(run) != raw:
        raise HistoricalInputError("noncanonical historical run encoding")
    return run


OwnerT = TypeVar("OwnerT")
StoreT = TypeVar("StoreT")


def admit_before_construction(
    run: HistoricalRunInput,
    owner_factory: Callable[[str], OwnerT],
    store_factory: Callable[[str], StoreT],
) -> tuple[str, OwnerT, StoreT]:
    """Small injectable proof seam: validation always precedes owner/store calls."""
    contract_hash = validate_historical_input(run)
    owner = owner_factory(contract_hash)
    store = store_factory(contract_hash)
    return contract_hash, owner, store
