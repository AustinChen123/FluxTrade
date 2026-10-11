"""Private, admitted MODEL initialization for the guarded Spider runtime."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal, DecimalException, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import cast

from src.core.backtest.spider_historical_input import (
    HistoricalInputError,
    HistoricalRunInput,
    _spec_value,
    decode_p2_configuration,
    encode_historical_run_input,
    historical_context_for_input,
    validate_historical_input,
)
from src.core.backtest.spider_historical_replay import (
    _prepare_historical_replay,
    _restore_initial_policy,
)
from src.core.backtest.spider_policy import DEFAULTS
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.spider_run_native_schema import (
    ConfigurationContext,
    inspection as validate_inspection,
    snapshot_fact as validate_snapshot_fact,
    snapshot_request as validate_snapshot_request,
)
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.backtest.synthetic_scenario_codec import loaded_native_artifact_path
from src.core.product_registry import (
    InstrumentSpec,
    MarketType,
    _parse_product_id,
)
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequestKind,
    SpiderConfigurationRequestRecord,
)
from src.core.spider_management.source_checkpoint_codec import (
    ModelBootstrapUnavailable,
    encode_source_checkpoint,
)


_SELECTION_KEYS = frozenset(
    {
        "schema_version",
        "environment",
        "execution_scope_id",
        "strategy_id",
        "confirmation_request_id",
        "configuration_sha256",
        "host_contract_id",
        "host_contract_sha256",
        "account_profile",
        "account_id",
        "provider_profile_sha256",
        "build_policy_id",
        "time_policy_id",
        "time_zone",
    }
)
@dataclass(frozen=True, slots=True)
class PreparedModelBootstrap:
    runtime_selection: dict[str, object]
    historical_input_sha256: str
    historical_input_artifact_sha256: str
    historical_provenance: dict[str, object]
    native_artifact_sha256: str
    source_checkpoint_bytes: bytes
    source_checkpoint_sha256: str
    initial_source_events_bytes: bytes
    initial_source_events_sha256: str
    build_checkpoint_bytes: bytes
    build_checkpoint_sha256: str
    owner_evidence: dict[str, object]
    owner_evidence_bytes: bytes
    specification_sha256: str
    product_map: dict[str, object]
    product_map_sha256: str
    valuation_sha256: str
    bootstrap_basis: dict[str, object]
    bootstrap_digest: str
    financial_cursor: str
    tape_cursor: str
    initial_manifest: dict[str, object]


def _sha(raw: bytes) -> str:
    return sha256(raw).hexdigest()


def _decimal_text(value: Decimal | None) -> str:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    from src.core.decimal_math import canonical_decimal_text

    return canonical_decimal_text(value)


def _model_product_map(
    run: HistoricalRunInput,
    specifications: tuple[tuple[str, InstrumentSpec], ...],
    selected_venue: str,
    specification_sha256: str,
) -> tuple[dict[str, object], str]:
    if (type(specifications) is not tuple or len(specifications) != len(run.ordered_products)
            or type(selected_venue) is not str or not selected_venue):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    sources: list[str] = []
    targets: list[str] = []
    entries: list[dict[str, object]] = []
    evidence_by_source = dict(zip(run.ordered_products, run.spec_before, strict=True))
    for pair, source in zip(specifications, run.ordered_products, strict=True):
        if type(pair) is not tuple or len(pair) != 2 or pair[0] != source:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        instrument = pair[1]
        if type(instrument) is not InstrumentSpec:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        parsed = _parse_product_id(instrument.product_id)
        if (parsed["market_type"] != MarketType.PERPETUAL.value
                or instrument.market_type is not MarketType.PERPETUAL
                or instrument.exchange != selected_venue
                or instrument.exchange != parsed["exchange"]
                or instrument.symbol != parsed.get("symbol", parsed.get("ccxt"))
                or instrument.base != parsed["base"]
                or instrument.quote != parsed["quote"]):
            raise ModelBootstrapUnavailable("model_source_unavailable")
        evidence = evidence_by_source[source]
        if (instrument.quantity_step != evidence.quantity_step
                or instrument.price_tick != evidence.price_tick
                or instrument.min_quantity != evidence.minimum_quantity
                or instrument.multiplier != evidence.contract_value * evidence.multiplier):
            raise ModelBootstrapUnavailable("model_source_unavailable")
        sources.append(source)
        targets.append(instrument.product_id)
        entries.append({
            "source_product_id": source,
            "instrument_spec": {
                "product_id": instrument.product_id,
                "exchange": instrument.exchange,
                "symbol": instrument.symbol,
                "base": instrument.base,
                "quote": instrument.quote,
                "market_type": instrument.market_type.value,
                "quantity_step": _decimal_text(instrument.quantity_step),
                "price_tick": _decimal_text(instrument.price_tick),
                "min_quantity": _decimal_text(instrument.min_quantity),
                "multiplier": _decimal_text(instrument.multiplier),
            },
        })
    if len(set(sources)) != len(sources) or len(set(targets)) != len(targets):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    projection: dict[str, object] = {
        "domain": "SPIDER_MODEL_PRODUCT_MAP_V1",
        "specification_sha256": specification_sha256,
        "ordered_products": list(run.ordered_products),
        "entries": entries,
    }
    return projection, _sha(canonical_bytes(projection))


def _model_scope(value: object) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != _SELECTION_KEYS:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    selection = cast(dict[str, object], value)
    if selection["schema_version"] != "SPIDER_RUNTIME_SELECTION_V1":
        raise ModelBootstrapUnavailable("model_source_unavailable")
    for key in _SELECTION_KEYS - {"schema_version", "configuration_sha256", "host_contract_sha256", "provider_profile_sha256"}:
        if type(selection[key]) is not str or not cast(str, selection[key]):
            raise ModelBootstrapUnavailable("model_source_unavailable")
    for key in ("configuration_sha256", "host_contract_sha256", "provider_profile_sha256"):
        item = selection[key]
        if type(item) is not str or len(item) != 64 or any(char not in "0123456789abcdef" for char in item):
            raise ModelBootstrapUnavailable("model_source_unavailable")
    return deepcopy(selection)


def _initial_source_events(
    run: HistoricalRunInput, composition: _ReplayComposition
) -> bytes:
    """Capture native initial emissions once, without consuming or dispatching them."""
    policy = composition._policy
    entries: list[dict[str, object]] = []
    for product in run.ordered_products:
        start = len(policy.events)
        policy.start_strategy(product)
        emitted = policy.events[start:]
        if not emitted or any(
            type(event) is not dict
            or event.get("kind") not in {"request_market", "send"}
            for event in emitted
        ):
            raise ModelBootstrapUnavailable("model_source_unavailable")
        sends = [event for event in emitted if event["kind"] == "send"]
        if not sends:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        for event in sends:
            orders = event.get("orders")
            if type(orders) is not list or not orders:
                raise ModelBootstrapUnavailable("model_source_unavailable")
            for order in orders:
                if (
                    type(order) is not dict
                    or order.get("instId") != product
                    or order.get("side") not in {"buy", "sell"}
                    or type(order.get("clOrdId")) is not str
                    or not order["clOrdId"]
                    or order.get("ordType") != "limit"
                    or type(order.get("px")) is not str
                    or type(order.get("sz")) is not str
                ):
                    raise ModelBootstrapUnavailable("model_source_unavailable")
                try:
                    price, quantity = Decimal(order["px"]), Decimal(order["sz"])
                except InvalidOperation:
                    raise ModelBootstrapUnavailable("model_source_unavailable") from None
                if not price.is_finite() or price <= 0 or not quantity.is_finite() or quantity <= 0:
                    raise ModelBootstrapUnavailable("model_source_unavailable")
        entries.append({"source_product_id": product, "events": emitted})
    if len(entries) != len(run.ordered_products):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    return canonical_bytes({
        "domain": "SPIDER_INITIAL_SOURCE_EVENTS_V1",
        "ordered_products": list(run.ordered_products),
        "entries": entries,
    })


def _manual_configuration_matches_source(
    confirmation: SpiderConfigurationRequestRecord,
    selection: dict[str, object],
    run: HistoricalRunInput,
) -> None:
    if type(confirmation) is not SpiderConfigurationRequestRecord:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    request = confirmation.request
    configuration = request.configuration.value
    if (
        request.request_kind is not SpiderConfigurationRequestKind.CONFIRMATION
        or request.request_id != selection["confirmation_request_id"]
        or request.configuration.sha256 != selection["configuration_sha256"]
        or request.environment != selection["environment"]
        or request.execution_scope_id != selection["execution_scope_id"]
        or request.strategy_id != selection["strategy_id"]
        or configuration["ordered_products"] != list(run.ordered_products)
        or configuration["policy_id"] not in {
            "SPIDER_HOST_ORIGINAL_PAID_V1",
            "SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1",
        }
        or run.policy_id != "SPIDER_GRID_ORIGINAL_V1"
        or run.policy_version != "1"
        or run.policy_source_sha256
        != "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec"
    ):
        raise ModelBootstrapUnavailable("model_source_unavailable")

    effective_parameters = DEFAULTS | dict(run.parameters)
    manual_parameters = cast(dict[str, object], configuration["parameters"])
    if set(effective_parameters) != set(manual_parameters) or any(
        type(effective_parameters[key]) not in (str, Decimal)
        or Decimal(cast(str | Decimal, effective_parameters[key]))
        != Decimal(cast(str, manual_parameters[key]))
        for key in DEFAULTS
    ):
        raise ModelBootstrapUnavailable("model_source_unavailable")

    cache = decode_canonical(run.initial_policy_cache)
    if type(cache) is not dict or type(cache.get("rows")) is not list:
        raise ModelBootstrapUnavailable("model_source_unavailable")
    rows = cast(list[object], cache["rows"])
    settings = cast(list[object], configuration["product_settings"])
    if len(rows) != len(run.ordered_products) or len(settings) != len(rows):
        raise ModelBootstrapUnavailable("model_source_unavailable")
    by_product = {
        cast(dict[str, object], row).get("name"): row
        for row in rows
        if type(row) is dict
    }
    for product, setting_value in zip(run.ordered_products, settings, strict=True):
        source_row = by_product.get(product)
        if type(source_row) is not dict or type(setting_value) is not dict:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        source = cast(dict[str, object], source_row)
        setting = cast(dict[str, object], setting_value)
        if source.get("active") != setting.get("active") or any(
            type(source.get(field)) not in (str, Decimal)
            or Decimal(cast(str | Decimal, source[field]))
            != Decimal(cast(str, setting[field]))
            for field in ("leverage", "歩差", "單數", "hold上限", "hold下限")
        ):
            raise ModelBootstrapUnavailable("model_source_unavailable")


def prepare_model_bootstrap(
    run: HistoricalRunInput,
    runtime_selection: object,
    configuration_confirmation: SpiderConfigurationRequestRecord,
    *,
    product_specifications: tuple[tuple[str, InstrumentSpec], ...],
    selected_venue: str,
) -> PreparedModelBootstrap:
    """Initialize one admitted research owner and capture its exact initial state."""
    selection = _model_scope(runtime_selection)
    try:
        historical_input_sha256 = validate_historical_input(run)
        historical_input_artifact_sha256 = _sha(encode_historical_run_input(run))
        if run.account_key != selection["account_id"]:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        _manual_configuration_matches_source(configuration_confirmation, selection, run)
        configuration = decode_p2_configuration(
            run.configuration_bytes, run.configuration_sha256, run.ordered_products
        )
        if tuple(run.ordered_products) != tuple(
            cast(str, item["product_id"])
            for item in cast(list[dict[str, object]], configuration["products"])
        ):
            raise ModelBootstrapUnavailable("model_source_unavailable")
        prepared = _prepare_historical_replay(run)
        composition = _ReplayComposition(
            "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
            prepared.account,
            configuration=prepared.configuration,
        )
        _restore_initial_policy(composition, run, prepared)
        native_account = prepared.account
        cutoff = composition._policy.now_ms
        owner_native = composition.capture_owner_evidence(
            cutoff,
            composition._paid_snapshot_request("TRADING", cutoff, "MODEL_BOOTSTRAP"),
            composition._paid_snapshot_request("POSITIONS", cutoff, "MODEL_BOOTSTRAP"),
            composition._paid_snapshot_request("OPEN_ORDERS", cutoff, "MODEL_BOOTSTRAP"),
        )["owner_evidence"]
        owner = cast(dict[str, object], decode_canonical(canonical_bytes(owner_native)))
        context = ConfigurationContext(
            "spider_configuration_context_v1",
            cast(str, configuration["config_id"]),
            run.configuration_sha256,
            run.ordered_products,
        )
        validate_inspection(owner["inspection"], context=context)
        for kind in ("trading", "positions", "open_orders"):
            validate_snapshot_request(owner[kind + "_request"])
            validate_snapshot_fact(owner[kind + "_fact"], context=context)
        inspection = cast(dict[str, object], owner["inspection"])
        account_key = cast(dict[str, object], inspection["account_key"])
        if account_key.get("account") != selection["account_id"]:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        account_version = inspection["account_version"]
        if type(account_version) is not int or account_version < 0:
            raise ModelBootstrapUnavailable("model_source_unavailable")
        if any(
            cast(dict[str, object], owner[kind + "_fact"]).get("captured_account_version")
            != account_version
            for kind in ("trading", "positions", "open_orders")
        ):
            raise ModelBootstrapUnavailable("model_source_unavailable")
        if any(
            cast(dict[str, object], owner[kind + "_fact"]).get("snapshot_as_of") != cutoff
            or cast(dict[str, object], owner[kind + "_request"]).get("account_key")
            != inspection["account_key"]
            for kind in ("trading", "positions", "open_orders")
        ):
            raise ModelBootstrapUnavailable("model_source_unavailable")

        history = historical_context_for_input(run)
        checkpoint_bytes = encode_source_checkpoint(
            composition._policy,
            ordered_products=run.ordered_products,
            policy_id=run.policy_id,
            policy_version=run.policy_version,
            policy_source_sha256=run.policy_source_sha256,
            source_sha256=cast(str, history["source_sha256"]),
            confirmation_request_id=cast(str, selection["confirmation_request_id"]),
            configuration_sha256=cast(str, selection["configuration_sha256"]),
        )
        source_checkpoint_sha256 = _sha(checkpoint_bytes)
        native_artifact_sha256 = _sha(Path(loaded_native_artifact_path()).read_bytes())
        specifications = canonical_bytes({
            "domain": "SPIDER_MODEL_ACTIVE_SPEC_V1",
            "ordered_products": list(run.ordered_products),
            "spec_before": [_spec_value(spec) for spec in run.spec_before],
        })
        specification_sha256 = _sha(specifications)
        product_map, product_map_sha256 = _model_product_map(
            run, product_specifications, selected_venue, specification_sha256
        )
        valuation_sha256 = cast(str, inspection["valuation_context_id"])
        owner_bytes = canonical_bytes(owner)
        basis = {
            "domain": "SPIDER_MODEL_BOOTSTRAP_BASIS_V1",
            "runtime_selection": selection,
            "historical_input_sha256": historical_input_sha256,
            "native_artifact_sha256": native_artifact_sha256,
            "source_checkpoint_sha256": source_checkpoint_sha256,
            "owner_evidence": owner,
            "initial_tape_sequence": 0,
            "specification_sha256": specification_sha256,
            "product_map_sha256": product_map_sha256,
            "valuation_sha256": valuation_sha256,
        }
        basis_digest = _sha(canonical_bytes(basis))
        if type(native_account) is not dict or native_account.get("environment") != "RESEARCH_ONLY":
            raise ModelBootstrapUnavailable("model_source_unavailable")
        source_events_bytes = _initial_source_events(run, composition)
        build_checkpoint_bytes = encode_source_checkpoint(
            composition._policy,
            ordered_products=run.ordered_products,
            policy_id=run.policy_id,
            policy_version=run.policy_version,
            policy_source_sha256=run.policy_source_sha256,
            source_sha256=cast(str, history["source_sha256"]),
            confirmation_request_id=cast(str, selection["confirmation_request_id"]),
            configuration_sha256=cast(str, selection["configuration_sha256"]),
        )
        source_events_sha256 = _sha(source_events_bytes)
        build_checkpoint_sha256 = _sha(build_checkpoint_bytes)
        manifest = {
            "historical_input_sha256": historical_input_sha256,
            "historical_input_artifact_sha256": historical_input_artifact_sha256,
            "historical_provenance": history,
            "native_artifact_sha256": native_artifact_sha256,
            "source_checkpoint_sha256": source_checkpoint_sha256,
            "owner_evidence_sha256": _sha(owner_bytes),
            "bootstrap_basis_sha256": basis_digest,
            "specification_sha256": specification_sha256,
            "product_map": product_map,
            "product_map_sha256": product_map_sha256,
            "valuation_sha256": valuation_sha256,
            "initial_source_events_sha256": source_events_sha256,
            "build_checkpoint_sha256": build_checkpoint_sha256,
        }
        return PreparedModelBootstrap(
            runtime_selection=selection,
            historical_input_sha256=historical_input_sha256,
            historical_input_artifact_sha256=historical_input_artifact_sha256,
            historical_provenance=history,
            native_artifact_sha256=native_artifact_sha256,
            source_checkpoint_bytes=checkpoint_bytes,
            source_checkpoint_sha256=source_checkpoint_sha256,
            initial_source_events_bytes=source_events_bytes,
            initial_source_events_sha256=source_events_sha256,
            build_checkpoint_bytes=build_checkpoint_bytes,
            build_checkpoint_sha256=build_checkpoint_sha256,
            owner_evidence=owner,
            owner_evidence_bytes=owner_bytes,
            specification_sha256=specification_sha256,
            product_map=product_map,
            product_map_sha256=product_map_sha256,
            valuation_sha256=valuation_sha256,
            bootstrap_basis=basis,
            bootstrap_digest=basis_digest,
            financial_cursor=f"model-financial-v1:{basis_digest}:{account_version}",
            tape_cursor=f"model-tape-v1:{basis_digest}:0",
            initial_manifest=manifest,
        )
    except ModelBootstrapUnavailable:
        raise
    except (
        HistoricalInputError,
        DecimalException,
        OSError,
        TypeError,
        ValueError,
        KeyError,
        IndexError,
        OverflowError,
        AttributeError,
    ) as exc:
        raise ModelBootstrapUnavailable("model_source_unavailable") from exc


__all__ = ["ModelBootstrapUnavailable", "PreparedModelBootstrap", "prepare_model_bootstrap"]
