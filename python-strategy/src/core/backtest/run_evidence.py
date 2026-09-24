"""Deterministic evidence identities shared by both backtest runners."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import tomllib
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from src.core.decimal_math import canonical_decimal_text
from src.core.models import Candlestick, OrderSide
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements

_DATASET_SCHEMA = "fluxtrade.backtest_dataset.v1"
_CONFIGURATION_SCHEMA = "fluxtrade.backtest_configuration.v1"
_PROGRAM_SCHEMA = "fluxtrade.backtest_program.v1"


@dataclass(frozen=True, slots=True)
class BacktestFillRecord:
    """Provider-ID-free fill projection suitable for runner parity checks."""

    fill_sequence: int
    strategy_id: str | None
    product_id: str
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal
    fee_asset: str | None
    timestamp: int


@dataclass(frozen=True, slots=True)
class BacktestRunProvenance:
    """Immutable identities required to reproduce one runner result."""

    schema_version: str
    runner_kind: str
    dataset_identity_version: str
    dataset_sha256: str
    dataset_candle_count: int
    dataset_first_timestamp: int | None
    dataset_last_timestamp: int | None
    program_version: str
    program_sha256: str
    extension_version: str
    extension_sha256: str
    configuration_identity_version: str
    configuration_sha256: str
    runner_configuration_sha256: str
    matching_model: str


class BacktestDatasetEvidence:
    """Incrementally hash the exact ordered candles consumed by a runner."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(f"{_DATASET_SCHEMA}\n".encode())
        self._count = 0
        self._first_timestamp: int | None = None
        self._last_timestamp: int | None = None

    @property
    def count(self) -> int:
        return self._count

    @property
    def first_timestamp(self) -> int | None:
        return self._first_timestamp

    @property
    def last_timestamp(self) -> int | None:
        return self._last_timestamp

    def observe(self, candle: Candlestick) -> None:
        if type(candle) is not Candlestick:
            raise TypeError("dataset evidence requires exact Candlestick instances")
        if self._last_timestamp is not None and candle.timestamp < self._last_timestamp:
            raise ValueError("dataset evidence timestamps must be non-decreasing")
        payload = (
            candle.product_id,
            candle.timeframe,
            candle.timestamp,
            canonical_decimal_text(candle.open),
            canonical_decimal_text(candle.high),
            canonical_decimal_text(candle.low),
            canonical_decimal_text(candle.close),
            canonical_decimal_text(candle.volume),
        )
        self._digest.update(
            json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
        )
        self._digest.update(b"\n")
        if self._first_timestamp is None:
            self._first_timestamp = candle.timestamp
        self._last_timestamp = candle.timestamp
        self._count += 1

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def configuration_sha256(configuration: object) -> str:
    """Hash an exact, context-independent canonical configuration payload."""
    return _canonical_sha256(_CONFIGURATION_SCHEMA, configuration)


def _requirements_projection(requirements: StrategyRequirements) -> dict[str, object]:
    projection: dict[str, object] = {
        "product_id": requirements.product_id,
        "timeframe": requirements.timeframe,
        "lookback_window": requirements.lookback_window,
        "required_context_capabilities": requirements.required_context_capabilities,
    }
    if requirements.profile_requirements:
        projection["profile_requirements"] = [
            json.loads(item.canonical_bytes)
            for item in requirements.profile_requirements
        ]
    return projection


def strategy_configuration_contract(
    strategies: Sequence[BaseStrategy],
) -> tuple[dict[str, object], ...]:
    """Project the strategy identity/configuration that affects decisions."""
    contracts: list[dict[str, object]] = []
    for strategy in strategies:
        try:
            replay_configuration = strategy.replay_configuration()
            replay_configuration_declared = True
        except NotImplementedError:
            replay_configuration = None
            replay_configuration_declared = False
        contracts.append(
            {
                "strategy_id": strategy.strategy_id,
                "class_module": type(strategy).__module__,
                "class_name": type(strategy).__qualname__,
                "requirements": _requirements_projection(strategy.requirements),
                "replay_configuration_declared": replay_configuration_declared,
                "replay_configuration": replay_configuration,
            }
        )
    return tuple(contracts)


def build_backtest_run_provenance(
    *,
    runner_kind: str,
    dataset: BacktestDatasetEvidence,
    configuration: object,
    runner_configuration: object,
    program_components: Sequence[type[object]],
) -> BacktestRunProvenance:
    if runner_kind not in {"full", "research"}:
        raise ValueError("runner_kind must be full or research")
    program_version, program_sha256 = _program_identity(
        (BacktestDatasetEvidence, *program_components)
    )
    extension_version, extension_sha256 = _extension_identity()
    return BacktestRunProvenance(
        schema_version="fluxtrade.backtest_run_provenance.v1",
        runner_kind=runner_kind,
        dataset_identity_version=_DATASET_SCHEMA,
        dataset_sha256=dataset.hexdigest(),
        dataset_candle_count=dataset.count,
        dataset_first_timestamp=dataset.first_timestamp,
        dataset_last_timestamp=dataset.last_timestamp,
        program_version=program_version,
        program_sha256=program_sha256,
        extension_version=extension_version,
        extension_sha256=extension_sha256,
        configuration_identity_version=_CONFIGURATION_SCHEMA,
        configuration_sha256=configuration_sha256(configuration),
        runner_configuration_sha256=configuration_sha256(runner_configuration),
        matching_model="atomic_whole_order_fixed_market_slippage_v2",
    )


def canonical_fill_records(trades: Iterable[object]) -> tuple[BacktestFillRecord, ...]:
    records: list[BacktestFillRecord] = []
    for sequence, trade in enumerate(trades):
        source_sequence = getattr(trade, "fill_sequence", None)
        if source_sequence is not None and (
            type(source_sequence) is not int or source_sequence != sequence
        ):
            raise ValueError("fill_sequence must be an exact contiguous integer")
        side = getattr(trade, "side")
        if isinstance(side, OrderSide):
            side = side.value
        if type(side) is not str or side not in {"buy", "sell"}:
            raise ValueError("fill side must be buy or sell")
        fee = getattr(trade, "fee", None)
        if fee is None:
            fee = Decimal("0")
        values = {
            "price": getattr(trade, "price"),
            "quantity": getattr(trade, "quantity"),
            "fee": fee,
        }
        for field_name, value in values.items():
            if type(value) is not Decimal or not value.is_finite():
                raise ValueError(f"fill {field_name} must be a finite exact Decimal")
        if values["price"] <= 0 or values["quantity"] <= 0 or values["fee"] < 0:
            raise ValueError("fill price/quantity must be positive and fee nonnegative")
        strategy_id = getattr(trade, "strategy_id", None)
        if strategy_id is not None and (
            type(strategy_id) is not str or not strategy_id.strip()
        ):
            raise ValueError("fill strategy_id must be a non-empty exact string")
        product_id = getattr(trade, "product_id")
        if type(product_id) is not str or not product_id.strip():
            raise ValueError("fill product_id must be a non-empty exact string")
        fee_asset = getattr(trade, "fee_asset", None)
        if fee_asset is not None and (
            type(fee_asset) is not str or not fee_asset.strip()
        ):
            raise ValueError("fill fee_asset must be a non-empty exact string")
        timestamp = getattr(trade, "timestamp")
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("fill timestamp must be a nonnegative exact integer")
        records.append(
            BacktestFillRecord(
                fill_sequence=sequence,
                strategy_id=strategy_id,
                product_id=product_id,
                side=side,
                price=values["price"],
                quantity=values["quantity"],
                fee=values["fee"],
                fee_asset=fee_asset,
                timestamp=timestamp,
            )
        )
    return tuple(records)


def canonical_decision_snapshot(context: StrategyContext) -> dict[str, object]:
    """Project decision semantics while excluding opaque local/provider IDs."""
    if type(context) is not StrategyContext:
        raise TypeError("decision evidence requires an exact StrategyContext")
    projection: dict[str, object] = {
        "strategy_id": context.strategy_id,
        "product_id": context.product_id,
        "timestamp": context.timestamp,
        "available_cash": context.available_cash,
        "total_equity": context.total_equity,
        "realized_pnl": context.realized_pnl,
        "unrealized_pnl": context.unrealized_pnl,
        "current_drawdown": context.current_drawdown,
        "max_drawdown": context.max_drawdown,
        "position": context.position,
        "open_orders": tuple(
            {
                "product_id": order.product_id,
                "side": order.side,
                "order_type": order.order_type,
                "quantity": order.quantity,
                "timestamp": order.timestamp,
                "price": order.price,
                "status": order.status,
            }
            for order in context.open_orders
        ),
        "latest_fills": tuple(
            {
                "product_id": fill.product_id,
                "side": fill.side,
                "price": fill.price,
                "quantity": fill.quantity,
                "fee": fill.fee,
                "timestamp": fill.timestamp,
            }
            for fill in context.latest_fills
        ),
        "latest_rejections": tuple(
            {
                "reason": rejection.reason,
                "timestamp": rejection.timestamp,
            }
            for rejection in context.latest_rejections
        ),
        "risk": context.risk,
        "capital": context.capital,
    }
    if context.market_data is not None:
        projection["market_data"] = json.loads(context.market_data.canonical_bytes)
    return projection


def _canonical_sha256(schema: str, value: object) -> str:
    payload = json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    digest = hashlib.sha256()
    digest.update(f"{schema}\n".encode())
    digest.update(payload)
    return digest.hexdigest()


def _canonical_value(value: object) -> object:
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is Decimal:
        return {"decimal": canonical_decimal_text(value)}
    if type(value) is float:
        return {"decimal": canonical_decimal_text(Decimal(str(value)))}
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonical_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        projected: dict[str, object] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("configuration mapping keys must be exact strings")
            projected[key] = _canonical_value(item)
        return projected
    if isinstance(value, (tuple, list)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_canonical_value(item) for item in value]
        return sorted(
            items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
    raise TypeError(
        "configuration contains unsupported provenance value: "
        f"{type(value).__module__}.{type(value).__qualname__}"
    )


@lru_cache(maxsize=128)
def _program_identity(
    components: tuple[type[object], ...],
) -> tuple[str, str]:
    if not components:
        raise ValueError("program provenance requires at least one component")
    digest = hashlib.sha256()
    digest.update(f"{_PROGRAM_SCHEMA}\n".encode())
    package_root = Path(__file__).resolve().parents[2]
    source_files = sorted(package_root.rglob("*.py"))
    if not source_files:
        raise RuntimeError("program provenance package sources are unavailable")
    for source_path in source_files:
        digest.update(source_path.relative_to(package_root).as_posix().encode())
        digest.update(b"\n")
        digest.update(source_path.read_bytes())
        digest.update(b"\n")

    external_sources: dict[Path, set[tuple[str, str]]] = {}
    for component in components:
        if not isinstance(component, type):
            raise TypeError("program provenance components must be types")
        identity = (component.__module__, component.__qualname__)
        raw_source_path = inspect.getsourcefile(component)
        if raw_source_path is None:
            raise RuntimeError(
                "program provenance source is unavailable for "
                f"{identity[0]}.{identity[1]}"
            )
        source_path = Path(raw_source_path).resolve()
        if source_path.is_relative_to(package_root):
            continue
        external_sources.setdefault(source_path, set()).add(identity)

    for source_path, identities in sorted(
        external_sources.items(),
        key=lambda item: sorted(item[1]),
    ):
        for module_name, qualified_name in sorted(identities):
            digest.update(f"{module_name}:{qualified_name}\n".encode())
        digest.update(Path(source_path).read_bytes())
        digest.update(b"\n")
    return _project_version(), digest.hexdigest()


@lru_cache(maxsize=1)
def _project_version() -> str:
    root_version = Path(__file__).resolve().parents[4] / "VERSION"
    if root_version.is_file():
        value = root_version.read_text(encoding="utf-8").strip()
        if value:
            return value
    return "unknown"


@lru_cache(maxsize=1)
def _extension_identity() -> tuple[str, str]:
    extension = importlib.import_module("fluxtrade_core")
    source_path = _native_extension_path(extension)
    try:
        extension_version = version("rust-data-service")
    except PackageNotFoundError:
        manifest = Path(__file__).resolve().parents[4] / "rust-data-service/Cargo.toml"
        if not manifest.is_file():
            extension_version = "unknown"
        else:
            with manifest.open("rb") as file:
                package = tomllib.load(file).get("package", {})
            raw_version = package.get("version")
            extension_version = (
                raw_version
                if type(raw_version) is str and raw_version.strip()
                else "unknown"
            )
    return extension_version, hashlib.sha256(source_path.read_bytes()).hexdigest()


def _native_extension_path(module: object) -> Path:
    """Resolve both installed-package and CI top-level extension layouts."""
    candidates = (getattr(module, "fluxtrade_core", None), module)
    for candidate in candidates:
        source_path = getattr(candidate, "__file__", None)
        if type(source_path) is str and any(
            source_path.endswith(suffix) for suffix in EXTENSION_SUFFIXES
        ):
            return Path(source_path)
    raise RuntimeError("native extension path is unavailable")
