"""Pure bootstrap cutover evidence, not proof of persistence or callback execution.

C is the first recorded bar start, excluded from the durable key. The caller
authoritatively supplies completed (never pending) suffix evidence. No latest
lookup, clock, provider, replay execution, or fallback is performed here.
"""

from dataclasses import InitVar, asdict, dataclass, fields
from decimal import Decimal, DecimalException
from enum import Enum
import hashlib
import json
from typing import Any, Literal

from src.core.data_provider import timeframe_to_ms
from src.core.decimal_math import canonical_decimal_text

from .decision_application import MarketDataDecisionKey, MarketDataDecisionOutcome
from .decision_context import ProfileDecisionBasis, StrategyMarketDataContext
from .context_enrichment import validate_profile_context_coverage
from .decision_input import (
    MarketDataDecisionInput,
    MAX_INPUT_PROFILES,
    MAX_INPUT_BYTES,
    MAX_INPUT_NODES,
    MAX_INPUT_BINS_PER_PROFILE,
    _context,
    _decimal_text,
    _keys,
)
from .publication import _canonical
from .read_types import _hex, _integer, _safe
from .requirements import ProfileRequirement
from .types import _decimal

MAX_BOOTSTRAP_BYTES = 32 * 1024 * 1024


class BootstrapSeedError(ValueError):
    def __init__(self, reason: str = "INVALID") -> None:
        super().__init__(f"PROFILE_BOOTSTRAP_{reason}")


def _bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _requirements(
    values: tuple[ProfileRequirement, ...],
) -> tuple[ProfileRequirement, ...]:
    if (
        type(values) is not tuple
        or len(values) > MAX_INPUT_PROFILES
        or any(type(value) is not ProfileRequirement for value in values)
    ):
        raise ValueError
    ordered = tuple(sorted(values, key=lambda value: value.canonical_bytes))
    if len({value.canonical_bytes for value in ordered}) != len(ordered):
        raise ValueError
    return ordered


@dataclass(frozen=True, slots=True)
class BootstrapKey:
    environment: str
    execution_scope_id: str
    strategy_id: str
    strategy_version: str
    config_hash: str
    product_id: str
    timeframe: str
    contract_version: int = 1

    def __post_init__(self) -> None:
        try:
            if type(self.contract_version) is not int or self.contract_version != 1:
                raise ValueError
            self.decision_key(0)
            _safe(self.timeframe, 32)
            if timeframe_to_ms(self.timeframe) <= 0:
                raise ValueError
        except (ValueError, TypeError, IndexError):
            raise BootstrapSeedError() from None

    def decision_key(self, bar_start_ms: int) -> MarketDataDecisionKey:
        return MarketDataDecisionKey.for_candle(
            environment=self.environment,
            execution_scope_id=self.execution_scope_id,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            config_hash=self.config_hash,
            product_id=self.product_id,
            timeframe=self.timeframe,
            bar_start_ms=bar_start_ms,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return _bytes({"schema_version": 1, **asdict(self)})

    @property
    def seed_id(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class BootstrapHistoryEvidence:
    """Trusted-reader result; exact typing alone does not establish DB authority."""

    key: BootstrapKey
    boundary_bar_start_ms: int
    state: Literal["ABSENT", "PRESENT", "UNKNOWN"]

    def __post_init__(self) -> None:
        try:
            _integer(self.boundary_bar_start_ms)
            if (
                type(self.key) is not BootstrapKey
                or type(self.state) is not str
                or self.state not in ("ABSENT", "PRESENT", "UNKNOWN")
                or self.boundary_bar_start_ms % timeframe_to_ms(self.key.timeframe)
            ):
                raise ValueError
        except ValueError:
            raise BootstrapSeedError("HISTORY_EVIDENCE_INVALID") from None


@dataclass(frozen=True, slots=True)
class BootstrapCandle:
    bar_start_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    context: StrategyMarketDataContext

    def __post_init__(self) -> None:
        try:
            _integer(self.bar_start_ms)
            if type(self.context) is not StrategyMarketDataContext:
                raise ValueError
            for name in ("open", "high", "low", "close", "volume"):
                value = _decimal(getattr(self, name), positive=name != "volume")
                if value < 0:
                    raise ValueError
                object.__setattr__(self, name, value)
            if (
                not self.low
                <= min(self.open, self.close)
                <= max(self.open, self.close)
                <= self.high
            ):
                raise ValueError
        except (ValueError, TypeError):
            raise BootstrapSeedError() from None


@dataclass(frozen=True, slots=True)
class BootstrapSeed:
    key: BootstrapKey
    requirements: tuple[ProfileRequirement, ...]
    cutover_ms: int
    lookback: int
    availability_policy_id: str
    availability_policy_digest: str
    dataset_digest: str
    candles: tuple[BootstrapCandle, ...]
    max_seed_candles: InitVar[int]

    def __post_init__(self, max_seed_candles: int) -> None:
        try:
            _integer(max_seed_candles, 1)
            _integer(self.lookback)
            _integer(self.cutover_ms)
            _safe(self.availability_policy_id)
            _hex(self.availability_policy_digest)
            _hex(self.dataset_digest)
            if type(self.key) is not BootstrapKey or type(self.candles) is not tuple:
                raise ValueError
            ordered = _requirements(self.requirements)
            if (
                not ordered
                or len(self.candles) != self.lookback
                or self.lookback > max_seed_candles
            ):
                raise ValueError
            object.__setattr__(self, "requirements", ordered)
            duration = timeframe_to_ms(self.key.timeframe)
            start = self.cutover_ms - self.lookback * duration
            if self.cutover_ms % duration or start < 0:
                raise ValueError
            for index, candle in enumerate(self.candles):
                if (
                    type(candle) is not BootstrapCandle
                    or candle.bar_start_ms != start + index * duration
                ):
                    raise ValueError
                decision = candle.bar_start_ms + duration
                if candle.context.decision_time_ms != decision or any(
                    item.basis is not ProfileDecisionBasis.MODELED
                    or item.request.availability_policy_id
                    != self.availability_policy_id
                    for item in candle.context.profiles
                ):
                    raise ValueError
            self.canonical_bytes  # Incremental admission before a whole-seed allocation.
        except (ValueError, TypeError, OverflowError):
            raise BootstrapSeedError() from None

    @property
    def canonical_bytes(self) -> bytes:
        prefix = _bytes(
            dict(
                availability_policy_digest=self.availability_policy_digest,
                availability_policy_id=self.availability_policy_id,
            )
        )[:-1]
        parts = [prefix + b',"candles":[']
        size = len(parts[0])
        for index, candle in enumerate(self.candles):
            validate_profile_context_coverage(
                self.requirements, candle.context, candle.context.decision_time_ms
            )
            if len(candle.context.profiles) > MAX_INPUT_PROFILES or any(
                item.profile is not None
                and len(item.profile.bins) > MAX_INPUT_BINS_PER_PROFILE
                for item in candle.context.profiles
            ):
                raise BootstrapSeedError()
            # Bootstrap evidence deliberately has no decision key or input identity.
            modeled = _canonical(
                {
                    "schema_version": 1,
                    "evidence_kind": "BOOTSTRAP_MODELED",
                    "context": json.loads(candle.context.canonical_bytes),
                },
                max_bytes=MAX_INPUT_BYTES,
                max_nodes=MAX_INPUT_NODES,
            ).encode("utf-8")
            values: dict[str, str | int] = {
                name: canonical_decimal_text(getattr(candle, name))
                for name in ("open", "high", "low", "close", "volume")
            }
            values["bar_start_ms"] = candle.bar_start_ms
            part = (
                (b"," if index else b"")
                + b'{"candle":'
                + _bytes(values)
                + b',"modeled_evidence":'
                + modeled
                + b"}"
            )
            size += len(part)
            if size > MAX_BOOTSTRAP_BYTES:
                raise BootstrapSeedError()
            parts.append(part)
        tail = (
            b"],"
            + _bytes(
                dict(
                    contract_version=1,
                    schema_version=1,
                    key=json.loads(self.key.canonical_bytes),
                    cutover_ms=self.cutover_ms,
                    lookback=self.lookback,
                    requirements=[
                        json.loads(r.canonical_bytes) for r in self.requirements
                    ],
                    dataset_digest=self.dataset_digest,
                )
            )[1:]
        )
        if size + len(tail) > MAX_BOOTSTRAP_BYTES:
            raise BootstrapSeedError()
        return b"".join((*parts, tail))

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_canonical_bytes(
        cls, raw: bytes, *, max_seed_candles: int
    ) -> "BootstrapSeed":
        """Restore bounded bootstrap evidence only; never create a live input key."""

        def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
            if len(dict(items)) != len(items):
                raise ValueError
            return dict(items)

        def reject(value: str) -> Any:
            raise ValueError

        def versioned(value: Any, names: set[str]) -> dict[str, Any]:
            row = _keys(value, names | {"schema_version"})
            version = row.pop("schema_version")
            if type(version) is not int or version != 1:
                raise ValueError
            return row

        try:
            _integer(max_seed_candles, 1)
            if type(raw) is not bytes or len(raw) > MAX_BOOTSTRAP_BYTES:
                raise ValueError
            data = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=pairs,
                parse_float=reject,
                parse_constant=reject,
            )
            if (
                _canonical(
                    data, max_bytes=MAX_BOOTSTRAP_BYTES, max_nodes=MAX_BOOTSTRAP_BYTES
                ).encode("utf-8")
                != raw
            ):
                raise ValueError
            row = versioned(data, {f.name for f in fields(cls)} | {"contract_version"})
            version = row.pop("contract_version")
            if type(version) is not int or version != 1:
                raise ValueError
            key = versioned(row["key"], {f.name for f in fields(BootstrapKey)})
            requirements = row["requirements"]
            candles = row["candles"]
            if (
                type(requirements) is not list
                or len(requirements) > MAX_INPUT_PROFILES
                or type(candles) is not list
                or len(candles) > max_seed_candles
            ):
                raise ValueError
            row["key"] = BootstrapKey(**key)
            row["requirements"] = tuple(
                ProfileRequirement(
                    **versioned(r, {f.name for f in fields(ProfileRequirement)})
                )
                for r in requirements
            )
            decoded = []
            for entry in candles:
                entry = _keys(entry, {"candle", "modeled_evidence"})
                evidence = entry["modeled_evidence"]
                _canonical(
                    evidence, max_bytes=MAX_INPUT_BYTES, max_nodes=MAX_INPUT_NODES
                )
                evidence = versioned(evidence, {"evidence_kind", "context"})
                if evidence["evidence_kind"] != "BOOTSTRAP_MODELED":
                    raise ValueError
                values = _keys(
                    entry["candle"],
                    {"bar_start_ms", "open", "high", "low", "close", "volume"},
                )
                for name in ("open", "high", "low", "close", "volume"):
                    values[name] = _decimal_text(values[name])
                decoded.append(
                    BootstrapCandle(**values, context=_context(evidence["context"]))
                )
            row["candles"] = tuple(decoded)
            result = cls(**row, max_seed_candles=max_seed_candles)
            if result.canonical_bytes != raw:
                raise ValueError
            return result
        except (
            ValueError,
            TypeError,
            OverflowError,
            RecursionError,
            KeyError,
            DecimalException,
        ):
            raise BootstrapSeedError() from None


class BootstrapDisposition(str, Enum):
    LEGACY = "LEGACY"
    BOOTSTRAP = "BOOTSTRAP"
    REPLAY = "REPLAY"


def classify_bootstrap(
    requirements: tuple[ProfileRequirement, ...],
    *,
    proposed: BootstrapSeed | None,
    stored: BootstrapSeed | None,
    history_known_absent: bool,
    completed_recorded_through_ms: int | None,
    recorded: tuple[
        tuple[MarketDataDecisionOutcome, MarketDataDecisionInput | None], ...
    ],
    max_seed_candles: int,
    max_recorded_candles: int,
) -> BootstrapDisposition:
    """Caller supplies terminal suffix evidence; missing history never selects current modeled data."""
    try:
        wanted = _requirements(requirements)
        _integer(max_seed_candles, 1)
        _integer(max_recorded_candles, 1)
        if type(history_known_absent) is not bool or type(recorded) is not tuple:
            raise ValueError
        if not wanted:
            if (
                proposed is not None
                or stored is not None
                or recorded
                or completed_recorded_through_ms is not None
            ):
                raise ValueError
            return BootstrapDisposition.LEGACY
        if (
            type(proposed) is not BootstrapSeed
            or proposed.requirements != wanted
            or len(proposed.candles) > max_seed_candles
        ):
            raise ValueError
        if stored is None:
            if (
                not history_known_absent
                or recorded
                or completed_recorded_through_ms is not None
            ):
                raise ValueError
            return BootstrapDisposition.BOOTSTRAP
        if type(stored) is not BootstrapSeed or history_known_absent:
            raise ValueError
        if stored.canonical_bytes != proposed.canonical_bytes:
            raise BootstrapSeedError("CONFLICT")
        duration = timeframe_to_ms(stored.key.timeframe)
        end = completed_recorded_through_ms
        count = 0
        if end is not None:
            _integer(end)
            if end < stored.cutover_ms or end % duration:
                raise ValueError
            count = (end - stored.cutover_ms) // duration + 1
        if count > max_recorded_candles or len(recorded) != count:
            raise ValueError
        for index, pair in enumerate(recorded):
            if type(pair) is not tuple or len(pair) != 2:
                raise ValueError
            outcome, pinned = pair
            expected_key = stored.key.decision_key(stored.cutover_ms + index * duration)
            if (
                type(outcome) is not MarketDataDecisionOutcome
                or outcome.key != expected_key
            ):
                raise ValueError
            if outcome.disposition == "SKIPPED" and outcome.input_id is None:
                # An orphan pin does not override terminal evidence that callback was skipped.
                if pinned is not None and type(pinned) is not MarketDataDecisionInput:
                    raise ValueError
            elif (
                type(pinned) is not MarketDataDecisionInput
                or pinned.key != expected_key
                or pinned.input_id != outcome.input_id
                or pinned.input_digest != outcome.input_digest
                or pinned.requirements != wanted
            ):
                raise ValueError
        return BootstrapDisposition.REPLAY
    except BootstrapSeedError:
        raise
    except (ValueError, TypeError, OverflowError):
        raise BootstrapSeedError() from None
