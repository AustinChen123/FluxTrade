"""Pure reversible pinned-input envelope. No durability or callback guarantee.

Admission: 8 MiB, 32 profiles/requirements, depth 16, one million JSON nodes,
100,000 bins per profile. Stored aggregate claims are restored, not recomputed;
this codec does not independently prove source completeness or aggregation.
"""

from dataclasses import dataclass, fields
from decimal import Decimal, DecimalException
import hashlib
import json
from typing import Any

from src.core.decimal_math import canonical_decimal_text
from .composite_types import CompositeProfile, CompositeProfilePoc
from .context_enrichment import _validate_coverage
from .decision_application import MarketDataDecisionKey
from .decision_context import (
    ProfileDecisionContext,
    ProfileDecisionBasis,
    ProfileDecisionStatus,
    StrategyMarketDataContext,
)
from .grid import ProfileGridDefinition
from .publication import _canonical
from .read_types import DailyProfileRef, OrderedProfileManifest, ProfileQueryRequest
from .requirements import ProfileRequirement
from .types import ProfileBin, _decimal

MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_INPUT_PROFILES = 32
MAX_INPUT_NODES = 1_000_000
MAX_INPUT_BINS_PER_PROFILE = 100_000


class DecisionInputError(ValueError):
    def __init__(self) -> None:
        super().__init__("MARKET_DATA_INPUT_INVALID")


def _encode(value: dict[str, Any]) -> bytes:
    return _canonical(
        value, max_bytes=MAX_INPUT_BYTES, max_nodes=MAX_INPUT_NODES
    ).encode("utf-8")


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _keys(value: Any, names: set[str]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != names:
        raise ValueError
    return value.copy()


def _record(value: Any, cls: Any) -> dict[str, Any]:
    return _keys(value, {field.name for field in fields(cls)})


def _decimal_text(value: Any) -> Decimal:
    if type(value) is not str or len(value) > 64 or len(value.encode("utf-8")) > 64:
        raise ValueError
    result = _decimal(Decimal(value))
    if canonical_decimal_text(result) != value:
        raise ValueError
    return result


def _manifest(value: Any) -> OrderedProfileManifest:
    data = _record(value, OrderedProfileManifest)
    if type(data["days"]) is not list or not 1 <= len(data["days"]) <= 90:
        raise ValueError
    data["days"] = tuple(
        DailyProfileRef(**_record(day, DailyProfileRef)) for day in data["days"]
    )
    return OrderedProfileManifest(**data)


def _profile(value: Any) -> CompositeProfile:
    data = _keys(
        value,
        {f.name for f in fields(CompositeProfile)}
        | {"merge_algorithm_version", "composite_id"},
    )
    version, identity = data.pop("merge_algorithm_version"), data.pop("composite_id")
    data["manifest"] = _manifest(data["manifest"])
    grid = _record(data["output_grid"], ProfileGridDefinition)
    for name in ("origin", "step"):
        grid[name] = _decimal_text(grid[name])
    data["output_grid"] = ProfileGridDefinition(**grid)
    if type(data["bins"]) is not list or len(data["bins"]) > MAX_INPUT_BINS_PER_PROFILE:
        raise ValueError
    bins = []
    for row in data["bins"]:
        row = _record(row, ProfileBin)
        for name in ("base_volume", "quote_volume"):
            row[name] = _decimal_text(row[name])
        bins.append(ProfileBin(**row))
    data["bins"] = tuple(bins)
    for name in ("base_volume", "quote_volume"):
        data[name] = _decimal_text(data[name])
    if data["poc"] is not None:
        poc = _record(data["poc"], CompositeProfilePoc)
        for name in ("low", "high_exclusive"):
            poc[name] = _decimal_text(poc[name])
        data["poc"] = CompositeProfilePoc(**poc)
    profile = CompositeProfile(**data)
    if profile.merge_algorithm_version != version or profile.composite_id != identity:
        raise ValueError
    return profile


def _context(value: Any) -> StrategyMarketDataContext:
    data = _keys(value, {"schema_version", "decision_time_ms", "profiles"})
    if (
        type(data.pop("schema_version")) is not int
        or value["schema_version"] != 1
        or type(data["profiles"]) is not list
        or len(data["profiles"]) > MAX_INPUT_PROFILES
    ):
        raise ValueError
    items = []
    for item in data["profiles"]:
        row = _keys(
            item, {f.name for f in fields(ProfileDecisionContext)} | {"schema_version"}
        )
        if type(row.pop("schema_version")) is not int or item["schema_version"] != 2:
            raise ValueError
        request = _record(row["request"], ProfileQueryRequest)
        if request["pinned_manifest"] is not None:
            request["pinned_manifest"] = _manifest(request["pinned_manifest"])
        row["request"] = ProfileQueryRequest(**request)
        row["basis"] = ProfileDecisionBasis(row["basis"])
        row["status"] = ProfileDecisionStatus(row["status"])
        if row["profile"] is not None:
            row["profile"] = _profile(row["profile"])
        items.append(ProfileDecisionContext(**row))
    return StrategyMarketDataContext(data["decision_time_ms"], tuple(items))


@dataclass(frozen=True, slots=True)
class MarketDataDecisionInput:
    key: MarketDataDecisionKey
    requirements: tuple[ProfileRequirement, ...]
    decision_time_ms: int
    context: StrategyMarketDataContext

    def __post_init__(self) -> None:
        try:
            if (
                type(self.key) is not MarketDataDecisionKey
                or type(self.requirements) is not tuple
                or len(self.requirements) > MAX_INPUT_PROFILES
                or type(self.context) is not StrategyMarketDataContext
                or len(self.context.profiles) > MAX_INPUT_PROFILES
            ):
                raise ValueError
            _validate_coverage(self.requirements, self.context, self.decision_time_ms)
            if any(
                item.profile is not None
                and len(item.profile.bins) > MAX_INPUT_BINS_PER_PROFILE
                for item in self.context.profiles
            ):
                raise ValueError
            object.__setattr__(
                self,
                "requirements",
                tuple(sorted(self.requirements, key=lambda r: r.canonical_bytes)),
            )
            self.canonical_bytes
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise DecisionInputError() from None

    @property
    def requirements_bytes(self) -> bytes:
        return _encode(
            {
                "schema_version": 1,
                "requirements": [
                    json.loads(r.canonical_bytes) for r in self.requirements
                ],
            }
        )

    @property
    def requirements_digest(self) -> str:
        return _digest(self.requirements_bytes)

    @property
    def policy_bytes(self) -> bytes:
        policies = []
        for requirement in self.requirements:
            item = next(
                item
                for item in self.context.profiles
                if (
                    item.request.product_id,
                    item.request.base_grid_id,
                    item.request.output_grid_id,
                    item.request.algorithm_version,
                    item.request.end_ms - item.request.start_ms,
                )
                == (
                    requirement.product_id,
                    requirement.base_grid_id,
                    requirement.output_grid_id,
                    requirement.algorithm_version,
                    requirement.window_days * 86400000,
                )
            )
            request = item.request
            policies.append(
                {
                    "requirement_digest": requirement.digest,
                    "basis": item.basis.value,
                    "purpose": request.purpose,
                    "freshness_policy_id": request.freshness_policy_id,
                    "availability_policy_id": request.availability_policy_id,
                    "as_of_ms": request.as_of_ms,
                }
            )
        return _encode({"schema_version": 1, "policies": policies})

    @property
    def policy_digest(self) -> str:
        return _digest(self.policy_bytes)

    @property
    def input_id(self) -> str:
        return self.key.input_id

    @property
    def canonical_bytes(self) -> bytes:
        return _encode(
            {
                "schema_version": 1,
                "key": json.loads(self.key.canonical_bytes),
                "requirements": json.loads(self.requirements_bytes),
                "policy": json.loads(self.policy_bytes),
                "decision_time_ms": self.decision_time_ms,
                "context": json.loads(self.context.canonical_bytes),
            }
        )

    @property
    def input_digest(self) -> str:
        return _digest(self.canonical_bytes)

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> "MarketDataDecisionInput":
        def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
            if len(dict(items)) != len(items):
                raise ValueError
            return dict(items)

        def reject(value: str) -> Any:
            raise ValueError

        try:
            if type(raw) is not bytes or len(raw) > MAX_INPUT_BYTES:
                raise ValueError
            data = _keys(
                json.loads(
                    raw.decode("utf-8"),
                    object_pairs_hook=pairs,
                    parse_float=reject,
                    parse_constant=reject,
                ),
                {
                    "schema_version",
                    "key",
                    "requirements",
                    "policy",
                    "decision_time_ms",
                    "context",
                },
            )
            if (
                _encode(data) != raw
                or type(data["schema_version"]) is not int
                or data["schema_version"] != 1
            ):
                raise ValueError
            key = _keys(
                data["key"],
                {f.name for f in fields(MarketDataDecisionKey)} | {"schema_version"},
            )
            if (
                type(key.pop("schema_version")) is not int
                or data["key"]["schema_version"] != 1
            ):
                raise ValueError
            requirements = _keys(
                data["requirements"], {"schema_version", "requirements"}
            )
            rows = requirements["requirements"]
            if (
                requirements["schema_version"] != 1
                or type(rows) is not list
                or len(rows) > MAX_INPUT_PROFILES
            ):
                raise ValueError
            decoded = []
            for row in rows:
                value = _keys(
                    row,
                    {f.name for f in fields(ProfileRequirement)} | {"schema_version"},
                )
                if value.pop("schema_version") != 1:
                    raise ValueError
                decoded.append(ProfileRequirement(**value))
            result = cls(
                MarketDataDecisionKey(**key),
                tuple(decoded),
                data["decision_time_ms"],
                _context(data["context"]),
            )
            if result.canonical_bytes != raw:
                raise ValueError
            return result
        except (
            ValueError,
            TypeError,
            KeyError,
            OverflowError,
            RecursionError,
            DecimalException,
        ):
            raise DecisionInputError() from None
