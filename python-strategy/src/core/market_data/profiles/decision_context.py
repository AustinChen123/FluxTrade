from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import StrEnum
import hashlib
import json

from src.core.decimal_math import canonical_decimal_text
from .composite_types import CompositeProfile
from .read_types import ProfileQueryRequest, _integer


class ProfileDecisionBasis(StrEnum):
    LIVE_OBSERVED = "LIVE_OBSERVED"
    MODELED = "MODELED"


class ProfileDecisionStatus(StrEnum):
    FRESH = "FRESH"
    MISSING = "MISSING"
    INVALID = "INVALID"


_UNAVAILABLE_REASONS = {
    ProfileDecisionStatus.MISSING: frozenset(
        {
            "PROFILE_NOT_READY",
            "PROFILE_EXPIRED",
            "BACKEND_UNAVAILABLE",
            "CLOCK_UNCERTAIN",
            "VALIDATION_EXPIRED",
        }
    ),
    ProfileDecisionStatus.INVALID: frozenset(
        {
            "SNAPSHOT_REVOKED",
            "INVALID_PROFILE",
            "QUERY_TOO_LARGE",
        }
    ),
}


def _decimal_json(value: object) -> str:
    if type(value) is Decimal:
        return canonical_decimal_text(value)
    raise TypeError("invalid decision projection")


@dataclass(frozen=True, slots=True)
class ProfileDecisionContext:
    request: ProfileQueryRequest
    decision_time_ms: int
    basis: ProfileDecisionBasis
    status: ProfileDecisionStatus
    reason: str | None = None
    profile: CompositeProfile | None = None
    available_at_ms: int | None = None
    validation_checked_at_ms: int | None = None
    observed_at_ms: int | None = None

    def __post_init__(self) -> None:
        try:
            _integer(self.decision_time_ms)
            if (
                type(self.request) is not ProfileQueryRequest
                or type(self.basis) is not ProfileDecisionBasis
                or type(self.status) is not ProfileDecisionStatus
            ):
                raise ValueError
            modeled = self.basis is ProfileDecisionBasis.MODELED
            if self.request.purpose != (
                "MODELED_RESEARCH" if modeled else "LIVE_QUERY"
            ):
                raise ValueError
            if modeled and self.request.as_of_ms != self.decision_time_ms:
                raise ValueError
            if self.status is not ProfileDecisionStatus.FRESH:
                if (
                    type(self.reason) is not str
                    or self.reason not in _UNAVAILABLE_REASONS[self.status]
                    or any(
                        value is not None
                        for value in (
                            self.profile,
                            self.available_at_ms,
                            self.validation_checked_at_ms,
                            self.observed_at_ms,
                        )
                    )
                ):
                    raise ValueError
                return
            if self.reason is not None:
                raise ValueError
            p, r = self.profile, self.request
            if type(p) is not CompositeProfile:
                raise ValueError
            if any(
                getattr(p, name) != getattr(r, name)
                for name in ("product_id", "base_grid_id", "algorithm_version")
            ) or (
                p.output_grid.grid_id != r.output_grid_id
                or p.window_start_ms != r.start_ms
                or p.window_end_ms != r.end_ms
            ):
                raise ValueError
            if r.revision is not None and p.manifest.days[0].revision != r.revision:
                raise ValueError
            if self.available_at_ms is None:
                raise ValueError
            _integer(self.available_at_ms)
            chain = [r.end_ms, self.available_at_ms]
            if modeled:
                if (
                    self.validation_checked_at_ms is not None
                    or self.observed_at_ms is not None
                ):
                    raise ValueError
            else:
                for stamp in (self.validation_checked_at_ms, self.observed_at_ms):
                    if stamp is None:
                        raise ValueError
                    _integer(stamp)
                    chain.append(stamp)
            chain.append(self.decision_time_ms)
            if any(a > b for a, b in zip(chain, chain[1:])):
                raise ValueError
        except ValueError:
            raise ValueError("PROFILE_DECISION_INVALID") from None

    @property
    def canonical_bytes(self) -> bytes:
        projection = {"schema_version": 1, **asdict(self)}
        return json.dumps(
            projection,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=_decimal_json,
        ).encode("utf-8")

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class StrategyMarketDataContext:
    """Canonical decision snapshot only; no provider or execution responsibility."""

    decision_time_ms: int
    profiles: tuple[ProfileDecisionContext, ...]

    def __post_init__(self) -> None:
        try:
            _integer(self.decision_time_ms)
            if type(self.profiles) is not tuple or any(
                type(item) is not ProfileDecisionContext for item in self.profiles
            ):
                raise ValueError
            if any(
                item.decision_time_ms != self.decision_time_ms for item in self.profiles
            ):
                raise ValueError
            keyed = [
                (
                    json.dumps(
                        asdict(item.request),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    item,
                )
                for item in self.profiles
            ]
            if len({key for key, _ in keyed}) != len(keyed):
                raise ValueError
            ordered = tuple(item for _, item in sorted(keyed, key=lambda pair: pair[0]))
        except ValueError:
            raise ValueError("PROFILE_DECISION_INVALID") from None
        object.__setattr__(self, "profiles", ordered)

    @property
    def canonical_bytes(self) -> bytes:
        return json.dumps(
            {
                "schema_version": 1,
                "decision_time_ms": self.decision_time_ms,
                "profiles": [
                    json.loads(item.canonical_bytes) for item in self.profiles
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()
