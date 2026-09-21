"""Pure schema-2 evidence decoding; no observation, freshness or daily-source proof."""

from dataclasses import dataclass
from decimal import Decimal, DecimalException
from fractions import Fraction
import json
from typing import Any

from src.core.decimal_math import (
    canonical_decimal_text,
    exact_decimal_add,
    decimal_from_fraction_significant,
)
from .composite_types import CompositeProfile, CompositeProfilePoc
from .grid import resolve_profile_grid
from .live_query import LiveProfileQueryResult
from .live_selection import LiveProfileSelection
from .live_validation import ValidatedLiveProfileQuery
from .read_types import (
    DailyProfileRef,
    OrderedProfileManifest,
    ProfileQueryRequest,
    _integer,
)
from .types import ProfileBin, VolumeProfileContent, _decimal

_MAX_BYTES = 2 * 1024 * 1024
_MAX_BINS = 5000
_COMMON = frozenset(
    "schema_version data_kind profile_kind validation_basis source_available_at_ms product_id base_grid_id output_grid_id grid algorithm_version window coverage manifest manifest_digest bins base_volume quote_volume aggregate_count poc validation_started_at_ms validation_completed_at_ms served_at_ms validation_expires_at_ms validation_max_age_ms validation_remaining_ms".split()
)
_DAILY = frozenset("snapshot_id revision content_sha256".split())
_COMPOSITE = frozenset("composite_id merge_algorithm_version".split())


class ProfileWireError(ValueError):
    def __init__(self, reason: str) -> None:
        if type(reason) is not str or reason not in (
            "QUERY_TOO_LARGE",
            "INVALID_PROFILE",
        ):
            raise ValueError("PROFILE_WIRE_INVALID_PROFILE") from None
        self.reason = reason
        super().__init__("PROFILE_WIRE_" + reason)


@dataclass(frozen=True, slots=True)
class ProfileWireEvidence:
    validated: ValidatedLiveProfileQuery
    served_at_ms: int

    def __post_init__(self) -> None:
        try:
            _integer(self.served_at_ms)
            if (
                type(self.validated) is not ValidatedLiveProfileQuery
                or not self.validated.validation_completed_at_ms
                <= self.served_at_ms
                < self.validated.validation_expires_at_ms
            ):
                raise ValueError
        except ValueError:
            raise ProfileWireError("INVALID_PROFILE") from None


def _object(value: Any, keys: str | frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict or value.keys() != (
        set(keys.split()) if type(keys) is str else keys
    ):
        raise ValueError
    return value


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject(value: str) -> Any:
    raise ValueError


def _money(value: Any) -> Decimal:
    if type(value) is not str or len(value) > 64 or len(value.encode("utf-8")) > 64:
        raise ValueError
    result = _decimal(Decimal(value))
    if canonical_decimal_text(result) != value:
        raise ValueError
    return result


def decode_live_profile_response(
    request: ProfileQueryRequest, raw: bytes
) -> ProfileWireEvidence:
    """Verify exact wire content; composite daily references are not re-aggregated."""
    try:
        if (
            type(request) is not ProfileQueryRequest
            or request.purpose != "LIVE_QUERY"
            or request.freshness_policy_id != "utc_complete_strict_v1"
            or type(raw) is not bytes
        ):
            raise ValueError
        if len(raw) > _MAX_BYTES:
            raise ProfileWireError("QUERY_TOO_LARGE")
        body = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject,
            parse_constant=_reject,
        )
        if type(body) is not dict or body.get("profile_kind") not in (
            "DAILY",
            "COMPOSITE",
        ):
            raise ValueError
        daily = body["profile_kind"] == "DAILY"
        _object(body, _COMMON | (_DAILY if daily else _COMPOSITE))
        if (
            type(body["schema_version"]) is not int
            or body["schema_version"] != 2
            or body["data_kind"] != "VOLUME_PROFILE"
            or body["validation_basis"] != "SERVER_PINNED_READ"
        ):
            raise ValueError
        rows, refs = body["bins"], body["manifest"]
        if type(rows) is not list or type(refs) is not list:
            raise ValueError
        if len(rows) > _MAX_BINS:
            raise ProfileWireError("QUERY_TOO_LARGE")
        if len(refs) > 90:
            raise ProfileWireError("QUERY_TOO_LARGE")
        if not refs or daily != (len(refs) == 1):
            raise ValueError
        days = []
        for row in refs:
            row = _object(row, "snapshot_id revision content_sha256 start_ms end_ms")
            days.append(
                DailyProfileRef(
                    row["snapshot_id"],
                    row["revision"],
                    row["content_sha256"],
                    row["start_ms"],
                    row["end_ms"],
                )
            )
        manifest = OrderedProfileManifest(
            body["product_id"],
            body["base_grid_id"],
            body["algorithm_version"],
            tuple(days),
        )
        if body["manifest_digest"] != manifest.manifest_digest:
            raise ValueError
        window = _object(body["window"], "start_ms end_ms")
        for key in window:
            _integer(window[key])
        if (window["start_ms"], window["end_ms"]) != (
            days[0].window_start_ms,
            days[-1].window_end_ms,
        ):
            raise ValueError
        coverage = _object(body["coverage"], "expected_days complete_days")
        if any(
            type(value) is not int or value != len(days) for value in coverage.values()
        ):
            raise ValueError
        grid = resolve_profile_grid(
            body["product_id"], body["output_grid_id"], body["algorithm_version"]
        )
        spec = _object(body["grid"], "origin step unit")
        if (_money(spec["origin"]), _money(spec["step"]), spec["unit"]) != (
            grid.origin,
            grid.step,
            grid.unit,
        ):
            raise ValueError
        bins = []
        base = quote = Decimal(0)
        count = 0
        for row in rows:
            row = _object(row, "bin_index base_volume quote_volume aggregate_count")
            bin_value = ProfileBin(
                row["bin_index"],
                _money(row["base_volume"]),
                _money(row["quote_volume"]),
                row["aggregate_count"],
            )
            bins.append(bin_value)
            base = exact_decimal_add(base, bin_value.base_volume)
            quote = exact_decimal_add(quote, bin_value.quote_volume)
            count += bin_value.aggregate_count
        _integer(body["aggregate_count"])
        if (base, quote, count) != (
            _money(body["base_volume"]),
            _money(body["quote_volume"]),
            body["aggregate_count"],
        ):
            raise ValueError
        poc = None
        if body["poc"] is not None:
            value = _object(body["poc"], "bin_index low high_exclusive")
            poc = CompositeProfilePoc(
                value["bin_index"],
                _money(value["low"]),
                _money(value["high_exclusive"]),
            )
        if bins:
            winner = min(bins, key=lambda b: (-Fraction(b.base_volume), b.bin_index))

            def edge(index: int) -> Decimal:
                return _decimal(
                    decimal_from_fraction_significant(
                        Fraction(grid.origin) + Fraction(grid.step) * index,
                        precision=100,
                    )
                )

            if poc != CompositeProfilePoc(
                winner.bin_index, edge(winner.bin_index), edge(winner.bin_index + 1)
            ):
                raise ValueError
        profile = CompositeProfile(manifest, grid, tuple(bins), base, quote, count, poc)
        if daily:
            content = VolumeProfileContent(
                profile.product_id,
                profile.window_start_ms,
                profile.window_end_ms,
                grid.grid_id,
                grid.origin,
                grid.step,
                profile.algorithm_version,
                profile.bins,
            )
            _integer(body["revision"], 1)
            if (
                (body["snapshot_id"], body["revision"], body["content_sha256"])
                != (days[0].snapshot_id, days[0].revision, days[0].content_sha256)
                or days[0].snapshot_id != content.content_sha256
                or days[0].content_sha256 != content.content_sha256
            ):
                raise ValueError
        elif (body["composite_id"], body["merge_algorithm_version"]) != (
            profile.composite_id,
            profile.merge_algorithm_version,
        ):
            raise ValueError
        for key in (
            "source_available_at_ms",
            "validation_started_at_ms",
            "validation_completed_at_ms",
            "served_at_ms",
            "validation_expires_at_ms",
            "validation_max_age_ms",
            "validation_remaining_ms",
        ):
            _integer(body[key])
        completed, expires = (
            body["validation_completed_at_ms"],
            body["validation_expires_at_ms"],
        )
        if (
            body["validation_max_age_ms"] != 300000
            or body["validation_remaining_ms"] != expires - body["served_at_ms"]
        ):
            raise ValueError
        selection = LiveProfileSelection(
            manifest, body["validation_started_at_ms"], body["source_available_at_ms"]
        )
        query = LiveProfileQueryResult(request, selection, profile)
        validated = ValidatedLiveProfileQuery(
            query,
            body["validation_started_at_ms"],
            completed,
            completed + 300000 - expires,
        )
        return ProfileWireEvidence(validated, body["served_at_ms"])
    except ProfileWireError:
        raise
    except (ValueError, TypeError, KeyError, DecimalException, RecursionError):
        raise ProfileWireError("INVALID_PROFILE") from None
