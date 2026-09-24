"""Admit verified reads, call Rust once, and validate its detached result (no aggregation)."""

from decimal import Decimal, InvalidOperation
from importlib import import_module
from typing import Callable

from src.core.decimal_math import canonical_decimal_text
from .composite_types import CompositeProfile, CompositeProfilePoc
from .grid import ProfileGridDefinition, resolve_profile_grid
from .read_results import VerifiedManifestRead
from .types import ProfileBin, _decimal, _integer

_BIN_CAP = 131072
_NATIVE_ERRORS = {
    "PROFILE_MERGE_RESOURCE_LIMIT": "TOO_LARGE",
    "PROFILE_MERGE_ARITHMETIC": "ARITHMETIC",
    **dict.fromkeys(
        (
            "PROFILE_MERGE_" + name
            for name in (
                "INVALID_INPUT",
                "INVALID_DECIMAL",
                "INVALID_BIN",
                "INVALID_PRODUCT",
                "INVALID_GRID",
                "INVALID_WINDOW",
                "INVALID_COMPOSITION",
                "INVALID_TRADE_SCOPE",
                "INVALID_TRADE_VALUE",
            )
        ),
        "INTEGRITY",
    ),
}


class ProfileCompositionError(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__("PROFILE_COMPOSITION_" + reason)


def _load_native() -> tuple[type, Callable[..., object]]:
    try:
        module = import_module("fluxtrade_core")
    except ModuleNotFoundError as error:
        if error.name != "fluxtrade_core":
            raise
        raise ProfileCompositionError("NATIVE_UNAVAILABLE") from None
    try:
        return module.VolumeProfileMergeResult, module.merge_volume_profiles
    except AttributeError:
        raise ProfileCompositionError("NATIVE_UNAVAILABLE") from None


def _text_decimal(value: object) -> Decimal:
    if type(value) is not str or len(value) > 64 or len(value.encode("utf-8")) > 64:
        raise ValueError
    try:
        result = _decimal(Decimal(value))
    except InvalidOperation:
        raise ValueError from None
    if canonical_decimal_text(result) != value:
        raise ValueError
    return result


def _result(
    raw: object,
    expected: type,
    read: VerifiedManifestRead,
    grid: ProfileGridDefinition,
    count: int,
) -> CompositeProfile:
    try:
        if type(raw) is not expected:
            raise ValueError
        for field, value in (
            ("product_id", grid.product_id),
            ("unit", grid.unit),
            ("window_start_ms", read.manifest.days[0].window_start_ms),
            ("window_end_ms", read.manifest.days[-1].window_end_ms),
        ):
            actual = getattr(raw, field)
            if type(actual) is not type(value) or actual != value:
                raise ValueError
        if (
            _text_decimal(getattr(raw, "bin_origin")) != grid.origin
            or _text_decimal(getattr(raw, "bin_step")) != grid.step
        ):
            raise ValueError
        rows = getattr(raw, "bins")
        if type(rows) is not list or len(rows) > min(count, _BIN_CAP):
            raise ValueError
        bins = []
        for row in rows:
            if type(row) is not tuple or len(row) != 4:
                raise ValueError
            bins.append(
                ProfileBin(row[0], _text_decimal(row[1]), _text_decimal(row[2]), row[3])
            )
        index, low, high = (
            getattr(raw, name)
            for name in ("poc_index", "poc_low", "poc_high_exclusive")
        )
        if index is None and low is None and high is None:
            poc = None
        else:
            poc = CompositeProfilePoc(index, _text_decimal(low), _text_decimal(high))
        total = getattr(raw, "aggregate_count")
        _integer(total, 0)
        return CompositeProfile(
            read.manifest,
            grid,
            tuple(bins),
            _text_decimal(getattr(raw, "base_volume")),
            _text_decimal(getattr(raw, "quote_volume")),
            total,
            poc,
        )
    except (ValueError, TypeError, AttributeError):
        raise ProfileCompositionError("INTEGRITY") from None


def compose_profile(read: VerifiedManifestRead) -> CompositeProfile:
    if type(read) is not VerifiedManifestRead:
        raise ProfileCompositionError("INVALID_INPUT") from None
    manifest = read.manifest
    try:
        grid = resolve_profile_grid(
            manifest.product_id, manifest.base_grid_id, manifest.algorithm_version
        )
    except ValueError:
        raise ProfileCompositionError("INTEGRITY") from None
    if any(day.invalidations for day in read.days):
        raise ProfileCompositionError("REVOKED") from None
    count = 0
    for day in read.days:
        content = day.publication.content
        if content.bin_origin != grid.origin or content.bin_step != grid.step:
            raise ProfileCompositionError("INTEGRITY") from None
        count += len(content.bins)
    if count > _BIN_CAP:
        raise ProfileCompositionError("TOO_LARGE") from None
    days = [
        (
            day.ref.window_start_ms,
            day.ref.window_end_ms,
            [
                (
                    b.bin_index,
                    canonical_decimal_text(b.base_volume),
                    canonical_decimal_text(b.quote_volume),
                    b.aggregate_count,
                )
                for b in day.publication.content.bins
            ],
        )
        for day in read.days
    ]
    result_class, merge = _load_native()
    try:
        raw = merge(
            grid.product_id,
            canonical_decimal_text(grid.origin),
            canonical_decimal_text(grid.step),
            grid.unit,
            days,
            canonical_decimal_text(grid.step),
        )
    except Exception as error:
        reason = (
            _NATIVE_ERRORS.get(str(error), "NATIVE_FAILURE")
            if type(error) is ValueError
            else "NATIVE_FAILURE"
        )
        raise ProfileCompositionError(reason) from None
    return _result(raw, result_class, read, grid, count)
