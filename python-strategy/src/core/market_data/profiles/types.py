"""Immutable daily profile content, not a source-completeness/publication claim."""

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal

from src.core.decimal_math import canonical_decimal_text, exact_decimal_add
from src.core.product_registry import validate_product_id

BIGINT_MAX = (1 << 63) - 1
DAY_MS = 86_400_000


def _integer(value: int, minimum: int) -> None:
    if type(value) is not int or not minimum <= value <= BIGINT_MAX:
        raise ValueError("expected exact int within BIGINT bounds")


def _decimal(value: Decimal, positive: bool = False) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or (positive and value <= 0):
        raise ValueError("expected finite exact Decimal with valid sign")
    sign, digits, exponent = value.as_tuple()
    if value.is_zero():
        return Decimal(0)
    assert isinstance(exponent, int)
    end = len(digits)
    while digits[end - 1] == 0:
        end -= 1
        exponent += 1
    if exponent < -28 or end + max(exponent, 0) > 29:
        raise ValueError("Decimal exceeds Rust coefficient/scale domain")
    coefficient = 0
    for digit in digits[:end]:
        coefficient = coefficient * 10 + digit
    if coefficient * 10 ** max(exponent, 0) > (1 << 96) - 1:
        raise ValueError("Decimal exceeds Rust coefficient/scale domain")
    return Decimal((sign, digits[:end], exponent))


def _identifier(value: str, limit: int) -> None:
    if type(value) is not str or not re.fullmatch(
        r"[A-Za-z0-9_-]{1," + str(limit) + "}", value
    ):
        raise ValueError("unsafe profile identifier")


@dataclass(frozen=True, slots=True)
class ProfileBin:
    bin_index: int
    base_volume: Decimal
    quote_volume: Decimal
    aggregate_count: int

    def __post_init__(self) -> None:
        _integer(self.bin_index, -(1 << 63))
        _integer(self.aggregate_count, 1)
        object.__setattr__(self, "base_volume", _decimal(self.base_volume, positive=True))
        object.__setattr__(self, "quote_volume", _decimal(self.quote_volume, positive=True))


@dataclass(frozen=True, slots=True)
class VolumeProfileContent:
    product_id: str
    window_start_ms: int
    window_end_ms: int
    grid_id: str
    bin_origin: Decimal
    bin_step: Decimal
    algorithm_version: str
    bins: tuple[ProfileBin, ...]

    def __post_init__(self) -> None:
        if type(self.product_id) is not str or len(self.product_id) > 64:
            raise ValueError("expected canonical product ID")
        validate_product_id(self.product_id)
        _integer(self.window_start_ms, 0)
        _integer(self.window_end_ms, 0)
        if (
            self.window_start_ms % DAY_MS
            or self.window_end_ms - self.window_start_ms != DAY_MS
        ):
            raise ValueError("expected one aligned UTC day [start, end)")
        _identifier(self.grid_id, 64)
        _identifier(self.algorithm_version, 32)
        object.__setattr__(self, "bin_origin", _decimal(self.bin_origin))
        object.__setattr__(self, "bin_step", _decimal(self.bin_step, positive=True))
        if type(self.bins) is not tuple or any(
            type(item) is not ProfileBin for item in self.bins
        ):
            raise ValueError("bins must be an exact tuple of ProfileBin")
        if any(a.bin_index >= b.bin_index for a, b in zip(self.bins, self.bins[1:])):
            raise ValueError("bins must be ascending and unique")
        _integer(self.aggregate_count, 0)

    @property
    def base_volume(self) -> Decimal:
        total = Decimal(0)
        for item in self.bins:
            total = exact_decimal_add(total, item.base_volume)
        return total

    @property
    def quote_volume(self) -> Decimal:
        total = Decimal(0)
        for item in self.bins:
            total = exact_decimal_add(total, item.quote_volume)
        return total

    @property
    def aggregate_count(self) -> int:
        return sum(item.aggregate_count for item in self.bins)

    @property
    def occupied_bins(self) -> int:
        return len(self.bins)

    @property
    def content_bytes(self) -> bytes:
        """Version 1: sorted keys, compact UTF-8 JSON, canonical Decimal strings."""
        payload = {
            "schema_version": 1,
            "product_id": self.product_id,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "period": "1d",
            "timezone": "UTC",
            "grid_id": self.grid_id,
            "bin_origin": canonical_decimal_text(self.bin_origin),
            "bin_step": canonical_decimal_text(self.bin_step),
            "algorithm_version": self.algorithm_version,
            "bins": [
                {
                    "bin_index": item.bin_index,
                    "aggregate_count": item.aggregate_count,
                    "base_volume": canonical_decimal_text(item.base_volume),
                    "quote_volume": canonical_decimal_text(item.quote_volume),
                }
                for item in self.bins
            ],
        }
        return json.dumps(
            payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content_bytes).hexdigest()
