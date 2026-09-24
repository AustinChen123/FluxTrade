"""Composite structure/identity only, not aggregation or source-completeness proof."""

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json

from .grid import ProfileGridDefinition, resolve_profile_grid
from .read_types import OrderedProfileManifest
from .types import ProfileBin, _decimal, _integer


class InvalidCompositeProfile(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_COMPOSITE_INVALID")


@dataclass(frozen=True, slots=True)
class CompositeProfilePoc:
    index: int
    low: Decimal
    high_exclusive: Decimal

    def __post_init__(self) -> None:
        try:
            _integer(self.index, -(1 << 63))
            low, high = _decimal(self.low), _decimal(self.high_exclusive)
            if low >= high:
                raise ValueError
        except ValueError:
            raise InvalidCompositeProfile() from None
        object.__setattr__(self, "low", low)
        object.__setattr__(self, "high_exclusive", high)


@dataclass(frozen=True, slots=True)
class CompositeProfile:
    """No bin sum/count, POC winner or edge recomputation is performed here."""

    manifest: OrderedProfileManifest
    output_grid: ProfileGridDefinition
    bins: tuple[ProfileBin, ...]
    base_volume: Decimal
    quote_volume: Decimal
    aggregate_count: int
    poc: CompositeProfilePoc | None

    def __post_init__(self) -> None:
        try:
            if (
                type(self.manifest) is not OrderedProfileManifest
                or type(self.output_grid) is not ProfileGridDefinition
            ):
                raise ValueError
            m, g = self.manifest, self.output_grid
            base = resolve_profile_grid(
                m.product_id, m.base_grid_id, m.algorithm_version
            )
            if (
                g != resolve_profile_grid(g.product_id, g.grid_id, g.algorithm_version)
                or g != base
            ):
                raise ValueError
            if type(self.bins) is not tuple or any(
                type(b) is not ProfileBin for b in self.bins
            ):
                raise ValueError
            if any(
                a.bin_index >= b.bin_index for a, b in zip(self.bins, self.bins[1:])
            ):
                raise ValueError
            base_volume, quote_volume = (
                _decimal(self.base_volume),
                _decimal(self.quote_volume),
            )
            _integer(self.aggregate_count, 0)
            if self.bins:
                if (
                    base_volume <= 0
                    or quote_volume <= 0
                    or self.aggregate_count == 0
                    or type(self.poc) is not CompositeProfilePoc
                    or not any(b.bin_index == self.poc.index for b in self.bins)
                ):
                    raise ValueError
            elif (
                base_volume != 0
                or quote_volume != 0
                or self.aggregate_count != 0
                or self.poc is not None
            ):
                raise ValueError
        except ValueError:
            raise InvalidCompositeProfile() from None
        object.__setattr__(self, "base_volume", base_volume)
        object.__setattr__(self, "quote_volume", quote_volume)

    @property
    def product_id(self) -> str:
        return self.manifest.product_id

    @property
    def window_start_ms(self) -> int:
        return self.manifest.days[0].window_start_ms

    @property
    def window_end_ms(self) -> int:
        return self.manifest.days[-1].window_end_ms

    @property
    def base_grid_id(self) -> str:
        return self.manifest.base_grid_id

    @property
    def algorithm_version(self) -> str:
        return self.manifest.algorithm_version

    @property
    def manifest_digest(self) -> str:
        return self.manifest.manifest_digest

    @property
    def merge_algorithm_version(self) -> str:
        return "aligned-sum-v1"

    @property
    def identity_bytes(self) -> bytes:
        return json.dumps(
            dict(
                product_id=self.product_id,
                base_grid_id=self.base_grid_id,
                output_grid_id=self.output_grid.grid_id,
                algorithm_version=self.algorithm_version,
                merge_algorithm_version=self.merge_algorithm_version,
                manifest_digest=self.manifest_digest,
            ),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    @property
    def composite_id(self) -> str:
        return hashlib.sha256(self.identity_bytes).hexdigest()
