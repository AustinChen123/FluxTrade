"""Exact profile grids; a valid definition does not imply support or registration."""

from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType

from src.core.product_registry import to_base_quote, validate_product_id

from .types import _decimal, _identifier


class InvalidProfileGrid(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_GRID_INVALID")


class UnsupportedProfileGrid(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_GRID_UNSUPPORTED")


def _scope(product: str, grid: str, algorithm: str) -> None:
    if type(product) is not str or len(product) > 64 or not product.isascii():
        raise ValueError("invalid product")
    validate_product_id(product)
    _identifier(grid, 64)
    _identifier(algorithm, 32)


@dataclass(frozen=True, slots=True)
class ProfileGridDefinition:
    """Immutable exact definition, not evidence that this grid is registered/supported."""

    product_id: str
    grid_id: str
    algorithm_version: str
    origin: Decimal
    step: Decimal
    unit: str

    def __post_init__(self) -> None:
        try:
            _scope(self.product_id, self.grid_id, self.algorithm_version)
            _identifier(self.unit, 64)
            if self.unit != to_base_quote(self.product_id)[1]:
                raise ValueError("quote mismatch")
            origin = _decimal(self.origin)
            step = _decimal(self.step, positive=True)
        except ValueError:
            raise InvalidProfileGrid() from None
        object.__setattr__(self, "origin", origin)
        object.__setattr__(self, "step", step)


_GRIDS = MappingProxyType(
    {
        ("BINANCE:BTCUSDT-SPOT", "btc_spot_usdt_10_v1", "vp-v1"): ("0", "10", "USDT"),
    }
)


def resolve_profile_grid(
    product_id: str, grid_id: str, algorithm_version: str
) -> ProfileGridDefinition:
    try:
        _scope(product_id, grid_id, algorithm_version)
    except ValueError:
        raise InvalidProfileGrid() from None
    values = _GRIDS.get((product_id, grid_id, algorithm_version))
    if values is None:
        raise UnsupportedProfileGrid() from None
    origin, step, unit = values
    return ProfileGridDefinition(
        product_id, grid_id, algorithm_version, Decimal(origin), Decimal(step), unit
    )
