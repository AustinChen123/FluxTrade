"""Profile demand intent only; definitions need not be registered or supported."""

from dataclasses import asdict, dataclass
import hashlib
import json

from .read_types import _safe, _scope


@dataclass(frozen=True, slots=True)
class ProfileRequirement:
    product_id: str
    base_grid_id: str
    output_grid_id: str
    algorithm_version: str
    window_days: int
    freshness_policy_id: str

    def __post_init__(self) -> None:
        _scope(self.product_id, self.base_grid_id, self.algorithm_version)
        _safe(self.output_grid_id)
        _safe(self.freshness_policy_id)
        if type(self.window_days) is not int or not 1 <= self.window_days <= 90:
            raise ValueError("invalid profile requirement window")

    @property
    def canonical_bytes(self) -> bytes:
        return json.dumps(
            {"schema_version": 1, **asdict(self)},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()
