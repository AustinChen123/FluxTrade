"""ORM-free persistence boundary for verified net-reduction replay."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class VerifiedNetReductionOrderSnapshot:
    id: str
    client_order_id: str | None
    strategy_id: str | None
    product_id: str
    type: str
    side: str
    quantity: Decimal
    filled_quantity: Decimal | None
    status: str
    intent_payload: Mapping[str, object] | None

    def __post_init__(self) -> None:
        if self.intent_payload is not None:
            object.__setattr__(
                self,
                "intent_payload",
                MappingProxyType(dict(self.intent_payload)),
            )


@runtime_checkable
class VerifiedNetReductionRepository(Protocol):
    def get_verified_net_reduction_order(
        self,
        order_id: str,
    ) -> VerifiedNetReductionOrderSnapshot | None: ...

    def get_verified_net_reduction_order_by_client_id(
        self,
        client_order_id: str,
    ) -> VerifiedNetReductionOrderSnapshot | None: ...

    def persist_verified_net_reduction(
        self,
        order_id: str,
        intent_payload: Mapping[str, object],
    ) -> None: ...
