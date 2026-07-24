from dataclasses import dataclass
from decimal import Decimal

from src.core.models import Signal


@dataclass(frozen=True, slots=True)
class ConditionalOrderIntent:
    order_type: str
    client_order_suffix: str
    trigger_price: Decimal | None = None
    trailing_distance: Decimal | None = None
    oco_group: str | None = None


def conditional_order_intents(signal: Signal) -> tuple[ConditionalOrderIntent, ...]:
    intents = []
    if signal.stop_loss:
        intents.append(
            ConditionalOrderIntent(
                order_type="stop_loss",
                client_order_suffix="sl",
                trigger_price=signal.stop_loss,
                oco_group="fixed_protection",
            )
        )
    if signal.take_profit:
        intents.append(
            ConditionalOrderIntent(
                order_type="take_profit",
                client_order_suffix="tp",
                trigger_price=signal.take_profit,
                oco_group="fixed_protection",
            )
        )
    if signal.trailing_distance:
        intents.append(
            ConditionalOrderIntent(
                order_type="trailing_stop",
                client_order_suffix="tr",
                trigger_price=signal.stop_loss,
                trailing_distance=signal.trailing_distance,
            )
        )
    return tuple(intents)


def conditional_oco_pairs(
    intents: tuple[ConditionalOrderIntent, ...],
) -> tuple[tuple[int, int], ...]:
    groups = {}
    for index, intent in enumerate(intents):
        if intent.oco_group is not None:
            groups.setdefault(intent.oco_group, []).append(index)
    return tuple(
        (indexes[0], indexes[1])
        for indexes in groups.values()
        if len(indexes) == 2
    )
