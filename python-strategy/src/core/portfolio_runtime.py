"""Deterministic multi-strategy portfolio construction and decision gating."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy


class PortfolioDecisionRejected(RuntimeError):
    """Raised before submission when a portfolio decision is ambiguous or unsafe."""


@dataclass(frozen=True)
class PortfolioExposureSnapshot:
    """Atomic projected exposure and already-persisted intent identities."""

    quantities: Mapping[str, Decimal]
    existing_client_order_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ActivationWindow:
    """Half-open millisecond interval in which one sleeve may open exposure."""

    start_ms: int | None = None
    end_ms: int | None = None

    def __post_init__(self) -> None:
        if (
            self.start_ms is not None
            and self.end_ms is not None
            and self.start_ms >= self.end_ms
        ):
            raise ValueError("portfolio activation window must be non-empty")

    def contains(self, timestamp_ms: int) -> bool:
        return (
            (self.start_ms is None or timestamp_ms >= self.start_ms)
            and (self.end_ms is None or timestamp_ms < self.end_ms)
        )


@dataclass(frozen=True)
class PortfolioSleeve:
    """One independently attributable strategy instance in a portfolio."""

    strategy: BaseStrategy
    activation_windows: tuple[ActivationWindow, ...] = ()

    def __post_init__(self) -> None:
        previous_end: int | None = None
        for index, window in enumerate(self.activation_windows):
            if index > 0 and (
                previous_end is None
                or window.start_ms is None
                or window.start_ms < previous_end
            ):
                raise ValueError(
                    "portfolio activation windows must be ordered and non-overlapping"
                )
            previous_end = window.end_ms

    def may_enter_at(self, timestamp_ms: int) -> bool:
        return not self.activation_windows or any(
            window.contains(timestamp_ms) for window in self.activation_windows
        )


@dataclass(frozen=True)
class PortfolioDefinition:
    """Immutable runtime definition returned by a hash-pinned artifact factory."""

    portfolio_id: str
    product_id: str
    sleeves: tuple[PortfolioSleeve, ...]
    max_gross_quantity: Decimal
    artifact_version: str | None = None
    readiness: str | None = None
    catalog_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.portfolio_id:
            raise ValueError("portfolio_id must be non-empty")
        if not self.product_id:
            raise ValueError("portfolio product_id must be non-empty")
        if not self.sleeves:
            raise ValueError("portfolio must contain at least one sleeve")
        if not self.max_gross_quantity.is_finite() or self.max_gross_quantity <= 0:
            raise ValueError("portfolio max_gross_quantity must be positive and finite")

        strategy_ids = [sleeve.strategy.strategy_id for sleeve in self.sleeves]
        if len(strategy_ids) != len(set(strategy_ids)):
            raise ValueError("portfolio sleeve strategy IDs must be unique")
        if self.portfolio_id in strategy_ids:
            raise ValueError("portfolio ID must differ from every sleeve strategy ID")

        requirements = {
            (
                sleeve.strategy.product_id,
                sleeve.strategy.requirements.timeframe,
            )
            for sleeve in self.sleeves
        }
        if requirements != {
            (
                self.product_id,
                self.sleeves[0].strategy.requirements.timeframe,
            )
        }:
            raise ValueError(
                "portfolio sleeves must share the definition product and timeframe"
            )


class PortfolioFactory(ABC):
    """Artifact-side factory for deterministic strategy sleeve construction."""

    @abstractmethod
    def build(
        self,
        *,
        portfolio_id: str,
        product_id: str,
        config: Mapping[str, Any],
    ) -> PortfolioDefinition:
        """Build a fresh portfolio without registering or executing it."""


def build_portfolio_artifact(
    factory_cls: type[PortfolioFactory],
    *,
    portfolio_id: str,
    product_id: str,
    config: Mapping[str, Any],
) -> PortfolioDefinition:
    """Build and bind loader-verified provenance to one portfolio definition."""
    definitions = tuple(
        factory_cls().build(
            portfolio_id=portfolio_id,
            product_id=product_id,
            config=config,
        )
        for _ in range(2)
    )
    for definition in definitions:
        if not isinstance(definition, PortfolioDefinition):
            raise TypeError("portfolio factory must return PortfolioDefinition")
        if definition.portfolio_id != portfolio_id:
            raise ValueError("portfolio factory changed portfolio_id")
        if definition.product_id != product_id:
            raise ValueError("portfolio factory changed product_id")
    definition, replay_definition = definitions
    if any(
        first.strategy is second.strategy
        for first, second in zip(
            definition.sleeves,
            replay_definition.sleeves,
            strict=False,
        )
    ) or _portfolio_replay_signature(
        definition
    ) != _portfolio_replay_signature(replay_definition):
        raise ValueError("portfolio factory is not deterministic and replay-safe")
    return replace(
        definition,
        artifact_version=getattr(
            factory_cls,
            "__fluxtrade_artifact_version__",
            None,
        ),
        readiness=getattr(factory_cls, "__fluxtrade_readiness__", None),
        catalog_sha256=getattr(
            factory_cls,
            "__fluxtrade_catalog_sha256__",
            None,
        ),
    )


def _portfolio_replay_signature(definition: PortfolioDefinition) -> tuple[object, ...]:
    sleeve_signatures: list[tuple[object, ...]] = []
    for sleeve in definition.sleeves:
        try:
            replay_configuration = sleeve.strategy.replay_configuration()
        except NotImplementedError as exc:
            raise ValueError(
                "portfolio sleeve is not replay-safe: "
                f"{sleeve.strategy.strategy_id}"
            ) from exc
        sleeve_signatures.append(
            (
                type(sleeve.strategy),
                sleeve.strategy.strategy_id,
                sleeve.strategy.product_id,
                sleeve.strategy.requirements,
                replay_configuration,
                sleeve.activation_windows,
            )
        )
    return (
        definition.portfolio_id,
        definition.product_id,
        definition.max_gross_quantity,
        tuple(sleeve_signatures),
    )


class PortfolioCoordinator:
    """Coordinate portfolio lifecycle identity and pre-submission decisions."""

    def __init__(self) -> None:
        self._portfolios: dict[str, PortfolioDefinition] = {}
        self._sleeve_owners: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(self, definition: PortfolioDefinition) -> None:
        with self._lock:
            if (
                definition.portfolio_id in self._portfolios
                or definition.portfolio_id in self._sleeve_owners
            ):
                raise ValueError(
                    f"portfolio is already registered: {definition.portfolio_id}"
                )
            duplicates = sorted(
                sleeve.strategy.strategy_id
                for sleeve in definition.sleeves
                if (
                    sleeve.strategy.strategy_id in self._sleeve_owners
                    or sleeve.strategy.strategy_id in self._portfolios
                )
            )
            if duplicates:
                raise ValueError(
                    f"portfolio sleeve IDs are already registered: {duplicates}"
                )
            self._portfolios[definition.portfolio_id] = definition
            for sleeve in definition.sleeves:
                self._sleeve_owners[sleeve.strategy.strategy_id] = (
                    definition.portfolio_id
                )

    def unregister(self, portfolio_id: str) -> PortfolioDefinition | None:
        with self._lock:
            definition = self._portfolios.pop(portfolio_id, None)
            if definition is None:
                return None
            for sleeve in definition.sleeves:
                self._sleeve_owners.pop(sleeve.strategy.strategy_id, None)
            return definition

    def get(self, portfolio_id: str) -> PortfolioDefinition | None:
        with self._lock:
            return self._portfolios.get(portfolio_id)

    def portfolio_id_for_sleeve(self, strategy_id: str) -> str | None:
        with self._lock:
            return self._sleeve_owners.get(strategy_id)

    def replace_sleeve_strategy(
        self,
        replacement: BaseStrategy,
    ) -> PortfolioDefinition | None:
        """Keep the immutable definition aligned after durable replay rebuilds."""
        with self._lock:
            portfolio_id = self._sleeve_owners.get(replacement.strategy_id)
            if portfolio_id is None:
                return None
            definition = self._portfolios[portfolio_id]
            updated_sleeves = tuple(
                (
                    replace(sleeve, strategy=replacement)
                    if sleeve.strategy.strategy_id == replacement.strategy_id
                    else sleeve
                )
                for sleeve in definition.sleeves
            )
            updated = replace(definition, sleeves=updated_sleeves)
            self._portfolios[portfolio_id] = updated
            return updated

    def lifecycle_id_for_strategy(self, strategy_id: str) -> str:
        return self.portfolio_id_for_sleeve(strategy_id) or strategy_id

    def coordinate_candle_decisions(
        self,
        candle: Candlestick,
        decisions: Sequence[tuple[str, list[Signal]]],
        *,
        exposure_loader: (
            Callable[
                [tuple[str, ...], str, Mapping[str, str]],
                PortfolioExposureSnapshot,
            ]
            | None
        ),
        default_quantity: Decimal,
    ) -> list[tuple[str, list[Signal]]]:
        """Validate complete portfolio batches and preserve standalone ordering."""
        with self._lock:
            portfolios = dict(self._portfolios)
            sleeve_owners = dict(self._sleeve_owners)

        grouped: dict[str, list[tuple[str, list[Signal]]]] = {}
        result: list[tuple[str, list[Signal]] | str] = []
        emitted_groups: set[str] = set()
        for strategy_id, signals in decisions:
            portfolio_id = sleeve_owners.get(strategy_id)
            if portfolio_id is None:
                result.append((strategy_id, signals))
                continue
            grouped.setdefault(portfolio_id, []).append((strategy_id, signals))
            if portfolio_id not in emitted_groups:
                result.append(portfolio_id)
                emitted_groups.add(portfolio_id)

        coordinated: list[tuple[str, list[Signal]]] = []
        for item in result:
            if not isinstance(item, str):
                coordinated.append(item)
                continue
            portfolio_id = item
            definition = portfolios[portfolio_id]
            coordinated.extend(
                self._coordinate_portfolio(
                    definition,
                    candle,
                    grouped.get(portfolio_id, []),
                    exposure_loader=exposure_loader,
                    default_quantity=default_quantity,
                )
            )
        return coordinated

    @staticmethod
    def _coordinate_portfolio(
        definition: PortfolioDefinition,
        candle: Candlestick,
        decisions: Sequence[tuple[str, list[Signal]]],
        *,
        exposure_loader: (
            Callable[
                [tuple[str, ...], str, Mapping[str, str]],
                PortfolioExposureSnapshot,
            ]
            | None
        ),
        default_quantity: Decimal,
    ) -> list[tuple[str, list[Signal]]]:
        expected_ids = [
            sleeve.strategy.strategy_id for sleeve in definition.sleeves
        ]
        decision_map = {strategy_id: signals for strategy_id, signals in decisions}
        if len(decision_map) != len(decisions) or set(decision_map) != set(expected_ids):
            raise PortfolioDecisionRejected(
                f"portfolio_decision_batch_incomplete:{definition.portfolio_id}"
            )
        if candle.product_id != definition.product_id:
            raise PortfolioDecisionRejected(
                f"portfolio_candle_product_mismatch:{definition.portfolio_id}"
            )
        if exposure_loader is None:
            raise PortfolioDecisionRejected(
                f"portfolio_exposure_loader_missing:{definition.portfolio_id}"
            )
        if not default_quantity.is_finite() or default_quantity <= 0:
            raise PortfolioDecisionRejected(
                f"portfolio_default_quantity_invalid:{definition.portfolio_id}"
            )

        requested_intents: dict[str, str] = {}
        for strategy_id, signals in decision_map.items():
            for signal in signals:
                client_order_id = str(
                    (signal.metadata or {}).get("client_order_id", "")
                )
                if not client_order_id:
                    continue
                if client_order_id in requested_intents:
                    raise PortfolioDecisionRejected(
                        "portfolio_duplicate_client_order_id:"
                        f"{definition.portfolio_id}"
                    )
                requested_intents[client_order_id] = strategy_id
        try:
            exposure_snapshot = exposure_loader(
                tuple(expected_ids),
                definition.product_id,
                requested_intents,
            )
        except Exception as exc:
            raise PortfolioDecisionRejected(
                f"portfolio_exposure_unavailable:{definition.portfolio_id}"
            ) from exc
        unexpected_ids = set(exposure_snapshot.quantities) - set(expected_ids)
        if unexpected_ids:
            raise PortfolioDecisionRejected(
                "portfolio_exposure_owner_mismatch:"
                f"{definition.portfolio_id}"
            )
        unexpected_client_order_ids = (
            exposure_snapshot.existing_client_order_ids
            - set(requested_intents)
        )
        if unexpected_client_order_ids:
            raise PortfolioDecisionRejected(
                "portfolio_exposure_intent_mismatch:"
                f"{definition.portfolio_id}"
            )
        quantities: dict[str, Decimal] = {}
        for strategy_id in expected_ids:
            projected_quantity = Decimal(
                str(
                    exposure_snapshot.quantities.get(
                        strategy_id,
                        Decimal("0"),
                    )
                )
            )
            if not projected_quantity.is_finite():
                raise PortfolioDecisionRejected(
                    f"portfolio_exposure_invalid:{strategy_id}"
                )
            quantities[strategy_id] = projected_quantity

        PortfolioCoordinator._assert_exposure_invariants(
            definition,
            quantities,
            existing=True,
        )

        coordinated: list[tuple[str, list[Signal]]] = []
        for sleeve in definition.sleeves:
            strategy_id = sleeve.strategy.strategy_id
            accepted: list[Signal] = []
            for signal in decision_map[strategy_id]:
                PortfolioCoordinator._validate_signal(
                    definition,
                    sleeve,
                    candle,
                    signal,
                )
                if signal.type == SignalType.NO_SIGNAL:
                    continue
                is_entry = signal.type in (SignalType.LONG, SignalType.SHORT)
                if is_entry and not sleeve.may_enter_at(candle.timestamp):
                    continue
                quantity = (
                    signal.quantity
                    if signal.quantity is not None
                    else default_quantity
                )
                if not quantity.is_finite() or quantity <= 0:
                    raise PortfolioDecisionRejected(
                        f"portfolio_signal_quantity_invalid:{strategy_id}"
                    )
                client_order_id = str(
                    (signal.metadata or {}).get("client_order_id", "")
                )
                if (
                    client_order_id
                    and client_order_id
                    in exposure_snapshot.existing_client_order_ids
                ):
                    accepted.append(signal)
                    continue
                current = quantities[strategy_id]
                quantities[strategy_id] = PortfolioCoordinator._apply_signal(
                    current,
                    signal.type,
                    quantity,
                    strategy_id,
                )
                PortfolioCoordinator._assert_exposure_invariants(
                    definition,
                    quantities,
                )
                accepted.append(signal)
            coordinated.append((strategy_id, accepted))

        return coordinated

    @staticmethod
    def _assert_exposure_invariants(
        definition: PortfolioDefinition,
        quantities: Mapping[str, Decimal],
        *,
        existing: bool = False,
    ) -> None:
        if PortfolioCoordinator._has_opposing_exposure(quantities.values()):
            reason = (
                "portfolio_existing_opposing_exposure"
                if existing
                else "portfolio_opposing_exposure"
            )
            raise PortfolioDecisionRejected(
                f"{reason}:{definition.portfolio_id}"
            )
        gross = sum(
            (abs(quantity) for quantity in quantities.values()),
            Decimal("0"),
        )
        if gross > definition.max_gross_quantity:
            raise PortfolioDecisionRejected(
                f"portfolio_gross_limit_exceeded:{definition.portfolio_id}:"
                f"{gross}>{definition.max_gross_quantity}"
            )

    @staticmethod
    def _validate_signal(
        definition: PortfolioDefinition,
        sleeve: PortfolioSleeve,
        candle: Candlestick,
        signal: Signal,
    ) -> None:
        strategy_id = sleeve.strategy.strategy_id
        if signal.strategy_id != strategy_id:
            raise PortfolioDecisionRejected(
                f"portfolio_signal_owner_mismatch:{strategy_id}"
            )
        if signal.product_id != definition.product_id:
            raise PortfolioDecisionRejected(
                f"portfolio_signal_product_mismatch:{strategy_id}"
            )
        if signal.timeframe != candle.timeframe:
            raise PortfolioDecisionRejected(
                f"portfolio_signal_timeframe_mismatch:{strategy_id}"
            )
        if signal.timestamp != candle.timestamp:
            raise PortfolioDecisionRejected(
                f"portfolio_signal_timestamp_mismatch:{strategy_id}"
            )

    @staticmethod
    def _apply_signal(
        current: Decimal,
        signal_type: SignalType,
        quantity: Decimal,
        strategy_id: str,
    ) -> Decimal:
        if signal_type == SignalType.LONG:
            if current < 0:
                raise PortfolioDecisionRejected(
                    f"portfolio_entry_crosses_sleeve_position:{strategy_id}"
                )
            return current + quantity
        if signal_type == SignalType.SHORT:
            if current > 0:
                raise PortfolioDecisionRejected(
                    f"portfolio_entry_crosses_sleeve_position:{strategy_id}"
                )
            return current - quantity
        if signal_type == SignalType.EXIT_LONG:
            if current < 0:
                raise PortfolioDecisionRejected(
                    f"portfolio_exit_side_mismatch:{strategy_id}"
                )
            return max(Decimal("0"), current - quantity)
        if signal_type == SignalType.EXIT_SHORT:
            if current > 0:
                raise PortfolioDecisionRejected(
                    f"portfolio_exit_side_mismatch:{strategy_id}"
                )
            return min(Decimal("0"), current + quantity)
        raise PortfolioDecisionRejected(
            f"portfolio_signal_type_unsupported:{strategy_id}"
        )

    @staticmethod
    def _has_opposing_exposure(quantities: Iterable[Decimal]) -> bool:
        quantities = tuple(quantities)
        has_long = any(quantity > 0 for quantity in quantities)
        has_short = any(quantity < 0 for quantity in quantities)
        return has_long and has_short
