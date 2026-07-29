"""Tests for deterministic portfolio construction and decision coordination."""

from __future__ import annotations

from decimal import Decimal

import pytest

from src.core.models import Candlestick, Position, PositionSide, Signal, SignalType
from src.core.portfolio_runtime import (
    ActivationWindow,
    PortfolioCoordinator,
    PortfolioDecisionRejected,
    PortfolioDefinition,
    PortfolioExposureSnapshot,
    PortfolioFactory,
    PortfolioSleeve,
    build_portfolio_artifact,
)
from src.strategies.base import BaseStrategy, StrategyRequirements


PRODUCT = "RITHMIC:MNQ_ROLL-PERP"
TIMESTAMP = 1_700_000_000_000


class SleeveStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 2)

    def on_candle(self, candle: Candlestick):
        return None

    def replay_configuration(self) -> object:
        return {
            "strategy_id": self.strategy_id,
            "product_id": self.product_id,
        }


def _candle(timestamp: int = TIMESTAMP) -> Candlestick:
    return Candlestick(
        product_id=PRODUCT,
        timeframe="5m",
        timestamp=timestamp,
        open=Decimal("20000"),
        high=Decimal("20001"),
        low=Decimal("19999"),
        close=Decimal("20000"),
        volume=Decimal("10"),
    )


def _signal(
    strategy_id: str,
    signal_type: SignalType,
    *,
    timestamp: int = TIMESTAMP,
    quantity: Decimal | None = Decimal("1"),
) -> Signal:
    return Signal(
        strategy_id=strategy_id,
        product_id=PRODUCT,
        timeframe="5m",
        timestamp=timestamp,
        type=signal_type,
        quantity=quantity,
    )


def _definition(
    *strategy_ids: str,
    max_gross_quantity: Decimal | None = None,
    windows: dict[str, tuple[ActivationWindow, ...]] | None = None,
) -> PortfolioDefinition:
    return PortfolioDefinition(
        portfolio_id="portfolio_v1",
        product_id=PRODUCT,
        sleeves=tuple(
            PortfolioSleeve(
                SleeveStrategy(strategy_id, PRODUCT),
                (windows or {}).get(strategy_id, ()),
            )
            for strategy_id in strategy_ids
        ),
        max_gross_quantity=max_gross_quantity or Decimal(len(strategy_ids)),
    )


def _position(
    strategy_id: str,
    side: PositionSide,
    quantity: str = "1",
) -> Position:
    return Position(
        strategy_id=strategy_id,
        product_id=PRODUCT,
        side=side,
        quantity=Decimal(quantity),
        entry_price=Decimal("20000"),
        unrealized_pnl=Decimal("0"),
    )


def _coordinate(
    definition: PortfolioDefinition,
    decisions: list[tuple[str, list[Signal]]],
    positions: dict[str, Position] | None = None,
    pending_entries: dict[str, Decimal] | None = None,
) -> list[tuple[str, list[Signal]]]:
    coordinator = PortfolioCoordinator()
    coordinator.register(definition)
    exposure = dict(pending_entries or {})
    for strategy_id, position in (positions or {}).items():
        signed = (
            position.quantity
            if position.side == PositionSide.LONG
            else -position.quantity
        )
        exposure[strategy_id] = exposure.get(strategy_id, Decimal("0")) + signed
    return coordinator.coordinate_candle_decisions(
        _candle(),
        decisions,
        exposure_loader=lambda _strategy_ids, _product_id, _client_order_ids: (
            PortfolioExposureSnapshot(exposure)
        ),
        default_quantity=Decimal("1"),
    )


def test_same_direction_sleeves_are_kept_in_definition_order() -> None:
    definition = _definition("sleeve_a", "sleeve_b")

    result = _coordinate(
        definition,
        [
            ("sleeve_b", [_signal("sleeve_b", SignalType.LONG)]),
            ("sleeve_a", [_signal("sleeve_a", SignalType.LONG)]),
        ],
    )

    assert [strategy_id for strategy_id, _ in result] == [
        "sleeve_a",
        "sleeve_b",
    ]


def test_opposing_entry_batch_is_rejected_before_any_signal_is_returned() -> None:
    definition = _definition("sleeve_a", "sleeve_b")

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_opposing_exposure",
    ):
        _coordinate(
            definition,
            [
                ("sleeve_a", [_signal("sleeve_a", SignalType.LONG)]),
                ("sleeve_b", [_signal("sleeve_b", SignalType.SHORT)]),
            ],
        )


def test_gross_limit_counts_existing_and_new_sleeve_exposure() -> None:
    definition = _definition(
        "sleeve_a",
        "sleeve_b",
        max_gross_quantity=Decimal("1"),
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_gross_limit_exceeded",
    ):
        _coordinate(
            definition,
            [
                ("sleeve_a", []),
                ("sleeve_b", [_signal("sleeve_b", SignalType.LONG)]),
            ],
            positions={
                "sleeve_a": _position("sleeve_a", PositionSide.LONG),
            },
        )


def test_gross_limit_counts_still_working_entry_orders() -> None:
    definition = _definition(
        "sleeve_a",
        "sleeve_b",
        max_gross_quantity=Decimal("1"),
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_gross_limit_exceeded",
    ):
        _coordinate(
            definition,
            [
                ("sleeve_a", []),
                ("sleeve_b", [_signal("sleeve_b", SignalType.LONG)]),
            ],
            pending_entries={"sleeve_a": Decimal("1")},
        )


def test_each_dispatched_signal_must_stay_within_gross_limit() -> None:
    definition = _definition(
        "sleeve_a",
        max_gross_quantity=Decimal("1"),
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_gross_limit_exceeded",
    ):
        _coordinate(
            definition,
            [
                (
                    "sleeve_a",
                    [
                        _signal("sleeve_a", SignalType.LONG),
                        _signal("sleeve_a", SignalType.EXIT_LONG),
                    ],
                )
            ],
            positions={
                "sleeve_a": _position("sleeve_a", PositionSide.LONG),
            },
        )


def test_each_dispatched_signal_must_avoid_intermediate_opposing_exposure() -> None:
    definition = _definition(
        "sleeve_b",
        "sleeve_a",
        max_gross_quantity=Decimal("2"),
    )

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_opposing_exposure",
    ):
        _coordinate(
            definition,
            [
                ("sleeve_b", [_signal("sleeve_b", SignalType.SHORT)]),
                ("sleeve_a", [_signal("sleeve_a", SignalType.EXIT_LONG)]),
            ],
            positions={
                "sleeve_a": _position("sleeve_a", PositionSide.LONG),
            },
        )


def test_inactive_window_suppresses_entry_but_preserves_exit() -> None:
    definition = _definition(
        "sleeve_a",
        windows={
            "sleeve_a": (
                ActivationWindow(
                    start_ms=TIMESTAMP + 1,
                    end_ms=TIMESTAMP + 100,
                ),
            )
        },
    )

    entry = _coordinate(
        definition,
        [("sleeve_a", [_signal("sleeve_a", SignalType.LONG)])],
    )
    exit_decision = _coordinate(
        definition,
        [("sleeve_a", [_signal("sleeve_a", SignalType.EXIT_LONG)])],
        positions={
            "sleeve_a": _position("sleeve_a", PositionSide.LONG),
        },
    )

    assert entry == [("sleeve_a", [])]
    assert exit_decision == [
        ("sleeve_a", [_signal("sleeve_a", SignalType.EXIT_LONG)])
    ]


@pytest.mark.parametrize(
    ("decisions", "reason"),
    [
        (
            [("sleeve_a", [_signal("other", SignalType.LONG)])],
            "portfolio_signal_owner_mismatch",
        ),
        (
            [("sleeve_a", [])],
            "portfolio_decision_batch_incomplete",
        ),
    ],
)
def test_unowned_or_incomplete_batch_is_rejected(decisions, reason) -> None:
    definition = (
        _definition("sleeve_a", "sleeve_b")
        if reason == "portfolio_decision_batch_incomplete"
        else _definition("sleeve_a")
    )

    with pytest.raises(PortfolioDecisionRejected, match=reason):
        _coordinate(definition, decisions)


def test_existing_opposing_exposure_requires_reconciliation() -> None:
    definition = _definition("sleeve_a", "sleeve_b")

    with pytest.raises(
        PortfolioDecisionRejected,
        match="portfolio_existing_opposing_exposure",
    ):
        _coordinate(
            definition,
            [("sleeve_a", []), ("sleeve_b", [])],
            positions={
                "sleeve_a": _position("sleeve_a", PositionSide.LONG),
                "sleeve_b": _position("sleeve_b", PositionSide.SHORT),
            },
        )


def test_definition_rejects_duplicate_sleeve_ids_and_mixed_timeframes() -> None:
    duplicate = SleeveStrategy("duplicate", PRODUCT)
    with pytest.raises(ValueError, match="unique"):
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=PRODUCT,
            sleeves=(PortfolioSleeve(duplicate), PortfolioSleeve(duplicate)),
            max_gross_quantity=Decimal("2"),
        )


def test_artifact_factory_must_build_replay_safe_definition() -> None:
    class NondeterministicFactory(PortfolioFactory):
        build_count = 0

        def build(self, *, portfolio_id, product_id, config):
            type(self).build_count += 1
            return PortfolioDefinition(
                portfolio_id=portfolio_id,
                product_id=product_id,
                sleeves=(
                    PortfolioSleeve(
                        SleeveStrategy(
                            f"sleeve_{type(self).build_count}",
                            product_id,
                        )
                    ),
                ),
                max_gross_quantity=Decimal("1"),
            )

    with pytest.raises(ValueError, match="not deterministic"):
        build_portfolio_artifact(
            NondeterministicFactory,
            portfolio_id="portfolio_v1",
            product_id=PRODUCT,
            config={},
        )


def test_artifact_factory_rejects_parameter_drift() -> None:
    class ParameterizedSleeve(SleeveStrategy):
        def __init__(self, strategy_id, product_id, threshold):
            super().__init__(strategy_id, product_id)
            self.threshold = threshold

        def replay_configuration(self) -> object:
            return {"threshold": self.threshold}

    class DriftingFactory(PortfolioFactory):
        build_count = 0

        def build(self, *, portfolio_id, product_id, config):
            type(self).build_count += 1
            return PortfolioDefinition(
                portfolio_id=portfolio_id,
                product_id=product_id,
                sleeves=(
                    PortfolioSleeve(
                        ParameterizedSleeve(
                            "sleeve",
                            product_id,
                            type(self).build_count,
                        )
                    ),
                ),
                max_gross_quantity=Decimal("1"),
            )

    with pytest.raises(ValueError, match="not deterministic"):
        build_portfolio_artifact(
            DriftingFactory,
            portfolio_id="portfolio_v1",
            product_id=PRODUCT,
            config={},
        )


def test_open_ended_activation_window_must_be_last() -> None:
    with pytest.raises(ValueError, match="ordered and non-overlapping"):
        PortfolioSleeve(
            SleeveStrategy("sleeve", PRODUCT),
            (
                ActivationWindow(start_ms=TIMESTAMP),
                ActivationWindow(
                    start_ms=TIMESTAMP + 100,
                    end_ms=TIMESTAMP + 200,
                ),
            ),
        )


def test_portfolio_id_must_not_equal_sleeve_id() -> None:
    with pytest.raises(ValueError, match="must differ"):
        PortfolioDefinition(
            portfolio_id="same",
            product_id=PRODUCT,
            sleeves=(PortfolioSleeve(SleeveStrategy("same", PRODUCT)),),
            max_gross_quantity=Decimal("1"),
        )


def test_replay_replacement_updates_registered_definition() -> None:
    definition = _definition("sleeve_a")
    coordinator = PortfolioCoordinator()
    coordinator.register(definition)
    replacement = SleeveStrategy("sleeve_a", PRODUCT)

    updated = coordinator.replace_sleeve_strategy(replacement)

    assert updated is not None
    assert updated.sleeves[0].strategy is replacement
    assert coordinator.get("portfolio_v1") is updated

    class OneMinuteSleeve(SleeveStrategy):
        @property
        def requirements(self) -> StrategyRequirements:
            return StrategyRequirements(self.product_id, "1m", 2)

    with pytest.raises(ValueError, match="share"):
        PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id=PRODUCT,
            sleeves=(
                PortfolioSleeve(SleeveStrategy("five", PRODUCT)),
                PortfolioSleeve(OneMinuteSleeve("one", PRODUCT)),
            ),
            max_gross_quantity=Decimal("2"),
        )
