from decimal import Decimal
import threading

import pytest

from src.core.portfolio_runtime import (
    PortfolioCoordinator,
    PortfolioDefinition,
    PortfolioSleeve,
)
from src.core.runtime_artifact_registry import RuntimeArtifactRegistry
from src.core.strategy_registry import StrategyRegistry


@pytest.fixture
def registry_owner():
    active_states: list[dict[str, str]] = []
    active_counts: list[int] = []
    strategy_registry = StrategyRegistry()
    coordinator = PortfolioCoordinator()
    owner = RuntimeArtifactRegistry(
        strategy_registry=strategy_registry,
        portfolio_coordinator=coordinator,
        state_lock=threading.Lock(),
        market_processing_lock=threading.Lock(),
        publish_active_state=active_states.append,
        record_active_count=active_counts.append,
    )
    return owner, strategy_registry, coordinator, active_states, active_counts


def _portfolio(mock_strategy_class) -> PortfolioDefinition:
    return PortfolioDefinition(
        portfolio_id="portfolio",
        product_id="PRODUCT",
        sleeves=(
            PortfolioSleeve(mock_strategy_class("sleeve-a", "PRODUCT")),
            PortfolioSleeve(mock_strategy_class("sleeve-b", "PRODUCT")),
        ),
        max_gross_quantity=Decimal("2"),
    )


def test_strategy_replacement_updates_every_runtime_identity(
    registry_owner,
    mock_strategy_class,
) -> None:
    owner, strategy_registry, _coordinator, _states, counts = registry_owner
    original = mock_strategy_class("strategy", "OLD")
    replacement = mock_strategy_class("strategy", "NEW")

    owner.register_strategy(original)
    owner.register_strategy(replacement)

    assert owner.strategy_instances == {"strategy": replacement}
    assert owner.strategies == {"OLD": [], "NEW": [replacement]}
    assert strategy_registry.get("strategy") is replacement
    assert counts == [1, 1]


def test_portfolio_sleeve_replacement_updates_both_definition_owners(
    registry_owner,
    mock_strategy_class,
) -> None:
    owner, _strategy_registry, coordinator, _states, _counts = registry_owner
    definition = _portfolio(mock_strategy_class)
    owner.register_portfolio(definition)
    replacement = mock_strategy_class("sleeve-a", "PRODUCT")

    owner.register_strategy(replacement)

    runtime_definition = owner.portfolio_instances["portfolio"]
    coordinator_definition = coordinator.get("portfolio")
    assert runtime_definition.sleeves[0].strategy is replacement
    assert coordinator_definition is runtime_definition


def test_portfolio_registration_rolls_back_every_partial_identity(
    registry_owner,
    mock_strategy_class,
    monkeypatch,
) -> None:
    owner, strategy_registry, coordinator, _states, counts = registry_owner
    definition = _portfolio(mock_strategy_class)
    register_strategy = owner.register_strategy
    calls = 0

    def fail_second_registration(strategy) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected registration failure")
        register_strategy(strategy)

    monkeypatch.setattr(owner, "register_strategy", fail_second_registration)

    with pytest.raises(RuntimeError, match="injected registration failure"):
        owner.register_portfolio(definition)

    assert owner.strategy_instances == {}
    assert owner.portfolio_instances == {}
    assert coordinator.get("portfolio") is None
    assert strategy_registry.get("sleeve-a") is None
    assert counts == [1, 0]


def test_portfolio_collision_fails_before_coordinator_mutation(
    registry_owner,
    mock_strategy_class,
) -> None:
    owner, _strategy_registry, coordinator, _states, _counts = registry_owner
    existing = mock_strategy_class("sleeve-a", "PRODUCT")
    owner.register_strategy(existing)
    definition = _portfolio(mock_strategy_class)

    with pytest.raises(
        ValueError,
        match=r"portfolio runtime IDs are already active: \['sleeve-a'\]",
    ):
        owner.register_portfolio(definition)

    assert owner.strategy_instances == {"sleeve-a": existing}
    assert owner.portfolio_instances == {}
    assert coordinator.get("portfolio") is None


def test_parent_unregister_removes_all_sleeves_but_sleeve_unregister_is_rejected(
    registry_owner,
    mock_strategy_class,
) -> None:
    owner, strategy_registry, coordinator, active_states, counts = registry_owner
    definition = _portfolio(mock_strategy_class)
    owner.register_portfolio(definition, publish_active_state=True)

    with pytest.raises(
        ValueError,
        match="portfolio sleeves must be controlled through the portfolio ID",
    ):
        owner.unregister("sleeve-a")

    assert set(owner.strategy_instances) == {"sleeve-a", "sleeve-b"}
    assert owner.unregister("portfolio") is True
    assert owner.strategy_instances == {}
    assert owner.portfolio_instances == {}
    assert coordinator.get("portfolio") is None
    assert strategy_registry.get("sleeve-a") is None
    assert strategy_registry.get("sleeve-b") is None
    assert active_states == [{"strategy_id": "portfolio", "status": "ACTIVE"}]
    assert counts == [1, 2, 1, 0]
