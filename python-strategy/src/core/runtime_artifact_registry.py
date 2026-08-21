"""Atomic runtime-only strategy and portfolio registration."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from src.core.models import StrategyStatus
from src.core.portfolio_runtime import PortfolioCoordinator, PortfolioDefinition
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class RuntimeArtifactRegistry:
    """Own the in-memory identities exposed to market processing."""

    def __init__(
        self,
        *,
        strategy_registry: StrategyRegistry,
        portfolio_coordinator: PortfolioCoordinator,
        state_lock: threading.Lock,
        market_processing_lock: threading.Lock,
        publish_active_state: Callable[[dict[str, str]], None],
        record_active_count: Callable[[int], None],
        event_logger: logging.Logger = logger,
    ) -> None:
        self.strategies: dict[str, list[BaseStrategy]] = {}
        self.strategy_instances: dict[str, BaseStrategy] = {}
        self.portfolio_instances: dict[str, PortfolioDefinition] = {}
        self.registration_lock = threading.RLock()
        self._strategy_registry = strategy_registry
        self._portfolio_coordinator = portfolio_coordinator
        self._state_lock = state_lock
        self._market_processing_lock = market_processing_lock
        self._publish_active_state = publish_active_state
        self._record_active_count = record_active_count
        self._logger = event_logger

    def register_strategy(self, instance: BaseStrategy) -> None:
        """Register or replace one live strategy instance."""
        with self.registration_lock:
            updated_portfolio = self._portfolio_coordinator.replace_sleeve_strategy(
                instance
            )
            with self._state_lock:
                old = self.strategy_instances.get(instance.strategy_id)
                if old is not None and old.product_id in self.strategies:
                    self.strategies[old.product_id] = [
                        strategy
                        for strategy in self.strategies[old.product_id]
                        if strategy.strategy_id != instance.strategy_id
                    ]
                self.strategy_instances[instance.strategy_id] = instance
                self.strategies.setdefault(instance.product_id, []).append(instance)
                self._strategy_registry.register(instance)
                if (
                    updated_portfolio is not None
                    and updated_portfolio.portfolio_id in self.portfolio_instances
                ):
                    self.portfolio_instances[updated_portfolio.portfolio_id] = (
                        updated_portfolio
                    )
                self._record_active_count(len(self.strategy_instances))

    def register_portfolio(
        self,
        definition: PortfolioDefinition,
        *,
        publish_active_state: bool = False,
    ) -> None:
        """Atomically expose a complete portfolio at the market boundary."""
        sleeve_ids = [sleeve.strategy.strategy_id for sleeve in definition.sleeves]
        new_ids = {definition.portfolio_id, *sleeve_ids}
        with self.registration_lock, self._market_processing_lock:
            with self._state_lock:
                existing_ids = {
                    *self.strategy_instances,
                    *self.portfolio_instances,
                }
                collisions = sorted(new_ids & existing_ids)
            if collisions:
                raise ValueError(
                    f"portfolio runtime IDs are already active: {collisions}"
                )

            registered: list[str] = []
            self._portfolio_coordinator.register(definition)
            try:
                for sleeve in definition.sleeves:
                    self.register_strategy(sleeve.strategy)
                    registered.append(sleeve.strategy.strategy_id)
                with self._state_lock:
                    self.portfolio_instances[definition.portfolio_id] = definition
            except Exception:
                for strategy_id in reversed(registered):
                    self.unregister_strategy(strategy_id)
                self._portfolio_coordinator.unregister(definition.portfolio_id)
                raise
        if publish_active_state:
            self._publish_active_state(
                {
                    "strategy_id": definition.portfolio_id,
                    "status": StrategyStatus.ACTIVE.value,
                }
            )
        self._logger.info(
            "Registered portfolio %s with %s sleeve(s)",
            definition.portfolio_id,
            len(definition.sleeves),
        )

    def unregister(self, runtime_id: str) -> bool:
        """Remove a parent portfolio or one standalone strategy."""
        with self.registration_lock, self._market_processing_lock:
            return self.unregister_locked(runtime_id)

    def unregister_locked(self, runtime_id: str) -> bool:
        definition = self._portfolio_coordinator.unregister(runtime_id)
        if definition is not None:
            for sleeve in definition.sleeves:
                self.unregister_strategy(sleeve.strategy.strategy_id)
            with self._state_lock:
                self.portfolio_instances.pop(runtime_id, None)
            return True
        if self._portfolio_coordinator.portfolio_id_for_sleeve(runtime_id) is not None:
            raise ValueError(
                "portfolio sleeves must be controlled through the portfolio ID"
            )
        return self.unregister_strategy(runtime_id)

    def unregister_strategy(self, strategy_id: str) -> bool:
        """Remove one live strategy instance from runtime-only structures."""
        with self.registration_lock:
            with self._state_lock:
                instance = self.strategy_instances.pop(strategy_id, None)
                if instance is None:
                    return False
                product_id = instance.product_id
                if product_id in self.strategies:
                    self.strategies[product_id] = [
                        strategy
                        for strategy in self.strategies[product_id]
                        if strategy.strategy_id != strategy_id
                    ]
                self._strategy_registry.unregister(strategy_id)
                self._record_active_count(len(self.strategy_instances))
                return True
