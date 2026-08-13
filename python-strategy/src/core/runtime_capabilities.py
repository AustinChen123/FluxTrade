from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Protocol, Sequence

from src.core.interfaces import IExchangeAdapter, IOrderRepository
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.models import Candlestick, Signal

if TYPE_CHECKING:
    from src.core.execution import ExecutionEngine, ExitDecision
    from src.core.ops_safety import OpsSafetyService
    from src.core.risk_manager import AccountService
    from src.core.runtime_environment import RuntimeEnvironment


OrderEventApply = Callable[[], dict[str, object]]
PendingProtectionFillResult = list[dict[str, object]] | None


class OrderEventProcessor(Protocol):
    def __call__(
        self,
        repository: IOrderRepository,
        event: ExchangeOrderEvent,
        apply_event: OrderEventApply,
    ) -> dict[str, object]: ...


class PendingProtectionFillProcessor(Protocol):
    def __call__(
        self,
        repository: IOrderRepository,
        entry_order: object,
        related_orders: Sequence[object],
    ) -> PendingProtectionFillResult: ...


def process_order_event_without_venue_policy(
    repository: IOrderRepository,
    event: ExchangeOrderEvent,
    apply_event: OrderEventApply,
) -> dict[str, object]:
    """Apply an order event when no venue-specific projection is selected."""
    return apply_event()


def audit_pending_protection_without_venue_policy(
    repository: IOrderRepository,
    entry_order: object,
    related_orders: Sequence[object],
) -> PendingProtectionFillResult:
    """Leave pending protection to generic placement policy."""
    return None


class RuntimeBootstrap(Protocol):
    @property
    def profile(self) -> str | None: ...

    @property
    def account_id(self) -> str | None: ...

    def resolve_reconciliation_schedule(
        self,
        *,
        generic_enabled: bool,
        generic_interval_resolver: Callable[[], float],
    ) -> tuple[bool, float | None]: ...

    def resolve_order_account_identity(
        self,
        product_id: str,
        *,
        is_backtest: bool,
    ) -> OrderAccountIdentity | None: ...

    def process_order_event(
        self,
        repository: IOrderRepository,
        event: ExchangeOrderEvent,
        apply_event: OrderEventApply,
    ) -> dict[str, object]: ...

    def audit_pending_protection_fill(
        self,
        repository: IOrderRepository,
        entry_order: object,
        related_orders: Sequence[object],
    ) -> PendingProtectionFillResult: ...


@dataclass(frozen=True)
class OrderAccountIdentity:
    account_profile: str
    account_id: str

    def __post_init__(self) -> None:
        profile = self.account_profile.strip()
        account_id = self.account_id.strip()
        if not profile or not account_id:
            raise ValueError("account identity must not be blank")
        object.__setattr__(self, "account_profile", profile)
        object.__setattr__(self, "account_id", account_id)


class OrderAccountIdentityResolver(Protocol):
    def __call__(
        self,
        product_id: str,
        *,
        is_backtest: bool,
    ) -> OrderAccountIdentity | None: ...


@dataclass(frozen=True)
class DefaultRuntimeBootstrap:
    profile: str | None = None
    account_id: str | None = None

    def resolve_reconciliation_schedule(
        self,
        *,
        generic_enabled: bool,
        generic_interval_resolver: Callable[[], float],
    ) -> tuple[bool, float | None]:
        if not generic_enabled:
            return False, None
        return True, generic_interval_resolver()

    def resolve_order_account_identity(
        self,
        product_id: str,
        *,
        is_backtest: bool,
    ) -> OrderAccountIdentity | None:
        return None

    def process_order_event(
        self,
        repository: IOrderRepository,
        event: ExchangeOrderEvent,
        apply_event: OrderEventApply,
    ) -> dict[str, object]:
        return process_order_event_without_venue_policy(
            repository,
            event,
            apply_event,
        )

    def audit_pending_protection_fill(
        self,
        repository: IOrderRepository,
        entry_order: object,
        related_orders: Sequence[object],
    ) -> PendingProtectionFillResult:
        return audit_pending_protection_without_venue_policy(
            repository,
            entry_order,
            related_orders,
        )


class RuntimeBootstrapFactory(Protocol):
    def __call__(
        self,
        *,
        adapter: IExchangeAdapter,
        adapter_config: dict[str, Any],
        audit_external_orders: bool,
        account_service: AccountService,
        runtime_environment: RuntimeEnvironment,
    ) -> RuntimeBootstrap: ...


@dataclass(frozen=True)
class RuntimeCallbacks:
    is_running: Callable[[], bool]
    publish_worker: Callable[[threading.Thread], None]
    on_runtime_started: Callable[[], None]
    reconcile_if_needed: Callable[[], bool]
    process_event: Callable[[ExchangeOrderEvent], dict[str, Any]]
    lockdown: Callable[[str], None]
    assert_runtime_leadership: Callable[[], None]
    halt_submissions: Callable[[], None]
    clear_local_halt: Callable[[], None]
    persist_lockdown_state: Callable[[str, str], None]
    persist_redis_lockdown: Callable[[], object]
    stop_order_event_stream: Callable[..., bool]
    start_order_event_stream: Callable[[], None]
    current_order_event_thread: Callable[[], threading.Thread | None]
    publish_authoritative_summary: Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class StartupReconciliationState:
    owner_handled: bool
    entry_admission_safe: bool
    blocking_reason: str | None


@dataclass(frozen=True)
class KillSwitchClearPreparation:
    allowed: bool
    drift_generation: int | None
    blocking_reason: str | None


class RuntimeCapabilities(Protocol):
    def start_order_event_stream(self) -> bool: ...

    def on_order_runtime_started(self) -> None: ...

    def reconcile_order_reconnect(self) -> bool | None: ...

    def detect_external_order_drift(self, reason: str) -> None: ...

    def prepare_kill_switch_clear(self) -> KillSwitchClearPreparation: ...

    def current_external_order_drift_generation(self) -> int: ...

    def finalize_external_order_drift_clear(
        self,
        *,
        prepared_generation: int,
        clear_succeeded: bool,
    ) -> None: ...

    def reconcile_startup(
        self,
        fallback: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any] | None: ...

    def publish_authoritative_summary(self, summary: dict[str, Any]) -> None: ...

    def select_runtime_reconciliation(
        self,
        fallback: Callable[[], object],
        run_exclusive: Callable[[Callable[[], object]], object],
    ) -> tuple[bool, Callable[[], object]]: ...

    def run_startup_balance_reconciliation(
        self,
        fallback: Callable[[], object],
    ) -> object | None: ...

    def classify_startup_reconciliation(
        self,
        summary: Any,
    ) -> StartupReconciliationState: ...

    def run_emergency_flatten(
        self,
        fallback: Callable[[], dict[str, Any]],
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict[str, Any]: ...

    def requires_authoritative_flatten_verification(self) -> bool: ...

    def route_authoritative_exit(
        self,
        signal: Signal,
        candle: Candlestick | None,
        portfolio_id_for_sleeve: Callable[[str], str | None],
        execute_authoritative_exit_signal: Callable[
            [
                Signal,
                Candlestick | None,
                Callable[[Signal, ExitDecision], dict[str, object]],
            ],
            bool,
        ],
    ) -> tuple[bool, bool]: ...


class RuntimeCapabilitiesFactory(Protocol):
    def __call__(
        self,
        *,
        adapter: IExchangeAdapter,
        profile: str | None,
        account_id: str | None,
        execution_engine: ExecutionEngine,
        account_service: AccountService,
        ops_safety: OpsSafetyService,
        stop_event: threading.Event,
        callbacks: RuntimeCallbacks,
        logger: logging.Logger,
    ) -> RuntimeCapabilities: ...


class NoopRuntimeCapabilities:
    def start_order_event_stream(self) -> bool:
        return False

    def on_order_runtime_started(self) -> None:
        return None

    def reconcile_order_reconnect(self) -> bool | None:
        return True

    def detect_external_order_drift(self, reason: str) -> None:
        raise RuntimeError("venue external-order drift owner is unavailable")

    def prepare_kill_switch_clear(self) -> KillSwitchClearPreparation:
        return KillSwitchClearPreparation(True, None, None)

    def current_external_order_drift_generation(self) -> int:
        return 0

    def finalize_external_order_drift_clear(
        self,
        *,
        prepared_generation: int,
        clear_succeeded: bool,
    ) -> None:
        raise RuntimeError("venue external-order drift owner is unavailable")

    def reconcile_startup(
        self,
        fallback: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        return fallback()

    def publish_authoritative_summary(self, summary: dict[str, Any]) -> None:
        raise RuntimeError("venue authoritative summary owner is unavailable")

    def select_runtime_reconciliation(
        self,
        fallback: Callable[[], object],
        run_exclusive: Callable[[Callable[[], object]], object],
    ) -> tuple[bool, Callable[[], object]]:
        return False, fallback

    def run_startup_balance_reconciliation(
        self,
        fallback: Callable[[], object],
    ) -> object | None:
        return fallback()

    def classify_startup_reconciliation(
        self,
        summary: Any,
    ) -> StartupReconciliationState:
        return StartupReconciliationState(False, False, None)

    def run_emergency_flatten(
        self,
        fallback: Callable[[], dict[str, Any]],
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        return fallback()

    def requires_authoritative_flatten_verification(self) -> bool:
        return False

    def route_authoritative_exit(
        self,
        signal: Signal,
        candle: Candlestick | None,
        portfolio_id_for_sleeve: Callable[[str], str | None],
        execute_authoritative_exit_signal: Callable[
            [
                Signal,
                Candlestick | None,
                Callable[[Signal, ExitDecision], dict[str, object]],
            ],
            bool,
        ],
    ) -> tuple[bool, bool]:
        return False, False
