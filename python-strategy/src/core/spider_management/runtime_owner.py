"""Private transactional owner for an explicitly selected Spider INSTALL."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from types import MappingProxyType
from typing import ContextManager, cast

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.core.backtest.spider_historical_input import HistoricalRunInput
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.client_order_id import market_signal_client_order_id
from src.core.command_router import CommandResult
from src.core.decimal_math import canonical_decimal_text
from src.core.models import OrderSide, Signal, SignalType
from src.core.orm_models import Order, StrategyState, Trade
from src.core.order_manager import OrderManager
from src.core.product_registry import InstrumentSpec
from src.core.repositories import LiveOrderRepository
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequestKind,
)
from src.core.spider_management.configuration_request_store import _hydrate
from src.core.spider_management.model_bootstrap import (
    ModelBootstrapUnavailable,
    PreparedModelBootstrap,
    prepare_model_bootstrap,
)
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.operation_request_store import _mapping
from src.core.spider_management.financial_delivery import (
    stage_financial_receipt,
    validate_order_receipt_chain,
)
from src.core.spider_management.control_command import (
    SpiderControlCommandError,
    SpiderManagementCommand,
    SpiderControlWriteResult,
    verify_stop_in_transaction,
    write_stop_in_transaction,
)
from src.core.spider_management.orm import (
    SpiderConfigurationRequest,
    SpiderOperationRequestRow,
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimeOperation,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
)
from src.core.strategy_state_manager import (
    StrategyStateManager,
)


@dataclass(frozen=True, slots=True)
class ModelInstallSource:
    """One explicit admitted model input and its trusted composition facts."""

    run: HistoricalRunInput
    product_specifications: tuple[tuple[str, InstrumentSpec], ...]
    selected_venue: str


@dataclass(frozen=True, slots=True)
class RuntimeScopeKey:
    environment: str
    execution_scope_id: str
    strategy_id: str


@dataclass(frozen=True, slots=True)
class SpiderRuntimeComposition:
    """Private trusted launcher input; absent input keeps ordinary startup."""

    owned_scopes: tuple[RuntimeScopeKey, ...]
    model_sources_by_confirmation: Mapping[str, ModelInstallSource]

    def __post_init__(self) -> None:
        if type(self.owned_scopes) is not tuple or not self.owned_scopes:
            raise ValueError("Spider composition requires ordered owned scopes")
        if len({row.strategy_id for row in self.owned_scopes}) != len(self.owned_scopes):
            raise ValueError("Spider composition strategy scopes must be unique")
        if not isinstance(self.model_sources_by_confirmation, Mapping):
            raise ValueError("Spider model sources must be an explicit mapping")
        object.__setattr__(
            self,
            "model_sources_by_confirmation",
            MappingProxyType(dict(self.model_sources_by_confirmation)),
        )


class _ExistingInstallReadback(Exception):
    """Leave the writer transaction before opening an independent reader."""


class SpiderRuntimeOwner:
    """Own INSTALL persistence; all writes share one caller-created PG transaction."""

    def __init__(
        self,
        sessions: Callable[[], ContextManager[Session]],
        order_manager: OrderManager,
        *,
        owned_scopes: tuple[RuntimeScopeKey, ...],
        model_sources_by_confirmation: Mapping[str, ModelInstallSource],
        strategy_state_manager: object | None = None,
        lifecycle_lock: Callable[[str], ContextManager[object]] | None = None,
        order_event_lock: ContextManager[object] | None = None,
        lock_timeout_ms: int = 2000,
        statement_timeout_ms: int = 10000,
    ) -> None:
        if not owned_scopes or len({row.strategy_id for row in owned_scopes}) != len(owned_scopes):
            raise ValueError("Spider owner requires unique explicit strategy scopes")
        if not 1 <= lock_timeout_ms <= 60000 or not lock_timeout_ms <= statement_timeout_ms <= 300000:
            raise ValueError("invalid Spider owner transaction timeouts")
        self._sessions = sessions
        self._order_manager = order_manager
        self._owned_scopes = {row.strategy_id: row for row in owned_scopes}
        self._model_sources = dict(model_sources_by_confirmation)
        self._strategy_state_manager = strategy_state_manager
        self._lifecycle_lock = lifecycle_lock
        self._order_event_lock = order_event_lock
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms

    def participate_in_order_transaction(
        self,
        session: Session,
        order: Order,
        trade: Trade | None,
    ) -> None:
        """Record a bound financial fact in the repository's caller-owned TX."""
        stage_financial_receipt(
            session,
            order,
            trade,
            {
                strategy_id: (scope.environment, scope.execution_scope_id)
                for strategy_id, scope in self._owned_scopes.items()
            },
        )

    def restore_owned_runtimes(self) -> frozenset[str]:
        """Validate committed owned projections before generic ACTIVE restore."""
        owned = frozenset(self._owned_scopes)
        for scope in self._owned_scopes.values():
            try:
                with self._sessions() as session:
                    if (
                        not isinstance(session, Session)
                        or session.in_transaction()
                        or not session.is_active
                        or session.get_bind().dialect.name != "postgresql"
                    ):
                        raise RuntimeError("SPIDER_RESTORE_SESSION_UNAVAILABLE")
                    operation_id = session.execute(
                        select(SpiderRuntimeScope.current_operation_id).where(
                            SpiderRuntimeScope.environment == scope.environment,
                            SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeScope.strategy_id == scope.strategy_id,
                        )
                    ).scalar_one_or_none()
                if operation_id is not None and self.readback(scope, operation_id) is None:
                    logging.getLogger(__name__).warning(
                        "Spider owned runtime readback unavailable for %s",
                        scope.strategy_id,
                    )
            except Exception:
                logging.getLogger(__name__).warning(
                    "Spider owned runtime restore unavailable for %s",
                    scope.strategy_id,
                )
        return owned

    def handle_owned_command(self, message: dict) -> CommandResult | None:
        if type(message) is not dict:
            return None
        command = str(message.get("command") or message.get("cmd") or "").upper()
        if command == "STOP":
            params = message.get("params")
            candidates: list[object] = [message.get("id"), message.get("strategy_id")]
            if type(params) is dict:
                candidates.extend((params.get("id"), params.get("strategy_id")))
            owned_candidates = [
                value for value in candidates
                if type(value) is str and value in self._owned_scopes
            ]
            if not owned_candidates:
                return None
            strategy_id = owned_candidates[0]
            if any(
                value is not None
                and (type(value) is not str or value != strategy_id)
                for value in candidates
            ):
                scope = self._owned_scopes[strategy_id]
                operation_id = self._current_operation_id(scope)
                return self._control_result(
                    scope,
                    "REJECTED",
                    operation_id,
                    "SPIDER_CONTROL_SCOPE_MISMATCH",
                    None if operation_id is None else self.readback(scope, operation_id),
                )
            return self.stop(self._owned_scopes[strategy_id], message)

        params = message.get("params")
        if type(params) is not dict:
            return None
        strategy_id = params.get("id") or params.get("strategy_id") or message.get("strategy_id")
        if type(strategy_id) is not str or strategy_id not in self._owned_scopes:
            return None
        scope = self._owned_scopes[strategy_id]
        if command != "START":
            return CommandResult(False, "Spider operation unavailable", {
                "reason_code": "SPIDER_OPERATION_UNAVAILABLE"
            })
        if (
            params.get("environment") != scope.environment
            or params.get("execution_scope_id") != scope.execution_scope_id
            or type(params.get("expected_version")) is not int
            or type(params.get("actor")) is not str
            or type(params.get("idempotency_key")) is not str
        ):
            return CommandResult(False, "Spider operation rejected", {
                "reason_code": "SPIDER_OPERATION_REQUEST_INVALID"
            })
        return self.install(
            scope,
            cast(str, params["actor"]),
            cast(str, params["idempotency_key"]),
            cast(int, params["expected_version"]),
        )

    def stop(self, scope: RuntimeScopeKey, message: object) -> CommandResult:
        """Commit one scoped STOP receipt and projection in the caller-owned PG TX."""
        if (
            self._strategy_state_manager is None
            or self._lifecycle_lock is None
            or self._order_event_lock is None
        ):
            return self._control_result(
                scope, "REJECTED", None, "SPIDER_CONTROL_UNAVAILABLE"
            )
        command = str(
            cast(dict[str, object], message).get("command")
            or cast(dict[str, object], message).get("cmd")
            or ""
        ).upper() if type(message) is dict else ""
        try:
            request = SpiderManagementCommand.from_message(message, command=command)
        except SpiderControlCommandError as error:
            return self._control_result(scope, "REJECTED", None, error.reason_code)
        if (request.environment, request.execution_scope_id, request.strategy_id) != (
            scope.environment, scope.execution_scope_id, scope.strategy_id
        ):
            return self._control_result(
                scope, "REJECTED", None, "SPIDER_CONTROL_SCOPE_MISMATCH"
            )

        write_result: SpiderControlWriteResult | None = None
        operation_id: str | None = None
        reason: str | None = None
        disposition: str | None = None
        try:
            with self._lifecycle_lock(scope.strategy_id):
                with cast(ContextManager[object], self._order_event_lock):
                    with self._sessions() as session:
                        if (
                            not isinstance(session, Session)
                            or session.in_transaction()
                            or not session.is_active
                        ):
                            disposition, reason = "REJECTED", "SPIDER_SESSION_INVALID"
                        elif session.get_bind().dialect.name != "postgresql":
                            disposition, reason = "REJECTED", "SPIDER_POSTGRES_REQUIRED"
                        else:
                            with session.begin():
                                self._configure_writer(session)
                                write_result = write_stop_in_transaction(
                                    session,
                                    request,
                                    cast(StrategyStateManager, self._strategy_state_manager),
                                )
                                disposition = write_result.disposition
                                reason = write_result.reason_code
                                operation_id = write_result.operation_id
        except Exception:
            pass

        readback = self._control_readback(request)
        if readback is None:
            current = self._current_operation_id(scope)
            if disposition == "REJECTED":
                current_id = operation_id or current
                current_readback = (
                    None if current_id is None else self.readback(scope, current_id)
                )
                return self._control_result(
                    scope,
                    "REJECTED",
                    current_id,
                    cast(str, reason),
                    current_readback,
                )
            return self._control_result(
                scope,
                "UNKNOWN",
                operation_id or current,
                "SPIDER_CONTROL_READBACK_UNKNOWN",
                None,
            )
        saved_operation_id = cast(
            str | None,
            cast(dict[str, object], readback["receipt"])["operation_id"],
        )
        if (
            disposition == "COMPLETED"
            and write_result is not None
            and write_result.transition is not None
        ):
            try:
                cast(StrategyStateManager, self._strategy_state_manager).after_committed_transition(
                    write_result.transition
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Committed Spider STOP projection could not be published"
                )
        return self._control_result(
            scope,
            "COMPLETED",
            saved_operation_id,
            "SPIDER_STOPPED",
            cast(dict[str, object] | None, readback["runtime"]),
        )

    def _control_readback(
        self, request: SpiderManagementCommand
    ) -> dict[str, object] | None:
        scope = RuntimeScopeKey(
            request.environment, request.execution_scope_id, request.strategy_id
        )
        try:
            with self._sessions() as session:
                if (
                    not isinstance(session, Session)
                    or session.in_transaction()
                    or not session.is_active
                    or session.get_bind().dialect.name != "postgresql"
                ):
                    return None
                with session.begin():
                    session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"))
                    self._configure_writer(session)
                    proof = verify_stop_in_transaction(session, request)
                    if proof is None:
                        return None
                    operation_id = cast(str | None, proof["operation_id"])
                runtime_readback = (
                    None if operation_id is None else self.readback(scope, cast(str, operation_id))
                )
                if operation_id is not None and (
                    runtime_readback is None or runtime_readback["runtime_control"] != "STOP"
                ):
                    return None
                return {"receipt": proof, "runtime": runtime_readback}
        except Exception:
            return None

    def _current_operation_id(self, scope: RuntimeScopeKey) -> str | None:
        try:
            with self._sessions() as session:
                if not isinstance(session, Session) or session.in_transaction() or not session.is_active:
                    return None
                return session.execute(
                    select(SpiderRuntimeScope.current_operation_id).where(
                        SpiderRuntimeScope.environment == scope.environment,
                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                    )
                ).scalar_one_or_none()
        except Exception:
            return None

    @staticmethod
    def _control_result(
        scope: RuntimeScopeKey,
        disposition: str,
        operation_id: str | None,
        reason: str,
        readback: dict[str, object] | None = None,
    ) -> CommandResult:
        result = {
            "schema_version": "spider_management_command_result_v1",
            "scope": _scope(scope),
            "command": "STOP",
            "disposition": disposition,
            "operation_id": operation_id,
            "phase": None if readback is None else readback["phase"],
            "reason_code": reason,
            "readback": readback,
        }
        return CommandResult(
            success=disposition == "COMPLETED",
            message=reason,
            data=result,
            completed=disposition in {"COMPLETED", "REJECTED"},
        )

    def install(
        self,
        scope: RuntimeScopeKey,
        actor: str,
        idempotency_key: str,
        expected_version: int,
    ) -> CommandResult:
        """Select the exact persisted confirmation and atomically install its held plan."""
        prepared: PreparedModelBootstrap | None = None
        operation_id: str | None = None
        unknown_commit = False
        try:
            with self._sessions() as session:
                if not isinstance(session, Session) or session.in_transaction() or not session.is_active:
                    return self._result(scope, "REJECTED", None, "SPIDER_SESSION_INVALID")
                if session.get_bind().dialect.name != "postgresql":
                    return self._result(scope, "REJECTED", None, "SPIDER_POSTGRES_REQUIRED")
                with session.begin():
                    self._configure_writer(session)
                    command_evidence = session.execute(
                        select(SpiderOperationRequestRow.request_id,
                               SpiderOperationRequestRow.operation_id)
                        .where(
                            SpiderOperationRequestRow.environment == scope.environment,
                            SpiderOperationRequestRow.execution_scope_id == scope.execution_scope_id,
                            SpiderOperationRequestRow.strategy_id == scope.strategy_id,
                            SpiderOperationRequestRow.request_kind == "CONFIRMATION",
                            SpiderOperationRequestRow.actor == actor,
                            SpiderOperationRequestRow.idempotency_key == idempotency_key,
                            SpiderOperationRequestRow.authorization_confirmed.is_(True),
                        )
                        .with_for_update()
                        .limit(2)
                    ).all()
                    if len(command_evidence) != 1:
                        return self._result(scope, "REJECTED", None, "SPIDER_OPERATION_NOT_FOUND")
                    operation_id = cast(str, command_evidence[0].operation_id)
                    head = session.execute(
                        select(SpiderRuntimeOperation)
                        .where(SpiderRuntimeOperation.operation_id == operation_id)
                        .with_for_update()
                    ).scalar_one_or_none()
                    if head is None or (head.environment, head.execution_scope_id, head.strategy_id) != (
                        scope.environment, scope.execution_scope_id, scope.strategy_id
                    ):
                        return self._result(scope, "REJECTED", operation_id, "SPIDER_OPERATION_NOT_FOUND")
                    existing = session.execute(
                        select(SpiderRuntimeScope).where(
                            SpiderRuntimeScope.environment == scope.environment,
                            SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeScope.strategy_id == scope.strategy_id,
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        if existing.current_operation_id != operation_id:
                            return self._result(scope, "REJECTED", operation_id, "SPIDER_OPERATION_STALE")
                        raise _ExistingInstallReadback()
                    try:
                        confirmation = self._confirmed_operation_request(
                            session, head, actor, idempotency_key
                        )
                        request = confirmation
                        core = request.operation_core
                        if core.get("expected_state_version") != expected_version:
                            return self._result(scope, "REJECTED", operation_id, "SPIDER_OPERATION_STALE")
                        state_version = session.execute(
                            select(StrategyState.version)
                            .where(StrategyState.strategy_id == scope.strategy_id)
                            .with_for_update()
                        ).scalar_one_or_none()
                        if type(state_version) is not int or state_version != expected_version:
                            return self._result(scope, "REJECTED", operation_id, "SPIDER_OPERATION_STALE")
                        selection = cast(dict[str, object], core["runtime_selection"])
                        source = self._model_sources.get(cast(str, selection["confirmation_request_id"]))
                        if source is None:
                            return self._result(scope, "REJECTED", operation_id, "MODEL_SOURCE_UNAVAILABLE")
                        configuration_row = session.execute(
                            select(SpiderConfigurationRequest.__table__).where(
                                SpiderConfigurationRequest.request_id == selection["confirmation_request_id"]
                            )
                        ).mappings().one_or_none()
                        if configuration_row is None:
                            return self._result(scope, "REJECTED", operation_id, "SPIDER_CONFIGURATION_UNAVAILABLE")
                        configuration = _hydrate(configuration_row)
                        if (
                            configuration.request.request_kind is not SpiderConfigurationRequestKind.CONFIRMATION
                            or configuration.request.request_id != selection["confirmation_request_id"]
                            or configuration.request.configuration.sha256 != selection["configuration_sha256"]
                            or (configuration.request.environment, configuration.request.execution_scope_id,
                                configuration.request.strategy_id)
                            != (scope.environment, scope.execution_scope_id, scope.strategy_id)
                        ):
                            return self._result(scope, "REJECTED", operation_id, "SPIDER_CONFIGURATION_CONFLICT")
                        prepared = prepare_model_bootstrap(
                            source.run,
                            selection,
                            configuration,
                            product_specifications=source.product_specifications,
                            selected_venue=source.selected_venue,
                        )
                        self._validate_confirmed_basis(request, prepared)
                    except ModelBootstrapUnavailable:
                        return self._result(scope, "REJECTED", operation_id, "MODEL_SOURCE_UNAVAILABLE")
                    except ValueError:
                        return self._result(scope, "REJECTED", operation_id, "SPIDER_INSTALL_UNAVAILABLE")
                    self._write_install(session, head, request, prepared, expected_version)
                    session.flush()
                # `session.begin()` has committed. Return a separate fresh read.
        except _ExistingInstallReadback:
            pass
        except Exception:
            # A transaction/commit exception is not evidence that the write failed.
            unknown_commit = True
        if operation_id is None:
            return self._result(scope, "UNKNOWN", None, "SPIDER_OPERATION_SELECTION_UNKNOWN")
        readback = self.readback(scope, operation_id)
        if readback is None:
            return self._result(
                scope, "UNKNOWN", operation_id,
                "SPIDER_INSTALL_READBACK_UNKNOWN" if unknown_commit else "SPIDER_INSTALL_NOT_PROVED",
            )
        return self._result(scope, "ACCEPTED", operation_id, "PROVIDER_PROFILE_UNAVAILABLE", readback)

    def _configure_writer(self, session: Session) -> None:
        session.execute(text(
            "SELECT set_config('TimeZone', 'UTC', true), "
            "set_config('lock_timeout', :lock_timeout, true), "
            "set_config('statement_timeout', :statement_timeout, true)"
        ), {
            "lock_timeout": f"{self._lock_timeout_ms}ms",
            "statement_timeout": f"{self._statement_timeout_ms}ms",
        })

    def _confirmed_operation_request(
        self,
        session: Session,
        head: SpiderRuntimeOperation,
        actor: str,
        idempotency_key: str,
    ) -> SpiderOperationRequest:
        if (
            head.phase != "PREVIEW"
            or head.confirmation_request_id is None
            or head.authorization_revision != 1
            or head.actor != actor
        ):
            raise ValueError("Spider INSTALL is not confirmed")
        row = session.execute(
            select(SpiderOperationRequestRow.__table__).where(
                SpiderOperationRequestRow.request_id == head.confirmation_request_id
            )
        ).mappings().one_or_none()
        if row is None:
            raise ValueError("Spider confirmation unavailable")
        record = _mapping(row)
        request = record.request
        if (
            not record.authorization_confirmed
            or request.request_kind != "CONFIRMATION"
            or request.operation_id != head.operation_id
            or request.actor != actor
            or request.idempotency_key != idempotency_key
            or request.operation_core_sha256 != head.operation_core_sha256
        ):
            raise ValueError("Spider confirmation mismatch")
        core = request.operation_core
        selection = cast(dict[str, object], core["runtime_selection"])
        if (
            selection["environment"] != head.environment
            or selection["execution_scope_id"] != head.execution_scope_id
            or selection["strategy_id"] != head.strategy_id
            or selection["confirmation_request_id"] != head.configuration_request_id
            or core["confirmation_request_id"] != head.configuration_request_id
        ):
            raise ValueError("Spider confirmation scope mismatch")
        return request

    @staticmethod
    def _validate_confirmed_basis(
        request: SpiderOperationRequest, prepared: PreparedModelBootstrap
    ) -> None:
        basis = cast(dict[str, object], request.authorization["basis"])
        if (
            basis["financial_cursor"] != prepared.financial_cursor
            or basis["tape_cursor"] != prepared.tape_cursor
            or basis["specification_sha256"] != prepared.specification_sha256
            or basis["valuation_sha256"] != prepared.valuation_sha256
        ):
            raise ValueError("Spider confirmed basis mismatch")

    def _write_install(
        self,
        session: Session,
        head: SpiderRuntimeOperation,
        request: SpiderOperationRequest,
        prepared: PreparedModelBootstrap,
        state_version: int,
    ) -> None:
        scope = RuntimeScopeKey(head.environment, head.execution_scope_id, head.strategy_id)
        operation_id = head.operation_id
        generation = cast(int, request.operation_core["source_generation"]) + 1
        selection = prepared.runtime_selection
        confirmation_id = cast(str, selection["confirmation_request_id"])
        configuration_sha256 = cast(str, selection["configuration_sha256"])
        plan_id = _digest({"domain": "SPIDER_INITIAL_PLAN_V1", "scope": _scope(scope),
                           "operation_id": operation_id, "generation": generation})
        manifest = {
            "domain": "SPIDER_INITIAL_MANIFEST_V1",
            "operation_id": operation_id,
            "operation_core_sha256": request.operation_core_sha256,
            "confirmation_request_id": confirmation_id,
            "configuration_sha256": configuration_sha256,
            "generation": generation,
            "bootstrap_basis": prepared.bootstrap_basis,
            "bootstrap_digest": prepared.bootstrap_digest,
            "initial_manifest": prepared.initial_manifest,
            "initial_source_events": decode_canonical(prepared.initial_source_events_bytes),
            "build_checkpoint_sha256": prepared.build_checkpoint_sha256,
            "source_product_map": prepared.product_map,
            "hold_reasons": ["PROVIDER_PROFILE_UNAVAILABLE"],
        }
        manifest_payload = canonical_bytes(manifest)
        manifest_sha256 = _digest_bytes(manifest_payload)
        events = cast(dict[str, object], decode_canonical(prepared.initial_source_events_bytes))
        entries = cast(list[dict[str, object]], events["entries"])
        product_targets = {
            cast(str, row["source_product_id"]): cast(
                str, cast(dict[str, object], row["instrument_spec"])["product_id"]
            )
            for row in cast(list[dict[str, object]], prepared.product_map["entries"])
        }
        action_rows: list[SpiderRuntimeAction] = []
        binding_rows: list[SpiderRuntimeBinding] = []
        missing_rows: list[dict[str, object]] = []
        candidate_ordinal = 0
        transmission_group = 0
        effects = cast(list[dict[str, object]], request.authorization["effects"])
        effect_index = {
            (effect["product_id"], effect["side"], effect["role"], effect["quantity_unit"]): effect
            for effect in effects
        }
        for entry in entries:
            source_product = cast(str, entry["source_product_id"])
            for event in cast(list[dict[str, object]], entry["events"]):
                if event.get("kind") != "send":
                    continue
                transmission_group += 1
                orders = cast(list[dict[str, object]], event["orders"])
                for transmission_ordinal, source_order in enumerate(orders):
                    candidate_ordinal += 1
                    action_id = _digest({
                        "domain": "SPIDER_INITIAL_ACTION_V1", "scope": _scope(scope),
                        "operation_id": operation_id, "plan_id": plan_id,
                        "candidate_ordinal": candidate_ordinal,
                    })
                    original_payload = canonical_bytes(source_order)
                    source_side = cast(str, source_order["side"])
                    side = OrderSide.BUY if source_side == "buy" else OrderSide.SELL
                    position_side = SignalType.LONG if source_side == "buy" else SignalType.SHORT
                    price, quantity = _positive_decimal(source_order["px"]), _positive_decimal(source_order["sz"])
                    effect_key = (source_product, position_side.value, "GRID", "CONTRACTS")
                    effect = effect_index.get(effect_key)
                    if effect is None or quantity > Decimal(cast(str, effect["quantity_cap"])):
                        raise ValueError("initial source candidate exceeds confirmed effects")
                    lower = effect["limit_price_min"]
                    upper = effect["limit_price_max"]
                    if (lower is not None and price < Decimal(cast(str, lower))) or (
                        upper is not None and price > Decimal(cast(str, upper))
                    ):
                        raise ValueError("initial source candidate exceeds confirmed price bounds")
                    local_product = product_targets.get(source_product)
                    if local_product is None:
                        raise ValueError("initial source product map incomplete")
                    signal = Signal(
                        strategy_id=scope.strategy_id,
                        product_id=local_product,
                        timeframe="SPIDER_INITIAL_INSTALL",
                        timestamp=cast(int, event["at_ms"]),
                        type=position_side,
                        quantity=quantity,
                        price=price,
                    )
                    source_client_order_id = cast(str, source_order["clOrdId"])
                    if len(source_client_order_id.encode("utf-8")) > 64:
                        raise ValueError("source client order identity exceeds binding contract")
                    local_client_order_id = market_signal_client_order_id(
                        scope.strategy_id,
                        local_product,
                        operation_id,
                        cast(int, event["at_ms"]),
                        "spiderinstall",
                        candidate_ordinal,
                    )
                    order = self._order_manager.prepare_order(
                        signal,
                        side,
                        "limit",
                        quantity,
                        price=price,
                        client_order_id=local_client_order_id,
                    )
                    binding_value = {
                        "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
                        "environment": scope.environment,
                        "execution_scope_id": scope.execution_scope_id,
                        "strategy_id": scope.strategy_id,
                        "action_id": action_id,
                        "product_id": local_product,
                    }
                    binding_bytes = canonical_bytes(binding_value)
                    binding_sha256 = _digest_bytes(binding_bytes)
                    wire_client_order_id = "sp" + sha256(
                        b"SPIDER_OKX_CLIENT_BINDING_V1\0" + binding_bytes
                    ).hexdigest()[:30]
                    order.intent_payload = {
                        "action_id": action_id,
                        "source_order": source_order,
                        "source_client_order_id": source_client_order_id,
                        "local_client_order_id": local_client_order_id,
                        "provider_wire_client_order_id": wire_client_order_id,
                        "provider_binding_bytes_hex": binding_bytes.hex(),
                        "provider_binding_sha256": binding_sha256,
                    }
                    repository = self._order_manager.repo
                    if not isinstance(repository, LiveOrderRepository):
                        raise ValueError("Spider INSTALL requires the actual live order repository")
                    repository.stage_order_in_transaction(session, order)
                    if (order.account_profile, order.account_id) != (
                        selection["account_profile"], selection["account_id"]
                    ):
                        raise ValueError("prepared Order account differs from confirmed selection")
                    action_rows.append(SpiderRuntimeAction(
                        action_id=action_id,
                        plan_id=plan_id,
                        environment=scope.environment,
                        execution_scope_id=scope.execution_scope_id,
                        strategy_id=scope.strategy_id,
                        trigger_delivery_id=None,
                        parent_action_id=None,
                        generation=generation,
                        configuration_sha256=configuration_sha256,
                        role="GRID",
                        product_id=source_product,
                        local_order_id=order.id,
                        original_payload=original_payload,
                        payload_sha256=_digest_bytes(original_payload),
                        candidate_ordinal=candidate_ordinal,
                        transmission_group=transmission_group,
                        transmission_ordinal=transmission_ordinal,
                        outcome_state="UNSENT",
                        outcome_reference=None,
                    ))
                    binding_rows.append(SpiderRuntimeBinding(
                        action_id=action_id,
                        environment=scope.environment,
                        execution_scope_id=scope.execution_scope_id,
                        strategy_id=scope.strategy_id,
                        local_order_id=order.id,
                        source_product_id=source_product,
                        canonical_product_id=local_product,
                        source_client_order_id=source_client_order_id,
                        product_map_sha256=prepared.product_map_sha256,
                    ))
                    missing_rows.append({
                        "initial_member_id": action_id,
                        "current_frontier_action_id": action_id,
                        "original_payload_sha256": _digest_bytes(original_payload),
                        "original_terms": {
                            "product_id": source_product,
                            "side": position_side.value,
                            "order_type": "limit",
                            "price": cast(str, source_order["px"]),
                            "quantity": cast(str, source_order["sz"]),
                            "quantity_unit": "CONTRACTS",
                        },
                        "last_outcome": "UNSENT",
                        "rejection_reason": None,
                        "resubmit_eligible": False,
                        "ineligible_reason": "PROVIDER_PROFILE_UNAVAILABLE",
                        "latest_resubmission_request_id": None,
                    })
        if not action_rows or len(action_rows) != len(binding_rows):
            raise ValueError("initial source emitted no persistable actions")
        missing_payload = canonical_bytes(missing_rows)
        session.add(SpiderRuntimePlan(
            plan_id=plan_id,
            environment=scope.environment,
            execution_scope_id=scope.execution_scope_id,
            strategy_id=scope.strategy_id,
            operation_id=operation_id,
            generation=generation,
            configuration_sha256=configuration_sha256,
            source_checkpoint_sha256=prepared.source_checkpoint_sha256,
            specification_sha256=prepared.specification_sha256,
            observed_input_sha256=prepared.historical_input_sha256,
            tape_cursor=prepared.tape_cursor,
            manifest_payload=manifest_payload,
            manifest_sha256=manifest_sha256,
            plan_status="INCOMPLETE",
            missing_members=missing_payload,
        ))
        # These records share one transaction, but the explicit flush order also
        # respects the immediate composite foreign keys on each child row.
        session.flush()
        session.add_all(action_rows)
        session.flush()
        session.add_all(binding_rows)
        session.add(SpiderRuntimeScope(
            environment=scope.environment,
            execution_scope_id=scope.execution_scope_id,
            strategy_id=scope.strategy_id,
            host_contract_id=cast(str, selection["host_contract_id"]),
            host_contract_sha256=cast(str, selection["host_contract_sha256"]),
            confirmation_request_id=confirmation_id,
            configuration_sha256=configuration_sha256,
            generation=generation,
            state_version=state_version,
            control_mode="RUN",
            current_operation_id=operation_id,
            last_received_sequence=0,
            last_consumed_sequence=0,
            source_checkpoint=prepared.build_checkpoint_bytes,
            source_checkpoint_sha256=prepared.build_checkpoint_sha256,
            control_barrier_sequence=0,
            initial_plan_id=plan_id,
        ))
        head.phase = "INCOMPLETE"

    def readback(self, scope: RuntimeScopeKey, operation_id: str) -> dict[str, object] | None:
        """Read the committed INSTALL with a fresh read-only PostgreSQL transaction."""
        try:
            with self._sessions() as session:
                if not isinstance(session, Session) or session.in_transaction() or not session.is_active:
                    return None
                bind = session.get_bind()
                if bind.dialect.name != "postgresql":
                    return None
                with session.begin():
                    session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"))
                    self._configure_writer(session)
                    runtime = session.execute(select(SpiderRuntimeScope).where(
                        SpiderRuntimeScope.environment == scope.environment,
                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                    )).scalar_one_or_none()
                    if runtime is None or runtime.current_operation_id != operation_id:
                        return None
                    head = session.execute(select(SpiderRuntimeOperation).where(
                        SpiderRuntimeOperation.operation_id == operation_id,
                        SpiderRuntimeOperation.environment == scope.environment,
                        SpiderRuntimeOperation.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeOperation.strategy_id == scope.strategy_id,
                    )).scalar_one_or_none()
                    plan = session.execute(select(SpiderRuntimePlan).where(
                        SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                        SpiderRuntimePlan.operation_id == operation_id,
                    )).scalar_one_or_none()
                    if (
                        head is None
                        or plan is None
                        or head.phase != "INCOMPLETE"
                        or plan.plan_status != "INCOMPLETE"
                    ):
                        return None
                    plan_order_ids = set(session.execute(
                        select(SpiderRuntimeAction.local_order_id)
                        .where(SpiderRuntimeAction.plan_id == plan.plan_id)
                    ).scalars().all())
                    scoped_receipts = session.execute(
                        select(
                            SpiderRuntimeDelivery.local_order_id,
                            SpiderRuntimeDelivery.sequence,
                        ).where(
                            SpiderRuntimeDelivery.environment == scope.environment,
                            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                        ).order_by(SpiderRuntimeDelivery.sequence)
                    ).mappings().all()
                    if (
                        len(scoped_receipts) != runtime.last_received_sequence
                        or any(
                            row["sequence"] != index
                            or row["local_order_id"] not in plan_order_ids
                            for index, row in enumerate(scoped_receipts, 1)
                        )
                    ):
                        return None
                    if _digest_bytes(runtime.source_checkpoint) != runtime.source_checkpoint_sha256:
                        return None
                    if _digest_bytes(plan.manifest_payload) != plan.manifest_sha256:
                        return None
                    manifest = cast(dict[str, object], decode_canonical(plan.manifest_payload))
                    product_map = cast(dict[str, object], manifest["source_product_map"])
                    initial_manifest = cast(dict[str, object], manifest["initial_manifest"])
                    bootstrap_basis = cast(dict[str, object], manifest["bootstrap_basis"])
                    runtime_selection = cast(
                        dict[str, object], bootstrap_basis["runtime_selection"]
                    )
                    if (
                        manifest.get("build_checkpoint_sha256") != runtime.source_checkpoint_sha256
                        or initial_manifest.get("build_checkpoint_sha256")
                        != runtime.source_checkpoint_sha256
                    ):
                        return None
                    product_map_sha256 = _digest(product_map)
                    bootstrap_digest = cast(str, manifest["bootstrap_digest"])
                    if (
                        initial_manifest["product_map"] != product_map
                        or initial_manifest["product_map_sha256"] != product_map_sha256
                        or bootstrap_basis["product_map_sha256"] != product_map_sha256
                        or initial_manifest["bootstrap_basis_sha256"] != bootstrap_digest
                        or bootstrap_digest != _digest(bootstrap_basis)
                        or initial_manifest["specification_sha256"] != plan.specification_sha256
                        or bootstrap_basis["specification_sha256"] != plan.specification_sha256
                    ):
                        return None
                    identity_rows = session.execute(
                        select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
                        .join(
                            SpiderRuntimeBinding,
                            SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id,
                        )
                        .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
                        .where(SpiderRuntimeAction.plan_id == plan.plan_id)
                        .order_by(SpiderRuntimeAction.candidate_ordinal)
                    ).all()
                    if {order.id for _action, _binding, order in identity_rows} != plan_order_ids:
                        return None
                    missing = decode_canonical(plan.missing_members)
                    if type(missing) is not list or len(identity_rows) != len(missing) or not missing:
                        return None
                    map_entries = cast(list[dict[str, object]], product_map["entries"])
                    targets = {
                        cast(str, row["source_product_id"]): cast(
                            str, cast(dict[str, object], row["instrument_spec"])["product_id"]
                        )
                        for row in map_entries
                    }
                    member_keys = {
                        "initial_member_id",
                        "current_frontier_action_id",
                        "original_payload_sha256",
                        "original_terms",
                        "last_outcome",
                        "rejection_reason",
                        "resubmit_eligible",
                        "ineligible_reason",
                        "latest_resubmission_request_id",
                    }
                    term_keys = {
                        "product_id", "side", "order_type", "price", "quantity", "quantity_unit"
                    }
                    previous_ordinal = 0
                    for (action, binding, order), member in zip(identity_rows, cast(list[object], missing), strict=True):
                        if type(member) is not dict or set(member) != member_keys:
                            return None
                        held_member = cast(dict[str, object], member)
                        terms = held_member["original_terms"]
                        if type(terms) is not dict or set(terms) != term_keys:
                            return None
                        original_terms = cast(dict[str, object], terms)
                        source_payload_value = decode_canonical(action.original_payload)
                        if type(source_payload_value) is not dict:
                            return None
                        source_payload = cast(dict[str, object], source_payload_value)
                        source_side = source_payload.get("side")
                        if source_side not in {"buy", "sell"}:
                            return None
                        position_side = "LONG" if source_side == "buy" else "SHORT"
                        source_price = source_payload.get("px")
                        source_quantity = source_payload.get("sz")
                        if (
                            type(source_price) is not str
                            or type(source_quantity) is not str
                            or source_payload.get("ordType") != "limit"
                            or type(action.candidate_ordinal) is not int
                            or action.candidate_ordinal <= previous_ordinal
                        ):
                            return None
                        previous_ordinal = action.candidate_ordinal
                        intent = order.intent_payload
                        if type(intent) is not dict:
                            return None
                        wire_bytes_hex = intent.get("provider_binding_bytes_hex")
                        if type(wire_bytes_hex) is not str:
                            return None
                        wire_bytes = bytes.fromhex(wire_bytes_hex)
                        wire_binding = cast(dict[str, object], decode_canonical(wire_bytes))
                        expected_binding = {
                            "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
                            "environment": scope.environment,
                            "execution_scope_id": scope.execution_scope_id,
                            "strategy_id": scope.strategy_id,
                            "action_id": action.action_id,
                            "product_id": binding.canonical_product_id,
                        }
                        expected_wire_id = "sp" + sha256(
                            b"SPIDER_OKX_CLIENT_BINDING_V1\0" + wire_bytes
                        ).hexdigest()[:30]
                        if (
                            held_member["initial_member_id"] != action.action_id
                            or held_member["current_frontier_action_id"] != action.action_id
                            or held_member["original_payload_sha256"] != action.payload_sha256
                            or original_terms != {
                                "product_id": binding.source_product_id,
                                "side": position_side,
                                "order_type": "limit",
                                "price": source_price,
                                "quantity": source_quantity,
                                "quantity_unit": "CONTRACTS",
                            }
                            or held_member["last_outcome"] != "UNSENT"
                            or held_member["rejection_reason"] is not None
                            or held_member["resubmit_eligible"] is not False
                            or held_member["ineligible_reason"] != "PROVIDER_PROFILE_UNAVAILABLE"
                            or held_member["latest_resubmission_request_id"] is not None
                            or action.outcome_state != "UNSENT"
                            or action.trigger_delivery_id is not None
                            or action.product_id != binding.source_product_id
                            or binding.canonical_product_id != targets.get(binding.source_product_id)
                            or binding.product_map_sha256 != product_map_sha256
                            or binding.local_order_id != order.id
                            or binding.source_client_order_id != source_payload.get("clOrdId")
                            or order.product_id != binding.canonical_product_id
                            or order.type != "limit"
                            or order.side != source_side
                            or order.price != _positive_decimal(source_price)
                            or order.quantity != _positive_decimal(source_quantity)
                            or order.account_profile != runtime_selection.get("account_profile")
                            or order.account_id != runtime_selection.get("account_id")
                            or order.client_order_id != intent.get("local_client_order_id")
                            or order.client_order_id == binding.source_client_order_id
                            or intent.get("source_client_order_id") != binding.source_client_order_id
                            or intent.get("provider_wire_client_order_id") != expected_wire_id
                            or intent.get("provider_binding_sha256") != _digest_bytes(wire_bytes)
                            or wire_binding != expected_binding
                            or _digest_bytes(action.original_payload) != action.payload_sha256
                        ):
                            return None
                        current_projection_row = session.execute(
                            select(
                                Order.status,
                                Order.filled_quantity,
                                Order.filled_price,
                                Order.exchange_order_id,
                            ).where(Order.id == order.id)
                        ).mappings().one_or_none()
                        if current_projection_row is None or not validate_order_receipt_chain(
                            session,
                            scope=_scope(scope),
                            action=action,
                            binding=binding,
                            order_id=order.id,
                            current_projection={
                                "status": current_projection_row["status"],
                                "filled_quantity": (
                                    None
                                    if current_projection_row["filled_quantity"] is None
                                    else canonical_decimal_text(current_projection_row["filled_quantity"])
                                ),
                                "filled_price": (
                                    None
                                    if current_projection_row["filled_price"] is None
                                    else canonical_decimal_text(current_projection_row["filled_price"])
                                ),
                                "exchange_order_id": current_projection_row["exchange_order_id"],
                            },
                        ):
                            return None
                    return {
                        "schema_version": "spider_runtime_readback_v1",
                        "scope": _scope(scope),
                        "host_contract_id": runtime.host_contract_id,
                        "host_contract_sha256": runtime.host_contract_sha256,
                        "effective": {
                            "confirmation_request_id": runtime.confirmation_request_id,
                            "configuration_sha256": runtime.configuration_sha256,
                            "generation": runtime.generation,
                        },
                        "pending": None,
                        "runtime_control": runtime.control_mode,
                        "phase": head.phase,
                        "state_version": runtime.state_version,
                        "operation_id": operation_id,
                        "operation_core_sha256": head.operation_core_sha256,
                        "authorization_revision": head.authorization_revision,
                        "barrier_sequence": runtime.control_barrier_sequence,
                        "received_sequence": runtime.last_received_sequence,
                        "consumed_sequence": runtime.last_consumed_sequence,
                        "checkpoint_sha256": runtime.source_checkpoint_sha256,
                        "initial_plan_id": runtime.initial_plan_id,
                        "initial_plan_status": plan.plan_status,
                        "initial_plan_missing_members": missing,
                        "scoped_hold_reasons": ["PROVIDER_PROFILE_UNAVAILABLE"],
                        "unresolved_action_ids": [row["current_frontier_action_id"] for row in missing],
                        "trading_ready": False,
                    }
        except Exception:
            return None

    def _independent_result(
        self, scope: RuntimeScopeKey, operation_id: str
    ) -> CommandResult:
        readback = self.readback(scope, operation_id)
        if readback is None:
            return self._result(scope, "UNKNOWN", operation_id, "SPIDER_INSTALL_READBACK_UNKNOWN")
        return self._result(scope, "ACCEPTED", operation_id, "PROVIDER_PROFILE_UNAVAILABLE", readback)

    @staticmethod
    def _result(
        scope: RuntimeScopeKey,
        disposition: str,
        operation_id: str | None,
        reason: str,
        readback: dict[str, object] | None = None,
    ) -> CommandResult:
        result = {
            "schema_version": "spider_management_command_result_v1",
            "scope": _scope(scope),
            "command": "START",
            "disposition": disposition,
            "operation_id": operation_id,
            "phase": None if readback is None else readback["phase"],
            "reason_code": reason,
            "readback": readback,
        }
        return CommandResult(
            success=disposition in {"ACCEPTED", "COMPLETED"},
            message=reason,
            data=result,
            completed=disposition in {"COMPLETED", "REJECTED"},
        )


def _scope(scope: RuntimeScopeKey) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "execution_scope_id": scope.execution_scope_id,
        "strategy_id": scope.strategy_id,
    }


def _digest(value: object) -> str:
    return _digest_bytes(canonical_bytes(value))


def _digest_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _positive_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError("initial action quantity/price is not canonical text")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise ValueError("initial action quantity/price is invalid") from None
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError("initial action quantity/price is invalid")
    return parsed
