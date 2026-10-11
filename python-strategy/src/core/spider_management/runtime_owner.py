"""Private transactional owner for an explicitly selected Spider INSTALL."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from types import MappingProxyType
from typing import Any, ContextManager, Protocol, cast

from sqlalchemy import func, select, text
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
    ORIGINAL_MODEL_POLICY_ID,
    ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
    ORIGINAL_MODEL_POLICY_VERSION,
    PreparedModelBootstrap,
    prepare_model_bootstrap,
)
from src.core.spider_management.source_checkpoint_codec import restore_source_checkpoint
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.operation_request_store import _mapping
from src.core.spider_management.financial_delivery import (
    stage_financial_receipt,
    validate_order_receipt_chain,
    validate_tape_index,
)
from src.core.spider_management.initial_place_projection import (
    MemberOutcome,
    OriginalPlaceCandidate,
    build_missing_projection,
    effective_member_outcome,
    expected_plan_status,
    validate_member_outcomes,
)
from src.core.spider_management.control_command import (
    SpiderControlCommandError,
    SpiderManagementCommand,
    SpiderControlWriteResult,
    hydrate_control_receipt,
    verify_stop_in_transaction,
    write_stop_in_transaction,
)
from src.core.spider_management.orm import (
    SpiderConfigurationRequest,
    SpiderControlReceipt,
    SpiderOperationRequestRow,
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimeOperation,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
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


class _BatchTransport(Protocol):
    def submit(self, request_payload: bytes) -> object: ...


class _BatchResponseClassifier(Protocol):
    def __call__(
        self, request_payload: bytes, response_payload: bytes
    ) -> Mapping[str, MemberOutcome] | None: ...


@dataclass(frozen=True, slots=True)
class _TransmissionOutcome:
    disposition: str
    transmission_group_id: str
    action_outcomes: tuple[tuple[str, str], ...] = ()


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
        transport_response_classifier: _BatchResponseClassifier | None = None,
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
        self._transport_response_classifier = transport_response_classifier
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms

    def participate_in_order_transaction(
        self,
        session: Session,
        order: Order,
        trade: Trade | None,
    ) -> None:
        """Record a bound financial fact in the repository's caller-owned TX."""
        staged = stage_financial_receipt(
            session,
            order,
            trade,
            {
                strategy_id: (scope.environment, scope.execution_scope_id)
                for strategy_id, scope in self._owned_scopes.items()
            },
        )
        if staged:
            scope = self._owned_scopes.get(order.strategy_id)
            if scope is not None:
                runtime = session.execute(
                    select(SpiderRuntimeScope).where(
                        SpiderRuntimeScope.environment == scope.environment,
                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                    )
                ).scalar_one_or_none()
                if runtime is None:
                    raise RuntimeError("SPIDER_FINANCIAL_RUNTIME_UNAVAILABLE")
                plan = session.execute(
                    select(SpiderRuntimePlan).where(
                        SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                        SpiderRuntimePlan.operation_id == runtime.current_operation_id,
                    )
                ).scalar_one_or_none()
                if plan is None:
                    raise RuntimeError("SPIDER_FINANCIAL_PLAN_UNAVAILABLE")
                projected = self._refresh_initial_place_projection(
                    session,
                    scope,
                    runtime,
                    plan,
                    persist=True,
                    current_order=order,
                )
                if projected is None:
                    logging.getLogger(__name__).warning(
                        "Spider financial fact committed; initial plan projection unavailable for %s",
                        scope.strategy_id,
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
                self._drain_stopped_scope(scope)
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

    def observe_order_event(self, local_order_id: str) -> None:
        """Wake STOP-only receipt consumption after committed financial application."""
        if type(local_order_id) is not str or not local_order_id:
            return
        try:
            with self._sessions() as session:
                if not isinstance(session, Session) or session.in_transaction() or not session.is_active:
                    return
                row = session.execute(
                    select(
                        SpiderRuntimeBinding.environment,
                        SpiderRuntimeBinding.execution_scope_id,
                        SpiderRuntimeBinding.strategy_id,
                    ).where(SpiderRuntimeBinding.local_order_id == local_order_id)
                ).mappings().one_or_none()
            if row is None:
                return
            scope = self._owned_scopes.get(cast(str, row["strategy_id"]))
            if scope is None or (scope.environment, scope.execution_scope_id) != (
                row["environment"], row["execution_scope_id"]
            ):
                return
            self._drain_stopped_scope(scope)
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider STOP delivery observer could not drain committed facts"
            )

    def transmit_initial_place_group(
        self,
        scope: RuntimeScopeKey,
        source_group: int,
        executing_account_ref: str,
        request_payload: bytes,
        transport: _BatchTransport,
    ) -> _TransmissionOutcome:
        """Commit one exact PLACE marker, call once outside locks, save raw outcome."""
        if (
            type(source_group) is not int
            or source_group <= 0
            or type(executing_account_ref) is not str
            or not executing_account_ref
            or type(request_payload) is not bytes
            or self._lifecycle_lock is None
            or self._order_event_lock is None
        ):
            return _TransmissionOutcome("UNAVAILABLE", "")
        try:
            marker = self._stage_initial_place_marker(
                scope, source_group, executing_account_ref, request_payload
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider batch marker could not be staged"
            )
            return _TransmissionOutcome("UNKNOWN", "")
        if marker is None:
            return _TransmissionOutcome("REJECTED", "")
        transmission_group_id, should_call = marker
        if not should_call:
            proved = self._transmission_readback(
                scope, transmission_group_id, request_payload
            )
            return proved or _TransmissionOutcome("UNKNOWN", transmission_group_id)

        try:
            call_result = transport.submit(request_payload)
            response_payload = getattr(call_result, "response_payload", None)
            response_failure = getattr(call_result, "response_failure", None)
            if type(response_payload) is not bytes:
                response_payload = None
            if response_failure not in {
                None, "TRANSPORT_UNKNOWN", "RESPONSE_INVALID", "RESPONSE_TOO_LARGE"
            }:
                response_failure = "RESPONSE_INVALID"
            if response_payload is not None:
                response_failure = None
            elif response_failure is None:
                response_failure = "RESPONSE_INVALID"
        except Exception:
            # The committed marker remains the only evidence after any call error.
            response_payload = None
            response_failure = "TRANSPORT_UNKNOWN"

        try:
            self._record_transmission_result(
                scope,
                transmission_group_id,
                request_payload,
                response_payload,
                cast(str, response_failure),
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider batch result write failed; proving exact saved group"
            )
        return self._transmission_readback(
            scope, transmission_group_id, request_payload
        ) or _TransmissionOutcome("UNKNOWN", transmission_group_id)

    def _stage_initial_place_marker(
        self,
        scope: RuntimeScopeKey,
        source_group: int,
        executing_account_ref: str,
        request_payload: bytes,
    ) -> tuple[str, bool] | None:
        request = _decode_transport_request(request_payload)
        lifecycle_lock = self._lifecycle_lock
        order_event_lock = self._order_event_lock
        if request is None or request.get("operation") != "PLACE" or lifecycle_lock is None or order_event_lock is None:
            return None
        with lifecycle_lock(scope.strategy_id):
            with order_event_lock:
                with self._sessions() as session:
                    if (
                        not isinstance(session, Session)
                        or session.in_transaction()
                        or not session.is_active
                        or session.get_bind().dialect.name != "postgresql"
                    ):
                        return None
                    with session.begin():
                        self._configure_writer(session)
                        runtime = session.execute(
                            select(SpiderRuntimeScope).where(
                                SpiderRuntimeScope.environment == scope.environment,
                                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                                SpiderRuntimeScope.strategy_id == scope.strategy_id,
                            ).with_for_update()
                        ).scalar_one_or_none()
                        if (
                            runtime is None
                            or not validate_tape_index(session, runtime)
                        ):
                            return None
                        head = session.execute(
                            select(SpiderRuntimeOperation).where(
                                SpiderRuntimeOperation.operation_id == runtime.current_operation_id,
                                SpiderRuntimeOperation.environment == scope.environment,
                                SpiderRuntimeOperation.execution_scope_id == scope.execution_scope_id,
                                SpiderRuntimeOperation.strategy_id == scope.strategy_id,
                            ).with_for_update()
                        ).scalar_one_or_none()
                        plan = session.execute(
                            select(SpiderRuntimePlan).where(
                                SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                                SpiderRuntimePlan.operation_id == runtime.current_operation_id,
                            )
                        ).scalar_one_or_none()
                        if (
                            head is None
                            or plan is None
                            or head.phase != "INCOMPLETE"
                            or plan.plan_status not in {"INCOMPLETE", "COMPLETE"}
                            or plan.generation != runtime.generation
                            or plan.configuration_sha256 != runtime.configuration_sha256
                        ):
                            return None
                        actions = session.execute(
                            select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
                            .join(
                                SpiderRuntimeBinding,
                                SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id,
                            )
                            .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
                            .where(
                                SpiderRuntimeAction.plan_id == plan.plan_id,
                                SpiderRuntimeAction.transmission_group == source_group,
                            )
                            .order_by(SpiderRuntimeAction.transmission_ordinal)
                            .with_for_update()
                        ).all()
                        if not 1 <= len(actions) <= 20:
                            return None
                        action_ids = [action.action_id for action, _binding, _order in actions]
                        group_id = _digest({
                            "domain": "SPIDER_TRANSMISSION_GROUP_V1",
                            "scope": _scope(scope),
                            "operation_id": runtime.current_operation_id,
                            "plan_id": plan.plan_id,
                            "generation": runtime.generation,
                            "configuration_sha256": runtime.configuration_sha256,
                            "operation": "PLACE",
                            "ordered_action_ids": action_ids,
                        })
                        saved = session.get(SpiderRuntimeTransmission, group_id)
                        if saved is not None:
                            return group_id, False
                        if runtime.control_mode != "RUN" or plan.plan_status != "INCOMPLETE":
                            return None
                        if not _valid_initial_place_request(
                            request,
                            scope,
                            executing_account_ref,
                            group_id,
                            cast(list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]], list(actions)),
                        ):
                            return None
                        if any(action.outcome_state != "UNSENT" or action.outcome_reference is not None
                               for action, _binding, _order in actions):
                            return None
                        runtime.last_tape_sequence += 1
                        session.add(SpiderRuntimeTransmission(
                            transmission_group_id=group_id,
                            environment=scope.environment,
                            execution_scope_id=scope.execution_scope_id,
                            strategy_id=scope.strategy_id,
                            operation_id=runtime.current_operation_id,
                            plan_id=plan.plan_id,
                            generation=runtime.generation,
                            configuration_sha256=runtime.configuration_sha256,
                            operation="PLACE",
                            request_payload=request_payload,
                            request_sha256=_digest_bytes(request_payload),
                            response_payload=None,
                            response_sha256=None,
                            response_failure=None,
                            recorded_at=_database_millisecond_time(session),
                            response_recorded_at=None,
                        ))
                        session.flush()
                        session.add(SpiderRuntimeTapeIndex(
                            environment=scope.environment,
                            execution_scope_id=scope.execution_scope_id,
                            strategy_id=scope.strategy_id,
                            ordinal=runtime.last_tape_sequence,
                            kind="SEND_MARKER",
                            delivery_sequence=None,
                            transmission_group_id=group_id,
                        ))
                        for action, _binding, _order in actions:
                            action.outcome_state = "SEND_UNKNOWN"
                            action.outcome_reference = group_id
                        if self._refresh_initial_place_projection(
                            session, scope, runtime, plan, persist=True
                        ) is None:
                            raise RuntimeError("SPIDER_INITIAL_PLAN_PROJECTION_UNAVAILABLE")
                        session.flush()
                    # Returning from `session.begin()` proves this invocation's
                    # marker commit; any exception above forbids the call.
                return group_id, True

    def _record_transmission_result(
        self,
        scope: RuntimeScopeKey,
        transmission_group_id: str,
        request_payload: bytes,
        response_payload: bytes | None,
        response_failure: str | None,
    ) -> None:
        if response_failure not in {None, "TRANSPORT_UNKNOWN", "RESPONSE_INVALID", "RESPONSE_TOO_LARGE"}:
            raise ValueError("invalid Spider transport failure token")
        if response_payload is None and response_failure is None:
            response_failure = "TRANSPORT_UNKNOWN"
        outcomes: dict[str, MemberOutcome] | None = None
        if response_payload is not None:
            if len(response_payload) > 1_048_576:
                response_payload = None
                response_failure = "RESPONSE_TOO_LARGE"
            else:
                outcomes = self._validated_transport_outcomes(request_payload, response_payload)
                if outcomes is None:
                    response_payload = None
                    response_failure = "RESPONSE_INVALID"
        lifecycle_lock = self._lifecycle_lock
        order_event_lock = self._order_event_lock
        if lifecycle_lock is None or order_event_lock is None:
            raise RuntimeError("SPIDER_LOCK_UNAVAILABLE")
        with lifecycle_lock(scope.strategy_id):
            with order_event_lock:
                with self._sessions() as session:
                    if (
                        not isinstance(session, Session)
                        or session.in_transaction()
                        or not session.is_active
                        or session.get_bind().dialect.name != "postgresql"
                    ):
                        raise RuntimeError("SPIDER_SESSION_INVALID")
                    with session.begin():
                        self._configure_writer(session)
                        runtime = session.execute(
                            select(SpiderRuntimeScope).where(
                                SpiderRuntimeScope.environment == scope.environment,
                                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                                SpiderRuntimeScope.strategy_id == scope.strategy_id,
                            ).with_for_update()
                        ).scalar_one_or_none()
                        transmission = session.execute(
                            select(SpiderRuntimeTransmission).where(
                                SpiderRuntimeTransmission.transmission_group_id == transmission_group_id,
                                SpiderRuntimeTransmission.environment == scope.environment,
                                SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id,
                                SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
                            ).with_for_update()
                        ).scalar_one_or_none()
                        if (
                            runtime is None
                            or transmission is None
                            or transmission.request_payload != request_payload
                            or transmission.request_sha256 != _digest_bytes(request_payload)
                            or not validate_tape_index(session, runtime)
                        ):
                            raise RuntimeError("SPIDER_TRANSMISSION_MARKER_UNAVAILABLE")
                        if transmission.response_recorded_at is not None:
                            raise RuntimeError("SPIDER_TRANSMISSION_RESULT_CONFLICT")
                        actions = session.execute(
                            select(SpiderRuntimeAction).where(
                                SpiderRuntimeAction.plan_id == transmission.plan_id,
                                SpiderRuntimeAction.outcome_reference == transmission_group_id,
                            ).order_by(SpiderRuntimeAction.transmission_ordinal)
                            .with_for_update()
                        ).scalars().all()
                        if not actions or any(action.outcome_state != "SEND_UNKNOWN" for action in actions):
                            raise RuntimeError("SPIDER_TRANSMISSION_ACTIONS_UNAVAILABLE")
                        if response_payload is not None:
                            response_failure = None
                        transmission.response_payload = response_payload
                        transmission.response_sha256 = (
                            None if response_payload is None else _digest_bytes(response_payload)
                        )
                        transmission.response_failure = response_failure
                        transmission.response_recorded_at = cast(
                            Any, _database_millisecond_time(session)
                        )
                        plan = session.execute(
                            select(SpiderRuntimePlan).where(
                                SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                                SpiderRuntimePlan.operation_id == runtime.current_operation_id,
                            ).with_for_update()
                        ).scalar_one_or_none()
                        if plan is None or self._refresh_initial_place_projection(
                            session, scope, runtime, plan, persist=True
                        ) is None:
                            raise RuntimeError("SPIDER_INITIAL_PLAN_PROJECTION_UNAVAILABLE")
                        session.flush()

    def _validated_transport_outcomes(
        self,
        request_payload: bytes,
        response_payload: bytes,
    ) -> dict[str, MemberOutcome] | None:
        classifier = self._transport_response_classifier
        if classifier is None:
            return None
        try:
            classified = classifier(request_payload, response_payload)
        except Exception:
            return None
        request = _decode_transport_request(request_payload)
        if request is None:
            return None
        members = cast(list[dict[str, object]], request["members"])
        action_ids = [cast(str, member["action_id"]) for member in members]
        return validate_member_outcomes(action_ids, classified)

    def _transmission_readback(
        self,
        scope: RuntimeScopeKey,
        transmission_group_id: str,
        request_payload: bytes,
    ) -> _TransmissionOutcome | None:
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
                    runtime = session.execute(
                        select(SpiderRuntimeScope).where(
                            SpiderRuntimeScope.environment == scope.environment,
                            SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeScope.strategy_id == scope.strategy_id,
                        )
                    ).scalar_one_or_none()
                    transmission = session.execute(
                        select(SpiderRuntimeTransmission).where(
                            SpiderRuntimeTransmission.transmission_group_id == transmission_group_id,
                            SpiderRuntimeTransmission.environment == scope.environment,
                            SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
                        )
                    ).scalar_one_or_none()
                    if (
                        runtime is None
                        or transmission is None
                        or transmission.request_payload != request_payload
                        or transmission.request_sha256 != _digest_bytes(request_payload)
                        or not validate_tape_index(session, runtime)
                    ):
                        return None
                    plan = session.execute(
                        select(SpiderRuntimePlan).where(
                            SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                            SpiderRuntimePlan.operation_id == runtime.current_operation_id,
                        )
                    ).scalar_one_or_none()
                    if plan is None or self._refresh_initial_place_projection(
                        session, scope, runtime, plan, persist=False
                    ) is None:
                        return None
                    actions = session.execute(
                        select(SpiderRuntimeAction).where(
                            SpiderRuntimeAction.plan_id == transmission.plan_id,
                            SpiderRuntimeAction.outcome_reference == transmission_group_id,
                        ).order_by(SpiderRuntimeAction.transmission_ordinal)
                    ).scalars().all()
                    if not actions:
                        return None
                    return _TransmissionOutcome(
                        "SEND_UNKNOWN" if transmission.response_recorded_at is None else "RECORDED",
                        transmission_group_id,
                        tuple((action.action_id, action.outcome_state) for action in actions),
                    )
        except Exception:
            return None

    def _current_stop_proof(
        self, session: Session, scope: RuntimeScopeKey, runtime: SpiderRuntimeScope
    ) -> dict[str, object] | None:
        table = SpiderControlReceipt.__table__
        rows = session.execute(
            select(table).where(
                table.c.environment == scope.environment,
                table.c.execution_scope_id == scope.execution_scope_id,
                table.c.strategy_id == scope.strategy_id,
                table.c.operation_id == runtime.current_operation_id,
                table.c.generation == runtime.generation,
                table.c.after_state_version == runtime.state_version,
                table.c.outcome == "COMPLETED",
            ).limit(2)
        ).mappings().all()
        if len(rows) != 1:
            return None
        try:
            request, proof = hydrate_control_receipt(rows[0])
        except SpiderControlCommandError:
            return None
        if (
            (request.environment, request.execution_scope_id, request.strategy_id)
            != (scope.environment, scope.execution_scope_id, scope.strategy_id)
            or proof["operation_id"] != runtime.current_operation_id
            or proof["generation"] != runtime.generation
            or proof["after_state_version"] != runtime.state_version
        ):
            return None
        return verify_stop_in_transaction(session, request)

    @staticmethod
    def _warn_stop_backlog_unavailable(
        scope: RuntimeScopeKey, sequence: int | None, reason: str
    ) -> None:
        logging.getLogger(__name__).warning(
            "SPIDER_STOP_BACKLOG_UNAVAILABLE strategy=%s sequence=%s reason=%s",
            scope.strategy_id,
            sequence,
            reason,
        )

    def _drain_stopped_scope(self, scope: RuntimeScopeKey) -> None:
        """Consume a finite committed STOP backlog, one caller-owned TX per row."""
        try:
            with self._sessions() as session:
                if (
                    not isinstance(session, Session)
                    or session.in_transaction()
                    or not session.is_active
                    or session.get_bind().dialect.name != "postgresql"
                ):
                    self._warn_stop_backlog_unavailable(scope, None, "SESSION_UNAVAILABLE")
                    return
                snapshot = session.execute(
                    select(
                        SpiderRuntimeScope.current_operation_id,
                        SpiderRuntimeScope.control_mode,
                        SpiderRuntimeScope.last_consumed_sequence,
                        SpiderRuntimeScope.last_received_sequence,
                    ).where(
                        SpiderRuntimeScope.environment == scope.environment,
                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                    )
                ).one_or_none()
            if snapshot is None:
                self._warn_stop_backlog_unavailable(scope, None, "RUNTIME_MISSING")
                return
            operation_id, control_mode, consumed, frontier = snapshot
            if type(operation_id) is not str or type(consumed) is not int or type(frontier) is not int:
                return
            if control_mode != "STOP" or consumed >= frontier:
                return
            if self._order_event_lock is None:
                self._warn_stop_backlog_unavailable(scope, None, "LOCK_UNAVAILABLE")
                return
            for sequence in range(consumed + 1, frontier + 1):
                with cast(ContextManager[object], self._order_event_lock):
                    try:
                        with self._sessions() as session:
                            if (
                                not isinstance(session, Session)
                                or session.in_transaction()
                                or not session.is_active
                                or session.get_bind().dialect.name != "postgresql"
                            ):
                                self._warn_stop_backlog_unavailable(
                                    scope, sequence, "SESSION_UNAVAILABLE"
                                )
                                return
                            with session.begin():
                                self._configure_writer(session)
                                runtime = session.execute(
                                    select(SpiderRuntimeScope).where(
                                        SpiderRuntimeScope.environment == scope.environment,
                                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                                    ).with_for_update()
                                ).scalar_one_or_none()
                                if runtime is None or runtime.current_operation_id != operation_id:
                                    self._warn_stop_backlog_unavailable(
                                        scope, sequence, "RUNTIME_IDENTITY_MISMATCH"
                                    )
                                    return
                                if runtime.control_mode != "STOP":
                                    return
                                if (
                                    runtime.last_consumed_sequence != sequence - 1
                                    or runtime.last_received_sequence < sequence
                                ):
                                    self._warn_stop_backlog_unavailable(
                                        scope, sequence, "RECEIPT_FRONTIER_MISMATCH"
                                    )
                                    return
                                if self._current_stop_proof(session, scope, runtime) is None:
                                    self._warn_stop_backlog_unavailable(
                                        scope, sequence, "STOP_PROOF_UNAVAILABLE"
                                    )
                                    return
                                if self.readback(scope, operation_id) is None:
                                    self._warn_stop_backlog_unavailable(
                                        scope, sequence, "INSTALL_OR_RECEIPT_PROOF_UNAVAILABLE"
                                    )
                                    return
                                delivery = session.execute(
                                    select(SpiderRuntimeDelivery).where(
                                        SpiderRuntimeDelivery.environment == scope.environment,
                                        SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                                        SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                                        SpiderRuntimeDelivery.sequence == sequence,
                                    ).with_for_update()
                                ).scalar_one_or_none()
                                if (
                                    delivery is None
                                    or delivery.disposition != "PENDING"
                                    or delivery.suppression_reason is not None
                                    or _digest_bytes(bytes(delivery.source_payload))
                                    != delivery.content_sha256
                                ):
                                    self._warn_stop_backlog_unavailable(
                                        scope, sequence, "DELIVERY_PROOF_UNAVAILABLE"
                                    )
                                    return
                                delivery.disposition = "SUPPRESSED"
                                delivery.suppression_reason = "STOP_CONTROL"
                                runtime.last_consumed_sequence = sequence
                    except Exception:
                        # Commit/close uncertainty is resolved only by fresh proof below.
                        pass
                proved = self.readback(scope, operation_id)
                consumed_after = None if proved is None else proved.get("consumed_sequence")
                if type(consumed_after) is not int or consumed_after < sequence:
                    self._warn_stop_backlog_unavailable(
                        scope, sequence, "SUPPRESSION_COMMIT_UNPROVED"
                    )
                    return
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider STOP backlog restoration unavailable for %s", scope.strategy_id
            )

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
            last_tape_sequence=0,
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
                    if not validate_tape_index(session, runtime):
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
                        or plan.plan_status not in {"INCOMPLETE", "COMPLETE"}
                    ):
                        return None
                    plan_order_ids = set(session.execute(
                        select(SpiderRuntimeAction.local_order_id)
                        .where(SpiderRuntimeAction.plan_id == plan.plan_id)
                    ).scalars().all())
                    scoped_receipts = session.execute(
                        select(SpiderRuntimeDelivery).where(
                            SpiderRuntimeDelivery.environment == scope.environment,
                            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                        ).order_by(SpiderRuntimeDelivery.sequence)
                    ).scalars().all()
                    if (
                        len(scoped_receipts) != runtime.last_received_sequence
                        or any(
                            row.sequence != index
                            or row.local_order_id not in plan_order_ids
                            or (
                                index <= runtime.last_consumed_sequence
                                and (
                                    row.disposition != "SUPPRESSED"
                                    or row.suppression_reason != "STOP_CONTROL"
                                )
                            )
                            or (
                                index > runtime.last_consumed_sequence
                                and (row.disposition != "PENDING" or row.suppression_reason is not None)
                            )
                            for index, row in enumerate(scoped_receipts, 1)
                        )
                    ):
                        return None
                    if (
                        runtime.last_consumed_sequence > 0
                        and runtime.control_mode != "STOP"
                    ) or (
                        runtime.control_mode == "STOP"
                        and self._current_stop_proof(session, scope, runtime) is None
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
                    projected_missing = self._refresh_initial_place_projection(
                        session, scope, runtime, plan, persist=False
                    )
                    if projected_missing is None:
                        return None
                    missing = projected_missing
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

    def _refresh_initial_place_projection(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        *,
        persist: bool,
        current_order: Order | None = None,
    ) -> list[dict[str, object]] | None:
        if not validate_tape_index(session, runtime):
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
        if not identity_rows:
            return None
        identity_rows = cast(
            list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]],
            list(identity_rows),
        )
        if current_order is not None:
            matches = [
                row for row in identity_rows
                if row[2].id == current_order.id
            ]
            if len(matches) != 1:
                return None
            identity_rows = [
                (action, binding, current_order if order.id == current_order.id else order)
                for action, binding, order in identity_rows
            ]
        if not self._validate_initial_install_source(
            session, scope, runtime, plan, identity_rows
        ):
            return None
        outcomes = self._validated_transmission_records(
            session,
            scope,
            runtime,
            plan,
            cast(list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]], identity_rows),
            verify_stored=not persist,
        )
        if outcomes is None:
            return None
        candidates: list[OriginalPlaceCandidate] = []
        for action, binding, _order in identity_rows:
            if _digest_bytes(action.original_payload) != action.payload_sha256:
                return None
            candidates.append(OriginalPlaceCandidate(
                action_id=action.action_id,
                source_product_id=binding.source_product_id,
                original_payload=action.original_payload,
                payload_sha256=action.payload_sha256,
            ))
        try:
            missing = build_missing_projection(candidates, outcomes)
            missing_payload = canonical_bytes(missing)
        except (TypeError, ValueError):
            return None
        plan_status = expected_plan_status(missing)
        if persist:
            for action, _binding, _order in identity_rows:
                action.outcome_state = outcomes[action.action_id][0]
            plan.missing_members = missing_payload
            plan.plan_status = plan_status
        elif (
            plan.missing_members != missing_payload
            or plan.plan_status != plan_status
        ):
            return None
        return missing

    def _validate_initial_install_source(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        identity_rows: list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]],
    ) -> bool:
        """Prove immutable INSTALL source events still produce every saved row."""
        if (
            runtime.current_operation_id is None
            or runtime.initial_plan_id != plan.plan_id
            or plan.operation_id != runtime.current_operation_id
            or (runtime.environment, runtime.execution_scope_id, runtime.strategy_id)
            != (scope.environment, scope.execution_scope_id, scope.strategy_id)
            or (plan.environment, plan.execution_scope_id, plan.strategy_id)
            != (scope.environment, scope.execution_scope_id, scope.strategy_id)
            or plan.generation != runtime.generation
            or plan.configuration_sha256 != runtime.configuration_sha256
            or type(runtime.source_checkpoint) is not bytes
            or _digest_bytes(runtime.source_checkpoint)
            != runtime.source_checkpoint_sha256
            or type(plan.manifest_payload) is not bytes
            or _digest_bytes(plan.manifest_payload) != plan.manifest_sha256
        ):
            return False
        try:
            manifest_value = decode_canonical(plan.manifest_payload)
            if type(manifest_value) is not dict:
                return False
            manifest = cast(dict[str, object], manifest_value)
            initial_manifest_value = manifest.get("initial_manifest")
            bootstrap_value = manifest.get("bootstrap_basis")
            product_map_value = manifest.get("source_product_map")
            events_value = manifest.get("initial_source_events")
            if any(
                type(value) is not dict
                for value in (
                    initial_manifest_value,
                    bootstrap_value,
                    product_map_value,
                    events_value,
                )
            ):
                return False
            initial_manifest = cast(dict[str, object], initial_manifest_value)
            bootstrap = cast(dict[str, object], bootstrap_value)
            product_map = cast(dict[str, object], product_map_value)
            source_events = cast(dict[str, object], events_value)
            historical_provenance_value = initial_manifest.get("historical_provenance")
            if type(historical_provenance_value) is not dict:
                return False
            historical_provenance = cast(dict[str, object], historical_provenance_value)
            source_sha256 = historical_provenance.get("source_sha256")
            if (
                manifest.get("domain") != "SPIDER_INITIAL_MANIFEST_V1"
                or manifest.get("operation_id") != runtime.current_operation_id
                or manifest.get("confirmation_request_id") != runtime.confirmation_request_id
                or manifest.get("configuration_sha256") != runtime.configuration_sha256
                or manifest.get("generation") != runtime.generation
                or manifest.get("build_checkpoint_sha256") != runtime.source_checkpoint_sha256
                or initial_manifest.get("build_checkpoint_sha256")
                != runtime.source_checkpoint_sha256
                or type(manifest.get("bootstrap_digest")) is not str
                or manifest.get("bootstrap_digest") != _digest(bootstrap)
                or initial_manifest.get("bootstrap_basis_sha256")
                != manifest.get("bootstrap_digest")
                or initial_manifest.get("product_map") != product_map
                or type(initial_manifest.get("product_map_sha256")) is not str
                or initial_manifest.get("product_map_sha256") != _digest(product_map)
                or bootstrap.get("product_map_sha256") != _digest(product_map)
                or type(initial_manifest.get("specification_sha256")) is not str
                or initial_manifest.get("specification_sha256") != plan.specification_sha256
                or bootstrap.get("specification_sha256") != plan.specification_sha256
                or initial_manifest.get("source_checkpoint_sha256")
                != plan.source_checkpoint_sha256
                or bootstrap.get("source_checkpoint_sha256")
                != plan.source_checkpoint_sha256
                or type(source_sha256) is not str
                or len(source_sha256) != 64
                or any(char not in "0123456789abcdef" for char in source_sha256)
                or manifest.get("initial_source_events") is None
                or type(initial_manifest.get("initial_source_events_sha256")) is not str
                or initial_manifest.get("initial_source_events_sha256")
                != _digest(source_events)
            ):
                return False
            operation_core_sha256 = manifest.get("operation_core_sha256")
            if type(operation_core_sha256) is not str:
                return False
            operation = session.execute(
                select(SpiderRuntimeOperation).where(
                    SpiderRuntimeOperation.operation_id == runtime.current_operation_id,
                    SpiderRuntimeOperation.environment == scope.environment,
                    SpiderRuntimeOperation.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeOperation.strategy_id == scope.strategy_id,
                )
            ).scalar_one_or_none()
            if (
                operation is None
                or operation.phase != "INCOMPLETE"
                or operation.operation_core_sha256 != operation_core_sha256
                or manifest.get("operation_core_sha256")
                != operation.operation_core_sha256
                or operation.configuration_request_id != runtime.confirmation_request_id
            ):
                return False

            basis_selection_value = bootstrap.get("runtime_selection")
            if type(basis_selection_value) is not dict:
                return False
            selection = cast(dict[str, object], basis_selection_value)
            if (
                selection.get("environment") != scope.environment
                or selection.get("execution_scope_id") != scope.execution_scope_id
                or selection.get("strategy_id") != scope.strategy_id
                or selection.get("confirmation_request_id")
                != runtime.confirmation_request_id
                or selection.get("configuration_sha256") != runtime.configuration_sha256
            ):
                return False
            configuration_row = session.execute(
                select(SpiderConfigurationRequest.__table__).where(
                    SpiderConfigurationRequest.request_id == runtime.confirmation_request_id
                )
            ).mappings().one_or_none()
            if configuration_row is None:
                return False
            configuration_record = _hydrate(configuration_row)
            confirmation = configuration_record.request
            configuration = confirmation.configuration.value
            if (
                confirmation.request_kind is not SpiderConfigurationRequestKind.CONFIRMATION
                or confirmation.request_id != runtime.confirmation_request_id
                or confirmation.configuration.sha256 != runtime.configuration_sha256
                or (confirmation.environment, confirmation.execution_scope_id, confirmation.strategy_id)
                != (scope.environment, scope.execution_scope_id, scope.strategy_id)
                or configuration.get("ordered_products") != product_map.get("ordered_products")
                or configuration.get("policy_id") not in {
                    "SPIDER_HOST_ORIGINAL_PAID_V1",
                    "SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1",
                }
            ):
                return False

            ordered_products_value = product_map.get("ordered_products")
            entries_value = product_map.get("entries")
            source_entries_value = source_events.get("entries")
            if (
                type(ordered_products_value) is not list
                or not ordered_products_value
                or any(type(product) is not str or not product for product in ordered_products_value)
                or len(set(ordered_products_value)) != len(ordered_products_value)
                or source_events.get("domain") != "SPIDER_INITIAL_SOURCE_EVENTS_V1"
                or source_events.get("ordered_products") != ordered_products_value
                or type(entries_value) is not list
                or type(source_entries_value) is not list
                or len(entries_value) != len(ordered_products_value)
                or len(source_entries_value) != len(ordered_products_value)
            ):
                return False
            ordered_products = cast(list[str], ordered_products_value)
            product_entries = cast(list[object], entries_value)
            source_entries = cast(list[object], source_entries_value)
            targets: dict[str, str] = {}
            for product, map_entry, source_entry in zip(
                ordered_products, product_entries, source_entries, strict=True
            ):
                if (
                    type(map_entry) is not dict
                    or type(source_entry) is not dict
                    or cast(dict[str, object], map_entry).get("source_product_id") != product
                    or cast(dict[str, object], source_entry).get("source_product_id") != product
                ):
                    return False
                instrument = cast(dict[str, object], map_entry).get("instrument_spec")
                if type(instrument) is not dict:
                    return False
                target_product = cast(dict[str, object], instrument).get("product_id")
                if type(target_product) is not str or not target_product:
                    return False
                targets[product] = target_product

            source_candidates: list[tuple[str, dict[str, object], int, int]] = []
            transmission_group = 0
            for product, source_entry_value in zip(
                ordered_products, source_entries, strict=True
            ):
                source_entry = cast(dict[str, object], source_entry_value)
                events_value = source_entry.get("events")
                if type(events_value) is not list or not events_value:
                    return False
                for event_value in cast(list[object], events_value):
                    if type(event_value) is not dict:
                        return False
                    event_payload = cast(dict[str, object], event_value)
                    event_kind = event_payload.get("kind")
                    if event_kind == "request_market":
                        continue
                    if event_kind != "send":
                        return False
                    orders_value = event_payload.get("orders")
                    if type(orders_value) is not list or not orders_value:
                        return False
                    transmission_group += 1
                    for ordinal, order_value in enumerate(cast(list[object], orders_value)):
                        if type(order_value) is not dict:
                            return False
                        source_order = cast(dict[str, object], order_value)
                        if (
                            source_order.get("instId") != product
                            or source_order.get("side") not in {"buy", "sell"}
                            or type(source_order.get("clOrdId")) is not str
                            or not cast(str, source_order["clOrdId"])
                            or source_order.get("ordType") != "limit"
                            or type(source_order.get("px")) is not str
                            or type(source_order.get("sz")) is not str
                        ):
                            return False
                        source_candidates.append(
                            (product, source_order, transmission_group, ordinal)
                        )
            if len(source_candidates) != len(identity_rows) or not source_candidates:
                return False
            for candidate_ordinal, (candidate, row) in enumerate(
                zip(source_candidates, identity_rows, strict=True), 1
            ):
                source_product, source_order, group, group_ordinal = candidate
                action, binding, order = row
                source_bytes = canonical_bytes(source_order)
                action_id = _digest({
                    "domain": "SPIDER_INITIAL_ACTION_V1",
                    "scope": _scope(scope),
                    "operation_id": runtime.current_operation_id,
                    "plan_id": plan.plan_id,
                    "candidate_ordinal": candidate_ordinal,
                })
                intent = order.intent_payload
                if type(intent) is not dict:
                    return False
                wire_bytes_hex = intent.get("provider_binding_bytes_hex")
                if type(wire_bytes_hex) is not str:
                    return False
                wire_bytes = bytes.fromhex(wire_bytes_hex)
                wire_binding = decode_canonical(wire_bytes)
                expected_wire_id = "sp" + sha256(
                    b"SPIDER_OKX_CLIENT_BINDING_V1\0" + wire_bytes
                ).hexdigest()[:30]
                expected_wire_binding = {
                    "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
                    "environment": scope.environment,
                    "execution_scope_id": scope.execution_scope_id,
                    "strategy_id": scope.strategy_id,
                    "action_id": action_id,
                    "product_id": targets[source_product],
                }
                if (
                    action.action_id != action_id
                    or action.plan_id != plan.plan_id
                    or action.environment != scope.environment
                    or action.execution_scope_id != scope.execution_scope_id
                    or action.strategy_id != scope.strategy_id
                    or action.generation != runtime.generation
                    or action.configuration_sha256 != runtime.configuration_sha256
                    or action.role != "GRID"
                    or action.product_id != source_product
                    or action.local_order_id != order.id
                    or action.trigger_delivery_id is not None
                    or action.candidate_ordinal != candidate_ordinal
                    or action.transmission_group != group
                    or action.transmission_ordinal != group_ordinal
                    or action.original_payload != source_bytes
                    or action.payload_sha256 != _digest_bytes(source_bytes)
                    or binding.action_id != action_id
                    or binding.environment != scope.environment
                    or binding.execution_scope_id != scope.execution_scope_id
                    or binding.strategy_id != scope.strategy_id
                    or binding.local_order_id != order.id
                    or binding.source_product_id != source_product
                    or binding.canonical_product_id != targets[source_product]
                    or binding.product_map_sha256 != _digest(product_map)
                    or order.product_id != targets[source_product]
                    or order.type != "limit"
                    or order.side != source_order["side"]
                    or order.price != _positive_decimal(source_order["px"])
                    or order.quantity != _positive_decimal(source_order["sz"])
                    or order.account_profile != selection.get("account_profile")
                    or order.account_id != selection.get("account_id")
                    or binding.source_client_order_id != source_order["clOrdId"]
                    or order.client_order_id != intent.get("local_client_order_id")
                    or order.client_order_id == binding.source_client_order_id
                    or intent.get("source_client_order_id") != binding.source_client_order_id
                    or intent.get("provider_wire_client_order_id") != expected_wire_id
                    or intent.get("provider_binding_sha256") != _digest_bytes(wire_bytes)
                    or wire_binding != expected_wire_binding
                ):
                    return False
            restore_source_checkpoint(
                runtime.source_checkpoint,
                ordered_products=tuple(ordered_products),
                policy_id=ORIGINAL_MODEL_POLICY_ID,
                policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                source_sha256=source_sha256,
                confirmation_request_id=runtime.confirmation_request_id,
                configuration_sha256=runtime.configuration_sha256,
                generation=runtime.generation,
            )
        except (InvalidOperation, KeyError, TypeError, ValueError, UnicodeDecodeError):
            return False
        return True

    def _validated_transmission_records(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        identity_rows: list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]],
        *,
        verify_stored: bool,
    ) -> dict[str, MemberOutcome] | None:
        by_action = {action.action_id: (action, binding, order)
                     for action, binding, order in identity_rows}
        if len(by_action) != len(identity_rows):
            return None
        financial_acceptance: dict[str, bool] = {}
        for action, binding, order in identity_rows:
            current_projection = {
                "status": order.status,
                "filled_quantity": (
                    None if order.filled_quantity is None
                    else canonical_decimal_text(order.filled_quantity)
                ),
                "filled_price": (
                    None if order.filled_price is None
                    else canonical_decimal_text(order.filled_price)
                ),
                "exchange_order_id": order.exchange_order_id,
            }
            if not validate_order_receipt_chain(
                session,
                scope=_scope(scope),
                action=action,
                binding=binding,
                order_id=order.id,
                current_projection=current_projection,
            ):
                return None
            receipts = session.execute(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.environment == scope.environment,
                    SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                    SpiderRuntimeDelivery.action_id == action.action_id,
                    SpiderRuntimeDelivery.local_order_id == order.id,
                ).order_by(SpiderRuntimeDelivery.sequence)
            ).scalars().all()
            proves_acceptance = False
            for receipt in receipts:
                try:
                    payload = decode_canonical(receipt.source_payload)
                except (TypeError, ValueError, UnicodeDecodeError):
                    return None
                if type(payload) is not dict:
                    return None
                binding_value = payload.get("binding")
                if type(binding_value) is not dict:
                    return None
                if (
                    payload.get("schema_version") != "SPIDER_FINANCIAL_RECEIPT_V1"
                    or payload.get("scope") != _scope(scope)
                    or payload.get("kind") != receipt.kind
                    or payload.get("binding_resolution") not in {"BOUND", "UNRESOLVED"}
                    or cast(dict[str, object], binding_value).get("action_id") != action.action_id
                    or cast(dict[str, object], binding_value).get("local_order_id") != order.id
                ):
                    return None
                if (
                    receipt.kind in {"FILL", "ORDER_STATE"}
                    and payload.get("binding_resolution") == "BOUND"
                ):
                    proves_acceptance = True
            financial_acceptance[action.action_id] = proves_acceptance

        transmissions = session.execute(
            select(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.environment == scope.environment,
                SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
            ).order_by(SpiderRuntimeTransmission.recorded_at)
        ).scalars().all()
        outcomes: dict[str, MemberOutcome] = {
            action_id: ("UNSENT", None, None) for action_id in by_action
        }
        transmission_action_ids: set[str] = set()
        transmission_reference_by_action: dict[str, str] = {}
        for transmission in transmissions:
            request = _decode_transport_request(transmission.request_payload)
            if (
                request is None
                or transmission.request_sha256 != _digest_bytes(transmission.request_payload)
                or transmission.operation != "PLACE"
                or transmission.operation_id != runtime.current_operation_id
                or transmission.plan_id != plan.plan_id
                or transmission.generation != runtime.generation
                or transmission.configuration_sha256 != runtime.configuration_sha256
                or request.get("transmission_group_id") != transmission.transmission_group_id
                or request.get("scope") != _scope(scope)
            ):
                return None
            members = cast(list[dict[str, object]], request["members"])
            action_ids = [cast(str, member["action_id"]) for member in members]
            if (
                not action_ids
                or len(set(action_ids)) != len(action_ids)
                or transmission_action_ids.intersection(action_ids)
                or any(action_id not in by_action for action_id in action_ids)
            ):
                return None
            transmission_action_ids.update(action_ids)
            transmission_reference_by_action.update(
                {action_id: transmission.transmission_group_id for action_id in action_ids}
            )
            expected_group = _digest({
                "domain": "SPIDER_TRANSMISSION_GROUP_V1",
                "scope": _scope(scope),
                "operation_id": transmission.operation_id,
                "plan_id": transmission.plan_id,
                "generation": transmission.generation,
                "configuration_sha256": transmission.configuration_sha256,
                "operation": transmission.operation,
                "ordered_action_ids": action_ids,
            })
            if expected_group != transmission.transmission_group_id:
                return None
            action_rows = [by_action[action_id] for action_id in action_ids]
            if (
                any(action.action_id != action_id
                    for action_id, (action, _binding, _order) in zip(action_ids, action_rows, strict=True))
                or any(
                    action.transmission_group != action_rows[0][0].transmission_group
                    or action.outcome_reference != transmission.transmission_group_id
                    for action, _binding, _order in action_rows
                )
                or not _valid_initial_place_request(
                    request, scope, cast(str, request["executing_account_ref"]),
                    transmission.transmission_group_id,
                    action_rows,
                    require_unsent=False,
                )
            ):
                return None
            if transmission.response_recorded_at is None:
                if (
                    transmission.response_payload is not None
                    or transmission.response_sha256 is not None
                    or transmission.response_failure is not None
                    or any(action.outcome_state != "SEND_UNKNOWN" for action, _binding, _order in action_rows)
                ):
                    return None
                raw_outcomes = {
                    action_id: ("SEND_UNKNOWN", None, None) for action_id in action_ids
                }
            elif transmission.response_payload is not None:
                if (
                    not 1 <= len(transmission.response_payload) <= 1_048_576
                    or transmission.response_sha256 != _digest_bytes(transmission.response_payload)
                    or transmission.response_failure is not None
                ):
                    return None
                raw_outcomes = self._validated_transport_outcomes(
                    transmission.request_payload, transmission.response_payload
                )
                if raw_outcomes is None:
                    return None
            else:
                if (
                    transmission.response_sha256 is not None
                    or transmission.response_failure not in {
                        "TRANSPORT_UNKNOWN", "RESPONSE_INVALID", "RESPONSE_TOO_LARGE"
                    }
                ):
                    return None
                raw_outcomes = {
                    action_id: ("SEND_UNKNOWN", None, None) for action_id in action_ids
                }
            for action_id, (_action, _binding, order) in zip(action_ids, action_rows, strict=True):
                outcomes[action_id] = effective_member_outcome(
                    raw_outcomes[action_id],
                    financial_acceptance_proved=financial_acceptance[action_id],
                    current_provider_order_id=order.exchange_order_id,
                )
        for action_id, (action, _binding, _order) in by_action.items():
            if action_id in transmission_action_ids:
                if action.outcome_reference != transmission_reference_by_action[action_id]:
                    return None
            elif action.outcome_reference is not None:
                return None
            if verify_stored and action.outcome_state != outcomes[action_id][0]:
                return None
        return outcomes

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


def _decode_transport_request(payload: bytes) -> dict[str, object] | None:
    if type(payload) is not bytes or not 1 <= len(payload) <= 1_048_576:
        return None
    try:
        decoded = decode_canonical(payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if type(decoded) is not dict:
        return None
    request = cast(dict[str, object], decoded)
    required = {
        "schema_version", "scope", "executing_account_ref", "transmission_group_id",
        "operation", "members", "payload_sha256",
    }
    if set(request) != required or request.get("schema_version") != "SPIDER_TRANSPORT_BATCH_V1":
        return None
    body = dict(request)
    supplied_digest = body.pop("payload_sha256")
    if type(supplied_digest) is not str or supplied_digest != _digest(body):
        return None
    scope = request.get("scope")
    if (
        type(scope) is not dict
        or set(cast(dict[str, object], scope))
        != {"environment", "execution_scope_id", "strategy_id"}
        or any(
            type(cast(dict[str, object], scope).get(key)) is not str
            or not cast(dict[str, object], scope)[key]
            for key in ("environment", "execution_scope_id", "strategy_id")
        )
        or type(request.get("executing_account_ref")) is not str
        or not cast(str, request["executing_account_ref"])
        or type(request.get("transmission_group_id")) is not str
        or type(request.get("members")) is not list
        or not 1 <= len(cast(list[object], request["members"])) <= 20
    ):
        return None
    if request.get("operation") != "PLACE":
        return None
    member_keys = {
        "action_id", "product_id", "local_order_id", "binding_sha256",
        "wire_clOrdId", "ordId", "request_payload",
    }
    for member in cast(list[object], request["members"]):
        if (
            type(member) is not dict
            or set(cast(dict[str, object], member)) != member_keys
            or type(cast(dict[str, object], member).get("request_payload")) is not dict
        ):
            return None
    return request


def _valid_initial_place_request(
    request: dict[str, object],
    scope: RuntimeScopeKey,
    executing_account_ref: str,
    group_id: str,
    actions: list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]],
    *,
    require_unsent: bool = True,
) -> bool:
    body = dict(request)
    supplied_digest = body.pop("payload_sha256", None)
    if (
        supplied_digest != _digest(body)
        or request.get("transmission_group_id") != group_id
        or request.get("operation") != "PLACE"
        or request.get("executing_account_ref") != executing_account_ref
        or request.get("scope") != _scope(scope)
    ):
        return False
    members = cast(list[dict[str, object]], request["members"])
    if len(members) != len(actions):
        return False
    for member, (action, binding, order) in zip(members, actions, strict=True):
        if (
            action.environment != scope.environment
            or action.execution_scope_id != scope.execution_scope_id
            or action.strategy_id != scope.strategy_id
            or action.role != "GRID"
            or action.parent_action_id is not None
            or action.trigger_delivery_id is not None
            or (require_unsent and action.outcome_state != "UNSENT")
            or (require_unsent and action.outcome_reference is not None)
            or binding.action_id != action.action_id
            or binding.local_order_id != order.id
            or member.get("action_id") != action.action_id
            or member.get("product_id") != action.product_id
            or member.get("local_order_id") != action.local_order_id
            or member.get("binding_sha256") is None
            or _digest_bytes(action.original_payload) != action.payload_sha256
        ):
            return False
        intent = order.intent_payload
        if type(intent) is not dict:
            return False
        try:
            binding_bytes = bytes.fromhex(cast(str, intent["provider_binding_bytes_hex"]))
            binding_value = decode_canonical(binding_bytes)
            source_payload = decode_canonical(action.original_payload)
        except (KeyError, TypeError, ValueError, UnicodeDecodeError):
            return False
        if type(binding_value) is not dict or type(source_payload) is not dict:
            return False
        saved_binding = cast(dict[str, object], binding_value)
        source_order = cast(dict[str, object], source_payload)
        binding_sha256 = _digest_bytes(binding_bytes)
        wire_client_order_id = "sp" + sha256(
            b"SPIDER_OKX_CLIENT_BINDING_V1\0" + binding_bytes
        ).hexdigest()[:30]
        expected_binding = {
            "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
            "action_id": action.action_id,
            "product_id": binding.canonical_product_id,
        }
        expected_request = dict(source_order)
        expected_request["clOrdId"] = wire_client_order_id
        if (
            saved_binding != expected_binding
            or intent.get("action_id") != action.action_id
            or intent.get("source_client_order_id") != binding.source_client_order_id
            or intent.get("local_client_order_id") != order.client_order_id
            or intent.get("provider_binding_sha256") != binding_sha256
            or intent.get("provider_wire_client_order_id") != wire_client_order_id
            or member.get("binding_sha256") != binding_sha256
            or member.get("wire_clOrdId") != wire_client_order_id
            or (
                require_unsent
                and member.get("ordId") != order.exchange_order_id
            )
            or (
                member.get("ordId") is not None
                and (
                    type(member.get("ordId")) is not str
                    or not cast(str, member.get("ordId"))
                )
            )
            or member.get("request_payload") != expected_request
            or order.id != action.local_order_id
            or order.product_id != binding.canonical_product_id
        ):
            return False
    return True


def _digest(value: object) -> str:
    return _digest_bytes(canonical_bytes(value))


def _digest_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _database_millisecond_time(session: Session) -> datetime:
    return cast(
        datetime,
        session.execute(
            select(func.date_trunc("milliseconds", func.clock_timestamp()))
        ).scalar_one(),
    )


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
