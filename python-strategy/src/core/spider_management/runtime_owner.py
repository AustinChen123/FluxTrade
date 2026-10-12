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
from uuid import NAMESPACE_URL, uuid5

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
from src.core.spider_management.source_checkpoint_codec import (
    decode_source_market_observation,
    replace_checkpoint_markets,
    restore_source_checkpoint,
    source_market_envelope_identity,
    encode_source_checkpoint,
)
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.operation_request_store import _mapping
from src.core.spider_management.financial_delivery import (
    stage_financial_receipt,
    validate_order_receipt_chain,
    validate_tape_index,
    prove_spider_order_wire_binding,
)
from src.core.spider_management.normal_callback import (
    classify_source_candidate,
    SourceClockUnavailable,
    build_orders_source_input,
    classify_source_order_transition,
    full_callback_identity,
    invoke_saved_source_callback,
    source_alias,
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
    register_market_source: Callable[[Callable[[bytes], CommandResult]], Callable[[], None]] | None = None

    def __post_init__(self) -> None:
        if type(self.owned_scopes) is not tuple or not self.owned_scopes:
            raise ValueError("Spider composition requires ordered owned scopes")
        if len({row.strategy_id for row in self.owned_scopes}) != len(self.owned_scopes):
            raise ValueError("Spider composition strategy scopes must be unique")
        if not isinstance(self.model_sources_by_confirmation, Mapping):
            raise ValueError("Spider model sources must be an explicit mapping")
        if self.register_market_source is not None and not callable(self.register_market_source):
            raise ValueError("Spider market source registrar must be callable")
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
                self._drain_source_market_scope(scope)
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
            self._drain_source_market_scope(scope)
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider STOP delivery observer could not drain committed facts"
            )

    def receive_source_market(self, raw: bytes) -> CommandResult:
        """Record one admitted MODEL market snapshot and consume only its next tape slot."""
        identity = source_market_envelope_identity(raw)
        if identity is None:
            return self._source_market_result(None, "REJECTED", "SPIDER_SOURCE_MARKET_INVALID")
        supplied_scope, snapshot_id = identity
        scope = self._owned_scopes.get(supplied_scope["strategy_id"])
        if scope is None or supplied_scope != _scope(scope):
            return self._source_market_result(scope, "REJECTED", "SPIDER_SOURCE_MARKET_SCOPE_UNKNOWN")
        if self._lifecycle_lock is None or self._order_event_lock is None:
            return self._source_market_result(scope, "UNKNOWN", "SPIDER_SOURCE_MARKET_UNAVAILABLE")

        status = "UNKNOWN"
        reason = "SPIDER_SOURCE_MARKET_WRITE_UNKNOWN"
        sequence: int | None = None
        operation_id: str | None = None
        try:
            with self._lifecycle_lock(scope.strategy_id):
                with cast(ContextManager[object], self._order_event_lock):
                    status, reason, operation_id, sequence = self._record_source_market(
                        scope, snapshot_id, raw
                    )
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider market receipt write failed; proving the same snapshot identity"
            )
            proved = self._source_market_receipt_readback(scope, snapshot_id, raw)
            if proved is None:
                return self._source_market_result(
                    scope, "UNKNOWN", "SPIDER_SOURCE_MARKET_WRITE_UNKNOWN"
                )
            operation_id = cast(str, proved["operation_id"])
            sequence = cast(int, proved["sequence"])
            readback = self.readback(scope, operation_id)
            if (
                readback is None
                or cast(int, readback["received_sequence"]) < sequence
            ):
                return self._source_market_result(
                    scope,
                    "UNKNOWN",
                    "SPIDER_SOURCE_MARKET_READBACK_UNKNOWN",
                    operation_id=operation_id,
                    sequence=sequence,
                )
            return self._source_market_result(
                scope,
                "ACCEPTED",
                "SPIDER_SOURCE_MARKET_COMMIT_PROVED",
                operation_id=operation_id,
                sequence=sequence,
                receipt_disposition=cast(str, proved["disposition"]),
                checkpoint_sha256=cast(str, proved["checkpoint_sha256"]),
            )

        if status in {"RECORDED", "DUPLICATE"}:
            self._drain_stopped_scope(scope)
            self._drain_source_market_scope(scope)
            proved = self._source_market_receipt_readback(scope, snapshot_id, raw)
            if (
                proved is None
                or self.readback(scope, cast(str, proved["operation_id"])) is None
            ):
                return self._source_market_result(
                    scope, "UNKNOWN", "SPIDER_SOURCE_MARKET_READBACK_UNKNOWN",
                    operation_id=operation_id, sequence=sequence,
                )
            return self._source_market_result(
                scope,
                "ACCEPTED" if status == "RECORDED" else "DUPLICATE",
                "SPIDER_SOURCE_MARKET_RECORDED" if status == "RECORDED"
                else "SPIDER_SOURCE_MARKET_DUPLICATE",
                operation_id=cast(str, proved["operation_id"]),
                sequence=cast(int, proved["sequence"]),
                receipt_disposition=cast(str, proved["disposition"]),
                checkpoint_sha256=cast(str, proved["checkpoint_sha256"]),
            )
        return self._source_market_result(
            scope, status, reason, operation_id=operation_id, sequence=sequence
        )

    @staticmethod
    def _source_market_result(
        scope: RuntimeScopeKey | None,
        disposition: str,
        reason: str,
        *,
        operation_id: str | None = None,
        sequence: int | None = None,
        receipt_disposition: str | None = None,
        checkpoint_sha256: str | None = None,
    ) -> CommandResult:
        data = {
            "scope": None if scope is None else _scope(scope),
            "disposition": disposition,
            "operation_id": operation_id,
            "reason_code": reason,
            "delivery_sequence": sequence,
            "delivery_disposition": receipt_disposition,
            "checkpoint_sha256": checkpoint_sha256,
        }
        return CommandResult(
            success=disposition in {"ACCEPTED", "DUPLICATE"},
            message=reason,
            data=data,
            completed=disposition in {"REJECTED", "CONFLICT"},
        )

    def _record_source_market(
        self, scope: RuntimeScopeKey, snapshot_id: str, raw: bytes
    ) -> tuple[str, str, str | None, int | None]:
        with self._sessions() as session:
            if (
                not isinstance(session, Session)
                or session.in_transaction()
                or not session.is_active
                or session.get_bind().dialect.name != "postgresql"
            ):
                return "REJECTED", "SPIDER_SOURCE_MARKET_SESSION_UNAVAILABLE", None, None
            with session.begin():
                self._configure_writer(session)
                runtime = session.execute(
                    select(SpiderRuntimeScope).where(
                        SpiderRuntimeScope.environment == scope.environment,
                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                    ).with_for_update()
                ).scalar_one_or_none()
                if runtime is None or runtime.current_operation_id is None:
                    return "REJECTED", "SPIDER_SOURCE_MARKET_RUNTIME_UNAVAILABLE", None, None
                operation_id = runtime.current_operation_id
                plan = session.execute(
                    select(SpiderRuntimePlan).where(
                        SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                        SpiderRuntimePlan.operation_id == operation_id,
                    )
                ).scalar_one_or_none()
                if (
                    plan is None
                    or plan.plan_status not in {"INCOMPLETE", "COMPLETE"}
                    or plan.generation != runtime.generation
                    or plan.configuration_sha256 != runtime.configuration_sha256
                ):
                    return "REJECTED", "SPIDER_SOURCE_MARKET_INSTALL_UNAVAILABLE", operation_id, None
                existing = session.execute(
                    select(SpiderRuntimeDelivery).where(
                        SpiderRuntimeDelivery.environment == scope.environment,
                        SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                        SpiderRuntimeDelivery.delivery_id == snapshot_id,
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    if (
                        existing.kind == "SOURCE_MARKET"
                        and existing.source_payload == raw
                        and existing.content_sha256 == _digest_bytes(raw)
                    ):
                        return "DUPLICATE", "SPIDER_SOURCE_MARKET_DUPLICATE", operation_id, existing.sequence
                    return "CONFLICT", "SPIDER_SOURCE_MARKET_ID_CONFLICT", operation_id, None
                if self._refresh_initial_place_projection(
                    session, scope, runtime, plan, persist=False
                ) is None:
                    return "REJECTED", "SPIDER_SOURCE_MARKET_INSTALL_UNAVAILABLE", operation_id, None
                products, evidence_sha256, _source_sha256 = self._source_market_basis(plan)
                try:
                    observation = decode_source_market_observation(
                        raw,
                        scope=_scope(scope),
                        generation=runtime.generation,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        evidence_sha256=evidence_sha256,
                        ordered_products=products,
                        build_checkpoint_bytes=plan.original_build_checkpoint,
                    )
                except ValueError:
                    return "REJECTED", "SPIDER_SOURCE_MARKET_INVALID", operation_id, None
                if observation.snapshot_id != snapshot_id:
                    return "REJECTED", "SPIDER_SOURCE_MARKET_ID_INVALID", operation_id, None
                if runtime.last_tape_sequence < runtime.last_received_sequence:
                    return "REJECTED", "SPIDER_SOURCE_MARKET_FRONTIER_INVALID", operation_id, None
                sequence = runtime.last_received_sequence + 1
                tape_ordinal = runtime.last_tape_sequence + 1
                if not 1 <= sequence < 2**63 or not 1 <= tape_ordinal < 2**63:
                    return "REJECTED", "SPIDER_SOURCE_MARKET_FRONTIER_EXHAUSTED", operation_id, None
                runtime.last_received_sequence = sequence
                runtime.last_tape_sequence = tape_ordinal
                session.add(SpiderRuntimeDelivery(
                    environment=scope.environment,
                    execution_scope_id=scope.execution_scope_id,
                    strategy_id=scope.strategy_id,
                    sequence=sequence,
                    delivery_id=snapshot_id,
                    content_sha256=_digest_bytes(raw),
                    kind="SOURCE_MARKET",
                    local_order_id=None,
                    trade_id=None,
                    action_id=None,
                    generation=runtime.generation,
                    configuration_sha256=runtime.configuration_sha256,
                    role=None,
                    source_payload=raw,
                    visible_at=session.execute(
                        select(func.date_trunc("milliseconds", func.clock_timestamp()))
                    ).scalar_one(),
                    elapsed_ms=None,
                    disposition="PENDING",
                    suppression_reason=None,
                ))
                session.add(SpiderRuntimeTapeIndex(
                    environment=scope.environment,
                    execution_scope_id=scope.execution_scope_id,
                    strategy_id=scope.strategy_id,
                    ordinal=tape_ordinal,
                    kind="DELIVERY",
                    delivery_sequence=sequence,
                    transmission_group_id=None,
                ))
                session.flush()
                return "RECORDED", "SPIDER_SOURCE_MARKET_RECEIVED", operation_id, sequence

    def _source_market_receipt_readback(
        self, scope: RuntimeScopeKey, snapshot_id: str, raw: bytes
    ) -> dict[str, object] | None:
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
                    row = session.execute(
                        select(SpiderRuntimeDelivery).where(
                            SpiderRuntimeDelivery.environment == scope.environment,
                            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                            SpiderRuntimeDelivery.delivery_id == snapshot_id,
                        )
                    ).scalar_one_or_none()
                    runtime = session.execute(
                        select(SpiderRuntimeScope).where(
                            SpiderRuntimeScope.environment == scope.environment,
                            SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeScope.strategy_id == scope.strategy_id,
                        )
                    ).scalar_one_or_none()
                    if (
                        row is None or runtime is None
                        or row.kind != "SOURCE_MARKET"
                        or row.source_payload != raw
                        or row.content_sha256 != _digest_bytes(raw)
                        or row.delivery_id != snapshot_id
                        or row.action_id is not None or row.local_order_id is not None
                        or row.trade_id is not None or row.role is not None
                        or row.generation != runtime.generation
                        or row.configuration_sha256 != runtime.configuration_sha256
                        or not validate_tape_index(session, runtime)
                    ):
                        return None
                    operation_id = runtime.current_operation_id
                    if operation_id is None:
                        return None
                    return {
                        "operation_id": operation_id,
                        "sequence": row.sequence,
                        "disposition": row.disposition,
                        "checkpoint_sha256": runtime.source_checkpoint_sha256,
                    }
        except Exception:
            return None

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
                        transport_payload = canonical_bytes({
                            "schema_version": "SPIDER_TRANSPORT_RESULT_DELIVERY_V1",
                            "scope": _scope(scope),
                            "transmission_group_id": transmission_group_id,
                            "request_sha256": transmission.request_sha256,
                            "response_sha256": transmission.response_sha256,
                            "response_failure": response_failure,
                        })
                        transport_identity = {
                            "domain": "SPIDER_TRANSPORT_RESULT_DELIVERY_ID_V1",
                            "scope": _scope(scope),
                            "transmission_group_id": transmission_group_id,
                        }
                        delivery_id = _digest(transport_identity)
                        existing_receipt = session.execute(
                            select(SpiderRuntimeDelivery).where(
                                SpiderRuntimeDelivery.environment == scope.environment,
                                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                                SpiderRuntimeDelivery.delivery_id == delivery_id,
                            )
                        ).scalar_one_or_none()
                        if existing_receipt is not None:
                            raise RuntimeError("SPIDER_TRANSPORT_RESULT_RECEIPT_CONFLICT")
                        next_sequence = runtime.last_received_sequence + 1
                        next_tape_sequence = runtime.last_tape_sequence + 1
                        if (
                            not 1 <= next_sequence <= 2**63 - 1
                            or not 1 <= next_tape_sequence <= 2**63 - 1
                        ):
                            raise RuntimeError("SPIDER_TRANSPORT_RESULT_SEQUENCE_EXHAUSTED")
                        session.add(SpiderRuntimeDelivery(
                            environment=scope.environment,
                            execution_scope_id=scope.execution_scope_id,
                            strategy_id=scope.strategy_id,
                            sequence=next_sequence,
                            delivery_id=delivery_id,
                            content_sha256=_digest_bytes(transport_payload),
                            kind="TRANSPORT_RESULT",
                            local_order_id=None,
                            trade_id=None,
                            action_id=None,
                            generation=transmission.generation,
                            configuration_sha256=transmission.configuration_sha256,
                            role=None,
                            source_payload=transport_payload,
                            visible_at=cast(Any, _database_millisecond_time(session)),
                            elapsed_ms=None,
                            disposition="PENDING",
                            suppression_reason=None,
                        ))
                        session.add(SpiderRuntimeTapeIndex(
                            environment=scope.environment,
                            execution_scope_id=scope.execution_scope_id,
                            strategy_id=scope.strategy_id,
                            ordinal=next_tape_sequence,
                            kind="DELIVERY",
                            delivery_sequence=next_sequence,
                            transmission_group_id=None,
                        ))
                        runtime.last_received_sequence = next_sequence
                        runtime.last_tape_sequence = next_tape_sequence
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
        self._drain_stopped_scope(scope)
        self._drain_source_market_scope(scope)

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

    def _drain_source_market_scope(self, scope: RuntimeScopeKey) -> None:
        """Drain only a finite, contiguous RUN prefix of source-market rows."""
        try:
            with self._sessions() as session:
                if (
                    not isinstance(session, Session)
                    or session.in_transaction()
                    or not session.is_active
                    or session.get_bind().dialect.name != "postgresql"
                ):
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
                return
            operation_id, control_mode, consumed, frontier = snapshot
            if (
                type(operation_id) is not str
                or control_mode != "RUN"
                or type(consumed) is not int
                or type(frontier) is not int
                or consumed >= frontier
            ):
                return
            for sequence in range(consumed + 1, frontier + 1):
                if self._lifecycle_lock is None or self._order_event_lock is None:
                    self._warn_source_market_unavailable(scope, sequence, "LOCK_UNAVAILABLE")
                    return
                with self._lifecycle_lock(scope.strategy_id):
                    with cast(ContextManager[object], self._order_event_lock):
                        kind = self._pending_delivery_kind(scope, sequence)
                        if kind == "SOURCE_MARKET":
                            outcome = self._consume_source_market(
                                scope, cast(str, operation_id), sequence
                            )
                        elif kind in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}:
                            outcome = self._consume_normal_delivery(
                                scope, cast(str, operation_id), sequence
                            )
                        else:
                            outcome = "BLOCKED"
                if outcome == "BLOCKED":
                    return
                if outcome != "CONSUMED":
                    self._warn_source_market_unavailable(
                        scope, sequence, "SOURCE_MARKET_CONSUMPTION_UNAVAILABLE"
                    )
                    return
                readback = self.readback(scope, cast(str, operation_id))
                if (
                    readback is None
                    or readback.get("consumed_sequence") != sequence
                ):
                    self._warn_source_market_unavailable(
                        scope, sequence, "SOURCE_MARKET_COMMIT_UNPROVED"
                    )
                    return
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider source-market backlog could not be consumed for %s",
                scope.strategy_id,
            )

    @staticmethod
    def _warn_source_market_unavailable(
        scope: RuntimeScopeKey, sequence: int, reason: str
    ) -> None:
        logging.getLogger(__name__).warning(
            "SPIDER_SOURCE_MARKET_UNAVAILABLE strategy=%s sequence=%s reason=%s",
            scope.strategy_id,
            sequence,
            reason,
        )

    def _pending_delivery_kind(self, scope: RuntimeScopeKey, sequence: int) -> str | None:
        try:
            with self._sessions() as session:
                if (
                    not isinstance(session, Session)
                    or session.in_transaction()
                    or not session.is_active
                ):
                    return None
                return session.execute(
                    select(SpiderRuntimeDelivery.kind).where(
                        SpiderRuntimeDelivery.environment == scope.environment,
                        SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                        SpiderRuntimeDelivery.sequence == sequence,
                    )
                ).scalar_one_or_none()
        except Exception:
            return None

    def _consume_normal_delivery(
        self, scope: RuntimeScopeKey, operation_id: str, sequence: int
    ) -> str:
        """Apply one proved source callback and immutable intents in one TX B."""
        try:
            with self._sessions() as session:
                if (
                    not isinstance(session, Session)
                    or session.in_transaction()
                    or not session.is_active
                    or session.get_bind().dialect.name != "postgresql"
                ):
                    return "UNAVAILABLE"
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
                        or runtime.current_operation_id != operation_id
                        or runtime.control_mode != "RUN"
                        or runtime.last_consumed_sequence != sequence - 1
                        or runtime.last_received_sequence < sequence
                    ):
                        return "BLOCKED"
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
                        or delivery.kind not in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}
                        or delivery.disposition != "PENDING"
                        or delivery.suppression_reason is not None
                        or delivery.generation != runtime.generation
                        or delivery.configuration_sha256 != runtime.configuration_sha256
                        or type(delivery.source_payload) is not bytes
                        or _digest_bytes(delivery.source_payload) != delivery.content_sha256
                    ):
                        return "UNAVAILABLE"
                    plan = session.execute(
                        select(SpiderRuntimePlan).where(
                            SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                            SpiderRuntimePlan.operation_id == operation_id,
                        ).with_for_update()
                    ).scalar_one_or_none()
                    if plan is None or self._refresh_initial_place_projection(
                        session, scope, runtime, plan, persist=False
                    ) is None:
                        return "UNAVAILABLE"
                    products, evidence_sha256, source_sha256 = self._source_market_basis(plan)
                    before_checkpoint = bytes(runtime.source_checkpoint)
                    before_hash = _digest_bytes(before_checkpoint)
                    policy = restore_source_checkpoint(
                        before_checkpoint,
                        ordered_products=products,
                        policy_id=ORIGINAL_MODEL_POLICY_ID,
                        policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                        policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                        source_sha256=source_sha256,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        generation=runtime.generation,
                    )
                    source_input, full_key, trigger_kind = self._normal_source_input(
                        session, scope, runtime, plan, delivery, evidence_sha256,
                        products, source_sha256,
                    )
                    if source_input is None:
                        return "UNAVAILABLE"
                    if delivery.decision_payload is not None:
                        # A saved policy failure is observable but not a retry token.
                        return "BLOCKED"
                    decision_full_key = full_key
                    if (
                        source_input.get("type") == "ORDERS"
                        and cast(dict[str, object], source_input.get("order", {})).get("state") == "filled"
                    ):
                        source_order = cast(dict[str, object], source_input["order"])
                        source_row = policy.row(cast(str, source_order["instId"]))
                        if not policy.running or source_row is None or not policy.active(source_row):
                            decision_full_key = None
                    try:
                        if source_input.get("type") != "NO_SOURCE":
                            invoke_saved_source_callback(policy, source_input)
                    except SourceClockUnavailable:
                        logging.getLogger(__name__).warning(
                            "SPIDER_SOURCE_CLOCK_UNAVAILABLE strategy=%s sequence=%s",
                            scope.strategy_id, sequence,
                        )
                        return "BLOCKED"
                    except Exception:
                        failed_payload = canonical_bytes(self._normal_decision_payload(
                            scope, runtime, delivery, before_hash, before_hash,
                            source_input, [], full_key, "POLICY_FAILED",
                            "SOURCE_POLICY_FAILED", list(policy.events), trigger_kind,
                        ))
                        delivery.decision_payload = failed_payload
                        delivery.decision_sha256 = _digest_bytes(failed_payload)
                        session.flush()
                        logging.getLogger(__name__).warning(
                            "SPIDER_SOURCE_POLICY_FAILED strategy=%s sequence=%s reason=SOURCE_POLICY_FAILED",
                            scope.strategy_id, sequence,
                        )
                        return "BLOCKED"
                    candidates, staged_actions = self._stage_normal_candidates(
                        session, scope, runtime, plan, delivery, policy.events
                    )
                    after_checkpoint = encode_source_checkpoint(
                        policy,
                        ordered_products=products,
                        policy_id=ORIGINAL_MODEL_POLICY_ID,
                        policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                        policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                        source_sha256=source_sha256,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        generation=runtime.generation,
                    )
                    after_hash = _digest_bytes(after_checkpoint)
                    decision = self._normal_decision_payload(
                        scope, runtime, delivery, before_hash, after_hash,
                        source_input, candidates, decision_full_key, "CONSUMED", None,
                        list(policy.events), trigger_kind,
                    )
                    decision_bytes = canonical_bytes(decision)
                    if not 1 <= len(decision_bytes) <= 1_048_576:
                        raise RuntimeError("SPIDER_NORMAL_DECISION_SIZE_INVALID")
                    delivery.decision_payload = decision_bytes
                    delivery.decision_sha256 = _digest_bytes(decision_bytes)
                    delivery.full_callback_key = decision_full_key
                    runtime.source_checkpoint = after_checkpoint
                    runtime.source_checkpoint_sha256 = after_hash
                    delivery.disposition = "CONSUMED"
                    runtime.last_consumed_sequence = sequence
                    if staged_actions:
                        session.add_all(staged_actions)
                    session.flush()
            return "CONSUMED"
        except Exception:
            if self._normal_delivery_commit_proven(scope, operation_id, sequence):
                return "CONSUMED"
            logging.getLogger(__name__).exception(
                "SPIDER_NORMAL_DELIVERY_UNAVAILABLE strategy=%s sequence=%s",
                scope.strategy_id, sequence,
            )
            return "UNAVAILABLE"

    def _normal_delivery_commit_proven(
        self, scope: RuntimeScopeKey, operation_id: str, sequence: int
    ) -> bool:
        """Resolve one TX B commit/close uncertainty by independent readback."""
        try:
            proof = self.readback(scope, operation_id)
            if (
                proof is None
                or type(proof.get("consumed_sequence")) is not int
                or cast(int, proof["consumed_sequence"]) < sequence
            ):
                return False
            with self._sessions() as session:
                if not isinstance(session, Session) or session.in_transaction():
                    return False
                delivery = session.execute(
                    select(SpiderRuntimeDelivery).where(
                        SpiderRuntimeDelivery.environment == scope.environment,
                        SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                        SpiderRuntimeDelivery.sequence == sequence,
                    )
                ).scalar_one_or_none()
                runtime = session.execute(
                    select(SpiderRuntimeScope).where(
                        SpiderRuntimeScope.environment == scope.environment,
                        SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeScope.strategy_id == scope.strategy_id,
                    )
                ).scalar_one_or_none()
                return bool(
                    delivery is not None
                    and runtime is not None
                    and runtime.current_operation_id == operation_id
                    and runtime.last_consumed_sequence >= sequence
                    and delivery.disposition == "CONSUMED"
                    and delivery.suppression_reason is None
                    and type(delivery.decision_payload) is bytes
                    and delivery.decision_sha256 == _digest_bytes(delivery.decision_payload)
                )
        except Exception:
            return False

    def _normal_source_input(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        delivery: SpiderRuntimeDelivery,
        evidence_sha256: str,
        products: tuple[str, ...],
        source_sha256: str,
    ) -> tuple[dict[str, object] | None, str | None, str]:
        """Translate one original receipt into the exact saved Policy call."""
        raw = decode_canonical(bytes(delivery.source_payload))
        if type(raw) is not dict:
            return None, None, delivery.kind
        record = cast(dict[str, object], raw)
        if delivery.kind == "TRANSPORT_RESULT":
            required = {
                "schema_version", "scope", "transmission_group_id",
                "request_sha256", "response_sha256", "response_failure",
            }
            if set(record) != required or record.get("schema_version") != "SPIDER_TRANSPORT_RESULT_DELIVERY_V1":
                return None, None, delivery.kind
            group_id = record.get("transmission_group_id")
            transmission = session.execute(
                select(SpiderRuntimeTransmission).where(
                    SpiderRuntimeTransmission.transmission_group_id == group_id,
                    SpiderRuntimeTransmission.environment == scope.environment,
                    SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
                )
            ).scalar_one_or_none()
            if (
                transmission is None
                or transmission.operation != "PLACE"
                or transmission.request_sha256 != record.get("request_sha256")
                or transmission.response_sha256 != record.get("response_sha256")
                or transmission.response_failure != record.get("response_failure")
                or transmission.response_recorded_at is None
            ):
                return None, None, delivery.kind
            request = _decode_transport_request(bytes(transmission.request_payload))
            if request is None or request.get("scope") != _scope(scope):
                return None, None, delivery.kind
            classified = (
                None if transmission.response_payload is None
                else self._validated_transport_outcomes(
                    bytes(transmission.request_payload), bytes(transmission.response_payload)
                )
            )
            entries: list[dict[str, object]] = []
            if classified is not None:
                request_members = cast(list[dict[str, object]], request["members"])
                request_action_ids = [
                    cast(str, member["action_id"]) for member in request_members
                ]
                action_rows = session.execute(
                    select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
                    .join(
                        SpiderRuntimeBinding,
                        SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id,
                    )
                    .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
                    .where(
                        SpiderRuntimeAction.plan_id == transmission.plan_id,
                        SpiderRuntimeAction.action_id.in_(request_action_ids),
                    )
                ).all()
                action_map = {
                    action.action_id: (action, binding, order)
                    for action, binding, order in action_rows
                }
                ordered_action_rows = [
                    action_map.get(action_id) for action_id in request_action_ids
                ]
                if (
                    any(row is None for row in ordered_action_rows)
                    or not _valid_initial_place_request(
                        request,
                        scope,
                        scope.execution_scope_id,
                        transmission.transmission_group_id,
                        cast(
                            list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]],
                            ordered_action_rows,
                        ),
                        require_unsent=False,
                    )
                ):
                    return None, None, delivery.kind
                verified_action_map = {
                    action.action_id: (action, binding)
                    for action, binding, _order in cast(
                        list[tuple[SpiderRuntimeAction, SpiderRuntimeBinding, Order]],
                        ordered_action_rows,
                    )
                }
                response_obj = (
                    {} if transmission.response_payload is None
                    else cast(dict[str, object], decode_canonical(bytes(transmission.response_payload)))
                )
                response_rows = response_obj.get("data")
                response_by_wire: dict[str, dict[str, object]] = {}
                if type(response_rows) is list:
                    for value in cast(list[object], response_rows):
                        if type(value) is dict and type(cast(dict[str, object], value).get("clOrdId")) is str:
                            response_by_wire[cast(str, cast(dict[str, object], value)["clOrdId"])] = cast(dict[str, object], value)
                for member in request_members:
                    action_pair = verified_action_map.get(cast(str, member["action_id"]))
                    outcome = classified.get(cast(str, member["action_id"]))
                    if action_pair is None or outcome is None or outcome[0] not in {"ACCEPTED", "REJECTED"}:
                        continue
                    action, binding = action_pair
                    source_order_value = decode_canonical(bytes(action.original_payload))
                    if type(source_order_value) is not dict:
                        return None, None, delivery.kind
                    source_order = cast(dict[str, object], source_order_value)
                    # REST is an acknowledgement, never a status authority. Once the
                    # local order has reached an economic terminal state, or its
                    # source full callback has been claimed, a delayed response must
                    # not move the saved Policy cache backwards to sent/live.
                    local_order = session.execute(
                        select(Order).where(Order.id == action.local_order_id)
                    ).scalar_one_or_none()
                    if local_order is None:
                        return None, None, delivery.kind
                    member_full_key = full_callback_identity(
                        _scope(scope), local_order.id, action.generation
                    )
                    prior_full = session.execute(
                        select(SpiderRuntimeDelivery.sequence).where(
                            SpiderRuntimeDelivery.environment == scope.environment,
                            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                            SpiderRuntimeDelivery.full_callback_key == member_full_key,
                            SpiderRuntimeDelivery.sequence < delivery.sequence,
                        ).limit(1)
                    ).scalar_one_or_none()
                    if prior_full is not None:
                        continue
                    prior_financial = session.execute(
                        select(SpiderRuntimeDelivery).where(
                            SpiderRuntimeDelivery.environment == scope.environment,
                            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                            SpiderRuntimeDelivery.local_order_id == local_order.id,
                            SpiderRuntimeDelivery.kind.in_(("FILL", "ORDER_STATE")),
                            SpiderRuntimeDelivery.sequence < delivery.sequence,
                        ).order_by(SpiderRuntimeDelivery.sequence)
                    ).scalars().all()
                    if prior_financial:
                        last_financial = prior_financial[-1]
                        try:
                            receipt = cast(
                                dict[str, object],
                                decode_canonical(bytes(last_financial.source_payload)),
                            )
                            projection = cast(dict[str, object], receipt["after"])
                            cumulative_quantity = Decimal(
                                cast(str, projection["filled_quantity"])
                            )
                        except (InvalidOperation, KeyError, TypeError, ValueError):
                            return None, None, delivery.kind
                        if not validate_order_receipt_chain(
                            session,
                            scope=_scope(scope),
                            action=action,
                            binding=binding,
                            order_id=local_order.id,
                            current_projection=projection,
                            through_sequence=last_financial.sequence,
                        ):
                            return None, None, delivery.kind
                        prior_fill_count = sum(
                            row.kind == "FILL" for row in prior_financial
                        )
                        prior_state = classify_source_order_transition(
                            projection.get("status"),
                            original_quantity=_positive_decimal(source_order["sz"]),
                            cumulative_quantity=cumulative_quantity,
                            trade_chain_proved=prior_fill_count > 0,
                        )
                        if prior_state.state is None:
                            return None, None, delivery.kind
                        if prior_state.state in {
                            "filled", "canceled", "failed", "economic_terminal"
                        }:
                            continue
                    alias = source_alias(binding.source_client_order_id)
                    provider_row = response_by_wire.get(cast(str, member["wire_clOrdId"]), {})
                    code = "0" if outcome[0] == "ACCEPTED" else cast(str, outcome[1])
                    message = provider_row.get("sMsg")
                    entries.append({
                        "clOrdId": alias,
                        "ordId": alias,
                        "sCode": code,
                        "sMsg": message if type(message) is str else None,
                        "instId": binding.source_product_id,
                        "side": source_order["side"],
                        "px": source_order["px"],
                        "sz": source_order["sz"],
                    })
            return {"type": "REST_ACK", "operation": "send", "entries": entries}, None, delivery.kind

        if delivery.kind not in {"FILL", "ORDER_STATE"} or delivery.action_id is None or delivery.local_order_id is None:
            return None, None, delivery.kind
        action = session.execute(
            select(SpiderRuntimeAction).where(
                SpiderRuntimeAction.action_id == delivery.action_id,
                SpiderRuntimeAction.environment == scope.environment,
                SpiderRuntimeAction.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
            )
        ).scalar_one_or_none()
        binding = session.execute(
            select(SpiderRuntimeBinding).where(
                SpiderRuntimeBinding.action_id == delivery.action_id,
                SpiderRuntimeBinding.environment == scope.environment,
                SpiderRuntimeBinding.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeBinding.strategy_id == scope.strategy_id,
            )
        ).scalar_one_or_none()
        order = session.execute(
            select(Order).where(Order.id == delivery.local_order_id)
        ).scalar_one_or_none()
        if (
            action is None or binding is None or order is None
            or action.operation != "PLACE"
            or action.role != "GRID"
            or action.trigger_delivery_id is not None and action.plan_id != plan.plan_id
            or action.local_order_id != order.id
            or binding.source_product_id != action.product_id
            or order.product_id != binding.canonical_product_id
            or order.side not in {"buy", "sell"}
            or delivery.generation != action.generation
            or delivery.configuration_sha256 != action.configuration_sha256
            or record.get("schema_version") != "SPIDER_FINANCIAL_RECEIPT_V1"
            or record.get("scope") != _scope(scope)
            or record.get("binding_resolution") != "BOUND"
            or record.get("reason_code") is not None
        ):
            return None, None, delivery.kind
        current_projection = record.get("after")
        if type(current_projection) is not dict:
            return None, None, delivery.kind
        if not validate_order_receipt_chain(
            session,
            scope=_scope(scope),
            action=action,
            binding=binding,
            order_id=order.id,
            current_projection=cast(dict[str, object], current_projection),
            through_sequence=delivery.sequence,
        ):
            return None, None, delivery.kind
        source_order_value = decode_canonical(bytes(action.original_payload))
        if type(source_order_value) is not dict:
            return None, None, delivery.kind
        original = cast(dict[str, object], source_order_value)
        after_values = cast(dict[str, object], current_projection)
        after_status = after_values.get("status")
        try:
            original_quantity = _positive_decimal(original["sz"])
            cumulative = Decimal(cast(str, after_values["filled_quantity"]))
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return None, None, delivery.kind
        filled_trades = session.execute(
            select(SpiderRuntimeDelivery.sequence, Trade.price)
            .join(Trade, Trade.id == SpiderRuntimeDelivery.trade_id)
            .where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == order.id,
                SpiderRuntimeDelivery.kind == "FILL",
                SpiderRuntimeDelivery.sequence <= delivery.sequence,
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).all()
        trade_chain_proved = (
            bool(filled_trades)
            and all(type(price) is Decimal and price.is_finite() and price > 0 for _seq, price in filled_trades)
        )
        transition = classify_source_order_transition(
            after_status,
            original_quantity=original_quantity,
            cumulative_quantity=cumulative,
            trade_chain_proved=trade_chain_proved,
        )
        if transition.state is None:
            return None, None, delivery.kind
        key = None
        source_input: dict[str, object]
        already_claimed = False
        if transition.full_callback:
            key = full_callback_identity(_scope(scope), order.id, action.generation)
            already_claimed = session.execute(
                select(SpiderRuntimeDelivery.sequence).where(
                    SpiderRuntimeDelivery.environment == scope.environment,
                    SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                    SpiderRuntimeDelivery.full_callback_key == key,
                    SpiderRuntimeDelivery.sequence < delivery.sequence,
                ).limit(1)
            ).scalar_one_or_none() is not None
        if transition.state == "economic_terminal":
            source_input = {"type": "NO_SOURCE", "reason_code": "ECONOMIC_TERMINAL"}
        elif already_claimed:
            source_input = {"type": "NO_SOURCE", "reason_code": "FULL_ALREADY_CONSUMED"}
            key = None
        else:
            state = {
                "new": "new", "sent": "sent", "live": "live",
                "partially_filled": "partially_filled", "filled": "filled",
                "canceled": "canceled", "failed": "failed",
            }[transition.state]
            last_price: Decimal | None = None
            if transition.full_callback:
                last_price = cast(Decimal, filled_trades[-1][1])
            source_input = build_orders_source_input(
                source_alias_id=binding.source_client_order_id,
                source_product_id=binding.source_product_id,
                side=cast(str, original["side"]),
                limit_price=_positive_decimal(original["px"]),
                original_quantity=original_quantity,
                cumulative_quantity=cumulative,
                state=state,
                fill_price=last_price,
                created_at_ms=order.timestamp,
            )
        return source_input, key, delivery.kind

    @staticmethod
    def _normal_decision_payload(
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        delivery: SpiderRuntimeDelivery,
        before_hash: str,
        after_hash: str,
        source_input: dict[str, object],
        candidates: list[dict[str, object]],
        full_key: str | None,
        outcome: str,
        reason_code: str | None,
        events: list[dict[str, object]],
        trigger_kind: str,
    ) -> dict[str, object]:
        return {
            "schema_version": "SPIDER_SOURCE_DECISION_V1",
            "scope": _scope(scope),
            "generation": runtime.generation,
            "configuration_sha256": runtime.configuration_sha256,
            "trigger_delivery_id": delivery.delivery_id,
            "trigger_kind": trigger_kind,
            "before_checkpoint_sha256": before_hash,
            "after_checkpoint_sha256": after_hash,
            "source_alias_policy_id": "SPIDER_SOURCE_LOCAL_ALIAS_V1",
            "source_input": source_input,
            "events": events,
            "candidates": candidates,
            "full_callback_key": full_key,
            "outcome": outcome,
            "reason_code": reason_code,
        }

    def _stage_normal_candidates(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        delivery: SpiderRuntimeDelivery,
        events: list[dict[str, object]],
    ) -> tuple[list[dict[str, object]], list[object]]:
        """Preserve source order while staging local PLACE and target CANCEL rows."""
        manifest = cast(dict[str, object], decode_canonical(plan.manifest_payload))
        source_map = cast(dict[str, object], manifest["source_product_map"])
        map_rows = cast(list[dict[str, object]], source_map["entries"])
        products = {
            cast(str, row["source_product_id"]): cast(
                str, cast(dict[str, object], row["instrument_spec"])["product_id"]
            ) for row in map_rows
        }
        previous_decisions = session.execute(
            select(SpiderRuntimeDelivery.decision_payload).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence < delivery.sequence,
                SpiderRuntimeDelivery.decision_payload.is_not(None),
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        last_ordinal = _initial_candidate_frontier(plan)
        for raw in previous_decisions:
            if type(raw) is not bytes:
                continue
            value = decode_canonical(bytes(raw))
            if type(value) is dict and type(cast(dict[str, object], value).get("candidates")) is list:
                for candidate in cast(list[dict[str, object]], cast(dict[str, object], value)["candidates"]):
                    ordinal = candidate.get("candidate_ordinal")
                    if type(ordinal) is int:
                        last_ordinal = max(last_ordinal, ordinal)
        candidates: list[dict[str, object]] = []
        staged: list[object] = []
        for event_index, event in enumerate(events):
            if type(event) is not dict or event.get("kind") not in {"send", "cancel"}:
                continue
            operation = "PLACE" if event.get("kind") == "send" else "CANCEL"
            orders = event.get("orders")
            if type(orders) is not list:
                raise ValueError("SPIDER_SOURCE_EVENT_INVALID")
            for member_index, order_value in enumerate(orders):
                if type(order_value) is not dict:
                    raise ValueError("SPIDER_SOURCE_EVENT_INVALID")
                source_payload = cast(dict[str, object], order_value)
                last_ordinal += 1
                original_payload = canonical_bytes(source_payload)
                payload_hash = _digest_bytes(original_payload)
                action_id = _digest({
                    "domain": "SPIDER_SOURCE_ACTION_V1",
                    "scope": _scope(scope),
                    "plan_id": plan.plan_id,
                    "generation": runtime.generation,
                    "configuration_sha256": runtime.configuration_sha256,
                    "trigger_delivery_id": delivery.delivery_id,
                    "candidate_ordinal": last_ordinal,
                    "operation": operation,
                    "original_payload_sha256": payload_hash,
                })
                classification = classify_source_candidate(
                    operation,
                    source_payload,
                    event_at_ms=event.get("at_ms"),
                    admitted_products=products,
                )
                target_rows: list[SpiderRuntimeAction] = []
                if classification.needs_cancel_target_lookup:
                    product = cast(str, source_payload.get("instId"))
                    alias_value = classification.cancel_alias
                    target_rows = list(session.execute(
                        select(SpiderRuntimeAction).join(
                            SpiderRuntimeBinding,
                            SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id,
                        ).where(
                            SpiderRuntimeAction.environment == scope.environment,
                            SpiderRuntimeAction.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeAction.strategy_id == scope.strategy_id,
                            SpiderRuntimeAction.plan_id == plan.plan_id,
                            SpiderRuntimeAction.operation == "PLACE",
                            SpiderRuntimeAction.generation == runtime.generation,
                            SpiderRuntimeAction.product_id == product,
                            SpiderRuntimeBinding.source_client_order_id == alias_value,
                        )
                    ).scalars().all())
                    classification = classify_source_candidate(
                        operation,
                        source_payload,
                        event_at_ms=event.get("at_ms"),
                        admitted_products=products,
                        cancel_targets=[
                            (target.action_id, target.local_order_id)
                            for target in target_rows
                        ],
                    )
                disposition = classification.disposition
                reason = classification.reason_code
                target_action: SpiderRuntimeAction | None = None
                local_order_id: str | None = classification.target_local_order_id
                if disposition == "PLANNED" and operation == "PLACE":
                    product = cast(str, source_payload["instId"])
                    price = cast(Decimal, classification.price)
                    quantity = cast(Decimal, classification.quantity)
                    side_value = classification.side
                    client_id = cast(str, classification.source_client_id)
                    at_ms = cast(int, classification.event_at_ms)
                    local_product = cast(str, classification.canonical_product_id)
                    position_side = SignalType.LONG if side_value == "buy" else SignalType.SHORT
                    order_side = OrderSide.BUY if side_value == "buy" else OrderSide.SELL
                    signal = Signal(
                        strategy_id=scope.strategy_id,
                        product_id=local_product,
                        timeframe="SPIDER_NORMAL_CALLBACK",
                        timestamp=at_ms,
                        type=position_side,
                        quantity=quantity,
                        price=price,
                    )
                    local_client_id = market_signal_client_order_id(
                        scope.strategy_id, local_product,
                        runtime.current_operation_id, at_ms,
                        "spidercallback", last_ordinal,
                    )
                    order = self._order_manager.prepare_order(
                        signal, order_side, "limit", quantity,
                        price=price, client_order_id=local_client_id,
                    )
                    order.id = str(uuid5(
                        NAMESPACE_URL,
                        "SPIDER_SOURCE_ORDER_V1\0" + action_id,
                    ))
                    binding_value = {
                        "schema_version": "SPIDER_OKX_CLIENT_BINDING_V1",
                        "environment": scope.environment,
                        "execution_scope_id": scope.execution_scope_id,
                        "strategy_id": scope.strategy_id,
                        "action_id": action_id,
                        "product_id": local_product,
                    }
                    binding_bytes = canonical_bytes(binding_value)
                    wire_client_id = "sp" + sha256(
                        b"SPIDER_OKX_CLIENT_BINDING_V1\0" + binding_bytes
                    ).hexdigest()[:30]
                    order.intent_payload = {
                        "action_id": action_id,
                        "source_order": source_payload,
                        "source_client_order_id": client_id,
                        "local_client_order_id": local_client_id,
                        "provider_wire_client_order_id": wire_client_id,
                        "provider_binding_bytes_hex": binding_bytes.hex(),
                        "provider_binding_sha256": _digest_bytes(binding_bytes),
                    }
                    repository = self._order_manager.repo
                    if not isinstance(repository, LiveOrderRepository):
                        raise ValueError("SPIDER_CALLBACK_REPOSITORY_UNAVAILABLE")
                    repository.stage_order_in_transaction(session, order)
                    local_order_id = order.id
                    action = SpiderRuntimeAction(
                        action_id=action_id,
                        plan_id=plan.plan_id,
                        environment=scope.environment,
                        execution_scope_id=scope.execution_scope_id,
                        strategy_id=scope.strategy_id,
                        trigger_delivery_id=delivery.delivery_id,
                        parent_action_id=None,
                        operation="PLACE",
                        generation=runtime.generation,
                        configuration_sha256=runtime.configuration_sha256,
                        role="GRID",
                        product_id=product,
                        local_order_id=order.id,
                        original_payload=original_payload,
                        payload_sha256=payload_hash,
                        candidate_ordinal=last_ordinal,
                        transmission_group=last_ordinal - member_index,
                        transmission_ordinal=member_index,
                        outcome_state="UNSENT",
                        outcome_reference=None,
                    )
                    binding = SpiderRuntimeBinding(
                        action_id=action_id,
                        environment=scope.environment,
                        execution_scope_id=scope.execution_scope_id,
                        strategy_id=scope.strategy_id,
                        local_order_id=order.id,
                        source_product_id=product,
                        canonical_product_id=local_product,
                        source_client_order_id=client_id,
                        product_map_sha256=_digest(source_map),
                    )
                    staged.extend((action, binding))
                elif disposition == "PLANNED" and operation == "CANCEL":
                    target_action = target_rows[0]
                    product = cast(str, source_payload["instId"])
                    staged.append(SpiderRuntimeAction(
                        action_id=action_id,
                        plan_id=plan.plan_id,
                        environment=scope.environment,
                        execution_scope_id=scope.execution_scope_id,
                        strategy_id=scope.strategy_id,
                        trigger_delivery_id=delivery.delivery_id,
                        parent_action_id=target_action.action_id,
                        operation="CANCEL",
                        generation=runtime.generation,
                        configuration_sha256=runtime.configuration_sha256,
                        role="GRID",
                        product_id=product,
                        local_order_id=target_action.local_order_id,
                        original_payload=original_payload,
                        payload_sha256=payload_hash,
                        candidate_ordinal=last_ordinal,
                        transmission_group=last_ordinal - member_index,
                        transmission_ordinal=member_index,
                        outcome_state="UNSENT",
                        outcome_reference=None,
                    ))
                candidates.append({
                    "candidate_ordinal": last_ordinal,
                    "operation": operation,
                    "action_id": action_id if disposition == "PLANNED" else None,
                    "payload_sha256": payload_hash,
                    "original_payload": source_payload,
                    "local_order_id": local_order_id,
                    "target_place_action_id": None if target_action is None else target_action.action_id,
                    "disposition": disposition,
                    "reason_code": reason,
                    "source_event_index": event_index,
                    "source_member_index": member_index,
                })
        return candidates, staged

    def _validate_normal_decision_history_step(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        delivery: SpiderRuntimeDelivery,
        expected_checkpoint: bytes,
        products: tuple[str, ...],
        evidence_sha256: str,
        source_sha256: str,
    ) -> bytes | None:
        """Purely re-evaluate one saved decision and prove its local actions."""
        if delivery.decision_payload is None:
            return expected_checkpoint if delivery.disposition == "PENDING" else None
        raw = bytes(delivery.decision_payload)
        if (
            not 1 <= len(raw) <= 1_048_576
            or delivery.decision_sha256 != _digest_bytes(raw)
        ):
            return None
        decoded = decode_canonical(raw)
        if type(decoded) is not dict or canonical_bytes(decoded) != raw:
            return None
        decision = cast(dict[str, object], decoded)
        required = {
            "schema_version", "scope", "generation", "configuration_sha256",
            "trigger_delivery_id", "trigger_kind", "before_checkpoint_sha256",
            "after_checkpoint_sha256", "source_alias_policy_id", "source_input",
            "events", "candidates", "full_callback_key", "outcome", "reason_code",
        }
        if (
            set(decision) != required
            or decision.get("schema_version") != "SPIDER_SOURCE_DECISION_V1"
            or decision.get("scope") != _scope(scope)
            or decision.get("generation") != runtime.generation
            or decision.get("configuration_sha256") != runtime.configuration_sha256
            or decision.get("trigger_delivery_id") != delivery.delivery_id
            or decision.get("trigger_kind") != delivery.kind
            or decision.get("source_alias_policy_id") != "SPIDER_SOURCE_LOCAL_ALIAS_V1"
            or decision.get("before_checkpoint_sha256") != _digest_bytes(expected_checkpoint)
            or type(decision.get("events")) is not list
            or type(decision.get("candidates")) is not list
            or type(decision.get("source_input")) is not dict
        ):
            return None
        try:
            expected_input, full_key, _trigger = self._normal_source_input(
                session, scope, runtime, plan, delivery, evidence_sha256,
                products, source_sha256,
            )
        except Exception:
            return None
        if expected_input is None or decision["source_input"] != expected_input:
            return None
        outcome = decision.get("outcome")
        candidate_values = cast(list[object], decision["candidates"])
        if outcome == "POLICY_FAILED":
            failure_disposition_proved = (
                delivery.disposition == "PENDING"
                and delivery.suppression_reason is None
            ) or (
                delivery.disposition == "SUPPRESSED"
                and delivery.suppression_reason == "STOP_CONTROL"
                and self._current_stop_proof(session, scope, runtime) is not None
            )
            if (
                not failure_disposition_proved
                or decision.get("reason_code") != "SOURCE_POLICY_FAILED"
                or decision.get("before_checkpoint_sha256")
                != _digest_bytes(expected_checkpoint)
                or decision.get("after_checkpoint_sha256")
                != _digest_bytes(expected_checkpoint)
                or candidate_values
                or delivery.full_callback_key is not None
                or decision.get("full_callback_key") != full_key
            ):
                return None
            if session.scalar(
                select(func.count()).select_from(SpiderRuntimeAction).where(
                    SpiderRuntimeAction.plan_id == plan.plan_id,
                    SpiderRuntimeAction.trigger_delivery_id == delivery.delivery_id,
                )
            ) != 0:
                return None
            policy = restore_source_checkpoint(
                expected_checkpoint,
                ordered_products=products,
                policy_id=ORIGINAL_MODEL_POLICY_ID,
                policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                source_sha256=source_sha256,
                confirmation_request_id=runtime.confirmation_request_id,
                configuration_sha256=runtime.configuration_sha256,
                generation=runtime.generation,
            )
            try:
                invoke_saved_source_callback(policy, expected_input)
                return None
            except SourceClockUnavailable:
                return None
            except Exception:
                if list(policy.events) != decision["events"]:
                    return None
                return expected_checkpoint
        if (
            outcome != "CONSUMED"
            or decision.get("reason_code") is not None
            or delivery.disposition != "CONSUMED"
            or delivery.suppression_reason is not None
        ):
            return None
        policy = restore_source_checkpoint(
            expected_checkpoint,
            ordered_products=products,
            policy_id=ORIGINAL_MODEL_POLICY_ID,
            policy_version=ORIGINAL_MODEL_POLICY_VERSION,
            policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
            source_sha256=source_sha256,
            confirmation_request_id=runtime.confirmation_request_id,
            configuration_sha256=runtime.configuration_sha256,
            generation=runtime.generation,
        )
        try:
            if expected_input.get("type") != "NO_SOURCE":
                invoke_saved_source_callback(policy, expected_input)
        except (SourceClockUnavailable, Exception):
            return None
        if list(policy.events) != decision["events"]:
            return None
        next_checkpoint = encode_source_checkpoint(
            policy,
            ordered_products=products,
            policy_id=ORIGINAL_MODEL_POLICY_ID,
            policy_version=ORIGINAL_MODEL_POLICY_VERSION,
            policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
            source_sha256=source_sha256,
            confirmation_request_id=runtime.confirmation_request_id,
            configuration_sha256=runtime.configuration_sha256,
            generation=runtime.generation,
        )
        if decision.get("after_checkpoint_sha256") != _digest_bytes(next_checkpoint):
            return None
        if full_key is None:
            if decision.get("full_callback_key") is not None or delivery.full_callback_key is not None:
                return None
        elif (
            decision.get("full_callback_key") != full_key
            or delivery.full_callback_key != full_key
        ):
            return None
        if not self._validate_normal_candidate_proof(
            session, scope, runtime, plan, delivery,
            cast(list[dict[str, object]], decision["events"]),
            candidate_values,
        ):
            return None
        return next_checkpoint

    def _validate_normal_candidate_proof(
        self,
        session: Session,
        scope: RuntimeScopeKey,
        runtime: SpiderRuntimeScope,
        plan: SpiderRuntimePlan,
        delivery: SpiderRuntimeDelivery,
        events: list[dict[str, object]],
        candidates_value: list[object],
    ) -> bool:
        expected_members: list[tuple[int, int, str, dict[str, object]]] = []
        for event_index, event in enumerate(events):
            if event.get("kind") not in {"send", "cancel"}:
                continue
            members = event.get("orders")
            if type(members) is not list:
                return False
            operation = "PLACE" if event["kind"] == "send" else "CANCEL"
            for member_index, member in enumerate(cast(list[object], members)):
                if type(member) is not dict:
                    return False
                expected_members.append((event_index, member_index, operation, cast(dict[str, object], member)))
        candidate_keys = {
            "candidate_ordinal", "operation", "action_id", "payload_sha256",
            "original_payload", "local_order_id", "target_place_action_id",
            "disposition", "reason_code", "source_event_index", "source_member_index",
        }
        if len(expected_members) != len(candidates_value):
            return False
        manifest = cast(dict[str, object], decode_canonical(plan.manifest_payload))
        source_map = cast(dict[str, object], manifest["source_product_map"])
        products = {
            cast(str, row["source_product_id"]): cast(
                str, cast(dict[str, object], row["instrument_spec"])["product_id"]
            )
            for row in cast(list[dict[str, object]], source_map["entries"])
        }
        previous = session.execute(
            select(SpiderRuntimeDelivery.decision_payload).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence < delivery.sequence,
                SpiderRuntimeDelivery.decision_payload.is_not(None),
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        next_ordinal = _initial_candidate_frontier(plan) + 1
        for saved in previous:
            if type(saved) is not bytes:
                continue
            prior = decode_canonical(saved)
            if type(prior) is dict and type(cast(dict[str, object], prior).get("candidates")) is list:
                for candidate in cast(list[dict[str, object]], cast(dict[str, object], prior)["candidates"]):
                    value = candidate.get("candidate_ordinal")
                    if type(value) is int:
                        next_ordinal = max(next_ordinal, value + 1)
        planned_ids: list[str] = []
        for candidate_value, (event_index, member_index, operation, member) in zip(
            candidates_value, expected_members, strict=True
        ):
            if type(candidate_value) is not dict:
                return False
            candidate = cast(dict[str, object], candidate_value)
            if (
                set(candidate) != candidate_keys
                or candidate.get("candidate_ordinal") != next_ordinal
                or candidate.get("operation") != operation
                or candidate.get("original_payload") != member
                or candidate.get("source_event_index") != event_index
                or candidate.get("source_member_index") != member_index
            ):
                return False
            raw_payload = canonical_bytes(member)
            payload_hash = _digest_bytes(raw_payload)
            if candidate.get("payload_sha256") != payload_hash:
                return False
            group = next_ordinal - member_index
            product = member.get("instId")
            disposition = candidate.get("disposition")
            candidate_action_id = candidate.get("action_id")
            target_id = candidate.get("target_place_action_id")
            source_event = events[event_index]
            classification = classify_source_candidate(
                operation,
                member,
                event_at_ms=source_event.get("at_ms"),
                admitted_products=products,
            )
            target_matches: list[SpiderRuntimeAction] = []
            if classification.needs_cancel_target_lookup:
                target_matches = list(session.execute(
                    select(SpiderRuntimeAction).join(
                        SpiderRuntimeBinding,
                        SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id,
                    ).where(
                        SpiderRuntimeAction.environment == scope.environment,
                        SpiderRuntimeAction.execution_scope_id == scope.execution_scope_id,
                        SpiderRuntimeAction.strategy_id == scope.strategy_id,
                        SpiderRuntimeAction.plan_id == plan.plan_id,
                        SpiderRuntimeAction.operation == "PLACE",
                        SpiderRuntimeAction.generation == runtime.generation,
                        SpiderRuntimeAction.product_id == product,
                        SpiderRuntimeBinding.source_client_order_id
                        == classification.cancel_alias,
                    )
                ).scalars().all())
                classification = classify_source_candidate(
                    operation,
                    member,
                    event_at_ms=source_event.get("at_ms"),
                    admitted_products=products,
                    cancel_targets=[
                        (target.action_id, target.local_order_id)
                        for target in target_matches
                    ],
                )
            if (
                disposition != classification.disposition
                or candidate.get("reason_code") != classification.reason_code
                or target_id != classification.target_action_id
                or (
                    (disposition != "PLANNED" or operation == "CANCEL")
                    and candidate.get("local_order_id")
                    != classification.target_local_order_id
                )
            ):
                return False
            if disposition == "PLANNED":
                expected_action_id = _digest({
                    "domain": "SPIDER_SOURCE_ACTION_V1",
                    "scope": _scope(scope),
                    "plan_id": plan.plan_id,
                    "generation": runtime.generation,
                    "configuration_sha256": runtime.configuration_sha256,
                    "trigger_delivery_id": delivery.delivery_id,
                    "candidate_ordinal": next_ordinal,
                    "operation": operation,
                    "original_payload_sha256": payload_hash,
                })
                if candidate_action_id != expected_action_id:
                    return False
                action = session.execute(
                    select(SpiderRuntimeAction).where(
                        SpiderRuntimeAction.action_id == expected_action_id,
                        SpiderRuntimeAction.plan_id == plan.plan_id,
                        SpiderRuntimeAction.trigger_delivery_id == delivery.delivery_id,
                    )
                ).scalar_one_or_none()
                if (
                    action is None
                    or action.operation != operation
                    or action.candidate_ordinal != next_ordinal
                    or action.transmission_group != group
                    or action.transmission_ordinal != member_index
                    or action.original_payload != raw_payload
                    or action.payload_sha256 != payload_hash
                    or action.generation != runtime.generation
                    or action.configuration_sha256 != runtime.configuration_sha256
                    or action.role != "GRID"
                    or action.product_id != product
                    or candidate.get("local_order_id") != action.local_order_id
                ):
                    return False
                if operation == "PLACE":
                    binding = session.execute(
                        select(SpiderRuntimeBinding).where(
                            SpiderRuntimeBinding.action_id == action.action_id,
                            SpiderRuntimeBinding.local_order_id == action.local_order_id,
                        )
                    ).scalar_one_or_none()
                    order = session.execute(
                        select(Order).where(Order.id == action.local_order_id)
                    ).scalar_one_or_none()
                    if (
                        binding is None or order is None
                        or binding.source_client_order_id != member.get("clOrdId")
                        or binding.source_product_id != product
                        or binding.canonical_product_id != order.product_id
                        or order.id != str(uuid5(NAMESPACE_URL, "SPIDER_SOURCE_ORDER_V1\0" + action.action_id))
                        or order.side != member.get("side")
                        or order.price != _positive_decimal(member.get("px"))
                        or order.quantity != _positive_decimal(member.get("sz"))
                        or prove_spider_order_wire_binding(
                            scope.environment, scope.execution_scope_id,
                            scope.strategy_id, action, binding, order,
                        ) is None
                    ):
                        return False
                else:
                    if (
                        type(target_id) is not str
                        or action.parent_action_id != target_id
                        or action.local_order_id != candidate.get("local_order_id")
                        or candidate.get("reason_code") is not None
                    ):
                        return False
                    target = session.execute(
                        select(SpiderRuntimeAction).where(
                            SpiderRuntimeAction.action_id == target_id,
                            SpiderRuntimeAction.operation == "PLACE",
                            SpiderRuntimeAction.local_order_id == action.local_order_id,
                        )
                    ).scalar_one_or_none()
                    if target is None:
                        return False
                    binding = session.execute(
                        select(SpiderRuntimeBinding.action_id).where(
                            SpiderRuntimeBinding.action_id == action.action_id
                        )
                    ).scalar_one_or_none()
                    if binding is not None:
                        return False
                planned_ids.append(expected_action_id)
            elif disposition in {"FILTERED_SOURCE", "UNRESOLVED_TARGET"}:
                if candidate_action_id is not None:
                    return False
            else:
                return False
            next_ordinal += 1
        saved_ids = session.execute(
            select(SpiderRuntimeAction.action_id).where(
                SpiderRuntimeAction.plan_id == plan.plan_id,
                SpiderRuntimeAction.trigger_delivery_id == delivery.delivery_id,
            ).order_by(SpiderRuntimeAction.candidate_ordinal)
        ).scalars().all()
        return saved_ids == planned_ids

    def _consume_source_market(
        self, scope: RuntimeScopeKey, operation_id: str, sequence: int
    ) -> str:
        try:
            with self._sessions() as session:
                if (
                    not isinstance(session, Session)
                    or session.in_transaction()
                    or not session.is_active
                    or session.get_bind().dialect.name != "postgresql"
                ):
                    return "UNAVAILABLE"
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
                        or runtime.current_operation_id != operation_id
                        or runtime.control_mode != "RUN"
                        or runtime.last_consumed_sequence != sequence - 1
                        or runtime.last_received_sequence < sequence
                    ):
                        return "BLOCKED"
                    delivery = session.execute(
                        select(SpiderRuntimeDelivery).where(
                            SpiderRuntimeDelivery.environment == scope.environment,
                            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                            SpiderRuntimeDelivery.sequence == sequence,
                        ).with_for_update()
                    ).scalar_one_or_none()
                    if delivery is None or delivery.kind != "SOURCE_MARKET":
                        # A financial fact is never skipped by this source-only consumer.
                        return "BLOCKED"
                    if (
                        delivery.disposition != "PENDING"
                        or delivery.suppression_reason is not None
                        or delivery.generation != runtime.generation
                        or delivery.configuration_sha256 != runtime.configuration_sha256
                    ):
                        return "UNAVAILABLE"
                    plan = session.execute(
                        select(SpiderRuntimePlan).where(
                            SpiderRuntimePlan.plan_id == runtime.initial_plan_id,
                            SpiderRuntimePlan.operation_id == operation_id,
                        ).with_for_update()
                    ).scalar_one_or_none()
                    if plan is None:
                        return "UNAVAILABLE"
                    if self._refresh_initial_place_projection(
                        session, scope, runtime, plan, persist=False
                    ) is None:
                        return "UNAVAILABLE"
                    products, evidence_sha256, source_sha256 = self._source_market_basis(plan)
                    observation = decode_source_market_observation(
                        bytes(delivery.source_payload),
                        scope=_scope(scope),
                        generation=runtime.generation,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        evidence_sha256=evidence_sha256,
                        ordered_products=products,
                        build_checkpoint_bytes=plan.original_build_checkpoint,
                    )
                    if (
                        delivery.delivery_id != observation.snapshot_id
                        or delivery.content_sha256 != _digest_bytes(bytes(delivery.source_payload))
                    ):
                        return "UNAVAILABLE"
                    policy = restore_source_checkpoint(
                        bytes(runtime.source_checkpoint),
                        ordered_products=products,
                        policy_id=ORIGINAL_MODEL_POLICY_ID,
                        policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                        policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                        source_sha256=source_sha256,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        generation=runtime.generation,
                    )
                    policy.apply_market(observation.markets)
                    updated = encode_source_checkpoint(
                        policy,
                        ordered_products=products,
                        policy_id=ORIGINAL_MODEL_POLICY_ID,
                        policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                        policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                        source_sha256=source_sha256,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        generation=runtime.generation,
                    )
                    if updated != replace_checkpoint_markets(
                        bytes(runtime.source_checkpoint),
                        ordered_products=products,
                        markets=observation.markets,
                    ):
                        return "UNAVAILABLE"
                    runtime.source_checkpoint = updated
                    runtime.source_checkpoint_sha256 = _digest_bytes(updated)
                    delivery.disposition = "CONSUMED"
                    runtime.last_consumed_sequence = sequence
                    session.flush()
            return "CONSUMED"
        except Exception:
            logging.getLogger(__name__).exception(
                "Spider source-market transaction failed for %s sequence %s",
                scope.strategy_id,
                sequence,
            )
            return "UNAVAILABLE"

    @staticmethod
    def _source_market_basis(plan: SpiderRuntimePlan) -> tuple[tuple[str, ...], str, str]:
        manifest = cast(dict[str, object], decode_canonical(plan.manifest_payload))
        initial_manifest = cast(dict[str, object], manifest["initial_manifest"])
        product_map = cast(dict[str, object], manifest["source_product_map"])
        provenance = cast(dict[str, object], initial_manifest["historical_provenance"])
        products_value = product_map["ordered_products"]
        evidence_sha256 = initial_manifest["historical_input_artifact_sha256"]
        source_sha256 = provenance["source_sha256"]
        if (
            type(products_value) is not list
            or not products_value
            or any(type(item) is not str or not item for item in products_value)
            or type(evidence_sha256) is not str
            or type(source_sha256) is not str
        ):
            raise ValueError("SPIDER_SOURCE_MARKET_BASIS_UNAVAILABLE")
        return tuple(cast(list[str], products_value)), evidence_sha256, source_sha256

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
            original_build_checkpoint=prepared.build_checkpoint_bytes,
            original_build_checkpoint_sha256=prepared.build_checkpoint_sha256,
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
                        .where(
                            SpiderRuntimeAction.plan_id == plan.plan_id,
                            SpiderRuntimeAction.operation == "PLACE",
                            SpiderRuntimeAction.trigger_delivery_id.is_(None),
                        )
                    ).scalars().all())
                    all_place_order_ids = set(session.execute(
                        select(SpiderRuntimeAction.local_order_id).where(
                            SpiderRuntimeAction.plan_id == plan.plan_id,
                            SpiderRuntimeAction.operation == "PLACE",
                        )
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
                            or row.generation != runtime.generation
                            or row.configuration_sha256 != runtime.configuration_sha256
                            or (
                                row.kind in {"FILL", "ORDER_STATE"}
                                and row.local_order_id not in all_place_order_ids
                            )
                            or (
                                row.kind == "TRANSPORT_RESULT"
                                and (
                                    row.local_order_id is not None
                                    or row.trade_id is not None
                                    or row.action_id is not None
                                    or row.role is not None
                                )
                            )
                            or (
                                row.kind == "SOURCE_MARKET"
                                and (
                                    row.local_order_id is not None
                                    or row.action_id is not None
                                    or row.role is not None
                                    or row.trade_id is not None
                                    or type(row.source_payload) is not bytes
                                    or _digest_bytes(row.source_payload) != row.content_sha256
                                )
                            )
                            or (
                                index <= runtime.last_consumed_sequence
                                and not (
                                    row.kind == "SOURCE_MARKET"
                                    and row.disposition == "CONSUMED"
                                    and row.suppression_reason is None
                                )
                                and not (
                                    row.kind in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}
                                    and row.disposition == "CONSUMED"
                                    and row.suppression_reason is None
                                    and row.decision_payload is not None
                                    and row.decision_sha256 == _digest_bytes(row.decision_payload)
                                )
                                and not (
                                    row.disposition == "SUPPRESSED"
                                    and row.suppression_reason == "STOP_CONTROL"
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
                        .where(
                            SpiderRuntimeAction.plan_id == plan.plan_id,
                            SpiderRuntimeAction.operation == "PLACE",
                            SpiderRuntimeAction.trigger_delivery_id.is_(None),
                        )
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
            .where(
                SpiderRuntimeAction.plan_id == plan.plan_id,
                SpiderRuntimeAction.operation == "PLACE",
                SpiderRuntimeAction.trigger_delivery_id.is_(None),
            )
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
                or manifest.get("build_checkpoint_sha256")
                != plan.original_build_checkpoint_sha256
                or initial_manifest.get("build_checkpoint_sha256")
                != plan.original_build_checkpoint_sha256
                or type(plan.original_build_checkpoint) is not bytes
                or _digest_bytes(plan.original_build_checkpoint)
                != plan.original_build_checkpoint_sha256
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
                wire_proof = prove_spider_order_wire_binding(
                    scope.environment,
                    scope.execution_scope_id,
                    scope.strategy_id,
                    action,
                    binding,
                    order,
                )
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
                    or wire_proof != (
                        binding.source_client_order_id,
                        order.client_order_id,
                        intent.get("provider_wire_client_order_id"),
                    )
                ):
                    return False
            restore_source_checkpoint(
                plan.original_build_checkpoint,
                ordered_products=tuple(ordered_products),
                policy_id=ORIGINAL_MODEL_POLICY_ID,
                policy_version=ORIGINAL_MODEL_POLICY_VERSION,
                policy_source_sha256=ORIGINAL_MODEL_POLICY_SOURCE_SHA256,
                source_sha256=source_sha256,
                confirmation_request_id=runtime.confirmation_request_id,
                configuration_sha256=runtime.configuration_sha256,
                generation=runtime.generation,
            )
            deliveries = session.execute(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.environment == scope.environment,
                    SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                ).order_by(SpiderRuntimeDelivery.sequence)
            ).scalars().all()
            all_place_order_ids = set(session.execute(
                select(SpiderRuntimeAction.local_order_id).where(
                    SpiderRuntimeAction.plan_id == plan.plan_id,
                    SpiderRuntimeAction.operation == "PLACE",
                )
            ).scalars().all())
            expected_checkpoint = plan.original_build_checkpoint
            seen_market_ids: set[str] = set()
            has_stop_suppression = False
            evidence_sha256 = initial_manifest.get("historical_input_artifact_sha256")
            if type(evidence_sha256) is not str:
                return False
            for sequence, delivery in enumerate(deliveries, 1):
                if (
                    delivery.sequence != sequence
                    or (delivery.environment, delivery.execution_scope_id, delivery.strategy_id)
                    != (scope.environment, scope.execution_scope_id, scope.strategy_id)
                    or delivery.generation != runtime.generation
                    or delivery.configuration_sha256 != runtime.configuration_sha256
                    or type(delivery.source_payload) is not bytes
                    or _digest_bytes(delivery.source_payload) != delivery.content_sha256
                ):
                    return False
                stopped = (
                    delivery.disposition == "SUPPRESSED"
                    and delivery.suppression_reason == "STOP_CONTROL"
                )
                if sequence <= runtime.last_consumed_sequence:
                    consumed_source = (
                        delivery.kind == "SOURCE_MARKET"
                        and delivery.disposition == "CONSUMED"
                        and delivery.suppression_reason is None
                    )
                    consumed_normal = (
                        delivery.kind in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}
                        and delivery.disposition == "CONSUMED"
                        and delivery.suppression_reason is None
                    )
                    if not (consumed_source or consumed_normal or stopped):
                        return False
                elif delivery.disposition != "PENDING" or delivery.suppression_reason is not None:
                    return False
                if stopped:
                    has_stop_suppression = True
                if delivery.kind == "SOURCE_MARKET":
                    if (
                        delivery.local_order_id is not None
                        or delivery.trade_id is not None
                        or delivery.action_id is not None
                        or delivery.role is not None
                    ):
                        return False
                    observation = decode_source_market_observation(
                        delivery.source_payload,
                        scope=_scope(scope),
                        generation=runtime.generation,
                        confirmation_request_id=runtime.confirmation_request_id,
                        configuration_sha256=runtime.configuration_sha256,
                        evidence_sha256=evidence_sha256,
                        ordered_products=tuple(ordered_products),
                        build_checkpoint_bytes=plan.original_build_checkpoint,
                    )
                    if (
                        delivery.delivery_id != observation.snapshot_id
                        or observation.snapshot_id in seen_market_ids
                    ):
                        return False
                    seen_market_ids.add(observation.snapshot_id)
                    if delivery.disposition == "CONSUMED":
                        expected_checkpoint = replace_checkpoint_markets(
                            expected_checkpoint,
                            ordered_products=tuple(ordered_products),
                            markets=observation.markets,
                        )
                    elif delivery.disposition == "SUPPRESSED":
                        if delivery.suppression_reason != "STOP_CONTROL":
                            return False
                    elif delivery.disposition != "PENDING":
                        return False
                elif delivery.kind in {"FILL", "ORDER_STATE"}:
                    if (
                        delivery.local_order_id not in all_place_order_ids
                        or delivery.action_id is None
                        or delivery.role != "GRID"
                    ):
                        return False
                elif delivery.kind == "TRANSPORT_RESULT":
                    if (
                        delivery.local_order_id is not None
                        or delivery.trade_id is not None
                        or delivery.action_id is not None
                        or delivery.role is not None
                    ):
                        return False
                else:
                    return False
                if (
                    delivery.kind in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}
                    and (
                        delivery.disposition == "CONSUMED"
                        or delivery.decision_payload is not None
                    )
                ):
                    expected_next = self._validate_normal_decision_history_step(
                        session,
                        scope,
                        runtime,
                        plan,
                        delivery,
                        expected_checkpoint,
                        tuple(ordered_products),
                        cast(str, evidence_sha256),
                        source_sha256,
                    )
                    if expected_next is None:
                        return False
                    expected_checkpoint = expected_next
            if has_stop_suppression and self._current_stop_proof(session, scope, runtime) is None:
                return False
            if (
                expected_checkpoint != runtime.source_checkpoint
                or _digest_bytes(expected_checkpoint) != runtime.source_checkpoint_sha256
            ):
                return False
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


def _initial_candidate_frontier(plan: SpiderRuntimePlan) -> int:
    """Count immutable original PLACE candidates, including non-action entries."""
    manifest = decode_canonical(bytes(plan.manifest_payload))
    if type(manifest) is not dict:
        raise ValueError("SPIDER_INITIAL_MANIFEST_INVALID")
    initial = manifest.get("initial_source_events")
    if type(initial) is not dict or type(initial.get("entries")) is not list:
        raise ValueError("SPIDER_INITIAL_CANDIDATE_FRONTIER_UNAVAILABLE")
    frontier = 0
    for entry in cast(list[object], initial["entries"]):
        if type(entry) is not dict or type(cast(dict[str, object], entry).get("events")) is not list:
            raise ValueError("SPIDER_INITIAL_CANDIDATE_FRONTIER_UNAVAILABLE")
        for event in cast(list[object], cast(dict[str, object], entry)["events"]):
            if type(event) is not dict:
                raise ValueError("SPIDER_INITIAL_CANDIDATE_FRONTIER_UNAVAILABLE")
            source_event = cast(dict[str, object], event)
            if source_event.get("kind") != "send":
                continue
            orders = source_event.get("orders")
            if type(orders) is not list:
                raise ValueError("SPIDER_INITIAL_CANDIDATE_FRONTIER_UNAVAILABLE")
            frontier += len(orders)
    return frontier


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
