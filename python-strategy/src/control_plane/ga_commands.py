"""Thin GA command orchestration over the existing store and worker owners."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, NoReturn

from sqlalchemy.exc import SQLAlchemyError

from src.control_plane.evaluation_data import (
    SealedDatasetBackendUnavailableError,
    SealedDatasetRejectedError,
)
from src.control_plane.ga_http_contract import GaCommandRequest
from src.control_plane.ga_lifecycle import (
    GaJobRecord,
    GaJobStoreError,
    retry_ga_job_record,
    transition_ga_job,
)
from src.control_plane.ga_profile import compile_golden_cross_request
from src.control_plane.jobs import JobStore
from src.control_plane.parameter_evaluation import ParameterSearchEvaluatorRegistry
from src.control_plane.parameter_search import (
    ParameterSearchJobExecutor,
    _validated_ga_worker_request,
)
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import Strategy
from src.core.research_datasets import ResearchDatasetIntegrityError

logger = logging.getLogger(__name__)


class GaCommandValidationError(ValueError):
    """A fixed, user-correctable GA command rejection."""


class GaCommandBackendError(RuntimeError):
    """A fixed GA command preflight or persistence availability failure."""


class GaCommandService:
    """Preflight, commit and dispatch through existing GA authorities."""

    def __init__(
        self,
        *,
        store: JobStore,
        executor: ParameterSearchJobExecutor,
        session_factory,
    ) -> None:
        self._store = store
        self._executor = executor
        self._session_factory = session_factory

    def execute(
        self, command: GaCommandRequest, *, actor: str
    ) -> tuple[int, GaJobRecord]:
        receipt = self._store.get_ga_command_receipt(
            actor=actor,
            idempotency_key=command.idempotency_key,
            operation=command.operation,
            job_id=command.job_id,
            expected_version=(
                None if command.body is None else command.body.get("expected_version")
            ),
            http_wire_intent=(command.body if command.operation == "submit" else None),
        )
        if receipt is not None:
            return _success_status(command.operation), receipt

        if command.operation == "submit":
            assert command.body is not None
            request = self._compile(command.body)
            record, replayed = self._store.submit_ga_job_command(
                actor=actor,
                idempotency_key=command.idempotency_key,
                request=request,
                http_wire_intent=command.body,
            )
        else:
            assert command.job_id is not None and command.body is not None
            expected_version = command.body["expected_version"]
            if command.operation in {"resume", "retry"}:
                source = self._store.get_ga_job(command.job_id)
                if source is None:
                    raise GaJobStoreError("ga_job_not_found")
                if command.operation == "resume":
                    transition_ga_job(source, "resume", expected_version)
                else:
                    retry_ga_job_record(
                        source, "preflight", "preflight", expected_version
                    )
                self._validate_existing_binding(source)

            if command.operation == "retry":
                record, replayed = self._store.retry_ga_job_command(
                    actor=actor,
                    idempotency_key=command.idempotency_key,
                    job_id=command.job_id,
                    expected_version=expected_version,
                )
            else:
                record, replayed = self._store.transition_ga_job_command(
                    actor=actor,
                    idempotency_key=command.idempotency_key,
                    job_id=command.job_id,
                    action=command.operation,
                    expected_version=expected_version,
                )

        if not replayed and command.operation in {"submit", "resume", "retry"}:
            self._dispatch(record)
        return _success_status(command.operation), record

    def _compile(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        dataset_id = payload.get("dataset_id")
        if type(dataset_id) is not str or not dataset_id.strip():
            raise GaCommandValidationError from None
        try:
            metadata = ResearchDatabaseDataSource(
                dataset_id, session_factory=self._session_factory
            ).get_dataset_metadata()
            request = compile_golden_cross_request(payload, metadata)
            self._validate_evaluator(request)
            self._require_strategy()
            return request.model_dump(mode="json")
        except Exception as exc:
            _raise_preflight_error(exc)

    def _validate_existing_binding(self, source: GaJobRecord) -> None:
        try:
            request = _validated_ga_worker_request(
                source,
                self._executor.evaluator,
                self._session_factory,
            )
            self._require_strategy()
            del request
        except Exception as exc:
            _raise_preflight_error(exc)

    def _validate_evaluator(self, request) -> None:
        evaluator = self._executor.evaluator
        if not isinstance(evaluator, ParameterSearchEvaluatorRegistry):
            raise GaCommandValidationError
        evaluator.validate_request(request)

    def _require_strategy(self) -> None:
        if self._session_factory is None:
            raise GaCommandBackendError
        with self._session_factory() as session:
            if session.get(Strategy, "golden_cross") is None:
                raise GaCommandValidationError

    def _dispatch(self, record: GaJobRecord) -> None:
        try:
            self._executor.dispatch_ga_job(record.id, record.version)
        except Exception:
            logger.warning("ga_dispatch_failed")


def _success_status(operation: str) -> int:
    return 202 if operation in {"submit", "retry"} else 200


def _raise_preflight_error(exc: Exception) -> NoReturn:
    if isinstance(exc, (SealedDatasetBackendUnavailableError, SQLAlchemyError)):
        raise GaCommandBackendError from None
    if isinstance(
        exc,
        (
            SealedDatasetRejectedError,
            ResearchDatasetIntegrityError,
            ValueError,
            TypeError,
        ),
    ):
        raise GaCommandValidationError from None
    raise GaCommandBackendError from None
