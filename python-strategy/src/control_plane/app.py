from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit
from typing import Any

from pydantic import ValidationError

from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.gene_control import GeneControlService
from src.control_plane.models import (
    BacktestJobRequest,
    GenePromotionRequest,
    JobRecord,
    ParameterSearchJobRequest,
    StrategyCommandRequest,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.control_plane.strategy_control import StrategyControlService
from src.control_plane.strategy_state_query import StrategyStateQueryService


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    body: dict[str, Any]

    def json(self) -> str:
        return json.dumps(self.body, separators=(",", ":"), default=str)


class ControlPlaneApp:
    """Small framework-neutral HTTP-style control-plane router."""

    def __init__(
        self,
        backtest_executor: BacktestJobExecutor,
        parameter_search_executor: ParameterSearchJobExecutor | None = None,
        gene_control: GeneControlService | None = None,
        strategy_control: StrategyControlService | None = None,
        strategy_state_query: StrategyStateQueryService | None = None,
    ) -> None:
        self.backtest_executor = backtest_executor
        self.parameter_search_executor = parameter_search_executor
        self.gene_control = gene_control
        self.strategy_control = strategy_control
        self.strategy_state_query = strategy_state_query

    def handle(
        self,
        method: str,
        path: str,
        body: str | bytes | None = None,
    ) -> HttpResponse:
        method = method.upper()
        parsed_url = urlsplit(path)
        clean_path = parsed_url.path.rstrip("/") or "/"
        query = parse_qs(parsed_url.query)

        if method == "GET" and clean_path == "/health":
            return HttpResponse(200, {"status": "ok"})

        if method == "POST" and clean_path == "/jobs/backtests":
            return self._submit_backtest(body)

        if method == "POST" and clean_path == "/jobs/parameter-searches":
            return self._submit_parameter_search(body)

        if method == "POST" and clean_path.startswith("/jobs/"):
            return self._handle_job_action(clean_path, body)

        if method == "POST" and clean_path.startswith("/genes/"):
            return self._submit_gene_action(clean_path, body)

        if method == "GET" and clean_path == "/genes":
            return self._list_genes(query)

        if method == "GET" and clean_path.startswith("/genes/"):
            return self._get_gene(clean_path)

        if method == "GET" and clean_path == "/evolution-epochs":
            return self._list_epochs(query)

        if method == "GET" and clean_path.startswith("/evolution-epochs/"):
            return self._get_epoch(clean_path)

        if method == "GET" and clean_path == "/system-events":
            return self._list_system_events(query)

        if method == "GET" and clean_path.startswith("/system-events/"):
            return self._get_system_event(clean_path)

        if method == "GET" and clean_path == "/strategy-states":
            return self._list_strategy_states(query)

        if method == "GET" and clean_path == "/strategy-states/summary":
            return self._summarize_strategy_states(query)

        if method == "GET" and clean_path.startswith("/strategy-states/"):
            return self._handle_strategy_state_get(clean_path, query)

        if method == "GET" and clean_path == "/jobs":
            return self._list_jobs(query)

        if method == "GET" and clean_path == "/strategies":
            if self.strategy_control is None:
                return HttpResponse(503, {"error": "strategy_control_unavailable"})
            result = self.strategy_control.list_strategies()
            return self._command_response(result)

        if method == "GET" and clean_path == "/strategies/health":
            if self.strategy_control is None:
                return HttpResponse(503, {"error": "strategy_control_unavailable"})
            result = self.strategy_control.health()
            return self._command_response(result)

        if method == "POST" and clean_path.startswith("/strategies/"):
            return self._submit_strategy_command(clean_path, body)

        if method == "GET" and clean_path.startswith("/jobs/"):
            job_id = clean_path.removeprefix("/jobs/")
            if not job_id:
                return HttpResponse(404, {"error": "not_found"})
            job = self.backtest_executor.store.get(job_id)
            if job is None:
                return HttpResponse(404, {"error": "job_not_found"})
            return HttpResponse(200, {"job": self._job_payload(job)})

        return HttpResponse(404, {"error": "not_found"})

    def _submit_backtest(self, body: str | bytes | None) -> HttpResponse:
        try:
            payload = self._parse_json_body(body)
            request = BacktestJobRequest.model_validate(payload)
        except json.JSONDecodeError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
        except ValidationError as exc:
            return HttpResponse(
                422,
                {
                    "error": "validation_error",
                    "detail": exc.errors(include_url=False),
                },
            )
        except ValueError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})

        job = self.backtest_executor.submit_backtest(request)
        status_code = 200 if job.finished_at is not None else 202
        return HttpResponse(status_code, {"job": self._job_payload(job)})

    def _submit_parameter_search(self, body: str | bytes | None) -> HttpResponse:
        if self.parameter_search_executor is None:
            return HttpResponse(503, {"error": "parameter_search_unavailable"})
        try:
            payload = self._parse_json_body(body)
            request = ParameterSearchJobRequest.model_validate(payload)
        except json.JSONDecodeError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
        except ValidationError as exc:
            return HttpResponse(
                422,
                {
                    "error": "validation_error",
                    "detail": exc.errors(include_url=False),
                },
            )
        except ValueError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})

        job = self.parameter_search_executor.submit_search(request)
        status_code = 200 if job.finished_at is not None else 202
        return HttpResponse(status_code, {"job": self._job_payload(job)})

    def _handle_job_action(
        self,
        path: str,
        body: str | bytes | None,
    ) -> HttpResponse:
        if path.endswith("/cancel"):
            job_id = path.removeprefix("/jobs/")[: -len("/cancel")]
            if not job_id:
                return HttpResponse(404, {"error": "not_found"})
            try:
                existing = self.backtest_executor.store.get(job_id)
                if existing is None:
                    return HttpResponse(404, {"error": "job_not_found"})
                payload = self._parse_json_body(body) if body not in (None, "") else {}
                reason = payload.get("reason")
                if reason is not None and not isinstance(reason, str):
                    return HttpResponse(422, {"error": "validation_error"})
                if existing.kind == "parameter_search":
                    if self.parameter_search_executor is None:
                        return HttpResponse(503, {"error": "parameter_search_unavailable"})
                    job = self.parameter_search_executor.cancel_search(job_id, reason)
                else:
                    job = self.backtest_executor.cancel_backtest(job_id, reason)
            except json.JSONDecodeError as exc:
                return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
            except ValueError as exc:
                return HttpResponse(409, {"error": "job_action_rejected", "detail": str(exc)})
            except KeyError:
                return HttpResponse(404, {"error": "job_not_found"})
            return HttpResponse(200, {"job": self._job_payload(job)})

        if path.endswith("/retry"):
            job_id = path.removeprefix("/jobs/")[: -len("/retry")]
            if not job_id:
                return HttpResponse(404, {"error": "not_found"})
            try:
                existing = self.backtest_executor.store.get(job_id)
                if existing is None:
                    return HttpResponse(404, {"error": "job_not_found"})
                if existing.kind == "parameter_search":
                    if self.parameter_search_executor is None:
                        return HttpResponse(503, {"error": "parameter_search_unavailable"})
                    job = self.parameter_search_executor.retry_search(job_id)
                else:
                    job = self.backtest_executor.retry_backtest(job_id)
            except ValueError as exc:
                return HttpResponse(409, {"error": "job_action_rejected", "detail": str(exc)})
            except KeyError:
                return HttpResponse(404, {"error": "job_not_found"})
            status_code = 200 if job.finished_at is not None else 202
            return HttpResponse(status_code, {"job": self._job_payload(job)})

        return HttpResponse(404, {"error": "not_found"})

    def _submit_strategy_command(
        self,
        path: str,
        body: str | bytes | None,
    ) -> HttpResponse:
        if self.strategy_control is None:
            return HttpResponse(503, {"error": "strategy_control_unavailable"})

        prefix = "/strategies/"
        suffix = "/commands"
        if not path.endswith(suffix):
            return HttpResponse(404, {"error": "not_found"})

        strategy_id = path.removeprefix(prefix)[: -len(suffix)]
        if not strategy_id:
            return HttpResponse(404, {"error": "not_found"})

        try:
            payload = self._parse_json_body(body)
            request = StrategyCommandRequest.model_validate(payload)
        except json.JSONDecodeError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
        except ValidationError as exc:
            return HttpResponse(
                422,
                {
                    "error": "validation_error",
                    "detail": exc.errors(include_url=False),
                },
            )
        except ValueError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})

        result = self.strategy_control.submit_command(strategy_id, request)
        return self._command_response(result)

    def _submit_gene_action(
        self,
        path: str,
        body: str | bytes | None,
    ) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})

        prefix = "/genes/"
        suffix = "/promote"
        if not path.endswith(suffix):
            return HttpResponse(404, {"error": "not_found"})

        raw_gene_id = path.removeprefix(prefix)[: -len(suffix)]
        try:
            gene_id = int(raw_gene_id)
        except ValueError:
            return HttpResponse(404, {"error": "not_found"})

        try:
            payload = self._parse_json_body(body) if body not in (None, "") else {}
            request = GenePromotionRequest.model_validate(payload)
            result = self.gene_control.promote_gene(
                gene_id,
                reason=request.reason,
                actor=request.actor,
            )
        except json.JSONDecodeError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
        except ValidationError as exc:
            return HttpResponse(
                422,
                {
                    "error": "validation_error",
                    "detail": exc.errors(include_url=False),
                },
            )
        except KeyError:
            return HttpResponse(404, {"error": "gene_not_found"})

        return HttpResponse(200, {"gene": result})

    def _list_genes(self, query: dict[str, list[str]]) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        pagination = _parse_pagination(query)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        strategy_id = _single_query_value(query, "strategy_id")
        role = _single_query_value(query, "role")
        genes, total = self.gene_control.list_genes(
            strategy_id=strategy_id,
            role=role,
            limit=limit,
            offset=offset,
        )
        return HttpResponse(
            200,
            _page_payload("genes", genes, total=total, limit=limit, offset=offset),
        )

    def _get_gene(self, path: str) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        raw_gene_id = path.removeprefix("/genes/")
        try:
            gene_id = int(raw_gene_id)
            gene = self.gene_control.get_gene(gene_id)
        except ValueError:
            return HttpResponse(404, {"error": "not_found"})
        except KeyError:
            return HttpResponse(404, {"error": "gene_not_found"})
        return HttpResponse(200, {"gene": gene})

    def _list_epochs(self, query: dict[str, list[str]]) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        pagination = _parse_pagination(query)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        strategy_id = _single_query_value(query, "strategy_id")
        epochs, total = self.gene_control.list_epochs(
            strategy_id=strategy_id,
            limit=limit,
            offset=offset,
        )
        return HttpResponse(
            200,
            _page_payload("epochs", epochs, total=total, limit=limit, offset=offset),
        )

    def _get_epoch(self, path: str) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        epoch_id = path.removeprefix("/evolution-epochs/")
        if not epoch_id:
            return HttpResponse(404, {"error": "not_found"})
        try:
            epoch = self.gene_control.get_epoch(epoch_id)
        except KeyError:
            return HttpResponse(404, {"error": "epoch_not_found"})
        return HttpResponse(200, {"epoch": epoch})

    def _list_system_events(self, query: dict[str, list[str]]) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        pagination = _parse_pagination(query)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        raw_gene_id = _single_query_value(query, "related_gene_id")
        try:
            related_gene_id = int(raw_gene_id) if raw_gene_id is not None else None
        except ValueError:
            return HttpResponse(422, {"error": "validation_error"})
        events, total = self.gene_control.list_system_events(
            event_type=_single_query_value(query, "event_type"),
            strategy_id=_single_query_value(query, "strategy_id"),
            related_gene_id=related_gene_id,
            limit=limit,
            offset=offset,
        )
        return HttpResponse(
            200,
            _page_payload("events", events, total=total, limit=limit, offset=offset),
        )

    def _get_system_event(self, path: str) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        raw_event_id = path.removeprefix("/system-events/")
        try:
            event_id = int(raw_event_id)
            event = self.gene_control.get_system_event(event_id)
        except ValueError:
            return HttpResponse(404, {"error": "not_found"})
        except KeyError:
            return HttpResponse(404, {"error": "system_event_not_found"})
        return HttpResponse(200, {"event": event})

    def _list_jobs(self, query: dict[str, list[str]]) -> HttpResponse:
        pagination = _parse_pagination(query)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        all_jobs = [self._job_payload(job) for job in self.backtest_executor.store.list()]
        jobs = all_jobs[offset : offset + limit]
        return HttpResponse(
            200,
            _page_payload(
                "jobs",
                jobs,
                total=len(all_jobs),
                limit=limit,
                offset=offset,
            ),
        )

    def _list_strategy_states(self, query: dict[str, list[str]]) -> HttpResponse:
        if self.strategy_state_query is None:
            return HttpResponse(503, {"error": "strategy_state_query_unavailable"})
        pagination = _parse_pagination(query)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        states, total = self.strategy_state_query.list_states(
            status=_single_query_value(query, "status"),
            limit=limit,
            offset=offset,
        )
        return HttpResponse(
            200,
            _page_payload("states", states, total=total, limit=limit, offset=offset),
        )

    def _summarize_strategy_states(self, query: dict[str, list[str]]) -> HttpResponse:
        if self.strategy_state_query is None:
            return HttpResponse(503, {"error": "strategy_state_query_unavailable"})
        stale_after_ms = _parse_optional_non_negative_int(query, "stale_after_ms")
        if isinstance(stale_after_ms, HttpResponse):
            return stale_after_ms
        summary = self.strategy_state_query.summarize_states(
            stale_after_ms=120_000 if stale_after_ms is None else stale_after_ms,
        )
        return HttpResponse(200, {"summary": summary})

    def _handle_strategy_state_get(
        self,
        path: str,
        query: dict[str, list[str]],
    ) -> HttpResponse:
        if self.strategy_state_query is None:
            return HttpResponse(503, {"error": "strategy_state_query_unavailable"})
        suffix = "/transitions"
        if path.endswith(suffix):
            strategy_id = path.removeprefix("/strategy-states/")[: -len(suffix)]
            if not strategy_id:
                return HttpResponse(404, {"error": "not_found"})
            return self._list_strategy_transitions(strategy_id, query)

        strategy_id = path.removeprefix("/strategy-states/")
        if not strategy_id:
            return HttpResponse(404, {"error": "not_found"})
        try:
            state = self.strategy_state_query.get_state(strategy_id)
        except KeyError:
            return HttpResponse(404, {"error": "strategy_state_not_found"})
        return HttpResponse(200, {"state": state})

    def _list_strategy_transitions(
        self,
        strategy_id: str,
        query: dict[str, list[str]],
    ) -> HttpResponse:
        pagination = _parse_pagination(query)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        transitions, total = self.strategy_state_query.list_transitions(
            strategy_id,
            limit=limit,
            offset=offset,
        )
        return HttpResponse(
            200,
            _page_payload(
                "transitions",
                transitions,
                total=total,
                limit=limit,
                offset=offset,
            ),
        )

    @staticmethod
    def _parse_json_body(body: str | bytes | None) -> dict[str, Any]:
        if body is None or body == "":
            return {}
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        parsed = json.loads(body)
        if not isinstance(parsed, dict):
            raise ValueError("JSON body must be an object")
        return parsed

    @staticmethod
    def _job_payload(job: JobRecord) -> dict[str, Any]:
        return job.model_dump(mode="json")

    @staticmethod
    def _command_response(result: dict[str, Any]) -> HttpResponse:
        status_code = 200 if result["success"] else 400
        return HttpResponse(status_code, {"result": result})


def _single_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    return values[0]


def _parse_pagination(query: dict[str, list[str]]) -> tuple[int, int] | HttpResponse:
    try:
        limit = int(_single_query_value(query, "limit") or "100")
        offset = int(_single_query_value(query, "offset") or "0")
    except ValueError:
        return HttpResponse(422, {"error": "validation_error"})
    if limit < 1 or limit > 500 or offset < 0:
        return HttpResponse(422, {"error": "validation_error"})
    return limit, offset


def _parse_optional_non_negative_int(
    query: dict[str, list[str]],
    key: str,
) -> int | None | HttpResponse:
    raw_value = _single_query_value(query, key)
    if raw_value is None:
        return None
    try:
        value = int(raw_value)
    except ValueError:
        return HttpResponse(422, {"error": "validation_error"})
    if value < 0:
        return HttpResponse(422, {"error": "validation_error"})
    return value


def _page_payload(
    key: str,
    items: list[dict[str, Any]],
    *,
    total: int,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    return {
        key: items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }
