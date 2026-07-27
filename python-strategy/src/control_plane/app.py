from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hmac import compare_digest
from urllib.parse import parse_qs, urlsplit
from typing import Any

from pydantic import ValidationError

from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import (
    BrowserAuthProvider,
    BrowserAuthRejected,
    BrowserPrincipal,
)
from src.control_plane.gene_control import GeneControlService
from src.control_plane.models import (
    BacktestJobRequest,
    GenePromotionRequest,
    JobRecord,
    ParameterSearchJobRequest,
    StrategyCommandRequest,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.control_plane.parameter_evaluation import UnsupportedParameterSearchError
from src.control_plane.presets import GoldenCrossParameterSearchPreset
from src.control_plane.strategy_control import (
    StrategyControlService,
    StrategyControlUnavailable,
)
from src.control_plane.strategy_state_query import StrategyStateQueryService


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    body: dict[str, Any]
    headers: tuple[tuple[str, str], ...] = ()

    def json(self) -> str:
        return json.dumps(self.body, separators=(",", ":"), default=str)


@dataclass(frozen=True, slots=True)
class _RequestIdentity:
    actor: str
    browser_principal: BrowserPrincipal | None = None


class ControlPlaneApp:
    """Small framework-neutral HTTP-style control-plane router."""

    def __init__(
        self,
        backtest_executor: BacktestJobExecutor,
        parameter_search_executor: ParameterSearchJobExecutor | None = None,
        gene_control: GeneControlService | None = None,
        strategy_control: StrategyControlService | None = None,
        strategy_state_query: StrategyStateQueryService | None = None,
        api_key: str | None = None,
        redis_client: Any | None = None,
        browser_auth: BrowserAuthProvider | None = None,
    ) -> None:
        if api_key == "":
            raise ValueError("api_key must be non-empty when provided")
        self.backtest_executor = backtest_executor
        self.parameter_search_executor = parameter_search_executor
        self.gene_control = gene_control
        self.strategy_control = strategy_control
        self.strategy_state_query = strategy_state_query
        self.api_key = api_key
        self.redis_client = redis_client
        self.browser_auth = browser_auth

    def handle(
        self,
        method: str,
        path: str,
        body: str | bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResponse:
        method = method.upper()
        parsed_url = urlsplit(path)
        clean_path = parsed_url.path.rstrip("/") or "/"
        query = parse_qs(parsed_url.query)

        if method == "GET" and clean_path == "/health":
            return HttpResponse(200, {"status": "ok"})

        if method == "POST" and clean_path == "/api/v1/auth/session":
            return self._create_browser_session(headers)

        identity = self._authorize(headers)
        if isinstance(identity, HttpResponse):
            if clean_path == "/api/v1/auth/session":
                return HttpResponse(
                    identity.status_code,
                    identity.body,
                    headers=(("Cache-Control", "no-store"),),
                )
            return identity

        browser_policy_response = self._authorize_browser_request(
            method,
            clean_path,
            headers,
            identity.browser_principal,
        )
        if browser_policy_response is not None:
            return browser_policy_response

        if method == "GET" and clean_path == "/api/v1/auth/session":
            if self.browser_auth is None:
                return HttpResponse(404, {"error": "not_found"})
            return self._get_browser_session(identity.browser_principal)

        if method == "POST" and clean_path == "/api/v1/auth/logout":
            return self._logout_browser_session(identity.browser_principal)

        if method == "POST" and clean_path == "/jobs/backtests":
            return self._submit_backtest(body)

        if method == "POST" and clean_path == "/jobs/parameter-searches":
            return self._submit_parameter_search(body)

        if (
            method == "POST"
            and clean_path == "/jobs/parameter-search-presets/golden-cross"
        ):
            return self._submit_golden_cross_parameter_search_preset(body)

        if method == "POST" and clean_path.startswith("/jobs/"):
            return self._handle_job_action(clean_path, body)

        if method == "POST" and clean_path.startswith("/genes/"):
            return self._submit_gene_action(clean_path, body, actor=identity.actor)

        if method == "GET" and clean_path == "/genes":
            return self._list_genes(query)

        if method == "GET" and clean_path.startswith("/genes/"):
            return self._get_gene(clean_path)

        if method == "GET" and clean_path == "/evolution-epochs":
            return self._list_epochs(query)

        if (
            method == "GET"
            and clean_path.startswith("/evolution-epochs/")
            and clean_path.endswith("/generations")
        ):
            return self._list_epoch_generations(clean_path)

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
            try:
                result = self.strategy_control.list_strategies()
            except StrategyControlUnavailable as exc:
                return HttpResponse(
                    503,
                    {"error": "strategy_control_unavailable", "detail": str(exc)},
                )
            return self._command_response(result)

        if method == "GET" and clean_path == "/strategies/health":
            if self.strategy_control is None:
                return HttpResponse(503, {"error": "strategy_control_unavailable"})
            try:
                result = self.strategy_control.health()
            except StrategyControlUnavailable as exc:
                return HttpResponse(
                    503,
                    {"error": "strategy_control_unavailable", "detail": str(exc)},
                )
            return self._command_response(result)

        if method == "POST" and clean_path.startswith("/strategies/"):
            return self._submit_strategy_command(
                clean_path,
                body,
                actor=identity.actor,
                browser_principal=identity.browser_principal,
            )

        if method == "GET" and clean_path.startswith("/jobs/"):
            job_id = clean_path.removeprefix("/jobs/")
            if not job_id:
                return HttpResponse(404, {"error": "not_found"})
            job = self.backtest_executor.store.get(job_id)
            if job is None:
                return HttpResponse(404, {"error": "job_not_found"})
            return HttpResponse(200, {"job": self._job_payload(job)})

        if method == "POST" and clean_path == "/ops/kill-switch/clear":
            return self._publish_ops_command(
                {
                    "command": "CLEAR_KILL_SWITCH",
                    "params": {"actor": identity.actor},
                }
            )

        if method == "POST" and clean_path == "/ops/kill-switch":
            return self._handle_kill_switch(
                body,
                headers=headers,
                actor=identity.actor,
                require_confirmation=identity.browser_principal is not None,
            )

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

        try:
            job = self.parameter_search_executor.submit_search(request)
        except UnsupportedParameterSearchError as exc:
            return HttpResponse(
                422,
                {"error": "parameter_search_rejected", "detail": str(exc)},
            )
        status_code = 200 if job.finished_at is not None else 202
        return HttpResponse(status_code, {"job": self._job_payload(job)})

    def _submit_golden_cross_parameter_search_preset(
        self,
        body: str | bytes | None,
    ) -> HttpResponse:
        if self.parameter_search_executor is None:
            return HttpResponse(503, {"error": "parameter_search_unavailable"})
        try:
            payload = self._parse_json_body(body)
            preset = GoldenCrossParameterSearchPreset.model_validate(payload)
            request = preset.to_parameter_search_request()
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

        try:
            job = self.parameter_search_executor.submit_search(request)
        except UnsupportedParameterSearchError as exc:
            return HttpResponse(
                422,
                {"error": "parameter_search_rejected", "detail": str(exc)},
            )
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
        *,
        actor: str,
        browser_principal: BrowserPrincipal | None,
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

        if (
            browser_principal is not None
            and request.command in {"START", "RESUME", "FORCE_RECOVER", "RELOAD"}
        ):
            assert self.browser_auth is not None
            if not self.browser_auth.has_step_up(browser_principal):
                return HttpResponse(403, {"error": "step_up_required"})

        try:
            result = self.strategy_control.submit_command(
                strategy_id,
                request,
                actor=actor,
            )
        except StrategyControlUnavailable as exc:
            return HttpResponse(
                503,
                {"error": "strategy_control_unavailable", "detail": str(exc)},
            )
        return self._command_response(result)

    def _handle_kill_switch(
        self,
        body: str | bytes | None,
        *,
        headers: Mapping[str, str] | None,
        actor: str,
        require_confirmation: bool,
    ) -> HttpResponse:
        """Validate and publish an actor-attributed KILL_SWITCH command."""
        try:
            payload = self._parse_json_body(body)
        except json.JSONDecodeError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
        except ValueError as exc:
            return HttpResponse(400, {"error": "invalid_json", "detail": str(exc)})
        reason = payload.get("reason")
        if reason is not None and not isinstance(reason, str):
            return HttpResponse(422, {"error": "validation_error"})
        if require_confirmation and payload.get("confirm") is not True:
            return HttpResponse(403, {"error": "confirmation_required"})
        idempotency_key = None
        if require_confirmation:
            idempotency_key = _extract_idempotency_key(headers)
            if idempotency_key is None:
                return HttpResponse(400, {"error": "idempotency_key_required"})
            if not _valid_idempotency_key(idempotency_key):
                return HttpResponse(400, {"error": "idempotency_key_invalid"})
        command = {
            "command": "KILL_SWITCH",
            "params": {
                "actor": actor,
                "reason": reason,
            },
        }
        if idempotency_key is not None:
            command["params"]["idempotency_key"] = idempotency_key
        return self._publish_ops_command(
            command,
            operation_id=idempotency_key,
        )

    def _publish_ops_command(
        self,
        command: dict[str, Any],
        *,
        operation_id: str | None = None,
    ) -> HttpResponse:
        if self.redis_client is None:
            return HttpResponse(503, {"error": "redis_unavailable"})
        try:
            subscribers = self.redis_client.publish(
                "cmd:strategy:control",
                json.dumps(command, separators=(",", ":")),
            )
        except Exception as exc:
            return HttpResponse(
                503,
                {"error": "redis_publish_failed", "detail": str(exc)},
            )
        if subscribers == 0:
            return HttpResponse(503, {"error": "kill_switch_no_listener"})
        body = {"status": "accepted"}
        if operation_id is not None:
            body["operation_id"] = operation_id
        return HttpResponse(202, body)

    def _submit_gene_action(
        self,
        path: str,
        body: str | bytes | None,
        *,
        actor: str,
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
                actor=actor,
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
        pagination = _parse_pagination(query, max_limit=10_000)
        if isinstance(pagination, HttpResponse):
            return pagination
        limit, offset = pagination
        strategy_id = _single_query_value(query, "strategy_id")
        role = _single_query_value(query, "role")
        generation_index = _parse_optional_non_negative_int(
            query,
            "generation_index",
        )
        if isinstance(generation_index, HttpResponse):
            return generation_index
        genes, total = self.gene_control.list_genes(
            strategy_id=strategy_id,
            role=role,
            epoch_id=_single_query_value(query, "epoch_id"),
            generation_index=generation_index,
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

    def _list_epoch_generations(self, path: str) -> HttpResponse:
        if self.gene_control is None:
            return HttpResponse(503, {"error": "gene_control_unavailable"})
        epoch_id = path.removeprefix("/evolution-epochs/").removesuffix(
            "/generations"
        )
        if not epoch_id or "/" in epoch_id:
            return HttpResponse(404, {"error": "not_found"})
        try:
            generations = self.gene_control.list_generation_summaries(epoch_id)
        except KeyError:
            return HttpResponse(404, {"error": "epoch_not_found"})
        return HttpResponse(200, {"generations": generations})

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
        assert self.strategy_state_query is not None
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
        status_code = 202 if result.get("accepted") else 200
        if not result["success"]:
            status_code = 400
        return HttpResponse(status_code, {"result": result})

    def _create_browser_session(
        self,
        headers: Mapping[str, str] | None,
    ) -> HttpResponse:
        if self.browser_auth is None:
            return HttpResponse(404, {"error": "not_found"})
        try:
            principal = self.browser_auth.issue(headers)
        except BrowserAuthRejected as exc:
            status_code = 403 if exc.reason == "origin_rejected" else 401
            return HttpResponse(
                status_code,
                {"error": exc.reason},
                headers=(("Cache-Control", "no-store"),),
            )
        return HttpResponse(
            201,
            {
                "actor": principal.actor,
                "capabilities": sorted(principal.capabilities),
                "csrf_token": principal.csrf_token,
                "expires_at": _utc_iso(principal.expires_at),
                "step_up_expires_at": _utc_iso(principal.step_up_expires_at),
            },
            headers=(
                ("Cache-Control", "no-store"),
                ("Set-Cookie", self.browser_auth.session_cookie(principal)),
            ),
        )

    @staticmethod
    def _get_browser_session(
        principal: BrowserPrincipal | None,
    ) -> HttpResponse:
        if principal is None:
            return HttpResponse(401, {"error": "browser_session_required"})
        return HttpResponse(
            200,
            {
                "actor": principal.actor,
                "capabilities": sorted(principal.capabilities),
                "csrf_token": principal.csrf_token,
                "expires_at": _utc_iso(principal.expires_at),
                "step_up_expires_at": _utc_iso(principal.step_up_expires_at),
            },
            headers=(("Cache-Control", "no-store"),),
        )

    def _logout_browser_session(
        self,
        principal: BrowserPrincipal | None,
    ) -> HttpResponse:
        if self.browser_auth is None or principal is None:
            return HttpResponse(401, {"error": "browser_session_required"})
        self.browser_auth.revoke(principal)
        return HttpResponse(
            200,
            {"status": "logged_out"},
            headers=(
                ("Cache-Control", "no-store"),
                ("Set-Cookie", self.browser_auth.expired_cookie()),
            ),
        )

    def _authorize(
        self,
        headers: Mapping[str, str] | None,
    ) -> _RequestIdentity | HttpResponse:
        supplied = _extract_api_key(headers)
        if supplied is not None:
            if self.api_key is not None and compare_digest(supplied, self.api_key):
                return _RequestIdentity(actor="api_key")
            return HttpResponse(401, {"error": "unauthorized"})
        if self.browser_auth is not None:
            try:
                principal = self.browser_auth.authenticate(headers)
            except BrowserAuthRejected as exc:
                return HttpResponse(401, {"error": exc.reason})
            if principal is not None:
                return _RequestIdentity(
                    actor=principal.actor,
                    browser_principal=principal,
                )
        if self.api_key is None and self.browser_auth is None:
            return _RequestIdentity(actor="operator")
        return HttpResponse(401, {"error": "unauthorized"})

    def _authorize_browser_request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str] | None,
        principal: BrowserPrincipal | None,
    ) -> HttpResponse | None:
        if principal is None or method == "GET":
            return None
        assert self.browser_auth is not None
        try:
            self.browser_auth.require_same_origin(headers)
            self.browser_auth.require_csrf(principal, headers)
        except BrowserAuthRejected as exc:
            return HttpResponse(403, {"error": exc.reason})
        if path == "/api/v1/auth/logout":
            return None
        if not principal.has_capability(self.browser_auth.operator_capability):
            return HttpResponse(403, {"error": "operator_capability_required"})
        if _requires_step_up(path) and not self.browser_auth.has_step_up(principal):
            return HttpResponse(403, {"error": "step_up_required"})
        return None


def _single_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    return values[0]


def _extract_api_key(headers: Mapping[str, str] | None) -> str | None:
    if headers is None:
        return None
    normalized = {key.lower(): value for key, value in headers.items()}
    authorization = normalized.get("authorization")
    if authorization is not None:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token:
            return token
    return normalized.get("x-api-key")


def _extract_idempotency_key(
    headers: Mapping[str, str] | None,
) -> str | None:
    if headers is None:
        return None
    normalized = {key.lower(): value for key, value in headers.items()}
    return normalized.get("idempotency-key")


def _valid_idempotency_key(value: str) -> bool:
    return (
        bool(value)
        and len(value) <= 128
        and value.isascii()
        and all(33 <= ord(character) <= 126 for character in value)
    )


def _requires_step_up(path: str) -> bool:
    return path == "/ops/kill-switch/clear" or (
        path.startswith("/genes/") and path.endswith("/promote")
    )


def _utc_iso(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=UTC).isoformat()


def _parse_pagination(
    query: dict[str, list[str]],
    *,
    max_limit: int = 500,
) -> tuple[int, int] | HttpResponse:
    try:
        limit = int(_single_query_value(query, "limit") or "100")
        offset = int(_single_query_value(query, "offset") or "0")
    except ValueError:
        return HttpResponse(422, {"error": "validation_error"})
    if limit < 1 or limit > max_limit or offset < 0:
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
