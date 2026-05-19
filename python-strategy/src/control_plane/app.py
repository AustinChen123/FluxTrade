from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.models import (
    BacktestJobRequest,
    JobRecord,
    StrategyCommandRequest,
)
from src.control_plane.strategy_control import StrategyControlService


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
        strategy_control: StrategyControlService | None = None,
    ) -> None:
        self.backtest_executor = backtest_executor
        self.strategy_control = strategy_control

    def handle(
        self,
        method: str,
        path: str,
        body: str | bytes | None = None,
    ) -> HttpResponse:
        method = method.upper()
        clean_path = path.rstrip("/") or "/"

        if method == "GET" and clean_path == "/health":
            return HttpResponse(200, {"status": "ok"})

        if method == "POST" and clean_path == "/jobs/backtests":
            return self._submit_backtest(body)

        if method == "GET" and clean_path == "/jobs":
            jobs = [self._job_payload(job) for job in self.backtest_executor.store.list()]
            return HttpResponse(200, {"jobs": jobs})

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
