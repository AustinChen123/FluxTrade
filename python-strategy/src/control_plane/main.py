from __future__ import annotations

import os

from src.control_plane import (
    BacktestJobExecutor,
    BrowserAuthProvider,
    BrowserSessionAuth,
    ControlPlaneApp,
    CsvSignalBacktestParameterEvaluator,
    GeneControlService,
    GoldenCrossResearchParameterEvaluator,
    InMemoryJobStore,
    ParameterSearchJobExecutor,
    ParameterSearchEvaluator,
    ParameterSearchEvaluatorRegistry,
    RedisStrategyCommandRouter,
    SqliteJobStore,
    StrategyControlService,
    StrategyStateQueryService,
)
from src.control_plane.jobs import JobStore
from src.control_plane.server import serve
from src.core.db import SessionLocal
from src.core.redis_factory import create_redis_client


def build_control_plane_app(
    *,
    redis_client=None,
    db_session_factory=SessionLocal,
    job_store: JobStore | None = None,
    parameter_search_evaluator: ParameterSearchEvaluator | None = None,
    api_key: str | None = None,
    browser_auth: BrowserAuthProvider | None = None,
) -> ControlPlaneApp:
    if redis_client is None:
        redis_client = create_redis_client()
    if job_store is None:
        job_db_path = os.getenv("CONTROL_PLANE_JOB_DB_PATH")
        job_store = (
            SqliteJobStore(job_db_path) if job_db_path else InMemoryJobStore()
        )

    state_query = StrategyStateQueryService(db_session_factory)
    recover_interrupted = isinstance(job_store, SqliteJobStore)
    if parameter_search_evaluator is None:
        parameter_search_evaluator = ParameterSearchEvaluatorRegistry(
            {
                "csv_signal": CsvSignalBacktestParameterEvaluator(
                    db_session_factory=db_session_factory
                ),
                "golden_cross": GoldenCrossResearchParameterEvaluator(),
            }
        )
    return ControlPlaneApp(
        BacktestJobExecutor(
            store=job_store,
            db_session_factory=db_session_factory,
            recover_interrupted=recover_interrupted,
        ),
        parameter_search_executor=ParameterSearchJobExecutor(
            parameter_search_evaluator,
            store=job_store,
            db_session_factory=db_session_factory,
        ),
        gene_control=GeneControlService(db_session_factory),
        strategy_control=StrategyControlService(
            RedisStrategyCommandRouter(redis_client, state_query)
        ),
        strategy_state_query=state_query,
        api_key=api_key,
        redis_client=redis_client,
        browser_auth=browser_auth,
    )


def build_browser_session_auth_from_env() -> BrowserSessionAuth | None:
    trusted_proxy_auth = os.getenv(
        "CONTROL_PLANE_TRUSTED_PROXY_AUTH",
        "false",
    ).lower()
    if trusted_proxy_auth not in {"true", "false"}:
        raise ValueError("CONTROL_PLANE_TRUSTED_PROXY_AUTH must be true or false")
    browser_values = {
        "allowed_origin": os.getenv("CONTROL_PLANE_BROWSER_ORIGIN", ""),
        "operator_capability": os.getenv(
            "CONTROL_PLANE_OPERATOR_CAPABILITY",
            "",
        ),
        "step_up_capability": os.getenv(
            "CONTROL_PLANE_STEP_UP_CAPABILITY",
            "",
        ),
    }
    if trusted_proxy_auth == "false":
        if any(browser_values.values()):
            raise ValueError(
                "browser auth settings require CONTROL_PLANE_TRUSTED_PROXY_AUTH=true"
            )
        return None
    if not all(browser_values.values()):
        raise ValueError(
            "trusted proxy auth requires browser origin and capability names"
        )
    return BrowserSessionAuth(
        allowed_origin=browser_values["allowed_origin"],
        operator_capability=browser_values["operator_capability"],
        step_up_capability=browser_values["step_up_capability"],
        session_ttl_seconds=_positive_env_int(
            "CONTROL_PLANE_SESSION_TTL_SECONDS",
            28_800,
        ),
        step_up_ttl_seconds=_positive_env_int(
            "CONTROL_PLANE_STEP_UP_TTL_SECONDS",
            300,
        ),
    )


def _positive_env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def main() -> None:
    host = os.getenv("CONTROL_PLANE_HOST", "127.0.0.1")
    port = int(os.getenv("CONTROL_PLANE_PORT", "8080"))
    static_dir = os.getenv("CONTROL_PLANE_STATIC_DIR") or None
    api_key = os.getenv("CONTROL_PLANE_API_KEY")
    browser_auth = build_browser_session_auth_from_env()
    if static_dir is not None and api_key and browser_auth is None:
        static_dir = None
    app = build_control_plane_app(
        api_key=api_key,
        browser_auth=browser_auth,
    )
    serve(app, host=host, port=port, static_dir=static_dir)


if __name__ == "__main__":
    main()
