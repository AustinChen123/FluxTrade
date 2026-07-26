from __future__ import annotations

import os

from src.control_plane import (
    BacktestJobExecutor,
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
    )


def main() -> None:
    host = os.getenv("CONTROL_PLANE_HOST", "127.0.0.1")
    port = int(os.getenv("CONTROL_PLANE_PORT", "8080"))
    app = build_control_plane_app(api_key=os.getenv("CONTROL_PLANE_API_KEY"))
    serve(app, host=host, port=port)


if __name__ == "__main__":
    main()
