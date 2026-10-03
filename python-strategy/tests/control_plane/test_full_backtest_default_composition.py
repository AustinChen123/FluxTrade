from __future__ import annotations

from pathlib import Path

from src.control_plane.jobs import InMemoryJobStore, JobStatus
from src.control_plane.main import build_control_plane_app
from src.core.strategy_loader import StrategyLoader
from test_full_backtest_resolution import _request


def _app(*, store, **kwargs):
    from unittest.mock import MagicMock

    return build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=kwargs.pop("db_session_factory", None),
        job_store=store,
        api_key="operator-key",
        readiness_probe=lambda: None,
        profile_query_service=MagicMock(),
        **kwargs,
    )


def test_injected_catalog_loader_is_composed_without_eager_invocation():
    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        return {}

    store = InMemoryJobStore()
    app = _app(store=store, strategy_loader=loader)
    try:
        assert app.backtest_executor._strategy_loader is loader
        assert calls == 0
    finally:
        assert app.shutdown(timeout=1)


def test_default_artifact_path_uses_existing_fallback_and_is_captured(
    tmp_path: Path, monkeypatch
):
    monkeypatch.delenv("STRATEGY_ARTIFACTS_PATH", raising=False)
    scanned: list[str] = []

    def scan(path: str):
        scanned.append(path)
        return {}

    monkeypatch.setattr(StrategyLoader, "scan_production_sources", scan)
    app = _app(store=InMemoryJobStore())
    try:
        assert scanned == []
        monkeypatch.setenv("STRATEGY_ARTIFACTS_PATH", str(tmp_path / "other_path"))
        loader = app.backtest_executor._strategy_loader
        assert loader is not None
        assert loader() == {}
        assert scanned == ["/app/strategy_artifacts"]
    finally:
        assert app.shutdown(timeout=1)


def test_default_missing_catalog_fails_job_lazily_after_builder_returns(
    tmp_path: Path, monkeypatch
):
    catalog_path = tmp_path / "empty_catalog"
    catalog_path.mkdir()
    monkeypatch.setenv("STRATEGY_ARTIFACTS_PATH", str(catalog_path))
    scans: list[str] = []
    original_scan = StrategyLoader.scan_production_sources

    def count_scan(path: str, *, break_glass_path: str | None = None):
        scans.append(path)
        return original_scan(path, break_glass_path=break_glass_path)

    monkeypatch.setattr(StrategyLoader, "scan_production_sources", count_scan)
    store = InMemoryJobStore()

    def unused_session_factory():
        raise AssertionError("missing catalog must fail before database access")

    app = _app(store=store, db_session_factory=unused_session_factory)
    try:
        assert scans == []
        monkeypatch.setenv("STRATEGY_ARTIFACTS_PATH", str(tmp_path / "wrong_path"))
        response = app.handle(
            "POST",
            "/jobs/backtests",
            body=_request().model_dump_json(exclude_none=False),
            headers={"Authorization": "Bearer operator-key"},
        )
        assert response.status_code == 202
        job_id = response.body["job"]["id"]
        assert app.backtest_executor.shutdown(timeout=10)
        failed = store.get(job_id)
        assert failed is not None
        assert failed.status is JobStatus.FAILED
        assert failed.error == "browser_result_subject_unavailable"
        assert scans == [str(catalog_path)]
    finally:
        assert app.shutdown(timeout=10)
