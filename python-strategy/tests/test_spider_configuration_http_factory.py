"""Production factory opt-in contract for Spider HTTP metadata."""

from unittest.mock import MagicMock

import pytest

from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.main import build_control_plane_app


def test_production_factory_scope_is_opt_in_and_explicit_value_wins(monkeypatch):
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "research")
    monkeypatch.delenv("CONTROL_PLANE_SPIDER_EXECUTION_SCOPE_ID", raising=False)

    def build(**kwargs):
        return build_control_plane_app(
            redis_client=MagicMock(),
            job_store=InMemoryJobStore(),
            parameter_search_evaluator=MagicMock(),
            strategy_loader=lambda: {},
            api_key="spider-http-test-key",
            readiness_probe=lambda: None,
            **kwargs,
        )

    disabled = build()
    try:
        assert disabled.spider_configuration_http_service is None
    finally:
        disabled.shutdown(timeout=0)

    monkeypatch.setenv("CONTROL_PLANE_SPIDER_EXECUTION_SCOPE_ID", "env-scope")
    enabled = build(spider_execution_scope_id="explicit-scope")
    try:
        assert enabled.spider_configuration_http_service is not None
        assert (
            enabled.spider_configuration_http_service._execution_scope_id
            == "explicit-scope"
        )
    finally:
        enabled.shutdown(timeout=0)

    with pytest.raises(ValueError, match="spider_configuration_scope_invalid"):
        build(spider_execution_scope_id="invalid scope")
