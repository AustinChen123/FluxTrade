"""Regression tests for production deployment hardening."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PROD_COMPOSE = REPO_ROOT / "docker-compose.prod.yml"


def _required_env() -> dict[str, str]:
    return {
        "REDIS_PASSWORD": "redis-secret",
        "POSTGRES_PASSWORD": "postgres-secret",
        "DASHBOARD_PASSWORD": "dashboard-secret",
        "CONTROL_PLANE_API_KEY": "control-plane-secret",
        "GRAFANA_PASSWORD": "grafana-secret",
        "EXCHANGE_API_KEY": "exchange-key",
        "EXCHANGE_SECRET": "exchange-secret",
        "BINANCE_API_KEY": "binance-key",
        "BINANCE_SECRET": "binance-secret",
    }


def _compose_config() -> dict:
    env = os.environ.copy()
    env.update(_required_env())
    result = subprocess.run(
        ["docker", "compose", "-f", str(PROD_COMPOSE), "config", "--format", "json"],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )
    return json.loads(result.stdout)


def test_production_compose_does_not_publish_datastores():
    config = _compose_config()
    services = config["services"]

    assert services["redis"].get("ports") in (None, [])
    assert services["db"].get("ports") in (None, [])


def test_production_compose_requires_sensitive_env_vars():
    text = PROD_COMPOSE.read_text()

    assert "${REDIS_PASSWORD:?REDIS_PASSWORD is required}" in text
    assert "${DASHBOARD_PASSWORD:?DASHBOARD_PASSWORD is required}" in text
    assert "${CONTROL_PLANE_API_KEY:?CONTROL_PLANE_API_KEY is required}" in text


@pytest.mark.parametrize(
    "env_name",
    ["REDIS_PASSWORD", "DASHBOARD_PASSWORD", "CONTROL_PLANE_API_KEY"],
)
def test_production_compose_rejects_missing_sensitive_env_vars(env_name: str):
    env = os.environ.copy()
    env.update(_required_env())
    env.pop(env_name)

    result = subprocess.run(
        ["docker", "compose", "-f", str(PROD_COMPOSE), "config"],
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode != 0
    assert f"{env_name} is required" in result.stderr


def test_production_compose_passes_required_auth_to_services():
    config = _compose_config()
    services = config["services"]
    redis_command = " ".join(services["redis"]["command"])
    redis_healthcheck = " ".join(services["redis"]["healthcheck"]["test"])

    assert services["redis"]["environment"]["REDIS_PASSWORD"] == "redis-secret"
    assert "--requirepass" in redis_command
    assert "redis-cli -a" in redis_healthcheck
    assert services["dashboard"]["environment"]["DASHBOARD_AUTH_REQUIRED"] == "true"
    assert services["dashboard"]["environment"]["DASHBOARD_PASSWORD"] == "dashboard-secret"
    assert (
        services["python-strategy"]["environment"]["CONTROL_PLANE_API_KEY"]
        == "control-plane-secret"
    )
