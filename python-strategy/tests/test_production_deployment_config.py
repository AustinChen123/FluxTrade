"""Regression tests for production deployment hardening."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PROD_COMPOSE = REPO_ROOT / "docker-compose.prod.yml"


def _required_env() -> dict[str, str]:
    return {
        "FLUXTRADE_ENVIRONMENT": "live",
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


def _compose_base_command() -> list[str]:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose CLI is required")

    result = subprocess.run(
        [docker, "compose", "version"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip("Docker Compose CLI is required")
    return [docker, "compose"]


def _write_env_file(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "\n".join(f"{name}={value}" for name, value in sorted(values.items())) + "\n"
    )


def _compose_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in _required_env():
        env.pop(name, None)
    return env


def _compose_config(tmp_path: Path) -> dict:
    env_file = tmp_path / "compose.env"
    _write_env_file(env_file, _required_env())
    result = subprocess.run(
        [
            *_compose_base_command(),
            "--env-file",
            str(env_file),
            "-f",
            str(PROD_COMPOSE),
            "config",
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        cwd=tmp_path,
        env=_compose_env(),
        text=True,
    )
    return json.loads(result.stdout)


@pytest.mark.integration
def test_production_compose_does_not_publish_datastores(tmp_path: Path):
    config = _compose_config(tmp_path)
    services = config["services"]

    assert services["redis"].get("ports") in (None, [])
    assert services["db"].get("ports") in (None, [])


def test_production_compose_requires_sensitive_env_vars():
    text = PROD_COMPOSE.read_text()

    assert "${REDIS_PASSWORD:?REDIS_PASSWORD is required}" in text
    assert "${DASHBOARD_PASSWORD:?DASHBOARD_PASSWORD is required}" in text
    assert "${CONTROL_PLANE_API_KEY:?CONTROL_PLANE_API_KEY is required}" in text
    assert "${FLUXTRADE_ENVIRONMENT:?FLUXTRADE_ENVIRONMENT is required}" in text


@pytest.mark.parametrize(
    "env_name",
    [
        "REDIS_PASSWORD",
        "DASHBOARD_PASSWORD",
        "CONTROL_PLANE_API_KEY",
        "FLUXTRADE_ENVIRONMENT",
    ],
)
@pytest.mark.integration
def test_production_compose_rejects_missing_sensitive_env_vars(
    tmp_path: Path,
    env_name: str,
):
    env_file = tmp_path / "compose.env"
    compose_env = _required_env()
    compose_env.pop(env_name)
    _write_env_file(env_file, compose_env)

    result = subprocess.run(
        [
            *_compose_base_command(),
            "--env-file",
            str(env_file),
            "-f",
            str(PROD_COMPOSE),
            "config",
        ],
        capture_output=True,
        cwd=tmp_path,
        env=_compose_env(),
        text=True,
    )

    assert result.returncode != 0
    assert f"{env_name} is required" in result.stderr


@pytest.mark.integration
def test_production_compose_passes_required_auth_to_services(tmp_path: Path):
    config = _compose_config(tmp_path)
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
    assert services["rust-data"]["environment"]["FLUXTRADE_ENVIRONMENT"] == "live"
    assert (
        services["python-strategy"]["environment"]["FLUXTRADE_ENVIRONMENT"]
        == "live"
    )
