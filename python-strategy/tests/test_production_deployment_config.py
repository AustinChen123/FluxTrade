"""Regression tests for production deployment hardening."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PROD_COMPOSE = REPO_ROOT / "docker-compose.prod.yml"
PYTHON_DOCKERFILE = REPO_ROOT / "python-strategy" / "Dockerfile"
PYTHON_PYPROJECT = REPO_ROOT / "python-strategy" / "pyproject.toml"
ROOT_DOCKERIGNORE = REPO_ROOT / ".dockerignore"


def _required_env() -> dict[str, str]:
    return {
        "FLUXTRADE_ENVIRONMENT": "live",
        "ADAPTER_MODE": "simulated",
        "AUDIT_EXTERNAL_ORDERS": "false",
        "REDIS_PASSWORD": "redis-secret",
        "POSTGRES_PASSWORD": "postgres-secret",
        "DASHBOARD_PASSWORD": "dashboard-secret",
        "CONTROL_PLANE_API_KEY": "control-plane-secret",
        "CONTROL_PLANE_TRUSTED_PROXY_AUTH": "true",
        "CONTROL_PLANE_BROWSER_ORIGIN": "https://fluxtrade.example.ts.net",
        "CONTROL_PLANE_OPERATOR_CAPABILITY": "example.com/cap/fluxtrade-operator",
        "CONTROL_PLANE_STEP_UP_CAPABILITY": "example.com/cap/fluxtrade-step-up",
        "GRAFANA_PASSWORD": "grafana-secret",
        "EXCHANGE_ENABLED": "binance",
        "MARKET_DATA_SYMBOLS": "BTCUSDT",
        "FLUXTRADE_SECRETS_DIR": "/private/tmp/fluxtrade-test-secrets",
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


def _compose_config(
    tmp_path: Path,
    values: dict[str, str] | None = None,
) -> dict:
    env_file = tmp_path / "compose.env"
    _write_env_file(env_file, values or _required_env())
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


@pytest.mark.integration
def test_production_compose_only_publishes_operator_endpoints_on_localhost(
    tmp_path: Path,
):
    services = _compose_config(tmp_path)["services"]

    assert services["rust-data"].get("ports") in (None, [])
    assert services["python-strategy"].get("ports") in (None, [])
    assert services["prometheus"].get("ports") in (None, [])
    for service_name in ["control-plane", "dashboard", "grafana"]:
        ports = services[service_name]["ports"]
        assert ports
        assert {port["host_ip"] for port in ports} == {"127.0.0.1"}


def test_production_compose_requires_sensitive_env_vars():
    text = PROD_COMPOSE.read_text()

    assert "${REDIS_PASSWORD:?REDIS_PASSWORD is required}" in text
    assert "${DASHBOARD_PASSWORD:?DASHBOARD_PASSWORD is required}" in text
    assert "${CONTROL_PLANE_API_KEY:?CONTROL_PLANE_API_KEY is required}" in text
    assert "${FLUXTRADE_ENVIRONMENT:?FLUXTRADE_ENVIRONMENT is required}" in text
    assert "${ADAPTER_MODE:?ADAPTER_MODE is required}" in text
    assert "${AUDIT_EXTERNAL_ORDERS:?AUDIT_EXTERNAL_ORDERS is required}" in text
    assert "${EXCHANGE_ENABLED:?EXCHANGE_ENABLED is required}" in text
    assert "${MARKET_DATA_SYMBOLS:?MARKET_DATA_SYMBOLS is required}" in text
    assert "${FLUXTRADE_SECRETS_DIR:?FLUXTRADE_SECRETS_DIR is required}" in text


@pytest.mark.parametrize(
    "env_name",
    [
        "REDIS_PASSWORD",
        "DASHBOARD_PASSWORD",
        "CONTROL_PLANE_API_KEY",
        "FLUXTRADE_ENVIRONMENT",
        "ADAPTER_MODE",
        "AUDIT_EXTERNAL_ORDERS",
        "EXCHANGE_ENABLED",
        "MARKET_DATA_SYMBOLS",
        "FLUXTRADE_SECRETS_DIR",
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
        services["control-plane"]["environment"]["CONTROL_PLANE_API_KEY"]
        == "control-plane-secret"
    )
    assert (
        services["control-plane"]["environment"]["CONTROL_PLANE_TRUSTED_PROXY_AUTH"]
        == "true"
    )
    assert (
        services["control-plane"]["environment"]["CONTROL_PLANE_BROWSER_ORIGIN"]
        == "https://fluxtrade.example.ts.net"
    )
    assert (
        services["control-plane"]["environment"][
            "CONTROL_PLANE_OPERATOR_CAPABILITY"
        ]
        == "example.com/cap/fluxtrade-operator"
    )
    assert (
        services["control-plane"]["environment"]["CONTROL_PLANE_STEP_UP_CAPABILITY"]
        == "example.com/cap/fluxtrade-step-up"
    )
    assert services["rust-data"]["environment"]["FLUXTRADE_ENVIRONMENT"] == "live"
    assert (
        services["python-strategy"]["environment"]["FLUXTRADE_ENVIRONMENT"]
        == "live"
    )


@pytest.mark.integration
def test_production_compose_wires_runtime_configuration_without_demo_overrides(
    tmp_path: Path,
):
    services = _compose_config(tmp_path)["services"]
    rust_data = services["rust-data"]
    strategy = services["python-strategy"]

    assert rust_data["command"] == ["live"]
    assert rust_data["environment"]["EXCHANGE_ENABLED"] == "binance"
    assert rust_data["environment"]["MARKET_DATA_SYMBOLS"] == "BTCUSDT"
    assert strategy["environment"]["ADAPTER_MODE"] == "simulated"
    assert strategy["environment"]["AUDIT_EXTERNAL_ORDERS"] == "false"
    assert strategy["environment"]["EXCHANGE_API_KEY"] == "exchange-key"
    assert strategy["environment"]["EXCHANGE_SECRET"] == "exchange-secret"
    assert "BINANCE_SECRET" not in strategy["environment"]


@pytest.mark.integration
def test_production_compose_mounts_private_runtime_credentials_read_only(
    tmp_path: Path,
):
    services = _compose_config(tmp_path)["services"]

    for service_name in ["rust-data", "python-strategy"]:
        service = services[service_name]
        assert (
            service["environment"]["FLUXTRADE_CREDENTIALS_PATH"]
            == "/run/secrets/fluxtrade/credentials.toml"
        )
        secret_mount = next(
            volume
            for volume in service["volumes"]
            if volume["target"] == "/run/secrets/fluxtrade"
        )
        assert secret_mount["read_only"] is True


@pytest.mark.integration
def test_production_compose_runs_persistent_control_plane(tmp_path: Path):
    services = _compose_config(tmp_path)["services"]
    control_plane = services["control-plane"]

    assert control_plane["command"] == [
        "python",
        "-m",
        "src.control_plane.main",
    ]
    assert (
        control_plane["environment"]["CONTROL_PLANE_JOB_DB_PATH"]
        == "/app/data/jobs.sqlite3"
    )
    assert any(
        volume["target"] == "/app/data"
        for volume in control_plane["volumes"]
    )
    assert control_plane["healthcheck"]["test"][0:2] == ["CMD", "python"]


@pytest.mark.integration
def test_database_backed_services_wait_for_migrations(tmp_path: Path):
    services = _compose_config(tmp_path)["services"]
    migration = services["migrate"]

    assert migration["command"] == [
        "alembic",
        "-c",
        "/app/database/alembic.ini",
        "upgrade",
        "head",
    ]
    assert migration["restart"] == "no"
    assert migration["depends_on"]["db"]["condition"] == "service_healthy"
    for service_name in [
        "rust-data",
        "python-strategy",
        "control-plane",
        "dashboard",
    ]:
        assert (
            services[service_name]["depends_on"]["migrate"]["condition"]
            == "service_completed_successfully"
        )


@pytest.mark.integration
def test_production_service_images_use_explicit_version_tags(tmp_path: Path):
    services = _compose_config(tmp_path)["services"]

    for service_name in ["redis", "db", "prometheus", "grafana"]:
        image = services[service_name]["image"]
        assert ":latest" not in image
        assert not image.endswith(":alpine")


@pytest.mark.integration
def test_python_services_use_clean_checkout_extension_image(tmp_path: Path):
    services = _compose_config(tmp_path)["services"]

    for service_name in ["migrate", "python-strategy", "control-plane"]:
        service = services[service_name]
        assert service["image"] == "fluxtrade-python-strategy:local"
        assert service["build"]["context"] == str(REPO_ROOT)
        assert service["build"]["dockerfile"] == "python-strategy/Dockerfile"


def test_python_production_image_contains_migration_runtime_contract():
    dockerfile = PYTHON_DOCKERFILE.read_text()
    dockerignore_entries = set(ROOT_DOCKERIGNORE.read_text().splitlines())
    pyproject = tomllib.loads(PYTHON_PYPROJECT.read_text())
    runtime_dependencies = pyproject["project"]["dependencies"]
    dev_dependencies = pyproject["dependency-groups"]["dev"]

    assert "FROM rust:1.82.0-bookworm AS extension-builder" in dockerfile
    assert "cargo build --lib --release" in dockerfile
    assert "uv sync --frozen --no-dev" in dockerfile
    assert (
        "COPY --from=extension-builder "
        "/build/target/release/libfluxtrade_core.so "
        "./fluxtrade_core.so"
    ) in dockerfile
    assert "COPY database/alembic.ini ./database/alembic.ini" in dockerfile
    assert "COPY database/alembic/env.py ./database/alembic/env.py" in dockerfile
    assert "COPY database/alembic/versions/ ./database/alembic/versions/" in dockerfile
    assert any(dependency.startswith("alembic") for dependency in runtime_dependencies)
    assert not any(dependency.startswith("alembic") for dependency in dev_dependencies)
    assert {
        "python-strategy/src/fluxtrade_core.so",
        "!database/alembic.ini",
        "!database/alembic/env.py",
        "!database/alembic/versions/*.py",
    } <= dockerignore_entries


@pytest.mark.integration
def test_runtime_images_can_be_overridden_for_private_builds(tmp_path: Path):
    env = _required_env()
    env.update(
        {
            "RUST_DATA_IMAGE": "registry.example/rust-rithmic:approved",
            "PYTHON_STRATEGY_IMAGE": "registry.example/python-rithmic:approved",
        }
    )
    services = _compose_config(tmp_path, env)["services"]

    assert services["rust-data"]["image"] == env["RUST_DATA_IMAGE"]
    assert services["migrate"]["image"] == env["PYTHON_STRATEGY_IMAGE"]
    assert services["python-strategy"]["image"] == env["PYTHON_STRATEGY_IMAGE"]
    assert services["control-plane"]["image"] == env["PYTHON_STRATEGY_IMAGE"]
