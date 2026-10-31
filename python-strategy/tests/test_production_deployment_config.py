"""Regression tests for production deployment hardening."""

from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
import subprocess
import textwrap
import tomllib
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PROD_COMPOSE = REPO_ROOT / "docker-compose.prod.yml"
PYTHON_DOCKERFILE = REPO_ROOT / "python-strategy" / "Dockerfile"
PYTHON_PYPROJECT = REPO_ROOT / "python-strategy" / "pyproject.toml"
ROOT_DOCKERIGNORE = REPO_ROOT / ".dockerignore"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


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
        "EXCHANGE_ID": "binance",
        "INSTRUMENT_PRODUCT_IDS": "BINANCE:BTCUSDT-PERP",
        "MARKET_DATA_SYMBOLS": "BTCUSDT",
        "FLUXTRADE_SECRETS_DIR": "/private/tmp/fluxtrade-test-secrets",
        "EXCHANGE_API_KEY": "exchange-key",
        "EXCHANGE_SECRET": "exchange-secret",
        "BINANCE_API_KEY": "binance-key",
        "BINANCE_ACCOUNT_ALIAS": "futures-main",
        "BINANCE_SECRET": "binance-secret",
        "BYBIT_USER_ID": "123456789",
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


def test_python_ci_treats_warnings_as_errors():
    workflow = CI_WORKFLOW.read_text()

    assert (
        'run: uv run pytest -W error -m "not integration and not slow" -n 4 '
        "--cov=src --cov-report=term-missing --cov-report=xml:coverage.xml "
        "--cov-fail-under=77"
    ) in workflow
    assert 'run: uv run pytest -W error -m "integration or rust" -v' in workflow
    assert "  push:\n    branches: ['**']\n  pull_request:\n" in workflow
    assert 'timeout-minutes: 60' in workflow
    assert 'timeout-minutes: 90' in workflow
    assert "--timeout=1800" in workflow
    assert "github.event_name == 'push'" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "github.ref == 'refs/heads/develop'" in workflow
    assert 'python-slow, rust-check, secret-scan' in workflow
    assert "schedule:" not in workflow
    assert 'slow_result_accepted.call("pull_request", "refs/pull/7/merge", "success")' in workflow
    assert '!slow_result_accepted.call("pull_request", "refs/pull/7/merge", "skipped")' in workflow
    assert 'UV_LOCKED: "true"' in workflow
    assert 'group: ${{ github.workflow }}-${{ github.event_name }}-${{ github.ref }}' in workflow
    assert "PYO3_PYTHON=$(uv run --no-sync python" in workflow
    assert "import sys, fluxtrade_core; print(sys.version); print(fluxtrade_core.__file__)" in workflow
    assert 'addopts = "-v --timeout=300"' in PYTHON_PYPROJECT.read_text()


def test_ci_required_jobs_manifest_matches_workflow():
    workflow_bytes = CI_WORKFLOW.read_bytes()
    manifest = json.loads(
        (REPO_ROOT / ".github" / "refactor-required-jobs.json").read_text()
    )

    assert hashlib.sha256(workflow_bytes).hexdigest() == manifest["workflow_sha256"]
    assert manifest["required_job_ids"] == [
        "frontend-check",
        "integration-check",
        "python-check",
        "python-slow",
        "rust-check",
        "secret-scan",
    ]
    assert manifest["excluded_job_ids"] == []
    assert '  python-slow:' in workflow_bytes.decode()


@pytest.mark.parametrize(
    ("event_name", "event_ref", "slow_result", "expected_success"),
    [
        ("pull_request", "refs/pull/7/merge", "success", True),
        ("pull_request", "refs/pull/7/merge", "skipped", False),
        ("pull_request", "refs/pull/7/merge", "failure", False),
        ("pull_request", "refs/pull/7/merge", "cancelled", False),
        ("push", "refs/heads/feature", "skipped", True),
        ("push", "refs/heads/feature", "success", False),
        ("push", "refs/heads/main", "failure", False),
        ("push", "refs/heads/main", "success", True),
        ("workflow_dispatch", "refs/heads/feature", "success", True),
        ("schedule", "refs/heads/main", "skipped", False),
        ("schedule", "refs/heads/main", "success", False),
        ("unknown", "refs/heads/main", "skipped", False),
    ],
)
def test_refactor_gate_python_slow_event_result_policy(
    event_name: str, event_ref: str, slow_result: str, expected_success: bool
):
    result = _run_refactor_gate(event_name, event_ref, slow_result)
    assert (result.returncode == 0) is expected_success, result.stderr


@pytest.mark.parametrize(
    ("event_name", "event_ref"),
    [
        ("pull_request", "refs/pull/7/merge"),
        ("push", "refs/heads/main"),
        ("push", "refs/heads/develop"),
        ("push", "refs/heads/feature"),
        ("push", "refs/tags/v1"),
        ("workflow_dispatch", "refs/heads/feature"),
    ],
)
def test_python_slow_job_condition_matches_refactor_gate(event_name: str, event_ref: str):
    workflow = CI_WORKFLOW.read_text()
    match = re.search(
        r"  python-slow:\n    name: Python Slow Tests\n    if: >-\n((?:      .*\n)+)", workflow
    )
    assert match is not None
    expression = " ".join(line.strip() for line in match.group(1).splitlines())
    assert re.fullmatch(r"[a-z_.='/ ()|&-]+", expression), expression
    python_expression = (
        expression.replace("github.event_name", "event_name")
        .replace("github.ref", "event_ref")
        .replace("||", " or ")
        .replace("&&", " and ")
    )
    runs = eval(python_expression, {}, {"event_name": event_name, "event_ref": event_ref})
    assert _run_refactor_gate(event_name, event_ref, "success").returncode == (0 if runs else 1)
    assert _run_refactor_gate(event_name, event_ref, "skipped").returncode == (1 if runs else 0)


def _run_refactor_gate(
    event_name: str, event_ref: str, slow_result: str
) -> subprocess.CompletedProcess[str]:
    workflow = CI_WORKFLOW.read_text()
    match = re.search(r"ruby <<'RUBY'\n(.*?)\n          RUBY", workflow, re.DOTALL)
    assert match is not None
    script = textwrap.dedent(match.group(1))
    needs = {
        job_id: {"result": "success"}
        for job_id in (
            "frontend-check",
            "integration-check",
            "python-check",
            "python-slow",
            "rust-check",
            "secret-scan",
        )
    }
    needs["python-slow"]["result"] = slow_result
    env = os.environ.copy()
    env.update(
        NEEDS_JSON=json.dumps(needs),
        GITHUB_EVENT_NAME=event_name,
        GITHUB_REF=event_ref,
    )
    return subprocess.run(
        ["ruby", "-e", script],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _write_env_file(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "\n".join(f"{name}={value}" for name, value in sorted(values.items())) + "\n"
    )


def _compose_env() -> dict[str, str]:
    env = os.environ.copy()
    interpolated_names = set(
        re.findall(r"\$\{([A-Z][A-Z0-9_]*)", PROD_COMPOSE.read_text())
    )
    for name in interpolated_names:
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
    assert "${EXCHANGE_ID:?EXCHANGE_ID is required}" in text
    assert "${INSTRUMENT_PRODUCT_IDS:?INSTRUMENT_PRODUCT_IDS is required}" in text
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
        "EXCHANGE_ID",
        "INSTRUMENT_PRODUCT_IDS",
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
    assert (
        services["dashboard"]["environment"]["DASHBOARD_PASSWORD"] == "dashboard-secret"
    )
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
        services["control-plane"]["environment"]["CONTROL_PLANE_OPERATOR_CAPABILITY"]
        == "example.com/cap/fluxtrade-operator"
    )
    assert (
        services["control-plane"]["environment"]["CONTROL_PLANE_STEP_UP_CAPABILITY"]
        == "example.com/cap/fluxtrade-step-up"
    )
    assert services["rust-data"]["environment"]["FLUXTRADE_ENVIRONMENT"] == "live"
    assert services["python-strategy"]["environment"]["FLUXTRADE_ENVIRONMENT"] == "live"
    assert (
        services["python-strategy"]["environment"]["MARKET_PENDING_CLAIM_IDLE_MS"]
        == "60000"
    )
    assert (
        services["python-strategy"]["environment"]["MARKET_CONSUMER_LEASE_MS"]
        == "10000"
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
    assert rust_data["environment"]["EXCHANGE_ID"] == "binance"
    assert rust_data["environment"]["INSTRUMENT_PRODUCT_IDS"] == "BINANCE:BTCUSDT-PERP"
    assert rust_data["environment"]["MARKET_DATA_SYMBOLS"] == "BTCUSDT"
    assert strategy["environment"]["ADAPTER_MODE"] == "simulated"
    assert strategy["environment"]["AUDIT_EXTERNAL_ORDERS"] == "false"
    assert strategy["environment"]["EXCHANGE_API_KEY"] == "exchange-key"
    assert strategy["environment"]["EXCHANGE_SECRET"] == "exchange-secret"
    assert strategy["environment"]["BINANCE_ACCOUNT_ALIAS"] == "futures-main"
    assert strategy["environment"]["BYBIT_USER_ID"] == "123456789"
    assert (
        strategy["environment"]["INSTRUMENT_PRODUCT_IDS"]
        == rust_data["environment"]["INSTRUMENT_PRODUCT_IDS"]
    )
    assert "BINANCE_SECRET" not in strategy["environment"]


@pytest.mark.integration
def test_production_compose_passes_backpack_market_symbols_only_to_rust_data(
    tmp_path: Path,
):
    omitted = _compose_config(tmp_path)["services"]
    assert omitted["rust-data"]["environment"]["BACKPACK_MARKET_DATA_SYMBOLS"] == ""

    configured_env = _required_env()
    configured_env["BACKPACK_MARKET_DATA_SYMBOLS"] = "BTC_USDC_PERP"
    configured = _compose_config(tmp_path, configured_env)["services"]
    assert (
        configured["rust-data"]["environment"]["BACKPACK_MARKET_DATA_SYMBOLS"]
        == "BTC_USDC_PERP"
    )
    assert (
        "BACKPACK_MARKET_DATA_SYMBOLS"
        not in configured["python-strategy"]["environment"]
    )


def test_compose_test_environment_excludes_all_interpolated_shell_values(
    monkeypatch,
):
    interpolated_names = set(
        re.findall(r"\$\{([A-Z][A-Z0-9_]*)", PROD_COMPOSE.read_text())
    )
    for name in interpolated_names:
        monkeypatch.setenv(name, "inherited-value")

    assert interpolated_names.isdisjoint(_compose_env())


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
    assert any(volume["target"] == "/app/data" for volume in control_plane["volumes"])
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


def test_root_dockerignore_excludes_generated_build_context():
    dockerignore_entries = set(ROOT_DOCKERIGNORE.read_text().splitlines())

    assert {
        "rust-data-service/target/",
        "rust-data-service/tests/",
        "rust-data-service/.venv/",
        "python-strategy/.venv/",
        "python-strategy/tests/",
        "python-strategy/data/",
        "frontend/node_modules/",
        "frontend/dist/",
        "database/venv/",
        "**/.DS_Store",
        "**/__pycache__/",
        "**/.pytest_cache/",
        "**/.ruff_cache/",
    } <= dockerignore_entries


def test_python_production_image_owns_immutable_strategy_artifact_root():
    dockerfile = PYTHON_DOCKERFILE.read_text()

    create_root = "mkdir -p /app/data /app/strategy_artifacts"
    switch_user = "USER trading"
    assert create_root in dockerfile
    assert dockerfile.index(create_root) < dockerfile.index(switch_user)
    assert "chown -R trading:trading /app/strategy_artifacts" not in dockerfile
    assert (
        re.search(r"chown\s+-R\s+trading:trading\s+/app(?:\s|\\|$)", dockerfile) is None
    )


@pytest.mark.integration
def test_production_strategy_source_is_image_owned_not_writable_mount(
    tmp_path: Path,
):
    strategy = _compose_config(tmp_path)["services"]["python-strategy"]

    assert strategy["environment"]["STRATEGY_ARTIFACTS_PATH"] == (
        "/app/strategy_artifacts"
    )
    assert "STRATEGY_BREAK_GLASS_ENABLED" not in strategy["environment"]
    assert all(
        volume["target"] not in {"/app/strategies_hot", "/app/strategy_artifacts"}
        for volume in strategy["volumes"]
    )


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


@pytest.mark.parametrize("mode", [("--self-test",), ()])
def test_frontend_workflow_validator_accepts_current_workflow(mode: tuple[str, ...]):
    result = subprocess.run(
        ["ruby", ".github/scripts/validate-frontend-job.rb", *mode],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
