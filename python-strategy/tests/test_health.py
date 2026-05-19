"""Tests for HealthChecker: component checks, aggregation, and serialization."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.core.health import (
    ComponentStatus,
    HealthChecker,
)


@pytest.fixture
def healthy_redis():
    mock = MagicMock()
    mock.ping.return_value = True
    return mock


@pytest.fixture
def unhealthy_redis():
    mock = MagicMock()
    mock.ping.side_effect = ConnectionError("Connection refused")
    return mock


@pytest.fixture
def healthy_db():
    mock = MagicMock()
    mock.execute.return_value = True
    return mock


@pytest.fixture
def unhealthy_db():
    mock = MagicMock()
    mock.execute.side_effect = Exception("OperationalError: connection lost")
    return mock


@pytest.fixture
def healthy_adapter():
    mock = MagicMock()
    mock.get_balance.return_value = Decimal("1000.00")
    return mock


@pytest.fixture
def unhealthy_adapter():
    mock = MagicMock()
    mock.get_balance.side_effect = Exception("ExchangeError: timeout")
    return mock


class TestCheckRedis:
    def test_healthy_redis(self, healthy_redis, healthy_db):
        checker = HealthChecker(healthy_redis, healthy_db)
        result = checker.check_redis()
        assert result.status == ComponentStatus.HEALTHY
        assert result.message == "ok"
        assert result.latency_ms >= 0

    def test_unhealthy_redis(self, unhealthy_redis, healthy_db):
        checker = HealthChecker(unhealthy_redis, healthy_db)
        result = checker.check_redis()
        assert result.status == ComponentStatus.UNHEALTHY
        assert "Connection refused" in result.message


class TestCheckDatabase:
    def test_healthy_database(self, healthy_redis, healthy_db):
        checker = HealthChecker(healthy_redis, healthy_db)
        result = checker.check_database()
        assert result.status == ComponentStatus.HEALTHY
        assert result.message == "ok"

    def test_unhealthy_database(self, healthy_redis, unhealthy_db):
        checker = HealthChecker(healthy_redis, unhealthy_db)
        result = checker.check_database()
        assert result.status == ComponentStatus.UNHEALTHY
        assert "OperationalError" in result.message


class TestCheckExchange:
    def test_healthy_exchange(self, healthy_redis, healthy_db, healthy_adapter):
        checker = HealthChecker(healthy_redis, healthy_db, adapter=healthy_adapter)
        result = checker.check_exchange()
        assert result.status == ComponentStatus.HEALTHY
        assert result.message == "ok"

    def test_unhealthy_exchange_is_degraded(self, healthy_redis, healthy_db, unhealthy_adapter):
        checker = HealthChecker(healthy_redis, healthy_db, adapter=unhealthy_adapter)
        result = checker.check_exchange()
        assert result.status == ComponentStatus.DEGRADED
        assert "ExchangeError" in result.message

    def test_no_adapter_returns_healthy(self, healthy_redis, healthy_db):
        checker = HealthChecker(healthy_redis, healthy_db, adapter=None)
        result = checker.check_exchange()
        assert result.status == ComponentStatus.HEALTHY
        assert result.message == "no adapter configured"


class TestGetStatus:
    def test_all_healthy(self, healthy_redis, healthy_db, healthy_adapter):
        checker = HealthChecker(healthy_redis, healthy_db, adapter=healthy_adapter)
        report = checker.get_status()
        assert report.status == ComponentStatus.HEALTHY
        assert report.uptime_seconds >= 0

    def test_unhealthy_component_makes_overall_unhealthy(self, unhealthy_redis, healthy_db):
        checker = HealthChecker(unhealthy_redis, healthy_db)
        report = checker.get_status()
        assert report.status == ComponentStatus.UNHEALTHY

    def test_degraded_exchange_makes_overall_degraded(self, healthy_redis, healthy_db, unhealthy_adapter):
        checker = HealthChecker(healthy_redis, healthy_db, adapter=unhealthy_adapter)
        report = checker.get_status()
        assert report.status == ComponentStatus.DEGRADED


class TestHealthReportSerialization:
    def test_to_dict_structure(self, healthy_redis, healthy_db):
        checker = HealthChecker(healthy_redis, healthy_db)
        report = checker.get_status()
        d = report.to_dict()

        assert "status" in d
        assert "redis" in d
        assert "database" in d
        assert "exchange" in d
        assert "uptime_seconds" in d
        assert "timestamp" in d

        # Nested structure
        assert d["redis"]["status"] == "healthy"
        assert "latency_ms" in d["redis"]
        assert "message" in d["redis"]

    def test_to_dict_values_are_json_serializable(self, healthy_redis, healthy_db):
        """All values must be JSON-serializable (no Enum objects)."""
        import json

        checker = HealthChecker(healthy_redis, healthy_db)
        report = checker.get_status()
        d = report.to_dict()

        # Should not raise
        serialized = json.dumps(d)
        assert isinstance(serialized, str)
