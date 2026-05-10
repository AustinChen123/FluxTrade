import logging
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, ContextManager, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session
logger = logging.getLogger(__name__)


class ComponentStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class CheckResult:
    status: ComponentStatus
    latency_ms: float
    message: str


@dataclass
class HealthReport:
    status: ComponentStatus
    redis: CheckResult
    database: CheckResult
    exchange: CheckResult
    uptime_seconds: float
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "redis": {
                "status": self.redis.status.value,
                "latency_ms": self.redis.latency_ms,
                "message": self.redis.message,
            },
            "database": {
                "status": self.database.status.value,
                "latency_ms": self.database.latency_ms,
                "message": self.database.message,
            },
            "exchange": {
                "status": self.exchange.status.value,
                "latency_ms": self.exchange.latency_ms,
                "message": self.exchange.message,
            },
            "uptime_seconds": self.uptime_seconds,
            "timestamp": self.timestamp,
        }


class HealthChecker:
    def __init__(
        self,
        redis_client,
        db_session=None,
        adapter=None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
    ):
        self.redis_client = redis_client
        self._db_session_factory = db_session_factory or (lambda: nullcontext(db_session))
        self.adapter = adapter
        self._start_time = time.monotonic()

    def check_redis(self) -> CheckResult:
        start = time.monotonic()
        try:
            self.redis_client.ping()
            latency = (time.monotonic() - start) * 1000
            return CheckResult(
                status=ComponentStatus.HEALTHY,
                latency_ms=round(latency, 2),
                message="ok",
            )
        except Exception as e:
            latency = (time.monotonic() - start) * 1000
            return CheckResult(
                status=ComponentStatus.UNHEALTHY,
                latency_ms=round(latency, 2),
                message=str(e),
            )

    def check_database(self) -> CheckResult:
        start = time.monotonic()
        try:
            with self._db_session_factory() as db_session:
                db_session.execute(text("SELECT 1"))
            latency = (time.monotonic() - start) * 1000
            return CheckResult(
                status=ComponentStatus.HEALTHY,
                latency_ms=round(latency, 2),
                message="ok",
            )
        except Exception as e:
            latency = (time.monotonic() - start) * 1000
            return CheckResult(
                status=ComponentStatus.UNHEALTHY,
                latency_ms=round(latency, 2),
                message=str(e),
            )

    def check_exchange(self) -> CheckResult:
        if self.adapter is None:
            return CheckResult(
                status=ComponentStatus.HEALTHY,
                latency_ms=0.0,
                message="no adapter configured",
            )
        start = time.monotonic()
        try:
            self.adapter.get_balance("USDT")
            latency = (time.monotonic() - start) * 1000
            return CheckResult(
                status=ComponentStatus.HEALTHY,
                latency_ms=round(latency, 2),
                message="ok",
            )
        except Exception as e:
            latency = (time.monotonic() - start) * 1000
            return CheckResult(
                status=ComponentStatus.DEGRADED,
                latency_ms=round(latency, 2),
                message=str(e),
            )

    def get_status(self) -> HealthReport:
        redis_result = self.check_redis()
        db_result = self.check_database()
        exchange_result = self.check_exchange()

        # Aggregate: worst component wins
        component_statuses = [redis_result.status, db_result.status, exchange_result.status]
        if ComponentStatus.UNHEALTHY in component_statuses:
            overall = ComponentStatus.UNHEALTHY
        elif ComponentStatus.DEGRADED in component_statuses:
            overall = ComponentStatus.DEGRADED
        else:
            overall = ComponentStatus.HEALTHY

        uptime = time.monotonic() - self._start_time

        return HealthReport(
            status=overall,
            redis=redis_result,
            database=db_result,
            exchange=exchange_result,
            uptime_seconds=round(uptime, 2),
        )
