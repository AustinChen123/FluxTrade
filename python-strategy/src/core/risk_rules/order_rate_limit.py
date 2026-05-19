"""Redis-backed order rate-limit risk rule."""

from __future__ import annotations

import time
import uuid
from typing import Callable, Optional

from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus

_TRY_RECORD_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local count = redis.call('ZCARD', KEYS[1])
local next_count = count + 1
if next_count > tonumber(ARGV[4]) then
  return {0, next_count}
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return {1, next_count}
"""


class OrderRateLimitRule:
    """Limit order submissions per strategy over a sliding Redis window."""

    def __init__(
        self,
        config: RiskConfig,
        redis_client,
        *,
        now_ms: Optional[Callable[[], int]] = None,
        window_ms: int = 60_000,
    ) -> None:
        self.config = config
        self.redis_client = redis_client
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self.window_ms = window_ms

    def evaluate(
        self,
        strategy_id: str,
        *,
        timestamp_ms: Optional[int] = None,
    ) -> tuple[RuleStatus, Optional[str]]:
        """Read current order count without recording a new order."""
        timestamp_ms = timestamp_ms if timestamp_ms is not None else self._now_ms()
        key = self._key(strategy_id)
        cutoff = timestamp_ms - self.window_ms
        pipe = self.redis_client.pipeline(transaction=True)
        pipe.zremrangebyscore(key, "-inf", cutoff)
        pipe.zcard(key)
        _, count = pipe.execute()
        next_count = int(count) + 1
        if next_count > self.config.max_orders_per_minute:
            return (
                RuleStatus.REJECT,
                (
                    "order_rate_limit_exceeded: "
                    f"{next_count} > {self.config.max_orders_per_minute}"
                ),
            )
        return RuleStatus.PASS, None

    def record_order(
        self,
        strategy_id: str,
        *,
        timestamp_ms: Optional[int] = None,
        member: Optional[str] = None,
    ):
        """Record a passed order in Redis."""
        timestamp_ms = timestamp_ms if timestamp_ms is not None else self._now_ms()
        key = self._key(strategy_id)
        member = member or self._member(timestamp_ms)
        pipe = self.redis_client.pipeline(transaction=True)
        pipe.zadd(key, {member: timestamp_ms})
        pipe.expire(key, self.window_ms // 1000)
        return pipe.execute()

    def try_record_order(
        self,
        strategy_id: str,
        *,
        timestamp_ms: Optional[int] = None,
        member: Optional[str] = None,
    ) -> tuple[RuleStatus, Optional[str]]:
        """Atomically evaluate and record a new order using Redis Lua."""
        timestamp_ms = timestamp_ms if timestamp_ms is not None else self._now_ms()
        member = member or self._member(timestamp_ms)
        cutoff = timestamp_ms - self.window_ms
        allowed, next_count = self.redis_client.eval(
            _TRY_RECORD_LUA,
            1,
            self._key(strategy_id),
            cutoff,
            timestamp_ms,
            member,
            self.config.max_orders_per_minute,
            self.window_ms // 1000,
        )
        if int(allowed) == 1:
            return RuleStatus.PASS, None
        return (
            RuleStatus.REJECT,
            f"order_rate_limit_exceeded: {int(next_count)} > {self.config.max_orders_per_minute}",
        )

    def _key(self, strategy_id: str) -> str:
        return f"risk:orders:{strategy_id}:1min"

    @staticmethod
    def _member(timestamp_ms: int) -> str:
        return f"{timestamp_ms}:{uuid.uuid4().hex}"
