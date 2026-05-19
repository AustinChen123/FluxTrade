"""Tests for Redis-backed order rate-limit risk rule."""

from __future__ import annotations

import threading
from decimal import Decimal

from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.order_rate_limit import OrderRateLimitRule


class _FakePipeline:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.commands = []

    def zremrangebyscore(self, key, min_score, max_score):
        self.commands.append(("zremrangebyscore", key, min_score, max_score))
        return self

    def zcard(self, key):
        self.commands.append(("zcard", key))
        return self

    def zadd(self, key, mapping):
        self.commands.append(("zadd", key, mapping))
        return self

    def expire(self, key, seconds):
        self.commands.append(("expire", key, seconds))
        return self

    def execute(self):
        results = []
        with self.redis_client.lock:
            for command in self.commands:
                name = command[0]
                if name == "zremrangebyscore":
                    _, key, _min_score, max_score = command
                    before = len(self.redis_client.zsets.get(key, {}))
                    self.redis_client.zsets[key] = {
                        member: score
                        for member, score in self.redis_client.zsets.get(key, {}).items()
                        if score > int(max_score)
                    }
                    results.append(before - len(self.redis_client.zsets[key]))
                elif name == "zcard":
                    _, key = command
                    results.append(len(self.redis_client.zsets.get(key, {})))
                elif name == "zadd":
                    _, key, mapping = command
                    self.redis_client.zsets.setdefault(key, {}).update(mapping)
                    results.append(len(mapping))
                elif name == "expire":
                    _, key, seconds = command
                    self.redis_client.expiry[key] = seconds
                    results.append(True)
        return results


class _FakeRedis:
    def __init__(self):
        self.zsets = {}
        self.expiry = {}
        self.lock = threading.Lock()

    def pipeline(self, transaction=True):
        assert transaction is True
        return _FakePipeline(self)

    def eval(self, script, numkeys, key, cutoff, timestamp_ms, member, max_orders, ttl):
        assert numkeys == 1
        with self.lock:
            self.zsets[key] = {
                existing_member: score
                for existing_member, score in self.zsets.get(key, {}).items()
                if score > int(cutoff)
            }
            next_count = len(self.zsets[key]) + 1
            if next_count > int(max_orders):
                return [0, next_count]
            self.zsets[key][member] = int(timestamp_ms)
            self.expiry[key] = int(ttl)
            return [1, next_count]


def _rule(redis_client=None, *, max_orders=2) -> OrderRateLimitRule:
    return OrderRateLimitRule(
        RiskConfig(
            max_orders_per_minute=max_orders,
            max_single_order_notional_pct=Decimal("0.05"),
        ),
        redis_client or _FakeRedis(),
    )


def test_order_rate_limit_evaluate_passes_below_limit() -> None:
    redis_client = _FakeRedis()
    rule = _rule(redis_client)

    status, reason = rule.evaluate("s1", timestamp_ms=1000)

    assert status == RuleStatus.PASS
    assert reason is None


def test_order_rate_limit_evaluate_rejects_next_order_over_limit() -> None:
    redis_client = _FakeRedis()
    rule = _rule(redis_client)
    rule.record_order("s1", timestamp_ms=1000, member="a")
    rule.record_order("s1", timestamp_ms=1001, member="b")

    status, reason = rule.evaluate("s1", timestamp_ms=1002)

    assert status == RuleStatus.REJECT
    assert reason == "order_rate_limit_exceeded: 3 > 2"


def test_order_rate_limit_drops_entries_outside_window() -> None:
    redis_client = _FakeRedis()
    rule = _rule(redis_client)
    rule.record_order("s1", timestamp_ms=1000, member="old")
    rule.record_order("s1", timestamp_ms=60_999, member="current")

    status, reason = rule.evaluate("s1", timestamp_ms=61_001)

    assert status == RuleStatus.PASS
    assert reason is None
    assert set(redis_client.zsets["risk:orders:s1:1min"]) == {"current"}


def test_order_rate_limit_isolated_by_strategy_id() -> None:
    redis_client = _FakeRedis()
    rule = _rule(redis_client)
    rule.record_order("s1", timestamp_ms=1000, member="a")
    rule.record_order("s1", timestamp_ms=1001, member="b")

    status, reason = rule.evaluate("s2", timestamp_ms=1002)

    assert status == RuleStatus.PASS
    assert reason is None


def test_order_rate_limit_try_record_order_is_atomic() -> None:
    redis_client = _FakeRedis()
    rule = _rule(redis_client, max_orders=10)
    results = []

    def attempt(index: int) -> None:
        results.append(
            rule.try_record_order("s1", timestamp_ms=1000, member=f"order-{index}")
        )

    threads = [threading.Thread(target=attempt, args=(index,)) for index in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    passes = [result for result in results if result[0] == RuleStatus.PASS]
    rejects = [result for result in results if result[0] == RuleStatus.REJECT]
    assert len(passes) == 10
    assert len(rejects) == 10
    assert len(redis_client.zsets["risk:orders:s1:1min"]) == 10
