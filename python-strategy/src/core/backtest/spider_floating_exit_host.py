"""Run-local host connection for causal source samples and native exits."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from src.core.backtest.spider_bayes_replay import Closed15mSample, prepare_closed15m_source14
from src.core.backtest.spider_floating_exit_input import (
    SpiderFloatingExitInput,
    validate_spider_floating_exit_input,
)
from src.core.backtest.spider_floating_exit_policy import decide_floating_exits


@dataclass
class SpiderFloatingExitHost:
    request: SpiderFloatingExitInput
    contract_hash: str
    samples: tuple[Closed15mSample, ...]
    batches: list[Any] = field(default_factory=list)
    _sample_by_visible_at: dict[int, Closed15mSample] = field(init=False)

    @classmethod
    def admit(cls, request: SpiderFloatingExitInput) -> "SpiderFloatingExitHost":
        contract_hash = validate_spider_floating_exit_input(request)
        projection = prepare_closed15m_source14(request.execution_input.historical_input)
        return cls(request, contract_hash, projection.samples)

    def on_market_cache(self, composition: Any, visible_at: int) -> None:
        endpoint = composition._paid_endpoint
        if (composition._floating_exit_host is not self or endpoint is None
                or endpoint.contract_hash != self.contract_hash
                or composition._historical_run is not self.request.execution_input.historical_input):
            raise ValueError("INVALID_EXECUTION_SCOPE")
        if composition._terminal is not None or composition._paid_native_terminal is not None:
            return
        sample = self._sample_by_visible_at.get(visible_at)
        run = self.request.execution_input.historical_input
        if (sample is None or sample.cutoff_ms <= run.range_start_ms
                or sample.cutoff_ms >= self.request.execution_input.policy_end_ms
                or composition._current_time != visible_at):
            return

        cost_projection = composition._codec.historical_cost_projection()
        cache_rows = {row["name"]: row for row in composition._policy.rows}
        if len(cache_rows) != len(composition._policy.rows):
            raise ValueError("INVALID_SCHEMA")
        running = composition._policy.running is True and composition._policy.paused is False
        active_products = tuple(
            product for product in self.samples_products
            if running and cache_rows.get(product, {}).get("active") == "true"
            and product not in endpoint.exit_products
        )
        batch = decide_floating_exits(
            policy_id=self.request.decision_policy_id,
            sample=sample,
            cost_projection=cost_projection,
            active_products=active_products,
            expected_account=composition._account,
            observed_at=visible_at,
        )
        self.batches.append(batch)
        for row in batch.rows:
            if row.active and row.reason is not None:
                endpoint.request_product_exit(composition, row.product_id, row.reason)

    @property
    def samples_products(self) -> tuple[str, ...]:
        return self.request.execution_input.historical_input.ordered_products

    def observation(self) -> dict[str, object]:
        return dict(
            schema_version="spider_floating_exit_host_observation_v1",
            contract_hash=self.contract_hash,
            batches=[asdict(batch) for batch in self.batches],
        )

    def __post_init__(self) -> None:
        self._sample_by_visible_at = {sample.visible_at: sample for sample in self.samples}
