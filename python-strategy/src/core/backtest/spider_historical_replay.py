"""Private P3 composition bootstrap and closed-bar policy-cache projection."""

from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any, cast

from src.core.backtest.spider_historical_input import (
    BAR_DURATION_MS,
    HistoricalBar,
    HistoricalInputError,
    HistoricalRunInput,
    _validate_configured_spec_timeline,
    _canonical,
    decode_p2_configuration,
    validate_historical_input,
)
from src.core.backtest.spider_policy import DEFAULTS
from src.core.backtest.spider_run_artifacts import decode_canonical
from src.core.backtest.synthetic_scenario_codec import Account


@dataclass(frozen=True, slots=True)
class _MarketRow:
    product_id: str
    price: Decimal
    contract_value: Decimal
    lot_size: Decimal
    minimum_size: Decimal
    price_increment: Decimal
    high_low_ratio: Decimal
    instrument_code: int

    def policy_entry(self) -> dict[str, object]:
        return {
            "product_id": self.product_id,
            "price": self.price,
            "ctVal": self.contract_value,
            "lotSz": self.lot_size,
            "minSz": self.minimum_size,
            "increment": self.price_increment,
            "ratioHL": self.high_low_ratio,
            "state": "live",
            "instIdCode": self.instrument_code,
        }


@dataclass(frozen=True, slots=True)
class _ClosedMarketSnapshot:
    close_ms: int
    markets: tuple[_MarketRow, ...]

    def content_bytes(self) -> bytes:
        return _canonical({
            "close_ms": self.close_ms,
            "markets": [row.policy_entry() for row in self.markets],
        })

    def policy_markets(self) -> dict[str, dict[str, object]]:
        return {
            row.product_id: {key: value for key, value in row.policy_entry().items() if key != "product_id"}
            for row in self.markets
        }

    def detached(self) -> dict[str, object]:
        return {"close_ms": self.close_ms, "markets": deepcopy([row.policy_entry() for row in self.markets])}


@dataclass(frozen=True, slots=True)
class _PreparedHistoricalReplay:
    contract_hash: str
    account: Account
    configuration: dict[str, object]
    policy_cache: dict[str, object]
    snapshots: tuple[_ClosedMarketSnapshot, ...]


def _policy_cache(raw: bytes) -> dict[str, object]:
    value = decode_canonical(raw)
    if type(value) is not dict:
        raise HistoricalInputError("invalid initial policy cache")
    return cast(dict[str, object], value)


def _closed_market_snapshots(
    run: HistoricalRunInput, configuration: dict[str, object]
) -> tuple[_ClosedMarketSnapshot, ...]:
    products = cast(list[dict[str, object]], configuration["products"])
    product_configuration = {cast(str, row["product_id"]): row for row in products}
    active_specs = _validate_configured_spec_timeline(run, configuration)
    trade_by_product: dict[str, tuple[HistoricalBar, ...]] = {
        product: tuple(row for row in run.trade_bars if row.product_id == product)
        for product in run.ordered_products
    }
    by_open: dict[tuple[int, str], HistoricalBar] = {
        (row.bar_open_ms, row.product_id): row for row in run.trade_bars
    }
    snapshots = []
    for open_ms in range(run.range_start_ms, run.range_end_ms, BAR_DURATION_MS):
        close_ms = open_ms + BAR_DURATION_MS
        rows: list[_MarketRow] = []
        for product_id in run.ordered_products:
            current = by_open[(open_ms, product_id)]
            history = tuple(
                row for row in trade_by_product[product_id]
                if close_ms - 86_400_000 <= row.bar_open_ms < close_ms
            )
            expected_times = tuple(range(close_ms - 86_400_000, close_ms, BAR_DURATION_MS))
            if len(history) != 1440 or tuple(row.bar_open_ms for row in history) != expected_times:
                raise HistoricalInputError("closed market cache lacks exact 24-hour trade history")
            configured = product_configuration[product_id]
            contract_value, _multiplier, price_tick, quantity_step, minimum_quantity = active_specs[product_id]
            code = configured["instrument_code"]
            if type(code) is not int:
                raise HistoricalInputError("invalid configured instrument code")
            with localcontext() as context:
                context.prec = 50
                ratio = (max(row.high for row in history) / min(row.low for row in history) - 1) * 100
            rows.append(_MarketRow(
                product_id=product_id,
                price=current.close,
                contract_value=contract_value,
                lot_size=quantity_step,
                minimum_size=minimum_quantity,
                price_increment=price_tick,
                high_low_ratio=ratio,
                instrument_code=code,
            ))
        snapshots.append(_ClosedMarketSnapshot(close_ms, tuple(rows)))
    return tuple(snapshots)


def _prepare_historical_replay(run: HistoricalRunInput) -> _PreparedHistoricalReplay:
    """Finish admission and deterministic projections before constructing an owner."""
    contract_hash = validate_historical_input(run)
    configuration = decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products
    )
    policy_cache = _policy_cache(run.initial_policy_cache)
    snapshots = _closed_market_snapshots(run, configuration)
    account: Account = {
        "venue": "SPIDER_HISTORICAL_RESEARCH",
        "environment": "RESEARCH_ONLY",
        "account": run.account_key,
    }
    return _PreparedHistoricalReplay(contract_hash, account, configuration, policy_cache, snapshots)


def _restore_initial_policy(composition: Any, run: HistoricalRunInput, prepared: _PreparedHistoricalReplay) -> None:
    state = prepared.policy_cache
    policy = composition._policy
    policy.running = state["running"]
    policy.paused = state["paused"]
    policy.online = state["online"]
    policy.ws_open = state["ws_open"]
    policy.order_id = state["order_id"]
    policy.rows = deepcopy(state["rows"])
    market_rows = cast(list[dict[str, object]], state["markets"])
    policy.markets = {
        cast(str, row["product_id"]): {key: deepcopy(value) for key, value in row.items() if key != "product_id"}
        for row in market_rows
    }
    policy.capital = deepcopy(state["capital"])
    policy.replies = deepcopy(state["replies"])
    policy.last_filled_price = deepcopy(state["last_filled_price"])
    policy.parameters = DEFAULTS | dict(run.parameters)
    # Replay starts at the admitted source range, never at wall-clock or run identity.
    policy.now_ms = run.range_start_ms
    policy.shared_ms = run.range_start_ms - cast(int, state.get("shared_elapsed_ms", 12_000))
    policy.last_earn_ms = run.range_start_ms - 61_000
    policy.last_market_ms = 0
    policy.reset_ms = {}
    policy.last_capital = None
    composition._historical_initial_cache_evidence = {
        "orders": deepcopy(state["orders"]),
        "positions": deepcopy(state["positions"]),
    }


def _historical_market_item(snapshot: _ClosedMarketSnapshot, sequence: int) -> dict[str, object]:
    return {
        "kind": "HISTORICAL_MARKET_CACHE",
        "schedule_sequence": sequence,
        "stable_id": f"MARKET_CLOSE_{snapshot.close_ms}",
        "snapshot": snapshot,
    }


__all__ = ["_prepare_historical_replay", "_restore_initial_policy", "_historical_market_item", "_ClosedMarketSnapshot"]
