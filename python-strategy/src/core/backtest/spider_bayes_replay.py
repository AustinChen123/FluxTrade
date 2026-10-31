"""Causal 15-minute observations for the named source-derived Spider model.

This is private input preparation only. It does not create a policy, owner,
runner, or financial projection.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from decimal import Decimal, localcontext

from src.core.backtest import spider_bayes_model
from src.core.backtest.spider_historical_input import (
    HistoricalInputError,
    HistoricalBar,
    HistoricalRunInput,
    validate_historical_input,
)


PROJECTION_ID = "CHAT_BAYES_CLOSED15M_SOURCE14_V1"
PRODUCTS = tuple(
    f"{symbol}-USDT-SWAP"
    for symbol in (
        "DOGE", "SUI", "ARB", "OP", "APT", "SEI", "SHIB", "BONK",
        "WIF", "NEIRO", "FET", "SAND", "W", "GALA",
    )
)
_MINUTE_MS = 60_000
_INTERVAL_MS = 900_000
_ZERO = Decimal("0")
_BASKET_SIZE = Decimal("14")


@dataclass(frozen=True, slots=True)
class Closed15mSample:
    cutoff_ms: int
    visible_at: int
    prices: tuple[Decimal, ...]
    observation: Decimal
    posterior: tuple[Decimal, ...]


@dataclass(frozen=True, slots=True)
class Closed15mProjection:
    projection_id: str
    model_id: str
    products: tuple[str, ...]
    anchors: tuple[Decimal, ...]
    samples: tuple[Closed15mSample, ...]


def _close_prices(
    bars: Mapping[tuple[int, str], HistoricalBar],
    cutoff_ms: int,
) -> tuple[Decimal, ...]:
    close_open_ms = cutoff_ms - _MINUTE_MS
    try:
        return tuple(bars[(close_open_ms, product)].close for product in PRODUCTS)
    except KeyError as error:
        raise HistoricalInputError("closed 15-minute basket lacks a trade close") from error


def _observation(prices: tuple[Decimal, ...], anchors: tuple[Decimal, ...]) -> Decimal:
    with localcontext(spider_bayes_model._model_context()):
        return sum((price / anchor).ln() for price, anchor in zip(prices, anchors, strict=True)) / _BASKET_SIZE


def prepare_closed15m_source14(run: HistoricalRunInput) -> Closed15mProjection:
    """Validate the complete P3 input, then derive a closed-candle causal prefix."""
    if type(run) is not HistoricalRunInput:
        raise HistoricalInputError("invalid historical input DTO")
    validate_historical_input(run)
    if run.ordered_products != PRODUCTS:
        raise HistoricalInputError("unsupported source basket identity or order")
    if run.range_start_ms % _INTERVAL_MS:
        raise HistoricalInputError("source basket launch is not 15-minute aligned")

    bars = {(bar.bar_open_ms, bar.product_id): bar for bar in run.trade_bars}
    anchors = _close_prices(bars, run.range_start_ms)
    samples = [Closed15mSample(
        cutoff_ms=run.range_start_ms,
        visible_at=run.range_start_ms * 16 + 6,
        prices=anchors,
        observation=_ZERO,
        posterior=spider_bayes_model.initial_probabilities(),
    )]

    previous = samples[0]
    for cutoff_ms in range(run.range_start_ms + _INTERVAL_MS, run.range_end_ms + 1, _INTERVAL_MS):
        prices = _close_prices(bars, cutoff_ms)
        observation = _observation(prices, anchors)
        posterior = spider_bayes_model.forward_step(
            previous.posterior, previous.observation, observation,
        )
        sample = Closed15mSample(
            cutoff_ms=cutoff_ms,
            visible_at=cutoff_ms * 16 + 6,
            prices=prices,
            observation=observation,
            posterior=posterior,
        )
        samples.append(sample)
        previous = sample

    return Closed15mProjection(
        projection_id=PROJECTION_ID,
        model_id=spider_bayes_model.MODEL_ID,
        products=PRODUCTS,
        anchors=anchors,
        samples=tuple(samples),
    )
