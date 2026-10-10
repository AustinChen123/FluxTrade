"""Real-admission causal tests for the private closed-15m source projection."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR, localcontext
from hashlib import sha256
from typing import cast

import pytest

from src.core.backtest import spider_bayes_model as model
from src.core.backtest.spider_bayes_replay import (
    PRODUCTS,
    PROJECTION_ID,
    prepare_closed15m_source14,
)
from src.core.backtest.spider_historical_input import (
    BAR_DURATION_MS,
    HistoricalBar,
    HistoricalInputError,
    InstrumentSpecEvidence,
    validate_historical_input,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from test_spider_historical_input import _initial_account_state, _p2_configuration, _policy_cache, _valid_run


_HASH = "a" * 64
_MINUTE = BAR_DURATION_MS
_FIFTEEN_MINUTES = 15 * _MINUTE


def _manifest(template, rows, *, trade: bool, products=PRODUCTS):
    product_mapping = tuple((product, product) for product in products)
    digest = sha256(b"".join(
        bytes.fromhex(row.source_row_hash)
        for product in products
        for row in rows
        if row.product_id == product
    )).hexdigest()
    return replace(
        template,
        canonical_rows_sha256=digest,
        product_mapping=product_mapping,
        units="CONTRACTS" if trade else "PRICE",
        source_row_count=len(rows),
        normalized_row_count=len(rows),
        first_source_timestamp_ms=min(row.bar_open_ms for row in rows),
        last_source_timestamp_ms=max(row.bar_open_ms for row in rows),
    )


def _valid_source14_run(*, start: int = 86_400_000, minutes: int = 46, products=PRODUCTS):
    template = _valid_run()
    end = start + minutes * _MINUTE
    warmup = start - 86_400_000
    configuration = _p2_configuration(products)
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = end
    config_bytes = canonical_bytes(configuration)

    cache = cast(dict[str, object], decode_canonical(_policy_cache(products)))
    market_rows = cast(list[dict[str, object]], cache["markets"])
    row_values = cast(list[dict[str, object]], cache["rows"])
    for index, market in enumerate(market_rows):
        market["price"] = str(200 + index)  # Deliberately not the historical anchor.
    inactive = cast(dict[str, object], next(row for row in row_values if row["product_id"] == products[-1]))
    inactive["active"] = "false"  # Inactivity does not remove it from observations.
    cache_bytes = canonical_bytes(cache)

    trades = []
    marks = []
    for product_index, product in enumerate(products):
        base_price = Decimal(100 + product_index)
        sequence = 0
        for bar_open in range(warmup, end, _MINUTE):
            close = base_price
            trade = HistoricalBar(product, bar_open, close, close, close, close, 1,
                                  sequence, Decimal("4"))
            trade = replace(trade, source_row_hash=sha256(trade.canonical_row(trade=True)).hexdigest())
            mark = HistoricalBar(product, bar_open, close, close, close, close, 1,
                                 sequence, None)
            mark = replace(mark, source_row_hash=sha256(mark.canonical_row(trade=False)).hexdigest())
            trades.append(trade)
            marks.append(mark)
            sequence += 1

    trade_rows = tuple(sorted(trades, key=lambda row: (row.bar_open_ms, products.index(row.product_id))))
    mark_rows = tuple(sorted(marks, key=lambda row: (row.bar_open_ms, products.index(row.product_id))))
    specs = tuple(InstrumentSpecEvidence(product, start, Decimal("1"), Decimal("1"),
                                         Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), _HASH)
                  for product in products)
    run = replace(
        template,
        run_id="source14-closed15m",
        configuration_bytes=config_bytes,
        configuration_sha256=sha256(config_bytes).hexdigest(),
        ordered_products=products,
        range_start_ms=start,
        range_end_ms=end,
        warmup_start_ms=warmup,
        first_timer_ms=start + 5_000,
        trade_manifest=_manifest(template.trade_manifest, trade_rows, trade=True, products=products),
        mark_manifest=_manifest(template.mark_manifest, mark_rows, trade=False, products=products),
        trade_bars=trade_rows,
        mark_bars=mark_rows,
        spec_before=specs,
        spec_after=tuple(replace(spec, effective_at_ms=end) for spec in specs),
        initial_policy_cache=cache_bytes,
        initial_account_state=_initial_account_state(),
    )
    validate_historical_input(run)
    return run


def _reprice(run, cutoff_ms: int, product_id: str, price: Decimal):
    target_open = cutoff_ms - _MINUTE
    changed = tuple(
        replace(row, open=price, high=price, low=price, close=price,
                source_row_hash=sha256(replace(row, open=price, high=price, low=price, close=price)
                                       .canonical_row(trade=True)).hexdigest())
        if row.product_id == product_id and row.bar_open_ms == target_open else row
        for row in run.trade_bars
    )
    assert any(row.product_id == product_id and row.bar_open_ms == target_open for row in run.trade_bars)
    return replace(run, trade_bars=changed,
                   trade_manifest=_manifest(run.trade_manifest, changed, trade=True))


@pytest.mark.parametrize(("minutes", "cut_count"), [(15, 2), (30, 3), (31, 3)])
def test_source14_projection_uses_verified_anchors_all_products_and_complete_closes(minutes, cut_count):
    run = _valid_source14_run(minutes=minutes)
    cutoff15 = run.range_start_ms + _FIFTEEN_MINUTES
    inactive = PRODUCTS[-1]
    changed = _reprice(run, cutoff15, inactive, Decimal("130"))

    projection = prepare_closed15m_source14(changed)
    assert projection.projection_id == PROJECTION_ID
    assert projection.model_id == model.MODEL_ID
    assert projection.products == PRODUCTS
    assert projection.anchors == tuple(Decimal(100 + i) for i in range(14))
    assert projection.anchors[-1] != Decimal("213")  # Cache ticker is not the anchor.
    expected_cutoffs = tuple(run.range_start_ms + i * _FIFTEEN_MINUTES for i in range(cut_count))
    assert tuple(sample.cutoff_ms for sample in projection.samples) == expected_cutoffs
    initial, first = projection.samples[:2]
    assert initial.visible_at == run.range_start_ms * 16 + 6
    assert initial.prices == projection.anchors
    assert initial.observation == Decimal("0")
    assert initial.posterior == model.PRIOR
    assert first.visible_at == cutoff15 * 16 + 6
    assert first.prices[-1] == Decimal("130")  # Inactive product remains observed.
    assert first.posterior == model.forward_step(model.PRIOR, Decimal("0"), first.observation)

    with localcontext(model._model_context()):
        expected = sum((price / anchor).ln() for price, anchor in zip(first.prices, projection.anchors, strict=True)) / Decimal("14")
    assert first.observation == expected
    if len(projection.samples) == 3:
        second = projection.samples[2]
        assert second.posterior == model.forward_step(first.posterior, first.observation, second.observation)


def test_source14_projection_is_deterministic_and_future_bars_do_not_change_prefix():
    run = _valid_source14_run(minutes=46)
    base = prepare_closed15m_source14(run)
    cutoff30 = run.range_start_ms + 2 * _FIFTEEN_MINUTES
    changed_future = prepare_closed15m_source14(_reprice(run, cutoff30, PRODUCTS[0], Decimal("155")))
    assert prepare_closed15m_source14(run) == base
    assert changed_future.samples[:2] == base.samples[:2]
    assert changed_future.samples[2] != base.samples[2]


def test_source14_nonzero_projection_is_independent_of_ambient_decimal_context():
    run = _valid_source14_run(minutes=30)
    run = _reprice(run, run.range_start_ms + 2 * _FIFTEEN_MINUTES, PRODUCTS[0], Decimal("155"))
    ordinary = prepare_closed15m_source14(run)
    assert ordinary.samples[-1].observation != Decimal("0")
    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_FLOOR
        altered = prepare_closed15m_source14(run)
    assert altered == ordinary


@pytest.mark.parametrize("mutation", ["wrong_basket", "unaligned_start", "missing_minute", "nonclosed_bar"])
def test_source14_projection_rejects_invalid_full_input_before_projection(mutation):
    run = _valid_source14_run(minutes=31)
    if mutation == "wrong_basket":
        bad = replace(run, ordered_products=PRODUCTS[:-1])
    elif mutation == "unaligned_start":
        bad = _valid_source14_run(start=run.range_start_ms + _MINUTE, minutes=31)
    elif mutation == "missing_minute":
        rows = run.trade_bars[:-1]
        bad = replace(run, trade_bars=rows,
                      trade_manifest=_manifest(run.trade_manifest, rows, trade=True))
    else:
        bad_rows = list(run.trade_bars)
        changed = replace(bad_rows[-1], confirm=0)
        bad_rows[-1] = replace(changed, source_row_hash=sha256(changed.canonical_row(trade=True)).hexdigest())
        rows = tuple(bad_rows)
        bad = replace(run, trade_bars=rows,
                      trade_manifest=_manifest(run.trade_manifest, rows, trade=True))
    with pytest.raises(HistoricalInputError) as error:
        prepare_closed15m_source14(bad)
    expected = {
        "wrong_basket": "P2 product order mismatch",
        "unaligned_start": "source basket launch is not 15-minute aligned",
        "missing_minute": "warmup/run coverage gap",
        "nonclosed_bar": "unknown product or non-closed bar",
    }[mutation]
    assert expected in str(error.value)


def test_source14_rejects_a_different_order_after_full_historical_admission():
    reordered = (PRODUCTS[1], PRODUCTS[0], *PRODUCTS[2:])
    run = _valid_source14_run(minutes=15, products=reordered)
    validate_historical_input(run)
    with pytest.raises(HistoricalInputError, match="unsupported source basket identity or order"):
        prepare_closed15m_source14(run)
