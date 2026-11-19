from dataclasses import asdict, fields, replace
from decimal import Decimal

import pytest

from src.core.backtest.spider_historical_input import (
    HistoricalBar,
    HistoricalInputError,
    HistoricalRunInput,
    _canonical_decimal,
    _canonical_plain,
    _digest,
    _parameter_projection,
    _semantic_input_digest,
    validate_historical_input,
)
from test_spider_historical_input import _valid_run


def _asdict_reference_digest(run: HistoricalRunInput, model_id: str | None = None) -> str:
    projection = asdict(run)
    projection.pop("run_id")
    projection["parameters"] = _parameter_projection(run.parameters)
    if model_id is not None:
        projection["model_id"] = model_id
    return _digest(projection)


@pytest.mark.parametrize("model_id", [None, "OHLC4_PATH_PAIR_V1"])
def test_semantic_digest_matches_asdict_reference(model_id: str | None) -> None:
    run = _valid_run()
    assert _semantic_input_digest(run, model_id=model_id) == _asdict_reference_digest(run, model_id)


def test_validate_returns_asdict_reference_digest_and_ignores_run_id() -> None:
    run = _valid_run()
    renamed = replace(run, run_id="another-transport-id")
    assert validate_historical_input(run) == _asdict_reference_digest(run)
    assert validate_historical_input(renamed) == validate_historical_input(run)


def test_bar_fast_path_projects_every_bar_field() -> None:
    bar = _valid_run().trade_bars[0]
    projected = _canonical_plain(bar)
    assert isinstance(projected, dict)
    assert list(projected) == [field.name for field in fields(HistoricalBar)]


@pytest.mark.parametrize(
    ("first", "second", "text"),
    [("1.0", "1", "1"), ("1E+2", "100", "100"), ("-0.00", "0", "0"), ("1.2345E+2", "123.4500", "123.45")],
)
def test_equal_decimals_share_canonical_text(first: str, second: str, text: str) -> None:
    assert _canonical_decimal(Decimal(first)) == _canonical_decimal(Decimal(second)) == text


@pytest.mark.parametrize("value", [1, True, 1.0, Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), "1"])
def test_cached_canonical_decimal_still_rejects_invalid_values(value: object) -> None:
    assert _canonical_decimal(Decimal("1")) == "1"
    with pytest.raises(HistoricalInputError):
        _canonical_decimal(value)  # type: ignore[arg-type]
