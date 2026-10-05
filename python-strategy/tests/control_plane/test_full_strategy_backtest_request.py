from __future__ import annotations

from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from src.control_plane.full_backtest_request import FullStrategyBacktestRequest
from test_full_backtest_request import _payload


def _replace(field: str, value: object) -> dict[str, object]:
    return _payload() | {field: value}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dataset_id", ""),
        ("dataset_id", "  "),
        ("dataset_id", "../dataset"),
        ("dataset_id", r"folder\dataset"),
        ("strategy_id", "../strategy"),
        ("artifact_version", "/tmp/version"),
    ],
)
def test_request_identity_is_nonblank_path_free_text(field: str, value: str) -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_replace(field, value))


@pytest.mark.parametrize("field", ["start", "end"])
@pytest.mark.parametrize("value", [True, False, -1, 253_402_300_800_000, 1.0, "0"])
def test_timestamps_are_bounded_nonboolean_integers(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_replace(field, value))


@pytest.mark.parametrize("field", ["initial_balance", "drawdown_limit"])
@pytest.mark.parametrize("value", [1, 1.25, True, "NaN", "Infinity", "-Infinity"])
def test_request_financial_fields_reject_non_decimal_values(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_replace(field, value))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("initial_balance", "0"),
        ("initial_balance", "-1"),
        ("drawdown_limit", "-0.1"),
    ],
)
def test_request_financial_constraints(field: str, value: str) -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_replace(field, value))


def test_range_is_ordered_inclusive_and_decimal_parse_ignores_context() -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_payload() | {"start": 2, "end": 1})
    parsed = FullStrategyBacktestRequest.model_validate(
        _payload() | {"start": 42, "end": 42}
    )
    assert parsed.start == parsed.end == 42

    exact = Decimal("10000.000000000000000000000123456789")
    with localcontext() as context:
        context.prec = 2
        parsed = FullStrategyBacktestRequest.model_validate(
            _replace("initial_balance", exact)
        )
    assert parsed.initial_balance == exact


@pytest.mark.parametrize(
    "field",
    [
        "kind",
        "dataset_id",
        "strategy_id",
        "artifact_version",
        "start",
        "end",
        "initial_balance",
        "instrument",
        "fees",
        "drawdown_limit",
    ],
)
def test_required_request_fields(field: str) -> None:
    payload = _payload()
    payload.pop(field)
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("report_path", "report.csv"),
        ("candles_csv_path", "candles.csv"),
        ("extra", "x"),
    ],
)
def test_request_forbids_legacy_paths_and_unknown_fields(
    field: str, value: str
) -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_replace(field, value))


@pytest.mark.parametrize("execution_timeframe", ["", " ", 5])
def test_execution_timeframe_is_null_or_nonblank_text(
    execution_timeframe: object,
) -> None:
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(
            _replace("execution_timeframe", execution_timeframe)
        )


def test_request_defaults_execution_timeframe_and_is_immutable() -> None:
    parsed = FullStrategyBacktestRequest.model_validate(_payload())
    assert parsed.execution_timeframe is None
    with pytest.raises(ValidationError):
        parsed.start = 1  # type: ignore[misc]
    with pytest.raises(ValidationError):
        FullStrategyBacktestRequest.model_validate(_replace("unexpected", "field"))
