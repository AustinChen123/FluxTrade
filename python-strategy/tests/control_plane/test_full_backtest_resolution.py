from __future__ import annotations

from collections.abc import Mapping

import pytest

from src.control_plane.full_backtest_request import FullStrategyBacktestRequest
from src.control_plane.full_backtest import (
    FullBacktestResolutionError,
    _resolve_subject,
)
from src.core.models import Candlestick
from src.core.portfolio_runtime import PortfolioFactory
from src.strategies.base import BaseStrategy, StrategyRequirements
from test_database_evaluation_data import (
    DATASET,
    END,
    PRODUCT,
    START,
    TIMEFRAME,
    stored_dataset as stored_dataset,
)

STRATEGY_ID = "catalog::ResolutionStrategy"
ARTIFACT_VERSION = "v1.2.3"
CATALOG_SHA256 = "a" * 64


def _request(
    *,
    start: int = START,
    end: int = END,
    execution_timeframe: str | None = None,
    product_id: str = PRODUCT,
    symbol: str = "BTC/USDT:USDT",
    base: str = "BTC",
) -> FullStrategyBacktestRequest:
    return FullStrategyBacktestRequest.model_validate(
        {
            "kind": "full_strategy_backtest",
            "dataset_id": DATASET,
            "strategy_id": STRATEGY_ID,
            "artifact_version": ARTIFACT_VERSION,
            "start": start,
            "end": end,
            "initial_balance": "1000",
            "instrument": {
                "product_id": product_id,
                "exchange": "binance",
                "symbol": symbol,
                "base": base,
                "quote": "USDT",
                "fee_model": "percentage_notional",
                "capital_model": "notional",
                "market_type": "perpetual",
            },
            "fees": {"maker": "0.0002", "taker": "0.0006"},
            "drawdown_limit": None,
            "execution_timeframe": execution_timeframe,
        }
    )


def _strategy_class(
    *,
    timeframe: str = TIMEFRAME,
    actual_product_id: str | None = None,
    requirement_product_id: str | None = None,
    actual_strategy_id: str | None = None,
    artifact_version: str = ARTIFACT_VERSION,
    catalog_sha256: str = CATALOG_SHA256,
    constructor_error: bool = False,
    requirement_error: bool = False,
) -> type[BaseStrategy]:
    class ResolutionStrategy(BaseStrategy):
        def __init__(self, strategy_id: str, product_id: str) -> None:
            if constructor_error:
                raise RuntimeError("private constructor detail")
            super().__init__(strategy_id, product_id)
            if actual_product_id is not None:
                self.product_id = actual_product_id
            if actual_strategy_id is not None:
                self.strategy_id = actual_strategy_id

        @property
        def requirements(self) -> StrategyRequirements:
            if requirement_error:
                raise RuntimeError("private requirements detail")
            return StrategyRequirements(
                product_id=requirement_product_id or self.product_id,
                timeframe=timeframe,
                lookback_window=1,
            )

        def on_candle(self, candle: Candlestick, context=None):
            return None

    setattr(ResolutionStrategy, "__fluxtrade_artifact_version__", artifact_version)
    setattr(ResolutionStrategy, "__fluxtrade_catalog_sha256__", catalog_sha256)
    return ResolutionStrategy


def _loader(
    artifact: object | None = None,
) -> Mapping[str, object]:
    default_artifact = _DEFAULT_ARTIFACT if artifact is None else artifact
    return {STRATEGY_ID: default_artifact}


_DEFAULT_ARTIFACT = _strategy_class()


def _resolve(
    session_factory,
    *,
    request: FullStrategyBacktestRequest | None = None,
    loader=None,
):
    from src.control_plane.full_backtest import resolve_full_backtest

    return resolve_full_backtest(
        request or _request(),
        strategy_loader=(lambda: _loader()) if loader is None else loader,
        session_factory=session_factory,
    )


def _assert_error(error: FullBacktestResolutionError, expected_code: str) -> None:
    assert error.code == expected_code
    assert str(error) == expected_code
    assert "private" not in str(error)


def test_subject_resolution_returns_fresh_verified_strategy_instances():
    request = _request()
    artifact = _strategy_class()

    def loader():
        return {STRATEGY_ID: artifact}

    resolved, digest, timeframe = _resolve_subject(request, loader)
    again, _, _ = _resolve_subject(request, loader)

    assert resolved is not again
    assert resolved.strategy_id == STRATEGY_ID
    assert resolved.product_id == PRODUCT
    assert timeframe == TIMEFRAME
    assert digest == CATALOG_SHA256


@pytest.mark.parametrize(
    "artifact",
    [
        "loader error detail",
        PortfolioFactory,
        object,
        BaseStrategy,
        _strategy_class(artifact_version="v2"),
        _strategy_class(catalog_sha256="A" * 64),
        _strategy_class(catalog_sha256="bad"),
        _strategy_class(constructor_error=True),
        _strategy_class(actual_product_id="BINANCE:ETHUSDT-PERP"),
        _strategy_class(actual_strategy_id="different"),
        _strategy_class(requirement_product_id="BINANCE:ETHUSDT-PERP"),
        _strategy_class(requirement_error=True),
        _strategy_class(timeframe="not-valid"),
        _strategy_class(timeframe="0m"),
        _strategy_class(timeframe="-1m"),
    ],
)
def test_subject_resolution_fails_closed_without_source_access(artifact):
    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve_subject(_request(), lambda: _loader(artifact))
    _assert_error(captured.value, "browser_result_subject_unavailable")


def test_subject_resolution_rejects_missing_and_failed_catalogs():
    with pytest.raises(FullBacktestResolutionError) as missing:
        _resolve_subject(_request(), lambda: {})
    _assert_error(missing.value, "browser_result_subject_unavailable")

    def failed_loader():
        raise RuntimeError("private catalog path")

    with pytest.raises(FullBacktestResolutionError) as failed:
        _resolve_subject(_request(), failed_loader)
    _assert_error(failed.value, "browser_result_subject_unavailable")


@pytest.fixture
def sealed_dataset(stored_dataset):  # noqa: F811
    return stored_dataset[0]


def test_valid_subject_and_sealed_data_resolve_to_fresh_runtime_instances(
    sealed_dataset,
):
    resolved = _resolve(sealed_dataset)
    again = _resolve(sealed_dataset)

    assert resolved.strategy is not again.strategy
    assert resolved.strategy.strategy_id == STRATEGY_ID
    assert resolved.strategy.product_id == PRODUCT
    assert resolved.dataset.id == DATASET
    assert resolved.dataset.product_id == PRODUCT
    assert resolved.dataset.timeframe == TIMEFRAME
    assert resolved.source_range == (START, END)
    assert resolved.decision_timeframe == TIMEFRAME
    assert resolved.execution_timeframe is None
    assert resolved.catalog_sha256 == CATALOG_SHA256


@pytest.mark.parametrize("dataset_state", ["absent", "unsealed"])
def test_missing_or_unsealed_dataset_is_dataset_unavailable(
    sealed_dataset, dataset_state
):
    if dataset_state == "unsealed":
        from src.core.orm_models import ResearchDataset

        with sealed_dataset() as session:
            row = session.get(ResearchDataset, DATASET)
            assert row is not None
            row.lifecycle_state = "importing"
            row.sealed_at = None
            session.commit()
        request = _request()
    else:
        request = _request().model_copy(update={"dataset_id": "absent"})

    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve(sealed_dataset, request=request)
    _assert_error(captured.value, "browser_result_dataset_unavailable")


def test_sqlalchemy_failure_is_backend_unavailable_without_raw_details():
    from sqlalchemy.exc import SQLAlchemyError

    def broken_factory():
        raise SQLAlchemyError("private database URL and credentials")

    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve(broken_factory)
    _assert_error(captured.value, "browser_result_backend_unavailable")


def test_dataset_product_must_match_the_requested_instrument(sealed_dataset):
    request = _request(
        product_id="BINANCE:ETHUSDT-PERP",
        symbol="ETH/USDT:USDT",
        base="ETH",
    )
    strategy = _strategy_class()
    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve(
            sealed_dataset,
            request=request,
            loader=lambda: {STRATEGY_ID: strategy},
        )
    _assert_error(captured.value, "browser_result_dataset_unavailable")


@pytest.mark.parametrize(
    ("backtest_request", "timeframe", "expected_code"),
    [
        (_request().model_copy(update={"start": START - 1}), TIMEFRAME, "dataset"),
        (_request().model_copy(update={"end": END + 1}), TIMEFRAME, "dataset"),
        (_request(execution_timeframe="5m"), "5m", "dataset"),
        (_request(), "5m", "dataset"),
        (_request(execution_timeframe="1m"), "1m", "dataset"),
        (_request(execution_timeframe="1m"), "30s", "dataset"),
    ],
)
def test_dataset_identity_and_inclusive_coverage_are_enforced(
    sealed_dataset, backtest_request, timeframe, expected_code
):
    strategy = _strategy_class(timeframe=timeframe)
    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve(
            sealed_dataset,
            request=backtest_request,
            loader=lambda: {STRATEGY_ID: strategy},
        )
    _assert_error(captured.value, f"browser_result_{expected_code}_unavailable")


@pytest.mark.parametrize(
    ("decision_timeframe", "execution_timeframe"),
    [("5m", "1m"), ("90s", "1m")],
)
def test_finer_execution_timeframe_must_match_source_and_divide_decisions(
    sealed_dataset, decision_timeframe, execution_timeframe
):
    strategy = _strategy_class(timeframe=decision_timeframe)
    request = _request(execution_timeframe=execution_timeframe)
    if decision_timeframe == "5m":
        resolved = _resolve(
            sealed_dataset,
            request=request,
            loader=lambda: {STRATEGY_ID: strategy},
        )
        assert resolved.decision_timeframe == "5m"
        assert resolved.execution_timeframe == "1m"
        return

    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve(
            sealed_dataset,
            request=request,
            loader=lambda: {STRATEGY_ID: strategy},
        )
    _assert_error(captured.value, "browser_result_dataset_unavailable")


def test_get_available_range_remains_authoritative(sealed_dataset, monkeypatch):
    from src.core.data_sources.research_database import ResearchDatabaseDataSource

    called: list[tuple[str, str]] = []
    original = ResearchDatabaseDataSource.get_available_range

    def narrowed(source, product_id: str, timeframe: str):
        called.append((product_id, timeframe))
        available = original(source, product_id, timeframe)
        assert available is not None
        return (available[0] + 60_000, available[1])

    monkeypatch.setattr(ResearchDatabaseDataSource, "get_available_range", narrowed)
    with pytest.raises(FullBacktestResolutionError) as captured:
        _resolve(sealed_dataset)
    _assert_error(captured.value, "browser_result_dataset_unavailable")
    assert called == [(PRODUCT, TIMEFRAME)]
