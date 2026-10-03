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
)

pytest_plugins = ["test_database_evaluation_data"]

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
