"""Resolve a verified full-backtest subject and sealed data boundary."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Literal

from src.control_plane.full_backtest_request import FullStrategyBacktestRequest
from src.core.data_provider import timeframe_to_ms
from src.strategies.base import BaseStrategy, StrategyRequirements

_SUBJECT_UNAVAILABLE = "browser_result_subject_unavailable"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
ResolutionErrorCode = Literal["browser_result_subject_unavailable"]


class FullBacktestResolutionError(RuntimeError):
    """One fixed, detail-free failure class for full-backtest resolution."""

    def __init__(self, code: ResolutionErrorCode) -> None:
        self.code = code
        super().__init__(code)


def _resolve_subject(
    request: FullStrategyBacktestRequest,
    strategy_loader: Callable[[], Mapping[str, object]],
) -> tuple[BaseStrategy, str, str]:
    """Resolve one fresh strategy class against the request's immutable identity."""
    try:
        strategies = strategy_loader()
        artifact = strategies.get(request.strategy_id)
        if (
            not isinstance(artifact, type)
            or not issubclass(artifact, BaseStrategy)
            or artifact is BaseStrategy
        ):
            raise ValueError

        artifact_version = getattr(artifact, "__fluxtrade_artifact_version__", None)
        catalog_sha256 = getattr(artifact, "__fluxtrade_catalog_sha256__", None)
        if (
            type(artifact_version) is not str
            or artifact_version != request.artifact_version
            or type(catalog_sha256) is not str
            or _SHA256_PATTERN.fullmatch(catalog_sha256) is None
        ):
            raise ValueError

        strategy = artifact(request.strategy_id, request.instrument.product_id)
        if (
            not isinstance(strategy, BaseStrategy)
            or strategy.strategy_id != request.strategy_id
            or strategy.product_id != request.instrument.product_id
        ):
            raise ValueError
        requirements = strategy.requirements
        if (
            not isinstance(requirements, StrategyRequirements)
            or requirements.product_id != strategy.product_id
            or type(requirements.timeframe) is not str
            or not requirements.timeframe.strip()
        ):
            raise ValueError
        if timeframe_to_ms(requirements.timeframe) <= 0:
            raise ValueError
        return strategy, catalog_sha256, requirements.timeframe
    except Exception:
        raise FullBacktestResolutionError(_SUBJECT_UNAVAILABLE) from None
