"""Real isolated persistence → detached warmup; no historical account claim."""

from dataclasses import replace
from decimal import Decimal
import os
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event

from src.core.signal_processor import SignalProcessor
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_registry import StrategyRegistry
from test_profile_context_enrichment import context
from test_profile_bootstrap_pg_fixture import (
    build,
    digest,
    DAY,
    MINUTE,
    CUTOVER,
    PRODUCT,
)

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]
if os.environ.get("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1":
    pytest.skip(
        "isolated PostgreSQL acceptance requires explicit opt-in",
        allow_module_level=True,
    )


@pytest.mark.parametrize("referenced", [False, True])
@pytest.mark.parametrize("mode,offset", [("BEFORE_PENDING", 3), ("THROUGH_APPLIED", 2)])
def test_real_persistence_detached_stateful_warmup(
    profile_repository_pg, monkeypatch, referenced, mode, offset
):
    engine = profile_repository_pg
    strategy, reader, seeds, inputs, active, forbidden = build(engine, referenced)
    bound = reader.prepare(strategy, CUTOVER + offset * MINUTE, mode)
    assert not active
    monkeypatch.setattr(seeds, "get", forbidden)
    monkeypatch.setattr(inputs, "get", forbidden)
    execution = MagicMock()
    processor = SignalProcessor(
        StrategyRegistry(),
        execution,
        strategy_context_loader=lambda s, c, _: replace(
            context(),
            strategy_id=s.strategy_id,
            product_id=c.product_id,
            timestamp=c.timestamp,
        ),
    )
    monkeypatch.setattr(processor, "_process_signals", forbidden)
    account = MagicMock()
    account.get_position.return_value = None
    hydration = StrategyHydrationService(
        signal_processor=processor, account_service=account
    )
    event.listen(engine, "before_cursor_execute", forbidden)
    try:
        assert (
            hydration.hydrate_candles(
                strategy,
                bound.candles,
                decision_scope_loader=bound.decision_scope_loader,
            )
            == 5
        )
    finally:
        event.remove(engine, "before_cursor_execute", forbidden)
    # Independent literal recurrence: 12 → 142 → 1454 → 14593; skipped close=40 never contributes.
    assert strategy.accumulator == Decimal(14593)
    assert [row[0] for row in strategy.trace] == [
        DAY,
        DAY + MINUTE,
        CUTOVER,
        CUTOVER + 2 * MINUTE,
    ]
    assert [row[1:5] for row in strategy.trace] == [
        [86460000, "MODELED", "FRESH", "1"],
        [86520000, "MODELED", "MISSING", "0"],
        [86580000, "LIVE_OBSERVED", "FRESH", "3"],
        [86700000, "LIVE_OBSERVED", "INVALID", "0"],
    ]
    expected_digests = (
        "45893ea38f8dc48bffd69b0082da521c9aafba30660a5d49af2614eab86a557b",
        "290cb4ec74cc6ef50d844047068820f59155e18f8e6414ba318dcaa307cd6bf5",
        "d7c98ec544318074ace807e4e8aa5af60edb357a8cd4c3007ca5dfbe150f374e",
        "c89d95845b784b278a9b58efd23b1f1c146a293e1e65c616f0015e2c7e551c6b",
    )
    assert tuple(row[6] for row in strategy.trace) == expected_digests
    expected = {
        "accumulator": "14593",
        "trace": [
            [
                DAY + i * MINUTE,
                DAY + (i + 1) * MINUTE,
                basis,
                status,
                volume,
                profile_id,
                expected_digest,
            ]
            for (i, basis, status, volume, profile_id), expected_digest in zip(
                (
                    (
                        0,
                        "MODELED",
                        "FRESH",
                        "1",
                        "5766b5d445bededce8aae025da6629f16784fd3c8463672452c7e0fb70dfd3a4",
                    ),
                    (1, "MODELED", "MISSING", "0", None),
                    (
                        2,
                        "LIVE_OBSERVED",
                        "FRESH",
                        "3",
                        "5766b5d445bededce8aae025da6629f16784fd3c8463672452c7e0fb70dfd3a4",
                    ),
                    (4, "LIVE_OBSERVED", "INVALID", "0", None),
                ),
                expected_digests,
                strict=True,
            )
        ],
    }
    assert digest(strategy.state()) == digest(expected)
    assert not forbidden.mock_calls  # Includes cache child methods, clocks, and reads.
    assert not execution.mock_calls
    account.get_position.assert_called_once_with("s", PRODUCT)


@pytest.mark.parametrize("damage", ["seed", "receipt", "ohlcv", "version", "config"])
def test_prepare_failures_never_start_callback(profile_repository_pg, damage):
    strategy, reader, _, _, active, forbidden = build(
        profile_repository_pg, False, damage
    )
    with pytest.raises((ValueError, RuntimeError)):
        reader.prepare(strategy, CUTOVER + 3 * MINUTE)
    assert strategy.trace == [] and strategy.accumulator == 0 and not active
    assert not forbidden.mock_calls  # Includes cache child methods, clocks, and reads.
