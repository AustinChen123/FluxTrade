"""Pure plan evidence tests; no authoritative database or runnable replay claim."""

import ast
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from unittest.mock import Mock
from typing import Any, cast

import pytest

from src.core.market_data.profiles import bootstrap_hydration as owner
from src.core.market_data.profiles.decision_context import ProfileDecisionStatus
from test_profile_bootstrap_seed import seed, suffix, MINUTE


def plan(value, rows=(), **changes):
    args = dict(
        expected_key=value.key,
        requirements=value.requirements,
        lookback=value.lookback,
        seed=value,
        recorded=rows,
        completed_recorded_through_ms=value.cutover_ms + (len(rows) - 1) * MINUTE
        if rows
        else None,
        max_seed_candles=10,
        max_recorded_candles=10,
    )
    return owner.BootstrapHydrationPlan(**cast(Any, args | changes))


@pytest.mark.parametrize("count", [0, 1, 3])
@pytest.mark.parametrize("status", list(ProfileDecisionStatus))
def test_ordered_seed_and_terminal_suffix(count, status):
    value = seed(status)
    recorded = tuple(suffix(value, index=i, skipped=i == 1) for i in range(count))
    result = plan(value, recorded)
    assert result.seed is value and result.recorded is recorded
    assert result.seed_candles is value.candles
    assert [c.bar_start_ms for c in result.seed_candles] + [
        int(row[0].key.trigger_id.split(":")[1]) for row in result.recorded
    ] == list(
        range(value.cutover_ms - 2 * MINUTE, value.cutover_ms + count * MINUTE, MINUTE)
    )
    assert all(c.context.profiles[0].status is status for c in result.seed_candles)
    if count > 1:
        assert (
            result.recorded[1][0].disposition == "SKIPPED"
            and result.recorded[1][1] is None
        )
    assert plan(value, recorded) == result
    with pytest.raises(FrozenInstanceError):
        setattr(result, "recorded", ())
    assert not hasattr(result, "__dict__")


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "reverse",
        "duplicate",
        "input",
        "key",
        "limit",
        "seed_limit",
        "list",
        "bool",
        "wrong_seed",
    ],
)
def test_fail_closed(damage):
    value = seed()
    recorded = (suffix(value), suffix(value, index=1))
    changes = {}
    if damage == "missing":
        changes["completed_recorded_through_ms"] = value.cutover_ms + 2 * MINUTE
    elif damage == "reverse":
        recorded = recorded[::-1]
    elif damage == "duplicate":
        recorded = (recorded[0], recorded[0])
    elif damage == "input":
        recorded = ((recorded[0][0], None), recorded[1])
    elif damage == "key":
        recorded = (
            suffix(
                replace(
                    value,
                    key=replace(value.key, strategy_id="other"),
                    max_seed_candles=10,
                )
            ),
            recorded[1],
        )
    else:
        changes = {
            "limit": {"max_recorded_candles": 1},
            "seed_limit": {"max_seed_candles": 1},
            "list": {"recorded": list(recorded)},
            "bool": {"max_recorded_candles": True},
            "wrong_seed": {"seed": True},
        }[damage]
    with pytest.raises(owner.BootstrapSeedError):
        plan(value, recorded, **changes)


def test_classifier_is_authoritative(monkeypatch):
    value = seed()
    error = owner.BootstrapSeedError()
    classifier = Mock(side_effect=error)
    monkeypatch.setattr(owner, "classify_bootstrap", classifier)
    with pytest.raises(owner.BootstrapSeedError) as caught:
        plan(value)
    assert caught.value is error
    assert classifier.call_count == 1
    assert classifier.call_args.args[0] is value.requirements
    assert classifier.call_args.kwargs["stored"] is value


def _imports(source):
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            prefix = "." * node.level + (node.module or "")
            names.add(prefix)
            names.update(prefix.rstrip(".") + "." + alias.name for alias in node.names)
    return {part for name in names for part in name.split(".") if part}


def _assert_boundary(source, *, store=False, service=False):
    imports = _imports(source)
    if service:
        assert "profiles" not in imports
        return
    forbidden = {
        "main",
        "strategy_registry",
        "decision_owner",
        "modeled_input",
        "snapshot_cache",
        "snapshot_client",
        "snapshot_http_transport",
        "signal_processor",
        "SignalProcessor",
        "strategy_hydration_service",
        "BaseStrategy",
        "strategies",
        "clock",
        "http",
        "time",
        "asyncio",
        "Engine",
    }
    # SQLAlchemy's engine module is legitimate only for the persistence owner.
    if not store:
        forbidden |= {
            "engine",
            "sqlalchemy",
            "orm",
            "bootstrap_seed_store",
            "Session",
            "repository",
        }
    assert not imports & forbidden
    if store:
        assert not any(
            name.startswith("src.core.engine")
            for name in (
                node.module or ""
                for node in ast.walk(ast.parse(source))
                if isinstance(node, ast.ImportFrom)
            )
        )
        assert not ({"core", "engine"} <= imports)


def test_architecture_import_ratchet():
    root = Path(__file__).resolve().parents[1] / "src/core"
    for filename in (
        "bootstrap_seed.py",
        "bootstrap_seed_store.py",
        "bootstrap_hydration.py",
    ):
        _assert_boundary(
            (root / "market_data/profiles" / filename).read_text(),
            store=filename == "bootstrap_seed_store.py",
        )
    _assert_boundary((root / "strategy_hydration_service.py").read_text(), service=True)


@pytest.mark.parametrize(
    "source",
    [
        "from src.core import engine",
        "from . import decision_owner",
        "from . import bootstrap_seed_store",
        "import src.core.engine",
        "from .. import repository",
        "from .snapshot_cache import ProfileSnapshotCache",
        "from src.core.signal_processor import SignalProcessor",
        "from src.strategies.base import BaseStrategy",
        "from src.core import strategy_hydration_service",
        "from .snapshot_http_transport import SnapshotHttpTransport",
        "from src.core.clock import Clock",
    ],
)
def test_import_ratchet_negative_controls(source):
    with pytest.raises(AssertionError):
        _assert_boundary(source)


@pytest.mark.parametrize(
    "source",
    [
        "from src.core.market_data import profiles",
        "from .market_data import profiles",
        "from . import profiles",
        "import src.core.market_data.profiles",
    ],
)
def test_service_import_ratchet_negative_controls(source):
    with pytest.raises(AssertionError):
        _assert_boundary(source, service=True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("strategy_id", "other"),
        ("strategy_version", "v2"),
        ("config_hash", "d" * 64),
        ("product_id", "BINANCE:BTCUSDT-SPOT"),
        ("timeframe", "5m"),
        ("execution_scope_id", "other"),
        ("environment", "paper"),
    ],
)
def test_expected_key_mismatch_before_classifier(field, value, monkeypatch):
    value_seed = seed()
    trap = Mock(side_effect=AssertionError("must not classify"))
    monkeypatch.setattr(owner, "classify_bootstrap", trap)
    with pytest.raises(owner.BootstrapSeedError):
        plan(value_seed, expected_key=replace(value_seed.key, **{field: value}))
    trap.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        {"requirements": ()},
        {"requirements": []},
        {"lookback": 1},
        {"lookback": True},
        {"expected_key": None},
    ],
)
def test_expected_contract_mismatch_before_classifier(changes, monkeypatch):
    trap = Mock(side_effect=AssertionError("must not classify"))
    monkeypatch.setattr(owner, "classify_bootstrap", trap)
    with pytest.raises(owner.BootstrapSeedError):
        plan(seed(), **changes)
    trap.assert_not_called()


def test_different_valid_requirements_rejected(monkeypatch):
    value = seed()
    changed = (replace(value.requirements[0], output_grid_id="another_grid"),)
    trap = Mock(side_effect=AssertionError("must not classify"))
    monkeypatch.setattr(owner, "classify_bootstrap", trap)
    with pytest.raises(owner.BootstrapSeedError):
        plan(value, requirements=changed)
    trap.assert_not_called()
