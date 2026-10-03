from __future__ import annotations

import ast
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy.orm import Session

import src.core.backtest_result_owner as owner_module
from src.core.backtest_result_owner import (
    BacktestResultPersistenceError,
    BacktestResultPersistenceOwner,
    BacktestResultRunIdentity,
)
from src.core.backtest_result_persistence import FullBacktestOutcome


def test_invalid_input_returns_fixed_error_before_opening_a_session() -> None:
    sessions: list[bool] = []
    clock_calls: list[bool] = []

    def session_factory() -> Session:
        sessions.append(True)
        raise AssertionError("invalid input must not open a session")

    def clock_ns() -> int:
        clock_calls.append(True)
        return 1_700_000_000_000

    with pytest.raises(BacktestResultPersistenceError) as error:
        BacktestResultPersistenceOwner(session_factory, clock_ns=clock_ns).persist(
            cast(BacktestResultRunIdentity, object()),
            cast(FullBacktestOutcome, object()),
        )

    assert str(error.value) == "browser_result_persistence_failed"
    assert sessions == []
    assert clock_calls == [True]
    assert error.value.__cause__ is None


def test_owner_imports_stay_at_core_database_boundary() -> None:
    tree = ast.parse(Path(owner_module.__file__).read_text())
    targets = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    ] + [
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    ]
    forbidden = (
        "src.core.adapters",
        "src.control_plane",
        "src.core.backtest_runner",
        "src.core.matching",
        "src.core.matcher",
        "src.core.execution",
        "src.strategies",
    )
    assert all(
        not any(
            target == prefix or target.startswith(prefix + ".") for prefix in forbidden
        )
        for target in targets
    )


@pytest.mark.parametrize(
    "source",
    [
        "from src.core.adapters.live_binance import LiveBinanceAdapter",
        "from src.core.execution import ExecutionService",
    ],
)
def test_owner_import_guard_rejects_forbidden_core_modules(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    monkeypatch.setattr(Path, "read_text", lambda self: source)
    with pytest.raises(AssertionError):
        test_owner_imports_stay_at_core_database_boundary()
