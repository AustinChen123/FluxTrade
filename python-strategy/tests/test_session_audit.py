"""Guard tests for Phase 2 session lifecycle refactoring."""

import inspect
import re
from pathlib import Path

import pytest

from src.core.engine import StrategyEngine
from src.core.execution import ExecutionEngine


CORE_DIR = Path(__file__).resolve().parents[1] / "src" / "core"
SESSION_AUDIT_FILES = [
    CORE_DIR / "engine.py",
    CORE_DIR / "execution.py",
    CORE_DIR / "health_monitor.py",
    CORE_DIR / "order_manager.py",
    CORE_DIR / "command_router.py",
    CORE_DIR / "signal_processor.py",
]
LONG_LIVED_SESSION_PATTERN = re.compile(r"\bself\.(?:db|session)\s*=")


def _strip_line_comments(source: str) -> str:
    return "\n".join(line.split("#", 1)[0] for line in source.splitlines())


@pytest.mark.xfail(reason="Tasks 2.2-2.5 pending", strict=True)
def test_no_long_lived_db_session_assignments():
    offenders: list[str] = []

    for path in SESSION_AUDIT_FILES:
        source = _strip_line_comments(path.read_text())
        if LONG_LIVED_SESSION_PATTERN.search(source):
            offenders.append(str(path.relative_to(CORE_DIR.parents[1])))

    assert offenders == []


@pytest.mark.xfail(reason="Tasks 2.2-2.5 pending", strict=True)
def test_engine_constructors_accept_session_factory():
    strategy_params = inspect.signature(StrategyEngine.__init__).parameters
    execution_params = inspect.signature(ExecutionEngine.__init__).parameters

    assert "db_session_factory" in strategy_params
    assert "db_session_factory" in execution_params
