"""Static common-form ratchet, not a reflection sandbox."""
import ast
from pathlib import Path

import pytest

from src.core.backtest import spider_policy_protocol, synthetic_scenario_replay

ROOT = Path(__file__).parents[1] / "src/core/backtest"
ENTRY_POINTS = {"_policy_event_bytes", "_policy_event_digest"}


def allowed(source, composition=False):
    tree = ast.parse(source)
    imports, aliases = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
            aliases.update(alias.asname or alias.name for alias in node.names if alias.name.endswith("spider_policy_protocol"))
        elif isinstance(node, ast.ImportFrom):
            if node.level or (composition and (node.module or "").endswith("spider_policy_protocol")
                              and any(a.name in ENTRY_POINTS | {"*"} for a in node.names)):
                return False
            imports.add(node.module)
        elif not composition and isinstance(node, ast.Name) and node.id == "__import__" and isinstance(node.ctx, ast.Load):
            return False
    if not composition:
        return imports == {"hashlib", "re", "typing"}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(node.value, ast.Attribute):
            if ast.unparse(node.value.value) in aliases and node.value.attr in ENTRY_POINTS:
                return False
    return True


def test_actual_dependency_direction_and_sole_entry():
    assert allowed((ROOT / "spider_policy_protocol.py").read_text())
    assert allowed((ROOT / "synthetic_scenario_replay.py").read_text(), composition=True)
    definitions = {name: [] for name in ENTRY_POINTS}
    for path in ROOT.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in ENTRY_POINTS:
                definitions[node.name].append(path.name)
    assert definitions == {name: ["spider_policy_protocol.py"] for name in ENTRY_POINTS}
    assert all(value is not getattr(spider_policy_protocol, name)
               for value in vars(synthetic_scenario_replay).values() for name in ENTRY_POINTS)


@pytest.mark.parametrize("snippet,composition", [
    ("load = __import__; load('src.core.backtest.spider_policy')", False),
    ("from src.core.backtest.spider_policy_protocol import _policy_event_digest as digest", True),
    ("from src.core.backtest.spider_policy_protocol import *", True),
    ("import src.core.backtest.spider_policy_protocol as p; digest = p._policy_event_digest", True),
])
def test_appended_alias_mutants_are_rejected(snippet, composition):
    filename = "synthetic_scenario_replay.py" if composition else "spider_policy_protocol.py"
    assert not allowed((ROOT / filename).read_text() + "\n" + snippet, composition)
    assert allowed("import src.core.backtest.spider_policy_protocol as p\np._policy_event_digest({})", True)
