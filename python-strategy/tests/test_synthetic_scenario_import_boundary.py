"""Static ratchet for common import/access forms, not a reflection sandbox."""
import ast
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
CODEC = "python-strategy/src/core/backtest/synthetic_scenario_codec.py"
NATIVE_TEST = "python-strategy/tests/test_synthetic_scenario_native.py"
SYMBOLS = {"_SyntheticScenarioReplaySession", "ScenarioReplayInputError", "ScenarioReplayLookupError", "ScenarioReplayConflictError", "ScenarioReplayInvariantError"}


def violations(source: str, path: str) -> list[int]:
    tree = ast.parse(source)
    native = set()
    loaders = {"__import__"}
    importlibs = set()
    codec_aliases = set()
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                if item.name == "fluxtrade_core":
                    native.add(item.asname or item.name)
                if item.name == "importlib":
                    importlibs.add(item.asname or item.name)
                if item.name.endswith("synthetic_scenario_codec"):
                    codec_aliases.add(item.asname or item.name)
        if isinstance(node, ast.ImportFrom):
            codec_aliases.update(item.asname or item.name for item in node.names if item.name == "synthetic_scenario_codec")
            if node.module == "importlib":
                loaders.update(item.asname or item.name for item in node.names if item.name == "import_module")
            if node.module == "fluxtrade_core" and any(item.name in SYMBOLS or item.name == "*" for item in node.names):
                bad.append(node.lineno)
            if node.module and node.module.endswith("synthetic_scenario_codec"):
                if any(item.name in SYMBOLS or item.name in {"*", "_native"} for item in node.names):
                    bad.append(node.lineno)

    def codec_module(node: ast.expr) -> bool:
        parts = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if not isinstance(node, ast.Name):
            return False
        return ".".join([node.id, *reversed(parts)]) in codec_aliases

    def module(node: ast.expr) -> bool:
        if isinstance(node, ast.Attribute) and codec_module(node.value) and node.attr == "_native":
            return True
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr" and len(node.args) >= 2:
            if codec_module(node.args[0]) and isinstance(node.args[1], ast.Constant) and node.args[1].value == "_native":
                return True
        if isinstance(node, ast.Name):
            return node.id in native
        if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
            function = node.func
            loader = isinstance(function, ast.Name) and function.id in loaders
            loader |= isinstance(function, ast.Attribute) and isinstance(function.value, ast.Name) and function.value.id in importlibs and function.attr == "import_module"
            return loader and node.args[0].value == "fluxtrade_core"
        return False

    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None and module(node.value):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            native.update(target.id for target in targets if isinstance(target, ast.Name))
        if isinstance(node, ast.Attribute):
            if module(node.value) and node.attr in SYMBOLS:
                bad.append(node.lineno)
            if isinstance(node.value, ast.Name) and node.value.id in codec_aliases and node.attr in SYMBOLS | {"_native"}:
                bad.append(node.lineno)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr" and len(node.args) >= 2:
            if module(node.args[0]) and isinstance(node.args[1], ast.Constant) and node.args[1].value in SYMBOLS:
                bad.append(node.lineno)
    if path == CODEC:
        exports = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                value = node.value
                direct = isinstance(value, ast.Attribute) and module(value.value) and value.attr in SYMBOLS
                indirect = isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "getattr" and len(value.args) >= 2 and module(value.args[0])
                if direct or indirect:
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    if any(isinstance(target, ast.Name) for target in targets):
                        exports.append(node.lineno)
        exports.extend(node.lineno for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module == "fluxtrade_core")
        return exports
    if path == NATIVE_TEST:
        return []
    return bad


def sources(root: Path) -> list[Path]:
    result = subprocess.run(["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.py", ":(exclude)backtest_output/**", ":(exclude,glob)**/backtest_output/**"], check=True, capture_output=True)
    return sorted({root / name for name in result.stdout.decode().split("\0") if name})


def test_only_codec_and_explicit_native_boundary_test_consume_new_surface():
    failures = {str(path.relative_to(ROOT)): violations(path.read_text(), str(path.relative_to(ROOT))) for path in sources(ROOT)}
    assert {path: lines for path, lines in failures.items() if lines} == {}
    codec = ast.parse((ROOT / CODEC).read_text())
    assert not any(isinstance(node, ast.ImportFrom) and node.module == "fluxtrade_core" for node in ast.walk(codec))
    assert not any(isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in SYMBOLS for target in node.targets) for node in ast.walk(codec))


@pytest.mark.parametrize("source", [
    "from fluxtrade_core import _SyntheticScenarioReplaySession as S",
    "from fluxtrade_core import *",
    "import fluxtrade_core as n; S = n._SyntheticScenarioReplaySession",
    "import fluxtrade_core as n; S = getattr(n, 'ScenarioReplayInputError')",
    "import importlib as i; n = i.import_module('fluxtrade_core'); S = n._SyntheticScenarioReplaySession",
    "from importlib import import_module as load; S = getattr(load('fluxtrade_core'), 'ScenarioReplayConflictError')",
    "S = __import__('fluxtrade_core')._SyntheticScenarioReplaySession",
    "from src.core.backtest.synthetic_scenario_codec import _native",
    "import src.core.backtest.synthetic_scenario_codec as c; S = c._native",
    "from src.core.backtest import synthetic_scenario_codec as c; S = c._native",
    "from src.core.backtest import synthetic_scenario_codec as codec; S = getattr(codec, '_native')._SyntheticScenarioReplaySession",
    "import src.core.backtest.synthetic_scenario_codec; S = src.core.backtest.synthetic_scenario_codec._native._SyntheticScenarioReplaySession",
])
def test_forbidden_forms_are_detected_in_any_unapproved_consumer(source):
    for path in ["python-strategy/src/strategies/x.py", "python-strategy/src/core/adapters/x.py", "control-plane/x.py", "runner.py"]:
        assert violations(source, path)
    assert violations(source, NATIVE_TEST) == []


@pytest.mark.parametrize("source", [
    "import fluxtrade_core as _native; Public = _native._SyntheticScenarioReplaySession",
    "import fluxtrade_core as _native; Public = getattr(_native, 'ScenarioReplayInputError')",
    "from fluxtrade_core import ScenarioReplayInputError as Public",
])
def test_codec_cannot_reexport_native_symbols_under_aliases(source):
    assert violations(source, CODEC)


def test_untracked_sources_are_scanned(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    path = tmp_path / "untracked.py"
    path.write_text("import fluxtrade_core as n; s = n._SyntheticScenarioReplaySession")
    assert path in sources(tmp_path)
    assert violations(path.read_text(), "untracked.py")
