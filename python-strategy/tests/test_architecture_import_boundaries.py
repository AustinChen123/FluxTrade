import ast
import importlib.util
import subprocess
from pathlib import Path

import pytest


_REPOSITORY_ROOT = Path(__file__).parents[2]
_PYTHON_ROOT = _REPOSITORY_ROOT / "python-strategy"
_ADAPTER_PACKAGE = "src.core.adapters"
_PORT_PACKAGE = "src.core.interfaces"
_ORM_FREE_APPLICATION_MODULES = frozenset(
    {
        "src.core.adapters.rithmic_native_bracket",
        "src.core.execution_conditional_orders",
        "src.core.execution_order_cancellation",
    }
)
_FORBIDDEN_APPLICATION_IMPORTS = frozenset(
    {
        "src.core.interfaces.IOrderRepository",
        "src.core.interfaces.repository",
        "src.core.interfaces.repository.IOrderRepository",
    }
)
_FORBIDDEN_PORT_IMPORT_ROOTS = ("sqlalchemy", "src.core.orm_models")
_LEGACY_PORT_ORM_IMPORTS = frozenset(
    {
        ("src.core.interfaces.exchange", "src.core.orm_models.Order"),
        ("src.core.interfaces.repository", "sqlalchemy.orm.Session"),
        ("src.core.interfaces.repository", "src.core.orm_models.Order"),
        ("src.core.interfaces.repository", "src.core.orm_models.Position"),
        ("src.core.interfaces.repository", "src.core.orm_models.Trade"),
    }
)
_LEGACY_PROVIDER_IMPORTS = frozenset(
    {
        (
            "src.core.adapter_runtime_composition",
            "src.core.adapters.backpack_live_config",
        ),
        (
            "src.core.adapter_runtime_composition",
            "src.core.adapters.binance_live_config",
        ),
        (
            "src.core.adapter_runtime_composition",
            "src.core.adapters.bybit_live_config",
        ),
        (
            "src.core.adapter_runtime_composition",
            "src.core.adapters.rithmic_live_config",
        ),
        (
            "src.core.adapter_runtime_composition",
            "src.core.adapters.rithmic_runtime_composition",
        ),
        ("src.core.backtest_runner", "src.core.adapters.simulated"),
        ("src.core.engine", "src.core.adapters.simulated"),
        ("src.core.research_backtest_runner", "src.core.adapters.simulated"),
        ("src.main", "src.core.adapters.create_adapter"),
        ("src.validation.live_like_capture", "src.core.adapters.simulated"),
        ("src.validation.paper_lifecycle", "src.core.adapters.simulated"),
        (
            "src.validation.portfolio_paper_lifecycle",
            "src.core.adapters.simulated",
        ),
    }
)


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(_PYTHON_ROOT).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _tracked_production_sources() -> tuple[dict[str, str], frozenset[str]]:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(_REPOSITORY_ROOT),
            "ls-files",
            "-z",
            "--",
            ":(glob)python-strategy/src/**/*.py",
        ],
        check=True,
        capture_output=True,
    )
    paths = [
        _REPOSITORY_ROOT / value
        for value in result.stdout.decode().split("\0")
        if value
    ]
    sources = {_module_name(path): path.read_text() for path in paths}
    packages = frozenset(
        _module_name(path) for path in paths if path.name == "__init__.py"
    )
    return sources, packages


def _resolve_from(
    importer: str,
    node: ast.ImportFrom,
    package_modules: frozenset[str],
) -> str:
    if node.level == 0:
        return node.module or ""
    package = importer if importer in package_modules else importer.rpartition(".")[0]
    prefix = package.split(".")
    prefix = prefix[: len(prefix) - node.level + 1]
    if node.module:
        prefix.extend(node.module.split("."))
    return ".".join(prefix)


def _static_imports(
    importer: str,
    tree: ast.AST,
    tracked_modules: frozenset[str],
    package_modules: frozenset[str],
) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_from(importer, node, package_modules)
            if base == _ADAPTER_PACKAGE:
                imported.update(f"{base}.{alias.name}" for alias in node.names)
            else:
                children = {f"{base}.{alias.name}" for alias in node.names}
                imported.update(child for child in children if child in tracked_modules)
                if not children.intersection(tracked_modules):
                    imported.add(base)
    return imported


def _dynamic_imports(tree: ast.AST) -> set[str]:
    direct_names = {"__import__"}
    qualified_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            qualified_names.update(
                alias.asname or "importlib"
                for alias in node.names
                if alias.name == "importlib"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "importlib":
            direct_names.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "import_module"
            )

    imported: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        direct = isinstance(node.func, ast.Name) and node.func.id in direct_names
        qualified = (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in qualified_names
        )
        if not (direct or qualified):
            continue
        target_node = (
            node.args[0]
            if node.args
            else next(
                (keyword.value for keyword in node.keywords if keyword.arg == "name"),
                None,
            )
        )
        if not isinstance(target_node, ast.Constant):
            continue
        target = target_node.value
        if not isinstance(target, str):
            continue
        if target.startswith("."):
            package = next(
                (
                    keyword.value.value
                    for keyword in node.keywords
                    if keyword.arg == "package"
                    and isinstance(keyword.value, ast.Constant)
                    and isinstance(keyword.value.value, str)
                ),
                None,
            )
            if package is None:
                continue
            target = importlib.util.resolve_name(target, package)
        imported.add(target)
    return imported


def _provider_pairs(
    sources: dict[str, str],
    *,
    package_modules: frozenset[str] = frozenset(),
) -> frozenset[tuple[str, str]]:
    tracked_modules = frozenset(sources)
    pairs: set[tuple[str, str]] = set()
    for importer, source in sources.items():
        if importer == _ADAPTER_PACKAGE or importer.startswith(f"{_ADAPTER_PACKAGE}."):
            continue
        tree = ast.parse(source)
        imports = _static_imports(
            importer,
            tree,
            tracked_modules,
            package_modules,
        )
        imports.update(_dynamic_imports(tree))
        pairs.update(
            (importer, imported)
            for imported in imports
            if imported == _ADAPTER_PACKAGE
            or imported.startswith(f"{_ADAPTER_PACKAGE}.")
        )
    return frozenset(pairs)


def _orm_free_pairs(
    sources: dict[str, str],
    *,
    package_modules: frozenset[str] = frozenset(),
) -> frozenset[tuple[str, str]]:
    tracked_modules = frozenset(sources)
    pairs: set[tuple[str, str]] = set()
    for importer, source in sources.items():
        is_port = importer == _PORT_PACKAGE or importer.startswith(f"{_PORT_PACKAGE}.")
        if not is_port and importer not in _ORM_FREE_APPLICATION_MODULES:
            continue
        tree = ast.parse(source)
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                base = _resolve_from(importer, node, package_modules)
                for alias in node.names:
                    target = f"{base}.{alias.name}" if base else alias.name
                    if target in tracked_modules or _is_forbidden_orm_free_import(
                        importer,
                        target,
                    ):
                        imported.add(target)
        imported.update(_dynamic_imports(tree))
        pairs.update(
            (importer, target)
            for target in imported
            if _is_forbidden_orm_free_import(importer, target)
        )
    return frozenset(pairs)


def _is_forbidden_orm_free_import(importer: str, target: str) -> bool:
    if any(
        target == root or target.startswith(f"{root}.")
        for root in _FORBIDDEN_PORT_IMPORT_ROOTS
    ):
        return True
    return (
        importer in _ORM_FREE_APPLICATION_MODULES
        and target in _FORBIDDEN_APPLICATION_IMPORTS
    )


def test_tracked_production_provider_imports_match_exact_baseline() -> None:
    sources, packages = _tracked_production_sources()
    assert (
        _provider_pairs(
            sources,
            package_modules=packages,
        )
        == _LEGACY_PROVIDER_IMPORTS
    )


def test_tracked_production_orm_free_port_imports_match_exact_baseline() -> None:
    sources, packages = _tracked_production_sources()
    assert (
        _orm_free_pairs(sources, package_modules=packages) == _LEGACY_PORT_ORM_IMPORTS
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import sqlalchemy", "sqlalchemy"),
        ("import sqlalchemy.orm as orm", "sqlalchemy.orm"),
        ("from sqlalchemy.orm import Session", "sqlalchemy.orm.Session"),
        (
            "from src.core.orm_models import StrategyState",
            "src.core.orm_models.StrategyState",
        ),
        ("from src.core import orm_models", "src.core.orm_models"),
        ("from .. import orm_models", "src.core.orm_models"),
        (
            'from importlib import import_module\nimport_module("sqlalchemy.orm")',
            "sqlalchemy.orm",
        ),
        ('__import__("src.core.orm_models")', "src.core.orm_models"),
    ],
)
def test_orm_free_port_import_forms_cannot_bypass_ratchet(
    source: str,
    expected: str,
) -> None:
    importer = "src.core.interfaces.new_port"
    sources = {
        importer: source,
        "src.core.orm_models": "",
    }
    pair = (importer, expected)
    assert pair in _orm_free_pairs(sources)
    assert pair not in _LEGACY_PORT_ORM_IMPORTS


def test_orm_free_port_package_relative_import_anchors_at_package() -> None:
    pair = ("src.core.interfaces", "src.core.orm_models")
    sources = {
        "src.core.interfaces": "from .. import orm_models",
        "src.core.orm_models": "",
    }
    assert pair in _orm_free_pairs(
        sources,
        package_modules=frozenset({"src.core.interfaces"}),
    )
    assert pair not in _LEGACY_PORT_ORM_IMPORTS


def test_migrated_cancellation_owner_cannot_reintroduce_orm_imports() -> None:
    importer = "src.core.execution_order_cancellation"
    pair = (importer, "src.core.orm_models.Order")

    assert pair in _orm_free_pairs(
        {
            importer: "from src.core.orm_models import Order",
            "src.core.orm_models": "",
        }
    )
    assert pair not in _LEGACY_PORT_ORM_IMPORTS


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "from src.core.interfaces import IOrderRepository",
            "src.core.interfaces.IOrderRepository",
        ),
        (
            "from src.core.interfaces.repository import IOrderRepository",
            "src.core.interfaces.repository.IOrderRepository",
        ),
        (
            "from .interfaces import IOrderRepository",
            "src.core.interfaces.IOrderRepository",
        ),
        (
            'from importlib import import_module\nimport_module("src.core.interfaces.repository")',
            "src.core.interfaces.repository",
        ),
        (
            'from importlib import import_module\nimport_module(".interfaces.repository", package="src.core")',
            "src.core.interfaces.repository",
        ),
    ],
)
def test_migrated_cancellation_owner_cannot_reintroduce_broad_repository(
    source: str,
    expected: str,
) -> None:
    importer = "src.core.execution_order_cancellation"
    pair = (importer, expected)

    assert pair in _orm_free_pairs({importer: source})
    assert pair not in _LEGACY_PORT_ORM_IMPORTS


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "from src.core.adapters import LiveBinanceAdapter",
            "src.core.adapters.LiveBinanceAdapter",
        ),
        (
            "from src.core.adapters import live_binance",
            "src.core.adapters.live_binance",
        ),
        ("from .core.adapters import live_binance", "src.core.adapters.live_binance"),
        (
            "import src.core.adapters.live_binance",
            "src.core.adapters.live_binance",
        ),
        (
            "import src.core.adapters.live_binance as venue",
            "src.core.adapters.live_binance",
        ),
        ("from src.core import adapters", "src.core.adapters"),
    ],
)
def test_facade_imports_retain_concrete_member_identity(
    source: str,
    expected: str,
) -> None:
    sources = {
        "src.main": source,
        "src.core.adapters": "",
        "src.core.adapters.live_binance": "",
    }
    pair = ("src.main", expected)
    assert pair in _provider_pairs(sources)
    assert pair not in _LEGACY_PROVIDER_IMPORTS


def test_package_relative_import_anchors_at_package_module() -> None:
    pair = ("src.core.interfaces", "src.core.adapters.live_binance")
    sources = {
        "src.core.interfaces": "from ..adapters import live_binance",
        "src.core.adapters.live_binance": "",
    }
    assert pair in _provider_pairs(
        sources,
        package_modules=frozenset({"src.core.interfaces"}),
    )
    assert pair not in _LEGACY_PROVIDER_IMPORTS


@pytest.mark.parametrize(
    "source",
    [
        'from importlib import import_module\nimport_module("src.core.adapters.live_binance")',
        'from importlib import import_module as load\nload("src.core.adapters.live_binance")',
        'import importlib\nimportlib.import_module("src.core.adapters.live_binance")',
        'import importlib as loader\nloader.import_module("src.core.adapters.live_binance")',
        'from importlib import import_module\nimport_module(name="src.core.adapters.live_binance")',
        'import importlib\nimportlib.import_module(name="src.core.adapters.live_binance")',
        '__import__("src.core.adapters.live_binance")',
    ],
)
def test_literal_dynamic_provider_imports_cannot_bypass_ratchet(source: str) -> None:
    pair = ("src.main", "src.core.adapters.live_binance")
    assert pair in _provider_pairs({"src.main": source})
    assert pair not in _LEGACY_PROVIDER_IMPORTS
