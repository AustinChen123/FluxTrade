import ast
import glob
import hashlib
import hmac
import importlib
import importlib.util
import inspect
import json
import logging
import os
import sys
import tempfile
import threading
import traceback
from pathlib import Path
from types import ModuleType
from typing import Dict, Type, Union, cast

from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class StrategyLoader:
    CATALOG_NAME = "strategy_catalog.json"
    READINESS_VALUES = {
        "RESEARCH_VALIDATED",
        "RESEARCH_FROZEN",
        "LIVE_APPROVED",
    }
    _SCAN_LOCK = threading.RLock()
    _catalog_snapshots: dict[str, tempfile.TemporaryDirectory[str]] = {}

    @staticmethod
    def scan_directory(path: str) -> Dict[str, Union[Type[BaseStrategy], str]]:
        """
        Scans a directory for Python files and loads subclasses of BaseStrategy.
        Returns a dictionary mapping "FileName::ClassName" to the strategy class.
        In case of loading errors, the value will be the error traceback string.
        """
        with StrategyLoader._SCAN_LOCK:
            return StrategyLoader._scan_directory(path)

    @staticmethod
    def _scan_directory(path: str) -> Dict[str, Union[Type[BaseStrategy], str]]:
        strategies: Dict[str, Union[Type[BaseStrategy], str]] = {}
        if not os.path.exists(path):
            logger.warning("Directory %s does not exist.", path)
            return strategies

        catalog_path = Path(path) / StrategyLoader.CATALOG_NAME
        if catalog_path.exists():
            return StrategyLoader._scan_catalog(catalog_path)

        nested_catalogs = sorted(Path(path).glob(f"*/{StrategyLoader.CATALOG_NAME}"))
        conflicting_modules = StrategyLoader._conflicting_catalog_modules(
            nested_catalogs
        )
        if conflicting_modules:
            return {
                f"{StrategyLoader.CATALOG_NAME}::LoadError": (
                    "strategy pack Python module namespace overlap: "
                    f"{sorted(conflicting_modules)}"
                )
            }

        strategies.update(StrategyLoader._scan_legacy_directory(Path(path)))
        for nested_catalog in nested_catalogs:
            nested = StrategyLoader._scan_catalog(nested_catalog)
            for strategy_id, result in nested.items():
                if strategy_id in strategies:
                    strategies[strategy_id] = (
                        f"duplicate strategy id across strategy packs: {strategy_id}"
                    )
                else:
                    strategies[strategy_id] = result
        return strategies

    @staticmethod
    def _conflicting_catalog_modules(catalog_paths: list[Path]) -> set[str]:
        claimed_by: dict[str, Path] = {}
        conflicts: set[str] = set()
        for catalog_path in catalog_paths:
            try:
                catalog = json.loads(catalog_path.read_bytes())
                if not isinstance(catalog, dict):
                    continue
                files = catalog.get("files")
                if not isinstance(files, dict):
                    continue
                module_names = StrategyLoader._declared_python_module_names(
                    {
                        relative_path: b""
                        for relative_path in files
                        if isinstance(relative_path, str)
                    }
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            for module_name in module_names:
                previous = claimed_by.setdefault(module_name, catalog_path)
                if previous != catalog_path:
                    conflicts.add(module_name)
        return conflicts

    @staticmethod
    def _scan_legacy_directory(path: Path) -> Dict[str, Union[Type[BaseStrategy], str]]:
        strategies: Dict[str, Union[Type[BaseStrategy], str]] = {}
        search_path = os.path.join(path, "*.py")
        for file_path in glob.glob(search_path):
            file_name = os.path.basename(file_path)
            if file_name == "__init__.py":
                continue

            try:
                module = StrategyLoader._load_module(
                    Path(file_path),
                    StrategyLoader._legacy_module_name(Path(file_path)),
                )

                found_any = False
                for name, obj in inspect.getmembers(module):
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, BaseStrategy)
                        and obj is not BaseStrategy
                        and obj.__module__ == module.__name__
                    ):
                        strategy_id = f"{file_name}::{name}"
                        strategies[strategy_id] = obj
                        logger.info("Loaded strategy: %s", strategy_id)
                        found_any = True

                if not found_any:
                    logger.debug("No BaseStrategy subclass found in %s", file_name)

            except Exception:
                error_trace = traceback.format_exc()
                logger.error("Failed to load module %s:\n%s", file_path, error_trace)
                strategies[f"{file_name}::LoadError"] = error_trace

        return strategies

    @staticmethod
    def _scan_catalog(
        catalog_path: Path,
    ) -> Dict[str, Union[Type[BaseStrategy], str]]:
        try:
            catalog_source = catalog_path.read_bytes()
            catalog = json.loads(catalog_source)
            entries, files = StrategyLoader._validate_catalog(
                catalog_path.parent,
                catalog,
            )
        except Exception:
            error_trace = traceback.format_exc()
            logger.error("Failed to validate strategy catalog:\n%s", error_trace)
            return {f"{StrategyLoader.CATALOG_NAME}::LoadError": error_trace}

        catalog_digest = hashlib.sha256(catalog_source).hexdigest()
        logger.info("Validated strategy catalog digest: %s", catalog_digest)
        snapshot_root, package_name = StrategyLoader._snapshot_catalog_files(
            catalog_path.parent,
            files,
        )
        strategies: Dict[str, Union[Type[BaseStrategy], str]] = {}
        StrategyLoader._ensure_catalog_package(package_name, snapshot_root)
        for entry in entries:
            strategy_id = entry["id"]
            try:
                module_path = StrategyLoader._resolve_pack_file(
                    snapshot_root,
                    entry["module"],
                )
                module_parent = Path(entry["module"]).parent
                StrategyLoader._ensure_catalog_module_parent(
                    package_name,
                    module_parent,
                )
                module = StrategyLoader._load_module(
                    module_path,
                    StrategyLoader._catalog_module_name(
                        package_name,
                        module_parent,
                        strategy_id,
                    ),
                    evict_modules=False,
                    expose_import_root=False,
                )
                strategy_class = getattr(module, entry["class"])
                if (
                    not inspect.isclass(strategy_class)
                    or not issubclass(strategy_class, BaseStrategy)
                    or strategy_class is BaseStrategy
                    or strategy_class.__module__ != module.__name__
                ):
                    raise TypeError(
                        f"{entry['class']} is not a strategy defined by "
                        f"{entry['module']}"
                    )
                setattr(
                    strategy_class,
                    "__fluxtrade_artifact_version__",
                    entry["artifact_version"],
                )
                setattr(
                    strategy_class,
                    "__fluxtrade_display_name__",
                    entry["display_name"],
                )
                setattr(
                    strategy_class,
                    "__fluxtrade_readiness__",
                    entry["readiness"],
                )
                setattr(
                    strategy_class,
                    "__fluxtrade_catalog_sha256__",
                    catalog_digest,
                )
                strategies[strategy_id] = strategy_class
                logger.info("Loaded catalog strategy: %s", strategy_id)
            except Exception:
                error_trace = traceback.format_exc()
                logger.error(
                    "Failed to load catalog strategy %s:\n%s",
                    strategy_id,
                    error_trace,
                )
                strategies[strategy_id] = error_trace
        return strategies

    @staticmethod
    def _validate_catalog(
        root: Path,
        catalog: object,
    ) -> tuple[list[dict[str, str]], dict[str, bytes]]:
        if not isinstance(catalog, dict) or catalog.get("schema_version") != 1:
            raise ValueError("strategy catalog schema_version must be 1")

        files = catalog.get("files")
        entries = catalog.get("strategies")
        if not isinstance(files, dict) or not files:
            raise ValueError("strategy catalog files must be a non-empty mapping")
        if not isinstance(entries, list) or not entries:
            raise ValueError("strategy catalog strategies must be a non-empty list")

        declared_files = set(files)
        pack_files = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file() and path.name != StrategyLoader.CATALOG_NAME
        }
        if declared_files != pack_files:
            missing = sorted(pack_files - declared_files)
            extra = sorted(declared_files - pack_files)
            raise ValueError(
                f"strategy catalog file set mismatch: missing={missing} extra={extra}"
            )

        verified_files: dict[str, bytes] = {}
        for relative_path, expected_digest in files.items():
            if not isinstance(relative_path, str) or not isinstance(
                expected_digest, str
            ):
                raise ValueError("strategy catalog file entries must be strings")
            file_path = StrategyLoader._resolve_pack_file(root, relative_path)
            source = file_path.read_bytes()
            actual_digest = hashlib.sha256(source).hexdigest()
            if not hmac.compare_digest(actual_digest, expected_digest.lower()):
                raise ValueError(f"strategy pack digest mismatch: {relative_path}")
            verified_files[relative_path] = source

        StrategyLoader._validate_pack_imports(verified_files)

        normalized: list[dict[str, str]] = []
        seen_ids: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("strategy catalog entries must be mappings")
            keys = (
                "id",
                "module",
                "class",
                "display_name",
                "artifact_version",
                "readiness",
            )
            values = {key: entry.get(key) for key in keys}
            if not all(isinstance(value, str) and value for value in values.values()):
                raise ValueError(
                    "strategy catalog entries require id, module, class, "
                    "display_name, artifact_version, readiness"
                )
            strategy_id = values["id"]
            module_path = values["module"]
            readiness = values["readiness"]
            assert isinstance(strategy_id, str)
            assert isinstance(module_path, str)
            assert isinstance(readiness, str)
            if strategy_id in seen_ids:
                raise ValueError(f"duplicate strategy id: {strategy_id}")
            if module_path not in files:
                raise ValueError(
                    f"strategy module is not integrity-pinned: {module_path}"
                )
            if Path(module_path).suffix != ".py":
                raise ValueError("strategy module must be a Python source file")
            if readiness not in StrategyLoader.READINESS_VALUES:
                raise ValueError(f"unknown strategy readiness: {readiness}")
            seen_ids.add(strategy_id)
            normalized.append(cast(dict[str, str], values))
        return normalized, verified_files

    @staticmethod
    def _validate_pack_imports(files: dict[str, bytes]) -> None:
        internal_modules = StrategyLoader._declared_python_module_names(files)
        for relative_path, source in files.items():
            if Path(relative_path).suffix != ".py":
                continue
            tree = ast.parse(source, filename=relative_path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported_modules = tuple(alias.name for alias in node.names)
                elif (
                    isinstance(node, ast.ImportFrom)
                    and node.level == 0
                    and node.module is not None
                ):
                    imported_modules = (node.module,)
                else:
                    continue
                for module_name in imported_modules:
                    if any(
                        module_name == internal
                        or module_name.startswith(f"{internal}.")
                        for internal in internal_modules
                    ):
                        raise ValueError(
                            "strategy pack internal imports must be relative: "
                            f"{relative_path}:{node.lineno} imports {module_name}"
                        )

    @staticmethod
    def _snapshot_catalog_files(
        root: Path,
        files: dict[str, bytes],
    ) -> tuple[Path, str]:
        generation = hashlib.sha256(str(root.resolve()).encode())
        for relative_path, source in sorted(files.items()):
            path_bytes = relative_path.encode()
            generation.update(len(path_bytes).to_bytes(8, "big"))
            generation.update(path_bytes)
            generation.update(hashlib.sha256(source).digest())
        package_name = f"_fluxtrade_pack_{generation.hexdigest()}"
        existing = StrategyLoader._catalog_snapshots.get(package_name)
        if existing is not None:
            return Path(existing.name), package_name

        snapshot = tempfile.TemporaryDirectory(prefix="fluxtrade-strategy-")
        snapshot_root = Path(snapshot.name)
        try:
            for relative_path, source in files.items():
                target = snapshot_root / relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source)
        except Exception:
            snapshot.cleanup()
            raise
        # A running instance may still lazy-import from an older generation.
        # Retain each small snapshot until process exit; add reference tracking
        # only if hot-reload volume makes this measurable.
        StrategyLoader._catalog_snapshots[package_name] = snapshot
        return snapshot_root, package_name

    @staticmethod
    def _ensure_catalog_package(package_name: str, snapshot_root: Path) -> None:
        if package_name in sys.modules:
            return
        package = ModuleType(package_name)
        package.__package__ = package_name
        package.__path__ = [str(snapshot_root)]  # type: ignore[attr-defined]
        package.__spec__ = importlib.util.spec_from_loader(
            package_name,
            loader=None,
            is_package=True,
        )
        sys.modules[package_name] = package

    @staticmethod
    def _ensure_catalog_module_parent(
        package_name: str,
        relative_parent: Path,
    ) -> None:
        if relative_parent == Path("."):
            return
        importlib.import_module(f"{package_name}.{'.'.join(relative_parent.parts)}")

    @staticmethod
    def _declared_python_module_names(files: dict[str, bytes]) -> set[str]:
        names: set[str] = set()
        for relative_path in files:
            path = Path(relative_path)
            if path.suffix != ".py":
                continue
            parts = list(path.with_suffix("").parts)
            if parts[-1] == "__init__":
                parts.pop()
            for length in range(1, len(parts) + 1):
                names.add(".".join(parts[:length]))
        return names

    @staticmethod
    def _catalog_module_name(
        package_name: str,
        relative_parent: Path,
        strategy_id: str,
    ) -> str:
        strategy_digest = hashlib.sha256(strategy_id.encode()).hexdigest()
        parent_name = ".".join(relative_parent.parts)
        prefix = f"{package_name}.{parent_name}" if parent_name else package_name
        return f"{prefix}._entry_{strategy_digest}"

    @staticmethod
    def _legacy_module_name(file_path: Path) -> str:
        path_digest = hashlib.sha256(str(file_path.resolve()).encode()).hexdigest()
        return f"_fluxtrade_legacy_strategy_{path_digest}"

    @staticmethod
    def _resolve_pack_file(root: Path, relative_path: str) -> Path:
        candidate = Path(relative_path)
        if candidate.is_absolute():
            raise ValueError("strategy pack paths must be relative")
        resolved_root = root.resolve()
        resolved = (resolved_root / candidate).resolve()
        if not resolved.is_relative_to(resolved_root) or not resolved.is_file():
            raise ValueError(f"invalid strategy pack file: {relative_path}")
        return resolved

    @staticmethod
    def _load_module(
        file_path: Path,
        module_name: str,
        *,
        import_root: Path | None = None,
        evict_modules: bool = True,
        expose_import_root: bool = True,
    ):
        module_root = import_root or file_path.parent
        if evict_modules:
            StrategyLoader._evict_modules_from(module_root)
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"unable to load strategy module: {file_path.name}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        import_path = str(module_root)
        if expose_import_root:
            sys.path.insert(0, import_path)
        previous_dont_write_bytecode = sys.dont_write_bytecode
        sys.dont_write_bytecode = True
        importlib.invalidate_caches()
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        finally:
            sys.dont_write_bytecode = previous_dont_write_bytecode
            if expose_import_root:
                sys.path.remove(import_path)
        return module

    @staticmethod
    def _evict_modules_from(root: Path) -> None:
        resolved_root = root.resolve()
        for name, module in tuple(sys.modules.items()):
            if StrategyLoader._module_is_from_root(module, resolved_root):
                sys.modules.pop(name, None)

    @staticmethod
    def _module_is_from_root(module: object, root: Path) -> bool:
        module_file = getattr(module, "__file__", None)
        if module_file is None:
            return False
        try:
            return Path(module_file).resolve().is_relative_to(root.resolve())
        except (OSError, RuntimeError):
            return False
