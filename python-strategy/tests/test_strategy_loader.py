"""
Tests for src/core/strategy_loader.py

Covers:
- Scanning empty directories
- Finding BaseStrategy subclasses
- Ignoring non-strategy files and __init__.py
- Handling syntax errors in strategy files
- Handling import errors in strategy files
- Multiple strategies in one file
"""

import hashlib
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.models import Candlestick
from src.core.portfolio_runtime import (
    PortfolioFactory,
    build_portfolio_artifact,
)
from src.core.strategy_loader import LoadResult, StrategyLoader
from src.strategies.base import BaseStrategy
from src.validation.strategy_evidence import portfolio_evidence_identity


def _expect_load_error(result: LoadResult) -> str:
    assert isinstance(result, str)
    return result


def _strategy_class(result: LoadResult) -> type[BaseStrategy]:
    assert isinstance(result, type)
    assert issubclass(result, BaseStrategy)
    return result


def _portfolio_factory_class(result: LoadResult) -> type[PortfolioFactory]:
    assert isinstance(result, type)
    assert issubclass(result, PortfolioFactory)
    return result


_IGNORED_CANDLE = Candlestick(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1m",
    timestamp=0,
    open=Decimal("1"),
    high=Decimal("1"),
    low=Decimal("1"),
    close=Decimal("1"),
    volume=Decimal("0"),
)

STRATEGY_CODE = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Signal, SignalType

class ManifestStrategy(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements(self.product_id, "5m", 10)
    def on_candle(self, candle):
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )
"""

PORTFOLIO_CODE = """
from decimal import Decimal
from src.core.portfolio_runtime import (
    PortfolioDefinition,
    PortfolioFactory,
    PortfolioSleeve,
)
from .sleeve_a import Sleeve as SleeveA
from .sleeve_b import Sleeve as SleeveB

class ManifestPortfolioFactory(PortfolioFactory):
    def build(self, *, portfolio_id, product_id, config):
        sleeve_class = SleeveB if config.get("sleeve") == "b" else SleeveA
        return PortfolioDefinition(
            portfolio_id=portfolio_id,
            product_id=product_id,
            sleeves=(
                PortfolioSleeve(sleeve_class(f"{portfolio_id}.sleeve", product_id)),
            ),
            max_gross_quantity=Decimal(str(config["max_gross_quantity"])),
        )
"""

PORTFOLIO_SLEEVE_A_CODE = """
from src.strategies.base import BaseStrategy, StrategyRequirements

class Sleeve(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements(self.product_id, "5m", 10)
    def on_candle(self, candle):
        return None
    def replay_configuration(self):
        return {"strategy_id": self.strategy_id, "product_id": self.product_id}
"""

PORTFOLIO_SLEEVE_B_CODE = """
from src.core.models import Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements

class Sleeve(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements(self.product_id, "5m", 10)
    def on_candle(self, candle):
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )
    def replay_configuration(self):
        return {"strategy_id": self.strategy_id, "product_id": self.product_id}
"""


def _write_catalog(
    tmp_path,
    *,
    digest: str | None = None,
    module_name: str = "strategy.py",
    **entry_overrides,
):
    module = tmp_path / module_name
    module.write_text(STRATEGY_CODE)
    entry = {
        "id": "stable_strategy_v1",
        "module": module.name,
        "class": "ManifestStrategy",
        "display_name": "Stable Strategy v1",
        "artifact_version": "1.0.0",
        "readiness": "RESEARCH_VALIDATED",
        **entry_overrides,
    }
    catalog = {
        "schema_version": 1,
        "files": {
            module.name: digest or hashlib.sha256(module.read_bytes()).hexdigest()
        },
        "strategies": [entry],
    }
    (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))
    return catalog


@contextmanager
def _read_only_tree(root: Path):
    paths = [root, *root.rglob("*")]
    for path in reversed(paths):
        if not path.is_symlink():
            path.chmod(0o555 if path.is_dir() else 0o444)
    try:
        yield
    finally:
        for path in paths:
            if path.exists() and not path.is_symlink():
                path.chmod(0o755 if path.is_dir() else 0o644)


def _load_error(result: dict) -> str:
    assert set(result) == {f"{StrategyLoader.CATALOG_NAME}::LoadError"}
    error = result[f"{StrategyLoader.CATALOG_NAME}::LoadError"]
    assert isinstance(error, str)
    return error


class TestProductionSources:
    def test_empty_read_only_primary_has_no_legacy_fallback(self, tmp_path):
        with _read_only_tree(tmp_path):
            assert StrategyLoader.scan_production_sources(str(tmp_path)) == {}

    def test_missing_primary_fails_closed(self, tmp_path):
        result = StrategyLoader.scan_production_sources(str(tmp_path / "missing"))

        assert "does not exist" in _load_error(result)

    def test_valid_catalog_loads_after_complete_preflight(self, tmp_path):
        _write_catalog(tmp_path)

        with _read_only_tree(tmp_path):
            result = StrategyLoader.scan_production_sources(str(tmp_path))

        assert set(result) == {"stable_strategy_v1"}
        strategy = result["stable_strategy_v1"]
        assert isinstance(strategy, type)
        assert issubclass(strategy, BaseStrategy)

    def test_loose_python_is_rejected_before_import(self, tmp_path):
        (tmp_path / "strategy.py").write_text(STRATEGY_CODE)

        with (
            _read_only_tree(tmp_path),
            patch.object(StrategyLoader, "_load_module") as load_module,
        ):
            result = StrategyLoader.scan_production_sources(str(tmp_path))

        assert "catalog-only" in _load_error(result)
        load_module.assert_not_called()

    @pytest.mark.parametrize("writable_part", ["root", "catalog", "module"])
    def test_writable_consumed_tree_is_rejected_before_import(
        self,
        tmp_path,
        writable_part,
    ):
        _write_catalog(tmp_path)
        targets = {
            "root": tmp_path,
            "catalog": tmp_path / StrategyLoader.CATALOG_NAME,
            "module": tmp_path / "strategy.py",
        }

        with _read_only_tree(tmp_path):
            target = targets[writable_part]
            target.chmod(0o755 if target.is_dir() else 0o644)
            with patch.object(StrategyLoader, "_load_module") as load_module:
                result = StrategyLoader.scan_production_sources(str(tmp_path))

        assert "writable" in _load_error(result)
        load_module.assert_not_called()

    def test_writable_nested_parent_is_rejected_before_import(self, tmp_path):
        pack = tmp_path / "pack"
        pack.mkdir()
        _write_catalog(pack)

        with _read_only_tree(tmp_path):
            pack.chmod(0o755)
            with patch.object(StrategyLoader, "_load_module") as load_module:
                result = StrategyLoader.scan_production_sources(str(tmp_path))

        assert "writable" in _load_error(result)
        load_module.assert_not_called()

    @pytest.mark.parametrize("link_name", ["strategy.py", "strategy_catalog.json"])
    def test_symlink_is_rejected_before_import(self, tmp_path, link_name):
        source = tmp_path.parent / f"outside-{link_name}"
        if link_name == "strategy.py":
            source.write_text(STRATEGY_CODE)
            module = tmp_path / link_name
            module.symlink_to(source)
            catalog = {
                "schema_version": 1,
                "files": {link_name: hashlib.sha256(source.read_bytes()).hexdigest()},
                "strategies": [
                    {
                        "id": "stable_strategy_v1",
                        "module": link_name,
                        "class": "ManifestStrategy",
                        "display_name": "Stable Strategy v1",
                        "artifact_version": "1.0.0",
                        "readiness": "RESEARCH_VALIDATED",
                    }
                ],
            }
            (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))
        else:
            (tmp_path / "strategy.py").write_text(STRATEGY_CODE)
            catalog_root = tmp_path.parent / "outside-catalog-root"
            catalog_root.mkdir()
            _write_catalog(catalog_root)
            (tmp_path / link_name).symlink_to(
                catalog_root / StrategyLoader.CATALOG_NAME
            )

        with (
            _read_only_tree(tmp_path),
            patch.object(StrategyLoader, "_load_module") as load_module,
        ):
            result = StrategyLoader.scan_production_sources(str(tmp_path))

        assert "symlink" in _load_error(result)
        load_module.assert_not_called()

    def test_duplicate_break_glass_id_rejects_complete_combined_result(
        self,
        tmp_path,
    ):
        primary = tmp_path / "primary"
        break_glass = tmp_path / "break-glass"
        primary.mkdir()
        break_glass.mkdir()
        _write_catalog(primary)
        _write_catalog(break_glass)

        with _read_only_tree(tmp_path):
            result = StrategyLoader.scan_production_sources(
                str(primary),
                break_glass_path=str(break_glass),
            )

        assert "duplicate strategy id" in _load_error(result)

    def test_invalid_break_glass_does_not_publish_primary_partial_result(
        self,
        tmp_path,
    ):
        primary = tmp_path / "primary"
        break_glass = tmp_path / "break-glass"
        primary.mkdir()
        break_glass.mkdir()
        _write_catalog(primary)
        (break_glass / "loose.py").write_text(STRATEGY_CODE)

        with (
            _read_only_tree(tmp_path),
            patch.object(StrategyLoader, "_load_module") as load_module,
        ):
            result = StrategyLoader.scan_production_sources(
                str(primary),
                break_glass_path=str(break_glass),
            )

        assert "catalog-only" in _load_error(result)
        load_module.assert_not_called()


class TestScanEmptyAndMissing:
    def test_scan_nonexistent_directory(self, tmp_path):
        """Should return empty dict for nonexistent path."""
        result = StrategyLoader.scan_directory(str(tmp_path / "nonexistent"))
        assert result == {}

    def test_scan_empty_directory(self, tmp_path):
        """Should return empty dict for empty directory."""
        result = StrategyLoader.scan_directory(str(tmp_path))
        assert result == {}

    def test_scan_directory_with_only_init(self, tmp_path):
        """Should skip __init__.py files."""
        (tmp_path / "__init__.py").write_text("# init")
        result = StrategyLoader.scan_directory(str(tmp_path))
        assert result == {}


class TestFindStrategies:
    def test_finds_single_strategy(self, tmp_path):
        """Should discover a valid BaseStrategy subclass."""
        code = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class MyStrategy(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="1m",
            timestamp=0,
            type=SignalType.NO_SIGNAL,
            value=candle.close,
        )
"""
        (tmp_path / "my_strat.py").write_text(code)
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "my_strat.py::MyStrategy" in result
        strategy_class = _strategy_class(result["my_strat.py::MyStrategy"])
        assert issubclass(strategy_class, BaseStrategy)

    def test_finds_hash_pinned_portfolio_factory(self, tmp_path):
        module = tmp_path / "portfolio.py"
        module.write_text(PORTFOLIO_CODE)
        sleeve_a = tmp_path / "sleeve_a.py"
        sleeve_a.write_text(PORTFOLIO_SLEEVE_A_CODE)
        sleeve_b = tmp_path / "sleeve_b.py"
        sleeve_b.write_text(PORTFOLIO_SLEEVE_B_CODE)
        catalog = {
            "schema_version": 2,
            "files": {
                module.name: hashlib.sha256(module.read_bytes()).hexdigest(),
                sleeve_a.name: hashlib.sha256(sleeve_a.read_bytes()).hexdigest(),
                sleeve_b.name: hashlib.sha256(sleeve_b.read_bytes()).hexdigest(),
            },
            "strategies": [],
            "portfolios": [
                {
                    "id": "portfolio_v1",
                    "module": module.name,
                    "class": "ManifestPortfolioFactory",
                    "display_name": "Portfolio v1",
                    "artifact_version": "1.0.0",
                    "readiness": "RESEARCH_FROZEN",
                }
            ],
        }
        (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))

        result = StrategyLoader.scan_directory(str(tmp_path))

        factory = _portfolio_factory_class(result["portfolio_v1"])
        assert issubclass(factory, PortfolioFactory)
        assert getattr(factory, "__fluxtrade_readiness__") == "RESEARCH_FROZEN"
        definition = build_portfolio_artifact(
            factory,
            portfolio_id="portfolio_v1",
            product_id="RITHMIC:MNQ_ROLL-PERP",
            config={"max_gross_quantity": "1"},
        )
        assert definition.readiness == "RESEARCH_FROZEN"
        assert definition.artifact_version == "1.0.0"
        assert definition.display_name == "Portfolio v1"
        assert definition.catalog_sha256 is not None

        relocated = tmp_path / "relocated"
        relocated.mkdir()
        for source in (module, sleeve_a, sleeve_b):
            (relocated / source.name).write_bytes(source.read_bytes())
        (relocated / StrategyLoader.CATALOG_NAME).write_bytes(
            (tmp_path / StrategyLoader.CATALOG_NAME).read_bytes()
        )
        relocated_factory = _portfolio_factory_class(
            StrategyLoader.scan_directory(str(relocated))["portfolio_v1"]
        )
        relocated_definition = build_portfolio_artifact(
            relocated_factory,
            portfolio_id="portfolio_v1",
            product_id="RITHMIC:MNQ_ROLL-PERP",
            config={"max_gross_quantity": "1"},
        )
        assert (
            portfolio_evidence_identity(definition).replay_configuration_sha256
            == portfolio_evidence_identity(
                relocated_definition
            ).replay_configuration_sha256
        )
        changed_config_definition = build_portfolio_artifact(
            relocated_factory,
            portfolio_id="portfolio_v1",
            product_id="RITHMIC:MNQ_ROLL-PERP",
            config={"max_gross_quantity": "2"},
        )
        assert (
            portfolio_evidence_identity(definition).replay_configuration_sha256
            != portfolio_evidence_identity(
                changed_config_definition
            ).replay_configuration_sha256
        )
        changed_class_definition = build_portfolio_artifact(
            relocated_factory,
            portfolio_id="portfolio_v1",
            product_id="RITHMIC:MNQ_ROLL-PERP",
            config={"max_gross_quantity": "1", "sleeve": "b"},
        )
        assert (
            portfolio_evidence_identity(definition).replay_configuration_sha256
            != portfolio_evidence_identity(
                changed_class_definition
            ).replay_configuration_sha256
        )

    def test_finds_multiple_strategies_in_one_file(self, tmp_path):
        """Should discover multiple subclasses in one file."""
        code = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class StratA(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)

class StratB(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "5m", 20)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="5m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
"""
        (tmp_path / "multi.py").write_text(code)
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "multi.py::StratA" in result
        assert "multi.py::StratB" in result

    def test_finds_strategies_across_files(self, tmp_path):
        """Should discover strategies from multiple .py files."""
        code_template = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class {name}(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
"""
        (tmp_path / "a.py").write_text(code_template.format(name="AlphaStrat"))
        (tmp_path / "b.py").write_text(code_template.format(name="BetaStrat"))
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "a.py::AlphaStrat" in result
        assert "b.py::BetaStrat" in result

    def test_ignores_imported_strategy_classes(self, tmp_path):
        code = """
from src.strategies.golden_cross import GoldenCrossStrategy
"""
        (tmp_path / "imports.py").write_text(code)

        assert StrategyLoader.scan_directory(str(tmp_path)) == {}

    def test_legacy_strategy_filename_does_not_replace_stdlib_module(
        self,
        tmp_path,
    ):
        stdlib_json = sys.modules["json"]
        (tmp_path / "json.py").write_text(STRATEGY_CODE)

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "json.py::ManifestStrategy" in result
        assert sys.modules["json"] is stdlib_json


class TestCatalogStrategies:
    def test_catalog_loads_only_declared_class_under_stable_id(self, tmp_path):
        _write_catalog(tmp_path)

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert list(result) == ["stable_strategy_v1"]
        strategy_class = _strategy_class(result["stable_strategy_v1"])
        assert issubclass(strategy_class, BaseStrategy)

    def test_catalog_rescan_reuses_unchanged_verified_generation(self, tmp_path):
        _write_catalog(tmp_path)

        first = _strategy_class(
            StrategyLoader.scan_directory(str(tmp_path))["stable_strategy_v1"]
        )
        snapshot_count = len(StrategyLoader._catalog_snapshots)
        second = _strategy_class(
            StrategyLoader.scan_directory(str(tmp_path))["stable_strategy_v1"]
        )

        assert first.__module__ == second.__module__
        assert len(StrategyLoader._catalog_snapshots) == snapshot_count

    def test_parent_directory_discovers_one_level_catalog_pack(self, tmp_path):
        pack = tmp_path / "stable_pack"
        pack.mkdir()
        _write_catalog(pack)

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert list(result) == ["stable_strategy_v1"]

    def test_parent_directory_rejects_duplicate_catalog_ids(self, tmp_path):
        for index, pack_name in enumerate(("first_pack", "second_pack")):
            pack = tmp_path / pack_name
            pack.mkdir()
            _write_catalog(pack, module_name=f"strategy_{index}.py")

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert result["stable_strategy_v1"] == (
            "duplicate strategy id across strategy packs: stable_strategy_v1"
        )

    def test_parent_directory_rejects_overlapping_helper_namespaces(
        self,
        tmp_path,
    ):
        for index, pack_name in enumerate(("first_pack", "second_pack")):
            pack = tmp_path / pack_name
            helper = pack / "private_lib/helper.py"
            helper.parent.mkdir(parents=True)
            helper.write_text(f"VALUE = {index}\n")
            module = pack / f"strategy_{index}.py"
            module.write_text(
                STRATEGY_CODE.replace(
                    "class ManifestStrategy",
                    f"class ManifestStrategy{index}",
                ).replace(
                    "    def on_candle(self, candle):\n",
                    "    def on_candle(self, candle):\n"
                    "        from private_lib.helper import VALUE\n"
                    "        self.helper_value = VALUE\n",
                )
            )
            catalog = {
                "schema_version": 1,
                "files": {
                    path.relative_to(pack).as_posix(): hashlib.sha256(
                        path.read_bytes()
                    ).hexdigest()
                    for path in (helper, module)
                },
                "strategies": [
                    {
                        "id": f"stable_strategy_v{index}",
                        "module": module.name,
                        "class": f"ManifestStrategy{index}",
                        "display_name": f"Stable Strategy v{index}",
                        "artifact_version": f"{index}.0.0",
                        "readiness": "RESEARCH_VALIDATED",
                    }
                ],
            }
            (pack / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert set(result) == {f"{StrategyLoader.CATALOG_NAME}::LoadError"}
        error = _expect_load_error(next(iter(result.values())))
        assert "module namespace overlap" in error
        assert "private_lib" in error

    @pytest.mark.parametrize(
        ("catalog_source", "expected_error"),
        [
            (b"[]", "schema_version"),
            (b"\xff", "UnicodeDecodeError"),
        ],
    )
    def test_parent_directory_returns_load_error_for_unreadable_child_catalog(
        self,
        tmp_path,
        catalog_source,
        expected_error,
    ):
        pack = tmp_path / "broken_pack"
        pack.mkdir()
        (pack / StrategyLoader.CATALOG_NAME).write_bytes(catalog_source)

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert expected_error in error

    def test_catalog_rejects_unpinned_pack_files(self, tmp_path):
        _write_catalog(tmp_path)
        (tmp_path / "helper.py").write_text("VALUE = 1\n")

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert "file set mismatch" in error
        assert "helper.py" in error

    def test_catalog_loads_integrity_pinned_helper(self, tmp_path):
        helper = tmp_path / "helper.py"
        helper.write_text("VALUE = 7\n")
        module = tmp_path / "strategy.py"
        module.write_text(
            f"from .helper import VALUE\n{STRATEGY_CODE}"
            "\nManifestStrategy.helper_value = VALUE\n"
        )
        catalog = {
            "schema_version": 1,
            "files": {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (helper, module)
            },
            "strategies": [
                {
                    "id": "stable_strategy_v1",
                    "module": module.name,
                    "class": "ManifestStrategy",
                    "display_name": "Stable Strategy v1",
                    "artifact_version": "1.0.0",
                    "readiness": "RESEARCH_VALIDATED",
                }
            ],
        }
        catalog_path = tmp_path / StrategyLoader.CATALOG_NAME
        catalog_path.write_text(json.dumps(catalog))
        catalog_digest = hashlib.sha256(catalog_path.read_bytes()).hexdigest()

        result = StrategyLoader.scan_directory(str(tmp_path))

        strategy_class = _strategy_class(result["stable_strategy_v1"])
        assert getattr(strategy_class, "helper_value") == 7
        assert (
            getattr(strategy_class, "__fluxtrade_readiness__") == "RESEARCH_VALIDATED"
        )
        assert getattr(strategy_class, "__fluxtrade_catalog_sha256__") == catalog_digest

        helper.write_text("VALUE = 8\n")
        catalog["files"]["helper.py"] = hashlib.sha256(helper.read_bytes()).hexdigest()
        catalog_path.write_text(json.dumps(catalog))
        rescanned_digest = hashlib.sha256(catalog_path.read_bytes()).hexdigest()

        rescanned = StrategyLoader.scan_directory(str(tmp_path))

        rescanned_class = _strategy_class(rescanned["stable_strategy_v1"])
        assert getattr(rescanned_class, "helper_value") == 8
        assert (
            getattr(rescanned_class, "__fluxtrade_catalog_sha256__") == rescanned_digest
        )

    def test_catalog_imports_the_verified_snapshot_when_source_changes(
        self,
        tmp_path,
        monkeypatch,
    ):
        helper = tmp_path / "helper.py"
        helper.write_text("VALUE = 7\n")
        module = tmp_path / "strategy.py"
        module.write_text(
            f"from .helper import VALUE\n{STRATEGY_CODE}"
            "\nManifestStrategy.helper_value = VALUE\n"
        )
        catalog = {
            "schema_version": 1,
            "files": {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (helper, module)
            },
            "strategies": [
                {
                    "id": "stable_strategy_v1",
                    "module": module.name,
                    "class": "ManifestStrategy",
                    "display_name": "Stable Strategy v1",
                    "artifact_version": "1.0.0",
                    "readiness": "RESEARCH_VALIDATED",
                }
            ],
        }
        (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))
        snapshot_catalog_files = StrategyLoader._snapshot_catalog_files

        def mutate_after_verification(root, files):
            helper.write_text("VALUE = 99\n")
            return snapshot_catalog_files(root, files)

        monkeypatch.setattr(
            StrategyLoader,
            "_snapshot_catalog_files",
            mutate_after_verification,
        )

        result = StrategyLoader.scan_directory(str(tmp_path))

        strategy_class = _strategy_class(result["stable_strategy_v1"])
        assert getattr(strategy_class, "helper_value") == 7

    def test_catalog_lazy_import_stays_bound_to_verified_generation(self, tmp_path):
        private_lib_before = sys.modules.get("private_lib")
        private_helper_before = sys.modules.get("private_lib.helper")
        helper = tmp_path / "private_lib/helper.py"
        helper.parent.mkdir()
        module = tmp_path / "strategy.py"
        module.write_text(
            "from src.strategies.base import BaseStrategy, StrategyRequirements\n"
            "class LazyStrategy(BaseStrategy):\n"
            "    @property\n"
            "    def requirements(self):\n"
            '        return StrategyRequirements(self.product_id, "5m", 1)\n'
            "    def on_candle(self, candle, context=None):\n"
            "        from .private_lib.helper import VALUE\n"
            "        return VALUE\n"
        )

        def write_catalog(value):
            helper.write_text(f"VALUE = {value}\n")
            catalog = {
                "schema_version": 1,
                "files": {
                    path.relative_to(tmp_path).as_posix(): hashlib.sha256(
                        path.read_bytes()
                    ).hexdigest()
                    for path in (helper, module)
                },
                "strategies": [
                    {
                        "id": "lazy_strategy_v1",
                        "module": module.name,
                        "class": "LazyStrategy",
                        "display_name": "Lazy Strategy v1",
                        "artifact_version": "1.0.0",
                        "readiness": "RESEARCH_VALIDATED",
                    }
                ],
            }
            (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))

        write_catalog(7)
        first_class = _strategy_class(
            StrategyLoader.scan_directory(str(tmp_path))["lazy_strategy_v1"]
        )
        helper.write_text("VALUE = 99\n")
        assert first_class("first", "product").on_candle(_IGNORED_CANDLE) == 7

        write_catalog(8)
        second_class = _strategy_class(
            StrategyLoader.scan_directory(str(tmp_path))["lazy_strategy_v1"]
        )

        assert first_class("first", "product").on_candle(_IGNORED_CANDLE) == 7
        assert second_class("second", "product").on_candle(_IGNORED_CANDLE) == 8
        assert sys.modules.get("private_lib") is private_lib_before
        assert sys.modules.get("private_lib.helper") is private_helper_before

    @pytest.mark.parametrize(
        "import_statement",
        [
            "import private_lib.helper",
            "from private_lib.helper import VALUE",
        ],
    )
    def test_catalog_rejects_absolute_internal_imports_during_scan(
        self,
        tmp_path,
        import_statement,
    ):
        helper = tmp_path / "private_lib/helper.py"
        helper.parent.mkdir()
        helper.write_text("VALUE = 7\n")
        module = tmp_path / "strategy.py"
        module.write_text(
            "from src.strategies.base import BaseStrategy, StrategyRequirements\n"
            "class InvalidStrategy(BaseStrategy):\n"
            "    @property\n"
            "    def requirements(self):\n"
            '        return StrategyRequirements(self.product_id, "5m", 1)\n'
            "    def on_candle(self, candle, context=None):\n"
            f"        {import_statement}\n"
            "        return None\n"
        )
        catalog = {
            "schema_version": 1,
            "files": {
                path.relative_to(tmp_path).as_posix(): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in (helper, module)
            },
            "strategies": [
                {
                    "id": "invalid_strategy_v1",
                    "module": module.name,
                    "class": "InvalidStrategy",
                    "display_name": "Invalid Strategy v1",
                    "artifact_version": "1.0.0",
                    "readiness": "LIVE_APPROVED",
                }
            ],
        }
        (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert "internal imports must be relative" in error
        assert "strategy.py:7" in error

    def test_concurrent_scans_are_serialized(self, monkeypatch):
        first_entered = threading.Event()
        release_first = threading.Event()
        calls = 0
        calls_lock = threading.Lock()

        def controlled_scan(_path):
            nonlocal calls
            with calls_lock:
                calls += 1
                call_number = calls
            if call_number == 1:
                first_entered.set()
                assert release_first.wait(timeout=1)
            return {}

        monkeypatch.setattr(StrategyLoader, "_scan_directory", controlled_scan)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(StrategyLoader.scan_directory, "first")
            assert first_entered.wait(timeout=1)
            second = executor.submit(StrategyLoader.scan_directory, "second")
            time.sleep(0.05)
            assert calls == 1
            release_first.set()
            assert first.result(timeout=1) == {}
            assert second.result(timeout=1) == {}
        assert calls == 2

    @pytest.mark.parametrize(
        ("catalog_update", "expected_error"),
        [
            ({"schema_version": 3}, "schema_version"),
            ({"files": {}}, "non-empty mapping"),
            ({"strategies": []}, "non-empty list"),
        ],
    )
    def test_invalid_catalog_fails_the_pack(
        self,
        tmp_path,
        catalog_update,
        expected_error,
    ):
        catalog = _write_catalog(tmp_path)
        catalog.update(catalog_update)
        (tmp_path / StrategyLoader.CATALOG_NAME).write_text(json.dumps(catalog))

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert expected_error in error

    def test_digest_mismatch_fails_before_import(self, tmp_path):
        _write_catalog(tmp_path, digest="0" * 64)

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert "digest mismatch" in error

    def test_catalog_rejects_unpinned_or_external_modules(self, tmp_path):
        outside = tmp_path.parent / "outside_strategy.py"
        outside.write_text(STRATEGY_CODE)
        catalog = _write_catalog(tmp_path, module="../outside_strategy.py")

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert "not integrity-pinned" in error
        assert catalog["files"] == {
            "strategy.py": hashlib.sha256(
                (tmp_path / "strategy.py").read_bytes()
            ).hexdigest()
        }

    def test_missing_or_imported_catalog_class_is_a_strategy_error(self, tmp_path):
        _write_catalog(tmp_path, **{"class": "BaseStrategy"})

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "BaseStrategy" in _expect_load_error(result["stable_strategy_v1"])

    def test_catalog_rejects_unknown_readiness(self, tmp_path):
        _write_catalog(tmp_path, readiness="READY_ENOUGH")

        result = StrategyLoader.scan_directory(str(tmp_path))

        error = _expect_load_error(result[f"{StrategyLoader.CATALOG_NAME}::LoadError"])
        assert "unknown strategy readiness" in error

    def test_catalog_accepts_research_frozen_readiness(self, tmp_path):
        _write_catalog(tmp_path, readiness="RESEARCH_FROZEN")

        result = StrategyLoader.scan_directory(str(tmp_path))

        strategy_class = _strategy_class(result["stable_strategy_v1"])
        assert getattr(strategy_class, "__fluxtrade_readiness__") == "RESEARCH_FROZEN"


class TestIgnoreNonStrategies:
    def test_ignores_file_without_base_subclass(self, tmp_path):
        """Files without BaseStrategy subclass should be ignored."""
        (tmp_path / "util.py").write_text("def helper(): return 42\n")
        result = StrategyLoader.scan_directory(str(tmp_path))
        # No strategy keys, only possibly debug log
        strategy_keys = [k for k in result if "LoadError" not in k]
        assert strategy_keys == []

    def test_ignores_non_python_files(self, tmp_path):
        """Non-.py files should not be scanned."""
        (tmp_path / "notes.txt").write_text("some notes")
        (tmp_path / "data.csv").write_text("a,b\n1,2")
        result = StrategyLoader.scan_directory(str(tmp_path))
        assert result == {}

    def test_ignores_base_strategy_itself(self, tmp_path):
        """BaseStrategy itself should not appear as a discovered strategy."""
        code = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class ConcreteStrat(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
"""
        (tmp_path / "strat.py").write_text(code)
        result = StrategyLoader.scan_directory(str(tmp_path))

        # Only the concrete subclass should appear
        keys = list(result.keys())
        assert len(keys) == 1
        assert "ConcreteStrat" in keys[0]


class TestErrorHandling:
    def test_syntax_error_captured_as_load_error(self, tmp_path):
        """Syntax errors should produce a LoadError entry."""
        (tmp_path / "bad_syntax.py").write_text("def foo(:\n  pass\n")
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "bad_syntax.py::LoadError" in result
        assert isinstance(result["bad_syntax.py::LoadError"], str)
        assert "SyntaxError" in result["bad_syntax.py::LoadError"]

    def test_import_error_captured_as_load_error(self, tmp_path):
        """Import errors should produce a LoadError entry."""
        (tmp_path / "bad_import.py").write_text("import nonexistent_module_xyz\n")
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "bad_import.py::LoadError" in result
        assert isinstance(result["bad_import.py::LoadError"], str)

    def test_runtime_error_captured_as_load_error(self, tmp_path):
        """Runtime errors at module level should produce a LoadError entry."""
        (tmp_path / "runtime_err.py").write_text("raise ValueError('boom')\n")
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "runtime_err.py::LoadError" in result
        assert "ValueError" in _expect_load_error(result["runtime_err.py::LoadError"])

    def test_mixed_good_and_bad_files(self, tmp_path):
        """Good files should load even when other files have errors."""
        good_code = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class GoodStrat(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
"""
        (tmp_path / "good.py").write_text(good_code)
        (tmp_path / "bad.py").write_text("def broken(:\n  pass\n")

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "good.py::GoodStrat" in result
        strategy_class = _strategy_class(result["good.py::GoodStrat"])
        assert issubclass(strategy_class, BaseStrategy)
        assert "bad.py::LoadError" in result
