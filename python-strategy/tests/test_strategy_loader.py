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

from src.core.strategy_loader import StrategyLoader
from src.strategies.base import BaseStrategy


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
        code = '''
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
'''
        (tmp_path / "my_strat.py").write_text(code)
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "my_strat.py::MyStrategy" in result
        assert issubclass(result["my_strat.py::MyStrategy"], BaseStrategy)

    def test_finds_multiple_strategies_in_one_file(self, tmp_path):
        """Should discover multiple subclasses in one file."""
        code = '''
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
'''
        (tmp_path / "multi.py").write_text(code)
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "multi.py::StratA" in result
        assert "multi.py::StratB" in result

    def test_finds_strategies_across_files(self, tmp_path):
        """Should discover strategies from multiple .py files."""
        code_template = '''
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class {name}(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
'''
        (tmp_path / "a.py").write_text(code_template.format(name="AlphaStrat"))
        (tmp_path / "b.py").write_text(code_template.format(name="BetaStrat"))
        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "a.py::AlphaStrat" in result
        assert "b.py::BetaStrat" in result


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
        code = '''
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class ConcreteStrat(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
'''
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
        assert "ValueError" in result["runtime_err.py::LoadError"]

    def test_mixed_good_and_bad_files(self, tmp_path):
        """Good files should load even when other files have errors."""
        good_code = '''
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class GoodStrat(BaseStrategy):
    @property
    def requirements(self):
        return StrategyRequirements("X:Y-PERP", "1m", 10)
    def on_candle(self, candle):
        return Signal(strategy_id=self.strategy_id, product_id=self.product_id,
                      timeframe="1m", timestamp=0, type=SignalType.NO_SIGNAL, value=candle.close)
'''
        (tmp_path / "good.py").write_text(good_code)
        (tmp_path / "bad.py").write_text("def broken(:\n  pass\n")

        result = StrategyLoader.scan_directory(str(tmp_path))

        assert "good.py::GoodStrat" in result
        assert issubclass(result["good.py::GoodStrat"], BaseStrategy)
        assert "bad.py::LoadError" in result
