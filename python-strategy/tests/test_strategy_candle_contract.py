import ast
from pathlib import Path

import pytest

_PRODUCTION_STRATEGIES = (
    ("callable_strategy.py", "CallableStrategy"),
    ("csv_signal_strategy.py", "CsvSignalStrategy"),
    ("example.py", "RandomStrategy"),
    ("golden_cross.py", "GoldenCrossStrategy"),
)


@pytest.mark.parametrize(("filename", "class_name"), _PRODUCTION_STRATEGIES)
def test_production_strategy_candle_contract_accepts_optional_context(
    filename: str,
    class_name: str,
):
    source = Path(__file__).parents[1] / "src" / "strategies" / filename
    module = ast.parse(source.read_text())
    strategy_class = next(
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    on_candle = next(
        node
        for node in strategy_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "on_candle"
    )

    assert [argument.arg for argument in on_candle.args.args] == [
        "self",
        "candle",
        "context",
    ]
    assert len(on_candle.args.defaults) == 1
    assert isinstance(on_candle.args.defaults[0], ast.Constant)
    assert on_candle.args.defaults[0].value is None
