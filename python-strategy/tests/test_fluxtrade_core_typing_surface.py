from __future__ import annotations

import ast
import inspect
from pathlib import Path

import fluxtrade_core


STUB_PATH = Path(__file__).parents[1] / "typings/fluxtrade_core/__init__.pyi"
COMMON_EXPORTS = (
    "CandleAggregator",
    "Candlestick",
    "FillEvent",
    "Order",
    "Position",
    "PyMatchingEngine",
    "ScaledCandlestick",
    "Trade",
    "VolumeProfileMergeResult",
)

MERGE_PARAMETERS = (
    "product_id",
    "bin_origin",
    "bin_step",
    "unit",
    "days",
    "output_step",
)


def _annotation(node: ast.expr | None) -> str:
    assert node is not None
    return ast.unparse(node)


def test_profile_merge_stub_contract() -> None:
    tree = ast.parse(STUB_PATH.read_text())
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
    assert [node.name for node in functions] == ["merge_volume_profiles"]
    function = functions[0]
    assert tuple(arg.arg for arg in function.args.args) == MERGE_PARAMETERS
    assert not function.args.defaults and not function.args.kwonlyargs
    assert _annotation(function.returns) == "VolumeProfileMergeResult"
    assert [_annotation(arg.annotation) for arg in function.args.args] == [
        "str",
        "str",
        "str",
        "str",
        "list[_ProfileDay] | tuple[_ProfileDay, ...]",
        "str",
    ]
    aliases = {
        ast.unparse(node.targets[0]): ast.unparse(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
    }
    assert aliases == {
        "_ProfileBin": "tuple[int, str, str, int]",
        "_ProfileDay": "tuple[int, int, list[_ProfileBin] | tuple[_ProfileBin, ...]]",
    }
    result = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "VolumeProfileMergeResult"
    )
    assert [ast.unparse(item) for item in result.decorator_list] == ["final"]
    expected = {
        "product_id": "str",
        "window_start_ms": "int",
        "window_end_ms": "int",
        "bin_origin": "str",
        "bin_step": "str",
        "unit": "str",
        "bins": "list[tuple[int, str, str, int]]",
        "base_volume": "str",
        "quote_volume": "str",
        "aggregate_count": "int",
        "poc_index": "int | None",
        "poc_low": "str | None",
        "poc_high_exclusive": "str | None",
    }
    constructor = result.body[0]
    assert isinstance(constructor, ast.FunctionDef)
    assert constructor.name == "__init__" and not constructor.decorator_list
    assert [arg.arg for arg in constructor.args.args] == ["self", "_not_constructible"]
    assert _annotation(constructor.args.args[1].annotation) == "Never"
    assert _annotation(constructor.returns) == "None"
    assert not constructor.args.defaults and not constructor.args.kwonlyargs
    assert len(result.body) == len(expected) + 1
    for member in result.body[1:]:
        assert isinstance(member, ast.FunctionDef)
        assert [ast.unparse(item) for item in member.decorator_list] == ["property"]
        assert [arg.arg for arg in member.args.args] == ["self"]
        assert _annotation(member.returns) == expected.pop(member.name)
    assert not expected


def test_profile_merge_runtime_signature() -> None:
    function = fluxtrade_core.merge_volume_profiles
    assert function.__name__ == "merge_volume_profiles"
    assert tuple(inspect.signature(function).parameters) == MERGE_PARAMETERS


def test_common_typing_surface_is_explicit_and_feature_independent() -> None:
    source = STUB_PATH.read_text()
    tree = ast.parse(source)

    declared = {node.name for node in tree.body if isinstance(node, ast.ClassDef)}
    assert declared == set(COMMON_EXPORTS)
    assert "Rithmic" not in source
    assert "Any" not in source
    assert "__getattr__" not in source
    assert "cast(" not in source
    assert "# type: ignore" not in source
    assert "# pyright:" not in source
    assert "noqa" not in source
    assert not any(
        isinstance(node, ast.arguments) and (node.vararg or node.kwarg)
        for node in ast.walk(tree)
    )


def test_common_runtime_exports_match_the_compiled_extension() -> None:
    for name in COMMON_EXPORTS:
        exported = getattr(fluxtrade_core, name)
        assert exported.__name__ == name

    candle = fluxtrade_core.Candlestick(
        "BINANCE:BTCUSDT-PERP", "1m", 60_000, "100", "101", "99", "100.5", "2"
    )
    assert (
        candle.product_id,
        candle.timeframe,
        candle.timestamp,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
    ) == ("BINANCE:BTCUSDT-PERP", "1m", 60_000, "100", "101", "99", "100.5", "2")

    order = fluxtrade_core.Order(
        "o1", "BINANCE:BTCUSDT-PERP", "LONG", "MARKET", "0", "1", 1
    )
    assert order.strategy_id == ""
    assert order.trigger_price is None
    assert order.trailing_distance is None
    assert order.linked_order_id is None

    trade = fluxtrade_core.Trade("t1", "BINANCE:BTCUSDT-PERP", "100.5", "1", "buy", 1)
    assert (trade.price, trade.quantity, trade.side) == ("100.5", "1", "buy")

    fill = fluxtrade_core.FillEvent(
        "o1", "BINANCE:BTCUSDT-PERP", "100.5", "1", "0.1", 1
    )
    assert (fill.fill_type, fill.strategy_id, fill.fee) == ("MARKET", "", "0.1")

    position = fluxtrade_core.Position(
        "BINANCE:BTCUSDT-PERP", "LONG", "1", "100.5", "0", "strategy"
    )
    assert (position.quantity, position.entry_price, position.unrealized_pnl) == (
        "1",
        "100.5",
        "0",
    )

    scaled = fluxtrade_core.ScaledCandlestick(
        "BINANCE:BTCUSDT-PERP", "1m", 1, 1000, 1010, 990, 1005, 20
    )
    assert (scaled.close_units, scaled.volume_units) == (1005, 20)

    aggregator = fluxtrade_core.CandleAggregator()
    assert aggregator.can_aggregate("1m", "5m") is True
    assert aggregator.add_candle(candle, "5m") is None
    assert aggregator.reset_product(candle.product_id) is None

    engine = fluxtrade_core.PyMatchingEngine("10000")
    assert engine.balance == "10000"
    assert engine.positions == {}
    assert engine.open_orders == []
    assert engine.submit_order(order) == "o1"
    assert engine.get_positions() == {}
    assert engine.get_position("strategy", candle.product_id) is None
    assert engine.cancel_order("o1") is True
    engine.set_scaled_precision("0.1", "0.01")
    assert engine.on_candle(candle) == []
    assert engine.on_matching_tick(candle) == []
    assert engine.on_scaled_candle(scaled) == []


def test_stub_constructor_names_match_runtime_signatures() -> None:
    expected = {
        "Candlestick": (
            "product_id",
            "timeframe",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ),
        "FillEvent": (
            "order_id",
            "product_id",
            "price",
            "quantity",
            "fee",
            "timestamp",
            "fill_type",
            "strategy_id",
        ),
        "Order": (
            "id",
            "product_id",
            "side",
            "order_type",
            "price",
            "quantity",
            "timestamp",
            "trigger_price",
            "trailing_distance",
            "linked_order_id",
            "strategy_id",
        ),
        "Position": (
            "product_id",
            "side",
            "quantity",
            "entry_price",
            "unrealized_pnl",
            "strategy_id",
        ),
        "ScaledCandlestick": (
            "product_id",
            "timeframe",
            "timestamp",
            "open_units",
            "high_units",
            "low_units",
            "close_units",
            "volume_units",
        ),
        "Trade": ("id", "product_id", "price", "quantity", "side", "timestamp"),
    }
    classes = {
        node.name: node
        for node in ast.parse(STUB_PATH.read_text()).body
        if isinstance(node, ast.ClassDef)
    }
    for name, parameters in expected.items():
        initializer = next(
            node
            for node in classes[name].body
            if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        )
        stub_parameters = tuple(argument.arg for argument in initializer.args.args[1:])
        assert stub_parameters == parameters
        assert (
            tuple(inspect.signature(getattr(fluxtrade_core, name)).parameters)
            == parameters
        )
