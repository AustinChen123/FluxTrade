from __future__ import annotations

import ast
import builtins
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter


ROOT = Path(__file__).parents[1]
FEATURE_SURFACE = ROOT / "src/core/adapters/rithmic_native_types.py"
COMMON_STUB = ROOT / "typings/fluxtrade_core/__init__.pyi"
ADAPTER_SOURCE = ROOT / "src/core/adapters/rithmic_adapter.py"
RUST_ORDER_SOURCE = ROOT.parent / "rust-data-service/src/binding/rithmic_order.rs"
RUST_MODULE_SOURCE = ROOT.parent / "rust-data-service/src/lib.rs"
PRODUCT_ID = "RITHMIC:NQ-202609"
INSTRUMENTS = {
    PRODUCT_ID: {
        "exchange": "CME",
        "quantity_step": "1",
        "price_tick": "0.25",
    }
}


def _method_parameters(class_node: ast.ClassDef) -> dict[str, tuple[str, ...]]:
    return {
        node.name: tuple(argument.arg for argument in node.args.args)
        for node in class_node.body
        if isinstance(node, ast.FunctionDef)
    }


def test_feature_typing_surface_is_separate_and_exact() -> None:
    source = FEATURE_SURFACE.read_text()
    tree = ast.parse(source)
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}

    assert set(classes) == {
        "RithmicLedgerOrder",
        "RithmicOrderAck",
        "RithmicOrderClient",
        "RithmicOrderEvent",
    }
    assert all(
        tuple(base.id for base in node.bases if isinstance(base, ast.Name))
        == ("Protocol",)
        for node in classes.values()
    )
    assert _method_parameters(classes["RithmicOrderClient"]) == {
        "__init__": ("self", "profile", "account_id"),
        "is_connected": ("self",),
        "connection_generation": ("self",),
        "submit": (
            "self",
            "client_order_id",
            "exchange",
            "symbol",
            "quantity",
            "side",
            "order_type",
            "price",
        ),
        "submit_bracket": (
            "self",
            "client_order_id",
            "exchange",
            "symbol",
            "quantity",
            "side",
            "order_type",
            "price",
            "stop_ticks",
            "target_ticks",
        ),
        "modify_protection": (
            "self",
            "basket_id",
            "exchange",
            "symbol",
            "quantity",
            "leg_type",
            "price",
        ),
        "cancel": ("self", "basket_id"),
        "exit_position": ("self", "exchange", "symbol", "window_name"),
        "lookup": ("self", "client_order_id", "exchange", "symbol"),
        "poll_event": ("self",),
    }
    assert "Rithmic" not in COMMON_STUB.read_text()
    for forbidden in (
        "Any",
        "object",
        "__getattr__",
        "cast(",
        "# type: ignore",
        "# pyright:",
        "noqa",
    ):
        assert forbidden not in source
    assert not any(
        isinstance(node, ast.arguments) and (node.vararg or node.kwarg)
        for node in ast.walk(tree)
    )
    assert "type RithmicOrderClientFactory = Callable" in source


def test_feature_surface_tracks_the_rithmic_pyo3_owner() -> None:
    order_source = RUST_ORDER_SOURCE.read_text()
    module_source = RUST_MODULE_SOURCE.read_text()

    for name in (
        "RithmicOrderAck",
        "RithmicOrderClient",
        "RithmicOrderEvent",
    ):
        assert f'name = "{name}"' in order_source
    assert (
        "RithmicLedgerOrder"
        in (ROOT.parent / "rust-data-service/src/binding/rithmic_ledger.rs").read_text()
    )
    assert '#[cfg(feature = "rithmic")]' in module_source
    assert "PyOrderClient" in module_source


def test_adapter_keeps_feature_import_type_only_and_runtime_lazy() -> None:
    source = ADAPTER_SOURCE.read_text()
    tree = ast.parse(source)
    type_gate = next(
        node
        for node in tree.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "TYPE_CHECKING"
    )
    type_checking_imports = [
        node
        for parent in ast.walk(tree)
        if isinstance(parent, ast.If)
        and isinstance(parent.test, ast.Name)
        and parent.test.id == "TYPE_CHECKING"
        for node in ast.walk(ast.Module(body=parent.body, type_ignores=[]))
        if isinstance(node, ast.ImportFrom)
    ]
    assert [
        (node.module, tuple(alias.name for alias in node.names))
        for node in type_checking_imports
    ] == [
        (
            "src.core.adapters.rithmic_native_types",
            ("RithmicOrderClient", "RithmicOrderClientFactory"),
        )
    ]
    runtime_loader = next(
        node
        for node in type_gate.orelse
        if isinstance(node, ast.FunctionDef)
        and node.name == "_load_rithmic_order_client_factory"
    )
    runtime_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "fluxtrade_core"
    ]
    assert runtime_imports == [runtime_loader.body[0]]
    assert tuple(alias.name for alias in runtime_imports[0].names) == (
        "RithmicOrderClient",
    )

    native_module = ModuleType("fluxtrade_core")
    factory = Mock(return_value=SimpleNamespace())
    setattr(native_module, "RithmicOrderClient", factory)
    imports: list[str] = []
    real_import = builtins.__import__

    def tracked_import(name, *args, **kwargs):
        if name == "fluxtrade_core":
            imports.append(name)
            return native_module
        return real_import(name, *args, **kwargs)

    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
    )
    assert imports == []

    original_import = builtins.__import__
    builtins.__import__ = tracked_import
    try:
        adapter.start_order_event_stream()
    finally:
        builtins.__import__ = original_import

    assert imports == ["fluxtrade_core"]
    factory.assert_called_once_with("test", "ACCOUNT")


def test_supplied_factory_performs_zero_native_import(monkeypatch) -> None:
    client = SimpleNamespace()
    factory = Mock(return_value=client)
    real_import = builtins.__import__

    def reject_native_import(name, *args, **kwargs):
        if name == "fluxtrade_core":
            raise AssertionError("supplied factory must not import fluxtrade_core")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_native_import)
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=factory,
    )
    adapter.start_order_event_stream()

    assert adapter._client is client
    factory.assert_called_once_with("test", "ACCOUNT")
