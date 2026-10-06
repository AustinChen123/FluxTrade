import ast
from pathlib import Path

import src.core.adapters.rithmic_order_observation as observation
import src.core.adapters.rithmic_order_status as status
import src.core.adapters.rithmic_recovery as recovery


def test_status_symbols_are_reexported_from_original_modules():
    assert observation._normalize_snapshot_status is status._normalize_snapshot_status
    assert recovery._normalize_status is status._normalize_status
    assert recovery.rithmic_order_may_be_working is status.rithmic_order_may_be_working


def test_status_module_imports_only_stdlib_and_exchange_error():
    tree = ast.parse(Path(status.__file__).read_text(encoding="utf-8"))
    imports = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imports.setdefault(node.module, set()).update(
                alias.name for alias in node.names
            )
        elif isinstance(node, ast.Import):
            imports.setdefault("direct_import", set()).update(
                alias.name for alias in node.names
            )

    assert imports == {
        "dataclasses": {"dataclass"},
        "decimal": {"Decimal", "InvalidOperation"},
        "src.core.interfaces.exchange": {"ExchangeError"},
    }
