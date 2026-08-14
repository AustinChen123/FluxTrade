from __future__ import annotations

import ast
import inspect
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from src.core.data_sources import yahoo
from src.core.data_sources.yahoo import _YFinanceModule, _load_yfinance


_OWNER_PATH = Path("src/core/data_sources/yahoo.py")


class _FakeTicker:
    @property
    def info(self) -> dict[str, object]:
        return {"regularMarketPrice": 1}


class _FakeYFinance:
    def download(
        self,
        tickers: str,
        *,
        start: object,
        end: object,
        interval: str,
        auto_adjust: bool,
        progress: bool,
    ) -> pd.DataFrame | None:
        return pd.DataFrame()

    def Ticker(self, ticker: str) -> _FakeTicker:
        return _FakeTicker()


def test_yfinance_type_surface_is_private_narrow_and_lazy() -> None:
    source = _OWNER_PATH.read_text()
    tree = ast.parse(source)

    provider_imports = [
        node
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Import)
            and any(alias.name == "yfinance" for alias in node.names)
        )
        or (isinstance(node, ast.ImportFrom) and node.module == "yfinance")
    ]
    assert provider_imports == []

    protocol_names = {
        node.name
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name.startswith("_YFinance")
    }
    assert protocol_names == {"_YFinanceTicker", "_YFinanceModule"}

    assert list(inspect.signature(_YFinanceModule.download).parameters) == [
        "self",
        "tickers",
        "start",
        "end",
        "interval",
        "auto_adjust",
        "progress",
    ]
    assert list(inspect.signature(_YFinanceModule.Ticker).parameters) == [
        "self",
        "ticker",
    ]

    for forbidden in ("typing.Any", "cast(", "# type: ignore", "# pyright:"):
        assert forbidden not in source


def test_loader_imports_once_and_returns_the_narrowed_provider(monkeypatch) -> None:
    provider = _FakeYFinance()
    importer = MagicMock(return_value=provider)
    monkeypatch.setattr(yahoo, "import_module", importer)

    assert _load_yfinance() is provider
    importer.assert_called_once_with("yfinance")
