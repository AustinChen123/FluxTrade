import ast
import json
from pathlib import Path

import pytest

from core.adapters.okx_public_candles import fetch_closed_1m


MINUTE = 60000
BASE = 1_700_000_000_000 // MINUTE * MINUTE


def row(ts: int, confirm: str = "1", opening: str = "1", high: str = "2", low: str = "0.5", close: str = "1.5") -> list[str]:
    return [str(ts), opening, high, low, close, "0", "0", "0", confirm]


def body(rows: list[list[str]]) -> bytes:
    return json.dumps({"code": "0", "msg": "", "data": rows}).encode()


def test_n3_url_paging_uses_oldest_timestamp() -> None:
    start, end = BASE, BASE + 600 * MINUTE
    calls: list[str] = []
    pages = [body([row(end - MINUTE - index * MINUTE) for index in range(300)]), body([row(start)])]

    def http_get(url: str) -> bytes:
        calls.append(url)
        return pages.pop(0)

    result = fetch_closed_1m("DOGE-USDT-SWAP", start, end, http_get)
    assert calls == [
        f"https://www.okx.com/api/v5/market/candles?instId=DOGE-USDT-SWAP&bar=1m&limit=300&after={end}",
        f"https://www.okx.com/api/v5/market/candles?instId=DOGE-USDT-SWAP&bar=1m&limit=300&after={end - 300 * MINUTE}",
    ]
    assert tuple(bar.ts_ms for bar in result.bars) == (start, *range(end - 300 * MINUTE, end, MINUTE))


def test_n1_three_pages_holes_and_unconfirmed_row() -> None:
    start, end = BASE, BASE + 600 * MINUTE
    all_ts = list(range(end + 49 * MINUTE, start - MINUTE, -MINUTE))
    holes = {start + 17 * MINUTE, start + 455 * MINUTE}
    page1_ts = [ts for ts in all_ts[:300] if ts not in holes]
    page2_ts = [ts for ts in all_ts[300:600] if ts not in holes]
    page3_ts = [ts for ts in all_ts[600:] if ts not in holes]
    page2 = [row(ts) for ts in page2_ts]
    page2[0] = row(int(page2[0][0]), confirm="0")
    page1 = [row(ts) for ts in page1_ts]
    page1.extend(row(page1_ts[-1]) for _ in range(300 - len(page1)))
    page2.extend(row(page2_ts[-1]) for _ in range(300 - len(page2)))
    page3 = [row(ts) for ts in page3_ts]
    page3.extend(row(page3_ts[-1]) for _ in range(50 - len(page3)))
    pages = [body(page1), body(page2), body(page3)]
    calls = 0

    def http_get(_: str) -> bytes:
        nonlocal calls
        calls += 1
        return pages.pop(0)

    result = fetch_closed_1m("DOGE-USDT-SWAP", start, end, http_get)
    expected_ts = sorted(ts for ts in set(page1_ts + page2_ts + page3_ts) if start <= ts < end and ts != int(page2[0][0]))
    assert tuple(bar.ts_ms for bar in result.bars) == tuple(expected_ts)
    assert result.missing == (1700001000000, 1700020920000, 1700027280000)
    assert calls == 3


@pytest.mark.parametrize("offset", [0, MINUTE])
def test_n2_range_boundaries(offset: int) -> None:
    start, end = BASE, BASE + 2 * MINUTE
    rows = [row(start), row(end), row(end - MINUTE)]
    result = fetch_closed_1m("X", start, end, lambda _: body(rows))
    assert tuple(bar.ts_ms for bar in result.bars) == (start, end - MINUTE)


def test_range_rejections_and_legal_twin() -> None:
    def valid() -> None:
        fetch_closed_1m("X", BASE, BASE + MINUTE, lambda _: body([row(BASE)]))

    valid()
    for start, end in ((BASE + 1, BASE + MINUTE), (BASE, BASE), (BASE + MINUTE, BASE)):
        with pytest.raises(ValueError):
            fetch_closed_1m("X", start, end, lambda _: body([]))


@pytest.mark.parametrize("bad,reason", [
    ({"code": "1", "data": []}, "response code"),
    ({"code": "0", "data": {}}, "data must"),
    ({"code": "0", "data": [["1"]]}, "nine strings"),
    ({"code": "0", "data": [row(BASE)[:-1] + [1]]}, "nine strings"),
    ({"code": "0", "data": [row(BASE + 1)]}, "timestamp"),
    ({"code": "0", "data": [["1.5", "1", "2", "0.5", "1.5", "0", "0", "0", "1"]]}, "timestamp"),
    ({"code": "0", "data": [row(BASE, opening="inf")]}, "finite"),
    ({"code": "0", "data": [row(BASE, opening="nan")]}, "finite"),
    ({"code": "0", "data": [row(BASE, opening="0")]}, "positive"),
    ({"code": "0", "data": [row(BASE, high="1")]}, "inconsistent"),
    ({"code": "0", "data": [row(BASE, low="1.6")]}, "inconsistent"),
    ({"code": "0", "data": [row(BASE, high="1")]}, "inconsistent"),
])
def test_n5_trust_boundary_rejections_have_legal_twins(bad: dict[str, object], reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        fetch_closed_1m("X", BASE, BASE + MINUTE, lambda _: json.dumps(bad).encode())
    valid = fetch_closed_1m("X", BASE, BASE + MINUTE, lambda _: body([row(BASE)]))
    assert valid.bars[0].ts_ms == BASE


@pytest.mark.parametrize("bad", [
    row(BASE, confirm="0", opening="nan"),
    row(BASE, confirm="0", high="0.5"),
])
def test_unconfirmed_rows_still_pass_trust_boundary(bad: list[str]) -> None:
    with pytest.raises(ValueError):
        fetch_closed_1m("X", BASE, BASE + MINUTE, lambda _: body([bad]))
    result = fetch_closed_1m("X", BASE, BASE + MINUTE, lambda _: body([row(BASE, confirm="0")]))
    assert result.bars == ()
    assert result.missing == (BASE,)


def test_n6_identical_duplicate_is_dropped_and_conflict_rejected() -> None:
    pages = [body([row(BASE + 300 * MINUTE - index * MINUTE) for index in range(300)]), body([row(BASE + MINUTE), row(BASE)])]
    result = fetch_closed_1m("X", BASE, BASE + 2 * MINUTE, lambda _: pages.pop(0))
    assert tuple(bar.ts_ms for bar in result.bars) == (BASE, BASE + MINUTE)
    pages = [body([row(BASE + 300 * MINUTE - index * MINUTE) for index in range(300)]), body([row(BASE + MINUTE, close="1.7")])]
    with pytest.raises(ValueError, match="conflicting duplicate"):
        fetch_closed_1m("X", BASE, BASE + 2 * MINUTE, lambda _: pages.pop(0))


def test_n4_page_cap_stops_repeated_old_pages() -> None:
    start, end = BASE, BASE + 600 * MINUTE
    old_rows = [row(end - (index + 1) * MINUTE) for index in range(300)]
    calls = 0

    def http_get(_: str) -> bytes:
        nonlocal calls
        calls += 1
        return body(old_rows)

    fetch_closed_1m("X", start, end, http_get)
    assert calls == 4


def _assert_module_import_isolation(source: str) -> None:
    tree = ast.parse(source)
    aliases: dict[str, str] = {}
    environment_names = {"environ", "getenv", "getenvb"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                aliases[bound] = alias.name
                assert alias.name != "ccxt" and not alias.name.startswith("core.adapters")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            assert module != "ccxt" and not module.startswith("core.adapters")
            for alias in node.names:
                assert not (module == "os" and alias.name in environment_names)
                aliases[alias.asname or alias.name] = f"{module}.{alias.name}"
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
            assert aliases.get(node.value.id) not in {f"os.{name}" for name in environment_names}
        if isinstance(node, ast.Attribute):
            root = node
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name):
                resolved = aliases.get(root.id, root.id)
                expression = ast.unparse(node)
                if resolved != root.id:
                    expression = expression.replace(root.id, resolved, 1)
                assert not any(expression == f"os.{name}" or expression.startswith(f"os.{name}.") for name in environment_names)


def test_module_import_isolation() -> None:
    source = Path(__file__).parents[1] / "src/core/adapters/okx_public_candles.py"
    _assert_module_import_isolation(source.read_text())


def test_module_import_isolation_rejects_environ_alias_subscript() -> None:
    source = "from os import environ as settings\ndef credential_probe():\n    return settings['OKX_API_KEY']\n"
    with pytest.raises(AssertionError):
        _assert_module_import_isolation(source)
