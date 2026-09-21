"""Hostile native responses; mocks do not replace the real-binding integration gate."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import composite as owner
from test_profile_composite import reading, result


def fixed(
    caught: pytest.ExceptionInfo[owner.ProfileCompositionError], reason: str
) -> None:
    assert type(caught.value) is owner.ProfileCompositionError
    assert caught.value.reason == reason
    assert str(caught.value) == "PROFILE_COMPOSITION_" + reason
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize(
    "code,reason",
    [
        ("RESOURCE_LIMIT", "TOO_LARGE"),
        ("ARITHMETIC", "ARITHMETIC"),
        *[
            (name, "INTEGRITY")
            for name in (
                "INVALID_INPUT",
                "INVALID_DECIMAL",
                "INVALID_BIN",
                "INVALID_PRODUCT",
                "INVALID_GRID",
                "INVALID_WINDOW",
                "INVALID_COMPOSITION",
                "INVALID_TRADE_SCOPE",
                "INVALID_TRADE_VALUE",
            )
        ],
    ],
)
def test_exact_native_codes(
    monkeypatch: pytest.MonkeyPatch, code: str, reason: str
) -> None:
    native = Mock(side_effect=ValueError("PROFILE_MERGE_" + code))
    monkeypatch.setattr(owner, "_load_native", lambda: (SimpleNamespace, native))
    with pytest.raises(owner.ProfileCompositionError) as caught:
        owner.compose_profile(reading())
    fixed(caught, reason)
    native.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        ValueError("SECRET"),
        RuntimeError("SECRET"),
        ValueError("PROFILE_MERGE_ARITHMETIC SECRET"),
    ],
)
def test_unknown_call_failure(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    native = Mock(side_effect=error)
    monkeypatch.setattr(owner, "_load_native", lambda: (SimpleNamespace, native))
    with pytest.raises(owner.ProfileCompositionError) as caught:
        owner.compose_profile(reading())
    fixed(caught, "NATIVE_FAILURE")
    native.assert_called_once()


def test_base_exception_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    error = KeyboardInterrupt("SECRET")
    native = Mock(side_effect=error)
    monkeypatch.setattr(owner, "_load_native", lambda: (SimpleNamespace, native))
    with pytest.raises(KeyboardInterrupt) as caught:
        owner.compose_profile(reading())
    assert caught.value is error
    native.assert_called_once()


@pytest.mark.parametrize(
    "module",
    [
        None,
        SimpleNamespace(VolumeProfileMergeResult=SimpleNamespace),
        SimpleNamespace(merge_volume_profiles=Mock()),
    ],
)
def test_unavailable(monkeypatch: pytest.MonkeyPatch, module: object) -> None:
    importer = Mock(return_value=module)
    if module is None:
        importer.side_effect = ModuleNotFoundError("SECRET", name="fluxtrade_core")
    monkeypatch.setattr(owner, "import_module", importer)
    with pytest.raises(owner.ProfileCompositionError) as caught:
        owner.compose_profile(reading())
    fixed(caught, "NATIVE_UNAVAILABLE")
    importer.assert_called_once_with("fluxtrade_core")


@pytest.mark.parametrize(
    "error",
    [
        ModuleNotFoundError("SECRET", name="dependency"),
        RuntimeError("SECRET"),
        ValueError("SECRET"),
    ],
)
def test_initialization_error_identity(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    monkeypatch.setattr(owner, "import_module", Mock(side_effect=error))
    with pytest.raises(type(error)) as caught:
        owner.compose_profile(reading())
    assert caught.value is error


def reject(
    monkeypatch: pytest.MonkeyPatch, raw: object, *, empty: bool = False
) -> None:
    native = Mock(return_value=raw)
    monkeypatch.setattr(owner, "_load_native", lambda: (SimpleNamespace, native))
    with pytest.raises(owner.ProfileCompositionError) as caught:
        owner.compose_profile(reading(empty=empty))
    fixed(caught, "INTEGRITY")
    native.assert_called_once()


class Text(str):
    pass


class Integer(int):
    pass


@pytest.mark.parametrize(
    "raw", [object(), type("Derived", (SimpleNamespace,), {})(**vars(result(False)))]
)
def test_result_exact_class(monkeypatch: pytest.MonkeyPatch, raw: object) -> None:
    reject(monkeypatch, raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("product_id", "SECRET"),
        ("product_id", Text("BINANCE:BTCUSDT-SPOT")),
        ("window_start_ms", 1),
        ("window_start_ms", False),
        ("window_end_ms", 1),
        ("window_end_ms", Integer(172800000)),
        ("unit", "USD"),
        ("unit", Text("USDT")),
        ("bin_origin", "1"),
        ("bin_step", "20"),
        ("aggregate_count", True),
        ("aggregate_count", Integer(1)),
        ("aggregate_count", -1),
        ("aggregate_count", 1 << 63),
        ("aggregate_count", 0),
        ("base_volume", "0"),
        ("quote_volume", "-1"),
        ("poc_index", True),
        ("poc_index", 2),
        ("poc_index", 1 << 63),
        ("poc_low", "10"),
        ("poc_high_exclusive", "-1"),
    ],
)
def test_identity_and_summary(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    raw = result(False)
    setattr(raw, field, value)
    reject(monkeypatch, raw)


@pytest.mark.parametrize(
    "rows",
    [
        (),
        type("ListSubclass", (list,), {})([(0, "1", "1", 1)]),
        [(index, "1", "1", 1) for index in range(5)],
        [[0, "1", "1", 1]],
        [(0, "1", "1")],
        [(0, "1", "1", 1, 0)],
        [type("TupleSubclass", (tuple,), {})((0, "1", "1", 1))],
        [(0, "1", "1", 1), (0, "1", "1", 1)],
        [(1, "1", "1", 1), (0, "1", "1", 1)],
        *[
            [(index, "1", "1", 1)]
            for index in (True, Integer(0), -(1 << 63) - 1, 1 << 63)
        ],
        *[[(0, "1", "1", count)] for count in (True, Integer(1), 0, -1, 1 << 63)],
        [(0, "0", "1", 1)],
        [(0, "1", "-1", 1)],
    ],
)
def test_bin_shape_and_domains(monkeypatch: pytest.MonkeyPatch, rows: object) -> None:
    raw = result(False)
    raw.bins = rows
    reject(monkeypatch, raw)


@pytest.mark.parametrize(
    "field",
    [
        "bin_origin",
        "bin_step",
        "base_volume",
        "quote_volume",
        "poc_low",
        "poc_high_exclusive",
        "bin_base",
        "bin_quote",
    ],
)
@pytest.mark.parametrize(
    "text",
    [
        Text("1"),
        1,
        "NaN",
        "Infinity",
        "1e0",
        "1.0",
        "-0",
        " 1",
        "1e-29",
        "0.00000000000000000000000000001",
        "79228162514264337593543950336",
        "1" * 65,
        "é" * 40,
        "\ud800",
    ],
)
def test_every_decimal_wire_boundary(
    monkeypatch: pytest.MonkeyPatch, field: str, text: object
) -> None:
    raw = result(False)
    if field in ("bin_base", "bin_quote"):
        row = list(raw.bins[0])
        row[1 if field == "bin_base" else 2] = text
        raw.bins = [tuple(row)]
    else:
        setattr(raw, field, text)
    reject(monkeypatch, raw)


@pytest.mark.parametrize("mask", range(1, 8))
def test_nonempty_poc_none_matrix(monkeypatch: pytest.MonkeyPatch, mask: int) -> None:
    raw = result(False)
    for bit, field in enumerate(("poc_index", "poc_low", "poc_high_exclusive")):
        if mask & (1 << bit):
            setattr(raw, field, None)
    reject(monkeypatch, raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("base_volume", "1"),
        ("quote_volume", "1"),
        ("aggregate_count", 1),
        ("poc_index", 0),
        ("bins", [(0, "1", "1", 1)]),
    ],
)
def test_empty_consistency(
    monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    raw = result(True)
    setattr(raw, field, value)
    reject(monkeypatch, raw, empty=True)


@pytest.mark.parametrize(
    "text,parsed", [("1" * 65, False), ("é" * 40, False), ("1" * 64, True)]
)
def test_decimal_length_precedes_parse(
    monkeypatch: pytest.MonkeyPatch, text: str, parsed: bool
) -> None:
    parser = Mock(side_effect=ValueError("parse reached"))
    monkeypatch.setattr(owner, "Decimal", parser)
    raw = result(False)
    raw.bin_origin = text
    reject(monkeypatch, raw)
    if parsed:
        parser.assert_called_once_with(text)
    else:
        parser.assert_not_called()
