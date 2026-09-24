import subprocess
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from src.core.market_data.profiles.publication import (
    MAX_JSON_BYTES,
    MAX_JSON_DEPTH,
    MAX_JSON_NODES,
    CanonicalJsonObject,
    VerifiedProfilePublication,
)
from test_volume_profile_types import profile


def publication() -> VerifiedProfilePublication:
    metadata = CanonicalJsonObject({})
    return VerifiedProfilePublication(profile(), metadata, metadata, None, "OBSERVED", "PRESENT")


@pytest.mark.parametrize(
    "value", [{"\x00": 1}, {"a": "\x00"}, {"a": [{"\x00": 1}]}, {"a": [{"b": "\x00"}]}]
)
def test_jsonb_nul_is_rejected_at_construction(value: dict[str, Any]) -> None:
    with pytest.raises(
        ValueError, match="^JSON text contains forbidden NUL$"
    ) as caught:
        CanonicalJsonObject(value)
    assert type(caught.value) is ValueError


def test_supplementary_unicode_remains_valid() -> None:
    value: dict[str, object] = {"\U0001f680": [{"value": "\U0001f600"}]}
    assert CanonicalJsonObject(value).thaw() == value


def test_canonical_json_detaches_aliases_and_thaws_fresh_objects() -> None:
    source: dict[str, Any] = {"z": [True, {"é": 2}], "a": None}
    value = CanonicalJsonObject(source)
    assert value.text == '{"a":null,"z":[true,{"é":2}]}'
    source["z"].append("mutation")
    assert value.thaw() == {"a": None, "z": [True, {"é": 2}]}
    first = value.thaw()
    first["a"] = "changed"
    assert value.thaw()["a"] is None
    assert value.thaw() is not first
    with pytest.raises(FrozenInstanceError):
        setattr(value, "text", "changed")
    assert not hasattr(value, "__dict__")
    assert (
        CanonicalJsonObject({"z": 1, "a": 2}).text
        == CanonicalJsonObject({"a": 2, "z": 1}).text
    )


@pytest.mark.parametrize(
    "bad",
    [
        Decimal("1"),
        object(),
        (),
        MappingProxyType({}),
        type("IntChild", (int,), {})(1),
        type("StrChild", (str,), {})("s"),
        type("ListChild", (list,), {})([]),
        type("DictChild", (dict,), {})({}),
    ],
)
def test_json_rejects_non_exact_supported_types(bad: Any) -> None:
    with pytest.raises(ValueError):
        CanonicalJsonObject({"value": bad})


def test_invalid_roots_keys_cycles_and_float() -> None:
    for bad in [[], MappingProxyType({}), {1: "bad"}, {type("S", (str,), {})("a"): 1}]:
        with pytest.raises(ValueError):
            CanonicalJsonObject(bad)  # type: ignore[arg-type]
    # JSON float rejection is a type-boundary test, not financial arithmetic.
    with pytest.raises(ValueError):
        CanonicalJsonObject({"value": float("nan")})
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError, match="cyclic"):
        CanonicalJsonObject(cyclic)
    loop: list[Any] = []
    loop.append(loop)
    with pytest.raises(ValueError, match="cyclic"):
        CanonicalJsonObject({"loop": loop})


def test_exact_byte_depth_and_node_limits() -> None:
    assert (
        len(CanonicalJsonObject({"a": "é" * ((MAX_JSON_BYTES - 8) // 2)}).text.encode())
        == MAX_JSON_BYTES
    )
    with pytest.raises(ValueError):
        CanonicalJsonObject({"a": "x" * (MAX_JSON_BYTES - 7)})
    child: Any = 0
    for _ in range(MAX_JSON_DEPTH - 1):
        child = [child]
    CanonicalJsonObject({"a": child})
    with pytest.raises(ValueError, match="depth"):
        CanonicalJsonObject({"a": [child]})
    CanonicalJsonObject({"a": [None] * (MAX_JSON_NODES - 3)})
    with pytest.raises(ValueError, match="node"):
        CanonicalJsonObject({"a": [None] * (MAX_JSON_NODES - 2)})
    with pytest.raises(ValueError):
        CanonicalJsonObject({"a": 1 << (MAX_JSON_BYTES * 4)})


def test_publication_metadata_utc_and_content_identity() -> None:
    value = publication()
    assert value.quality == "VERIFIED"
    assert value.content_sha256 == value.content.content_sha256
    for basis in ("OBSERVED", "MODELED"):
        for retention in ("PRESENT", "DELETED", "NOT_STORED"):
            changed = replace(
                value,
                source_manifest=CanonicalJsonObject({"source": "fixture"}),
                reconciliation=CanonicalJsonObject({"count": 1}),
                availability_basis=basis,
                raw_retention_state=retention,
                source_available_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
            )
            assert changed.content_sha256 == value.content_sha256
    invalids: dict[str, list[Any]] = {
        "content": [None, object()],
        "source_manifest": [{}, None],
        "reconciliation": [{}, None],
        "source_available_at": [
            datetime(2020, 1, 1),
            "2020-01-01",
            datetime(2020, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            type("D", (datetime,), {})(2020, 1, 1, tzinfo=timezone.utc),
        ],
        "availability_basis": ["observed", "", type("S", (str,), {})("OBSERVED")],
        "raw_retention_state": ["UNKNOWN", "", type("S", (str,), {})("PRESENT")],
    }
    for field, values in invalids.items():
        for invalid in values:
            with pytest.raises(ValueError):
                replace(value, **{field: invalid})
    for field in ("id", "revision", "computed_at", "published_at", "quality"):
        with pytest.raises(TypeError):
            replace(value, **{field: "forbidden"})
    with pytest.raises(FrozenInstanceError):
        setattr(value, "availability_basis", "MODELED")
    assert not hasattr(value, "__dict__")


def test_publication_import_is_persistence_free() -> None:
    script = "import sys; import src.core.market_data.profiles.publication; assert 'src.core.orm_models' not in sys.modules; assert not any(n == 'sqlalchemy' or n.startswith('sqlalchemy.') for n in sys.modules)"
    script += "; from src.core.market_data.profiles.publication import CanonicalJsonObject as C; v=C({'n':2**64-1}); sys.set_int_max_str_digits(640); assert v.thaw()['n']==2**64-1; assert C({'n':2**64-1}).text==v.text"
    subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        timeout=10,
    )


def test_integer_and_unicode_construction_boundaries() -> None:
    for value in [-(1 << 63), (1 << 64) - 1]:
        assert CanonicalJsonObject({"n": value}).thaw() == {"n": value}
    for value in [-(1 << 63) - 1, 1 << 64, 10**700, 10**4299]:
        with pytest.raises(ValueError, match="integer outside"):
            CanonicalJsonObject({"n": value})
    objects: list[dict[str, object]] = [{"key": "\ud800"}, {"\udfff": "value"}]
    for obj in objects:
        with pytest.raises(ValueError, match="^invalid UTF-8 JSON text$") as error:
            CanonicalJsonObject(obj)
        assert type(error.value) is ValueError


def test_mutable_timezone_is_detached_and_callback_error_normalized() -> None:
    class MutableZone(tzinfo):
        broken = False
        def utcoffset(self, dt):
            if self.broken:
                raise RuntimeError("private callback error")
            return timedelta(0)
    zone = MutableZone()
    stamp = datetime(2020, 1, 1, tzinfo=zone)
    value = replace(publication(), source_available_at=stamp)
    zone.broken = True
    assert type(value.source_available_at) is datetime
    assert value.source_available_at.tzinfo is timezone.utc
    assert value.source_available_at.utcoffset() == timedelta(0)
    with pytest.raises(ValueError, match="^invalid UTC availability timestamp$"):
        replace(publication(), source_available_at=stamp)
