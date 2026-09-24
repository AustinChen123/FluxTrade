import json
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import wire
from test_profile_wire import sample


def reject(body, request=None, reason="INVALID_PROFILE"):
    original = request if request is not None else sample()[0]
    raw = body if type(body) is bytes else json.dumps(body).encode()
    with pytest.raises(wire.ProfileWireError) as caught:
        wire.decode_live_profile_response(request or original, raw)
    assert caught.value.reason == reason
    assert str(caught.value) == "PROFILE_WIRE_" + reason
    assert caught.value.__cause__ is None and "SECRET" not in str(caught.value)


@pytest.mark.parametrize("days", [1, 7])
def test_all_object_keys_missing_unknown_and_duplicate(days):
    request, _, raw = sample(days)
    baseline = json.loads(raw)
    paths = [
        (),
        ("grid",),
        ("window",),
        ("coverage",),
        ("manifest", 0),
        ("bins", 0),
        ("poc",),
    ]
    for path in paths:
        for mutation in ("extra", "duplicate", "missing"):
            for key in list(baseline) if not path else list(_at(baseline, path)):
                body = json.loads(raw)
                target = _at(body, path)
                if mutation == "missing":
                    del target[key]
                    encoded = json.dumps(body).encode()
                elif mutation == "extra":
                    target["SECRET"] = 1
                    encoded = json.dumps(body).encode()
                else:
                    duplicate = json.dumps(target).replace(
                        json.dumps(key) + ":",
                        json.dumps(key) + ":null," + json.dumps(key) + ":",
                        1,
                    )
                    if path:
                        _at(body, path[:-1])[path[-1]] = "DUPLICATE_OBJECT"
                        duplicate = json.dumps(body).replace(
                            '"DUPLICATE_OBJECT"', duplicate
                        )
                    encoded = duplicate.encode()
                reject(encoded, request)


def _at(body, path):
    for part in path:
        body = body[part]
    return body


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff",
        b"{}",
        b"[]",
        b"null",
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":1.0}',
        b'{"x":',
        b"[" * 2000,
    ],
)
def test_invalid_json(raw):
    reject(raw)


def test_limits_before_parsing_or_bin_materialization(monkeypatch):
    request, _, raw = sample()
    parse = Mock(side_effect=AssertionError("parse forbidden"))
    with monkeypatch.context() as patch:
        patch.setattr(wire.json, "loads", parse)
        reject(b" " * (2 * 1024 * 1024 + 1), request, "QUERY_TOO_LARGE")
        parse.assert_not_called()
    body = json.loads(raw)
    body["bins"] = [None] * 5001
    create = Mock(side_effect=AssertionError("bin constructor forbidden"))
    with monkeypatch.context() as patch:
        patch.setattr(wire, "ProfileBin", create)
        reject(body, request, "QUERY_TOO_LARGE")
        create.assert_not_called()
    padded = raw + b" " * (2 * 1024 * 1024 - len(raw))
    assert wire.decode_live_profile_response(request, padded)


def test_manifest_limit_precedes_ref_materialization(monkeypatch):
    request, _, raw = sample(7)
    body = json.loads(raw)
    body["manifest"] = [None] * 91
    constructor = Mock(side_effect=AssertionError("ref constructor forbidden"))
    monkeypatch.setattr(wire, "DailyProfileRef", constructor)
    reject(body, request, "QUERY_TOO_LARGE")
    constructor.assert_not_called()


def test_decimal_length_admission_precedes_decimal_parse(monkeypatch):
    request, _, raw = sample()
    for text in ("1" * 65, "é" * 33):
        body = json.loads(raw)
        body["grid"]["origin"] = text
        parser = Mock(side_effect=AssertionError("Decimal must not be called"))
        with monkeypatch.context() as patch:
            patch.setattr(wire, "Decimal", parser)
            reject(body, request)
            parser.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        ("grid",),
        ("window",),
        ("coverage",),
        ("manifest",),
        ("manifest", 0),
        ("bins",),
        ("bins", 0),
        ("poc",),
    ],
)
@pytest.mark.parametrize("bad", [True, "SECRET", 1])
def test_nested_shapes(path, bad):
    _, _, raw = sample()
    body = json.loads(raw)
    _at(body, path[:-1])[path[-1]] = bad
    reject(body)
