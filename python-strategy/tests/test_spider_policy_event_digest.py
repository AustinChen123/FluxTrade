from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
from struct import pack

import pytest

from src.core.backtest.synthetic_scenario_replay import _policy_event_bytes, _policy_event_digest


def text(value):
    raw = value.encode("utf-8")
    return pack(">q", len(raw)) + raw


def prefix(kind):
    return text("SPIDER_POLICY_EVENT_V1") + pack(">q", 100000) + text(kind)


SEND = dict(instId="P_A", instIdCode=1, tdMode="cross", clOrdId="0000014998", tag="tag",
            side="buy", ordType="limit", px="9", sz="1")
CANCEL = dict(ordId="O", instId="P_A", instIdCode=1)
UNSUPPORTED = dict(instId="P_A", side="sell", px="", clOrdId="manualxxxx", state="filled",
                   sz="2", accFillSz="2", cTime=None, sMsg=None)
SEND_BYTES = text("P_A") + pack(">q", 1) + b"".join(map(text, ["cross", "0000014998", "tag", "buy", "limit"])) + b"\x01" + text("9") + text("1")
CANCEL_BYTES = text("O") + text("P_A") + pack(">q", 1)


def batch(kind="send", operation="send", orders=None):
    return dict(at_ms=100000, kind=kind, operation=operation, route="REST",
                orders=deepcopy([SEND if operation == "send" else CANCEL] if orders is None else orders))


def unsupported(order=None):
    return dict(at_ms=100000, kind="unsupported_source_path", reason="non_grid_fill_enters_orderFilled",
                order=deepcopy(UNSUPPORTED if order is None else order))


def check(event, expected):
    before = deepcopy(event)
    assert _policy_event_bytes(event) == expected
    assert _policy_event_digest(event) == sha256(expected).hexdigest()
    assert event == before


@pytest.mark.parametrize("kind", ["request_market", "request_earn", "check_websocket"])
def test_no_payload_independent_vectors(kind):
    check(dict(at_ms=100000, kind=kind), prefix(kind))


@pytest.mark.parametrize("reason,extra,suffix", [
    ("offline", {"operation": "cancel"}, text("cancel")),
    ("reset_step", {"name": "雪"}, text("雪")),
    ("high_low_limit", {"name": "P_A"}, text("P_A")),
    ("individual_limit", {"name": "P_A"}, text("P_A")),
    ("total_limit", {}, b""), ("daily_stop", {}, b""),
])
def test_alert_independent_vectors(reason, extra, suffix):
    check(dict(at_ms=100000, kind="alert", reason=reason, **extra), prefix("alert") + text(reason) + suffix)


@pytest.mark.parametrize("kind,operation,row", [
    ("send", "send", SEND_BYTES), ("cancel", "cancel", CANCEL_BYTES),
    ("console_only", "send", SEND_BYTES), ("console_only", "cancel", CANCEL_BYTES),
])
def test_batch_independent_vectors(kind, operation, row):
    check(batch(kind, operation), prefix(kind) + text(operation) + text("REST") + pack(">q", 1) + row)


def test_optional_and_list_vectors():
    market = SEND | {"ordType": "market", "px": ""}
    raw_market = text("P_A") + pack(">q", 1) + b"".join(map(text, ["cross", "0000014998", "tag", "buy", "market"])) + b"\x00" + text("1")
    check(batch(orders=[SEND, market]), prefix("send") + text("send") + text("REST") + pack(">q", 2) + SEND_BYTES + raw_market)
    base = prefix("unsupported_source_path") + text("non_grid_fill_enters_orderFilled") + text("P_A") + text("sell")
    tail = b"".join(map(text, ["manualxxxx", "filled", "2", "2"]))
    check(unsupported(), base + b"\x00" + tail + b"\x00\x00")
    present = UNSUPPORTED | {"px": "-0.25", "cTime": "501", "sMsg": "雪"}
    check(unsupported(present), base + b"\x01" + text("-0.25") + tail + b"\x01" + text("501") + b"\x01" + text("雪"))
    absent = {k: v for k, v in UNSUPPORTED.items() if k not in ("cTime", "sMsg")}
    check(unsupported(absent), base + b"\x00" + tail + b"\x00\x00")
    assert _policy_event_digest(batch(orders=[SEND, market])) != _policy_event_digest(batch(orders=[market, SEND]))
    assert _policy_event_digest(batch(orders=[])) != _policy_event_digest(batch())


@pytest.mark.parametrize("original,changes", [
    (SEND, dict(instId="P_B", instIdCode=2, tdMode="isolated", clOrdId="other", tag="other", side="sell", px="10", sz="2")),
    (CANCEL, dict(ordId="other", instId="P_B", instIdCode=2)),
    (UNSUPPORTED, dict(instId="P_B", side="buy", px="1", clOrdId="other", state="failed", sz="3", accFillSz="1", cTime="502", sMsg="failure")),
])
def test_every_nested_field_sensitivity(original, changes):
    make = unsupported if original == UNSUPPORTED else lambda row: batch("cancel", "cancel", [row]) if original == CANCEL else batch(orders=[row])
    baseline = _policy_event_digest(make(original))
    for key, value in changes.items():
        assert _policy_event_digest(make(original | {key: value})) != baseline, key


def test_envelope_sensitivity_and_variant_collision_controls():
    event = batch()
    baseline = _policy_event_digest(event)
    for field, value in [("at_ms", 100001), ("route", "WS"), ("kind", "console_only")]:
        assert _policy_event_digest(event | {field: value}) != baseline
    events = [dict(at_ms=100000, kind=k) for k in ["request_market", "request_earn", "check_websocket"]]
    events += [dict(at_ms=100000, kind="alert", reason=r) for r in ["daily_stop", "total_limit"]]
    assert len({_policy_event_digest(e) for e in events}) == len(events)
    for field, a, b in [("operation", "send", "cancel"), ("name", "P_A", "P_B")]:
        event = dict(at_ms=100000, kind="alert", reason="offline" if field == "operation" else "reset_step")
        assert _policy_event_digest(event | {field: a}) != _policy_event_digest(event | {field: b})


@pytest.mark.parametrize("event", [
    None, [], {}, {"at_ms": 1, "kind": "unknown"}, batch() | {"operation": "cancel"},
    batch() | {"operation": "unknown"}, batch() | {"route": "unknown"}, batch() | {"orders": {}},
    dict(at_ms=1, kind="alert", reason="unknown"), dict(at_ms=1, kind="alert", reason="offline", operation="unknown"),
    unsupported() | {"reason": "unknown"}, unsupported() | {"order": []},
    batch(orders=[SEND | {"ordType": "market"}]), batch(orders=[SEND | {"px": ""}]),
    batch(orders=[SEND | {"ordType": "unknown"}]), batch(orders=[SEND | {"side": "LONG"}]),
])
def test_unknown_and_mismatched_shapes(event):
    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
        _policy_event_digest(event)


def test_every_layer_missing_extra_and_wrong_primitives():
    events = [batch(), batch("cancel", "cancel"), unsupported(), dict(at_ms=1, kind="request_earn")]
    events += [dict(at_ms=1, kind="alert", reason="offline", operation="send"),
               dict(at_ms=1, kind="alert", reason="reset_step", name="P_A"), dict(at_ms=1, kind="alert", reason="daily_stop")]
    for event in events:
        paths = [()]
        if "orders" in event:
            paths += [("orders", 0)]
        if "order" in event:
            paths += [("order",)]
        for path in paths:
            row = event
            for key in path:
                row = row[key]
            for field in [*row, "extra"]:
                for wrong in [None, True, [], {}, 1.0]:
                    if field in ("cTime", "sMsg") and wrong is None:
                        continue
                    if field == "orders" and wrong == []:
                        continue
                    changed = deepcopy(event)
                    target = changed
                    for key in path:
                        target = target[key]
                    target[field] = wrong
                    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
                        _policy_event_digest(changed)
                if field in row and field not in ("cTime", "sMsg"):
                    changed = deepcopy(event)
                    target = changed
                    for key in path:
                        target = target[key]
                    del target[field]
                    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
                        _policy_event_digest(changed)


@pytest.mark.parametrize("value", ["-0", "01", "+1", "1.0", "1e2", " 1", "1_0", "NaN", "Infinity", "", 1, True, Decimal("1")])
def test_noncanonical_financial_text(value):
    for field in ("px", "sz"):
        with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
            _policy_event_digest(batch(orders=[SEND | {field: value}]))
    for field in ("px", "sz", "accFillSz"):
        if field == "px" and value == "":
            continue
        with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
            _policy_event_digest(unsupported(UNSUPPORTED | {field: value}))


def test_integer_boundaries_and_unicode_rejection():
    for value in (-(1 << 63), (1 << 63) - 1):
        check(dict(at_ms=value, kind="request_earn"), text("SPIDER_POLICY_EVENT_V1") + pack(">q", value) + text("request_earn"))
    for value in (True, "1", 1.0, 1 << 63, -(1 << 63) - 1):
        for event in [batch() | {"at_ms": value}, batch(orders=[SEND | {"instIdCode": value}])]:
            with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
                _policy_event_digest(event)
    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
        _policy_event_digest(batch(orders=[SEND | {"instId": "\ud800"}]))
    for value in ("", " P_A", "P_A "):
        for event in [batch(orders=[SEND | {"clOrdId": value}]),
                      batch("cancel", "cancel", [CANCEL | {"ordId": value}]),
                      unsupported(UNSUPPORTED | {"instId": value})]:
            with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
                _policy_event_digest(event)
