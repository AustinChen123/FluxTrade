from copy import deepcopy
from decimal import Decimal as D, localcontext
from hashlib import sha256
from struct import pack
from typing import Any

import pytest

from src.core.backtest.spider_policy_protocol import _poll_occurrence_plan_bytes as encode, _poll_occurrence_plan_digest as digest


def raw(*values):
    return b"".join(pack(">q", len(v.encode())) + v.encode() if isinstance(v, str) else pack(">q", v) for v in values)


def fixture(present=True) -> dict[str, Any]:
    account = dict(venue="V", environment="E", account="雪", subaccount="S")
    plan: dict[str, Any] = dict(account_key=account, poll_id="P", issued_at=-1, continuation_id="Q")
    for i, (name, kind, payload) in enumerate(zip(
        ("earn", "trading", "positions", "open_orders"), ("EARN", "TRADING", "POSITIONS", "OPEN_ORDERS"),
        ("EARN_SNAPSHOT", "TRADING_SNAPSHOT", "POSITION_SNAPSHOT", "OPEN_ORDER_SNAPSHOT"), strict=True)):
        plan[name] = dict(capture_sequence=i, snapshot_request=dict(schema_version="snapshot_request_v1",
            account_key=deepcopy(account), snapshot_id=name, snapshot_kind=kind, capture_mode="FROZEN_POLL_FIXTURE",
            fixture_key="fixture", captured_at=i, continuation_id="Q"), delivery_projection=dict(
            schema_version="delivery_projection_v1", reference=dict(namespace="SNAPSHOT", fact_id=name),
            payload_kind=payload, occurrence_index=i, schedule_sequence=i+4, visible_at=i+1, continuation_id="Q"))
    return plan if present else {k: v for k, v in plan.items() if k != "earn"}


def oracle(present):
    # Independent literal reconstruction; no production segment encoders or fixture traversal.
    account = raw("V", "E", "雪") + b"\x01" + raw("S")
    def stage(i, name, kind, payload):
        return (raw(i, "snapshot_request_v1") + account + raw(name, kind, "FROZEN_POLL_FIXTURE")
                + b"\x01" + raw("fixture", i) + b"\x01" + raw("Q", "delivery_projection_v1", "SNAPSHOT", name,
                    payload, i, i+4, i+1) + b"\x01" + raw("Q") + b"\x00")
    return (raw("SPIDER_POLL_OCCURRENCE_PLAN_V1") + account + raw("P", -1, "Q")
            + (b"\x01" + stage(0, "earn", "EARN", "EARN_SNAPSHOT") if present else b"\x00")
            + stage(1, "trading", "TRADING", "TRADING_SNAPSHOT") + stage(2, "positions", "POSITIONS", "POSITION_SNAPSHOT")
            + stage(3, "open_orders", "OPEN_ORDERS", "OPEN_ORDER_SNAPSHOT"))


@pytest.mark.parametrize("present", [False, True])
def test_literal_vectors_and_representation_only(present):
    plan = fixture(present)
    before = deepcopy(plan)
    assert encode(plan) == oracle(present) and digest(plan) == sha256(oracle(present)).hexdigest()
    assert digest(plan) != sha256(oracle(present)[len(raw("SPIDER_POLL_OCCURRENCE_PLAN_V1")):]).hexdigest()
    assert plan == before
    assert present or encode(dict(plan, earn=None)) == encode(plan)


def test_fields_options_and_stage_order():
    plan = fixture()
    baseline = digest(plan)
    paths = [(key,) for key in ("poll_id", "issued_at", "continuation_id")]
    paths += [("account_key", key) for key in ("venue", "environment", "account", "subaccount")]
    for stage in ("earn", "trading", "positions", "open_orders"):
        paths += [(stage, "capture_sequence"), (stage, "snapshot_request", "snapshot_id"), (stage, "delivery_projection", "visible_at")]
    for path in paths:
        changed = deepcopy(plan)
        target = changed
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] += 1 if type(target[path[-1]]) is int else "x"
        assert digest(changed) != baseline
    for left, right in [("earn", "trading"), ("trading", "positions"), ("positions", "open_orders")]:
        changed = deepcopy(plan)
        changed[left], changed[right] = changed[right], changed[left]
        assert digest(changed) != baseline
    for field in ("fixture_key", "continuation_id"):
        changed = deepcopy(plan)
        del changed["trading"]["snapshot_request"][field]
        assert digest(changed) != baseline
    plan["trading"]["delivery_projection"]["transport"] = dict(route="REST", operation="ORDER", client_order_id="C", code="0", limit_price=D("123.450"))
    expected = encode(plan)
    with localcontext() as context:
        context.prec = 2
        assert encode(plan) == expected and raw("123.45") in expected


@pytest.mark.parametrize("key,value", [("extra", 1), ("poll_id", ""), ("poll_id", "\ud800"), ("issued_at", True), ("issued_at", 2**63), ("trading", {}), ("positions", None), ("trading", dict(fixture()["trading"], capture_sequence=-1))]
    + [(key, ...) for key in ("account_key", "poll_id", "issued_at", "continuation_id", "trading", "positions", "open_orders")]
    + [(stage, {k: v for k, v in fixture()[stage].items() if k != field}) for stage in ("earn", "trading", "positions", "open_orders") for field in ("capture_sequence", "snapshot_request", "delivery_projection")])
def test_malformed_representation(key, value):
    plan = fixture()
    if value is ...:
        del plan[key]
    else:
        plan[key] = value
    before = deepcopy(plan)
    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
        encode(plan)
    assert plan == before
