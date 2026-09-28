"""Test-only literal inputs and independent acceptance evidence; no accounting."""
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json
from struct import pack
from typing import Any

from src.core.backtest import synthetic_scenario_codec as wire

ACCOUNT: wire.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}


def canonical(value):
    def normalize(v):
        if isinstance(v, Decimal):
            if not v.is_finite():
                raise ValueError("nonfinite evidence")
            text = format(v, "f")
            text = text.rstrip("0").rstrip(".") if "." in text else text
            return {"decimal": "0" if v.is_zero() else text}
        if isinstance(v, bytes):
            return {"bytes": v.hex()}
        if v is None or type(v) in (str, int, bool):
            return v
        if isinstance(v, (tuple, list)):
            return [normalize(x) for x in v]
        if isinstance(v, dict):
            if all(isinstance(k, str) for k in v):
                return {k: normalize(x) for k, x in v.items()}
            if all(isinstance(k, tuple) and len(k) == 2 and type(k[0]) is int and isinstance(k[1], str) for k in v):
                return [[normalize(k), normalize(v[k])] for k in sorted(v)]
        raise TypeError("unsupported evidence")
    return json.dumps(normalize(value), sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def delivery_id(fact, kind, namespace="SOURCE", occurrence=0, account=ACCOUNT):
    def text(value):
        raw = value.encode("utf-8")
        return pack(">q", len(raw)) + raw
    raw = text("SCENARIO_DELIVERY_V1")
    raw += b"".join(text(account[k]) for k in ("venue", "environment", "account"))
    sub = account.get("subaccount")
    raw += b"\x00" if sub is None else b"\x01" + text(sub)
    raw += text(namespace) + text(fact) + text(kind) + pack(">q", occurrence)
    return sha256(raw).hexdigest()


def group(name, at, kind, payload, ordinal):
    return dict(schema_version="scenario_group_v1", group_id=name, account_key=deepcopy(ACCOUNT),
                ordering_contract_id="S_order_v1", group_effective_at=at, declared_member_count=1,
                members=[dict(kind=kind, payload=payload, stamp=dict(event_id=name, effective_at=at,
                    ordering_contract_id="S_order_v1", scenario_ordinal=ordinal, causal_parent_ids=[]))])


def snapshot(name, kind, at):
    return dict(schema_version="snapshot_request_v1", account_key=deepcopy(ACCOUNT), snapshot_id=name,
                snapshot_kind=kind, capture_mode="OWNER_CURRENT", captured_at=at)


def projection(fact, kind, visible, sequence, transport=None):
    result = dict(schema_version="delivery_projection_v1", reference=dict(namespace="SOURCE", fact_id=fact),
                  payload_kind=kind, occurrence_index=0, schedule_sequence=sequence, visible_at=visible)
    if transport is not None:
        result["transport"] = transport
    return result


def empty_plan(fact, kind) -> dict[str, Any]:
    return dict(delivery_id=delivery_id(fact, kind), expected_policy_events=[], financial_items=[], market_requests=[])


def record_native(monkeypatch, log):
    # Call-through only: retain every actual native result, never patch its value.
    for name in ("apply_group", "capture_snapshot", "build_delivery", "inspect_state"):
        original = getattr(wire.ScenarioCodec, name)
        def record(self, *args, _name=name, _original=original):
            result = _original(self, *args)
            log.append(dict(operation=_name, result=deepcopy(result)))
            return result
        monkeypatch.setattr(wire.ScenarioCodec, name, record)


def checkpoint(replay, trace, name):
    inspection = replay._codec.inspect_state()
    trace.append(dict(checkpoint=name, owner=inspection, scheduler=replay._scheduler_observation(),
                      policy=deepcopy(vars(replay._policy)), continuations=deepcopy(replay._continuations)))
    return inspection


def source(replay, value, at, sequence=0):
    assert replay._enqueue(dict(kind="SOURCE_GROUP", group=value, schedule_sequence=sequence))["classification"] == "PENDING"
    result = replay._dispatch_due(at)
    observation = replay._scheduler_observation()
    return result, observation["records"][(0, value["group_id"])]["result"].get("group_result")


def twice(monkeypatch, run):
    outcomes: list[Any] = []
    for _ in range(2):
        native, trace = [], []
        with monkeypatch.context() as patch:
            record_native(patch, native)
            replay = run(trace)
            final = checkpoint(replay, trace, "final")
            outcomes.append((canonical(dict(native=native, trace=trace)), final["owner_state_digest"],
                             deepcopy(replay._policy.events)))
    assert outcomes[0] == outcomes[1]
    return outcomes[0]
