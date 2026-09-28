import ast
from copy import deepcopy
from itertools import product
from pathlib import Path
from typing import Any

import pytest

from src.core.backtest import spider_run_envelope_schema as e
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from test_spider_run_native_schema import INSPECTION, REQUEST, RESULT, at, delivery, fact, group, objects

SEND = dict(instId="P_A", instIdCode=1, tdMode="", clOrdId="c", tag="", side="buy", ordType="limit", px="1", sz="1")
CANCEL = dict(ordId="o", instId="P_A", instIdCode=1)
EVENTS: list[dict[str, Any]] = [dict(at_ms=1, kind=kind) for kind in ["request_market", "request_earn", "check_websocket"]]
EVENTS += [dict(at_ms=1, kind="alert", reason="offline", operation=op) for op in ["send", "cancel"]]
EVENTS += [dict(at_ms=1, kind="alert", reason=reason, name="") for reason in ["reset_step", "high_low_limit", "individual_limit"]]
EVENTS += [dict(at_ms=1, kind="alert", reason=reason) for reason in ["total_limit", "daily_stop"]]
EVENTS += [dict(at_ms=1, kind=kind, operation=op, route=route, orders=[order])
           for kind, op, order in [("send", "send", SEND), ("cancel", "cancel", CANCEL),
                                   ("console_only", "send", SEND), ("console_only", "cancel", CANCEL)]
           for route in ["REST", "WS"]]
EVENTS += [dict(at_ms=1, kind="send", operation="send", route="REST", orders=[{**SEND, "ordType": "market", "px": ""}])]
EVENTS += [dict(at_ms=1, kind="unsupported_source_path", reason="non_grid_fill_enters_orderFilled",
               order=dict(instId="P_A", side="sell", px="", clOrdId="c", state="", sz="1", accFillSz="0"))]


def action(event=0, index=0) -> dict[str, Any]:
    return dict(event_index=event, action_index=index, group_id=None, status="UNSUBMITTED", group_result=None, native_failure=None)


def callback(events=None) -> dict[str, Any]:
    events = deepcopy(events or [])
    actions = [action(i, j) for i, event in enumerate(events) if event["kind"] in ("send", "cancel")
               for j in range(len(event["orders"]))]
    return dict(delivery_id="d", outcome="SUCCESS", policy_events=events, actions=actions, failure=None)


def key(kind="SOURCE_GROUP", stable="s", time=1, sequence=0):
    return dict(visible_at=time, queue_class=kind, schedule_sequence=sequence, stable_id=stable)


def owner() -> dict[str, Any]:
    value: dict[str, Any] = dict(cutoff=1, inspection=deepcopy(INSPECTION))
    for name, kind in [("trading", "TRADING"), ("positions", "POSITIONS"), ("open_orders", "OPEN_ORDERS")]:
        value[name + "_request"] = {**deepcopy(REQUEST), "snapshot_kind": kind}
        value[name + "_fact"] = fact(kind)
    return value


PAYLOADS = {
    "SOURCE_GROUP_RESULT": dict(request=group(), result=deepcopy(RESULT), owner_evidence_before=owner(), owner_evidence_after=owner()),
    "SNAPSHOT_FACT": dict(purpose="POLL", request=deepcopy(REQUEST), fact=fact("TRADING")),
    "DELIVERY_ATTEMPT": dict(delivery=delivery("TRADING_SNAPSHOT", {"outcome": "SUCCESS", "equity": "1", "available_equity": "1"}), emission_plan_digest=None),
    "CALLBACK_RESULT": callback(),
}


def record(kind="CALLBACK_RESULT", seq=1) -> dict[str, Any]:
    queue_class = {"SOURCE_GROUP_RESULT": "SOURCE_GROUP", "SNAPSHOT_FACT": "SNAPSHOT_CAPTURE"}.get(kind, "DELIVERY")
    return dict(schema_version="spider_journal_record_v1", run_id="r", journal_seq=seq,
                barrier_id=f"barrier-{seq}", record_kind=kind, scheduler_key=key(queue_class), causal_parent_ids=[],
                effective_at=1, visible_at=2, account_version_before=0 if kind == "SOURCE_GROUP_RESULT" else None,
                account_version_after=1 if kind == "SOURCE_GROUP_RESULT" else None, payload=deepcopy(PAYLOADS[kind]))


def observation() -> dict[str, Any]:
    return dict(current_time=1, last_popped=None, gate="FAILED", terminal=None,
                pending_keys=[], records=[], polls=[], callback_actions=[])


def endpoint() -> dict[str, Any]:
    return dict(schema_version="spider_endpoint_v1", run_id="r", terminal_reason="SCHEDULED_MTM",
                cutoff=dict(scheduler_time=1, persisted_boundary=None), initial_owner_evidence=owner(),
                final_owner_evidence=owner(), scheduler_observation=observation(), remaining_planned_barriers=[])


CASES = [(e.owner_evidence, owner()), (e.scheduler_key, key()), (e.callback_action, action()),
         (e.callback_result, callback()), (e.scheduler_observation, observation()), (e.endpoint, endpoint())]
CASES += [(e.journal_record, record(kind)) for kind in PAYLOADS]
CASES += [(e.callback_result, callback([event])) for event in EVENTS]


@pytest.mark.parametrize("check,fixture", CASES)
def test_closed_nested_keys_types_and_input_immutability(check, fixture):
    before = deepcopy(fixture)
    check(fixture)
    assert fixture == before
    for path, node in objects(fixture):
        for field in [*node, "extra"]:
            changed = deepcopy(fixture)
            if field == "extra":
                at(changed, path)[field] = None
            else:
                del at(changed, path)[field]
            with pytest.raises(ValueError):
                check(changed)
        for field, value in node.items():
            changed = deepcopy(fixture)
            at(changed, path)[field] = True if type(value) is int else 1.5
            with pytest.raises(ValueError):
                check(changed)


@pytest.mark.parametrize("kind,other", product(PAYLOADS, repeat=2))
def test_journal_payload_cross_wiring(kind, other):
    value = record(kind)
    value["payload"] = deepcopy(PAYLOADS[other])
    if kind == other:
        e.journal_record(value)
    else:
        with pytest.raises(ValueError):
            e.journal_record(value)


@pytest.mark.parametrize("kind", PAYLOADS)
def test_journal_versions_and_continuity(kind):
    for field in ["account_version_before", "account_version_after"]:
        value = record(kind)
        value[field] = None if kind == "SOURCE_GROUP_RESULT" else 1
        with pytest.raises(ValueError):
            e.journal_record(value)
    e.journal([record(kind, 1), record(kind, 2)])
    for rows in [[record(kind, 2)], [record(kind, 1), record(kind, 3)], [record(kind, 1)] * 2,
                 [record(kind, 1), {**record(kind, 2), "barrier_id": "barrier-1"}]]:
        with pytest.raises(ValueError):
            e.journal(rows)
    with pytest.raises(ValueError):
        e.journal_record({**record(kind), "causal_parent_ids": ["p", "p"]})


@pytest.mark.parametrize("outcome", ["SUCCESS", "CALLBACK_FAILED", "PLAN_FAILED"])
def test_prefix_action_indexes_preserve_nonfinancial_events_and_reset_per_order_list(outcome):
    events = [dict(at_ms=1, kind="alert", reason="total_limit"),
              dict(at_ms=1, kind="send", operation="send", route="REST", orders=[SEND, {**SEND, "clOrdId": "c2"}]),
              dict(at_ms=1, kind="cancel", operation="cancel", route="WS", orders=[CANCEL])]
    for length, pairs in [(1, []), (2, [(1, 0), (1, 1)]), (3, [(1, 0), (1, 1), (2, 0)])]:
        value = callback(events[:length])
        value.update(outcome=outcome, actions=[action(i, j) for i, j in pairs],
                     failure=None if outcome == "SUCCESS" else {"kind": "CALLBACK", "reason": "CALLBACK_FAILED"})
        e.callback_result(value)
        for bad in [list(reversed(value["actions"])), [action(0, j) for _, j in pairs], value["actions"][:-1], value["actions"] * 2]:
            if bad != value["actions"]:
                with pytest.raises(ValueError):
                    e.callback_result({**value, "actions": bad})
        observed = [{**a, "delivery_id": "d"} for a in value["actions"]]
        e.scheduler_observation({**observation(), "callback_actions": observed})
        assert [(a["event_index"], a["action_index"]) for a in observed] == pairs


def test_policy_event_union_decimal_optional_and_enum_rejections():
    for event in EVENTS:
        for field in ["kind", "reason", "operation", "route"]:
            if field in event:
                with pytest.raises(ValueError):
                    e.callback_result(callback([{**event, field: "UNKNOWN"}]))
    for field, invalid in [("ordType", "other"), ("side", "LONG"), ("px", "1.0"), ("sz", "1e0"), ("instIdCode", 1 << 63)]:
        event = dict(at_ms=1, kind="send", operation="send", route="REST", orders=[{**SEND, field: invalid}])
        with pytest.raises(ValueError):
            e.callback_result(callback([event]))
    for px in ["", "1"]:
        event = deepcopy(EVENTS[-1])
        event["order"].update(px=px, cTime=None, sMsg="")
        e.callback_result(callback([event]))
    for kind, op, order in [("send", "cancel", CANCEL), ("cancel", "send", SEND)]:
        with pytest.raises(ValueError):
            e.callback_result(callback([dict(at_ms=1, kind=kind, operation=op, route="REST", orders=[order])]))


@pytest.mark.parametrize("status,awaiting", product(["PAUSED", "IN_PROGRESS", "COMPLETED", "CALLBACK_FAILED"],
                                                   [None, "EARN_IF_DUE", "TRADING", "POSITIONS", "OPEN_ORDERS", "LOCAL_ACTIONS"]))
def test_poll_structural_matrix_does_not_decide_completion(status, awaiting):
    poll = dict(poll_id="p", continuation_id="c", status=status, awaiting=awaiting)
    e.scheduler_observation({**observation(), "polls": [poll]})
    for field in ["status", "awaiting"]:
        with pytest.raises(ValueError):
            e.poll({**poll, field: "UNKNOWN"})


@pytest.mark.parametrize("kind", ["SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "POLL", "DISPATCH", "QUEUE"])
def test_terminal_nullability(kind):
    for stable in [None, "s"]:
        value = dict(kind=kind, stable_id=stable, classification="TERMINAL", reason="NEW_CODE")
        if stable is not None or kind in ("DISPATCH", "QUEUE"):
            e.scheduler_terminal(value)
        else:
            with pytest.raises(ValueError):
                e.scheduler_terminal(value)


def test_all_scheduler_sort_and_duplicate_rules_use_exact_identity():
    pending = [key("SOURCE_GROUP", "z", 1, 9), key("SNAPSHOT_CAPTURE", "a", 1, 0), key("DELIVERY", "a", 1, 0)]
    records = [dict(key=key("SOURCE_GROUP", "z", 9), kind="SOURCE_GROUP", stable_id="z", classification="PENDING"),
               dict(key=key("DELIVERY", "a", 0), kind="DELIVERY", stable_id="a", classification="TERMINAL")]
    polls = [dict(poll_id="a", continuation_id="z", status="PAUSED", awaiting=None),
             dict(poll_id="b", continuation_id="a", status="IN_PROGRESS", awaiting="TRADING")]
    actions = [{**action(2, 1), "delivery_id": "a"}, {**action(0, 0), "delivery_id": "b"}]
    value = {**observation(), "pending_keys": pending, "records": records, "polls": polls, "callback_actions": actions}
    e.scheduler_observation(value)
    for field in ["pending_keys", "records", "polls", "callback_actions"]:
        for bad in [list(reversed(value[field])), value[field] + [value[field][-1]]]:
            with pytest.raises(ValueError):
                e.scheduler_observation({**value, field: bad})
    for identities in [[("a", "a"), ("a", "b")], [("a", "c"), ("b", "c")]]:
        duplicates = [dict(poll_id=p, continuation_id=c, status="PAUSED", awaiting=None) for p, c in identities]
        with pytest.raises(ValueError):
            e.scheduler_observation({**observation(), "polls": duplicates})
    with pytest.raises(ValueError):
        e.scheduler_observation({**observation(), "records": [{**records[0], "stable_id": "different"}]})


def test_callback_failure_and_action_branches_remain_diagnostic():
    for status, group_id, result, failure in product(["SUBMITTED", "UNSUBMITTED"], [None, "g"],
                                                    [None, RESULT], [None, {"reason": "CODE", "poisoned": True}]):
        e.callback_action({**action(), "status": status, "group_id": group_id, "group_result": result, "native_failure": failure})
    for kind in ["CALLBACK", "PLAN", "NATIVE"]:
        e.callback_result({**callback(), "outcome": "PLAN_FAILED", "failure": {"kind": kind, "reason": "CODE"}})
    for failure in [{"reason": "lowercase", "poisoned": True}, {"reason": "CODE", "poisoned": 1}, {"reason": "CODE"}]:
        with pytest.raises(ValueError):
            e.callback_action({**action(), "native_failure": failure})


def test_endpoint_structure_does_not_enforce_selected_protocol():
    value = endpoint()
    value.update(terminal_reason="O03_NON_ATOMIC_COMPLETE", remaining_planned_barriers=[
        {"ordinal": 8, "barrier_id": "future", "record_kind": "CALLBACK_RESULT"}])
    value["scheduler_observation"].update(pending_keys=[key()], last_popped=key())
    value["cutoff"]["persisted_boundary"] = {"ordinal": 2, "barrier_id": "other", "journal_seq": 2}
    e.endpoint(value)
    for path, bad in [(('terminal_reason',), "UNKNOWN"), (('cutoff', 'scheduler_time'), True),
                      (('cutoff', 'persisted_boundary'), {}), (('final_owner_evidence', 'inspection', 'cash'), "1.0")]:
        changed = deepcopy(value)
        at(changed, path[:-1])[path[-1]] = bad
        with pytest.raises(ValueError):
            e.endpoint(changed)


def test_import_boundary_and_no_io_or_dynamic_execution():
    tree = ast.parse(Path(e.__file__).read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            assert isinstance(node, ast.ImportFrom)
            assert node.module in {"typing", "src.core.backtest", "src.core.backtest.spider_run_artifacts"}
            if node.module == "src.core.backtest":
                assert [alias.name for alias in node.names] == ["spider_run_native_schema"]
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"open", "eval", "exec", "__import__"}


def test_pending_full_key_axes_and_record_identity_are_not_interchangeable():
    pending = [key("DELIVERY", "z", 0, 9), key("SOURCE_GROUP", "z", 1, 0),
               key("SOURCE_GROUP", "a", 1, 1), key("SOURCE_GROUP", "z", 1, 1)]
    records = [dict(key=key("SOURCE_GROUP", "a", 9, 9), kind="SOURCE_GROUP", stable_id="a", classification="SUCCESS"),
               dict(key=key("SOURCE_GROUP", "z", 0, 0), kind="SOURCE_GROUP", stable_id="z", classification="SUCCESS")]
    e.scheduler_observation({**observation(), "pending_keys": pending, "records": records})
    e.scheduler_observation({**observation(), "pending_keys": [key(time=1), key(time=2)]})
    for index in range(len(pending) - 1):
        changed = deepcopy(pending)
        changed[index], changed[index + 1] = changed[index + 1], changed[index]
        with pytest.raises(ValueError):
            e.scheduler_observation({**observation(), "pending_keys": changed})
    with pytest.raises(ValueError):
        e.scheduler_observation({**observation(), "records": records[::-1]})
    with pytest.raises(ValueError):
        e.scheduler_observation({**observation(), "records": [records[0], {**records[0], "key": key("SOURCE_GROUP", "a", 10)}]})


def test_observed_action_sort_uses_delivery_then_original_event_then_original_action():
    actions = [{**action(1, 8), "delivery_id": "a"}, {**action(2, 0), "delivery_id": "a"},
               {**action(2, 1), "delivery_id": "a"}, {**action(0, 0), "delivery_id": "b"}]
    before = deepcopy(actions)
    e.scheduler_observation({**observation(), "callback_actions": actions})
    assert actions == before
    for index in range(3):
        changed = deepcopy(actions)
        changed[index], changed[index + 1] = changed[index + 1], changed[index]
        with pytest.raises(ValueError):
            e.scheduler_observation({**observation(), "callback_actions": changed})
    for field in ["event_index", "action_index", "delivery_id"]:
        changed = deepcopy(actions)
        del changed[0][field]
        with pytest.raises(ValueError):
            e.scheduler_observation({**observation(), "callback_actions": changed})


def test_owner_and_journal_structural_discriminants():
    for field, value in [("capture_mode", "FROZEN_POLL_FIXTURE"), ("snapshot_kind", "EARN")]:
        changed = owner()
        changed["trading_request"][field] = value
        with pytest.raises(ValueError):
            e.owner_evidence(changed)
    changed = owner()
    changed["trading_fact"] = fact("EARN")
    with pytest.raises(ValueError):
        e.owner_evidence(changed)
    for kind in PAYLOADS:
        changed = record(kind)
        changed["scheduler_key"]["queue_class"] = "DELIVERY" if kind == "SOURCE_GROUP_RESULT" else "SOURCE_GROUP"
        with pytest.raises(ValueError):
            e.journal_record(changed)


def test_callback_failure_objects_and_unapproved_nulls_are_closed():
    for failure in [{"kind": "UNKNOWN", "reason": "CODE"}, {"kind": "PLAN", "reason": "bad reason"},
                    {"kind": "PLAN"}, {"kind": "PLAN", "reason": "CODE", "extra": None}]:
        with pytest.raises(ValueError):
            e.callback_result({**callback(), "failure": failure})
    for check, fixture, field in [(e.endpoint, endpoint(), "cutoff"), (e.owner_evidence, owner(), "inspection"),
                                  (e.scheduler_observation, observation(), "records"),
                                  (e.callback_result, callback(), "policy_events")]:
        with pytest.raises(ValueError):
            check({**fixture, field: None})
    event = dict(at_ms=1, kind="send", operation="send", route="REST", orders=[{**SEND, "ordType": "market", "px": "1"}])
    with pytest.raises(ValueError):
        e.callback_result(callback([event]))
    for key in ["cTime", "sMsg"]:
        event = deepcopy(EVENTS[-1])
        event["order"][key] = 1
        with pytest.raises(ValueError):
            e.callback_result(callback([event]))


def test_remaining_barrier_order_duplicate_and_optional_digest():
    barriers = [dict(ordinal=3, barrier_id="a", record_kind="CALLBACK_RESULT"),
                dict(ordinal=4, barrier_id="b", record_kind="DELIVERY_ATTEMPT")]
    e.endpoint({**endpoint(), "remaining_planned_barriers": barriers})
    for bad in [barriers[::-1], [barriers[0], {**barriers[1], "ordinal": 3}],
                [barriers[0], {**barriers[1], "barrier_id": "a"}]]:
        with pytest.raises(ValueError):
            e.endpoint({**endpoint(), "remaining_planned_barriers": bad})
    for digest in [None, "a" * 64, "A" * 64]:
        value = record("DELIVERY_ATTEMPT")
        value["payload"]["emission_plan_digest"] = digest
        if digest == "A" * 64:
            with pytest.raises(ValueError):
                e.journal_record(value)
        else:
            e.journal_record(value)


@pytest.mark.parametrize("text", ["\ud800", "\udc00", "中文😀"])
def test_policy_text_utf8_boundary_through_canonical_json(text):
    events = deepcopy(EVENTS)
    events[-1]["order"].update(cTime="", sMsg="")
    fields = {"ordId", "instId", "clOrdId", "tdMode", "tag", "name", "state", "cTime", "sMsg"}
    for event in events:
        for path, node in objects(event):
            for field in fields & node.keys():
                changed = deepcopy(event)
                at(changed, path)[field] = text
                value = decode_canonical(canonical_bytes(callback([changed])))
                if text == "中文😀":
                    e.callback_result(value)
                else:
                    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
                        e.callback_result(value)
