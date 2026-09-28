"""Actual owner capture, detached projections, and unresolved endpoint failures."""

from copy import deepcopy
from decimal import Decimal
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_artifacts as artifacts
from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_run_evidence as evidence
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_run_evidence import RUN, at, fixture
from test_spider_scheduler import empty, poll_plan, source


def normalized(value):
    return cast(Any, artifacts.decode_canonical(artifacts.canonical_bytes(value)))


def capture(owner, template):
    return owner.capture_owner_evidence(template["cutoff"], *[template[name + "_request"] for name in ("trading", "positions", "open_orders")])


def build(values):
    attempt, status, journal, endpoint, _ = values
    return evidence.build_endpoint_artifacts(RUN, attempt, status, journal, endpoint["initial_owner_evidence"],
                                             endpoint["final_owner_evidence"], endpoint["scheduler_observation"])


@pytest.mark.parametrize("index", range(3))
def test_actual_initial_post_final_capture_and_reconstruction(index):
    values = fixture(index)
    attempt, _, expected_journal, endpoint, expected_report = values
    owner = _ReplayComposition(attempt["profile_id"], attempt["account_key"])
    initial = capture(owner, endpoint["initial_owner_evidence"])["owner_evidence"]
    assert isinstance(initial["inspection"]["cash"], Decimal)
    previous, journal = initial, []
    def barrier(kind, key, payload):
        nonlocal previous
        expected = expected_journal[len(journal)]
        assert kind == expected["record_kind"] and key == expected["scheduler_key"]
        actual = deepcopy(expected)
        if kind == "SOURCE_GROUP_RESULT":
            after = capture(owner, expected["payload"]["owner_evidence_after"])["owner_evidence"]
            payload.update(owner_evidence_before=previous, owner_evidence_after=after)
            previous = after
        actual["payload"] = normalized(payload)
        journal.append(actual)
    owner._evidence_callback = barrier
    for row in expected_journal:
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            group = wire._decode(artifacts.canonical_bytes(row["payload"]["request"]).decode())
            owner._enqueue(dict(kind="SOURCE_GROUP", schedule_sequence=row["scheduler_key"]["schedule_sequence"], group=group))
            assert owner._dispatch_due(row["visible_at"])["classification"] == "SUCCESS"
        elif row["record_kind"] == "DELIVERY_ATTEMPT":
            frozen = row["payload"]["delivery"]
            delivery = owner._codec.build_delivery(cast(wire.Projection, dict(schema_version="delivery_projection_v1",
                reference=dict(namespace=frozen["source_namespace"], fact_id=frozen["source_fact_id"]),
                **{key: frozen[key] for key in ("payload_kind", "occurrence_index", "schedule_sequence", "visible_at")})))
            owner._enqueue(dict(kind="DELIVERY", delivery=delivery), empty(delivery))
            assert owner._dispatch_due(row["visible_at"])["classification"] == "SUCCESS"
    owner._dispatch_due(endpoint["cutoff"]["scheduler_time"])
    final = normalized(capture(owner, endpoint["final_owner_evidence"]))
    assert journal == expected_journal
    values[2] = journal
    endpoint.update(initial_owner_evidence=normalized(initial), final_owner_evidence=final["owner_evidence"],
                    scheduler_observation=final["scheduler_observation"])
    before = deepcopy(values)
    actual = build(values)
    assert actual["endpoint"] == before[3] and actual["report"] == expected_report
    assert actual["reconciliation"]["result"] == "OK"
    assert values == before
    actual["endpoint"]["final_owner_evidence"].clear()
    actual["report"][0]["source_evidence_refs"].clear()
    assert values == before
    if index == 2:
        assert [row["payload"]["result"]["classification"] for row in journal] == ["COMMITTED", "COMMITTED", "REJECTED", "REJECTED"]
        assert journal[2]["payload"]["result"]["rejections"]


@pytest.mark.parametrize("stop", range(4))
@pytest.mark.parametrize("native", [False, True])
def test_capture_first_error_stops_without_translation_or_retry(stop, native, monkeypatch):
    values = fixture()
    owner = _ReplayComposition(values[0]["profile_id"], values[0]["account_key"])
    error: Exception = RuntimeError("original")
    if native:
        with pytest.raises(ValueError) as rejected:
            owner._codec.capture_snapshot(cast(wire.SnapshotRequest, {}))
        error = rejected.value
    calls = []
    inspect, snapshot = wire.ScenarioCodec.inspect_state, wire.ScenarioCodec.capture_snapshot
    def observe(self, request=None):
        calls.append("inspect" if request is None else request["snapshot_kind"])
        if len(calls) - 1 == stop:
            raise error
        return inspect(self) if request is None else snapshot(self, request)
    monkeypatch.setattr(wire.ScenarioCodec, "inspect_state", observe)
    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", observe)
    with pytest.raises(type(error)) as caught:
        capture(owner, values[3]["initial_owner_evidence"])
    assert caught.value is error
    assert calls == ["inspect", "TRADING", "POSITIONS", "OPEN_ORDERS"][:stop + 1]
    assert owner._current_time == 500 and owner._terminal is None


@pytest.mark.parametrize("field,value", [("cutoff", True), ("cutoff", "500"), ("cutoff", -1),
    ("snapshot_kind", "POSITIONS"), ("capture_mode", "FROZEN_POLL_FIXTURE"), ("captured_at", 501), ("account_key", {})])
def test_capture_request_preflight_before_native(field, value, monkeypatch):
    values = fixture()
    owner = _ReplayComposition(values[0]["profile_id"], values[0]["account_key"])
    template = values[3]["initial_owner_evidence"]
    (template if field == "cutoff" else template["trading_request"])[field] = value
    def forbidden(_):
        pytest.fail("inspection must not begin")
    monkeypatch.setattr(wire.ScenarioCodec, "inspect_state", forbidden)
    with pytest.raises(ValueError):
        capture(owner, template)


def test_capture_retains_pending_poll_actions_terminal_and_detaches():
    values = fixture()
    owner = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", values[0]["account_key"])
    owner._begin_poll(poll_plan())
    owner._enqueue(source("later", 100020))
    owner._audit = {"z": [dict(event={}, actions=[dict(group_id=None, status="UNSUBMITTED")])],
                    "a": [dict(event={}), dict(event={}, actions=[dict(group_id="g", status="SUBMITTED")])]}
    owner._dispatch_due(False)
    before = deepcopy(owner._scheduler_observation())
    policy_before = deepcopy(vars(owner._policy))
    def forbidden(*_):
        pytest.fail("capture must not emit a barrier")
    owner._evidence_callback = forbidden
    result = capture(owner, values[3]["initial_owner_evidence"])
    observed = result["scheduler_observation"]
    envelope.scheduler_observation(normalized(observed))
    assert observed["current_time"] == owner._current_time and observed["terminal"]["reason"] == "INVALID_SCHEMA"
    assert len(observed["pending_keys"]) == 2 and observed["polls"][0]["status"] == "IN_PROGRESS"
    assert observed["polls"][0]["awaiting"] == "TRADING"
    assert [(row["delivery_id"], row["event_index"]) for row in observed["callback_actions"]] == [("a", 1), ("z", 0)]
    assert all(row["classification"] == "PENDING" for row in observed["records"])
    result["owner_evidence"]["trading_request"]["account_key"].clear()
    observed["callback_actions"][0]["status"] = "UNSUBMITTED"
    observed["pending_keys"].clear()
    assert owner._scheduler_observation() == before and vars(owner._policy) == policy_before
    assert values[3]["initial_owner_evidence"]["trading_request"]["account_key"] == values[0]["account_key"]


@pytest.mark.parametrize("path,replacement,check", [
    (("pending_keys",), [dict(visible_at=999, queue_class="SOURCE_GROUP", schedule_sequence=0, stable_id="risk")], 6),
    (("pending_keys",), [dict(visible_at=999, queue_class="SNAPSHOT_CAPTURE", schedule_sequence=0, stable_id="snapshot")], 6),
    (("pending_keys",), [dict(visible_at=999, queue_class="DELIVERY", schedule_sequence=0, stable_id="delivery")], 6),
    *[(("polls",), [dict(poll_id="p", continuation_id="c", status=status, awaiting=None)], 7)
      for status in ("PAUSED", "IN_PROGRESS", "CALLBACK_FAILED", "COMPLETED")],
    *[(("callback_actions",), [dict(delivery_id="d", event_index=0, action_index=0, group_id=None,
        status=status, group_result=None, native_failure=None)], 8) for status in ("UNSUBMITTED", "SUBMITTED")],
    (("current_time",), 999, 9),
])
def test_representable_pending_observations_return_complete_failed(path, replacement, check):
    values = fixture()
    at(values[3]["scheduler_observation"], path[:-1])[path[-1]] = replacement
    result = build(values)
    assert result["endpoint"]["scheduler_observation"] == values[3]["scheduler_observation"]
    assert len(result["reconciliation"]["checks"]) == 11
    assert result["reconciliation"]["checks"][check]["result"] == "FAILED"


@pytest.mark.parametrize("index", range(3))
def test_remaining_suffix_and_actual_financial_values(index):
    values = fixture(index)
    values[1]["persisted_boundary"] = dict(ordinal=1, journal_seq=1, barrier_id=values[2][0]["barrier_id"])
    final = values[3]["final_owner_evidence"]
    final["inspection"].update(cash="123", gross_realized="-7", total_fees="9")
    final["trading_fact"]["immutable_payload"].update(equity="456", available_equity="321")
    product = values[4][0]["product_id"]
    final["positions_fact"]["immutable_payload"]["rows"] = [dict(product_id=product, margin_mode="cross",
        position_contracts="2", last_price="3", notional_usd="4")]
    result = build(values)
    assert result["endpoint"]["remaining_planned_barriers"] == values[0]["planned_coverage"][1:]
    row = result["report"][0]
    assert [row[key] for key in ("account_cash", "account_gross_realized", "account_total_fees", "account_equity",
        "account_available_equity", "position_contracts", "mark_price", "notional_usd")] == ["123", "-7", "9", "456", "321", "2", "3", "4"]
    assert result["reconciliation"]["result"] == "FAILED"


@pytest.mark.parametrize("kind", ["trading", "positions", "open_orders"])
def test_failure_payload_is_unrepresentable(kind):
    values = fixture()
    values[3]["final_owner_evidence"][kind + "_fact"]["immutable_payload"] = dict(outcome="FAILURE", reason="INVALID_SCHEMA")
    with pytest.raises(evidence.ReconciliationProjectionError):
        build(values)


@pytest.mark.parametrize("selector", [None, 1, True, "unknown"])
def test_builder_invalid_selector(selector):
    values = fixture()
    values[0]["scenario_plan_id"] = selector
    with pytest.raises(evidence.ReconciliationProjectionError):
        build(values)


def test_builder_run_id_precedes_selection_and_unknown_errors_propagate(monkeypatch):
    values = fixture()
    error = ValueError("programming defect")
    def broken(_):
        raise error
    monkeypatch.setattr(evidence, "_plan_bundle", broken)
    with pytest.raises(ValueError) as caught:
        build(values)
    assert caught.value is error
    values[1]["run_id"] = "other"
    with pytest.raises(evidence.ReconciliationProjectionError):
        build(values)


@pytest.mark.parametrize("path", [(1, "processed_boundary"), (1, "persisted_boundary"),
    *[(3, "final_owner_evidence", kind + "_fact", "captured_account_version") for kind in ("trading", "positions", "open_orders")],
    (3, "final_owner_evidence", "positions_fact", "immutable_payload", "rows", 0, "last_price"),
    (3, "final_owner_evidence", "positions_fact", "immutable_payload", "rows", 0, "notional_usd")])
def test_builder_missing_required_projection_emits_nothing(path):
    values = fixture(2)
    if len(path) == 2:
        at(values, path[:-1])[path[-1]] = None
    else:
        del at(values, path[:-1])[path[-1]]
    with pytest.raises(evidence.ReconciliationProjectionError):
        build(values)


@pytest.mark.parametrize("path,replacement,check", [
    (("final_owner_evidence", "trading_fact", "captured_account_version"), 99, 5),
    (("final_owner_evidence", "positions_fact", "snapshot_as_of"), 999, 5),
    (("final_owner_evidence", "inspection", "lifecycle"), "AWAITING_CANCEL_EFFECTIVE", 9),
    (("scheduler_observation", "gate"), "FAILED", 9),
])
def test_builder_representable_versions_and_terminal_mismatch(path, replacement, check):
    values = fixture()
    at(values[3], path[:-1])[path[-1]] = replacement
    result = build(values)
    assert result["reconciliation"]["checks"][check]["result"] == "FAILED"
    assert result["reconciliation"]["result"] == "FAILED"
