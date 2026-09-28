import ast
from copy import deepcopy
from decimal import Decimal as D
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

from src.core.adapters.simulated import SimulatedAdapter
from src.core.backtest import spider_policy_protocol as protocol
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.mocks.account_service import BacktestAccountService
from src.core.repositories import BacktestOrderRepository
from spider_acceptance_fixtures import ACCOUNT, canonical, checkpoint, delivery_id, twice
from test_spider_integrated_financial import minimum_run
from test_synthetic_scenario_import_boundary import ROOT, sources
from test_architecture_import_boundaries import _module_name, _static_imports, _dynamic_imports


def stage(name, kind="TRADING", fixture="POLL_Q1_S1", at=500, visible=501, continuation=None, sequence=0, account=ACCOUNT) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = {"TRADING": "TRADING_SNAPSHOT", "POSITIONS": "POSITION_SNAPSHOT", "OPEN_ORDERS": "OPEN_ORDER_SNAPSHOT"}[kind]
    request = dict(schema_version="snapshot_request_v1", account_key=deepcopy(account), snapshot_id=name,
                   snapshot_kind=kind, capture_mode="FROZEN_POLL_FIXTURE", fixture_key=fixture, captured_at=at)
    projection = dict(schema_version="delivery_projection_v1", reference=dict(namespace="SNAPSHOT", fact_id=name),
                      payload_kind=payload, occurrence_index=0, schedule_sequence=sequence, visible_at=visible)
    if continuation is not None:
        request["continuation_id"] = projection["continuation_id"] = continuation
    emission = dict(delivery_id=delivery_id(name, payload, "SNAPSHOT", account=account),
                    expected_policy_events=[], financial_items=[], market_requests=[])
    return dict(capture_sequence=sequence, snapshot_request=request, delivery_projection=projection), emission


def capture(replay, item):
    before = replay._codec.inspect_state()
    fact = replay._codec.capture_snapshot(cast(wire.SnapshotRequest, item["snapshot_request"]))
    delivery = replay._codec.build_delivery(cast(wire.Projection, item["delivery_projection"]))
    assert delivery["immutable_payload"] == fact["immutable_payload"]
    assert replay._codec.inspect_state() == before
    return fact, delivery


def poll_run(trace, reverse):
    plans, emissions = [], {}
    for i, name in enumerate(("Q1", "Q2")):
        start = 100010 + (1-i if reverse else i)*10
        plan: dict[str, Any] = dict(account_key=deepcopy(ACCOUNT), poll_id=name, issued_at=100000, continuation_id=name)
        for j, (key, kind, fixture) in enumerate((
            ("trading", "TRADING", "POLL_Q1_S1" if i == 0 else "POLL_Q2_S2"),
            ("positions", "POSITIONS", "POLL_POSITIONS_EMPTY"), ("open_orders", "OPEN_ORDERS", "POLL_OPEN_ORDERS_EMPTY"))):
            item, emission = stage(name+key, kind, fixture, start+j*2, start+j*2, name, i*3+j)
            plan[key] = item
            emissions[emission["delivery_id"]] = emission
        plans.append(plan)
    replay = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT, emissions)
    initial = checkpoint(replay, trace, "initial")
    calls, local_actions = [], []
    raise_leverage = replay._policy.raise_leverage
    def observe_local():
        local_actions.append(tuple(name for name, record in replay._polls.items()
                                   if record.index == 2 and record.observation.status == "IN_PROGRESS"))
        return raise_leverage()
    with patch.object(replay._policy, "raise_leverage", side_effect=observe_local) as leverage, \
         patch.object(replay._policy, "check_risk", wraps=replay._policy.check_risk) as risk, \
         patch.object(replay._policy, "order_filled", wraps=replay._policy.order_filled) as fill:
        for plan in plans:
            assert replay._begin_poll(plan)["classification"] == "SUCCESS"
        for plan in sorted(plans, key=lambda p: p["trading"]["delivery_projection"]["visible_at"]):
            assert replay._dispatch_due(plan["open_orders"]["delivery_projection"]["visible_at"])["classification"] == "SUCCESS"
            record = replay._polls[plan["poll_id"]]
            assert record.observation.status == "COMPLETED" and record.index == 3
            calls.append((plan["poll_id"], replay._policy.capital["total"]))
        assert leverage.call_count == risk.call_count == 2 and fill.call_count == 0
    assert calls == ([("Q2", D(110)), ("Q1", D(100))] if reverse else [("Q1", D(100)), ("Q2", D(110))])
    assert local_actions == ([("Q2",), ("Q1",)] if reverse else [("Q1",), ("Q2",)])
    for plan in plans:
        saved = replay._scheduler_observation()
        assert replay._begin_poll(plan)["status"] == "COMPLETED"
        assert replay._scheduler_observation() == saved
        for key in ("trading", "positions", "open_orders"):
            fact, delivery = capture(replay, plan[key])
            assert delivery["delivery_id"] in emissions
            assert "captured_account_version" not in fact
            expected = dict(outcome="SUCCESS", equity=D(100 if plan["poll_id"] == "Q1" else 110),
                            available_equity=D(100 if plan["poll_id"] == "Q1" else 110)) if key == "trading" else dict(outcome="SUCCESS", rows=[])
            assert delivery["immutable_payload"] == expected
    assert replay._codec.inspect_state() == initial and replay._policy.events == []
    assert not any(key[0] == 0 for key in replay._records) and not replay._queue
    trace.append(dict(poll_completion_order=calls, local_actions=local_actions, fill_callbacks=0))
    checkpoint(replay, trace, "both-completed")
    return replay


def test_gt04_global_queue_normal_reverse_twice(monkeypatch):
    normal = twice(monkeypatch, lambda trace: poll_run(trace, False))
    reverse = twice(monkeypatch, lambda trace: poll_run(trace, True))
    assert normal[1:] == reverse[1:] and normal[0] != reverse[0]


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("conflict", ["payload", "plan"])
def test_native_delivery_identity_duplicate_conflict_retention(completed, conflict):
    item, emission = stage("S")
    tail, tail_plan = stage("tail", at=502, visible=503)
    replay = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT, {tail_plan["delivery_id"]: tail_plan})
    _, delivery = capture(replay, item)
    native = replay._codec.inspect_state()
    queued: dict[str, Any] = dict(kind="DELIVERY", delivery=delivery)
    first = replay._enqueue(queued, emission)
    assert replay._enqueue(deepcopy(queued), deepcopy(emission)) == first
    if completed:
        assert replay._dispatch_due(501)["classification"] == "SUCCESS"
    saved = deepcopy(vars(replay._policy)), replay._scheduler_observation()
    result = replay._enqueue(queued, emission)
    assert result["classification"] == ("SUCCESS" if completed else "PENDING")
    assert (vars(replay._policy), replay._scheduler_observation()) == saved
    assert replay._enqueue(replay._capture_item(tail))["classification"] == "PENDING"
    changed, plan = deepcopy(queued), deepcopy(emission)
    if conflict == "payload":
        changed["delivery"]["payload_digest"] = "00"*32
    else:
        plan["expected_policy_events"] = [dict(kind="request_market", event_digest="00"*32)]
    failure = replay._enqueue(changed, plan)
    assert failure["reason"] == "DELIVERY_ID_CONFLICT"
    assert replay._enqueue(None) == replay._begin_poll(None) == replay._dispatch_due(999999) == failure
    assert replay._codec.inspect_state() == native and replay._records[(1, "tail")]["result"]["classification"] == "PENDING"
    assert replay._scheduler_observation()["callbacks"] == saved[1]["callbacks"] and vars(replay._policy) == saved[0]


def callback_failure_run(trace):
    item, emission = stage("open", "OPEN_ORDERS", "POLL_OPEN_ORDERS_EMPTY")
    tail, tail_plan = stage("tail", at=502, visible=503)
    expected = dict(at_ms=100000, kind="request_market")
    emission["expected_policy_events"] = [dict(kind="request_market", event_digest=protocol._policy_event_digest(expected))]
    replay = _ReplayComposition("SYNTHETIC_GOLDEN_CANCEL_V1", ACCOUNT,
                                {emission["delivery_id"]: emission, tail_plan["delivery_id"]: tail_plan})
    native = replay._codec.inspect_state()
    del replay._policy.capital["total"]  # Real source missing-cache exception after request_market.
    assert replay._enqueue(replay._capture_item(item))["classification"] == "PENDING"
    assert replay._enqueue(replay._capture_item(tail))["classification"] == "PENDING"
    failure = replay._dispatch_due(600)
    assert failure["reason"] == "CALLBACK_FAILED" and failure["events"] == [dict(event=expected)]
    assert replay._policy.events == [expected] and replay._policy.last_market_ms == 100000
    assert replay._current_time == 501 and replay._codec.inspect_state() == native
    assert replay._records[(1, "tail")]["result"]["classification"] == "PENDING"
    assert replay._enqueue(None) == replay._dispatch_due(None) == replay._begin_poll(None) == failure
    checkpoint(replay, trace, "real-source-exception-prefix")
    return replay


def test_actual_policy_prefix_failure_twice(monkeypatch):
    twice(monkeypatch, callback_failure_run)


def test_account_isolation_and_deep_detached_poison():
    other = cast(wire.Account, {**ACCOUNT, "account": "B"})
    a, b = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT), _ReplayComposition("SYNTHETIC_BTC_ETH_V1", other)
    item, plan = stage("S")
    original = deepcopy(item)
    fact, delivery = capture(a, item)
    item_b, plan_b = stage("S", account=other)
    fact_b, delivery_b = capture(b, item_b)
    owners = a._codec.inspect_state(), b._codec.inspect_state()
    assert delivery["delivery_id"] != delivery_b["delivery_id"] and fact["immutable_payload"] == fact_b["immutable_payload"]
    with pytest.raises(ValueError, match="^ACCOUNT_KEY_MISMATCH$"):
        b._codec.capture_snapshot(cast(wire.SnapshotRequest, original["snapshot_request"]))
    a._enqueue(dict(kind="DELIVERY", delivery=delivery), plan)
    saved = a._scheduler_observation()
    cast(wire.Trading, fact["immutable_payload"])["equity"] = D(-999)
    cast(wire.Trading, delivery["immutable_payload"])["equity"] = D(-888)
    item["snapshot_request"]["fixture_key"] = "poison"
    plan["expected_policy_events"].append(dict(kind="bad", event_digest="00"*32))
    returned = a._scheduler_observation()
    returned["records"][(2, delivery["delivery_id"])]["item"]["delivery"]["immutable_payload"]["equity"] = D(-777)
    assert a._scheduler_observation() == saved
    stored_fact, stored_delivery = capture(a, original)
    assert stored_fact["immutable_payload"] == stored_delivery["immutable_payload"] == dict(outcome="SUCCESS", equity=D(100), available_equity=D(100))
    assert a._dispatch_due(501)["classification"] == "SUCCESS" and a._policy.capital["total"] == D(100)
    assert b._scheduler_observation()["records"] == {} and b._policy.events == []
    b._enqueue(dict(kind="DELIVERY", delivery=delivery_b), plan_b)
    assert b._dispatch_due(501)["classification"] == "SUCCESS"
    assert (a._codec.inspect_state(), b._codec.inspect_state()) == owners


def test_legacy_financial_boundaries_are_not_a_second_owner(monkeypatch):
    baseline = twice(monkeypatch, minimum_run)
    repository = BacktestOrderRepository(None, 123, D(-999))
    service = BacktestAccountService(repo=repository, initial_balance=D(-888))
    adapter = SimulatedAdapter.__new__(SimulatedAdapter)
    adapter._engine = cast(Any, SimpleNamespace(balance=D(-777), positions={"BTC-USDT-SWAP": D(999)}))
    adapter._order_map = cast(Any, {"MIN-O1": SimpleNamespace(quantity=D(888), price=D(1))})
    assert repository.balance == service.get_balance() == D(-888)
    assert adapter._engine.balance == D(-777) and adapter._engine.positions["BTC-USDT-SWAP"] == D(999)
    assert adapter._order_map["MIN-O1"].quantity == D(888)
    calls = []
    targets = [(BacktestOrderRepository, "get_position"), (BacktestOrderRepository, "get_order"),
               (BacktestAccountService, "get_balance"), (BacktestAccountService, "get_position"),
               (SimulatedAdapter, "get_balance"), (SimulatedAdapter, "get_position"), (SimulatedAdapter, "get_open_orders")]
    with monkeypatch.context() as guarded:
        for cls, name in targets:
            def trap(*args, _name=f"{cls.__name__}.{name}", **kwargs):
                calls.append(_name)
                raise AssertionError("legacy financial read")
            guarded.setattr(cls, name, trap)
        for obj, name in [(repository, "get_position"), (repository, "get_order"), (service, "get_balance"),
                          (service, "get_position"), (adapter, "get_balance"), (adapter, "get_position"), (adapter, "get_open_orders")]:
            with pytest.raises(AssertionError, match="^legacy financial read$"):
                getattr(obj, name)()
        assert len(calls) == len(targets)
        calls.clear()
        assert twice(monkeypatch, minimum_run) == baseline and calls == []


def test_o03_is_explicitly_deferred_not_report_admitted():
    evidence = {key: "DEFERRED_SLICE_6" for key in (
        "fee_zero_P_A", "provider_rejection_rows", "persistence", "journal", "report_admission")}
    encoded = canonical(evidence)
    assert set(evidence.values()) == {"DEFERRED_SLICE_6"}
    assert all(token not in encoded for token in (b"PASS", b"COMPLETE", b"report_artifact"))


def scheduler_imports(source, module, modules, packages):
    tree = ast.parse(source)
    return "src.core.backtest.synthetic_scenario_replay" in (_static_imports(module, tree, modules, packages) | _dynamic_imports(tree))


def scheduler_sources():
    paths = [p for p in sources(ROOT) if p.is_relative_to(ROOT / "python-strategy/src")]
    return paths, frozenset(_module_name(p) for p in paths), frozenset(_module_name(p) for p in paths if p.name == "__init__.py")


def scheduler_consumer(path):
    parts = path.replace("\\", "/").split("/")
    return "src" in parts and "tests" not in parts and any(
        token in {"provider", "providers", "adapter", "adapters", "strategy", "strategies", "runner", "runners"}
        for part in parts for token in part.removesuffix(".py").split("_"))


def test_scheduler_has_no_production_inbound_imports():
    paths, modules, packages = scheduler_sources()
    production = [p for p in paths if scheduler_consumer(str(p.relative_to(ROOT)))]
    assert production
    assert not scheduler_imports("from unrelated.module import _ReplayComposition as R", "src.core.backtest_runner", modules, packages)
    assert not scheduler_consumer("python-strategy/src/core/backtest/persistence_bridge.py")
    assert not [p for p in production if scheduler_imports(p.read_text(), _module_name(p), modules, packages)]


def test_cli_and_store_composition_have_one_way_ownership():
    paths, modules, packages = scheduler_sources()
    paired = []
    for path in paths:
        tree = ast.parse(path.read_text())
        imports = _static_imports(_module_name(path), tree, modules, packages) | _dynamic_imports(tree)
        if {"src.core.backtest.synthetic_scenario_replay", "src.core.backtest.spider_run_store"} <= imports:
            paired.append(path.name)
    assert paired == ["spider_scenario_run.py"]
    path = ROOT / "python-strategy/examples/run_spider_scenario_replay.py"
    tree = ast.parse(path.read_text())
    imports = _static_imports("examples.run_spider_scenario_replay", tree, modules, packages) | _dynamic_imports(tree)
    assert imports == {"argparse", "json", "pathlib", "sys", "src.core.backtest.spider_scenario_run"}
    assert not any(isinstance(node, ast.Attribute) and node.attr.startswith("_") for node in ast.walk(tree))


@pytest.mark.parametrize("snippet", [
    "from src.core.backtest.synthetic_scenario_replay import _ReplayComposition",
    "from src.core.backtest.synthetic_scenario_replay import _ReplayComposition as R",
    "from src.core.backtest.synthetic_scenario_replay import *",
    "import src.core.backtest.synthetic_scenario_replay as r; R = r._ReplayComposition",
    "import src.core.backtest.synthetic_scenario_replay; R = src.core.backtest.synthetic_scenario_replay._ReplayComposition",
    "from src.core.backtest import synthetic_scenario_replay as r; R = r._ReplayComposition",
    "R = __import__('src.core.backtest.synthetic_scenario_replay', fromlist=['_ReplayComposition'])._ReplayComposition",
    "import importlib as lib; R = lib.import_module('src.core.backtest.synthetic_scenario_replay')._ReplayComposition",
    "from importlib import import_module as load; R = load('src.core.backtest.synthetic_scenario_replay')._ReplayComposition",
    "import importlib as lib; R = lib.import_module(name='src.core.backtest.synthetic_scenario_replay')._ReplayComposition",
])
def test_scheduler_import_mutants_in_real_consumers(snippet):
    _, modules, packages = scheduler_sources()
    for relative in ("core/backtest_runner.py", "core/adapters/simulated.py", "strategies/golden_cross.py", "core/data_provider.py"):
        assert scheduler_consumer("python-strategy/src/" + relative)
        path = ROOT / "python-strategy/src" / relative
        source, module = path.read_text(), _module_name(path)
        assert not scheduler_imports(source, module, modules, packages)
        assert scheduler_imports(source + "\n" + snippet + "\n", module, modules, packages)


@pytest.mark.parametrize("relative,snippet", [
    ("core/backtest_runner.py", "from .backtest.synthetic_scenario_replay import _ReplayComposition as R"),
    ("core/adapters/simulated.py", "from ..backtest import synthetic_scenario_replay as r"),
    ("strategies/golden_cross.py", "from ..core.backtest.synthetic_scenario_replay import _ReplayComposition"),
])
def test_relative_scheduler_import_mutants(relative, snippet):
    _, modules, packages = scheduler_sources()
    path = ROOT / "python-strategy/src" / relative
    assert scheduler_consumer(str(path.relative_to(ROOT)))
    assert scheduler_imports(path.read_text() + "\n" + snippet + "\n", _module_name(path), modules, packages)
