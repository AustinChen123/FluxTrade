"""Independent complete liquidation DTO oracle and retained-digest mutations."""

import ast
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from src.core.backtest import spider_run_completion_schema as completion
from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_scenario_plan_liquidation as liquidation
from src.core.backtest.spider_run_artifacts import canonical_bytes

ACCOUNT = dict(venue="okx-scenario", environment="test", account="A")
GROUP = "S6-L-MARK-1501"
GROUP_HASH = "856b48e28b1e5bedb22d3f13daf62be641f51868919de89a038b371000de20f3"
LIQUIDATION = "7fb9f5bec4fe944a93de3c08e44d149a42279d93ff24c37b7712c0a5037db810"
POLICY = "LEGAL_NATIVE_LIQUIDATION_FINAL_EVENT"
CONTEXTS = ["6158a5ae1d44e8306c56e617550a4cc8d6ad333d7fff2cceb5cdc2f17e7bb892", "dee6233edc4b6aae76b2bcdf27459aa049a6b3aaabc04ff7d2e93618b4af39b6"]
SNAPSHOTS = [
    ("5a37afb42c186e355c0061296dabc028928cb8a3b4d944443645a033dcfaf237", "6b1c05759909cb5877e22c32aadcd0ba1d42d691d72ffe1deeb73709faaf4c88"),
    ("f8a06b4b9570bed66c004efba40f6ae726516409168a33260e0c370c5bd7c85f", "d7d01f06e20507198cc7a9d9453b50c19ed9a0353fc17b32783cdb1db634c64e"),
    ("42fea6c1b4625f044a630f130e86dee199dea390771b0fc99aaa4d0859344261", "67fbb8688e7045ffa93c2699030a9dd9721dd5f62e6a8c52782faeaa85658286"),
    ("649bd73e7acbb95c602ef1e41264d8e90fb702badd6fcaa71ce7df658893b76b", "f998a97f36243ff940c98df9612a2affbcb895853b1fe555d16ce43e4e2ad0e5"),
    ("ba76bb883c006b68633ead56fdfae050facc43d7df802487131ad8d55606d25f", "738629c13d0fb407ae4676d367dbbd4a71d7331df7f19cd5fb0dd1e9658c65c3"),
    ("34455a3c3d7c8b7da76f930a0188ac9d7c89f69268f2f74bd346e14e8562787a", "17b3a684f5b59aee8f0637679d4c642106ef191aead4c9b917609c1793f815bd"),
    ("7c0c67f19ac3562f8e4a8a32df7d7c386fe3f3458207bd478fe74b8d628e5fe9", "0394d5ce083b06d54fc4071b504f58b401222670a054dc6384aa06ffa0fa324c"),
    ("559b66f9cf856778d96ad48566c0177e6df6d9c914e1dfae843d82c7fae1123e", "db2bba4562410f0e93500196f997b559506da4e440d9a01e88c31fe0e2f5b331"),
    ("fb87864aaedbc71e83c4950fdd98cf0d8b17b0ac407c7b07f7e79fcf6fe0bfa2", "c5edcc402c28c938cff279bdaaaa0b353e7032d0bda7b76106aaa95ae93d71dd"),
]


def expected_bundle():
    owners = []
    for index, stage in enumerate(["INITIAL", "POST-S6-L-MARK-1501", "FINAL"]):
        at, version, cash, gross, fees, available, lifecycle = [(1500, 0, "3", "0", "0", "-47", "RISK_STABLE"),
            (1501, 2, "-1.01602", "-1", "3.01602", "-1.01602", "LIQUIDATED_INSOLVENT"), (1501, 2, "-1.01602", "-1", "3.01602", "-1.01602", "LIQUIDATED_INSOLVENT")][index]
        positions, orders, reservations, owner_hash = (
            ["d462ef662ec7afee486297f6763adffa20690a92e92290e88f96469398980e9d", "cf125b37f9df463dfe002fede0c5f8dab40b7848f549ba0232309cc11b98daeb", "4695ea99e32a159d3bc5d9dddd3b6071f5301617fdb256fab6356d93b48bdffe", "134b24a87d3490abca366cadb5e081d698562474c3285e7b4e1393300c4da94d"] if index == 0 else
            ["4fe4cb189a4d0af356f106da00680f149a13984cd7c7073a03357a222962f4da", "cf125b37f9df463dfe002fede0c5f8dab40b7848f549ba0232309cc11b98daeb", "d03759993b78ed62d75ab18ce9b691cb0eee02b374fafe4df1292704f1e21a31", "542cb313c7e688a6107f408aa24414041cd4321bd7e1c75d26b1d2d7e9277fb4"])
        owner = dict(cutoff=at, inspection=dict(schema_version="inspect_state_v1", account_key=ACCOUNT, profile_id="SYNTHETIC_P1_LIQUIDATION_V1",
            config_id="scenario-v1", account_version=version, valuation_context_id=CONTEXTS[0 if index == 0 else 1], gate="RUNNING", lifecycle=lifecycle,
            cash=cash, gross_realized=gross, total_fees=fees, positions_digest=positions, orders_digest=orders, reservations_digest=reservations, owner_state_digest=owner_hash))
        payloads = [dict(outcome="SUCCESS", equity=cash, available_equity=available), dict(outcome="SUCCESS", rows=[
            dict(product_id="BTC-USDT-SWAP", margin_mode="cross", position_contracts="-1", last_price="50000", notional_usd="500")] if index == 0 else []), dict(outcome="SUCCESS", rows=[])]
        for offset, kind in enumerate(["TRADING", "POSITIONS", "OPEN_ORDERS"]):
            name = f"S6-L-{stage}-{kind.replace('_', '-')}"
            request_hash, payload_hash = SNAPSHOTS[index * 3 + offset]
            owner[kind.lower() + "_request"] = dict(schema_version="snapshot_request_v1", account_key=ACCOUNT, snapshot_id=name, snapshot_kind=kind, capture_mode="OWNER_CURRENT", captured_at=at)
            owner[kind.lower() + "_fact"] = dict(schema_version="snapshot_fact_v1", reference=dict(namespace="SNAPSHOT", fact_id=name), request_digest=request_hash,
                snapshot_kind=kind, snapshot_as_of=at, captured_account_version=version, immutable_payload=payloads[offset], payload_digest=payload_hash)
        owners.append(owner)
    coverage = [dict(ordinal=1, barrier_id="SOURCE_GROUP:S6-L-MARK-1501", record_kind="SOURCE_GROUP_RESULT")]
    plan = dict(schema_version="spider_scenario_plan_v1", scenario_plan_id="SPIDER_P1_LEGAL_LIQUIDATION_V1", native_profile="SYNTHETIC_P1_LIQUIDATION_V1", account_key=ACCOUNT,
        initial_cutoff=1500, final_cutoff=1501, terminal_policy=POLICY, planned_coverage=coverage, deliveries=[], source_groups=[dict(schedule_sequence=0, group_id=GROUP, group_sha256=GROUP_HASH)])
    request = dict(schema_version="scenario_group_v1", group_id=GROUP, account_key=ACCOUNT, ordering_contract_id="S_order_v1", group_effective_at=1501, declared_member_count=1,
        members=[dict(kind="CONTEXT_MARKS", stamp=dict(event_id=GROUP, effective_at=1501, causal_parent_ids=[], ordering_contract_id="S_order_v1", scenario_ordinal=20),
                      payload=dict(expected_before=CONTEXTS[0], expected_after=CONTEXTS[1], rows=[dict(product_id="BTC-USDT-SWAP", mark="50100", valid_from=1501, valid_to=3000), dict(product_id="ETH-USDT-SWAP", mark="1900", valid_from=0, valid_to=3000)]))])
    result = dict(schema_version="group_result_v1", group_id=GROUP, classification="COMMITTED", account_version_before=0, account_version_after=2, gate_after="RUNNING", lifecycle_after="LIQUIDATED_INSOLVENT",
        committed_references=[dict(namespace="SOURCE", fact_id=GROUP), dict(namespace="LIQUIDATION", fact_id=LIQUIDATION)], rejections=[], group_digest=GROUP_HASH,
        result_digest="b2ae5e8b6f92947b1938e4b78760d90c152884f19d5ce9203ab227412fe8acf1", owner_state_digest="542cb313c7e688a6107f408aa24414041cd4321bd7e1c75d26b1d2d7e9277fb4")
    key = dict(visible_at=1501, queue_class="SOURCE_GROUP", schedule_sequence=0, stable_id=GROUP)
    journal = [dict(schema_version="spider_journal_record_v1", journal_seq=1, barrier_id="SOURCE_GROUP:S6-L-MARK-1501", record_kind="SOURCE_GROUP_RESULT", scheduler_key=key,
        causal_parent_ids=[], effective_at=1501, visible_at=1501, account_version_before=0, account_version_after=2, payload=dict(request=request, result=result, owner_evidence_before=owners[0], owner_evidence_after=owners[1]))]
    endpoint = dict(schema_version="spider_endpoint_v1", terminal_reason=POLICY, remaining_planned_barriers=[], initial_owner_evidence=owners[0], final_owner_evidence=owners[2],
        cutoff=dict(scheduler_time=1501, persisted_boundary=dict(ordinal=1, barrier_id="SOURCE_GROUP:S6-L-MARK-1501", journal_seq=1)),
        scheduler_observation=dict(current_time=1501, last_popped=key, gate="RUNNING", terminal=None, pending_keys=[], polls=[], callback_actions=[], records=[dict(key=key, kind="SOURCE_GROUP", stable_id=GROUP, classification="SUCCESS")]))
    reports = [dict(schema_version="spider_product_report_v1", product_id=product, terminal_reason=POLICY, position_contracts="0", mark_price=None, notional_usd="0", open_orders=[],
        account_cash="-1.01602", account_equity="-1.01602", account_available_equity="-1.01602", account_gross_realized="-1", account_total_fees="3.01602",
        committed_execution_refs=refs, source_evidence_refs=evidence) for product, refs, evidence in [
            ("BTC-USDT-SWAP", ["SOURCE:S6-L-MARK-1501", "LIQUIDATION:" + LIQUIDATION], ["journal:1", "artifact:endpoint.json#/final_owner_evidence"]),
            ("ETH-USDT-SWAP", [], ["artifact:endpoint.json#/final_owner_evidence"])]]
    return dict(plan=plan, plan_sha256="4e149110d228fc7422b6149efe31de1dc036eabf648340049ca95ce12dda2512", journal=journal, endpoint=endpoint, report=reports)


def test_complete_literal_equality_and_canonical_plan():
    actual = liquidation.detached_bundle()
    assert actual == expected_bundle()
    assert len(canonical_bytes(actual["plan"])) == 598
    assert sha256(canonical_bytes(actual["plan"])).hexdigest() == "4e149110d228fc7422b6149efe31de1dc036eabf648340049ca95ce12dda2512"


def test_full_structures_and_no_run_identity():
    value = liquidation.detached_bundle()
    for row in [*value["journal"], value["endpoint"], *value["report"]]:
        assert "run_id" not in row
        row["run_id"] = "contract-proof"
    envelope.journal(value["journal"])
    envelope.endpoint(value["endpoint"])
    completion.report(value["report"])
    assert b"contract-proof" not in liquidation.BUNDLE_BYTES and b'"run_id"' not in liquidation.BUNDLE_BYTES


def paths(value, path=()):
    if isinstance(value, dict):
        for key, item in value.items():
            yield path + (key,)
            yield from paths(item, path + (key,))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield path + (index,)
            yield from paths(item, path + (index,))


def test_every_field_retained_digest_mutations(monkeypatch):
    original = liquidation.detached_bundle()
    for path in paths(original):
        changed = deepcopy(original)
        parent: Any = changed
        for key in path[:-1]:
            parent = parent[key]
        old = parent[path[-1]]
        parent[path[-1]] = old + 1 if type(old) is int else "99"
        monkeypatch.setattr(liquidation, "BUNDLE_BYTES", canonical_bytes(changed))
        with pytest.raises(AssertionError):
            test_complete_literal_equality_and_canonical_plan()
        del parent[path[-1]]
        monkeypatch.setattr(liquidation, "BUNDLE_BYTES", canonical_bytes(changed))
        with pytest.raises(AssertionError):
            test_complete_literal_equality_and_canonical_plan()


@pytest.mark.parametrize("case", ["terminal", "remaining", "report_order"])
def test_terminal_remaining_and_order_retained_digest_mutations(monkeypatch, case):
    value = liquidation.detached_bundle()
    if case == "terminal":
        value["endpoint"]["terminal_reason"] = "SCHEDULED_MTM"
    elif case == "remaining":
        value["endpoint"]["remaining_planned_barriers"] = [dict(ordinal=2, barrier_id="SOURCE_GROUP:extra", record_kind="SOURCE_GROUP_RESULT")]
    else:
        value["report"].reverse()
    monkeypatch.setattr(liquidation, "BUNDLE_BYTES", canonical_bytes(value))
    with pytest.raises(AssertionError):
        test_complete_literal_equality_and_canonical_plan()


def test_alias_isolation_and_import_boundary():
    original = liquidation.BUNDLE_BYTES
    value = liquidation.detached_bundle()
    value["endpoint"]["initial_owner_evidence"]["inspection"]["cash"] = "99"
    assert value["journal"][0]["payload"]["owner_evidence_before"]["inspection"]["cash"] == "3"
    assert canonical_bytes(liquidation.detached_bundle()) == original
    tree = ast.parse(Path(liquidation.__file__).read_text())
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    assert {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)} == {"typing", "src.core.backtest.spider_run_artifacts"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))
