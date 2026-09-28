"""Independent scheduled literals and complete detached DTO proof."""

import ast
from hashlib import sha256
from pathlib import Path

import pytest

from src.core.backtest import spider_run_completion_schema as completion
from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_scenario_plan_scheduled as scheduled
from src.core.backtest.spider_run_artifacts import canonical_bytes


def test_plan_independent_canonical_vector():
    value = scheduled.detached_bundle()
    raw = canonical_bytes(value["plan"])
    assert len(raw) == 1451
    assert sha256(raw).hexdigest() == value["plan_sha256"] == "8fb335d6a98afb6bb08fa837386347c4db90f0b00e8661e305599618f05f1520"


def test_complete_structural_validation_and_run_identity_exclusion():
    value = scheduled.detached_bundle()
    for row in [*value["journal"], value["endpoint"], *value["report"]]:
        assert "run_id" not in row
        row["run_id"] = "contract-proof"
    envelope.journal(value["journal"])
    envelope.endpoint(value["endpoint"])
    completion.report(value["report"])
    assert b"contract-proof" not in scheduled.BUNDLE_BYTES
    assert b'"run_id"' not in scheduled.BUNDLE_BYTES


def test_snapshot_payloads_are_complete_literal_values():
    value = scheduled.detached_bundle()
    owners = [value["endpoint"]["initial_owner_evidence"],
              *[row["payload"]["owner_evidence_after"] for row in value["journal"][:3]], value["endpoint"]["final_owner_evidence"]]
    for index, owner in enumerate(owners):
        initial = index == 0
        assert owner["cutoff"] == (500, 501, 502, 503, 504)[index]
        inspection = owner["inspection"]
        assert (inspection["account_version"], inspection["cash"], inspection["gross_realized"], inspection["total_fees"]) == (
            (0, 1, 1, 2, 2)[index], "1000" if initial else "990", "0", "0" if initial else "10")
        assert inspection["account_key"] == dict(venue="okx-scenario", environment="test", account="A")
        assert (inspection["profile_id"], inspection["config_id"], inspection["gate"], inspection["lifecycle"]) == (
            "SYNTHETIC_MIN_CASH_V1", "scenario-v1", "RUNNING", "RISK_STABLE")
        payloads = [dict(outcome="SUCCESS", equity="1000" if initial else "990", available_equity="-10" if initial else "990"),
                    dict(outcome="SUCCESS", rows=[dict(product_id="BTC-USDT-SWAP", margin_mode="cross", position_contracts="20",
                                                      last_price="50000", notional_usd="10000")] if initial else []),
                    dict(outcome="SUCCESS", rows=[dict(order_id="MIN-O1", client_order_id="MIN-C1", product_id="BTC-USDT-SWAP", state="live",
                                                      side="sell", limit_price="50000", original_size_contracts="20", cumulative_filled_size_contracts="0", created_at=500)] if initial else [])]
        for kind, payload in zip(["TRADING", "POSITIONS", "OPEN_ORDERS"], payloads, strict=True):
            identity = "S6-N-" + ["INITIAL", "POST-MIN-X1", "POST-reject", "POST-context", "FINAL"][index] + "-" + kind.replace("_", "-")
            assert owner[kind.lower() + "_request"] == dict(schema_version="snapshot_request_v1", account_key=inspection["account_key"],
                                                          snapshot_id=identity, snapshot_kind=kind, capture_mode="OWNER_CURRENT", captured_at=500 + index)
            fact = owner[kind.lower() + "_fact"]
            assert fact["immutable_payload"] == payload
            assert fact["reference"] == dict(namespace="SNAPSHOT", fact_id=identity)
            assert (fact["snapshot_kind"], fact["snapshot_as_of"], fact["captured_account_version"]) == (kind, 500 + index, (0, 1, 1, 2, 2)[index])
    for index in range(3):
        assert value["journal"][index]["payload"]["owner_evidence_before"] == owners[index]


def test_source_member_payloads_and_results():
    rows = scheduled.detached_bundle()["journal"][:3]
    assert rows[0]["payload"]["request"]["members"][0]["payload"] == dict(
        namespace="synthetic-v1", product_id="BTC-USDT-SWAP", external_execution_id="MIN-X1", order_id="MIN-O1", side="SHORT",
        price="50000", quantity_contracts="20", liquidity="SYNTHETIC_TAKER", matching_effective_at=501, candidate_id="candidate-MIN-X1",
        source_id="source-MIN-X1", visible_at=501, expected_account_version=0, expected_order_version=0, spec_version="spec-v1", rule_data_version="tier-v1")
    assert rows[1]["payload"]["request"]["members"][0]["payload"] == dict(intent_id="MIN-NEGATIVE-I", client_order_id="MIN-NEGATIVE-C",
        config_id="scenario-v1", product_id="BTC-USDT-SWAP", strategy_id="min-cash-policy", side="LONG", order_type="LIMIT",
        quantity_contracts="1", limit_price="50000", reduce_only=False, requested_at=502)
    assert rows[2]["payload"]["request"]["members"][0]["payload"] == dict(
        expected_before="a130fcbba43b12aad016ffdd1e09f250658721a313fceb909c6a40750938c357",
        expected_after="1ea48d053febc00bb7b39f380dbde9dc5e486607bb92286e345df3408a222195",
        rows=[dict(product_id="BTC-USDT-SWAP", mark="50000", valid_from=0, valid_to=3000),
              dict(product_id="ETH-USDT-SWAP", mark="101", valid_from=503, valid_to=3000)])
    for index, row in enumerate(rows):
        request, result = row["payload"]["request"], row["payload"]["result"]
        assert request["account_key"] == dict(venue="okx-scenario", environment="test", account="A")
        assert result["group_id"] == ["MIN-X1", "reject", "context"][index]
        assert request["members"][0]["stamp"] == dict(event_id=["MIN-X1", "reject", "context"][index], effective_at=501 + index,
            causal_parent_ids=[], ordering_contract_id="S_order_v1", scenario_ordinal=[30, 60, 20][index])
        assert (result["classification"], result["account_version_before"], result["account_version_after"], result["gate_after"], result["lifecycle_after"]) == (
            ["COMMITTED", "REJECTED", "COMMITTED"][index], [0, 1, 1][index], [1, 1, 2][index], "RUNNING", "RISK_STABLE")
        assert result["committed_references"] == ([] if index == 1 else [dict(namespace="SOURCE", fact_id=request["group_id"])])
        assert result["rejections"] == ([dict(event_id="reject", reason="MIN_CASH")] if index == 1 else [])
        assert result["group_digest"] == scheduled.detached_bundle()["plan"]["source_groups"][index]["group_sha256"]
        assert result["owner_state_digest"] == row["payload"]["owner_evidence_after"]["inspection"]["owner_state_digest"]
        assert (request["group_id"], request["group_effective_at"], request["declared_member_count"], request["ordering_contract_id"]) == (["MIN-X1", "reject", "context"][index], 501 + index, 1, "S_order_v1")


def test_delivery_callback_scheduler_and_report_full_payloads():
    value = scheduled.detached_bundle()
    rows, endpoint = value["journal"], value["endpoint"]
    delivery = rows[3]["payload"]["delivery"]
    assert set(endpoint) == {"schema_version", "terminal_reason", "cutoff", "initial_owner_evidence", "final_owner_evidence", "scheduler_observation", "remaining_planned_barriers"}
    assert (endpoint["terminal_reason"], endpoint["remaining_planned_barriers"]) == ("SCHEDULED_MTM", [])
    assert delivery["account_key"] == dict(venue="okx-scenario", environment="test", account="A")
    assert delivery["immutable_payload"] == dict(order_id="MIN-O1", owner_client_order_id="MIN-C1", policy_client_order_id="MIN-C1",
        product_id="BTC-USDT-SWAP", state="filled", side="sell", limit_price="50000", fill_price="50000", original_size_contracts="20",
        cumulative_filled_size_contracts="20", contract_value="0.01", execution_effective_at=501, commit_account_version=1,
        spec_version="spec-v1", rule_data_version="tier-v1")
    assert (delivery["source_namespace"], delivery["source_fact_id"], delivery["payload_kind"], delivery["occurrence_index"], delivery["schedule_sequence"], delivery["visible_at"]) == ("SOURCE", "MIN-X1", "EXECUTION_FACT", 0, 0, 504)
    identity = "31529036c2549db7175401337d24abb78e3827841ef263fa7e6d4060f22a8b50"
    assert delivery["delivery_id"] == identity
    assert rows[4]["payload"] == dict(delivery_id=identity, outcome="SUCCESS", policy_events=[], actions=[], failure=None)
    for i, row in enumerate(rows):
        assert (row["journal_seq"], row["effective_at"], row["visible_at"]) == (i + 1, [501, 502, 503, 501, 504][i], [501, 502, 503, 504, 504][i])
        assert row["causal_parent_ids"] == [[], [], [], ["SOURCE:MIN-X1"], ["DELIVERY_ATTEMPT:" + identity]][i]
        assert row["scheduler_key"] == dict(visible_at=[501, 502, 503, 504, 504][i], queue_class="SOURCE_GROUP" if i < 3 else "DELIVERY", schedule_sequence=0, stable_id=["MIN-X1", "reject", "context", identity, identity][i])
        assert (row["account_version_before"], row["account_version_after"]) == [(0, 1), (1, 1), (1, 2), (None, None), (None, None)][i]
        assert (row["barrier_id"], row["record_kind"]) == (value["plan"]["planned_coverage"][i]["barrier_id"], value["plan"]["planned_coverage"][i]["record_kind"])
    observation = endpoint["scheduler_observation"]
    assert observation == dict(current_time=504, last_popped=rows[3]["scheduler_key"], gate="RUNNING", terminal=None, pending_keys=[], polls=[], callback_actions=[],
        records=[dict(key=rows[i]["scheduler_key"], kind=rows[i]["scheduler_key"]["queue_class"], stable_id=rows[i]["scheduler_key"]["stable_id"], classification="SUCCESS") for i in [0, 2, 1, 3]])
    assert endpoint["cutoff"] == dict(scheduler_time=504, persisted_boundary=dict(ordinal=5, barrier_id="CALLBACK_RESULT:" + identity, journal_seq=5))
    for row, product in zip(value["report"], ["BTC-USDT-SWAP", "ETH-USDT-SWAP"], strict=True):
        assert row == dict(schema_version="spider_product_report_v1", product_id=product, terminal_reason="SCHEDULED_MTM", position_contracts="0",
            mark_price=None, notional_usd="0", open_orders=[], committed_execution_refs=["SOURCE:MIN-X1"] if product.startswith("BTC") else [],
            account_cash="990", account_equity="990", account_available_equity="990", account_gross_realized="0", account_total_fees="10",
            source_evidence_refs=(["journal:1"] if product.startswith("BTC") else []) + ["artifact:endpoint.json#/final_owner_evidence"])


def test_detached_copy_and_nested_alias_isolation():
    original = scheduled.BUNDLE_BYTES
    value = scheduled.detached_bundle()
    value["journal"][0]["payload"]["owner_evidence_after"]["inspection"]["cash"] = "123"
    assert value["journal"][1]["payload"]["owner_evidence_before"]["inspection"]["cash"] == "990"
    value["endpoint"]["initial_owner_evidence"]["trading_fact"]["immutable_payload"]["equity"] = "0"
    value["report"].clear()
    assert scheduled.BUNDLE_BYTES == original
    assert canonical_bytes(scheduled.detached_bundle()) == original
    with pytest.raises(TypeError):
        scheduled.BUNDLE_BYTES[0] = 0  # type: ignore[index]


def test_independent_native_digest_vectors():
    value = scheduled.detached_bundle()
    rows = value["journal"]
    owners = [value["endpoint"]["initial_owner_evidence"], *[row["payload"]["owner_evidence_after"] for row in rows[:3]], value["endpoint"]["final_owner_evidence"]]
    assert [owner["inspection"]["owner_state_digest"] for owner in owners] == [
        "bdc29d7d956bd583687a23d1c96be72086bea62e1a28753424b5e41eee69a565", "f2510544e5b294b8f6be63d9f37a7dc3937b08c8423fa2f26db40f16139e9cce",
        "6e7b4cfce22d18114c24e879a6dbd1cf6ab6e9e8881d803d2b656897f9b9e445", "0666b600cc7c6fd0e34d9b5ec3c6d00a99e2376b9595a646dc6e7b9809085b27", "0666b600cc7c6fd0e34d9b5ec3c6d00a99e2376b9595a646dc6e7b9809085b27"]
    assert [row["payload"]["result"]["result_digest"] for row in rows[:3]] == ["fc0adad1af0e086c166a9bb7956d9684703c0c5c94a14346553a8f9bafc1ef36", "74f1e8289f4c0dea5e759c54b0728f755e3e38915d38edf88c9bf8339e553360", "945f951fa2dd75938d68f14a1e75ed744998a479e25368b3e6862919f7372acc"]
    for index, owner in enumerate(owners):
        assert [owner["inspection"][key] for key in ["positions_digest", "orders_digest", "reservations_digest"]] == (
            ["25c5000da8bae9ae0e3cb00a075f861e2c34fc6d82793f443ecada5316ed9da9", "759215981bb201ecbc5aa8fb904f83fd8bea7862171e24bab1ba41fe4512eb27", "91cd431d81d6ba51eee4f780d61a51679e1e4cb3bd2f2cc16842cdc0ff5d30bd"] if index == 0 else
            ["4fe4cb189a4d0af356f106da00680f149a13984cd7c7073a03357a222962f4da", "7d9d0890d78e5ebf2ad0b567673b545c99e527841495315e3034b8adef622b3f", "391be9ceb2f10246d117c65a21526273ecf4dfe4be561af665946c1f3a7a26f0"])
        assert owner["inspection"]["valuation_context_id"] == ("a130fcbba43b12aad016ffdd1e09f250658721a313fceb909c6a40750938c357" if index < 3 else "1ea48d053febc00bb7b39f380dbde9dc5e486607bb92286e345df3408a222195")
    expected = [
        ("338c9182ff29bf91e94f529768edcec55945d57b0cbf74537bdfedd76168a0c0", "38db446ed330e457d0e1d052cb5ff936c70d4b3587a337d1687ee48efed3ef25"),
        ("a06decdc30797fdb56e246f9cd02060a3de669d1ed5ca349c8d493686af8f901", "f1d22d675452ec9146c366dd514181726cc38817b364c5d2ad588ed8ce1a47a1"),
        ("d945afff6bc1175cc08fe41e8fc9f35c1d40bd67fda10c8a43d4ba144fb8f736", "71e74fceddc3abc910a6d8a93ac1c0976896fdc03e1a0a142007457f774832cd"),
        ("340811f1039c6ee64d79f74a1cd761206b7505f22bab664676d4dab00c9ef7aa", "ff36226475e73bb42fd402113bdb4bb3bfe26c8b9d99e9eb1962ec4c7e219e41"),
        ("d669a420cbdc5a7e8a3d3a8d085df4e1277d10b2d01426a9c8e8a810b65930d4", "fdf986711f1249bfbc5d8dcf8a0f99ced92856331e0409235c4b773adb33e451"),
        ("54abed9022fd70a5dd6df222faa355fbb2c8282d517aa8ef15b2dc1191b3e113", "750eff037bf993d7324a9ab783807e07f2a644b072fb8cde66a507b2469de5ae"),
        ("e2ab929a9b62493a8d75da57e27ddbe12b66a3348f5feb4252ad1ba38d3fdfc6", "df8a08774d477d9d20fe7e4b780aa4419248f411c5172469ca364a232da2fa55"),
        ("58ccc6710b2d8f05f14eb23cda553275b6372f9de0d392e6612e1fe7db53eae8", "9472e7ad5cc09f27b406f54cdddd5b6957fe82f426c8e21dd1a2605468bf2cf7"),
        ("81f07c3bc62bebfb4ddb24f1205cc0d35b9bf6ec09e7686b160430b495d32e9f", "3187e0e8b9a276a8ff5086033532a15e4b39b80209059d459726abdb6be777f2"),
        ("a694eaa3617f529ba368158df980927e8638b72bc60821b2e5dfbe8a7c0a9354", "82aa1dc1960c6e953e68b837dfe88af245faeb8d6db2df9ad29036943db274b1"),
        ("ed5b2073a7c175ced16ea6f5b05ebf5e52902b76d57add93abb52d9cc40c4756", "e8429a68e87425fb10d6fc614170ae660fa3c6f5e2b59ce904d763b9a2af0084"),
        ("3246c6d13033a5a2f91b57c7e38c18b3997a3b87f86621c969b1bdfeadcbb337", "4834eefd8dcf9a73756d1023595d0599631cfcdbd3ab76700adab7bcbf12d7af"),
        ("91b8e2a384002fe65e1830a0ad9d474e6fb22380d3c7d116e05a89320554a41b", "ea6ac2535aa6734483fd1672c99d826a9657f838423ddb1d5beb1ae699b21332"),
        ("d83d8bc2b3f8e716451d656ba5d3eb4f6f0889cf1e6c71489de857f4b2577aa9", "d788f869ccaf759ca653719c0bbae82ec35383614086a6cc2eb3c761d5bddf2d"),
        ("ea0e41f5755589424f70432631560266f79f8a4187b66ef3b53e6cd52cb287bf", "2da41a710531e5ceb01b3f2fea7d96a8c39f1ae53dd2679f161581326323acd7"),
    ]
    assert [(owner[kind + "_fact"]["request_digest"], owner[kind + "_fact"]["payload_digest"]) for owner in owners for kind in ["trading", "positions", "open_orders"]] == expected
    assert rows[3]["payload"]["delivery"]["payload_digest"] == "c9bd8e77a7eb523da03fdd1395d09225d1e60683dc8c0c95c1553b8f195a77de"
    assert rows[3]["payload"]["emission_plan_digest"] == "87a8b8ed07e10cb842391b59a71890b8d453e0a1f070dcde844c8c61c9402e3f"


def test_import_boundary():
    tree = ast.parse(Path(scheduled.__file__).read_text())
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    assert {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)} == {"typing", "src.core.backtest.spider_run_artifacts"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and
                   node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))
