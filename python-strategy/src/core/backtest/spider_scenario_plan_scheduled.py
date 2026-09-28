"""Frozen scheduled-MTM literals; no run identity, runtime or evidence logic."""

from typing import Any, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

PLAN_SHA256 = "8fb335d6a98afb6bb08fa837386347c4db90f0b00e8661e305599618f05f1520"
_DELIVERY = "31529036c2549db7175401337d24abb78e3827841ef263fa7e6d4060f22a8b50"
_EMISSION = "87a8b8ed07e10cb842391b59a71890b8d453e0a1f070dcde844c8c61c9402e3f"
_CONTEXTS = ("a130fcbba43b12aad016ffdd1e09f250658721a313fceb909c6a40750938c357",
             "1ea48d053febc00bb7b39f380dbde9dc5e486607bb92286e345df3408a222195")
_OWNERS = (
    "bdc29d7d956bd583687a23d1c96be72086bea62e1a28753424b5e41eee69a565",
    "f2510544e5b294b8f6be63d9f37a7dc3937b08c8423fa2f26db40f16139e9cce",
    "6e7b4cfce22d18114c24e879a6dbd1cf6ab6e9e8881d803d2b656897f9b9e445",
    "0666b600cc7c6fd0e34d9b5ec3c6d00a99e2376b9595a646dc6e7b9809085b27",
    "0666b600cc7c6fd0e34d9b5ec3c6d00a99e2376b9595a646dc6e7b9809085b27",
)
_COMPONENTS = (
    ("25c5000da8bae9ae0e3cb00a075f861e2c34fc6d82793f443ecada5316ed9da9", "759215981bb201ecbc5aa8fb904f83fd8bea7862171e24bab1ba41fe4512eb27", "91cd431d81d6ba51eee4f780d61a51679e1e4cb3bd2f2cc16842cdc0ff5d30bd"),
    ("4fe4cb189a4d0af356f106da00680f149a13984cd7c7073a03357a222962f4da", "7d9d0890d78e5ebf2ad0b567673b545c99e527841495315e3034b8adef622b3f", "391be9ceb2f10246d117c65a21526273ecf4dfe4be561af665946c1f3a7a26f0"),
)
_GROUP_HASHES = (
    "9184aec115b1fc9ab81b18fb9bec0489bfa3b174e9a8af33fb8c9bc522dc2560",
    "c68550be29764948632d744abbf4502974c010a9edac18406c1e4c4a7abedaff",
    "dd7f3ad9e7d1c1bcb49b45ea0c664b59f714f2d17bb487d2033bcd1b1a175eb9",
)
_RESULT_HASHES = (
    "fc0adad1af0e086c166a9bb7956d9684703c0c5c94a14346553a8f9bafc1ef36",
    "74f1e8289f4c0dea5e759c54b0728f755e3e38915d38edf88c9bf8339e553360",
    "945f951fa2dd75938d68f14a1e75ed744998a479e25368b3e6862919f7372acc",
)
# Each row is TRADING, POSITIONS, OPEN_ORDERS: request digest then payload digest.
_SNAPSHOTS = (
    (("338c9182ff29bf91e94f529768edcec55945d57b0cbf74537bdfedd76168a0c0", "38db446ed330e457d0e1d052cb5ff936c70d4b3587a337d1687ee48efed3ef25"),
     ("a06decdc30797fdb56e246f9cd02060a3de669d1ed5ca349c8d493686af8f901", "f1d22d675452ec9146c366dd514181726cc38817b364c5d2ad588ed8ce1a47a1"),
     ("d945afff6bc1175cc08fe41e8fc9f35c1d40bd67fda10c8a43d4ba144fb8f736", "71e74fceddc3abc910a6d8a93ac1c0976896fdc03e1a0a142007457f774832cd")),
    (("340811f1039c6ee64d79f74a1cd761206b7505f22bab664676d4dab00c9ef7aa", "ff36226475e73bb42fd402113bdb4bb3bfe26c8b9d99e9eb1962ec4c7e219e41"),
     ("d669a420cbdc5a7e8a3d3a8d085df4e1277d10b2d01426a9c8e8a810b65930d4", "fdf986711f1249bfbc5d8dcf8a0f99ced92856331e0409235c4b773adb33e451"),
     ("54abed9022fd70a5dd6df222faa355fbb2c8282d517aa8ef15b2dc1191b3e113", "750eff037bf993d7324a9ab783807e07f2a644b072fb8cde66a507b2469de5ae")),
    (("e2ab929a9b62493a8d75da57e27ddbe12b66a3348f5feb4252ad1ba38d3fdfc6", "df8a08774d477d9d20fe7e4b780aa4419248f411c5172469ca364a232da2fa55"),
     ("58ccc6710b2d8f05f14eb23cda553275b6372f9de0d392e6612e1fe7db53eae8", "9472e7ad5cc09f27b406f54cdddd5b6957fe82f426c8e21dd1a2605468bf2cf7"),
     ("81f07c3bc62bebfb4ddb24f1205cc0d35b9bf6ec09e7686b160430b495d32e9f", "3187e0e8b9a276a8ff5086033532a15e4b39b80209059d459726abdb6be777f2")),
    (("a694eaa3617f529ba368158df980927e8638b72bc60821b2e5dfbe8a7c0a9354", "82aa1dc1960c6e953e68b837dfe88af245faeb8d6db2df9ad29036943db274b1"),
     ("ed5b2073a7c175ced16ea6f5b05ebf5e52902b76d57add93abb52d9cc40c4756", "e8429a68e87425fb10d6fc614170ae660fa3c6f5e2b59ce904d763b9a2af0084"),
     ("3246c6d13033a5a2f91b57c7e38c18b3997a3b87f86621c969b1bdfeadcbb337", "4834eefd8dcf9a73756d1023595d0599631cfcdbd3ab76700adab7bcbf12d7af")),
    (("91b8e2a384002fe65e1830a0ad9d474e6fb22380d3c7d116e05a89320554a41b", "ea6ac2535aa6734483fd1672c99d826a9657f838423ddb1d5beb1ae699b21332"),
     ("d83d8bc2b3f8e716451d656ba5d3eb4f6f0889cf1e6c71489de857f4b2577aa9", "d788f869ccaf759ca653719c0bbae82ec35383614086a6cc2eb3c761d5bddf2d"),
     ("ea0e41f5755589424f70432631560266f79f8a4187b66ef3b53e6cd52cb287bf", "2da41a710531e5ceb01b3f2fea7d96a8c39f1ae53dd2679f161581326323acd7")),
)


def _account() -> dict[str, str]:
    return dict(venue="okx-scenario", environment="test", account="A")


def _owners() -> list[dict[str, Any]]:
    stages = ("INITIAL", "POST-MIN-X1", "POST-reject", "POST-context", "FINAL")
    owners = []
    for index, (stage, at, version) in enumerate(zip(stages, (500, 501, 502, 503, 504), (0, 1, 1, 2, 2), strict=True)):
        initial = index == 0
        cash, fees, available = ("1000", "0", "-10") if initial else ("990", "10", "990")
        components = _COMPONENTS[0 if initial else 1]
        inspection = dict(schema_version="inspect_state_v1", account_key=_account(), profile_id="SYNTHETIC_MIN_CASH_V1",
                          config_id="scenario-v1", account_version=version, valuation_context_id=_CONTEXTS[0 if index < 3 else 1],
                          gate="RUNNING", lifecycle="RISK_STABLE", cash=cash, gross_realized="0", total_fees=fees,
                          positions_digest=components[0], orders_digest=components[1], reservations_digest=components[2], owner_state_digest=_OWNERS[index])
        positions = [dict(product_id="BTC-USDT-SWAP", margin_mode="cross", position_contracts="20", last_price="50000", notional_usd="10000")] if initial else []
        orders = [dict(order_id="MIN-O1", client_order_id="MIN-C1", product_id="BTC-USDT-SWAP", state="live", side="sell",
                       limit_price="50000", original_size_contracts="20", cumulative_filled_size_contracts="0", created_at=500)] if initial else []
        owner: dict[str, Any] = dict(cutoff=at, inspection=inspection)
        payloads = (dict(outcome="SUCCESS", equity=cash, available_equity=available),
                    dict(outcome="SUCCESS", rows=positions), dict(outcome="SUCCESS", rows=orders))
        for kind, payload, hashes in zip(("TRADING", "POSITIONS", "OPEN_ORDERS"), payloads, _SNAPSHOTS[index], strict=True):
            identity = f"S6-N-{stage}-{kind.replace('_', '-')}"
            owner[kind.lower() + "_request"] = dict(schema_version="snapshot_request_v1", account_key=_account(), snapshot_id=identity,
                                                    snapshot_kind=kind, capture_mode="OWNER_CURRENT", captured_at=at)
            owner[kind.lower() + "_fact"] = dict(schema_version="snapshot_fact_v1", reference=dict(namespace="SNAPSHOT", fact_id=identity),
                                                 request_digest=hashes[0], snapshot_kind=kind, snapshot_as_of=at, captured_account_version=version,
                                                 immutable_payload=payload, payload_digest=hashes[1])
        owners.append(owner)
    return owners


def _bundle() -> dict[str, Any]:
    owners = _owners()
    names = ("MIN-X1", "reject", "context")
    payloads = (
        dict(namespace="synthetic-v1", product_id="BTC-USDT-SWAP", external_execution_id="MIN-X1", order_id="MIN-O1", side="SHORT",
             price="50000", quantity_contracts="20", liquidity="SYNTHETIC_TAKER", matching_effective_at=501, candidate_id="candidate-MIN-X1",
             source_id="source-MIN-X1", visible_at=501, expected_account_version=0, expected_order_version=0, spec_version="spec-v1", rule_data_version="tier-v1"),
        dict(intent_id="MIN-NEGATIVE-I", client_order_id="MIN-NEGATIVE-C", config_id="scenario-v1", product_id="BTC-USDT-SWAP",
             strategy_id="min-cash-policy", side="LONG", order_type="LIMIT", quantity_contracts="1", limit_price="50000", reduce_only=False, requested_at=502),
        dict(expected_before=_CONTEXTS[0], expected_after=_CONTEXTS[1], rows=[
            dict(product_id="BTC-USDT-SWAP", mark="50000", valid_from=0, valid_to=3000),
            dict(product_id="ETH-USDT-SWAP", mark="101", valid_from=503, valid_to=3000)]),
    )
    journal: list[dict[str, Any]] = []
    for index, (name, at, ordinal, kind) in enumerate(zip(names, (501, 502, 503), (30, 60, 20), ("EXECUTION", "INTENT", "CONTEXT_MARKS"), strict=True)):
        stamp = dict(event_id=name, effective_at=at, causal_parent_ids=[], ordering_contract_id="S_order_v1", scenario_ordinal=ordinal)
        group = dict(schema_version="scenario_group_v1", group_id=name, account_key=_account(), ordering_contract_id="S_order_v1",
                     group_effective_at=at, declared_member_count=1, members=[dict(kind=kind, stamp=stamp, payload=payloads[index])])
        result = dict(schema_version="group_result_v1", group_id=name, classification="REJECTED" if index == 1 else "COMMITTED",
                      committed_references=[] if index == 1 else [dict(namespace="SOURCE", fact_id=name)],
                      rejections=[dict(event_id="reject", reason="MIN_CASH")] if index == 1 else [],
                      account_version_before=(0, 1, 1)[index], account_version_after=(1, 1, 2)[index], gate_after="RUNNING",
                      lifecycle_after="RISK_STABLE", owner_state_digest=_OWNERS[index + 1], result_digest=_RESULT_HASHES[index], group_digest=_GROUP_HASHES[index])
        journal.append(dict(schema_version="spider_journal_record_v1", journal_seq=index + 1, barrier_id="SOURCE_GROUP:" + name,
                            record_kind="SOURCE_GROUP_RESULT", scheduler_key=dict(visible_at=at, queue_class="SOURCE_GROUP", schedule_sequence=0, stable_id=name),
                            causal_parent_ids=[], effective_at=at, visible_at=at, account_version_before=(0, 1, 1)[index], account_version_after=(1, 1, 2)[index],
                            payload=dict(request=group, result=result, owner_evidence_before=owners[index], owner_evidence_after=owners[index + 1])))
    fact = dict(order_id="MIN-O1", owner_client_order_id="MIN-C1", policy_client_order_id="MIN-C1", product_id="BTC-USDT-SWAP",
                state="filled", side="sell", limit_price="50000", fill_price="50000", original_size_contracts="20", cumulative_filled_size_contracts="20",
                contract_value="0.01", execution_effective_at=501, commit_account_version=1, spec_version="spec-v1", rule_data_version="tier-v1")
    delivery = dict(account_key=_account(), delivery_id=_DELIVERY, source_fact_id="MIN-X1", source_namespace="SOURCE", payload_kind="EXECUTION_FACT",
                    occurrence_index=0, schedule_sequence=0, immutable_payload=fact,
                    payload_digest="c9bd8e77a7eb523da03fdd1395d09225d1e60683dc8c0c95c1553b8f195a77de", visible_at=504)
    key = dict(visible_at=504, queue_class="DELIVERY", schedule_sequence=0, stable_id=_DELIVERY)
    for seq, kind, parent, at, payload in (
        (4, "DELIVERY_ATTEMPT", "SOURCE:MIN-X1", 501, dict(delivery=delivery, emission_plan_digest=_EMISSION)),
        (5, "CALLBACK_RESULT", "DELIVERY_ATTEMPT:" + _DELIVERY, 504, dict(delivery_id=_DELIVERY, outcome="SUCCESS", policy_events=[], actions=[], failure=None)),
    ):
        journal.append(dict(schema_version="spider_journal_record_v1", journal_seq=seq, barrier_id=kind + ":" + _DELIVERY, record_kind=kind,
                            scheduler_key=key, causal_parent_ids=[parent], effective_at=at, visible_at=504,
                            account_version_before=None, account_version_after=None, payload=payload))
    coverage = [dict(ordinal=row["journal_seq"], barrier_id=row["barrier_id"], record_kind=row["record_kind"]) for row in journal]
    plan = dict(schema_version="spider_scenario_plan_v1", scenario_plan_id="SPIDER_P1_SCHEDULED_MTM_V1", native_profile="SYNTHETIC_MIN_CASH_V1",
                account_key=_account(), initial_cutoff=500, final_cutoff=504, terminal_policy="SCHEDULED_MTM", planned_coverage=coverage,
                source_groups=[dict(schedule_sequence=0, group_id=name, group_sha256=sha) for name, sha in zip(names, _GROUP_HASHES, strict=True)],
                deliveries=[dict(schedule_sequence=0, delivery_id=_DELIVERY, emission_plan_sha256=_EMISSION)])
    records = [dict(key=row["scheduler_key"], kind=row["scheduler_key"]["queue_class"], stable_id=row["scheduler_key"]["stable_id"], classification="SUCCESS")
               for row in (journal[0], journal[2], journal[1], journal[3])]
    endpoint = dict(schema_version="spider_endpoint_v1", terminal_reason="SCHEDULED_MTM",
                    cutoff=dict(scheduler_time=504, persisted_boundary=dict(ordinal=5, barrier_id=journal[4]["barrier_id"], journal_seq=5)),
                    initial_owner_evidence=owners[0], final_owner_evidence=owners[4], remaining_planned_barriers=[],
                    scheduler_observation=dict(current_time=504, last_popped=key, gate="RUNNING", terminal=None, pending_keys=[], records=records, polls=[], callback_actions=[]))
    reports = [dict(schema_version="spider_product_report_v1", product_id=product, terminal_reason="SCHEDULED_MTM",
                    position_contracts="0", mark_price=None, notional_usd="0", open_orders=[],
                    committed_execution_refs=["SOURCE:MIN-X1"] if product == "BTC-USDT-SWAP" else [], account_cash="990", account_equity="990",
                    account_available_equity="990", account_gross_realized="0", account_total_fees="10",
                    source_evidence_refs=(["journal:1"] if product == "BTC-USDT-SWAP" else []) + ["artifact:endpoint.json#/final_owner_evidence"])
               for product in ("BTC-USDT-SWAP", "ETH-USDT-SWAP")]
    return dict(plan=plan, plan_sha256=PLAN_SHA256, journal=journal, endpoint=endpoint, report=reports)


BUNDLE_BYTES = canonical_bytes(_bundle())


def detached_bundle() -> dict[str, Any]:
    """Return fresh detached literals; callers cannot mutate the frozen bytes."""
    return cast(dict[str, Any], decode_canonical(BUNDLE_BYTES))
