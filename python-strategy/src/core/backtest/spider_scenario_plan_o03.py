"""Immutable test-only O03 literals; no selector or runtime behavior."""

from typing import Any, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

PLAN_SHA256 = "439906a52cba32b5d1d1bcc1cbd4439270aad175f58a118568fcb754dd42eef8"
_CONTEXT = "e85635423876fd5d56120d49804e26d3c85f8058788128ea978922d9cfe28711"
_NAMES = ("O03-X1", "O03-C1", "O03-C2-REJECT", "O03-N1")
_VECTORS = (
    ("program", "a73fc14a6bafb5762e1254f4331fd13b621bad66cdaf669c1af98f3b4fc96efd"),
    ("seed_execution_id", "7c61f0cf501f488d599eff50e62ff380b5262d43f6272845dd7276786793be28"),
    ("execution_id", "5acc68b3a65fc9131c3201c0e3e9fab83ca7d561253420c62a2d146d71099b97"),
    ("execution_financial_digest", "badfc3922598dd6942b3eedda1d1cb8f37345486b5a476ada08bd47b8b543ddb"),
    ("intent_financial_digest", "0adf83c147bd79850f89d0e5c04262d61bb9f86f75a31feceafd36df6a562619"),
    ("effect_action_id", "73b4d337d0b3177ba4d366e0c8142a967fca8e535682e90dd2560957a9c55e14"),
    ("effect_action_digest", "25598fc69ae1c2ef2ce7ee502639961d117a9679c8f26526fb8b0ad9b5061b34"),
    ("effect_batch_digest", "98c23d2348ea8bba857c63039765c2d7cb43f5c8bfbfb8d057841327cf38b5ec"),
    ("request_old1_id", "387528c8abf3bb002a091e59d6a6c1bbec94987bd068d587957c1fec4a1068b3"),
    ("request_old1_digest", "5fe1e935743af4b03200e7184bb9ca737ea556b6d72165d3eab903f75ac34284"),
    ("request_old2_id", "00ff9bd57fc4edaf611a3d2384719c6dd647caf311b089bb3ff6c5e3a7f1890f"),
    ("request_old2_digest", "0913b9a488c6a790297675cc8d6c4826cdb6ecb0a240e91d0daa20a82f7cfff2"),
    ("request_batch_digest", "52db88f274da4b4921481fc7bdbddcedf97573ba225fca61739fe2e2e88a7e6d"),
)
_GROUPS = (
    ("2436b0d8c2e31fcccba0ca7172b8db7b9f1575da6b6778342a4f6c4aeb0d128e", "59422ea359e7e8f18c23c8e50295cfd2bdd4bbe9152fdf21843a848459660fcc"),
    ("c7b6766c8a6501b7774ff7798c8091b11a1c9146c2b0863260f3a62695ca4f6b", "216e5886537456bec2d3e292164bb2152c32fc4c3958a96efcad2e0808e4b7d9"),
    ("a84a3f62173a2d9f96e54cf7a8b0bfb7eb00cdea0d3f021048b6ba254aa2a840", "5bc75c1382e8670873b3173e4dc622df897d36ec2e93faeb32cd6252b3b72c87"),
    ("1d1c6256eb808be746388dc965a51916d75d8125789032d976d67a721efd70b4", "d0fa89e95fe94162b9ab4633f2a779c10d845dfd51c512f9419e9aeee4da4ab2"),
)
_OWNERS = (
    ("84d7b457f8b08fda32c4f8d46da551029d613fbd496be01e54a33a5a65114a98", "d6f4a3e706a352ad3bb109a0ee8e9913ee4709f4984f68ceec45798b0e7d5619", "50e7b62e5288e6c4b174a8fcbc9b8e303c2e884de4fcb717dc00672a945b43a9", "374bc18e60e94c6a000ab3da6e06d2f2d97742873826b14f024b067e3eee0053"),
    ("521f708b90b8b57ee06fdc7f2e31717ac4c36aa27d4f619820cc52b044af6168", "0edd11fcf178d00ff496623e6185e4ca903647c6431f1aacfb636263262a630c", "cb64e6b9ec6d84467178e96885a45dae7f275f3f2215ce142c426c8f341c15a9", "96314943a01b7a5ea0bc0179d11b92a95fe81b6a89d019c41d873be52f77c808"),
    ("521f708b90b8b57ee06fdc7f2e31717ac4c36aa27d4f619820cc52b044af6168", "1cd1d5c2b719b6291d43625a2e7d847afb7d15c77f2761f0162942e0c8c354b7", "903dcb37c15a7987606c6bc529398eeef93d0860cf04337c22a684e0b442a9c7", "40216c2d057a3c3da364c3673f06c80797b34aeece6174883427e000406bb8e3"),
    ("521f708b90b8b57ee06fdc7f2e31717ac4c36aa27d4f619820cc52b044af6168", "1cd1d5c2b719b6291d43625a2e7d847afb7d15c77f2761f0162942e0c8c354b7", "903dcb37c15a7987606c6bc529398eeef93d0860cf04337c22a684e0b442a9c7", "8be430c2effe086770f2ea765905f6f72874e4dab3891652bea16aa9d1a6e768"),
    ("521f708b90b8b57ee06fdc7f2e31717ac4c36aa27d4f619820cc52b044af6168", "1cd1d5c2b719b6291d43625a2e7d847afb7d15c77f2761f0162942e0c8c354b7", "903dcb37c15a7987606c6bc529398eeef93d0860cf04337c22a684e0b442a9c7", "accb39fea6015022252aa3eb5be778c309c2ccc434fb3759ce4ed0d7c3061366"),
)
_SNAPSHOTS = (
    ("eebba7c9851331f30d2977ab6c887038a344243c12853179008735e6bb2270fd", "08093f12f3bd7e47a0e245d8e3fa0e7f9b207073866324489e583730b59f321a"),
    ("f3696eb94d0a36808d979851448532205d9f40fb18a253cd08d27ddf32c19b4d", "0bf37ffbdcfd2c783646c58d0a30962c5cd3d568e0c72315c21af33759b939ea"),
    ("f6941b31a2e8db81ecfc995ffa0edefc72671d2ae624afbc409ff5b1d7191624", "40a95e791e2cd4038925ad45f72a4eb08239c59710bba147f6a7f0256966360a"),
    ("69d3f12296633f87f67e8391a5c53629789015afcac2a4d8da08614142493e79", "b7513a9f1d8fcf26b8af6bc778aa7f894ca134eeb4fcbe540847834507d3b857"),
    ("6ee2da0fafc7d5ec44ec8cbab8238526f331ab8edcea50611d65aba1c38d7cec", "747988f7775b92462b5ad565931093a5f70eaf21e93b4675c88e25ad5960dc68"),
    ("1b40ebef28f2a856e2a80ccb759903a04ab5884f7b708fb3a39b9be352fe5f5a", "38f853b25de307af31cbbf76fba324ee90b227b2e1748da7b4830c1f47f7a529"),
    ("0a499ff442f06be78e88bf57af10682230a44b06261b29e2b7b6659b282c6382", "d77f725aa7b7af6deea24e82364906caafc1968b9f2ed3b6df34b074e565d961"),
    ("ab62ce9a986fd270e3fafe478b072a39bf3ec6eeefa9995b1d743275a559ca17", "aa6350d13134e255e8a85333c02c6bd01b141b7402db0d3bb938c263eab5819c"),
    ("6370f22c91d8a55c63c83e6dc5cebe2f62bf81008bf241c5714efac46ee38df8", "801a2e4cee436aafbdfb48e49fc4310436c18a2e2bf4a18291ce87881d908ff3"),
    ("761171e9bea7f5f2dc4fe0051fdd25e2caeb02095c5bed24f776cfbe3343942a", "2c09343f40f6282d62226e1fe224e6fcb510691063656c8dacac778ef4d2d07e"),
    ("540a984788ea253f9763238824741d84713f5386c83554da22b3bd6b1e6ce66a", "31a70d7f34d11a23bf88389da26c4a620976d71d8c29b24ef2533a753deee322"),
    ("f14fecd6b56d0b368aef9d012e9ed7a01d889007ee082b54d9e48f291d4d048c", "7c3d7a1f65e6e464f8756299a085cac9bcb41976e352294441067db8ccddc41f"),
    ("4fb5454fd82163a6549087352243a8fc70bbce0f0bb9f9cca86c69b4c38526dc", "d8edf94212fa27ad920124bf710b04a18ca0fa4be41e15fab874621b1e2133bc"),
    ("6198a781be75d6ffa834e3c5d81971d9c97a63a85e7f970bd7fb8b921314048c", "bde8b6cba609d514dda10747d13146c8606c1f0e5592a444626d035b9d7dbecf"),
    ("e68b10465ab1e914d8460bf98002d06dc914beb9f99ff0d84ee5c00789ae0751", "085a293ad00e65259252a0272782141e34d65f153ec160e9cae466158592a783"),
    ("3f0d33e8a4bf954643a1157aa0cdc0b8e6f5d17460bc0ae31841ca9bff1440d2", "773b380d6cecc6c473f5a2c448f5b79def6a16ffea25aaccd098f528cc0208d8"),
    ("d886a3c19018ac8c45c8d0498136e367eb37c960b5e799878f92469eb8c3bac6", "68311c02eceeab17bd3f51ff4f23d76514a1319487d5b6857628f01115320d57"),
    ("ccf1e408cbdf6d789fa07c8ffe39f9e016b951db0535778fcd62659dba0d201c", "27b8da1f43b6c67f7a11be5a340d7a8e2ba42e3ab7d6029a578b482bf910d206"),
)


def _account() -> dict[str, str]:
    return dict(venue="okx-scenario", environment="test", account="A")


def _owners() -> list[dict[str, Any]]:
    owners = []
    for index, stage in enumerate(("INITIAL", "POST-O03-X1", "POST-O03-C1", "POST-O03-C2-REJECT", "POST-O03-N1", "FINAL")):
        at, version, contracts, notional, available = ((600, 0, "5", "50", "950"), (601, 1, "6", "60", "960"),
            (602, 2, "6", "60", "970"), (603, 2, "6", "60", "970"), (604, 2, "6", "60", "970"), (604, 2, "6", "60", "970"))[index]
        hashes = _OWNERS[index if index < 5 else 4]
        inspection = dict(schema_version="inspect_state_v1", account_key=_account(), profile_id="SYNTHETIC_P1_O03_V1", config_id="scenario-v1",
                          account_version=version, valuation_context_id=_CONTEXT, gate="RUNNING", lifecycle="RISK_STABLE", cash="1000", gross_realized="0", total_fees="0",
                          positions_digest=hashes[0], orders_digest=hashes[1], reservations_digest=hashes[2], owner_state_digest=hashes[3])
        orders = [dict(order_id=name, client_order_id=client, product_id="P_A", state=state, side="buy", limit_price="10",
                       original_size_contracts=size, cumulative_filled_size_contracts=filled, created_at=600)
                  for name, client, state, size, filled in (("O_OLD1", "C_OLD1", "live" if index == 0 else "partially_filled", "2", "0" if index == 0 else "1"),
                                                           ("O_OLD2", "C_OLD2", "live", "3", "0")) if name == "O_OLD2" or index < 2]
        payloads = (dict(outcome="SUCCESS", equity="1000", available_equity=available),
                    dict(outcome="SUCCESS", rows=[dict(product_id="P_A", margin_mode="cross", position_contracts=contracts, last_price="10", notional_usd=notional)]),
                    dict(outcome="SUCCESS", rows=orders))
        owner: dict[str, Any] = dict(cutoff=at, inspection=inspection)
        for offset, (kind, payload) in enumerate(zip(("TRADING", "POSITIONS", "OPEN_ORDERS"), payloads, strict=True)):
            identity = f"S6-O03-{stage}-{kind.replace('_', '-')}"
            request_hash, payload_hash = _SNAPSHOTS[index * 3 + offset]
            owner[kind.lower() + "_request"] = dict(schema_version="snapshot_request_v1", account_key=_account(), snapshot_id=identity,
                                                    snapshot_kind=kind, capture_mode="OWNER_CURRENT", captured_at=at)
            owner[kind.lower() + "_fact"] = dict(schema_version="snapshot_fact_v1", reference=dict(namespace="SNAPSHOT", fact_id=identity),
                                                 request_digest=request_hash, snapshot_kind=kind, snapshot_as_of=at, captured_account_version=version,
                                                 immutable_payload=payload, payload_digest=payload_hash)
        owners.append(owner)
    return owners


def _bundle() -> dict[str, Any]:
    owners = _owners()
    payloads = (
        dict(namespace="synthetic-v1", product_id="P_A", external_execution_id="O03-X1", order_id="O_OLD1", side="LONG", price="10", quantity_contracts="1",
             liquidity="SYNTHETIC_TAKER", matching_effective_at=601, candidate_id="candidate-O03-X1", source_id="source-O03-X1", visible_at=601,
             expected_account_version=0, expected_order_version=0, spec_version="gt03-spec-v1", rule_data_version="gt03-rule-v1"),
        dict(effects=[dict(detecting_event_id="O03-C1-EFFECT", target_order_id="O_OLD1", reason="SPEC_MIGRATION")]),
        dict(targets=[dict(target_order_id="O_OLD1", reason="EXPLICIT_SCENARIO"), dict(target_order_id="O_OLD2", reason="EXPLICIT_SCENARIO")]),
        dict(intent_id="O03-N1-I", client_order_id="O03-N1-C", config_id="scenario-v1", product_id="P_A", strategy_id="o03-grid", side="LONG",
             order_type="LIMIT", quantity_contracts="100", limit_price="10", reduce_only=False, requested_at=604),
    )
    journal = []
    for index, name in enumerate(_NAMES):
        at, event, ordinal, kind, before, after, reason = (
            (601, "O03-X1", 30, "EXECUTION", 0, 1, None), (602, "O03-C1-EFFECT", 50, "CANCEL_EFFECT", 1, 2, None),
            (603, "O03-C2-REJECT", 40, "CANCEL_REQUEST", 2, 2, "CANCEL_REQUEST_TOO_LATE"), (604, "O03-N1", 60, "INTENT", 2, 2, "CAPACITY_EXCEEDED"))[index]
        stamp = dict(event_id=event, effective_at=at, causal_parent_ids=[], ordering_contract_id="S_order_v1", scenario_ordinal=ordinal)
        group = dict(schema_version="scenario_group_v1", group_id=name, account_key=_account(), ordering_contract_id="S_order_v1", group_effective_at=at,
                     declared_member_count=1, members=[dict(kind=kind, stamp=stamp, payload=payloads[index])])
        result = dict(schema_version="group_result_v1", classification="REJECTED" if reason else "COMMITTED", group_id=name,
                      committed_references=[] if reason else [dict(namespace="SOURCE", fact_id=event)], rejections=[dict(event_id=event, reason=reason)] if reason else [],
                      account_version_before=before, account_version_after=after, gate_after="RUNNING", lifecycle_after="RISK_STABLE",
                      owner_state_digest=_OWNERS[index + 1][3], group_digest=_GROUPS[index][0], result_digest=_GROUPS[index][1])
        journal.append(dict(schema_version="spider_journal_record_v1", journal_seq=index + 1, barrier_id="SOURCE_GROUP:" + name, record_kind="SOURCE_GROUP_RESULT",
                            scheduler_key=dict(visible_at=at, queue_class="SOURCE_GROUP", schedule_sequence=0, stable_id=name), causal_parent_ids=[],
                            effective_at=at, visible_at=at, account_version_before=before, account_version_after=after,
                            payload=dict(request=group, result=result, owner_evidence_before=owners[index], owner_evidence_after=owners[index + 1])))
    coverage = [dict(ordinal=index + 1, barrier_id="SOURCE_GROUP:" + name, record_kind="SOURCE_GROUP_RESULT") for index, name in enumerate(_NAMES)]
    plan = dict(schema_version="spider_scenario_plan_v1", scenario_plan_id="SPIDER_P1_O03_DURABLE_V1", native_profile="SYNTHETIC_P1_O03_V1", account_key=_account(),
                initial_cutoff=600, final_cutoff=604, terminal_policy="O03_NON_ATOMIC_COMPLETE", planned_coverage=coverage, deliveries=[],
                source_groups=[dict(schedule_sequence=0, group_id=name, group_sha256=hashes[0]) for name, hashes in zip(_NAMES, _GROUPS, strict=True)])
    records = [dict(key=journal[index]["scheduler_key"], kind="SOURCE_GROUP", stable_id=_NAMES[index], classification="SUCCESS") for index in (1, 2, 3, 0)]
    endpoint = dict(schema_version="spider_endpoint_v1", terminal_reason="O03_NON_ATOMIC_COMPLETE", remaining_planned_barriers=[],
                    cutoff=dict(scheduler_time=604, persisted_boundary=dict(ordinal=4, barrier_id="SOURCE_GROUP:O03-N1", journal_seq=4)),
                    initial_owner_evidence=owners[0], final_owner_evidence=owners[5], scheduler_observation=dict(current_time=604,
                        last_popped=journal[3]["scheduler_key"], gate="RUNNING", terminal=None, pending_keys=[], records=records, polls=[], callback_actions=[]))
    report = [dict(schema_version="spider_product_report_v1", product_id="P_A", terminal_reason="O03_NON_ATOMIC_COMPLETE", position_contracts="6", mark_price="10", notional_usd="60",
                   open_orders=owners[5]["open_orders_fact"]["immutable_payload"]["rows"], committed_execution_refs=["SOURCE:O03-X1"], account_cash="1000", account_equity="1000",
                   account_available_equity="970", account_gross_realized="0", account_total_fees="0",
                   source_evidence_refs=["journal:1", "journal:2", "journal:3", "journal:4", "artifact:endpoint.json#/final_owner_evidence"])]
    return dict(plan=plan, plan_sha256=PLAN_SHA256, journal=journal, endpoint=endpoint, report=report, native_identity_vectors=dict(_VECTORS))


BUNDLE_BYTES = canonical_bytes(_bundle())


def detached_bundle() -> dict[str, Any]:
    """Return detached literals; no shared mutable nested objects escape."""
    return cast(dict[str, Any], decode_canonical(BUNDLE_BYTES))
