use super::super::super::group::{Applied, Reference};
use super::*;
use serde_json::{json, Value};

fn owner() -> ScenarioAccount {
    let (seed, config, marks) = super::super::super::tests::fixture();
    ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
}
fn request(kind: &str) -> Value {
    let payload = match kind {
        "INTENT" => json!({"intent_id":"I","client_order_id":"C","config_id":"scenario-v1",
            "product_id":"BTC-USDT-SWAP","strategy_id":"S","side":"SHORT","order_type":"LIMIT",
            "quantity_contracts":"1","limit_price":"50000.1","reduce_only":true,"requested_at":500}),
        "EXECUTION" => {
            json!({"namespace":"test","product_id":"BTC-USDT-SWAP","external_execution_id":"X",
            "order_id":"O1","side":"SHORT","price":"50000.1","quantity_contracts":"1","liquidity":"SYNTHETIC_TAKER",
            "matching_effective_at":500,"candidate_id":"candidate","source_id":"source","visible_at":501,
            "expected_account_version":0,"expected_order_version":0,"spec_version":"spec-v1","rule_data_version":"tier-v1"})
        }
        "CONTEXT_MARKS" => {
            json!({"expected_before":"00".repeat(32),"expected_after":"11".repeat(32),
            "rows":[{"product_id":"BTC-USDT-SWAP","valid_from":0,"valid_to":3000,"mark":"50001"}]})
        }
        "CANCEL_REQUEST" => {
            json!({"targets":[{"target_order_id":"O1","reason":"EXPLICIT_SCENARIO"}]})
        }
        "CANCEL_EFFECT" => {
            json!({"effects":[{"detecting_event_id":"request","target_order_id":"O1","reason":"EXPLICIT_SCENARIO"}]})
        }
        _ => unreachable!(),
    };
    let ordinal = match kind {
        "INTENT" => 60,
        "EXECUTION" => 30,
        "CONTEXT_MARKS" => 20,
        "CANCEL_REQUEST" => 40,
        _ => 50,
    };
    json!({"schema_version":"scenario_group_v1","group_id":"G",
        "account_key":{"venue":"okx-scenario","environment":"test","account":"A"},
        "ordering_contract_id":"S_order_v1","group_effective_at":500,"declared_member_count":1,
        "members":[{"kind":kind,"payload":payload,"stamp":{"event_id":"event","effective_at":500,
            "causal_parent_ids":[],"ordering_contract_id":"S_order_v1","scenario_ordinal":ordinal}}]})
}
fn parse(value: &Value, owner: &ScenarioAccount) -> Result<Group, Fault> {
    let before = owner.clone();
    let result = decode_group(&value.to_string(), owner);
    assert_eq!(*owner, before);
    result
}

fn configured_owner(count: usize) -> ScenarioAccount {
    let (seed, products) = super::super::super::configured_tests::input(count);
    ScenarioAccount::from_configured(&seed, super::super::super::tests::d("10"), products).unwrap()
}

fn set_product(value: &mut Value, kind: &str, product: Value) {
    let path = if kind == "CONTEXT_MARKS" {
        "/members/0/payload/rows/0/product_id"
    } else {
        "/members/0/payload/product_id"
    };
    *value.pointer_mut(path).unwrap() = product;
}

fn historical_intent(group_id: &str, event_id: &str, source_sequence: i64) -> Value {
    let mut value = request("INTENT");
    value["group_id"] = json!(group_id);
    value["ordering_contract_id"] = json!("HISTORICAL_ORDER_V1");
    value["group_effective_at"] = json!(501);
    value["members"][0]["stamp"]["event_id"] = json!(event_id);
    value["members"][0]["stamp"]["effective_at"] = json!(501);
    value["members"][0]["stamp"]["ordering_contract_id"] = json!("HISTORICAL_ORDER_V1");
    value["members"][0]["stamp"]["source_sequence"] = json!(source_sequence);
    value["members"][0]["payload"]["intent_id"] = json!(format!("I-{group_id}"));
    value["members"][0]["payload"]["client_order_id"] = json!(format!("C-{group_id}"));
    value["members"][0]["payload"]["config_id"] = json!("SYNTHETIC_P2_SCALE_12_V1");
    value["members"][0]["payload"]["reduce_only"] = json!(false);
    value["members"][0]["payload"]["quantity_contracts"] = json!("0.5");
    value["members"][0]["payload"]["limit_price"] = json!("100");
    value["members"][0]["payload"]["requested_at"] = json!(500);
    value
}

fn historical_cancel(kind: &str, group_id: &str, event_id: &str, source_sequence: i64) -> Value {
    let mut value = request(kind);
    value["group_id"] = json!(group_id);
    value["ordering_contract_id"] = json!("HISTORICAL_ORDER_V1");
    value["group_effective_at"] = json!(501);
    value["members"][0]["stamp"]["event_id"] = json!(event_id);
    value["members"][0]["stamp"]["effective_at"] = json!(501);
    value["members"][0]["stamp"]["scenario_ordinal"] =
        json!(if kind == "CANCEL_REQUEST" { 40 } else { 50 });
    value["members"][0]["stamp"]["ordering_contract_id"] = json!("HISTORICAL_ORDER_V1");
    value["members"][0]["stamp"]["source_sequence"] = json!(source_sequence);
    value
}

#[test]
fn configured_group_products_are_exactly_configuration_owned_for_all_decoders() {
    let owner = configured_owner(12);
    for kind in ["INTENT", "EXECUTION", "CONTEXT_MARKS"] {
        let mut accepted = request(kind);
        set_product(&mut accepted, kind, json!("ETH-USDT-SWAP"));
        let decoded = parse(&accepted, &owner).unwrap();
        let product = match &decoded.members[0].input {
            Input::Intent(value) => value.wire_product().clone(),
            Input::Execution(value) => value.wire_product().clone(),
            Input::Context(value) => match &value.rows {
                context::Rows::Marks(rows) => ProfileProduct::BtcEth(rows[0].0.clone()),
                _ => panic!("expected marks"),
            },
            _ => panic!("unexpected member type"),
        };
        assert_eq!(
            product,
            ProfileProduct::BtcEth(Product("ETH-USDT-SWAP".into()))
        );

        for rejected in [json!("UNCONFIGURED-SWAP"), json!(7), json!("P_A")] {
            let mut invalid = request(kind);
            set_product(&mut invalid, kind, rejected);
            assert_eq!(parse(&invalid, &owner), Err("INVALID_SCHEMA"), "{kind}");
        }
    }

    let (seed, mut products) = super::super::super::configured_tests::input(1);
    let only_eth = Product("ETH-USDT-SWAP".into());
    products[0].product = only_eth.clone();
    products[0].specs[0].product = only_eth.clone();
    products[0].tiers[0].product = only_eth.clone();
    products[0].marks[0].product = only_eth;
    let eth_owner =
        ScenarioAccount::from_configured(&seed, super::super::super::tests::d("10"), products)
            .unwrap();
    for kind in ["INTENT", "EXECUTION", "CONTEXT_MARKS"] {
        let mut legacy_known = request(kind);
        set_product(&mut legacy_known, kind, json!("BTC-USDT-SWAP"));
        assert_eq!(
            parse(&legacy_known, &eth_owner),
            Err("INVALID_SCHEMA"),
            "{kind}"
        );
    }
}

#[test]
fn configured_pa_requires_explicit_membership_and_is_never_the_p1_pa_variant() {
    let (seed, mut products) = super::super::super::configured_tests::input(1);
    let product = Product("P_A".into());
    products[0].product = product.clone();
    products[0].specs[0].product = product.clone();
    products[0].tiers[0].product = product.clone();
    products[0].marks[0].product = product;
    let owner =
        ScenarioAccount::from_configured(&seed, super::super::super::tests::d("10"), products)
            .unwrap();
    for kind in ["INTENT", "EXECUTION", "CONTEXT_MARKS"] {
        let mut value = request(kind);
        set_product(&mut value, kind, json!("P_A"));
        let decoded = parse(&value, &owner).unwrap();
        let product = match &decoded.members[0].input {
            Input::Intent(value) => value.wire_product().clone(),
            Input::Execution(value) => value.wire_product().clone(),
            Input::Context(value) => match &value.rows {
                context::Rows::Marks(rows) => ProfileProduct::BtcEth(rows[0].0.clone()),
                _ => panic!("expected marks"),
            },
            _ => panic!("unexpected member type"),
        };
        assert_eq!(product, ProfileProduct::BtcEth(Product("P_A".into())));
    }
}

#[test]
fn configured_product_type_and_account_error_precedence_are_preserved() {
    let owner = configured_owner(12);
    let mut invalid = request("INTENT");
    set_product(&mut invalid, "INTENT", json!(false));
    invalid["account_key"]["account"] = json!("OTHER");
    assert_eq!(parse(&invalid, &owner), Err("INVALID_SCHEMA"));
    let mut valid = request("INTENT");
    valid["account_key"]["account"] = json!("OTHER");
    assert_eq!(parse(&valid, &owner), Err("ACCOUNT_KEY_MISMATCH"));
}

#[test]
fn historical_order_groups_are_configured_only_and_same_tick_source_sequence_is_authoritative() {
    let unconfigured = owner();
    assert_eq!(
        parse(&historical_intent("HG0", "HE0", 0), &unconfigured),
        Err("INVALID_SCENARIO_GROUP")
    );

    let mut configured = configured_owner(12);
    let first = parse(&historical_intent("HG0", "HE0", 7), &configured).unwrap();
    configured.apply_group_observed(&first, |_| Ok(())).unwrap();
    assert_eq!(configured.orders.len(), 1);

    let second = parse(&historical_intent("HG1", "HE1", 8), &configured).unwrap();
    configured
        .apply_group_observed(&second, |_| Ok(()))
        .unwrap();
    assert_eq!(configured.orders.len(), 2);

    let mut repeated_sequence_owner = configured.clone();
    let repeated = parse(
        &historical_intent("HG-REPEATED", "HE-REPEATED", 8),
        &configured,
    )
    .unwrap();
    assert!(matches!(
        repeated_sequence_owner.apply_group_observed(&repeated, |_| Ok(())),
        Err("STALE_EVENT")
    ));

    let reversed = parse(&historical_intent("HG2", "HE2", 6), &configured).unwrap();
    assert!(matches!(
        configured.apply_group_observed(&reversed, |_| Ok(())),
        Err("STALE_EVENT")
    ));
    assert_eq!(configured.orders.len(), 2);
}

#[test]
fn historical_order_stamp_requires_source_sequence() {
    let configured = configured_owner(12);
    let mut value = historical_intent("HG0", "HE0", 0);
    value["members"][0]["stamp"]
        .as_object_mut()
        .unwrap()
        .remove("source_sequence");
    assert_eq!(parse(&value, &configured), Err("INVALID_SCENARIO_GROUP"));
}

#[test]
fn historical_order_contract_narrowly_accepts_cancel_request_and_effect_kinds() {
    let configured = configured_owner(12);
    for (kind, ordinal) in [("CANCEL_REQUEST", 40), ("CANCEL_EFFECT", 50)] {
        let value = historical_cancel(kind, "HG-CANCEL", "HE-CANCEL", 7);
        let parsed = parse(&value, &configured).unwrap();
        assert_eq!(parsed.members[0].stamp.source_sequence, Some(7));
        assert_eq!(parsed.members[0].stamp.scenario_ordinal, ordinal);

        let mut wrong_ordinal = value.clone();
        wrong_ordinal["members"][0]["stamp"]["scenario_ordinal"] = json!(60);
        assert_eq!(
            parse(&wrong_ordinal, &configured),
            Err("INVALID_SCENARIO_GROUP")
        );

        let mut missing_sequence = value.clone();
        missing_sequence["members"][0]["stamp"]
            .as_object_mut()
            .unwrap()
            .remove("source_sequence");
        assert_eq!(
            parse(&missing_sequence, &configured),
            Err("INVALID_SCENARIO_GROUP")
        );

        let unconfigured = owner();
        assert_eq!(parse(&value, &unconfigured), Err("INVALID_SCENARIO_GROUP"));
    }

    for kind in ["EXECUTION", "CONTEXT_MARKS"] {
        let mut unsupported = request(kind);
        unsupported["ordering_contract_id"] = json!("HISTORICAL_ORDER_V1");
        unsupported["group_effective_at"] = json!(501);
        unsupported["members"][0]["stamp"]["effective_at"] = json!(501);
        unsupported["members"][0]["stamp"]["ordering_contract_id"] = json!("HISTORICAL_ORDER_V1");
        unsupported["members"][0]["stamp"]["source_sequence"] = json!(0);
        assert_eq!(
            parse(&unsupported, &configured),
            Err("INVALID_SCENARIO_GROUP")
        );
    }
}

#[test]
fn reverse_contract_keeps_reverse_cancel_effect_ordinal() {
    let mut value = request("CANCEL_EFFECT");
    value["ordering_contract_id"] = json!("S_order_v1_reverse_execution_cancel_effective");
    value["members"][0]["stamp"]["ordering_contract_id"] =
        json!("S_order_v1_reverse_execution_cancel_effective");
    value["members"][0]["stamp"]["scenario_ordinal"] = json!(30);
    let decoded = parse(&value, &owner()).unwrap();
    assert_eq!(decoded.members[0].stamp.scenario_ordinal, 30);
}

#[test]
fn historical_cancel_request_uses_source_sequence_before_ordinal_only_at_same_tick() {
    let mut configured = configured_owner(12);
    let first = parse(&historical_intent("HG-ORDER", "HE-ORDER", 7), &configured).unwrap();
    configured.apply_group_observed(&first, |_| Ok(())).unwrap();
    let target = configured.orders.keys().next().unwrap().clone();

    let mut request = historical_cancel("CANCEL_REQUEST", "HG-REQUEST", "HE-REQUEST", 8);
    request["members"][0]["payload"]["targets"][0]["target_order_id"] = json!(target);
    let decoded = parse(&request, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();

    let mut effect = historical_cancel("CANCEL_EFFECT", "HG-EFFECT", "HE-EFFECT", 9);
    effect["members"][0]["payload"]["effects"][0]["target_order_id"] = json!(target);
    effect["members"][0]["payload"]["effects"][0]["detecting_event_id"] = json!("HE-REQUEST");
    effect["members"][0]["stamp"]["causal_parent_ids"] = json!(["HE-REQUEST"]);
    let decoded = parse(&effect, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();
}

#[test]
fn historical_source_sequence_only_breaks_equal_effective_time_ties() {
    let (seed, mut products) = super::super::super::configured_tests::input(12);
    for product in &mut products {
        product.marks[0].valid_to = 50_000;
    }
    let mut configured =
        ScenarioAccount::from_configured(&seed, super::super::super::tests::d("10"), products)
            .unwrap();
    let first = parse(&historical_intent("HG-BASE", "HE-BASE", 0), &configured).unwrap();
    configured.apply_group_observed(&first, |_| Ok(())).unwrap();
    let target = configured.orders.keys().next().unwrap().clone();

    let mut request = historical_cancel("CANCEL_REQUEST", "HG-REQUEST", "HE-REQUEST", 6);
    request["group_effective_at"] = json!(40_003);
    request["members"][0]["stamp"]["effective_at"] = json!(40_003);
    request["members"][0]["payload"]["targets"][0]["target_order_id"] = json!(target);
    let decoded = parse(&request, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();

    let mut intervening = historical_intent("HG-INTERVENING", "HE-INTERVENING", 8);
    intervening["group_effective_at"] = json!(41_003);
    intervening["members"][0]["stamp"]["effective_at"] = json!(41_003);
    let decoded = parse(&intervening, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();

    let mut effect = historical_cancel("CANCEL_EFFECT", "HG-EFFECT", "HE-EFFECT", 7);
    effect["group_effective_at"] = json!(45_003);
    effect["members"][0]["stamp"]["effective_at"] = json!(45_003);
    effect["members"][0]["payload"]["effects"][0]["target_order_id"] = json!(target);
    effect["members"][0]["payload"]["effects"][0]["detecting_event_id"] = json!("HE-REQUEST");
    effect["members"][0]["stamp"]["causal_parent_ids"] = json!(["HE-REQUEST"]);
    let decoded = parse(&effect, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();
    assert!(matches!(
        configured.orders[&target].cancel,
        risk_transition::cancel::State::EffectiveCanceled(_)
    ));
}

#[test]
fn generic_same_tick_source_sequence_cannot_move_backwards() {
    let mut configured = configured_owner(12);
    let mut first = historical_intent("SG-FIRST", "SE-FIRST", 7);
    first["ordering_contract_id"] = json!("S_order_v1");
    first["members"][0]["stamp"]["ordering_contract_id"] = json!("S_order_v1");
    let decoded = parse(&first, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();

    let mut reversed = historical_intent("SG-REVERSED", "SE-REVERSED", 6);
    reversed["ordering_contract_id"] = json!("S_order_v1");
    reversed["members"][0]["stamp"]["ordering_contract_id"] = json!("S_order_v1");
    let decoded = parse(&reversed, &configured).unwrap();
    assert!(matches!(
        configured.apply_group_observed(&decoded, |_| Ok(())),
        Err("INVALID_SCENARIO_GROUP")
    ));
}

#[test]
fn historical_pending_cancel_request_is_a_nonterminal_too_late_rejection() {
    let mut configured = configured_owner(12);
    let first = parse(&historical_intent("HG-ORDER", "HE-ORDER", 0), &configured).unwrap();
    configured.apply_group_observed(&first, |_| Ok(())).unwrap();
    let target = configured.orders.keys().next().unwrap().clone();

    let mut request = historical_cancel("CANCEL_REQUEST", "HG-REQUEST", "HE-REQUEST", 1);
    request["members"][0]["payload"]["targets"][0]["target_order_id"] = json!(target);
    let decoded = parse(&request, &configured).unwrap();
    configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap();

    let mut repeated = historical_cancel("CANCEL_REQUEST", "HG-REPEATED", "HE-REPEATED", 2);
    repeated["members"][0]["payload"]["targets"][0]["target_order_id"] = json!(target);
    let decoded = parse(&repeated, &configured).unwrap();
    let Applied::Fresh(completion) = configured
        .apply_group_observed(&decoded, |_| Ok(()))
        .unwrap()
    else {
        panic!("repeated request should be recorded as a fresh rejection")
    };
    assert_eq!(completion.failure, None);
    assert_eq!(completion.committed, Vec::<Reference>::new());
    assert_eq!(
        completion.rejections,
        vec![("HE-REPEATED".into(), "CANCEL_REQUEST_TOO_LATE")]
    );
    assert!(matches!(
        configured.orders[&target].cancel,
        risk_transition::cancel::State::Requested(_)
    ));
}

#[test]
fn twelve_product_first_intent_passes_configured_context_validation() {
    let mut owner = configured_owner(12);
    let mut value = request("INTENT");
    value["group_effective_at"] = json!(501);
    value["members"][0]["stamp"]["effective_at"] = json!(501);
    value["members"][0]["payload"]["requested_at"] = json!(501);
    value["members"][0]["payload"]["limit_price"] = json!("100");
    value["members"][0]["payload"]["config_id"] = json!(owner.config_id);
    let decoded = parse(&value, &owner).unwrap();
    let result = owner.apply_group_observed(&decoded, |_| Ok(())).unwrap();
    let crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::group::Applied::Fresh(completion) = result else {
        panic!("first intent should be fresh")
    };
    assert_ne!(completion.failure, Some("UNSUPPORTED_CONTEXT_TRANSITION"));
}

#[test]
fn legacy_wire_product_matrix_keeps_p1_tokens_and_context_rejection() {
    let owner = owner();
    for token in ["BTC-USDT-SWAP", "ETH-USDT-SWAP"] {
        for kind in ["INTENT", "EXECUTION", "CONTEXT_MARKS"] {
            let mut value = request(kind);
            set_product(&mut value, kind, json!(token));
            let decoded = parse(&value, &owner).unwrap();
            let product = match &decoded.members[0].input {
                Input::Intent(value) => value.wire_product().clone(),
                Input::Execution(value) => value.wire_product().clone(),
                Input::Context(value) => match &value.rows {
                    context::Rows::Marks(rows) => ProfileProduct::BtcEth(rows[0].0.clone()),
                    _ => panic!("expected marks"),
                },
                _ => panic!("unexpected member type"),
            };
            assert_eq!(product.canonical_id(), token);
            assert!(matches!(product, ProfileProduct::BtcEth(_)));
        }
    }
    for kind in ["INTENT", "EXECUTION"] {
        let mut value = request(kind);
        set_product(&mut value, kind, json!("P_A"));
        let decoded = parse(&value, &owner).unwrap();
        let product = match &decoded.members[0].input {
            Input::Intent(value) => value.wire_product(),
            Input::Execution(value) => value.wire_product(),
            _ => panic!("unexpected member type"),
        };
        assert_eq!(product, &ProfileProduct::Pa);
    }
    let mut context = request("CONTEXT_MARKS");
    set_product(&mut context, "CONTEXT_MARKS", json!("P_A"));
    assert_eq!(parse(&context, &owner), Err("INVALID_SCHEMA"));
}

#[test]
fn every_nested_required_unknown_duplicate_and_forbidden_field_is_rejected() {
    let owner = owner();
    for kind in [
        "INTENT",
        "EXECUTION",
        "CONTEXT_MARKS",
        "CANCEL_REQUEST",
        "CANCEL_EFFECT",
    ] {
        let value = request(kind);
        assert!(parse(&value, &owner).is_ok());
        let mut paths = vec![
            "",
            "/account_key",
            "/members/0",
            "/members/0/stamp",
            "/members/0/payload",
        ];
        match kind {
            "CONTEXT_MARKS" => paths.push("/members/0/payload/rows/0"),
            "CANCEL_REQUEST" => paths.push("/members/0/payload/targets/0"),
            "CANCEL_EFFECT" => paths.push("/members/0/payload/effects/0"),
            _ => (),
        }
        for path in paths {
            let object = value.pointer(path).unwrap().as_object().unwrap();
            for key in object.keys() {
                let mut missing = value.clone();
                missing
                    .pointer_mut(path)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove(key);
                assert_eq!(
                    parse(&missing, &owner),
                    Err("INVALID_SCHEMA"),
                    "{kind}/{key}"
                );
            }
            let mut extra = value.clone();
            extra
                .pointer_mut(path)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("forbidden".into(), json!(0));
            assert_eq!(parse(&extra, &owner), Err("INVALID_SCHEMA"));
            let original = value.pointer(path).unwrap().to_string();
            let key = object.keys().next().unwrap();
            let duplicate = format!(
                "{{{}:{},{}",
                serde_json::to_string(key).unwrap(),
                object[key],
                &original[1..]
            );
            assert_eq!(
                decode_group(
                    &value.to_string().replacen(&original, &duplicate, 1),
                    &owner
                ),
                Err("INVALID_SCHEMA")
            );
        }
    }
}

#[test]
fn optional_tokens_and_checked_numbers_preserve_the_closed_wire_domain() {
    let owner = owner();
    for (kind, pointer, fields) in [
        ("INTENT", "/members/0/stamp", &["source_sequence"][..]),
        (
            "EXECUTION",
            "/members/0/payload",
            &["fee_asset", "reported_fee"][..],
        ),
    ] {
        let mut value = request(kind);
        let absent = parse(&value, &owner).unwrap();
        for field in fields {
            value.pointer_mut(pointer).unwrap()[field] = Value::Null;
        }
        assert_eq!(parse(&value, &owner).unwrap(), absent);
    }
    let mut positive = request("EXECUTION");
    positive["members"][0]["stamp"]["source_sequence"] = json!(7);
    positive["members"][0]["payload"]["fee_asset"] = json!("USDT");
    positive["members"][0]["payload"]["reported_fee"] = json!("0.5");
    let parsed = parse(&positive, &owner).unwrap();
    assert_eq!(parsed.members[0].stamp.source_sequence, Some(7));
    assert_ne!(parsed, parse(&request("EXECUTION"), &owner).unwrap());
    for path in [
        "/schema_version",
        "/ordering_contract_id",
        "/members/0/kind",
        "/members/0/payload/side",
        "/members/0/payload/liquidity",
    ] {
        let mut value = request("EXECUTION");
        *value.pointer_mut(path).unwrap() = json!("unknown");
        assert_eq!(parse(&value, &owner), Err("INVALID_SCHEMA"));
    }
    for kind in ["INTENT", "EXECUTION"] {
        for product in ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A", "unknown"] {
            let mut value = request(kind);
            value["members"][0]["payload"]["product_id"] = json!(product);
            assert_eq!(parse(&value, &owner).is_ok(), product != "unknown");
        }
    }
    let mut marks = request("CONTEXT_MARKS");
    marks["members"][0]["payload"]["rows"][0]["product_id"] = json!("P_A");
    assert_eq!(parse(&marks, &owner), Err("INVALID_SCHEMA"));
    for path in [
        "/declared_member_count",
        "/members/0/payload/expected_account_version",
        "/members/0/payload/expected_order_version",
    ] {
        for invalid in [json!(-1), json!(true), json!("1"), json!(u64::MAX)] {
            let mut value = request("EXECUTION");
            *value.pointer_mut(path).unwrap() = invalid;
            assert_eq!(parse(&value, &owner), Err("INVALID_SCHEMA"));
        }
    }
    for reason in [
        "EXPLICIT_SCENARIO",
        "RISK_SHORTFALL",
        "MMR_BREACH",
        "SPEC_MIGRATION",
        "UNSUPPORTED",
        "other",
    ] {
        let mut value = request("CANCEL_REQUEST");
        value["members"][0]["payload"]["targets"][0]["reason"] = json!(reason);
        assert_eq!(parse(&value, &owner).is_ok(), reason != "other");
    }
    let mut mismatch = request("INTENT");
    mismatch["account_key"]["account"] = json!("B");
    assert_eq!(parse(&mismatch, &owner), Err("ACCOUNT_KEY_MISMATCH"));
}

#[test]
fn owner_not_wire_classifies_market_profile_and_group_business_rules() {
    let mut market = request("INTENT");
    market["members"][0]["payload"]["order_type"] = json!("MARKET");
    assert_eq!(parse(&market, &owner()), Err("INVALID_SCHEMA"));
    market["members"][0]["payload"]
        .as_object_mut()
        .unwrap()
        .remove("limit_price");
    let mut current = owner();
    let group = parse(&market, &current).unwrap();
    assert_eq!(
        current.apply_group(&group).unwrap().failure,
        Some("UNSUPPORTED_ORDER_TYPE")
    );
    market["members"][0]["payload"]["limit_price"] = Value::Null;
    assert_eq!(parse(&market, &owner()).unwrap(), group);
    for path in [
        "/declared_member_count",
        "/members/0/stamp/scenario_ordinal",
        "/members/0/stamp/ordering_contract_id",
    ] {
        let mut value = request("INTENT");
        *value.pointer_mut(path).unwrap() = if path.ends_with("ordering_contract_id") {
            json!("unknown")
        } else {
            json!(0)
        };
        let mut current = owner();
        let group = parse(&value, &current).unwrap();
        assert_eq!(current.apply_group(&group), Err("INVALID_SCENARIO_GROUP"));
    }
    let mut value = request("INTENT");
    value["members"][0]["payload"]["product_id"] = json!("P_A");
    let mut current = owner();
    let group = parse(&value, &current).unwrap();
    assert_eq!(
        current.apply_group(&group).unwrap().failure,
        Some("PROFILE_MISMATCH")
    );
    let mut current = owner();
    let mut value = request("CONTEXT_MARKS");
    let hex = |hash: Hash| hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let (config, marks) = current.btc_context().unwrap();
    value["group_effective_at"] = json!(501);
    value["members"][0]["stamp"]["effective_at"] = json!(501);
    value["members"][0]["payload"]["expected_before"] = json!(hex(current.valuation_context_id));
    value["members"][0]["payload"]["expected_after"] =
        json!(hex(context_id(config, marks, 501).unwrap()));
    let group = parse(&value, &current).unwrap();
    assert_eq!(
        current.apply_group(&group),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
}

#[test]
fn historical_identity_and_stale_precedence_remain_in_owner() {
    let mut current = owner();
    let value = request("INTENT");
    let group = parse(&value, &current).unwrap();
    assert_eq!(current.apply_group(&group).unwrap().failure, None);
    let mut conflict = value.clone();
    conflict["members"][0]["payload"]["quantity_contracts"] = json!("2");
    conflict["members"][0]["stamp"]["scenario_ordinal"] = json!(0);
    let parsed = parse(&conflict, &current).unwrap();
    assert_eq!(
        current.clone().apply_group(&parsed),
        Err("IDEMPOTENCY_KEY_CONFLICT")
    );
    let mut stale = request("CANCEL_REQUEST");
    stale["group_id"] = json!("later-group");
    stale["members"][0]["stamp"]["event_id"] = json!("later-event");
    let parsed = parse(&stale, &current).unwrap();
    assert_eq!(current.apply_group(&parsed), Err("STALE_EVENT"));
}
