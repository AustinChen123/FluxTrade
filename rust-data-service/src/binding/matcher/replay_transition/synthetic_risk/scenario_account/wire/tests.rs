use super::*;
use profiles::construct;

fn text(s: &str) -> Json {
    Json::Text(s.into())
}
fn account() -> Json {
    decode(r#"{"venue":"okx-scenario","environment":"test","account":"A"}"#).unwrap()
}

#[test]
fn strict_json_preserves_keys_and_separates_syntax_from_schema() {
    let nested = |n| format!("{{\"x\":{}0{}}}", "[".repeat(n), "]".repeat(n));
    assert!(decode(&nested(127)).is_ok());
    assert_eq!(decode(&nested(128)), Err("INVALID_SCHEMA"));
    for input in ["", "{}{}", "{", "[1,]", "01", "NaN", r#""\x00""#] {
        assert_eq!(decode(input), Err("INVALID_JSON"), "{input}");
    }
    for input in [
        r#"{"a":1,"a":2}"#,
        r#"{"a":1,"\u0061":2}"#,
        r#"{"outer":[{"x":1,"x":2}]}"#,
        r#""\ud800""#,
        r#""\udc00""#,
        r#"{"\ud800":1}"#,
    ] {
        assert_eq!(decode(input), Err("INVALID_SCHEMA"), "{input}");
    }
    let value = decode(" { \"z\" : [true,1], \"a\": {\"b\":\"c\"} } ").unwrap();
    assert_eq!(
        value.canonical().unwrap(),
        r#"{"a":{"b":"c"},"z":[true,1]}"#
    );
    let rows = value.object(&["a", "z"], &[]).unwrap();
    assert!(rows["a"].object(&["b"], &[]).is_ok());
    for (required, optional) in [(&["missing"][..], &[][..]), (&[][..], &["a"][..])] {
        assert_eq!(rows["a"].object(required, optional), Err("INVALID_SCHEMA"));
    }
    for scalar in [Json::Null, Json::Bool(true), text("x"), Json::Array(vec![])] {
        assert_eq!(scalar.object(&[], &[]), Err("INVALID_SCHEMA"));
    }
}

#[test]
fn canonical_unicode_and_all_control_bytes_are_independent_literals() {
    let scalar = "é中😀\u{2028}\u{2029}\"\\/";
    let expected = "\"é中😀\u{2028}\u{2029}\\\"\\\\/\"";
    assert_eq!(
        text(scalar).canonical().unwrap().as_bytes(),
        expected.as_bytes()
    );
    let escaped = r#""\u00e9\u4e2d\ud83d\ude00\u2028\u2029\"\\\/""#;
    assert_eq!(decode(escaped).unwrap(), text(scalar));
    let controls: String = (0..32).map(char::from).collect();
    let expected = concat!(
        "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007",
        "\\u0008\\u0009\\u000a\\u000b\\u000c\\u000d\\u000e\\u000f",
        "\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017",
        "\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f\""
    );
    assert_eq!(text(&controls).canonical().unwrap(), expected);
    assert_eq!(decode(expected).unwrap(), text(&controls));
    assert_eq!(decode(r#""\b\t\n\f\r""#).unwrap(), text("\u{8}\t\n\u{c}\r"));
    assert_ne!(text("é").canonical(), text("e\u{301}").canonical());
    assert_eq!(Json::Null.canonical(), Err("NATIVE_INVARIANT"));
}

#[test]
fn numeric_primitives_never_round_or_coerce() {
    for s in ["-9223372036854775808", "9223372036854775807", "0", "-0"] {
        assert_eq!(
            decode(s).unwrap().integer(false).unwrap(),
            s.parse::<i64>().unwrap()
        );
    }
    for s in [
        "true",
        "\"1\"",
        "1.0",
        "1e0",
        "1E0",
        "9223372036854775808",
        "-9223372036854775809",
        "1e9999",
    ] {
        let value = decode(s).unwrap();
        assert_eq!(value.integer(false), Err("INVALID_SCHEMA"), "{s}");
    }
    assert_eq!(decode("-1").unwrap().integer(true), Err("INVALID_SCHEMA"));
    for s in [
        "0",
        "-1.25",
        "0.0000000000000000000000000001",
        "79228162514264337593543950335",
    ] {
        assert_eq!(text(s).decimal().unwrap().normalize().to_string(), s);
    }
    for s in [
        "-0",
        "+1",
        "01",
        "1.0",
        "1e0",
        "1_0",
        " 1",
        "1 ",
        "NaN",
        "inf",
        ".1",
        "1.",
        "79228162514264337593543950336",
        "0.00000000000000000000000000001",
        "1.00000000000000000000000000001",
    ] {
        assert_eq!(text(s).decimal(), Err("INVALID_SCHEMA"), "{s}");
    }
    assert_eq!(decode("1").unwrap().decimal(), Err("INVALID_SCHEMA"));
    assert_eq!(text(&"ab".repeat(32)).hash().unwrap(), [0xab; 32]);
    for s in [
        "ab".repeat(31),
        "ab".repeat(33),
        "AB".repeat(32),
        "gg".repeat(32),
    ] {
        assert_eq!(text(&s).hash(), Err("INVALID_SCHEMA"));
    }
    for s in ["", " x", "x ", "\u{2003}x"] {
        assert_eq!(text(s).id(), Err("INVALID_SCHEMA"));
    }
    assert_eq!(text("中").id().unwrap(), "中");
}

#[test]
fn account_schema_and_absent_null_equivalence_are_exact() {
    let original = account();
    let mut rows = original
        .object(&["venue", "environment", "account"], &[])
        .unwrap()
        .clone();
    rows.insert("subaccount".into(), Json::Null);
    assert_eq!(Json::Object(rows.clone()).account(), original.account());
    rows.insert("subaccount".into(), text("S"));
    assert_eq!(
        Json::Object(rows.clone())
            .account()
            .unwrap()
            .subaccount
            .as_deref(),
        Some("S")
    );
    for key in ["venue", "environment", "account"] {
        let mut missing = rows.clone();
        missing.remove(key);
        assert_eq!(Json::Object(missing).account(), Err("INVALID_SCHEMA"));
        let mut wrong = rows.clone();
        wrong.insert(key.into(), Json::Null);
        assert_eq!(Json::Object(wrong).account(), Err("INVALID_SCHEMA"));
    }
    rows.insert("extra".into(), text("x"));
    assert_eq!(Json::Object(rows).account(), Err("INVALID_SCHEMA"));
}

#[test]
fn closed_constructors_equal_existing_owners_and_literal_anchors() {
    let key = account().account().unwrap();
    let (seed, config, marks) = super::super::tests::fixture();
    let btc = construct("SYNTHETIC_BTC_ETH_V1", key.clone()).unwrap();
    assert_eq!(
        btc,
        ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
    );
    let golden = construct("SYNTHETIC_GOLDEN_CANCEL_V1", key.clone()).unwrap();
    assert_eq!(
        (golden.cash, golden.state_version, golden.seed_effective_at),
        (Decimal::from(1000), 0, 500)
    );
    assert!(golden.orders.is_empty() && golden.positions.is_empty());
    assert_eq!(golden.golden_cancel_reservation().unwrap(), Decimal::ZERO);
    assert_eq!(
        golden.profile,
        ProfileContext::GoldenCancel(golden_cancel::Config::frozen())
    );
    let min = construct("SYNTHETIC_MIN_CASH_V1", key.clone()).unwrap();
    assert_eq!(
        min,
        ScenarioAccount::synthetic_min_cash(key.clone()).unwrap()
    );
    assert_eq!(min.cash, Decimal::from(1000));
    assert_eq!(min.orders["MIN-O1"].facts.remaining, Decimal::from(20));
    assert_eq!(
        min.positions.btc().unwrap()[&Product::Btc].contracts,
        Decimal::from(20)
    );
    for profile in [
        "SYNTHETIC_BTC_ETH_V1",
        "SYNTHETIC_GOLDEN_CANCEL_V1",
        "SYNTHETIC_MIN_CASH_V1",
    ] {
        let mut other = key.clone();
        other.account = "B".into();
        assert_eq!(construct(profile, other.clone()).unwrap().key, other);
    }
    assert_eq!(construct("unknown", key.clone()), Err("INVALID_SCHEMA"));
    let mut invalid = key;
    invalid.account.clear();
    assert_eq!(
        construct("SYNTHETIC_MIN_CASH_V1", invalid),
        Err("INVALID_SCHEMA")
    );
}

#[test]
fn p1_liquidation_profile_reuses_frozen_owner_transition() {
    let (mut seed, config, mut marks) = super::super::tests::fixture();
    seed.effective_at = 1500;
    seed.cash = Decimal::from(3);
    seed.orders.clear();
    seed.positions[0].side = Side::Short;
    seed.positions[0].contracts = Decimal::ONE;
    seed.positions[0].lots.truncate(1);
    marks[0].price = Decimal::from(50000);
    marks[0].valid_to = 1501;
    marks.push(Mark {
        product: Product::Btc,
        price: Decimal::from(50100),
        valid_from: 1501,
        valid_to: 3000,
    });
    let mut expected = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let ProfileContext::BtcEthScenario {
        p1_liquidation_profile,
        ..
    } = &mut expected.profile
    else {
        unreachable!()
    };
    *p1_liquidation_profile = true;
    let mut owner = construct("SYNTHETIC_P1_LIQUIDATION_V1", seed.key).unwrap();
    assert_eq!(owner, expected);
    // Independent raw-byte SHA256: entry basis 500; lot entry 50000; base 0.01.
    let hex = |hash: Hash| hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
    assert_eq!(
        hex(owner.owner_evidence_digest().unwrap()),
        "134b24a87d3490abca366cadb5e081d698562474c3285e7b4e1393300c4da94d"
    );
    let mut input =
        context::tests::activation(&owner, 1501, context::tests::mark_rows(&marks, 1501));
    input.stamp.event_id = "S6-L-MARK-1501".into();
    owner.activate_context(&input).unwrap();
    assert_eq!(
        (owner.cash, owner.gross_realized, owner.fees),
        (
            Decimal::new(-101602, 5),
            -Decimal::ONE,
            Decimal::new(301602, 5)
        )
    );
    assert_eq!(owner.state_version, 2);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(
        owner.transition.lifecycle,
        risk_transition::Lifecycle::LiquidatedInsolvent
    );
    assert!(owner.positions.is_empty() && owner.orders.is_empty());
    assert_eq!(owner.liquidation_ids().count(), 1);
    assert_eq!(
        hex(owner.owner_evidence_digest().unwrap()),
        "542cb313c7e688a6107f408aa24414041cd4321bd7e1c75d26b1d2d7e9277fb4"
    );
    assert_eq!(
        hex(owner.liquidation_ids().next().unwrap()),
        "7fb9f5bec4fe944a93de3c08e44d149a42279d93ff24c37b7712c0a5037db810"
    );
}
