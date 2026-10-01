use super::super::super::tests::fixture;
use super::*;
use serde_json::{json, Value};

fn owner() -> ScenarioAccount {
    let (seed, config, marks) = fixture();
    ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
}
fn request() -> Value {
    json!({"schema_version":"snapshot_request_v1","account_key":{"venue":"okx-scenario","environment":"test","account":"A"},
        "snapshot_id":"S","snapshot_kind":"TRADING","capture_mode":"OWNER_CURRENT","captured_at":500})
}
fn parse(v: &Value, a: &ScenarioAccount) -> Result<Request, Fault> {
    let before = a.clone();
    let result = decode_request(&v.to_string(), &a.key);
    assert_eq!(*a, before);
    result
}
fn envelope(fact: &Fact, payload: &str) {
    let hex = |h: Hash| h.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let mut expected = json!({"schema_version":"snapshot_fact_v1",
        "reference":{"namespace":"SNAPSHOT","fact_id":"S"},
        "request_digest":hex(fact.request_digest),"snapshot_kind":fact.kind.name(),
        "snapshot_as_of":fact.snapshot_as_of,"payload_digest":hex(fact.payload_digest),
        "immutable_payload":serde_json::from_str::<Value>(payload).unwrap()});
    if let Some(v) = fact.captured_account_version {
        expected["captured_account_version"] = json!(v);
    }
    if let Some(v) = &fact.continuation_id {
        expected["continuation_id"] = json!(v);
    }
    assert_eq!(
        fact.wire_json().unwrap().canonical().unwrap(),
        expected.to_string()
    );
}
#[test]
fn all_request_shapes_and_store_precedence() {
    let a = owner();
    for kind in ["MARKET", "EARN", "TRADING", "POSITIONS", "OPEN_ORDERS"] {
        for mode in ["OWNER_CURRENT", "FROZEN_POLL_FIXTURE"] {
            let mut v = request();
            v["snapshot_kind"] = json!(kind);
            v["capture_mode"] = json!(mode);
            assert!(parse(&v, &a).is_ok());
            for path in ["", "/account_key"] {
                for key in v.pointer(path).unwrap().as_object().unwrap().keys() {
                    let mut bad = v.clone();
                    bad.pointer_mut(path)
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .remove(key);
                    assert_eq!(parse(&bad, &a), Err("INVALID_SCHEMA"));
                }
                let mut bad = v.clone();
                bad.pointer_mut(path).unwrap()["extra"] = json!(1);
                assert_eq!(parse(&bad, &a), Err("INVALID_SCHEMA"));
                let object = v.pointer(path).unwrap().to_string();
                let duplicated = format!("{{{},{}", &object[1..object.len() - 1], &object[1..]);
                assert_eq!(
                    decode_request(&v.to_string().replacen(&object, &duplicated, 1), &a.key),
                    Err("INVALID_SCHEMA")
                );
            }
        }
    }
    let mut v = request();
    let absent = parse(&v, &a).unwrap();
    v["fixture_key"] = Value::Null;
    v["continuation_id"] = Value::Null;
    assert_eq!(parse(&v, &a).unwrap(), absent);
    for (key, bad) in [
        ("captured_at", json!(-1)),
        ("captured_at", json!(true)),
        ("captured_at", json!(u64::MAX)),
        ("schema_version", json!("wrong")),
        ("snapshot_kind", json!("wrong")),
        ("capture_mode", json!("wrong")),
        ("snapshot_id", json!("")),
        ("fixture_key", json!(" ")),
    ] {
        let mut bad_request = v.clone();
        bad_request[key] = bad;
        assert_eq!(parse(&bad_request, &a), Err("INVALID_SCHEMA"));
    }
    v["account_key"]["account"] = json!("B");
    assert_eq!(parse(&v, &a), Err("ACCOUNT_KEY_MISMATCH"));
    let mut store = Store::new(a.key.clone());
    let before = a.clone();
    let original = store.capture(&a, &absent).unwrap();
    for (key, value) in [
        ("fixture_key", json!("unknown")),
        ("snapshot_kind", json!("MARKET")),
        ("captured_at", json!(0)),
        ("capture_mode", json!("FROZEN_POLL_FIXTURE")),
    ] {
        let mut v = request();
        v[key] = value;
        let r = parse(&v, &a).unwrap();
        assert_eq!(store.capture(&a, &r), Err("SNAPSHOT_ID_CONFLICT"));
        v["snapshot_id"] = json!("new");
        assert_eq!(
            store.capture(&a, &parse(&v, &a).unwrap()),
            Err("INVALID_SCHEMA")
        );
    }
    assert_eq!(store.capture(&a, &absent).unwrap(), original);
    assert_eq!(a, before);
}
#[test]
fn every_frozen_payload_and_current_nonempty_rows_are_literal() {
    let a = owner();
    let before = a.clone();
    let cases = [
        (
            "POLL_Q1_S1",
            "TRADING",
            r#"{"available_equity":"100","equity":"100","outcome":"SUCCESS"}"#,
        ),
        (
            "POLL_Q2_S2",
            "TRADING",
            r#"{"available_equity":"110","equity":"110","outcome":"SUCCESS"}"#,
        ),
        (
            "POLL_EARN_ZERO",
            "EARN",
            r#"{"earn":"0","outcome":"SUCCESS"}"#,
        ),
        (
            "POLL_POSITIONS_EMPTY",
            "POSITIONS",
            r#"{"outcome":"SUCCESS","rows":[]}"#,
        ),
        (
            "POLL_OPEN_ORDERS_EMPTY",
            "OPEN_ORDERS",
            r#"{"outcome":"SUCCESS","rows":[]}"#,
        ),
        (
            "MARKET_GOLDEN_V1",
            "MARKET",
            r#"{"markets":[{"contract_value":"1","high_low_ratio":"0.1","instrument_code":1,"lot_size":"1","minimum_size":"1","price":"10","price_increment":"1","product_id":"P_A","state":"live"}]}"#,
        ),
    ];
    for (fixture, kind, expected) in cases.into_iter().chain(
        [
            ("POLL_EARN_FAILURE", "EARN"),
            ("POLL_TRADING_FAILURE", "TRADING"),
            ("POLL_POSITIONS_FAILURE", "POSITIONS"),
            ("POLL_OPEN_ORDERS_FAILURE", "OPEN_ORDERS"),
        ]
        .map(|(f, k)| {
            (
                f,
                k,
                r#"{"outcome":"FAILURE","reason":"SYNTHETIC_FAILURE"}"#,
            )
        }),
    ) {
        let mut v = request();
        v["capture_mode"] = json!("FROZEN_POLL_FIXTURE");
        v["snapshot_kind"] = json!(kind);
        v["fixture_key"] = json!(fixture);
        let fact = Store::new(a.key.clone())
            .capture(&a, &parse(&v, &a).unwrap())
            .unwrap();
        assert_eq!(fact.payload_json().unwrap().canonical().unwrap(), expected);
        envelope(&fact, expected);
        let output = fact.wire_json().unwrap();
        let serialized = output.canonical().unwrap();
        assert_eq!(serialized, fact.wire_json().unwrap().canonical().unwrap());
        assert!(
            !serialized.contains("captured_account_version")
                && !serialized.contains("continuation_id")
        );
        assert_eq!(a, before);
    }
    for (kind, expected) in [
        (
            "POSITIONS",
            r#"{"outcome":"SUCCESS","rows":[{"last_price":"50001","margin_mode":"cross","notional_usd":"1500.03","position_contracts":"3","product_id":"BTC-USDT-SWAP"}]}"#,
        ),
        (
            "OPEN_ORDERS",
            r#"{"outcome":"SUCCESS","rows":[{"client_order_id":"C1","created_at":500,"cumulative_filled_size_contracts":"1","limit_price":"50000.1","order_id":"O1","original_size_contracts":"2","product_id":"BTC-USDT-SWAP","side":"sell","state":"partially_filled"}]}"#,
        ),
    ] {
        let mut v = request();
        v["snapshot_kind"] = json!(kind);
        v["continuation_id"] = json!("Q");
        let mut store = Store::new(a.key.clone());
        let fact = store.capture(&a, &parse(&v, &a).unwrap()).unwrap();
        assert_eq!(fact.payload_json().unwrap().canonical().unwrap(), expected);
        envelope(&fact, expected);
        let mut detached = fact.wire_json().unwrap();
        let Json::Object(ref mut rows) = detached else {
            panic!("object")
        };
        assert_eq!(rows["captured_account_version"], number(0).unwrap());
        assert_eq!(rows["continuation_id"], string("Q"));
        rows.clear();
        assert_ne!(detached, fact.wire_json().unwrap());
        assert_eq!(store.lookup(&a.key, "S").unwrap(), fact);
        assert_eq!(a, before);
    }
}
