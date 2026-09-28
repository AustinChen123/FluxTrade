use super::*;
use serde_json::{json, Value};

fn request() -> Value {
    json!({"schema_version":"delivery_projection_v1","reference":{"namespace":"SOURCE","fact_id":"unknown"},
        "payload_kind":"TRANSPORT_ACK","occurrence_index":0,"schedule_sequence":1,"visible_at":0,
        "transport":{"route":"REST","operation":"ORDER","client_order_id":"C","code":"  arbitrary ",
            "product_id":"OBSERVATION_ONLY","side":"buy","limit_price":"10.25","size_contracts":"3"}})
}
fn parse(v: &Value) -> Result<Projection, Fault> {
    let (seed, config, marks) = super::super::super::tests::fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let before = owner.clone();
    let result = decode_projection(&v.to_string());
    assert_eq!(owner, before);
    result
}

#[test]
fn every_decoder_field_rejects_wrong_types_without_masking_optional_values() {
    let mut v = request();
    v["transport"]["route"] = json!("WS");
    v["transport"]["operation"] = json!("CANCEL");
    v["transport"]["order_id"] = json!("O");
    v["transport"]["message"] = json!(" message ");
    v["continuation_id"] = json!("Q");
    assert!(parse(&v).is_ok());
    let optional = [
        "continuation_id",
        "order_id",
        "message",
        "product_id",
        "side",
        "limit_price",
        "size_contracts",
    ];
    for path in ["", "/reference", "/transport"] {
        for wrong in [Value::Null, json!(true), json!(0), json!("text"), json!([])] {
            let mut bad = v.clone();
            *bad.pointer_mut(path).unwrap() = wrong;
            assert_eq!(parse(&bad), Err("INVALID_SCHEMA"), "container {path}");
        }
        for (key, original) in v.pointer(path).unwrap().as_object().unwrap() {
            if original.is_object() {
                continue;
            }
            for wrong in [
                Value::Null,
                json!(true),
                json!(0),
                json!("text"),
                json!([]),
                json!({}),
            ] {
                if (original.is_string() && wrong.is_string())
                    || (original.is_number() && wrong.is_number())
                {
                    continue;
                }
                let mut bad = v.clone();
                bad.pointer_mut(path).unwrap()[key] = wrong.clone();
                if optional.contains(&key.as_str()) && wrong.is_null() {
                    let with_null = parse(&bad).unwrap();
                    bad.pointer_mut(path)
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .remove(key);
                    assert_eq!(parse(&bad).unwrap(), with_null);
                } else {
                    assert_eq!(parse(&bad), Err("INVALID_SCHEMA"), "{path}/{key}: {wrong}");
                }
            }
        }
    }
    for path in [
        "/reference/fact_id",
        "/continuation_id",
        "/transport/client_order_id",
        "/transport/order_id",
        "/transport/product_id",
    ] {
        for bad_id in ["", " ", " x", "x "] {
            let mut bad = v.clone();
            *bad.pointer_mut(path).unwrap() = json!(bad_id);
            assert_eq!(parse(&bad), Err("INVALID_SCHEMA"), "{path}: {bad_id:?}");
        }
    }
    for path in ["/transport/limit_price", "/transport/size_contracts"] {
        for wrong in [json!(10), json!("10.250"), json!("3.0")] {
            let mut bad = v.clone();
            *bad.pointer_mut(path).unwrap() = wrong;
            assert_eq!(parse(&bad), Err("INVALID_SCHEMA"), "{path}");
        }
    }
    v["payload_kind"] = json!("EXECUTION_FACT");
    v["transport"] = Value::Null;
    let with_null = parse(&v).unwrap();
    v.as_object_mut().unwrap().remove("transport");
    assert_eq!(parse(&v).unwrap(), with_null);
}

#[test]
fn nested_schema_and_closed_shape_matrix() {
    let v = request();
    for path in ["", "/reference", "/transport"] {
        let object = v.pointer(path).unwrap().as_object().unwrap();
        for key in object.keys() {
            let mut bad = v.clone();
            bad.pointer_mut(path)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(key);
            assert_eq!(parse(&bad), Err("INVALID_SCHEMA"), "{path}/{key}");
        }
        let mut extra = v.clone();
        extra.pointer_mut(path).unwrap()["extra"] = json!(0);
        assert_eq!(parse(&extra), Err("INVALID_SCHEMA"));
        let original = v.pointer(path).unwrap().to_string();
        let duplicate = format!("{{{},{}", &original[1..original.len() - 1], &original[1..]);
        assert_eq!(
            decode_projection(&v.to_string().replacen(&original, &duplicate, 1)),
            Err("INVALID_SCHEMA")
        );
    }
    for path in [
        "/schema_version",
        "/reference/namespace",
        "/payload_kind",
        "/transport/route",
        "/transport/operation",
        "/transport/side",
    ] {
        let mut bad = v.clone();
        *bad.pointer_mut(path).unwrap() = json!("unknown");
        assert_eq!(parse(&bad), Err("INVALID_SCHEMA"));
    }
    for path in ["/occurrence_index", "/schedule_sequence", "/visible_at"] {
        for invalid in [json!(-1), json!(true), json!("1"), json!(u64::MAX)] {
            let mut bad = v.clone();
            *bad.pointer_mut(path).unwrap() = invalid;
            assert_eq!(parse(&bad), Err("INVALID_SCHEMA"));
        }
    }
    for route in ["REST", "WS"] {
        for operation in ["ORDER", "CANCEL"] {
            let mut shape = v.clone();
            shape["transport"] =
                json!({"route":route,"operation":operation,"client_order_id":"C","code":""});
            assert_eq!(
                parse(&shape).is_ok(),
                route != "REST" || operation != "ORDER"
            );
        }
    }
}
#[test]
fn optionals_values_and_postduplicate_errors_are_preserved() {
    let mut v = request();
    let p = parse(&v).unwrap();
    let t = p.transport.unwrap();
    assert_eq!(
        (t.product.as_deref(), t.side, t.price, t.size),
        (
            Some("OBSERVATION_ONLY"),
            Some(Side::Long),
            Some(Decimal::new(1025, 2)),
            Some(Decimal::from(3))
        )
    );
    assert_eq!(t.code, "  arbitrary ");
    v["transport"] = json!({"route":"WS","operation":"CANCEL","client_order_id":"C","code":"0"});
    let absent = parse(&v).unwrap();
    for field in [
        "order_id",
        "message",
        "product_id",
        "side",
        "limit_price",
        "size_contracts",
    ] {
        v["transport"][field] = Value::Null;
    }
    v["continuation_id"] = Value::Null;
    assert_eq!(parse(&v).unwrap(), absent);
    v["transport"]["order_id"] = json!("O");
    v["transport"]["message"] = json!(" message ");
    v["transport"]["side"] = json!("sell");
    v["continuation_id"] = json!("source-continuation-is-deferred");
    let p = parse(&v).unwrap();
    let t = p.transport.unwrap();
    assert_eq!(
        (t.order.as_deref(), t.message.as_deref(), t.side),
        (Some("O"), Some(" message "), Some(Side::Short))
    );
    assert_eq!(
        p.continuation.as_deref(),
        Some("source-continuation-is-deferred")
    );
    for namespace in ["SOURCE", "SNAPSHOT", "LIQUIDATION"] {
        for kind in [
            "EXECUTION_FACT",
            "TRANSPORT_ACK",
            "MARKET_SNAPSHOT",
            "EARN_SNAPSHOT",
            "TRADING_SNAPSHOT",
            "POSITION_SNAPSHOT",
            "OPEN_ORDER_SNAPSHOT",
        ] {
            let mut shape = v.clone();
            shape["reference"] = json!({"namespace":namespace,"fact_id":if namespace == "LIQUIDATION" {"ab".repeat(32)} else {"unknown".into()}});
            shape["payload_kind"] = json!(kind);
            if kind != "TRANSPORT_ACK" {
                shape["transport"] = Value::Null;
            }
            assert!(parse(&shape).is_ok()); // Lookup, kind/matching and continuation follow duplicate classification.
        }
    }
    for invalid in ["AB".repeat(32), "ab".repeat(31), "gg".repeat(32)] {
        v["reference"] = json!({"namespace":"LIQUIDATION","fact_id":invalid});
        assert_eq!(parse(&v), Err("INVALID_SCHEMA"));
    }
    v = request();
    v["payload_kind"] = json!("EXECUTION_FACT");
    assert_eq!(parse(&v), Err("INVALID_SCHEMA"));
    v["payload_kind"] = json!("TRANSPORT_ACK");
    v["transport"] = Value::Null;
    assert_eq!(parse(&v), Err("INVALID_SCHEMA"));
}
