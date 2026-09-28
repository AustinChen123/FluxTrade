use super::*;
use serde_json::{json, Value};
const KEY: &str = r#"{"venue":"okx-scenario","environment":"test","account":"A"}"#;
fn session() -> Session {
    Session::new("SYNTHETIC_BTC_ETH_V1", KEY).unwrap()
}
fn value(reply: Reply) -> Value {
    serde_json::from_str(&reply.unwrap()).unwrap()
}
fn group(id: &str, at: i64) -> Value {
    json!({"schema_version":"scenario_group_v1","group_id":id,
        "account_key":serde_json::from_str::<Value>(KEY).unwrap(),"ordering_contract_id":"S_order_v1",
        "group_effective_at":at,"declared_member_count":1,"members":[{"kind":"INTENT",
        "stamp":{"event_id":id,"effective_at":at,"causal_parent_ids":[],"ordering_contract_id":"S_order_v1","scenario_ordinal":60},
        "payload":{"intent_id":id,"client_order_id":id,"config_id":"scenario-v1","product_id":"BTC-USDT-SWAP",
        "strategy_id":"S","side":"SHORT","order_type":"LIMIT","quantity_contracts":"1",
        "limit_price":"50000.1","reduce_only":true,"requested_at":at}}]})
}
#[test]
fn completed_bytes_survive_mutation_and_identity_is_owned_by_four_c() {
    let mut s = session();
    let g = group("G", 500);
    let original = s.apply_group(&g.to_string()).unwrap();
    assert_eq!(value(Ok(original.clone()))["classification"], "COMMITTED");
    let first = value(Ok(original.clone()));
    assert_eq!(first["account_version_before"], 0);
    assert_eq!(first["account_version_after"], 1);
    let key = ("S_order_v1".into(), "G".into());
    assert_eq!(s.completed[&key].0, s.owner.transition.groups[&key].digest);
    assert_eq!(s.completed[&key].1, original);
    let h = group("H", 501).to_string();
    let second = s.apply_group(&h).unwrap();
    let decoded = value(Ok(second.clone()));
    assert_eq!(decoded["account_version_before"], 1);
    assert_eq!(decoded["account_version_after"], 2);
    assert_eq!(s.completed[&("S_order_v1".into(), "H".into())].1, second);
    assert_eq!(s.owner.state_version, 2);
    let before = s.owner.clone();
    assert_eq!(s.apply_group(&g.to_string()), Ok(original.clone()));
    assert_eq!(s.owner, before);
    let mut detached = original.clone();
    detached.push('x');
    assert_eq!(s.apply_group(&g.to_string()), Ok(original));
    let mut conflict = g.clone();
    conflict["members"][0]["payload"]["quantity_contracts"] = json!("2");
    assert_eq!(
        value(s.apply_group(&conflict.to_string()))["classification"],
        "FAULT"
    );
    assert_eq!(s.completed.len(), 2);
    let after_conflict = s.owner.clone();
    assert_eq!(s.apply_group(&h), Ok(second));
    assert_eq!(s.owner, after_conflict);
    let mut rejected = session();
    let mut market = group("M", 500);
    market["members"][0]["payload"]["order_type"] = json!("MARKET");
    market["members"][0]["payload"]
        .as_object_mut()
        .unwrap()
        .remove("limit_price");
    assert_eq!(
        value(rejected.apply_group(&market.to_string()))["classification"],
        "FAULT"
    );
    assert_eq!(rejected.completed.len(), 1);
    assert!(rejected.inspect_state().is_ok());
    let saved = rejected.apply_group(&market.to_string()).unwrap();
    assert_eq!(value(Ok(saved))["failure"], "UNSUPPORTED_ORDER_TYPE");
    let mut invalid = session();
    let mut shape = group("bad", 500);
    shape["declared_member_count"] = json!(2);
    assert_eq!(
        value(invalid.apply_group(&shape.to_string()))["classification"],
        "FAULT"
    );
    assert!(invalid.completed.is_empty());
    let mut reverse = session();
    let reverse_id = "reverse-execution-cancel-effective-v1";
    let ordering = "S_order_v1_reverse_execution_cancel_effective";
    let mut g = group(reverse_id, 500);
    g["ordering_contract_id"] = json!(ordering);
    g["members"][0]["stamp"]["ordering_contract_id"] = json!(ordering);
    let bytes = reverse.apply_group(&g.to_string()).unwrap();
    assert_eq!(value(Ok(bytes.clone()))["classification"], "COMMITTED");
    assert_eq!(
        reverse.completed[&(ordering.into(), reverse_id.into())].1,
        bytes
    );
    assert_eq!(reverse.apply_group(&g.to_string()), Ok(bytes));
}
#[test]
fn actual_prefix_and_terminal_preflight_are_retained_without_fabrication() {
    for fault in [false, true] {
        let (owner, completion) = wire::result::tests::run(fault);
        let mut s = session();
        s.owner = owner;
        let g = completion.group.clone();
        let expected =
            wire::result::GroupResult::from_outcome(&s.owner, &g.group_id, 0, Ok(&completion))
                .unwrap()
                .canonical()
                .unwrap();
        assert_eq!(
            value(Ok(expected.clone()))["classification"],
            if fault { "FAULT" } else { "REJECTED" }
        );
        let before = s.owner.clone();
        assert_eq!(
            s.group_result(&g, 0, Ok(group::Applied::Fresh(completion))),
            Ok(expected.clone())
        );
        assert_eq!(s.owner, before);
        let duplicate = s.owner.apply_group_observed(&g, |_| Ok(()));
        assert_eq!(s.group_result(&g, 999, duplicate), Ok(expected));
        if !fault {
            let result = value(s.apply_group(&group("terminal", 502).to_string()));
            assert_eq!(result["classification"], "REJECTED");
            assert_eq!(result["failure"], "RUN_TERMINAL");
            assert_eq!(s.completed.len(), 1);
            assert_eq!(s.owner, before);
        }
    }
}
#[test]
fn ordinary_errors_and_detached_projections_leave_financial_owner_unchanged() {
    let mut s = session();
    let before = s.owner.clone();
    assert_eq!(
        s.apply_group("{"),
        Err(BoundaryError::Input("INVALID_JSON"))
    );
    assert_eq!(
        s.capture_snapshot("[]"),
        Err(BoundaryError::Input("INVALID_SCHEMA"))
    );
    let snapshot = json!({"schema_version":"snapshot_request_v1","account_key":serde_json::from_str::<Value>(KEY).unwrap(),
        "snapshot_id":"S","snapshot_kind":"TRADING","capture_mode":"OWNER_CURRENT","captured_at":500});
    let original = s.capture_snapshot(&snapshot.to_string()).unwrap();
    assert_eq!(
        s.capture_snapshot(&snapshot.to_string()),
        Ok(original.clone())
    );
    let mut conflict = snapshot.clone();
    conflict["captured_at"] = json!(501);
    assert_eq!(
        s.capture_snapshot(&conflict.to_string()),
        Err(BoundaryError::Conflict("SNAPSHOT_ID_CONFLICT"))
    );
    let mut projection = json!({"schema_version":"delivery_projection_v1","reference":{"namespace":"SNAPSHOT","fact_id":"missing"},
        "payload_kind":"TRADING_SNAPSHOT","occurrence_index":0,"schedule_sequence":1,"visible_at":502});
    assert_eq!(
        s.build_delivery(&projection.to_string()),
        Err(BoundaryError::Lookup("UNKNOWN_RECEIPT_REFERENCE"))
    );
    projection["reference"]["fact_id"] = json!("S");
    let delivery = s.build_delivery(&projection.to_string()).unwrap();
    assert_eq!(s.build_delivery(&projection.to_string()), Ok(delivery));
    projection["visible_at"] = json!(503);
    assert_eq!(
        s.build_delivery(&projection.to_string()),
        Err(BoundaryError::Conflict("DELIVERY_ID_CONFLICT"))
    );
    let inspection = s.inspect_state().unwrap();
    assert_eq!(s.inspect_state(), Ok(inspection));
    let mut detached = value(Ok(original));
    detached["immutable_payload"] = json!({"poison":true});
    assert_ne!(value(s.capture_snapshot(&snapshot.to_string())), detached);
    assert_eq!(s.owner, before);
    let mut other_key: Value = serde_json::from_str(KEY).unwrap();
    other_key["account"] = json!("B");
    let mut other = Session::new("SYNTHETIC_BTC_ETH_V1", &other_key.to_string()).unwrap();
    let other_before = other.owner.clone();
    assert_eq!(
        other.capture_snapshot(&snapshot.to_string()),
        Err(BoundaryError::Input("ACCOUNT_KEY_MISMATCH"))
    );
    assert_eq!(
        other.build_delivery(&projection.to_string()),
        Err(BoundaryError::Lookup("UNKNOWN_RECEIPT_REFERENCE"))
    );
    assert_eq!(other.owner, other_before);
    assert_ne!(other.inspect_state(), s.inspect_state());
}
