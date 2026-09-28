use super::super::super::super::tests::fixture;
use super::*;
use execution::commit::tests::{at, input};
use serde_json::{json, Value};

fn output(d: &Delivery, owner: &ScenarioAccount) -> Value {
    let before = owner.clone();
    let saved = d.clone();
    let text = d.wire_json().unwrap().canonical().unwrap();
    assert_eq!(text, d.wire_json().unwrap().canonical().unwrap());
    assert_eq!(*d, saved);
    assert_eq!(*owner, before);
    serde_json::from_str(&text).unwrap()
}
fn delivery(owner: &ScenarioAccount, body: Body) -> Delivery {
    Delivery {
        account: owner.key.clone(),
        projection: decode_projection(r#"{"schema_version":"delivery_projection_v1","reference":{"namespace":"SOURCE","fact_id":"event-X"},"payload_kind":"EXECUTION_FACT","occurrence_index":2,"schedule_sequence":3,"visible_at":504}"#).unwrap(),
        delivery_id: [1; 32], payload_digest: [2; 32], body,
        snapshot_version: None, snapshot_as_of: None,
    }
}
#[test]
fn receipt_envelope_is_complete_and_survives_later_fill() {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    owner
        .execute(&at(input(&owner, "O1", "X", "0.5", "50001"), 501))
        .unwrap();
    let payload = source(&owner, "event-X", "EXECUTION_FACT", None).unwrap();
    let delivery = delivery(&owner, Body::Source(payload));
    let expected = json!({"account_key":{"venue":"okx-scenario","environment":"test","account":"A"},
        "delivery_id":"01".repeat(32),"payload_digest":"02".repeat(32),"source_fact_id":"event-X",
        "source_namespace":"SOURCE","payload_kind":"EXECUTION_FACT","occurrence_index":2,
        "schedule_sequence":3,"visible_at":504,"immutable_payload":{
            "order_id":"O1","owner_client_order_id":"C1","policy_client_order_id":"C1",
            "product_id":"BTC-USDT-SWAP","state":"partially_filled","side":"sell",
            "limit_price":"50000.1","fill_price":"50001","original_size_contracts":"2",
            "cumulative_filled_size_contracts":"1.5","contract_value":"0.01",
            "execution_effective_at":501,"commit_account_version":1,"spec_version":"spec-v1","rule_data_version":"tier-v1"}});
    assert_eq!(output(&delivery, &owner), expected);
    owner
        .execute(&at(input(&owner, "O1", "Y", "0.5", "50001"), 502))
        .unwrap();
    assert_eq!(output(&delivery, &owner), expected);
    let full = delivery::source(&owner, "event-Y", "EXECUTION_FACT", None).unwrap();
    let mut changed = delivery.clone();
    changed.body = Body::Source(full);
    assert_eq!(
        output(&changed, &owner)["immutable_payload"]["state"],
        "filled"
    );
    let mut detached = delivery.wire_json().unwrap();
    detached = fields(vec![("poison", detached)]);
    assert_ne!(detached, delivery.wire_json().unwrap());
    assert_eq!(output(&delivery, &owner), expected);
}
#[test]
fn transport_options_and_captured_snapshot_are_copied_verbatim() {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let value = json!({"route":"WS","operation":"CANCEL","client_order_id":"C","order_id":"O",
        "code":"0","message":"雪\n\"\\","product_id":"observation","side":"sell","limit_price":"10.25","size_contracts":"3"});
    let transport = super::super::transport(
        &super::super::super::super::wire::decode(&value.to_string()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&transport.wire_json().canonical().unwrap()).unwrap(),
        value
    );
    for field in [
        "order_id",
        "message",
        "product_id",
        "side",
        "limit_price",
        "size_contracts",
    ] {
        let mut absent = value.clone();
        absent.as_object_mut().unwrap().remove(field);
        let t = super::super::transport(
            &super::super::super::super::wire::decode(&absent.to_string()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&t.wire_json().canonical().unwrap()).unwrap(),
            absent
        );
    }
    let mut d = delivery(&owner, Body::Source(Payload::Transport(transport)));
    d.projection.kind = "TRANSPORT_ACK";
    assert_eq!(output(&d, &owner)["immutable_payload"], value);
    for route in ["REST", "WS"] {
        for operation in ["ORDER", "CANCEL"] {
            for side in ["buy", "sell"] {
                let mut expected = value.clone();
                expected["route"] = json!(route);
                expected["operation"] = json!(operation);
                expected["side"] = json!(side);
                let t = super::super::transport(
                    &super::super::super::super::wire::decode(&expected.to_string()).unwrap(),
                )
                .unwrap();
                d.body = Body::Source(Payload::Transport(t));
                assert_eq!(output(&d, &owner)["immutable_payload"], expected);
            }
        }
    }
    let (_, fact) = snapshot::tests::delivery_fixture(&owner, true);
    let fact = fact.unwrap();
    d.body = Body::Snapshot(Box::new(fact.clone()));
    d.projection.reference.namespace = "SNAPSHOT";
    d.projection.reference.fact_id = "S".into();
    d.projection.kind = "TRADING_SNAPSHOT";
    d.projection.continuation = Some("Q".into());
    d.snapshot_version = Some(0);
    d.snapshot_as_of = Some(500);
    let saved = output(&d, &owner);
    assert_eq!(saved["snapshot_version"], 0);
    assert_eq!(saved["snapshot_as_of"], 500);
    assert_eq!(saved["continuation_id"], "Q");
    assert_eq!(
        saved["immutable_payload"],
        serde_json::from_str::<Value>(&fact.payload_json().unwrap().canonical().unwrap()).unwrap()
    );
    owner
        .execute(&at(input(&owner, "O1", "X", "1", "50001"), 501))
        .unwrap();
    assert_eq!(output(&d, &owner), saved);
    let before = owner.clone();
    let fact = snapshot::tests::frozen_fixture(&owner);
    assert_eq!(owner, before);
    let (version, as_of) = fact.delivery_metadata("EARN_SNAPSHOT", None).unwrap();
    d.snapshot_version = version;
    d.snapshot_as_of = Some(as_of);
    d.projection.continuation = None;
    d.projection.kind = "EARN_SNAPSHOT";
    d.projection.reference.fact_id = "F".into();
    d.body = Body::Snapshot(Box::new(fact));
    let frozen = output(&d, &owner);
    assert_eq!(frozen["snapshot_as_of"], 90);
    assert!(frozen.get("snapshot_version").is_none());
    assert!(frozen.get("continuation_id").is_none());
    assert_eq!(
        frozen["immutable_payload"],
        json!({"outcome":"SUCCESS","earn":"0"})
    );
}

#[test]
fn golden_source_delivery_preserves_both_client_identities() {
    let (seed, _, _) = fixture();
    let mut owner = super::super::super::super::wire::profiles::construct(
        "SYNTHETIC_GOLDEN_CANCEL_V1",
        seed.key,
    )
    .unwrap();
    admission::inspection_tests::golden_admit(&mut owner);
    let id = owner.orders.keys().next().unwrap().clone();
    owner.execute(&input(&owner, &id, "G", "4", "10")).unwrap();
    let before = owner.clone();
    let payload = source(&owner, "event-G", "EXECUTION_FACT", None).unwrap();
    assert_eq!(owner, before);
    let mut d = delivery(&owner, Body::Source(payload));
    d.projection.reference.fact_id = "event-G".into();
    let actual = output(&d, &owner);
    assert_eq!(actual["immutable_payload"]["owner_client_order_id"], "C");
    assert_eq!(
        actual["immutable_payload"]["policy_client_order_id"],
        "0000015000"
    );
    assert_eq!(owner, before);
}
