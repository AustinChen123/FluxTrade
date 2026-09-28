use self::identity::Reference;
use super::super::super::tests::{d, fixture};
use super::*;
use execution::commit::tests::{at, input};
mod closure;
fn projection(namespace: &'static str, id: &str, kind: &'static str) -> Projection {
    Projection {
        schema_version: "delivery_projection_v1".into(),
        reference: Reference {
            namespace,
            fact_id: id.into(),
        },
        kind,
        occurrence: 0,
        sequence: 0,
        visible_at: 0,
        continuation: None,
        transport: None,
    }
}
fn build(
    store: &mut Store,
    owner: &ScenarioAccount,
    snapshots: &snapshot::Store,
    p: &Projection,
) -> Result<Delivery, Fault> {
    let saved = owner.clone();
    let before = store.clone();
    let result = store.build(owner, snapshots, p);
    inspection::completion_tests::assert_inspection(owner);
    assert_eq!(owner, &saved);
    if result.is_err() {
        assert_eq!(store, &before);
    }
    result
}
#[test]
fn captured_snapshot_is_reused_and_identity_precedes_all_later_resolution() {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let (snapshots, fact) = snapshot::tests::delivery_fixture(&owner, true);
    let (empty, _) = snapshot::tests::delivery_fixture(&owner, false);
    owner
        .execute(&at(input(&owner, "O1", "X", "1", "50001"), 501))
        .unwrap();
    let mut store = Store::new(owner.key.clone());
    let mut p = projection("SNAPSHOT", "S", "TRADING_SNAPSHOT");
    p.continuation = Some("Q".into());
    let original = build(&mut store, &owner, &snapshots, &p).unwrap();
    assert_eq!(original.body, Body::Snapshot(Box::new(fact.unwrap())));
    assert_eq!(
        (original.snapshot_version, original.snapshot_as_of),
        (Some(0), Some(500))
    );
    assert_eq!(original.payload_digest, original.digest().unwrap());
    let mut detached = original.clone();
    detached.projection.visible_at = 99;
    assert_eq!(build(&mut store, &owner, &empty, &p), Ok(original.clone()));
    assert_ne!(detached, original);
    for field in 0..7 {
        let mut bad = p.clone();
        match field {
            0 => bad.visible_at = 1,
            1 => bad.sequence = 1,
            2 => bad.continuation = None,
            3 => bad.visible_at = -1,
            4 => bad.occurrence = 1,
            5 => bad.reference.fact_id = "missing".into(),
            _ => bad.kind = "EARN_SNAPSHOT",
        }
        let error = match field {
            0..=2 => "DELIVERY_ID_CONFLICT",
            3 => "INVALID_SCHEMA",
            _ => "UNKNOWN_RECEIPT_REFERENCE",
        };
        assert_eq!(build(&mut store, &owner, &empty, &bad), Err(error));
    }
    for (kind, continuation) in [
        ("EARN_SNAPSHOT", Some("Q")),
        ("TRADING_SNAPSHOT", None),
        ("TRADING_SNAPSHOT", Some("other")),
    ] {
        let mut bad = p.clone();
        bad.occurrence = 1;
        bad.kind = kind;
        bad.continuation = continuation.map(str::to_owned);
        assert_eq!(
            build(&mut store, &owner, &snapshots, &bad),
            Err("INVALID_SCHEMA")
        );
    }
    p.occurrence = 1;
    let repeated = build(&mut store, &owner, &snapshots, &p).unwrap();
    assert_ne!(original.delivery_id, repeated.delivery_id);
    assert_eq!(original.body, repeated.body);
}
#[test]
fn source_execution_and_transport_build_without_financial_or_time_effects() {
    let (mut seed, config, marks) = fixture();
    seed.orders.clear();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let (snapshots, _) = snapshot::tests::delivery_fixture(&owner, false);
    let intent = admission::fixture_intent(&owner, "I", Side::Long, d("2"), d("50000"));
    admission::fixture_admit(&mut owner, "A", 500, &intent).unwrap();
    let order = owner.orders.values().next().unwrap().facts.clone();
    let mut p = projection("SOURCE", "A", "TRANSPORT_ACK");
    p.transport = Some(Transport {
        route: "WS",
        operation: "ORDER",
        client: order.client_id,
        order: Some(order.order_id.clone()),
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    });
    let mut store = Store::new(owner.key.clone());
    let ack = build(&mut store, &owner, &snapshots, &p).unwrap();
    owner
        .execute(&at(input(&owner, &order.order_id, "X", "1", "50000"), 501))
        .unwrap();
    assert_eq!(build(&mut store, &owner, &snapshots, &p), Ok(ack));
    let execution = projection("SOURCE", "event-X", "EXECUTION_FACT");
    let original = build(&mut store, &owner, &snapshots, &execution).unwrap();
    assert_eq!(
        (original.snapshot_version, original.snapshot_as_of),
        (None, None)
    );
    use risk_transition::cancel::{EffectInput, Reason, RequestInput};
    owner
        .request_cancel(&RequestInput {
            stamp: source::stamp("C", 502, 40),
            targets: vec![(order.order_id.clone(), Reason::ExplicitScenario)],
        })
        .unwrap();
    owner
        .effect_cancel(&EffectInput {
            stamp: source::stamp("E", 503, 50),
            effects: vec![("C".into(), order.order_id, Reason::ExplicitScenario)],
        })
        .unwrap();
    let mut cancel = p.clone();
    cancel.reference.fact_id = "E".into();
    cancel.transport.as_mut().unwrap().operation = "CANCEL";
    let canceled = build(&mut store, &owner, &snapshots, &cancel).unwrap();
    assert_eq!(
        canceled.body,
        Body::Source(Payload::Transport(cancel.transport.unwrap()))
    );
    for field in 0..5 {
        let mut bad = p.clone();
        match field {
            0 => bad.transport.as_mut().unwrap().client = "unknown".into(),
            1 => bad.continuation = Some("Q".into()),
            2 => {
                bad.occurrence = 1;
                bad.continuation = Some("Q".into());
            }
            3 => {
                bad.occurrence = 1;
                bad.transport.as_mut().unwrap().operation = "CANCEL";
            }
            _ => bad.sequence = -1,
        }
        assert_eq!(
            build(&mut store, &owner, &snapshots, &bad),
            Err(if field < 2 {
                "DELIVERY_ID_CONFLICT"
            } else {
                "INVALID_SCHEMA"
            })
        );
    }
    owner.execution_receipts.clear(); // Exact duplicate must not re-resolve a receipt.
    assert_eq!(
        build(&mut store, &owner, &snapshots, &execution),
        Ok(original)
    );
}
