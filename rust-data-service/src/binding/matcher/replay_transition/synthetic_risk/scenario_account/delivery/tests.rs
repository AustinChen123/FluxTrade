use super::super::tests::{d, fixture};
use super::*;
use execution::commit::tests::{at, input};
use risk_transition::cancel::{EffectInput, Reason, RequestInput};
mod matrix;

fn transport(order: &SeedOrder) -> Transport {
    Transport {
        route: "REST",
        operation: "ORDER",
        client: order.client_id.clone(),
        order: Some(order.order_id.clone()),
        code: "0".into(),
        message: None,
        product: Some(order.product.canonical_id().into()),
        side: Some(order.side),
        price: Some(order.price),
        size: Some(order.original),
    }
}
fn project(
    owner: &ScenarioAccount,
    event: &str,
    kind: &str,
    t: Option<&Transport>,
) -> Result<Payload, Fault> {
    let before = owner.clone();
    let result = resolve(owner, &group::Reference::Source(event.into()), kind, t);
    assert_eq!(owner, &before);
    result
}
#[test]
fn receipt_projection_survives_real_fills_and_cancel_with_closed_matching() {
    let (mut seed, config, marks) = fixture();
    seed.orders.clear();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let intent = admission::fixture_intent(&owner, "I", Side::Long, d("2"), d("50000"));
    assert_eq!(
        admission::fixture_admit(&mut owner, "A", 500, &intent)
            .unwrap()
            .0,
        "accepted"
    );
    let id = owner.orders.keys().next().unwrap().clone();
    let original = owner.orders[&id].facts.clone();
    let ack = transport(&original);
    let expected = project(&owner, "A", "TRANSPORT_ACK", Some(&ack)).unwrap();
    owner
        .execute(&at(input(&owner, &id, "X", "1", "49999"), 501))
        .unwrap();
    let execution = project(&owner, "event-X", "EXECUTION_FACT", None).unwrap();
    let request = RequestInput {
        stamp: source::stamp("C", 502, 40),
        targets: vec![(id.clone(), Reason::ExplicitScenario)],
    };
    let requested = owner.request_cancel(&request).unwrap();
    let effect = EffectInput {
        stamp: source::stamp("E", 503, 50),
        effects: vec![("C".into(), id.clone(), Reason::ExplicitScenario)],
    };
    owner.effect_cancel(&effect).unwrap();
    assert_eq!(
        project(&owner, "A", "TRANSPORT_ACK", Some(&ack)),
        Ok(expected)
    );
    assert_eq!(
        project(&owner, "event-X", "EXECUTION_FACT", None),
        Ok(execution.clone())
    );
    let mut cancel = ack.clone();
    cancel.operation = "CANCEL";
    cancel.order = None;
    assert_eq!(
        project(&owner, "E", "TRANSPORT_ACK", Some(&cancel)),
        Ok(Payload::Transport(cancel.clone()))
    );
    assert_eq!(
        owner.cancel_facts.actions[&requested.receipts[0].action_ids[1]].phase,
        2
    );
    let (_, marks) = owner.btc_context().unwrap();
    let context = context::tests::activation(&owner, 504, context::tests::mark_rows(marks, 504));
    owner.activate_context(&context).unwrap();
    for event in ["A", "event-X", "C", "E", "activate", "missing"] {
        for (kind, transport) in [
            ("EXECUTION_FACT", None),
            ("EXECUTION_FACT", Some(&ack)),
            ("TRANSPORT_ACK", None),
            ("TRANSPORT_ACK", Some(&ack)),
            ("TRANSPORT_ACK", Some(&cancel)),
        ] {
            let expected = match (event, kind, transport.map(|t| t.operation)) {
                ("event-X", "EXECUTION_FACT", None) => Ok(execution.clone()),
                ("A", "TRANSPORT_ACK", Some("ORDER")) => Ok(Payload::Transport(ack.clone())),
                ("E", "TRANSPORT_ACK", Some("CANCEL")) => Ok(Payload::Transport(cancel.clone())),
                ("missing", _, _) => Err("UNKNOWN_RECEIPT_REFERENCE"),
                _ => Err("INVALID_SCHEMA"),
            };
            assert_eq!(project(&owner, event, kind, transport), expected);
        }
    }
    for field in 0..8 {
        let mut bad = ack.clone();
        match field {
            0 => bad.client = "wrong".into(),
            1 => bad.order = Some("wrong".into()),
            2 => bad.product = Some("wrong".into()),
            3 => bad.side = Some(Side::Short),
            4 => bad.price = Some(d("1")),
            5 => bad.size = Some(d("1")),
            6 => bad.operation = "CANCEL",
            _ => bad.product = None,
        }
        assert_eq!(
            project(&owner, "A", "TRANSPORT_ACK", Some(&bad)),
            Err("INVALID_SCHEMA")
        );
    }
    let mut ws = ack;
    ws.route = "WS";
    ws.product = None;
    ws.side = None;
    ws.price = None;
    ws.size = None;
    assert!(project(&owner, "A", "TRANSPORT_ACK", Some(&ws)).is_ok());
    let action = requested.receipts[0].action_ids[1];
    owner
        .cancel_facts
        .assert_ambiguous_delivery_rejected("E", &cancel);
    owner.cancel_facts.actions.remove(&action);
    assert_eq!(
        project(&owner, "E", "TRANSPORT_ACK", Some(&cancel)),
        Err("INVALID_SCHEMA")
    );
    // Literal independent execution encoding locks limit versus fill and commit-time remainder.
    let mut bytes = Vec::new();
    for s in [
        "vector",
        "EXECUTION_FACT",
        &id,
        "client",
        "client",
        "BTC-USDT-SWAP",
        "partially_filled",
        "buy",
        "50000",
        "49999",
        "2",
        "1",
        "0.01",
    ] {
        bytes.extend_from_slice(&(s.len() as i64).to_be_bytes());
        bytes.extend_from_slice(s.as_bytes());
    }
    bytes.extend_from_slice(&501_i64.to_be_bytes());
    bytes.extend_from_slice(&2_i64.to_be_bytes());
    for s in ["spec-v1", "tier-v1"] {
        bytes.extend_from_slice(&(s.len() as i64).to_be_bytes());
        bytes.extend_from_slice(s.as_bytes());
    }
    let mut encoded = Encoding::new("vector");
    execution.encode(&mut encoded).unwrap();
    assert_eq!(
        encoded.finish().as_slice(),
        ring::digest::digest(&ring::digest::SHA256, &bytes).as_ref()
    );
}
