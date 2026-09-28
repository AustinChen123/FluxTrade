use super::*;
use inspection::completion_tests::{assert_inspection, mixed_owner};
#[test]
fn terminal_delivery_and_receipts_ignore_later_current_order_state() {
    let mut owner = mixed_owner();
    let (snapshots, _) = snapshot::tests::delivery_fixture(&owner, false);
    let mut store = Store::new(owner.key.clone());
    let p = projection("SOURCE", "event-X", "EXECUTION_FACT");
    let original = build(&mut store, &owner, &snapshots, &p).unwrap();
    assert!(owner.is_terminal());
    let order = owner
        .orders
        .values()
        .find(|o| o.facts.client_id == "client")
        .unwrap();
    let mut ack = projection("SOURCE", "admission", "TRANSPORT_ACK");
    ack.transport = Some(Transport {
        route: "WS",
        operation: "ORDER",
        client: "client".into(),
        order: Some(order.facts.order_id.clone()),
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    });
    let accepted = build(&mut store, &owner, &snapshots, &ack).unwrap();
    for request in [&p, &ack] {
        let saved = owner.clone();
        let expected = if request.kind == "EXECUTION_FACT" {
            &original
        } else {
            &accepted
        };
        assert_eq!(
            build(&mut store, &owner, &snapshots, request).unwrap(),
            *expected
        );
        let mut new = request.clone();
        new.occurrence = 1;
        assert_eq!(
            build(&mut store, &owner, &snapshots, &new).unwrap().body,
            expected.body
        );
        assert_eq!(owner, saved);
    }
    owner.orders.clear(); // Test-only deletion equivalent, never a financial operation.
    let saved = owner.clone();
    for (mut request, expected) in [(p, original), (ack, accepted)] {
        request.occurrence = 2;
        assert_eq!(
            build(&mut store, &owner, &snapshots, &request)
                .unwrap()
                .body,
            expected.body
        );
    }
    assert_eq!(owner, saved);
    assert!(owner.is_terminal());
}
#[test]
fn isolated_accounts_and_deep_detached_payload_poison() {
    let (seed, config, marks) = fixture();
    let a = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let mut seed_b = seed.clone();
    seed_b.key.account = "B".into();
    let b = ScenarioAccount::from_seed(&seed_b, &config, &marks).unwrap();
    let owners = (a.clone(), b.clone());
    let (snap_a, fact) = snapshot::tests::delivery_fixture(&a, true);
    let (snap_b, _) = snapshot::tests::delivery_fixture(&b, true);
    let originals = (snap_a.clone(), snap_b.clone());
    let mut detached = fact.clone().unwrap();
    snapshot::tests::poison(&mut detached);
    assert_ne!(Some(detached), fact);
    assert_eq!(snap_a.lookup(&a.key, "S").unwrap(), fact.unwrap());
    let mut p = projection("SNAPSHOT", "S", "TRADING_SNAPSHOT");
    p.continuation = Some("Q".into());
    let mut store_a = Store::new(a.key.clone());
    let mut store_b = Store::new(b.key.clone());
    let original = build(&mut store_a, &a, &snap_a, &p).unwrap();
    let mut detached = original.clone();
    let Body::Snapshot(fact) = &mut detached.body else {
        panic!("snapshot")
    };
    snapshot::tests::poison(fact);
    assert_ne!(detached.body, original.body);
    assert_eq!(build(&mut store_a, &a, &snap_a, &p), Ok(original.clone()));
    let other = build(&mut store_b, &b, &snap_b, &p).unwrap();
    assert_ne!(original.delivery_id, other.delivery_id);
    let stores = (store_a.clone(), store_b.clone());
    assert_eq!(
        build(&mut store_a, &b, &snap_b, &p),
        Err("ACCOUNT_KEY_MISMATCH")
    );
    assert_eq!(
        build(&mut store_b, &a, &snap_a, &p),
        Err("ACCOUNT_KEY_MISMATCH")
    );
    p.occurrence = 1;
    assert_eq!(
        build(&mut store_a, &a, &snap_b, &p),
        Err("ACCOUNT_KEY_MISMATCH")
    );
    assert_eq!(
        build(&mut store_b, &b, &snap_a, &p),
        Err("ACCOUNT_KEY_MISMATCH")
    );
    assert_eq!(snap_a.lookup(&b.key, "S"), Err("ACCOUNT_KEY_MISMATCH"));
    assert_eq!(
        snap_b.lookup(&b.key, "missing"),
        Err("UNKNOWN_RECEIPT_REFERENCE")
    );
    assert_eq!((store_a, store_b), stores);
    assert_eq!((snap_a, snap_b), originals);
    assert_inspection(&a);
    assert_inspection(&b);
    assert_eq!((a, b), owners);
}
#[test]
fn a07a_delayed_version_one_fill_survives_rejection_and_version_two_context() {
    let (seed, _, _) = fixture();
    let mut owner = ScenarioAccount::synthetic_min_cash(seed.key).unwrap();
    let fill = at(input(&owner, "MIN-O1", "MIN-X1", "20", "50000"), 501);
    owner.execute(&fill).unwrap();
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.gross_realized,
            owner.state_version
        ),
        (d("990"), d("10"), d("0"), 1)
    );
    let before = owner.clone();
    let original = source(&owner, "event-MIN-X1", "EXECUTION_FACT", None).unwrap();
    assert_eq!(owner, before);
    let Payload::Execution(receipt, client) = &original else {
        panic!("execution")
    };
    let mut expected = inspection::receipt_vectors::Raw(Vec::new());
    expected.text("A07");
    for s in [
        "MIN-O1",
        "MIN-C1",
        "MIN-C1",
        "BTC-USDT-SWAP",
        "filled",
        "sell",
        "50000",
        "50000",
        "20",
        "20",
        "0.01",
    ] {
        expected.text(s);
    }
    expected.num(501);
    expected.num(1);
    expected.text("spec-v1");
    expected.text("tier-v1");
    let mut bytes = Encoding::new("A07");
    receipt.encode_delivery(&mut bytes, client).unwrap();
    assert_eq!(bytes.finish(), expected.finish());
    assert_inspection(&owner);
    admission::inspection_tests::reject_min_cash(&mut owner);
    assert_eq!(owner.state_version, 1);
    assert_inspection(&owner);
    let (_, marks) = owner.btc_context().unwrap();
    let eth: Vec<_> = marks
        .iter()
        .filter(|m| m.product == Product::Eth)
        .map(|m| (m.valid_from, m.price))
        .collect();
    assert_eq!(eth, vec![(0, d("100")), (503, d("101"))]);
    let context = context::tests::activation(&owner, 503, context::tests::mark_rows(marks, 503));
    owner.activate_context(&context).unwrap();
    assert_eq!(
        (owner.state_version, owner.cash, owner.fees),
        (2, d("990"), d("10"))
    );
    let (snapshots, _) = snapshot::tests::delivery_fixture(&owner, false);
    let mut store = Store::new(owner.key.clone());
    let mut p = projection("SOURCE", "event-MIN-X1", "EXECUTION_FACT");
    p.visible_at = 504;
    let saved = owner.clone();
    let delivered = build(&mut store, &owner, &snapshots, &p).unwrap();
    assert_eq!(delivered.body, Body::Source(original));
    assert_eq!(build(&mut store, &owner, &snapshots, &p), Ok(delivered));
    assert_eq!(owner.execution_receipts.len(), 1);
    assert_eq!(owner, saved);
}
