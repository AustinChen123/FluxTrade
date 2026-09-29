use super::*;
use execution::commit::{tests::configured_input, Reply};
use risk_transition::cancel::{EffectInput, Reason, RequestInput};

fn owner(account: &str, cash: &str) -> ScenarioAccount {
    let (mut seed, mut products) = crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::configured_tests::input(2);
    seed.key.account = account.into();
    seed.cash = d(cash);
    products[0].taker_fee = d("0.002");
    products[1].taker_fee = d("0.003");
    let prototype = fixture().0.orders[0].clone();
    for (order, intent, client, product) in [
        (
            "SAME-P-ORDER",
            "SAME-P-INTENT",
            "SAME-P-CLIENT",
            &products[0].product,
        ),
        (
            "SAME-Q-ORDER",
            "SAME-Q-INTENT",
            "SAME-Q-CLIENT",
            &products[1].product,
        ),
    ] {
        seed.orders.push(SeedOrder {
            order_id: order.into(),
            intent_id: intent.into(),
            client_id: client.into(),
            product: ProfileProduct::BtcEth(product.clone()),
            side: Side::Long,
            price: d("100"),
            reduce_only: false,
            original: d("1"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "OPEN".into(),
            ..prototype.clone()
        });
    }
    ScenarioAccount::from_configured(&seed, d("10"), products).unwrap()
}

fn lifecycle(owner: &mut ScenarioAccount) {
    let fill = configured_input(owner, "SAME-P-ORDER", "SAME-X", "0.5", "100", 501);
    assert!(matches!(owner.execute(&fill), Ok(Reply::Committed { .. })));
    owner
        .request_cancel(&RequestInput {
            stamp: source::stamp("SAME-C", 502, 40),
            targets: vec![("SAME-P-ORDER".into(), Reason::ExplicitScenario)],
        })
        .unwrap();
    owner
        .effect_cancel(&EffectInput {
            stamp: source::stamp("SAME-E", 503, 50),
            effects: vec![(
                "SAME-C".into(),
                "SAME-P-ORDER".into(),
                Reason::ExplicitScenario,
            )],
        })
        .unwrap();
}

#[test]
fn configured_source_deliveries_are_account_scoped_and_read_only() {
    let mut a = owner("ACCOUNT-A", "100");
    let mut b = owner("ACCOUNT-B", "200");
    assert_ne!(a.key, b.key);
    assert_eq!(a.config_id, b.config_id);
    assert_eq!(
        a.orders["SAME-P-ORDER"].facts.client_id,
        b.orders["SAME-P-ORDER"].facts.client_id
    );
    lifecycle(&mut a);
    lifecycle(&mut b);
    let owners = (a.clone(), b.clone());
    let (snap_a, _) = snapshot::tests::delivery_fixture(&a, false);
    let (snap_b, _) = snapshot::tests::delivery_fixture(&b, false);
    let snapshots = (snap_a.clone(), snap_b.clone());
    let mut store_a = Store::new(a.key.clone());
    let mut store_b = Store::new(b.key.clone());
    let execution = projection("SOURCE", "event-SAME-X", "EXECUTION_FACT");
    let mut ack = projection("SOURCE", "SAME-E", "TRANSPORT_ACK");
    ack.visible_at = 510;
    ack.transport = Some(Transport {
        route: "WS",
        operation: "CANCEL",
        client: "SAME-P-CLIENT".into(),
        order: Some("SAME-P-ORDER".into()),
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    });

    let a_execution = build(&mut store_a, &a, &snap_a, &execution).unwrap();
    let a_ack = build(&mut store_a, &a, &snap_a, &ack).unwrap();
    let b_execution = build(&mut store_b, &b, &snap_b, &execution).unwrap();
    let b_ack = build(&mut store_b, &b, &snap_b, &ack).unwrap();
    assert_eq!(a_execution.account, a.key);
    assert_eq!(b_execution.account, b.key);
    assert_eq!(a_ack.account, a.key);
    assert_eq!(b_ack.account, b.key);
    assert_ne!(a_execution.delivery_id, b_execution.delivery_id);
    assert_ne!(a_ack.delivery_id, b_ack.delivery_id);
    assert_eq!(a_execution.projection, b_execution.projection);
    assert_eq!(a_ack.projection, b_ack.projection);
    assert_eq!(a_ack.body, b_ack.body);
    assert_eq!((a.clone(), b.clone()), owners);
    assert_eq!((snap_a.clone(), snap_b.clone()), snapshots);

    let stores = (store_a.clone(), store_b.clone());
    assert_eq!(
        build(&mut store_a, &a, &snap_a, &execution),
        Ok(a_execution)
    );
    assert_eq!(build(&mut store_a, &a, &snap_a, &ack), Ok(a_ack));
    assert_eq!(
        build(&mut store_b, &b, &snap_b, &execution),
        Ok(b_execution)
    );
    assert_eq!(build(&mut store_b, &b, &snap_b, &ack), Ok(b_ack));
    assert_eq!((store_a.clone(), store_b.clone()), stores);
    assert_eq!((a.clone(), b.clone()), owners);
    assert_eq!((snap_a.clone(), snap_b.clone()), snapshots);

    assert_eq!(
        build(&mut store_a, &b, &snap_b, &execution),
        Err("ACCOUNT_KEY_MISMATCH")
    );
    assert_eq!(
        build(&mut store_b, &a, &snap_a, &execution),
        Err("ACCOUNT_KEY_MISMATCH")
    );
    assert_eq!((store_a, store_b), stores);
    assert_eq!((snap_a, snap_b), snapshots);
    assert_eq!((a.clone(), b.clone()), owners);
}
