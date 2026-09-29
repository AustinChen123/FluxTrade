use super::*;
use execution::commit::{tests::configured_input, Reply};
use execution::{CommittedExecution, ExecutionCandidate};
use risk_transition::cancel::{EffectInput, Reason, RequestInput};

const IDS: [&str; 3] = ["CANCEL-FIRST", "CANCEL-MIDDLE", "CANCEL-LAST"];
const FEES: [&str; 3] = ["0.2", "0.4", "0.6"];

fn owner_with_three_products() -> (ScenarioAccount, Product) {
    let (mut seed, mut products) = crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::configured_tests::input(3);
    seed.cash = d("1000");
    let prototype = fixture().0.orders[0].clone();
    for (i, product) in products.iter_mut().enumerate() {
        product.taker_fee = d(["0.001", "0.002", "0.003"][i]);
        seed.orders.push(SeedOrder {
            order_id: IDS[i].into(),
            intent_id: format!("I-{}", IDS[i]),
            client_id: format!("C-{}", IDS[i]),
            product: ProfileProduct::BtcEth(product.product.clone()),
            side: Side::Long,
            price: d("100"),
            reduce_only: false,
            original: d("2"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("2"),
            status: "OPEN".into(),
            ..prototype.clone()
        });
    }
    let target = products[0].product.clone();
    (
        ScenarioAccount::from_configured(&seed, d("10"), products).unwrap(),
        target,
    )
}

fn committed(owner: &mut ScenarioAccount, candidate: &ExecutionCandidate) -> CommittedExecution {
    let Reply::Committed { receipt, .. } = owner.execute(candidate).unwrap() else {
        panic!("commit required")
    };
    receipt
}

fn assert_state(
    owner: &ScenarioAccount,
    product: &Product,
    filled: &str,
    remaining: &str,
    canceled: &str,
    status: &str,
    cash: &str,
    fees: &str,
    hold: &str,
    used: &str,
    available: &str,
    target_fee_hold: Option<&str>,
) {
    let facts = &owner.orders[IDS[0]].facts;
    assert_eq!(
        (
            facts.side,
            facts.original,
            facts.filled,
            facts.remaining,
            facts.canceled,
            facts.status.as_str()
        ),
        (
            Side::Long,
            d("2"),
            d(filled),
            d(remaining),
            d(canceled),
            status
        )
    );
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d(cash), d(fees), Decimal::ZERO)
    );
    if filled == "0" {
        assert!(!owner.positions.btc().unwrap().contains_key(product));
    } else {
        let position = &owner.positions.btc().unwrap()[product];
        assert_eq!(
            (position.side, position.contracts, position.entry_basis),
            (Side::Long, d(filled), d(filled) * d("100"))
        );
    }
    let snapshot = owner.reservation().unwrap();
    assert_eq!(
        (
            snapshot.orders.len(),
            snapshot.products.len(),
            snapshot.equity,
            snapshot.total_fee_hold,
            snapshot.used_margin,
            snapshot.available_margin
        ),
        (
            if target_fee_hold.is_some() { 3 } else { 2 },
            3,
            d(cash),
            d(hold),
            d(used),
            d(available)
        )
    );
    let target_row = snapshot.orders.iter().find(|row| row.order_id == IDS[0]);
    assert_eq!(
        target_row.map(|row| (row.remaining_contracts, row.fee_hold)),
        target_fee_hold.map(|fee| (d(remaining), d(fee)))
    );
    for i in 1..3 {
        let row = snapshot
            .orders
            .iter()
            .find(|row| row.order_id == IDS[i])
            .unwrap();
        assert_eq!(
            (
                row.product.clone(),
                row.side,
                row.remaining_contracts,
                row.fee_hold
            ),
            (
                snapshot
                    .products
                    .iter()
                    .find(|p| p.product == row.product)
                    .unwrap()
                    .product
                    .clone(),
                Side::Long,
                d("2"),
                d(FEES[i])
            )
        );
        let facts = &owner.orders[IDS[i]].facts;
        assert_eq!(
            (
                facts.original,
                facts.filled,
                facts.remaining,
                facts.canceled,
                facts.status.as_str()
            ),
            (d("2"), Decimal::ZERO, d("2"), Decimal::ZERO, "OPEN")
        );
    }
}

#[test]
fn configured_first_product_fee_cancel_remainder_and_delayed_ack() {
    let (mut owner, product) = owner_with_three_products();
    assert_state(
        &owner,
        &product,
        "0",
        "2",
        "0",
        "OPEN",
        "1000",
        "0",
        "1.2",
        "61.2",
        "938.8",
        Some("0.2"),
    );
    let first = configured_input(&owner, IDS[0], "CANCEL-FILL-1", "0.5", "100", 501);
    let receipt1 = committed(&mut owner, &first);
    assert_state(
        &owner,
        &product,
        "0.5",
        "1.5",
        "0",
        "PARTIALLY_FILLED",
        "999.95",
        "0.05",
        "1.15",
        "61.15",
        "938.8",
        Some("0.15"),
    );

    let request = RequestInput {
        stamp: source::stamp("CANCEL-REQUEST", 502, 40),
        targets: vec![(IDS[0].into(), Reason::ExplicitScenario)],
    };
    let requested = owner.request_cancel(&request).unwrap();
    assert_state(
        &owner,
        &product,
        "0.5",
        "1.5",
        "0",
        "PARTIALLY_FILLED",
        "999.95",
        "0.05",
        "1.15",
        "61.15",
        "938.8",
        Some("0.15"),
    );
    let saved = owner.clone();
    assert_eq!(owner.request_cancel(&request), Ok(requested));
    assert_eq!(owner, saved);

    let second = configured_input(&owner, IDS[0], "CANCEL-FILL-2", "0.5", "100", 503);
    let receipt2 = committed(&mut owner, &second);
    assert_state(
        &owner,
        &product,
        "1",
        "1",
        "0",
        "PARTIALLY_FILLED",
        "999.9",
        "0.1",
        "1.1",
        "61.1",
        "938.8",
        Some("0.1"),
    );

    let effect = EffectInput {
        stamp: source::stamp("CANCEL-EFFECT", 504, 50),
        effects: vec![(
            request.stamp.event_id.clone(),
            IDS[0].into(),
            Reason::ExplicitScenario,
        )],
    };
    let effected = owner.effect_cancel(&effect).unwrap();
    assert_state(
        &owner, &product, "1", "0", "1", "CANCELED", "999.9", "0.1", "1", "51", "948.9", None,
    );
    let saved = owner.clone();
    assert_eq!(owner.effect_cancel(&effect), Ok(effected));
    assert_eq!(owner, saved);
    assert_eq!(owner.execute(&first), Ok(Reply::Duplicate(receipt1)));
    assert_eq!(owner, saved);
    assert_eq!(owner.execute(&second), Ok(Reply::Duplicate(receipt2)));
    assert_eq!(owner, saved);

    let (snapshots, _) = snapshot::tests::delivery_fixture(&owner, false);
    let mut store = Store::new(owner.key.clone());
    let mut ack = projection("SOURCE", "CANCEL-EFFECT", "TRANSPORT_ACK");
    ack.visible_at = 510;
    ack.transport = Some(Transport {
        route: "WS",
        operation: "CANCEL",
        client: owner.orders[IDS[0]].facts.client_id.clone(),
        order: Some(IDS[0].into()),
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    });
    let delivered = build(&mut store, &owner, &snapshots, &ack).unwrap();
    assert_eq!(
        delivered.body,
        Body::Source(Payload::Transport(ack.transport.clone().unwrap()))
    );
    let saved = (owner.clone(), store.clone());
    assert_eq!(
        build(&mut store, &owner, &snapshots, &ack),
        Ok(delivered.clone())
    );
    assert_eq!((owner, store), saved);
}
