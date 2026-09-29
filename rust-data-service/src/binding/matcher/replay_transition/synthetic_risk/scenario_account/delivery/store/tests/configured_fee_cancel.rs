use super::*;
use execution::commit::{tests::configured_input, Reply};
use execution::{CommittedExecution, ExecutionCandidate};
use risk_transition::cancel::{EffectInput, Reason, RequestInput};

const IDS: [&str; 3] = ["CANCEL-FIRST", "CANCEL-MIDDLE", "CANCEL-LAST"];
const FEES: [&str; 3] = ["0.2", "0.4", "0.6"];
const PRODUCTS: [&str; 3] = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"];

fn owner_with_three_products(target: usize) -> (ScenarioAccount, Product) {
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
    let target = products[target].product.clone();
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
    let target = IDS
        .iter()
        .copied()
        .find(|id| owner.orders[*id].facts.product == ProfileProduct::BtcEth(product.clone()))
        .unwrap();
    let facts = &owner.orders[target].facts;
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
    let target_row = snapshot.orders.iter().find(|row| row.order_id == target);
    assert_eq!(
        target_row.map(|row| (row.remaining_contracts, row.fee_hold)),
        target_fee_hold.map(|fee| (d(remaining), d(fee)))
    );
    for id in IDS {
        if id == target {
            continue;
        }
        let index = IDS.iter().position(|candidate| *candidate == id).unwrap();
        let expected_product = Product(PRODUCTS[index].into());
        let row = snapshot
            .orders
            .iter()
            .find(|row| row.order_id == id)
            .unwrap();
        assert_eq!(
            (
                row.product.clone(),
                row.side,
                row.remaining_contracts,
                row.fee_hold
            ),
            (
                expected_product.clone(),
                Side::Long,
                d("2"),
                d(FEES[IDS.iter().position(|v| *v == id).unwrap()])
            )
        );
        let product_row = snapshot
            .products
            .iter()
            .find(|p| p.product == expected_product)
            .unwrap();
        assert_eq!(
            (
                product_row.position_value,
                product_row.long_remaining_value,
                product_row.short_remaining_value,
                product_row.exposure_margin
            ),
            (Decimal::ZERO, d("200"), Decimal::ZERO, d("20"))
        );
        let facts = &owner.orders[id].facts;
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
    let target = 0;
    let (
        fill_fee,
        cash1,
        hold1,
        used1,
        target_hold1,
        fee2,
        cash2,
        hold2,
        used2,
        target_hold2,
        final_hold,
        final_used,
        final_available,
    ) = (
        "0.05", "999.95", "1.15", "61.15", "0.15", "0.1", "999.9", "1.1", "61.1", "0.1", "1", "51",
        "948.9",
    );
    let (mut owner, product) = owner_with_three_products(target);
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
        Some(FEES[target]),
    );
    let first = configured_input(
        &owner,
        IDS[target],
        &format!("CANCEL-FILL-{target}-1"),
        "0.5",
        "100",
        501,
    );
    let receipt1 = committed(&mut owner, &first);
    assert_state(
        &owner,
        &product,
        "0.5",
        "1.5",
        "0",
        "PARTIALLY_FILLED",
        cash1,
        fill_fee,
        hold1,
        used1,
        "938.8",
        Some(target_hold1),
    );

    let request_event = format!("CANCEL-REQUEST-{target}");
    let effect_event = format!("CANCEL-EFFECT-{target}");
    let request = RequestInput {
        stamp: source::stamp(&request_event, 502, 40),
        targets: vec![(IDS[target].into(), Reason::ExplicitScenario)],
    };
    let requested = owner.request_cancel(&request).unwrap();
    assert_state(
        &owner,
        &product,
        "0.5",
        "1.5",
        "0",
        "PARTIALLY_FILLED",
        cash1,
        fill_fee,
        hold1,
        used1,
        "938.8",
        Some(target_hold1),
    );
    let saved = owner.clone();
    assert_eq!(owner.request_cancel(&request), Ok(requested));
    assert_eq!(owner, saved);

    let second = configured_input(
        &owner,
        IDS[target],
        &format!("CANCEL-FILL-{target}-2"),
        "0.5",
        "100",
        503,
    );
    let receipt2 = committed(&mut owner, &second);
    assert_state(
        &owner,
        &product,
        "1",
        "1",
        "0",
        "PARTIALLY_FILLED",
        cash2,
        fee2,
        hold2,
        used2,
        "938.8",
        Some(target_hold2),
    );

    let effect = EffectInput {
        stamp: source::stamp(&effect_event, 504, 50),
        effects: vec![(
            request.stamp.event_id.clone(),
            IDS[target].into(),
            Reason::ExplicitScenario,
        )],
    };
    let effected = owner.effect_cancel(&effect).unwrap();
    assert_state(
        &owner,
        &product,
        "1",
        "0",
        "1",
        "CANCELED",
        cash2,
        fee2,
        final_hold,
        final_used,
        final_available,
        None,
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
    let mut ack = projection("SOURCE", &effect_event, "TRANSPORT_ACK");
    ack.visible_at = 510;
    ack.transport = Some(Transport {
        route: "WS",
        operation: "CANCEL",
        client: owner.orders[IDS[target]].facts.client_id.clone(),
        order: Some(IDS[target].into()),
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    });
    let delivered = build(&mut store, &owner, &snapshots, &ack).unwrap();
    assert_eq!(delivered.projection, ack);
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

#[test]
fn configured_middle_product_fee_cancel_remainder_and_delayed_ack() {
    let target = 1;
    let (mut owner, product) = owner_with_three_products(target);
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
        Some("0.4"),
    );
    let first = configured_input(
        &owner,
        IDS[target],
        "CANCEL-MIDDLE-FILL-1",
        "0.5",
        "100",
        501,
    );
    let receipt1 = committed(&mut owner, &first);
    assert_state(
        &owner,
        &product,
        "0.5",
        "1.5",
        "0",
        "PARTIALLY_FILLED",
        "999.9",
        "0.1",
        "1.1",
        "61.1",
        "938.8",
        Some("0.3"),
    );

    let request = RequestInput {
        stamp: source::stamp("CANCEL-MIDDLE-REQUEST", 502, 40),
        targets: vec![(IDS[target].into(), Reason::ExplicitScenario)],
    };
    let requested = owner.request_cancel(&request).unwrap();
    assert_state(
        &owner,
        &product,
        "0.5",
        "1.5",
        "0",
        "PARTIALLY_FILLED",
        "999.9",
        "0.1",
        "1.1",
        "61.1",
        "938.8",
        Some("0.3"),
    );
    let saved = owner.clone();
    assert_eq!(owner.request_cancel(&request), Ok(requested));
    assert_eq!(owner, saved);

    let second = configured_input(
        &owner,
        IDS[target],
        "CANCEL-MIDDLE-FILL-2",
        "0.5",
        "100",
        503,
    );
    let receipt2 = committed(&mut owner, &second);
    assert_state(
        &owner,
        &product,
        "1",
        "1",
        "0",
        "PARTIALLY_FILLED",
        "999.8",
        "0.2",
        "1",
        "61",
        "938.8",
        Some("0.2"),
    );

    let effect = EffectInput {
        stamp: source::stamp("CANCEL-MIDDLE-EFFECT", 504, 50),
        effects: vec![(
            request.stamp.event_id.clone(),
            IDS[target].into(),
            Reason::ExplicitScenario,
        )],
    };
    let effected = owner.effect_cancel(&effect).unwrap();
    assert_state(
        &owner, &product, "1", "0", "1", "CANCELED", "999.8", "0.2", "0.8", "50.8", "949", None,
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
    let mut ack = projection("SOURCE", "CANCEL-MIDDLE-EFFECT", "TRANSPORT_ACK");
    ack.visible_at = 510;
    ack.transport = Some(Transport {
        route: "WS",
        operation: "CANCEL",
        client: owner.orders[IDS[target]].facts.client_id.clone(),
        order: Some(IDS[target].into()),
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    });
    let delivered = build(&mut store, &owner, &snapshots, &ack).unwrap();
    assert_eq!(delivered.projection, ack);
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
