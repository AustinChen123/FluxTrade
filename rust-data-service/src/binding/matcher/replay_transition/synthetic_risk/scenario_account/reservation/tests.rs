use super::super::tests::{d, fixture};
use super::*;

fn order(id: &str, side: Side, remaining: &str) -> SeedOrder {
    SeedOrder {
        order_id: id.into(),
        intent_id: format!("I{id}"),
        client_id: format!("C{id}"),
        strategy_id: "strategy".into(),
        product: ProfileProduct::BtcEth(Product::Btc),
        side,
        price: d("50000"),
        reduce_only: false,
        original: d(remaining),
        filled: d("0"),
        canceled: Decimal::ZERO,
        remaining: d(remaining),
        status: "OPEN".into(),
    }
}

#[test]
fn opposing_remainder_dominates_each_legal_reservation_state() {
    for (position_side, order_side, position_value, long, short, exposure, used) in [
        (None, Side::Short, "0", "0", "1500", "150", "151.5"),
        (
            Some(Side::Long),
            Side::Short,
            "500",
            "0",
            "1500",
            "100",
            "101.5",
        ),
        (
            Some(Side::Short),
            Side::Long,
            "500",
            "1500",
            "0",
            "100",
            "101.5",
        ),
    ] {
        let (mut seed, config, mut marks) = fixture();
        seed.cash = d("1000"); // Non-flat equity 1000 exceeds MMR 2.
        marks[0].price = d("50000");
        if let Some(side) = position_side {
            seed.positions[0].side = side;
            seed.positions[0].contracts = d("1");
            seed.positions[0].lots.truncate(1);
        } else {
            seed.positions.clear();
        }
        seed.orders = vec![order("opposing", order_side, "3")];
        assert!(!seed.orders[0].reduce_only);
        let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let before = owner.clone();
        let snapshot = owner.reservation().unwrap();
        let product = &snapshot.products[0];
        assert_eq!(
            (
                product.position_value,
                product.long_remaining_value,
                product.short_remaining_value
            ),
            (d(position_value), d(long), d(short))
        );
        assert_eq!(
            (
                product.exposure_margin,
                snapshot.total_fee_hold,
                snapshot.total_order_loss,
                snapshot.used_margin
            ),
            (d(exposure), d("1.5"), d("0"), d(used))
        );
        assert_eq!(
            (snapshot.orders[0].fee_hold, snapshot.orders[0].order_loss),
            (d("1.5"), d("0"))
        );
        if let Some(side) = position_side {
            let (same, opposing) = match side {
                Side::Long => (
                    add(product.position_value, product.long_remaining_value),
                    add(product.short_remaining_value, -product.position_value),
                ),
                Side::Short => (
                    add(product.position_value, product.short_remaining_value),
                    add(product.long_remaining_value, -product.position_value),
                ),
            };
            assert_eq!((same.unwrap(), opposing.unwrap()), (d("500"), d("1000")));
        }
        assert_eq!(owner, before);
    }
}

#[test]
fn whole_product_nonadditive_reservation_matches_golden_facts() {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("1000");
    seed.positions.clear();
    marks[0].price = d("50000");
    seed.orders = vec![order("L", Side::Long, "1")];
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(owner.reservation().unwrap().used_margin, d("50.5"));
    seed.orders.push(order("S", Side::Short, "1"));
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let snap = owner.reservation().unwrap();
    assert_eq!(
        (
            snap.used_margin,
            snap.total_fee_hold,
            snap.products[0].exposure_margin
        ),
        (d("51"), d("1"), d("50"))
    );
    let mut position = fixture().0.positions.remove(0);
    position.contracts = d("0.5");
    position.lots.truncate(1);
    position.lots[0].contracts = d("0.5");
    seed.positions.push(position);
    seed.orders[0].filled = d("0.5");
    seed.orders[0].remaining = d("0.5");
    seed.orders[0].status = "PARTIALLY_FILLED".into();
    for (mark, used, exposure, short_loss, equity) in [
        ("50000", "50.75", "50", "0", "1000"),
        ("50100", "51.85", "50.1", "1", "1000.5"),
    ] {
        marks[0].price = d(mark);
        let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let before = owner.clone();
        let snap = owner.reservation().unwrap();
        assert_eq!(
            (
                snap.used_margin,
                snap.total_fee_hold,
                snap.products[0].exposure_margin,
                snap.equity
            ),
            (d(used), d("0.75"), d(exposure), d(equity))
        );
        assert_eq!(
            (
                snap.orders[0].fee_hold,
                snap.orders[1].fee_hold,
                snap.orders[1].order_loss
            ),
            (d("0.25"), d("0.5"), d(short_loss))
        );
        assert_eq!(owner, before);
        assert_eq!(
            (
                snap.orders[0].remaining_base_exposure,
                snap.orders[1].remaining_base_exposure
            ),
            (d("0.005"), d("0.01"))
        );
        let mut short_seed = seed.clone();
        short_seed.positions[0].side = Side::Short;
        short_seed.orders[0].side = Side::Short;
        short_seed.orders[1].side = Side::Long;
        let short = ScenarioAccount::from_seed(&short_seed, &config, &marks)
            .unwrap()
            .reservation()
            .unwrap();
        assert_eq!(
            short.used_margin,
            d(if mark == "50000" { "50.75" } else { "51.35" })
        );
        seed.orders.reverse();
        assert_eq!(
            ScenarioAccount::from_seed(&seed, &config, &marks)
                .unwrap()
                .reservation()
                .unwrap(),
            snap
        );
        seed.orders.reverse();
    }
}

#[test]
fn shared_equity_and_atomic_capacity_anchors_are_only_snapshots() {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("1000");
    seed.orders = vec![order("B", Side::Long, "20")];
    seed.positions[0].product = Product::Eth;
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].entry = d("2000");
    marks[0].price = d("50000");
    for (price, available) in [("2000", "-30"), ("3000", "60")] {
        marks[1].price = d(price);
        let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        assert_eq!(owner.reservation().unwrap().available_margin, d(available));
        assert_eq!(owner.state_version, 0);
        assert!(owner.intent_results.is_empty());
    }
    seed.positions.clear();
    seed.orders = vec![order("L1", Side::Long, "10")];
    assert_eq!(
        ScenarioAccount::from_seed(&seed, &config, &marks)
            .unwrap()
            .reservation()
            .unwrap()
            .available_margin,
        d("495")
    );
    seed.orders.push(order("L2", Side::Long, "10"));
    let first = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(first.reservation().unwrap().available_margin, d("-10"));
    seed.key.account = "B".into();
    seed.cash = d("2000");
    let second = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(second.reservation().unwrap().available_margin, d("990"));
    assert_eq!(first.reservation().unwrap().available_margin, d("-10"));
}

#[test]
fn invalid_reservation_inputs_fail_without_modifying_authoritative_facts() {
    let (seed, config, marks) = fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let projection = owner.projection().unwrap();
    let mutations: &[fn(&mut SeedOrder)] = &[
        |o| o.remaining = d("0"),
        |o| o.remaining = d("0.001"),
        |o| o.price = d("0"),
        |o| o.price = d("50000.01"),
        |o| o.original = d("3"),
        |o| o.status = "FILLED".into(),
        |o| o.order_id.clear(),
        |o| o.price = Decimal::MAX,
    ];
    for mutate in mutations {
        let mut order = seed.orders[0].clone();
        mutate(&mut order);
        let before = order.clone();
        assert!(calculate(&projection, &[&order], &config, &marks).is_err());
        assert_eq!(order, before);
    }
    assert!(calculate(
        &projection,
        &[&seed.orders[0], &seed.orders[0]],
        &config,
        &marks
    )
    .is_err());
    for bad_marks in [
        Vec::new(),
        vec![marks[0].clone(), marks[0].clone()],
        vec![
            Mark {
                valid_from: 501,
                ..marks[0].clone()
            },
            marks[1].clone(),
        ],
    ] {
        assert!(calculate(&projection, &[], &config, &bad_marks).is_err());
    }
    let mut unsupported = config.clone();
    unsupported.leverage = d("5");
    assert!(calculate(&projection, &[], &unsupported, &marks).is_err());
    assert_eq!(owner.projection().unwrap(), projection);
}
