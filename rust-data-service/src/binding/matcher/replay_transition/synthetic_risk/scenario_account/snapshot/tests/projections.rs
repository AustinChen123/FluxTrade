use super::*;

fn capture(owner: &ScenarioAccount, kind: Kind) -> Payload {
    let before = owner.clone();
    let mut r = request(owner, kind);
    r.captured_at = 900;
    let result = Store::new(owner.key.clone())
        .capture(owner, &r)
        .unwrap()
        .payload;
    assert_eq!(owner, &before);
    result
}
pub(super) fn golden(owner: &ScenarioAccount, contracts: &str, open: bool) {
    let filled = d(contracts);
    let Payload::Positions(rows) = capture(owner, Kind::Positions) else {
        panic!("positions")
    };
    if filled.is_zero() {
        assert!(rows.is_empty());
    } else {
        assert_eq!(
            rows,
            vec![payload::PositionRow {
                product: "P_A".into(),
                contracts: filled,
                mark: d("10"),
                notional: filled * d("10")
            }]
        );
    }
    let Payload::OpenOrders(rows) = capture(owner, Kind::OpenOrders) else {
        panic!("orders")
    };
    assert_eq!(rows.len(), usize::from(open));
    for row in rows {
        assert_eq!(row.created_at, 500);
        assert_eq!(row.facts.filled, filled);
    }
}

#[test]
fn real_short_close_and_runtime_order_keep_detached_sorted_snapshots() {
    let (mut seed, config, marks) = fixture();
    seed.orders.clear();
    seed.positions[0].side = Side::Short;
    let mut eth = seed.positions[0].clone();
    eth.product = Product::Eth;
    eth.side = Side::Long;
    for lot in &mut eth.lots {
        lot.entry = d("1900");
        lot.seed_execution_id.push_str("-ETH");
        lot.seed_sequence += 2;
    }
    seed.positions.insert(0, eth); // Deliberately opposite to canonical product order.
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let Payload::Positions(rows) = capture(&owner, Kind::Positions) else {
        panic!("positions")
    };
    assert_eq!(
        rows.iter()
            .map(|r| (r.product.as_str(), r.contracts, r.notional))
            .collect::<Vec<_>>(),
        vec![
            ("BTC-USDT-SWAP", d("-3"), d("1500.03")),
            ("ETH-USDT-SWAP", d("3"), d("570"))
        ]
    );
    let intent = admission::fixture_intent(&owner, "runtime", Side::Long, d("3"), d("50001"));
    assert_eq!(
        admission::fixture_admit(&mut owner, "runtime", 600, &intent)
            .unwrap()
            .0,
        "accepted"
    );
    let id = owner.orders.keys().next().unwrap().clone();
    for (quantity, time, remainder) in [("1", 601, "-2"), ("2", 602, "0")] {
        let Payload::OpenOrders(rows) = capture(&owner, Kind::OpenOrders) else {
            panic!("orders")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].created_at, 600);
        assert_eq!(rows[0].facts.order_id, id);
        let execution = at(
            input(&owner, &id, &format!("fill-{time}"), quantity, "50001"),
            time,
        );
        owner.execute(&execution).unwrap();
        let Payload::Positions(rows) = capture(&owner, Kind::Positions) else {
            panic!("positions")
        };
        if remainder == "0" {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].product, "ETH-USDT-SWAP");
        } else {
            assert_eq!(rows[0].contracts, d(remainder));
            assert_eq!(rows[0].notional, d("1000.02"));
        }
    }
    assert_eq!(
        capture(&owner, Kind::OpenOrders),
        Payload::OpenOrders(vec![])
    );
}
