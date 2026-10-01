use super::*;

const NAMES: [&str; 12] = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ARB", "OP", "NEAR", "APT", "SUI", "ADA",
];
const IDS: [&str; 12] = [
    "SCALE-01", "SCALE-02", "SCALE-03", "SCALE-04", "SCALE-05", "SCALE-06", "SCALE-07", "SCALE-08",
    "SCALE-09", "SCALE-10", "SCALE-11", "SCALE-12",
];
const SIDES: [Side; 12] = [
    Side::Long,
    Side::Short,
    Side::Long,
    Side::Short,
    Side::Long,
    Side::Short,
    Side::Long,
    Side::Short,
    Side::Long,
    Side::Short,
    Side::Long,
    Side::Short,
];

fn product_at(i: usize) -> Product {
    Product(format!("{}-USDT-SWAP", NAMES[i]).into())
}

fn product_row<'a>(
    s: &'a reservation::Snapshot,
    p: &Product,
) -> &'a reservation::ProductReservation {
    s.products.iter().find(|row| row.product == *p).unwrap()
}

fn order_row<'a>(s: &'a reservation::Snapshot, id: &str) -> &'a reservation::OrderReservation {
    s.orders.iter().find(|row| row.order_id == id).unwrap()
}

#[test]
fn twelve_configured_products_share_literal_fee_scale_through_503() {
    let rates = [
        "0.001", "0.002", "0.003", "0.004", "0.005", "0.006", "0.007", "0.008", "0.009", "0.010",
        "0.011", "0.012",
    ];
    let contract_values = ["2", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "2"];
    let multipliers = ["1", "1", "1", "1", "1", "1", "3", "1", "1", "1", "1", "2"];
    let base = ["2", "1", "1", "1", "1", "1", "3", "1", "1", "1", "1", "4"];
    let notional = [
        "200", "100", "100", "100", "100", "100", "300", "100", "100", "100", "100", "400",
    ];
    let exposure = [
        "20", "10", "10", "10", "10", "10", "30", "10", "10", "10", "10", "40",
    ];
    let initial_fees = [
        "0.2", "0.2", "0.3", "0.4", "0.5", "0.6", "2.1", "0.8", "0.9", "1", "1.1", "4.8",
    ];
    let (mut seed, mut rows) = super::super::super::super::configured_tests::input(12);
    seed.cash = d("1000");
    let prototype = fixture().0.orders[0].clone();
    for i in 0..12 {
        let p = product_at(i);
        assert_eq!(rows[i].product, p);
        rows[i].taker_fee = d(rates[i]);
        rows[i].specs[0].contract_value = d(contract_values[i]);
        rows[i].specs[0].multiplier = d(multipliers[i]);
        seed.orders.push(configured_order(
            &prototype, IDS[i], &p, SIDES[i], "100", "1",
        ));
    }
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), rows).unwrap();
    let initial = owner.reservation().unwrap();
    assert_eq!(
        (
            initial.orders.len(),
            initial.products.len(),
            owner.cash,
            owner.fees,
            initial.equity,
            initial.total_fee_hold,
            initial.used_margin,
            initial.available_margin
        ),
        (
            12,
            12,
            d("1000"),
            d("0"),
            d("1000"),
            d("12.9"),
            d("192.9"),
            d("807.1")
        )
    );
    for i in 0..12 {
        let p = product_at(i);
        let (long, short) = if SIDES[i] == Side::Long {
            (notional[i], "0")
        } else {
            ("0", notional[i])
        };
        let product = product_row(&initial, &p);
        let order = order_row(&initial, IDS[i]);
        assert_eq!(
            (
                product.position_value,
                product.long_remaining_value,
                product.short_remaining_value,
                product.exposure_margin
            ),
            (d("0"), d(long), d(short), d(exposure[i]))
        );
        assert_eq!(
            (
                order.product.clone(),
                order.side,
                order.remaining_contracts,
                order.remaining_base_exposure,
                order.fee_hold
            ),
            (p, SIDES[i], d("1"), d(base[i]), d(initial_fees[i]))
        );
    }

    let filled = [0, 6, 11];
    let fill_fees = ["0.1", "1.05", "2.4"];
    let cumulative = ["0.1", "1.15", "3.55"];
    let cash = ["999.9", "998.85", "996.45"];
    let holds = ["12.8", "11.75", "9.35"];
    let used = ["192.8", "191.75", "189.35"];
    let position_base = ["1", "1.5", "2"];
    let position_value = ["100", "150", "200"];
    let remaining_fee = ["0.1", "1.05", "2.4"];
    for step in 0..3 {
        let i = filled[step];
        let p = product_at(i);
        let mut candidate = input(&owner, IDS[i], &format!("SCALE-FILL-{step}"), "0.5", "100");
        candidate.template.matching_effective_at = 501 + step as i64;
        candidate.spec_version = "scale-spec-v1".into();
        candidate.rule_data_version = "scale-tier-v1".into();
        let receipt = committed(&mut owner, &candidate);
        assert_eq!(
            (
                receipt.fee_amount,
                receipt.realized_pnl_delta,
                receipt.cash_deltas[0].1
            ),
            (d(fill_fees[step]), d("0"), -d(fill_fees[step]))
        );
        assert_eq!(
            (
                receipt.order_after.side,
                receipt.order_after.original,
                receipt.order_after.filled,
                receipt.order_after.remaining,
                receipt.order_after.status.as_str()
            ),
            (SIDES[i], d("1"), d("0.5"), d("0.5"), "PARTIALLY_FILLED")
        );
        assert_eq!(
            (
                owner.cash,
                owner.fees,
                owner.gross_realized,
                owner.positions.btc().unwrap().len(),
                owner.gate.clone()
            ),
            (
                d(cash[step]),
                d(cumulative[step]),
                d("0"),
                step + 1,
                Gate::Running
            )
        );
        let position = &owner.positions.btc().unwrap()[&p];
        assert_eq!(
            (
                position.side,
                position.contracts,
                position.entry_basis,
                position.lots[0].base_quantity,
                position.lots[0].source.entry
            ),
            (
                SIDES[i],
                d("0.5"),
                d(position_value[step]),
                d(position_base[step]),
                d("100")
            )
        );

        let after = owner.reservation().unwrap();
        assert_eq!(
            (
                after.equity,
                after.total_fee_hold,
                after.used_margin,
                after.available_margin
            ),
            (d(cash[step]), d(holds[step]), d(used[step]), d("807.1"))
        );
        for j in 0..12 {
            let q = product_at(j);
            let pr = product_row(&after, &q);
            let or = order_row(&after, IDS[j]);
            if filled[..=step].contains(&j) {
                let k = filled[..=step].iter().position(|x| *x == j).unwrap();
                let value = d(position_value[k]);
                let (long, short) = if SIDES[j] == Side::Long {
                    (value, Decimal::ZERO)
                } else {
                    (Decimal::ZERO, value)
                };
                assert_eq!(
                    (
                        pr.position_value,
                        pr.long_remaining_value,
                        pr.short_remaining_value,
                        pr.exposure_margin
                    ),
                    (value, long, short, d(exposure[j]))
                );
                assert_eq!(
                    (
                        or.side,
                        or.remaining_contracts,
                        or.remaining_base_exposure,
                        or.fee_hold
                    ),
                    (SIDES[j], d("0.5"), d(position_base[k]), d(remaining_fee[k]))
                );
                assert_eq!(
                    (
                        owner.orders[IDS[j]].facts.filled,
                        owner.orders[IDS[j]].facts.remaining,
                        owner.orders[IDS[j]].facts.status.as_str()
                    ),
                    (d("0.5"), d("0.5"), "PARTIALLY_FILLED")
                );
            } else {
                assert_eq!(pr, product_row(&initial, &q));
                assert_eq!(or, order_row(&initial, IDS[j]));
                assert_eq!(
                    (
                        owner.orders[IDS[j]].facts.filled,
                        owner.orders[IDS[j]].facts.remaining,
                        owner.orders[IDS[j]].facts.status.as_str()
                    ),
                    (d("0"), d("1"), "OPEN")
                );
            }
        }
        let after_commit = owner.clone();
        assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
        assert_eq!(owner, after_commit);
    }
}
