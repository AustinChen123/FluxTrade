use super::*;

#[test]
fn configured_taker_fee_is_shared_by_reservation_settlement_and_duplicate() {
    for (rate, hold, fee, cash) in [("0", "0", "0", "121.2"), ("0.002", "0.2", "0.1", "121.1")] {
        let (mut seed, mut products) = super::super::super::super::configured_tests::input(1);
        let product = products[0].product.clone();
        products[0].taker_fee = d(rate);
        let prototype = fixture().0.orders[0].clone();
        seed.orders.push(configured_order(
            &prototype,
            "FEE-ORDER",
            &product,
            Side::Long,
            "100",
            "1",
        ));
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        assert_eq!(owner.reservation().unwrap().total_fee_hold, d(hold));

        let mut candidate = input(&owner, "FEE-ORDER", &format!("FEE-{rate}"), "0.5", "100");
        candidate.template.matching_effective_at = 501;
        candidate.spec_version = "scale-spec-v1".into();
        candidate.rule_data_version = "scale-tier-v1".into();
        let receipt = committed(&mut owner, &candidate);
        assert_eq!((owner.fees, owner.cash), (d(fee), d(cash)));
        assert_eq!(receipt.cash_deltas[0].1, -d(fee));
        assert_eq!(
            owner.reservation().unwrap().total_fee_hold,
            d(hold) / d("2")
        );
        assert_eq!(owner.orders["FEE-ORDER"].facts.remaining, d("0.5"));
        let committed = owner.clone();
        assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
        assert_eq!(owner, committed);
    }
}

#[test]
fn configured_fee_overflow_fails_before_financial_publication() {
    let (mut seed, mut products) = super::super::super::super::configured_tests::input(1);
    let product = products[0].product.clone();
    products[0].taker_fee = d("100000000000000000000");
    let prototype = fixture().0.orders[0].clone();
    seed.orders.push(configured_order(
        &prototype,
        "OVERFLOW-ORDER",
        &product,
        Side::Short,
        "1",
        "1",
    ));
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let mut candidate = input(
        &owner,
        "OVERFLOW-ORDER",
        "OVERFLOW-FILL",
        "1",
        "10000000000",
    );
    candidate.template.matching_effective_at = 501;
    candidate.spec_version = "scale-spec-v1".into();
    candidate.rule_data_version = "scale-tier-v1".into();
    let before = owner.clone();
    assert_eq!(owner.execute(&candidate), Err("DECIMAL_OVERFLOW"));
    assert_eq!(owner.gate, Gate::Failed("DECIMAL_OVERFLOW"));
    let mut after = owner;
    after.gate = before.gate.clone();
    assert_eq!(after, before);
}
