use super::*;

#[test]
fn configured_fee_changes_reachable_reducing_stress_decision() {
    for (rate, expected) in [
        ("0", super::super::super::ReplyKind::Accepted),
        ("0.002", super::super::super::ReplyKind::Rejected),
    ] {
        let (mut seed, mut products) = super::super::super::super::configured_tests::input(1);
        let product = products[0].product.clone();
        products[0].taker_fee = d(rate);
        products[0].tiers[0].tiers[0].mmr = d("0.002");
        let mut position = fixture().0.positions[0].clone();
        position.product = product.clone();
        position.contracts = d("1");
        position.lots.truncate(1);
        position.lots[0].contracts = d("1");
        position.lots[0].entry = d("100");
        seed.positions = vec![position];
        seed.cash = d("0.21");
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let mut candidate = super::super::super::fixture_intent(
            &owner,
            &format!("REDUCE-{rate}"),
            Side::Short,
            d("0.5"),
            d("100"),
        );
        candidate.product = ProfileProduct::BtcEth(product);
        candidate.reduce_only = true;
        let reply = owner
            .admit(&super::super::super::Envelope {
                event_id: &format!("REDUCE-EVENT-{rate}"),
                effective_at: 500,
                intent: &candidate,
            })
            .unwrap();
        assert_eq!(reply.kind, expected);
        let super::super::super::Evaluation::BtcEth(evidence) = &reply.result.evaluation else {
            panic!("configured BTC-like risk evidence expected");
        };
        let stress = evidence
            .stress
            .as_ref()
            .expect("stress branch must be reached");
        assert_eq!(stress.current_excess, d("0.01"));
        assert_eq!(
            stress.excess,
            if rate == "0" { d("0.11") } else { d("0.01") }
        );
        assert_eq!(
            stress.fee,
            if rate == "0" { Decimal::ZERO } else { d("0.1") }
        );
        assert_eq!(
            reply.result.reason_code,
            (expected == super::super::super::ReplyKind::Rejected)
                .then_some("INSUFFICIENT_SHARED_EQUITY")
        );
        assert_eq!(owner.cash, d("0.21"));
        assert_eq!(owner.fees, Decimal::ZERO);
    }
}
