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
        let mut owner = ScenarioAccount::from_configured(&seed, d("5"), products).unwrap();
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
        assert!(evidence.post_reservation.as_ref().unwrap().available_margin < Decimal::ZERO);
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

#[test]
fn historical_reduce_only_market_minimum_is_a_nonmutating_business_rejection() {
    use super::super::super::ReplyKind;

    for (quantity, expected, reason) in [
        ("0.099", ReplyKind::Rejected, Some("MINIMUM_ORDER_QUANTITY")),
        ("0.1", ReplyKind::Accepted, None),
        ("0.101", ReplyKind::Accepted, None),
    ] {
        let (mut owner, product) = minimum_owner(true, "121.2");
        let candidate = minimum_market(
            &owner,
            &product,
            &format!("MIN-{quantity}"),
            quantity,
            Side::Short,
            true,
        );
        let event_id = format!("E-MIN-{quantity}");
        let before = owner.clone();
        let mut order_drafted = false;
        let reply = historical_admit_with_hook(&mut owner, &candidate, &event_id, 500, |stage| {
            order_drafted |= stage == super::super::super::PrepareStage::OrderDrafted;
            Ok(())
        })
        .unwrap();

        assert_eq!(reply.kind, expected);
        assert_eq!(reply.result.reason_code, reason);
        assert_eq!(owner.gate, Gate::Running);
        assert_eq!(owner.positions, before.positions);
        assert_eq!(
            (owner.cash, owner.fees, owner.gross_realized),
            (before.cash, before.fees, before.gross_realized)
        );
        if quantity == "0.099" {
            let Evaluation::BtcEth(value) = &reply.result.evaluation else {
                panic!("configured BTC-like risk evidence expected");
            };
            assert_eq!(value.role, Role::RiskReducing);
            assert_eq!(value.post_reservation, None);
            assert_eq!(value.stress, None);
            assert_eq!(reply.result.account_version_before, 0);
            assert_eq!(reply.result.account_version_after, 0);
            assert!(reply.result.accepted_order.is_none());
            assert!(reply.result.order_id.is_none());
            assert!(owner.orders.is_empty());
            assert!(!order_drafted);
            assert_eq!(owner.reservation().unwrap(), before.reservation().unwrap());
            assert!(owner.transition.events.contains_key(&event_id));

            let duplicate_before = owner.clone();
            let duplicate = historical_admit(&mut owner, &candidate, &event_id, 500).unwrap();
            assert_eq!(duplicate.kind, ReplyKind::Duplicate);
            assert_eq!(duplicate.result, reply.result);
            assert_eq!(owner, duplicate_before);
            let mut conflict = candidate.clone();
            conflict.quantity = d("0.098");
            assert_eq!(
                historical_admit(&mut owner, &conflict, &event_id, 500).unwrap_err(),
                "IDEMPOTENCY_KEY_CONFLICT"
            );
            assert_eq!(owner.gate, Gate::Failed("IDEMPOTENCY_KEY_CONFLICT"));
            assert_eq!(owner.positions, duplicate_before.positions);
            assert_eq!(
                (owner.cash, owner.fees, owner.gross_realized),
                (
                    duplicate_before.cash,
                    duplicate_before.fees,
                    duplicate_before.gross_realized
                )
            );
        } else {
            assert_eq!(reply.result.account_version_after, 1);
            assert!(reply.result.accepted_order.is_some());
            assert!(reply.result.order_id.is_some());
        }
    }
}

fn minimum_owner(with_position: bool, cash: &str) -> (ScenarioAccount, Product) {
    let (mut seed, mut products) = super::super::super::super::configured_tests::input(1);
    seed.cash = d(cash);
    let product = products[0].product.clone();
    products[0].specs[0].lot = d("0.001");
    products[0].specs[0].minimum = d("0.1");
    if with_position {
        let mut position = fixture().0.positions[0].clone();
        position.product = product.clone();
        position.side = Side::Long;
        position.contracts = d("0.2");
        position.lots.truncate(1);
        position.lots[0].contracts = d("0.2");
        position.lots[0].entry = d("100");
        seed.positions = vec![position];
    }
    (
        ScenarioAccount::from_configured(&seed, d("10"), products).unwrap(),
        product,
    )
}

fn minimum_market(
    owner: &ScenarioAccount,
    product: &Product,
    id: &str,
    quantity: &str,
    side: Side,
    reduce_only: bool,
) -> OrderIntent {
    let mut candidate = super::super::super::fixture_intent(owner, id, side, d(quantity), d("100"));
    candidate.product = ProfileProduct::BtcEth(product.clone());
    candidate.order_type = OrderType::Market;
    candidate.limit_price = None;
    candidate.reduce_only = reduce_only;
    candidate
}

fn historical_admit(
    owner: &mut ScenarioAccount,
    candidate: &OrderIntent,
    event_id: &str,
    effective_at: i64,
) -> Result<super::super::super::Reply, Fault> {
    historical_admit_with_hook(owner, candidate, event_id, effective_at, |_| Ok(()))
}

fn historical_admit_with_hook(
    owner: &mut ScenarioAccount,
    candidate: &OrderIntent,
    event_id: &str,
    effective_at: i64,
    hook: impl FnMut(super::super::super::PrepareStage) -> Result<(), Fault>,
) -> Result<super::super::super::Reply, Fault> {
    let mut stamp = super::super::super::super::source::stamp(event_id, effective_at, 60);
    stamp.ordering_contract_id = "HISTORICAL_ORDER_V1".into();
    stamp.source_sequence = Some(1);
    owner.admit_stamped_checked(
        &super::super::super::Envelope {
            event_id,
            effective_at,
            intent: candidate,
        },
        &stamp,
        hook,
    )
}

#[test]
fn minimum_shortcut_preserves_invalid_and_role_precedence() {
    for (quantity, order_type, reduce_only, side) in [
        ("0", OrderType::Market, true, Side::Short),
        ("-0.05", OrderType::Market, true, Side::Short),
        ("0.0505", OrderType::Market, true, Side::Short),
        ("0.05", OrderType::Market, false, Side::Short),
        ("0.05", OrderType::Limit, true, Side::Short),
    ] {
        let (mut owner, product) = minimum_owner(true, "121.2");
        let mut candidate = minimum_market(
            &owner,
            &product,
            &format!("INVALID-{quantity}-{order_type:?}-{reduce_only}"),
            quantity,
            side,
            reduce_only,
        );
        candidate.order_type = order_type;
        if order_type == OrderType::Limit {
            candidate.limit_price = Some(d("100"));
        }
        assert_eq!(
            historical_admit(&mut owner, &candidate, &candidate.intent_id, 500).unwrap_err(),
            "INVALID_BTC_INTENT"
        );
    }

    let (mut owner, product) = minimum_owner(false, "121.2");
    let nonreducing = minimum_market(&owner, &product, "NONREDUCING", "0.05", Side::Long, true);
    let reply = historical_admit(&mut owner, &nonreducing, "E-NONREDUCING", 500).unwrap();
    assert_eq!(reply.kind, super::super::super::ReplyKind::Rejected);
    assert_eq!(reply.result.reason_code, Some("REDUCE_ONLY_NOT_REDUCING"));
    let super::super::super::Evaluation::BtcEth(value) = &reply.result.evaluation else {
        panic!("configured BTC-like risk evidence expected");
    };
    assert_eq!(value.role, Role::Rejected("REDUCE_ONLY_NOT_REDUCING"));
    assert_eq!(value.post_reservation, None);
    assert_eq!(value.stress, None);
}

#[test]
fn minimum_shortcut_does_not_bypass_scope_identity_or_context_admission() {
    let cases: &[(fn(&mut OrderIntent), Fault)] = &[
        (|i| i.account_key.account.clear(), "INVALID_ACCOUNT_KEY"),
        (|i| i.account_key.account.push('x'), "ACCOUNT_MISMATCH"),
        (|i| i.config_id.push('x'), "CONFIG_MISMATCH"),
        (|i| i.intent_id.clear(), "INVALID_ADMISSION_IDENTITY"),
        (|i| i.strategy_id.clear(), "INVALID_ADMISSION_IDENTITY"),
    ];
    for (change, expected) in cases {
        let (mut owner, product) = minimum_owner(true, "121.2");
        let mut candidate = minimum_market(
            &owner,
            &product,
            "MIN-SCOPE-IDENTITY",
            "0.05",
            Side::Short,
            true,
        );
        change(&mut candidate);
        assert_eq!(
            historical_admit(&mut owner, &candidate, "E-MIN-SCOPE-IDENTITY", 500).unwrap_err(),
            *expected,
        );
    }

    let (mut owner, product) = minimum_owner(true, "121.2");
    let candidate = minimum_market(&owner, &product, "MIN-CONTEXT", "0.05", Side::Short, true);
    assert_eq!(
        historical_admit(&mut owner, &candidate, "E-MIN-CONTEXT", 3_500).unwrap_err(),
        "UNSUPPORTED_CONTEXT_TRANSITION",
    );
}

#[test]
fn existing_mmr_rejection_precedes_minimum_quantity_classification() {
    let (mut owner, product) = minimum_owner(true, "121.2");
    owner.cash = d("0"); // Reachable post-seed loss state; not a new seed capability.
    let candidate = minimum_market(
        &owner,
        &product,
        "MMR-BEFORE-MIN",
        "0.05",
        Side::Short,
        true,
    );
    let reply = historical_admit(&mut owner, &candidate, "E-MMR-BEFORE-MIN", 500).unwrap();
    assert_eq!(reply.kind, super::super::super::ReplyKind::Rejected);
    assert_eq!(reply.result.reason_code, Some("MMR_BREACH"));
    let Evaluation::BtcEth(value) = &reply.result.evaluation else {
        panic!("configured BTC-like risk evidence expected");
    };
    assert_eq!(value.current_risk, MaintenanceState::Breach);
    assert_eq!(value.role, Role::RiskReducing);
    assert_eq!(value.post_reservation, None);
    assert_eq!(value.stress, None);
    assert!(owner.orders.is_empty());
}
