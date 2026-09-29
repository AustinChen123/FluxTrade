use super::*;
use risk_transition::cancel::{EffectInput, Reason, RequestInput};

const P: &str = "BTC-USDT-SWAP";
const Q: &str = "ETH-USDT-SWAP";

fn owner(account: &str, cash: &str) -> ScenarioAccount {
    let (mut seed, mut products) = crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::configured_tests::input(2);
    seed.key.account = account.into();
    seed.cash = d(cash);
    products[0].taker_fee = d("0.002");
    products[1].taker_fee = d("0.003");
    let prototype = fixture().0.orders[0].clone();
    for (id, product) in [("SAME-P-ORDER", P), ("SAME-Q-ORDER", Q)] {
        seed.orders.push(SeedOrder {
            order_id: id.into(),
            intent_id: format!("I-{id}"),
            client_id: format!("C-{id}"),
            product: ProfileProduct::BtcEth(Product(product.into())),
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

fn check(
    owner: &ScenarioAccount,
    cash: &str,
    fees: &str,
    p_filled: &str,
    p_remaining: &str,
    p_canceled: &str,
    p_status: &str,
    p_position: Option<&str>,
    hold: &str,
    used: &str,
    available: &str,
    active_orders: usize,
) {
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d(cash), d(fees), Decimal::ZERO)
    );
    let p_order = &owner.orders["SAME-P-ORDER"].facts;
    assert_eq!(
        (
            p_order.original,
            p_order.filled,
            p_order.remaining,
            p_order.canceled,
            p_order.status.as_str()
        ),
        (d("1"), d(p_filled), d(p_remaining), d(p_canceled), p_status)
    );
    let q_order = &owner.orders["SAME-Q-ORDER"].facts;
    assert_eq!(
        (q_order.filled, q_order.remaining, q_order.status.as_str()),
        (Decimal::ZERO, d("1"), "OPEN")
    );
    let positions = owner.positions.btc().unwrap();
    match p_position {
        Some(contracts) => assert_eq!(
            (
                positions[&Product(P.into())].side,
                positions[&Product(P.into())].contracts,
                positions[&Product(P.into())].entry_basis
            ),
            (Side::Long, d(contracts), d(contracts) * d("100"))
        ),
        None => assert!(!positions.contains_key(&Product(P.into()))),
    }
    let r = owner.reservation().unwrap();
    assert_eq!(
        (
            r.orders.len(),
            r.products.len(),
            r.equity,
            r.total_fee_hold,
            r.used_margin,
            r.available_margin
        ),
        (active_orders, 2, d(cash), d(hold), d(used), d(available))
    );
    let p = r
        .products
        .iter()
        .find(|row| row.product == Product(P.into()))
        .unwrap();
    assert_eq!(
        (p.position_value, p.long_remaining_value, p.exposure_margin),
        (
            if p_position.is_some() {
                d("50")
            } else {
                Decimal::ZERO
            },
            d(p_remaining) * d("100"),
            if p_remaining == "0" { d("5") } else { d("10") }
        )
    );
    let q = r
        .products
        .iter()
        .find(|row| row.product == Product(Q.into()))
        .unwrap();
    assert_eq!(
        (q.position_value, q.long_remaining_value, q.exposure_margin),
        (Decimal::ZERO, d("100"), d("10"))
    );
    let p_row = r.orders.iter().find(|row| row.order_id == "SAME-P-ORDER");
    if p_remaining == "0" {
        assert!(p_row.is_none());
    } else {
        let p_row = p_row.unwrap();
        assert_eq!(
            (
                p_row.product.clone(),
                p_row.side,
                p_row.remaining_contracts,
                p_row.fee_hold
            ),
            (
                Product(P.into()),
                Side::Long,
                d(p_remaining),
                if p_position.is_some() {
                    d("0.1")
                } else {
                    d("0.2")
                }
            )
        );
    }
    let q_row = r
        .orders
        .iter()
        .find(|row| row.order_id == "SAME-Q-ORDER")
        .unwrap();
    assert_eq!(
        (
            q_row.product.clone(),
            q_row.side,
            q_row.remaining_contracts,
            q_row.fee_hold
        ),
        (Product(Q.into()), Side::Long, d("1"), d("0.3"))
    );
}

fn fill(owner: &mut ScenarioAccount) -> (ExecutionCandidate, CommittedExecution) {
    let candidate = configured_input(owner, "SAME-P-ORDER", "SAME-X", "0.5", "100", 501);
    let Reply::Committed { receipt, .. } = owner.execute(&candidate).unwrap() else {
        panic!("commit required")
    };
    (candidate, receipt)
}

#[test]
fn configured_financial_owners_isolate_same_execution_and_cancel_identities() {
    let mut a = owner("ACCOUNT-A", "100");
    let mut b = owner("ACCOUNT-B", "200");
    assert_ne!(a.key, b.key);
    assert_eq!(a.config_id, b.config_id);
    check(
        &a, "100", "0", "0", "1", "0", "OPEN", None, "0.5", "20.5", "79.5", 2,
    );
    check(
        &b, "200", "0", "0", "1", "0", "OPEN", None, "0.5", "20.5", "179.5", 2,
    );

    let b_initial = b.clone();
    let (a_candidate, a_receipt) = fill(&mut a);
    check(
        &a,
        "99.9",
        "0.1",
        "0.5",
        "0.5",
        "0",
        "PARTIALLY_FILLED",
        Some("0.5"),
        "0.4",
        "20.4",
        "79.5",
        2,
    );
    assert_eq!(b, b_initial);
    let request = RequestInput {
        stamp: crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::source::stamp("SAME-C", 502, 40),
        targets: vec![("SAME-P-ORDER".into(), Reason::ExplicitScenario)],
    };
    let requested_a = a.request_cancel(&request).unwrap();
    assert_eq!((requested_a.rejected, requested_a.failure), (None, None));
    assert_eq!(
        requested_a.receipts[0].outcome,
        risk_transition::cancel::Outcome::Requested
    );
    check(
        &a,
        "99.9",
        "0.1",
        "0.5",
        "0.5",
        "0",
        "PARTIALLY_FILLED",
        Some("0.5"),
        "0.4",
        "20.4",
        "79.5",
        2,
    );
    assert_eq!(b, b_initial);
    let a_after_request = a.clone();
    assert_eq!(a.request_cancel(&request), Ok(requested_a.clone()));
    assert_eq!(a, a_after_request);
    let effect = EffectInput {
        stamp: crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::source::stamp("SAME-E", 503, 50),
        effects: vec![(
            request.stamp.event_id.clone(),
            "SAME-P-ORDER".into(),
            Reason::ExplicitScenario,
        )],
    };
    let effected_a = a.effect_cancel(&effect).unwrap();
    assert_eq!((effected_a.rejected, effected_a.failure), (None, None));
    assert_eq!(
        effected_a.receipts[0].outcome,
        risk_transition::cancel::Outcome::EffectiveCanceled
    );
    assert_eq!(a.gate, Gate::Running);
    let a_after_effect = a.clone();
    assert_eq!(a.effect_cancel(&effect), Ok(effected_a.clone()));
    assert_eq!(a, a_after_effect);
    check(
        &a,
        "99.9",
        "0.1",
        "0.5",
        "0",
        "0.5",
        "CANCELED",
        Some("0.5"),
        "0.3",
        "15.3",
        "84.6",
        1,
    );
    assert_eq!(b, b_initial);

    let (b_candidate, b_receipt) = fill(&mut b);
    assert_eq!(a_candidate.template.key.external_id, "SAME-X");
    assert_eq!(b_candidate.template.key.external_id, "SAME-X");
    assert_ne!(b_receipt.execution_id, a_receipt.execution_id);
    check(
        &b,
        "199.9",
        "0.1",
        "0.5",
        "0.5",
        "0",
        "PARTIALLY_FILLED",
        Some("0.5"),
        "0.4",
        "20.4",
        "179.5",
        2,
    );
    assert_eq!(a, a_after_effect);
    let requested_b = b.request_cancel(&request).unwrap();
    assert_eq!((requested_b.rejected, requested_b.failure), (None, None));
    assert_eq!(
        requested_b.receipts[0].outcome,
        risk_transition::cancel::Outcome::Requested
    );
    check(
        &b,
        "199.9",
        "0.1",
        "0.5",
        "0.5",
        "0",
        "PARTIALLY_FILLED",
        Some("0.5"),
        "0.4",
        "20.4",
        "179.5",
        2,
    );
    assert_eq!(requested_b.event_id, requested_a.event_id);
    assert_eq!(a, a_after_effect);
    let effected_b = b.effect_cancel(&effect).unwrap();
    assert_eq!((effected_b.rejected, effected_b.failure), (None, None));
    assert_eq!(
        effected_b.receipts[0].outcome,
        risk_transition::cancel::Outcome::EffectiveCanceled
    );
    assert_eq!(b.gate, Gate::Running);
    assert_eq!(effected_b.event_id, effected_a.event_id);
    assert_eq!(a, a_after_effect);
    check(
        &b,
        "199.9",
        "0.1",
        "0.5",
        "0",
        "0.5",
        "CANCELED",
        Some("0.5"),
        "0.3",
        "15.3",
        "184.6",
        1,
    );
    assert_eq!(a, a_after_effect);

    assert_eq!(a.execute(&a_candidate), Ok(Reply::Duplicate(a_receipt)));
    assert_eq!(a, a_after_effect);
    let b_after_effect = b.clone();
    assert_eq!(b.execute(&b_candidate), Ok(Reply::Duplicate(b_receipt)));
    assert_eq!(b, b_after_effect);
}
