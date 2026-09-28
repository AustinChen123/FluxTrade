use super::super::tests::{d, fixture};
use super::*;
use execution::commit::tests::{at, input};

#[test]
fn exact_cash_floor_boundary_is_reached_only_through_real_fills() {
    for (quantity, cash, reason) in [("10", "995", None), ("10.01", "994.995", Some("MIN_CASH"))] {
        let (seed, _, _) = fixture();
        let mut owner = ScenarioAccount::synthetic_min_cash(seed.key).unwrap();
        let fill = at(
            input(&owner, "MIN-O1", "boundary-fill", quantity, "50000"),
            501,
        );
        owner.execute(&fill).unwrap();
        assert_eq!(owner.cash, d(cash));
        let mut intent = fixture_intent(&owner, "boundary", Side::Short, d("1"), d("50000"));
        intent.reduce_only = true;
        intent.requested_at = 502;
        let before = owner.clone();
        let (_, result) = fixture_admit(&mut owner, "boundary", 502, &intent).unwrap();
        assert_eq!(result.reason_code, reason);
        if reason.is_some() {
            assert_eq!(result.outcome, Outcome::Rejected);
            let mut expected = before;
            expected.intent_results = owner.intent_results.clone();
            expected.transition = owner.transition.clone();
            assert_eq!(owner, expected);
        } else {
            assert_eq!(result.outcome, Outcome::Accepted);
            assert_eq!(owner.state_version, before.state_version + 1);
        }
    }
}

#[test]
fn closed_min_cash_controls_preserve_standard_rejection_and_identity_precedence() {
    let (seed, _, _) = fixture();
    let initial = ScenarioAccount::synthetic_min_cash(seed.key).unwrap();
    let r = initial.reservation().unwrap();
    assert_eq!(
        (r.maintenance_margin, r.total_fee_hold, r.available_margin),
        (d("40"), d("10"), d("-10"))
    );
    assert_eq!(initial.btc_context().unwrap().0.leverage, d("10"));
    let mut positive = fixture_intent(&initial, "MIN-POSITIVE-I", Side::Short, d("1"), d("50000"));
    positive.client_order_id = "MIN-POSITIVE-C".into();
    positive.strategy_id = "min-cash-policy".into();
    positive.reduce_only = true;
    let mut owner = initial.clone();
    let (_, accepted) = fixture_admit(&mut owner, "positive", 500, &positive).unwrap();
    assert_eq!(accepted.outcome, Outcome::Accepted);
    let Evaluation::BtcEth(evidence) = accepted.evaluation else {
        panic!("BTC evidence")
    };
    let stress = evidence.stress.unwrap();
    assert_eq!(
        (
            stress.equity,
            stress.maintenance_margin,
            stress.excess,
            stress.current_excess
        ),
        (d("999.5"), d("38"), d("961.5"), d("960"))
    );
    assert_eq!(
        owner.orders[accepted.order_id.as_ref().unwrap()].created_at,
        500
    );
    let mut owner = initial;
    let close = at(input(&owner, "MIN-O1", "MIN-X1", "20", "50000"), 501);
    owner.execute(&close).unwrap();
    let mut negative = fixture_intent(&owner, "MIN-NEGATIVE-I", Side::Long, d("1"), d("50000"));
    negative.client_order_id = "MIN-NEGATIVE-C".into();
    negative.strategy_id = "min-cash-policy".into();
    negative.requested_at = 502;
    let before = owner.clone();
    let (_, rejected) = fixture_admit(&mut owner, "negative", 502, &negative).unwrap();
    assert_eq!(
        (rejected.outcome, rejected.reason_code),
        (Outcome::Rejected, Some("MIN_CASH"))
    );
    let mut expected = before.clone();
    expected.intent_results = owner.intent_results.clone();
    expected.transition = owner.transition.clone();
    assert_eq!(owner, expected);
    assert_eq!(
        fixture_admit(&mut owner, "negative", 502, &negative)
            .unwrap()
            .1,
        rejected
    );
    assert_eq!(owner, expected);
    let mut ordinary = before.clone();
    if let ProfileContext::BtcEthScenario {
        min_cash_profile, ..
    } = &mut ordinary.profile
    {
        *min_cash_profile = false;
    }
    let (_, accepted) = fixture_admit(&mut ordinary, "negative", 502, &negative).unwrap();
    assert_eq!(accepted.outcome, Outcome::Accepted);
    assert_eq!(ordinary.reservation().unwrap().used_margin, d("50.5"));
    assert_eq!(
        ordinary.orders[accepted.order_id.as_ref().unwrap()].created_at,
        502
    );
    for conflict in [false, true] {
        let mut bad = negative.clone();
        bad.quantity = d("0.001");
        let mut probe = if conflict {
            owner.clone()
        } else {
            before.clone()
        };
        assert_eq!(
            fixture_admit(&mut probe, "negative", 502, &bad),
            Err(if conflict {
                "IDEMPOTENCY_KEY_CONFLICT"
            } else {
                "INVALID_BTC_INTENT"
            })
        );
        let mut expected = if conflict {
            owner.clone()
        } else {
            before.clone()
        };
        expected.gate = probe.gate.clone();
        assert_eq!(probe, expected);
    }
}
