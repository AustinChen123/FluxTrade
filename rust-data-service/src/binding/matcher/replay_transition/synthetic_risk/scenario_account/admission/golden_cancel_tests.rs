use super::super::tests::{d, fixture};
use super::*;

fn setup() -> (ScenarioAccount, OrderIntent) {
    let (mut seed, _, _) = fixture();
    seed.cash = d("1000");
    seed.positions.clear();
    seed.orders.clear();
    let owner =
        ScenarioAccount::from_golden_cancel_seed(&seed, &golden_cancel::Config::frozen()).unwrap();
    let intent = OrderIntent {
        intent_id: "I".into(),
        client_order_id: "C".into(),
        account_key: owner.key.clone(),
        config_id: owner.config_id.clone(),
        product: ProfileProduct::Pa,
        strategy_id: "S".into(),
        side: Side::Long,
        order_type: OrderType::Limit,
        quantity: d("10"),
        limit_price: Some(d("10")),
        reduce_only: false,
        requested_at: 500,
    };
    (owner, intent)
}

#[test]
fn golden_cancel_admission_is_closed_and_uses_canonical_order_identity() {
    let (mut owner, intent) = setup();
    let envelope = Envelope {
        event_id: "E",
        effective_at: 500,
        intent: &intent,
    };
    let accepted = owner.admit(&envelope).unwrap();
    assert_eq!(accepted.result.order_id, Some(intent.order_id()));
    assert_eq!(
        accepted.result.evaluation,
        Evaluation::GoldenCancel(d("100"))
    );
    assert_eq!(
        (
            accepted.result.account_version_before,
            accepted.result.account_version_after,
            accepted.result.order_version_before,
            accepted.result.order_version_after
        ),
        (0, 1, None, Some(1))
    );
    assert_eq!(
        (
            accepted.result.spec_version.as_str(),
            accepted.result.rule_data_version.as_str()
        ),
        ("gt03-spec-v1", "gt03-rule-v1")
    );
    let before = owner.clone();
    assert_eq!(owner.admit(&envelope).unwrap().kind, ReplyKind::Duplicate);
    assert_eq!(owner, before);
    let mut second = intent.clone();
    second.intent_id = "second".into();
    second.client_order_id = "second-client".into();
    assert_eq!(
        owner.admit(&Envelope {
            event_id: "E2",
            effective_at: 500,
            intent: &second
        }),
        Err("INVALID_GOLDEN_CANCEL_INTENT")
    );
    for (change, error) in [
        (
            (|i: &mut OrderIntent| i.product = ProfileProduct::BtcEth(Product::Btc))
                as fn(&mut OrderIntent),
            "INVALID_GOLDEN_CANCEL_INTENT",
        ),
        (
            |i: &mut OrderIntent| i.side = Side::Short,
            "INVALID_GOLDEN_CANCEL_INTENT",
        ),
        (
            |i: &mut OrderIntent| i.quantity = d("9"),
            "INVALID_GOLDEN_CANCEL_INTENT",
        ),
        (
            |i: &mut OrderIntent| i.quantity = d("11"),
            "INVALID_GOLDEN_CANCEL_INTENT",
        ),
        (
            |i: &mut OrderIntent| i.limit_price = Some(d("11")),
            "INVALID_GOLDEN_CANCEL_INTENT",
        ),
        (
            |i: &mut OrderIntent| i.reduce_only = true,
            "INVALID_GOLDEN_CANCEL_INTENT",
        ),
        (
            |i: &mut OrderIntent| i.order_type = OrderType::Market,
            "UNSUPPORTED_ORDER_TYPE",
        ),
        (
            |i: &mut OrderIntent| i.account_key.subaccount = Some("other".into()),
            "ACCOUNT_MISMATCH",
        ),
        (
            |i: &mut OrderIntent| i.config_id = "other".into(),
            "CONFIG_MISMATCH",
        ),
    ] {
        let (mut owner, mut intent) = setup();
        change(&mut intent);
        let before = owner.clone();
        assert_eq!(
            owner.admit(&Envelope {
                event_id: "E",
                effective_at: 500,
                intent: &intent
            }),
            Err(error)
        );
        let mut expected = before;
        expected.gate = Gate::Failed(error);
        assert_eq!(owner, expected);
    }
}
