use super::super::execution::commit::{tests::configured_input, Reply};
use super::super::tests::d;
use super::*;

fn tier(product: &Product, rows: &[(&str, &str, &str)]) -> TierVersion {
    TierVersion {
        product: product.clone(),
        version: "configured-tier-v1".into(),
        interval: Interval { from: 0, to: None },
        tiers: rows
            .iter()
            .map(|(minimum, maximum, max_leverage)| Tier {
                minimum: d(minimum),
                maximum: d(maximum),
                mmr: d("0.005"),
                imr: d("0.1"),
                max_leverage: d(max_leverage),
            })
            .collect(),
    }
}

fn order(product: &Product, id: &str, side: Side, quantity: &str, reduce_only: bool) -> SeedOrder {
    SeedOrder {
        intent_id: format!("I-{id}"),
        order_id: id.into(),
        client_id: format!("C-{id}"),
        strategy_id: "strategy".into(),
        product: ProfileProduct::BtcEth(product.clone()),
        side,
        price: d("100"),
        reduce_only,
        original: d(quantity),
        filled: Decimal::ZERO,
        canceled: Decimal::ZERO,
        remaining: d(quantity),
        status: "OPEN".into(),
    }
}

fn position(product: &Product, side: Side, quantity: &str) -> SeedPosition {
    SeedPosition {
        product: product.clone(),
        side,
        contracts: d(quantity),
        lots: vec![SeedLot {
            seed_execution_id: "SEED-POSITION".into(),
            seed_sequence: 0,
            strategy_id: "strategy".into(),
            contracts: d(quantity),
            entry: d("100"),
        }],
    }
}

fn configured_owner(
    leverage: &str,
    cash: &str,
    tier_rows: &[(&str, &str, &str)],
    position: Option<(Side, &str)>,
    orders: Vec<SeedOrder>,
) -> Result<ScenarioAccount, Fault> {
    let (mut seed, mut products) = configured_tests::input(1);
    let product = products[0].product.clone();
    seed.cash = d(cash);
    seed.orders = orders;
    seed.positions = position
        .map(|(side, quantity)| vec![self::position(&product, side, quantity)])
        .unwrap_or_default();
    products[0].tiers = vec![tier(&product, tier_rows)];
    ScenarioAccount::from_configured(&seed, d(leverage), products)
}

#[test]
fn configured_leverage_scales_only_configured_reservation_and_admission() {
    for (leverage, exposure, used, available) in
        [("5", "20", "20.1", "-10"), ("20", "5", "5.1", "5")]
    {
        let (mut seed, mut products) = configured_tests::input(1);
        seed.cash = d("10.1");
        let product = products[0].product.clone();
        products[0].tiers[0].tiers[0].max_leverage = d("20");
        let mut owner = ScenarioAccount::from_configured(&seed, d(leverage), products).unwrap();
        let intent = OrderIntent {
            intent_id: "LEVERAGE-INTENT".into(),
            client_order_id: "LEVERAGE-CLIENT".into(),
            account_key: owner.key.clone(),
            config_id: owner.config_id.clone(),
            product: ProfileProduct::BtcEth(product),
            strategy_id: "strategy".into(),
            side: Side::Long,
            order_type: OrderType::Limit,
            quantity: d("1"),
            limit_price: Some(d("100")),
            reduce_only: false,
            requested_at: 500,
        };
        let result = owner
            .admit(&Envelope {
                event_id: "LEVERAGE-ADMISSION",
                effective_at: 500,
                intent: &intent,
            })
            .unwrap();
        let Evaluation::BtcEth(evidence) = &result.result.evaluation else {
            panic!("configured BTC/ETH reservation evidence expected");
        };
        let post = evidence.post_reservation.as_ref().unwrap();
        assert_eq!(
            (
                post.products[0].exposure_margin,
                post.used_margin,
                post.available_margin
            ),
            (d(exposure), d(used), d(available))
        );
        if leverage == "20" {
            assert_eq!(result.kind, ReplyKind::Accepted);
            assert_eq!(owner.orders.len(), 1);
        } else {
            assert_eq!(result.kind, ReplyKind::Rejected);
            assert_eq!(
                result.result.reason_code,
                Some("INSUFFICIENT_SHARED_EQUITY")
            );
            assert!(owner.orders.is_empty());
        }
    }
    assert_eq!(
        exact_div(Decimal::ONE, d("3")),
        Err("DECIMAL_PRECISION_LOSS")
    );
    assert_eq!(
        exact_div(Decimal::ONE, d("536870912")),
        Err("DECIMAL_PRECISION_LOSS")
    );
    assert_eq!(exact_div(Decimal::MAX, d("0.1")), Err("DECIMAL_OVERFLOW"));
}

#[test]
fn configured_worst_reservation_table_covers_flat_long_short_and_cancel_pending() {
    let product = configured_tests::input(1).1.remove(0).product;
    for (position, orders, value, long, short, margin, fees, used) in [
        (
            None,
            vec![
                order(&product, "LONG", Side::Long, "1", false),
                order(&product, "SHORT", Side::Short, "2", false),
            ],
            "0",
            "100",
            "200",
            "40",
            "0.3",
            "40.3",
        ),
        (
            Some((Side::Long, "1.5")),
            vec![
                order(&product, "LONG", Side::Long, "0.5", false),
                order(&product, "SHORT", Side::Short, "1", false),
            ],
            "150",
            "50",
            "100",
            "40",
            "0.15",
            "40.15",
        ),
        (
            Some((Side::Short, "1.5")),
            vec![
                order(&product, "LONG", Side::Long, "1", false),
                order(&product, "SHORT", Side::Short, "0.5", false),
            ],
            "150",
            "100",
            "50",
            "40",
            "0.15",
            "40.15",
        ),
    ] {
        let mut owner =
            configured_owner("5", "1000", &[("0", "100000", "10")], position, orders).unwrap();
        let before = owner.reservation().unwrap();
        assert_eq!(
            (
                before.products[0].position_value,
                before.products[0].long_remaining_value,
                before.products[0].short_remaining_value,
                before.products[0].exposure_margin,
                before.total_fee_hold,
                before.used_margin,
            ),
            (d(value), d(long), d(short), d(margin), d(fees), d(used))
        );
        if position.is_none() {
            owner
                .request_cancel(&risk_transition::cancel::RequestInput {
                    stamp: source::stamp("CANCEL-PENDING", 501, 40),
                    targets: vec![(
                        "LONG".into(),
                        risk_transition::cancel::Reason::ExplicitScenario,
                    )],
                })
                .unwrap();
            assert_eq!(owner.reservation().unwrap(), before);
        }
    }
}

#[test]
fn active_tier_classifies_actual_and_worst_contracts_but_not_future_rows() {
    let ranges = [
        ("0", "1", "20"),
        ("1.5", "2", "20"),
        ("2.5", "100000", "20"),
    ];
    let (_, products) = configured_tests::input(1);
    let product = products[0].product.clone();
    let at_boundary = configured_owner(
        "20",
        "1000",
        &ranges,
        Some((Side::Long, "1.5")),
        vec![order(&product, "INCREASE", Side::Long, "0.5", false)],
    )
    .unwrap();
    let snapshot = at_boundary.reservation().unwrap();
    assert_eq!(
        (
            snapshot.products[0].position_value,
            snapshot.products[0].long_remaining_value,
            snapshot.products[0].exposure_margin
        ),
        (d("150"), d("50"), d("10"))
    ); // Actual tier minimum 1.5; reserved tier maximum 2.
    let (scenario, marks) = at_boundary.btc_context().unwrap();
    let valuation = scenario
        .evaluate(&at_boundary.projection().unwrap(), marks)
        .unwrap();
    assert_eq!(valuation.products[0].tier, 2);
    let untouched_limit = [
        ("0", "1", "20"),
        ("1.5", "2", "20"),
        ("2.5", "100000", "10"),
    ];
    assert!(configured_owner(
        "20",
        "1000",
        &untouched_limit,
        Some((Side::Long, "1.5")),
        vec![order(&product, "TOUCHED", Side::Long, "0.5", false)],
    )
    .is_ok());
    let first_tier =
        configured_owner("20", "1000", &ranges, Some((Side::Long, "1")), vec![]).unwrap();
    let (scenario, marks) = first_tier.btc_context().unwrap();
    assert_eq!(
        scenario
            .evaluate(&first_tier.projection().unwrap(), marks)
            .unwrap()
            .products[0]
            .tier,
        1
    );
    assert!(configured_owner("20", "1000", &ranges, Some((Side::Long, "2")), vec![]).is_ok());
    assert!(configured_owner("20", "1000", &ranges, Some((Side::Long, "2.5")), vec![]).is_ok());

    let low_limit = [("0", "1", "10"), ("1.5", "2", "20")];
    assert_eq!(
        configured_owner("20", "1000", &low_limit, Some((Side::Long, "1")), vec![]),
        Err("LEVERAGE_TIER_CONFLICT")
    );
    let reserved_limit = [("0", "1", "20"), ("1.5", "2", "10")];
    assert_eq!(
        configured_owner(
            "20",
            "1000",
            &reserved_limit,
            Some((Side::Long, "0.5")),
            vec![order(&product, "CROSS-TIER", Side::Long, "1", false)],
        ),
        Err("LEVERAGE_TIER_CONFLICT")
    );
    let reverse_dominance = [("0", "1", "20"), ("1.5", "2", "10"), ("2.5", "10", "20")];
    for (position_side, order_side, quantity) in [
        (Side::Long, Side::Short, "2.5"),
        (Side::Short, Side::Long, "2.5"),
    ] {
        assert_eq!(
            configured_owner(
                "20",
                "1000",
                &reverse_dominance,
                Some((position_side, "1")),
                vec![order(
                    &product,
                    "REVERSE-DOMINANCE",
                    order_side,
                    quantity,
                    false
                )],
            ),
            Err("LEVERAGE_TIER_CONFLICT")
        );
    }
    let gap = [("0", "0.4", "20"), ("0.6", "10", "20")];
    assert_eq!(
        configured_owner("20", "1000", &gap, Some((Side::Long, "0.5")), vec![]),
        Err("UNSUPPORTED_POSITION_TIER")
    );

    let (seed, mut configured) = configured_tests::input(1);
    let p = configured[0].product.clone();
    let mut current = configured[0].tiers[0].clone();
    current.interval = Interval {
        from: 0,
        to: Some(600),
    };
    current.tiers[0].max_leverage = d("10");
    let mut future = current.clone();
    future.version = "future-tier".into();
    future.interval = Interval {
        from: 600,
        to: None,
    };
    future.tiers[0].max_leverage = d("30");
    configured[0].tiers = vec![current, future];
    let mut owner = ScenarioAccount::from_configured(&seed, d("20"), configured).unwrap();
    let mut intent = fixture_intent(&owner, "FUTURE-TIER-INTENT", Side::Long, d("0.5"), d("100"));
    intent.product = ProfileProduct::BtcEth(p);
    assert_eq!(
        owner.admit(&Envelope {
            event_id: "FUTURE-TIER-ADMIT",
            effective_at: 500,
            intent: &intent
        }),
        Err("LEVERAGE_TIER_CONFLICT")
    );
    assert_eq!(owner.gate, Gate::Failed("LEVERAGE_TIER_CONFLICT"));
    assert!(owner.orders.is_empty() && owner.execution_receipts.is_empty());

    let (seed, mut configured) = configured_tests::input(1);
    let p = configured[0].product.clone();
    let mut current = configured[0].tiers[0].clone();
    current.interval = Interval {
        from: 0,
        to: Some(600),
    };
    current.tiers[0].max_leverage = d("30");
    let mut future = current.clone();
    future.version = "future-low-leverage-tier".into();
    future.interval = Interval {
        from: 600,
        to: None,
    };
    future.tiers[0].max_leverage = d("10");
    configured[0].tiers = vec![current, future];
    let mut owner = ScenarioAccount::from_configured(&seed, d("20"), configured).unwrap();
    let mut intent = fixture_intent(
        &owner,
        "CURRENT-TIER-INTENT",
        Side::Long,
        d("0.5"),
        d("100"),
    );
    intent.product = ProfileProduct::BtcEth(p);
    assert!(owner
        .admit(&Envelope {
            event_id: "CURRENT-TIER-ADMIT",
            effective_at: 500,
            intent: &intent,
        })
        .is_ok());
}

#[test]
fn configured_nonterminating_execution_is_atomic_before_exact_close_and_duplicate() {
    let product = configured_tests::input(1).1.remove(0).product;
    let reducing = order(&product, "REDUCE", Side::Short, "1", true);
    let mut owner = configured_owner(
        "3",
        "1000",
        &[("0", "100000", "3")],
        Some((Side::Long, "3")),
        vec![reducing],
    )
    .unwrap();
    let candidate = configured_input(&owner, "REDUCE", "NONTERMINATING", "0.5", "100", 501);
    let before = owner.clone();
    assert_eq!(owner.execute(&candidate), Err("DECIMAL_PRECISION_LOSS"));
    assert_eq!(owner.gate, Gate::Failed("DECIMAL_PRECISION_LOSS"));
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.state_version,
            owner.execution_receipts.len()
        ),
        (before.cash, before.fees, before.state_version, 0)
    );
    let mut normalized = owner.clone();
    normalized.gate = before.gate.clone();
    assert_eq!(normalized, before);

    let mut exact = configured_owner(
        "3",
        "1000",
        &[("0", "100000", "3")],
        Some((Side::Long, "3")),
        vec![order(&product, "REDUCE", Side::Short, "3", true)],
    )
    .unwrap();
    let fill = configured_input(&exact, "REDUCE", "EXACT-CLOSE", "3", "100", 501);
    let Reply::Committed { receipt, .. } = exact.execute(&fill).unwrap() else {
        panic!("commit expected")
    };
    assert_eq!(
        (exact.cash, exact.fees, exact.gross_realized),
        (d("999.7"), d("0.3"), Decimal::ZERO)
    );
    assert!(exact.positions.btc().unwrap().is_empty());
    assert_eq!(
        (
            exact.orders["REDUCE"].facts.status.as_str(),
            exact.orders["REDUCE"].facts.remaining
        ),
        ("FILLED", Decimal::ZERO)
    );
    let after = exact.clone();
    assert_eq!(exact.execute(&fill), Ok(Reply::Duplicate(receipt)));
    assert_eq!(exact, after);
}
