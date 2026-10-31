use super::super::*;
use super::*;
use std::str::FromStr;

fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn configured_owner(
    count: usize,
    cash: &str,
    fee: &str,
    positions: Vec<SeedPosition>,
) -> ScenarioAccount {
    let (mut seed, mut products) = configured_tests::input(count);
    seed.cash = d(cash);
    seed.positions = positions;
    seed.orders.clear();
    for product in &mut products {
        product.taker_fee = d(fee);
    }
    ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap()
}

fn gross_configuration(fee_rate: &str, mark_valid_to: i64) -> String {
    format!(
        r#"{{"schema_version":"synthetic_multi_product_config_v1","config_id":"gross-wire","seed_effective_at":500,"cash":"1000","leverage":"10","products":[{{"product_id":"arbitrary-private-id","instrument_code":1,"taker_fee_rate":"{fee_rate}","liquidation_fee_rate":"0","specs":[{{"version":"s1","valid_from":0,"valid_to":null,"contract_value":"1","multiplier":"1","price_tick":"1","quantity_step":"0.5","minimum_quantity":"0.5"}}],"tiers":[{{"version":"t1","valid_from":0,"valid_to":null,"rows":[{{"minimum_contracts":"0","maximum_contracts":"100000","mmr":"0.005","imr":"0.1","max_leverage":"10"}}]}}],"marks":[{{"valid_from":0,"valid_to":{mark_valid_to},"mark":"100"}}]}}],"positions":[],"orders":[]}}"#
    )
}

fn position(
    product: Product,
    side: Side,
    quantity: &str,
    entry: &str,
    sequence: u64,
) -> SeedPosition {
    SeedPosition {
        product,
        side,
        contracts: d(quantity),
        lots: vec![SeedLot {
            seed_execution_id: format!("SEED-{sequence}"),
            seed_sequence: sequence,
            strategy_id: "gross-test".into(),
            contracts: d(quantity),
            entry: d(entry),
        }],
    }
}

fn intent(
    owner: &ScenarioAccount,
    id: &str,
    product: Product,
    side: Side,
    quantity: &str,
    price: &str,
) -> OrderIntent {
    fixture_intent_for_product(
        owner,
        id,
        ProfileProduct::BtcEth(product),
        side,
        d(quantity),
        d(price),
    )
}

fn admit(owner: &mut ScenarioAccount, value: &OrderIntent) -> Reply {
    admit_at(owner, value, 500)
}

fn admit_at(owner: &mut ScenarioAccount, value: &OrderIntent, effective_at: i64) -> Reply {
    owner
        .admit(&Envelope {
            event_id: &value.intent_id,
            effective_at,
            intent: value,
        })
        .unwrap()
}

fn admit_historical(
    owner: &mut ScenarioAccount,
    value: &OrderIntent,
    event_id: &str,
    sequence: i64,
) -> Reply {
    admit_historical_at(owner, value, event_id, sequence, 500)
}

fn admit_historical_at(
    owner: &mut ScenarioAccount,
    value: &OrderIntent,
    event_id: &str,
    sequence: i64,
    effective_at: i64,
) -> Reply {
    let mut stamp = source::stamp(event_id, effective_at, 60);
    stamp.ordering_contract_id = "HISTORICAL_ORDER_V1".into();
    stamp.source_sequence = Some(sequence);
    owner
        .admit_stamped_checked(
            &Envelope {
                event_id,
                effective_at,
                intent: value,
            },
            &stamp,
            |_| Ok(()),
        )
        .unwrap()
}

fn reducing_market(owner: &ScenarioAccount, id: &str) -> OrderIntent {
    reducing_market_for(owner, id, &Product("arbitrary-private-id".into()))
}

fn reducing_market_for(owner: &ScenarioAccount, id: &str, product: &Product) -> OrderIntent {
    let mut value = OrderIntent::historical_market_fixture(
        owner,
        format!("REDUCE-{id}"),
        format!("CLIENT-{id}"),
        "arbitrary-private-id",
        Side::Short,
        d("0.5"),
        None,
    );
    value.product = ProfileProduct::BtcEth(product.clone());
    value.reduce_only = true;
    value
}

#[test]
fn configured_gross_projection_uses_absolute_product_gross_and_admission_is_nonmutating() {
    let (seed, products) = configured_tests::input(2);
    let a = products[0].product.clone();
    let b = products[1].product.clone();
    let owner = configured_owner(
        2,
        "1000",
        "0",
        vec![
            position(a.clone(), Side::Long, "10", "100", 0),
            position(b, Side::Short, "10", "100", 1),
        ],
    );
    let projection = gross_admission::Projection::current(&owner).unwrap();
    assert_eq!(
        (
            projection.effective_equity,
            projection.gross,
            projection.net,
            projection.worst_gross,
            projection.order_loss_hold,
            projection.fee_hold,
            projection.equity_floor,
        ),
        (
            d("1000"),
            d("2000"),
            Decimal::ZERO,
            d("2000"),
            Decimal::ZERO,
            Decimal::ZERO,
            Some(d("1000"))
        )
    );
    let encoded = gross_admission::project(&owner).unwrap();
    let fields = encoded
        .object(
            &[
                "schema_version",
                "risk_policy_id",
                "bound_scope",
                "account_key",
                "account_version",
                "valuation_context_id",
                "cap",
                "effective_equity",
                "gross",
                "net",
                "worst_gross",
                "order_loss_hold",
                "fee_hold",
                "equity_floor",
                "gross_ratio",
                "bound_reason",
            ],
            &[],
        )
        .unwrap();
    assert_eq!(fields["gross"].decimal().unwrap(), d("2000"));
    assert_eq!(fields["net"].decimal().unwrap(), Decimal::ZERO);
    assert_eq!(
        fields["gross_ratio"]
            .object(&["numerator", "denominator"], &[])
            .unwrap()["numerator"]
            .decimal()
            .unwrap(),
        d("2000")
    );

    // The configured public profile remains the legacy identity and context.
    let legacy = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    assert!(!legacy.is_gross_admission_profile());
    assert_ne!(legacy.valuation_context_id, owner.valuation_context_id);

    let mut owner = owner;
    let candidate = intent(&owner, "OVER-CAP", a, Side::Long, "1", "100");
    let before = owner.clone();
    let reply = admit(&mut owner, &candidate);
    assert_eq!(reply.kind, ReplyKind::Rejected);
    assert_eq!(reply.result.reason_code, Some("GROSS_CAP_EXCEEDED"));
    assert!(reply.result.accepted_order.is_none());
    assert!(reply.result.order_id.is_none());
    assert_eq!(owner.cash, before.cash);
    assert_eq!(owner.fees, before.fees);
    assert_eq!(owner.positions, before.positions);
    assert_eq!(owner.orders, before.orders);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.state_version, before.state_version);
    let receipt = &owner.intent_results[&candidate.intent_id];
    assert_eq!(
        receipt.gross_decision.as_ref().unwrap().reason,
        Some("GROSS_CAP_EXCEEDED")
    );
    let duplicate_baseline = owner.clone();
    let duplicate = admit(&mut owner, &candidate);
    assert_eq!(duplicate.kind, ReplyKind::Duplicate);
    assert_eq!(
        duplicate.result,
        duplicate_baseline.intent_results[&candidate.intent_id]
    );
    assert_eq!(owner, duplicate_baseline);

    let (mut context_seed, mut context_products) = configured_tests::input(2);
    context_seed.cash = d("1000");
    for product in &mut context_products {
        let initial = product.marks[0].clone();
        product.marks = vec![
            Mark {
                valid_to: 600,
                ..initial.clone()
            },
            Mark {
                price: d("101"),
                valid_from: 600,
                valid_to: 3000,
                ..initial
            },
        ];
    }
    let mut context_owner =
        ScenarioAccount::from_configured_profile(&context_seed, d("10"), context_products, true)
            .unwrap();
    let prior_context = context_owner.valuation_context_id;
    let (scenario, marks) = context_owner.btc_context().unwrap();
    let mark_rows = scenario
        .products()
        .into_iter()
        .map(|product| {
            let mark = marks
                .iter()
                .find(|mark| {
                    mark.product == product && mark.valid_from <= 600 && 600 < mark.valid_to
                })
                .unwrap();
            (product, mark.valid_from, mark.valid_to, mark.price)
        })
        .collect();
    let context = context::tests::activation(&context_owner, 600, context::Rows::Marks(mark_rows));
    let context_product = context_owner.btc_context().unwrap().0.products()[0].clone();
    context_owner.activate_context(&context).unwrap();
    assert_ne!(context_owner.valuation_context_id, prior_context);
    assert!(context_owner.is_gross_admission_profile());
    assert_eq!(
        gross_admission::Projection::current(&context_owner)
            .unwrap()
            .effective_equity,
        d("1000")
    );
    let admitted_after_context = intent(
        &context_owner,
        "AFTER-CONTEXT",
        context_product,
        Side::Long,
        "19.5",
        "101",
    );
    let reply = admit_at(&mut context_owner, &admitted_after_context, 601);
    assert_eq!(reply.kind, ReplyKind::Accepted);
    let decision = context_owner.intent_results[&admitted_after_context.intent_id]
        .gross_decision
        .as_ref()
        .unwrap();
    assert_eq!(decision.reason, None);
    let projection_value = gross_admission::project(&context_owner).unwrap();
    let projection = projection_value
        .object(
            &[
                "schema_version",
                "risk_policy_id",
                "bound_scope",
                "account_key",
                "account_version",
                "valuation_context_id",
                "cap",
                "effective_equity",
                "gross",
                "net",
                "worst_gross",
                "order_loss_hold",
                "fee_hold",
                "equity_floor",
                "gross_ratio",
                "bound_reason",
            ],
            &[],
        )
        .unwrap();
    assert_eq!(
        projection["risk_policy_id"].text().unwrap(),
        gross_admission::POLICY_ID
    );
    assert_eq!(
        projection["valuation_context_id"].text().unwrap(),
        context_owner
            .valuation_context_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}

#[test]
fn gross_limit_admission_enforces_fee_and_adverse_price_equity_floor() {
    let product = Product("arbitrary-private-id".into());
    let mut exact = configured_owner(1, "1000", "0", Vec::new());
    let exact_candidate = intent(
        &exact,
        "EXACT-CAP",
        product.clone(),
        Side::Long,
        "20",
        "100",
    );
    let exact_reply = admit(&mut exact, &exact_candidate);
    assert_eq!(exact_reply.kind, ReplyKind::Accepted);
    assert_eq!(exact_reply.result.reason_code, None);
    assert_eq!(exact.reservation().unwrap().worst_gross, d("2000"));

    let mut over = configured_owner(1, "1000", "0", Vec::new());
    let over_candidate = intent(
        &over,
        "OVER-CAP",
        product.clone(),
        Side::Long,
        "20.5",
        "100",
    );
    let over_reply = admit(&mut over, &over_candidate);
    assert_eq!(over_reply.kind, ReplyKind::Rejected);
    assert_eq!(over_reply.result.reason_code, Some("GROSS_CAP_EXCEEDED"));

    let mut fee_bound = configured_owner(1, "1000", "0.001", Vec::new());
    let fee_candidate = intent(
        &fee_bound,
        "FEE-FLOOR",
        product.clone(),
        Side::Long,
        "20",
        "100",
    );
    let fee_reply = admit(&mut fee_bound, &fee_candidate);
    assert_eq!(fee_reply.kind, ReplyKind::Rejected);
    assert_eq!(fee_reply.result.reason_code, Some("GROSS_CAP_EXCEEDED"));
    let fee_decision = fee_reply.result.gross_decision.as_ref().unwrap();
    assert_eq!(fee_decision.projection.fee_hold, d("2"));
    assert_eq!(fee_decision.projection.equity_floor, Some(d("998")));

    let mut positive_fee = configured_owner(1, "1000", "0.001", Vec::new());
    let fee_candidate = intent(
        &positive_fee,
        "POSITIVE-FEE-BOUND",
        product.clone(),
        Side::Long,
        "19.5",
        "100",
    );
    let accepted = admit_historical(&mut positive_fee, &fee_candidate, "POSITIVE-FEE-EVENT", 1);
    assert_eq!(accepted.kind, ReplyKind::Accepted);
    let before_fill = gross_admission::Projection::current(&positive_fee).unwrap();
    assert_eq!(before_fill.worst_gross, d("1950"));
    assert_eq!(before_fill.fee_hold, d("1.95"));
    assert_eq!(before_fill.equity_floor, Some(d("998.05")));
    let order_id = accepted.result.order_id.unwrap();
    let fill = execution::commit::tests::configured_input(
        &positive_fee,
        &order_id,
        "POSITIVE-FEE-ACTUAL",
        "1",
        "100",
        501,
    );
    assert!(matches!(
        positive_fee.execute(&fill).unwrap(),
        execution::commit::Reply::Committed {
            terminal_reason: None,
            ..
        }
    ));
    let after_fill = gross_admission::Projection::current(&positive_fee).unwrap();
    assert_eq!(positive_fee.fees, d("0.1"));
    assert!(after_fill.effective_equity >= before_fill.equity_floor.unwrap());

    let mut loss_bound = configured_owner(1, "1000", "0", Vec::new());
    let loss_candidate = intent(&loss_bound, "LOSS-FLOOR", product, Side::Long, "20", "110");
    let loss_reply = admit(&mut loss_bound, &loss_candidate);
    assert_eq!(loss_reply.kind, ReplyKind::Rejected);
    assert_eq!(loss_reply.result.reason_code, Some("GROSS_CAP_EXCEEDED"));
    let loss_decision = loss_reply.result.gross_decision.as_ref().unwrap();
    assert_eq!(loss_decision.projection.order_loss_hold, d("200"));
    assert_eq!(loss_decision.projection.equity_floor, Some(d("800")));
}

#[test]
fn ordered_same_time_intents_reserve_gross_without_cross_product_netting() {
    let (_seed, products) = configured_tests::input(2);
    let a = products[0].product.clone();
    let b = products[1].product.clone();
    let mut owner = configured_owner(
        2,
        "1000",
        "0",
        vec![
            position(a.clone(), Side::Long, "4", "100", 0),
            position(b.clone(), Side::Short, "4", "100", 1),
        ],
    );
    let first = intent(&owner, "SAME-TIME-LONG", a, Side::Long, "6", "100");
    let accepted = admit_historical_at(&mut owner, &first, "SAME-TIME-ONE", 1, 501);
    assert_eq!(accepted.kind, ReplyKind::Accepted);
    let after_first = gross_admission::Projection::current(&owner).unwrap();
    assert_eq!(after_first.worst_gross, d("1400"));

    let opposing = intent(&owner, "SAME-TIME-SHORT", b, Side::Short, "7", "100");
    let rejected = admit_historical_at(&mut owner, &opposing, "SAME-TIME-TWO", 2, 501);
    assert_eq!(rejected.kind, ReplyKind::Rejected);
    assert_eq!(rejected.result.reason_code, Some("GROSS_CAP_EXCEEDED"));
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.orders.len(), 1);
    assert_eq!(
        gross_admission::Projection::current(&owner)
            .unwrap()
            .worst_gross,
        d("1400")
    );
    assert!(owner.intent_results.contains_key(&opposing.intent_id));
    assert_eq!(owner.cash, d("1000"));
}

#[test]
fn limit_fill_subsets_keep_the_native_worst_bound_and_equity_floor() {
    let fill_levels = [("zero", "0"), ("partial", "2"), ("full", "5")];
    for (long_name, long_qty) in fill_levels {
        for (short_name, short_qty) in fill_levels {
            for long_first in [true, false] {
                let (mut seed, mut products) = configured_tests::input(2);
                seed.cash = d("1000");
                for row in &mut products {
                    row.taker_fee = d("0.001");
                }
                let a = products[0].product.clone();
                let b = products[1].product.clone();
                let mut owner =
                    ScenarioAccount::from_configured_profile(&seed, d("10"), products, true)
                        .unwrap();
                let long = intent(&owner, "BOUND-LONG", a, Side::Long, "5", "100");
                let long_reply = admit_historical(&mut owner, &long, "BOUND-LONG-EVENT", 1);
                assert_eq!(long_reply.kind, ReplyKind::Accepted);
                let short = intent(&owner, "BOUND-SHORT", b, Side::Short, "5", "100");
                let short_reply = admit_historical(&mut owner, &short, "BOUND-SHORT-EVENT", 2);
                assert_eq!(short_reply.kind, ReplyKind::Accepted);

                let bound = gross_admission::Projection::current(&owner).unwrap();
                assert_eq!(bound.worst_gross, d("1000"));
                assert_eq!(bound.fee_hold, d("1"));
                assert_eq!(bound.equity_floor, Some(d("999")));
                let long_order = long_reply.result.order_id.unwrap();
                let short_order = short_reply.result.order_id.unwrap();
                let version_before_fills = owner.state_version;
                let mut fill_count = 0;
                let fills = if long_first {
                    [
                        (&long_order, "FILL-LONG", long_qty, 501),
                        (&short_order, "FILL-SHORT", short_qty, 502),
                    ]
                } else {
                    [
                        (&short_order, "FILL-SHORT", short_qty, 501),
                        (&long_order, "FILL-LONG", long_qty, 502),
                    ]
                };
                for (order_id, execution_id, quantity, at) in fills {
                    if quantity == "0" {
                        continue;
                    }
                    let fill = execution::commit::tests::configured_input(
                        &owner,
                        order_id,
                        execution_id,
                        quantity,
                        "100",
                        at,
                    );
                    assert!(matches!(
                        owner.execute(&fill).unwrap(),
                        execution::commit::Reply::Committed {
                            terminal_reason: None,
                            ..
                        }
                    ));
                    fill_count += 1;
                }

                let actual = gross_admission::Projection::current(&owner).unwrap();
                let long_filled = d(long_qty);
                let short_filled = d(short_qty);
                let total_filled = long_filled + short_filled;
                let expected_gross = total_filled * d("100");
                let expected_fees = total_filled * d("0.1");
                assert_eq!(actual.gross, expected_gross, "{long_name}/{short_name}");
                assert!(
                    actual.gross <= bound.worst_gross,
                    "{long_name}/{short_name}"
                );
                assert_eq!(owner.fees, expected_fees, "{long_name}/{short_name}");
                assert!(
                    actual.effective_equity >= bound.equity_floor.unwrap(),
                    "{long_name}/{short_name}"
                );
                assert_eq!(owner.state_version, version_before_fills + fill_count);
                assert_eq!(
                    owner.orders[&long_order].facts.remaining,
                    d("5") - long_filled
                );
                assert_eq!(
                    owner.orders[&short_order].facts.remaining,
                    d("5") - short_filled
                );
                let positions = owner.positions.btc().unwrap();
                assert_eq!(
                    positions.get(&Product::Btc).map(|p| p.contracts),
                    (long_filled > Decimal::ZERO).then_some(long_filled)
                );
                assert_eq!(
                    positions.get(&Product::Eth).map(|p| p.contracts),
                    (short_filled > Decimal::ZERO).then_some(short_filled)
                );
            }
        }
    }

    let (mut seed, mut products) = configured_tests::input(2);
    seed.cash = d("1000");
    for row in &mut products {
        row.taker_fee = d("0.001");
    }
    let a = products[0].product.clone();
    let b = products[1].product.clone();
    let mut owner =
        ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap();
    let long = intent(&owner, "IMPROVEMENT-LONG", a, Side::Long, "5", "100");
    let long_order = admit_historical(&mut owner, &long, "IMPROVEMENT-LONG-EVENT", 1)
        .result
        .order_id
        .unwrap();
    let short = intent(&owner, "IMPROVEMENT-SHORT", b, Side::Short, "5", "100");
    let short_order = admit_historical(&mut owner, &short, "IMPROVEMENT-SHORT-EVENT", 2)
        .result
        .order_id
        .unwrap();
    let bound = gross_admission::Projection::current(&owner).unwrap();
    let short_fee_hold = owner
        .reservation()
        .unwrap()
        .orders
        .iter()
        .find(|order| order.order_id == short_order)
        .unwrap()
        .fee_hold;
    assert_eq!(short_fee_hold, d("0.5"));
    assert_eq!(bound.fee_hold, d("1"));
    let fill = execution::commit::tests::configured_input(
        &owner,
        &short_order,
        "SHORT-PRICE-IMPROVEMENT",
        "5",
        "105",
        501,
    );
    assert!(matches!(
        owner.execute(&fill).unwrap(),
        execution::commit::Reply::Committed {
            terminal_reason: None,
            ..
        }
    ));
    let actual = gross_admission::Projection::current(&owner).unwrap();
    assert_eq!(owner.fees, d("0.525"));
    assert!(owner.fees > short_fee_hold);
    assert!(owner.fees <= bound.fee_hold);
    assert!(actual.gross <= bound.worst_gross);
    assert!(actual.effective_equity >= bound.equity_floor.unwrap());
    assert_eq!(owner.orders[&long_order].facts.remaining, d("5"));
    assert_eq!(owner.orders[&short_order].facts.remaining, Decimal::ZERO);
    let short_position = &owner.positions.btc().unwrap()[&Product::Eth];
    assert_eq!(short_position.side, Side::Short);
    assert_eq!(short_position.contracts, d("5"));
}

#[test]
fn cancel_pending_reservation_releases_only_when_effective() {
    let mut owner = configured_owner(1, "1000", "0", Vec::new());
    let product = Product("arbitrary-private-id".into());
    let first = intent(
        &owner,
        "RESERVE-EXACT",
        product.clone(),
        Side::Long,
        "20",
        "100",
    );
    let admitted = admit(&mut owner, &first);
    assert_eq!(admitted.kind, ReplyKind::Accepted);
    let order_id = admitted.result.order_id.unwrap();
    owner
        .request_cancel(&risk_transition::cancel::RequestInput {
            stamp: source::stamp("CANCEL-REQUEST", 501, 40),
            targets: vec![(
                order_id.clone(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    assert_eq!(
        gross_admission::Projection::current(&owner)
            .unwrap()
            .worst_gross,
        d("2000")
    );
    let blocked = intent(
        &owner,
        "STILL-RESERVED",
        product.clone(),
        Side::Long,
        "0.5",
        "100",
    );
    assert_eq!(
        admit_at(&mut owner, &blocked, 502).result.reason_code,
        Some("GROSS_CAP_EXCEEDED")
    );
    assert_eq!(
        gross_admission::Projection::current(&owner)
            .unwrap()
            .worst_gross,
        d("2000")
    );

    owner
        .effect_cancel(&risk_transition::cancel::EffectInput {
            stamp: source::stamp("CANCEL-EFFECT", 503, 50),
            effects: vec![(
                "CANCEL-REQUEST".into(),
                order_id.clone(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    let after_first_effect = owner.clone();
    owner
        .effect_cancel(&risk_transition::cancel::EffectInput {
            stamp: source::stamp("CANCEL-EFFECT", 503, 50),
            effects: vec![(
                "CANCEL-REQUEST".into(),
                order_id.clone(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    assert_eq!(owner, after_first_effect);
    assert_eq!(
        gross_admission::Projection::current(&owner)
            .unwrap()
            .worst_gross,
        Decimal::ZERO
    );
    let after_effect = intent(&owner, "AFTER-EFFECT", product, Side::Long, "0.5", "100");
    assert_eq!(
        admit_at(&mut owner, &after_effect, 504).kind,
        ReplyKind::Accepted
    );
}

#[test]
fn market_bounds_and_collective_reduce_only_capacity_use_native_working_orders() {
    let product = Product("arbitrary-private-id".into());
    let mut owner = configured_owner(
        1,
        "1000",
        "0",
        vec![position(product.clone(), Side::Long, "1", "100", 0)],
    );
    let mut ordinary_market = OrderIntent::historical_market_fixture(
        &owner,
        "ORDINARY-MARKET".into(),
        "ORDINARY-CLIENT".into(),
        "arbitrary-private-id",
        Side::Long,
        d("0.5"),
        None,
    );
    ordinary_market.reduce_only = false;
    let ordinary_reply = admit_historical(&mut owner, &ordinary_market, "ORDINARY-MARKET-EVENT", 1);
    assert_eq!(ordinary_reply.kind, ReplyKind::Rejected);
    assert_eq!(
        ordinary_reply.result.reason_code,
        Some("GROSS_PRICE_BOUND_UNAVAILABLE")
    );
    assert!(owner.orders.is_empty());
    assert_eq!(owner.gate, Gate::Running);

    let first = reducing_market(&owner, "ONE");
    let first_reply = admit_historical(&mut owner, &first, "REDUCE-EVENT-ONE", 2);
    assert_eq!(first_reply.kind, ReplyKind::Accepted);
    let second = reducing_market(&owner, "TWO");
    let second_reply = admit_historical(&mut owner, &second, "REDUCE-EVENT-TWO", 3);
    assert_eq!(second_reply.kind, ReplyKind::Accepted);
    let first_order = first_reply.result.order_id.as_ref().unwrap();
    owner
        .request_cancel(&risk_transition::cancel::RequestInput {
            stamp: source::stamp("REDUCE-CANCEL-REQUEST", 501, 40),
            targets: vec![(
                first_order.clone(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    let third = reducing_market(&owner, "THREE");
    let third_reply = admit_historical_at(&mut owner, &third, "REDUCE-EVENT-THREE", 4, 502);
    assert_eq!(third_reply.kind, ReplyKind::Rejected);
    assert_eq!(
        third_reply.result.reason_code,
        Some("REDUCE_ONLY_CAPACITY_EXCEEDED")
    );
    assert_eq!(owner.orders.len(), 2);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(
        gross_admission::Projection::current(&owner)
            .unwrap()
            .equity_floor,
        None
    );

    let order_id = first_reply.result.order_id.unwrap();
    let fill = execution::commit::tests::configured_input(
        &owner,
        &order_id,
        "REDUCE-FILL",
        "0.5",
        "100",
        503,
    );
    assert!(matches!(
        owner.execute(&fill).unwrap(),
        execution::commit::Reply::Committed {
            terminal_reason: None,
            ..
        }
    ));
    assert_eq!(owner.positions.btc().unwrap()[&product].contracts, d("0.5"));
    let risk_increasing = intent(
        &owner,
        "BLOCKED-BY-MARKET",
        product.clone(),
        Side::Long,
        "0.5",
        "100",
    );
    let risk_reply = admit_at(&mut owner, &risk_increasing, 504);
    assert_eq!(risk_reply.kind, ReplyKind::Rejected);
    assert_eq!(
        risk_reply.result.reason_code,
        Some("GROSS_PRICE_BOUND_UNAVAILABLE")
    );
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn reduce_only_native_reasons_cover_wrong_side_oversize_and_late_nonreduction() {
    let product = Product("arbitrary-private-id".into());
    let mut wrong_side_owner = configured_owner(
        1,
        "1000",
        "0",
        vec![position(product.clone(), Side::Long, "1", "100", 0)],
    );
    let mut wrong_side = reducing_market(&wrong_side_owner, "WRONG-SIDE");
    wrong_side.side = Side::Long;
    let wrong_reply = admit_historical(&mut wrong_side_owner, &wrong_side, "WRONG-SIDE-EVENT", 1);
    assert_eq!(wrong_reply.kind, ReplyKind::Rejected);
    assert_eq!(
        wrong_reply.result.reason_code,
        Some("REDUCE_ONLY_NOT_REDUCING")
    );
    assert_eq!(wrong_side_owner.gate, Gate::Running);

    let mut oversize_owner = configured_owner(
        1,
        "1000",
        "0",
        vec![position(product.clone(), Side::Long, "1", "100", 0)],
    );
    let mut oversize = reducing_market(&oversize_owner, "OVERSIZE");
    oversize.quantity = d("1.5");
    let oversize_reply = admit_historical(&mut oversize_owner, &oversize, "OVERSIZE-EVENT", 1);
    assert_eq!(oversize_reply.kind, ReplyKind::Rejected);
    assert_eq!(
        oversize_reply.result.reason_code,
        Some("REDUCE_ONLY_NOT_REDUCING")
    );
    assert_eq!(oversize_owner.gate, Gate::Running);

    let mut late_owner = configured_owner(
        1,
        "1000",
        "0",
        vec![position(product.clone(), Side::Long, "1", "100", 0)],
    );
    let ordinary_close = intent(
        &late_owner,
        "ORDINARY-CLOSE",
        product.clone(),
        Side::Short,
        "1",
        "100",
    );
    let close = admit_historical_at(
        &mut late_owner,
        &ordinary_close,
        "ORDINARY-CLOSE-EVENT",
        1,
        501,
    );
    assert_eq!(close.kind, ReplyKind::Accepted);
    let stale_reduction = reducing_market(&late_owner, "LATE");
    let reduction = admit_historical_at(&mut late_owner, &stale_reduction, "LATE-RO-EVENT", 2, 502);
    assert_eq!(reduction.kind, ReplyKind::Accepted);
    let reduction_order = reduction.result.order_id.unwrap();
    let close_fill = execution::commit::tests::configured_input(
        &late_owner,
        close.result.order_id.as_ref().unwrap(),
        "ORDINARY-CLOSE-FILL",
        "1",
        "100",
        503,
    );
    assert!(matches!(
        late_owner.execute(&close_fill).unwrap(),
        execution::commit::Reply::Committed {
            terminal_reason: None,
            ..
        }
    ));
    assert!(!late_owner.positions.btc().unwrap().contains_key(&product));
    let late_fill = execution::commit::tests::configured_input(
        &late_owner,
        &reduction_order,
        "LATE-RO-FILL",
        "0.5",
        "100",
        504,
    );
    assert!(matches!(
        late_owner.execute(&late_fill).unwrap(),
        execution::commit::Reply::Rejected("REDUCE_ONLY_NOT_REDUCING")
    ));
    assert!(!late_owner.positions.btc().unwrap().contains_key(&product));
    assert_eq!(late_owner.gate, Gate::Running);
}

#[test]
fn nonpositive_equity_projection_is_explicit_and_does_not_add_a_lock() {
    let mut owner = configured_owner(1, "0", "0", Vec::new());
    let projection = gross_admission::project(&owner).unwrap();
    let fields = projection
        .object(
            &[
                "effective_equity",
                "gross_ratio",
                "equity_floor",
                "bound_reason",
            ],
            &[
                "schema_version",
                "risk_policy_id",
                "bound_scope",
                "account_key",
                "account_version",
                "valuation_context_id",
                "cap",
                "gross",
                "net",
                "worst_gross",
                "order_loss_hold",
                "fee_hold",
            ],
        )
        .unwrap();
    assert_eq!(fields["effective_equity"].decimal().unwrap(), Decimal::ZERO);
    assert_eq!(fields["gross_ratio"], super::super::wire::Json::Null);
    let candidate = intent(
        &owner,
        "NONPOSITIVE-EQUITY",
        Product("arbitrary-private-id".into()),
        Side::Long,
        "0.5",
        "100",
    );
    let reply = admit(&mut owner, &candidate);
    assert_eq!(reply.kind, ReplyKind::Rejected);
    assert_eq!(reply.result.reason_code, Some("INSUFFICIENT_SHARED_EQUITY"));
    assert_eq!(owner.gate, Gate::Running);
    assert!(owner.orders.is_empty());
}

#[test]
fn over_cap_seed_blocks_new_risk_but_keeps_native_reducing_market_available() {
    let (_seed, products) = configured_tests::input(2);
    let a = products[0].product.clone();
    let b = products[1].product.clone();
    let mut owner = configured_owner(
        2,
        "1000",
        "0",
        vec![
            position(a.clone(), Side::Long, "11", "100", 0),
            position(b, Side::Short, "11", "100", 1),
        ],
    );
    let risk = intent(
        &owner,
        "OVER-SEED-RISK",
        a.clone(),
        Side::Long,
        "0.5",
        "100",
    );
    let risk_reply = admit_at(&mut owner, &risk, 501);
    assert_eq!(risk_reply.kind, ReplyKind::Rejected);
    assert_eq!(risk_reply.result.reason_code, Some("GROSS_CAP_EXCEEDED"));
    let projection = gross_admission::Projection::current(&owner).unwrap();
    assert_eq!(projection.gross, d("2200"));
    assert_eq!(projection.effective_equity, d("1000"));
    assert!(projection.gross > d("2") * projection.effective_equity);
    assert_eq!(owner.gate, Gate::Running);

    let reduction = reducing_market_for(&owner, "OVER-SEED-SAFE-EXIT", &a);
    let accepted = admit_historical_at(&mut owner, &reduction, "OVER-SEED-REDUCE-EVENT", 1, 502);
    assert_eq!(accepted.kind, ReplyKind::Accepted);
    assert_eq!(accepted.result.reason_code, None);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.orders.len(), 1);
    let exit_fill = execution::commit::tests::configured_input(
        &owner,
        accepted.result.order_id.as_ref().unwrap(),
        "OVER-SEED-SAFE-EXIT-FILL",
        "0.5",
        "100",
        503,
    );
    assert!(matches!(
        owner.execute(&exit_fill).unwrap(),
        execution::commit::Reply::Committed {
            terminal_reason: None,
            ..
        }
    ));
    assert_eq!(owner.positions.btc().unwrap()[&a].contracts, d("10.5"));
    assert_eq!(
        gross_admission::Projection::current(&owner).unwrap().gross,
        d("2150")
    );
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn gross_profile_fee_and_mark_coverage_are_closed_at_construction() {
    let (seed, _) = configured_tests::input(1);
    let key = seed.key;
    for rate in ["0", "1"] {
        let config = gross_configuration(rate, 3000);
        assert!(super::super::wire::profiles::construct(
            "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1",
            key.clone(),
            Some(&config),
        )
        .is_ok());
    }
    for rate in ["-0.0001", "1.0001"] {
        let config = gross_configuration(rate, 3000);
        assert_eq!(
            super::super::wire::profiles::construct(
                "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1",
                key.clone(),
                Some(&config),
            )
            .unwrap_err(),
            "INVALID_SCHEMA"
        );
    }
    let uncovered = gross_configuration("0.001", 400);
    assert_eq!(
        super::super::wire::profiles::construct(
            "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1",
            key,
            Some(&uncovered),
        )
        .unwrap_err(),
        "INVALID_SCHEMA"
    );
}

#[test]
fn legacy_reservation_does_not_evaluate_gross_only_aggregates() {
    let (mut seed, products) = configured_tests::input(2);
    seed.cash = d("20000000000000000000000000000");
    let mut products = products;
    for product in &mut products {
        product.tiers[0].tiers[0].maximum = d("1000000000000000000000000000");
    }
    seed.positions = products
        .iter()
        .enumerate()
        .map(|(sequence, row)| {
            position(
                row.product.clone(),
                Side::Long,
                "500000000000000000000000000",
                "100",
                sequence as u64,
            )
        })
        .collect();
    let legacy = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
    let snapshot = legacy.reservation().unwrap();
    assert_eq!(snapshot.gross, Decimal::ZERO);
    assert_eq!(snapshot.net, Decimal::ZERO);
    assert_eq!(snapshot.worst_gross, Decimal::ZERO);
}
