use super::*;

fn configured_ab() -> (CleanSeed, Vec<ConfiguredProduct>) {
    let (mut seed, mut products) = configured_tests::input(2);
    seed.effective_at = 499;
    seed.positions.clear();
    seed.orders.clear();
    for (index, id) in ["A-USDT-SWAP", "B-USDT-SWAP"].into_iter().enumerate() {
        let row = &mut products[index];
        row.product = Product(id.into());
        for spec in &mut row.specs {
            spec.product = row.product.clone();
        }
        for tier in &mut row.tiers {
            tier.product = row.product.clone();
        }
        for mark in &mut row.marks {
            mark.product = row.product.clone();
            mark.price = if index == 0 { d("105") } else { d("100") };
        }
    }
    (seed, products)
}

fn historical_orders(
    owner: &ScenarioAccount,
    sequences: &[(&str, i64)],
) -> Vec<historical::WorkingOrderMeta> {
    sequences
        .iter()
        .filter_map(|(id, sequence)| owner.orders.get(*id).map(|order| (*id, *sequence, order)))
        .filter(|(_, _, order)| order.facts.projects_remainder("INVALID_HISTORICAL_ORDER_SNAPSHOT").unwrap())
        .filter(|(_, _, order)| !matches!(order.cancel, risk_transition::cancel::State::EffectiveCanceled(_)))
        .map(|(id, sequence, order)| historical::WorkingOrderMeta {
            order_id: id.into(),
            product: order.facts.product.btc().unwrap().clone(),
            order_version: order.version,
            status: order.facts.status.clone(),
            remaining: order.facts.remaining,
            accepted_at: order.created_at,
            accepted_source_sequence: sequence,
            kind: historical::OrderKind::Limit,
            side: order.facts.side,
            limit_price: Some(order.facts.price),
            risk_cancel_pending: matches!(
                order.cancel,
                risk_transition::cancel::State::Requested(ref request)
                    if matches!(request.reason, risk_transition::cancel::Reason::RiskShortfall | risk_transition::cancel::Reason::MmrBreach)
            ),
        })
        .collect()
}

#[derive(Clone, Copy)]
struct NodeBar<'a> {
    model: &'a str,
    bar_open_ms: i64,
    step_index: u8,
    a_ohlc: [Decimal; 4],
    a_volume: &'a str,
    a_mark: [Decimal; 4],
}

fn node_input(
    owner: &ScenarioAccount,
    node: NodeBar<'_>,
    orders: &[(&str, i64)],
) -> historical::NodeInput {
    let products = [Product("A-USDT-SWAP".into()), Product("B-USDT-SWAP".into())];
    historical::NodeInput {
        model_id: node.model.into(),
        model_version: "1".into(),
        run_contract_hash: [44; 32],
        step_index: node.step_index,
        market_slippage_bps: Decimal::ZERO,
        bars: products
            .into_iter()
            .enumerate()
            .map(|(index, product)| historical::BarPair {
                product,
                bar_open_ms: node.bar_open_ms,
                bar_duration_ms: 60_000,
                trade_ohlc: if index == 0 {
                    node.a_ohlc
                } else {
                    [d("100"); 4]
                },
                mark_ohlc: if index == 0 {
                    node.a_mark
                } else {
                    [d("100"); 4]
                },
                volume_contracts: if index == 0 {
                    d(node.a_volume)
                } else {
                    Decimal::ZERO
                },
                confirmed: true,
                trade_source_row_hash: [50 + index as u8 + node.step_index; 32],
                mark_source_row_hash: [80 + index as u8 + node.step_index; 32],
            })
            .collect(),
        working_orders: historical_orders(owner, orders),
    }
}

fn market_intent(
    owner: &ScenarioAccount,
    id: &str,
    product: &'static str,
    side: Side,
    quantity: Decimal,
    limit_price: Option<Decimal>,
) -> admission::OrderIntent {
    admission::OrderIntent::historical_market_fixture(
        owner,
        format!("I-{id}"),
        format!("C-{id}"),
        product,
        side,
        quantity,
        limit_price,
    )
}

fn admit_market(owner: &mut ScenarioAccount, id: &str, side: Side) -> String {
    admit_market_quantity(owner, id, side, d("0.5"))
}

fn admit_market_quantity(
    owner: &mut ScenarioAccount,
    id: &str,
    side: Side,
    quantity: Decimal,
) -> String {
    let intent = market_intent(owner, id, "A-USDT-SWAP", side, quantity, None);
    let group = market_group(owner, id, intent);
    let completion = owner.apply_group(&group).unwrap();
    assert!(completion.failure.is_none());
    assert!(completion.rejections.is_empty());
    owner
        .intent_results
        .values()
        .find(|result| result.created_at_event_id() == format!("E-{id}"))
        .and_then(|result| result.order_id().map(str::to_owned))
        .unwrap()
}

fn market_group(owner: &ScenarioAccount, id: &str, intent: admission::OrderIntent) -> group::Group {
    let effective_at = 1_000 * 16 + 8;
    let mut stamp = source::stamp(&format!("E-{id}"), effective_at, 60);
    stamp.ordering_contract_id = "HISTORICAL_ORDER_V1".into();
    stamp.source_sequence = Some(1);
    group::Group {
        group_id: format!("G-{id}"),
        account_key: owner.key.clone(),
        ordering_contract_id: "HISTORICAL_ORDER_V1".into(),
        group_effective_at: effective_at,
        declared_member_count: 1,
        members: vec![group::Member {
            stamp,
            input: group::Input::Intent(intent),
        }],
    }
}

fn assert_market_failure_preserves_financial_state(
    owner: &ScenarioAccount,
    before: &ScenarioAccount,
    fault: Fault,
) {
    assert_eq!(owner.gate, Gate::Failed(fault.into()));
    assert_eq!(owner.cash, before.cash);
    assert_eq!(owner.positions, before.positions);
    assert_eq!(owner.orders, before.orders);
    assert_eq!(owner.fees, before.fees);
    assert_eq!(owner.gross_realized, before.gross_realized);
    assert_eq!(owner.state_version, before.state_version);
    assert_eq!(owner.execution_receipts, before.execution_receipts);
    assert_eq!(owner.intent_results, before.intent_results);
    assert_eq!(owner.reservation().unwrap(), before.reservation().unwrap());
}

#[test]
fn configured_historical_market_order_keeps_kind_and_settles_only_on_later_capacity() {
    for (id, side, expected_fill) in [
        ("LONG", Side::Long, d("91")),
        ("SHORT", Side::Short, d("89")),
    ] {
        let (seed, products) = configured_ab();
        let mut products = products;
        for product in &mut products {
            for mark in &mut product.marks {
                mark.valid_to = i64::MAX;
            }
        }
        let mut seed = seed;
        seed.effective_at = 0;
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let before_admission = node_input(
            &owner,
            NodeBar {
                model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
                bar_open_ms: 1_000,
                step_index: 0,
                a_ohlc: [d("100"), d("110"), d("90"), d("105")],
                a_volume: "4",
                a_mark: [d("105"), d("115"), d("95"), d("110")],
            },
            &[],
        );
        assert!(owner
            .historical_market_step(&before_admission)
            .unwrap()
            .products[0]
            .fills
            .is_empty());
        let order_id = admit_market(&mut owner, id, side);
        assert_eq!(
            owner.admitted_order_type(&owner.orders[&order_id].facts),
            admission::OrderType::Market
        );
        assert_eq!(owner.orders[&order_id].facts.price, d("105"));
        let projection = owner.historical_working_orders().unwrap();
        assert_eq!(projection.len(), 1);
        assert_eq!(projection[0].order_id, order_id);
        assert_eq!(projection[0].kind, historical::OrderKind::Market);
        assert_eq!(projection[0].limit_price, None);

        let mut same_segment = node_input(
            &owner,
            NodeBar {
                model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
                bar_open_ms: 1_000,
                step_index: 1,
                a_ohlc: [d("100"), d("110"), d("90"), d("105")],
                a_volume: "4",
                a_mark: [d("105"), d("115"), d("95"), d("110")],
            },
            &[],
        );
        same_segment.working_orders = projection.clone();
        same_segment.market_slippage_bps = d("10");
        assert!(owner
            .historical_market_step(&same_segment)
            .unwrap()
            .products[0]
            .fills
            .is_empty());
        assert_eq!(owner.orders[&order_id].facts.remaining, d("0.5"));

        let mut zero_owner = owner.clone();
        let mut zero_capacity = node_input(
            &owner,
            NodeBar {
                model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
                bar_open_ms: 1_000,
                step_index: 2,
                a_ohlc: [d("100"), d("110"), d("90"), d("105")],
                a_volume: "0",
                a_mark: [d("100"), d("110"), d("90"), d("105")],
            },
            &[],
        );
        zero_capacity.working_orders = projection.clone();
        zero_capacity.market_slippage_bps = d("10");
        let before = zero_owner.clone();
        let zero = zero_owner.historical_market_step(&zero_capacity).unwrap();
        assert!(zero.products[0].fills.is_empty());
        assert_eq!(zero_owner.orders, before.orders);
        assert_eq!(zero_owner.cash, before.cash);
        assert_eq!(zero_owner.fees, before.fees);
        assert_eq!(zero_owner.gross_realized, before.gross_realized);

        let mut input = node_input(
            &owner,
            NodeBar {
                model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
                bar_open_ms: 1_000,
                step_index: 2,
                a_ohlc: [d("100"), d("110"), d("90"), d("105")],
                a_volume: "4",
                a_mark: [d("100"), d("110"), d("90"), d("105")],
            },
            &[],
        );
        input.working_orders = projection;
        input.market_slippage_bps = d("10");
        let result = owner.historical_market_step(&input).unwrap();
        assert_eq!(result.raw_time_ms, 41_000);
        assert_eq!(result.products[0].fills.len(), 1);
        assert_eq!(result.products[0].fills[0].order_id, order_id);
        assert_eq!(result.products[0].fills[0].quantity, d("0.5"));
        assert_eq!(result.products[0].fills[0].price, expected_fill);
        let fill = &result.products[0].fills[0];
        let settled_cash = owner.cash.clone();
        let settled_fees = owner.fees;
        let settled_gross = owner.gross_realized;
        let settled_version = owner.state_version;
        let duplicate = execution::historical_candidate(
            &owner,
            &order_id,
            fill.price,
            fill.quantity,
            result.effective_at,
            fill.execution_id,
        )
        .unwrap();
        let duplicate_stamp = source::stamp(&fill.source_event_id, result.effective_at, 30);
        assert!(matches!(
            owner.execute_stamped(&duplicate, &duplicate_stamp, |_| Ok(())),
            Ok(execution::commit::Reply::Duplicate(_))
        ));
        assert_eq!(owner.cash, settled_cash);
        assert_eq!(owner.fees, settled_fees);
        assert_eq!(owner.gross_realized, settled_gross);
        assert_eq!(owner.state_version, settled_version);
        let receipt = owner.execution_receipts.values().next().unwrap();
        assert_eq!(receipt.order_type(), admission::OrderType::Market);
        assert!(receipt
            .delivery_json(&format!("C-{id}"))
            .unwrap()
            .canonical()
            .unwrap()
            .contains("\"limit_price\":null"));
        assert_eq!(owner.orders[&order_id].facts.status, "FILLED");
        assert_eq!(owner.orders[&order_id].facts.filled, d("0.5"));
    }
}

#[test]
fn historical_market_admission_rejections_do_not_publish_account_mutations() {
    let (seed, mut products) = configured_ab();
    for product in &mut products {
        for mark in &mut product.marks {
            mark.valid_to = i64::MAX;
        }
    }

    let mut limited = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
    let before = limited.clone();
    let intent = market_intent(
        &limited,
        "LIMITED-MARKET",
        "A-USDT-SWAP",
        Side::Long,
        d("0.5"),
        Some(d("100")),
    );
    let completion = limited
        .apply_group(&market_group(&limited, "LIMITED-MARKET", intent))
        .unwrap();
    let error = completion.failure.unwrap();
    assert_eq!(error, "INVALID_BTC_INTENT");
    assert_market_failure_preserves_financial_state(&limited, &before, error);

    let (seed, products) = configured_ab(); // Fixture mark ends at 3000, before admission at 16008.
    let mut uncovered = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let before = uncovered.clone();
    let intent = market_intent(
        &uncovered,
        "UNCOVERED-MARKET",
        "A-USDT-SWAP",
        Side::Short,
        d("0.5"),
        None,
    );
    let completion = uncovered
        .apply_group(&market_group(&uncovered, "UNCOVERED-MARKET", intent))
        .unwrap();
    let error = completion.failure.unwrap();
    assert_eq!(error, "UNSUPPORTED_CONTEXT_TRANSITION");
    assert_market_failure_preserves_financial_state(&uncovered, &before, error);
}

#[test]
fn historical_market_reference_admission_rounds_adversely_and_receipts_fix_price() {
    for (id, side, expected) in [
        ("ROUND-LONG", Side::Long, d("106")),
        ("ROUND-SHORT", Side::Short, d("105")),
    ] {
        let (seed, mut products) = configured_ab();
        products[0].specs[0].tick = d("1");
        products[0].marks[0].price = d("105.3");
        for product in &mut products {
            for mark in &mut product.marks {
                mark.valid_to = i64::MAX;
            }
        }
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let order_id = admit_market(&mut owner, id, side);
        let receipt = &owner.intent_results[&format!("I-{id}")];
        assert_eq!(receipt.order_type(), admission::OrderType::Market);
        assert_eq!(
            receipt.delivery_order(&format!("E-{id}")).unwrap().price,
            expected
        );
        assert_eq!(owner.orders[&order_id].facts.price, expected);
    }
}

#[test]
fn historical_step_commits_through_the_existing_execution_owner() {
    let (seed, config, mut marks) = fixture();
    for mark in &mut marks {
        mark.valid_to = i64::MAX;
    }
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let input = historical::NodeInput {
        model_id: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1".into(),
        model_version: "1".into(),
        run_contract_hash: [4; 32],
        step_index: 0,
        market_slippage_bps: d("0"),
        bars: [Product::Btc, Product::Eth]
            .into_iter()
            .map(|product| historical::BarPair {
                product,
                bar_open_ms: 31,
                bar_duration_ms: 60_000,
                trade_ohlc: [d("50000.2"), d("50001"), d("49999"), d("50000.5")],
                mark_ohlc: [d("50000"), d("50001"), d("49999"), d("50000")],
                volume_contracts: d("4"),
                confirmed: true,
                trade_source_row_hash: [5; 32],
                mark_source_row_hash: [6; 32],
            })
            .collect(),
        working_orders: vec![historical::WorkingOrderMeta {
            order_id: "O1".into(),
            product: Product::Btc,
            order_version: 0,
            status: "PARTIALLY_FILLED".into(),
            remaining: d("1"),
            accepted_at: 500,
            accepted_source_sequence: 0,
            kind: historical::OrderKind::Limit,
            side: Side::Short,
            limit_price: Some(d("50000.1")),
            risk_cancel_pending: false,
        }],
    };
    let before = owner.clone();
    let mut economic_drift = input.clone();
    economic_drift.working_orders[0].limit_price = Some(d("50000.2"));
    assert_eq!(
        owner.historical_market_step(&economic_drift),
        Err("INVALID_HISTORICAL_ORDER_SNAPSHOT")
    );
    assert_eq!(owner, before);
    let mut market = input.clone();
    market.working_orders[0].kind = historical::OrderKind::Market;
    market.working_orders[0].limit_price = None;
    assert_eq!(
        owner.historical_market_step(&market),
        Err("INVALID_HISTORICAL_ORDER_SNAPSHOT")
    );
    assert_eq!(owner, before);
    let mut spec_crossing = input.clone();
    spec_crossing.step_index = 1;
    for bar in &mut spec_crossing.bars {
        bar.bar_open_ms = 1_999;
    }
    let mut boundary_owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let boundary_before = boundary_owner.clone();
    assert_eq!(
        boundary_owner.historical_market_step(&spec_crossing),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    assert_eq!(boundary_owner, boundary_before);
    let result = owner.historical_market_step(&input).unwrap();
    assert_eq!(result.raw_time_ms, 31);
    assert_eq!(result.effective_at, 31 * 16 + 7);
    assert_eq!(result.products[0].fills.len(), 1);
    assert_eq!(result.products[0].fills[0].order_id, "O1");
    assert_eq!(result.products[0].fills[0].quantity, d("1"));
    assert_eq!(owner.orders["O1"].facts.status, "FILLED");
    assert_eq!(owner.execution_receipts.len(), 1);
}

#[test]
fn configured_seed_order_projection_uses_owner_id_rank_and_seed_time() {
    let (mut seed, products) = configured_ab();
    seed.orders = ["SEED-ORDER-2", "SEED-ORDER-1"]
        .into_iter()
        .map(|order_id| SeedOrder {
            intent_id: format!("INTENT-{order_id}"),
            order_id: order_id.into(),
            client_id: format!("CLIENT-{order_id}"),
            strategy_id: "seed-policy".into(),
            product: ProfileProduct::BtcEth(Product("A-USDT-SWAP".into())),
            side: Side::Long,
            price: d("100"),
            reduce_only: false,
            original: d("0.5"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("0.5"),
            status: "OPEN".into(),
        })
        .collect();
    let owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let projected = owner.historical_working_orders().unwrap();
    assert_eq!(
        projected
            .iter()
            .map(|order| (
                order.order_id.as_str(),
                order.accepted_at,
                order.accepted_source_sequence,
                order.kind,
                order.side,
                order.limit_price,
                order.remaining,
            ))
            .collect::<Vec<_>>(),
        [
            (
                "SEED-ORDER-1",
                seed.effective_at,
                1,
                historical::OrderKind::Limit,
                Side::Long,
                Some(d("100")),
                d("0.5"),
            ),
            (
                "SEED-ORDER-2",
                seed.effective_at,
                2,
                historical::OrderKind::Limit,
                Side::Long,
                Some(d("100")),
                d("0.5"),
            ),
        ]
    );

    let mut invalid = owner.clone();
    invalid.orders.get_mut("SEED-ORDER-1").unwrap().created_at += 1;
    assert_eq!(
        invalid.historical_working_orders(),
        Err("INVALID_HISTORICAL_ORDER_SNAPSHOT")
    );
    let mut invalid = owner;
    invalid
        .orders
        .get_mut("SEED-ORDER-1")
        .unwrap()
        .facts
        .order_id = "OTHER-ORDER".into();
    assert_eq!(
        invalid.historical_working_orders(),
        Err("INVALID_HISTORICAL_ORDER_SNAPSHOT")
    );
}

#[test]
fn h04_owner_steps_honor_pending_cancel_then_effect_and_exact_settlement() {
    let (mut seed, mut products) = configured_ab();
    seed.cash = d("1000");
    products[0].taker_fee = d("0.001");
    products[0].specs[0].tick = d("0.5");
    products[0].specs[0].lot = d("0.5");
    products[0].specs[0].minimum = d("0.5");
    seed.orders.push(SeedOrder {
        intent_id: "H04-INTENT".into(),
        order_id: "H04-ORDER-1".into(),
        client_id: "H04-CLIENT".into(),
        strategy_id: "H04-STRATEGY".into(),
        product: ProfileProduct::BtcEth(products[0].product.clone()),
        side: Side::Short,
        price: d("105"),
        reduce_only: false,
        original: d("3"),
        filled: Decimal::ZERO,
        canceled: Decimal::ZERO,
        remaining: d("3"),
        status: "OPEN".into(),
    });
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
    let orders = [("H04-ORDER-1", 1)];
    for (step, expected_remaining) in [(0, "2"), (1, "1")] {
        let input = node_input(
            &owner,
            NodeBar {
                model: "OHLC4_OPEN_LOW_HIGH_CLOSE_V1",
                bar_open_ms: 500,
                step_index: step,
                a_ohlc: [d("105"), d("110"), d("100"), d("105")],
                a_volume: "3",
                a_mark: [d("105"), d("110"), d("100"), d("105")],
            },
            &orders,
        );
        let result = owner.historical_market_step(&input).unwrap();
        assert_eq!(result.products[0].fills.len(), 1);
        assert_eq!(
            (
                result.products[0].fills[0].quantity,
                result.products[0].fills[0].price
            ),
            (d("1"), d("105"))
        );
        assert_eq!(
            owner.orders["H04-ORDER-1"].facts.remaining,
            d(expected_remaining)
        );
    }
    owner
        .request_cancel(&risk_transition::cancel::RequestInput {
            stamp: source::stamp("H04-CANCEL-REQUEST", 328_009, 40),
            targets: vec![(
                "H04-ORDER-1".into(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    let step3 = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_LOW_HIGH_CLOSE_V1",
            bar_open_ms: 500,
            step_index: 2,
            a_ohlc: [d("105"), d("110"), d("100"), d("105")],
            a_volume: "3",
            a_mark: [d("105"), d("110"), d("100"), d("105")],
        },
        &orders,
    );
    let result = owner.historical_market_step(&step3).unwrap();
    assert_eq!(result.products[0].fills.len(), 1);
    assert_eq!(
        (
            result.products[0].fills[0].quantity,
            result.products[0].fills[0].price
        ),
        (d("0.5"), d("105"))
    );
    assert_eq!(owner.orders["H04-ORDER-1"].facts.remaining, d("0.5"));
    owner
        .effect_cancel(&risk_transition::cancel::EffectInput {
            stamp: source::stamp("H04-CANCEL-EFFECT", 648_009, 50),
            effects: vec![(
                "H04-CANCEL-REQUEST".into(),
                "H04-ORDER-1".into(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    let later = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_LOW_HIGH_CLOSE_V1",
            bar_open_ms: 60_500,
            step_index: 2,
            a_ohlc: [d("105"), d("110"), d("105"), d("110")],
            a_volume: "3",
            a_mark: [d("110"); 4],
        },
        &orders,
    );
    let result = owner.historical_market_step(&later).unwrap();
    assert!(result.products[0].fills.is_empty());
    assert_eq!(owner.cash, d("999.7375"));
    assert_eq!(owner.fees, d("0.2625"));
    let position = &owner.positions.btc().unwrap()[&products[0].product];
    assert_eq!(
        (position.side, position.contracts, position.entry_basis),
        (Side::Short, d("2.5"), d("262.5"))
    );
    let valuation = owner.reservation().unwrap();
    assert_eq!(
        (valuation.equity, valuation.maintenance_margin),
        (d("987.2375"), d("1.375"))
    );
    assert!(owner.orders["H04-ORDER-1"]
        .facts
        .projects_remainder("INVALID_ORDER")
        .is_ok_and(|working| !working));
}

#[test]
fn historical_market_partial_keeps_null_limit_and_settles_remaining_capacity_once() {
    let (mut seed, mut products) = configured_ab();
    seed.effective_at = 0;
    for product in &mut products {
        for mark in &mut product.marks {
            mark.valid_to = i64::MAX;
        }
    }
    let a = products[0].product.clone();
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let order_id = admit_market_quantity(&mut owner, "PARTIAL-MARKET", Side::Long, d("1.5"));
    assert_eq!(owner.orders[&order_id].facts.price, d("105"));

    let mut first = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 1_000,
            step_index: 2,
            a_ohlc: [d("100"), d("110"), d("90"), d("105")],
            a_volume: "4",
            a_mark: [d("100"), d("110"), d("90"), d("105")],
        },
        &[],
    );
    first.working_orders = owner.historical_working_orders().unwrap();
    let first = owner.historical_market_step(&first).unwrap();
    assert_eq!(first.raw_time_ms, 41_000);
    assert_eq!(first.products[0].fills.len(), 1);
    assert_eq!(
        (
            first.products[0].fills[0].order_id.as_str(),
            first.products[0].fills[0].quantity,
            first.products[0].fills[0].price
        ),
        (order_id.as_str(), d("1"), d("90"))
    );
    let remaining = owner.historical_working_orders().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].order_id, order_id);
    assert_eq!(remaining[0].kind, historical::OrderKind::Market);
    assert_eq!(remaining[0].limit_price, None);
    assert_eq!(remaining[0].remaining, d("0.5"));
    let first_receipt = owner
        .execution_receipts
        .values()
        .find(|receipt| receipt.historical_order_id() == order_id)
        .unwrap();
    assert_eq!(first_receipt.order_type(), admission::OrderType::Market);
    assert!(first_receipt
        .delivery_json("C-PARTIAL-MARKET")
        .unwrap()
        .canonical()
        .unwrap()
        .contains("\"limit_price\":null"));

    let mut second = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 1_000,
            step_index: 3,
            a_ohlc: [d("100"), d("110"), d("90"), d("105")],
            a_volume: "4",
            a_mark: [d("100"), d("110"), d("90"), d("105")],
        },
        &[],
    );
    second.working_orders = owner.historical_working_orders().unwrap();
    let second = owner.historical_market_step(&second).unwrap();
    assert_eq!(second.raw_time_ms, 61_000);
    assert_eq!(second.products[0].fills.len(), 1);
    assert_eq!(
        (
            second.products[0].fills[0].quantity,
            second.products[0].fills[0].price
        ),
        (d("0.5"), d("105"))
    );
    assert_eq!(owner.orders[&order_id].facts.status, "FILLED");
    assert_eq!(owner.orders[&order_id].facts.remaining, Decimal::ZERO);
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d("121.0575"), d("0.1425"), d("0"))
    );
    assert_eq!(owner.positions.btc().unwrap()[&a].contracts, d("1.5"));
    assert_eq!(owner.execution_receipts.len(), 2);
    assert_eq!(owner.gate, Gate::Running);

    let settled = (
        owner.cash,
        owner.fees,
        owner.gross_realized,
        owner.state_version,
    );
    for fill in [&first.products[0].fills[0], &second.products[0].fills[0]] {
        let duplicate = execution::historical_candidate(
            &owner,
            &order_id,
            fill.price,
            fill.quantity,
            if fill.execution_id == first.products[0].fills[0].execution_id {
                first.effective_at
            } else {
                second.effective_at
            },
            fill.execution_id,
        )
        .unwrap();
        let effective_at = if fill.execution_id == first.products[0].fills[0].execution_id {
            first.effective_at
        } else {
            second.effective_at
        };
        let stamp = source::stamp(&fill.source_event_id, effective_at, 30);
        assert!(matches!(
            owner.execute_stamped(&duplicate, &stamp, |_| Ok(())),
            Ok(execution::commit::Reply::Duplicate(_))
        ));
    }
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.gross_realized,
            owner.state_version
        ),
        settled
    );
    assert_eq!(owner.execution_receipts.len(), 2);
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn subminimum_segment_fills_allow_completion_and_cancellation_of_dust() {
    for market in [true, false] {
        let (mut seed, mut products) = configured_ab();
        seed.effective_at = 0;
        seed.cash = d("1000");
        products[0].specs[0].lot = d("0.5");
        products[0].specs[0].minimum = d("1");
        for product in &mut products {
            for mark in &mut product.marks {
                mark.valid_to = i64::MAX;
            }
        }
        if !market {
            seed.orders.push(SeedOrder {
                intent_id: "DUST-LIMIT-INTENT".into(),
                order_id: "DUST-LIMIT".into(),
                client_id: "DUST-LIMIT-CLIENT".into(),
                strategy_id: "DUST-LIMIT-STRATEGY".into(),
                product: ProfileProduct::BtcEth(products[0].product.clone()),
                side: Side::Long,
                price: d("110"),
                reduce_only: false,
                original: d("1.5"),
                filled: Decimal::ZERO,
                canceled: Decimal::ZERO,
                remaining: d("1.5"),
                status: "OPEN".into(),
            });
        }
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let order_id = if market {
            admit_market_quantity(&mut owner, "DUST-MARKET", Side::Long, d("1.5"))
        } else {
            "DUST-LIMIT".into()
        };

        let fill_segment = |owner: &mut ScenarioAccount, bar_open_ms| {
            let mut input = node_input(
                owner,
                NodeBar {
                    model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
                    bar_open_ms,
                    step_index: 1,
                    a_ohlc: [d("100"), d("110"), d("90"), d("105")],
                    a_volume: "2",
                    a_mark: [d("100"), d("110"), d("90"), d("105")],
                },
                &[],
            );
            input.working_orders = owner.historical_working_orders().unwrap();
            owner.historical_market_step(&input).unwrap()
        };
        let first = fill_segment(&mut owner, 2_000);
        assert_eq!(first.products[0].fills.len(), 1);
        assert_eq!(first.products[0].fills[0].quantity, d("0.5"));
        assert_eq!(owner.orders[&order_id].facts.remaining, d("1"));
        assert_eq!(
            owner.reservation().unwrap().orders[0].remaining_contracts,
            d("1")
        );

        let second = fill_segment(&mut owner, 4_000);
        assert_eq!(second.products[0].fills.len(), 1);
        assert_eq!(second.products[0].fills[0].quantity, d("0.5"));
        assert_eq!(owner.orders[&order_id].facts.remaining, d("0.5"));
        assert_eq!(
            owner.reservation().unwrap().orders[0].remaining_contracts,
            d("0.5")
        );

        let mut canceled_dust = owner.clone();
        canceled_dust
            .request_cancel(&risk_transition::cancel::RequestInput {
                stamp: source::stamp("DUST-CANCEL-REQUEST", 70_000 * 16 + 10, 40),
                targets: vec![(
                    order_id.clone(),
                    risk_transition::cancel::Reason::ExplicitScenario,
                )],
            })
            .unwrap();
        canceled_dust
            .effect_cancel(&risk_transition::cancel::EffectInput {
                stamp: source::stamp("DUST-CANCEL-EFFECT", 70_000 * 16 + 11, 50),
                effects: vec![(
                    "DUST-CANCEL-REQUEST".into(),
                    order_id.clone(),
                    risk_transition::cancel::Reason::ExplicitScenario,
                )],
            })
            .unwrap();
        assert_eq!(canceled_dust.orders[&order_id].facts.status, "CANCELED");
        assert!(canceled_dust.reservation().unwrap().orders.is_empty());

        let third = fill_segment(&mut owner, 6_000);
        assert_eq!(third.products[0].fills.len(), 1);
        assert_eq!(third.products[0].fills[0].quantity, d("0.5"));
        assert_eq!(owner.orders[&order_id].facts.status, "FILLED");
        assert_eq!(owner.orders[&order_id].facts.remaining, Decimal::ZERO);
        assert_eq!(owner.gate, Gate::Running);
    }
}

#[test]
fn historical_gap_crossing_settles_both_eligible_limit_orders_without_terminal_gate() {
    let (mut seed, mut products) = configured_ab();
    seed.cash = d("1000");
    seed.effective_at = 0;
    products[0].taker_fee = d("0.001");
    for mark in &mut products[0].marks {
        mark.valid_to = i64::MAX;
    }
    let a = products[0].product.clone();
    seed.orders = [
        ("GAP-ORDER-1", Side::Long, "105"),
        ("GAP-ORDER-2", Side::Long, "95"),
    ]
    .into_iter()
    .map(|(id, side, price)| SeedOrder {
        intent_id: format!("{id}-INTENT"),
        order_id: id.into(),
        client_id: format!("{id}-CLIENT"),
        strategy_id: "GAP-STRATEGY".into(),
        product: ProfileProduct::BtcEth(a.clone()),
        side,
        price: d(price),
        reduce_only: false,
        original: d("1"),
        filled: Decimal::ZERO,
        canceled: Decimal::ZERO,
        remaining: d("1"),
        status: "OPEN".into(),
    })
    .collect();
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let input = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 1_000,
            step_index: 2,
            a_ohlc: [d("100"), d("110"), d("90"), d("105")],
            a_volume: "8",
            a_mark: [d("100"); 4],
        },
        &[("GAP-ORDER-1", 1), ("GAP-ORDER-2", 2)],
    );

    let result = owner.historical_market_step(&input).unwrap();
    let fills = &result.products[0].fills;
    assert_eq!(
        fills
            .iter()
            .map(|fill| (fill.order_id.as_str(), fill.quantity, fill.price))
            .collect::<Vec<_>>(),
        [
            ("GAP-ORDER-1", d("1"), d("105")),
            ("GAP-ORDER-2", d("1"), d("95")),
        ]
    );
    assert_eq!(owner.execution_receipts.len(), 2);
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d("999.8"), d("0.2"), d("0"))
    );
    assert_eq!(owner.positions.btc().unwrap()[&a].contracts, d("2"));
    assert_eq!(owner.gate, Gate::Running);

    let settled = (
        owner.cash,
        owner.fees,
        owner.gross_realized,
        owner.state_version,
    );
    for fill in fills {
        let duplicate = execution::historical_candidate(
            &owner,
            &fill.order_id,
            fill.price,
            fill.quantity,
            result.effective_at,
            fill.execution_id,
        )
        .unwrap();
        let stamp = source::stamp(&fill.source_event_id, result.effective_at, 30);
        assert!(matches!(
            owner.execute_stamped(&duplicate, &stamp, |_| Ok(())),
            Ok(execution::commit::Reply::Duplicate(_))
        ));
    }
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.gross_realized,
            owner.state_version
        ),
        settled
    );
    assert_eq!(owner.execution_receipts.len(), 2);
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn terminal_historical_market_nodes_validate_but_do_not_mutate_the_owner() {
    for lifecycle in [
        risk_transition::Lifecycle::LiquidatedFlat,
        risk_transition::Lifecycle::LiquidatedInsolvent,
    ] {
        let (mut seed, products) = configured_ab();
        seed.cash = d("1000");
        seed.effective_at = 0;
        seed.orders.push(SeedOrder {
            intent_id: "TERMINAL-INTENT".into(),
            order_id: "TERMINAL-ORDER".into(),
            client_id: "TERMINAL-CLIENT".into(),
            strategy_id: "TERMINAL-STRATEGY".into(),
            product: ProfileProduct::BtcEth(products[0].product.clone()),
            side: Side::Long,
            price: d("105"),
            reduce_only: false,
            original: d("1"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "OPEN".into(),
        });
        let bar = NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 1_000,
            step_index: 2,
            a_ohlc: [d("100"), d("110"), d("90"), d("105")],
            a_volume: "8",
            a_mark: [d("100"); 4],
        };
        let mut control =
            ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
        let control_input = node_input(&control, bar, &[("TERMINAL-ORDER", 1)]);
        let control_result = control.historical_market_step(&control_input).unwrap();
        assert_eq!(control_result.products[0].fills.len(), 1);
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        owner.transition.lifecycle = lifecycle;
        let before = owner.inspect_state().unwrap();
        let before_version = owner.state_version;
        let mut input = node_input(&owner, bar, &[("TERMINAL-ORDER", 1)]);
        let result = owner.historical_market_step(&input).unwrap();
        assert_eq!(result.products.len(), 2);
        assert!(result
            .products
            .iter()
            .all(|product| product.fills.is_empty()));
        assert!(result
            .products
            .iter()
            .any(|product| product.capacity > Decimal::ZERO));
        assert_eq!(
            result
                .products
                .iter()
                .map(|product| (
                    &product.product_id,
                    product.capacity,
                    product.discarded_volume
                ))
                .collect::<Vec<_>>(),
            control_result
                .products
                .iter()
                .map(|product| (
                    &product.product_id,
                    product.capacity,
                    product.discarded_volume
                ))
                .collect::<Vec<_>>()
        );
        assert!(owner.execution_receipts.is_empty());
        assert_eq!(owner.state_version, before_version);
        assert_eq!(owner.inspect_state().unwrap(), before);
        assert_eq!(owner.historical_market_step(&input).unwrap(), result);

        input.bars[0].confirmed = false;
        assert_eq!(
            owner.historical_market_step(&input),
            Err("INVALID_HISTORICAL_INPUT")
        );
        assert_eq!(owner.state_version, before_version);
        assert_eq!(owner.inspect_state().unwrap(), before);
    }
}

#[test]
fn h10_owner_rechecks_second_candidate_after_immediate_risk_and_liquidation() {
    let (mut seed, mut products) = configured_ab();
    seed.cash = d("2001");
    let a = products[0].product.clone();
    products[0].taker_fee = d("0.001");
    products[0].liquidation_fee = d("0.00602");
    products[0].specs[0].contract_value = d("0.01");
    products[0].specs[0].tick = d("1");
    products[0].specs[0].lot = d("1");
    products[0].specs[0].minimum = d("1");
    products[0].marks[0].price = d("49900");
    products[0].tiers[0].tiers = vec![
        Tier {
            minimum: d("0"),
            maximum: d("1000"),
            mmr: d("0.004"),
            imr: d("0.001"),
            max_leverage: d("1000"),
        },
        Tier {
            minimum: d("1000.01"),
            maximum: d("5000"),
            mmr: d("0.005"),
            imr: d("0.001"),
            max_leverage: d("1000"),
        },
    ];
    products[1].marks[0].price = d("200");
    seed.positions = vec![SeedPosition {
        product: a.clone(),
        side: Side::Long,
        contracts: d("1000"),
        lots: vec![SeedLot {
            seed_execution_id: "H10-SEED-EXECUTION".into(),
            seed_sequence: 0,
            strategy_id: "H10-STRATEGY".into(),
            contracts: d("1000"),
            entry: d("49900"),
        }],
    }];
    seed.orders = ["H10-ORDER-1", "H10-ORDER-2"]
        .into_iter()
        .map(|id| SeedOrder {
            intent_id: format!("{id}-INTENT"),
            order_id: id.into(),
            client_id: format!("{id}-CLIENT"),
            strategy_id: "H10-STRATEGY".into(),
            product: ProfileProduct::BtcEth(a.clone()),
            side: Side::Long,
            price: d("49900"),
            reduce_only: false,
            original: d("1"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "OPEN".into(),
        })
        .collect();
    let mut owner = ScenarioAccount::from_configured(&seed, d("1000"), products.clone()).unwrap();
    let input = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 500,
            step_index: 0,
            a_ohlc: [d("49900"); 4],
            a_volume: "8",
            a_mark: [d("49900"); 4],
        },
        &[("H10-ORDER-1", 1), ("H10-ORDER-2", 2)],
    );
    let result = owner.historical_market_step(&input).unwrap();
    assert_eq!(result.products[0].fills.len(), 1);
    assert_eq!(result.products[0].fills[0].order_id, "H10-ORDER-1");
    assert_eq!(result.products[0].fills[0].quantity, d("1"));
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.reservation().unwrap().maintenance_margin
        ),
        (d("1997.49702"), d("3.50298"), d("1996"))
    );
    assert_eq!(owner.execution_receipts.len(), 1);
    assert_eq!(owner.orders["H10-ORDER-2"].facts.status, "CANCELED");
    assert_eq!((owner.cash, owner.fees), (d("1997.49702"), d("3.50298")));
    assert_eq!(owner.positions.btc().unwrap()[&a].contracts, d("1000"));
    let valuation = owner.reservation().unwrap();
    assert_eq!(
        (valuation.equity, valuation.maintenance_margin),
        (d("1997.49702"), d("1996"))
    );
    assert_eq!(
        owner.transition.lifecycle,
        risk_transition::Lifecycle::RiskStable
    );
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn consecutive_close_then_open_at_one_raw_boundary_uses_effective_phase_order() {
    let (seed, products) = configured_ab();
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let close = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 500,
            step_index: 3,
            a_ohlc: [d("100"); 4],
            a_volume: "0",
            a_mark: [d("105"); 4],
        },
        &[],
    );
    let close_result = owner.historical_market_step(&close).unwrap();
    assert_eq!(close_result.raw_time_ms, 60_500);
    assert_eq!(close_result.effective_at, 60_500 * 16 + 4);

    let open = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 60_500,
            step_index: 0,
            a_ohlc: [d("100"); 4],
            a_volume: "0",
            a_mark: [d("100"); 4],
        },
        &[],
    );
    let open_result = owner.historical_market_step(&open).unwrap();
    assert_eq!(open_result.raw_time_ms, close_result.raw_time_ms);
    assert_eq!(open_result.effective_at, 60_500 * 16 + 7);
    assert!(open_result.effective_at > close_result.effective_at);
    assert_eq!(
        owner
            .transition
            .accepted_stamp
            .as_ref()
            .unwrap()
            .effective_at,
        open_result.effective_at
    );
}

#[test]
fn same_node_product_loop_uses_unique_monotonic_execution_ordinals() {
    let (mut seed, products) = configured_ab();
    for (index, id) in ["A-ORDER", "B-ORDER"].into_iter().enumerate() {
        seed.orders.push(SeedOrder {
            intent_id: format!("{id}-INTENT"),
            order_id: id.into(),
            client_id: format!("{id}-CLIENT"),
            strategy_id: "P3-STRATEGY".into(),
            product: ProfileProduct::BtcEth(products[index].product.clone()),
            side: Side::Short,
            price: d("99"),
            reduce_only: false,
            original: d("1"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "OPEN".into(),
        });
    }
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let mut input = node_input(
        &owner,
        NodeBar {
            model: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            bar_open_ms: 500,
            step_index: 0,
            a_ohlc: [d("100"); 4],
            a_volume: "8",
            a_mark: [d("105"); 4],
        },
        &[("A-ORDER", 1), ("B-ORDER", 2)],
    );
    input.bars[1].volume_contracts = d("8");
    let result = owner.historical_market_step(&input).unwrap();
    assert_eq!(
        result
            .products
            .iter()
            .map(|product| product.product_id.as_str())
            .collect::<Vec<_>>(),
        ["A-USDT-SWAP", "B-USDT-SWAP"]
    );
    assert_eq!(result.products[0].fills[0].order_id, "A-ORDER");
    assert_eq!(result.products[1].fills[0].order_id, "B-ORDER");
    assert_eq!(owner.execution_receipts.len(), 2);
    assert_eq!(owner.fees, d("0.2"));
    let execution_stamps: Vec<_> = owner
        .transition
        .events
        .iter()
        .filter(|(event_id, _)| {
            owner.transition.event_kinds.get(*event_id) == Some(&source::Kind::Execution)
        })
        .map(|(_, (stamp, _))| (stamp.effective_at, stamp.scenario_ordinal))
        .collect();
    assert_eq!(
        execution_stamps,
        [(result.effective_at, 30), (result.effective_at, 31)]
    );
}
