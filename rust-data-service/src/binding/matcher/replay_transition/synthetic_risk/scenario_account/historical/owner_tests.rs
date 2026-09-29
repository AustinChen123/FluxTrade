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
    let mut market = input.clone();
    market.working_orders[0].kind = historical::OrderKind::Market;
    market.working_orders[0].limit_price = None;
    assert_eq!(
        owner.historical_market_step(&market),
        Err("HISTORICAL_MARKET_OWNER_UNSUPPORTED")
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
