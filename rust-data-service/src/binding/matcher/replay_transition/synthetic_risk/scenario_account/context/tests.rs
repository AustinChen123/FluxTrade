use super::super::configured_tests;
use super::super::execution::commit::tests::configured_input;
use super::super::tests::{d, fixture};
use super::*;
use risk_transition::{cancel, Lifecycle};

pub(in super::super) fn stamp(id: &str, at: i64, ordinal: i64) -> Stamp {
    Stamp {
        event_id: id.into(),
        effective_at: at,
        source_sequence: None,
        causal_parent_ids: vec![],
        ordering_contract_id: "S_order_v1".into(),
        scenario_ordinal: ordinal,
    }
}

pub(in super::super) fn activation(owner: &ScenarioAccount, at: i64, rows: Rows) -> Input {
    let (scenario, marks) = owner.btc_context().unwrap();
    Input {
        account_key: owner.key.clone(),
        stamp: stamp(
            "activate",
            at,
            if matches!(rows, Rows::Marks(_)) {
                20
            } else {
                10
            },
        ),
        expected_before: owner.valuation_context_id,
        expected_after: owner_context_id(
            scenario,
            marks,
            at,
            owner.seed_effective_at,
            &owner.config_id,
        )
        .unwrap(),
        rows,
    }
}

pub(in super::super) fn mark_rows(marks: &[Mark], at: i64) -> Rows {
    Rows::Marks(
        [Product::Btc, Product::Eth]
            .into_iter()
            .map(|p| {
                let m = marks
                    .iter()
                    .find(|m| m.product == p && m.valid_from <= at && at < m.valid_to)
                    .unwrap();
                (p, m.valid_from, m.valid_to, m.price)
            })
            .collect(),
    )
}

fn configured_mark_rows(owner: &ScenarioAccount, at: i64) -> Rows {
    let (scenario, marks) = owner.btc_context().unwrap();
    Rows::Marks(
        scenario
            .products()
            .into_iter()
            .map(|product| {
                let row = marks
                    .iter()
                    .find(|m| m.product == product && m.valid_from <= at && at < m.valid_to)
                    .unwrap();
                (product, row.valid_from, row.valid_to, row.price)
            })
            .collect(),
    )
}

fn configured_mark_fixture(count: usize, cash: &str) -> (CleanSeed, Vec<ConfiguredProduct>) {
    let (mut seed, mut products) = configured_tests::input(count);
    seed.cash = d(cash);
    for row in &mut products {
        let mut old = row.marks[0].clone();
        old.valid_to = 600;
        let next = Mark {
            price: d("101"),
            valid_from: 600,
            valid_to: 3000,
            ..old.clone()
        };
        row.marks = vec![old, next];
    }
    (seed, products)
}

fn configured_order(product: &Product, id: &str, price: &str) -> SeedOrder {
    let prototype = fixture().0.orders[0].clone();
    SeedOrder {
        order_id: id.into(),
        intent_id: format!("I-{id}"),
        client_id: format!("C-{id}"),
        product: ProfileProduct::BtcEth(product.clone()),
        side: Side::Long,
        price: d(price),
        reduce_only: false,
        original: d("0.5"),
        filled: Decimal::ZERO,
        canceled: Decimal::ZERO,
        remaining: d("0.5"),
        status: "OPEN".into(),
        ..prototype
    }
}

fn configured_position(product: &Product, id: &str, quantity: &str, sequence: u64) -> SeedPosition {
    SeedPosition {
        product: product.clone(),
        side: Side::Long,
        contracts: d(quantity),
        lots: vec![SeedLot {
            seed_execution_id: id.into(),
            seed_sequence: sequence,
            strategy_id: "strategy".into(),
            contracts: d(quantity),
            entry: d("100"),
        }],
    }
}

fn configured_activation(owner: &ScenarioAccount, at: i64, rows: Rows) -> Input {
    let mut input = activation(owner, at, rows);
    input.stamp.event_id = format!("configured-{}", input.stamp.event_id);
    input
}

fn configure_spec_change(row: &mut ConfiguredProduct, at: i64, tick: &str) {
    let mut old = row.specs[0].clone();
    old.interval.to = Some(at);
    row.specs = vec![
        old.clone(),
        Spec {
            version: "spec-v2".into(),
            interval: Interval { from: at, to: None },
            tick: d(tick),
            ..old
        },
    ];
}

fn configure_tier_change(row: &mut ConfiguredProduct, at: i64, mmr: &str) {
    let mut old = row.tiers[0].clone();
    old.interval.to = Some(at);
    let mut new = old.clone();
    new.version = "tier-v2".into();
    new.interval = Interval { from: at, to: None };
    new.tiers[0].mmr = d(mmr);
    row.tiers = vec![old, new];
}

fn assert_context_fault_has_no_draft(owner: &ScenarioAccount, original: &ScenarioAccount) {
    assert_context_fault_has_no_draft_with(owner, original, "UNSUPPORTED_CONTEXT_TRANSITION");
}

fn assert_context_fault_has_no_draft_with(
    owner: &ScenarioAccount,
    original: &ScenarioAccount,
    fault: Fault,
) {
    let mut expected = original.clone();
    expected.gate = Gate::Failed(fault);
    assert_eq!(owner, &expected);
    assert_eq!(owner.state_version, original.state_version);
    assert_eq!(owner.valuation_context_id, original.valuation_context_id);
    assert_eq!(owner.transition.context_at, original.transition.context_at);
    assert_eq!(owner.transition.contexts, original.transition.contexts);
    assert_eq!(owner.transition.events, original.transition.events);
    assert_eq!(
        owner.transition.event_kinds,
        original.transition.event_kinds
    );
    assert_eq!(owner.cash, original.cash);
    assert_eq!(owner.fees, original.fees);
    assert_eq!(owner.positions, original.positions);
    assert_eq!(owner.orders, original.orders);
}

#[test]
fn configured_mark_activation_accepts_one_and_three_products_in_config_order() {
    for count in [1, 3] {
        let (seed, products) = configured_mark_fixture(count, "1000");
        let expected_products = products
            .iter()
            .map(|row| row.product.clone())
            .collect::<Vec<_>>();
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let input = activation(&owner, 600, configured_mark_rows(&owner, 600));
        assert_ne!(input.expected_before, input.expected_after);
        let receipt = owner.activate_context(&input).unwrap();
        assert_eq!(owner.valuation_context_id, input.expected_after);
        assert_eq!(
            receipt
                .rows_after
                .iter()
                .map(|(_, _, mark)| mark.product.clone())
                .collect::<Vec<_>>(),
            expected_products
        );
        assert_eq!(owner.state_version, 1);
        assert_eq!(owner.transition.context_at, Some(600));
        assert_eq!(owner.transition.events.len(), 1);
    }
}

#[test]
fn configured_mark_vector_invalid_shapes_fail_without_publishing_context() {
    for variant in [
        "missing",
        "extra",
        "duplicate",
        "reordered",
        "stale",
        "wrong-mark",
    ] {
        let (seed, products) = configured_mark_fixture(3, "1000");
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let mut input = activation(&owner, 600, configured_mark_rows(&owner, 600));
        let Rows::Marks(rows) = &mut input.rows else {
            unreachable!()
        };
        match variant {
            "missing" => {
                rows.pop();
            }
            "extra" => rows.push(rows[0].clone()),
            "duplicate" => rows[2] = rows[1].clone(),
            "reordered" => rows.swap(0, 1),
            "stale" => {
                let (scenario, marks) = owner.btc_context().unwrap();
                let product = &scenario.products()[0];
                let old = marks
                    .iter()
                    .find(|m| m.product == *product && m.valid_from < 600)
                    .unwrap();
                rows[0] = (product.clone(), old.valid_from, old.valid_to, old.price);
            }
            _ => rows[1].3 += Decimal::ONE,
        }
        input.stamp.event_id = format!("invalid-{variant}");
        let original = owner.clone();
        assert_eq!(
            owner.activate_context(&input),
            Err("UNSUPPORTED_CONTEXT_TRANSITION"),
            "{variant}"
        );
        assert_context_fault_has_no_draft(&owner, &original);
    }
}

#[test]
fn configured_mark_activation_rejects_unchanged_identity_and_spec_tier_boundaries() {
    let (mut seed, products) = configured_tests::input(1);
    seed.cash = d("1000");
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
    let input = activation(&owner, 600, configured_mark_rows(&owner, 600));
    assert_eq!(input.expected_before, input.expected_after);
    let original = owner.clone();
    assert_eq!(
        owner.activate_context(&input),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    assert_context_fault_has_no_draft(&owner, &original);

    for boundary in ["spec", "tier"] {
        let (seed, mut products) = configured_mark_fixture(1, "1000");
        if boundary == "spec" {
            let mut old = products[0].specs[0].clone();
            old.interval.to = Some(600);
            let new = Spec {
                version: "spec-v2".into(),
                interval: Interval {
                    from: 600,
                    to: None,
                },
                ..old.clone()
            };
            products[0].specs = vec![old, new];
        } else {
            let mut old = products[0].tiers[0].clone();
            old.interval.to = Some(600);
            let new = TierVersion {
                version: "tier-v2".into(),
                interval: Interval {
                    from: 600,
                    to: None,
                },
                ..old.clone()
            };
            products[0].tiers = vec![old, new];
        }
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let input = activation(&owner, 600, configured_mark_rows(&owner, 600));
        let original = owner.clone();
        assert_eq!(
            owner.activate_context(&input),
            Err("UNSUPPORTED_CONTEXT_TRANSITION"),
            "{boundary}"
        );
        assert_context_fault_has_no_draft(&owner, &original);
    }
}

#[test]
fn configured_mark_shortfall_commits_source_then_waits_for_cancel_effect_before_liquidation() {
    let (mut seed, mut products) = configured_mark_fixture(3, "25.2");
    let product = products[0].product.clone();
    let shared_product = products[1].product.clone();
    products[0].taker_fee = d("0.001");
    products[0].liquidation_fee = d("0.001");
    products[1].tiers[0].tiers[0].mmr = d("0.00001");
    products[0].marks[1].price = d("1");
    for row in &mut products[1..] {
        row.marks[1].price = d("100");
    }
    let prototype = fixture().0.orders[0].clone();
    seed.positions.push(SeedPosition {
        product: shared_product.clone(),
        side: Side::Long,
        contracts: d("0.5"),
        lots: vec![SeedLot {
            seed_execution_id: "SHARED-ETH-POSITION".into(),
            seed_sequence: 0,
            strategy_id: "strategy".into(),
            contracts: d("0.5"),
            entry: d("100"),
        }],
    });
    seed.orders = vec![SeedOrder {
        order_id: "PARTIAL".into(),
        intent_id: "I-PARTIAL".into(),
        client_id: "C-PARTIAL".into(),
        product: ProfileProduct::BtcEth(product.clone()),
        side: Side::Long,
        price: d("100"),
        reduce_only: false,
        original: d("2"),
        filled: Decimal::ZERO,
        canceled: Decimal::ZERO,
        remaining: d("2"),
        status: "OPEN".into(),
        ..prototype
    }];
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let seed_snapshot = owner.reservation().unwrap();
    assert!(seed_snapshot.equity > seed_snapshot.maintenance_margin);
    assert!(seed_snapshot.available_margin >= Decimal::ZERO);
    assert!(owner.transition.episode.is_none());

    let candidate = configured_input(&owner, "PARTIAL", "MARK-PARTIAL-FILL", "1", "100", 501);
    assert!(matches!(
        owner.execute(&candidate),
        Ok(execution::commit::Reply::Committed { .. })
    ));
    assert_eq!((owner.cash, owner.fees), (d("25.1"), d("0.1")));
    assert_eq!(owner.positions.btc().unwrap()[&product].contracts, d("1"));
    assert_eq!(
        (
            owner.orders["PARTIAL"].facts.filled,
            owner.orders["PARTIAL"].facts.remaining
        ),
        (d("1"), d("1"))
    );
    let after_fill = owner.reservation().unwrap();
    assert_eq!(
        (
            after_fill.equity,
            after_fill.maintenance_margin,
            after_fill.available_margin
        ),
        (d("25.1"), d("0.5005"), d("0"))
    );
    let no_fee_equity = after_fill.equity + owner.fees;
    assert_eq!(no_fee_equity, d("25.2"));
    assert!(no_fee_equity > after_fill.maintenance_margin);
    assert!(after_fill.equity > after_fill.maintenance_margin);
    assert!(after_fill.available_margin >= Decimal::ZERO);
    assert!(owner.transition.episode.is_none());

    let (scenario, marks) = owner.btc_context().unwrap();
    let mut projection = owner.projection().unwrap();
    projection.effective_at = 600;
    let orders = owner
        .orders
        .values()
        .map(|order| &order.facts)
        .collect::<Vec<_>>();
    let old_marks = scenario
        .products()
        .into_iter()
        .map(|p| {
            let mark = marks
                .iter()
                .find(|m| m.product == p && m.valid_from < 600)
                .unwrap();
            Mark {
                valid_from: 600,
                valid_to: 3000,
                ..mark.clone()
            }
        })
        .collect::<Vec<_>>();
    let old_counterfactual =
        reservation::calculate(&projection, &orders, scenario, &old_marks).unwrap();
    let new_counterfactual = reservation::calculate(&projection, &orders, scenario, marks).unwrap();
    assert_eq!(
        (
            old_counterfactual.equity,
            old_counterfactual.maintenance_margin
        ),
        (d("25.1"), d("0.5005"))
    );
    let old_without_fee = old_counterfactual.equity + owner.fees;
    assert_eq!(old_without_fee, d("25.2"));
    assert!(old_without_fee > old_counterfactual.maintenance_margin);
    assert!(old_counterfactual.equity > old_counterfactual.maintenance_margin);
    assert_eq!(new_counterfactual.equity, d("-73.9"));
    assert_eq!(new_counterfactual.maintenance_margin, d("0.0055"));
    assert!(new_counterfactual.equity <= new_counterfactual.maintenance_margin);

    let mut mark_input = activation(&owner, 600, configured_mark_rows(&owner, 600));
    mark_input.stamp.event_id = "mark-drop".into();
    let mark_receipt = owner.activate_context(&mark_input).unwrap();
    assert_eq!(mark_receipt.after.equity, d("-73.9"));
    assert_eq!(mark_receipt.after.maintenance_margin, d("0.0055"));
    assert_eq!(owner.valuation_context_id, mark_input.expected_after);
    assert_eq!(owner.state_version, 3);
    assert_eq!(owner.transition.contexts.len(), 1);
    assert_eq!(owner.transition.events.len(), 2);
    assert!(matches!(
        owner.orders["PARTIAL"].cancel,
        cancel::State::Requested(_)
    ));
    assert_eq!(owner.liquidation_ids().count(), 0);
    assert_eq!(owner.orders["PARTIAL"].facts.filled, d("1"));
    assert_eq!(owner.positions.btc().unwrap()[&product].contracts, d("1"));
    assert_eq!(owner.fees, d("0.1"));

    let effect = cancel::EffectInput {
        stamp: stamp("MARK-CANCEL-EFFECT", 601, 50),
        effects: vec![(
            "mark-drop".into(),
            "PARTIAL".into(),
            cancel::Reason::MmrBreach,
        )],
    };
    owner.effect_cancel(&effect).unwrap();
    assert_eq!(owner.liquidation_ids().count(), 2);
    assert_eq!(owner.orders["PARTIAL"].facts.status, "CANCELED");
    assert_eq!(
        (
            owner.orders["PARTIAL"].facts.filled,
            owner.orders["PARTIAL"].facts.canceled
        ),
        (d("1"), d("1"))
    );
    assert!(owner.positions.is_empty());
    assert_eq!(owner.cash, d("-73.951"));
    assert_eq!(owner.fees, d("0.151"));
    assert_eq!(owner.gross_realized, d("-99"));
    assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
    assert_eq!(owner.execution_receipts.len(), 1);
    assert_eq!(owner.transition.contexts.len(), 1);
    let terminal = owner.clone();
    owner.effect_cancel(&effect).unwrap();
    assert_eq!(owner, terminal);
}

#[test]
fn exact_mmr_boundary_requests_reducing_order_and_stops_only_after_effect() {
    for (cash, equity, breach) in [
        ("2.99", "1.99", true),
        ("3", "2", true),
        ("3.01", "2.01", false),
    ] {
        let (mut seed, config, mut marks) = fixture();
        seed.cash = d(cash);
        seed.positions[0].contracts = d("1");
        seed.positions[0].lots.truncate(1);
        seed.positions[0].lots[0].entry = d("50100");
        seed.orders[0].price = d("50000");
        marks[0].price = d("50100");
        marks[0].valid_to = 600;
        marks.push(Mark {
            product: Product::Btc,
            price: d("50000"),
            valid_from: 600,
            valid_to: 3000,
        });
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let input = activation(&owner, 600, mark_rows(&marks, 600));
        let receipt = owner.activate_context(&input).unwrap();
        assert_eq!(receipt.after.equity, d(equity));
        assert_eq!(receipt.after.maintenance_margin, d("2"));
        assert_eq!(receipt.after.used_margin, d("50.5"));
        assert_eq!(owner.state_version, if breach { 2 } else { 1 });
        assert_eq!(
            owner.transition.lifecycle,
            if breach {
                Lifecycle::AwaitingCancelEffective
            } else {
                Lifecycle::RiskStable
            }
        );
        assert_eq!(owner.gate, Gate::Running);
        if breach {
            let cancel::State::Requested(request) = owner.orders["O1"].cancel.clone() else {
                panic!("required pending")
            };
            let effect = cancel::EffectInput {
                stamp: stamp("effect", 600, 50),
                effects: vec![(
                    request.detecting_event_id,
                    "O1".into(),
                    cancel::Reason::MmrBreach,
                )],
            };
            let result = owner.effect_cancel(&effect).unwrap();
            assert_eq!(owner.state_version, 4);
            assert_eq!(owner.orders["O1"].version, 2);
            assert_eq!(owner.reservation().unwrap().used_margin, d("0"));
            assert_eq!(owner.gate, Gate::Running);
            assert_eq!(
                owner.cash,
                d(if cash == "2.99" { "-1.02" } else { "-1.01" })
            );
            assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
            assert!(
                result.receipts[0]
                    .risk_after
                    .as_ref()
                    .unwrap()
                    .liquidation_required
            );
            assert_eq!(owner.commit_sequence, 2);
            assert_eq!(owner.fees, d("3.01"));
        } else {
            assert!(owner.transition.episode.is_none());
            assert!(owner.transition.batches.is_empty());
        }
        let before = owner.clone();
        assert_eq!(owner.activate_context(&input).unwrap(), receipt);
        assert_eq!(owner, before);
    }
}

#[test]
fn tier_activation_retains_source_fact_before_exact_liquidation() {
    let (mut seed, config, marks) = fixture();
    seed.effective_at = 999;
    seed.cash = d("2100");
    seed.orders.clear();
    seed.positions[0].contracts = d("1000");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].contracts = d("1000");
    seed.positions[0].lots[0].entry = d("50000");
    let mut marks = marks;
    marks[0].price = d("50000");
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let input = activation(
        &owner,
        1000,
        Rows::Tiers(
            [Product::Btc, Product::Eth]
                .into_iter()
                .map(|p| (p, "tier-v1".into(), "tier-v2".into(), 1000))
                .collect(),
        ),
    );
    let receipt = owner.activate_context(&input).unwrap();
    assert_eq!(receipt.after.maintenance_margin, d("2250"));
    assert_eq!(receipt.after.equity, d("2100"));
    assert_eq!(receipt.after.available_margin, d("-47900"));
    assert_eq!(owner.state_version, 2);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.fees, d("3010"));
    assert_eq!(owner.cash, d("-910"));
    assert_eq!(receipt.after_version, 1);
    assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
    assert_eq!(owner.seed_effective_at, 999);
}

#[test]
fn tick_migration_cancels_only_misaligned_and_reuses_pending_phase_one() {
    for (product, entry, mark, before_used, after_used) in [
        (Product::Btc, "50000.1", "50000", "50.5000005", "50.25"),
        (Product::Eth, "2000.01", "2000", "20.2000005", "20.1"),
    ] {
        for pending in [false, true] {
            let (mut seed, config, mut marks) = fixture();
            seed.effective_at = 1999;
            seed.positions[0].contracts = d("1");
            seed.positions[0].lots.truncate(1);
            seed.positions[0].product = product.clone();
            seed.positions[0].lots[0].entry = d(entry);
            marks
                .iter_mut()
                .find(|m| m.product == product)
                .unwrap()
                .price = d(mark);
            let order = &mut seed.orders[0];
            order.product = ProfileProduct::BtcEth(product.clone());
            order.price = d(entry);
            order.original = d("0.5");
            order.filled = Decimal::ZERO;
            order.remaining = d("0.5");
            order.status = "OPEN".into();
            let mut aligned = order.clone();
            aligned.intent_id = "I2".into();
            aligned.order_id = "O2".into();
            aligned.client_id = "C2".into();
            aligned.price = d(mark);
            seed.orders.push(aligned);
            let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
            let original = owner.positions.clone();
            if pending {
                owner
                    .request_cancel(&cancel::RequestInput {
                        stamp: stamp("request", 1999, 40),
                        targets: vec![("O1".into(), cancel::Reason::ExplicitScenario)],
                    })
                    .unwrap();
            }
            let input = activation(
                &owner,
                2000,
                Rows::Specs(
                    [Product::Btc, Product::Eth]
                        .into_iter()
                        .map(|p| (p, "spec-v1".into(), "spec-v2".into(), 2000))
                        .collect(),
                ),
            );
            let receipt = owner.activate_context(&input).unwrap();
            assert_eq!(receipt.after.equity, d("9999.999"));
            assert_eq!(receipt.before.used_margin, d(before_used));
            assert_eq!(receipt.after.used_margin, d(after_used));
            assert_eq!(owner.positions, original);
            assert_eq!(owner.state_version, if pending { 2 } else { 1 });
            assert_eq!(owner.orders["O1"].version, if pending { 2 } else { 1 });
            assert_eq!(owner.orders["O2"].version, 0);
            if pending {
                assert_eq!(
                    owner
                        .cancel_facts
                        .actions
                        .values()
                        .filter(|a| a.phase == 2 && a.outcome.is_none())
                        .count(),
                    1
                );
            }
        }
    }
}

#[test]
fn cross_product_pending_set_is_stable_after_first_effect_restores_margin() {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("1000");
    seed.positions.clear();
    let base = &mut seed.orders[0];
    base.side = Side::Long;
    base.reduce_only = false;
    base.price = d("50000");
    base.original = d("10");
    base.remaining = d("10");
    base.filled = Decimal::ZERO;
    base.status = "OPEN".into();
    let mut eth = base.clone();
    eth.order_id = "ETH-order".into();
    eth.intent_id = "ETH-intent".into();
    eth.client_id = "ETH-client".into();
    eth.product = ProfileProduct::BtcEth(Product::Eth);
    eth.price = d("2000");
    eth.original = d("20");
    eth.remaining = d("20");
    seed.orders.push(eth);
    marks[0].price = d("50000");
    marks[1].price = d("2000");
    for m in &mut marks {
        m.valid_to = 600;
    }
    marks.extend(
        [(Product::Btc, "49000"), (Product::Eth, "1900")]
            .into_iter()
            .map(|(product, price)| Mark {
                product,
                price: d(price),
                valid_from: 600,
                valid_to: 3000,
            }),
    );
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(owner.reservation().unwrap().used_margin, d("909"));
    let input = activation(&owner, 600, mark_rows(&marks, 600));
    for stage in [
        Stage::RiskActionDrafted(0),
        Stage::RiskActionDrafted(1),
        Stage::RiskBeforeSwap,
    ] {
        for panic in [false, true] {
            let mut interrupted = owner.clone();
            assert!(interrupted
                .activate_context_checked(&input, |point| {
                    if point == stage {
                        if panic {
                            panic!("risk draft interruption")
                        };
                        return Err("INJECTED_RISK");
                    }
                    Ok(())
                })
                .is_err());
            assert_eq!(interrupted.state_version, 1);
            assert_eq!(interrupted.transition.contexts.len(), 1);
            assert!(interrupted.transition.batches.is_empty());
            assert!(interrupted
                .orders
                .values()
                .all(|o| o.version == 0 && matches!(o.cancel, cancel::State::None)));
            assert_eq!(interrupted.reservation().unwrap().used_margin, d("1179"));
            assert_eq!(interrupted.cash, d("1000"));
            assert!(interrupted.cancel_facts.actions.is_empty());
        }
    }
    let receipt = owner.activate_context(&input).unwrap();
    assert_eq!(receipt.after.used_margin, d("1179"));
    assert_eq!(owner.state_version, 2);
    let batch = owner.transition.batches.values().next().unwrap();
    assert_eq!(
        batch
            .batch
            .receipts
            .iter()
            .map(|r| r.target_order_id.as_str())
            .collect::<Vec<_>>(),
        vec!["O1", "ETH-order"]
    );
    for (index, (id, used)) in [("O1", "584"), ("ETH-order", "0")].into_iter().enumerate() {
        owner
            .effect_cancel(&cancel::EffectInput {
                stamp: stamp(&format!("effect-{index}"), 600 + index as i64, 50),
                effects: vec![("activate".into(), id.into(), cancel::Reason::RiskShortfall)],
            })
            .unwrap();
        assert_eq!(owner.state_version, 3 + index as u64);
        assert_eq!(owner.reservation().unwrap().used_margin, d(used));
        assert_eq!(
            owner.transition.lifecycle,
            if index == 0 {
                Lifecycle::AwaitingCancelEffective
            } else {
                Lifecycle::RiskStable
            }
        );
    }
    assert_eq!(owner.cash, d("1000"));
    assert_eq!(owner.commit_sequence, 0);
}

#[test]
fn context_and_group_invalid_shape_and_fault_retention_are_atomic() {
    use super::super::group::{Group, Input as GroupInput, Member};
    let (mut seed, config, marks) = fixture();
    seed.effective_at = 999;
    seed.orders.clear();
    let original = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let input = activation(
        &original,
        1000,
        Rows::Tiers(
            [Product::Btc, Product::Eth]
                .into_iter()
                .map(|p| (p, "tier-v1".into(), "tier-v2".into(), 1000))
                .collect(),
        ),
    );
    for stage in [Stage::BeforeSwap, Stage::AfterSwap] {
        for panic in [false, true] {
            let mut owner = original.clone();
            let result = owner.activate_context_checked(&input, |point| {
                if point == stage {
                    if panic {
                        panic!("injected")
                    };
                    Err("INJECTED")
                } else {
                    Ok(())
                }
            });
            assert_eq!(
                result,
                Err(if panic { "CONTEXT_PANIC" } else { "INJECTED" })
            );
            assert_eq!(
                owner.state_version,
                if stage == Stage::BeforeSwap { 0 } else { 1 }
            );
            assert_eq!(
                owner.transition.contexts.len(),
                if stage == Stage::BeforeSwap { 0 } else { 1 }
            );
            assert_eq!(owner.cash, original.cash);
            assert_eq!(owner.positions, original.positions);
        }
    }
    let group = Group {
        group_id: "tier-group".into(),
        account_key: original.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: 1000,
        declared_member_count: 1,
        members: vec![Member {
            stamp: input.stamp.clone(),
            input: GroupInput::Context(input.clone()),
        }],
    };
    for field in 0..8 {
        let mut invalid = group.clone();
        match field {
            0 => invalid.declared_member_count = 2,
            1 => invalid.account_key.account = "other".into(),
            2 => invalid.group_effective_at = 1001,
            3 => invalid.group_id = "reverse-execution-cancel-effective-v1".into(),
            4 => invalid.ordering_contract_id = "unknown".into(),
            5 => invalid.members[0].stamp.scenario_ordinal = 20,
            6 => invalid.members[0].stamp.causal_parent_ids = vec!["unknown".into()],
            _ => invalid.members.push(invalid.members[0].clone()),
        }
        let mut owner = original.clone();
        assert!(owner.apply_group(&invalid).is_err());
        let mut expected = original.clone();
        expected.gate = owner.gate.clone();
        assert_eq!(owner, expected, "field {field}");
    }
    let mut owner = original.clone();
    let result = owner.apply_group(&group).unwrap();
    assert_eq!(
        result.committed,
        vec![group::Reference::Source("activate".into())]
    );
    let before = owner.clone();
    assert_eq!(owner.apply_group(&group).unwrap(), result);
    assert_eq!(owner, before);
    let mut stale = group.clone();
    stale.group_id = "stale".into();
    stale.group_effective_at = 999;
    stale.members[0].stamp.event_id = "stale-event".into();
    stale.members[0].stamp.effective_at = 999;
    let stale_stamp = stale.members[0].stamp.clone();
    if let GroupInput::Context(input) = &mut stale.members[0].input {
        input.stamp = stale_stamp;
    }
    assert_eq!(owner.apply_group(&stale), Err("STALE_EVENT"));
    assert_eq!(owner.state_version, before.state_version);

    let mut boundary_marks = marks.clone();
    boundary_marks[0].valid_to = 1000;
    boundary_marks.push(Mark {
        product: Product::Btc,
        price: marks[0].price,
        valid_from: 1000,
        valid_to: 3000,
    });
    let mut expired = ScenarioAccount::from_seed(&seed, &config, &boundary_marks).unwrap();
    let boundary = activation(&expired, 1000, input.rows.clone());
    let later_mark = activation(&expired, 1000, mark_rows(&boundary_marks, 1000));
    let mut invalid = group.clone();
    invalid.members = vec![
        Member {
            stamp: boundary.stamp.clone(),
            input: GroupInput::Context(boundary),
        },
        Member {
            stamp: Stamp {
                event_id: "later-mark".into(),
                ..later_mark.stamp.clone()
            },
            input: GroupInput::Context(Input {
                stamp: Stamp {
                    event_id: "later-mark".into(),
                    ..later_mark.stamp.clone()
                },
                ..later_mark
            }),
        },
    ];
    invalid.declared_member_count = 2;
    assert_eq!(
        expired.apply_group(&invalid),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    assert_eq!(expired.state_version, 0);
    assert!(expired.transition.admissions.is_empty());
}

#[test]
fn configured_spec_subset_migrates_only_changed_product_and_reuses_pending_request() {
    let (mut seed, mut products) = configured_tests::input(3);
    seed.cash = d("1000");
    seed.orders.clear();
    let first = products[0].product.clone();
    let middle = products[1].product.clone();
    let last = products[2].product.clone();
    configure_spec_change(&mut products[1], 600, "2");
    seed.orders = vec![
        configured_order(&first, "FIRST-ALIGNED", "101"),
        configured_order(&middle, "MID-MISALIGNED", "101"),
        configured_order(&middle, "MID-ALIGNED", "100"),
        configured_order(&middle, "MID-PENDING", "103"),
        configured_order(&last, "LAST-ALIGNED", "101"),
    ];
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let old_tick = owner
        .btc_context()
        .unwrap()
        .0
        .resolve(&middle, 599)
        .unwrap()
        .0
        .tick;
    assert_eq!(old_tick, d("1"));
    assert_eq!(owner.reservation().unwrap().orders.len(), 5);
    let reservations_before = owner.reservation().unwrap().orders;

    owner
        .request_cancel(&cancel::RequestInput {
            stamp: stamp("pending-migration-request", 550, 40),
            targets: vec![("MID-PENDING".into(), cancel::Reason::ExplicitScenario)],
        })
        .unwrap();
    let cancel::State::Requested(pending_request) = owner.orders["MID-PENDING"].cancel.clone()
    else {
        panic!("pending cancellation request must exist before spec activation")
    };
    assert_eq!(
        pending_request.detecting_event_id,
        "pending-migration-request"
    );
    assert_eq!(pending_request.reason, cancel::Reason::ExplicitScenario);
    let input = configured_activation(
        &owner,
        600,
        Rows::Specs(vec![(
            middle.clone(),
            "scale-spec-v1".into(),
            "spec-v2".into(),
            600,
        )]),
    );
    let receipt = owner.activate_context(&input).unwrap();
    assert_eq!(
        owner.transition.contexts["configured-activate"].rows_before[1]
            .0
            .tick,
        old_tick
    );
    let (active, _) = owner.btc_context().unwrap();
    assert_eq!(active.resolve(&middle, 600).unwrap().0.tick, d("2"));
    assert_eq!(receipt.rows_before[1].0.version, "scale-spec-v1");
    assert_eq!(receipt.rows_after[1].0.version, "spec-v2");
    assert_eq!(receipt.input.rows, input.rows);
    assert_eq!(receipt.migration_effects.len(), 2);
    assert_eq!(receipt.after_version, receipt.before_version + 1);
    assert_eq!(owner.transition.context_at, Some(600));
    assert_eq!(owner.valuation_context_id, input.expected_after);
    assert_eq!(owner.transition.contexts["configured-activate"], receipt);

    for id in ["MID-MISALIGNED", "MID-PENDING"] {
        assert_eq!(owner.orders[id].facts.status, "CANCELED");
        assert_eq!(owner.orders[id].facts.remaining, Decimal::ZERO);
    }
    let cancel::State::EffectiveCanceled(effect) = &owner.orders["MID-PENDING"].cancel else {
        panic!("pending migration must retain its original effective cancellation")
    };
    assert_eq!(effect.action_id, pending_request.effect_action_id);
    assert_eq!(
        effect.request.detecting_event_id,
        "pending-migration-request"
    );
    assert_eq!(effect.request.reason, cancel::Reason::ExplicitScenario);
    let cancel::State::MigrationEffective(direct_migration) =
        &owner.orders["MID-MISALIGNED"].cancel
    else {
        panic!("unaligned order must record the context migration effect")
    };
    assert_eq!(
        receipt.migration_effects,
        vec![*direct_migration, pending_request.effect_action_id]
    );
    for id in ["FIRST-ALIGNED", "MID-ALIGNED", "LAST-ALIGNED"] {
        assert_eq!(owner.orders[id].facts.status, "OPEN");
        assert_eq!(owner.orders[id].facts.remaining, d("0.5"));
        assert_eq!(owner.orders[id].version, 0);
    }
    let effect_actions = owner
        .cancel_facts
        .actions
        .iter()
        .filter(|(id, _)| receipt.migration_effects.contains(id))
        .map(|(_, action)| {
            (
                action.target_order_id.as_str(),
                action.detecting_event_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        effect_actions,
        BTreeSet::from([("MID-PENDING", "pending-migration-request")])
    );
    let reservations_after = owner.reservation().unwrap().orders;
    let unchanged = ["FIRST-ALIGNED", "MID-ALIGNED", "LAST-ALIGNED"];
    assert_eq!(
        reservations_before
            .iter()
            .filter(|row| unchanged.contains(&row.order_id.as_str()))
            .collect::<Vec<_>>(),
        reservations_after.iter().collect::<Vec<_>>()
    );
    assert_eq!(reservations_after.len(), 3);
    let before_duplicate = owner.clone();
    assert_eq!(owner.activate_context(&input).unwrap(), receipt);
    assert_eq!(owner, before_duplicate);
}

#[test]
fn configured_spec_activation_accepts_ordered_changed_subset_of_multiple_products() {
    let (mut seed, mut products) = configured_tests::input(3);
    seed.cash = d("1000");
    configure_spec_change(&mut products[0], 600, "2");
    configure_spec_change(&mut products[2], 600, "5");
    let p0 = products[0].product.clone();
    let p2 = products[2].product.clone();
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let input = configured_activation(
        &owner,
        600,
        Rows::Specs(vec![
            (p0.clone(), "scale-spec-v1".into(), "spec-v2".into(), 600),
            (p2.clone(), "scale-spec-v1".into(), "spec-v2".into(), 600),
        ]),
    );
    let receipt = owner.activate_context(&input).unwrap();
    assert_eq!(receipt.rows_after[0].0.tick, d("2"));
    assert_eq!(receipt.rows_after[1].0.tick, d("1"));
    assert_eq!(receipt.rows_after[2].0.tick, d("5"));
    assert_eq!(owner.transition.contexts["configured-activate"], receipt);

    let (mut seed, mut products) = configured_tests::input(3);
    seed.cash = d("1000");
    configure_spec_change(&mut products[0], 600, "2");
    configure_spec_change(&mut products[2], 600, "5");
    let p0 = products[0].product.clone();
    let p2 = products[2].product.clone();
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let input = configured_activation(
        &owner,
        600,
        Rows::Specs(vec![
            (p2, "scale-spec-v1".into(), "spec-v2".into(), 600),
            (p0, "scale-spec-v1".into(), "spec-v2".into(), 600),
        ]),
    );
    let original = owner.clone();
    assert_eq!(
        owner.activate_context(&input),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    assert_context_fault_has_no_draft(&owner, &original);
}

#[test]
fn configured_tier_subset_revalues_shared_risk_then_waits_for_cancel_effect() {
    let (mut seed, mut products) = configured_tests::input(3);
    seed.cash = d("16");
    seed.orders.clear();
    let first = products[0].product.clone();
    let middle = products[1].product.clone();
    let last = products[2].product.clone();
    configure_tier_change(&mut products[1], 600, "0.4");
    seed.positions = vec![
        configured_position(&first, "POSITION-FIRST", "0.5", 0),
        configured_position(&middle, "POSITION-MIDDLE", "0.5", 1),
    ];
    seed.orders = vec![configured_order(&last, "LAST-OPEN", "100")];
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let before = owner.reservation().unwrap();
    assert_eq!(before.equity, d("16"));
    assert_eq!(before.maintenance_margin, d("0.5"));
    assert!(before.available_margin >= Decimal::ZERO);

    let input = configured_activation(
        &owner,
        600,
        Rows::Tiers(vec![(
            middle.clone(),
            "scale-tier-v1".into(),
            "tier-v2".into(),
            600,
        )]),
    );
    let receipt = owner.activate_context(&input).unwrap();
    assert_eq!(receipt.after.equity, d("16"));
    assert_eq!(receipt.after.maintenance_margin, d("20.25"));
    assert_eq!(receipt.after.used_margin, before.used_margin);
    assert_eq!(receipt.rows_before[1].1.version, "scale-tier-v1");
    assert_eq!(receipt.rows_after[1].1.version, "tier-v2");
    assert!(matches!(
        owner.orders["LAST-OPEN"].cancel,
        cancel::State::Requested(_)
    ));
    assert_eq!(owner.liquidation_ids().count(), 0);
    assert_eq!(owner.positions.btc().unwrap()[&middle].contracts, d("0.5"));
    assert_eq!(owner.positions.btc().unwrap()[&first].contracts, d("0.5"));
    assert_eq!(owner.transition.context_at, Some(600));

    let cancel::State::Requested(request) = owner.orders["LAST-OPEN"].cancel.clone() else {
        panic!("tier revaluation must wait for its cancel effect")
    };
    owner
        .effect_cancel(&cancel::EffectInput {
            stamp: stamp("tier-cancel-effect", 601, 50),
            effects: vec![(
                request.detecting_event_id,
                "LAST-OPEN".into(),
                request.reason,
            )],
        })
        .unwrap();
    assert_eq!(owner.orders["LAST-OPEN"].facts.status, "CANCELED");
    assert_eq!(owner.liquidation_ids().count(), 1);
    assert!(!owner.positions.btc().unwrap().contains_key(&middle));
    assert_eq!(owner.positions.btc().unwrap()[&first].contracts, d("0.5"));
    assert_eq!(owner.transition.lifecycle, Lifecycle::RiskStable);
    assert_eq!(owner.commit_sequence, 3);
    assert_eq!(owner.fees, d("0.05"));
}

#[test]
fn configured_activation_changed_subset_and_boundary_faults_are_atomic() {
    for case in [
        "missing",
        "extra",
        "duplicate",
        "reordered",
        "unchanged",
        "wrong-version",
        "wrong-boundary",
        "mark-axis",
        "tier-axis",
        "missing-coverage",
        "unsupported-scaling",
    ] {
        let (mut seed, mut products) = configured_tests::input(3);
        seed.cash = d("1000");
        configure_spec_change(&mut products[1], 600, "2");
        if case == "unsupported-scaling" {
            products[1].specs[1].contract_value = d("2");
        }
        if case == "tier-axis" {
            configure_tier_change(&mut products[0], 600, "0.01");
        }
        if case == "mark-axis" || case == "missing-coverage" {
            let mut old = products[0].marks[0].clone();
            old.valid_to = if case == "missing-coverage" { 550 } else { 600 };
            let new = Mark {
                price: d("101"),
                valid_from: if case == "missing-coverage" { 650 } else { 600 },
                valid_to: 3000,
                ..old.clone()
            };
            products[0].marks = vec![old, new];
        }
        let p0 = products[0].product.clone();
        let p1 = products[1].product.clone();
        let p2 = products[2].product.clone();
        if case == "reordered" {
            configure_spec_change(&mut products[0], 600, "4");
            configure_spec_change(&mut products[2], 600, "3");
        }
        let tuple = |product: Product, old: &str, new: &str, boundary| {
            (product, old.into(), new.into(), boundary)
        };
        let middle_change = tuple(
            p1.clone(),
            "scale-spec-v1",
            "spec-v2",
            if case == "wrong-boundary" { 599 } else { 600 },
        );
        let rows = match case {
            "missing" | "missing-coverage" => vec![],
            "extra" => vec![
                middle_change,
                tuple(p0.clone(), "scale-spec-v1", "spec-v2", 600),
            ],
            "duplicate" => vec![middle_change.clone(), middle_change],
            "reordered" => vec![
                tuple(p2.clone(), "scale-spec-v1", "spec-v2", 600),
                tuple(p0.clone(), "scale-spec-v1", "spec-v2", 600),
            ],
            "unchanged" => vec![tuple(p0.clone(), "scale-spec-v1", "spec-v2", 600)],
            "wrong-version" => vec![tuple(p1.clone(), "wrong-version", "spec-v2", 600)],
            _ => vec![middle_change],
        };
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let input = if case == "missing-coverage" {
            Input {
                account_key: owner.key.clone(),
                stamp: stamp("configured-missing-coverage", 600, 10),
                expected_before: owner.valuation_context_id,
                expected_after: [0; 32],
                rows: Rows::Specs(rows),
            }
        } else {
            configured_activation(&owner, 600, Rows::Specs(rows))
        };
        let original = owner.clone();
        let expected_fault = if case == "unsupported-scaling" {
            "UNSUPPORTED_SPEC_MIGRATION"
        } else {
            "UNSUPPORTED_CONTEXT_TRANSITION"
        };
        assert_eq!(
            owner.activate_context(&input),
            Err(expected_fault),
            "{case}"
        );
        assert_context_fault_has_no_draft_with(&owner, &original, expected_fault);
    }
}

#[test]
fn configured_tier_activation_rejects_concurrent_spec_or_mark_change() {
    for other_axis in ["spec", "mark"] {
        let (mut seed, mut products) = configured_tests::input(3);
        seed.cash = d("1000");
        configure_tier_change(&mut products[1], 600, "0.01");
        if other_axis == "spec" {
            configure_spec_change(&mut products[0], 600, "2");
        } else {
            let mut old = products[0].marks[0].clone();
            old.valid_to = 600;
            let new = Mark {
                price: d("101"),
                valid_from: 600,
                valid_to: 3000,
                ..old.clone()
            };
            products[0].marks = vec![old, new];
        }
        let middle = products[1].product.clone();
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let input = configured_activation(
            &owner,
            600,
            Rows::Tiers(vec![(
                middle,
                "scale-tier-v1".into(),
                "tier-v2".into(),
                600,
            )]),
        );
        let original = owner.clone();
        assert_eq!(
            owner.activate_context(&input),
            Err("UNSUPPORTED_CONTEXT_TRANSITION"),
            "{other_axis}"
        );
        assert_context_fault_has_no_draft(&owner, &original);
    }
}

#[test]
fn configured_tier_position_invalid_at_incoming_boundary_fails_before_swap() {
    let (mut seed, mut products) = configured_tests::input(1);
    seed.cash = d("1000");
    let product = products[0].product.clone();
    seed.positions = vec![configured_position(&product, "POSITION", "0.5", 0)];
    configure_tier_change(&mut products[0], 600, "0.01");
    products[0].tiers[1].tiers[0].maximum = d("0.25");
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let input = configured_activation(
        &owner,
        600,
        Rows::Tiers(vec![(
            product,
            "scale-tier-v1".into(),
            "tier-v2".into(),
            600,
        )]),
    );
    let original = owner.clone();
    assert_eq!(owner.activate_context(&input), Err("INVALID_FIFO_POSITION"));
    assert_context_fault_has_no_draft_with(&owner, &original, "INVALID_FIFO_POSITION");
}
