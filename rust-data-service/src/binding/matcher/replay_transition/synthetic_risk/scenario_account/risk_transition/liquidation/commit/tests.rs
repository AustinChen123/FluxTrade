use super::super::super::super::tests::{d, fixture};
use super::super::tests::{anchor, source_draft, two_product_seed};
use super::*;
use context::tests::activation;

fn breached(cash: &str) -> ScenarioAccount {
    source_draft(
        &anchor(Side::Long, "1001", "50000", cash, 500, "49900"),
        501,
    )
}

#[test]
fn m01_m02_atomic_steps_reconcile_and_repeat_exactly() {
    let mut runs = Vec::new();
    for _ in 0..2 {
        let mut owner = breached("3000");
        let source = owner.clone();
        let prepared = prepare_step(&owner, &[]).unwrap();
        let first = owner.commit_liquidation(&prepared, |_| Ok(())).unwrap();
        assert_eq!(
            (owner.cash, owner.fees, owner.gross_realized),
            (d("2995.99602"), d("3.00398"), d("-1"))
        );
        assert_eq!((owner.state_version, owner.commit_sequence), (2, 3));
        assert_eq!(
            owner.positions.btc().unwrap()[&Product::Btc].contracts,
            d("1000")
        );
        assert_eq!(
            (
                first.valuation_after.equity,
                first.valuation_after.maintenance_margin
            ),
            (d("1995.99602"), d("1996"))
        );
        assert_eq!(first.valuation_after.products[0].unrealized_pnl, d("-1000"));
        assert_eq!(first.post_step_decision, StepDecision::ContinueLiquidation);
        let rest = owner.liquidate_for_test(|_| Ok(())).unwrap();
        assert_eq!(rest.len(), 1);
        let last = &rest[0];
        assert_eq!(
            (last.step_index, last.contracts, last.base_quantity),
            (2, d("1000"), d("10"))
        );
        assert_eq!(
            (last.fee, last.gross_realized_delta),
            (d("3003.98"), d("-1000"))
        );
        assert_eq!(
            (owner.cash, owner.fees, owner.gross_realized),
            (d("-1007.98398"), d("3006.98398"), d("-1001"))
        );
        assert_eq!((owner.state_version, owner.commit_sequence), (3, 4));
        assert!(owner.positions.is_empty());
        assert_eq!(owner.reservation().unwrap().equity, d("-1007.98398"));
        assert_eq!(owner.reservation().unwrap().used_margin, Decimal::ZERO);
        assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
        assert!(owner.transition.episode.is_none());
        assert_eq!(
            owner.transition.completed_episodes,
            vec![source.transition.episode.clone().unwrap()]
        );
        assert_eq!(first.risk_action_episode_id, last.risk_action_episode_id);
        assert_eq!(first.trigger_event_id, last.trigger_event_id);
        assert_eq!(owner.transition.liquidations, vec![first, last.clone()]);
        assert_eq!(owner.orders, source.orders);
        assert_eq!(owner.transition.contexts, source.transition.contexts);
        assert_eq!(
            owner.transition.accepted_stamp,
            source.transition.accepted_stamp
        );
        assert_eq!(owner.transition.events, source.transition.events);
        assert_eq!(owner.gate, Gate::Running);
        let terminal = owner.clone();
        assert_eq!(
            owner.commit_liquidation(&prepared, |_| panic!("duplicate hook")),
            Ok(terminal.transition.liquidations[0].clone())
        );
        assert_eq!(owner, terminal);
        assert_eq!(owner.liquidate_for_test(|_| Ok(())), Err("RUN_TERMINAL"));
        assert_eq!(owner, terminal);
        runs.push(owner);
    }
    assert_eq!(runs[0], runs[1]);
    let mut uninterrupted = breached("3000");
    assert_eq!(
        uninterrupted.liquidate_for_test(|_| Ok(())).unwrap().len(),
        2
    );
    assert_eq!(uninterrupted, runs[0]);
}

#[test]
fn tier_activation_and_partial_recovery_use_exact_owner_commits() {
    let (mut seed, config, mut marks) = fixture();
    seed.effective_at = 999;
    seed.cash = d("2100");
    seed.orders.clear();
    seed.positions[0].contracts = d("1000");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].contracts = d("1000");
    marks[0].price = d("50000");
    let initial = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let input = activation(
        &initial,
        1000,
        context::Rows::Tiers(
            [Product::Btc, Product::Eth]
                .into_iter()
                .map(|p| (p, "tier-v1".into(), "tier-v2".into(), 1000))
                .collect(),
        ),
    );
    let (source, draft) = initial.prepare_context(&input).unwrap();
    let mut owner = draft.unwrap();
    assert_eq!(source.after.maintenance_margin, d("2250"));
    let receipts = owner.liquidate_for_test(|_| Ok(())).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d("-910"), d("3010"), d("0"))
    );
    assert_eq!((owner.state_version, owner.commit_sequence), (2, 2));
    assert_eq!(owner.transition.contexts[&input.stamp.event_id], source);
    assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);

    let mut owner = breached("3001");
    let receipts = owner.liquidate_for_test(|_| Ok(())).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].post_step_decision, StepDecision::RiskStable);
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d("2996.99602"), d("3.00398"), d("-1"))
    );
    assert_eq!(
        (
            receipts[0].valuation_after.equity,
            receipts[0].valuation_after.maintenance_margin
        ),
        (d("1996.99602"), d("1996"))
    );
    assert_eq!(owner.transition.lifecycle, Lifecycle::RiskStable);
    assert!(owner.transition.episode.is_none());
    assert_eq!(
        owner.positions.btc().unwrap()[&Product::Btc].contracts,
        d("1000")
    );
}

#[test]
fn every_pre_swap_fault_and_panic_retains_only_committed_prefix() {
    for step in [1, 2] {
        for stage in [Stage::Prepared(step), Stage::BeforeSwap(step)] {
            for panic in [false, true] {
                let mut owner = breached("3000");
                let mut expected = owner.clone();
                if step == 2 {
                    let prepared = prepare_step(&expected, &[]).unwrap();
                    expected.commit_liquidation(&prepared, |_| Ok(())).unwrap();
                }
                let fault = if panic {
                    "LIQUIDATION_PANIC"
                } else {
                    "INJECTED_LIQUIDATION"
                };
                let result = owner.liquidate_for_test(|point| {
                    if point == stage {
                        if panic {
                            panic!("liquidation before swap");
                        }
                        return Err("INJECTED_LIQUIDATION");
                    }
                    Ok(())
                });
                expected.gate = Gate::Failed(fault);
                assert_eq!(result, Err(fault));
                assert_eq!(owner, expected);
                assert_eq!(owner.transition.liquidations.len(), (step - 1) as usize);
                assert_eq!(owner.cash, d(if step == 1 { "3000" } else { "2995.99602" }));
                let before = owner.clone();
                assert_eq!(
                    owner.liquidate_for_test(|_| panic!("failed hook")),
                    Err("RUN_FAILED")
                );
                assert_eq!(owner, before);
            }
        }
    }
}

#[test]
fn historical_precedence_stale_preparation_and_reconciliation_fail_closed() {
    let original = breached("3000");
    let prepared = prepare_step(&original, &[]).unwrap();
    let mut owner = original.clone();
    owner.liquidate_for_test(|_| Ok(())).unwrap();
    let terminal = owner.clone();
    let mut conflict = prepared.clone();
    conflict.receipt.mark = d("49901");
    conflict.receipt.canonical_payload_digest = conflict.receipt.digest().unwrap();
    assert_eq!(
        owner.commit_liquidation(&conflict, |_| Ok(())),
        Err("LIQUIDATION_ID_CONFLICT")
    );
    let mut expected = terminal;
    expected.gate = Gate::Failed("LIQUIDATION_ID_CONFLICT");
    assert_eq!(owner, expected);
    assert!(owner
        .commit_liquidation(&prepared, |_| panic!("duplicate"))
        .is_ok());
    assert_eq!(owner, expected);
    owner.gate = Gate::Failed("FIRST_FAILURE");
    let expected = owner.clone();
    assert_eq!(
        owner.commit_liquidation(&conflict, |_| Ok(())),
        Err("LIQUIDATION_ID_CONFLICT")
    );
    assert_eq!(owner, expected);
    for field in 0..6 {
        let mut owner = original.clone();
        let mut prepared = prepared.clone();
        match field {
            0 => owner.state_version += 1,
            1 => owner.commit_sequence += 1,
            2 => owner.cash = d("3001"),
            3 => owner.transition.episode.as_mut().unwrap().release_event = Some("other".into()),
            4 => prepared.cash = d("1"),
            _ => prepared.receipt.resulting_lifecycle = Some(Lifecycle::LiquidatedFlat),
        }
        let mut expected = owner.clone();
        expected.gate = Gate::Failed("INVALID_RISK_EPISODE");
        assert_eq!(
            owner.commit_liquidation(&prepared, |_| Ok(())),
            Err("INVALID_RISK_EPISODE")
        );
        assert_eq!(owner, expected);
    }
    let mut orphan = original;
    let mut unknown_episode = prepared.receipt.clone();
    unknown_episode.risk_action_episode_id = [8; 32];
    orphan.transition.liquidations.push(unknown_episode);
    let mut expected = orphan.clone();
    expected.gate = Gate::Failed("INVALID_RISK_EPISODE");
    assert_eq!(
        orphan.liquidate_for_test(|_| Ok(())),
        Err("INVALID_RISK_EPISODE")
    );
    assert_eq!(orphan, expected);
}

#[test]
fn frozen_tier_one_breach_cannot_liquidate_to_solvent_flat() {
    // Closing converts UPL into cash without adding equity; the fee exceeds
    // even the largest admitted tier-one MMR at the exact breach boundary.
    for product in [Product::Btc, Product::Eth] {
        for second in [false, true] {
            let rate = frozen_tiers(product, second).tiers[0].mmr;
            assert!(rate <= d("0.0045"));
            assert!(rate < FeePolicy::SyntheticLiquidation.rate());
        }
    }
    let mut short = source_draft(&anchor(Side::Short, "1", "50000", "3", 1500, "50100"), 1501);
    short.liquidate_for_test(|_| Ok(())).unwrap();
    assert_eq!(
        (short.cash, short.fees, short.gross_realized),
        (d("-1.01602"), d("3.01602"), d("-1"))
    );
    assert_eq!(short.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
}

#[test]
fn frozen_loop_bound_includes_both_products_and_rejects_non_decreasing_tier() {
    let (seed, config, marks) = two_product_seed("4.25", "2.5");
    let initial = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let mut owner = source_draft(&initial, 500);
    let receipts = owner.liquidate_for_test(|_| Ok(())).unwrap();
    assert_eq!(
        receipts.iter().map(|r| r.product).collect::<Vec<_>>(),
        vec![Product::Btc, Product::Eth]
    );
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d("-2.02"), d("6.02"), d("-0.25"))
    );
    assert_eq!((owner.state_version, owner.commit_sequence), (3, 4));
    assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);

    let mut owner = breached("3000");
    let mut bad = prepare_step(&owner, &[]).unwrap();
    bad.positions = owner.positions.clone();
    let mut unchanged_tier = owner.clone();
    unchanged_tier.cash = bad.cash;
    unchanged_tier.state_version = bad.receipt.account_version_after;
    let (scenario, marks) = unchanged_tier.btc_context().unwrap();
    bad.receipt.valuation_after = scenario
        .evaluate(&unchanged_tier.projection().unwrap(), marks)
        .unwrap();
    bad.receipt.reservation_after = unchanged_tier.reservation().unwrap();
    let mut expected = owner.clone();
    expected.gate = Gate::Failed("INVALID_RISK_EPISODE");
    assert_eq!(
        owner.commit_liquidation(&bad, |_| Ok(())),
        Err("INVALID_RISK_EPISODE")
    );
    assert_eq!(owner, expected);
}
