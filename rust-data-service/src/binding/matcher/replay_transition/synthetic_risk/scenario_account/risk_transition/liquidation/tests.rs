use super::super::super::tests::{d, fixture};
use super::*;
use context::tests::{activation, mark_rows};

// Consume the existing complete source draft at its pre-continuation boundary.
// Production still publishes that same draft and takes the 4C-2 sentinel.
fn source_draft(owner: &ScenarioAccount, at: i64) -> ScenarioAccount {
    let (_, marks) = owner.btc_context().unwrap();
    let mut input = activation(owner, at, mark_rows(marks, at));
    input.stamp.event_id = format!("mark-{at}");
    let (receipt, draft) = owner.prepare_context(&input).unwrap();
    let draft = draft.unwrap();
    assert_eq!(draft.transition.contexts[&input.stamp.event_id], receipt);
    assert_eq!(draft.state_version, owner.state_version + 1);
    assert_eq!(draft.gate, Gate::Running);
    draft
}

fn anchor(
    side: Side,
    quantity: &str,
    entry: &str,
    cash: &str,
    at: i64,
    mark: &str,
) -> ScenarioAccount {
    let (mut seed, config, mut marks) = fixture();
    seed.effective_at = at;
    seed.cash = d(cash);
    seed.orders.clear();
    let p = &mut seed.positions[0];
    p.side = side;
    p.contracts = d(quantity);
    p.lots.truncate(1);
    p.lots[0].contracts = d(quantity);
    p.lots[0].entry = d(entry);
    if quantity == "1001" {
        p.lots[0].contracts = d("1");
        let mut second = p.lots[0].clone();
        second.contracts = d("1000");
        second.seed_execution_id = "second".into();
        second.seed_sequence = 1;
        p.lots.push(second);
    }
    marks[0].price = d("50000");
    let mark_at = if at == 1999 { 2001 } else { at + 1 };
    marks[0].valid_to = mark_at;
    marks.push(Mark {
        product: Product::Btc,
        price: d(mark),
        valid_from: mark_at,
        valid_to: 3000,
    });
    ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
}

#[test]
fn long_short_exact_preparation_is_pure_and_production_remains_unwired() {
    for (side, quantity, cash, at, mark, qty, fee, pnl, equity, mmr, decision) in [
        (
            Side::Long,
            "1001",
            "3000",
            500,
            "49900",
            "1",
            "3.00398",
            "-1",
            "1995.99602",
            "1996",
            StepDecision::ContinueLiquidation,
        ),
        (
            Side::Short,
            "1",
            "3",
            1500,
            "50100",
            "1",
            "3.01602",
            "-1",
            "-1.01602",
            "0",
            StepDecision::LiquidatedInsolvent,
        ),
    ] {
        let initial = anchor(side, quantity, "50000", cash, at, mark);
        let owner = source_draft(&initial, at + 1);
        let before = owner.clone();
        let r = prepare(&owner, &[]).unwrap();
        assert_eq!(owner, before);
        assert_eq!(r, prepare(&owner, &[]).unwrap());
        assert_eq!(r.position_side_before, side);
        assert_ne!(r.execution_side, side);
        assert_eq!((r.contracts, r.base_quantity), (d(qty), d("0.01")));
        assert_eq!(
            (r.fee, r.fee_rate, r.gross_realized_delta),
            (d(fee), d("0.00602"), d(pnl))
        );
        assert_eq!(r.cash_delta, add(d(pnl), -d(fee)).unwrap());
        assert_eq!(
            (
                r.valuation_after.equity,
                r.valuation_after.maintenance_margin
            ),
            (d(equity), d(mmr))
        );
        assert_eq!(
            (
                r.account_version_before,
                r.account_version_after,
                r.commit_sequence_before,
                r.commit_sequence
            ),
            if side == Side::Long {
                (1, 2, 2, 3)
            } else {
                (1, 2, 1, 2)
            }
        );
        assert_eq!(r.post_step_decision, decision);
        assert_eq!(r.resulting_lifecycle, decision.lifecycle());
        assert_eq!(r.step_index, 1);
        assert_eq!(
            r.liquidation_id,
            step_id(&owner.key, owner.transition.episode.as_ref().unwrap().id, 1)
        );
        if side == Side::Long {
            assert_eq!(r.position_after.as_ref().unwrap().contracts, d("1000"));
            assert_eq!(
                r.position_after.as_ref().unwrap().lots[0].source.contracts,
                d("1000")
            );
            assert_eq!(
                r.position_after.as_ref().unwrap().lots[0]
                    .source
                    .seed_execution_id,
                "second"
            );
            assert_eq!(add(owner.cash, r.cash_delta).unwrap(), d("2995.99602"));
        } else {
            assert!(r.position_after.is_none());
        }
        let mut production = initial;
        let (_, marks) = production.btc_context().unwrap();
        let input = activation(&production, at + 1, mark_rows(marks, at + 1));
        production.activate_context(&input).unwrap();
        assert_eq!(production.gate, Gate::Failed("UNSUPPORTED_RISK_TRANSITION"));
        assert_eq!(production.fees, Decimal::ZERO);
        assert_eq!(production.positions, owner.positions);
    }
}

#[test]
fn origin_spec_v1_survives_real_spec_v2_activation_and_preparation() {
    let mut initial = anchor(Side::Long, "1", "50000.1", "3", 1999, "49900");
    let input = activation(
        &initial,
        2000,
        context::Rows::Specs(
            [Product::Btc, Product::Eth]
                .into_iter()
                .map(|p| (p, "spec-v1".into(), "spec-v2".into(), 2000))
                .collect(),
        ),
    );
    initial.activate_context(&input).unwrap();
    let mut owner = source_draft(&initial, 2001);
    let r = prepare(&owner, &[]).unwrap();
    assert_eq!(r.spec_version, "spec-v2");
    assert_eq!(r.position_before.lots[0].origin_spec_version, "spec-v1");
    assert_eq!(r.position_before.lots[0].source.entry, d("50000.1"));
    assert_eq!(
        (r.fee, r.gross_realized_delta, r.valuation_after.equity),
        (d("3.00398"), d("-1.001"), d("-1.00498"))
    );
    owner
        .positions
        .btc_mut()
        .unwrap()
        .get_mut(&Product::Btc)
        .unwrap()
        .lots[0]
        .origin_spec_version = "unknown".into();
    let before = owner.clone();
    assert_eq!(prepare(&owner, &[]), Err("INVALID_LOT_ORIGIN_SPEC"));
    assert_eq!(owner, before);
}

fn two_product_seed(cash: &str, eth_quantity: &str) -> (CleanSeed, FrozenScenario, Vec<Mark>) {
    let (mut seed, config, mut marks) = fixture();
    seed.effective_at = 499;
    seed.cash = d(cash);
    seed.orders.clear();
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    let mut eth = seed.positions[0].clone();
    eth.product = Product::Eth;
    eth.side = Side::Short;
    eth.contracts = d(eth_quantity);
    eth.lots[0].contracts = d(eth_quantity);
    eth.lots[0].entry = d("1999");
    eth.lots[0].seed_sequence = 1;
    eth.lots[0].seed_execution_id = "ETH-seed".into();
    seed.positions.push(eth);
    marks[0].price = d("50000");
    marks[1].price = d("1999");
    marks[1].valid_to = 500;
    marks.push(Mark {
        product: Product::Eth,
        price: d("2000"),
        valid_from: 500,
        valid_to: 3000,
    });
    (seed, config, marks)
}

#[test]
fn exact_mmr_tie_uses_canonical_product_not_seed_order() {
    let (mut seed, config, marks) = two_product_seed("4.25", "2.5");
    let mut results = vec![];
    for reverse in [false, true] {
        if reverse {
            seed.positions.reverse();
        }
        let initial = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        assert_eq!(
            initial.reservation().unwrap().maintenance_margin,
            d("3.999")
        );
        let owner = source_draft(&initial, 500);
        let r = prepare(&owner, &[]).unwrap();
        assert_eq!(
            (
                r.valuation_before.equity,
                r.valuation_before.maintenance_margin
            ),
            (d("4"), d("4"))
        );
        assert_eq!(r.product, Product::Btc);
        assert_eq!(
            (r.fee, r.gross_realized_delta, r.valuation_after.equity),
            (d("3.01"), d("0"), d("0.99"))
        );
        results.push((owner.transition.contexts, r));
    }
    assert_eq!(results[0], results[1]);
}

#[test]
fn greater_eth_mmr_precedes_canonical_first_btc_without_mutation() {
    let (seed, config, marks) = two_product_seed("4.6", "3");
    let initial = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(
        initial.reservation().unwrap().maintenance_margin,
        d("4.3988")
    );
    let owner = source_draft(&initial, 500);
    let before = owner.clone();
    let r = prepare(&owner, &[]).unwrap();
    assert_eq!(owner, before);
    assert_eq!(
        (
            r.valuation_before.equity,
            r.valuation_before.maintenance_margin
        ),
        (d("4.3"), d("4.4"))
    );
    assert_eq!(
        r.valuation_before
            .products
            .iter()
            .map(|p| (p.product, p.maintenance_margin))
            .collect::<Vec<_>>(),
        vec![(Product::Btc, d("2")), (Product::Eth, d("2.4"))]
    );
    assert_eq!(
        (r.product, r.position_side_before, r.execution_side),
        (Product::Eth, Side::Short, Side::Long)
    );
    assert_eq!(
        (r.contracts, r.base_quantity, r.fee, r.gross_realized_delta),
        (d("3"), d("0.3"), d("3.612"), d("-0.3"))
    );
    assert_eq!(add(owner.cash, r.cash_delta).unwrap(), d("0.688"));
    assert!(r.position_after.is_none());
    assert_eq!(
        (
            r.valuation_after.equity,
            r.valuation_after.maintenance_margin
        ),
        (d("0.688"), d("2"))
    );
    assert_eq!(r.valuation_after.products[0].product, Product::Btc);
    assert_eq!(r.post_step_decision, StepDecision::ContinueLiquidation);
    assert_eq!(r.resulting_lifecycle, None);
}

#[test]
fn identity_prefix_duplicate_conflict_and_faults_do_not_mutate_owner() {
    let owner = source_draft(
        &anchor(Side::Long, "1001", "50000", "3000", 500, "49900"),
        501,
    );
    let r = prepare(&owner, &[]).unwrap();
    let history = vec![r.clone()];
    // Independent SHA-256 anchor from the contract's signed length-prefix bytes.
    assert_eq!(
        r.liquidation_id.map(|b| format!("{b:02x}")).concat(),
        "d0f12263c95316fabf789052c22816550d68ac8dcb1e59efff3864995d94703d"
    );
    assert_eq!(historical(&history, &r).unwrap(), Some(&r));
    assert_eq!(
        next_index(&owner.key, r.risk_action_episode_id, &history),
        Ok(2)
    );
    let mut gap = r.clone();
    gap.step_index = 2;
    gap.liquidation_id = step_id(&owner.key, gap.risk_action_episode_id, 2);
    gap.canonical_payload_digest = gap.digest().unwrap();
    assert_eq!(
        next_index(&owner.key, r.risk_action_episode_id, &[gap]),
        Err("INVALID_RISK_EPISODE")
    );
    let mut conflict = r.clone();
    conflict.mark = d("49901");
    conflict.canonical_payload_digest = conflict.digest().unwrap();
    assert_eq!(
        historical(&history, &conflict),
        Err("LIQUIDATION_ID_CONFLICT")
    );
    for field in 0..4 {
        let mut bad = r.clone();
        match field {
            0 => bad.step_index = 2,
            1 => bad.risk_action_episode_id = [7; 32],
            2 => bad.liquidation_id = [9; 32],
            _ => bad.canonical_payload_digest = [3; 32],
        }
        assert_eq!(
            next_index(&owner.key, r.risk_action_episode_id, &[bad]),
            Err("INVALID_RISK_EPISODE")
        );
    }
    assert_eq!(
        next_index(
            &owner.key,
            r.risk_action_episode_id,
            &[r.clone(), r.clone()]
        ),
        Err("INVALID_RISK_EPISODE")
    );
    for field in 0..8 {
        let mut bad = owner.clone();
        let expected = match field {
            0 => {
                bad.gate = Gate::Failed("FIRST");
                "RUN_FAILED"
            }
            1 => {
                bad.transition.lifecycle = Lifecycle::LiquidatedFlat;
                "RUN_TERMINAL"
            }
            2 => {
                bad.transition.episode = None;
                "INVALID_RISK_EPISODE"
            }
            3 => {
                bad.transition.episode.as_mut().unwrap().current_reason =
                    cancel::Reason::RiskShortfall;
                "INVALID_RISK_EPISODE"
            }
            4 => {
                bad.valuation_context_id = [0; 32];
                "UNSUPPORTED_CONTEXT_TRANSITION"
            }
            5 => {
                bad.fees = Decimal::MAX;
                "DECIMAL_OVERFLOW"
            }
            6 => {
                bad.state_version = u64::MAX;
                "VERSION_OVERFLOW"
            }
            _ => {
                bad.commit_sequence = u64::MAX;
                "SEQUENCE_OVERFLOW"
            }
        };
        let before = bad.clone();
        assert_eq!(prepare(&bad, &[]), Err(expected));
        assert_eq!(bad, before);
        assert_eq!(historical(&history, &r).unwrap(), Some(&r));
    }
}

#[test]
fn pending_cancel_and_invalid_preparation_inputs_fail_without_a_step() {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("2.99");
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].entry = d("50100");
    marks[0].price = d("50100");
    marks[0].valid_to = 600;
    marks.push(Mark {
        product: Product::Btc,
        price: d("50000"),
        valid_from: 600,
        valid_to: 3000,
    });
    let mut pending = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let input = activation(&pending, 600, mark_rows(&marks, 600));
    pending.activate_context(&input).unwrap();
    assert_eq!(
        pending.transition.lifecycle,
        Lifecycle::AwaitingCancelEffective
    );
    assert!(matches!(
        pending.orders["O1"].cancel,
        cancel::State::Requested(_)
    ));
    let before = pending.clone();
    assert_eq!(prepare(&pending, &[]), Err("INVALID_RISK_EPISODE"));
    assert_eq!(pending, before);

    let owner = source_draft(
        &anchor(Side::Long, "1001", "50000", "3000", 500, "49900"),
        501,
    );
    for field in 0..7 {
        let mut bad = owner.clone();
        let expected = match field {
            0 => {
                bad.profile = ProfileContext::GoldenCancel(golden_cancel::Config::frozen());
                "PROFILE_MISMATCH"
            }
            1 => {
                bad.positions = PositionState::CapacityFlat;
                "PROFILE_MISMATCH"
            }
            2 => {
                bad.positions
                    .btc_mut()
                    .unwrap()
                    .get_mut(&Product::Btc)
                    .unwrap()
                    .contracts = d("1001.001");
                "INVALID_POSITION"
            }
            _ => {
                let ProfileContext::BtcEthScenario { scenario, marks } = &mut bad.profile else {
                    unreachable!()
                };
                match field {
                    3 => {
                        marks.remove(2);
                    }
                    4 => {
                        marks.push(marks[2].clone());
                    }
                    5 => {
                        marks[2].price = d("49900.01");
                    }
                    _ => {
                        scenario
                            .tiers
                            .retain(|t| !(t.product == Product::Btc && t.version == "tier-v1"));
                    }
                }
                if field == 5 {
                    bad.valuation_context_id = context_id(scenario, marks, 501).unwrap();
                }
                match field {
                    3 | 6 => "UNSUPPORTED_CONTEXT_TRANSITION",
                    4 => "OVERLAPPING_MARKS",
                    _ => "INVALID_HYPOTHETICAL_EXECUTION",
                }
            }
        };
        let before = bad.clone();
        assert_eq!(prepare(&bad, &[]), Err(expected), "field {field}");
        assert_eq!(bad, before);
    }
}
