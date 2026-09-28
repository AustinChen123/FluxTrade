use super::super::super::tests::{d, fixture};
use super::tests::anchor;
use super::*;
use context::tests::{activation, mark_rows, stamp};
use group::{Group, Input, Member, Reference};

#[test]
fn duplicate_intent_group_references_original_source_without_republication() {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let intent = admission::fixture_intent(&owner, "I", Side::Short, d("0.01"), d("50000"));
    assert_eq!(
        admission::fixture_admit(&mut owner, "A", 500, &intent)
            .unwrap()
            .0,
        "accepted"
    );
    let before = owner.clone();
    let group = Group {
        group_id: "G2".into(),
        account_key: owner.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: 501,
        declared_member_count: 1,
        members: vec![Member {
            stamp: stamp("B", 501, 60),
            input: Input::Intent(intent),
        }],
    };
    let completion = owner.apply_group(&group).unwrap();
    assert_eq!(completion.committed, vec![Reference::Source("A".into())]);
    assert_eq!(completion.failure, None);
    assert!(!owner.transition.events.contains_key("B"));
    let mut expected = before;
    expected.transition.admissions = owner.transition.admissions.clone();
    expected.transition.groups = owner.transition.groups.clone();
    assert_eq!(owner, expected);
}

#[test]
fn terminal_unknown_effect_matches_direct_rejection_but_history_conflict_wins() {
    let mut owner = anchor(Side::Long, "1001", "50000", "3000", 500, "49900");
    let source = mark(&owner, 501, "terminal-mark");
    owner.activate_context(&source).unwrap();
    let saved = owner.clone();
    for (id, error) in [
        (owner.liquidation_ids().next().unwrap(), "INVALID_SCHEMA"),
        ([0; 32], "UNKNOWN_RECEIPT_REFERENCE"),
    ] {
        assert_eq!(
            delivery::resolve(&owner, &Reference::Liquidation(id), "EXECUTION_FACT", None),
            Err(error)
        );
        assert_eq!(owner, saved);
    }
    let input = cancel::EffectInput {
        stamp: stamp("fresh", 502, 50),
        effects: vec![(
            "request".into(),
            "unknown".into(),
            cancel::Reason::ExplicitScenario,
        )],
    };
    let group = Group {
        group_id: "terminal".into(),
        account_key: owner.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: 502,
        declared_member_count: 1,
        members: vec![Member {
            stamp: input.stamp.clone(),
            input: Input::Effect(input.clone()),
        }],
    };
    let before = owner.clone();
    assert_eq!(owner.effect_cancel(&input), Err("RUN_TERMINAL"));
    assert_eq!(owner, before);
    assert_eq!(owner.apply_group(&group), Err("RUN_TERMINAL"));
    assert_eq!(owner, before);
    let mut conflict = group;
    conflict.members[0].stamp.event_id = "terminal-mark".into();
    if let Input::Effect(input) = &mut conflict.members[0].input {
        input.stamp.event_id = "terminal-mark".into();
    }
    assert_eq!(owner.apply_group(&conflict), Err("EVENT_ID_CONFLICT"));
    let mut expected = before;
    expected.gate = Gate::Failed("EVENT_ID_CONFLICT");
    assert_eq!(owner, expected);
}

fn mark(owner: &ScenarioAccount, at: i64, id: &str) -> context::Input {
    let (_, marks) = owner.btc_context().unwrap();
    let mut input = activation(owner, at, mark_rows(marks, at));
    input.stamp.event_id = id.into();
    input
}

#[test]
fn tied_products_complete_identically_across_seed_insertion_orders() {
    let (mut seed, config, marks) = super::tests::two_product_seed("4.25", "2.5");
    let mut runs = Vec::new();
    for reverse in [false, true] {
        if reverse {
            seed.positions.reverse();
        }
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let input = mark(&owner, 500, "tie-mark");
        let source = owner.activate_context(&input).unwrap();
        assert_eq!(source.after_version, 1);
        assert_eq!(source.after.equity, d("4"));
        assert_eq!(owner.transition.contexts["tie-mark"], source);
        assert_eq!(owner.transition.events.len(), 1);
        assert_eq!(owner.transition.accepted_stamp.as_ref(), Some(&input.stamp));
        let receipts = &owner.transition.liquidations;
        assert_eq!(
            receipts
                .iter()
                .map(|r| r.product.clone())
                .collect::<Vec<_>>(),
            vec![Product::Btc, Product::Eth]
        );
        assert_eq!(
            receipts.iter().map(|r| r.step_index).collect::<Vec<_>>(),
            vec![1, 2]
        );
        for receipt in receipts {
            assert_eq!(
                receipt.liquidation_id,
                step_id(
                    &owner.key,
                    receipt.risk_action_episode_id,
                    receipt.step_index
                )
            );
            assert_eq!(receipt.trigger_event_id, "tie-mark");
        }
        assert_eq!((owner.cash, owner.fees), (d("-2.02"), d("6.02")));
        assert_eq!(
            receipts
                .iter()
                .map(|r| r.gross_realized_delta)
                .sum::<Decimal>(),
            d("-0.25")
        );
        assert_eq!((owner.state_version, owner.commit_sequence), (3, 4));
        assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
        assert!(owner.positions.is_empty());
        let before = owner.clone();
        assert_eq!(owner.activate_context(&input).unwrap(), source);
        assert_eq!(owner, before);
        runs.push((source, owner));
    }
    assert_eq!(runs[0], runs[1]);
}

fn mark_group(owner: &ScenarioAccount) -> Group {
    let mark = mark(owner, 501, "mark-501");
    let intent = admission::fixture_intent(owner, "later", Side::Short, d("0.01"), d("49900"));
    Group {
        group_id: "liquidation-group".into(),
        account_key: owner.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: 501,
        declared_member_count: 2,
        members: vec![
            Member {
                stamp: mark.stamp.clone(),
                input: Input::Context(mark),
            },
            Member {
                stamp: stamp("later", 501, 60),
                input: Input::Intent(intent),
            },
        ],
    }
}

#[test]
fn group_finishes_liquidation_before_next_member_and_replays_exactly() {
    for cash in ["3000", "3001"] {
        let mut runs = Vec::new();
        for _ in 0..2 {
            let mut owner = anchor(Side::Long, "1001", "50000", cash, 500, "49900");
            let group = mark_group(&owner);
            let result = owner.apply_group(&group).unwrap();
            let ids = owner.liquidation_ids().collect::<Vec<_>>();
            let mut expected = vec![Reference::Source("mark-501".into())];
            expected.extend(ids.iter().copied().map(Reference::Liquidation));
            if cash == "3001" {
                expected.push(Reference::Source("later".into()));
            }
            assert_eq!(result.committed, expected);
            assert_eq!(result.failure, None);
            assert_eq!(
                result.rejections,
                if cash == "3000" {
                    vec![("later".into(), "RUN_TERMINAL")]
                } else {
                    vec![]
                }
            );
            assert_eq!(ids.len(), if cash == "3000" { 2 } else { 1 });
            assert_eq!(
                owner.cash,
                d(if cash == "3000" {
                    "-1007.98398"
                } else {
                    "2996.99602"
                })
            );
            assert_eq!(owner.state_version, 3);
            assert_eq!(owner.transition.contexts["mark-501"].after_version, 1);
            assert_eq!(
                owner.transition.accepted_stamp.as_ref().unwrap().event_id,
                if cash == "3000" { "mark-501" } else { "later" }
            );
            assert_eq!(
                owner.transition.events.len(),
                if cash == "3000" { 1 } else { 2 }
            );
            let before = owner.clone();
            assert_eq!(owner.apply_group(&group).unwrap(), result);
            assert_eq!(owner, before);
            let original = match &group.members[0].input {
                Input::Context(i) => i,
                _ => unreachable!(),
            };
            assert_eq!(
                owner.activate_context(original).unwrap(),
                before.transition.contexts["mark-501"]
            );
            assert_eq!(owner, before);
            runs.push(owner);
        }
        assert_eq!(runs[0], runs[1]);
    }
}

#[test]
fn group_liquidation_fault_keeps_source_prefix_and_duplicate_completion() {
    for step in [1, 2] {
        for panic in [false, true] {
            let mut owner = anchor(Side::Long, "1001", "50000", "3000", 500, "49900");
            let group = mark_group(&owner);
            let fault = if panic {
                "LIQUIDATION_PANIC"
            } else {
                "INJECTED"
            };
            let result = owner
                .apply_group_checked(&group, |stage| {
                    if stage == Stage::LiquidationBeforeSwap(step) {
                        if panic {
                            panic!("liquidation prefix");
                        }
                        return Err("INJECTED");
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(result.failure, Some(fault));
            let mut expected = vec![Reference::Source("mark-501".into())];
            expected.extend(owner.liquidation_ids().map(Reference::Liquidation));
            assert_eq!(result.committed, expected);
            assert_eq!(result.committed.len(), step as usize);
            assert_eq!(owner.cash, d(if step == 1 { "3000" } else { "2995.99602" }));
            assert_eq!(owner.state_version, step as u64);
            assert_eq!(owner.transition.contexts.len(), 1);
            assert_eq!(owner.transition.events.len(), 1);
            assert!(owner.orders.is_empty());
            let before = owner.clone();
            assert_eq!(owner.apply_group(&group).unwrap(), result);
            assert_eq!(owner, before);
            let mut conflict = group.clone();
            conflict.members[0].stamp.source_sequence = Some(8);
            assert!(owner.apply_group(&conflict).is_err());
            assert_eq!(owner, before);
        }
    }
}

#[test]
fn escalation_intervening_fill_and_final_effect_keep_distinct_causal_ids() {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("3");
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    let order = &mut seed.orders[0];
    order.side = Side::Long;
    order.reduce_only = false;
    order.original = d("0.01");
    order.remaining = d("0.01");
    order.filled = Decimal::ZERO;
    order.price = d("50000");
    order.status = "OPEN".into();
    let mut second = order.clone();
    second.order_id = "O2".into();
    second.intent_id = "I2".into();
    second.client_id = "C2".into();
    seed.orders.push(second);
    marks[0].price = d("50000");
    marks[0].valid_to = 600;
    for (from, to, price) in [(600, 700, "49900"), (700, 3000, "49800")] {
        marks.push(Mark {
            product: Product::Btc,
            price: d(price),
            valid_from: from,
            valid_to: to,
        });
    }
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let first = mark(&owner, 600, "shortfall");
    owner.activate_context(&first).unwrap();
    let episode = owner.transition.episode.clone().unwrap();
    assert_eq!(episode.initial_reason, cancel::Reason::RiskShortfall);
    let escalation = mark(&owner, 700, "escalate");
    owner.activate_context(&escalation).unwrap();
    let candidate = execution::commit::tests::input(&owner, "O1", "intervening", "0.01", "50000");
    owner.execute(&candidate).unwrap();
    assert_eq!(owner.cash, d("2.995"));
    assert_eq!(owner.liquidation_ids().count(), 0);
    for (id, event, at) in [("O1", "first-effect", 701), ("O2", "release", 702)] {
        let effect = cancel::EffectInput {
            stamp: stamp(event, at, 50),
            effects: vec![("shortfall".into(), id.into(), cancel::Reason::RiskShortfall)],
        };
        let receipt = owner.effect_cancel(&effect).unwrap();
        if id == "O1" {
            assert_eq!(owner.liquidation_ids().count(), 0);
        }
        let before = owner.clone();
        assert_eq!(owner.effect_cancel(&effect).unwrap(), receipt);
        assert_eq!(owner, before);
    }
    let r = &owner.transition.liquidations[0];
    assert_eq!(r.risk_action_episode_id, episode.id);
    assert_eq!(r.trigger_event_id, "shortfall");
    assert_eq!(r.escalation_event_id.as_deref(), Some("escalate"));
    assert_eq!(r.release_event_id.as_deref(), Some("release"));
    assert_eq!(
        (r.contracts, r.fee, r.gross_realized_delta),
        (d("1.01"), d("3.0279396"), d("-2.02"))
    );
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.state_version,
            owner.commit_sequence
        ),
        (d("-2.0529396"), d("3.0329396"), 7, 3)
    );
    assert_eq!(owner.orders["O1"].facts.status, "FILLED");
    assert_eq!(owner.orders["O2"].facts.status, "CANCELED");
}

#[test]
fn tick_migration_final_effect_can_release_origin_spec_liquidation() {
    let (mut seed, config, mut marks) = fixture();
    seed.effective_at = 1998;
    seed.cash = d("3");
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].entry = d("50000.1");
    marks[0].price = d("50000");
    marks[0].valid_to = 1999;
    marks.push(Mark {
        product: Product::Btc,
        price: d("49900"),
        valid_from: 1999,
        valid_to: 3000,
    });
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let breach = mark(&owner, 1999, "breach");
    owner.activate_context(&breach).unwrap();
    assert_eq!(owner.liquidation_ids().count(), 0);
    let input = activation(
        &owner,
        2000,
        context::Rows::Specs(
            [Product::Btc, Product::Eth]
                .into_iter()
                .map(|p| (p, "spec-v1".into(), "spec-v2".into(), 2000))
                .collect(),
        ),
    );
    let source = owner.activate_context(&input).unwrap();
    assert_eq!(source.migration_effects.len(), 1);
    assert_eq!(source.after_version, 3);
    assert_eq!(owner.state_version, 4);
    let r = &owner.transition.liquidations[0];
    assert_eq!(r.spec_version, "spec-v2");
    assert_eq!(r.position_before.lots[0].origin_spec_version, "spec-v1");
    assert_eq!(r.trigger_event_id, "breach");
    assert_eq!(r.release_event_id.as_deref(), Some("activate"));
    assert_eq!(
        (owner.cash, r.fee, r.gross_realized_delta),
        (d("-1.00498"), d("3.00398"), d("-1.001"))
    );
}

#[test]
fn a04_post_accept_shortfall_cancels_without_liquidation() {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("1000");
    seed.orders.clear();
    seed.positions[0].product = Product::Eth;
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].entry = d("2000");
    marks[0].price = d("50000");
    marks[1].price = d("3000");
    marks[1].valid_to = 600;
    marks.push(Mark {
        product: Product::Eth,
        price: d("2000"),
        valid_from: 600,
        valid_to: 3000,
    });
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let intent = admission::fixture_intent(&owner, "a04", Side::Long, d("20"), d("50000"));
    let (kind, accepted) = admission::fixture_admit(&mut owner, "accept", 500, &intent).unwrap();
    assert_eq!(kind, "accepted");
    assert_eq!(owner.reservation().unwrap().available_margin, d("60"));
    let input = mark(&owner, 600, "shortfall");
    let source = owner.activate_context(&input).unwrap();
    assert_eq!(source.after.available_margin, d("-30"));
    assert!(!source.decision.liquidation_required);
    owner
        .effect_cancel(&cancel::EffectInput {
            stamp: stamp("effect", 600, 50),
            effects: vec![(
                "shortfall".into(),
                owner.orders.keys().next().unwrap().clone(),
                cancel::Reason::RiskShortfall,
            )],
        })
        .unwrap();
    assert_eq!(owner.reservation().unwrap().available_margin, d("980"));
    assert_eq!(
        (owner.cash, owner.fees, owner.state_version),
        (d("1000"), d("0"), 4)
    );
    assert_eq!(owner.transition.lifecycle, Lifecycle::RiskStable);
    assert_eq!(owner.liquidation_ids().count(), 0);
    assert_eq!(
        admission::fixture_admit(&mut owner, "accept", 500, &intent)
            .unwrap()
            .1,
        accepted
    );
}
