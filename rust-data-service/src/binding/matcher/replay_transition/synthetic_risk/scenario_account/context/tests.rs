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
