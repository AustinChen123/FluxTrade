use super::super::super::golden_cancel::Config;
use super::super::super::risk_transition::cancel::{
    self, EffectInput, Reason, RequestInput, Stamp,
};
use super::super::super::tests::{d, fixture};
use super::*;

#[test]
fn activated_tick_context_executes_aligned_price_against_fractional_origin_lot() {
    use super::super::super::context::{tests::activation, Rows};
    for (product, entry, price, cash) in [
        (Product::Btc, "50000.1", "50000", "9999.7495"),
        (Product::Eth, "2000.01", "2000", "9999.8995"),
    ] {
        let (mut seed, config, mut marks) = fixture();
        seed.effective_at = 1999;
        seed.positions[0].product = product;
        seed.positions[0].contracts = d("1");
        seed.positions[0].lots.truncate(1);
        seed.positions[0].lots[0].entry = d(entry);
        marks
            .iter_mut()
            .find(|m| m.product == product)
            .unwrap()
            .price = d(price);
        let order = &mut seed.orders[0];
        order.product = ProfileProduct::BtcEth(product);
        order.price = d(price);
        order.original = d("0.5");
        order.filled = Decimal::ZERO;
        order.remaining = d("0.5");
        order.status = "OPEN".into();
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
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
        owner.activate_context(&input).unwrap();
        let mut c = candidate(&owner, "O1", "after-activation", "0.5");
        c.template.matching_effective_at = 2000;
        c.spec_version = "spec-v2".into();
        c.rule_data_version = "tier-v2".into();
        let Reply::Committed { receipt, .. } = owner.execute(&c).unwrap() else {
            panic!("commit")
        };
        assert_eq!(receipt.realized_pnl_delta, d("-0.0005"));
        assert_eq!(owner.cash, d(cash));
        assert_eq!(owner.state_version, 2);
        assert_eq!(owner.commit_sequence, 2);
        let lot = &owner.positions.btc().unwrap()[&product].lots[0];
        assert_eq!(lot.origin_spec_version, "spec-v1");
        assert_eq!(lot.source.entry, d(entry));
        assert_eq!(lot.source.contracts, d("0.5"));
    }
}

#[test]
fn canonical_and_reserved_reverse_groups_retain_exact_gt03_prefix() {
    use super::super::super::group::{Group, Input, Member};
    for reverse in [false, true] {
        let (mut owner, order) = golden();
        fill(&mut owner, &order, "X1", "4");
        owner.request_cancel(&request("C1", &[&order])).unwrap();
        let ordering = if reverse {
            "S_order_v1_reverse_execution_cancel_effective"
        } else {
            "S_order_v1"
        };
        let mut execution = candidate(&owner, &order, "X2", "3");
        execution.event_id = "X2-event".into();
        execution.template.matching_effective_at = 600;
        if reverse {
            execution.expected_account_version += 1;
            execution.expected_order_version += 1;
        }
        let mut e_stamp = stamp("X2-event", if reverse { 50 } else { 30 });
        e_stamp.effective_at = 600;
        e_stamp.ordering_contract_id = ordering.into();
        let mut effect = effect("effect", "C1", &[&order]);
        effect.stamp.effective_at = 600;
        effect.stamp.scenario_ordinal = if reverse { 30 } else { 50 };
        effect.stamp.ordering_contract_id = ordering.into();
        let group = Group {
            group_id: if reverse {
                "reverse-execution-cancel-effective-v1"
            } else {
                "canonical"
            }
            .into(),
            account_key: owner.key.clone(),
            ordering_contract_id: ordering.into(),
            group_effective_at: 600,
            declared_member_count: 2,
            members: vec![
                Member {
                    stamp: e_stamp,
                    input: Input::Execution(execution),
                },
                Member {
                    stamp: effect.stamp.clone(),
                    input: Input::Effect(effect),
                },
            ],
        };
        if !reverse {
            for field in 0..5 {
                let mut invalid = group.clone();
                match field {
                    0 => {
                        invalid.members[0].stamp.source_sequence = Some(2);
                        invalid.members[1].stamp.source_sequence = Some(1);
                    }
                    1 => {
                        invalid.members[0].stamp.causal_parent_ids = vec!["effect".into()];
                        invalid.members[1].stamp.causal_parent_ids = vec!["X2-event".into()];
                    }
                    2 => invalid.members[0].stamp.causal_parent_ids = vec!["unknown".into()],
                    3 => invalid.members[1].stamp.scenario_ordinal = 30,
                    _ => {
                        invalid.members[0].stamp.source_sequence = Some(1);
                        invalid.members[1].stamp.source_sequence = Some(1);
                    }
                }
                let effect_stamp = invalid.members[1].stamp.clone();
                if let Input::Effect(effect) = &mut invalid.members[1].input {
                    effect.stamp = effect_stamp;
                }
                let mut actual = owner.clone();
                assert!(actual.apply_group(&invalid).is_err());
                let mut expected = owner.clone();
                expected.gate = actual.gate.clone();
                assert_eq!(actual, expected);
            }
        }
        let completion = owner.apply_group(&group).unwrap();
        assert_eq!(
            completion.failure,
            if reverse {
                Some("UNSUPPORTED_EXECUTION")
            } else {
                None
            }
        );
        assert_eq!(owner.state_version, if reverse { 4 } else { 5 });
        assert_eq!(owner.cash, d(if reverse { "999.6" } else { "999.3" }));
        assert_eq!(
            owner.orders[&order].facts.canceled,
            d(if reverse { "6" } else { "3" })
        );
        let before = owner.clone();
        assert_eq!(owner.apply_group(&group).unwrap(), completion);
        assert_eq!(owner, before);
    }
}

fn stamp(event: &str, ordinal: i64) -> Stamp {
    Stamp {
        event_id: event.into(),
        effective_at: if ordinal == 40 { 510 } else { 520 },
        source_sequence: None,
        causal_parent_ids: Vec::new(),
        ordering_contract_id: "S_order_v1".into(),
        scenario_ordinal: ordinal,
    }
}
fn request(event: &str, targets: &[&str]) -> RequestInput {
    RequestInput {
        stamp: stamp(event, 40),
        targets: targets
            .iter()
            .map(|id| ((*id).into(), Reason::ExplicitScenario))
            .collect(),
    }
}
fn effect(event: &str, detecting: &str, targets: &[&str]) -> EffectInput {
    EffectInput {
        stamp: stamp(event, 50),
        effects: targets
            .iter()
            .map(|id| (detecting.into(), (*id).into(), Reason::ExplicitScenario))
            .collect(),
    }
}
fn golden() -> (ScenarioAccount, String) {
    let (mut seed, _, _) = fixture();
    seed.cash = d("1000");
    seed.positions.clear();
    seed.orders.clear();
    let mut owner = ScenarioAccount::from_golden_cancel_seed(&seed, &Config::frozen()).unwrap();
    let order = admission::admit_execution_fixture(&mut owner, Side::Long, d("10"), d("10"));
    (owner, order)
}

fn candidate(
    owner: &ScenarioAccount,
    id: &str,
    external: &str,
    quantity: &str,
) -> ExecutionCandidate {
    let (_, mut c) = super::super::tests::fixture_candidate();
    let order = &owner.orders[id];
    c.template.key.account = owner.key.clone();
    c.template.key.product = order.facts.product;
    c.template.key.external_id = external.into();
    c.event_id = format!("event-{external}");
    c.template.matching_effective_at = owner
        .transition
        .accepted_stamp
        .as_ref()
        .map_or(owner.seed_effective_at, |s| s.effective_at + 1);
    c.template.order_id = id.into();
    c.template.side = order.facts.side;
    c.template.price = order.facts.price;
    c.template.quantity = d(quantity);
    c.expected_account_version = owner.state_version;
    c.expected_order_version = order.version;
    if order.facts.product == ProfileProduct::Pa {
        c.spec_version = "gt03-spec-v1".into();
        c.rule_data_version = "gt03-rule-v1".into();
    }
    c
}
fn fill(
    owner: &mut ScenarioAccount,
    id: &str,
    external: &str,
    quantity: &str,
) -> CommittedExecution {
    let c = candidate(owner, id, external, quantity);
    let Reply::Committed {
        receipt,
        terminal_reason: None,
    } = owner.execute(&c).unwrap()
    else {
        panic!("commit required");
    };
    receipt
}
fn assert_golden(
    owner: &ScenarioAccount,
    id: &str,
    version: u64,
    filled: &str,
    canceled: &str,
    remaining: &str,
    cash: &str,
    fees: &str,
    reservation: &str,
) {
    let order = &owner.orders[id];
    assert_eq!((owner.state_version, order.version), (version, version));
    assert_eq!(
        (
            order.facts.filled,
            order.facts.canceled,
            order.facts.remaining
        ),
        (d(filled), d(canceled), d(remaining))
    );
    assert_eq!(
        add(
            add(order.facts.filled, order.facts.canceled).unwrap(),
            order.facts.remaining
        ),
        Ok(d("10"))
    );
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (d(cash), d(fees), Decimal::ZERO)
    );
    assert_eq!(owner.golden_cancel_reservation(), Ok(d(reservation)));
    let PositionState::GoldenCancel(position) = &owner.positions else {
        panic!("profile");
    };
    if d(filled) == Decimal::ZERO {
        assert!(position.is_none());
    } else {
        let position = position.as_ref().unwrap();
        assert_eq!(position.contracts, d(filled));
        assert_eq!(position.side, Side::Long);
        assert!(position
            .lots
            .iter()
            .all(|lot| lot.origin_spec_version == "gt03-spec-v1"
                && lot.source.entry == d("10")
                && lot.base_quantity == lot.source.contracts));
    }
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn gt03_partial_and_full_fill_cancel_traces_are_exact_and_repeatable() {
    for full in [false, true] {
        let run = || {
            let (mut owner, id) = golden();
            assert_golden(&owner, &id, 1, "0", "0", "10", "1000", "0", "100");
            let x1 = fill(&mut owner, &id, "X1", "4");
            assert_eq!(x1.fee_amount, d("0.4"));
            assert_golden(&owner, &id, 2, "4", "0", "6", "999.6", "0.4", "60");
            let request_input = request("C1", &[&id]);
            let c1 = owner.request_cancel(&request_input).unwrap();
            assert_golden(&owner, &id, 3, "4", "0", "6", "999.6", "0.4", "60");
            assert_eq!(c1.receipts.len(), 1);
            let r = &c1.receipts[0];
            assert_eq!(r.before, r.after);
            assert_eq!(r.reservation_before, r.reservation_after);
            assert_eq!(
                (
                    r.account_version_before,
                    r.account_version_after,
                    r.order_version_before,
                    r.order_version_after
                ),
                (2, 3, 2, 3)
            );
            assert_eq!(r.action_ids.len(), 2);
            assert_ne!(r.action_ids[0], r.action_ids[1]);
            let request_state = owner.orders[&id].cancel.clone();
            let x2 = fill(&mut owner, &id, "X2", if full { "6" } else { "3" });
            assert_eq!(owner.orders[&id].cancel, request_state);
            assert_eq!(owner.commit_sequence, 2);
            assert_eq!(x2.fee_amount, d(if full { "0.6" } else { "0.3" }));
            assert_golden(
                &owner,
                &id,
                4,
                if full { "10" } else { "7" },
                "0",
                if full { "0" } else { "3" },
                if full { "999" } else { "999.3" },
                if full { "1" } else { "0.7" },
                if full { "0" } else { "30" },
            );
            let effect_input = effect("C1-effective", "C1", &[&id]);
            let effective = owner.effect_cancel(&effect_input).unwrap();
            assert_golden(
                &owner,
                &id,
                5,
                if full { "10" } else { "7" },
                if full { "0" } else { "3" },
                "0",
                if full { "999" } else { "999.3" },
                if full { "1" } else { "0.7" },
                "0",
            );
            let receipt = &effective.receipts[0];
            assert_eq!(
                receipt.lifecycle_after,
                risk_transition::Lifecycle::RiskStable
            );
            assert_eq!(receipt.lifecycle_after, owner.transition.lifecycle);
            assert_eq!(receipt.episode_after, owner.transition.episode);
            assert_eq!(receipt.risk_after, None);
            assert_eq!(
                receipt.outcome,
                if full {
                    cancel::Outcome::EffectiveTooLate
                } else {
                    cancel::Outcome::EffectiveCanceled
                }
            );
            assert_eq!(
                (
                    receipt.account_version_before,
                    receipt.account_version_after,
                    receipt.order_version_before,
                    receipt.order_version_after
                ),
                (4, 5, 4, 5)
            );
            assert_eq!(receipt.action_id, r.action_ids[0]);
            assert_eq!(
                owner.orders[&id].facts.status,
                if full { "FILLED" } else { "CANCELED" }
            );
            assert_eq!(owner.cancel_facts.actions[&r.action_ids[1]].outcome, None);
            assert_eq!(owner.cancel_facts.actions[&r.action_ids[1]].phase, 2);
            let before = owner.clone();
            assert_eq!(owner.request_cancel(&request_input), Ok(c1));
            assert_eq!(owner.effect_cancel(&effect_input), Ok(effective));
            assert_eq!(owner, before);
            owner
        };
        assert_eq!(run(), run());
    }
}

#[test]
fn cancel_identity_terminal_and_invalid_input_matrix_fails_closed() {
    for mutation in [0, 1, 2, 3, 4, 5, 6, 7] {
        let (mut owner, id) = golden();
        let input = request("C1", &[&id]);
        let receipt = owner.request_cancel(&input).unwrap();
        let before = owner.clone();
        let mut changed = input.clone();
        match mutation {
            0 => changed.stamp.effective_at += 1,
            1 => changed.stamp.source_sequence = Some(1),
            2 => changed.stamp.causal_parent_ids.push("parent".into()),
            3 => changed.stamp.ordering_contract_id = "other".into(),
            4 => changed.stamp.scenario_ordinal += 1,
            5 => changed.targets[0].1 = Reason::Unsupported,
            6 => changed.targets.push(changed.targets[0].clone()),
            _ => changed.stamp.event_id = "C2".into(),
        }
        let error = if mutation == 7 {
            "CANCEL_ACTION_CONFLICT"
        } else {
            "EVENT_ID_CONFLICT"
        };
        assert_eq!(owner.request_cancel(&changed), Err(error));
        let mut expected = before;
        expected.gate = Gate::Failed(error);
        assert_eq!(owner, expected);
        assert_eq!(owner.request_cancel(&input), Ok(receipt));
        assert_eq!(owner, expected);
    }
    let (mut owner, id) = golden();
    let before = owner.clone();
    assert_eq!(
        owner.effect_cancel(&effect("E", "C", &[&id])),
        Err("CANCEL_EFFECT_BEFORE_REQUEST")
    );
    let mut expected = before;
    expected.gate = Gate::Failed("CANCEL_EFFECT_BEFORE_REQUEST");
    assert_eq!(owner, expected);
    for unknown_effect in [false, true] {
        let (mut owner, _) = golden();
        let result = if unknown_effect {
            owner.effect_cancel(&effect("E", "C", &["missing"]))
        } else {
            owner.request_cancel(&request("C", &["missing"]))
        };
        assert_eq!(result, Err("UNKNOWN_ORDER"));
    }
    let (mut owner, id) = golden();
    fill(&mut owner, &id, "X", "10");
    let before = owner.clone();
    let rejected = owner.request_cancel(&request("late", &[&id])).unwrap();
    assert_eq!(rejected.rejected, Some("CANCEL_REQUEST_TOO_LATE"));
    assert_eq!(
        (owner.state_version, owner.orders[&id].version, owner.cash),
        (
            before.state_version,
            before.orders[&id].version,
            before.cash
        )
    );
    assert_eq!(owner.orders[&id].cancel, cancel::State::None);
    assert_eq!(owner.request_cancel(&request("late", &[&id])), Ok(rejected));
}

fn btc_two_orders() -> ScenarioAccount {
    let (mut seed, config, marks) = fixture();
    seed.orders[0].reduce_only = false;
    let mut second = seed.orders[0].clone();
    second.order_id = "O2".into();
    second.intent_id = "I2".into();
    second.client_id = "C2".into();
    seed.orders.push(second);
    ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
}

#[test]
fn terminal_reduce_only_orders_fail_before_flat_position_role_rejection() {
    for side in [Side::Long, Side::Short] {
        for target in ["O1", "O2"] {
            let (mut seed, config, marks) = fixture();
            seed.positions[0].side = side;
            seed.positions[0].contracts = d("1");
            seed.positions[0].lots.truncate(1);
            seed.orders[0].side = if side == Side::Long {
                Side::Short
            } else {
                Side::Long
            };
            let mut second = seed.orders[0].clone();
            second.order_id = "O2".into();
            second.intent_id = "I2".into();
            second.client_id = "C2".into();
            seed.orders.push(second);
            let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
            owner.request_cancel(&request("C", &["O1", "O2"])).unwrap();
            owner
                .effect_cancel(&effect("cancel-second", "C", &["O2"]))
                .unwrap();
            let closing = candidate(&owner, "O1", "full-close", "1");
            let Reply::Committed {
                receipt,
                terminal_reason: None,
            } = owner.execute(&closing).unwrap()
            else {
                panic!("full close required");
            };
            let mut late_input = effect("late-first", "C", &["O1"]);
            late_input.stamp.effective_at = closing.template.matching_effective_at + 1;
            let late = owner.effect_cancel(&late_input).unwrap();
            assert_eq!(late.receipts[0].outcome, cancel::Outcome::EffectiveTooLate);
            assert!(owner.positions.is_empty());
            assert_eq!(
                (
                    owner.state_version,
                    owner.orders["O1"].version,
                    owner.orders["O2"].version
                ),
                (4, 3, 2)
            );
            assert_eq!(owner.orders["O1"].facts.status, "FILLED");
            assert_eq!(owner.orders["O2"].facts.status, "CANCELED");
            assert_eq!(
                owner.cash,
                d(if side == Side::Long {
                    "9999.500999"
                } else {
                    "9999.498999"
                })
            );
            let before = owner.clone();
            assert_eq!(
                owner.execute(&closing),
                Ok(Reply::Duplicate(receipt.clone()))
            );
            assert_eq!(owner, before);
            let fresh = candidate(&owner, target, "fresh-terminal", "1");
            assert_eq!(owner.execute(&fresh), Err("UNSUPPORTED_EXECUTION"));
            let mut expected = before;
            expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
            assert_eq!(owner, expected);
            assert_eq!(owner.execute(&closing), Ok(Reply::Duplicate(receipt)));
            assert_eq!(owner, expected);
        }
    }
}

#[test]
fn effect_batch_order_and_shape_are_canonical_before_first_mutation() {
    let mut owner = btc_two_orders();
    owner.request_cancel(&request("C", &["O2", "O1"])).unwrap();
    let input = effect("E", "C", &["O2", "O1"]);
    let before = owner.clone();
    for field in 0..3 {
        let mut owner = before.clone();
        let mut bad = input.clone();
        match field {
            0 => bad.effects.clear(),
            1 => bad.effects.push(bad.effects[0].clone()),
            _ => bad.stamp.scenario_ordinal = 0,
        }
        assert_eq!(owner.effect_cancel(&bad), Err("INVALID_SCENARIO_GROUP"));
        let mut expected = before.clone();
        expected.gate = Gate::Failed("INVALID_SCENARIO_GROUP");
        assert_eq!(owner, expected);
    }
    let result = owner.effect_cancel(&input).unwrap();
    assert_eq!(
        result
            .receipts
            .iter()
            .map(|r| r.target_order_id.as_str())
            .collect::<Vec<_>>(),
        ["O1", "O2"]
    );
    assert_eq!(
        (
            owner.state_version,
            owner.orders["O1"].version,
            owner.orders["O2"].version
        ),
        (3, 2, 2)
    );
    assert!(owner.orders.values().all(|o| o.facts.filled == d("1")
        && o.facts.canceled == d("1")
        && o.facts.remaining == Decimal::ZERO));
    let mut reordered = input;
    reordered.effects.reverse();
    let before = owner.clone();
    assert_eq!(owner.effect_cancel(&reordered), Ok(result));
    assert_eq!(owner, before);
    // CANCELED + NONE is unreachable in 4C-1: clean seeds cannot be canceled,
    // and the only effective writer requires and retains a canonical request.
}

#[test]
fn cancel_architecture_remains_private_single_owner_and_single_settlement() {
    let cancel = include_str!("../../risk_transition/cancel.rs");
    let identity = include_str!("../../risk_transition/cancel/identity.rs");
    let context = include_str!("../../hypothetical_settlement/context.rs");
    let profile = include_str!("../../golden_cancel.rs");
    for source in [cancel, identity, context, profile] {
        for forbidden in [
            "pub fn ",
            "#[py",
            "tokio::",
            "std::fs",
            "reqwest",
            "std::thread",
            "struct ScenarioAccount",
        ] {
            assert!(!source.contains(forbidden), "{forbidden}");
        }
    }
    assert_eq!(identity.matches("fn classify<").count(), 1);
    assert!(!cancel.contains("fee_amount("));
    assert!(!profile.contains("fee_amount("));
    assert!(include_str!("../commit.rs")
        .contains("hypothetical_settlement::Context::GoldenCancel(config)"));
}

#[test]
fn effect_identity_and_terminal_execution_preserve_first_failure_and_original_receipts() {
    for changed_field in 0..7 {
        let (mut owner, id) = golden();
        let request_input = request("C", &[&id]);
        let requested = owner.request_cancel(&request_input).unwrap();
        let input = effect("E", "C", &[&id]);
        let committed = owner.effect_cancel(&input).unwrap();
        let mut changed = input.clone();
        match changed_field {
            0 => changed.stamp.effective_at += 1,
            1 => changed.stamp.source_sequence = Some(1),
            2 => changed.stamp.causal_parent_ids.push("parent".into()),
            3 => changed.stamp.scenario_ordinal += 1,
            4 => changed.effects[0].2 = Reason::Unsupported,
            5 => {
                changed.stamp.event_id = "E2".into();
                changed.stamp.effective_at += 1;
            }
            _ => {
                changed.stamp.event_id = "E2".into();
                changed.effects[0].0 = "other-request".into();
            }
        }
        let error = if changed_field < 5 {
            "EVENT_ID_CONFLICT"
        } else {
            "CANCEL_ACTION_CONFLICT"
        };
        let before = owner.clone();
        assert_eq!(owner.effect_cancel(&changed), Err(error));
        let mut expected = before;
        expected.gate = Gate::Failed(error);
        assert_eq!(owner, expected);
        assert_eq!(owner.effect_cancel(&input), Ok(committed.clone()));
        assert_eq!(owner.request_cancel(&request_input), Ok(requested));
        assert_eq!(owner, expected);
        let mut alias = input.clone();
        alias.stamp.event_id = "alias".into();
        let duplicated = owner.effect_cancel(&alias).unwrap();
        assert_eq!(duplicated.receipts, committed.receipts);
        assert_eq!(
            (owner.state_version, owner.cash, owner.orders[&id].clone()),
            (
                expected.state_version,
                expected.cash,
                expected.orders[&id].clone()
            )
        );
        assert_eq!(owner.gate, Gate::Failed(error));
    }
    let (mut owner, id) = golden();
    owner.request_cancel(&request("C", &[&id])).unwrap();
    owner.effect_cancel(&effect("E", "C", &[&id])).unwrap();
    let new_execution = candidate(&owner, &id, "after-cancel", "1");
    let before = owner.clone();
    assert_eq!(owner.execute(&new_execution), Err("UNSUPPORTED_EXECUTION"));
    let mut expected = before;
    expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
    assert_eq!(owner, expected);
    assert_eq!(
        owner.request_cancel(&request("new-request", &[&id])),
        Err("CANCEL_ACTION_CONFLICT")
    );
    assert_eq!(owner.gate, Gate::Failed("UNSUPPORTED_EXECUTION"));
}

#[test]
fn malformed_batches_and_effect_prepublication_interruptions_never_publish_money() {
    for field in 0..8 {
        let (mut owner, id) = golden();
        let mut input = request("C", &[&id]);
        match field {
            0 => input.targets.clear(),
            1 => input.targets.push(input.targets[0].clone()),
            2 => input.stamp.event_id.clear(),
            3 => input.stamp.effective_at = -1,
            4 => input.stamp.scenario_ordinal = 0,
            5 => input.stamp.ordering_contract_id = "unknown".into(),
            6 => input.targets[0].1 = Reason::Unsupported,
            _ => input.stamp.causal_parent_ids = vec!["P".into(), "P".into()],
        }
        let before = owner.clone();
        assert_eq!(owner.request_cancel(&input), Err("INVALID_SCENARIO_GROUP"));
        let mut expected = before;
        expected.gate = Gate::Failed("INVALID_SCENARIO_GROUP");
        assert_eq!(owner, expected);
    }
    for stage in [
        cancel::Stage::Prepared,
        cancel::Stage::ActionDrafted(0),
        cancel::Stage::BeforeSwap(0),
    ] {
        for panic in [false, true] {
            let (mut owner, id) = golden();
            owner.request_cancel(&request("C", &[&id])).unwrap();
            let before = owner.clone();
            let result = owner.effect_cancel_checked(&effect("E", "C", &[&id]), |at| {
                if at == stage {
                    if panic {
                        panic!("injected");
                    }
                    return Err("INJECTED");
                }
                Ok(())
            });
            let error = if panic { "CANCEL_PANIC" } else { "INJECTED" };
            if stage == cancel::Stage::Prepared {
                assert_eq!(result, Err(error));
            } else {
                let result = result.unwrap();
                assert_eq!(result.failure, Some(error));
                assert!(result.receipts.is_empty());
            }
            assert_eq!(
                (
                    owner.state_version,
                    owner.cash,
                    owner.fees,
                    owner.positions.clone(),
                    owner.orders.clone(),
                    owner.cancel_facts.actions.clone()
                ),
                (
                    before.state_version,
                    before.cash,
                    before.fees,
                    before.positions,
                    before.orders,
                    before.cancel_facts.actions
                )
            );
            assert_eq!(owner.gate, Gate::Failed(error));
        }
    }
}

#[test]
fn context_coverage_subaccount_and_profile_isolation_are_explicit() {
    let (seed, _, _) = fixture();
    let mut clean = seed.clone();
    clean.cash = d("1000");
    clean.positions.clear();
    clean.orders.clear();
    let run = |subaccount: Option<&str>| {
        let mut seed = clean.clone();
        seed.key.subaccount = subaccount.map(str::to_owned);
        let mut owner = ScenarioAccount::from_golden_cancel_seed(&seed, &Config::frozen()).unwrap();
        let id = admission::admit_execution_fixture(&mut owner, Side::Long, d("10"), d("10"));
        owner.request_cancel(&request("C", &[&id])).unwrap()
    };
    assert_ne!(
        run(None).receipts[0].action_id,
        run(Some("sub")).receipts[0].action_id
    );
    for at in [-1, 1000] {
        clean.effective_at = at;
        assert!(ScenarioAccount::from_golden_cancel_seed(&clean, &Config::frozen()).is_err());
    }
    let (mut owner, id) = golden();
    let mut input = request("C", &[&id]);
    input.stamp.effective_at = 1000;
    assert_eq!(
        owner.request_cancel(&input),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    let (mut owner, id) = golden();
    let mut c = candidate(&owner, &id, "wrong-profile", "1");
    c.template.key.product = ProfileProduct::BtcEth(Product::Btc);
    assert_eq!(owner.execute(&c), Err("UNSUPPORTED_EXECUTION"));
}

#[test]
fn request_batch_is_atomic_and_effect_fault_retains_prior_committed_fill_and_effect() {
    for stage in [
        cancel::Stage::Prepared,
        cancel::Stage::ActionDrafted(0),
        cancel::Stage::ActionDrafted(1),
        cancel::Stage::BeforeSwap(0),
    ] {
        for panic in [false, true] {
            let mut owner = btc_two_orders();
            let before = owner.clone();
            let result = owner.request_cancel_checked(&request("C1", &["O2", "O1"]), |at| {
                if at == stage {
                    if panic {
                        panic!("injected");
                    }
                    return Err("INJECTED");
                }
                Ok(())
            });
            let reason = if panic { "CANCEL_PANIC" } else { "INJECTED" };
            assert_eq!(result, Err(reason));
            let mut expected = before;
            expected.gate = Gate::Failed(reason);
            assert_eq!(owner, expected);
        }
    }
    for stage in [
        cancel::Stage::ActionDrafted(1),
        cancel::Stage::BeforeSwap(1),
    ] {
        for panic in [false, true] {
            let mut owner = btc_two_orders();
            let execution = fill(&mut owner, "O1", "retained", "0.5");
            let financial = (
                owner.cash,
                owner.fees,
                owner.positions.clone(),
                owner.commit_sequence,
            );
            assert_eq!(
                (owner.cash, owner.fees, owner.gross_realized),
                (d("9999.7504995"), d("0.2500005"), d("0.0005"))
            );
            let request_input = request("C1", &["O2", "O1"]);
            let requested = owner.request_cancel(&request_input).unwrap();
            assert_eq!(
                requested
                    .receipts
                    .iter()
                    .map(|r| r.target_order_id.as_str())
                    .collect::<Vec<_>>(),
                ["O1", "O2"]
            );
            let mut permuted = request_input.clone();
            permuted.targets.reverse();
            assert_eq!(owner.request_cancel(&permuted), Ok(requested));
            let input = effect("E", "C1", &["O2", "O1"]);
            let batch = owner
                .effect_cancel_checked(&input, |at| {
                    if at == stage {
                        if panic {
                            panic!("injected");
                        }
                        return Err("INJECTED");
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                batch.failure,
                Some(if panic { "CANCEL_PANIC" } else { "INJECTED" })
            );
            assert_eq!(batch.receipts.len(), 1);
            assert_eq!(owner.orders["O1"].facts.canceled, d("0.5"));
            assert_eq!(owner.orders["O2"].facts.remaining, d("1"));
            let snapshot = owner.reservation().unwrap();
            assert_eq!(
                (
                    snapshot.total_fee_hold,
                    snapshot.total_order_loss,
                    snapshot.used_margin,
                    snapshot.available_margin
                ),
                (
                    d("0.500001"),
                    d("0.009"),
                    d("125.511501"),
                    // Cash 9999.7504995 + FIFO UPL (.005 + .018) - used margin.
                    d("9874.2619985")
                )
            );
            assert_eq!(
                (
                    owner.state_version,
                    owner.orders["O1"].version,
                    owner.orders["O2"].version
                ),
                (3, 3, 1)
            );
            assert_eq!(
                (
                    owner.cash,
                    owner.fees,
                    owner.positions.clone(),
                    owner.commit_sequence
                ),
                financial
            );
            assert_eq!(owner.execution_receipts[&execution.execution_id], execution);
            let before = owner.clone();
            assert_eq!(owner.effect_cancel(&input), Ok(batch));
            assert_eq!(owner, before);
        }
    }
}
