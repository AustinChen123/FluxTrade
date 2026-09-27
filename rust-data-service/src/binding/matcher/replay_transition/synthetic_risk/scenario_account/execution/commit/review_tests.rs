//! Causal regressions for shared source admission and terminal stop ordering.
use super::super::super::group::{Group, Input as GroupInput, Member};
use super::super::super::tests::{d, fixture};
use super::tests::input;
use super::*;
use risk_transition::{cancel, Lifecycle};

fn group(owner: &ScenarioAccount, members: Vec<Member>) -> Group {
    Group {
        group_id: "review-group".into(),
        account_key: owner.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: 500,
        declared_member_count: members.len(),
        members,
    }
}

#[test]
fn group_mixed_account_children_fail_before_valid_prior_member() {
    for execution in [false, true] {
        let (seed, config, mut marks) = fixture();
        for mark in &mut marks {
            mark.valid_to = 500;
        }
        let next: Vec<_> = marks
            .iter()
            .map(|m| Mark {
                valid_from: 500,
                valid_to: 3000,
                ..m.clone()
            })
            .collect();
        marks.extend(next.clone());
        let original = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let context = context::Input {
            account_key: original.key.clone(),
            stamp: source::stamp("prior", 500, 20),
            expected_before: original.valuation_context_id,
            expected_after: context_id(&config, &marks, 500).unwrap(),
            rows: context::Rows::Marks(
                next.iter()
                    .map(|m| (m.product, m.valid_from, m.valid_to, m.price))
                    .collect(),
            ),
        };
        let child = if execution {
            let mut child = input(&original, "O1", "foreign", "0.5", "50000.1");
            child.template.key.account.account = "foreign".into();
            child.template.matching_effective_at = 500;
            Member {
                stamp: source::stamp(&child.event_id, 500, 30),
                input: GroupInput::Execution(child),
            }
        } else {
            let mut foreign = original.clone();
            foreign.key.account = "foreign".into();
            let child = admission::fixture_intent(
                &foreign,
                "foreign-intent",
                Side::Long,
                d("1"),
                d("50000"),
            );
            Member {
                stamp: source::stamp("intent", 500, 60),
                input: GroupInput::Intent(child),
            }
        };
        let group = group(
            &original,
            vec![
                Member {
                    stamp: context.stamp.clone(),
                    input: GroupInput::Context(context),
                },
                child,
            ],
        );
        let mut owner = original.clone();
        assert_eq!(owner.apply_group(&group), Err("ACCOUNT_MISMATCH"));
        let mut expected = original;
        expected.gate = Gate::Failed("ACCOUNT_MISMATCH");
        assert_eq!(owner, expected);
    }
}

#[test]
fn wrapper_event_identity_conflicts_after_success_and_partial_group_failure() {
    for partial in [false, true] {
        let mut owner = closing_owner();
        let intent = admission::fixture_intent(
            &owner,
            "group-intent",
            Side::Long,
            if partial { Decimal::ZERO } else { d("0.01") },
            d("50000"),
        );
        let request = cancel::RequestInput {
            stamp: source::stamp("request", 500, 40),
            targets: vec![("O1".into(), cancel::Reason::ExplicitScenario)],
        };
        let original = group(
            &owner,
            vec![
                Member {
                    stamp: request.stamp.clone(),
                    input: GroupInput::Request(request),
                },
                Member {
                    stamp: source::stamp("intent-wrapper", 500, 60),
                    input: GroupInput::Intent(intent),
                },
            ],
        );
        let completion = owner.apply_group(&original).unwrap();
        assert_eq!(completion.failure.is_some(), partial);
        assert_eq!(
            completion.committed[0],
            group::Reference::Source("request".into())
        );
        let before = owner.clone();
        assert_eq!(owner.apply_group(&original), Ok(completion));
        assert_eq!(owner, before);
        let mut changed = original;
        changed.members[1].stamp.event_id = "changed-wrapper".into();
        assert_eq!(owner.apply_group(&changed), Err("EVENT_ID_CONFLICT"));
        let mut expected = before;
        if !partial {
            expected.gate = Gate::Failed("EVENT_ID_CONFLICT");
        }
        assert_eq!(owner, expected);
    }
}

#[test]
fn every_direct_source_entry_rejects_cross_kind_reuse_and_stale_clock_atomically() {
    for kind in 0..5 {
        for reused in [false, true] {
            let mut owner = closing_owner();
            let request = cancel::RequestInput {
                stamp: source::stamp("accepted-source", 500, 40),
                targets: vec![("O1".into(), cancel::Reason::ExplicitScenario)],
            };
            owner.request_cancel(&request).unwrap();
            // For the request entry, use a different committed source kind.
            if kind == 3 {
                let intent = admission::fixture_intent(
                    &owner,
                    "first-intent",
                    Side::Long,
                    d("0.01"),
                    d("50000"),
                );
                admission::fixture_admit(&mut owner, "intent-source", 501, &intent).unwrap();
            }
            let id = if reused {
                if kind == 3 {
                    "intent-source"
                } else {
                    "accepted-source"
                }
            } else {
                "stale-source"
            };
            let at = if reused { 600 } else { 499 };
            let before = owner.clone();
            let result = match kind {
                0 => {
                    let candidate = context::Input {
                        account_key: owner.key.clone(),
                        stamp: source::stamp(id, at, 20),
                        expected_before: owner.valuation_context_id,
                        expected_after: owner.valuation_context_id,
                        rows: context::Rows::Marks(vec![]),
                    };
                    owner.activate_context(&candidate).map(|_| ())
                }
                1 => {
                    let mut candidate = input(&owner, "O1", "fresh", "0.5", "1");
                    candidate.event_id = id.into();
                    candidate.template.matching_effective_at = at;
                    owner.execute(&candidate).map(|_| ())
                }
                2 => {
                    let intent = admission::fixture_intent(
                        &owner,
                        "fresh-intent",
                        Side::Long,
                        d("0.01"),
                        d("50000"),
                    );
                    admission::fixture_admit(&mut owner, id, at, &intent).map(|_| ())
                }
                3 => owner
                    .request_cancel(&cancel::RequestInput {
                        stamp: source::stamp(id, at, 40),
                        targets: vec![("unknown".into(), cancel::Reason::ExplicitScenario)],
                    })
                    .map(|_| ()),
                _ => owner
                    .effect_cancel(&cancel::EffectInput {
                        stamp: source::stamp(id, at, 50),
                        effects: vec![(
                            "accepted-source".into(),
                            "O1".into(),
                            cancel::Reason::ExplicitScenario,
                        )],
                    })
                    .map(|_| ()),
            };
            let reason = if reused {
                "EVENT_ID_CONFLICT"
            } else {
                "STALE_EVENT"
            };
            assert_eq!(result, Err(reason), "kind={kind}, reused={reused}");
            let mut expected = before;
            expected.gate = Gate::Failed(reason);
            assert_eq!(owner, expected);
        }
    }
}

fn closing_owner() -> ScenarioAccount {
    let (mut seed, config, mut marks) = fixture();
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    seed.cash = d("100");
    seed.orders[0].price = d("1");
    marks[0].price = d("50000");
    ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
}

#[test]
fn rejected_members_keep_original_outcome_across_group_identities() {
    for cancel_request in [false, true] {
        let (mut seed, config, marks) = fixture();
        seed.positions[0].contracts = d("1");
        seed.positions[0].lots.truncate(1);
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let (member, reason) = if cancel_request {
            let close = input(&owner, "O1", "full-close", "1", "50000.1");
            owner.execute(&close).unwrap();
            let request = cancel::RequestInput {
                stamp: source::stamp("too-late", 500, 40),
                targets: vec![("O1".into(), cancel::Reason::ExplicitScenario)],
            };
            (
                Member {
                    stamp: request.stamp.clone(),
                    input: GroupInput::Request(request),
                },
                "CANCEL_REQUEST_TOO_LATE",
            )
        } else {
            let intent = admission::fixture_intent(
                &owner,
                "rejected-intent",
                Side::Short,
                d("10000"),
                d("50000"),
            );
            (
                Member {
                    stamp: source::stamp("rejected-intent-source", 500, 60),
                    input: GroupInput::Intent(intent),
                },
                "POSITION_CROSS_ZERO_UNSUPPORTED",
            )
        };
        let original = group(&owner, vec![member]);
        let first = owner.apply_group(&original).unwrap();
        assert!(first.committed.is_empty());
        assert_eq!(
            first.rejections,
            vec![(original.members[0].stamp.event_id.clone(), reason)]
        );
        let before = owner.clone();
        let mut replay = original.clone();
        replay.group_id = "another-group".into();
        let repeated = owner.apply_group(&replay).unwrap();
        assert_eq!(repeated.rejections, first.rejections);
        assert!(repeated.committed.is_empty());
        assert_eq!(repeated.failure, None);
        let mut expected = before;
        // Only the newly submitted group envelope may be recorded.
        expected.transition.admissions = owner.transition.admissions.clone();
        expected.transition.groups = owner.transition.groups.clone();
        assert_eq!(owner, expected);
        assert_eq!(owner.apply_group(&original).unwrap(), first);
        assert_eq!(owner.apply_group(&replay).unwrap(), repeated);
        assert_eq!(owner, expected);
    }
}

#[test]
fn flat_negative_close_retains_resting_order_without_automatic_cancel() {
    let mut owner = closing_owner();
    let resting =
        admission::admit_execution_fixture(&mut owner, Side::Short, d("0.01"), d("51000"));
    let before = owner.clone();
    let close = input(&owner, "O1", "close", "1", "1");
    assert!(matches!(
        owner.execute(&close),
        Ok(Reply::Committed {
            terminal_reason: None,
            ..
        })
    ));
    assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
    assert_eq!(owner.gate, Gate::Running);
    assert!(owner.positions.is_empty());
    assert_eq!(owner.cash, d("-399.99001"));
    assert_eq!(owner.state_version, before.state_version + 1);
    assert_eq!(owner.commit_sequence, before.commit_sequence + 1);
    assert_eq!(owner.orders[&resting], before.orders[&resting]);
    assert_eq!(owner.cancel_facts, before.cancel_facts);
    assert_eq!(owner.pending_actions, before.pending_actions);
    assert_eq!(owner.execution_receipts.len(), 1);
}

#[test]
fn admission_duplicate_and_conflict_precede_failed_and_causal_terminal() {
    for accepted in [false, true] {
        for terminal in [false, true] {
            let mut owner = closing_owner();
            let quantity = if accepted { "0.01" } else { "10000" };
            let price = "51000";
            let intent =
                admission::fixture_intent(&owner, "retry", Side::Short, d(quantity), d(price));
            let stamp = source::stamp("admit", 500, 60);
            let original =
                admission::fixture_admit(&mut owner, &stamp.event_id, 500, &intent).unwrap();
            assert_eq!(original.0, if accepted { "accepted" } else { "rejected" });
            if terminal {
                let close = input(&owner, "O1", "close", "1", "1");
                owner.execute(&close).unwrap();
                assert_eq!(owner.transition.lifecycle, Lifecycle::LiquidatedInsolvent);
            } else {
                let mut invalid = input(&owner, "O1", "invalid", "1", "1");
                invalid.template.quantity = Decimal::ZERO;
                assert_eq!(owner.execute(&invalid), Err("UNSUPPORTED_EXECUTION"));
            }
            let before = owner.clone();
            let retry =
                admission::fixture_admit(&mut owner, "different-wrapper", 999, &intent).unwrap();
            assert_eq!(retry.0, "duplicate");
            assert_eq!(retry.1, original.1);
            assert_eq!(owner, before);
            let changed = admission::fixture_intent(
                &owner,
                "retry",
                Side::Short,
                d(quantity) + Decimal::ONE,
                d(price),
            );
            assert_eq!(
                admission::fixture_admit(&mut owner, "conflict", 999, &changed),
                Err("IDEMPOTENCY_KEY_CONFLICT")
            );
            let mut expected = before;
            if terminal {
                expected.gate = Gate::Failed("IDEMPOTENCY_KEY_CONFLICT");
            }
            assert_eq!(owner, expected);
        }
    }
}

#[test]
fn direct_new_inputs_after_causal_terminal_do_not_publish_any_fact() {
    let mut owner = closing_owner();
    let close = input(&owner, "O1", "close", "1", "1");
    owner.execute(&close).unwrap();
    let before = owner.clone();
    let intent = admission::fixture_intent(&owner, "new", Side::Long, d("1"), d("1"));
    assert_eq!(
        admission::fixture_admit(&mut owner, "new-intent", 600, &intent),
        Err("RUN_TERMINAL")
    );
    let request = cancel::RequestInput {
        stamp: source::stamp("new-request", 600, 40),
        targets: vec![("O1".into(), cancel::Reason::ExplicitScenario)],
    };
    assert_eq!(owner.request_cancel(&request), Err("RUN_TERMINAL"));
    let effect = cancel::EffectInput {
        stamp: source::stamp("new-effect", 600, 50),
        effects: vec![(
            "absent".into(),
            "O1".into(),
            cancel::Reason::ExplicitScenario,
        )],
    };
    assert_eq!(owner.effect_cancel(&effect), Err("RUN_TERMINAL"));
    let next = input(&owner, "O1", "next", "1", "1");
    assert_eq!(owner.execute(&next), Ok(Reply::Rejected("RUN_TERMINAL")));
    let context = context::Input {
        account_key: owner.key.clone(),
        stamp: source::stamp("new-context", 600, 20),
        expected_before: owner.valuation_context_id,
        expected_after: owner.valuation_context_id,
        rows: context::Rows::Marks(vec![]),
    };
    assert_eq!(owner.activate_context(&context), Err("RUN_TERMINAL"));
    assert_eq!(owner, before);
}

#[test]
fn terminal_source_conflicts_precede_terminal_and_retain_first_failure() {
    let mut original = closing_owner();
    let close = input(&original, "O1", "terminal-close", "1", "1");
    original.execute(&close).unwrap();
    assert_eq!(
        original.transition.lifecycle,
        Lifecycle::LiquidatedInsolvent
    );
    for context_input in [false, true] {
        let mut owner = original.clone();
        let result = if context_input {
            owner
                .activate_context(&context::Input {
                    account_key: owner.key.clone(),
                    stamp: source::stamp(&close.event_id, 600, 20),
                    expected_before: owner.valuation_context_id,
                    expected_after: owner.valuation_context_id,
                    rows: context::Rows::Marks(vec![]),
                })
                .map(|_| ())
        } else {
            let intent = admission::fixture_intent(&owner, "fresh", Side::Long, d("1"), d("1"));
            admission::fixture_admit(&mut owner, &close.event_id, 600, &intent).map(|_| ())
        };
        assert_eq!(result, Err("EVENT_ID_CONFLICT"));
        let mut expected = original.clone();
        expected.gate = Gate::Failed("EVENT_ID_CONFLICT");
        assert_eq!(owner, expected);
        let mut conflicting_execution = close.clone();
        conflicting_execution.template.quantity = d("0.5");
        assert_eq!(
            owner.execute(&conflicting_execution),
            Err("EXECUTION_ID_CONFLICT")
        );
        assert_eq!(owner, expected);
    }
}

#[test]
fn terminal_group_envelope_has_no_publication_but_historical_identity_wins() {
    let mut owner = closing_owner();
    let intent =
        admission::fixture_intent(&owner, "historical", Side::Short, d("10000"), d("50000"));
    let original = group(
        &owner,
        vec![Member {
            stamp: source::stamp("historical-source", 500, 60),
            input: GroupInput::Intent(intent),
        }],
    );
    let completion = owner.apply_group(&original).unwrap();
    let mut close = input(&owner, "O1", "terminal-close", "1", "1");
    close.template.matching_effective_at = 501;
    owner.execute(&close).unwrap();
    let terminal = owner.clone();
    assert_eq!(owner.apply_group(&original), Ok(completion));
    assert_eq!(owner, terminal);
    let mut fresh = original.clone();
    fresh.group_id = "new-group".into();
    assert_eq!(owner.apply_group(&fresh), Err("RUN_TERMINAL"));
    assert_eq!(owner, terminal);
    fresh.group_effective_at = 600;
    fresh.members = vec![Member {
        stamp: source::stamp("fresh-terminal-source", 600, 60),
        input: GroupInput::Intent(admission::fixture_intent(
            &owner,
            "fresh-terminal",
            Side::Long,
            d("1"),
            d("1"),
        )),
    }];
    assert_eq!(owner.apply_group(&fresh), Err("RUN_TERMINAL"));
    assert_eq!(owner, terminal);
    for member_conflict in [false, true] {
        let mut owner = terminal.clone();
        let mut conflict = original.clone();
        if member_conflict {
            conflict.group_id = "new-conflicting-group".into();
            conflict.members[0].stamp.event_id = close.event_id.clone();
        } else {
            conflict.members[0].stamp.event_id = "changed-wrapper".into();
        }
        assert_eq!(owner.apply_group(&conflict), Err("EVENT_ID_CONFLICT"));
        let mut expected = terminal.clone();
        expected.gate = Gate::Failed("EVENT_ID_CONFLICT");
        assert_eq!(owner, expected);
    }
}

#[test]
fn orphaned_source_without_domain_receipt_cannot_be_accepted_again() {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let execution = input(&owner, "O1", "partial", "0.5", "50000.1");
    owner.execute(&execution).unwrap();
    // Corrupt only provenance; a matching source is not a replacement receipt.
    owner.execution_receipts.clear();
    let mut expected = owner.clone();
    expected.gate = Gate::Failed("EVENT_ID_CONFLICT");
    assert_eq!(owner.execute(&execution), Err("EVENT_ID_CONFLICT"));
    assert_eq!(owner, expected);
}
