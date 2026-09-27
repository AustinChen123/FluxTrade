use super::super::super::tests::{d, fixture};
use super::*;
use event_limit::Program;

fn owner(program: Program) -> ScenarioAccount {
    ScenarioAccount::from_event_limit_seed(
        &event_limit::tests::seed(),
        &event_limit::Config::frozen(program),
    )
    .unwrap()
}

fn fact(reply: &Reply) -> &CommittedExecution {
    let Reply::Committed {
        receipt,
        terminal_reason: None,
    } = reply
    else {
        panic!("stable committed fact required");
    };
    receipt
}

fn snapshot(
    value: &FinancialSnapshot,
    position: &str,
    remaining: &str,
    used: &str,
    available: &str,
) {
    let FinancialSnapshot::EventLimit(value) = value else {
        panic!("not BTC financial evidence");
    };
    event_limit::tests::assert_snapshot(value, position, remaining, used, available);
}

#[test]
fn c09_v1_v2_exact_sequential_receipts_and_repeatability() {
    let mut common_templates = None;
    let mut contexts = BTreeSet::new();
    for (program, count) in [(Program::V1, 1), (Program::V2, 2)] {
        let mut previous = None;
        for _ in 0..2 {
            let mut account = owner(program);
            let templates = frozen_templates(&account);
            if let Some(expected) = &common_templates {
                assert_eq!(&templates, expected);
            }
            common_templates = Some(templates.clone());
            contexts.insert(account.valuation_context_id);
            let replies = account.run_event_c(&templates).unwrap();
            assert_eq!(replies.len(), 2);
            let first = fact(&replies[0]);
            assert_eq!(
                (
                    first.state_version_before,
                    first.state_version_after,
                    first.order_version_before,
                    first.order_version_after,
                    first.commit_sequence
                ),
                (0, 1, 0, 1, 0)
            );
            snapshot(&first.reservation_before, "0", "20", "20", "980");
            snapshot(&first.reservation_after, "100", "10", "110", "890");
            if count == 1 {
                assert_eq!(replies[1], Reply::Rejected("EVENT_EXECUTION_LIMIT"));
            } else {
                let second = fact(&replies[1]);
                assert_eq!(
                    (
                        second.state_version_before,
                        second.state_version_after,
                        second.order_version_before,
                        second.order_version_after,
                        second.commit_sequence
                    ),
                    (1, 2, 0, 1, 1)
                );
                assert_eq!(second.position_before, first.position_after);
                assert_eq!(second.reservation_before, first.reservation_after);
                snapshot(&second.reservation_after, "200", "0", "200", "800");
            }
            assert_eq!(
                (account.cash, account.gross_realized, account.fees),
                (d("1000"), d("0"), d("0"))
            );
            assert_eq!(
                (account.state_version, account.commit_sequence),
                (count, count)
            );
            assert_eq!(account.execution_receipts.len(), count as usize);
            assert!(account.intent_results.is_empty() && account.pending_actions.is_empty());
            let PositionState::EventLimit(Some(position)) = &account.positions else {
                panic!("one P_A store required");
            };
            assert_eq!(
                (position.side, position.contracts, position.entry_basis),
                (Side::Long, Decimal::from(count), Decimal::from(count * 10))
            );
            for (index, lot) in position.lots.iter().enumerate() {
                let receipt = fact(&replies[index]);
                assert_eq!(lot.execution_id, templates[index].key.canonical_id());
                assert_eq!(
                    lot.source.seed_execution_id,
                    templates[index].key.external_id
                );
                assert_eq!(lot.source.seed_sequence, index as u64);
                assert_eq!(
                    (lot.source.contracts, lot.base_quantity, lot.source.entry),
                    (d("1"), d("1"), d("10"))
                );
                assert_eq!(
                    (receipt.fee_amount, receipt.realized_pnl_delta),
                    (d("0"), d("0"))
                );
                assert_eq!(receipt.cash_deltas, vec![("USDT".into(), d("0"))]);
                assert_eq!(receipt.risk_state_after, ProfileRisk::CapacitySafe);
                assert_eq!(receipt.event_id, "C");
                assert!(receipt.pending_action_ids.is_empty());
                assert_eq!(&account.execution_receipts[&receipt.execution_id], receipt);
            }
            for (index, id) in ["O_A", "O_B"].iter().enumerate() {
                let order = &account.orders[*id];
                let filled = u64::from(index < count as usize);
                assert_eq!(
                    (order.version, order.facts.filled, order.facts.remaining),
                    (filled, Decimal::from(filled), Decimal::from(1 - filled))
                );
                assert_eq!(
                    order.facts.status,
                    if filled == 1 { "FILLED" } else { "OPEN" }
                );
            }
            let before = account.clone();
            let repeated = account.run_event_c(&templates).unwrap();
            assert_eq!(repeated[0], Reply::Duplicate(first.clone()));
            assert_eq!(
                repeated[1],
                if count == 1 {
                    Reply::Rejected("EVENT_EXECUTION_LIMIT")
                } else {
                    Reply::Duplicate(fact(&replies[1]).clone())
                }
            );
            assert_eq!(account, before);
            if count == 1 {
                let mut another_event = account.clone();
                let mut candidate = another_event.event_c_candidate(&templates[1], 1).unwrap();
                candidate.event_id = "D".into();
                let mut expected = account.clone();
                expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
                assert_eq!(
                    another_event.execute(&candidate),
                    Err("UNSUPPORTED_EXECUTION")
                );
                assert_eq!(another_event, expected);
            }
            if let Some(expected) = &previous {
                assert_eq!(&(account.clone(), replies.clone()), expected);
            }
            previous = Some((account, replies));
        }
    }
    assert_eq!(contexts.len(), 2);
}

#[test]
fn stale_batch_and_reversed_shape_cannot_bypass_revalidation() {
    let base = owner(Program::V2);
    let templates = frozen_templates(&base);
    let mut account = base.clone();
    let first = account.event_c_candidate(&templates[0], 0).unwrap();
    let stale_second = account.event_c_candidate(&templates[1], 1).unwrap();
    account.execute(&first).unwrap();
    let mut expected = account.clone();
    expected.gate = Gate::Failed("STALE_VERSION");
    assert_eq!(account.execute(&stale_second), Err("STALE_VERSION"));
    assert_eq!(account, expected);
    for input in [
        vec![],
        vec![templates[0].clone()],
        vec![templates[1].clone(), templates[0].clone()],
        vec![
            templates[0].clone(),
            templates[1].clone(),
            templates[0].clone(),
        ],
    ] {
        let mut account = base.clone();
        let mut expected = base.clone();
        expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
        assert_eq!(account.run_event_c(&input), Err("UNSUPPORTED_EXECUTION"));
        assert_eq!(account, expected);
    }
}

#[test]
fn malformed_e2_is_validated_after_e1_and_before_the_limit() {
    let changes: &[fn(&mut ExecutionTemplate)] = &[
        |t| t.quantity = d("2"),
        |t| t.price = d("11"),
        |t| t.key.external_id = "wrong".into(),
        |t| t.key.namespace = "other".into(),
        |t| t.key.account.account = "B".into(),
        |t| t.side = Side::Short,
        |t| t.key.product = ProfileProduct::BtcEth(Product::Btc),
        |t| t.fee_amount = Some(d("0")),
        |t| t.fee_asset = Some("USDT".into()),
        |t| t.liquidity = LiquidityRole::Unsupported,
    ];
    for program in [Program::V1, Program::V2] {
        for change in changes {
            let mut account = owner(program);
            let mut templates = frozen_templates(&account);
            let first = account.event_c_candidate(&templates[0], 0).unwrap();
            let mut expected = account.clone();
            let original = expected.execute(&first).unwrap();
            expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
            change(&mut templates[1]);
            assert_eq!(
                account.run_event_c(&templates),
                Err("UNSUPPORTED_EXECUTION")
            );
            assert_eq!(account, expected);
            assert_eq!(
                account.execute(&first),
                Ok(Reply::Duplicate(fact(&original).clone()))
            );
            let mut conflict = first.clone();
            conflict.template.quantity = d("2");
            assert_eq!(account.execute(&conflict), Err("EXECUTION_ID_CONFLICT"));
            assert_eq!(account, expected);
        }
    }
}

#[test]
fn second_candidate_fault_or_panic_retains_only_the_first_commit() {
    for stage in [
        Stage::Preparing,
        Stage::Validated,
        Stage::Settled,
        Stage::OrdersDrafted,
        Stage::Valued,
        Stage::ReceiptDrafted,
    ] {
        for panic in [false, true] {
            let mut account = owner(Program::V2);
            let templates = frozen_templates(&account);
            let first = account.event_c_candidate(&templates[0], 0).unwrap();
            let mut expected = account.clone();
            expected.execute(&first).unwrap();
            let reason = if panic {
                "EXECUTION_PANIC"
            } else {
                "INJECTED_SECOND_FAULT"
            };
            expected.gate = Gate::Failed(reason);
            assert_eq!(
                account.run_event_c_checked(&templates, |index, at| {
                    if index == 1 && at == stage {
                        assert!(!panic, "second-candidate panic");
                        return Err(reason);
                    }
                    Ok(())
                }),
                Err(reason)
            );
            assert_eq!(account, expected);
            assert_eq!(account.run_event_c(&templates), Err("RUN_FAILED"));
            assert_eq!(account, expected);
        }
    }
}

#[test]
fn profile_context_and_event_identity_do_not_cross() {
    let base = owner(Program::V1);
    let templates = frozen_templates(&base);
    let (seed, scenario, marks) = fixture();
    let mut btc = ScenarioAccount::from_seed(&seed, &scenario, &marks).unwrap();
    let mut expected = btc.clone();
    expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
    assert_eq!(btc.run_event_c(&templates), Err("UNSUPPORTED_EXECUTION"));
    assert_eq!(btc, expected);
    let changes: &[fn(&mut ExecutionCandidate)] = &[
        |c| c.spec_version = "spec-v1".into(),
        |c| c.rule_data_version = "tier-v1".into(),
        |c| c.candidate_id = "E2".into(),
    ];
    for change in changes {
        let mut account = base.clone();
        let mut candidate = account.event_c_candidate(&templates[0], 0).unwrap();
        change(&mut candidate);
        let mut expected = base.clone();
        expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
        assert_eq!(account.execute(&candidate), Err("UNSUPPORTED_EXECUTION"));
        assert_eq!(account, expected);
    }
    let mut account = base.clone();
    let mut other_event = account.event_c_candidate(&templates[0], 0).unwrap();
    other_event.event_id = "D".into();
    assert_eq!(account.execute(&other_event), Err("UNSUPPORTED_EXECUTION"));
    let mut account = base.clone();
    let mut bad = templates.clone();
    bad[1].matching_effective_at = 1000;
    let first = account.event_c_candidate(&bad[0], 0).unwrap();
    let mut expected = account.clone();
    expected.execute(&first).unwrap();
    expected.gate = Gate::Failed("UNSUPPORTED_CONTEXT_TRANSITION");
    assert_eq!(
        account.run_event_c(&bad),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    assert_eq!(account, expected);
}
