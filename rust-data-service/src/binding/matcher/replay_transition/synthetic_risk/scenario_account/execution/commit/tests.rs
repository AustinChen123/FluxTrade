use super::super::super::tests::{d, fixture};
use super::super::tests::fixture_candidate;
use super::*;

pub(in super::super::super) fn at(mut input: ExecutionCandidate, at: i64) -> ExecutionCandidate {
    input.template.matching_effective_at = at;
    input
}

#[test]
fn min_cash_execution_metadata_is_frozen_across_context_and_duplicates() {
    let (seed, _, _) = fixture();
    let mut owner = ScenarioAccount::synthetic_min_cash(seed.key).unwrap();
    let candidate = at(input(&owner, "MIN-O1", "MIN-X1", "20", "50000"), 501);
    let receipt = committed(&mut owner, &candidate);
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.gross_realized,
            owner.state_version
        ),
        (d("990"), d("10"), d("0"), 1)
    );
    assert_eq!(
        receipt.financial_payload_digest,
        candidate.template.digest()
    );
    assert_eq!(
        (
            receipt.order_created_at,
            receipt.execution_effective_at,
            receipt.contract_value
        ),
        (500, 501, d("0.01"))
    );
    assert_eq!(receipt.order_after, owner.orders["MIN-O1"].facts);
    assert_eq!(receipt.order_after.status, "FILLED");
    assert_eq!(
        (receipt.order_after.original, receipt.order_after.filled),
        (d("20"), d("20"))
    );
    use super::super::super::context::tests::{activation, mark_rows};
    let (_, marks) = owner.btc_context().unwrap();
    let change = activation(&owner, 503, mark_rows(marks, 503));
    owner.activate_context(&change).unwrap();
    let before = owner.clone();
    assert_eq!(
        owner.execute(&candidate),
        Ok(Reply::Duplicate(receipt.clone()))
    );
    assert_eq!(owner, before);
    assert_eq!(owner.execution_receipts[&receipt.execution_id], receipt);
    assert_eq!(
        (owner.state_version, owner.orders["MIN-O1"].created_at),
        (2, 500)
    );
}

#[test]
fn liquidation_fault_after_execution_retains_immutable_source_commit() {
    for panic in [false, true] {
        let mut owner = flat("2.1", "50000", &[(Side::Long, "1", "50000")]);
        let candidate = input(&owner, "O0", "risk", "1", "50000");
        let fault = if panic {
            "LIQUIDATION_PANIC"
        } else {
            "INJECTED_LIQUIDATION"
        };
        assert_eq!(
            owner.execute_checked(&candidate, |stage| {
                if stage == Stage::Risk(risk_transition::Stage::LiquidationBeforeSwap(1)) {
                    if panic {
                        panic!("liquidation hook");
                    }
                    return Err("INJECTED_LIQUIDATION");
                }
                Ok(())
            }),
            Err(fault)
        );
        assert_eq!(
            (
                owner.cash,
                owner.fees,
                owner.state_version,
                owner.commit_sequence
            ),
            (d("1.6"), d("0.5"), 1, 1)
        );
        assert_eq!(
            owner.positions.btc().unwrap()[&Product::Btc].contracts,
            d("1")
        );
        assert_eq!(owner.execution_receipts.len(), 1);
        assert_eq!(owner.liquidation_ids().count(), 0);
        assert_eq!(owner.gate, Gate::Failed(fault));
        let before = owner.clone();
        assert!(matches!(owner.execute(&candidate), Ok(Reply::Duplicate(_))));
        assert_eq!(owner, before);
    }
}

fn flat(cash: &str, mark: &str, orders: &[(Side, &str, &str)]) -> ScenarioAccount {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d(cash);
    seed.positions.clear();
    let prototype = seed.orders[0].clone();
    seed.orders = orders
        .iter()
        .enumerate()
        .map(|(index, (side, qty, price))| SeedOrder {
            order_id: format!("O{index}"),
            intent_id: format!("I{index}"),
            client_id: format!("C{index}"),
            side: *side,
            original: d(qty),
            remaining: d(qty),
            filled: d("0"),
            canceled: Decimal::ZERO,
            price: d(price),
            reduce_only: false,
            status: "OPEN".into(),
            ..prototype.clone()
        })
        .collect();
    marks[0].price = d(mark);
    ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
}

pub(in super::super::super) fn input(
    owner: &ScenarioAccount,
    order_id: &str,
    execution: &str,
    qty: &str,
    price: &str,
) -> ExecutionCandidate {
    let (_, mut result) = fixture_candidate();
    let order = &owner.orders[order_id];
    result.template.key.account = owner.key.clone();
    result.template.key.product = order.facts.product;
    result.template.key.external_id = execution.into();
    result.event_id = format!("event-{execution}");
    result.template.matching_effective_at = owner
        .transition
        .accepted_stamp
        .as_ref()
        .map_or(owner.seed_effective_at, |s| s.effective_at + 1);
    result.template.order_id = order_id.into();
    result.template.side = order.facts.side;
    result.template.quantity = d(qty);
    result.template.price = d(price);
    result.expected_account_version = owner.state_version;
    result.expected_order_version = order.version;
    result
}

fn committed(owner: &mut ScenarioAccount, candidate: &ExecutionCandidate) -> CommittedExecution {
    let Reply::Committed {
        receipt,
        terminal_reason: None,
    } = owner.execute(candidate).unwrap()
    else {
        panic!("stable commit required");
    };
    assert_eq!(owner.execution_receipts[&receipt.execution_id], receipt);
    assert_eq!(receipt.pending_action_ids, Vec::<String>::new());
    assert_eq!(receipt.fee_asset, "USDT");
    assert_eq!(
        (&receipt.spec_version, &receipt.rule_data_version),
        (&candidate.spec_version, &candidate.rule_data_version)
    );
    receipt
}

#[test]
fn long_and_short_golden_fifo_runs_use_actual_commits() {
    let long = [
        (Side::Long, "1", "50000"),
        (Side::Long, "2", "50000.1"),
        (Side::Short, "1", "50001"),
        (Side::Short, "2", "49999.9"),
    ];
    let short = [
        (Side::Short, "2", "50000"),
        (Side::Short, "1", "49999.5"),
        (Side::Long, "1.5", "49900"),
        (Side::Long, "1.5", "50100"),
    ];
    for (orders, cash, gross, fees) in [
        (
            long,
            ["9999.5", "9998.499998", "9998.009988", "9997.00599"],
            "0.006",
            "3.00001",
        ),
        (
            short,
            ["9999", "9998.500005", "9999.251505", "9996.995005"],
            "-0.005",
            "2.999995",
        ),
    ] {
        let mut owner = flat("10000", "50001", &orders);
        let mut receipts = Vec::new();
        for (index, (_, qty, price)) in orders.iter().enumerate() {
            let candidate = input(
                &owner,
                &format!("O{index}"),
                &format!("X{index}"),
                qty,
                price,
            );
            let receipt = committed(&mut owner, &candidate);
            assert_eq!(
                (
                    receipt.state_version_before,
                    receipt.state_version_after,
                    receipt.commit_sequence
                ),
                (index as u64, index as u64 + 1, index as u64)
            );
            assert_eq!(
                (receipt.order_version_before, receipt.order_version_after),
                (0, 1)
            );
            assert_eq!(owner.cash, d(cash[index]));
            assert_eq!(owner.orders[&format!("O{index}")].facts.status, "FILLED");
            assert_eq!(
                receipt.position_after.as_ref(),
                owner.positions.btc().unwrap().get(&Product::Btc)
            );
            assert_eq!(
                receipt.reservation_after,
                FinancialSnapshot::BtcEth(owner.reservation().unwrap())
            );
            receipts.push(receipt);
            if index == 1 && orders[0].0 == Side::Long {
                let (scenario, marks) = owner.btc_context().unwrap();
                assert_eq!(
                    scenario
                        .evaluate(&owner.projection().unwrap(), marks)
                        .unwrap()
                        .equity
                        - owner.cash,
                    d("0.028")
                );
            }
            if index == 2 {
                let lots = &owner.positions.btc().unwrap()[&Product::Btc].lots;
                if orders[0].0 == Side::Long {
                    assert_eq!(lots.len(), 1);
                    assert_eq!(lots[0].execution_id, receipts[1].execution_id);
                } else {
                    let original = &receipts[0].position_after.as_ref().unwrap().lots[0];
                    assert_eq!(
                        (
                            lots[0].execution_id,
                            lots[0].source.seed_sequence,
                            lots[0].source.entry
                        ),
                        (
                            original.execution_id,
                            original.source.seed_sequence,
                            d("50000")
                        )
                    );
                    assert_eq!(lots[0].source.contracts, d("0.5"));
                }
            }
        }
        assert!(owner.positions.is_empty());
        assert_eq!(
            (owner.gross_realized, owner.fees, owner.commit_sequence),
            (d(gross), d(fees), 4)
        );
        assert_eq!(owner.orders.len(), 4);
        assert_eq!(owner.execution_receipts.len(), 4);
    }
}

#[test]
fn partial_reservation_and_changed_position_rejection_are_authoritative() {
    for (mark, used) in [("50000", "50.75"), ("50100", "51.85")] {
        let mut owner = flat(
            "1000",
            mark,
            &[(Side::Long, "1", "50000"), (Side::Short, "1", "50000")],
        );
        let candidate = input(&owner, "O0", "partial", "0.5", "50000");
        let receipt = committed(&mut owner, &candidate);
        assert_eq!(owner.reservation().unwrap().used_margin, d(used));
        assert_eq!(owner.orders["O0"].facts.status, "PARTIALLY_FILLED");
        assert_eq!(owner.orders["O0"].facts.remaining, d("0.5"));
        let before = owner.clone();
        let opposing = input(&owner, "O1", "opposing", "0.5", "50000");
        assert_eq!(
            owner.execute(&opposing),
            Ok(Reply::Rejected(
                "EXECUTION_INELIGIBLE_REMAINDER_EXCEEDS_POSITION"
            ))
        );
        assert_eq!(owner, before);
        assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
        assert_eq!(owner, before);
    }
}

#[test]
fn runtime_duplicate_conflict_seed_reference_and_first_failure_are_preserved() {
    let (mut owner, candidate) = fixture_candidate();
    let receipt = committed(&mut owner, &candidate);
    assert_eq!(receipt.commit_sequence, 2);
    assert_eq!(owner.orders["O1"].version, 1);
    let mut conflicting = candidate.clone();
    conflicting.template.quantity = d("0.4");
    let mut before = owner.clone();
    before.gate = Gate::Failed("EXECUTION_ID_CONFLICT");
    assert_eq!(owner.execute(&conflicting), Err("EXECUTION_ID_CONFLICT"));
    assert_eq!(owner, before);
    assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
    conflicting.template.key.namespace = "seed".into();
    assert_eq!(owner.execute(&conflicting), Err("SEED_IDENTITY_CONFLICT"));
    assert_eq!(owner, before);
    conflicting.template.key.namespace = "new".into();
    assert_eq!(owner.execute(&conflicting), Err("EVENT_ID_CONFLICT"));
    assert_eq!(owner, before);
}

#[test]
fn runtime_opening_after_seed_retains_fifo_sequence_and_provenance() {
    let (mut seed, config, marks) = fixture();
    seed.orders[0].side = Side::Long;
    seed.orders[0].reduce_only = false;
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let candidate = input(&owner, "O1", "opening-after-seed", "0.5", "50000.1");
    let before = owner.clone();
    let mut clone = owner.clone();
    let receipt = committed(&mut clone, &candidate);
    assert_eq!(owner, before);
    assert_eq!(committed(&mut owner, &candidate), receipt);
    let lots = &owner.positions.btc().unwrap()[&Product::Btc].lots;
    assert_eq!(
        &lots[..2],
        before.positions.btc().unwrap()[&Product::Btc].lots
    );
    assert_eq!(lots[2].source.seed_sequence, 2);
    assert_eq!(lots[2].execution_id, receipt.execution_id);
    assert_eq!(lots[2].source.strategy_id, seed.orders[0].strategy_id);
    assert_eq!(owner.commit_sequence, 3);
}

#[test]
fn admitted_order_partial_full_versions_improved_prices_and_old_duplicate() {
    for (side, limit) in [(Side::Long, "50001"), (Side::Short, "49999")] {
        let mut owner = flat("10000", "50000", &[]);
        let order = admission::admit_execution_fixture(&mut owner, side, d("2"), d(limit));
        let results = owner.intent_results.clone();
        let first = input(&owner, &order, "first", "1", "50000");
        let receipt = committed(&mut owner, &first);
        assert_eq!(
            (
                receipt.state_version_before,
                receipt.state_version_after,
                receipt.order_version_before,
                receipt.order_version_after
            ),
            (1, 2, 1, 2)
        );
        let mut stale = owner.clone();
        let mut stale_input = first.clone();
        stale_input.template.key.external_id = "stale".into();
        stale_input.event_id = "stale-event".into();
        stale_input.template.matching_effective_at += 1;
        assert_eq!(stale.execute(&stale_input), Err("STALE_VERSION"));
        let second = input(&owner, &order, "second", "1", "50000");
        let final_receipt = committed(&mut owner, &second);
        assert_eq!(
            (
                final_receipt.state_version_after,
                final_receipt.order_version_after
            ),
            (3, 3)
        );
        assert_eq!(owner.orders[&order].facts.filled, d("2"));
        assert_eq!(owner.orders[&order].facts.remaining, d("0"));
        assert_eq!(owner.intent_results, results);
        let before = owner.clone();
        assert_eq!(owner.execute(&first), Ok(Reply::Duplicate(receipt)));
        assert_eq!(owner, before);
        let terminal = input(&owner, &order, "terminal", "1", "50000");
        assert_eq!(owner.execute(&terminal), Err("UNSUPPORTED_EXECUTION"));
        let mut expected = before;
        expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
        assert_eq!(owner, expected);
    }
}

#[test]
fn same_external_id_isolated_by_account_product_and_namespace() {
    let (mut seed, config, marks) = fixture();
    seed.positions.clear();
    seed.orders[0].reduce_only = false;
    let mut eth = seed.orders[0].clone();
    eth.product = ProfileProduct::BtcEth(Product::Eth);
    eth.price = d("2000");
    eth.order_id = "ETH".into();
    eth.intent_id = "I-ETH".into();
    eth.client_id = "C-ETH".into();
    seed.orders.push(eth);
    let mut ids = BTreeSet::new();
    for account in ["A", "B"] {
        seed.key.account = account.into();
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        for (order, price) in [("O1", "50000.1"), ("ETH", "2000")] {
            for namespace in ["first", "second"] {
                let mut candidate = input(&owner, order, "same-external", "0.5", price);
                candidate.template.key.namespace = namespace.into();
                candidate.event_id = format!("event-{order}-{namespace}");
                let receipt = committed(&mut owner, &candidate);
                assert!(ids.insert(receipt.execution_id));
                assert_eq!(receipt.account_key.account, account);
            }
        }
        assert_eq!(owner.execution_receipts.len(), 4);
        assert_eq!(owner.positions.len(), 2);
    }
}

#[test]
fn every_prepublication_fault_and_panic_discards_the_draft() {
    let (owner, candidate) = fixture_candidate();
    for stage in [
        Stage::Preparing,
        Stage::Validated,
        Stage::Settled,
        Stage::OrdersDrafted,
        Stage::Valued,
        Stage::ReceiptDrafted,
    ] {
        for panic in [false, true] {
            let mut actual = owner.clone();
            let mut expected = owner.clone();
            let reason = if panic {
                "EXECUTION_PANIC"
            } else {
                "INJECTED_CALC_ERROR"
            };
            expected.gate = Gate::Failed(reason);
            assert_eq!(
                actual.execute_checked(&candidate, |at| {
                    if at == stage {
                        assert!(!panic, "injected execution panic");
                        return Err(reason);
                    }
                    Ok(())
                }),
                Err(reason)
            );
            assert_eq!(actual, expected);
        }
    }
    for field in 0..5 {
        let mut actual = owner.clone();
        match field {
            0 => actual.state_version = u64::MAX,
            1 => actual.orders.get_mut("O1").unwrap().version = u64::MAX,
            2 => actual.commit_sequence = u64::MAX,
            3 => actual.fees = Decimal::MAX,
            _ => {
                actual
                    .positions
                    .btc_mut()
                    .unwrap()
                    .get_mut(&Product::Btc)
                    .unwrap()
                    .entry_basis = d("1")
            }
        }
        let input = input(&actual, "O1", "failure", "0.5", "50000.1");
        let reason = match field {
            0..=2 => "VERSION_OVERFLOW",
            3 => "DECIMAL_OVERFLOW",
            _ => "INVALID_FIFO_POSITION",
        };
        let mut before = actual.clone();
        before.gate = Gate::Failed(reason);
        assert_eq!(actual.execute(&input), Err(reason));
        assert_eq!(actual, before);
    }
}

#[test]
fn committed_source_receipt_survives_liquidation_and_negative_available_can_be_stable() {
    for (cash, extra_order, failed) in [
        ("2.1", false, true),
        ("40", true, false),
        ("40", false, false),
    ] {
        let mut orders = vec![(Side::Long, "1", "50000")];
        if extra_order {
            orders.push((Side::Long, "0.1", "50000"));
        }
        let mut owner = flat(cash, "50000", &orders);
        let candidate = input(&owner, "O0", "risk", "1", "50000");
        let Reply::Committed {
            receipt,
            terminal_reason,
        } = owner.execute(&candidate).unwrap()
        else {
            panic!("must retain commit");
        };
        assert_eq!(terminal_reason, None);
        assert_eq!(
            (
                owner.state_version,
                owner.orders["O0"].version,
                owner.commit_sequence
            ),
            (
                if extra_order || failed { 2 } else { 1 },
                1,
                if failed { 2 } else { 1 }
            )
        );
        assert_eq!(
            owner.cash,
            if failed {
                d("-1.41")
            } else {
                d(cash) - d("0.5")
            }
        );
        assert!(owner.reservation().unwrap().available_margin < d("0"));
        assert!(owner.pending_actions.is_empty());
        let before = owner.clone();
        assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
        assert_eq!(owner, before);
        if failed {
            let mut conflict = candidate.clone();
            conflict.template.quantity = d("0.9");
            assert_eq!(owner.execute(&conflict), Err("EXECUTION_ID_CONFLICT"));
            let mut expected = before;
            expected.gate = Gate::Failed("EXECUTION_ID_CONFLICT");
            assert_eq!(owner, expected);
            let next = input(&owner, "O0", "next", "1", "50000");
            assert_eq!(owner.execute(&next), Ok(Reply::Rejected("RUN_TERMINAL")));
            assert_eq!(owner, expected);
        }
    }
    let (mut seed, config, mut marks) = fixture();
    seed.positions[0].contracts = d("1");
    seed.positions[0].lots.truncate(1);
    seed.cash = d("3");
    marks[0].price = d("50000");
    seed.orders[0].price = d("49000");
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let candidate = input(&owner, "O1", "flat-negative", "1", "49000");
    assert!(matches!(
        owner.execute(&candidate),
        Ok(Reply::Committed {
            terminal_reason: None,
            ..
        })
    ));
    assert!(owner.positions.is_empty());
    assert_eq!(
        owner.transition.lifecycle,
        risk_transition::Lifecycle::LiquidatedInsolvent
    );
    assert_eq!(owner.cash, d("-7.49"));
    assert_eq!(owner.execution_receipts.len(), 1);
    // Negative available with only a genuinely reducing remainder needs no 4C action.
    seed.cash = d("40");
    seed.orders[0].price = d("50000");
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let candidate = input(&owner, "O1", "reducing-only", "0.1", "50000");
    committed(&mut owner, &candidate);
    assert_eq!(owner.gate, Gate::Running);
    assert!(owner.reservation().unwrap().available_margin < d("0"));
}
