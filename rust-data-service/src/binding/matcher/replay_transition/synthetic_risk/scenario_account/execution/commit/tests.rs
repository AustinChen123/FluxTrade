use super::super::super::tests::{d, fixture};
use super::super::tests::fixture_candidate;
use super::*;
mod configured_fee;
mod configured_fee_scale;

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
    result.template.key.product = order.facts.product.clone();
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
    if order.facts.product == ProfileProduct::Pa {
        result.spec_version = "gt03-spec-v1".into();
        result.rule_data_version = "gt03-rule-v1".into();
    }
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

fn configured_position(owner: &mut ScenarioAccount, product: &Product, origin: &str) {
    owner.commit_sequence = 1;
    owner.positions.btc_mut().unwrap().insert(
        product.clone(),
        ProductPosition {
            side: Side::Long,
            contracts: d("0.5"),
            entry_basis: d("50"),
            lots: vec![EntryLot {
                source: SeedLot {
                    seed_execution_id: "seed".into(),
                    seed_sequence: 0,
                    strategy_id: "strategy".into(),
                    contracts: d("0.5"),
                    entry: d("100"),
                },
                origin_spec_version: origin.into(),
                execution_id: [9; 32],
                base_quantity: d("0.5"),
            }],
        },
    );
}

fn configured_order(
    seed: &SeedOrder,
    id: &str,
    product: &Product,
    side: Side,
    price: &str,
    qty: &str,
) -> SeedOrder {
    SeedOrder {
        order_id: id.into(),
        intent_id: format!("I-{id}"),
        client_id: format!("C-{id}"),
        product: ProfileProduct::BtcEth(product.clone()),
        side,
        price: d(price),
        reduce_only: false,
        original: d(qty),
        filled: Decimal::ZERO,
        canceled: Decimal::ZERO,
        remaining: d(qty),
        status: "OPEN".into(),
        ..seed.clone()
    }
}

#[test]
fn configured_execution_uses_product_owned_specs_for_fifo_and_duplicates() {
    for index in [0, 2, 3] {
        let (mut seed, mut products) = super::super::super::configured_tests::input(4);
        let product = products[index].product.clone();
        let (contract_value, multiplier) = match index {
            0 => ("1", "1"),
            2 => ("2", "1.25"),
            _ => ("3", "1.5"),
        };
        let unit = d(contract_value) * d(multiplier);
        products[index].specs[0].contract_value = d(contract_value);
        products[index].specs[0].multiplier = d(multiplier);
        products[index].tiers[0].tiers[0].minimum = d("0.5");
        let prototype = fixture().0.orders[0].clone();
        seed.orders = [("L", Side::Long, "101"), ("S", Side::Short, "99")]
            .map(|(id, side, price)| configured_order(&prototype, id, &product, side, price, "1"))
            .into();
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let mut second_lot = None;
        for (step, (order, side, quantity, price, remaining)) in [
            ("L", Side::Long, "0.5", "100", "0.5"),
            ("L", Side::Long, "0.5", "101", "0"),
            ("S", Side::Short, "0.5", "102", "0.5"),
            ("S", Side::Short, "0.5", "99", "0"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut candidate = input(
                &owner,
                order,
                &format!("CFG-{index}-{step}"),
                quantity,
                price,
            );
            candidate.template.matching_effective_at = 501 + step as i64;
            candidate.spec_version = "scale-spec-v1".into();
            candidate.rule_data_version = "scale-tier-v1".into();
            candidate.expected_account_version = owner.state_version;
            candidate.expected_order_version = owner.orders[order].version;
            candidate.template.side = side;
            let receipt = committed(&mut owner, &candidate);
            let fee = unit * d(["0.05", "0.0505", "0.051", "0.0495"][step]);
            let pnl = unit * d(["0", "0", "1", "-1"][step]);
            assert_eq!(receipt.fee_amount, fee);
            assert_eq!(receipt.realized_pnl_delta, pnl);
            assert_eq!(
                (receipt.contract_value, receipt.quantity, receipt.price),
                (d(contract_value), d("0.5"), d(price))
            );
            assert_eq!(receipt.product, ProfileProduct::BtcEth(product.clone()));
            let cumulative_fees = unit * d(["0.05", "0.1005", "0.1515", "0.201"][step]);
            assert_eq!(
                owner.cash,
                d("121.2") + unit * d(["0", "0", "1", "0"][step]) - cumulative_fees
            );
            assert_eq!(owner.fees, cumulative_fees);
            assert_eq!(
                (
                    receipt.state_version_before,
                    receipt.state_version_after,
                    receipt.commit_sequence
                ),
                (step as u64, step as u64 + 1, step as u64)
            );
            assert_eq!(
                (receipt.order_version_before, receipt.order_version_after),
                ([0, 1, 0, 1][step], [1, 2, 1, 2][step])
            );
            assert_eq!(
                (
                    receipt.order_after.filled,
                    receipt.order_after.remaining,
                    receipt.order_after.status.as_str()
                ),
                (
                    d(["0.5", "1", "0.5", "1"][step]),
                    d(remaining),
                    if remaining == "0" {
                        "FILLED"
                    } else {
                        "PARTIALLY_FILLED"
                    }
                )
            );
            if step == 0 {
                assert!(receipt.position_before.is_none());
            } else {
                let before = receipt.position_before.as_ref().unwrap();
                assert_eq!(
                    (before.contracts, before.entry_basis),
                    (
                        d(["0.5", "1", "0.5"][step - 1]),
                        unit * d(["50", "100.5", "50.5"][step - 1])
                    )
                );
            }
            if step == 1 {
                second_lot = Some(receipt.execution_id);
            }
            assert_eq!(
                (
                    receipt.spec_version.as_str(),
                    receipt.rule_data_version.as_str()
                ),
                ("scale-spec-v1", "scale-tier-v1")
            );
            let position = owner.positions.btc().unwrap().get(&product);
            if step == 3 {
                assert!(position.is_none() && receipt.position_after.is_none());
            } else {
                let position = position.unwrap();
                assert_eq!(
                    (position.contracts, position.entry_basis),
                    (
                        d(["0.5", "1", "0.5"][step.min(2)]),
                        unit * d(["50", "100.5", "50.5"][step.min(2)])
                    )
                );
                assert_eq!(receipt.position_after.as_ref(), Some(position));
                if step == 2 {
                    assert_eq!(position.lots.len(), 1);
                    assert_eq!(position.lots[0].execution_id, second_lot.unwrap());
                    assert_eq!(position.lots[0].source.entry, d("101"));
                }
            }
            assert_eq!(receipt.cash_deltas[0].1, pnl - fee);
            if step == 0 {
                let after = owner.clone();
                assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
                assert_eq!(owner, after);
            }
        }
    }
}

#[test]
fn configured_settlement_invalid_origin_leverage_and_tiers_publish_no_financial_state() {
    let (mut seed, mut products) = super::super::super::configured_tests::input(2);
    let product = products[0].product.clone();
    let prototype = fixture().0.orders[0].clone();
    seed.orders = vec![configured_order(
        &prototype,
        "L",
        &product,
        Side::Long,
        "100",
        "1",
    )];
    products[1].specs[0].version = "foreign-spec".into();
    let expected = [
        "INVALID_LOT_ORIGIN_SPEC",
        "INVALID_LOT_ORIGIN_SPEC",
        "UNSUPPORTED_CONFIGURED_SETTLEMENT",
        "UNSUPPORTED_POSITION_TIER",
        "UNSUPPORTED_POSITION_TIER",
    ];
    for index in 0..expected.len() {
        let mut rows = products.clone();
        let mut leverage = d("10");
        match index {
            2 => leverage = d("5"),
            3 => rows[0].tiers[0].tiers[0].maximum = d("0.75"),
            4 => {
                rows[0].tiers[0].interval.to = Some(501);
                rows[0].tiers[0].tiers[0].maximum = d("0.75");
                let mut future = rows[0].tiers[0].clone();
                future.version = "future-tier".into();
                future.interval = Interval {
                    from: 501,
                    to: None,
                };
                future.tiers[0].maximum = d("100");
                rows[0].tiers.push(future);
                assert!(rows[0].tiers[1].tiers[0].maximum >= d("1"));
            }
            _ => {}
        }
        let case_seed = seed.clone();
        let mut owner = ScenarioAccount::from_configured(&case_seed, leverage, rows).unwrap();
        match index {
            0 => configured_position(&mut owner, &product, "unknown-spec"),
            1 => configured_position(&mut owner, &product, "foreign-spec"),
            3 | 4 => configured_position(&mut owner, &product, "scale-spec-v1"),
            _ => {}
        }
        let mut candidate = input(&owner, "L", &format!("BAD-{index}"), "0.5", "100");
        candidate.spec_version = "scale-spec-v1".into();
        candidate.rule_data_version = "scale-tier-v1".into();
        candidate.expected_account_version = owner.state_version;
        candidate.expected_order_version = owner.orders["L"].version;
        assert_eq!(candidate.template.matching_effective_at, 500);
        let before = owner.clone();
        assert_eq!(owner.execute(&candidate), Err(expected[index]));
        assert_eq!(owner.gate, Gate::Failed(expected[index]));
        let mut after = owner.clone();
        after.gate = before.gate.clone();
        assert_eq!(after, before);
    }
}

#[test]
fn configured_liquidation_fails_after_committed_fill_without_liquidation_publication() {
    for liquidation_fee in [d("0.02"), d("0.00602")] {
        let (mut seed, mut products) = super::super::super::configured_tests::input(2);
        products[0].liquidation_fee = liquidation_fee;
        seed.cash = d("0.04");
        let product = products[0].product.clone();
        let template = fixture().0.orders[0].clone();
        seed.orders = vec![configured_order(
            &template,
            "L",
            &product,
            Side::Long,
            "100",
            "0.5",
        )];
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let mut candidate = input(&owner, "L", "CFG-LIQ", "0.5", "100");
        candidate.spec_version = "scale-spec-v1".into();
        candidate.rule_data_version = "scale-tier-v1".into();
        candidate.expected_account_version = owner.state_version;
        candidate.expected_order_version = owner.orders["L"].version;
        assert_eq!(
            owner.execute(&candidate),
            Err("UNSUPPORTED_CONFIGURED_LIQUIDATION")
        );
        assert_eq!((owner.cash, owner.fees), (d("-0.01"), d("0.05")));
        assert_eq!(owner.positions.btc().unwrap()[&product].contracts, d("0.5"));
        assert_eq!(owner.orders["L"].facts.status, "FILLED");
        assert_eq!(owner.execution_receipts.len(), 1);
        assert_eq!(owner.liquidation_ids().count(), 0);
        assert_eq!(
            owner.gate,
            Gate::Failed("UNSUPPORTED_CONFIGURED_LIQUIDATION")
        );
        let after = owner.clone();
        let receipt = owner.execution_receipts.values().next().unwrap().clone();
        assert_eq!(owner.execute(&candidate), Ok(Reply::Duplicate(receipt)));
        assert_eq!(owner, after);
    }
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
