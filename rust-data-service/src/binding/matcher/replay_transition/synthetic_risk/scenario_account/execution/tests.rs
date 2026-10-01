use super::super::tests::{d, fixture};
use super::*;

#[test]
fn event_limit_rejects_unmapped_execution_identity() {
    let mut owner = ScenarioAccount::from_event_limit_seed(
        &event_limit::tests::seed(),
        &event_limit::Config::frozen(event_limit::Program::V1),
    )
    .unwrap();
    let (_, mut candidate) = fixture_candidate();
    candidate.template.key.product = ProfileProduct::Pa;
    candidate.template.order_id = "O_A".into();
    candidate.template.quantity = d("1");
    candidate.template.price = d("10");
    let mut expected = owner.clone();
    expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
    assert_eq!(owner.execute(&candidate), Err("UNSUPPORTED_EXECUTION"));
    assert_eq!(owner, expected);
}

pub(super) fn fixture_candidate() -> (ScenarioAccount, ExecutionCandidate) {
    let (seed, scenario, marks) = fixture();
    let (spec, tier) = scenario.resolve(&Product::Btc, 500).unwrap();
    let candidate = ExecutionCandidate {
        template: ExecutionTemplate {
            key: ExternalExecutionKey {
                account: seed.key.clone(),
                namespace: "synthetic-v1".into(),
                product: ProfileProduct::BtcEth(Product::Btc),
                external_id: "X3".into(),
            },
            order_id: "O1".into(),
            side: Side::Short,
            price: d("50000.1"),
            quantity: d("0.5"),
            liquidity: LiquidityRole::SyntheticTaker,
            fee_asset: None,
            fee_amount: None,
            matching_effective_at: 500,
        },
        canonical_execution_id: None,
        candidate_id: "candidate-1".into(),
        event_id: "event-1".into(),
        source_id: "transport-A".into(),
        visible_at: 510,
        expected_account_version: 0,
        expected_order_version: 0,
        spec_version: spec.version.clone(),
        rule_data_version: tier.version.clone(),
    };
    (
        ScenarioAccount::from_seed(&seed, &scenario, &marks).unwrap(),
        candidate,
    )
}

fn p1o03_owner() -> ScenarioAccount {
    let (seed, _, _) = fixture();
    super::super::wire::profiles::construct("SYNTHETIC_P1_O03_V1", seed.key, None).unwrap()
}

fn admit_p1o03_order(
    owner: &mut ScenarioAccount,
    intent_id: &str,
    side: Side,
    quantity: Decimal,
    limit: Decimal,
) -> String {
    let intent = admission::fixture_intent(owner, intent_id, side, quantity, limit);
    let (kind, result) = admission::fixture_admit(owner, intent_id, 601, &intent).unwrap();
    assert_eq!(kind, "accepted");
    result.order_id().unwrap().to_owned()
}

fn p1o03_candidate(
    owner: &ScenarioAccount,
    order_id: &str,
    side: Side,
    quantity: Decimal,
    execution_id: &str,
) -> ExecutionCandidate {
    let (_, mut candidate) = fixture_candidate();
    candidate.template.key.account = owner.key.clone();
    candidate.template.key.product = ProfileProduct::Pa;
    candidate.template.key.external_id = execution_id.into();
    candidate.template.order_id = order_id.into();
    candidate.template.side = side;
    candidate.template.price = d("10");
    candidate.template.quantity = quantity;
    candidate.template.matching_effective_at = 602;
    candidate.event_id = execution_id.into();
    candidate.expected_account_version = owner.state_version;
    candidate.expected_order_version = owner.orders[order_id].version;
    candidate.spec_version = "gt03-spec-v1".into();
    candidate.rule_data_version = "gt03-rule-v1".into();
    candidate
}

#[test]
fn p1o03_execution_must_match_resting_order_side_product_and_limit() {
    for incompatible in ["side", "long_limit", "short_limit"] {
        let mut owner = p1o03_owner();
        let (order_side, limit, execution_side) = match incompatible {
            "side" => (Side::Short, "10", Side::Long),
            "long_limit" => (Side::Long, "1", Side::Long),
            _ => (Side::Short, "11", Side::Short),
        };
        let order_id = admit_p1o03_order(&mut owner, incompatible, order_side, d("1"), d(limit));
        let candidate = p1o03_candidate(
            &owner,
            &order_id,
            execution_side,
            Decimal::ONE,
            &format!("O03-{incompatible}"),
        );
        let before = owner.clone();
        assert_eq!(
            owner.prepare_execution(&candidate),
            Err("UNSUPPORTED_EXECUTION"),
            "incompatible {incompatible} must not be eligible"
        );
        assert_eq!(
            owner, before,
            "incompatible {incompatible} changed owner state"
        );
        assert_eq!(owner.gate, Gate::Running);

        let mut expected_failure = before;
        expected_failure.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
        assert_eq!(
            owner.execute(&candidate),
            Err("UNSUPPORTED_EXECUTION"),
            "incompatible {incompatible} execution must fail closed"
        );
        assert_eq!(owner, expected_failure, "only the fault gate may change");
    }

    let mut owner = p1o03_owner();
    let order_id = admit_p1o03_order(
        &mut owner,
        "wrong-product",
        Side::Long,
        Decimal::ONE,
        d("10"),
    );
    owner.orders.get_mut(&order_id).unwrap().facts.product = ProfileProduct::BtcEth(Product::Btc);
    let candidate = p1o03_candidate(
        &owner,
        &order_id,
        Side::Long,
        Decimal::ONE,
        "O03-WRONG-PRODUCT",
    );
    let before = owner.clone();
    assert_eq!(
        owner.prepare_execution(&candidate),
        Err("UNSUPPORTED_EXECUTION")
    );
    assert_eq!(owner, before);
}

#[test]
fn p1o03_execution_limit_eligibility_preserves_both_sides() {
    for (side, limit) in [
        (Side::Long, "10"),
        (Side::Long, "11"),
        (Side::Short, "9"),
        (Side::Short, "10"),
    ] {
        let mut owner = p1o03_owner();
        let order_id = admit_p1o03_order(&mut owner, "eligible", side, d("5"), d(limit));
        let candidate = p1o03_candidate(&owner, &order_id, side, Decimal::ONE, "O03-ELIGIBLE");
        let before = owner.clone();
        assert!(matches!(
            owner.prepare_execution(&candidate),
            Ok(Preparation::Eligible { .. })
        ));
        assert_eq!(owner, before, "preparation must remain read-only");
        let commit::Reply::Committed { receipt, .. } = owner.execute(&candidate).unwrap() else {
            panic!("compatible P1O03 execution must commit");
        };
        assert_eq!(receipt.price, Decimal::TEN);
        assert_eq!(owner.orders[&order_id].facts.filled, Decimal::ONE);
        assert_eq!(owner.gate, Gate::Running);
    }
}

#[test]
fn p1o03_admitted_short_reduces_then_closes_without_opening_identity() {
    let mut owner = p1o03_owner();
    let order_id = admit_p1o03_order(&mut owner, "short-close", Side::Short, d("5"), d("10"));

    let partial = p1o03_candidate(&owner, &order_id, Side::Short, d("2"), "O03-SHORT-PARTIAL");
    owner.execute(&partial).unwrap();
    let PositionState::GoldenCancel(Some(position)) = &owner.positions else {
        panic!("partial reduction must retain the long position");
    };
    assert_eq!(position.side, Side::Long);
    assert_eq!(position.contracts, d("3"));
    assert_eq!(owner.orders[&order_id].facts.remaining, d("3"));
    assert_eq!(owner.execution_receipts.len(), 1);
    assert_eq!(owner.gate, Gate::Running);

    let mut close = p1o03_candidate(&owner, &order_id, Side::Short, d("3"), "O03-SHORT-CLOSE");
    close.template.matching_effective_at = 603;
    owner.execute(&close).unwrap();
    assert_eq!(owner.positions, PositionState::GoldenCancel(None));
    assert_eq!(owner.orders[&order_id].facts.remaining, Decimal::ZERO);
    assert_eq!(owner.orders[&order_id].facts.status, "FILLED");
    assert_eq!(owner.execution_receipts.len(), 2);
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn p1o03_admitted_short_cannot_cross_zero() {
    let mut owner = p1o03_owner();
    let order_id = admit_p1o03_order(&mut owner, "short-cross-zero", Side::Short, d("6"), d("10"));
    let candidate = p1o03_candidate(
        &owner,
        &order_id,
        Side::Short,
        d("6"),
        "O03-SHORT-CROSS-ZERO",
    );
    assert!(matches!(
        owner.prepare_execution(&candidate),
        Ok(Preparation::Eligible { .. })
    ));
    let mut expected_failure = owner.clone();
    expected_failure.gate = Gate::Failed("CROSS_ZERO_OR_UNEXPECTED_OPENING_IDENTITY");
    assert_eq!(
        owner.execute(&candidate),
        Err("CROSS_ZERO_OR_UNEXPECTED_OPENING_IDENTITY")
    );
    assert_eq!(
        owner, expected_failure,
        "cross-zero failure must only fail the gate"
    );
}

#[test]
fn every_identity_axis_and_financial_field_is_canonical() {
    let (_, candidate) = fixture_candidate();
    let template = &candidate.template;
    let key_changes: &[fn(&mut ExternalExecutionKey)] = &[
        |k| k.account.venue.push('x'),
        |k| k.account.environment.push('x'),
        |k| k.account.account.push('x'),
        |k| k.account.subaccount = Some("sub".into()),
        |k| k.namespace.push('x'),
        |k| k.product = ProfileProduct::BtcEth(Product::Eth),
        |k| k.product = ProfileProduct::Pa,
        |k| k.external_id.push('x'),
    ];
    for change in key_changes {
        let mut other = template.clone();
        change(&mut other.key);
        assert_ne!(other.key.canonical_id(), template.key.canonical_id());
        assert_ne!(other.digest(), template.digest());
    }
    let financial_changes: &[fn(&mut ExecutionTemplate)] = &[
        |t| t.order_id.push('x'),
        |t| t.side = Side::Long,
        |t| t.price += d("0.1"),
        |t| t.quantity += d("0.1"),
        |t| t.liquidity = LiquidityRole::Unsupported,
        |t| t.fee_asset = Some("USDT".into()),
        |t| t.fee_amount = Some(d("0")),
        |t| t.matching_effective_at += 1,
    ];
    for change in financial_changes {
        let mut other = template.clone();
        change(&mut other);
        assert_eq!(other.key.canonical_id(), template.key.canonical_id());
        assert_ne!(other.digest(), template.digest());
    }
    let mut canonical = template.clone();
    canonical.quantity = d("0.50000");
    canonical.price = d("50000.10000");
    assert_eq!(canonical.digest(), template.digest());
}

// Synthetic runtime receipt DTO fixture only; never a historical seed receipt or
// evidence of a real commit. Production has no receipt insertion in this slice.
fn receipt_fixture(owner: &ScenarioAccount, candidate: &ExecutionCandidate) -> CommittedExecution {
    let template = &candidate.template;
    let snapshot = FinancialSnapshot::BtcEth(owner.reservation().unwrap());
    CommittedExecution {
        order_after: owner.orders[&template.order_id].facts.clone(),
        order_created_at: owner.orders[&template.order_id].created_at,
        execution_effective_at: template.matching_effective_at,
        contract_value: d("0.01"),
        risk_decision_after: None,
        lifecycle_after: risk_transition::Lifecycle::RiskStable,
        episode_after: None,
        account_key: owner.key.clone(),
        execution_id: template.key.canonical_id(),
        financial_payload_digest: template.digest(),
        event_id: candidate.event_id.clone(),
        commit_sequence: 2,
        state_version_before: 0,
        state_version_after: 1,
        order_version_before: 0,
        order_version_after: 1,
        product: template.key.product.clone(),
        order_id: template.order_id.clone(),
        quantity: template.quantity,
        price: template.price,
        fee_asset: "USDT".into(),
        fee_amount: d("0.2500005"),
        realized_pnl_delta: d("0.0005"),
        cash_deltas: vec![("USDT".into(), d("-0.2495005"))],
        position_before: owner.positions.btc().unwrap().get(&Product::Btc).cloned(),
        position_after: owner.positions.btc().unwrap().get(&Product::Btc).cloned(),
        reservation_before: snapshot.clone(),
        reservation_after: snapshot,
        spec_version: candidate.spec_version.clone(),
        rule_data_version: candidate.rule_data_version.clone(),
        risk_state_after: ProfileRisk::BtcEth(MaintenanceState::Safe),
        pending_action_ids: Vec::new(),
        order_type: admission::OrderType::Limit,
    }
}

#[test]
fn runtime_lookup_precedes_gate_context_versions_and_delivery_metadata() {
    let (mut owner, candidate) = fixture_candidate();
    assert!(owner.execution_receipts.is_empty());
    let receipt = receipt_fixture(&owner, &candidate);
    owner
        .execution_receipts
        .insert(receipt.execution_id, receipt.clone());
    owner.gate = Gate::Failed("UNSUPPORTED_RISK_TRANSITION");
    owner.state_version = 99;
    let before = owner.clone();
    let mut redelivery = candidate.clone();
    redelivery.source_id = "transport-B".into();
    redelivery.visible_at = 3000;
    redelivery.event_id = "redelivery".into();
    redelivery.candidate_id = "another-candidate".into();
    redelivery.expected_account_version = 42;
    redelivery.expected_order_version = 42;
    redelivery.spec_version = "stale".into();
    redelivery.rule_data_version = "stale".into();
    assert_eq!(
        owner.prepare_execution(&redelivery),
        Ok(Preparation::Duplicate(&receipt))
    );
    redelivery.template.quantity = d("0.4");
    assert_eq!(
        owner.prepare_execution(&redelivery),
        Err("EXECUTION_ID_CONFLICT")
    );
    redelivery.template = candidate.template.clone();
    redelivery.template.matching_effective_at = 1000;
    assert_eq!(
        owner.prepare_execution(&redelivery),
        Err("EXECUTION_ID_CONFLICT")
    );
    redelivery.template.key.external_id = "new".into();
    assert_eq!(owner.prepare_execution(&redelivery), Err("RUN_FAILED"));
    assert_eq!(owner, before);
}

#[test]
fn seed_collision_precedes_lookup_and_seed_target_is_legal() {
    let (mut owner, mut candidate) = fixture_candidate();
    let before = owner.clone();
    assert!(matches!(
        owner.prepare_execution(&candidate),
        Ok(Preparation::Eligible { .. })
    ));
    assert_eq!(owner, before);
    candidate.template.key.namespace = "seed".into();
    candidate.template.quantity = d("0");
    assert_eq!(
        owner.prepare_execution(&candidate),
        Err("SEED_IDENTITY_CONFLICT")
    );
    let mut actual = owner.clone();
    let mut expected = owner.clone();
    expected.gate = Gate::Failed("SEED_IDENTITY_CONFLICT");
    assert_eq!(actual.execute(&candidate), Err("SEED_IDENTITY_CONFLICT"));
    assert_eq!(actual, expected);
    assert!(owner.execution_receipts.is_empty());
    candidate.template.key.namespace = "synthetic-v1".into();
    // Defensive canonical collision, even if a runtime record were inconsistent.
    let receipt = receipt_fixture(&owner, &candidate);
    owner.seed_executions.insert(receipt.execution_id);
    owner
        .execution_receipts
        .insert(receipt.execution_id, receipt);
    let before = owner.clone();
    assert_eq!(
        owner.prepare_execution(&candidate),
        Err("SEED_IDENTITY_CONFLICT")
    );
    assert_eq!(owner, before);
}

#[test]
fn symmetric_full_remainder_matrix_precedes_malformed_candidate() {
    let (baseline, candidate) = fixture_candidate();
    for position_side in [None, Some(Side::Long), Some(Side::Short)] {
        for order_side in [Side::Long, Side::Short] {
            for reduce_only in [false, true] {
                for remaining in ["1", "3", "4"] {
                    let mut owner = baseline.clone();
                    if let Some(side) = position_side {
                        owner
                            .positions
                            .btc_mut()
                            .unwrap()
                            .get_mut(&Product::Btc)
                            .unwrap()
                            .side = side;
                    } else {
                        owner.positions.btc_mut().unwrap().clear();
                    }
                    let facts = &mut owner.orders.get_mut("O1").unwrap().facts;
                    facts.side = order_side;
                    facts.reduce_only = reduce_only;
                    facts.original = d(remaining);
                    facts.filled = d("0");
                    facts.remaining = d(remaining);
                    facts.status = "OPEN".into();
                    let opposing = position_side.is_some_and(|side| side != order_side);
                    let expected = if reduce_only && (!opposing || remaining == "4") {
                        Err("REDUCE_ONLY_NOT_REDUCING")
                    } else if opposing && remaining == "4" {
                        Err("EXECUTION_INELIGIBLE_REMAINDER_EXCEEDS_POSITION")
                    } else {
                        Ok(if opposing {
                            RemainderRole::Reducing
                        } else {
                            RemainderRole::Increasing
                        })
                    };
                    assert_eq!(
                        remainder_eligibility(
                            facts,
                            owner.positions.btc().unwrap().get(&Product::Btc)
                        ),
                        expected
                    );
                    let before = owner.clone();
                    for (qty, price) in [("0", "0"), ("0.5", "50000.1"), ("100", "1")] {
                        let mut input = candidate.clone();
                        input.template.side = order_side;
                        input.template.quantity = d(qty);
                        input.template.price = d(price);
                        let result = owner.prepare_execution(&input);
                        match expected {
                            Err(reason) => assert_eq!(result, Ok(Preparation::Rejected(reason))),
                            Ok(_) if qty == "0.5" => {
                                assert!(matches!(result, Ok(Preparation::Eligible { .. })))
                            }
                            Ok(_) => assert_eq!(result, Err("UNSUPPORTED_EXECUTION")),
                        }
                        if let Err(reason) = expected {
                            assert_eq!(owner.execute(&input), Ok(commit::Reply::Rejected(reason)));
                        }
                        assert_eq!(owner, before);
                    }
                }
            }
        }
    }
}

#[test]
fn fatal_validation_matrix_is_read_only_and_ordered() {
    let (owner, candidate) = fixture_candidate();
    let mutations: &[fn(&mut ExecutionCandidate)] = &[
        |c| c.template.key.account.account.push('x'),
        |c| c.template.key.product = ProfileProduct::Pa,
        |c| c.template.key.namespace.clear(),
        |c| c.template.key.external_id.clear(),
        |c| c.template.side = Side::Long,
        |c| c.template.quantity = d("0"),
        |c| c.template.quantity = d("-1"),
        |c| c.template.quantity = d("2"),
        |c| c.template.quantity = d("0.001"),
        |c| c.template.price = d("0"),
        |c| c.template.price = d("50000.01"),
        |c| c.template.price = d("50000"),
        |c| c.template.liquidity = LiquidityRole::Unsupported,
        |c| c.template.fee_asset = Some("USDT".into()),
        |c| c.template.fee_amount = Some(d("0")),
        |c| c.spec_version.clear(),
        |c| c.rule_data_version.clear(),
        |c| c.event_id.clear(),
        |c| c.candidate_id.clear(),
    ];
    for change in mutations {
        let mut input = candidate.clone();
        change(&mut input);
        assert_eq!(
            owner.prepare_execution(&input),
            Err("UNSUPPORTED_EXECUTION")
        );
        let mut actual = owner.clone();
        let mut expected = owner.clone();
        expected.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
        assert_eq!(actual.execute(&input), Err("UNSUPPORTED_EXECUTION"));
        assert_eq!(actual, expected);
    }
    for time in [-1, 1000, 2000] {
        let mut input = candidate.clone();
        input.template.matching_effective_at = time;
        input.template.order_id = "missing".into();
        assert_eq!(
            owner.prepare_execution(&input),
            Err("UNSUPPORTED_CONTEXT_TRANSITION")
        );
        let mut actual = owner.clone();
        let mut expected = owner.clone();
        expected.gate = Gate::Failed("UNSUPPORTED_CONTEXT_TRANSITION");
        assert_eq!(
            actual.execute(&input),
            Err("UNSUPPORTED_CONTEXT_TRANSITION")
        );
        assert_eq!(actual, expected);
    }
    let mut input = candidate.clone();
    input.template.order_id = "missing".into();
    assert_eq!(owner.prepare_execution(&input), Err("UNKNOWN_ORDER"));
    for account_version in [false, true] {
        let mut input = candidate.clone();
        if account_version {
            input.expected_account_version = 1;
        } else {
            input.expected_order_version = 1;
        }
        input.template.quantity = d("0");
        assert_eq!(owner.prepare_execution(&input), Err("STALE_VERSION"));
    }
    let (original, _) = fixture_candidate();
    assert_eq!(owner, original);
}

#[test]
fn terminal_projection_matrix_keeps_owner_facts_and_excludes_remainders() {
    let (baseline, candidate) = fixture_candidate();
    for (status, filled, remaining, valid) in [
        ("OPEN", "0", "2", true),
        ("PARTIALLY_FILLED", "1", "1", true),
        ("FILLED", "2", "0", true),
        ("FILLED", "1", "1", false),
        ("OPEN", "2", "0", false),
        ("PARTIALLY_FILLED", "2", "0", false),
        ("OPEN", "1", "1", false),
        ("PARTIALLY_FILLED", "0", "2", false),
        ("CANCELED", "2", "0", false),
        ("UNKNOWN", "0", "2", false),
        ("FILLED", "1", "0", false),
        ("OPEN", "-1", "3", false),
    ] {
        let mut owner = baseline.clone();
        let facts = &mut owner.orders.get_mut("O1").unwrap().facts;
        facts.status = status.into();
        facts.filled = d(filled);
        facts.remaining = d(remaining);
        facts.reduce_only = false;
        let before = owner.clone();
        let snapshot = owner.reservation();
        assert_eq!(snapshot.is_ok(), valid, "{status}/{filled}/{remaining}");
        if valid {
            assert_eq!(
                snapshot.unwrap().orders.len(),
                usize::from(remaining != "0")
            );
        }
        if status == "FILLED" && valid {
            assert_eq!(
                owner.prepare_execution(&candidate),
                Err("UNSUPPORTED_EXECUTION")
            );
            let mut without = owner.clone();
            without.orders.clear();
            assert_eq!(owner.reservation(), without.reservation());
        }
        assert_eq!(owner, before);
    }
}
