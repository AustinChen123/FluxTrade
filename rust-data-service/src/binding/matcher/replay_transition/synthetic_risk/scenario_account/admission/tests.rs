use super::super::tests::{d, fixture};
use super::*;

fn setup(program: capacity::Program) -> (ScenarioAccount, OrderIntent) {
    let (mut seed, _, _) = fixture();
    seed.positions.clear();
    seed.orders.clear();
    seed.cash = d("1000");
    let owner =
        ScenarioAccount::from_capacity_seed(&seed, &capacity::Config::frozen(program)).unwrap();
    let intent = OrderIntent {
        intent_id: "I1".into(),
        client_order_id: "C1".into(),
        account_key: seed.key,
        config_id: seed.config_id,
        product: ProfileProduct::Pa,
        strategy_id: "strategy".into(),
        side: Side::Long,
        order_type: OrderType::Limit,
        quantity: d("6"),
        limit_price: Some(d("100")),
        reduce_only: false,
        requested_at: 500,
    };
    (owner, intent)
}

fn event(intent: &OrderIntent) -> Envelope<'_> {
    Envelope {
        event_id: "E1",
        effective_at: 500,
        intent,
    }
}

fn assert_fatal(mut owner: ScenarioAccount, intent: &OrderIntent, at: i64, expected: Fault) {
    let mut before = owner.clone();
    assert_eq!(
        owner.admit(&Envelope {
            effective_at: at,
            ..event(intent)
        }),
        Err(expected)
    );
    before.gate = Gate::Failed(expected);
    assert_eq!(owner, before);
}

#[test]
fn capacity_trace_caches_both_outcomes_and_conflict_stops_run() {
    let (mut owner, first) = setup(capacity::Program::CapacityV1);
    let accepted = owner.admit(&event(&first)).unwrap();
    assert_eq!(accepted.kind, ReplyKind::Accepted);
    assert_eq!(
        (
            accepted.result.account_version_before,
            accepted.result.account_version_after
        ),
        (0, 1)
    );
    assert_eq!(
        (
            accepted.result.order_version_before,
            accepted.result.order_version_after
        ),
        (None, Some(1))
    );
    assert_eq!(accepted.result.created_at_event_id, "E1");
    assert_eq!(accepted.result.spec_version, "capacity-spec-v1");
    assert_eq!(accepted.result.rule_data_version, "capacity-data-v1");
    let order = owner
        .target_order(accepted.result.order_id.as_ref().unwrap())
        .unwrap();
    assert_eq!(
        (
            order.version,
            order.facts.original,
            order.facts.filled,
            order.facts.remaining
        ),
        (1, d("6"), d("0"), d("6"))
    );
    assert_eq!(order.facts.product, ProfileProduct::Pa);
    assert_eq!(order.facts.status, "OPEN");
    let mut second = first.clone();
    second.intent_id = "I2".into();
    second.quantity = d("5");
    let before = owner.clone();
    let rejected = owner.admit(&event(&second)).unwrap();
    assert_eq!(rejected.kind, ReplyKind::Rejected);
    assert_eq!(rejected.result.reason_code, Some("CAPACITY_EXCEEDED"));
    assert_eq!(
        (
            rejected.result.account_version_before,
            rejected.result.account_version_after
        ),
        (1, 1)
    );
    assert_eq!(
        (
            rejected.result.order_version_before,
            rejected.result.order_version_after
        ),
        (None, None)
    );
    assert!(rejected.result.order_id.is_none() && rejected.result.reservation_after.is_none());
    let Evaluation::GoldenCapacity(projection) = &rejected.result.evaluation;
    assert_eq!(projection.required, d("1100"));
    let mut expected = before;
    expected
        .intent_results
        .insert(second.intent_id.clone(), rejected.result.clone());
    assert_eq!(owner, expected);
    let duplicate = owner
        .admit(&Envelope {
            effective_at: 1000,
            event_id: "late",
            intent: &second,
        })
        .unwrap();
    assert_eq!(
        duplicate,
        Reply {
            kind: ReplyKind::Duplicate,
            result: rejected.result
        }
    );
    assert_eq!(owner, expected);
    second.quantity = d("4");
    assert_eq!(
        owner.admit(&event(&second)),
        Err("IDEMPOTENCY_KEY_CONFLICT")
    );
    expected.gate = Gate::Failed("IDEMPOTENCY_KEY_CONFLICT");
    assert_eq!(owner, expected);
    assert_eq!(owner.admit(&event(&first)), Err("RUN_FAILED"));
}

#[test]
fn c02_same_inputs_only_program_changes_and_original_receipts_survive_versions() {
    let (mut v1, intent) = setup(capacity::Program::CapacityV1);
    let (mut tight, same) = setup(capacity::Program::CapacityTightV1);
    assert_eq!(intent, same);
    assert_eq!(
        (v1.key.clone(), v1.config_id.clone()),
        (tight.key.clone(), tight.config_id.clone())
    );
    let accepted = v1.admit(&event(&intent)).unwrap();
    let rejected = tight.admit(&event(&same)).unwrap();
    assert_eq!(rejected.kind, ReplyKind::Rejected);
    let Evaluation::GoldenCapacity(a) = &accepted.result.evaluation;
    let Evaluation::GoldenCapacity(b) = &rejected.result.evaluation;
    assert_eq!((a.required, b.required), (d("600"), d("600")));
    assert_eq!((a.threshold, b.threshold), (d("1000"), d("500")));
    assert_ne!(a.program_hash, b.program_hash);
    assert_eq!((v1.state_version, tight.state_version), (1, 0));
    assert_eq!(rejected.result.outcome, Outcome::Rejected);
    assert!(tight.orders.is_empty());
    assert_eq!(
        accepted.result.reservation_after,
        Some(accepted.result.evaluation.clone())
    );
    let mut next = intent.clone();
    next.intent_id = "next".into();
    next.quantity = d("1");
    next.side = Side::Short;
    let next_result = v1.admit(&event(&next)).unwrap();
    assert_eq!(next_result.kind, ReplyKind::Accepted);
    let Evaluation::GoldenCapacity(next_projection) = next_result.result.evaluation;
    assert_eq!(next_projection.required, d("700"));
    assert_eq!(v1.state_version, 2);
    assert_eq!(v1.orders.len(), 2); // Reused client ID is not an idempotency key.
    let before = v1.clone();
    let mut canonical = intent.clone();
    canonical.quantity = d("6.000");
    canonical.limit_price = Some(d("100.000"));
    assert_eq!(
        v1.admit(&Envelope {
            effective_at: 1000,
            event_id: "later",
            intent: &canonical
        })
        .unwrap(),
        Reply {
            kind: ReplyKind::Duplicate,
            result: accepted.result
        }
    );
    assert_eq!(v1, before);
}

#[test]
fn complete_payload_digest_matrix_and_order_identity_are_canonical() {
    let (owner, intent) = setup(capacity::Program::CapacityV1);
    let changes: &[fn(&mut OrderIntent)] = &[
        |i| i.client_order_id.push('x'),
        |i| i.account_key.venue.push('x'),
        |i| i.account_key.environment.push('x'),
        |i| i.account_key.account.push('x'),
        |i| i.account_key.subaccount = Some("sub".into()),
        |i| i.config_id.push('x'),
        |i| i.product = ProfileProduct::BtcEth(Product::Btc),
        |i| i.strategy_id.push('x'),
        |i| i.side = Side::Short,
        |i| i.order_type = OrderType::Market,
        |i| i.quantity = d("4"),
        |i| i.limit_price = Some(d("101")),
        |i| i.limit_price = None,
        |i| i.reduce_only = true,
        |i| i.requested_at += 1,
    ];
    let mut accepted = owner.clone();
    accepted.admit(&event(&intent)).unwrap();
    for change in changes {
        let mut changed = intent.clone();
        change(&mut changed);
        assert_ne!(changed.digest(), intent.digest());
        assert_fatal(accepted.clone(), &changed, 1000, "IDEMPOTENCY_KEY_CONFLICT");
    }
    let mut other = intent.clone();
    other.intent_id.push('x');
    assert_ne!(other.digest(), intent.digest());
    assert_ne!(other.order_id(), intent.order_id());
    other = intent.clone();
    other.account_key.account.push('x');
    assert_ne!(other.order_id(), intent.order_id());
    assert_eq!(intent.order_id(), intent.clone().order_id());
    assert_ne!(intent.order_id(), intent.client_order_id);
    assert!(owner.orders.is_empty() && owner.intent_results.is_empty());
}

#[test]
fn invalid_input_context_and_seed_identity_precedence_are_atomic() {
    let (owner, intent) = setup(capacity::Program::CapacityV1);
    for at in [-1, 1000] {
        assert_fatal(owner.clone(), &intent, at, "UNSUPPORTED_CONTEXT_TRANSITION");
    }
    let cases: &[(fn(&mut OrderIntent), Fault)] = &[
        (|i| i.account_key.account.clear(), "INVALID_ACCOUNT_KEY"),
        (|i| i.account_key.account.push('x'), "ACCOUNT_MISMATCH"),
        (|i| i.config_id.clear(), "CONFIG_MISMATCH"),
        (|i| i.config_id.push('x'), "CONFIG_MISMATCH"),
        (|i| i.intent_id.clear(), "INVALID_ADMISSION_IDENTITY"),
        (|i| i.client_order_id.clear(), "INVALID_ADMISSION_IDENTITY"),
        (|i| i.strategy_id.clear(), "INVALID_ADMISSION_IDENTITY"),
        (
            |i| i.product = ProfileProduct::BtcEth(Product::Eth),
            "PROFILE_MISMATCH",
        ),
        (
            |i| i.order_type = OrderType::Market,
            "UNSUPPORTED_ORDER_TYPE",
        ),
        (
            |i| i.order_type = OrderType::Unsupported,
            "UNSUPPORTED_ORDER_TYPE",
        ),
        (|i| i.limit_price = None, "LIMIT_PRICE_REQUIRED"),
        (|i| i.reduce_only = true, "UNSUPPORTED_ADMISSION_ROLE"),
        (|i| i.quantity = d("0"), "INVALID_CAPACITY_CANDIDATE"),
        (|i| i.quantity = d("-1"), "INVALID_CAPACITY_CANDIDATE"),
        (|i| i.quantity = d("0.5"), "INVALID_CAPACITY_CANDIDATE"),
        (
            |i| i.limit_price = Some(d("0")),
            "INVALID_CAPACITY_CANDIDATE",
        ),
        (
            |i| i.limit_price = Some(d("100.1")),
            "INVALID_CAPACITY_CANDIDATE",
        ),
    ];
    for (change, reason) in cases {
        let mut invalid = intent.clone();
        change(&mut invalid);
        assert_fatal(owner.clone(), &invalid, 500, reason);
    }
    let (mut seed, scenario, marks) = fixture();
    let btc = ScenarioAccount::from_seed(&seed, &scenario, &marks).unwrap();
    let mut collision = intent.clone();
    collision.intent_id = seed.orders[0].intent_id.clone();
    assert_fatal(btc.clone(), &collision, 1000, "SEED_IDENTITY_CONFLICT");
    collision.intent_id = "new".into();
    seed.orders[0].order_id = collision.order_id();
    let btc = ScenarioAccount::from_seed(&seed, &scenario, &marks).unwrap();
    assert_fatal(btc, &collision, 500, "SEED_IDENTITY_CONFLICT");
    seed.orders.clear();
    let btc = ScenarioAccount::from_seed(&seed, &scenario, &marks).unwrap();
    collision.product = ProfileProduct::BtcEth(Product::Btc);
    assert_fatal(btc, &collision, 500, "UNSUPPORTED_ADMISSION_PROFILE");
}

#[test]
fn preparation_faults_panics_and_dropped_drafts_never_publish() {
    let (owner, intent) = setup(capacity::Program::CapacityV1);
    let prepared = owner
        .prepare_admission(&event(&intent), |_| Ok(()))
        .unwrap();
    assert_eq!(prepared.draft.orders.len(), 1);
    drop(prepared);
    assert!(owner.orders.is_empty() && owner.intent_results.is_empty());
    for stage in [
        PrepareStage::Validated,
        PrepareStage::OrderDrafted,
        PrepareStage::ResultDrafted,
    ] {
        for panic in [false, true] {
            let mut changed = owner.clone();
            let error = if panic {
                "ADMISSION_PANIC"
            } else {
                "INJECTED_PREPARE_ERROR"
            };
            assert_eq!(
                changed.admit_checked(&event(&intent), |current| {
                    if current == stage {
                        assert!(!panic, "injected admission panic");
                        return Err("INJECTED_PREPARE_ERROR");
                    }
                    Ok(())
                }),
                Err(error)
            );
            let mut expected = owner.clone();
            expected.gate = Gate::Failed(error);
            assert_eq!(changed, expected);
        }
    }
    let mut overflow = owner.clone();
    overflow.state_version = u64::MAX;
    assert_fatal(overflow, &intent, 500, "VERSION_OVERFLOW");
    let mut extreme = intent.clone();
    extreme.quantity = Decimal::MAX;
    assert_fatal(owner.clone(), &extreme, 500, "DECIMAL_OVERFLOW");
    let mut changed = owner.clone();
    changed.admit(&event(&intent)).unwrap();
    let mut next = intent.clone();
    next.intent_id = "next".into();
    let mut conflicting = changed.clone();
    let existing = conflicting.orders.values().next().unwrap().clone();
    conflicting.orders.insert(next.order_id(), existing);
    assert_fatal(conflicting, &next, 500, "ORDER_ID_CONFLICT");
    let mut invalid_facts = changed.clone();
    invalid_facts
        .orders
        .values_mut()
        .next()
        .unwrap()
        .facts
        .product = ProfileProduct::BtcEth(Product::Btc);
    assert_fatal(invalid_facts, &next, 500, "INVALID_CAPACITY_CANDIDATE");
    let mut invalid_event = owner.clone();
    assert_eq!(
        invalid_event.admit(&Envelope {
            event_id: "",
            ..event(&intent)
        }),
        Err("INVALID_ADMISSION_IDENTITY")
    );
    let mut expected = owner.clone();
    expected.gate = Gate::Failed("INVALID_ADMISSION_IDENTITY");
    assert_eq!(invalid_event, expected);
    assert!(owner.orders.is_empty() && owner.intent_results.is_empty());
}
