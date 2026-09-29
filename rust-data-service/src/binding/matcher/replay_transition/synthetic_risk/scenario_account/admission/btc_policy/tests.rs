use super::super::super::tests::{d, fixture};
use super::*;
mod configured_fee;
mod configured_fee_causality;

#[test]
fn a04_causal_mark_admission_shortfall_request_effect_versions() {
    use super::super::super::context::tests::{activation, mark_rows, stamp};
    use super::super::super::risk_transition::{cancel, Lifecycle};
    let (mut seed, config, mut marks) = seed("1000", "1", Side::Long);
    seed.positions[0].product = Product::Eth;
    seed.positions[0].lots[0].entry = d("2000");
    marks[1].price = d("2000");
    marks[1].valid_to = 600;
    marks.extend([
        Mark {
            product: Product::Eth,
            price: d("3000"),
            valid_from: 600,
            valid_to: 700,
        },
        Mark {
            product: Product::Eth,
            price: d("2000"),
            valid_from: 700,
            valid_to: 3000,
        },
    ]);
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    owner
        .activate_context(&activation(&owner, 600, mark_rows(&marks, 600)))
        .unwrap();
    let mut candidate = intent(&owner, "20", Side::Long);
    candidate.requested_at = 600;
    let reply = owner
        .admit(&Envelope {
            event_id: "admit",
            effective_at: 600,
            intent: &candidate,
        })
        .unwrap();
    assert_eq!(reply.kind, ReplyKind::Accepted);
    assert_eq!(owner.state_version, 2);
    assert_eq!(owner.reservation().unwrap().available_margin, d("60"));
    let mut lower = activation(&owner, 700, mark_rows(&marks, 700));
    lower.stamp.event_id = "lower".into();
    let receipt = owner.activate_context(&lower).unwrap();
    assert_eq!(receipt.after.available_margin, d("-30"));
    assert_eq!(owner.state_version, 4);
    assert_eq!(
        owner.transition.lifecycle,
        Lifecycle::AwaitingCancelEffective
    );
    let order = reply.result.order_id.unwrap();
    owner
        .effect_cancel(&cancel::EffectInput {
            stamp: stamp("effective", 700, 50),
            effects: vec![("lower".into(), order.clone(), cancel::Reason::RiskShortfall)],
        })
        .unwrap();
    assert_eq!(owner.state_version, 5);
    assert_eq!(owner.orders[&order].version, 3);
    assert_eq!(owner.reservation().unwrap().available_margin, d("980"));
    assert_eq!(owner.fees, Decimal::ZERO);
    assert_eq!(owner.cash, d("1000"));
}

fn seed(cash: &str, contracts: &str, side: Side) -> (CleanSeed, FrozenScenario, Vec<Mark>) {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d(cash);
    seed.orders.clear();
    marks[0].price = d("50000");
    if contracts == "0" {
        seed.positions.clear();
    } else {
        let position = &mut seed.positions[0];
        position.side = side;
        position.contracts = d(contracts);
        position.lots.truncate(1);
        position.lots[0].contracts = d(contracts);
    }
    (seed, config, marks)
}

fn intent(owner: &ScenarioAccount, quantity: &str, side: Side) -> OrderIntent {
    OrderIntent {
        intent_id: "runtime".into(),
        client_order_id: "client".into(),
        account_key: owner.key.clone(),
        config_id: owner.config_id.clone(),
        product: ProfileProduct::BtcEth(Product::Btc),
        strategy_id: "strategy".into(),
        side,
        order_type: OrderType::Limit,
        quantity: d(quantity),
        limit_price: Some(d("50000")),
        reduce_only: false,
        requested_at: 500,
    }
}

fn submit(owner: &mut ScenarioAccount, intent: &OrderIntent) -> Reply {
    owner
        .admit(&Envelope {
            intent,
            event_id: &format!("BTC-{}", intent.intent_id),
            effective_at: 500 + owner.state_version as i64,
        })
        .unwrap()
}

fn evidence(reply: &Reply) -> &Evidence {
    match &reply.result.evaluation {
        Evaluation::BtcEth(value) => value,
        Evaluation::GoldenCapacity(_) | Evaluation::GoldenCancel(_) | Evaluation::MinCash(_) => {
            panic!("BTC evidence required")
        }
    }
}

#[test]
fn a04_only_eth_mark_changes_shared_equity_admission() {
    let (mut seed, config, mut marks) = seed("1000", "1", Side::Long);
    seed.positions[0].product = Product::Eth;
    seed.positions[0].lots[0].entry = d("2000");
    let mut same_intent = None;
    for (mark, available, accepted) in [("2000", "-30", false), ("3000", "60", true)] {
        marks[1].price = d(mark);
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let candidate = intent(&owner, "20", Side::Long);
        if let Some(prior) = &same_intent {
            assert_eq!(prior, &candidate);
        }
        same_intent = Some(candidate.clone());
        let before = owner.clone();
        let reply = submit(&mut owner, &candidate);
        let value = evidence(&reply);
        assert_eq!(value.role, Role::RiskIncreasing);
        assert_eq!(
            value.post_reservation.as_ref().unwrap().available_margin,
            d(available)
        );
        assert_eq!(value.stress, None);
        assert_eq!(reply.result.spec_version, "spec-v1");
        assert_eq!(reply.result.rule_data_version, "tier-v1");
        assert_eq!(owner.state_version, u64::from(accepted));
        assert_eq!(owner.orders.len(), usize::from(accepted));
        assert_eq!(
            reply.result.reason_code,
            (!accepted).then_some("INSUFFICIENT_SHARED_EQUITY")
        );
        assert_eq!(owner.positions, before.positions);
        assert_eq!(owner.cash, before.cash);
        assert_eq!(owner.intent_results[&candidate.intent_id], reply.result);
        if accepted {
            assert_eq!(
                reply.result.reservation_after,
                Some(reply.result.evaluation.clone())
            );
        } else {
            assert_eq!(reply.result.reservation_after, None);
        }
    }
}

#[test]
fn c03_atomic_reservation_and_both_receipts_survive_later_versions() {
    let (seed, config, marks) = seed("1000", "0", Side::Long);
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let first = intent(&owner, "10", Side::Long);
    let accepted = submit(&mut owner, &first);
    assert_eq!(accepted.kind, ReplyKind::Accepted);
    assert_eq!(
        evidence(&accepted)
            .post_reservation
            .as_ref()
            .unwrap()
            .available_margin,
        d("495")
    );
    let mut second = first.clone();
    second.intent_id = "second".into();
    let before = owner.clone();
    let rejected = submit(&mut owner, &second);
    assert_eq!(rejected.kind, ReplyKind::Rejected);
    assert_eq!(
        evidence(&rejected)
            .post_reservation
            .as_ref()
            .unwrap()
            .available_margin,
        d("-10")
    );
    let mut expected = before;
    expected
        .intent_results
        .insert(second.intent_id.clone(), rejected.result.clone());
    expected.publish_source(
        &source::stamp(
            &format!("BTC-{}", second.intent_id),
            500 + expected.state_version as i64,
            60,
        ),
        second.digest(),
        source::Kind::Intent,
        false,
    );
    assert_eq!(owner, expected);
    let mut third = first.clone();
    third.intent_id = "third".into();
    third.quantity = d("1");
    assert_eq!(submit(&mut owner, &third).kind, ReplyKind::Accepted);
    assert_eq!(owner.state_version, 2);
    let before = owner.clone();
    for (candidate, original) in [(&first, accepted), (&second, rejected)] {
        let duplicate = owner
            .admit(&Envelope {
                intent: candidate,
                event_id: "late",
                effective_at: 1000,
            })
            .unwrap();
        assert_eq!(duplicate.kind, ReplyKind::Duplicate);
        assert_eq!(duplicate.result, original.result);
        assert_eq!(owner, before);
    }
}

#[test]
fn single_role_classifier_and_actual_path_cover_both_sides_without_clipping() {
    for (side, opposing) in [(Side::Long, Side::Short), (Side::Short, Side::Long)] {
        let cases = [
            ("0", side, "1", false, Role::RiskIncreasing),
            (
                "0",
                side,
                "1",
                true,
                Role::Rejected("REDUCE_ONLY_NOT_REDUCING"),
            ),
            ("2", side, "1", false, Role::RiskIncreasing),
            (
                "2",
                side,
                "1",
                true,
                Role::Rejected("REDUCE_ONLY_NOT_REDUCING"),
            ),
            ("2", opposing, "1", false, Role::RiskReducing),
            ("2", opposing, "2", false, Role::RiskReducing),
            ("2", opposing, "2", true, Role::RiskReducing),
            (
                "2",
                opposing,
                "3",
                false,
                Role::Rejected("POSITION_CROSS_ZERO_UNSUPPORTED"),
            ),
            (
                "2",
                opposing,
                "3",
                true,
                Role::Rejected("REDUCE_ONLY_NOT_REDUCING"),
            ),
        ];
        for (contracts, order_side, quantity, reduce_only, role) in cases {
            let (seed, config, marks) = seed("10000", contracts, side);
            let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
            let mut candidate = intent(&owner, quantity, order_side);
            candidate.reduce_only = reduce_only;
            assert_eq!(
                classify(
                    owner
                        .positions
                        .btc()
                        .unwrap()
                        .get(&Product::Btc)
                        .map(|p| (p.side, p.contracts)),
                    order_side,
                    d(quantity),
                    reduce_only
                ),
                role
            );
            let before = owner.clone();
            let reply = submit(&mut owner, &candidate);
            assert_eq!(evidence(&reply).role, role);
            assert_eq!(owner.gate, Gate::Running);
            assert_eq!(owner.positions, before.positions);
            assert_eq!(
                (owner.cash, owner.fees, owner.gross_realized),
                (before.cash, before.fees, before.gross_realized)
            );
            if let Role::Rejected(reason) = role {
                assert_eq!(reply.result.reason_code, Some(reason));
                assert_eq!(owner.state_version, 0);
                assert!(owner.orders.is_empty());
                assert_eq!(evidence(&reply).post_reservation, None);
            } else {
                assert_eq!(reply.kind, ReplyKind::Accepted);
                let order = owner.orders.values().next().unwrap();
                assert_eq!(
                    (order.facts.remaining, order.facts.reduce_only),
                    (d(quantity), reduce_only)
                );
                assert_eq!((owner.state_version, order.version), (1, 1));
            }
        }
    }
}

#[test]
fn reducing_exception_reserves_fee_but_does_not_apply_settlement() {
    for (position_side, order_side) in [(Side::Long, Side::Short), (Side::Short, Side::Long)] {
        for (quantity, equity, mmr, excess, available, fee) in [
            ("0.5", "39.75", "1", "38.75", "-10.25", "0.25"),
            ("1", "39.5", "0", "39.5", "-10.5", "0.5"),
        ] {
            let (seed, config, marks) = seed("40", "1", position_side);
            let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
            let before = owner.clone();
            let mut candidate = intent(&owner, quantity, order_side);
            candidate.reduce_only = quantity == "0.5";
            let reply = submit(&mut owner, &candidate);
            assert_eq!(reply.kind, ReplyKind::Accepted);
            let value = evidence(&reply);
            assert_eq!(
                value.post_reservation.as_ref().unwrap().available_margin,
                d(available)
            );
            assert_eq!(
                value.stress,
                Some(Stress {
                    equity: d(equity),
                    maintenance_margin: d(mmr),
                    excess: d(excess),
                    current_excess: d("38"),
                    fee: d(fee)
                })
            );
            assert_eq!(owner.positions, before.positions);
            assert_eq!(
                (
                    owner.cash,
                    owner.fees,
                    owner.gross_realized,
                    owner.commit_sequence
                ),
                (d("40"), d("0"), d("0"), before.commit_sequence)
            );
        }
    }
}

#[test]
fn each_strict_stress_inequality_is_required() {
    for (cash, mark, price, equity, mmr, excess, available, fee) in [
        (
            "40", "49950", "49800", "38.501", "0.999", "37.502", "-11.449", "0.249",
        ),
        ("3.248", "50000", "49600", "1", "1", "1.248", "-49", "0.248"),
    ] {
        let (mut seed, config, mut marks) = seed(cash, "1", Side::Long);
        marks[0].price = d(mark);
        seed.effective_at = 500;
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let before = owner.clone();
        let mut candidate = intent(&owner, "0.5", Side::Short);
        candidate.limit_price = Some(d(price));
        let reply = submit(&mut owner, &candidate);
        assert_eq!(reply.result.reason_code, Some("INSUFFICIENT_SHARED_EQUITY"));
        let value = evidence(&reply);
        assert_eq!(
            value.post_reservation.as_ref().unwrap().available_margin,
            d(available)
        );
        let stress = value.stress.as_ref().unwrap();
        assert_eq!(
            (
                stress.equity,
                stress.maintenance_margin,
                stress.current_excess,
                stress.fee
            ),
            (d(equity), d(mmr), d(excess), d(fee))
        );
        if cash == "40" {
            assert_eq!(stress.excess, stress.current_excess);
        } else {
            assert_eq!(stress.equity, stress.maintenance_margin);
        }
        let mut expected = before;
        expected
            .intent_results
            .insert(candidate.intent_id.clone(), reply.result);
        expected.publish_source(
            &source::stamp(
                &format!("BTC-{}", candidate.intent_id),
                500 + expected.state_version as i64,
                60,
            ),
            candidate.digest(),
            source::Kind::Intent,
            false,
        );
        assert_eq!(owner, expected);
    }
}

#[test]
fn current_breach_precedes_roles_and_candidate_arithmetic() {
    let (seed, config, marks) = seed("40", "1", Side::Long);
    for (side, quantity, reduce_only) in [
        (Side::Long, "1", false),
        (Side::Short, "0.5", false),
        (Side::Short, "0.5", true),
        (Side::Long, "1", true),
    ] {
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        owner.cash = d("2"); // Reachable future fee/loss state, not a new seed capability.
        let mut candidate = intent(&owner, quantity, side);
        candidate.reduce_only = reduce_only;
        if side == Side::Long && !reduce_only {
            candidate.quantity = Decimal::MAX;
        }
        let before = owner.clone();
        let reply = submit(&mut owner, &candidate);
        assert_eq!(reply.result.reason_code, Some("MMR_BREACH"));
        assert_eq!(evidence(&reply).current_risk, MaintenanceState::Breach);
        assert_eq!(evidence(&reply).post_reservation, None);
        let mut expected = before;
        expected
            .intent_results
            .insert(candidate.intent_id.clone(), reply.result);
        expected.publish_source(
            &source::stamp(
                &format!("BTC-{}", candidate.intent_id),
                500 + expected.state_version as i64,
                60,
            ),
            candidate.digest(),
            source::Kind::Intent,
            false,
        );
        assert_eq!(owner, expected);
    }
}

#[test]
fn lot_aware_stress_preserves_nonterminating_entry_basis_and_fifo() {
    let (mut seed, config, marks) = fixture();
    seed.cash = d("100");
    seed.orders.clear();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let before = owner.clone();
    assert_eq!(owner.reservation().unwrap().equity, d("100.028"));
    let mut candidate = intent(&owner, "1", Side::Short);
    candidate.limit_price = Some(d("50001"));
    let reply = submit(&mut owner, &candidate);
    assert_eq!(reply.kind, ReplyKind::Accepted);
    let stress = evidence(&reply).stress.as_ref().unwrap();
    assert_eq!(
        (stress.equity, stress.maintenance_margin, stress.fee),
        (d("99.52799"), d("4.00008"), d("0.50001"))
    );
    assert_eq!(owner.positions, before.positions);
    assert_eq!(owner.cash, before.cash);
}

#[test]
fn invalid_btc_inputs_and_faults_leave_only_failed_gate() {
    let (seed, config, marks) = seed("1000", "0", Side::Long);
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let base = intent(&owner, "1", Side::Long);
    let cases: &[(fn(&mut OrderIntent), Fault)] = &[
        (|i| i.quantity = d("0"), "INVALID_BTC_INTENT"),
        (|i| i.quantity = d("0.001"), "INVALID_BTC_INTENT"),
        (|i| i.quantity = d("-1"), "INVALID_BTC_INTENT"),
        (
            |i| i.limit_price = Some(d("50000.01")),
            "INVALID_BTC_INTENT",
        ),
        (|i| i.limit_price = Some(d("0")), "INVALID_BTC_INTENT"),
        (|i| i.quantity = Decimal::MAX, "DECIMAL_OVERFLOW"),
        (|i| i.product = ProfileProduct::Pa, "PROFILE_MISMATCH"),
    ];
    for (change, fault) in cases {
        let mut candidate = base.clone();
        change(&mut candidate);
        let mut changed = owner.clone();
        assert_eq!(
            changed.admit(&Envelope {
                intent: &candidate,
                event_id: "E",
                effective_at: 500
            }),
            Err(*fault)
        );
        let mut expected = owner.clone();
        expected.gate = Gate::Failed(fault);
        assert_eq!(changed, expected);
    }
    for at in [1000, 2000] {
        let mut changed = owner.clone();
        assert_eq!(
            changed.admit(&Envelope {
                intent: &base,
                event_id: "E",
                effective_at: at
            }),
            Err("UNSUPPORTED_CONTEXT_TRANSITION")
        );
        let mut expected = owner.clone();
        expected.gate = Gate::Failed("UNSUPPORTED_CONTEXT_TRANSITION");
        assert_eq!(changed, expected);
    }
}

#[test]
fn eth_versions_account_isolation_and_post_policy_fault_are_exact() {
    let (mut seed, config, marks) = seed("1000", "0", Side::Long);
    seed.effective_at = 2000;
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let mut eth = intent(&owner, "1", Side::Long);
    eth.product = ProfileProduct::BtcEth(Product::Eth);
    eth.limit_price = Some(d("1900"));
    let envelope = Envelope {
        intent: &eth,
        event_id: "ETH",
        effective_at: 2000,
    };
    let mut accepted = owner.clone();
    let reply = accepted.admit(&envelope).unwrap();
    assert_eq!(reply.kind, ReplyKind::Accepted);
    assert_eq!(
        (
            &reply.result.spec_version[..],
            &reply.result.rule_data_version[..]
        ),
        ("spec-v2", "tier-v2")
    );
    assert_eq!(
        evidence(&reply)
            .post_reservation
            .as_ref()
            .unwrap()
            .available_margin,
        d("980.81")
    );
    assert!(owner.orders.is_empty());
    seed.key.account = "B".into();
    let mut other = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let mut other_intent = eth.clone();
    other_intent.account_key = seed.key;
    let other_reply = other
        .admit(&Envelope {
            intent: &other_intent,
            event_id: "ETH",
            effective_at: 2000,
        })
        .unwrap();
    assert_ne!(reply.result.order_id, other_reply.result.order_id);
    let first_value = evidence(&reply).post_reservation.as_ref().unwrap();
    let other_value = evidence(&other_reply).post_reservation.as_ref().unwrap();
    assert_eq!(first_value.products, other_value.products);
    assert_eq!(first_value.available_margin, other_value.available_margin);
    for panic in [false, true] {
        let mut failed = owner.clone();
        let fault = if panic {
            "ADMISSION_PANIC"
        } else {
            "BTC_POST_POLICY_FAULT"
        };
        assert_eq!(
            failed.admit_checked(&envelope, |stage| {
                if stage == PrepareStage::ResultDrafted {
                    assert!(!panic, "injected post-policy panic");
                    return Err("BTC_POST_POLICY_FAULT");
                }
                Ok(())
            }),
            Err(fault)
        );
        let mut expected = owner.clone();
        expected.gate = Gate::Failed(fault);
        assert_eq!(failed, expected);
    }
}

#[test]
fn opposing_excess_precedes_financial_overflow() {
    let (seed, config, marks) = seed("1000", "1", Side::Long);
    for reduce_only in [false, true] {
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let mut candidate = intent(&owner, "1", Side::Short);
        candidate.quantity = Decimal::MAX;
        candidate.reduce_only = reduce_only;
        let reply = submit(&mut owner, &candidate);
        let reason = if reduce_only {
            "REDUCE_ONLY_NOT_REDUCING"
        } else {
            "POSITION_CROSS_ZERO_UNSUPPORTED"
        };
        assert_eq!(reply.result.reason_code, Some(reason));
        assert_eq!(evidence(&reply).post_reservation, None);
        assert_eq!(
            (owner.state_version, owner.orders.len(), owner.gate),
            (0, 0, Gate::Running)
        );
    }
}
