use super::super::tests::{d, fixture};
use super::*;

pub(in super::super) fn assert_snapshot(
    snapshot: &Snapshot,
    position: &str,
    remaining: &str,
    used: &str,
    available: &str,
) {
    assert_eq!(snapshot.position_value, d(position));
    assert_eq!(snapshot.long_remaining_value, d(remaining));
    assert_eq!(snapshot.short_remaining_value, d("0"));
    assert_eq!(
        (
            snapshot.exposure_margin,
            snapshot.used_margin,
            snapshot.available_margin
        ),
        (d(used), d(used), d(available))
    );
    assert_eq!(
        (snapshot.equity, snapshot.eligible_collateral),
        (d("1000"), d("1000"))
    );
    assert_eq!(
        (
            snapshot.initial_margin,
            snapshot.maintenance_margin,
            snapshot.total_order_loss,
            snapshot.total_fee_hold
        ),
        (d("0"), d("0"), d("0"), d("0"))
    );
    assert_eq!(snapshot.risk, ProfileRisk::CapacitySafe);
    let expected_orders = match remaining {
        "20" => vec!["O_A", "O_B"],
        "10" => vec!["O_B"],
        "0" => vec![],
        _ => panic!("closed C09 remainder required"),
    };
    assert_eq!(
        snapshot
            .orders
            .iter()
            .map(|row| row.order_id.as_str())
            .collect::<Vec<_>>(),
        expected_orders
    );
    for row in &snapshot.orders {
        assert_eq!(
            (
                row.remaining_contracts,
                row.remaining_base_exposure,
                row.order_loss,
                row.fee_hold
            ),
            (d("1"), d("1"), d("0"), d("0"))
        );
    }
}

pub(in super::super) fn seed() -> CleanSeed {
    let (mut seed, _, _) = fixture();
    seed.cash = d("1000");
    seed.positions.clear();
    let prototype = seed.orders[0].clone();
    seed.orders = ["A", "B"]
        .map(|suffix| SeedOrder {
            order_id: format!("O_{suffix}"),
            intent_id: format!("I_{suffix}"),
            client_id: format!("C_{suffix}"),
            product: ProfileProduct::Pa,
            side: Side::Long,
            original: d("1"),
            filled: d("0"),
            canceled: Decimal::ZERO,
            remaining: d("1"),
            price: d("10"),
            reduce_only: false,
            status: "OPEN".into(),
            ..prototype.clone()
        })
        .to_vec();
    seed
}

#[test]
fn closed_context_seed_and_exclusive_owner_are_deterministic() {
    let seed = seed();
    let mut hashes = BTreeSet::new();
    for program in [Program::V1, Program::V2] {
        let config = Config::frozen(program);
        let owner = ScenarioAccount::from_event_limit_seed(&seed, &config).unwrap();
        assert_eq!(
            owner,
            ScenarioAccount::from_event_limit_seed(&seed, &config).unwrap()
        );
        assert!(hashes.insert(owner.valuation_context_id));
        assert_eq!(owner.positions, PositionState::EventLimit(None));
        assert_eq!((owner.state_version, owner.commit_sequence), (0, 0));
        assert_eq!(
            (owner.cash, owner.gross_realized, owner.fees),
            (d("1000"), d("0"), d("0"))
        );
        assert_eq!(owner.gate, Gate::Running);
        assert!(
            owner.execution_receipts.is_empty()
                && owner.intent_results.is_empty()
                && owner.pending_actions.is_empty()
        );
        assert!(owner.orders.values().all(|o| o.version == 0));
        assert_eq!(owner.validate_context(999), Ok(()));
        for at in [-1, 1000] {
            assert_eq!(
                owner.validate_context(at),
                Err("UNSUPPORTED_CONTEXT_TRANSITION")
            );
        }
        assert_eq!(owner.projection(), Err("PROFILE_MISMATCH"));
        assert_eq!(owner.reservation(), Err("PROFILE_MISMATCH"));
        assert_eq!(
            owner.capacity_projection(&capacity::Candidate {
                product: ProfileProduct::Pa,
                quantity: d("1"),
                price: d("10")
            }),
            Err("PROFILE_MISMATCH")
        );
        let mut clone = owner.clone();
        clone.positions = PositionState::CapacityFlat;
        assert_eq!(clone.event_limit_projection(), Err("PROFILE_MISMATCH"));
        assert_eq!(owner.positions, PositionState::EventLimit(None));
    }
    let mut v2 = Config::frozen(Program::V2);
    v2.program_id = Program::V1.id().into();
    v2.program_hash = Program::V1.hash();
    assert_eq!(v2, Config::frozen(Program::V1));
    let (btc, scenario, marks) = fixture();
    let btc = ScenarioAccount::from_seed(&btc, &scenario, &marks).unwrap();
    assert_eq!(btc.event_limit_projection(), Err("PROFILE_MISMATCH"));
    let mut mixed = btc.clone();
    mixed.positions = PositionState::EventLimit(None);
    assert_eq!(mixed.projection(), Err("PROFILE_MISMATCH"));
}

#[test]
fn all_context_and_seed_mismatches_fail_without_partial_owner() {
    let valid = Config::frozen(Program::V1);
    let changes: &[fn(&mut Config)] = &[
        |c| c.program_id = "SYNTHETIC_CAPACITY_V1".into(),
        |c| c.program_id = "unknown".into(),
        |c| c.program_hash = [0; 32],
        |c| c.multiplier = d("0.01"),
        |c| c.tick = d("0.1"),
        |c| c.step = d("0.1"),
        |c| c.mark = d("101"),
        |c| c.fee = d("0.001"),
        |c| c.interval.from = -1,
        |c| c.interval.to = Some(1001),
    ];
    for change in changes {
        let mut config = valid.clone();
        change(&mut config);
        let before = config.clone();
        assert!(ScenarioAccount::from_event_limit_seed(&seed(), &config).is_err());
        assert_eq!(config, before);
    }
    let changes: &[fn(&mut CleanSeed)] = &[
        |s| s.cash = d("999"),
        |s| s.key.account = "B".into(),
        |s| s.config_id.clear(),
        |s| s.effective_at = -1,
        |s| s.effective_at = 1000,
        |s| s.positions = fixture().0.positions,
        |s| s.orders.reverse(),
        |s| {
            s.orders.pop();
        },
        |s| s.orders[0].product = ProfileProduct::BtcEth(Product::Btc),
        |s| s.orders[0].side = Side::Short,
        |s| s.orders[0].price = d("11"),
        |s| s.orders[0].original = d("2"),
        |s| s.orders[0].remaining = d("0"),
        |s| s.orders[0].filled = d("1"),
        |s| s.orders[0].status = "FILLED".into(),
        |s| s.orders[0].reduce_only = true,
        |s| s.orders[1].intent_id = s.orders[0].intent_id.clone(),
        |s| s.orders[1].client_id = s.orders[0].client_id.clone(),
        |s| s.orders[0].strategy_id.clear(),
    ];
    for change in changes {
        let mut seed = seed();
        change(&mut seed);
        let before = seed.clone();
        assert!(ScenarioAccount::from_event_limit_seed(&seed, &valid).is_err());
        assert_eq!(seed, before);
    }
}

#[test]
fn pure_authoritative_facts_produce_exact_c09_snapshots_not_executions() {
    let seed = seed();
    let config = Config::frozen(Program::V1);
    let owner = ScenarioAccount::from_event_limit_seed(&seed, &config).unwrap();
    let before = owner.clone();
    for (filled, used, available) in [(0, "20", "980"), (1, "110", "890"), (2, "200", "800")] {
        let mut orders = seed.orders.clone();
        let lots = (0..filled)
            .map(|sequence| EntryLot {
                origin_spec_version: "event-limit-neutral".into(),
                source: SeedLot {
                    seed_execution_id: format!("X_{sequence}"),
                    seed_sequence: sequence,
                    strategy_id: "strategy".into(),
                    contracts: d("1"),
                    entry: d("10"),
                },
                execution_id: [sequence as u8; 32],
                base_quantity: d("1"),
            })
            .collect::<Vec<_>>();
        let position = (filled != 0).then_some(ProductPosition {
            side: Side::Long,
            contracts: Decimal::from(filled),
            lots,
            entry_basis: Decimal::from(filled * 10),
        });
        for order in orders.iter_mut().take(filled as usize) {
            order.status = "FILLED".into();
            order.filled = d("1");
            order.remaining = d("0");
        }
        let facts = orders.iter().collect::<Vec<_>>();
        let snapshot = calculate(&config, 500, d("1000"), position.as_ref(), &facts).unwrap();
        if filled == 0 {
            assert_eq!(owner.event_limit_projection(), Ok(snapshot.clone()));
        }
        assert_eq!(
            snapshot,
            calculate(
                &Config::frozen(Program::V2),
                500,
                d("1000"),
                position.as_ref(),
                &facts
            )
            .unwrap()
        );
        assert_eq!(
            (snapshot.total_order_loss, snapshot.total_fee_hold),
            (d("0"), d("0"))
        );
        assert_eq!(
            snapshot,
            calculate(&config, 500, d("1000"), position.as_ref(), &facts).unwrap()
        );
        assert_eq!(
            (snapshot.used_margin, snapshot.available_margin),
            (d(used), d(available))
        );
        assert_eq!(snapshot.position_value, Decimal::from(filled * 100));
        assert_eq!(
            snapshot.long_remaining_value,
            Decimal::from((2 - filled) * 10)
        );
        assert_eq!(snapshot.short_remaining_value, d("0"));
        assert_eq!(snapshot.exposure_margin, snapshot.used_margin);
        assert_eq!(
            (snapshot.equity, snapshot.eligible_collateral),
            (d("1000"), d("1000"))
        );
        assert_eq!(
            (snapshot.initial_margin, snapshot.maintenance_margin),
            (d("0"), d("0"))
        );
        assert_eq!(snapshot.orders.len(), 2 - filled as usize);
        assert_eq!(snapshot.risk, ProfileRisk::CapacitySafe);
        assert!(snapshot.pending_action_ids.is_empty());
        for row in &snapshot.orders {
            assert_eq!(
                (
                    row.remaining_contracts,
                    row.remaining_base_exposure,
                    row.order_loss,
                    row.fee_hold
                ),
                (d("1"), d("1"), d("0"), d("0"))
            );
        }
        let mut malformed = orders.clone();
        malformed[0].status = "CANCELED".into();
        assert!(calculate(
            &config,
            500,
            d("1000"),
            position.as_ref(),
            &malformed.iter().collect::<Vec<_>>()
        )
        .is_err());
        if let Some(position) = &position {
            let changes: &[fn(&mut ProductPosition)] = &[
                |p| p.side = Side::Short,
                |p| p.contracts = d("0"),
                |p| p.entry_basis = d("0"),
                |p| p.lots[0].source.entry = d("11"),
                |p| p.lots[0].source.contracts = d("0.5"),
                |p| p.lots[0].base_quantity = d("0.5"),
                |p| p.lots[0].source.seed_sequence = 2,
                |p| p.lots[0].source.seed_execution_id.clear(),
                |p| p.lots[0].source.strategy_id.clear(),
            ];
            for change in changes {
                let mut bad = position.clone();
                change(&mut bad);
                assert!(calculate(&config, 500, d("1000"), Some(&bad), &facts).is_err());
            }
        }
        assert_eq!(owner, before);
        assert!(owner.execution_receipts.is_empty());
    }
}
