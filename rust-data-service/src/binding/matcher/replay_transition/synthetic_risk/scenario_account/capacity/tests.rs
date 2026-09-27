use super::super::tests::{d, fixture};
use super::*;

#[test]
fn event_limit_program_cannot_masquerade_as_capacity_admission() {
    for program in ["SYNTHETIC_EVENT_LIMIT_V1", "SYNTHETIC_EVENT_LIMIT_V2"] {
        let mut config = Config::frozen(Program::CapacityV1);
        config.program_id = program.into();
        assert_eq!(
            ScenarioAccount::from_capacity_seed(&seed(), &config),
            Err("UNSUPPORTED_CAPACITY_PROGRAM")
        );
    }
}

#[test]
fn authoritative_projection_retains_terminal_order_without_reservation() {
    let config = Config::frozen(Program::CapacityV1);
    for (status, filled, remaining, expected) in [
        ("OPEN", "0", "2", Some("200")),
        ("PARTIALLY_FILLED", "1", "1", Some("100")),
        ("FILLED", "2", "0", Some("0")),
        ("FILLED", "1", "1", None),
        ("OPEN", "2", "0", None),
        ("PARTIALLY_FILLED", "2", "0", None),
        ("CANCELED", "2", "0", None),
        ("UNKNOWN", "0", "2", None),
        ("FILLED", "1", "0", None),
    ] {
        let mut owner = ScenarioAccount::from_capacity_seed(&seed(), &config).unwrap();
        let mut facts = fixture().0.orders.remove(0);
        facts.product = ProfileProduct::Pa;
        facts.price = d("100");
        facts.status = status.into();
        facts.filled = d(filled);
        facts.remaining = d(remaining);
        owner
            .orders
            .insert("O1".into(), RestingOrder { facts, version: 7 });
        let before = owner.clone();
        let snapshot = owner.capacity_projection(&candidate("1"));
        match expected {
            Some(value) => assert_eq!(snapshot.unwrap().current, d(value)),
            None => assert_eq!(snapshot, Err("INVALID_CAPACITY_ORDER")),
        }
        assert_eq!(owner, before);
        assert_eq!(owner.orders["O1"].version, 7);
    }
}

fn seed() -> CleanSeed {
    let (mut seed, _, _) = fixture();
    seed.cash = d("1000");
    seed.positions.clear();
    seed.orders.clear();
    seed
}

fn candidate(quantity: &str) -> Candidate {
    Candidate {
        product: ProfileProduct::Pa,
        quantity: d(quantity),
        price: d("100"),
    }
}

#[test]
fn closed_program_substitution_and_clean_owner_baselines() {
    let seed = seed();
    let v1 = Config::frozen(Program::CapacityV1);
    let tight = Config::frozen(Program::CapacityTightV1);
    let mut same_config = tight.clone();
    same_config.program_id = v1.program_id.clone();
    same_config.program_hash = v1.program_hash;
    assert_eq!(same_config, v1);
    assert_eq!(v1.product.canonical_id(), "P_A");
    let mut hashes = BTreeSet::new();
    for (config, threshold, available) in [(v1, "1000", "400"), (tight, "500", "-100")] {
        let owner = ScenarioAccount::from_capacity_seed(&seed, &config).unwrap();
        let before = owner.clone();
        let projection = owner.capacity_projection(&candidate("6")).unwrap();
        assert_eq!(projection.current, d("0"));
        assert_eq!(projection.new, d("600"));
        assert_eq!(projection.required, d("600"));
        assert_eq!(projection.threshold, d(threshold));
        assert_eq!(projection.available, d(available));
        assert_eq!(projection.program_hash, config.program_hash);
        assert_eq!(owner.state_version, 0);
        assert_eq!(owner.gate, Gate::Running);
        assert_eq!(owner.cash, d("1000"));
        assert_eq!((owner.gross_realized, owner.fees), (d("0"), d("0")));
        assert_eq!(owner.commit_sequence, 0);
        assert!(owner.positions.is_empty() && owner.orders.is_empty());
        assert!(owner.seed_intents.is_empty() && owner.seed_orders.is_empty());
        assert!(owner.seed_executions.is_empty() && owner.pending_actions.is_empty());
        assert!(owner.intent_results.is_empty() && owner.execution_receipts.is_empty());
        assert_eq!(owner.projection(), Err("PROFILE_MISMATCH"));
        assert_eq!(owner.reservation(), Err("PROFILE_MISMATCH"));
        assert_eq!(owner.validate_context(999), Ok(()));
        assert_eq!(
            owner.validate_context(1000),
            Err("UNSUPPORTED_CONTEXT_TRANSITION")
        );
        assert_eq!(owner, before);
        assert!(hashes.insert(owner.valuation_context_id));
        let mut draft = owner.clone();
        draft.cash = d("1");
        draft.key.account = "other".into();
        assert_eq!(owner, before);
        let mut other_seed = seed.clone();
        other_seed.key.account = "other".into();
        let other = ScenarioAccount::from_capacity_seed(&other_seed, &config).unwrap();
        assert_ne!(other.key, owner.key);
        assert_eq!(other.capacity_projection(&candidate("6")), Ok(projection));
    }
}

#[test]
fn immutable_authoritative_remainders_and_occupied_capacity_are_additive() {
    let config = Config::frozen(Program::CapacityV1);
    let orders = vec![RemainingOrder {
        order_id: "O1".into(),
        remainder: candidate("6"),
    }];
    let before = orders.clone();
    let second = calculate(&config, 500, d("0"), &orders, &candidate("5")).unwrap();
    assert_eq!(
        (
            second.current,
            second.new,
            second.required,
            second.available
        ),
        (d("600"), d("500"), d("1100"), d("-100"))
    );
    let mut multiple = orders.clone();
    multiple.push(RemainingOrder {
        order_id: "O2".into(),
        remainder: candidate("2"),
    });
    let with_position = calculate(&config, 500, d("1"), &multiple, &candidate("1")).unwrap();
    assert_eq!(with_position.current, d("900"));
    assert_eq!(with_position.required, d("1000"));
    multiple.reverse();
    assert_eq!(
        calculate(&config, 500, d("1"), &multiple, &candidate("1")),
        Ok(with_position)
    );
    assert_eq!(orders, before);
}

#[test]
fn every_context_and_seed_mismatch_fails_without_mutating_inputs() {
    let config = Config::frozen(Program::CapacityV1);
    let seed = seed();
    let config_before = config.clone();
    let seed_before = seed.clone();
    let mutations: &[fn(&mut Config)] = &[
        |c| c.product = ProfileProduct::BtcEth(Product::Btc),
        |c| c.product = ProfileProduct::BtcEth(Product::Eth),
        |c| c.program_id = "unknown".into(),
        |c| c.program_id = Program::CapacityTightV1.id().into(),
        |c| c.program_hash[0] ^= 1,
        |c| c.rule_data_version = "unknown".into(),
        |c| c.spec_version = "unknown".into(),
        |c| c.quantity_unit = "base".into(),
        |c| c.multiplier = d("2"),
        |c| c.tick = d("0.1"),
        |c| c.step = d("0.5"),
        |c| c.fee_rate = d("0.001"),
        |c| c.mark = d("101"),
        |c| c.spec_interval.from = 1,
        |c| c.spec_interval.to = None,
        |c| c.mark_interval.from = 1,
        |c| c.mark_interval.to = Some(1001),
    ];
    for mutate in mutations {
        let mut invalid = config.clone();
        mutate(&mut invalid);
        let before = invalid.clone();
        assert!(ScenarioAccount::from_capacity_seed(&seed, &invalid).is_err());
        assert!(calculate(&invalid, 500, d("0"), &[], &candidate("6")).is_err());
        assert_eq!(invalid, before);
    }
    for at in [-1, 1000] {
        let mut invalid = seed.clone();
        invalid.effective_at = at;
        assert_eq!(
            ScenarioAccount::from_capacity_seed(&invalid, &config),
            Err("CAPACITY_CONTEXT_COVERAGE")
        );
    }
    for at in [0, 999] {
        let mut valid = seed.clone();
        valid.effective_at = at;
        assert!(ScenarioAccount::from_capacity_seed(&valid, &config).is_ok());
    }
    let (btc_seed, btc_config, marks) = fixture();
    for with_position in [true, false] {
        let mut invalid = seed.clone();
        if with_position {
            invalid.positions = btc_seed.positions.clone();
        } else {
            invalid.orders = btc_seed.orders.clone();
        }
        assert_eq!(
            ScenarioAccount::from_capacity_seed(&invalid, &config),
            Err("UNSUPPORTED_CAPACITY_SEED_STATE")
        );
    }
    for invalid_account in [true, false] {
        let mut invalid = seed.clone();
        if invalid_account {
            invalid.key.account.clear();
        } else {
            invalid.config_id.clear();
        }
        assert!(ScenarioAccount::from_capacity_seed(&invalid, &config).is_err());
    }
    let btc = ScenarioAccount::from_seed(&btc_seed, &btc_config, &marks).unwrap();
    assert_eq!(
        btc.capacity_projection(&candidate("6")),
        Err("PROFILE_MISMATCH")
    );
    assert_eq!(
        btc.valuation_context_id,
        context_id(&btc_config, &marks, btc_seed.effective_at).unwrap()
    );
    assert_eq!(config, config_before);
    assert_eq!(seed, seed_before);
}

#[test]
fn exact_candidate_validation_overflow_and_duplicate_order_rejection() {
    let config = Config::frozen(Program::CapacityV1);
    let owner = ScenarioAccount::from_capacity_seed(&seed(), &config).unwrap();
    let before = owner.clone();
    for (quantity, price) in [
        ("0", "100"),
        ("-1", "100"),
        ("0.5", "100"),
        ("1", "0"),
        ("1", "-100"),
        ("1", "100.1"),
    ] {
        let invalid = Candidate {
            price: d(price),
            ..candidate(quantity)
        };
        assert_eq!(
            owner.capacity_projection(&invalid),
            Err("INVALID_CAPACITY_CANDIDATE")
        );
    }
    let mut mixed = candidate("6");
    mixed.product = ProfileProduct::BtcEth(Product::Btc);
    assert_eq!(
        owner.capacity_projection(&mixed),
        Err("INVALID_CAPACITY_CANDIDATE")
    );
    let extreme = Candidate {
        quantity: Decimal::MAX,
        price: d("100"),
        ..candidate("1")
    };
    assert!(owner.capacity_projection(&extreme).is_err());
    let maximum_order = RemainingOrder {
        order_id: "MAX".into(),
        remainder: Candidate {
            quantity: Decimal::MAX,
            price: d("1"),
            ..candidate("1")
        },
    };
    assert!(calculate(&config, 500, d("0"), &[maximum_order], &candidate("1")).is_err());
    let unnamed = RemainingOrder {
        order_id: String::new(),
        remainder: candidate("1"),
    };
    assert_eq!(
        calculate(&config, 500, d("0"), &[unnamed], &candidate("1")),
        Err("INVALID_CAPACITY_ORDER_ID")
    );
    let order = RemainingOrder {
        order_id: "O1".into(),
        remainder: candidate("6"),
    };
    assert_eq!(
        calculate(
            &config,
            500,
            d("0"),
            &[order.clone(), order],
            &candidate("1")
        ),
        Err("INVALID_CAPACITY_ORDER_ID")
    );
    for occupied in [d("-1"), d("0.1"), Decimal::MAX] {
        assert!(calculate(&config, 500, occupied, &[], &candidate("1")).is_err());
    }
    assert_eq!(
        owner.capacity_projection(&candidate("6.000")),
        owner.capacity_projection(&candidate("6"))
    );
    assert_eq!(owner, before);
}
