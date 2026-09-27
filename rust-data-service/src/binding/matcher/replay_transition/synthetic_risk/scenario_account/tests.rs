use super::*;
use std::str::FromStr;

pub(super) fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

pub(super) fn fixture() -> (CleanSeed, FrozenScenario, Vec<Mark>) {
    let mut specs = Vec::new();
    let mut tiers = Vec::new();
    for product in [Product::Btc, Product::Eth] {
        for second in [false, true] {
            specs.push(frozen_spec(product, second));
            tiers.push(frozen_tiers(product, second));
        }
    }
    let positions = vec![SeedPosition {
        product: Product::Btc,
        side: Side::Long,
        contracts: d("3"),
        lots: [
            ("X1", 0, "strategy-a", "1", "50000"),
            ("X2", 1, "strategy-b", "2", "50000.1"),
        ]
        .into_iter()
        .map(|(id, seq, strategy, qty, entry)| SeedLot {
            seed_execution_id: id.into(),
            seed_sequence: seq,
            strategy_id: strategy.into(),
            contracts: d(qty),
            entry: d(entry),
        })
        .collect(),
    }];
    let seed = CleanSeed {
        key: AccountKey {
            venue: "okx-scenario".into(),
            environment: "test".into(),
            account: "A".into(),
            subaccount: None,
        },
        config_id: "scenario-v1".into(),
        effective_at: 500,
        cash: d("10000"),
        positions,
        orders: vec![SeedOrder {
            intent_id: "I1".into(),
            order_id: "O1".into(),
            client_id: "C1".into(),
            strategy_id: "strategy-c".into(),
            product: ProfileProduct::BtcEth(Product::Btc),
            side: Side::Short,
            price: d("50000.1"),
            reduce_only: true,
            original: d("2"),
            filled: d("1"),
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "PARTIALLY_FILLED".into(),
        }],
    };
    let marks = [(Product::Btc, "50001"), (Product::Eth, "1900")]
        .into_iter()
        .map(|(product, price)| Mark {
            product,
            price: d(price),
            valid_from: 0,
            valid_to: 3000,
        })
        .collect();
    (
        seed,
        FrozenScenario::new(d("10"), specs, tiers).unwrap(),
        marks,
    )
}

#[test]
fn seed_origin_is_resolved_by_product_and_seed_effective_time() {
    for product in [Product::Btc, Product::Eth] {
        for (at, version) in [(1999, "spec-v1"), (2000, "spec-v2")] {
            let (mut seed, config, marks) = fixture();
            seed.effective_at = at;
            seed.orders.clear();
            seed.positions[0].product = product;
            for lot in &mut seed.positions[0].lots {
                lot.entry = d("2000");
            }
            let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
            assert!(owner.positions.btc().unwrap()[&product]
                .lots
                .iter()
                .all(|lot| lot.origin_spec_version == version));
        }
    }
}

#[test]
fn risk_and_settlement_boundaries_remain_private_and_single_entry() {
    let owner = include_str!("../scenario_account.rs");
    let commit = include_str!("execution/commit.rs");
    let risk = include_str!("risk_transition.rs");
    let settlement = include_str!("hypothetical_settlement.rs");
    assert!(owner.contains("mod risk_transition;"));
    assert!(!owner.contains("pub mod risk_transition"));
    assert!(!owner.contains("fn execution_snapshot_after"));
    assert!(!commit.contains("fn execution_snapshot_after"));
    assert!(commit.contains("self.execution_snapshot_after(stamp)?"));
    assert_eq!(risk.matches("fn execution_snapshot_after").count(), 1);
    assert!(risk.contains("impl ScenarioAccount"));
    assert!(!risk.contains("struct ScenarioAccount"));
    assert_eq!(settlement.matches("fn calculate").count(), 1);
    assert_eq!(settlement.matches("fn fee_amount(").count(), 1);
    assert!(!risk.contains("fee_amount("));
    for source in [
        risk,
        include_str!("context.rs"),
        include_str!("group.rs"),
        include_str!("risk_transition/cancel.rs"),
    ] {
        for forbidden in [
            "pyo3::",
            "reqwest::",
            "tokio::",
            "std::fs",
            "std::net",
            "struct ScenarioAccount",
        ] {
            assert!(
                !source.contains(forbidden),
                "private transition boundary: {forbidden}"
            );
        }
    }
    assert_eq!(risk.matches("fn classify_risk(").count(), 1);
}

#[test]
fn seed_reservation_arithmetic_is_validated_before_publishing_owner() {
    let (mut seed, config, mut marks) = fixture();
    seed.positions.clear();
    seed.cash = d("1000");
    marks[0].price = d("50000");
    let order = &mut seed.orders[0];
    order.side = Side::Long;
    order.price = d("50000");
    order.reduce_only = false;
    order.original = Decimal::ONE;
    order.filled = Decimal::ZERO;
    order.remaining = Decimal::ONE;
    order.status = "OPEN".into();

    let snapshot = ScenarioAccount::from_seed(&seed, &config, &marks)
        .unwrap()
        .reservation()
        .unwrap();
    assert_eq!(snapshot.equity, d("1000"));
    assert_eq!(snapshot.maintenance_margin, Decimal::ZERO);
    assert_eq!(snapshot.total_order_loss, Decimal::ZERO);
    assert_eq!(snapshot.total_fee_hold, d("0.5"));
    assert_eq!(snapshot.products[0].exposure_margin, d("50"));
    assert_eq!(snapshot.used_margin, d("50.5"));
    assert_eq!(snapshot.available_margin, d("949.5"));
    let mut negative_available = seed.clone();
    negative_available.cash = d("40");
    let owner = ScenarioAccount::from_seed(&negative_available, &config, &marks).unwrap();
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.reservation().unwrap().available_margin, d("-10.5"));

    for (quantity, mark, count, fault) in [
        (Decimal::MAX, d("50000"), 1, "DECIMAL_OVERFLOW"),
        (
            d("0.01"),
            d("0.0000000000000000000000000001"),
            1,
            "DECIMAL_PRECISION_LOSS",
        ),
        (
            d("80000000000000000000000000"),
            d("50000"),
            2,
            "DECIMAL_OVERFLOW",
        ),
    ] {
        let mut invalid = seed.clone();
        let mut invalid_marks = marks.clone();
        invalid_marks[0].price = mark;
        invalid.orders[0].original = quantity;
        invalid.orders[0].remaining = quantity;
        if count == 2 {
            // Each order is representable; only their shared aggregate overflows.
            assert!(ScenarioAccount::from_seed(&invalid, &config, &invalid_marks).is_ok());
            let mut second = invalid.orders[0].clone();
            second.intent_id = "I2".into();
            second.order_id = "O2".into();
            second.client_id = "C2".into();
            invalid.orders.push(second);
        }
        let unchanged = (invalid.clone(), invalid_marks.clone());
        assert_eq!(
            ScenarioAccount::from_seed(&invalid, &config, &invalid_marks),
            Err(fault)
        );
        assert_eq!((invalid, invalid_marks), unchanged);
    }
}

#[test]
fn seed_lots_are_product_net_fifo_not_strategy_positions() {
    let (mut seed, config, marks) = fixture();
    seed.positions[0].lots.reverse();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let p = &owner.positions.btc().unwrap()[&Product::Btc];
    assert_eq!(
        (owner.positions.len(), p.contracts, p.entry_basis),
        (1, d("3"), d("1500.002"))
    );
    assert_eq!(
        p.lots
            .iter()
            .map(|l| l.source.seed_sequence)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        (p.lots[0].base_quantity, p.lots[1].base_quantity),
        (d("0.01"), d("0.02"))
    );
    let value = config
        .evaluate(&owner.projection().unwrap(), &marks)
        .unwrap();
    assert_eq!(
        (value.products[0].unrealized_pnl, value.equity),
        (d("0.028"), d("10000.028"))
    );
    assert_eq!(
        (value.products[0].tier, value.maintenance_margin),
        (1, d("6.00012"))
    );
    seed.positions[0].lots[0].strategy_id = "strategy-a".into();
    seed.positions[0].lots[1].strategy_id = "strategy-b".into();
    let swapped = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(swapped.projection().unwrap(), owner.projection().unwrap());
    assert_eq!(
        (owner.state_version, owner.commit_sequence, &owner.gate),
        (0, 2, &Gate::Running)
    );
    assert_eq!(
        (owner.fees, owner.gross_realized, owner.cash),
        (d("0"), d("0"), d("10000"))
    );
    assert!(
        owner.intent_results.is_empty()
            && owner.execution_receipts.is_empty()
            && owner.pending_actions.is_empty()
    );
    assert_eq!(owner.target_order("O1").unwrap().version, 0);
}

#[test]
fn two_products_short_lots_and_clone_projection_are_isolated() {
    let (mut seed, config, marks) = fixture();
    let mut eth = seed.positions[0].clone();
    eth.product = Product::Eth;
    eth.side = Side::Short;
    for (lot, (id, seq, entry)) in eth
        .lots
        .iter_mut()
        .zip([("E1", 2, "2000"), ("E2", 3, "1999.5")])
    {
        lot.seed_execution_id = id.into();
        lot.seed_sequence = seq;
        lot.entry = d(entry);
    }
    seed.positions.push(eth);
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let mut projection = owner.projection().unwrap();
    let value = config.evaluate(&projection, &marks).unwrap();
    assert_eq!(
        (
            value.products[1].unrealized_pnl,
            value.equity,
            value.maintenance_margin
        ),
        (d("29.9"), d("10029.928"), d("8.28012"))
    );
    projection.positions[0].lots[0].entry = d("1");
    let mut draft = owner.clone();
    draft.cash = d("1");
    draft
        .positions
        .btc_mut()
        .unwrap()
        .get_mut(&Product::Btc)
        .unwrap()
        .lots[0]
        .source
        .entry = d("1");
    draft.orders.get_mut("O1").unwrap().facts.remaining = d("0.5");
    assert_eq!(owner.cash, d("10000"));
    assert_eq!(
        owner.positions.btc().unwrap()[&Product::Btc].lots[0]
            .source
            .entry,
        d("50000")
    );
    assert_eq!(owner.orders["O1"].facts.remaining, d("1"));
    assert_eq!(
        config
            .evaluate(&owner.projection().unwrap(), &marks)
            .unwrap(),
        value
    );
    seed.key.account = "B".into();
    let other = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_ne!(other.seed_executions, owner.seed_executions);
    assert_eq!(other.projection().unwrap(), owner.projection().unwrap());
}

#[test]
fn invalid_seed_matrix_never_returns_a_partial_owner() {
    let mutations: &[fn(&mut CleanSeed)] = &[
        |s| s.key.account.clear(),
        |s| s.config_id.clear(),
        |s| s.effective_at = -1,
        |s| s.positions.push(s.positions[0].clone()),
        |s| s.positions[0].lots[1].seed_execution_id = "X1".into(),
        |s| s.positions[0].lots[1].seed_sequence = 0,
        |s| s.positions[0].lots[1].seed_sequence = 2,
        |s| s.positions[0].lots[0].strategy_id.clear(),
        |s| s.positions[0].lots[0].seed_execution_id.clear(),
        |s| s.positions[0].contracts = d("4"),
        |s| s.positions[0].lots.clear(),
        |s| s.positions[0].lots[0].contracts = d("0"),
        |s| s.positions[0].lots[0].contracts = d("0.001"),
        |s| s.positions[0].lots[0].entry = d("0"),
        |s| s.positions[0].lots[0].entry = d("50000.01"),
        |s| s.positions[0].lots[0].entry = Decimal::MAX,
        |s| {
            s.positions[0].contracts = d("25002");
            s.positions[0].lots[0].contracts = d("25000");
        },
        |s| s.cash = d("0"),
        |s| s.orders.push(s.orders[0].clone()),
        |s| s.orders[0].intent_id.clear(),
        |s| s.orders[0].status = "CANCELED".into(),
        |s| s.orders[0].status = "FILLED".into(),
        |s| s.orders[0].status = "OPEN".into(),
        |s| s.orders[0].status = "CANCEL_REQUESTED".into(),
        |s| s.orders[0].original = d("3"),
        |s| s.orders[0].filled = d("-1"),
        |s| s.orders[0].remaining = d("0"),
        |s| s.orders[0].remaining = d("0.001"),
        |s| {
            s.orders[0].filled = d("0");
            s.orders[0].original = d("1");
        },
        |s| s.orders[0].price = d("0"),
        |s| s.orders[0].price = d("50000.01"),
        |s| s.orders[0].side = Side::Long,
        |s| {
            s.orders[0].remaining = d("4");
            s.orders[0].original = d("5");
        },
        |s| s.positions.clear(),
    ];
    for mutate in mutations {
        let (mut seed, config, marks) = fixture();
        mutate(&mut seed);
        let before = seed.clone();
        assert!(
            ScenarioAccount::from_seed(&seed, &config, &marks).is_err(),
            "{seed:?}"
        );
        assert_eq!(seed, before);
    }
    for duplicate in ["intent", "order", "client"] {
        let (mut seed, config, marks) = fixture();
        let mut second = seed.orders[0].clone();
        if duplicate != "intent" {
            second.intent_id = "I2".into();
        }
        if duplicate != "order" {
            second.order_id = "O2".into();
        }
        if duplicate != "client" {
            second.client_id = "C2".into();
        }
        seed.orders.push(second);
        assert!(ScenarioAccount::from_seed(&seed, &config, &marks).is_err());
    }
    let (seed, config, mut marks) = fixture();
    marks.pop(); // Even an unpositioned product needs a fixed context.
    assert_eq!(
        ScenarioAccount::from_seed(&seed, &config, &marks),
        Err("MARK_COVERAGE_MISSING")
    );
}

#[test]
fn seed_identity_collision_matrix_is_fatal_but_target_reference_is_legal() {
    let (seed, config, marks) = fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let seed_id = *owner.seed_executions.first().unwrap();
    for new in [
        NewIdentity::Intent("I1"),
        NewIdentity::Order("O1"),
        NewIdentity::Execution {
            namespace: "runtime",
            canonical_id: seed_id,
        },
        NewIdentity::Execution {
            namespace: "seed",
            canonical_id: [0; 32],
        },
    ] {
        let mut a = owner.clone();
        assert_eq!(a.guard_new_identity(new), Err("SEED_IDENTITY_CONFLICT"));
        assert_eq!(a.gate, Gate::Failed("SEED_IDENTITY_CONFLICT"));
        assert_eq!(
            a.guard_new_identity(NewIdentity::Intent("new")),
            Err("RUN_FAILED")
        );
        a.gate = Gate::Running;
        assert_eq!(a, owner); // Includes cash, versions, lots, orders and empty caches.
    }
    let mut a = owner.clone();
    for new in [
        NewIdentity::Intent("I2"),
        NewIdentity::Order("O2"),
        NewIdentity::Execution {
            namespace: "runtime",
            canonical_id: [0; 32],
        },
    ] {
        assert_eq!(a.guard_new_identity(new), Ok(()));
    }
    assert_eq!(a.target_order("O1").unwrap().version, 0);
    assert_eq!(a, owner);
}

#[test]
fn fixed_context_boundaries_require_independent_clean_seeds() {
    for (before, boundary) in [(999, 1000), (1999, 2000)] {
        let (mut seed, config, marks) = fixture();
        seed.effective_at = before;
        let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        assert_eq!(owner.validate_context(before), Ok(()));
        assert_eq!(
            owner.validate_context(boundary),
            Err("UNSUPPORTED_CONTEXT_TRANSITION")
        );
        assert_eq!(
            owner,
            ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
        );
        seed.effective_at = boundary;
        if boundary == 2000 {
            assert!(ScenarioAccount::from_seed(&seed, &config, &marks).is_err());
            seed.orders[0].price = d("50001");
            seed.positions[0].lots[1].entry = d("50001");
        }
        let next = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        assert_ne!(next.valuation_context_id, owner.valuation_context_id);
        assert_eq!(next.validate_context(boundary), Ok(()));
    }
    let (mut seed, config, mut marks) = fixture();
    seed.effective_at = 599;
    marks[0].valid_to = 600;
    marks.push(Mark {
        valid_from: 600,
        valid_to: 900,
        ..marks[0].clone()
    });
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!(
        owner.validate_context(600),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    seed.effective_at = 600;
    assert!(ScenarioAccount::from_seed(&seed, &config, &marks).is_ok());
    marks[1].price = d("1901");
    assert_ne!(
        context_id(&config, &marks, 599).unwrap(),
        owner.valuation_context_id
    );
}

#[test]
fn flat_and_available_shortfall_seeds_do_not_invent_reservation_acceptance() {
    let (mut seed, config, marks) = fixture();
    seed.cash = d("40");
    assert!(ScenarioAccount::from_seed(&seed, &config, &marks).is_ok());
    seed.positions.clear();
    seed.orders.clear();
    seed.cash = d("-1");
    let flat = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    assert_eq!((flat.commit_sequence, flat.state_version), (0, 0));
    seed.orders = fixture().0.orders;
    seed.orders[0].filled = d("0");
    seed.orders[0].original = d("1");
    seed.orders[0].status = "OPEN".into();
    seed.orders[0].reduce_only = false;
    assert!(ScenarioAccount::from_seed(&seed, &config, &marks).is_ok());
}
