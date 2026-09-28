use super::super::tests::{d, fixture};
use super::*;
use execution::commit::tests::{at, input};
fn owners() -> Vec<ScenarioAccount> {
    let (mut seed, config, marks) = fixture();
    let btc = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let min = ScenarioAccount::synthetic_min_cash(seed.key.clone()).unwrap();
    seed.cash = d("1000");
    seed.positions.clear();
    seed.orders.clear();
    vec![
        btc,
        min,
        ScenarioAccount::from_golden_cancel_seed(&seed, &golden_cancel::Config::frozen()).unwrap(),
    ]
}
fn basis(owner: &ScenarioAccount) -> CurrentBasis {
    let before = owner.clone();
    let b = owner.inspection_basis().unwrap();
    assert_eq!(owner.current_evidence().unwrap().0, b);
    assert_eq!(*owner, before);
    b
}
fn hex(h: Hash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}
#[test]
fn independent_three_profile_vectors() {
    // SHA256 vectors generated independently with Python hashlib/struct.pack('>q').
    // Literal frozen seed rows, normalized decimal strings, raw seed execution hashes;
    // no production encoder or production digest was used to construct expectations.
    let vectors = [
        [
            "f3e94e9d1ab91103e9e988d3a9e89f2c8ea4329b7b1df922fb5c119028e2635e",
            "bb1958795aebb1c2c1148771b6a4010a9f6b00fd7dbe48e882cacac623a4424e",
            "3364d1cb16603122442906d6e5ea3e943f18355d6270b0c3e37f1bfa4b357ded",
        ],
        [
            "25c5000da8bae9ae0e3cb00a075f861e2c34fc6d82793f443ecada5316ed9da9",
            "759215981bb201ecbc5aa8fb904f83fd8bea7862171e24bab1ba41fe4512eb27",
            "91cd431d81d6ba51eee4f780d61a51679e1e4cb3bd2f2cc16842cdc0ff5d30bd",
        ],
        [
            "4fe4cb189a4d0af356f106da00680f149a13984cd7c7073a03357a222962f4da",
            "cf125b37f9df463dfe002fede0c5f8dab40b7848f549ba0232309cc11b98daeb",
            "b65c940835e83a2ffabef60df46c506d0990f2355c339a6b5a3b7302d7e595f9",
        ],
    ];
    for (owner, expected) in owners().iter().zip(vectors) {
        let b = basis(owner);
        assert_eq!(
            [hex(b.positions), hex(b.orders), hex(b.reservations)],
            expected
        );
    }
}
#[test]
fn order_fields_terminal_cancel_and_checked_numbers() {
    let owner = owners().remove(0);
    let original = basis(&owner);
    for mutate in [
        |o: &mut RestingOrder| o.facts.intent_id.push('x'),
        |o: &mut RestingOrder| o.facts.order_id.push('x'),
        |o: &mut RestingOrder| o.facts.client_id.push('x'),
        |o: &mut RestingOrder| o.facts.strategy_id.push('x'),
        |o: &mut RestingOrder| o.facts.side = Side::Long,
        |o: &mut RestingOrder| o.facts.price += Decimal::ONE,
        |o: &mut RestingOrder| o.facts.reduce_only = false,
        |o: &mut RestingOrder| o.facts.original += Decimal::ONE,
        |o: &mut RestingOrder| o.facts.filled += Decimal::ONE,
        |o: &mut RestingOrder| o.facts.canceled += Decimal::ONE,
        |o: &mut RestingOrder| o.facts.remaining += Decimal::ONE,
        |o: &mut RestingOrder| o.facts.status = "FILLED".into(),
        |o: &mut RestingOrder| o.facts.status = "CANCELED".into(),
        |o: &mut RestingOrder| o.created_at += 1,
        |o: &mut RestingOrder| o.version += 1,
    ] {
        let mut changed = owner.clone();
        mutate(changed.orders.get_mut("O1").unwrap());
        assert_ne!(
            changed
                .component(
                    "SCENARIO_ORDERS_EVIDENCE_V1",
                    ScenarioAccount::encode_orders
                )
                .unwrap(),
            original.orders
        );
    }
    let request = cancel::CanonicalRequest {
        reason: cancel::Reason::ExplicitScenario,
        request_id: [1; 32],
        detecting_event_id: "request".into(),
        effect_action_id: [2; 32],
        delivery_action_id: [3; 32],
    };
    let fact = cancel::EffectFact {
        request: request.clone(),
        action_id: [4; 32],
    };
    let mut hashes = vec![original.orders];
    for state in [
        cancel::State::Requested(request),
        cancel::State::EffectiveCanceled(fact.clone()),
        cancel::State::EffectiveTooLate(fact),
    ] {
        let mut changed = owner.clone();
        changed.orders.get_mut("O1").unwrap().cancel = state;
        let digest = basis(&changed).orders;
        assert!(!hashes.contains(&digest));
        hashes.push(digest);
    }
    let mut bad = owner.clone();
    bad.orders.get_mut("O1").unwrap().cancel = cancel::State::MigrationEffective([0; 32]);
    assert_eq!(bad.inspection_basis(), Err("NATIVE_INVARIANT"));
    for change in [
        |a: &mut ScenarioAccount| a.state_version = u64::MAX,
        |a: &mut ScenarioAccount| a.orders.get_mut("O1").unwrap().version = u64::MAX,
    ] {
        let mut bad = owner.clone();
        change(&mut bad);
        assert_eq!(bad.inspection_basis(), Err("NATIVE_INVARIANT"));
    }
    let mut e = Encoding::new("checked");
    assert_eq!(number(&mut e, u64::MAX), Err("NATIVE_INVARIANT"));
}
#[test]
fn fifo_normalization_current_prefix_and_golden_nonempty() {
    let mut owner = owners().remove(0);
    let before = basis(&owner);
    let PositionState::BtcEth(positions) = &mut owner.positions else {
        unreachable!()
    };
    positions.get_mut(&Product::Btc).unwrap().lots.reverse();
    assert_ne!(basis(&owner).positions, before.positions);
    let PositionState::BtcEth(positions) = &mut owner.positions else {
        unreachable!()
    };
    positions.get_mut(&Product::Btc).unwrap().lots.reverse();
    positions.get_mut(&Product::Btc).unwrap().contracts = d("3.000");
    owner.orders.get_mut("O1").unwrap().facts.price = d("50000.1000");
    assert_eq!(basis(&owner), before);
    let prefix = owner.current_evidence().unwrap().1.finish();
    owner.cash += Decimal::ONE;
    assert_ne!(owner.current_evidence().unwrap().1.finish(), prefix);
    assert_eq!(basis(&owner).positions, before.positions);
    assert_ne!(basis(&owner).reservations, before.reservations);
    let mut golden = owners().remove(2);
    let empty = basis(&golden);
    // Independent current-prefix vector appends the same raw segments, not component hashes/domains.
    let raw_prefix = hex(golden.current_evidence().unwrap().1.finish());
    assert_eq!(
        raw_prefix,
        "fa4013bd94dc6d022dacc8e6509f53df2703a914e7a1208e264275784be42651"
    );
    let id = admission::admit_execution_fixture(&mut golden, Side::Long, d("10"), d("10"));
    let fill = at(input(&golden, &id, "golden-fill", "4", "10"), 501);
    golden.execute(&fill).unwrap();
    let filled = basis(&golden);
    // Independent SHA256/struct vector: P_A LONG 4, basis 40, one runtime lot,
    // sequence 0, strategy, entry 10, gt03-spec-v1, base 4 (raw execution key hash).
    assert_eq!(
        hex(filled.positions),
        "2a9752696589060f2303545f7e2d4130362de7a7a4de90a3491e8043aa63b9bd"
    );
    assert_ne!(filled.positions, empty.positions);
    assert_ne!(filled.orders, empty.orders);
    assert_ne!(filled.reservations, empty.reservations);
}
#[test]
fn position_fields_overflow_and_canonical_order() {
    let owner = owners().remove(0);
    let original = basis(&owner);
    for mutate in [
        |p: &mut ProductPosition| p.side = Side::Short,
        |p: &mut ProductPosition| p.contracts += Decimal::ONE,
        |p: &mut ProductPosition| p.entry_basis += Decimal::ONE,
        |p: &mut ProductPosition| p.lots[0].source.seed_execution_id.push('x'),
        |p: &mut ProductPosition| p.lots[0].source.seed_sequence += 1,
        |p: &mut ProductPosition| p.lots[0].source.strategy_id.push('x'),
        |p: &mut ProductPosition| p.lots[0].source.contracts += Decimal::ONE,
        |p: &mut ProductPosition| p.lots[0].source.entry += Decimal::ONE,
        |p: &mut ProductPosition| p.lots[0].origin_spec_version.push('x'),
        |p: &mut ProductPosition| p.lots[0].execution_id[0] ^= 1,
        |p: &mut ProductPosition| p.lots[0].base_quantity += Decimal::ONE,
    ] {
        let mut changed = owner.clone();
        let PositionState::BtcEth(rows) = &mut changed.positions else {
            unreachable!()
        };
        mutate(rows.get_mut(&Product::Btc).unwrap());
        assert_ne!(
            changed
                .component(
                    "SCENARIO_POSITIONS_EVIDENCE_V1",
                    ScenarioAccount::encode_positions
                )
                .unwrap(),
            original.positions
        );
    }
    let mut changed = owner.clone();
    let PositionState::BtcEth(rows) = &mut changed.positions else {
        unreachable!()
    };
    rows.get_mut(&Product::Btc).unwrap().lots[0]
        .source
        .seed_sequence = u64::MAX;
    assert_eq!(changed.inspection_basis(), Err("NATIVE_INVARIANT"));
    let mut a = owner.clone();
    let mut other = a.orders["O1"].clone();
    other.facts.order_id = "A0".into();
    a.orders.insert("Z-key".into(), other);
    let mut b = a.clone();
    let first = b.orders.remove("O1").unwrap();
    let last = b.orders.remove("Z-key").unwrap();
    b.orders.insert("A-key".into(), last);
    b.orders.insert("Z-key".into(), first);
    assert_eq!(basis(&a), basis(&b));
}
