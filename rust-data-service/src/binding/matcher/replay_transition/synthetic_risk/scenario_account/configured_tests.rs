use super::*;
use tests::{d, fixture};

// Oracle from the committed c7c57dd singleton configured encoding.
const PARENT_SINGLETON_CONTEXT: [u8; 32] = [
    0x95, 0x37, 0xb1, 0x11, 0x6d, 0xa6, 0x12, 0x8f, 0x4d, 0xfa, 0xd8, 0x01, 0x7a, 0xbd, 0x02, 0xa5,
    0x11, 0xb4, 0xf5, 0xc3, 0xe5, 0x9f, 0x32, 0x57, 0x06, 0xa1, 0xa9, 0x9e, 0x40, 0x57, 0xca, 0xfe,
];

pub(super) fn input(count: usize) -> (CleanSeed, Vec<ConfiguredProduct>) {
    let mut seed = fixture().0;
    seed.config_id = "SYNTHETIC_P2_SCALE_12_V1".into();
    seed.effective_at = 500;
    seed.cash = d("121.2");
    seed.positions.clear();
    seed.orders.clear();
    let names = [
        "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ARB", "OP", "NEAR", "APT", "SUI", "ADA",
    ];
    let products = names[..count]
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let product = Product(if count == 1 {
                "arbitrary-private-id".into()
            } else {
                format!("{name}-USDT-SWAP").into()
            });
            ConfiguredProduct {
                product: product.clone(),
                instrument_code: i as i64 + 1,
                taker_fee: d("0.001"),
                liquidation_fee: d("0.001"),
                specs: vec![Spec {
                    product: product.clone(),
                    version: "scale-spec-v1".into(),
                    interval: Interval { from: 0, to: None },
                    contract_value: d("1"),
                    multiplier: d("1"),
                    tick: d("1"),
                    lot: d("0.5"),
                    minimum: d("0.5"),
                }],
                tiers: vec![TierVersion {
                    product: product.clone(),
                    version: "scale-tier-v1".into(),
                    interval: Interval { from: 0, to: None },
                    tiers: vec![Tier {
                        minimum: d("0"),
                        maximum: d("100000"),
                        mmr: d("0.005"),
                        imr: d("0.1"),
                        max_leverage: d("10"),
                    }],
                }],
                marks: vec![Mark {
                    product,
                    price: d("100"),
                    valid_from: 0,
                    valid_to: 3000,
                }],
            }
        })
        .collect();
    (seed, products)
}

fn multi_version_input() -> (CleanSeed, ConfiguredProduct) {
    let (mut seed, mut products) = input(1);
    seed.effective_at = 50;
    let product = &mut products[0];
    let mut spec1 = product.specs[0].clone();
    spec1.interval = Interval {
        from: 0,
        to: Some(40),
    };
    let spec2 = Spec {
        version: "spec-v2".into(),
        interval: Interval {
            from: 40,
            to: Some(100),
        },
        contract_value: d("2"),
        ..spec1.clone()
    };
    let spec3 = Spec {
        version: "spec-v3".into(),
        interval: interval(true, 100),
        ..spec2.clone()
    };
    product.specs = vec![spec1, spec2, spec3];
    let mut tier1 = product.tiers[0].clone();
    tier1.interval = Interval {
        from: 0,
        to: Some(40),
    };
    let mut tier2 = TierVersion {
        version: "tier-v2".into(),
        interval: Interval {
            from: 40,
            to: Some(100),
        },
        ..tier1.clone()
    };
    tier2.tiers[0].mmr = d("0.01");
    let tier3 = TierVersion {
        version: "tier-v3".into(),
        interval: interval(true, 100),
        ..tier2.clone()
    };
    product.tiers = vec![tier1, tier2, tier3];
    let mut mark1 = product.marks[0].clone();
    mark1.valid_to = 30;
    let mark2 = Mark {
        valid_from: 40,
        valid_to: 80,
        price: d("101"),
        ..mark1.clone()
    };
    let mark3 = Mark {
        valid_from: 90,
        valid_to: 200,
        price: d("102"),
        ..mark1.clone()
    };
    product.marks = vec![mark1, mark2, mark3];
    (seed, products.remove(0))
}
fn assert_invalid_timeline(change: impl FnOnce(&mut ConfiguredProduct)) {
    let (seed, mut product) = multi_version_input();
    change(&mut product);
    let result = ScenarioAccount::from_configured(&seed, d("10"), vec![product]);
    assert_eq!(result, Err("INVALID_SCHEMA"));
}
fn assert_context_changes(
    seed: &CleanSeed,
    leverage: Decimal,
    products: &[ConfiguredProduct],
    change: impl FnOnce(&mut CleanSeed, &mut Decimal, &mut Vec<ConfiguredProduct>),
) {
    let original = ScenarioAccount::from_configured(seed, leverage, products.to_vec()).unwrap();
    let mut changed_seed = seed.clone();
    let mut changed_leverage = leverage;
    let mut changed_products = products.to_vec();
    change(
        &mut changed_seed,
        &mut changed_leverage,
        &mut changed_products,
    );
    let changed =
        ScenarioAccount::from_configured(&changed_seed, changed_leverage, changed_products)
            .unwrap();
    assert_ne!(original.valuation_context_id, changed.valuation_context_id);
}

fn context_hash(
    scenario: &FrozenScenario,
    seed: &CleanSeed,
    products: &[ConfiguredProduct],
) -> Hash {
    let marks = products
        .iter()
        .flat_map(|row| row.marks.iter().cloned())
        .collect::<Vec<_>>();
    context_id(scenario, &marks, seed.effective_at).unwrap()
}

fn configured_scenario(leverage: Decimal, products: &[ConfiguredProduct]) -> FrozenScenario {
    FrozenScenario {
        configured: Some(products.to_vec()),
        leverage,
        specs: products
            .iter()
            .flat_map(|row| row.specs.iter().cloned())
            .collect(),
        tiers: products
            .iter()
            .flat_map(|row| row.tiers.iter().cloned())
            .collect(),
    }
}

fn timeline_hash(seed: &CleanSeed, product: &ConfiguredProduct) -> Hash {
    let rows = [product.clone()];
    context_hash(&configured_scenario(d("10"), &rows), seed, &rows)
}

fn active_projection(seed: &CleanSeed, product: &ConfiguredProduct) -> (String, String, Decimal) {
    let rows = [product.clone()];
    let scenario = configured_scenario(d("10"), &rows);
    let (spec, tier) = scenario
        .resolve(&product.product, seed.effective_at)
        .unwrap();
    let mark = product
        .marks
        .iter()
        .find(|m| m.valid_from <= seed.effective_at && seed.effective_at < m.valid_to)
        .unwrap();
    (spec.version.clone(), tier.version.clone(), mark.price)
}

fn assert_timeline_changes(
    seed: &CleanSeed,
    base: &ConfiguredProduct,
    cases: &[fn(&mut ConfiguredProduct)],
) {
    // Mutations use the private hash seam; some intentionally make inactive rows invalid.
    let original = timeline_hash(seed, base);
    let active = active_projection(seed, base);
    for change in cases {
        let mut changed = base.clone();
        change(&mut changed);
        assert_eq!(active_projection(seed, &changed), active);
        assert_ne!(timeline_hash(seed, &changed), original);
    }
}

fn assert_timeline_scale_equivalent(
    seed: &CleanSeed,
    base: &ConfiguredProduct,
    cases: &[fn(&mut ConfiguredProduct)],
) {
    let original = timeline_hash(seed, base);
    for change in cases {
        let mut scaled = base.clone();
        change(&mut scaled);
        assert_eq!(
            active_projection(seed, &scaled),
            active_projection(seed, base)
        );
        assert_eq!(timeline_hash(seed, &scaled), original);
    }
}

#[test]
fn configured_empty_owners_preserve_order_and_shared_cash() {
    for count in [1, 2, 12] {
        let (seed, products) = input(count);
        let owner = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
        let snapshot = owner.reservation().unwrap();
        assert_eq!(
            (
                snapshot.equity,
                snapshot.available_margin,
                snapshot.used_margin
            ),
            (d("121.2"), d("121.2"), d("0"))
        );
        assert_eq!(
            snapshot
                .products
                .iter()
                .map(|p| &p.product)
                .collect::<Vec<_>>(),
            products.iter().map(|p| &p.product).collect::<Vec<_>>()
        );
        assert!(owner.positions.is_empty() && owner.orders.is_empty());
        if count == 1 {
            assert_eq!(owner.valuation_context_id, PARENT_SINGLETON_CONTEXT);
        }
        let wire::Json::Object(facts) = owner.inspect_state().unwrap().wire_json().unwrap() else {
            panic!()
        };
        assert_eq!(
            facts["profile_id"],
            wire::Json::Text("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1".into())
        );
        let mut reversed = products;
        reversed.reverse();
        let other = ScenarioAccount::from_configured(&seed, d("10"), reversed).unwrap();
        assert_eq!(
            owner.valuation_context_id == other.valuation_context_id,
            count == 1
        );
    }
}

#[test]
fn configured_shape_deviations_never_expose_owner() {
    let (seed, products) = input(2);
    type Mutation = fn(&mut ConfiguredProduct);
    let mutations: &[Mutation] = &[
        |p| p.product = Product("".into()),
        |p| p.instrument_code = 0,
        |p| p.taker_fee = d("-0.001"),
        |p| p.liquidation_fee = d("-0.001"),
        |p| p.specs.clear(),
        |p| p.specs.push(p.specs[0].clone()),
        |p| p.tiers.clear(),
        |p| p.tiers.push(p.tiers[0].clone()),
        |p| p.marks.clear(),
        |p| p.marks.push(p.marks[0].clone()),
        |p| p.specs[0].product = Product("other".into()),
        |p| p.specs[0].version.clear(),
        |p| p.specs[0].interval.from = 1,
        |p| p.specs[0].interval.to = Some(3000),
        |p| p.specs[0].contract_value = d("0"),
        |p| p.specs[0].multiplier = d("0"),
        |p| p.specs[0].tick = d("0"),
        |p| p.specs[0].lot = d("0"),
        |p| p.specs[0].minimum = d("0"),
        |p| p.tiers[0].product = Product("other".into()),
        |p| p.tiers[0].version.clear(),
        |p| p.tiers[0].interval.from = 1,
        |p| p.tiers[0].interval.to = Some(3000),
        |p| p.tiers[0].tiers.clear(),
        |p| {
            let duplicate = p.tiers[0].tiers[0].clone();
            p.tiers[0].tiers.push(duplicate);
        },
        |p| p.tiers[0].tiers[0].minimum = d("-1"),
        |p| p.tiers[0].tiers[0].maximum = d("-1"),
        |p| p.tiers[0].tiers[0].mmr = d("-0.1"),
        |p| p.tiers[0].tiers[0].imr = d("-0.1"),
        |p| p.tiers[0].tiers[0].max_leverage = d("0"),
        |p| p.marks[0].product = Product("other".into()),
        |p| p.marks[0].price = d("0"),
        |p| p.marks[0].valid_from = -1,
        |p| p.marks[0].valid_from = 501,
        |p| p.marks[0].valid_to = 0,
    ];
    for change in mutations {
        let mut rows = products.clone();
        change(&mut rows[0]);
        assert_eq!(
            ScenarioAccount::from_configured(&seed, d("10"), rows),
            Err("INVALID_SCHEMA")
        );
    }
    let mut duplicate_id = vec![products[0].clone(); 2];
    duplicate_id[1].instrument_code = 2;
    for rows in [vec![], duplicate_id] {
        assert_eq!(
            ScenarioAccount::from_configured(&seed, d("10"), rows),
            Err("INVALID_SCHEMA")
        );
    }
    let mut duplicate_code = products.clone();
    duplicate_code[1].instrument_code = duplicate_code[0].instrument_code;
    assert_eq!(
        ScenarioAccount::from_configured(&seed, d("10"), duplicate_code),
        Err("INVALID_SCHEMA")
    );
    for change in [
        |s: &mut CleanSeed| s.key.venue.clear(),
        |s: &mut CleanSeed| s.effective_at = -1,
        |s: &mut CleanSeed| s.config_id.clear(),
    ] {
        let mut invalid = seed.clone();
        change(&mut invalid);
        assert_eq!(
            ScenarioAccount::from_configured(&invalid, d("10"), products.clone()),
            Err("INVALID_SCHEMA")
        );
    }
    assert_eq!(
        ScenarioAccount::from_configured(&seed, d("0"), products),
        Err("INVALID_SCHEMA")
    );
}

#[test]
fn configured_empty_accepts_non_scale_values_and_multiple_tiers() {
    let (mut seed, mut products) = input(2);
    seed.config_id = "custom-config".into();
    seed.effective_at = 73;
    seed.cash = d("-17.25");
    for (index, product) in products.iter_mut().enumerate() {
        product.taker_fee = d("0");
        product.liquidation_fee = d("0.02");
        product.specs[0].version = format!("custom-spec-{index}");
        product.specs[0].contract_value = d("3.25");
        product.specs[0].multiplier = d("2");
        product.specs[0].tick = d("0.25");
        product.specs[0].lot = d("0.1");
        product.specs[0].minimum = d("0.2");
        product.tiers[0].version = format!("custom-tier-{index}");
        product.tiers[0].tiers = vec![
            Tier {
                minimum: d("0"),
                maximum: d("10"),
                mmr: d("0"),
                imr: d("0.03"),
                max_leverage: d("2"),
            },
            Tier {
                minimum: d("10.1"),
                maximum: d("20"),
                mmr: d("0.2"),
                imr: d("0.1"),
                max_leverage: d("40"),
            },
        ];
        product.marks[0].price = d("12.75");
        product.marks[0].valid_from = 50;
        product.marks[0].valid_to = 100;
    }
    let owner = ScenarioAccount::from_configured(&seed, d("25"), products.clone()).unwrap();
    assert!(owner.positions.is_empty() && owner.orders.is_empty());
    assert_eq!(owner.cash, d("-17.25"));
    assert_eq!(owner.seed_effective_at, 73);
    let ProfileContext::BtcEthScenario { scenario, .. } = &owner.profile else {
        panic!()
    };
    assert_eq!(scenario.leverage, d("25"));
    let mut changed_seed = seed.clone();
    changed_seed.config_id = "another-config".into();
    let other = ScenarioAccount::from_configured(&changed_seed, d("25"), products).unwrap();
    assert_ne!(owner.valuation_context_id, other.valuation_context_id);
}

#[test]
fn configured_multi_version_timelines_resolve_boundaries_and_mark_gaps() {
    let (seed, product) = multi_version_input();
    let owner = ScenarioAccount::from_configured(&seed, d("10"), vec![product]).unwrap();
    let (scenario, marks) = owner.btc_context().unwrap();
    let products = scenario.products();
    for (at, expected) in [
        (39, ("scale-spec-v1", "scale-tier-v1")),
        (40, ("spec-v2", "tier-v2")),
        (99, ("spec-v2", "tier-v2")),
        (100, ("spec-v3", "tier-v3")),
    ] {
        let (spec, tier) = scenario.resolve(&products[0], at).unwrap();
        assert_eq!((spec.version.as_str(), tier.version.as_str()), expected);
    }
    assert_eq!(marks[1].price, d("101"));
}

#[test]
fn configured_multi_version_identity_covers_inactive_timelines_causally() {
    let (seed, product) = multi_version_input();
    let changes: &[fn(&mut ConfiguredProduct)] = &[
        |p| p.specs[0].product = Product("changed".into()),
        |p| p.specs[0].version.push('x'),
        |p| p.specs[0].interval.from = 1,
        |p| p.specs[0].interval.to = Some(39),
        |p| p.specs[0].contract_value += d("1"),
        |p| p.specs[0].multiplier += d("1"),
        |p| p.specs[0].tick += d("1"),
        |p| p.specs[0].lot += d("0.1"),
        |p| p.specs[0].minimum += d("0.1"),
        |p| {
            p.specs.remove(0);
        },
        |p| p.specs.push(p.specs[2].clone()),
        |p| p.tiers[0].product = Product("changed".into()),
        |p| p.tiers[0].version.push('x'),
        |p| p.tiers[0].interval.from = 1,
        |p| p.tiers[0].interval.to = Some(39),
        |p| p.tiers[0].tiers[0].minimum += d("1"),
        |p| p.tiers[0].tiers[0].maximum += d("1"),
        |p| p.tiers[0].tiers[0].mmr += d("0.001"),
        |p| p.tiers[0].tiers[0].imr += d("0.01"),
        |p| p.tiers[0].tiers[0].max_leverage += d("1"),
        |p| {
            p.tiers.remove(0);
        },
        |p| p.tiers.push(p.tiers[2].clone()),
        |p| {
            let row = p.tiers[0].tiers[0].clone();
            p.tiers[0].tiers.push(row);
        },
        |p| {
            p.tiers[0].tiers.remove(0);
        },
        |p| p.marks[0].product = Product("changed".into()),
        |p| p.marks[0].valid_from = 1,
        |p| p.marks[0].valid_to = 29,
        |p| p.marks[0].price += d("1"),
        |p| {
            p.marks.remove(0);
        },
        |p| p.marks.push(p.marks[0].clone()),
        |p| p.specs.reverse(),
        |p| p.tiers.reverse(),
        |p| p.marks.reverse(),
    ];
    assert_timeline_changes(&seed, &product, changes);
    let scales: &[fn(&mut ConfiguredProduct)] = &[
        |p| p.specs[0].contract_value = d("1.0"),
        |p| p.specs[0].multiplier = d("1.0"),
        |p| p.specs[0].tick = d("1.0"),
        |p| p.specs[0].lot = d("0.50"),
        |p| p.specs[0].minimum = d("0.50"),
        |p| p.tiers[0].tiers[0].minimum = d("0.00"),
        |p| p.tiers[0].tiers[0].maximum = d("100000.0"),
        |p| p.tiers[0].tiers[0].mmr = d("0.0050"),
        |p| p.tiers[0].tiers[0].imr = d("0.100"),
        |p| p.tiers[0].tiers[0].max_leverage = d("10.0"),
        |p| p.marks[0].price = d("100.0"),
    ];
    assert_timeline_scale_equivalent(&seed, &product, scales);
}
#[test]
fn configured_multi_version_rejects_invalid_timeline_shapes_before_owner() {
    let mutations: &[fn(&mut ConfiguredProduct)] = &[
        |p| p.specs.clear(),
        |p| p.specs[0].interval.from = 1,
        |p| p.specs[0].interval.to = Some(0),
        |p| p.specs[0].interval.to = Some(90),
        |p| p.specs[1].interval.to = Some(200),
        |p| p.specs.swap(0, 1),
        |p| p.specs[1].version = p.specs[0].version.clone(),
        |p| p.specs[1].product = Product("other".into()),
        |p| p.tiers.clear(),
        |p| p.tiers[0].interval.to = Some(90),
        |p| p.tiers.swap(0, 1),
        |p| p.tiers[1].interval.to = Some(200),
        |p| p.tiers[1].version = p.tiers[0].version.clone(),
        |p| p.tiers[1].version.clear(),
        |p| p.tiers[1].product = Product("other".into()),
        |p| p.tiers[1].tiers.clear(),
        |p| p.tiers[0].interval.to = Some(110),
        |p| p.marks.clear(),
        |p| p.marks[0].valid_to = 45,
        |p| p.marks.swap(0, 1),
        |p| p.marks[1].valid_from = 51,
    ];
    for change in mutations {
        assert_invalid_timeline(change);
    }
}

#[test]
fn configured_context_identity_covers_every_configured_semantic_field() {
    let (seed, products) = input(2);
    let leverage = d("10");
    assert_context_changes(&seed, leverage, &products, |seed, _, _| {
        seed.effective_at += 1
    });
    assert_context_changes(&seed, leverage, &products, |_, leverage, _| {
        *leverage += d("1")
    });
    assert_context_changes(&seed, leverage, &products, |_, _, rows| rows.reverse());

    macro_rules! change_each_product {
        ($change:expr) => {
            for index in 0..products.len() {
                assert_context_changes(&seed, leverage, &products, |_, _, rows| {
                    ($change)(index, &mut rows[index]);
                });
            }
        };
    }
    change_each_product!(|_, row: &mut ConfiguredProduct| {
        row.product = Product(format!("{}-changed", row.product.0).into());
        row.specs[0].product = row.product.clone();
        row.tiers[0].product = row.product.clone();
        row.marks[0].product = row.product.clone();
    });
    change_each_product!(
        |index, row: &mut ConfiguredProduct| row.instrument_code += 10 + index as i64
    );
    change_each_product!(|_, row: &mut ConfiguredProduct| row.taker_fee += d("0.001"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.liquidation_fee += d("0.001"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.specs[0].version.push('x'));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.specs[0].contract_value += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.specs[0].multiplier += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.specs[0].tick += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.specs[0].lot += d("0.1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.specs[0].minimum += d("0.1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tiers[0].version.push('x'));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tiers[0].tiers[0].minimum = d("0.1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tiers[0].tiers[0].maximum += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tiers[0].tiers[0].mmr += d("0.001"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tiers[0].tiers[0].imr += d("0.01"));
    change_each_product!(
        |_, row: &mut ConfiguredProduct| row.tiers[0].tiers[0].max_leverage += d("1")
    );
    change_each_product!(|_, row: &mut ConfiguredProduct| row.marks[0].price += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.marks[0].valid_from += 1);
    change_each_product!(|_, row: &mut ConfiguredProduct| row.marks[0].valid_to += 1);

    // Spec and tier intervals are fixed by schema, so exercise their encoded
    // fields directly through the private hash function rather than construct
    // an invalid configured owner.
    for interval in [
        Interval { from: 1, to: None },
        Interval {
            from: 0,
            to: Some(3000),
        },
    ] {
        for is_spec in [true, false] {
            let mut changed = products.clone();
            if is_spec {
                changed[0].specs[0].interval = interval;
            } else {
                changed[0].tiers[0].interval = interval;
            }
            let before = context_hash(&configured_scenario(leverage, &products), &seed, &products);
            let after = context_hash(&configured_scenario(leverage, &changed), &seed, &changed);
            assert_ne!(before, after);
        }
    }

    let mut equivalent = products.clone();
    equivalent[0].taker_fee = d("0.0010");
    equivalent[0].liquidation_fee = d("0.0010");
    equivalent[0].specs[0].contract_value = d("1.0");
    equivalent[0].tiers[0].tiers[0].mmr = d("0.0050");
    equivalent[0].marks[0].price = d("100.0");
    let original = ScenarioAccount::from_configured(&seed, leverage, products).unwrap();
    let same = ScenarioAccount::from_configured(&seed, leverage, equivalent).unwrap();
    assert_eq!(original.valuation_context_id, same.valuation_context_id);

    let mut cash_only = seed.clone();
    cash_only.cash += d("1");
    let cash_owner = ScenarioAccount::from_configured(&cash_only, leverage, input(2).1).unwrap();
    assert_eq!(
        original.valuation_context_id,
        cash_owner.valuation_context_id
    );
}

fn configured_context_owner(products: Vec<ConfiguredProduct>, at: i64) -> ScenarioAccount {
    let mut seed = input(products.len()).0;
    seed.effective_at = at;
    ScenarioAccount::from_configured(&seed, d("10"), products).unwrap()
}

#[test]
fn configured_context_identity_is_stable_within_active_rows_and_p1_singleton_is_exact() {
    let (seed, products) = input(1);
    let owner = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
    assert_eq!(owner.valuation_context_id, PARENT_SINGLETON_CONTEXT);
    assert_eq!(owner.validate_context(seed.effective_at), Ok(()));
    assert_eq!(owner.validate_context(seed.effective_at + 1), Ok(()));
    assert_eq!(owner.validate_context(2999), Ok(()));

    let mut other_id = seed.clone();
    other_id.config_id.push_str("-other");
    let other_id = ScenarioAccount::from_configured(&other_id, d("10"), products.clone()).unwrap();
    assert_ne!(owner.valuation_context_id, other_id.valuation_context_id);

    let later_seed = configured_context_owner(products, seed.effective_at + 1);
    assert_ne!(owner.valuation_context_id, later_seed.valuation_context_id);
}

#[test]
fn configured_spec_tier_and_mark_boundaries_mismatch_independently() {
    let (_seed, base) = input(1);

    let mut spec_products = base.clone();
    let mut spec_v1 = spec_products[0].specs[0].clone();
    spec_v1.interval.to = Some(100);
    let spec_v2 = Spec {
        version: "spec-v2".into(),
        interval: Interval {
            from: 100,
            to: None,
        },
        ..spec_v1.clone()
    };
    spec_products[0].specs = vec![spec_v1, spec_v2];
    let spec_owner = configured_context_owner(spec_products, 50);
    let before_spec = spec_owner.valuation_context_id;
    assert_eq!(spec_owner.validate_context(99), Ok(()));
    assert_ne!(
        before_spec,
        owner_context_id(
            &spec_owner.btc_context().unwrap().0,
            spec_owner.btc_context().unwrap().1,
            100,
            spec_owner.seed_effective_at,
            &spec_owner.config_id,
        )
        .unwrap()
    );
    assert_eq!(
        spec_owner.validate_context(100),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );

    let mut tier_products = base.clone();
    let mut tier_v1 = tier_products[0].tiers[0].clone();
    tier_v1.interval.to = Some(100);
    let tier_v2 = TierVersion {
        version: "tier-v2".into(),
        interval: Interval {
            from: 100,
            to: None,
        },
        ..tier_v1.clone()
    };
    tier_products[0].tiers = vec![tier_v1, tier_v2];
    let tier_owner = configured_context_owner(tier_products, 50);
    assert_eq!(tier_owner.validate_context(99), Ok(()));
    assert_eq!(
        tier_owner.validate_context(100),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );

    let mut mark_products = base.clone();
    let mut mark_v1 = mark_products[0].marks[0].clone();
    mark_v1.valid_to = 100;
    let mark_v2 = Mark {
        price: d("101"),
        valid_from: 100,
        valid_to: 3000,
        ..mark_v1.clone()
    };
    mark_products[0].marks = vec![mark_v1, mark_v2];
    let mark_owner = configured_context_owner(mark_products, 50);
    assert_eq!(mark_owner.validate_context(99), Ok(()));
    assert_eq!(
        mark_owner.validate_context(100),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
}

#[test]
fn configured_mark_gap_keeps_the_existing_failure_and_owner_is_unchanged() {
    let (seed, mut products) = input(1);
    let second_mark = products[0].marks[0].clone();
    products[0].marks[0].valid_to = 100;
    products[0].marks.push(Mark {
        valid_from: 150,
        valid_to: 3000,
        ..second_mark
    });
    let owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
    let before = owner.clone();
    assert_eq!(
        owner.validate_context(125),
        Err("UNSUPPORTED_CONTEXT_TRANSITION")
    );
    assert_eq!(owner, before);
}

#[test]
fn configured_two_product_mark_activation_updates_and_stabilizes_owner_identity() {
    let (seed, mut products) = input(2);
    for product in &mut products {
        product.marks[0].valid_to = 600;
        product.marks.push(Mark {
            price: d("101"),
            valid_from: 600,
            valid_to: 3000,
            ..product.marks[0].clone()
        });
    }
    let mut owner = ScenarioAccount::from_configured(&seed, d("10"), products.clone()).unwrap();
    assert_eq!(owner.validate_context(501), Ok(()));
    let marks = products
        .iter()
        .flat_map(|row| row.marks.iter().cloned())
        .collect::<Vec<_>>();
    let activation =
        context::tests::activation(&owner, 600, context::tests::mark_rows(&marks, 600));
    let receipt = owner.activate_context(&activation).unwrap();
    assert_eq!(owner.valuation_context_id, activation.expected_after);
    assert_ne!(receipt.input.expected_before, receipt.input.expected_after);
    assert_eq!(owner.validate_context(601), Ok(()));
}
