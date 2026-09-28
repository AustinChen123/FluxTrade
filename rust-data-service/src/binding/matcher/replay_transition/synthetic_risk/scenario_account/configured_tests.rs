use super::*;
use tests::{d, fixture};

fn input(count: usize) -> (CleanSeed, Vec<ConfiguredProduct>) {
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
                spec: Spec {
                    product: product.clone(),
                    version: "scale-spec-v1".into(),
                    interval: Interval { from: 0, to: None },
                    contract_value: d("1"),
                    multiplier: d("1"),
                    tick: d("1"),
                    lot: d("0.5"),
                    minimum: d("0.5"),
                },
                tier: TierVersion {
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
                },
                mark: Mark {
                    product,
                    price: d("100"),
                    valid_from: 0,
                    valid_to: 3000,
                },
            }
        })
        .collect();
    (seed, products)
}

fn assert_context_changes(
    seed: &CleanSeed,
    leverage: Decimal,
    products: &[ConfiguredProduct],
    change: impl FnOnce(&mut CleanSeed, &mut Decimal, &mut Vec<ConfiguredProduct>),
) {
    let original =
        ScenarioAccount::from_configured_empty(seed, leverage, products.to_vec()).unwrap();
    let mut changed_seed = seed.clone();
    let mut changed_leverage = leverage;
    let mut changed_products = products.to_vec();
    change(
        &mut changed_seed,
        &mut changed_leverage,
        &mut changed_products,
    );
    let changed =
        ScenarioAccount::from_configured_empty(&changed_seed, changed_leverage, changed_products)
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
        .map(|row| row.mark.clone())
        .collect::<Vec<_>>();
    context_id(scenario, &marks, seed.effective_at).unwrap()
}

fn configured_scenario(leverage: Decimal, products: &[ConfiguredProduct]) -> FrozenScenario {
    FrozenScenario {
        configured: Some(products.to_vec()),
        leverage,
        specs: products.iter().map(|row| row.spec.clone()).collect(),
        tiers: products.iter().map(|row| row.tier.clone()).collect(),
    }
}

#[test]
fn configured_empty_owners_preserve_order_and_shared_cash() {
    for count in [1, 2, 12] {
        let (seed, products) = input(count);
        let owner =
            ScenarioAccount::from_configured_empty(&seed, d("10"), products.clone()).unwrap();
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
        let wire::Json::Object(facts) = owner.inspect_state().unwrap().wire_json().unwrap() else {
            panic!()
        };
        assert_eq!(
            facts["profile_id"],
            wire::Json::Text("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1".into())
        );
        let mut reversed = products;
        reversed.reverse();
        let other = ScenarioAccount::from_configured_empty(&seed, d("10"), reversed).unwrap();
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
        |p| p.spec.product = Product("other".into()),
        |p| p.spec.version.clear(),
        |p| p.spec.interval.from = 1,
        |p| p.spec.interval.to = Some(3000),
        |p| p.spec.contract_value = d("0"),
        |p| p.spec.multiplier = d("0"),
        |p| p.spec.tick = d("0"),
        |p| p.spec.lot = d("0"),
        |p| p.spec.minimum = d("0"),
        |p| p.tier.product = Product("other".into()),
        |p| p.tier.version.clear(),
        |p| p.tier.interval.from = 1,
        |p| p.tier.interval.to = Some(3000),
        |p| p.tier.tiers.clear(),
        |p| p.tier.tiers.push(p.tier.tiers[0].clone()),
        |p| p.tier.tiers[0].minimum = d("-1"),
        |p| p.tier.tiers[0].maximum = d("-1"),
        |p| p.tier.tiers[0].mmr = d("-0.1"),
        |p| p.tier.tiers[0].imr = d("-0.1"),
        |p| p.tier.tiers[0].max_leverage = d("0"),
        |p| p.mark.product = Product("other".into()),
        |p| p.mark.price = d("0"),
        |p| p.mark.valid_from = -1,
        |p| p.mark.valid_from = 501,
        |p| p.mark.valid_to = 0,
    ];
    for change in mutations {
        let mut rows = products.clone();
        change(&mut rows[0]);
        assert_eq!(
            ScenarioAccount::from_configured_empty(&seed, d("10"), rows),
            Err("INVALID_SCHEMA")
        );
    }
    let mut duplicate_id = vec![products[0].clone(); 2];
    duplicate_id[1].instrument_code = 2;
    for rows in [vec![], duplicate_id] {
        assert_eq!(
            ScenarioAccount::from_configured_empty(&seed, d("10"), rows),
            Err("INVALID_SCHEMA")
        );
    }
    let mut duplicate_code = products.clone();
    duplicate_code[1].instrument_code = duplicate_code[0].instrument_code;
    assert_eq!(
        ScenarioAccount::from_configured_empty(&seed, d("10"), duplicate_code),
        Err("INVALID_SCHEMA")
    );
    for change in [
        |s: &mut CleanSeed| s.key.venue.clear(),
        |s: &mut CleanSeed| s.effective_at = -1,
        |s: &mut CleanSeed| s.config_id.clear(),
        |s: &mut CleanSeed| s.positions = fixture().0.positions,
        |s: &mut CleanSeed| s.orders = fixture().0.orders,
    ] {
        let mut invalid = seed.clone();
        change(&mut invalid);
        assert_eq!(
            ScenarioAccount::from_configured_empty(&invalid, d("10"), products.clone()),
            Err("INVALID_SCHEMA")
        );
    }
    assert_eq!(
        ScenarioAccount::from_configured_empty(&seed, d("0"), products),
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
        product.spec.version = format!("custom-spec-{index}");
        product.spec.contract_value = d("3.25");
        product.spec.multiplier = d("2");
        product.spec.tick = d("0.25");
        product.spec.lot = d("0.1");
        product.spec.minimum = d("0.2");
        product.tier.version = format!("custom-tier-{index}");
        product.tier.tiers = vec![
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
        product.mark.price = d("12.75");
        product.mark.valid_from = 50;
        product.mark.valid_to = 100;
    }
    let owner = ScenarioAccount::from_configured_empty(&seed, d("25"), products.clone()).unwrap();
    assert!(owner.positions.is_empty() && owner.orders.is_empty());
    assert_eq!(owner.cash, d("-17.25"));
    assert_eq!(owner.seed_effective_at, 73);
    let ProfileContext::BtcEthScenario { scenario, .. } = &owner.profile else {
        panic!()
    };
    assert_eq!(scenario.leverage, d("25"));
    let mut changed_seed = seed.clone();
    changed_seed.config_id = "another-config".into();
    let other = ScenarioAccount::from_configured_empty(&changed_seed, d("25"), products).unwrap();
    assert_ne!(owner.valuation_context_id, other.valuation_context_id);
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
        row.spec.product = row.product.clone();
        row.tier.product = row.product.clone();
        row.mark.product = row.product.clone();
    });
    change_each_product!(
        |index, row: &mut ConfiguredProduct| row.instrument_code += 10 + index as i64
    );
    change_each_product!(|_, row: &mut ConfiguredProduct| row.taker_fee += d("0.001"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.liquidation_fee += d("0.001"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.spec.version.push('x'));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.spec.contract_value += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.spec.multiplier += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.spec.tick += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.spec.lot += d("0.1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.spec.minimum += d("0.1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tier.version.push('x'));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tier.tiers[0].minimum = d("0.1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tier.tiers[0].maximum += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tier.tiers[0].mmr += d("0.001"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tier.tiers[0].imr += d("0.01"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.tier.tiers[0].max_leverage += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.mark.price += d("1"));
    change_each_product!(|_, row: &mut ConfiguredProduct| row.mark.valid_from += 1);
    change_each_product!(|_, row: &mut ConfiguredProduct| row.mark.valid_to += 1);

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
                changed[0].spec.interval = interval;
            } else {
                changed[0].tier.interval = interval;
            }
            let before = context_hash(&configured_scenario(leverage, &products), &seed, &products);
            let after = context_hash(&configured_scenario(leverage, &changed), &seed, &changed);
            assert_ne!(before, after);
        }
    }

    let mut equivalent = products.clone();
    equivalent[0].taker_fee = d("0.0010");
    equivalent[0].liquidation_fee = d("0.0010");
    equivalent[0].spec.contract_value = d("1.0");
    equivalent[0].tier.tiers[0].mmr = d("0.0050");
    equivalent[0].mark.price = d("100.0");
    let original = ScenarioAccount::from_configured_empty(&seed, leverage, products).unwrap();
    let same = ScenarioAccount::from_configured_empty(&seed, leverage, equivalent).unwrap();
    assert_eq!(original.valuation_context_id, same.valuation_context_id);

    let mut cash_only = seed.clone();
    cash_only.cash += d("1");
    let cash_owner =
        ScenarioAccount::from_configured_empty(&cash_only, leverage, input(2).1).unwrap();
    assert_eq!(
        original.valuation_context_id,
        cash_owner.valuation_context_id
    );
}
