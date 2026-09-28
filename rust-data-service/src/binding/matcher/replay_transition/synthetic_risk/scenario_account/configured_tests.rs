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
        |p| p.taker_fee = d("0"),
        |p| p.liquidation_fee = d("0"),
        |p| p.spec.product = Product("other".into()),
        |p| p.spec.version.clear(),
        |p| p.spec.interval.from = 1,
        |p| p.spec.interval.to = Some(3000),
        |p| p.spec.contract_value = d("2"),
        |p| p.spec.multiplier = d("2"),
        |p| p.spec.tick = d("2"),
        |p| p.spec.lot = d("1"),
        |p| p.spec.minimum = d("1"),
        |p| p.tier.product = Product("other".into()),
        |p| p.tier.version.clear(),
        |p| p.tier.interval.from = 1,
        |p| p.tier.interval.to = Some(3000),
        |p| p.tier.tiers.clear(),
        |p| p.tier.tiers.push(p.tier.tiers[0].clone()),
        |p| p.tier.tiers[0].minimum = d("1"),
        |p| p.tier.tiers[0].maximum = d("99999"),
        |p| p.tier.tiers[0].mmr = d("0"),
        |p| p.tier.tiers[0].imr = d("0"),
        |p| p.tier.tiers[0].max_leverage = d("9"),
        |p| p.mark.product = Product("other".into()),
        |p| p.mark.price = d("101"),
        |p| p.mark.valid_from = 1,
        |p| p.mark.valid_to = 2999,
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
        |s: &mut CleanSeed| s.cash = d("121"),
        |s: &mut CleanSeed| s.effective_at = 501,
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
        ScenarioAccount::from_configured_empty(&seed, d("9"), products),
        Err("INVALID_SCHEMA")
    );
}
