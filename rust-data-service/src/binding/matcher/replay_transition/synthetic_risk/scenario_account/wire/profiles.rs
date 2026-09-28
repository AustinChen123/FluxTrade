use super::*;

pub(in super::super) fn construct(
    profile: &str,
    key: AccountKey,
) -> Result<ScenarioAccount, Fault> {
    key.validate().map_err(|_| "INVALID_SCHEMA")?;
    if profile == "SYNTHETIC_MIN_CASH_V1" {
        return ScenarioAccount::synthetic_min_cash(key).map_err(|_| "NATIVE_INVARIANT");
    }
    let mut seed = CleanSeed {
        key,
        config_id: "scenario-v1".into(),
        effective_at: 500,
        cash: Decimal::from(1000),
        positions: Vec::new(),
        orders: Vec::new(),
    };
    if profile == "SYNTHETIC_GOLDEN_CANCEL_V1" {
        return ScenarioAccount::from_golden_cancel_seed(&seed, &golden_cancel::Config::frozen())
            .map_err(|_| "NATIVE_INVARIANT");
    }
    let liquidation = profile == "SYNTHETIC_P1_LIQUIDATION_V1";
    if profile != "SYNTHETIC_BTC_ETH_V1" && !liquidation {
        return Err("INVALID_SCHEMA");
    }
    seed.cash = Decimal::from(10000);
    seed.positions.push(SeedPosition {
        product: Product::Btc,
        side: Side::Long,
        contracts: Decimal::from(3),
        lots: [
            ("X1", 0, "strategy-a", 1, Decimal::from(50000)),
            ("X2", 1, "strategy-b", 2, Decimal::new(500001, 1)),
        ]
        .into_iter()
        .map(|(id, sequence, strategy, quantity, entry)| SeedLot {
            seed_execution_id: id.into(),
            seed_sequence: sequence,
            strategy_id: strategy.into(),
            contracts: Decimal::from(quantity),
            entry,
        })
        .collect(),
    });
    seed.orders.push(SeedOrder {
        intent_id: "I1".into(),
        order_id: "O1".into(),
        client_id: "C1".into(),
        strategy_id: "strategy-c".into(),
        product: ProfileProduct::BtcEth(Product::Btc),
        side: Side::Short,
        price: Decimal::new(500001, 1),
        reduce_only: true,
        original: Decimal::from(2),
        filled: Decimal::ONE,
        canceled: Decimal::ZERO,
        remaining: Decimal::ONE,
        status: "PARTIALLY_FILLED".into(),
    });
    let mut specs = Vec::new();
    let mut tiers = Vec::new();
    for product in [Product::Btc, Product::Eth] {
        for second in [false, true] {
            specs.push(frozen_spec(product, second));
            tiers.push(frozen_tiers(product, second));
        }
    }
    let scenario =
        FrozenScenario::new(Decimal::TEN, specs, tiers).map_err(|_| "NATIVE_INVARIANT")?;
    let mut marks = [(Product::Btc, 50001), (Product::Eth, 1900)]
        .into_iter()
        .map(|(product, price)| Mark {
            product,
            price: Decimal::from(price),
            valid_from: 0,
            valid_to: 3000,
        })
        .collect::<Vec<_>>();
    if liquidation {
        seed.effective_at = 1500;
        seed.cash = Decimal::from(3);
        seed.orders.clear();
        let position = &mut seed.positions[0];
        position.side = Side::Short;
        position.contracts = Decimal::ONE;
        position.lots.truncate(1);
        marks[0].price = Decimal::from(50000);
        marks[0].valid_to = 1501;
        marks.push(Mark {
            product: Product::Btc,
            price: Decimal::from(50100),
            valid_from: 1501,
            valid_to: 3000,
        });
    }
    let mut owner =
        ScenarioAccount::from_seed(&seed, &scenario, &marks).map_err(|_| "NATIVE_INVARIANT")?;
    if let ProfileContext::BtcEthScenario {
        p1_liquidation_profile,
        ..
    } = &mut owner.profile
    {
        *p1_liquidation_profile = liquidation;
    }
    Ok(owner)
}
