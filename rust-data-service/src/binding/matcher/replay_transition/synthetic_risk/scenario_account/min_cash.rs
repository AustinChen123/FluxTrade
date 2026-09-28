//! Closed Slice 5 profile; all financial calculation remains in the existing owner.
use super::*;

impl ScenarioAccount {
    pub(super) fn synthetic_min_cash(key: AccountKey) -> Result<Self, Fault> {
        let seed = CleanSeed {
            key,
            config_id: "scenario-v1".into(),
            effective_at: 500,
            cash: Decimal::from(1000),
            positions: vec![SeedPosition {
                product: Product::Btc,
                side: Side::Long,
                contracts: Decimal::from(20),
                lots: vec![SeedLot {
                    seed_execution_id: "MIN-SEED-X1".into(),
                    seed_sequence: 0,
                    strategy_id: "min-cash-seed".into(),
                    contracts: Decimal::from(20),
                    entry: Decimal::from(50000),
                }],
            }],
            orders: vec![SeedOrder {
                intent_id: "MIN-I1".into(),
                order_id: "MIN-O1".into(),
                client_id: "MIN-C1".into(),
                strategy_id: "min-cash-close".into(),
                product: ProfileProduct::BtcEth(Product::Btc),
                side: Side::Short,
                price: Decimal::from(50000),
                reduce_only: true,
                original: Decimal::from(20),
                filled: Decimal::ZERO,
                canceled: Decimal::ZERO,
                remaining: Decimal::from(20),
                status: "OPEN".into(),
            }],
        };
        let mut specs = Vec::new();
        let mut tiers = Vec::new();
        for product in [Product::Btc, Product::Eth] {
            for second in [false, true] {
                specs.push(frozen_spec(&product, second));
                tiers.push(frozen_tiers(&product, second));
            }
        }
        let scenario = FrozenScenario::new(Decimal::TEN, specs, tiers)?;
        let marks = [
            (Product::Btc, 50000, 0, 3000),
            (Product::Eth, 100, 0, 503),
            (Product::Eth, 101, 503, 3000),
        ]
        .into_iter()
        .map(|(product, price, valid_from, valid_to)| Mark {
            product,
            price: Decimal::from(price),
            valid_from,
            valid_to,
        })
        .collect::<Vec<_>>();
        let mut owner = Self::from_seed(&seed, &scenario, &marks)?;
        if let ProfileContext::BtcEthScenario {
            min_cash_profile, ..
        } = &mut owner.profile
        {
            *min_cash_profile = true;
        }
        Ok(owner)
    }
}
