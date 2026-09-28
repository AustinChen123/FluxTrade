//! Frozen P1 scenario configuration construction and exact validation.
use super::*;

pub(super) fn interval(second: bool, boundary: i64) -> Interval {
    if second {
        Interval {
            from: boundary,
            to: None,
        }
    } else {
        Interval {
            from: 0,
            to: Some(boundary),
        }
    }
}

// These are the frozen Scenario §§3/7 inputs, never provider defaults.
pub(super) fn frozen_spec(product: &Product, second: bool) -> Spec {
    let btc = p1_btc(product);
    Spec {
        product: product.clone(),
        version: if second { "spec-v2" } else { "spec-v1" }.into(),
        interval: interval(second, 2000),
        contract_value: Decimal::new(1, if btc { 2 } else { 1 }),
        multiplier: Decimal::ONE,
        tick: Decimal::new(
            1,
            match (btc, second) {
                (true, true) => 0,
                (true, false) | (false, true) => 1,
                (false, false) => 2,
            },
        ),
        lot: Decimal::new(1, 2),
        minimum: Decimal::new(1, 2),
    }
}

pub(super) fn frozen_tiers(product: &Product, second: bool) -> TierVersion {
    let maximums = if p1_btc(product) {
        [1000, 5000, 20000]
    } else {
        [5000, 10000, 25000]
    };
    let mmr = if second { [45, 55, 80] } else { [40, 50, 75] };
    let imr = if second { [11, 16, 21] } else { [10, 15, 20] };
    let leverage = if second {
        [9000, 6000, 4500]
    } else {
        [10000, 6666, 5000]
    };
    TierVersion {
        product: product.clone(),
        version: if second { "tier-v2" } else { "tier-v1" }.into(),
        interval: interval(second, 1000),
        tiers: (0..3)
            .map(|i| Tier {
                minimum: if i == 0 {
                    Decimal::ZERO
                } else {
                    Decimal::new(maximums[i - 1] * 100 + 1, 2)
                },
                maximum: Decimal::from(maximums[i]),
                mmr: Decimal::new(mmr[i], 4),
                imr: Decimal::new(imr[i], 3),
                max_leverage: Decimal::new(leverage[i], 2),
            })
            .collect(),
    }
}

// These helpers describe only frozen P1 products, not product admission.
fn p1_btc(product: &Product) -> bool {
    assert!(*product == Product::Btc || *product == Product::Eth);
    *product == Product::Btc
}

impl FrozenScenario {
    pub(super) fn new(
        leverage: Decimal,
        specs: Vec<Spec>,
        tiers: Vec<TierVersion>,
    ) -> Result<Self, Fault> {
        let scenario = Self {
            leverage,
            specs,
            tiers,
        };
        scenario.validate()?;
        Ok(scenario)
    }

    pub(super) fn products(&self) -> [Product; 2] {
        [Product::Btc, Product::Eth]
    }

    pub(super) fn validate(&self) -> Result<(), Fault> {
        if self.leverage <= Decimal::ZERO
            || self
                .tiers
                .iter()
                .flat_map(|v| &v.tiers)
                .any(|t| self.leverage > t.max_leverage)
        {
            return Err("LEVERAGE_TIER_CONFLICT");
        }
        if self.leverage != Decimal::TEN || self.specs.len() != 4 || self.tiers.len() != 4 {
            return Err("UNSUPPORTED_SCENARIO_CONFIG");
        }
        // Exact whole-table validation rejects gaps/overlaps/third versions and
        // unsupported migrations, including future versions, before any output.
        for product in self.products() {
            for second in [false, true] {
                let spec = frozen_spec(&product, second);
                let tier = frozen_tiers(&product, second);
                if self.specs.iter().filter(|v| **v == spec).count() != 1
                    || self.tiers.iter().filter(|v| **v == tier).count() != 1
                {
                    return Err("INVALID_FROZEN_TIMELINE");
                }
            }
        }
        Ok(())
    }
}
