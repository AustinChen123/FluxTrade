//! Frozen P1 scenario configuration construction and exact validation.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ConfiguredProduct {
    pub product: Product,
    pub instrument_code: i64,
    pub taker_fee: Decimal,
    pub liquidation_fee: Decimal,
    pub specs: Vec<Spec>,
    pub tiers: Vec<TierVersion>,
    pub marks: Vec<Mark>,
}

impl ConfiguredProduct {
    fn valid(&self) -> bool {
        super::super::identity(&self.product.0)
            && self.instrument_code > 0
            && self.taker_fee >= Decimal::ZERO
            && self.liquidation_fee >= Decimal::ZERO
            && !self.specs.is_empty()
            && self.specs.iter().enumerate().all(|(index, spec)| {
                spec.product == self.product
                    && super::super::identity(&spec.version)
                    && self.specs[..index]
                        .iter()
                        .all(|previous| previous.version != spec.version)
                    && spec.interval.from >= 0
                    && spec.interval.to.is_none_or(|to| to > spec.interval.from)
                    && (if index == 0 {
                        spec.interval.from == 0
                    } else {
                        self.specs[index - 1].interval.to == Some(spec.interval.from)
                    })
                    && (index + 1 == self.specs.len() || spec.interval.to.is_some())
                    && [
                        spec.contract_value,
                        spec.multiplier,
                        spec.tick,
                        spec.lot,
                        spec.minimum,
                    ]
                    .iter()
                    .all(|value| *value > Decimal::ZERO)
            })
            && self
                .specs
                .last()
                .is_some_and(|spec| spec.interval.to.is_none())
            && !self.tiers.is_empty()
            && self.tiers.iter().enumerate().all(|(index, tier)| {
                tier.product == self.product
                    && super::super::identity(&tier.version)
                    && self.tiers[..index]
                        .iter()
                        .all(|previous| previous.version != tier.version)
                    && tier.interval.from >= 0
                    && tier.interval.to.is_none_or(|to| to > tier.interval.from)
                    && (if index == 0 {
                        tier.interval.from == 0
                    } else {
                        self.tiers[index - 1].interval.to == Some(tier.interval.from)
                    })
                    && (index + 1 == self.tiers.len() || tier.interval.to.is_some())
                    && !tier.tiers.is_empty()
                    && tier.tiers.iter().enumerate().all(|(row_index, row)| {
                        row.minimum >= Decimal::ZERO
                            && row.maximum >= row.minimum
                            && row.mmr >= Decimal::ZERO
                            && row.imr >= Decimal::ZERO
                            && row.max_leverage > Decimal::ZERO
                            && (row_index == 0 || tier.tiers[row_index - 1].maximum < row.minimum)
                    })
            })
            && self
                .tiers
                .last()
                .is_some_and(|tier| tier.interval.to.is_none())
            && !self.marks.is_empty()
            && self.marks.iter().enumerate().all(|(index, mark)| {
                mark.product == self.product
                    && mark.price > Decimal::ZERO
                    && mark.valid_from >= 0
                    && mark.valid_to > mark.valid_from
                    && (index == 0 || self.marks[index - 1].valid_to <= mark.valid_from)
            })
    }
}

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
            configured: None,
            leverage,
            specs,
            tiers,
        };
        scenario.validate()?;
        Ok(scenario)
    }

    pub(super) fn products(&self) -> Vec<Product> {
        self.configured.as_ref().map_or_else(
            || vec![Product::Btc, Product::Eth],
            |rows| rows.iter().map(|r| r.product.clone()).collect(),
        )
    }

    pub(super) fn validate(&self) -> Result<(), Fault> {
        if let Some(rows) = &self.configured {
            let mut ids = std::collections::BTreeSet::new();
            let mut codes = std::collections::BTreeSet::new();
            return if self.leverage > Decimal::ZERO
                && !rows.is_empty()
                && rows
                    .iter()
                    .all(|r| r.valid() && ids.insert(&r.product) && codes.insert(r.instrument_code))
                && self.specs
                    == rows
                        .iter()
                        .flat_map(|r| r.specs.iter().cloned())
                        .collect::<Vec<_>>()
                && self.tiers
                    == rows
                        .iter()
                        .flat_map(|r| r.tiers.iter().cloned())
                        .collect::<Vec<_>>()
            {
                Ok(())
            } else {
                Err("INVALID_SCHEMA")
            };
        }
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
