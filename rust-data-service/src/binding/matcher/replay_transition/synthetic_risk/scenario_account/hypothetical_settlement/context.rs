//! Closed read-only settlement inputs; no caller-defined scaling or fee rate.
use super::*;

#[derive(Clone, Copy)]
pub(in super::super) enum Context<'a> {
    BtcEth(&'a FrozenScenario, &'a Spec),
    Configured(&'a FrozenScenario, &'a Product, &'a Spec, &'a TierVersion),
    GoldenCancel(&'a golden_cancel::Config),
    P1O03,
}

impl<'a> From<(&'a FrozenScenario, &'a Spec)> for Context<'a> {
    fn from((scenario, spec): (&'a FrozenScenario, &'a Spec)) -> Self {
        Self::BtcEth(scenario, spec)
    }
}

pub(super) struct Scaling<'a> {
    pub version: &'a str,
    pub contract_value: Decimal,
    pub multiplier: Decimal,
    pub tick: Decimal,
    pub lot: Decimal,
    pub minimum: Decimal,
    pub maximum: Decimal,
}

impl<'a> Context<'a> {
    fn btc(spec: &'a Spec) -> Scaling<'a> {
        Scaling {
            version: &spec.version,
            contract_value: spec.contract_value,
            multiplier: spec.multiplier,
            tick: spec.tick,
            lot: spec.lot,
            minimum: spec.minimum,
            maximum: frozen_tiers(&spec.product, false).tiers[2].maximum,
        }
    }

    pub(super) fn active(self, fee: FeePolicy) -> Result<Scaling<'a>, Fault> {
        match self {
            Self::BtcEth(_, spec) => {
                if *spec != frozen_spec(&spec.product, spec.version == "spec-v2")
                    || matches!(
                        fee,
                        FeePolicy::GoldenCancelTradingTaker | FeePolicy::P1O03Zero
                    )
                {
                    return Err("UNSUPPORTED_SPEC");
                }
                Ok(Self::btc(spec))
            }
            Self::Configured(scenario, product, spec, tier) => {
                let configured_fee = scenario
                    .configured
                    .as_ref()
                    .and_then(|rows| rows.iter().find(|row| row.product == *product))
                    .map(|row| row.taker_fee)
                    .ok_or("UNSUPPORTED_CONFIGURED_SETTLEMENT")?;
                if scenario.leverage != Decimal::TEN
                    || scenario
                        .configured
                        .as_ref()
                        .is_none_or(|rows| rows.iter().any(|row| row.taker_fee < Decimal::ZERO))
                    || !matches!(fee, FeePolicy::BtcEthTradingTaker)
                        && fee != FeePolicy::ConfiguredTaker(configured_fee)
                    || !scenario.configured.as_ref().is_some_and(|rows| {
                        rows.iter().any(|row| {
                            row.product == *product
                                && row.specs.contains(spec)
                                && row.tiers.contains(tier)
                        })
                    })
                {
                    return Err("UNSUPPORTED_CONFIGURED_SETTLEMENT");
                }
                Ok(Scaling {
                    version: &spec.version,
                    contract_value: spec.contract_value,
                    multiplier: spec.multiplier,
                    tick: spec.tick,
                    lot: spec.lot,
                    minimum: spec.minimum,
                    maximum: tier
                        .tiers
                        .last()
                        .ok_or("UNSUPPORTED_POSITION_TIER")?
                        .maximum,
                })
            }
            Self::GoldenCancel(_) | Self::P1O03 => {
                let expected = match self {
                    Self::GoldenCancel(config) => {
                        config.validate(0)?;
                        FeePolicy::GoldenCancelTradingTaker
                    }
                    _ => FeePolicy::P1O03Zero,
                };
                if fee != expected {
                    return Err("UNSUPPORTED_FEE_INPUT");
                }
                Ok(Scaling {
                    version: "gt03-spec-v1",
                    contract_value: Decimal::ONE,
                    multiplier: Decimal::ONE,
                    tick: Decimal::ONE,
                    lot: Decimal::ONE,
                    minimum: Decimal::ONE,
                    maximum: Decimal::TEN,
                })
            }
        }
    }

    pub(super) fn origin(self, version: &str) -> Result<Scaling<'a>, Fault> {
        match self {
            Self::BtcEth(scenario, active) => scenario
                .specs
                .iter()
                .find(|s| s.product == active.product && s.version == version)
                .map(Self::btc)
                .ok_or("INVALID_LOT_ORIGIN_SPEC"),
            Self::Configured(scenario, product, _, _) => scenario
                .configured
                .as_ref()
                .and_then(|rows| rows.iter().find(|row| row.product == *product))
                .and_then(|row| row.specs.iter().find(|spec| spec.version == version))
                .map(|spec| Scaling {
                    version: &spec.version,
                    contract_value: spec.contract_value,
                    multiplier: spec.multiplier,
                    tick: spec.tick,
                    lot: spec.lot,
                    minimum: spec.minimum,
                    maximum: scenario
                        .configured
                        .as_ref()
                        .and_then(|rows| rows.iter().find(|row| row.product == *product))
                        .and_then(|row| row.tiers.last())
                        .and_then(|tiers| tiers.tiers.last())
                        .map_or(Decimal::ZERO, |row| row.maximum),
                })
                .ok_or("INVALID_LOT_ORIGIN_SPEC"),
            Self::GoldenCancel(_) if version == "gt03-spec-v1" => {
                self.active(FeePolicy::GoldenCancelTradingTaker)
            }
            Self::P1O03 if version == "gt03-spec-v1" => self.active(FeePolicy::P1O03Zero),
            _ => Err("INVALID_LOT_ORIGIN_SPEC"),
        }
    }

    pub(super) fn tier_ceiling(self, contracts: Decimal, fee: FeePolicy) -> Result<Decimal, Fault> {
        match self {
            Self::Configured(_, _, _, tier) => tier
                .tiers
                .iter()
                .find(|row| contracts >= row.minimum && contracts <= row.maximum)
                .map(|row| row.maximum)
                .ok_or("UNSUPPORTED_POSITION_TIER"),
            _ => Ok(self.active(fee)?.maximum),
        }
    }
}
