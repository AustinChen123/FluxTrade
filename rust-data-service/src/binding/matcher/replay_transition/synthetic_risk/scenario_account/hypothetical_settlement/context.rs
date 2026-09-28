//! Closed read-only settlement inputs; no caller-defined scaling or fee rate.
use super::*;

#[derive(Clone, Copy)]
pub(in super::super) enum Context<'a> {
    BtcEth(&'a FrozenScenario, &'a Spec),
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
            Self::GoldenCancel(_) if version == "gt03-spec-v1" => {
                self.active(FeePolicy::GoldenCancelTradingTaker)
            }
            Self::P1O03 if version == "gt03-spec-v1" => self.active(FeePolicy::P1O03Zero),
            _ => Err("INVALID_LOT_ORIGIN_SPEC"),
        }
    }
}
