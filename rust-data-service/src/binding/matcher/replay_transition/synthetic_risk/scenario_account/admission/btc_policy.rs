//! Pure BTC/ETH admission policy; settlement drafts are evidence, never commits.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    RiskIncreasing,
    RiskReducing,
    Rejected(Fault),
}

fn classify(
    current: Option<(Side, Decimal)>,
    side: Side,
    quantity: Decimal,
    reduce_only: bool,
) -> Role {
    match current {
        Some((position_side, contracts)) if side != position_side && quantity <= contracts => {
            Role::RiskReducing
        }
        _ if reduce_only => Role::Rejected("REDUCE_ONLY_NOT_REDUCING"),
        Some((position_side, _)) if side != position_side => {
            Role::Rejected("POSITION_CROSS_ZERO_UNSUPPORTED")
        }
        _ => Role::RiskIncreasing,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Stress {
    pub equity: Decimal,
    pub maintenance_margin: Decimal,
    pub excess: Decimal,
    pub current_excess: Decimal,
    pub fee: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Evidence {
    pub role: Role,
    pub current_risk: MaintenanceState,
    pub post_reservation: Option<reservation::Snapshot>,
    pub stress: Option<Stress>,
}

pub(super) fn evaluate(
    current: &ScenarioAccount,
    candidate_draft: &ScenarioAccount,
    intent: &OrderIntent,
    price: Decimal,
) -> Result<(Evidence, Option<Fault>), Fault> {
    let product = intent.product.btc()?;
    let (scenario, marks) = current.btc_context()?;
    let input = current.projection()?;
    let value = scenario.evaluate(&input, marks)?;
    let position = current.positions.btc()?.get(product);
    let role = classify(
        position.map(|p| (p.side, p.contracts)),
        intent.side,
        intent.quantity,
        intent.reduce_only,
    );
    let mut evidence = Evidence {
        role,
        current_risk: value.risk,
        post_reservation: None,
        stress: None,
    };
    if value.risk == MaintenanceState::Breach {
        return Ok((evidence, Some("MMR_BREACH")));
    }
    if let Role::Rejected(reason) = role {
        return Ok((evidence, Some(reason)));
    }
    let post = candidate_draft.reservation()?;
    let post_safe = maintenance_state(
        !input.positions.is_empty(),
        post.equity,
        post.maintenance_margin,
    ) == MaintenanceState::Safe;
    let sufficient = post_safe && post.available_margin >= Decimal::ZERO;
    evidence.post_reservation = Some(post);
    if sufficient {
        return Ok((evidence, None));
    }
    if role == Role::RiskReducing {
        let (spec, tier) = scenario.resolve(product, input.effective_at)?;
        let context = if scenario.configured.is_some() {
            hypothetical_settlement::Context::Configured(scenario, product, spec, tier)
        } else {
            hypothetical_settlement::Context::BtcEth(scenario, spec)
        };
        let fee_policy = hypothetical_settlement::configured_fee_policy(scenario, product)?;
        let settlement = hypothetical_settlement::calculate(
            position,
            intent.side,
            intent.quantity,
            price,
            context,
            fee_policy,
            None,
        )?;
        let mut stress_input = input;
        stress_input.cash = add(stress_input.cash, settlement.cash_delta)?;
        stress_input.positions.retain(|p| &p.product != product);
        if let Some(position) = settlement.position {
            stress_input.positions.push(NetPosition {
                product: product.clone(),
                side: position.side,
                contracts: position.contracts,
                lots: position
                    .lots
                    .iter()
                    .map(|lot| ValuationLot {
                        contracts: lot.source.contracts,
                        entry: lot.source.entry,
                    })
                    .collect(),
            });
        }
        let after = scenario.evaluate(&stress_input, marks)?;
        let stress = Stress {
            equity: after.equity,
            maintenance_margin: after.maintenance_margin,
            excess: add(after.equity, -after.maintenance_margin)?,
            current_excess: add(value.equity, -value.maintenance_margin)?,
            fee: settlement.fee,
        };
        let improves =
            stress.equity > stress.maintenance_margin && stress.excess > stress.current_excess;
        evidence.stress = Some(stress);
        if improves {
            return Ok((evidence, None));
        }
    }
    Ok((evidence, Some("INSUFFICIENT_SHARED_EQUITY")))
}

#[cfg(test)]
mod tests;
