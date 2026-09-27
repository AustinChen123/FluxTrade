//! Post-settlement risk classification on the sole owner's unobservable draft.
use super::execution::{remainder_eligibility, FinancialSnapshot, RemainderRole};
use super::*;
pub(super) mod cancel;

impl ScenarioAccount {
    pub(super) fn execution_snapshot_after(
        &mut self,
    ) -> Result<(FinancialSnapshot, ProfileRisk), Fault> {
        if matches!(self.profile, ProfileContext::GoldenCancel(_)) {
            return Ok((
                FinancialSnapshot::GoldenCancel(self.golden_cancel_reservation()?),
                ProfileRisk::CapacitySafe,
            ));
        }
        if matches!(self.profile, ProfileContext::EventLimit(_)) {
            return Ok((
                FinancialSnapshot::EventLimit(self.event_limit_projection()?),
                ProfileRisk::CapacitySafe,
            ));
        }
        let after_reservation = self.reservation()?;
        let (scenario, marks) = self.btc_context()?;
        let valuation = scenario.evaluate(&self.projection()?, marks)?;
        let mut needs_order_action = false;
        for order in self.orders.values() {
            if order
                .facts
                .projects_remainder("INVALID_RESERVATION_ORDER")?
                && remainder_eligibility(
                    &order.facts,
                    self.positions.btc()?.get(&order.facts.product.btc()?),
                ) != Ok(RemainderRole::Reducing)
            {
                needs_order_action = true;
            }
        }
        if valuation.risk == MaintenanceState::Breach
            || (self.positions.is_empty() && valuation.equity < Decimal::ZERO)
            || (after_reservation.available_margin < Decimal::ZERO && needs_order_action)
        {
            self.gate = Gate::Failed("UNSUPPORTED_RISK_TRANSITION");
        }
        Ok((
            FinancialSnapshot::BtcEth(after_reservation),
            ProfileRisk::BtcEth(valuation.risk),
        ))
    }
}
