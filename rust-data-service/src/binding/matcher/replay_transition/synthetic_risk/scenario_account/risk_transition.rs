//! Post-settlement risk classification on the sole owner's unobservable draft.
use super::execution::{remainder_eligibility, FinancialSnapshot, RemainderRole};
use super::*;
pub(super) mod cancel;
mod liquidation;
#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Lifecycle {
    #[default]
    RiskStable,
    AwaitingCancelEffective,
    LiquidatedFlat,
    LiquidatedInsolvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Episode {
    pub id: Hash,
    pub first_detecting_event: String,
    pub initial_reason: cancel::Reason,
    pub current_reason: cancel::Reason,
    pub escalation_event: Option<String>,
    pub release_event: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Facts {
    pub lifecycle: Lifecycle,
    pub episode: Option<Episode>,
    pub context_at: Option<i64>,
    pub accepted_stamp: Option<cancel::Stamp>,
    pub contexts: BTreeMap<String, context::Receipt>,
    pub groups: BTreeMap<(String, String), group::Completion>,
    pub admissions: BTreeMap<(String, String), group::Admission>,
    pub events: BTreeMap<String, (cancel::Stamp, Hash)>,
    pub event_kinds: BTreeMap<String, source::Kind>,
    pub reverse_group: bool,
    pub batches: BTreeMap<Hash, AutomaticReceipt>,
    pub completed_episodes: Vec<Episode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AutomaticReceipt {
    pub id: Hash,
    pub digest: Hash,
    pub episode: Episode,
    pub batch: cancel::BatchResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Decision {
    pub reason: Option<cancel::Reason>,
    pub targets: Vec<String>,
    pub pending: Vec<Hash>,
    pub liquidation_required: bool,
    pub insolvent: bool,
}

impl ScenarioAccount {
    pub(super) fn execution_snapshot_after(
        &mut self,
        stamp: &cancel::Stamp,
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
        self.prepare_risk(stamp, &after_reservation)?;
        Ok((
            FinancialSnapshot::BtcEth(after_reservation),
            ProfileRisk::BtcEth(valuation.risk),
        ))
    }

    pub(super) fn classify_risk(
        &self,
        snapshot: &reservation::Snapshot,
    ) -> Result<Decision, Fault> {
        let breach = !self.positions.is_empty() && snapshot.equity <= snapshot.maintenance_margin;
        let reason = if breach
            || self
                .transition
                .episode
                .as_ref()
                .is_some_and(|e| e.current_reason == cancel::Reason::MmrBreach)
        {
            Some(cancel::Reason::MmrBreach)
        } else if snapshot.available_margin < Decimal::ZERO {
            Some(cancel::Reason::RiskShortfall)
        } else {
            None
        };
        let mut targets = Vec::new();
        let mut pending = Vec::new();
        for order in self.orders.values() {
            if let cancel::State::Requested(request) = &order.cancel {
                pending.push(request.effect_action_id);
            }
            if !order
                .facts
                .projects_remainder("INVALID_RESERVATION_ORDER")?
            {
                continue;
            }
            if (reason == Some(cancel::Reason::MmrBreach)
                || (reason == Some(cancel::Reason::RiskShortfall)
                    && remainder_eligibility(
                        &order.facts,
                        self.positions.btc()?.get(&order.facts.product.btc()?),
                    ) != Ok(RemainderRole::Reducing)))
                && matches!(order.cancel, cancel::State::None)
            {
                targets.push(order.facts.order_id.clone());
            }
        }
        targets.sort_by_key(|id| (self.orders[id].facts.product.canonical_id(), id.clone()));
        pending.sort();
        Ok(Decision {
            reason,
            liquidation_required: breach && targets.is_empty() && pending.is_empty(),
            insolvent: self.positions.is_empty() && snapshot.equity < Decimal::ZERO,
            targets,
            pending,
        })
    }

    pub(super) fn immediate_risk(&mut self, stamp: &cancel::Stamp) -> Result<(), Fault> {
        self.immediate_risk_checked(stamp, |_| Ok(()))
    }

    pub(super) fn immediate_risk_checked(
        &mut self,
        stamp: &cancel::Stamp,
        hook: impl FnMut(cancel::Stage) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        if !matches!(self.profile, ProfileContext::BtcEthScenario { .. }) {
            self.transition.lifecycle = if self
                .orders
                .values()
                .any(|o| matches!(o.cancel, cancel::State::Requested(_)))
            {
                Lifecycle::AwaitingCancelEffective
            } else {
                Lifecycle::RiskStable
            };
            return Ok(());
        }
        let decision = self.prepare_risk(stamp, &self.reservation()?)?;
        if decision.insolvent {
            return Ok(());
        }
        if !decision.targets.is_empty() {
            self.automatic_cancel_checked(
                &cancel::RequestInput {
                    stamp: stamp.clone(),
                    targets: decision
                        .targets
                        .iter()
                        .map(|id| (id.clone(), decision.reason.expect("selected policy")))
                        .collect(),
                },
                hook,
            )?;
        }
        if decision.liquidation_required && self.gate == Gate::Running {
            self.gate = Gate::Failed("UNSUPPORTED_RISK_TRANSITION");
        }
        Ok(())
    }

    pub(super) fn prepare_risk(
        &mut self,
        stamp: &cancel::Stamp,
        snapshot: &reservation::Snapshot,
    ) -> Result<Decision, Fault> {
        let decision = self.classify_risk(snapshot)?;
        if self.transition.lifecycle == Lifecycle::AwaitingCancelEffective
            && decision.pending.is_empty()
            && decision.targets.is_empty()
        {
            if let Some(episode) = &mut self.transition.episode {
                episode.release_event = Some(stamp.event_id.clone());
            }
        }
        if decision.insolvent {
            self.transition.lifecycle = Lifecycle::LiquidatedInsolvent;
            self.finish_episode(stamp);
            return Ok(decision);
        }
        if !decision.targets.is_empty()
            || !decision.pending.is_empty()
            || decision.liquidation_required
        {
            if let Some(reason) = decision.reason {
                if self.transition.episode.is_none() {
                    let mut e = cancel::identity::Encoding::new("RISK_ACTION_EPISODE");
                    e.account(&self.key);
                    e.text(&stamp.event_id);
                    e.integer(self.state_version as i64);
                    e.text(reason.name());
                    self.transition.episode = Some(Episode {
                        id: e.finish(),
                        first_detecting_event: stamp.event_id.clone(),
                        initial_reason: reason,
                        current_reason: reason,
                        escalation_event: None,
                        release_event: None,
                    });
                } else if reason == cancel::Reason::MmrBreach {
                    let episode = self.transition.episode.as_mut().expect("existing episode");
                    if episode.current_reason != reason {
                        episode.current_reason = reason;
                        episode.escalation_event = Some(stamp.event_id.clone());
                    }
                }
            }
        }
        if !decision.targets.is_empty() || !decision.pending.is_empty() {
            self.transition.lifecycle = Lifecycle::AwaitingCancelEffective;
        } else if !decision.liquidation_required {
            self.transition.lifecycle = Lifecycle::RiskStable;
            self.finish_episode(stamp);
        }
        Ok(decision)
    }

    fn finish_episode(&mut self, stamp: &cancel::Stamp) {
        if let Some(mut episode) = self.transition.episode.take() {
            episode.release_event = Some(stamp.event_id.clone());
            self.transition.completed_episodes.push(episode);
        }
    }
}
