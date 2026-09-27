//! Sole-owner publication of sealed financial preparation; no source wiring.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Preconditions {
    key: AccountKey,
    version: u64,
    sequence: u64,
    cash: Decimal,
    fees: Decimal,
    gross_realized: Decimal,
    positions: PositionState,
    orders: BTreeMap<String, RestingOrder>,
    profile: ProfileContext,
    context: Hash,
    context_at: Option<i64>,
    episode: Option<Episode>,
    lifecycle: Lifecycle,
}

impl Preconditions {
    pub(super) fn capture(owner: &ScenarioAccount) -> Self {
        Self {
            key: owner.key.clone(),
            version: owner.state_version,
            sequence: owner.commit_sequence,
            cash: owner.cash,
            fees: owner.fees,
            gross_realized: owner.gross_realized,
            positions: owner.positions.clone(),
            orders: owner.orders.clone(),
            profile: owner.profile.clone(),
            context: owner.valuation_context_id,
            context_at: owner.transition.context_at,
            episode: owner.transition.episode.clone(),
            lifecycle: owner.transition.lifecycle,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Prepared {
    pub(super) receipt: Receipt,
    pub(super) before: Preconditions,
    pub(super) cash: Decimal,
    pub(super) fees: Decimal,
    pub(super) gross_realized: Decimal,
    pub(super) positions: PositionState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Prepared(i64),
    BeforeSwap(i64),
}

fn history(owner: &ScenarioAccount) -> Result<Vec<Receipt>, Fault> {
    let episode = owner
        .transition
        .episode
        .as_ref()
        .ok_or("INVALID_RISK_EPISODE")?;
    let mut current = Vec::new();
    for receipt in &owner.transition.liquidations {
        if receipt.risk_action_episode_id == episode.id {
            current.push(receipt.clone());
        } else if !owner
            .transition
            .completed_episodes
            .iter()
            .any(|e| e.id == receipt.risk_action_episode_id)
        {
            return Err("INVALID_RISK_EPISODE");
        }
    }
    next_index(&owner.key, episode.id, &current)?;
    Ok(current)
}

fn running(owner: &ScenarioAccount) -> Result<(), Fault> {
    if owner.is_terminal() {
        return Err("RUN_TERMINAL");
    }
    if owner.gate != Gate::Running {
        return Err("RUN_FAILED");
    }
    Ok(())
}

impl ScenarioAccount {
    fn commit_liquidation(
        &mut self,
        prepared: &Prepared,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<Receipt, Fault> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(old) = historical(&self.transition.liquidations, &prepared.receipt)? {
                return Ok(old.clone());
            }
            running(self)?;
            let r = &prepared.receipt;
            let prefix = history(self)?;
            if prepared.before != Preconditions::capture(self)
                || next_index(&self.key, r.risk_action_episode_id, &prefix)? != r.step_index
                || r.canonical_payload_digest != r.digest()?
            {
                return Err("INVALID_RISK_EPISODE");
            }
            self.validate_context(r.valuation_before.effective_at)?;
            let before = self.reservation()?;
            if before != r.reservation_before || !self.classify_risk(&before)?.liquidation_required
            {
                return Err("INVALID_RISK_EPISODE");
            }
            hook(Stage::Prepared(r.step_index))?;
            let mut draft = self.clone();
            draft.cash = prepared.cash;
            draft.fees = prepared.fees;
            draft.gross_realized = prepared.gross_realized;
            draft.positions = prepared.positions.clone();
            draft.state_version = r.account_version_after;
            draft.commit_sequence = r.commit_sequence;
            let (scenario, marks) = draft.btc_context()?;
            let after = scenario.evaluate(&draft.projection()?, marks)?;
            let old_tier = r
                .valuation_before
                .products
                .iter()
                .find(|p| p.product == r.product)
                .ok_or("INVALID_RISK_EPISODE")?
                .tier;
            let new_tier = after
                .products
                .iter()
                .find(|p| p.product == r.product)
                .map_or(0, |p| p.tier);
            if new_tier.checked_add(1) != Some(old_tier)
                || after != r.valuation_after
                || StepDecision::from_valuation(&after) != r.post_step_decision
                || r.post_step_decision.lifecycle() != r.resulting_lifecycle
                || draft.reservation()? != r.reservation_after
                || add(self.cash, r.cash_delta)? != draft.cash
                || add(self.fees, r.fee)? != draft.fees
                || add(self.gross_realized, r.gross_realized_delta)? != draft.gross_realized
                || add(r.gross_realized_delta, -r.fee)? != r.cash_delta
                || self.state_version.checked_add(1) != Some(draft.state_version)
                || self.commit_sequence.checked_add(1) != Some(draft.commit_sequence)
            {
                return Err("INVALID_RISK_EPISODE");
            }
            if let Some(lifecycle) = r.resulting_lifecycle {
                draft.transition.lifecycle = lifecycle;
                let episode = draft
                    .transition
                    .episode
                    .take()
                    .ok_or("INVALID_RISK_EPISODE")?;
                draft.transition.completed_episodes.push(episode);
            }
            draft.transition.liquidations.push(r.clone());
            hook(Stage::BeforeSwap(r.step_index))?;
            *self = draft;
            Ok(r.clone())
        }))
        .map_err(|_| "LIQUIDATION_PANIC")
        .and_then(|r| r);
        result.map_err(|fault| self.source_failure(fault))
    }

    fn liquidation_loop(
        &mut self,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<Vec<Receipt>, Fault> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            running(self)?;
            let (scenario, marks) = self.btc_context()?;
            let bound = scenario
                .evaluate(&self.projection()?, marks)?
                .products
                .iter()
                .try_fold(0usize, |n, p| {
                    n.checked_add(p.tier).ok_or("INVALID_RISK_EPISODE")
                })?;
            let mut receipts = Vec::new();
            for _ in 0..bound {
                let prepared = prepare_step(self, &history(self)?)?;
                let receipt = self.commit_liquidation(&prepared, &mut hook)?;
                let finished = receipt.post_step_decision != StepDecision::ContinueLiquidation;
                receipts.push(receipt);
                if finished {
                    return Ok(receipts);
                }
            }
            Err("INVALID_RISK_EPISODE")
        }))
        .map_err(|_| "LIQUIDATION_PANIC")
        .and_then(|r| r);
        result.map_err(|fault| self.source_failure(fault))
    }

    #[cfg(test)]
    fn liquidate_for_test(
        &mut self,
        hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<Vec<Receipt>, Fault> {
        self.liquidation_loop(hook)
    }
}

#[cfg(test)]
mod tests;
