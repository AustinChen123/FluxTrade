//! Preloaded whole-account context activation on the sole owner's draft.
use super::*;
use risk_transition::cancel::{identity::Encoding, Stamp};
#[cfg(test)]
pub(super) mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Rows {
    Marks(Vec<(Product, i64, i64, Decimal)>),
    Specs(Vec<(Product, String, String, i64)>),
    Tiers(Vec<(Product, String, String, i64)>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Input {
    pub account_key: AccountKey,
    pub stamp: Stamp,
    pub expected_before: Hash,
    pub expected_after: Hash,
    pub rows: Rows,
}

impl Input {
    pub(super) fn digest(&self) -> Hash {
        let mut e = Encoding::new(match self.rows {
            Rows::Marks(_) => "MARK_SET",
            Rows::Specs(_) => "SPEC_ACTIVATION",
            Rows::Tiers(_) => "TIER_ACTIVATION",
        });
        e.text(&self.stamp.event_id);
        e.account(&self.account_key);
        e.hash(self.expected_before);
        e.hash(self.expected_after);
        match &self.rows {
            Rows::Marks(rows) => {
                e.integer(rows.len() as i64);
                for (product, from, to, mark) in rows {
                    e.text(product_id(product));
                    e.integer(*from);
                    e.integer(*to);
                    e.text(&mark.normalize().to_string());
                }
            }
            Rows::Specs(rows) | Rows::Tiers(rows) => {
                e.integer(rows.len() as i64);
                for (product, from, to, boundary) in rows {
                    e.text(product_id(product));
                    e.text(from);
                    e.text(to);
                    e.integer(*boundary);
                }
            }
        }
        e.integer(self.stamp.effective_at);
        e.stamp_tail(&self.stamp);
        e.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Receipt {
    pub input: Input,
    pub digest: Hash,
    pub before_version: u64,
    pub after_version: u64,
    pub before: reservation::Snapshot,
    pub after: reservation::Snapshot,
    pub decision: risk_transition::Decision,
    pub migration_effects: Vec<Hash>,
    pub lifecycle_before: risk_transition::Lifecycle,
    pub lifecycle_after: risk_transition::Lifecycle,
    pub stamp_before: Option<Stamp>,
    pub episode_after: Option<risk_transition::Episode>,
    pub rows_before: Vec<(Spec, TierVersion, Mark)>,
    pub rows_after: Vec<(Spec, TierVersion, Mark)>,
    pub pending_action_ids: Vec<Hash>,
    pub migration_cause: Option<risk_transition::cancel::Reason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    BeforeSwap,
    AfterSwap,
    RiskActionDrafted(usize),
    RiskBeforeSwap,
    LiquidationPrepared(i64),
    LiquidationBeforeSwap(i64),
}

impl ScenarioAccount {
    pub(super) fn activate_context(&mut self, input: &Input) -> Result<Receipt, Fault> {
        self.activate_context_checked(input, |_| Ok(()))
    }

    pub(super) fn activate_context_checked(
        &mut self,
        input: &Input,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<Receipt, Fault> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let prepared = self.prepare_context(input)?;
            if prepared.1.is_some() {
                hook(Stage::BeforeSwap)?;
            }
            Ok(prepared)
        }))
        .map_err(|_| "CONTEXT_PANIC")
        .and_then(|r: Result<_, Fault>| r);
        match result {
            Ok((receipt, Some(draft))) => {
                *self = draft;
                let risk = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook(Stage::AfterSwap)?;
                    self.immediate_risk_checked(&input.stamp, |stage| match stage {
                        risk_transition::cancel::Stage::ActionDrafted(index) => {
                            hook(Stage::RiskActionDrafted(index))
                        }
                        risk_transition::cancel::Stage::BeforeSwap(_) => {
                            hook(Stage::RiskBeforeSwap)
                        }
                        risk_transition::cancel::Stage::Prepared => Ok(()),
                        risk_transition::Stage::LiquidationPrepared(step) => {
                            hook(Stage::LiquidationPrepared(step))
                        }
                        risk_transition::Stage::LiquidationBeforeSwap(step) => {
                            hook(Stage::LiquidationBeforeSwap(step))
                        }
                    })
                }))
                .map_err(|_| "CONTEXT_PANIC")
                .and_then(|r| r);
                if let Err(f) = risk {
                    if self.gate == Gate::Running {
                        self.gate = Gate::Failed(f);
                    }
                    return Err(f);
                }
                Ok(receipt)
            }
            Ok((receipt, None)) => Ok(receipt),
            Err(f) => Err(self.source_failure(f)),
        }
    }

    pub(super) fn prepare_context(&self, input: &Input) -> Result<(Receipt, Option<Self>), Fault> {
        let digest = input.digest();
        if let Some(old) = self.transition.contexts.get(&input.stamp.event_id) {
            return if old.digest == digest {
                Ok((old.clone(), None))
            } else {
                Err("EVENT_ID_CONFLICT")
            };
        }
        self.source_boundary(&input.stamp, digest, source::Kind::Context)?;
        risk_transition::cancel::identity::stamp_shape(
            &input.stamp,
            self.transition.reverse_group,
            false,
        )?;
        if input.stamp.scenario_ordinal
            != if matches!(input.rows, Rows::Marks(_)) {
                20
            } else {
                10
            }
        {
            return Err("INVALID_SCENARIO_GROUP");
        }
        if input.account_key != self.key
            || input.expected_before != self.valuation_context_id
            || !identity(&input.stamp.event_id)
            || input.stamp.effective_at < 0
        {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        let at = input.stamp.effective_at;
        let old_at = self.transition.context_at.unwrap_or(self.seed_effective_at);
        if at <= old_at {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        let (scenario, marks) = self.btc_context()?;
        let after_id =
            owner_context_id(scenario, marks, at, self.seed_effective_at, &self.config_id)
                .map_err(|fault| match fault {
                    "MARK_COVERAGE_MISSING" => "UNSUPPORTED_CONTEXT_TRANSITION",
                    other => other,
                })?;
        if input.expected_after != after_id {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        if scenario.configured.is_some()
            && matches!(input.rows, Rows::Marks(_))
            && input.expected_before == input.expected_after
        {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        let mut migrations = Vec::new();
        let mut configured_changes = Vec::new();
        let mut rows_before = Vec::new();
        let mut rows_after = Vec::new();
        for (index, product) in scenario.products().into_iter().enumerate() {
            let (old_spec, old_tier) = scenario.resolve(&product, old_at)?;
            let (spec, tier) = scenario.resolve(&product, at)?;
            if let Some(position) = self.positions.btc()?.get(&product) {
                let (settlement_context, fee_policy) = if scenario.configured.is_some() {
                    (
                        hypothetical_settlement::Context::Configured(
                            scenario, &product, spec, tier, at,
                        ),
                        hypothetical_settlement::configured_fee_policy(scenario, &product)?,
                    )
                } else {
                    (
                        (scenario, spec).into(),
                        hypothetical_settlement::FeePolicy::BtcEthTradingTaker,
                    )
                };
                hypothetical_settlement::validate_existing(
                    position,
                    settlement_context,
                    fee_policy,
                )?;
            }
            let old_mark = marks
                .iter()
                .find(|m| m.product == product && m.valid_from <= old_at && old_at < m.valid_to)
                .ok_or("MARK_COVERAGE_MISSING")?;
            let mark = marks
                .iter()
                .find(|m| m.product == product && m.valid_from <= at && at < m.valid_to)
                .ok_or("MARK_COVERAGE_MISSING")?;
            rows_before.push((old_spec.clone(), old_tier.clone(), old_mark.clone()));
            rows_after.push((spec.clone(), tier.clone(), mark.clone()));
            match &input.rows {
                Rows::Marks(rows) => {
                    let expected_len = if scenario.configured.is_some() {
                        scenario.products().len()
                    } else {
                        2
                    };
                    if rows.len() != expected_len
                        || rows.get(index)
                            != Some(&(product, mark.valid_from, mark.valid_to, mark.price))
                        || old_spec != spec
                        || old_tier != tier
                    {
                        return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                    }
                }
                Rows::Specs(rows) | Rows::Tiers(rows) => {
                    let configured = scenario.configured.is_some();
                    if (!configured && rows.len() != 2) || old_mark != mark {
                        return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                    }
                    let expected = if matches!(input.rows, Rows::Specs(_)) {
                        if old_tier != tier {
                            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                        }
                        if old_spec == spec {
                            if configured {
                                continue;
                            }
                            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                        }
                        if (
                            old_spec.contract_value,
                            old_spec.multiplier,
                            old_spec.lot,
                            old_spec.minimum,
                        ) != (spec.contract_value, spec.multiplier, spec.lot, spec.minimum)
                        {
                            return Err("UNSUPPORTED_SPEC_MIGRATION");
                        }
                        for order in self.orders.values() {
                            if matches!(&order.facts.product, ProfileProduct::BtcEth(p) if p == &product)
                                && order
                                    .facts
                                    .projects_remainder("INVALID_RESERVATION_ORDER")?
                                && !aligned(order.facts.price, spec.tick)
                            {
                                migrations.push(order.facts.order_id.clone());
                            }
                        }
                        (
                            product,
                            old_spec.version.clone(),
                            spec.version.clone(),
                            spec.interval.from,
                        )
                    } else {
                        if old_spec != spec {
                            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                        }
                        if old_tier == tier {
                            if configured {
                                continue;
                            }
                            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                        }
                        (
                            product,
                            old_tier.version.clone(),
                            tier.version.clone(),
                            tier.interval.from,
                        )
                    };
                    if expected.3 != at || (!configured && rows.get(index) != Some(&expected)) {
                        return Err("UNSUPPORTED_CONTEXT_TRANSITION");
                    }
                    if configured {
                        configured_changes.push(expected);
                    }
                }
            }
        }
        if scenario.configured.is_some()
            && match &input.rows {
                Rows::Specs(rows) | Rows::Tiers(rows) => {
                    configured_changes.is_empty() || rows != &configured_changes
                }
                Rows::Marks(_) => false,
            }
        {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        let before = self.reservation()?;
        let mut draft = self.clone();
        draft.state_version = self
            .state_version
            .checked_add(1)
            .ok_or("VERSION_OVERFLOW")?;
        let mut migration_effects = Vec::new();
        for id in migrations {
            migration_effects.push(draft.migrate_order(&id, &input.stamp)?);
        }
        draft.transition.context_at = Some(at);
        draft.valuation_context_id = after_id;
        let after = draft.reservation()?;
        let decision = draft.prepare_risk(&input.stamp, &after)?;
        let receipt = Receipt {
            input: input.clone(),
            digest,
            before_version: self.state_version,
            after_version: draft.state_version,
            before,
            after,
            decision,
            migration_effects,
            lifecycle_before: self.transition.lifecycle,
            lifecycle_after: draft.transition.lifecycle,
            stamp_before: self.transition.accepted_stamp.clone(),
            episode_after: draft.transition.episode.clone(),
            rows_before,
            rows_after,
            pending_action_ids: Vec::new(),
            migration_cause: if matches!(input.rows, Rows::Specs(_)) {
                Some(risk_transition::cancel::Reason::SpecMigration)
            } else {
                None
            },
        };
        draft
            .transition
            .contexts
            .insert(input.stamp.event_id.clone(), receipt.clone());
        draft.publish_source(&input.stamp, digest, source::Kind::Context, true);
        Ok((receipt, Some(draft)))
    }
}
