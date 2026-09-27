//! Pure liquidation preparation; no publication or source entry-point wiring.
use super::*;
use cancel::identity::{classify, Encoding, Stored};
use hypothetical_settlement::FeePolicy;
mod commit;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StepDecision {
    ContinueLiquidation,
    RiskStable,
    LiquidatedFlat,
    LiquidatedInsolvent,
}

impl StepDecision {
    fn from_valuation(value: &IncompleteValuation) -> Self {
        if value.products.is_empty() {
            if value.equity < Decimal::ZERO {
                Self::LiquidatedInsolvent
            } else {
                Self::LiquidatedFlat
            }
        } else if value.risk == MaintenanceState::Safe {
            Self::RiskStable
        } else {
            Self::ContinueLiquidation
        }
    }

    fn lifecycle(self) -> Option<Lifecycle> {
        match self {
            Self::ContinueLiquidation => None,
            Self::RiskStable => Some(Lifecycle::RiskStable),
            Self::LiquidatedFlat => Some(Lifecycle::LiquidatedFlat),
            Self::LiquidatedInsolvent => Some(Lifecycle::LiquidatedInsolvent),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Receipt {
    account_key: AccountKey,
    liquidation_id: Hash,
    canonical_payload_digest: Hash,
    trigger_event_id: String,
    step_index: i64,
    product: Product,
    position_side_before: Side,
    execution_side: Side,
    contracts: Decimal,
    base_quantity: Decimal,
    mark: Decimal,
    fee_policy: FeePolicy,
    fee_rate: Decimal,
    fee: Decimal,
    gross_realized_delta: Decimal,
    cash_delta: Decimal,
    position_before: ProductPosition,
    position_after: Option<ProductPosition>,
    valuation_before: IncompleteValuation,
    valuation_after: IncompleteValuation,
    reservation_before: reservation::Snapshot,
    reservation_after: reservation::Snapshot,
    account_version_before: u64,
    account_version_after: u64,
    commit_sequence_before: u64,
    commit_sequence: u64,
    spec_version: String,
    rule_data_version: String,
    risk_action_episode_id: Hash,
    escalation_event_id: Option<String>,
    release_event_id: Option<String>,
    active_context_id: Hash,
    post_step_decision: StepDecision,
    resulting_lifecycle: Option<Lifecycle>,
}

fn step_id(key: &AccountKey, episode: Hash, index: i64) -> Hash {
    let mut e = Encoding::new("SYNTHETIC_LIQUIDATION");
    e.account(key);
    e.hash(episode);
    e.integer(index);
    e.finish()
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Long => "LONG",
        Side::Short => "SHORT",
    }
}

impl Receipt {
    fn digest(&self) -> Result<Hash, Fault> {
        let mut e = Encoding::new("SYNTHETIC_LIQUIDATION");
        e.account(&self.account_key);
        e.hash(self.liquidation_id);
        e.text(&self.trigger_event_id);
        e.integer(self.step_index);
        e.text(product_id(self.product));
        e.text(side_name(self.position_side_before));
        e.text(side_name(self.execution_side));
        for value in [self.contracts, self.base_quantity, self.mark] {
            e.text(&value.normalize().to_string());
        }
        e.text("SyntheticLiquidation");
        e.text(&self.fee_rate.normalize().to_string());
        e.text(&self.spec_version);
        e.text(&self.rule_data_version);
        e.hash(self.risk_action_episode_id);
        e.optional_text(self.escalation_event_id.as_deref());
        e.optional_text(self.release_event_id.as_deref());
        e.integer(i64::try_from(self.account_version_before).map_err(|_| "VERSION_OVERFLOW")?);
        e.integer(i64::try_from(self.commit_sequence_before).map_err(|_| "SEQUENCE_OVERFLOW")?);
        e.hash(self.active_context_id);
        Ok(e.finish())
    }
}

// History is an immutable projection, never a second receipt store or writer.
fn next_index(key: &AccountKey, episode: Hash, history: &[Receipt]) -> Result<i64, Fault> {
    let mut indices = BTreeSet::new();
    for receipt in history {
        if receipt.account_key != *key
            || receipt.risk_action_episode_id != episode
            || receipt.step_index <= 0
            || receipt.liquidation_id != step_id(key, episode, receipt.step_index)
            || receipt.canonical_payload_digest != receipt.digest()?
            || !indices.insert(receipt.step_index)
        {
            return Err("INVALID_RISK_EPISODE");
        }
    }
    let next = i64::try_from(history.len())
        .map_err(|_| "INVALID_RISK_EPISODE")?
        .checked_add(1)
        .ok_or("INVALID_RISK_EPISODE")?;
    if !indices.into_iter().eq(1..next) {
        return Err("INVALID_RISK_EPISODE");
    }
    Ok(next)
}

fn historical<'a>(
    history: &'a [Receipt],
    prepared: &Receipt,
) -> Result<Option<&'a Receipt>, Fault> {
    let stored = history
        .iter()
        .find(|r| r.liquidation_id == prepared.liquidation_id)
        .map(|r| Stored {
            digest: r.canonical_payload_digest,
            value: r,
        });
    classify(
        stored.as_ref(),
        prepared.canonical_payload_digest,
        "LIQUIDATION_ID_CONFLICT",
    )
    .map(|value| value.copied())
}

fn prepare_step(owner: &ScenarioAccount, history: &[Receipt]) -> Result<commit::Prepared, Fault> {
    if owner.gate != Gate::Running {
        return Err("RUN_FAILED");
    }
    if matches!(
        owner.transition.lifecycle,
        Lifecycle::LiquidatedFlat | Lifecycle::LiquidatedInsolvent
    ) {
        return Err("RUN_TERMINAL");
    }
    let episode = owner
        .transition
        .episode
        .as_ref()
        .ok_or("INVALID_RISK_EPISODE")?;
    if episode.current_reason != cancel::Reason::MmrBreach {
        return Err("INVALID_RISK_EPISODE");
    }
    let step_index = next_index(&owner.key, episode.id, history)?;
    let before = owner.projection()?;
    let (scenario, marks) = owner.btc_context()?;
    owner.validate_context(before.effective_at)?;
    let reservation_before = owner.reservation()?;
    let risk = owner.classify_risk(&reservation_before)?;
    if !risk.liquidation_required {
        return Err("INVALID_RISK_EPISODE");
    }
    let valuation_before = scenario.evaluate(&before, marks)?;
    for (product, position) in owner.positions.btc()? {
        let (spec, _) = scenario.resolve(*product, before.effective_at)?;
        hypothetical_settlement::validate_existing(
            position,
            (scenario, spec).into(),
            FeePolicy::SyntheticLiquidation,
        )?;
    }
    let selected = valuation_before
        .products
        .iter()
        .min_by(|a, b| {
            b.maintenance_margin
                .cmp(&a.maintenance_margin)
                .then_with(|| product_id(a.product).cmp(product_id(b.product)))
        })
        .ok_or("INVALID_RISK_EPISODE")?;
    let product = selected.product;
    let position_before = owner.positions.btc()?[&product].clone();
    let (spec, tiers) = scenario.resolve(product, before.effective_at)?;
    let target = if selected.tier == 1 {
        Decimal::ZERO
    } else {
        tiers.tiers[selected.tier - 2].maximum
    };
    let raw = add(position_before.contracts, -target)?;
    let remainder = raw.checked_rem(spec.lot).ok_or("DECIMAL_OVERFLOW")?;
    let contracts = if remainder == Decimal::ZERO {
        raw
    } else {
        add(raw, add(spec.lot, -remainder)?)?
    };
    if contracts <= Decimal::ZERO || contracts > position_before.contracts {
        return Err("INVALID_HYPOTHETICAL_EXECUTION");
    }
    let mark = marks
        .iter()
        .find(|m| {
            m.product == product
                && m.valid_from <= before.effective_at
                && before.effective_at < m.valid_to
        })
        .ok_or("MARK_COVERAGE_MISSING")?
        .price;
    let execution_side = match position_before.side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    };
    let settlement = hypothetical_settlement::calculate(
        Some(&position_before),
        execution_side,
        contracts,
        mark,
        (scenario, spec),
        FeePolicy::SyntheticLiquidation,
        None,
    )?;
    let mut draft = owner.clone();
    draft.cash = add(owner.cash, settlement.cash_delta)?;
    draft.fees = add(owner.fees, settlement.fee)?;
    draft.gross_realized = add(owner.gross_realized, settlement.gross_realized_delta)?;
    draft.state_version = owner
        .state_version
        .checked_add(1)
        .ok_or("VERSION_OVERFLOW")?;
    draft.commit_sequence = owner
        .commit_sequence
        .checked_add(1)
        .ok_or("SEQUENCE_OVERFLOW")?;
    match &settlement.position {
        Some(position) => {
            draft.positions.btc_mut()?.insert(product, position.clone());
        }
        None => {
            draft.positions.btc_mut()?.remove(&product);
        }
    }
    let valuation_after = scenario.evaluate(&draft.projection()?, marks)?;
    let reservation_after = draft.reservation()?;
    let post_step_decision = StepDecision::from_valuation(&valuation_after);
    let mut receipt = Receipt {
        account_key: owner.key.clone(),
        liquidation_id: step_id(&owner.key, episode.id, step_index),
        canonical_payload_digest: [0; 32],
        trigger_event_id: episode.first_detecting_event.clone(),
        step_index,
        product,
        position_side_before: position_before.side,
        execution_side,
        contracts,
        base_quantity: mul(mul(contracts, spec.contract_value)?, spec.multiplier)?,
        mark,
        fee_policy: FeePolicy::SyntheticLiquidation,
        fee_rate: FeePolicy::SyntheticLiquidation.rate(),
        fee: settlement.fee,
        gross_realized_delta: settlement.gross_realized_delta,
        cash_delta: settlement.cash_delta,
        position_before,
        position_after: settlement.position,
        valuation_before,
        valuation_after,
        reservation_before,
        reservation_after,
        account_version_before: owner.state_version,
        account_version_after: draft.state_version,
        commit_sequence_before: owner.commit_sequence,
        commit_sequence: draft.commit_sequence,
        spec_version: spec.version.clone(),
        rule_data_version: tiers.version.clone(),
        risk_action_episode_id: episode.id,
        escalation_event_id: episode.escalation_event.clone(),
        release_event_id: episode.release_event.clone(),
        active_context_id: owner.valuation_context_id,
        post_step_decision,
        resulting_lifecycle: post_step_decision.lifecycle(),
    };
    receipt.canonical_payload_digest = receipt.digest()?;
    Ok(commit::Prepared {
        receipt,
        before: commit::Preconditions::capture(owner),
        cash: draft.cash,
        fees: draft.fees,
        gross_realized: draft.gross_realized,
        positions: draft.positions,
    })
}

#[cfg(test)]
fn prepare(owner: &ScenarioAccount, history: &[Receipt]) -> Result<Receipt, Fault> {
    prepare_step(owner, history).map(|p| p.receipt)
}

#[cfg(test)]
mod tests;
