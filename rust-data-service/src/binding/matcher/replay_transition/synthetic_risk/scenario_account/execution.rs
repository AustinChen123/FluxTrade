//! Private execution identity/preparation and the sole BTC/ETH commit boundary.
use super::*;
mod golden_cancel;

mod commit;
mod event_c;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LiquidityRole {
    SyntheticTaker,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExternalExecutionKey {
    account: AccountKey,
    namespace: String,
    product: ProfileProduct,
    external_id: String,
}

impl ExternalExecutionKey {
    fn fields(&self) -> Vec<Option<String>> {
        vec![
            Some(self.account.venue.clone()),
            Some(self.account.environment.clone()),
            Some(self.account.account.clone()),
            self.account.subaccount.clone(),
            Some(self.namespace.clone()),
            Some(self.product.canonical_id().into()),
            Some(self.external_id.clone()),
        ]
    }

    fn canonical_id(&self) -> Hash {
        let mut fields = vec![Some("scenario-execution-key-v1".into())];
        fields.extend(self.fields());
        hash_fields(&fields)
    }
}

// A template carries financial facts, never preselected account/order versions.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ExecutionTemplate {
    key: ExternalExecutionKey,
    order_id: String,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    liquidity: LiquidityRole,
    fee_asset: Option<String>,
    fee_amount: Option<Decimal>,
    matching_effective_at: i64,
}

impl ExecutionTemplate {
    fn digest(&self) -> Hash {
        let mut fields = vec![Some("scenario-execution-financial-v1".into())];
        fields.extend(self.key.fields());
        fields.extend([
            Some(self.order_id.clone()),
            Some(
                match self.side {
                    Side::Long => "LONG",
                    Side::Short => "SHORT",
                }
                .into(),
            ),
            Some(self.price.normalize().to_string()),
            Some(self.quantity.normalize().to_string()),
            Some(
                match self.liquidity {
                    LiquidityRole::SyntheticTaker => "SYNTHETIC_TAKER",
                    LiquidityRole::Unsupported => "UNSUPPORTED",
                }
                .into(),
            ),
            self.fee_asset.clone(),
            self.fee_amount.map(|value| value.normalize().to_string()),
            Some(self.matching_effective_at.to_string()),
        ]);
        hash_fields(&fields)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExecutionCandidate {
    template: ExecutionTemplate,
    candidate_id: String,
    event_id: String,
    source_id: String,
    visible_at: i64,
    expected_account_version: u64,
    expected_order_version: u64,
    spec_version: String,
    rule_data_version: String,
}

// Closed BTC evidence for this checkpoint; neutral execution remains unsupported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum FinancialSnapshot {
    GoldenCancel(Decimal),
    BtcEth(reservation::Snapshot),
    EventLimit(event_limit::Snapshot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CommittedExecution {
    account_key: AccountKey,
    execution_id: Hash,
    financial_payload_digest: Hash,
    event_id: String,
    commit_sequence: u64,
    state_version_before: u64,
    state_version_after: u64,
    order_version_before: u64,
    order_version_after: u64,
    product: ProfileProduct,
    order_id: String,
    quantity: Decimal,
    price: Decimal,
    fee_asset: String,
    fee_amount: Decimal,
    realized_pnl_delta: Decimal,
    cash_deltas: Vec<(String, Decimal)>,
    position_before: Option<ProductPosition>,
    position_after: Option<ProductPosition>,
    reservation_before: FinancialSnapshot,
    reservation_after: FinancialSnapshot,
    spec_version: String,
    rule_data_version: String,
    risk_state_after: ProfileRisk,
    pending_action_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Preparation<'a> {
    Duplicate(&'a CommittedExecution),
    Rejected(Fault),
    Eligible { execution_id: Hash, digest: Hash },
}

// The whole order remainder, not candidate quantity, owns execution eligibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RemainderRole {
    Reducing,
    Increasing,
}

pub(super) fn remainder_eligibility(
    order: &SeedOrder,
    position: Option<&ProductPosition>,
) -> Result<RemainderRole, Fault> {
    let reducing = position.is_some_and(|p| p.side != order.side && order.remaining <= p.contracts);
    if order.reduce_only && !reducing {
        return Err("REDUCE_ONLY_NOT_REDUCING");
    }
    if position.is_some_and(|p| p.side != order.side && order.remaining > p.contracts) {
        return Err("EXECUTION_INELIGIBLE_REMAINDER_EXCEEDS_POSITION");
    }
    Ok(if reducing {
        RemainderRole::Reducing
    } else {
        RemainderRole::Increasing
    })
}

impl ScenarioAccount {
    // The writer consumes this result and owns fatal gate publication. This seam
    // itself is read-only, including all business rejection and fault exits.
    fn prepare_execution(&self, candidate: &ExecutionCandidate) -> Result<Preparation<'_>, Fault> {
        let template = &candidate.template;
        let execution_id = template.key.canonical_id();
        let digest = template.digest();
        if template.key.namespace == "seed" || self.seed_executions.contains(&execution_id) {
            return Err("SEED_IDENTITY_CONFLICT");
        }
        if let Some(receipt) = self.execution_receipts.get(&execution_id) {
            return if receipt.financial_payload_digest == digest {
                Ok(Preparation::Duplicate(receipt))
            } else {
                Err("EXECUTION_ID_CONFLICT")
            };
        }
        if self.gate != Gate::Running {
            return Err("RUN_FAILED");
        }
        self.validate_context(template.matching_effective_at)?;
        let order = self.target_order(&template.order_id)?;
        if candidate.expected_account_version != self.state_version
            || candidate.expected_order_version != order.version
        {
            return Err("STALE_VERSION");
        }
        if matches!(self.profile, ProfileContext::EventLimit(_)) {
            return self.prepare_event_c_candidate(candidate, order, execution_id, digest);
        }
        if matches!(self.profile, ProfileContext::GoldenCancel(_)) {
            return self.prepare_golden_cancel_candidate(candidate, order, execution_id, digest);
        }
        let product = order
            .facts
            .product
            .btc()
            .map_err(|_| "UNSUPPORTED_EXECUTION")?;
        // Terminal orders cannot become role-based business rejections after
        // another accepted execution has flattened or changed the position.
        if !order.facts.projects_remainder("UNSUPPORTED_EXECUTION")? {
            return Err("UNSUPPORTED_EXECUTION");
        }
        if let Err(reason) =
            remainder_eligibility(&order.facts, self.positions.btc()?.get(&product))
        {
            return Ok(Preparation::Rejected(reason));
        }
        let (scenario, _) = self.btc_context().map_err(|_| "UNSUPPORTED_EXECUTION")?;
        let (spec, tier) = scenario.resolve(product, template.matching_effective_at)?;
        let facts = &order.facts;
        if template.key.account != self.key
            || !identity(&template.key.namespace)
            || !identity(&template.key.external_id)
            || !identity(&candidate.candidate_id)
            || !identity(&candidate.event_id)
            || template.key.product != facts.product
            || template.side != facts.side
            || candidate.spec_version != spec.version
            || candidate.rule_data_version != tier.version
            || template.liquidity != LiquidityRole::SyntheticTaker
            || template.fee_asset.is_some()
            || template.fee_amount.is_some()
            || template.quantity <= Decimal::ZERO
            || template.quantity > facts.remaining
            || !aligned(template.quantity, spec.lot)
            || template.price <= Decimal::ZERO
            || !aligned(template.price, spec.tick)
            || match facts.side {
                Side::Long => template.price > facts.price,
                Side::Short => template.price < facts.price,
            }
        {
            return Err("UNSUPPORTED_EXECUTION");
        }
        Ok(Preparation::Eligible {
            execution_id,
            digest,
        })
    }

    fn fail_execution(&mut self, fault: Fault) -> Fault {
        if self.gate == Gate::Running {
            self.gate = Gate::Failed(fault);
        }
        fault
    }
}

#[cfg(test)]
mod tests;
