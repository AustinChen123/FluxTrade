//! Private execution identity/preparation and the sole BTC/ETH commit boundary.
use super::*;
mod golden_cancel;
#[cfg(test)]
mod inspection_tests;
pub(super) mod wire;

pub(super) mod commit;
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
pub(super) struct ExecutionCandidate {
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

impl ExecutionCandidate {
    pub(super) fn wire_product(&self) -> &ProfileProduct {
        &self.template.key.product
    }
    pub(super) fn account_key(&self) -> &AccountKey {
        &self.template.key.account
    }
    pub(super) fn preflight_identity(&self, owner: &ScenarioAccount) -> Result<(), Fault> {
        let (_, _, key, digest) = self.group_identity();
        if owner
            .execution_receipts
            .get(&key)
            .is_some_and(|receipt| receipt.financial_payload_digest != digest)
        {
            return Err("EXECUTION_ID_CONFLICT");
        }
        Ok(())
    }
    pub(super) fn group_identity(&self) -> (&str, i64, Hash, Hash) {
        (
            &self.event_id,
            self.template.matching_effective_at,
            self.template.key.canonical_id(),
            self.template.digest(),
        )
    }
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
    order_after: SeedOrder,
    order_created_at: i64,
    execution_effective_at: i64,
    contract_value: Decimal,
    risk_decision_after: Option<risk_transition::Decision>,
    lifecycle_after: risk_transition::Lifecycle,
    episode_after: Option<risk_transition::Episode>,
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

impl CommittedExecution {
    pub(super) fn source_event(&self) -> &str {
        &self.event_id
    }
    pub(super) fn policy_client(&self, golden: bool) -> Result<String, Fault> {
        if golden {
            if self.order_after.client_id != "C" {
                return Err("NATIVE_INVARIANT");
            }
            Ok("0000015000".into())
        } else {
            Ok(self.order_after.client_id.clone())
        }
    }
    pub(super) fn encode_delivery(
        &self,
        e: &mut risk_transition::cancel::identity::Encoding,
        policy_client: &str,
    ) -> Result<(), Fault> {
        let o = &self.order_after;
        let state = match o.status.as_str() {
            "PARTIALLY_FILLED" => "partially_filled",
            "FILLED" => "filled",
            _ => return Err("NATIVE_INVARIANT"),
        };
        for s in [
            o.order_id.as_str(),
            &o.client_id,
            policy_client,
            o.product.canonical_id(),
            state,
            delivery::side(o.side),
        ] {
            e.text(s);
        }
        for v in [
            o.price,
            self.price,
            o.original,
            o.filled,
            self.contract_value,
        ] {
            e.text(&v.normalize().to_string());
        }
        e.integer(self.execution_effective_at);
        e.integer(i64::try_from(self.state_version_after).map_err(|_| "NATIVE_INVARIANT")?);
        e.text(&self.spec_version);
        e.text(&self.rule_data_version);
        Ok(())
    }
}

impl ScenarioAccount {
    pub(super) fn encode_inspection_executions(
        &self,
        e: &mut risk_transition::cancel::identity::Encoding,
    ) -> Result<(), Fault> {
        use inspection::{decimals, number, order_facts};
        number(e, self.execution_receipts.len())?;
        for (key, r) in &self.execution_receipts {
            e.hash(*key);
            e.account(&r.account_key);
            e.hash(r.execution_id);
            e.hash(r.financial_payload_digest);
            e.text(&r.event_id);
            for n in [
                r.commit_sequence,
                r.state_version_before,
                r.state_version_after,
                r.order_version_before,
                r.order_version_after,
            ] {
                number(e, n)?;
            }
            e.text(r.product.canonical_id());
            e.text(&r.order_id);
            decimals(e, &[r.quantity, r.price]);
            e.text(&r.fee_asset);
            decimals(e, &[r.fee_amount, r.realized_pnl_delta]);
            number(e, r.cash_deltas.len())?;
            for (asset, value) in &r.cash_deltas {
                e.text(asset);
                decimals(e, &[*value]);
            }
            order_facts(e, &r.order_after)?;
            e.integer(r.order_created_at);
            e.integer(r.execution_effective_at);
            decimals(e, &[r.contract_value]);
            e.text(&r.spec_version);
            e.text(&r.rule_data_version);
            e.text(match r.lifecycle_after {
                risk_transition::Lifecycle::RiskStable => "RISK_STABLE",
                risk_transition::Lifecycle::AwaitingCancelEffective => "AWAITING_CANCEL_EFFECTIVE",
                risk_transition::Lifecycle::LiquidatedFlat => "LIQUIDATED_FLAT",
                risk_transition::Lifecycle::LiquidatedInsolvent => "LIQUIDATED_INSOLVENT",
            });
            number(e, r.pending_action_ids.len())?;
            for id in &r.pending_action_ids {
                e.text(id);
            }
            let client =
                r.policy_client(matches!(self.profile, ProfileContext::GoldenCancel(_)))?;
            e.text("EXECUTION_FACT");
            r.encode_delivery(e, &client)?;
        }
        Ok(())
    }
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
    fn execution_source(&self, candidate: &ExecutionCandidate) -> (Hash, source::Kind) {
        if matches!(self.profile, ProfileContext::EventLimit(_)) {
            (
                hash_fields(&[
                    Some("golden-event-c-source".into()),
                    Some(candidate.event_id.clone()),
                    Some(candidate.template.matching_effective_at.to_string()),
                    Some(format!("{:02x?}", self.valuation_context_id)),
                ]),
                source::Kind::EventC,
            )
        } else {
            (candidate.template.digest(), source::Kind::Execution)
        }
    }
    // The writer consumes this result and owns fatal gate publication. This seam
    // itself is read-only, including all business rejection and fault exits.
    fn prepare_execution(&self, candidate: &ExecutionCandidate) -> Result<Preparation<'_>, Fault> {
        let stamp = source::stamp(
            &candidate.event_id,
            candidate.template.matching_effective_at,
            30,
        );
        self.prepare_execution_stamped(candidate, &stamp)
    }

    fn prepare_execution_stamped(
        &self,
        candidate: &ExecutionCandidate,
        stamp: &risk_transition::cancel::Stamp,
    ) -> Result<Preparation<'_>, Fault> {
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
        let (source_digest, source_kind) = self.execution_source(candidate);
        if let Err(fault) = self.source_boundary(stamp, source_digest, source_kind) {
            return if fault == "RUN_TERMINAL" {
                Ok(Preparation::Rejected(fault))
            } else {
                Err(fault)
            };
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
        if matches!(
            self.profile,
            ProfileContext::GoldenCancel(_) | ProfileContext::P1O03
        ) {
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
        if let Err(reason) = remainder_eligibility(&order.facts, self.positions.btc()?.get(product))
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
