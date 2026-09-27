//! Only the two frozen Event C templates, materialized one at a time.
use super::commit::{Reply, Stage};
use super::*;

fn frozen_templates(owner: &ScenarioAccount) -> [ExecutionTemplate; 2] {
    [("O_A", "X_A"), ("O_B", "X_B")].map(|(order, external)| ExecutionTemplate {
        key: ExternalExecutionKey {
            account: owner.key.clone(),
            namespace: "golden-c09-v1".into(),
            product: ProfileProduct::Pa,
            external_id: external.into(),
        },
        order_id: order.into(),
        side: Side::Long,
        price: Decimal::TEN,
        quantity: Decimal::ONE,
        liquidity: LiquidityRole::SyntheticTaker,
        fee_asset: None,
        fee_amount: None,
        matching_effective_at: owner.seed_effective_at,
    })
}

impl ScenarioAccount {
    fn event_c_candidate(
        &self,
        template: &ExecutionTemplate,
        index: usize,
    ) -> Result<ExecutionCandidate, Fault> {
        let order = self.target_order(&template.order_id)?;
        Ok(ExecutionCandidate {
            template: template.clone(),
            candidate_id: format!("E{}", index + 1),
            event_id: "C".into(),
            source_id: "golden-c09".into(),
            visible_at: template.matching_effective_at,
            expected_account_version: self.state_version,
            expected_order_version: order.version,
            spec_version: "event-limit-spec-v1".into(),
            rule_data_version: "event-limit-data-v1".into(),
        })
    }

    fn run_event_c(&mut self, templates: &[ExecutionTemplate]) -> Result<Vec<Reply>, Fault> {
        self.run_event_c_checked(templates, |_, _| Ok(()))
    }

    // Shape/order are upfront checks; all individual financial fields are checked
    // only when their template is reached. A later fault never rolls back E1.
    fn run_event_c_checked(
        &mut self,
        templates: &[ExecutionTemplate],
        mut hook: impl FnMut(usize, Stage) -> Result<(), Fault>,
    ) -> Result<Vec<Reply>, Fault> {
        if !matches!(self.profile, ProfileContext::EventLimit(_))
            || templates.len() != 2
            || templates[0].order_id != "O_A"
            || templates[1].order_id != "O_B"
        {
            return Err(self.fail_execution("UNSUPPORTED_EXECUTION"));
        }
        let mut replies = Vec::new();
        for (index, template) in templates.iter().enumerate() {
            let candidate = self
                .event_c_candidate(template, index)
                .map_err(|fault| self.fail_execution(fault))?;
            let reply = self.execute_checked(&candidate, |stage| hook(index, stage))?;
            let terminal = matches!(
                reply,
                Reply::Committed {
                    terminal_reason: Some(_),
                    ..
                }
            );
            replies.push(reply);
            if terminal {
                break;
            }
        }
        Ok(replies)
    }

    pub(super) fn prepare_event_c_candidate(
        &self,
        candidate: &ExecutionCandidate,
        order: &RestingOrder,
        execution_id: Hash,
        digest: Hash,
    ) -> Result<Preparation<'_>, Fault> {
        let (ProfileContext::EventLimit(config), PositionState::EventLimit(position)) =
            (&self.profile, &self.positions)
        else {
            return Err("PROFILE_MISMATCH");
        };
        if let Err(reason) = remainder_eligibility(&order.facts, position.as_ref()) {
            return Ok(Preparation::Rejected(reason));
        }
        let template = &candidate.template;
        let (external, id) = match template.order_id.as_str() {
            "O_A" => ("X_A", "E1"),
            "O_B" => ("X_B", "E2"),
            _ => return Err("UNSUPPORTED_EXECUTION"),
        };
        if template.key.account != self.key
            || template.key.product != ProfileProduct::Pa
            || template.key.namespace != "golden-c09-v1"
            || template.key.external_id != external
            || candidate.event_id != "C"
            || candidate.candidate_id != id
            || candidate.spec_version != "event-limit-spec-v1"
            || candidate.rule_data_version != "event-limit-data-v1"
            || template.side != Side::Long
            || template.quantity != Decimal::ONE
            || template.price != Decimal::TEN
            || template.liquidity != LiquidityRole::SyntheticTaker
            || template.fee_asset.is_some()
            || template.fee_amount.is_some()
            || !order.facts.projects_remainder("UNSUPPORTED_EXECUTION")?
            || (id == "E2" && self.target_order("O_A")?.facts.status != "FILLED")
        {
            return Err("UNSUPPORTED_EXECUTION");
        }
        self.event_limit_projection()?;
        let committed = self
            .execution_receipts
            .values()
            .filter(|receipt| receipt.event_id == "C")
            .count();
        if committed as u64 >= config.execution_limit(template.matching_effective_at)? {
            return Ok(Preparation::Rejected("EVENT_EXECUTION_LIMIT"));
        }
        Ok(Preparation::Eligible {
            execution_id,
            digest,
        })
    }

    // This uses the shared draft DTO only, never the BTC calculator or fee math.
    pub(super) fn event_c_settlement(
        &self,
        candidate: &ExecutionCandidate,
        execution_id: Hash,
    ) -> Result<hypothetical_settlement::Draft, Fault> {
        self.event_limit_projection()?;
        let PositionState::EventLimit(current) = &self.positions else {
            return Err("PROFILE_MISMATCH");
        };
        let mut position = current.clone().unwrap_or(ProductPosition {
            side: Side::Long,
            contracts: Decimal::ZERO,
            lots: Vec::new(),
            entry_basis: Decimal::ZERO,
        });
        position.contracts = add(position.contracts, Decimal::ONE)?;
        position.entry_basis = add(position.entry_basis, Decimal::TEN)?;
        position.lots.push(EntryLot {
            source: SeedLot {
                seed_execution_id: candidate.template.key.external_id.clone(),
                seed_sequence: self.commit_sequence,
                strategy_id: self
                    .target_order(&candidate.template.order_id)?
                    .facts
                    .strategy_id
                    .clone(),
                contracts: Decimal::ONE,
                entry: Decimal::TEN,
            },
            execution_id,
            base_quantity: Decimal::ONE,
        });
        Ok(hypothetical_settlement::Draft {
            position: Some(position),
            gross_realized_delta: Decimal::ZERO,
            fee: Decimal::ZERO,
            cash_delta: Decimal::ZERO,
        })
    }
}

#[cfg(test)]
mod tests;
