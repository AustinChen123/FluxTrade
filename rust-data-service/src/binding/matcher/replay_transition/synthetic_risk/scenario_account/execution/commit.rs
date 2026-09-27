//! One draft publication for settlement, orders, dedup, versions and terminal gate.
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reply {
    Committed {
        receipt: CommittedExecution,
        terminal_reason: Option<Fault>,
    },
    Duplicate(CommittedExecution),
    Rejected(Fault),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    Preparing,
    Validated,
    Settled,
    OrdersDrafted,
    Valued,
    ReceiptDrafted,
}

impl ScenarioAccount {
    pub(super) fn execute(&mut self, candidate: &ExecutionCandidate) -> Result<Reply, Fault> {
        self.execute_checked(candidate, |_| Ok(()))
    }

    // The interruption hook receives no owner or draft reference.
    pub(super) fn execute_checked(
        &mut self,
        candidate: &ExecutionCandidate,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<Reply, Fault> {
        let prepared = catch_unwind(AssertUnwindSafe(|| {
            hook(Stage::Preparing)?;
            let (execution_id, digest) = match self.prepare_execution(candidate)? {
                Preparation::Duplicate(receipt) => {
                    return Ok((Reply::Duplicate(receipt.clone()), None))
                }
                Preparation::Rejected(reason) => return Ok((Reply::Rejected(reason), None)),
                Preparation::Eligible {
                    execution_id,
                    digest,
                } => (execution_id, digest),
            };
            hook(Stage::Validated)?;
            let mut draft = self.clone();
            let receipt = draft.settle_execution(candidate, execution_id, digest, &mut hook)?;
            let terminal_reason = match draft.gate {
                Gate::Running => None,
                Gate::Failed(reason) => Some(reason),
            };
            draft
                .execution_receipts
                .insert(execution_id, receipt.clone());
            hook(Stage::ReceiptDrafted)?;
            Ok((
                Reply::Committed {
                    receipt,
                    terminal_reason,
                },
                Some(draft),
            ))
        }))
        .map_err(|_| "EXECUTION_PANIC")
        .and_then(|result| result);
        match prepared {
            Ok((reply, draft)) => {
                if let Some(draft) = draft {
                    *self = draft;
                }
                Ok(reply)
            }
            Err(fault) => Err(self.fail_execution(fault)),
        }
    }

    // Called only on the unobservable cloned owner, never on authoritative self.
    fn settle_execution(
        &mut self,
        candidate: &ExecutionCandidate,
        execution_id: Hash,
        digest: Hash,
        hook: &mut impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<CommittedExecution, Fault> {
        let template = &candidate.template;
        let order = self.target_order(&template.order_id)?;
        let order_version_before = order.version;
        let (product, before_reservation, position_before, settled) =
            if matches!(self.profile, ProfileContext::EventLimit(_)) {
                let PositionState::EventLimit(position) = &self.positions else {
                    return Err("PROFILE_MISMATCH");
                };
                (
                    None,
                    FinancialSnapshot::EventLimit(self.event_limit_projection()?),
                    position.clone(),
                    self.event_c_settlement(candidate, execution_id)?,
                )
            } else {
                let product = template.key.product.btc()?;
                let before_reservation = self.reservation()?;
                let position_before = self.positions.btc()?.get(&product).cloned();
                let opening = position_before
                    .as_ref()
                    .is_none_or(|p| p.side == template.side)
                    .then(|| hypothetical_settlement::OpeningIdentity {
                        source_id: format!("runtime:{execution_id:02x?}"),
                        strategy_id: order.facts.strategy_id.clone(),
                        execution_id,
                        sequence: self.commit_sequence,
                    });
                let (scenario, _) = self.btc_context()?;
                let (spec, _) = scenario.resolve(product, template.matching_effective_at)?;
                let settled = hypothetical_settlement::calculate(
                    position_before.as_ref(),
                    template.side,
                    template.quantity,
                    template.price,
                    (scenario, spec),
                    hypothetical_settlement::FeePolicy::BtcEthTradingTaker,
                    opening.as_ref(),
                )?;
                (
                    Some(product),
                    FinancialSnapshot::BtcEth(before_reservation),
                    position_before,
                    settled,
                )
            };
        hook(Stage::Settled)?;
        let state_version_before = self.state_version;
        let sequence = self.commit_sequence;
        self.state_version = self
            .state_version
            .checked_add(1)
            .ok_or("VERSION_OVERFLOW")?;
        self.commit_sequence = sequence.checked_add(1).ok_or("VERSION_OVERFLOW")?;
        self.cash = add(self.cash, settled.cash_delta)?;
        self.gross_realized = add(self.gross_realized, settled.gross_realized_delta)?;
        self.fees = add(self.fees, settled.fee)?;
        match (&mut self.positions, product) {
            (PositionState::BtcEth(positions), Some(product)) => match &settled.position {
                Some(position) => {
                    positions.insert(product, position.clone());
                }
                None => {
                    positions.remove(&product);
                }
            },
            (PositionState::EventLimit(position), None) => *position = settled.position.clone(),
            _ => return Err("PROFILE_MISMATCH"),
        }
        let order = self
            .orders
            .get_mut(&template.order_id)
            .ok_or("UNKNOWN_ORDER")?;
        order.version = order.version.checked_add(1).ok_or("VERSION_OVERFLOW")?;
        order.facts.filled = add(order.facts.filled, template.quantity)?;
        order.facts.remaining = add(order.facts.remaining, -template.quantity)?;
        order.facts.status = if order.facts.remaining == Decimal::ZERO {
            "FILLED"
        } else {
            "PARTIALLY_FILLED"
        }
        .into();
        let order_version_after = order.version;
        hook(Stage::OrdersDrafted)?;
        let (after_reservation, risk) = self.execution_snapshot_after()?;
        hook(Stage::Valued)?;
        Ok(CommittedExecution {
            account_key: self.key.clone(),
            execution_id,
            financial_payload_digest: digest,
            event_id: candidate.event_id.clone(),
            commit_sequence: sequence,
            state_version_before,
            state_version_after: self.state_version,
            order_version_before,
            order_version_after,
            product: template.key.product,
            order_id: template.order_id.clone(),
            quantity: template.quantity,
            price: template.price,
            fee_asset: "USDT".into(),
            fee_amount: settled.fee,
            realized_pnl_delta: settled.gross_realized_delta,
            cash_deltas: vec![("USDT".into(), settled.cash_delta)],
            position_before,
            position_after: settled.position,
            reservation_before: before_reservation,
            reservation_after: after_reservation,
            spec_version: candidate.spec_version.clone(),
            rule_data_version: candidate.rule_data_version.clone(),
            risk_state_after: risk,
            pending_action_ids: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests;
