//! GT-03 execution eligibility adapter; the existing writer and FIFO calculator settle.
use super::*;

impl ScenarioAccount {
    pub(super) fn prepare_golden_cancel_candidate<'a>(
        &self,
        candidate: &ExecutionCandidate,
        order: &RestingOrder,
        execution_id: Hash,
        digest: Hash,
    ) -> Result<Preparation<'a>, Fault> {
        let t = &candidate.template;
        if t.key.account != self.key
            || t.key.product != ProfileProduct::Pa
            || t.key.product != order.facts.product
            || t.side != order.facts.side
            || !identity(&t.key.namespace)
            || !identity(&t.key.external_id)
            || !identity(&candidate.candidate_id)
            || !identity(&candidate.event_id)
            || !order.facts.projects_remainder("UNSUPPORTED_EXECUTION")?
            || t.price != Decimal::TEN
            || (self.admitted_order_type(&order.facts) == admission::OrderType::Limit
                && match order.facts.side {
                    Side::Long => t.price > order.facts.price,
                    Side::Short => t.price < order.facts.price,
                })
            || t.quantity < Decimal::ONE
            || t.quantity > order.facts.remaining
            || !aligned(t.quantity, Decimal::ONE)
            || candidate.spec_version != "gt03-spec-v1"
            || candidate.rule_data_version != "gt03-rule-v1"
            || t.liquidity != LiquidityRole::SyntheticTaker
            || t.fee_asset.is_some()
            || t.fee_amount.is_some()
        {
            return Err("UNSUPPORTED_EXECUTION");
        }
        Ok(Preparation::Eligible {
            execution_id,
            digest,
        })
    }
}
