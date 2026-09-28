//! Bounded current economic evidence, not a restart state or a second ledger.
use super::*;
use risk_transition::{cancel, cancel::identity::Encoding, Lifecycle};
mod wire;

#[derive(Clone, Debug, PartialEq, Eq)]
struct CurrentBasis {
    account: AccountKey,
    profile: &'static str,
    config: String,
    version: i64,
    context: Hash,
    gate: Gate,
    lifecycle: Lifecycle,
    cash: Decimal,
    gross: Decimal,
    fees: Decimal,
    positions: Hash,
    orders: Hash,
    reservations: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Inspection {
    basis: CurrentBasis,
    owner_state_digest: Hash,
}
pub(super) fn lifecycle(value: Lifecycle) -> &'static str {
    match value {
        Lifecycle::RiskStable => "RISK_STABLE",
        Lifecycle::AwaitingCancelEffective => "AWAITING_CANCEL_EFFECTIVE",
        Lifecycle::LiquidatedFlat => "LIQUIDATED_FLAT",
        Lifecycle::LiquidatedInsolvent => "LIQUIDATED_INSOLVENT",
    }
}
pub(super) fn number(e: &mut Encoding, n: impl TryInto<i64>) -> Result<(), Fault> {
    e.integer(n.try_into().map_err(|_| "NATIVE_INVARIANT")?);
    Ok(())
}
pub(super) fn decimals(e: &mut Encoding, values: &[Decimal]) {
    for v in values {
        e.text(&v.normalize().to_string());
    }
}
fn side(e: &mut Encoding, s: Side) {
    e.text(if s == Side::Long { "LONG" } else { "SHORT" });
}
pub(super) fn order_facts(e: &mut Encoding, o: &SeedOrder) -> Result<(), Fault> {
    for s in [
        &o.intent_id,
        &o.order_id,
        &o.client_id,
        &o.strategy_id,
        o.product.canonical_id(),
    ] {
        e.text(s);
    }
    side(e, o.side);
    decimals(e, &[o.price]);
    e.presence(o.reduce_only);
    decimals(e, &[o.original, o.filled, o.canceled, o.remaining]);
    if !matches!(
        o.status.as_str(),
        "OPEN" | "PARTIALLY_FILLED" | "FILLED" | "CANCELED"
    ) {
        return Err("NATIVE_INVARIANT");
    }
    e.text(&o.status);
    Ok(())
}
fn cancel_request(e: &mut Encoding, r: &cancel::CanonicalRequest) {
    e.text(r.reason.name());
    e.hash(r.request_id);
    e.text(&r.detecting_event_id);
    e.hash(r.effect_action_id);
    e.hash(r.delivery_action_id);
}
impl ScenarioAccount {
    pub(super) fn owner_evidence_digest(&self) -> Result<Hash, Fault> {
        Ok(self.inspect_state()?.owner_state_digest)
    }
    fn encode_inspection_sources(&self, e: &mut Encoding) -> Result<(), Fault> {
        number(e, self.transition.event_kinds.len())?;
        for (id, kind) in &self.transition.event_kinds {
            e.text(id);
            e.text(match kind {
                source::Kind::Context => "CONTEXT",
                source::Kind::Execution => "EXECUTION",
                source::Kind::Intent => "INTENT",
                source::Kind::CancelRequest => "CANCEL_REQUEST",
                source::Kind::CancelEffect => "CANCEL_EFFECT",
                source::Kind::EventC => return Err("NATIVE_INVARIANT"),
            });
        }
        Ok(())
    }
    fn inspect_state(&self) -> Result<Inspection, Fault> {
        let (basis, mut e) = self.current_evidence()?;
        self.encode_inspection_intents(&mut e)?;
        self.encode_inspection_executions(&mut e)?;
        self.cancel_facts.encode_inspection_receipts(&mut e)?;
        self.cancel_facts.encode_inspection_actions(&mut e)?;
        self.encode_inspection_liquidations(&mut e)?;
        self.encode_inspection_sources(&mut e)?;
        Ok(Inspection {
            basis,
            owner_state_digest: e.finish(),
        })
    }
    fn encode_positions(&self, e: &mut Encoding) -> Result<(), Fault> {
        let mut rows: Vec<_> = match &self.positions {
            PositionState::BtcEth(p) => p.iter().map(|(p, v)| (product_id(*p), v)).collect(),
            PositionState::GoldenCancel(p) => p.iter().map(|p| ("P_A", p)).collect(),
            _ => return Err("NATIVE_INVARIANT"),
        };
        rows.sort_by_key(|r| r.0);
        number(e, rows.len())?;
        for (product, p) in rows {
            e.text(product);
            side(e, p.side);
            decimals(e, &[p.contracts, p.entry_basis]);
            number(e, p.lots.len())?;
            for lot in &p.lots {
                let l = &lot.source;
                e.text(&l.seed_execution_id);
                number(e, l.seed_sequence)?;
                e.text(&l.strategy_id);
                decimals(e, &[l.contracts, l.entry]);
                e.text(&lot.origin_spec_version);
                e.hash(lot.execution_id);
                decimals(e, &[lot.base_quantity]);
            }
        }
        Ok(())
    }
    fn encode_orders(&self, e: &mut Encoding) -> Result<(), Fault> {
        let mut rows: Vec<_> = self.orders.values().collect();
        rows.sort_by_key(|o| (o.facts.product.canonical_id(), &o.facts.order_id));
        number(e, rows.len())?;
        for o in rows {
            order_facts(e, &o.facts)?;
            e.integer(o.created_at);
            number(e, o.version)?;
            match &o.cancel {
                cancel::State::None => e.text("NONE"),
                cancel::State::Requested(r) => {
                    e.text("REQUESTED");
                    cancel_request(e, r);
                }
                cancel::State::EffectiveCanceled(f) | cancel::State::EffectiveTooLate(f) => {
                    e.text(if matches!(o.cancel, cancel::State::EffectiveCanceled(_)) {
                        "EFFECTIVE_CANCELED"
                    } else {
                        "EFFECTIVE_TOO_LATE"
                    });
                    cancel_request(e, &f.request);
                    e.hash(f.action_id);
                }
                cancel::State::MigrationEffective(_) => return Err("NATIVE_INVARIANT"),
            }
        }
        Ok(())
    }
    fn encode_reservations(&self, e: &mut Encoding) -> Result<(), Fault> {
        if matches!(self.profile, ProfileContext::GoldenCancel(_)) {
            e.text("GOLDEN_CANCEL");
            decimals(
                e,
                &[self
                    .golden_cancel_reservation()
                    .map_err(|_| "NATIVE_INVARIANT")?],
            );
        } else {
            let mut r = self.reservation().map_err(|_| "NATIVE_INVARIANT")?;
            e.text("BTC_ETH");
            decimals(
                e,
                &[
                    r.equity,
                    r.maintenance_margin,
                    r.total_order_loss,
                    r.total_fee_hold,
                    r.used_margin,
                    r.available_margin,
                ],
            );
            r.products.sort_by_key(|p| product_id(p.product));
            number(e, r.products.len())?;
            for p in r.products {
                e.text(product_id(p.product));
                decimals(
                    e,
                    &[
                        p.position_value,
                        p.long_remaining_value,
                        p.short_remaining_value,
                        p.exposure_margin,
                    ],
                );
            }
            r.orders
                .sort_by_key(|o| (product_id(o.product), o.order_id.clone()));
            number(e, r.orders.len())?;
            for o in r.orders {
                e.text(product_id(o.product));
                e.text(&o.order_id);
                side(e, o.side);
                decimals(
                    e,
                    &[
                        o.remaining_contracts,
                        o.remaining_base_exposure,
                        o.order_loss,
                        o.fee_hold,
                    ],
                );
            }
        }
        Ok(())
    }
    fn component(
        &self,
        domain: &str,
        encode: fn(&Self, &mut Encoding) -> Result<(), Fault>,
    ) -> Result<Hash, Fault> {
        let mut e = Encoding::new(domain);
        encode(self, &mut e)?;
        Ok(e.finish())
    }
    fn inspection_basis(&self) -> Result<CurrentBasis, Fault> {
        let profile = match self.profile {
            ProfileContext::BtcEthScenario {
                min_cash_profile: false,
                ..
            } => "SYNTHETIC_BTC_ETH_V1",
            ProfileContext::BtcEthScenario {
                min_cash_profile: true,
                ..
            } => "SYNTHETIC_MIN_CASH_V1",
            ProfileContext::GoldenCancel(_) => "SYNTHETIC_GOLDEN_CANCEL_V1",
            _ => return Err("NATIVE_INVARIANT"),
        };
        Ok(CurrentBasis {
            account: self.key.clone(),
            profile,
            config: self.config_id.clone(),
            version: i64::try_from(self.state_version).map_err(|_| "NATIVE_INVARIANT")?,
            context: self.valuation_context_id,
            gate: self.gate.clone(),
            lifecycle: self.transition.lifecycle,
            cash: self.cash,
            gross: self.gross_realized,
            fees: self.fees,
            positions: self.component("SCENARIO_POSITIONS_EVIDENCE_V1", Self::encode_positions)?,
            orders: self.component("SCENARIO_ORDERS_EVIDENCE_V1", Self::encode_orders)?,
            reservations: self.component(
                "SCENARIO_RESERVATIONS_EVIDENCE_V1",
                Self::encode_reservations,
            )?,
        })
    }
    // The unfinished encoder is consumed by 3b; no partial owner digest escapes.
    fn current_evidence(&self) -> Result<(CurrentBasis, Encoding), Fault> {
        let b = self.inspection_basis()?;
        let mut e = Encoding::new("SCENARIO_OWNER_EVIDENCE_V1");
        e.account(&b.account);
        e.text(b.profile);
        e.text(&b.config);
        e.integer(b.version);
        e.hash(b.context);
        let failure = match b.gate {
            Gate::Running => {
                e.text("RUNNING");
                None
            }
            Gate::Failed(f) => {
                e.text("FAILED");
                Some(f)
            }
        };
        e.optional_text(failure);
        e.text(match b.lifecycle {
            Lifecycle::RiskStable => "RISK_STABLE",
            Lifecycle::AwaitingCancelEffective => "AWAITING_CANCEL_EFFECTIVE",
            Lifecycle::LiquidatedFlat => "LIQUIDATED_FLAT",
            Lifecycle::LiquidatedInsolvent => "LIQUIDATED_INSOLVENT",
        });
        decimals(&mut e, &[b.cash, b.gross, b.fees]);
        self.encode_positions(&mut e)?;
        self.encode_orders(&mut e)?;
        self.encode_reservations(&mut e)?;
        Ok((b, e))
    }
}
#[cfg(test)]
pub(super) mod completion_tests;
#[cfg(test)]
pub(super) mod receipt_vectors;
#[cfg(test)]
mod tests;
