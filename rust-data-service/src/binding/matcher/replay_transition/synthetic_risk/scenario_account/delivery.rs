//! Pure committed-fact projection; no delivery store, clock, or financial writer.
use super::*;
use risk_transition::cancel::identity::Encoding;
mod identity;
pub(super) mod store;
pub(super) mod wire;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Transport {
    pub route: &'static str,
    pub operation: &'static str,
    pub client: String,
    pub order: Option<String>,
    pub code: String,
    pub message: Option<String>,
    pub product: Option<String>,
    pub side: Option<Side>,
    pub price: Option<Decimal>,
    pub size: Option<Decimal>,
}
pub(super) fn side(side: Side) -> &'static str {
    if side == Side::Long {
        "buy"
    } else {
        "sell"
    }
}
impl Transport {
    fn validate(&self) -> Result<(), Fault> {
        if !matches!(self.route, "REST" | "WS")
            || !matches!(self.operation, "ORDER" | "CANCEL")
            || !identity(&self.client)
            || self.order.as_deref().is_some_and(|s| !identity(s))
            || self.product.as_deref().is_some_and(|s| !identity(s))
        {
            return Err("INVALID_SCHEMA");
        }
        if self.route == "REST"
            && self.operation == "ORDER"
            && (self.product.is_none()
                || self.side.is_none()
                || self.price.is_none()
                || self.size.is_none())
        {
            return Err("INVALID_SCHEMA");
        }
        Ok(())
    }
    pub(super) fn matches(&self, order: &SeedOrder) -> bool {
        self.client == order.client_id
            && self.order.as_ref().is_none_or(|v| v == &order.order_id)
            && self
                .product
                .as_ref()
                .is_none_or(|v| v == order.product.canonical_id())
            && self.side.is_none_or(|v| v == order.side)
            && self.price.is_none_or(|v| v == order.price)
            && self.size.is_none_or(|v| v == order.original)
    }
    fn encode(&self, e: &mut Encoding) {
        for s in [self.route, self.operation, &self.client] {
            e.text(s);
        }
        e.optional_text(self.order.as_deref());
        e.text(&self.code);
        e.optional_text(self.message.as_deref());
        e.optional_text(self.product.as_deref());
        e.optional_text(self.side.map(side));
        e.optional_text(self.price.map(|v| v.normalize().to_string()).as_deref());
        e.optional_text(self.size.map(|v| v.normalize().to_string()).as_deref());
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Payload {
    Execution(Box<execution::CommittedExecution>, String),
    Transport(Transport),
}
impl Payload {
    pub(super) fn encode(&self, e: &mut Encoding) -> Result<(), Fault> {
        match self {
            Self::Execution(receipt, client) => {
                e.text("EXECUTION_FACT");
                receipt.encode_delivery(e, client)?;
            }
            Self::Transport(t) => {
                e.text("TRANSPORT_ACK");
                t.encode(e);
            }
        }
        Ok(())
    }
}
pub(super) fn source(
    owner: &ScenarioAccount,
    event: &str,
    kind: &str,
    transport: Option<&Transport>,
) -> Result<Payload, Fault> {
    if !identity(event) {
        return Err("INVALID_SCHEMA");
    }
    let source_kind = owner
        .transition
        .event_kinds
        .get(event)
        .ok_or("UNKNOWN_RECEIPT_REFERENCE")?;
    match (source_kind, kind, transport) {
        (source::Kind::Execution, "EXECUTION_FACT", None) => {
            let receipt = owner
                .execution_receipts
                .values()
                .find(|r| r.source_event() == event)
                .ok_or("NATIVE_INVARIANT")?;
            let client =
                receipt.policy_client(matches!(owner.profile, ProfileContext::GoldenCancel(_)))?;
            Ok(Payload::Execution(Box::new(receipt.clone()), client))
        }
        (source::Kind::Intent | source::Kind::CancelEffect, "TRANSPORT_ACK", Some(t)) => {
            t.validate()?;
            let valid = if *source_kind == source::Kind::Intent {
                t.operation == "ORDER"
                    && owner
                        .intent_results
                        .values()
                        .filter_map(|r| r.delivery_order(event))
                        .any(|o| t.matches(o))
            } else {
                t.operation == "CANCEL" && owner.cancel_facts.delivery_match(event, t)
            };
            if !valid {
                return Err("INVALID_SCHEMA");
            }
            Ok(Payload::Transport(t.clone()))
        }
        (source::Kind::CancelEffect, "TRANSPORT_ACK", None) if matches!(owner.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some()) =>
        {
            let (stamp, _) = owner
                .transition
                .events
                .get(event)
                .ok_or("UNKNOWN_RECEIPT_REFERENCE")?;
            if stamp.ordering_contract_id != "HISTORICAL_ORDER_V1"
                || stamp.source_sequence.is_none()
                || stamp.scenario_ordinal != 50
            {
                return Err("INVALID_SCHEMA");
            }
            Ok(Payload::Transport(
                owner.historical_cancel_ack_transport(event)?,
            ))
        }
        _ => Err("INVALID_SCHEMA"),
    }
}

pub(super) fn resolve(
    owner: &ScenarioAccount,
    reference: &group::Reference,
    kind: &str,
    transport: Option<&Transport>,
) -> Result<Payload, Fault> {
    match reference {
        group::Reference::Source(event) => source(owner, event, kind, transport),
        group::Reference::Liquidation(id) => {
            Err(if owner.liquidation_ids().any(|known| known == *id) {
                "INVALID_SCHEMA"
            } else {
                "UNKNOWN_RECEIPT_REFERENCE"
            })
        }
    }
}
#[cfg(test)]
mod tests;
