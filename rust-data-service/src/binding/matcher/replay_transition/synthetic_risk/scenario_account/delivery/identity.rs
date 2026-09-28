//! Final delivery values and canonical identity only; no lookup or publication.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Reference {
    pub namespace: &'static str,
    pub fact_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Projection {
    pub schema_version: String,
    pub(super) reference: Reference,
    pub kind: &'static str,
    pub occurrence: i64,
    pub sequence: i64,
    pub visible_at: i64,
    pub continuation: Option<String>,
    pub transport: Option<Transport>,
}
impl Projection {
    pub(super) fn validate(&self) -> Result<(), Fault> {
        if self.schema_version != "delivery_projection_v1"
            || !matches!(
                self.reference.namespace,
                "SOURCE" | "LIQUIDATION" | "SNAPSHOT"
            )
            || !identity(&self.reference.fact_id)
            || !matches!(
                self.kind,
                "EXECUTION_FACT"
                    | "TRANSPORT_ACK"
                    | "MARKET_SNAPSHOT"
                    | "EARN_SNAPSHOT"
                    | "TRADING_SNAPSHOT"
                    | "POSITION_SNAPSHOT"
                    | "OPEN_ORDER_SNAPSHOT"
            )
            || self.occurrence < 0
            || self.sequence < 0
            || self.visible_at < 0
            || self.continuation.as_deref().is_some_and(|s| !identity(s))
            || (self.kind == "TRANSPORT_ACK") != self.transport.is_some()
        {
            return Err("INVALID_SCHEMA");
        }
        if self.reference.namespace == "LIQUIDATION"
            && (self.reference.fact_id.len() != 64
                || !self
                    .reference
                    .fact_id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        {
            return Err("INVALID_SCHEMA");
        }
        if let Some(t) = &self.transport {
            t.validate()?;
        }
        Ok(())
    }
    pub(super) fn id(&self, account: &AccountKey) -> Hash {
        let mut e = Encoding::new("SCENARIO_DELIVERY_V1");
        e.account(account);
        e.text(self.reference.namespace);
        e.text(&self.reference.fact_id);
        e.text(self.kind);
        e.integer(self.occurrence);
        e.finish()
    }
    pub(super) fn request_digest(&self, account: &AccountKey) -> Hash {
        let mut e = Encoding::new("SCENARIO_DELIVERY_PROJECTION_V1");
        e.account(account);
        e.text(&self.schema_version);
        e.text(self.reference.namespace);
        e.text(&self.reference.fact_id);
        e.text(self.kind);
        e.integer(self.occurrence);
        e.integer(self.sequence);
        e.integer(self.visible_at);
        e.optional_text(self.continuation.as_deref());
        e.presence(self.transport.is_some());
        if let Some(t) = &self.transport {
            t.encode(&mut e);
        }
        e.finish()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Body {
    Source(Payload),
    Snapshot(Box<snapshot::Fact>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Delivery {
    pub account: AccountKey,
    pub projection: Projection,
    pub delivery_id: Hash,
    pub payload_digest: Hash,
    pub body: Body,
    pub snapshot_version: Option<i64>,
    pub snapshot_as_of: Option<i64>,
}
impl Delivery {
    pub(super) fn digest(&self) -> Result<Hash, Fault> {
        let p = &self.projection;
        let mut e = Encoding::new("SCENARIO_DELIVERY_PAYLOAD_V1");
        e.account(&self.account);
        e.text(&p.reference.fact_id);
        e.text(p.reference.namespace);
        e.text(p.kind);
        e.integer(p.occurrence);
        e.integer(p.sequence);
        match &self.body {
            Body::Source(payload) => payload.encode(&mut e)?,
            Body::Snapshot(fact) => fact.encode_payload(&mut e)?,
        }
        e.optional_integer(self.snapshot_version);
        e.optional_integer(self.snapshot_as_of);
        e.integer(p.visible_at);
        e.optional_text(p.continuation.as_deref());
        Ok(e.finish())
    }
}
#[cfg(test)]
mod tests;
