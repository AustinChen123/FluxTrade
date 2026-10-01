//! Immutable output only; saved identities and payloads are never recomputed.
use super::super::super::wire::{account, decimal, fields, hash, number, string};
use super::super::identity::{Body, Delivery};
use super::*;
#[cfg(test)]
mod tests;

impl Delivery {
    pub(in super::super::super) fn wire_json(&self) -> Result<Json, Fault> {
        let p = &self.projection;
        let payload = match &self.body {
            Body::Source(Payload::Execution(r, client)) => r.delivery_json(client)?,
            Body::Source(Payload::Transport(t)) => t.wire_json(),
            Body::Snapshot(f) => f.payload_json()?,
        };
        let mut rows = vec![
            ("account_key", account(&self.account)),
            ("delivery_id", hash(self.delivery_id)),
            ("source_fact_id", string(&p.reference.fact_id)),
            ("source_namespace", string(p.reference.namespace)),
            ("payload_kind", string(p.kind)),
            ("occurrence_index", number(p.occurrence)?),
            ("schedule_sequence", number(p.sequence)?),
            ("immutable_payload", payload),
            ("payload_digest", hash(self.payload_digest)),
            ("visible_at", number(p.visible_at)?),
        ];
        for (key, value) in [
            ("snapshot_version", self.snapshot_version),
            ("snapshot_as_of", self.snapshot_as_of),
        ] {
            if let Some(v) = value {
                rows.push((key, number(v)?));
            }
        }
        if let Some(v) = &p.continuation {
            rows.push(("continuation_id", string(v)));
        }
        Ok(fields(rows))
    }
}
impl Transport {
    fn wire_json(&self) -> Json {
        let mut rows = vec![
            ("route", string(self.route)),
            ("operation", string(self.operation)),
            ("client_order_id", string(&self.client)),
            ("code", string(&self.code)),
        ];
        for (key, value) in [
            ("order_id", self.order.as_deref()),
            ("message", self.message.as_deref()),
            ("product_id", self.product.as_deref()),
            ("side", self.side.map(side)),
        ] {
            if let Some(v) = value {
                rows.push((key, string(v)));
            }
        }
        for (key, value) in [("limit_price", self.price), ("size_contracts", self.size)] {
            if let Some(v) = value {
                rows.push((key, decimal(v)));
            }
        }
        fields(rows)
    }
}
