//! Immutable in-memory observations; borrowing the financial owner is read-only.
use self::identity::{Body, Delivery, Projection};
use super::*;
use risk_transition::cancel::identity::{classify, Stored};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Store {
    account: AccountKey,
    deliveries: BTreeMap<Hash, Stored<Delivery>>,
}
impl Store {
    pub(in super::super) fn new(account: AccountKey) -> Self {
        Self {
            account,
            deliveries: BTreeMap::new(),
        }
    }
    pub(in super::super) fn build(
        &mut self,
        owner: &ScenarioAccount,
        snapshots: &snapshot::Store,
        p: &Projection,
    ) -> Result<Delivery, Fault> {
        p.validate()?;
        self.account.validate().map_err(|_| "INVALID_SCHEMA")?;
        if self.account != owner.key {
            return Err("ACCOUNT_KEY_MISMATCH");
        }
        let id = p.id(&self.account);
        let digest = p.request_digest(&self.account);
        if let Some(original) = classify(self.deliveries.get(&id), digest, "DELIVERY_ID_CONFLICT")?
        {
            return Ok(original.clone());
        }
        let (body, snapshot_version, snapshot_as_of) = match p.reference.namespace {
            "SNAPSHOT" => {
                let fact = snapshots.lookup(&self.account, &p.reference.fact_id)?;
                let (version, as_of) = fact.delivery_metadata(p.kind, p.continuation.as_deref())?;
                (Body::Snapshot(Box::new(fact)), version, Some(as_of))
            }
            _ => {
                let reference = if p.reference.namespace == "SOURCE" {
                    group::Reference::Source(p.reference.fact_id.clone())
                } else {
                    let mut hash = [0; 32];
                    for (i, byte) in hash.iter_mut().enumerate() {
                        *byte = u8::from_str_radix(&p.reference.fact_id[i * 2..i * 2 + 2], 16)
                            .map_err(|_| "INVALID_SCHEMA")?;
                    }
                    group::Reference::Liquidation(hash)
                };
                let payload = resolve(owner, &reference, p.kind, p.transport.as_ref())?;
                if p.continuation.is_some() {
                    return Err("INVALID_SCHEMA");
                }
                (Body::Source(payload), None, None)
            }
        };
        let mut delivery = Delivery {
            account: self.account.clone(),
            projection: p.clone(),
            delivery_id: id,
            payload_digest: [0; 32],
            body,
            snapshot_version,
            snapshot_as_of,
        };
        delivery.payload_digest = delivery.digest()?;
        self.deliveries.insert(
            id,
            Stored {
                digest,
                value: delivery.clone(),
            },
        );
        Ok(delivery)
    }
}
#[cfg(test)]
mod tests;
