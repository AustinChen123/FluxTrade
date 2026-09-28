//! Unexposed routing checkpoint; panic containment and poison require 5B-2b.
use super::*;
#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq, Eq)]
enum BoundaryError {
    Input(Fault),
    Lookup(Fault),
    Conflict(Fault),
    Invariant,
}
fn boundary(reason: Fault) -> BoundaryError {
    match reason {
        "INVALID_JSON" | "INVALID_SCHEMA" | "ACCOUNT_KEY_MISMATCH" => BoundaryError::Input(reason),
        "UNKNOWN_RECEIPT_REFERENCE" => BoundaryError::Lookup(reason),
        "SNAPSHOT_ID_CONFLICT" | "DELIVERY_ID_CONFLICT" => BoundaryError::Conflict(reason),
        _ => BoundaryError::Invariant,
    }
}
type Reply = Result<String, BoundaryError>;
struct Session {
    owner: ScenarioAccount,
    snapshots: snapshot::Store,
    deliveries: delivery::store::Store,
    completed: BTreeMap<(String, String), (Hash, String)>,
}
impl Session {
    fn new(profile: &str, account: &str) -> Result<Self, BoundaryError> {
        let key = wire::decode(account)
            .and_then(|v| v.account())
            .map_err(boundary)?;
        let owner = wire::profiles::construct(profile, key).map_err(boundary)?;
        Ok(Self {
            snapshots: snapshot::Store::new(owner.key.clone()),
            deliveries: delivery::store::Store::new(owner.key.clone()),
            owner,
            completed: BTreeMap::new(),
        })
    }
    fn apply_group(&mut self, request: &str) -> Reply {
        let group = wire::group::decode_group(request, &self.owner.key).map_err(boundary)?;
        let before = self.owner.state_version;
        let outcome = self.owner.apply_group_observed(&group, |_| Ok(()));
        self.group_result(&group, before, outcome)
    }
    fn group_result(
        &mut self,
        group: &group::Group,
        before: u64,
        outcome: Result<group::Applied, Fault>,
    ) -> Reply {
        let key = (group.ordering_contract_id.clone(), group.group_id.clone());
        match outcome {
            Ok(group::Applied::Duplicate(c)) => self
                .completed
                .get(&key)
                .filter(|(digest, _)| *digest == c.digest)
                .map(|(_, bytes)| bytes.clone())
                .ok_or(BoundaryError::Invariant),
            Ok(group::Applied::Fresh(c)) => {
                let bytes = wire::result::GroupResult::from_outcome(
                    &self.owner,
                    &group.group_id,
                    before,
                    Ok(&c),
                )
                .and_then(|r| r.canonical())
                .map_err(|_| BoundaryError::Invariant)?;
                if self.completed.contains_key(&key) {
                    return Err(BoundaryError::Invariant);
                }
                self.completed.insert(key, (c.digest, bytes.clone()));
                Ok(bytes)
            }
            Err(reason) => wire::result::GroupResult::from_outcome(
                &self.owner,
                &group.group_id,
                before,
                Err(reason),
            )
            .and_then(|r| r.canonical())
            .map_err(|_| BoundaryError::Invariant),
        }
    }
    fn capture_snapshot(&mut self, request: &str) -> Reply {
        let request = snapshot::wire::decode_request(request, &self.owner.key).map_err(boundary)?;
        self.snapshots
            .capture(&self.owner, &request)
            .map_err(boundary)?
            .wire_json()
            .and_then(|v| v.canonical())
            .map_err(|_| BoundaryError::Invariant)
    }
    fn build_delivery(&mut self, request: &str) -> Reply {
        let request = delivery::wire::decode_projection(request).map_err(boundary)?;
        self.deliveries
            .build(&self.owner, &self.snapshots, &request)
            .map_err(boundary)?
            .wire_json()
            .and_then(|v| v.canonical())
            .map_err(|_| BoundaryError::Invariant)
    }
    fn inspect_state(&self) -> Reply {
        self.owner
            .inspect_state()
            .and_then(|v| v.wire_json())
            .and_then(|v| v.canonical())
            .map_err(|_| BoundaryError::Invariant)
    }
}
