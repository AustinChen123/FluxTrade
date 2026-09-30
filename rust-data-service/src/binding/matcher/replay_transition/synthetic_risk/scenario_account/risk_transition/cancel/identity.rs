//! Canonical typed encoding and the shared duplicate/conflict classifier.
use super::*;
use ring::digest::{digest, SHA256};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super::super) struct Stored<T> {
    pub digest: Hash,
    pub value: T,
}

pub(in super::super::super) fn classify<T>(
    stored: Option<&Stored<T>>,
    incoming: Hash,
    conflict: Fault,
) -> Result<Option<&T>, Fault> {
    match stored {
        Some(s) if s.digest == incoming => Ok(Some(&s.value)),
        Some(_) => Err(conflict),
        None => Ok(None),
    }
}

pub(in super::super::super) struct Encoding(Vec<u8>);
impl Encoding {
    pub(in super::super::super) fn presence(&mut self, present: bool) {
        self.0.push(u8::from(present));
    }
    pub(in super::super::super) fn optional_integer(&mut self, value: Option<i64>) {
        match value {
            Some(v) => {
                self.0.push(1);
                self.integer(v);
            }
            None => self.0.push(0),
        }
    }
    pub(in super::super::super) fn optional_text(&mut self, value: Option<&str>) {
        match value {
            Some(v) => {
                self.0.push(1);
                self.text(v);
            }
            None => self.0.push(0),
        }
    }
    pub(in super::super::super) fn hash(&mut self, value: Hash) {
        self.0.extend_from_slice(&value);
    }
    pub(in super::super::super) fn new(kind: &str) -> Self {
        let mut e = Self(Vec::new());
        e.text(kind);
        e
    }
    pub(in super::super::super) fn text(&mut self, value: &str) {
        self.integer(value.len() as i64);
        self.0.extend_from_slice(value.as_bytes());
    }
    pub(in super::super::super) fn integer(&mut self, value: i64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    pub(in super::super::super) fn account(&mut self, key: &AccountKey) {
        self.text(&key.venue);
        self.text(&key.environment);
        self.text(&key.account);
        match &key.subaccount {
            Some(s) => {
                self.0.push(1);
                self.text(s);
            }
            None => self.0.push(0),
        }
    }
    pub(in super::super::super) fn stamp_tail(&mut self, stamp: &Stamp) {
        match stamp.source_sequence {
            Some(s) => {
                self.0.push(1);
                self.integer(s);
            }
            None => self.0.push(0),
        }
        let mut parents = stamp.causal_parent_ids.clone();
        parents.sort();
        self.integer(parents.len() as i64);
        for parent in parents {
            self.text(&parent);
        }
        self.text(&stamp.ordering_contract_id);
        self.integer(stamp.scenario_ordinal);
    }
    pub(in super::super::super) fn finish(self) -> Hash {
        digest(&SHA256, &self.0)
            .as_ref()
            .try_into()
            .expect("SHA-256 length")
    }
}

fn derived(key: &AccountKey, detecting: &str, target: &str, phase: Option<i64>) -> Hash {
    let mut e = Encoding::new(if phase.is_some() {
        "CANCEL_ACTION"
    } else {
        "CANCEL_REQUEST"
    });
    e.account(key);
    e.text(detecting);
    e.text(target);
    if let Some(p) = phase {
        e.integer(p);
    }
    e.finish()
}

#[derive(Clone)]
pub(super) struct PreparedAction {
    pub id: Hash,
    pub digest: Hash,
    pub target: String,
    pub detecting: String,
    pub request: CanonicalRequest,
    pub reason: Reason,
}

fn prepare(
    owner: &ScenarioAccount,
    stamp: &Stamp,
    kind: Kind,
    rows: Vec<(String, String, Reason)>,
) -> Vec<PreparedAction> {
    let mut actions: Vec<_> = rows
        .into_iter()
        .map(|(detecting, target, reason)| {
            let request = CanonicalRequest {
                reason,
                request_id: derived(&owner.key, &detecting, &target, None),
                detecting_event_id: detecting.clone(),
                effect_action_id: derived(&owner.key, &detecting, &target, Some(1)),
                delivery_action_id: derived(&owner.key, &detecting, &target, Some(2)),
            };
            let id = if kind == Kind::Request {
                request.request_id
            } else {
                request.effect_action_id
            };
            let mut e = Encoding::new(if kind == Kind::Request {
                "CANCEL_REQUEST"
            } else {
                "CANCEL_EFFECT"
            });
            e.0.extend_from_slice(&id);
            e.account(&owner.key);
            e.text(&detecting);
            e.text(&target);
            if kind == Kind::Effect {
                e.integer(1);
            }
            e.text(reason.name());
            e.integer(stamp.effective_at);
            e.stamp_tail(stamp);
            PreparedAction {
                id,
                digest: e.finish(),
                target,
                detecting,
                request,
                reason,
            }
        })
        .collect();
    // Unknown products are only a structural sorting sentinel; validate rejects
    // their absent target before any financial action, never inventing a product.
    actions.sort_by_key(|a| {
        (
            owner
                .orders
                .get(&a.target)
                .map(|o| o.facts.product.canonical_id()),
            a.target.clone(),
            a.id,
        )
    });
    actions
}

pub(super) fn requests(
    owner: &ScenarioAccount,
    input: &RequestInput,
) -> Result<Vec<PreparedAction>, Fault> {
    Ok(prepare(
        owner,
        &input.stamp,
        Kind::Request,
        input
            .targets
            .iter()
            .map(|(target, reason)| (input.stamp.event_id.clone(), target.clone(), *reason))
            .collect(),
    ))
}
pub(super) fn effects(
    owner: &ScenarioAccount,
    input: &EffectInput,
) -> Result<Vec<PreparedAction>, Fault> {
    Ok(prepare(
        owner,
        &input.stamp,
        Kind::Effect,
        input.effects.clone(),
    ))
}

pub(super) fn batch_digest(
    key: &AccountKey,
    stamp: &Stamp,
    kind: Kind,
    actions: &[PreparedAction],
) -> Hash {
    let mut e = Encoding::new(if kind == Kind::Request {
        "CANCEL_REQUEST_BATCH"
    } else {
        "CANCEL_EFFECT_BATCH"
    });
    e.text(&stamp.event_id);
    e.account(key);
    e.integer(stamp.effective_at);
    e.integer(actions.len() as i64);
    for action in actions {
        e.0.extend_from_slice(&action.id);
        e.0.extend_from_slice(&action.digest);
    }
    e.stamp_tail(stamp);
    e.finish()
}

pub(super) fn validate(
    owner: &ScenarioAccount,
    stamp: &Stamp,
    actions: &[PreparedAction],
    kind: Kind,
) -> Result<(), Fault> {
    let expected_historical_ordinal = match kind {
        Kind::Request => 40,
        Kind::Effect => 50,
    };
    let historical = matches!(owner.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some())
        && stamp.ordering_contract_id == "HISTORICAL_ORDER_V1"
        && stamp.source_sequence.is_some()
        && stamp.scenario_ordinal == expected_historical_ordinal;
    stamp_shape(stamp, owner.transition.reverse_group, historical)?;
    let mut ids = BTreeSet::new();
    let mut targets = BTreeSet::new();
    let mut parents = BTreeSet::new();
    if !identity(&stamp.event_id)
        || stamp.effective_at < 0
        || stamp.scenario_ordinal <= 0
        || actions.is_empty()
        || !(stamp.ordering_contract_id == "S_order_v1"
            || (owner.transition.reverse_group
                && stamp.ordering_contract_id == "S_order_v1_reverse_execution_cancel_effective")
            || historical)
        || stamp
            .causal_parent_ids
            .iter()
            .any(|p| !identity(p) || !parents.insert(p))
        || actions.iter().any(|a| {
            !identity(&a.detecting)
                || !identity(&a.target)
                || a.reason == Reason::Unsupported
                || (a.reason == Reason::SpecMigration && !owner.o03_direct_effect(stamp, actions))
                || !ids.insert(a.id)
                || !targets.insert(&a.target)
        })
    {
        return Err("INVALID_SCENARIO_GROUP");
    }
    owner.validate_context(stamp.effective_at)?;
    owner.cancel_snapshot()?;
    Ok(())
}

pub(in super::super::super) fn stamp_shape(
    stamp: &Stamp,
    reverse: bool,
    historical: bool,
) -> Result<(), Fault> {
    if !identity(&stamp.event_id)
        || stamp.effective_at < 0
        || stamp.scenario_ordinal <= 0
        || !(stamp.ordering_contract_id == "S_order_v1"
            || (reverse
                && stamp.ordering_contract_id == "S_order_v1_reverse_execution_cancel_effective")
            || historical)
        || stamp.causal_parent_ids.iter().any(|p| !identity(p))
        || stamp
            .causal_parent_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err("INVALID_SCENARIO_GROUP");
    }
    Ok(())
}
