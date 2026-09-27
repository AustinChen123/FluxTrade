//! Explicit cancel financial transitions on the sole account owner.
use super::super::execution::FinancialSnapshot;
use super::super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

mod identity;
use identity::{classify, PreparedAction, Stored};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Reason {
    ExplicitScenario,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Stamp {
    pub event_id: String,
    pub effective_at: i64,
    pub source_sequence: Option<i64>,
    pub causal_parent_ids: Vec<String>,
    pub ordering_contract_id: String,
    pub scenario_ordinal: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct RequestInput {
    pub stamp: Stamp,
    pub targets: Vec<(String, Reason)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct EffectInput {
    pub stamp: Stamp,
    pub effects: Vec<(String, String, Reason)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct CanonicalRequest {
    pub request_id: Hash,
    pub detecting_event_id: String,
    pub effect_action_id: Hash,
    pub delivery_action_id: Hash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct EffectFact {
    pub request: CanonicalRequest,
    pub action_id: Hash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) enum State {
    None,
    Requested(CanonicalRequest),
    EffectiveCanceled(EffectFact),
    EffectiveTooLate(EffectFact),
}

impl State {
    fn request(&self) -> Option<&CanonicalRequest> {
        match self {
            Self::None => None,
            Self::Requested(r) => Some(r),
            Self::EffectiveCanceled(f) | Self::EffectiveTooLate(f) => Some(&f.request),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Request,
    Effect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Outcome {
    Requested,
    EffectiveCanceled,
    EffectiveTooLate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Action {
    pub detecting_event_id: String,
    pub target_order_id: String,
    pub phase: i64,
    pub outcome: Option<Outcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Receipt {
    pub account_key: AccountKey,
    pub action_id: Hash,
    pub payload_digest: Hash,
    pub event_id: String,
    pub detecting_event_id: String,
    pub target_order_id: String,
    pub reason: Reason,
    pub phase: i64,
    pub outcome: Outcome,
    pub effective_at: i64,
    pub account_version_before: u64,
    pub account_version_after: u64,
    pub order_version_before: u64,
    pub order_version_after: u64,
    pub before: SeedOrder,
    pub after: SeedOrder,
    pub reservation_before: FinancialSnapshot,
    pub reservation_after: FinancialSnapshot,
    pub spec_version: String,
    pub rule_data_version: String,
    pub action_ids: Vec<Hash>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct BatchResult {
    pub event_id: String,
    pub payload_digest: Hash,
    pub receipts: Vec<Receipt>,
    pub rejected: Option<Fault>,
    pub failure: Option<Fault>,
    pub stopping_action: Option<Hash>,
    pub account_version_before: u64,
    pub account_version_after: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in super::super) struct Facts {
    batches: BTreeMap<(Kind, String), Stored<BatchResult>>,
    requests: BTreeMap<Hash, Stored<Receipt>>,
    effects: BTreeMap<Hash, Stored<Receipt>>,
    pub actions: BTreeMap<Hash, Action>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Stage {
    Prepared,
    ActionDrafted(usize),
    BeforeSwap(usize),
}

impl ScenarioAccount {
    fn cancel_snapshot(&self) -> Result<FinancialSnapshot, Fault> {
        match self.profile {
            ProfileContext::BtcEthScenario { .. } => {
                Ok(FinancialSnapshot::BtcEth(self.reservation()?))
            }
            ProfileContext::GoldenCancel(_) => Ok(FinancialSnapshot::GoldenCancel(
                self.golden_cancel_reservation()?,
            )),
            _ => Err("UNSUPPORTED_CANCEL_PROFILE"),
        }
    }

    fn cancel_versions(&self, order: &SeedOrder, at: i64) -> Result<(String, String), Fault> {
        match &self.profile {
            ProfileContext::BtcEthScenario { scenario, .. } => {
                let (s, t) = scenario.resolve(order.product.btc()?, at)?;
                Ok((s.version.clone(), t.version.clone()))
            }
            ProfileContext::GoldenCancel(_) => Ok(("gt03-spec-v1".into(), "gt03-rule-v1".into())),
            _ => Err("UNSUPPORTED_CANCEL_PROFILE"),
        }
    }

    fn cancel_failure(&mut self, fault: Fault) -> Fault {
        if self.gate == Gate::Running {
            self.gate = Gate::Failed(fault);
        }
        fault
    }

    pub(in super::super) fn request_cancel(
        &mut self,
        input: &RequestInput,
    ) -> Result<BatchResult, Fault> {
        self.request_cancel_checked(input, |_| Ok(()))
    }

    pub(in super::super) fn request_cancel_checked(
        &mut self,
        input: &RequestInput,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<BatchResult, Fault> {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let actions = identity::requests(self, input)?;
            let digest = identity::batch_digest(&self.key, &input.stamp, Kind::Request, &actions);
            let key = (Kind::Request, input.stamp.event_id.clone());
            if let Some(original) = classify(
                self.cancel_facts.batches.get(&key),
                digest,
                "EVENT_ID_CONFLICT",
            )? {
                return Ok((original.clone(), None));
            }
            let duplicates = self.classify_cancel_actions(&actions, Kind::Request)?;
            identity::validate(self, &input.stamp, &actions)?;
            if self.gate != Gate::Running {
                return Err("RUN_FAILED");
            }
            let mut batch = BatchResult {
                event_id: input.stamp.event_id.clone(),
                payload_digest: digest,
                receipts: Vec::new(),
                rejected: None,
                failure: None,
                stopping_action: None,
                account_version_before: self.state_version,
                account_version_after: self.state_version,
            };
            for (action, duplicate) in actions.iter().zip(&duplicates) {
                if duplicate.is_some() {
                    continue;
                }
                let order = self.target_order(&action.target)?;
                if !order.facts.projects_remainder("INVALID_CANCEL_ORDER")? {
                    batch.rejected = Some("CANCEL_REQUEST_TOO_LATE");
                }
            }
            let mut draft = self.clone();
            hook(Stage::Prepared)?;
            if batch.rejected.is_none() {
                if duplicates.iter().any(Option::is_none) {
                    draft.state_version = draft
                        .state_version
                        .checked_add(1)
                        .ok_or("VERSION_OVERFLOW")?;
                }
                for (index, (action, duplicate)) in actions.iter().zip(duplicates).enumerate() {
                    if let Some(receipt) = duplicate {
                        batch.receipts.push(receipt);
                        continue;
                    }
                    let receipt = draft.apply_cancel(
                        action,
                        &input.stamp,
                        Kind::Request,
                        self.state_version,
                    )?;
                    draft.cancel_facts.requests.insert(
                        action.id,
                        Stored {
                            digest: action.digest,
                            value: receipt.clone(),
                        },
                    );
                    batch.receipts.push(receipt);
                    hook(Stage::ActionDrafted(index))?;
                }
            }
            batch.account_version_after = draft.state_version;
            draft.cancel_facts.batches.insert(
                key,
                Stored {
                    digest,
                    value: batch.clone(),
                },
            );
            hook(Stage::BeforeSwap(0))?;
            Ok((batch, Some(draft)))
        }))
        .map_err(|_| "CANCEL_PANIC")
        .and_then(|r| r);
        match result {
            Ok((batch, draft)) => {
                if let Some(draft) = draft {
                    *self = draft;
                }
                Ok(batch)
            }
            Err(f) => Err(self.cancel_failure(f)),
        }
    }

    pub(in super::super) fn effect_cancel(
        &mut self,
        input: &EffectInput,
    ) -> Result<BatchResult, Fault> {
        self.effect_cancel_checked(input, |_| Ok(()))
    }

    pub(in super::super) fn effect_cancel_checked(
        &mut self,
        input: &EffectInput,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<BatchResult, Fault> {
        let prepared = catch_unwind(AssertUnwindSafe(|| {
            let actions = identity::effects(self, input)?;
            let digest = identity::batch_digest(&self.key, &input.stamp, Kind::Effect, &actions);
            let key = (Kind::Effect, input.stamp.event_id.clone());
            if let Some(original) = classify(
                self.cancel_facts.batches.get(&key),
                digest,
                "EVENT_ID_CONFLICT",
            )? {
                return Ok((key, digest, actions, Vec::new(), Some(original.clone())));
            }
            let duplicates = self.classify_cancel_actions(&actions, Kind::Effect)?;
            identity::validate(self, &input.stamp, &actions)?;
            if duplicates.iter().any(Option::is_none) && self.gate != Gate::Running {
                return Err("RUN_FAILED");
            }
            hook(Stage::Prepared)?;
            Ok((key, digest, actions, duplicates, None))
        }))
        .map_err(|_| "CANCEL_PANIC")
        .and_then(|r| r);
        let (key, digest, actions, duplicates, original) = match prepared {
            Ok(p) => p,
            Err(f) => return Err(self.cancel_failure(f)),
        };
        if let Some(original) = original {
            return Ok(original);
        }
        let mut batch = BatchResult {
            event_id: input.stamp.event_id.clone(),
            payload_digest: digest,
            receipts: Vec::new(),
            rejected: None,
            failure: None,
            stopping_action: None,
            account_version_before: self.state_version,
            account_version_after: self.state_version,
        };
        for (index, (action, duplicate)) in actions.iter().zip(duplicates).enumerate() {
            if let Some(receipt) = duplicate {
                batch.receipts.push(receipt);
                continue;
            }
            let prepared = catch_unwind(AssertUnwindSafe(|| {
                let mut draft = self.clone();
                draft.state_version = draft
                    .state_version
                    .checked_add(1)
                    .ok_or("VERSION_OVERFLOW")?;
                let receipt =
                    draft.apply_cancel(action, &input.stamp, Kind::Effect, self.state_version)?;
                draft.cancel_facts.effects.insert(
                    action.id,
                    Stored {
                        digest: action.digest,
                        value: receipt.clone(),
                    },
                );
                hook(Stage::ActionDrafted(index))?;
                hook(Stage::BeforeSwap(index))?;
                Ok((draft, receipt))
            }))
            .map_err(|_| "CANCEL_PANIC")
            .and_then(|r: Result<_, Fault>| r);
            match prepared {
                Ok((draft, receipt)) => {
                    *self = draft;
                    batch.receipts.push(receipt);
                }
                Err(f) => {
                    batch.failure = Some(self.cancel_failure(f));
                    batch.stopping_action = Some(action.id);
                    break;
                }
            }
        }
        batch.account_version_after = self.state_version;
        self.cancel_facts.batches.insert(
            key,
            Stored {
                digest,
                value: batch.clone(),
            },
        );
        Ok(batch)
    }

    fn classify_cancel_actions(
        &self,
        actions: &[PreparedAction],
        kind: Kind,
    ) -> Result<Vec<Option<Receipt>>, Fault> {
        actions
            .iter()
            .map(|action| {
                let store = if kind == Kind::Request {
                    &self.cancel_facts.requests
                } else {
                    &self.cancel_facts.effects
                };
                if let Some(receipt) = classify(
                    store.get(&action.id),
                    action.digest,
                    "CANCEL_ACTION_CONFLICT",
                )? {
                    return Ok(Some(receipt.clone()));
                }
                let order = self.target_order(&action.target)?;
                match (kind, order.cancel.request()) {
                    (Kind::Request, Some(_)) => return Err("CANCEL_ACTION_CONFLICT"),
                    (Kind::Effect, None) => return Err("CANCEL_EFFECT_BEFORE_REQUEST"),
                    (Kind::Effect, Some(request)) if request.effect_action_id != action.id => {
                        return Err("CANCEL_ACTION_CONFLICT")
                    }
                    _ => {}
                }
                Ok(None)
            })
            .collect()
    }

    fn apply_cancel(
        &mut self,
        action: &PreparedAction,
        stamp: &Stamp,
        kind: Kind,
        before_version: u64,
    ) -> Result<Receipt, Fault> {
        let reservation_before = self.cancel_snapshot()?;
        let before_order = self.target_order(&action.target)?.clone();
        before_order
            .facts
            .projects_remainder("INVALID_CANCEL_ORDER")?;
        let (spec_version, rule_data_version) =
            self.cancel_versions(&before_order.facts, stamp.effective_at)?;
        let request = if kind == Kind::Request {
            action.request.clone()
        } else {
            before_order
                .cancel
                .request()
                .ok_or("CANCEL_EFFECT_BEFORE_REQUEST")?
                .clone()
        };
        let order = self.orders.get_mut(&action.target).ok_or("UNKNOWN_ORDER")?;
        order.version = order.version.checked_add(1).ok_or("VERSION_OVERFLOW")?;
        let outcome = if kind == Kind::Request {
            order.cancel = State::Requested(request.clone());
            Outcome::Requested
        } else if order.facts.remaining > Decimal::ZERO {
            order.facts.canceled = add(order.facts.canceled, order.facts.remaining)?;
            order.facts.remaining = Decimal::ZERO;
            order.facts.status = "CANCELED".into();
            order.cancel = State::EffectiveCanceled(EffectFact {
                request: request.clone(),
                action_id: action.id,
            });
            Outcome::EffectiveCanceled
        } else {
            if order.facts.status != "FILLED" {
                return Err("INVALID_CANCEL_ORDER");
            }
            order.cancel = State::EffectiveTooLate(EffectFact {
                request: request.clone(),
                action_id: action.id,
            });
            Outcome::EffectiveTooLate
        };
        order.facts.projects_remainder("INVALID_CANCEL_ORDER")?;
        let after_order = order.clone();
        if kind == Kind::Request {
            for (id, phase) in [
                (request.effect_action_id, 1),
                (request.delivery_action_id, 2),
            ] {
                if self
                    .cancel_facts
                    .actions
                    .insert(
                        id,
                        Action {
                            detecting_event_id: action.detecting.clone(),
                            target_order_id: action.target.clone(),
                            phase,
                            outcome: None,
                        },
                    )
                    .is_some()
                {
                    return Err("CANCEL_ACTION_CONFLICT");
                }
            }
        } else {
            let pending = self
                .cancel_facts
                .actions
                .get_mut(&action.id)
                .ok_or("CANCEL_EFFECT_BEFORE_REQUEST")?;
            if pending.outcome.is_some() {
                return Err("CANCEL_ACTION_CONFLICT");
            }
            pending.outcome = Some(outcome);
        }
        let reservation_after = self.cancel_snapshot()?;
        Ok(Receipt {
            account_key: self.key.clone(),
            action_id: action.id,
            payload_digest: action.digest,
            event_id: stamp.event_id.clone(),
            detecting_event_id: action.detecting.clone(),
            target_order_id: action.target.clone(),
            reason: Reason::ExplicitScenario,
            phase: if kind == Kind::Request { 0 } else { 1 },
            outcome,
            effective_at: stamp.effective_at,
            account_version_before: before_version,
            account_version_after: self.state_version,
            order_version_before: before_order.version,
            order_version_after: after_order.version,
            before: before_order.facts,
            after: after_order.facts,
            reservation_before,
            reservation_after,
            spec_version,
            rule_data_version,
            action_ids: if kind == Kind::Request {
                vec![request.effect_action_id, request.delivery_action_id]
            } else {
                vec![request.effect_action_id]
            },
        })
    }
}
