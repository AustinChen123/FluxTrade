//! Explicit cancel financial transitions on the sole account owner.
use super::super::execution::FinancialSnapshot;
use super::super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

pub(in super::super) mod identity;
use identity::{classify, PreparedAction, Stored};
#[cfg(test)]
mod inspection_tests;

fn inspection_outcome(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Requested => "REQUESTED",
        Outcome::EffectiveCanceled => "EFFECTIVE_CANCELED",
        Outcome::EffectiveTooLate => "EFFECTIVE_TOO_LATE",
    }
}
impl Facts {
    pub(in super::super) fn encode_inspection_receipts(
        &self,
        e: &mut identity::Encoding,
    ) -> Result<(), Fault> {
        use inspection::{number, order_facts};
        for receipts in [&self.requests, &self.effects] {
            number(e, receipts.len())?;
            for (key, stored) in receipts {
                let r = &stored.value;
                e.hash(*key);
                e.hash(stored.digest);
                e.account(&r.account_key);
                e.hash(r.action_id);
                e.hash(r.payload_digest);
                for s in [&r.event_id, &r.detecting_event_id, &r.target_order_id] {
                    e.text(s);
                }
                e.text(r.reason.name());
                e.integer(r.phase);
                e.text(inspection_outcome(r.outcome));
                e.integer(r.effective_at);
                for n in [
                    r.account_version_before,
                    r.account_version_after,
                    r.order_version_before,
                    r.order_version_after,
                ] {
                    number(e, n)?;
                }
                order_facts(e, &r.before)?;
                order_facts(e, &r.after)?;
                e.text(match r.lifecycle_after {
                    super::Lifecycle::RiskStable => "RISK_STABLE",
                    super::Lifecycle::AwaitingCancelEffective => "AWAITING_CANCEL_EFFECTIVE",
                    super::Lifecycle::LiquidatedFlat => "LIQUIDATED_FLAT",
                    super::Lifecycle::LiquidatedInsolvent => "LIQUIDATED_INSOLVENT",
                });
                e.text(&r.spec_version);
                e.text(&r.rule_data_version);
                number(e, r.action_ids.len())?;
                for id in &r.action_ids {
                    e.hash(*id);
                }
            }
        }
        Ok(())
    }
    pub(in super::super) fn encode_inspection_actions(
        &self,
        e: &mut identity::Encoding,
    ) -> Result<(), Fault> {
        inspection::number(e, self.actions.len())?;
        for (id, action) in &self.actions {
            e.hash(*id);
            e.text(&action.detecting_event_id);
            e.text(&action.target_order_id);
            e.integer(action.phase);
            e.optional_text(action.outcome.map(inspection_outcome));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Reason {
    ExplicitScenario,
    RiskShortfall,
    MmrBreach,
    SpecMigration,
    Unsupported,
}

impl Reason {
    pub(in super::super) fn name(self) -> &'static str {
        match self {
            Self::ExplicitScenario => "EXPLICIT_SCENARIO",
            Self::RiskShortfall => "RISK_SHORTFALL",
            Self::MmrBreach => "MMR_BREACH",
            Self::SpecMigration => "SPEC_MIGRATION",
            Self::Unsupported => "UNSUPPORTED",
        }
    }
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
    pub reason: Reason,
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
    MigrationEffective(Hash),
}

impl State {
    fn request(&self) -> Option<&CanonicalRequest> {
        match self {
            Self::None | Self::MigrationEffective(_) => None,
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
    pub risk_after: Option<super::Decision>,
    pub lifecycle_after: super::Lifecycle,
    pub episode_after: Option<super::Episode>,
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

impl BatchResult {
    fn duplicates(stamp: &Stamp, digest: Hash, receipts: Vec<Receipt>) -> Self {
        Self {
            event_id: stamp.event_id.clone(),
            payload_digest: digest,
            account_version_before: receipts
                .iter()
                .map(|r| r.account_version_before)
                .min()
                .unwrap_or(0),
            account_version_after: receipts
                .iter()
                .map(|r| r.account_version_after)
                .max()
                .unwrap_or(0),
            receipts,
            rejected: None,
            failure: None,
            stopping_action: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in super::super) struct Facts {
    batches: BTreeMap<(Kind, String), Stored<BatchResult>>,
    requests: BTreeMap<Hash, Stored<Receipt>>,
    effects: BTreeMap<Hash, Stored<Receipt>>,
    pub actions: BTreeMap<Hash, Action>,
}

impl Facts {
    pub(in super::super) fn historical_effect_receipt(&self, action_id: &Hash) -> Option<&Receipt> {
        self.effects.get(action_id).map(|stored| &stored.value)
    }

    #[cfg(test)]
    pub(in super::super) fn assert_ambiguous_delivery_rejected(
        &self,
        event: &str,
        transport: &super::super::delivery::Transport,
    ) {
        let mut ambiguous = self.clone();
        let receipt = self.effects.values().next().unwrap().clone();
        ambiguous.effects.insert([255; 32], receipt);
        assert!(!ambiguous.delivery_match(event, transport));
        let mut wrong = transport.clone();
        wrong.client = "unknown".into();
        assert!(!self.delivery_match(event, &wrong));
    }
    pub(in super::super) fn delivery_match(
        &self,
        event: &str,
        transport: &super::super::delivery::Transport,
    ) -> bool {
        let mut matching = self
            .effects
            .values()
            .map(|s| &s.value)
            .filter(|r| r.event_id == event && transport.matches(&r.after));
        let Some(receipt) = matching.next() else {
            return false;
        };
        if matching.next().is_some() {
            return false;
        }
        self.requests.values().map(|s| &s.value).any(|r| {
            r.detecting_event_id == receipt.detecting_event_id
                && r.target_order_id == receipt.target_order_id
                && r.action_ids.contains(&receipt.action_id)
                && r.action_ids.iter().any(|id| {
                    self.actions.get(id).is_some_and(|a| {
                        a.phase == 2
                            && a.outcome.is_none()
                            && a.detecting_event_id == receipt.detecting_event_id
                            && a.target_order_id == receipt.target_order_id
                    })
                })
        })
    }
}

impl ScenarioAccount {
    pub(in super::super) fn historical_cancel_ack_transport(
        &self,
        event: &str,
    ) -> Result<super::super::delivery::Transport, Fault> {
        let mut receipts = self
            .cancel_facts
            .effects
            .values()
            .map(|stored| &stored.value)
            .filter(|receipt| receipt.event_id == event);
        let receipt = receipts.next().ok_or("INVALID_SCHEMA")?;
        if receipts.next().is_some()
            || receipt.target_order_id != receipt.after.order_id
            || self
                .orders
                .get(&receipt.target_order_id)
                .is_none_or(|order| order.facts != receipt.after)
        {
            return Err("INVALID_SCHEMA");
        }
        let transport = super::super::delivery::Transport {
            route: "WS",
            operation: "CANCEL",
            client: receipt.after.client_id.clone(),
            order: Some(receipt.target_order_id.clone()),
            code: "0".into(),
            message: None,
            product: None,
            side: None,
            price: None,
            size: None,
        };
        if !self.cancel_facts.delivery_match(event, &transport) {
            return Err("INVALID_SCHEMA");
        }
        Ok(transport)
    }
}

pub(in super::super) use super::Stage;

impl ScenarioAccount {
    fn o03_direct_effect(&self, stamp: &Stamp, actions: &[PreparedAction]) -> bool {
        self.profile == ProfileContext::P1O03
            && actions.len() == 1
            && actions.iter().all(|a| {
                a.id == a.request.effect_action_id
                    && a.reason == Reason::SpecMigration
                    && a.detecting == stamp.event_id
                    && self.orders.get(&a.target).is_some_and(|o| {
                        matches!(o.cancel, State::None)
                            && o.facts.projects_remainder("INVALID_CANCEL_ORDER") == Ok(true)
                    })
            })
    }
    pub(in super::super) fn automatic_cancel(&mut self, input: &RequestInput) -> Result<(), Fault> {
        self.automatic_cancel_checked(input, |_| Ok(()))
    }

    pub(in super::super) fn automatic_cancel_checked(
        &mut self,
        input: &RequestInput,
        mut hook: impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.prepare_automatic_cancel(input, &mut hook)
        }))
        .map_err(|_| "CANCEL_PANIC")
        .and_then(|r| r);
        match result {
            Ok(draft) => {
                *self = draft;
                Ok(())
            }
            Err(f) => Err(self.cancel_failure(f)),
        }
    }

    fn prepare_automatic_cancel(
        &self,
        input: &RequestInput,
        hook: &mut impl FnMut(Stage) -> Result<(), Fault>,
    ) -> Result<Self, Fault> {
        let actions = identity::requests(self, input)?;
        self.classify_cancel_actions(&actions, Kind::Request, false)?;
        identity::validate(self, &input.stamp, &actions, Kind::Request)?;
        let episode = self
            .transition
            .episode
            .clone()
            .ok_or("INVALID_RISK_EPISODE")?;
        let mut id = identity::Encoding::new("RISK_CANCEL_BATCH");
        id.hash(episode.id);
        id.text(episode.current_reason.name());
        id.integer(actions.len() as i64);
        for a in &actions {
            id.hash(a.id);
        }
        let id = id.finish();
        let mut digest = identity::Encoding::new("RISK_CANCEL_BATCH");
        digest.hash(id);
        digest.account(&self.key);
        digest.hash(episode.id);
        digest.text(episode.initial_reason.name());
        digest.text(episode.current_reason.name());
        digest.optional_text(episode.escalation_event.as_deref());
        digest.integer(actions.len() as i64);
        for a in &actions {
            digest.hash(a.id);
            digest.hash(a.digest);
        }
        let digest = digest.finish();
        let mut draft = self.clone();
        draft.state_version = self
            .state_version
            .checked_add(1)
            .ok_or("VERSION_OVERFLOW")?;
        let mut receipts = Vec::new();
        hook(Stage::Prepared)?;
        for (index, action) in actions.into_iter().enumerate() {
            let receipt =
                draft.apply_cancel(&action, &input.stamp, Kind::Request, self.state_version)?;
            draft.cancel_facts.requests.insert(
                action.id,
                Stored {
                    digest: action.digest,
                    value: receipt.clone(),
                },
            );
            receipts.push(receipt);
            hook(Stage::ActionDrafted(index))?;
        }
        let batch = BatchResult {
            event_id: input.stamp.event_id.clone(),
            payload_digest: digest,
            receipts,
            rejected: None,
            failure: None,
            stopping_action: None,
            account_version_before: self.state_version,
            account_version_after: draft.state_version,
        };
        draft.transition.batches.insert(
            id,
            super::AutomaticReceipt {
                id,
                digest,
                episode,
                batch,
            },
        );
        hook(Stage::BeforeSwap(0))?;
        Ok(draft)
    }
    pub(in super::super) fn request_group_digest(
        &self,
        input: &RequestInput,
    ) -> Result<Hash, Fault> {
        let actions = identity::requests(self, input)?;
        self.classify_cancel_actions(
            &actions,
            Kind::Request,
            input.stamp.ordering_contract_id == "HISTORICAL_ORDER_V1"
                && matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some()),
        )?;
        let digest = identity::batch_digest(&self.key, &input.stamp, Kind::Request, &actions);
        classify(
            self.cancel_facts
                .batches
                .get(&(Kind::Request, input.stamp.event_id.clone())),
            digest,
            "EVENT_ID_CONFLICT",
        )?;
        Ok(digest)
    }
    pub(in super::super) fn effect_group_digest(&self, input: &EffectInput) -> Result<Hash, Fault> {
        let actions = identity::effects(self, input)?;
        for action in &actions {
            classify(
                self.cancel_facts.effects.get(&action.id),
                action.digest,
                "CANCEL_ACTION_CONFLICT",
            )?;
            if let Some(request) = self
                .orders
                .get(&action.target)
                .and_then(|o| o.cancel.request())
            {
                if request.effect_action_id != action.id || request.reason != action.reason {
                    return Err("CANCEL_ACTION_CONFLICT");
                }
            }
        }
        let digest = identity::batch_digest(&self.key, &input.stamp, Kind::Effect, &actions);
        classify(
            self.cancel_facts
                .batches
                .get(&(Kind::Effect, input.stamp.event_id.clone())),
            digest,
            "EVENT_ID_CONFLICT",
        )?;
        Ok(digest)
    }
    pub(in super::super) fn migrate_order(
        &mut self,
        id: &str,
        stamp: &Stamp,
    ) -> Result<Hash, Fault> {
        let order = self.target_order(id)?.clone();
        if !order.facts.projects_remainder("INVALID_CANCEL_ORDER")? {
            return Err("INVALID_CANCEL_ORDER");
        }
        let action_id = if let State::Requested(request) = &order.cancel {
            let input = EffectInput {
                stamp: stamp.clone(),
                effects: vec![(
                    request.detecting_event_id.clone(),
                    id.into(),
                    request.reason,
                )],
            };
            let action = identity::effects(self, &input)?.remove(0);
            self.classify_cancel_actions(std::slice::from_ref(&action), Kind::Effect, false)?;
            let receipt = self.apply_cancel(
                &action,
                stamp,
                Kind::Effect,
                self.state_version
                    .checked_sub(1)
                    .ok_or("VERSION_OVERFLOW")?,
            )?;
            self.cancel_facts.effects.insert(
                action.id,
                Stored {
                    digest: action.digest,
                    value: receipt,
                },
            );
            action.id
        } else if matches!(order.cancel, State::None) {
            let input = EffectInput {
                stamp: stamp.clone(),
                effects: vec![(stamp.event_id.clone(), id.into(), Reason::SpecMigration)],
            };
            let action = identity::effects(self, &input)?.remove(0);
            let receipt = self.apply_cancel(
                &action,
                stamp,
                Kind::Effect,
                self.state_version
                    .checked_sub(1)
                    .ok_or("VERSION_OVERFLOW")?,
            )?;
            self.cancel_facts.effects.insert(
                action.id,
                Stored {
                    digest: action.digest,
                    value: receipt,
                },
            );
            action.id
        } else {
            return Err("CANCEL_ACTION_CONFLICT");
        };
        Ok(action_id)
    }
    fn cancel_snapshot(&self) -> Result<FinancialSnapshot, Fault> {
        match self.profile {
            ProfileContext::BtcEthScenario { .. } => {
                Ok(FinancialSnapshot::BtcEth(self.reservation()?))
            }
            ProfileContext::GoldenCancel(_) | ProfileContext::P1O03 => Ok(
                FinancialSnapshot::GoldenCancel(self.golden_cancel_reservation()?),
            ),
            _ => Err("UNSUPPORTED_CANCEL_PROFILE"),
        }
    }

    fn cancel_versions(&self, order: &SeedOrder, at: i64) -> Result<(String, String), Fault> {
        match &self.profile {
            ProfileContext::BtcEthScenario { scenario, .. } => {
                let (s, t) = scenario.resolve(order.product.btc()?, at)?;
                Ok((s.version.clone(), t.version.clone()))
            }
            ProfileContext::GoldenCancel(_) | ProfileContext::P1O03 => {
                Ok(("gt03-spec-v1".into(), "gt03-rule-v1".into()))
            }
            _ => Err("UNSUPPORTED_CANCEL_PROFILE"),
        }
    }

    fn cancel_failure(&mut self, fault: Fault) -> Fault {
        self.source_failure(fault)
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
            let historical_order = input.stamp.ordering_contract_id == "HISTORICAL_ORDER_V1"
                && matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some());
            let duplicates = self.classify_cancel_actions(&actions, Kind::Request, historical_order)?;
            if !duplicates.is_empty() && duplicates.iter().all(Option::is_some) {
                return Ok((
                    BatchResult::duplicates(
                        &input.stamp,
                        digest,
                        duplicates.into_iter().flatten().collect(),
                    ),
                    None,
                ));
            }
            self.source_boundary(&input.stamp, digest, source::Kind::CancelRequest)?;
            identity::validate(self, &input.stamp, &actions, Kind::Request)?;
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
                if (historical_order && historical_cancel_started(&order.cancel))
                    || !order.facts.projects_remainder("INVALID_CANCEL_ORDER")?
                {
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
            draft.publish_source(
                &input.stamp,
                digest,
                source::Kind::CancelRequest,
                batch.rejected.is_none(),
            );
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
            let duplicates = self.classify_cancel_actions(&actions, Kind::Effect, false)?;
            if !duplicates.is_empty() && duplicates.iter().all(Option::is_some) {
                let original = BatchResult::duplicates(
                    &input.stamp,
                    digest,
                    duplicates.into_iter().flatten().collect(),
                );
                return Ok((key, digest, actions, Vec::new(), Some(original)));
            }
            self.source_boundary(&input.stamp, digest, source::Kind::CancelEffect)?;
            for action in &actions {
                if self
                    .target_order(&action.target)?
                    .cancel
                    .request()
                    .is_none()
                    && !self.o03_direct_effect(&input.stamp, &actions)
                {
                    return Err("CANCEL_EFFECT_BEFORE_REQUEST");
                }
            }
            identity::validate(self, &input.stamp, &actions, Kind::Effect)?;
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
                draft.publish_source(&input.stamp, digest, source::Kind::CancelEffect, true);
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
                    if let Err(f) = self.immediate_risk_checked(&input.stamp, &mut hook) {
                        batch.failure = Some(self.cancel_failure(f));
                        batch.stopping_action = Some(action.id);
                        break;
                    }
                    if let Gate::Failed(f) = self.gate {
                        batch.failure = Some(f);
                        batch.stopping_action = Some(action.id);
                        break;
                    }
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
        historical_order: bool,
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
                let Some(order) = self.orders.get(&action.target) else {
                    return Ok(None);
                };
                match (kind, order.cancel.request()) {
                    (Kind::Request, Some(_))
                        if historical_order && historical_cancel_started(&order.cancel) =>
                    {
                        return Ok(None)
                    }
                    (Kind::Request, Some(_)) => return Err("CANCEL_ACTION_CONFLICT"),
                    (Kind::Effect, Some(request))
                        if request.effect_action_id != action.id
                            || request.reason != action.reason =>
                    {
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
        let direct_migration = kind == Kind::Effect
            && action.reason == Reason::SpecMigration
            && matches!(before_order.cancel, State::None);
        let request = if kind == Kind::Request || direct_migration {
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
            order.cancel = if direct_migration {
                State::MigrationEffective(action.id)
            } else {
                State::EffectiveCanceled(EffectFact {
                    request: request.clone(),
                    action_id: action.id,
                })
            };
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
        } else if !direct_migration {
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
        if kind == Kind::Request {
            self.transition.lifecycle = super::Lifecycle::AwaitingCancelEffective;
        }
        let risk_after = if let FinancialSnapshot::BtcEth(snapshot) = &reservation_after {
            if kind == Kind::Effect {
                Some(self.prepare_risk(stamp, snapshot)?)
            } else {
                Some(self.classify_risk(snapshot)?)
            }
        } else {
            if kind == Kind::Effect {
                self.immediate_risk_checked(stamp, |_| Ok(()))?;
            }
            None
        };
        Ok(Receipt {
            account_key: self.key.clone(),
            action_id: action.id,
            payload_digest: action.digest,
            event_id: stamp.event_id.clone(),
            detecting_event_id: action.detecting.clone(),
            target_order_id: action.target.clone(),
            reason: action.reason,
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
            risk_after,
            lifecycle_after: self.transition.lifecycle,
            episode_after: self.transition.episode.clone(),
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

fn historical_cancel_started(state: &State) -> bool {
    matches!(state, State::Requested(_) | State::EffectiveCanceled(_))
}
