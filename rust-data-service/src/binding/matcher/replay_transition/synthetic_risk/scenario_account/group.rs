//! Complete synchronous group validation; no scheduler or second financial owner.
use super::*;
use risk_transition::cancel::{identity::Encoding, EffectInput, RequestInput, Stamp};

const REVERSE: &str = "S_order_v1_reverse_execution_cancel_effective";
const HISTORICAL: &str = "HISTORICAL_ORDER_V1";
const REVERSE_ID: &str = "reverse-execution-cancel-effective-v1";
type PreparedGroup<'a> = (Hash, Vec<(&'a Member, Hash)>, Option<Completion>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    Context(context::Input),
    Execution(execution::ExecutionCandidate),
    Intent(admission::OrderIntent),
    Request(RequestInput),
    Effect(EffectInput),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Member {
    pub stamp: Stamp,
    pub input: Input,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Group {
    pub group_id: String,
    pub account_key: AccountKey,
    pub ordering_contract_id: String,
    pub group_effective_at: i64,
    pub declared_member_count: usize,
    pub members: Vec<Member>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reference {
    Source(String),
    Liquidation(Hash),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Completion {
    pub group: Group,
    pub digest: Hash,
    pub committed: Vec<Reference>,
    pub rejections: Vec<(String, Fault)>,
    pub failure: Option<Fault>,
}

pub(super) enum Applied {
    Fresh(Completion),
    Duplicate(Completion),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Admission {
    pub digest: Hash,
    pub ordered_members: Vec<(String, Hash)>,
}

impl Member {
    fn source_kind(&self) -> source::Kind {
        match self.input {
            Input::Context(_) => source::Kind::Context,
            Input::Execution(_) => source::Kind::Execution,
            Input::Intent(_) => source::Kind::Intent,
            Input::Request(_) => source::Kind::CancelRequest,
            Input::Effect(_) => source::Kind::CancelEffect,
        }
    }
    fn encode_key(&self, encoded: &mut Encoding) {
        encoded.text(&self.stamp.event_id);
        match &self.input {
            Input::Execution(input) => {
                encoded.text("EXECUTION");
                encoded.hash(input.group_identity().2);
            }
            Input::Intent(input) => {
                encoded.text("INTENT");
                encoded.text(input.group_identity().0);
            }
            Input::Context(input) => {
                encoded.text(match input.rows {
                    context::Rows::Marks(_) => "MARK_SET",
                    context::Rows::Specs(_) => "SPEC_ACTIVATION",
                    context::Rows::Tiers(_) => "TIER_ACTIVATION",
                });
                encoded.text(&self.stamp.event_id);
            }
            Input::Request(_) => {
                encoded.text("CANCEL_REQUEST_BATCH");
                encoded.text(&self.stamp.event_id);
            }
            Input::Effect(_) => {
                encoded.text("CANCEL_EFFECT_BATCH");
                encoded.text(&self.stamp.event_id);
            }
        }
    }
    fn digest(&self, owner: &ScenarioAccount) -> Result<Hash, Fault> {
        Ok(match &self.input {
            Input::Context(input) => {
                if input.stamp != self.stamp {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                let digest = input.digest();
                if owner
                    .transition
                    .contexts
                    .get(&input.stamp.event_id)
                    .is_some_and(|receipt| receipt.digest != digest)
                {
                    return Err("EVENT_ID_CONFLICT");
                }
                digest
            }
            Input::Execution(input) => {
                input.preflight_identity(owner)?;
                if input.account_key() != &owner.key {
                    return Err("ACCOUNT_MISMATCH");
                }
                let (event, at, _, digest) = input.group_identity();
                if event != self.stamp.event_id || at != self.stamp.effective_at {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                digest
            }
            Input::Intent(input) => {
                input.preflight_identity(owner)?;
                if input.account_key() != &owner.key {
                    return Err("ACCOUNT_MISMATCH");
                }
                input.group_identity().1
            }
            Input::Request(input) => {
                if input.stamp != self.stamp {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                owner.request_group_digest(input)?
            }
            Input::Effect(input) => {
                if input.stamp != self.stamp {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                owner.effect_group_digest(input)?
            }
        })
    }
    fn ordinal(&self, reverse: bool) -> i64 {
        if self.stamp.ordering_contract_id == HISTORICAL {
            if let Some(ordinal) = historical_member_ordinal(&self.input) {
                return ordinal;
            }
        }
        match self.input {
            Input::Context(context::Input {
                rows: context::Rows::Marks(_),
                ..
            }) => 20,
            Input::Context(_) => 10,
            Input::Execution(_) => {
                if reverse {
                    50
                } else {
                    30
                }
            }
            Input::Intent(_) => 60,
            Input::Request(_) => 40,
            Input::Effect(_) => {
                if reverse {
                    30
                } else {
                    50
                }
            }
        }
    }
}

pub(super) fn historical_member_ordinal(input: &Input) -> Option<i64> {
    match input {
        Input::Intent(_) => source::historical_ordinal(source::Kind::Intent),
        Input::Request(_) => source::historical_ordinal(source::Kind::CancelRequest),
        Input::Effect(_) => source::historical_ordinal(source::Kind::CancelEffect),
        Input::Context(_) => source::historical_ordinal(source::Kind::Context),
        Input::Execution(_) => source::historical_ordinal(source::Kind::Execution),
    }
}

impl ScenarioAccount {
    pub(super) fn apply_group(&mut self, group: &Group) -> Result<Completion, Fault> {
        self.apply_group_checked(group, |_| Ok(()))
    }

    pub(super) fn apply_group_checked(
        &mut self,
        group: &Group,
        hook: impl FnMut(risk_transition::Stage) -> Result<(), Fault>,
    ) -> Result<Completion, Fault> {
        self.apply_group_observed(group, hook)
            .map(|outcome| match outcome {
                Applied::Fresh(c) | Applied::Duplicate(c) => c,
            })
    }

    pub(super) fn apply_group_observed(
        &mut self,
        group: &Group,
        mut hook: impl FnMut(risk_transition::Stage) -> Result<(), Fault>,
    ) -> Result<Applied, Fault> {
        let result = self.prepare_group(group);
        let (digest, members, duplicate) = match result {
            Ok(prepared) => prepared,
            Err(f) => return Err(self.source_failure(f)),
        };
        if let Some(original) = duplicate {
            return Ok(Applied::Duplicate(original));
        }
        let mut completion = Completion {
            group: group.clone(),
            digest,
            committed: Vec::new(),
            rejections: Vec::new(),
            failure: None,
        };
        self.transition.reverse_group = group.ordering_contract_id == REVERSE;
        self.transition.admissions.insert(
            (group.ordering_contract_id.clone(), group.group_id.clone()),
            Admission {
                digest,
                ordered_members: members
                    .iter()
                    .map(|(m, d)| (m.stamp.event_id.clone(), *d))
                    .collect(),
            },
        );
        for (member, _) in members {
            let version_before = self.state_version;
            let liquidation_before = self.liquidation_ids().count();
            let mut source_reference = member.stamp.event_id.clone();
            let result = match &member.input {
                Input::Context(input) => self
                    .activate_context_checked(input, |stage| match stage {
                        context::Stage::LiquidationPrepared(step) => {
                            hook(risk_transition::Stage::LiquidationPrepared(step))
                        }
                        context::Stage::LiquidationBeforeSwap(step) => {
                            hook(risk_transition::Stage::LiquidationBeforeSwap(step))
                        }
                        _ => Ok(()),
                    })
                    .map(|_| None),
                Input::Execution(input) => self
                    .execute_stamped(input, &member.stamp, |stage| match stage {
                        execution::commit::Stage::Risk(stage) => hook(stage),
                        _ => Ok(()),
                    })
                    .map(|reply| match reply {
                        execution::commit::Reply::Rejected(reason) => Some(reason),
                        _ => None,
                    }),
                Input::Intent(input) => {
                    self.group_admit(&member.stamp, input)
                        .map(|(reason, event)| {
                            source_reference = event;
                            reason
                        })
                }
                Input::Request(input) => self.request_cancel(input).map(|r| r.rejected),
                Input::Effect(input) => self
                    .effect_cancel_checked(input, &mut hook)
                    .and_then(|r| r.failure.map_or(Ok(r.rejected), Err)),
            };
            match result {
                Ok(None) => {
                    completion
                        .committed
                        .push(Reference::Source(source_reference));
                }
                Ok(Some(reason)) => completion
                    .rejections
                    .push((member.stamp.event_id.clone(), reason)),
                Err(f) => {
                    if f == "RUN_TERMINAL" {
                        completion
                            .rejections
                            .push((member.stamp.event_id.clone(), f));
                        continue;
                    }
                    if self.state_version != version_before {
                        completion
                            .committed
                            .push(Reference::Source(member.stamp.event_id.clone()));
                    }
                    completion.failure = Some(f);
                }
            }
            completion.committed.extend(
                self.liquidation_ids()
                    .skip(liquidation_before)
                    .map(Reference::Liquidation),
            );
            if completion.failure.is_some() {
                break;
            }
            if let Gate::Failed(f) = self.gate {
                completion.failure = Some(f);
                break;
            }
        }
        self.transition.reverse_group = false;
        self.transition.groups.insert(
            (group.ordering_contract_id.clone(), group.group_id.clone()),
            completion.clone(),
        );
        Ok(Applied::Fresh(completion))
    }

    fn prepare_group<'a>(&self, group: &'a Group) -> Result<PreparedGroup<'a>, Fault> {
        let mut members: Vec<_> = group.members.iter().collect();
        members.sort_by_key(|m| m.stamp.scenario_ordinal);
        let mut encoded = Encoding::new("SCENARIO_GROUP");
        encoded.text(&group.ordering_contract_id);
        encoded.text(&group.group_id);
        encoded.account(&group.account_key);
        encoded.integer(group.group_effective_at);
        encoded.integer(members.len() as i64);
        for member in &members {
            member.encode_key(&mut encoded);
        }
        encoded.integer(members.len() as i64);
        let mut prepared = Vec::new();
        for member in &members {
            let digest = member.digest(self)?;
            encoded.hash(digest);
            prepared.push((*member, digest));
        }
        let mut edges: Vec<_> = members
            .iter()
            .flat_map(|m| {
                m.stamp
                    .causal_parent_ids
                    .iter()
                    .map(move |parent| (parent.as_str(), m.stamp.event_id.as_str()))
            })
            .collect();
        edges.sort();
        encoded.integer(edges.len() as i64);
        for (parent, child) in edges {
            encoded.text(parent);
            encoded.text(child);
        }
        encoded.integer(members.len() as i64);
        for member in &members {
            encoded.optional_integer(member.stamp.source_sequence);
        }
        encoded.integer(members.len() as i64);
        for member in &members {
            encoded.integer(member.stamp.scenario_ordinal);
        }
        let digest = encoded.finish();
        if let Some(old) = self
            .transition
            .groups
            .get(&(group.ordering_contract_id.clone(), group.group_id.clone()))
        {
            return if old.digest == digest {
                Ok((digest, prepared, Some(old.clone())))
            } else {
                Err("EVENT_ID_CONFLICT")
            };
        }
        for (member, digest) in &prepared {
            if let Some((old_stamp, old)) = self.transition.events.get(&member.stamp.event_id) {
                if old != digest
                    || old_stamp != &member.stamp
                    || self.transition.event_kinds.get(&member.stamp.event_id)
                        != Some(&member.source_kind())
                {
                    return Err("EVENT_ID_CONFLICT");
                }
            }
        }
        if self.is_terminal() {
            return Err("RUN_TERMINAL");
        }
        let reverse = group.ordering_contract_id == REVERSE;
        let historical = group.ordering_contract_id == HISTORICAL;
        let configured_owner = matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some());
        if group.account_key != self.key
            || group.members.is_empty()
            || group.declared_member_count != members.len()
            || !identity(&group.group_id)
            || (reverse && group.group_id != REVERSE_ID)
            || (!reverse
                && (group.group_id == REVERSE_ID
                    || !(group.ordering_contract_id == "S_order_v1"
                        || (historical && configured_owner))))
            || (historical && (!configured_owner || members.len() != 1))
        {
            return Err("INVALID_SCENARIO_GROUP");
        }
        let mut ids = BTreeMap::new();
        let mut ordinals = BTreeSet::new();
        let mut sequences = BTreeSet::new();
        for (member, _) in &prepared {
            let stamp = &member.stamp;
            if !ordinals.insert(stamp.scenario_ordinal) {
                return Err("DUPLICATE_SCENARIO_ORDINAL");
            }
            if stamp.effective_at != group.group_effective_at
                || stamp.effective_at < 0
                || stamp.ordering_contract_id != group.ordering_contract_id
                || stamp.scenario_ordinal != member.ordinal(reverse)
                || (historical
                    && (stamp.source_sequence.is_none()
                        || historical_member_ordinal(&member.input)
                            != Some(stamp.scenario_ordinal)))
                || !identity(&stamp.event_id)
                || ids.insert(stamp.event_id.clone(), stamp).is_some()
                || stamp.source_sequence.is_some_and(|s| !sequences.insert(s))
            {
                return Err("INVALID_SCENARIO_GROUP");
            }
        }
        let mut last = self.transition.accepted_stamp.as_ref();
        let mut prior_sequence = None;
        let mut context_validator = self.clone();
        context_validator.transition.reverse_group = reverse;
        for (member, _) in &prepared {
            let stamp = &member.stamp;
            if let Some(sequence) = stamp.source_sequence {
                if prior_sequence.is_some_and(|previous| previous >= sequence) {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                prior_sequence = Some(sequence);
            }
            if stamp
                .causal_parent_ids
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            {
                return Err("INVALID_SCENARIO_GROUP");
            }
            let mut parents = BTreeSet::new();
            for parent in &stamp.causal_parent_ids {
                if !parents.insert(parent) {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                let p = ids
                    .get(parent)
                    .copied()
                    .or_else(|| self.transition.events.get(parent).map(|(s, _)| s))
                    .ok_or("INVALID_SCENARIO_GROUP")?;
                if (if historical {
                    (
                        p.effective_at,
                        p.source_sequence.unwrap_or(-1),
                        p.scenario_ordinal,
                    ) >= (
                        stamp.effective_at,
                        stamp.source_sequence.unwrap_or(-1),
                        stamp.scenario_ordinal,
                    )
                } else {
                    (p.effective_at, p.scenario_ordinal)
                        >= (stamp.effective_at, stamp.scenario_ordinal)
                }) || matches!((p.source_sequence, stamp.source_sequence), (Some(a), Some(b)) if a >= b)
                {
                    return Err("INVALID_SCENARIO_GROUP");
                }
            }
            if !self.transition.events.contains_key(&stamp.event_id) {
                if !historical
                    && last.is_some_and(|p| {
                        p.effective_at == stamp.effective_at
                            && matches!((p.source_sequence, stamp.source_sequence), (Some(a), Some(b)) if a >= b)
                    })
                {
                    return Err("INVALID_SCENARIO_GROUP");
                }
                if last.is_some_and(|p| {
                    if historical {
                        (
                            p.effective_at,
                            p.source_sequence.unwrap_or(-1),
                            p.scenario_ordinal,
                        ) >= (
                            stamp.effective_at,
                            stamp.source_sequence.unwrap_or(-1),
                            stamp.scenario_ordinal,
                        )
                    } else {
                        (p.effective_at, p.scenario_ordinal)
                            >= (stamp.effective_at, stamp.scenario_ordinal)
                    }
                }) {
                    return Err("STALE_EVENT");
                }
                last = Some(stamp);
            }
            if let Input::Context(input) = &member.input {
                if !self.is_terminal() {
                    if let (_, Some(draft)) = context_validator.prepare_context(input)? {
                        context_validator = draft;
                    }
                }
            }
        }
        if self.gate != Gate::Running {
            return Err("RUN_FAILED");
        }
        Ok((digest, prepared, None))
    }
}
