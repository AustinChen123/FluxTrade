//! Shared post-domain-identity source admission and atomic clock publication.
use super::*;
use risk_transition::cancel::Stamp;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Context,
    Execution,
    Intent,
    CancelRequest,
    CancelEffect,
    EventC,
}

pub(super) fn historical_ordinal(kind: Kind) -> Option<i64> {
    match kind {
        Kind::Intent => Some(60),
        Kind::CancelRequest => Some(40),
        Kind::CancelEffect => Some(50),
        Kind::Context | Kind::Execution | Kind::EventC => None,
    }
}

pub(super) fn stamp(event: &str, at: i64, ordinal: i64) -> Stamp {
    Stamp {
        event_id: event.into(),
        effective_at: at,
        source_sequence: None,
        causal_parent_ids: Vec::new(),
        ordering_contract_id: "S_order_v1".into(),
        scenario_ordinal: ordinal,
    }
}

impl ScenarioAccount {
    pub(super) fn is_terminal(&self) -> bool {
        matches!(
            self.transition.lifecycle,
            risk_transition::Lifecycle::LiquidatedFlat
                | risk_transition::Lifecycle::LiquidatedInsolvent
        )
    }
    pub(super) fn source_boundary(
        &self,
        stamp: &Stamp,
        digest: Hash,
        kind: Kind,
    ) -> Result<(), Fault> {
        if let Some((old, prior)) = self.transition.events.get(&stamp.event_id) {
            if old != stamp
                || *prior != digest
                || self.transition.event_kinds.get(&stamp.event_id) != Some(&kind)
            {
                return Err("EVENT_ID_CONFLICT");
            }
            // All other exact source matches must already have returned their
            // domain receipt. Never republish an orphaned source as a new fact.
            if kind != Kind::EventC {
                return Err("EVENT_ID_CONFLICT");
            }
        }
        if self.is_terminal() {
            return Err("RUN_TERMINAL");
        }
        if kind == Kind::EventC && self.transition.events.contains_key(&stamp.event_id) {
            return if self.gate == Gate::Running {
                Ok(())
            } else {
                Err("RUN_FAILED")
            };
        }
        let historical = stamp.ordering_contract_id == "HISTORICAL_ORDER_V1";
        let historical_member = matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some())
            && historical
            && stamp.source_sequence.is_some()
            && historical_ordinal(kind) == Some(stamp.scenario_ordinal);
        if matches!(
            kind,
            Kind::Context | Kind::CancelRequest | Kind::CancelEffect
        ) {
            risk_transition::cancel::identity::stamp_shape(
                stamp,
                self.transition.reverse_group,
                historical_member,
            )?;
        }
        if historical && !historical_member {
            return Err("INVALID_SCENARIO_GROUP");
        }
        if self.transition.accepted_stamp.as_ref().is_some_and(|old| {
            if historical && old.ordering_contract_id == "HISTORICAL_ORDER_V1" {
                (
                    stamp.effective_at,
                    stamp.source_sequence.unwrap_or(-1),
                    stamp.scenario_ordinal,
                ) <= (
                    old.effective_at,
                    old.source_sequence.unwrap_or(-1),
                    old.scenario_ordinal,
                )
            } else {
                (stamp.effective_at, stamp.scenario_ordinal)
                    <= (old.effective_at, old.scenario_ordinal)
            }
        }) {
            return Err("STALE_EVENT");
        }
        if self.gate != Gate::Running {
            return Err("RUN_FAILED");
        }
        Ok(())
    }

    pub(super) fn publish_source(
        &mut self,
        stamp: &Stamp,
        digest: Hash,
        kind: Kind,
        accepted: bool,
    ) {
        if kind == Kind::EventC && self.transition.events.contains_key(&stamp.event_id) {
            return;
        }
        self.transition
            .events
            .insert(stamp.event_id.clone(), (stamp.clone(), digest));
        self.transition
            .event_kinds
            .insert(stamp.event_id.clone(), kind);
        if accepted {
            self.transition.accepted_stamp = Some(stamp.clone());
        }
    }

    pub(super) fn source_failure(&mut self, fault: Fault) -> Fault {
        if fault != "RUN_TERMINAL" && self.gate == Gate::Running {
            self.gate = Gate::Failed(fault);
        }
        fault
    }
}
