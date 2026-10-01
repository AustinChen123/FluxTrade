//! Detached group evidence only; no retention or duplicate handling.
use super::super::group::{Completion, Reference};
use super::*;
use risk_transition::{cancel::identity::Encoding, Lifecycle};
#[cfg(test)]
pub(in super::super) mod tests;
#[cfg(test)]
mod vectors;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct GroupResult {
    classification: &'static str,
    group_id: String,
    group_digest: Option<Hash>,
    references: Vec<Reference>,
    rejections: Vec<(String, Fault)>,
    failure: Option<Fault>,
    before: i64,
    after: i64,
    gate: Gate,
    lifecycle: Lifecycle,
    owner_digest: Hash,
}
fn hex(hash: Hash) -> String {
    hash.iter().fold(String::with_capacity(64), |mut out, b| {
        write!(out, "{b:02x}").expect("String write");
        out
    })
}
fn reference(value: &Reference) -> (&'static str, String) {
    match value {
        Reference::Source(id) => ("SOURCE", id.clone()),
        Reference::Liquidation(id) => ("LIQUIDATION", hex(*id)),
    }
}
impl GroupResult {
    pub(in super::super) fn from_outcome(
        owner: &ScenarioAccount,
        requested_group_id: &str,
        version_before: u64,
        outcome: Result<&Completion, Fault>,
    ) -> Result<Self, Fault> {
        let (classification, group_id, group_digest, references, rejections, failure) =
            match outcome {
                Ok(c) => (
                    if c.failure.is_some() {
                        "FAULT"
                    } else if !c.rejections.is_empty() {
                        "REJECTED"
                    } else {
                        "COMMITTED"
                    },
                    c.group.group_id.clone(),
                    Some(c.digest),
                    c.committed.clone(),
                    c.rejections.clone(),
                    c.failure,
                ),
                Err(fault) => (
                    if fault == "RUN_TERMINAL" {
                        "REJECTED"
                    } else {
                        "FAULT"
                    },
                    requested_group_id.into(),
                    None,
                    Vec::new(),
                    Vec::new(),
                    Some(fault),
                ),
            };
        Ok(Self {
            classification,
            group_id,
            group_digest,
            references,
            rejections,
            failure,
            before: version_before.try_into().map_err(|_| "NATIVE_INVARIANT")?,
            after: owner
                .state_version
                .try_into()
                .map_err(|_| "NATIVE_INVARIANT")?,
            gate: owner.gate.clone(),
            lifecycle: owner.transition.lifecycle,
            owner_digest: owner.owner_evidence_digest()?,
        })
    }
    fn gate_fields(&self) -> (&'static str, Option<Fault>) {
        match self.gate {
            Gate::Running => ("RUNNING", None),
            Gate::Failed(f) => ("FAILED", Some(f)),
        }
    }
    fn encode(&self) -> Result<Encoding, Fault> {
        let mut e = Encoding::new("SCENARIO_GROUP_RESULT_V1");
        e.text("group_result_v1");
        e.text(self.classification);
        e.text(&self.group_id);
        e.presence(self.group_digest.is_some());
        if let Some(hash) = self.group_digest {
            e.hash(hash);
        }
        inspection::number(&mut e, self.references.len())?;
        for item in &self.references {
            let (namespace, id) = reference(item);
            e.text(namespace);
            e.text(&id);
        }
        inspection::number(&mut e, self.rejections.len())?;
        for (event, reason) in &self.rejections {
            e.text(event);
            e.text(reason);
        }
        e.optional_text(self.failure);
        e.integer(self.before);
        e.integer(self.after);
        let (gate, failure) = self.gate_fields();
        e.text(gate);
        e.optional_text(failure);
        e.text(inspection::lifecycle(self.lifecycle));
        e.hash(self.owner_digest);
        Ok(e)
    }
    pub(in super::super) fn canonical(&self) -> Result<String, Fault> {
        let text = |s: &str| Json::Text(s.into());
        let object = |rows: Vec<(&str, Json)>| {
            Json::Object(rows.into_iter().map(|(k, v)| (k.into(), v)).collect())
        };
        let references = self
            .references
            .iter()
            .map(|r| {
                let (namespace, id) = reference(r);
                object(vec![("namespace", text(namespace)), ("fact_id", text(&id))])
            })
            .collect();
        let rejections = self
            .rejections
            .iter()
            .map(|(id, reason)| object(vec![("event_id", text(id)), ("reason", text(reason))]))
            .collect();
        let (gate, gate_failure) = self.gate_fields();
        let mut rows = vec![
            ("schema_version", text("group_result_v1")),
            ("classification", text(self.classification)),
            ("group_id", text(&self.group_id)),
            ("committed_references", Json::Array(references)),
            ("rejections", Json::Array(rejections)),
            (
                "account_version_before",
                Json::Number(self.before.to_string()),
            ),
            (
                "account_version_after",
                Json::Number(self.after.to_string()),
            ),
            ("gate_after", text(gate)),
            (
                "lifecycle_after",
                text(inspection::lifecycle(self.lifecycle)),
            ),
            ("owner_state_digest", text(&hex(self.owner_digest))),
            ("result_digest", text(&hex(self.encode()?.finish()))),
        ];
        if let Some(hash) = self.group_digest {
            rows.push(("group_digest", text(&hex(hash))));
        }
        if let Some(failure) = self.failure {
            rows.push(("failure", text(failure)));
        }
        if let Some(failure) = gate_failure {
            rows.push(("gate_failure", text(failure)));
        }
        object(rows).canonical()
    }
}
