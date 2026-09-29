//! Closed schema conversion only; 4C owns group ordering and business outcomes.
use super::super::group::{Group, Input, Member};
use super::*;
use risk_transition::cancel::{EffectInput, Reason, RequestInput, Stamp};
#[cfg(test)]
mod section6_scale_tests;
#[cfg(test)]
mod tests;

pub(in super::super) fn decode_group(input: &str, owner: &ScenarioAccount) -> Result<Group, Fault> {
    let value = decode(input)?;
    let rows = value.object(
        &[
            "schema_version",
            "group_id",
            "account_key",
            "ordering_contract_id",
            "group_effective_at",
            "declared_member_count",
            "members",
        ],
        &[],
    )?;
    if rows["schema_version"].text()? != "scenario_group_v1" {
        return Err("INVALID_SCHEMA");
    }
    let key = rows["account_key"].account()?;
    let group = Group {
        group_id: rows["group_id"].id()?,
        account_key: key,
        ordering_contract_id: ordering(&rows["ordering_contract_id"])?,
        group_effective_at: rows["group_effective_at"].integer(false)?,
        declared_member_count: rows["declared_member_count"]
            .integer(true)?
            .try_into()
            .map_err(|_| "INVALID_SCHEMA")?,
        members: rows["members"]
            .array()?
            .iter()
            .map(|v| member(v, owner))
            .collect::<Result<_, _>>()?,
    };
    if group.account_key != owner.key {
        return Err("ACCOUNT_KEY_MISMATCH");
    }
    Ok(group)
}
fn ordering(value: &Json) -> Result<String, Fault> {
    match value.text()? {
        "S_order_v1" | "S_order_v1_reverse_execution_cancel_effective" => value.id(),
        _ => Err("INVALID_SCHEMA"),
    }
}
fn stamp(value: &Json) -> Result<Stamp, Fault> {
    let rows = value.object(
        &[
            "event_id",
            "effective_at",
            "causal_parent_ids",
            "ordering_contract_id",
            "scenario_ordinal",
        ],
        &["source_sequence"],
    )?;
    Ok(Stamp {
        event_id: rows["event_id"].id()?,
        effective_at: rows["effective_at"].integer(false)?,
        source_sequence: optional(rows, "source_sequence")
            .map(|v| v.integer(false))
            .transpose()?,
        causal_parent_ids: rows["causal_parent_ids"]
            .array()?
            .iter()
            .map(Json::id)
            .collect::<Result<_, _>>()?,
        ordering_contract_id: rows["ordering_contract_id"].id()?,
        scenario_ordinal: rows["scenario_ordinal"].integer(false)?,
    })
}
fn reason(value: &Json) -> Result<Reason, Fault> {
    Ok(match value.text()? {
        "EXPLICIT_SCENARIO" => Reason::ExplicitScenario,
        "RISK_SHORTFALL" => Reason::RiskShortfall,
        "MMR_BREACH" => Reason::MmrBreach,
        "SPEC_MIGRATION" => Reason::SpecMigration,
        "UNSUPPORTED" => Reason::Unsupported,
        _ => return Err("INVALID_SCHEMA"),
    })
}
fn member(value: &Json, owner: &ScenarioAccount) -> Result<Member, Fault> {
    let rows = value.object(&["stamp", "kind", "payload"], &[])?;
    let stamp = stamp(&rows["stamp"])?;
    let payload = &rows["payload"];
    let input = match rows["kind"].text()? {
        "INTENT" => Input::Intent(admission::wire::decode(payload, owner)?),
        "EXECUTION" => Input::Execution(execution::wire::decode(payload, owner, &stamp.event_id)?),
        "CONTEXT_MARKS" => {
            let p = payload.object(&["expected_before", "expected_after", "rows"], &[])?;
            let marks = p["rows"]
                .array()?
                .iter()
                .map(|value| {
                    let r = value.object(&["product_id", "valid_from", "valid_to", "mark"], &[])?;
                    Ok((
                        owner
                            .resolve_wire_product(&r["product_id"])?
                            .btc()
                            .cloned()
                            .map_err(|_| "INVALID_SCHEMA")?,
                        r["valid_from"].integer(false)?,
                        r["valid_to"].integer(false)?,
                        r["mark"].decimal()?,
                    ))
                })
                .collect::<Result<_, Fault>>()?;
            Input::Context(context::Input {
                account_key: owner.key.clone(),
                stamp: stamp.clone(),
                expected_before: p["expected_before"].hash()?,
                expected_after: p["expected_after"].hash()?,
                rows: context::Rows::Marks(marks),
            })
        }
        "CANCEL_REQUEST" => {
            let p = payload.object(&["targets"], &[])?;
            let targets = p["targets"]
                .array()?
                .iter()
                .map(|value| {
                    let r = value.object(&["target_order_id", "reason"], &[])?;
                    Ok((r["target_order_id"].id()?, reason(&r["reason"])?))
                })
                .collect::<Result<_, Fault>>()?;
            Input::Request(RequestInput {
                stamp: stamp.clone(),
                targets,
            })
        }
        "CANCEL_EFFECT" => {
            let p = payload.object(&["effects"], &[])?;
            let effects = p["effects"]
                .array()?
                .iter()
                .map(|value| {
                    let r =
                        value.object(&["detecting_event_id", "target_order_id", "reason"], &[])?;
                    Ok((
                        r["detecting_event_id"].id()?,
                        r["target_order_id"].id()?,
                        reason(&r["reason"])?,
                    ))
                })
                .collect::<Result<_, Fault>>()?;
            Input::Effect(EffectInput {
                stamp: stamp.clone(),
                effects,
            })
        }
        _ => return Err("INVALID_SCHEMA"),
    };
    Ok(Member { stamp, input })
}
