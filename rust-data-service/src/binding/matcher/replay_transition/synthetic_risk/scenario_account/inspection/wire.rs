//! Encode an already detached inspection; never read or mutate the owner.
use super::super::wire::{account, decimal, fields, hash, number, string, Json};
use super::*;
#[cfg(test)]
mod tests;

impl Inspection {
    pub(in super::super) fn wire_json(&self) -> Result<Json, Fault> {
        let b = &self.basis;
        let mut rows = vec![
            ("schema_version", string("inspect_state_v1")),
            ("account_key", account(&b.account)),
            ("profile_id", string(b.profile)),
            ("config_id", string(&b.config)),
            ("account_version", number(b.version)?),
            ("valuation_context_id", hash(b.context)),
            (
                "gate",
                string(if b.gate == Gate::Running {
                    "RUNNING"
                } else {
                    "FAILED"
                }),
            ),
            ("lifecycle", string(lifecycle(b.lifecycle))),
            ("cash", decimal(b.cash)),
            ("gross_realized", decimal(b.gross)),
            ("total_fees", decimal(b.fees)),
            ("positions_digest", hash(b.positions)),
            ("orders_digest", hash(b.orders)),
            ("reservations_digest", hash(b.reservations)),
            ("owner_state_digest", hash(self.owner_state_digest)),
        ];
        if let Gate::Failed(reason) = b.gate {
            rows.push(("gate_failure", string(reason)));
        }
        Ok(fields(rows))
    }
}
