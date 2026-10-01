//! Snapshot representation only; cross-field decisions remain in the existing store.
use super::super::wire::{decimal, fields, hash, number, optional, string, Json};
use super::*;
#[cfg(test)]
mod tests;

pub(in super::super) fn decode_request(input: &str, key: &AccountKey) -> Result<Request, Fault> {
    let value = super::super::wire::decode(input)?;
    let r = value.object(
        &[
            "schema_version",
            "account_key",
            "snapshot_id",
            "snapshot_kind",
            "capture_mode",
            "captured_at",
        ],
        &["fixture_key", "continuation_id"],
    )?;
    if r["schema_version"].text()? != "snapshot_request_v1" {
        return Err("INVALID_SCHEMA");
    }
    let request = Request {
        schema_version: "snapshot_request_v1".into(),
        account_key: r["account_key"].account()?,
        snapshot_id: r["snapshot_id"].id()?,
        kind: match r["snapshot_kind"].text()? {
            "MARKET" => Kind::Market,
            "EARN" => Kind::Earn,
            "TRADING" => Kind::Trading,
            "POSITIONS" => Kind::Positions,
            "OPEN_ORDERS" => Kind::OpenOrders,
            _ => return Err("INVALID_SCHEMA"),
        },
        mode: match r["capture_mode"].text()? {
            "OWNER_CURRENT" => Mode::OwnerCurrent,
            "FROZEN_POLL_FIXTURE" => Mode::FrozenPollFixture,
            _ => return Err("INVALID_SCHEMA"),
        },
        fixture_key: optional(r, "fixture_key").map(Json::id).transpose()?,
        captured_at: r["captured_at"].integer(true)?,
        continuation_id: optional(r, "continuation_id").map(Json::id).transpose()?,
    };
    if request.account_key != *key {
        return Err("ACCOUNT_KEY_MISMATCH");
    }
    Ok(request)
}
impl Fact {
    pub(in super::super) fn wire_json(&self) -> Result<Json, Fault> {
        let mut rows = vec![
            ("schema_version", string("snapshot_fact_v1")),
            (
                "reference",
                fields(vec![
                    ("namespace", string("SNAPSHOT")),
                    ("fact_id", string(&self.snapshot_id)),
                ]),
            ),
            ("request_digest", hash(self.request_digest)),
            ("snapshot_kind", string(self.kind.name())),
            ("snapshot_as_of", number(self.snapshot_as_of)?),
            ("immutable_payload", self.payload_json()?),
            ("payload_digest", hash(self.payload_digest)),
        ];
        if let Some(v) = self.captured_account_version {
            rows.push(("captured_account_version", number(v)?));
        }
        if let Some(v) = &self.continuation_id {
            rows.push(("continuation_id", string(v)));
        }
        Ok(fields(rows))
    }
    pub(in super::super) fn payload_json(&self) -> Result<Json, Fault> {
        let success = ("outcome", string("SUCCESS"));
        Ok(match &self.payload {
            Payload::Failure(_) => fields(vec![
                ("outcome", string("FAILURE")),
                ("reason", string("SYNTHETIC_FAILURE")),
            ]),
            Payload::Earn(v) => fields(vec![success, ("earn", decimal(*v))]),
            Payload::Trading(e, a) => fields(vec![
                success,
                ("equity", decimal(*e)),
                ("available_equity", decimal(*a)),
            ]),
            Payload::MarketGolden => fields(vec![(
                "markets",
                Json::Array(vec![fields(vec![
                    ("product_id", string("P_A")),
                    ("price", decimal(Decimal::TEN)),
                    ("contract_value", decimal(Decimal::ONE)),
                    ("lot_size", decimal(Decimal::ONE)),
                    ("minimum_size", decimal(Decimal::ONE)),
                    ("price_increment", decimal(Decimal::ONE)),
                    ("high_low_ratio", decimal(Decimal::new(1, 1))),
                    ("state", string("live")),
                    ("instrument_code", number(1)?),
                ])]),
            )]),
            Payload::Positions(rows) => fields(vec![
                success,
                (
                    "rows",
                    Json::Array(
                        rows.iter()
                            .map(|r| {
                                fields(vec![
                                    ("product_id", string(&r.product)),
                                    ("margin_mode", string("cross")),
                                    ("position_contracts", decimal(r.contracts)),
                                    ("last_price", decimal(r.mark)),
                                    ("notional_usd", decimal(r.notional)),
                                ])
                            })
                            .collect(),
                    ),
                ),
            ]),
            Payload::OpenOrders(rows) => fields(vec![
                success,
                (
                    "rows",
                    Json::Array(
                        rows.iter()
                            .map(|r| {
                                let o = &r.facts;
                                Ok(fields(vec![
                                    ("order_id", string(&o.order_id)),
                                    ("client_order_id", string(&o.client_id)),
                                    ("product_id", string(o.product.canonical_id())),
                                    (
                                        "state",
                                        string(if o.filled.is_zero() {
                                            "live"
                                        } else {
                                            "partially_filled"
                                        }),
                                    ),
                                    ("side", string(delivery::side(o.side))),
                                    (
                                        "limit_price",
                                        r.limit_price.map(decimal).unwrap_or(Json::Null),
                                    ),
                                    ("original_size_contracts", decimal(o.original)),
                                    ("cumulative_filled_size_contracts", decimal(o.filled)),
                                    ("created_at", number(r.created_at)?),
                                ]))
                            })
                            .collect::<Result<_, Fault>>()?,
                    ),
                ),
            ]),
        })
    }
}
