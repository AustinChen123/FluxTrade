//! Current-mark gross-cap policy and read-only native projection.
use super::*;

pub(super) const POLICY_ID: &str = "CURRENT_MARK_GROSS_CAP_TWO_V1";
const PROJECTION_SCHEMA: &str = "spider_gross_admission_projection_v1";
const BOUND_SCOPE: &str = "FROZEN_MARK_LIMIT_FILL_SUBSETS_V1";

fn checked(value: Option<Decimal>) -> Result<Decimal, Fault> {
    value.ok_or("DECIMAL_OVERFLOW")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Projection {
    pub effective_equity: Decimal,
    pub gross: Decimal,
    pub net: Decimal,
    pub worst_gross: Decimal,
    pub order_loss_hold: Decimal,
    pub fee_hold: Decimal,
    pub equity_floor: Option<Decimal>,
    market_pending: bool,
}

impl Projection {
    fn from_snapshot(snapshot: reservation::Snapshot, market_pending: bool) -> Result<Self, Fault> {
        let equity_floor = if market_pending {
            None
        } else {
            Some(checked(
                checked(snapshot.equity.checked_sub(snapshot.total_order_loss))?
                    .checked_sub(snapshot.total_fee_hold),
            )?)
        };
        Ok(Self {
            effective_equity: snapshot.equity,
            gross: snapshot.gross,
            net: snapshot.net,
            worst_gross: snapshot.worst_gross,
            order_loss_hold: snapshot.total_order_loss,
            fee_hold: snapshot.total_fee_hold,
            equity_floor,
            market_pending,
        })
    }

    pub(super) fn current(owner: &ScenarioAccount) -> Result<Self, Fault> {
        let snapshot = owner.reservation()?;
        let market_pending = owner.orders.values().any(|order| {
            owner.admitted_order_type(&order.facts) == admission::OrderType::Market
                && order.facts.remaining > Decimal::ZERO
        });
        Self::from_snapshot(snapshot, market_pending)
    }

    pub(super) fn wire_json(&self, owner: &ScenarioAccount) -> Result<wire::Json, Fault> {
        use wire::{account, decimal, fields, hash, number, string, Json};
        let text = |s: &str| string(s);
        let ratio = if self.effective_equity > Decimal::ZERO {
            fields(vec![
                ("numerator", decimal(self.gross)),
                ("denominator", decimal(self.effective_equity)),
            ])
        } else {
            Json::Null
        };
        Ok(fields(vec![
            ("schema_version", text(PROJECTION_SCHEMA)),
            ("risk_policy_id", text(POLICY_ID)),
            ("bound_scope", text(BOUND_SCOPE)),
            ("account_key", account(&owner.key)),
            ("account_version", number(owner.state_version)?),
            ("valuation_context_id", hash(owner.valuation_context_id)),
            ("cap", decimal(Decimal::TWO)),
            ("effective_equity", decimal(self.effective_equity)),
            ("gross", decimal(self.gross)),
            ("net", decimal(self.net)),
            ("worst_gross", decimal(self.worst_gross)),
            ("order_loss_hold", decimal(self.order_loss_hold)),
            ("fee_hold", decimal(self.fee_hold)),
            (
                "equity_floor",
                self.equity_floor.map(decimal).unwrap_or(Json::Null),
            ),
            ("gross_ratio", ratio),
            (
                "bound_reason",
                if self.market_pending {
                    text("MARKET_ORDER_PENDING")
                } else {
                    Json::Null
                },
            ),
        ]))
    }

    pub(super) fn encode(&self, encoded: &mut risk_transition::cancel::identity::Encoding) {
        encoded.text(POLICY_ID);
        encoded.text(BOUND_SCOPE);
        encoded.text(&self.effective_equity.normalize().to_string());
        encoded.text(&self.gross.normalize().to_string());
        encoded.text(&self.net.normalize().to_string());
        encoded.text(&self.worst_gross.normalize().to_string());
        encoded.text(&self.order_loss_hold.normalize().to_string());
        encoded.text(&self.fee_hold.normalize().to_string());
        encoded.presence(self.equity_floor.is_some());
        if let Some(floor) = self.equity_floor {
            encoded.text(&floor.normalize().to_string());
        }
        encoded.presence(self.market_pending);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Decision {
    pub projection: Projection,
    pub reason: Option<Fault>,
    pub reducing_exemption: bool,
}

impl Decision {
    pub(super) fn encode(&self, encoded: &mut risk_transition::cancel::identity::Encoding) {
        encoded.text("GROSS_ADMISSION_DECISION_V1");
        self.projection.encode(encoded);
        encoded.optional_text(self.reason);
        encoded.presence(self.reducing_exemption);
    }
}

pub(super) fn reducing_capacity(
    owner: &ScenarioAccount,
    intent: &admission::OrderIntent,
) -> Result<Option<Decimal>, Fault> {
    let (product_value, side, quantity, reduce_only, _) = intent.gross_inputs();
    if !reduce_only {
        return Ok(None);
    }
    let product = product_value.btc()?;
    let Some(position) = owner.positions.btc()?.get(product) else {
        return Ok(None);
    };
    if side == position.side || quantity > position.contracts {
        return Ok(None);
    }
    let committed = committed_reducing(owner, product, position.side)?;
    let residual = checked(position.contracts.checked_sub(committed))?.max(Decimal::ZERO);
    Ok(Some(residual))
}

pub(super) fn closing_capacity(
    owner: &ScenarioAccount,
    product: &Product,
    position_side: Side,
    position_contracts: Decimal,
) -> Result<Decimal, Fault> {
    let committed = committed_reducing(owner, product, position_side)?;
    Ok(checked(position_contracts.checked_sub(committed))?.max(Decimal::ZERO))
}

pub(super) fn committed_reducing(
    owner: &ScenarioAccount,
    product: &Product,
    position_side: Side,
) -> Result<Decimal, Fault> {
    owner
        .orders
        .values()
        .filter(|order| {
            order.facts.product == ProfileProduct::BtcEth(product.clone())
                && order.facts.reduce_only
                && order.facts.side != position_side
        })
        .try_fold(Decimal::ZERO, |sum, order| {
            if order.facts.projects_remainder("NATIVE_INVARIANT")? {
                checked(sum.checked_add(order.facts.remaining))
            } else {
                Ok(sum)
            }
        })
}

pub(super) fn current_state_ready(
    owner: &ScenarioAccount,
    projection: &Projection,
    reservation: &reservation::Snapshot,
) -> Result<bool, Fault> {
    if owner.gate != Gate::Running
        || owner.transition.lifecycle != risk_transition::Lifecycle::RiskStable
    {
        return Ok(false);
    }
    if current_bound_reason(projection)?.is_some()
        || projection.effective_equity <= reservation.maintenance_margin
        || reservation.available_margin < Decimal::ZERO
    {
        return Ok(false);
    }
    Ok(true)
}

fn current_bound_reason(projection: &Projection) -> Result<Option<Fault>, Fault> {
    if projection.effective_equity <= Decimal::ZERO {
        return Ok(Some("GROSS_NON_POSITIVE_EQUITY"));
    }
    if projection.market_pending {
        return Ok(Some("GROSS_PRICE_BOUND_UNAVAILABLE"));
    }
    let Some(equity_floor) = projection.equity_floor else {
        return Ok(Some("GROSS_EQUITY_BOUND_NON_POSITIVE"));
    };
    if equity_floor <= Decimal::ZERO {
        return Ok(Some("GROSS_EQUITY_BOUND_NON_POSITIVE"));
    }
    let cap = checked(Decimal::TWO.checked_mul(equity_floor))?;
    Ok((projection.worst_gross > cap).then_some("GROSS_CAP_EXCEEDED"))
}

pub(super) fn evaluate(
    current: &ScenarioAccount,
    candidate: &ScenarioAccount,
    intent: &admission::OrderIntent,
) -> Result<Decision, Fault> {
    let (_, _, _, _, order_type) = intent.gross_inputs();
    let reducing_capacity = reducing_capacity(current, intent)?;
    let snapshot = candidate.reservation()?;
    let market_pending = order_type == admission::OrderType::Market
        || candidate.orders.values().any(|order| {
            order.facts.remaining > Decimal::ZERO
                && candidate.admitted_order_type(&order.facts) == admission::OrderType::Market
        });
    let projection = Projection::from_snapshot(snapshot, market_pending)?;
    if let Some(residual) = reducing_capacity {
        let (_, _, quantity, _, _) = intent.gross_inputs();
        let reason = (quantity > residual).then_some("REDUCE_ONLY_CAPACITY_EXCEEDED");
        return Ok(Decision {
            projection,
            reason,
            reducing_exemption: reason.is_none(),
        });
    }
    let reason = current_bound_reason(&projection)?;
    Ok(Decision {
        projection,
        reason,
        reducing_exemption: false,
    })
}

pub(super) fn project(owner: &ScenarioAccount) -> Result<wire::Json, Fault> {
    Projection::current(owner)?.wire_json(owner)
}
