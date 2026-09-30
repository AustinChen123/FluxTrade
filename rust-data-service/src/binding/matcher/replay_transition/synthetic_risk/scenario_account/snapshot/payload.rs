use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PositionRow {
    pub product: String,
    pub contracts: Decimal,
    pub mark: Decimal,
    pub notional: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrderRow {
    pub facts: SeedOrder,
    pub created_at: i64,
    pub limit_price: Option<Decimal>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Payload {
    MarketGolden,
    Earn(Decimal),
    Trading(Decimal, Decimal),
    Positions(Vec<PositionRow>),
    OpenOrders(Vec<OrderRow>),
    Failure(Kind),
}
fn decimal(e: &mut Encoding, value: Decimal) {
    e.text(&value.normalize().to_string());
}
fn count(e: &mut Encoding, n: usize) -> Result<(), Fault> {
    e.integer(i64::try_from(n).map_err(|_| "NATIVE_INVARIANT")?);
    Ok(())
}
impl Payload {
    pub(super) fn kind(&self) -> Kind {
        match self {
            Self::MarketGolden => Kind::Market,
            Self::Earn(_) => Kind::Earn,
            Self::Trading(..) => Kind::Trading,
            Self::Positions(_) => Kind::Positions,
            Self::OpenOrders(_) => Kind::OpenOrders,
            Self::Failure(k) => *k,
        }
    }
    pub(super) fn encode(&self, e: &mut Encoding) -> Result<(), Fault> {
        e.text(self.kind().payload_name());
        if matches!(self, Self::Failure(_)) {
            e.text("FAILURE");
            e.text("SYNTHETIC_FAILURE");
            return Ok(());
        }
        if !matches!(self, Self::MarketGolden) {
            e.text("SUCCESS");
        }
        match self {
            Self::MarketGolden => {
                e.integer(1);
                e.text("P_A");
                for v in [
                    Decimal::TEN,
                    Decimal::ONE,
                    Decimal::ONE,
                    Decimal::ONE,
                    Decimal::ONE,
                    Decimal::new(1, 1),
                ] {
                    decimal(e, v);
                }
                e.text("live");
                e.integer(1);
            }
            Self::Earn(v) => decimal(e, *v),
            Self::Trading(equity, available) => {
                decimal(e, *equity);
                decimal(e, *available);
            }
            Self::Positions(rows) => {
                count(e, rows.len())?;
                for r in rows {
                    e.text(&r.product);
                    e.text("cross");
                    decimal(e, r.contracts);
                    e.optional_text(Some(&r.mark.normalize().to_string()));
                    e.optional_text(Some(&r.notional.normalize().to_string()));
                }
            }
            Self::OpenOrders(rows) => {
                count(e, rows.len())?;
                for r in rows {
                    let o = &r.facts;
                    for s in [
                        o.order_id.as_str(),
                        o.client_id.as_str(),
                        o.product.canonical_id(),
                        if o.filled.is_zero() {
                            "live"
                        } else {
                            "partially_filled"
                        },
                        if o.side == Side::Long { "buy" } else { "sell" },
                    ] {
                        e.text(s);
                    }
                    if let Some(limit_price) = r.limit_price {
                        decimal(e, limit_price);
                    } else {
                        e.text("MARKET");
                    }
                    for v in [o.original, o.filled] {
                        decimal(e, v);
                    }
                    e.integer(r.created_at);
                }
            }
            Self::Failure(_) => unreachable!(),
        }
        Ok(())
    }
}
pub(super) fn fixture(key: &str) -> Result<(Payload, i64), Fault> {
    use Payload::*;
    Ok(match key {
        "POLL_Q1_S1" => (Trading(Decimal::from(100), Decimal::from(100)), 100),
        "POLL_Q2_S2" => (Trading(Decimal::from(110), Decimal::from(110)), 110),
        "POLL_EARN_ZERO" => (Earn(Decimal::ZERO), 90),
        "POLL_POSITIONS_EMPTY" => (Positions(vec![]), 100),
        "POLL_OPEN_ORDERS_EMPTY" => (OpenOrders(vec![]), 100),
        "POLL_EARN_FAILURE" => (Failure(Kind::Earn), 90),
        "POLL_TRADING_FAILURE" => (Failure(Kind::Trading), 100),
        "POLL_POSITIONS_FAILURE" => (Failure(Kind::Positions), 100),
        "POLL_OPEN_ORDERS_FAILURE" => (Failure(Kind::OpenOrders), 100),
        "MARKET_GOLDEN_V1" => (MarketGolden, 500),
        _ => return Err("INVALID_SCHEMA"),
    })
}
pub(super) fn current(owner: &ScenarioAccount, kind: Kind) -> Result<Payload, Fault> {
    if !matches!(
        owner.profile,
        ProfileContext::BtcEthScenario { .. }
            | ProfileContext::GoldenCancel(_)
            | ProfileContext::P1O03
    ) {
        return Err("INVALID_SCHEMA");
    }
    Ok(match kind {
        Kind::Trading => {
            if matches!(
                owner.profile,
                ProfileContext::GoldenCancel(_) | ProfileContext::P1O03
            ) {
                Payload::Trading(
                    owner.cash,
                    add(owner.cash, -owner.golden_cancel_reservation()?)?,
                )
            } else {
                let r = owner.reservation()?;
                Payload::Trading(r.equity, r.available_margin)
            }
        }
        Kind::Positions => {
            let mut rows = Vec::new();
            match &owner.positions {
                PositionState::BtcEth(positions) => {
                    let (scenario, marks) = owner.btc_context()?;
                    let input = owner.projection()?;
                    let value = scenario.evaluate(&input, marks)?;
                    for v in value.products {
                        let p = &positions[&v.product];
                        let mark = marks
                            .iter()
                            .find(|m| {
                                m.product == v.product
                                    && m.valid_from <= input.effective_at
                                    && input.effective_at < m.valid_to
                            })
                            .ok_or("NATIVE_INVARIANT")?;
                        rows.push(PositionRow {
                            product: product_id(&v.product).into(),
                            contracts: if p.side == Side::Long {
                                p.contracts
                            } else {
                                -p.contracts
                            },
                            mark: mark.price,
                            notional: v.notional,
                        });
                    }
                }
                PositionState::GoldenCancel(p) => {
                    if let Some(p) = p {
                        rows.push(PositionRow {
                            product: "P_A".into(),
                            contracts: if p.side == Side::Long {
                                p.contracts
                            } else {
                                -p.contracts
                            },
                            mark: Decimal::TEN,
                            notional: mul(p.contracts, Decimal::TEN)?,
                        });
                    }
                }
                _ => return Err("INVALID_SCHEMA"),
            }
            rows.sort_by(|a, b| a.product.cmp(&b.product));
            Payload::Positions(rows)
        }
        Kind::OpenOrders => {
            let mut rows = Vec::new();
            for order in owner.orders.values() {
                if order.facts.projects_remainder("NATIVE_INVARIANT")? {
                    rows.push(OrderRow {
                        facts: order.facts.clone(),
                        created_at: order.created_at,
                        limit_price: (owner.admitted_order_type(&order.facts)
                            == admission::OrderType::Limit)
                            .then_some(order.facts.price),
                    });
                }
            }
            rows.sort_by(|a, b| {
                (a.facts.product.canonical_id(), &a.facts.order_id)
                    .cmp(&(b.facts.product.canonical_id(), &b.facts.order_id))
            });
            Payload::OpenOrders(rows)
        }
        _ => return Err("INVALID_SCHEMA"),
    })
}
