//! Pure whole-product reservation; per-order rows never own shared exposure margin.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrderReservation {
    pub order_id: String,
    pub product: Product,
    pub side: Side,
    pub remaining_contracts: Decimal,
    pub remaining_base_exposure: Decimal,
    pub order_loss: Decimal,
    pub fee_hold: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProductReservation {
    pub product: Product,
    pub position_value: Decimal,
    pub long_remaining_value: Decimal,
    pub short_remaining_value: Decimal,
    pub exposure_margin: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    pub orders: Vec<OrderReservation>,
    pub products: Vec<ProductReservation>,
    pub equity: Decimal,
    pub maintenance_margin: Decimal,
    pub total_order_loss: Decimal,
    pub total_fee_hold: Decimal,
    pub used_margin: Decimal,
    pub available_margin: Decimal,
}

pub(super) fn calculate(
    input: &ValuationInput,
    orders: &[&SeedOrder],
    scenario: &FrozenScenario,
    marks: &[Mark],
) -> Result<Snapshot, Fault> {
    let mut ids = BTreeSet::new();
    let mut resting = Vec::new();
    for order in orders {
        order.product.btc()?;
        if !identity(&order.order_id) || !ids.insert(order.order_id.clone()) {
            return Err("INVALID_RESERVATION_ORDER");
        }
        if order.projects_remainder("INVALID_RESERVATION_ORDER")? {
            resting.push(*order);
        }
    }
    scenario.validate()?;
    validate_marks(marks)?;
    context_id(scenario, marks, input.effective_at)?;
    let valuation = scenario.evaluate(input, marks)?;
    let mut snapshot = Snapshot {
        orders: Vec::new(),
        products: Vec::new(),
        equity: valuation.equity,
        maintenance_margin: valuation.maintenance_margin,
        total_order_loss: Decimal::ZERO,
        total_fee_hold: Decimal::ZERO,
        used_margin: Decimal::ZERO,
        available_margin: Decimal::ZERO,
    };
    for product in scenario.products() {
        let (spec, _) = scenario.resolve(&product, input.effective_at)?;
        let mark = marks
            .iter()
            .find(|m| {
                m.product == product
                    && m.valid_from <= input.effective_at
                    && input.effective_at < m.valid_to
            })
            .ok_or("MARK_COVERAGE_MISSING")?;
        let value = valuation
            .products
            .iter()
            .find(|p| p.product == product)
            .map_or(Decimal::ZERO, |p| p.notional);
        let mut long = Decimal::ZERO;
        let mut short = Decimal::ZERO;
        for order in resting
            .iter()
            .filter(|o| matches!(&o.product, ProfileProduct::BtcEth(p) if p == &product))
        {
            if order.remaining < spec.minimum
                || order.original < spec.minimum
                || order.filled < Decimal::ZERO
                || [order.remaining, order.original, order.filled]
                    .iter()
                    .any(|q| !aligned(*q, spec.lot))
                || order.price <= Decimal::ZERO
                || !aligned(order.price, spec.tick)
            {
                return Err("INVALID_RESERVATION_ORDER");
            }
            let base = mul(mul(order.remaining, spec.contract_value)?, spec.multiplier)?;
            let marked = mul(base, mark.price)?;
            let difference = match order.side {
                Side::Long => {
                    long = add(long, marked)?;
                    add(order.price, -mark.price)?
                }
                Side::Short => {
                    short = add(short, marked)?;
                    add(mark.price, -order.price)?
                }
            };
            let loss = mul(base, difference.max(Decimal::ZERO))?;
            let fee_policy = hypothetical_settlement::configured_fee_policy(scenario, &product)?;
            let fee = hypothetical_settlement::fee_amount(base, order.price, fee_policy)?;
            snapshot.total_order_loss = add(snapshot.total_order_loss, loss)?;
            snapshot.total_fee_hold = add(snapshot.total_fee_hold, fee)?;
            snapshot.orders.push(OrderReservation {
                order_id: order.order_id.clone(),
                product: product.clone(),
                side: order.side,
                remaining_contracts: order.remaining,
                remaining_base_exposure: base,
                order_loss: loss,
                fee_hold: fee,
            });
        }
        let worst = match input
            .positions
            .iter()
            .find(|p| p.product == product)
            .map(|p| p.side)
        {
            None => long.max(short),
            Some(Side::Long) => add(value, long)?.max(add(short, -value)?),
            Some(Side::Short) => add(long, -value)?.max(add(value, short)?),
        };
        let exposure = exact(worst.mantissa(), worst.scale() + 1)?;
        snapshot.used_margin = add(snapshot.used_margin, exposure)?;
        snapshot.products.push(ProductReservation {
            product,
            position_value: value,
            long_remaining_value: long,
            short_remaining_value: short,
            exposure_margin: exposure,
        });
    }
    snapshot
        .orders
        .sort_by(|a, b| (&a.product, &a.order_id).cmp(&(&b.product, &b.order_id)));
    snapshot.used_margin = add(
        add(snapshot.used_margin, snapshot.total_order_loss)?,
        snapshot.total_fee_hold,
    )?;
    snapshot.available_margin = add(snapshot.equity, -snapshot.used_margin)?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests;
