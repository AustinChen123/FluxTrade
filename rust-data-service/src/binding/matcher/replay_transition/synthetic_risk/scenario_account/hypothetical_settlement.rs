//! Pure FIFO calculation shared by future admission stress and execution commit.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OpeningIdentity {
    pub source_id: String,
    pub strategy_id: String,
    pub execution_id: Hash,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Draft {
    pub position: Option<ProductPosition>,
    pub gross_realized_delta: Decimal,
    pub fee: Decimal,
    pub cash_delta: Decimal,
}

pub(super) fn fee_amount(base: Decimal, price: Decimal, rate: Decimal) -> Result<Decimal, Fault> {
    if rate != Decimal::new(1, 3) || base <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err("UNSUPPORTED_FEE_INPUT");
    }
    mul(mul(base, price)?, rate)
}

fn validate_position(position: &ProductPosition, spec: &Spec) -> Result<(), Fault> {
    let mut contracts = Decimal::ZERO;
    let mut basis = Decimal::ZERO;
    let mut ids = BTreeSet::new();
    let mut sources = BTreeSet::new();
    let mut previous = None;
    for lot in &position.lots {
        let facts = &lot.source;
        if !identity(&facts.seed_execution_id)
            || !identity(&facts.strategy_id)
            || !sources.insert(&facts.seed_execution_id)
            || !ids.insert(lot.execution_id)
            || previous.is_some_and(|seq| seq >= facts.seed_sequence)
            || facts.contracts < spec.minimum
            || !aligned(facts.contracts, spec.lot)
            || facts.entry <= Decimal::ZERO
            || !aligned(facts.entry, spec.tick)
            || lot.base_quantity
                != mul(mul(facts.contracts, spec.contract_value)?, spec.multiplier)?
        {
            return Err("INVALID_FIFO_POSITION");
        }
        previous = Some(facts.seed_sequence);
        contracts = add(contracts, facts.contracts)?;
        basis = add(basis, mul(lot.base_quantity, facts.entry)?)?;
    }
    if contracts <= Decimal::ZERO
        || contracts > spec_tier_maximum(spec.product)
        || contracts != position.contracts
        || basis != position.entry_basis
    {
        return Err("INVALID_FIFO_POSITION");
    }
    Ok(())
}

pub(super) fn calculate(
    current: Option<&ProductPosition>,
    side: Side,
    quantity: Decimal,
    price: Decimal,
    spec: &Spec,
    fee_rate: Decimal,
    opening: Option<&OpeningIdentity>,
) -> Result<Draft, Fault> {
    if *spec != frozen_spec(spec.product, spec.version == "spec-v2") {
        return Err("UNSUPPORTED_SPEC");
    }
    if quantity < spec.minimum
        || !aligned(quantity, spec.lot)
        || price <= Decimal::ZERO
        || !aligned(price, spec.tick)
    {
        return Err("INVALID_HYPOTHETICAL_EXECUTION");
    }
    if let Some(position) = current {
        validate_position(position, spec)?;
    }
    let base = mul(mul(quantity, spec.contract_value)?, spec.multiplier)?;
    let fee = fee_amount(base, price, fee_rate)?;
    let mut position = current.cloned().unwrap_or(ProductPosition {
        side,
        contracts: Decimal::ZERO,
        lots: Vec::new(),
        entry_basis: Decimal::ZERO,
    });
    let mut realized = Vec::new();
    if position.side == side {
        let id = opening.ok_or("MISSING_OPENING_IDENTITY")?;
        if !identity(&id.source_id)
            || !identity(&id.strategy_id)
            || position.lots.iter().any(|lot| {
                lot.execution_id == id.execution_id
                    || lot.source.seed_execution_id == id.source_id
                    || lot.source.seed_sequence >= id.sequence
            })
        {
            return Err("INVALID_OPENING_IDENTITY");
        }
        position.contracts = add(position.contracts, quantity)?;
        position.entry_basis = add(position.entry_basis, mul(base, price)?)?;
        position.lots.push(EntryLot {
            source: SeedLot {
                seed_execution_id: id.source_id.clone(),
                seed_sequence: id.sequence,
                strategy_id: id.strategy_id.clone(),
                contracts: quantity,
                entry: price,
            },
            execution_id: id.execution_id,
            base_quantity: base,
        });
    } else {
        if opening.is_some() || quantity > position.contracts {
            return Err("CROSS_ZERO_OR_UNEXPECTED_OPENING_IDENTITY");
        }
        let mut remaining = quantity;
        for lot in &mut position.lots {
            let closed = remaining.min(lot.source.contracts);
            let closed_base = mul(mul(closed, spec.contract_value)?, spec.multiplier)?;
            let difference = match position.side {
                Side::Long => add(price, -lot.source.entry)?,
                Side::Short => add(lot.source.entry, -price)?,
            };
            realized.push(mul(closed_base, difference)?);
            lot.source.contracts = add(lot.source.contracts, -closed)?;
            lot.base_quantity = add(lot.base_quantity, -closed_base)?;
            remaining = add(remaining, -closed)?;
            if remaining == Decimal::ZERO {
                break;
            }
        }
        position
            .lots
            .retain(|lot| lot.source.contracts > Decimal::ZERO);
        position.contracts = add(position.contracts, -quantity)?;
        position.entry_basis = position.lots.iter().try_fold(Decimal::ZERO, |basis, lot| {
            add(basis, mul(lot.base_quantity, lot.source.entry)?)
        })?;
    }
    let gross_realized_delta = if realized.is_empty() {
        Decimal::ZERO
    } else {
        signed_sum(&realized)?
    };
    if position.contracts > spec_tier_maximum(spec.product) {
        return Err("UNSUPPORTED_POSITION_TIER");
    }
    Ok(Draft {
        position: (position.contracts > Decimal::ZERO).then_some(position),
        gross_realized_delta,
        fee,
        cash_delta: add(gross_realized_delta, -fee)?,
    })
}

fn spec_tier_maximum(product: Product) -> Decimal {
    frozen_tiers(product, false).tiers[2].maximum
}

#[cfg(test)]
mod tests;
