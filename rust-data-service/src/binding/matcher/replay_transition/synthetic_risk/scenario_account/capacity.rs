//! Closed Golden capacity context and pure decision inputs, never admission receipts.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Program {
    CapacityV1,
    CapacityTightV1,
}

impl Program {
    fn id(self) -> &'static str {
        match self {
            Self::CapacityV1 => "SYNTHETIC_CAPACITY_V1",
            Self::CapacityTightV1 => "SYNTHETIC_CAPACITY_TIGHT_V1",
        }
    }

    fn threshold(self) -> Decimal {
        Decimal::from(match self {
            Self::CapacityV1 => 1000,
            Self::CapacityTightV1 => 500,
        })
    }

    fn hash(self) -> Hash {
        hash_fields(&[
            Some("GoldenCapacity/program/v1".into()),
            Some(self.id().into()),
            Some("occupied+resting+candidate<=threshold".into()),
            Some(self.threshold().to_string()),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Config {
    product: ProfileProduct,
    program_id: String,
    program_hash: Hash,
    pub(super) rule_data_version: String,
    pub(super) spec_version: String,
    quantity_unit: String,
    multiplier: Decimal,
    tick: Decimal,
    step: Decimal,
    fee_rate: Decimal,
    spec_interval: Interval,
    mark: Decimal,
    mark_interval: Interval,
}

impl Config {
    pub(super) fn frozen(program: Program) -> Self {
        Self {
            product: ProfileProduct::Pa,
            program_id: program.id().into(),
            program_hash: program.hash(),
            rule_data_version: "capacity-data-v1".into(),
            spec_version: "capacity-spec-v1".into(),
            quantity_unit: "contract".into(),
            multiplier: Decimal::ONE,
            tick: Decimal::ONE,
            step: Decimal::ONE,
            fee_rate: Decimal::ZERO,
            spec_interval: interval(false, 1000),
            mark: Decimal::from(100),
            mark_interval: interval(false, 1000),
        }
    }

    fn validate(&self, at: i64) -> Result<Program, Fault> {
        let program = match self.program_id.as_str() {
            "SYNTHETIC_CAPACITY_V1" => Program::CapacityV1,
            "SYNTHETIC_CAPACITY_TIGHT_V1" => Program::CapacityTightV1,
            _ => return Err("UNSUPPORTED_CAPACITY_PROGRAM"),
        };
        if self != &Self::frozen(program) {
            return Err("INVALID_CAPACITY_CONTEXT");
        }
        if !self.spec_interval.contains(at) || !self.mark_interval.contains(at) {
            return Err("CAPACITY_CONTEXT_COVERAGE");
        }
        Ok(program)
    }

    pub(super) fn context_id(&self, at: i64) -> Result<Hash, Fault> {
        let program = self.validate(at)?;
        let mut fields = vec![
            Some("GoldenCapacity/context/v1".into()),
            Some(self.product.canonical_id().into()),
            Some(self.quantity_unit.clone()),
            Some(self.spec_version.clone()),
            Some(self.rule_data_version.clone()),
            Some(program.id().into()),
        ];
        fields.extend(self.program_hash.iter().map(|byte| Some(byte.to_string())));
        for interval in [self.spec_interval, self.mark_interval] {
            fields.push(Some(interval.from.to_string()));
            fields.push(interval.to.map(|v| v.to_string()));
        }
        for value in [
            self.multiplier,
            self.tick,
            self.step,
            self.fee_rate,
            self.mark,
            program.threshold(),
        ] {
            fields.push(Some(value.normalize().to_string()));
        }
        Ok(hash_fields(&fields))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Candidate {
    pub(super) product: ProfileProduct,
    pub(super) quantity: Decimal,
    pub(super) price: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RemainingOrder {
    order_id: String,
    remainder: Candidate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CapacityProjection {
    program: Program,
    pub(super) program_hash: Hash,
    current: Decimal,
    new: Decimal,
    pub(super) required: Decimal,
    pub(super) threshold: Decimal,
    available: Decimal,
}

fn reservation(config: &Config, candidate: &Candidate) -> Result<Decimal, Fault> {
    if candidate.product != ProfileProduct::Pa
        || candidate.quantity <= Decimal::ZERO
        || candidate.price <= Decimal::ZERO
        || !aligned(candidate.quantity, config.step)
        || !aligned(candidate.price, config.tick)
    {
        return Err("INVALID_CAPACITY_CANDIDATE");
    }
    mul(mul(candidate.quantity, candidate.price)?, config.multiplier)
}

// Pure authoritative facts only. This does not authorize storing nonflat owners.
fn calculate(
    config: &Config,
    at: i64,
    occupied_contracts: Decimal,
    orders: &[RemainingOrder],
    candidate: &Candidate,
) -> Result<CapacityProjection, Fault> {
    let program = config.validate(at)?;
    if occupied_contracts < Decimal::ZERO || !aligned(occupied_contracts, config.step) {
        return Err("INVALID_CAPACITY_OCCUPIED");
    }
    let mut current = mul(mul(occupied_contracts, config.mark)?, config.multiplier)?;
    let mut identities = BTreeSet::new();
    for order in orders {
        if !identity(&order.order_id) || !identities.insert(&order.order_id) {
            return Err("INVALID_CAPACITY_ORDER_ID");
        }
        current = add(current, reservation(config, &order.remainder)?)?;
    }
    let new = reservation(config, candidate)?;
    let required = add(current, new)?;
    let threshold = program.threshold();
    Ok(CapacityProjection {
        program,
        program_hash: program.hash(),
        current,
        new,
        required,
        threshold,
        available: signed_sum(&[threshold, -required])?,
    })
}

impl ScenarioAccount {
    pub(super) fn from_capacity_seed(seed: &CleanSeed, config: &Config) -> Result<Self, Fault> {
        seed.key.validate()?;
        if !identity(&seed.config_id) {
            return Err("INVALID_CONFIG_ID");
        }
        let valuation_context_id = config.context_id(seed.effective_at)?;
        if !seed.positions.is_empty() || !seed.orders.is_empty() {
            return Err("UNSUPPORTED_CAPACITY_SEED_STATE");
        }
        Ok(Self {
            key: seed.key.clone(),
            config_id: seed.config_id.clone(),
            profile: ProfileContext::GoldenCapacity(config.clone()),
            valuation_context_id,
            seed_effective_at: seed.effective_at,
            state_version: 0,
            gate: Gate::Running,
            cash: seed.cash,
            gross_realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            positions: PositionState::CapacityFlat,
            orders: BTreeMap::new(),
            seed_intents: BTreeSet::new(),
            seed_orders: BTreeSet::new(),
            seed_executions: BTreeSet::new(),
            commit_sequence: 0,
            intent_results: BTreeMap::new(),
            execution_receipts: BTreeMap::new(),
            pending_actions: Vec::new(),
            cancel_facts: Default::default(),
            transition: Default::default(),
        })
    }

    pub(super) fn capacity_projection(
        &self,
        candidate: &Candidate,
    ) -> Result<CapacityProjection, Fault> {
        let ProfileContext::GoldenCapacity(config) = &self.profile else {
            return Err("PROFILE_MISMATCH");
        };
        if self.positions != PositionState::CapacityFlat {
            return Err("UNSUPPORTED_CAPACITY_SEED_STATE");
        }
        let orders = self
            .orders
            .values()
            .filter_map(|order| {
                let facts = &order.facts;
                if facts.product != ProfileProduct::Pa {
                    return Some(Err("INVALID_CAPACITY_CANDIDATE"));
                }
                match facts.projects_remainder("INVALID_CAPACITY_ORDER") {
                    Ok(false) => return None,
                    Err(fault) => return Some(Err(fault)),
                    Ok(true) => {}
                }
                Some(Ok(RemainingOrder {
                    order_id: facts.order_id.clone(),
                    remainder: Candidate {
                        product: facts.product,
                        quantity: facts.remaining,
                        price: facts.price,
                    },
                }))
            })
            .collect::<Result<Vec<_>, Fault>>()?;
        calculate(
            config,
            self.seed_effective_at,
            Decimal::ZERO,
            &orders,
            candidate,
        )
    }
}

#[cfg(test)]
mod tests;
