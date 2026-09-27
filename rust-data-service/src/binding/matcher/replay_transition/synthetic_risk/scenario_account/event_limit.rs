//! Closed Golden C09 seed/context and pure financial projection.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Program {
    V1,
    V2,
}

impl Program {
    fn id(self) -> &'static str {
        match self {
            Self::V1 => "SYNTHETIC_EVENT_LIMIT_V1",
            Self::V2 => "SYNTHETIC_EVENT_LIMIT_V2",
        }
    }
    fn limit(self) -> u64 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }
    fn hash(self) -> Hash {
        hash_fields(&[
            Some("GoldenEventLimit/program/v1".into()),
            Some(self.id().into()),
            Some("committed-in-event<limit".into()),
            Some(self.limit().to_string()),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Config {
    program_id: String,
    program_hash: Hash,
    multiplier: Decimal,
    tick: Decimal,
    step: Decimal,
    mark: Decimal,
    fee: Decimal,
    interval: Interval,
}

impl Config {
    pub(super) fn execution_limit(&self, at: i64) -> Result<u64, Fault> {
        Ok(self.validate(at)?.limit())
    }
    pub(super) fn frozen(program: Program) -> Self {
        Self {
            program_id: program.id().into(),
            program_hash: program.hash(),
            multiplier: Decimal::ONE,
            tick: Decimal::ONE,
            step: Decimal::ONE,
            mark: Decimal::from(100),
            fee: Decimal::ZERO,
            interval: interval(false, 1000),
        }
    }

    fn validate(&self, at: i64) -> Result<Program, Fault> {
        let program = match self.program_id.as_str() {
            "SYNTHETIC_EVENT_LIMIT_V1" => Program::V1,
            "SYNTHETIC_EVENT_LIMIT_V2" => Program::V2,
            _ => return Err("UNSUPPORTED_EVENT_LIMIT_PROGRAM"),
        };
        if *self != Self::frozen(program) {
            return Err("INVALID_EVENT_LIMIT_CONTEXT");
        }
        if !self.interval.contains(at) {
            return Err("EVENT_LIMIT_CONTEXT_COVERAGE");
        }
        Ok(program)
    }

    pub(super) fn context_id(&self, at: i64) -> Result<Hash, Fault> {
        self.validate(at)?;
        let mut fields = vec![
            Some("GoldenEventLimit/context/v1".into()),
            Some("P_A".into()),
            Some("contract".into()),
            Some("event-limit-spec-v1".into()),
            Some("event-limit-data-v1".into()),
            Some(self.program_id.clone()),
            Some(self.interval.from.to_string()),
            self.interval.to.map(|v| v.to_string()),
        ];
        fields.extend(self.program_hash.iter().map(|byte| Some(byte.to_string())));
        fields.extend(
            [self.multiplier, self.tick, self.step, self.mark, self.fee]
                .map(|value| Some(value.normalize().to_string())),
        );
        Ok(hash_fields(&fields))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrderRow {
    order_id: String,
    product: ProfileProduct,
    side: Side,
    remaining_contracts: Decimal,
    remaining_base_exposure: Decimal,
    order_loss: Decimal,
    fee_hold: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    orders: Vec<OrderRow>,
    position_value: Decimal,
    long_remaining_value: Decimal,
    short_remaining_value: Decimal,
    exposure_margin: Decimal,
    total_order_loss: Decimal,
    total_fee_hold: Decimal,
    used_margin: Decimal,
    available_margin: Decimal,
    equity: Decimal,
    eligible_collateral: Decimal,
    initial_margin: Decimal,
    maintenance_margin: Decimal,
    risk: ProfileRisk,
    pending_action_ids: Vec<String>,
}

fn validate_orders(orders: &[&SeedOrder]) -> Result<(), Fault> {
    let mut intents = BTreeSet::new();
    let mut clients = BTreeSet::new();
    if orders.len() != 2 {
        return Err("INVALID_EVENT_LIMIT_ORDERS");
    }
    for (order, expected) in orders.iter().zip(["O_A", "O_B"]) {
        if order.order_id != expected
            || order.product != ProfileProduct::Pa
            || order.side != Side::Long
            || order.price != Decimal::TEN
            || order.original != Decimal::ONE
            || order.reduce_only
            || !aligned(order.filled, Decimal::ONE)
            || !aligned(order.remaining, Decimal::ONE)
            || !identity(&order.intent_id)
            || !identity(&order.client_id)
            || !identity(&order.strategy_id)
            || !intents.insert(&order.intent_id)
            || !clients.insert(&order.client_id)
        {
            return Err("INVALID_EVENT_LIMIT_ORDERS");
        }
        order.projects_remainder("INVALID_EVENT_LIMIT_ORDERS")?;
    }
    Ok(())
}

// Pure authoritative projection: callers may supply post-fill facts, not a fill.
fn calculate(
    config: &Config,
    at: i64,
    cash: Decimal,
    position: Option<&ProductPosition>,
    orders: &[&SeedOrder],
) -> Result<Snapshot, Fault> {
    config.validate(at)?;
    validate_orders(orders)?;
    if cash != Decimal::from(1000) {
        return Err("INVALID_EVENT_LIMIT_CASH");
    }
    let mut contracts = Decimal::ZERO;
    if let Some(position) = position {
        let mut ids = BTreeSet::new();
        for (sequence, lot) in position.lots.iter().enumerate() {
            if lot.source.seed_sequence != sequence as u64
                || !ids.insert(lot.execution_id)
                || !identity(&lot.source.seed_execution_id)
                || !identity(&lot.source.strategy_id)
                || lot.source.contracts != Decimal::ONE
                || lot.base_quantity != Decimal::ONE
                || lot.source.entry != Decimal::TEN
            {
                return Err("INVALID_EVENT_LIMIT_POSITION");
            }
            contracts = add(contracts, lot.source.contracts)?;
        }
        if position.side != Side::Long
            || contracts <= Decimal::ZERO
            || contracts > Decimal::from(2)
            || position.contracts != contracts
            || position.entry_basis != mul(contracts, Decimal::TEN)?
        {
            return Err("INVALID_EVENT_LIMIT_POSITION");
        }
    }
    let filled = orders
        .iter()
        .try_fold(Decimal::ZERO, |sum, order| add(sum, order.filled))?;
    if filled != contracts {
        return Err("INVALID_EVENT_LIMIT_POSITION");
    }
    let position_value = mul(mul(contracts, config.mark)?, config.multiplier)?;
    let mut snapshot = Snapshot {
        orders: Vec::new(),
        position_value,
        long_remaining_value: Decimal::ZERO,
        short_remaining_value: Decimal::ZERO,
        exposure_margin: position_value,
        total_order_loss: Decimal::ZERO,
        total_fee_hold: Decimal::ZERO,
        used_margin: position_value,
        available_margin: cash,
        equity: cash,
        eligible_collateral: cash,
        initial_margin: Decimal::ZERO,
        maintenance_margin: Decimal::ZERO,
        risk: ProfileRisk::CapacitySafe,
        pending_action_ids: Vec::new(),
    };
    for order in orders {
        if !order.projects_remainder("INVALID_EVENT_LIMIT_ORDERS")? {
            continue;
        }
        let base = mul(order.remaining, config.multiplier)?;
        snapshot.long_remaining_value =
            add(snapshot.long_remaining_value, mul(base, order.price)?)?;
        snapshot.orders.push(OrderRow {
            order_id: order.order_id.clone(),
            product: ProfileProduct::Pa,
            side: Side::Long,
            remaining_contracts: order.remaining,
            remaining_base_exposure: base,
            order_loss: Decimal::ZERO,
            fee_hold: Decimal::ZERO,
        });
    }
    snapshot.exposure_margin = add(position_value, snapshot.long_remaining_value)?;
    snapshot.used_margin = snapshot.exposure_margin;
    snapshot.available_margin = add(cash, -snapshot.used_margin)?;
    Ok(snapshot)
}

impl ScenarioAccount {
    pub(super) fn from_event_limit_seed(seed: &CleanSeed, config: &Config) -> Result<Self, Fault> {
        seed.key.validate()?;
        let valuation_context_id = config.context_id(seed.effective_at)?;
        if seed.key.account != "A"
            || !identity(&seed.config_id)
            || seed.cash != Decimal::from(1000)
            || !seed.positions.is_empty()
            || seed.orders.iter().any(|order| order.status != "OPEN")
        {
            return Err("INVALID_EVENT_LIMIT_SEED");
        }
        validate_orders(&seed.orders.iter().collect::<Vec<_>>())?;
        Ok(Self {
            key: seed.key.clone(),
            config_id: seed.config_id.clone(),
            profile: ProfileContext::EventLimit(config.clone()),
            valuation_context_id,
            seed_effective_at: seed.effective_at,
            state_version: 0,
            gate: Gate::Running,
            cash: seed.cash,
            gross_realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            positions: PositionState::EventLimit(None),
            orders: seed
                .orders
                .iter()
                .map(|facts| {
                    (
                        facts.order_id.clone(),
                        RestingOrder {
                            facts: facts.clone(),
                            version: 0,
                            cancel: risk_transition::cancel::State::None,
                        },
                    )
                })
                .collect(),
            seed_intents: seed.orders.iter().map(|o| o.intent_id.clone()).collect(),
            seed_orders: seed.orders.iter().map(|o| o.order_id.clone()).collect(),
            seed_executions: BTreeSet::new(),
            commit_sequence: 0,
            intent_results: BTreeMap::new(),
            execution_receipts: BTreeMap::new(),
            pending_actions: Vec::new(),
            cancel_facts: Default::default(),
            transition: Default::default(),
        })
    }

    pub(super) fn event_limit_projection(&self) -> Result<Snapshot, Fault> {
        let (ProfileContext::EventLimit(config), PositionState::EventLimit(position)) =
            (&self.profile, &self.positions)
        else {
            return Err("PROFILE_MISMATCH");
        };
        calculate(
            config,
            self.seed_effective_at,
            self.cash,
            position.as_ref(),
            &self.orders.values().map(|o| &o.facts).collect::<Vec<_>>(),
        )
    }
}

#[cfg(test)]
pub(super) mod tests;
