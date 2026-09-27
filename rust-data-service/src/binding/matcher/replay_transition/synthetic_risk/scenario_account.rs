//! Private incomplete scenario account; no public API or 4C transitions.
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;

use super::super::{hash_fields, identity, AccountKey, Gate, Hash};
use super::*;

mod admission;
mod capacity;
mod event_limit;
mod execution;
mod golden_cancel;
mod hypothetical_settlement;
mod reservation;
mod risk_transition;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProfileContext {
    BtcEthScenario {
        scenario: FrozenScenario,
        marks: Vec<Mark>,
    },
    GoldenCapacity(capacity::Config),
    EventLimit(event_limit::Config),
    GoldenCancel(golden_cancel::Config),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileProduct {
    BtcEth(Product),
    Pa,
}

impl ProfileProduct {
    fn canonical_id(self) -> &'static str {
        match self {
            Self::BtcEth(product) => product_id(product),
            Self::Pa => "P_A",
        }
    }

    fn btc(self) -> Result<Product, Fault> {
        match self {
            Self::BtcEth(product) => Ok(product),
            Self::Pa => Err("PROFILE_MISMATCH"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeedLot {
    seed_execution_id: String,
    seed_sequence: u64,
    strategy_id: String,
    contracts: Decimal,
    entry: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeedPosition {
    product: Product,
    side: Side,
    contracts: Decimal,
    lots: Vec<SeedLot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeedOrder {
    intent_id: String,
    order_id: String,
    client_id: String,
    strategy_id: String,
    product: ProfileProduct,
    side: Side,
    price: Decimal,
    reduce_only: bool,
    original: Decimal,
    filled: Decimal,
    canceled: Decimal,
    remaining: Decimal,
    status: String,
}

impl SeedOrder {
    // Terminal facts stay in the owner, but never become financial remainders.
    fn projects_remainder(&self, invalid: Fault) -> Result<bool, Fault> {
        if self.original <= Decimal::ZERO
            || self.filled < Decimal::ZERO
            || self.remaining < Decimal::ZERO
            || self.canceled < Decimal::ZERO
            || add(add(self.filled, self.canceled)?, self.remaining)? != self.original
        {
            return Err(invalid);
        }
        match self.status.as_str() {
            "OPEN"
                if self.filled == Decimal::ZERO
                    && self.canceled == Decimal::ZERO
                    && self.remaining > Decimal::ZERO =>
            {
                Ok(true)
            }
            "PARTIALLY_FILLED"
                if self.filled > Decimal::ZERO
                    && self.canceled == Decimal::ZERO
                    && self.remaining > Decimal::ZERO =>
            {
                Ok(true)
            }
            "FILLED" if self.remaining == Decimal::ZERO && self.canceled == Decimal::ZERO => {
                Ok(false)
            }
            "CANCELED" if self.remaining == Decimal::ZERO && self.canceled > Decimal::ZERO => {
                Ok(false)
            }
            _ => Err(invalid),
        }
    }
}

// No derived equity, reservation, basis or margin can enter the seed.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CleanSeed {
    key: AccountKey,
    config_id: String,
    effective_at: i64,
    cash: Decimal,
    positions: Vec<SeedPosition>,
    orders: Vec<SeedOrder>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryLot {
    source: SeedLot,
    origin_spec_version: String,
    execution_id: Hash,
    base_quantity: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProductPosition {
    side: Side,
    contracts: Decimal,
    lots: Vec<EntryLot>,
    entry_basis: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PositionState {
    BtcEth(BTreeMap<Product, ProductPosition>),
    CapacityFlat,
    EventLimit(Option<ProductPosition>),
    GoldenCancel(Option<ProductPosition>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileRisk {
    BtcEth(MaintenanceState),
    CapacitySafe,
}

impl PositionState {
    fn btc(&self) -> Result<&BTreeMap<Product, ProductPosition>, Fault> {
        match self {
            Self::BtcEth(value) => Ok(value),
            _ => Err("PROFILE_MISMATCH"),
        }
    }

    fn btc_mut(&mut self) -> Result<&mut BTreeMap<Product, ProductPosition>, Fault> {
        match self {
            Self::BtcEth(value) => Ok(value),
            _ => Err("PROFILE_MISMATCH"),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::BtcEth(value) => value.len(),
            Self::CapacityFlat => 0,
            Self::EventLimit(value) | Self::GoldenCancel(value) => usize::from(value.is_some()),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestingOrder {
    facts: SeedOrder,
    version: u64,
    cancel: risk_transition::cancel::State,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScenarioAccount {
    key: AccountKey,
    config_id: String,
    profile: ProfileContext,
    valuation_context_id: Hash,
    seed_effective_at: i64,
    state_version: u64,
    gate: Gate,
    cash: Decimal,
    gross_realized: Decimal,
    fees: Decimal,
    positions: PositionState,
    orders: BTreeMap<String, RestingOrder>,
    seed_intents: BTreeSet<String>,
    seed_orders: BTreeSet<String>,
    seed_executions: BTreeSet<Hash>,
    commit_sequence: u64,
    intent_results: BTreeMap<String, admission::AdmissionResult>,
    // Only the future execution writer may publish a receipt; seeds have none.
    execution_receipts: BTreeMap<Hash, execution::CommittedExecution>,
    pending_actions: Vec<Infallible>,
    cancel_facts: risk_transition::cancel::Facts,
}

enum NewIdentity<'a> {
    Intent(&'a str),
    Order(&'a str),
    Execution {
        namespace: &'a str,
        canonical_id: Hash,
    },
}

fn seed_execution_key(key: &AccountKey, product: Product, id: &str) -> Hash {
    hash_fields(&[
        Some(key.venue.clone()),
        Some(key.environment.clone()),
        Some(key.account.clone()),
        key.subaccount.clone(),
        Some("seed".into()),
        Some(product_id(product).into()),
        Some(id.into()),
    ])
}

fn product_id(product: Product) -> &'static str {
    match product {
        Product::Btc => "BTC-USDT-SWAP",
        Product::Eth => "ETH-USDT-SWAP",
    }
}

fn context_id(scenario: &FrozenScenario, marks: &[Mark], at: i64) -> Result<Hash, Fault> {
    let mut fields = Vec::new();
    for product in [Product::Btc, Product::Eth] {
        let (spec, tier) = scenario.resolve(product, at)?;
        let mark = marks
            .iter()
            .find(|m| m.product == product && m.valid_from <= at && at < m.valid_to)
            .ok_or("MARK_COVERAGE_MISSING")?;
        fields.extend([
            Some(product_id(product).into()),
            Some(spec.version.clone()),
            Some(spec.interval.from.to_string()),
            spec.interval.to.map(|v| v.to_string()),
            Some(tier.version.clone()),
            Some(tier.interval.from.to_string()),
            tier.interval.to.map(|v| v.to_string()),
            Some(mark.valid_from.to_string()),
            Some(mark.valid_to.to_string()),
        ]);
        for value in [
            spec.contract_value,
            spec.multiplier,
            spec.tick,
            spec.lot,
            spec.minimum,
            mark.price,
            scenario.leverage,
        ] {
            fields.push(Some(value.normalize().to_string()));
        }
        for row in &tier.tiers {
            for value in [row.minimum, row.maximum, row.mmr, row.imr, row.max_leverage] {
                fields.push(Some(value.normalize().to_string()));
            }
        }
    }
    Ok(hash_fields(&fields))
}

fn aligned(value: Decimal, step: Decimal) -> bool {
    value.checked_rem(step) == Some(Decimal::ZERO)
}

impl ScenarioAccount {
    fn btc_context(&self) -> Result<(&FrozenScenario, &[Mark]), Fault> {
        match &self.profile {
            ProfileContext::BtcEthScenario { scenario, marks } => Ok((scenario, marks)),
            ProfileContext::GoldenCapacity(_)
            | ProfileContext::EventLimit(_)
            | ProfileContext::GoldenCancel(_) => Err("PROFILE_MISMATCH"),
        }
    }

    fn reservation(&self) -> Result<reservation::Snapshot, Fault> {
        let (scenario, marks) = self.btc_context()?;
        reservation::calculate(
            &self.projection()?,
            &self.orders.values().map(|o| &o.facts).collect::<Vec<_>>(),
            scenario,
            marks,
        )
    }

    fn from_seed(
        seed: &CleanSeed,
        scenario: &FrozenScenario,
        marks: &[Mark],
    ) -> Result<Self, Fault> {
        seed.key.validate()?;
        if !identity(&seed.config_id) {
            return Err("INVALID_CONFIG_ID");
        }
        let scenario = FrozenScenario::new(
            scenario.leverage,
            scenario.specs.clone(),
            scenario.tiers.clone(),
        )?;
        validate_marks(marks)?;
        let valuation_context_id = context_id(&scenario, marks, seed.effective_at)?;
        let mut positions = BTreeMap::new();
        let mut sequences = BTreeSet::new();
        let mut seed_ids = BTreeSet::new();
        let mut seed_executions = BTreeSet::new();
        for position in &seed.positions {
            let (spec, _) = scenario.resolve(position.product, seed.effective_at)?;
            let mut lots = Vec::new();
            let mut basis = Decimal::ZERO;
            for source in &position.lots {
                if !identity(&source.seed_execution_id)
                    || !identity(&source.strategy_id)
                    || !seed_ids.insert(source.seed_execution_id.clone())
                    || !sequences.insert(source.seed_sequence)
                    || source.contracts < spec.minimum
                    || !aligned(source.contracts, spec.lot)
                    || source.entry <= Decimal::ZERO
                    || !aligned(source.entry, spec.tick)
                {
                    return Err("INVALID_SEED_LOT");
                }
                let base_quantity =
                    mul(mul(source.contracts, spec.contract_value)?, spec.multiplier)?;
                basis = add(basis, mul(base_quantity, source.entry)?)?;
                let execution_id =
                    seed_execution_key(&seed.key, position.product, &source.seed_execution_id);
                seed_executions.insert(execution_id);
                lots.push(EntryLot {
                    source: source.clone(),
                    origin_spec_version: spec.version.clone(),
                    execution_id,
                    base_quantity,
                });
            }
            lots.sort_by(|a, b| {
                (a.source.seed_sequence, &a.source.seed_execution_id)
                    .cmp(&(b.source.seed_sequence, &b.source.seed_execution_id))
            });
            if positions
                .insert(
                    position.product,
                    ProductPosition {
                        side: position.side,
                        contracts: position.contracts,
                        lots,
                        entry_basis: basis,
                    },
                )
                .is_some()
            {
                return Err("DUPLICATE_NET_POSITION");
            }
        }
        let commit_sequence = u64::try_from(sequences.len()).map_err(|_| "SEQUENCE_OVERFLOW")?;
        if !sequences.into_iter().eq(0..commit_sequence) {
            return Err("INVALID_SEED_SEQUENCE");
        }
        let mut orders = BTreeMap::new();
        let mut seed_intents = BTreeSet::new();
        let mut clients = BTreeSet::new();
        for order in &seed.orders {
            let product = order.product.btc()?;
            let (spec, _) = scenario.resolve(product, seed.effective_at)?;
            if [
                &order.intent_id,
                &order.order_id,
                &order.client_id,
                &order.strategy_id,
            ]
            .iter()
            .any(|id| !identity(id))
                || !seed_intents.insert(order.intent_id.clone())
                || !clients.insert(order.client_id.clone())
                || order.price <= Decimal::ZERO
                || !aligned(order.price, spec.tick)
                || order.original < spec.minimum
                || order.remaining < spec.minimum
                || order.filled < Decimal::ZERO
                || order.canceled != Decimal::ZERO
                || [order.original, order.filled, order.remaining]
                    .iter()
                    .any(|q| !aligned(*q, spec.lot))
                || add(order.filled, order.remaining)? != order.original
                || !matches!(
                    (order.status.as_str(), order.filled > Decimal::ZERO),
                    ("OPEN", false) | ("PARTIALLY_FILLED", true)
                )
            {
                return Err("INVALID_SEED_ORDER");
            }
            if order.reduce_only
                && !positions
                    .get(&product)
                    .is_some_and(|p| p.side != order.side && order.remaining <= p.contracts)
            {
                return Err("REDUCE_ONLY_NOT_REDUCING");
            }
            if orders
                .insert(
                    order.order_id.clone(),
                    RestingOrder {
                        facts: order.clone(),
                        version: 0,
                        cancel: risk_transition::cancel::State::None,
                    },
                )
                .is_some()
            {
                return Err("DUPLICATE_SEED_ORDER");
            }
        }
        let owner = Self {
            key: seed.key.clone(),
            config_id: seed.config_id.clone(),
            profile: ProfileContext::BtcEthScenario {
                scenario,
                marks: marks.to_vec(),
            },
            valuation_context_id,
            seed_effective_at: seed.effective_at,
            state_version: 0,
            gate: Gate::Running,
            cash: seed.cash,
            gross_realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            positions: PositionState::BtcEth(positions),
            seed_orders: orders.keys().cloned().collect(),
            orders,
            seed_intents,
            seed_executions,
            commit_sequence,
            intent_results: BTreeMap::new(),
            execution_receipts: BTreeMap::new(),
            pending_actions: Vec::new(),
            cancel_facts: Default::default(),
        };
        let (scenario, marks) = owner.btc_context()?;
        if scenario.evaluate(&owner.projection()?, marks)?.risk != MaintenanceState::Safe {
            return Err("SEED_MMR_BREACH");
        }
        owner.reservation()?;
        Ok(owner)
    }

    fn projection(&self) -> Result<ValuationInput, Fault> {
        self.btc_context()?;
        Ok(ValuationInput {
            account_version: self.state_version,
            effective_at: self.seed_effective_at,
            cash: self.cash,
            positions: self
                .positions
                .btc()?
                .iter()
                .map(|(product, position)| NetPosition {
                    product: *product,
                    side: position.side,
                    contracts: position.contracts,
                    lots: position
                        .lots
                        .iter()
                        .map(|lot| ValuationLot {
                            contracts: lot.source.contracts,
                            entry: lot.source.entry,
                        })
                        .collect(),
                })
                .collect(),
        })
    }

    // Only for nonduplicates, after canonical identity lookup in future consumers.
    // No mutation: a future transition must fail its gate on this fault.
    fn validate_context(&self, effective_at: i64) -> Result<(), Fault> {
        let current = match &self.profile {
            ProfileContext::BtcEthScenario { scenario, marks } => {
                context_id(scenario, marks, effective_at)
            }
            ProfileContext::GoldenCapacity(config) => config.context_id(effective_at),
            ProfileContext::EventLimit(config) => config.context_id(effective_at),
            ProfileContext::GoldenCancel(config) => config.context_id(effective_at),
        };
        if current.ok() != Some(self.valuation_context_id) {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        Ok(())
    }

    fn guard_new_identity(&mut self, new: NewIdentity<'_>) -> Result<(), Fault> {
        if self.gate != Gate::Running {
            return Err("RUN_FAILED");
        }
        let conflict = match new {
            NewIdentity::Intent(id) => self.seed_intents.contains(id),
            NewIdentity::Order(id) => self.seed_orders.contains(id),
            NewIdentity::Execution {
                namespace,
                canonical_id,
            } => namespace == "seed" || self.seed_executions.contains(&canonical_id),
        };
        if conflict {
            self.gate = Gate::Failed("SEED_IDENTITY_CONFLICT");
            return Err("SEED_IDENTITY_CONFLICT");
        }
        Ok(())
    }

    // A target reference is not creation of another order identity.
    fn target_order(&self, id: &str) -> Result<&RestingOrder, Fault> {
        self.orders.get(id).ok_or("UNKNOWN_ORDER")
    }
}

#[cfg(test)]
mod tests;
