//! Closed GT-03 fixture profile. Financial settlement belongs to the shared calculator.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Config {
    interval: Interval,
    price: Decimal,
    maximum: Decimal,
}

impl Config {
    pub(super) fn frozen() -> Self {
        Self {
            interval: interval(false, 1000),
            price: Decimal::TEN,
            maximum: Decimal::TEN,
        }
    }
    pub(super) fn validate(&self, at: i64) -> Result<(), Fault> {
        if *self != Self::frozen() {
            return Err("INVALID_GOLDEN_CANCEL_CONTEXT");
        }
        if !self.interval.contains(at) {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        Ok(())
    }
    pub(super) fn context_id(&self, at: i64) -> Result<Hash, Fault> {
        self.validate(at)?;
        Ok(hash_fields(&[Some(
            "GoldenCancel/gt03-spec-v1/gt03-rule-v1".into(),
        )]))
    }
}

impl ScenarioAccount {
    pub(super) fn from_golden_cancel_seed(
        seed: &CleanSeed,
        config: &Config,
    ) -> Result<Self, Fault> {
        seed.key.validate()?;
        let valuation_context_id = config.context_id(seed.effective_at)?;
        if !identity(&seed.config_id)
            || seed.cash != Decimal::from(1000)
            || !seed.positions.is_empty()
            || !seed.orders.is_empty()
        {
            return Err("INVALID_GOLDEN_CANCEL_SEED");
        }
        Ok(Self {
            key: seed.key.clone(),
            config_id: seed.config_id.clone(),
            profile: ProfileContext::GoldenCancel(config.clone()),
            valuation_context_id,
            seed_effective_at: seed.effective_at,
            state_version: 0,
            gate: Gate::Running,
            cash: seed.cash,
            gross_realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            positions: PositionState::GoldenCancel(None),
            orders: BTreeMap::new(),
            seed_intents: BTreeSet::new(),
            seed_orders: BTreeSet::new(),
            seed_executions: BTreeSet::new(),
            commit_sequence: 0,
            intent_results: BTreeMap::new(),
            execution_receipts: BTreeMap::new(),
            pending_actions: Vec::new(),
            cancel_facts: Default::default(),
        })
    }

    pub(super) fn golden_cancel_reservation(&self) -> Result<Decimal, Fault> {
        let ProfileContext::GoldenCancel(config) = &self.profile else {
            return Err("PROFILE_MISMATCH");
        };
        config.validate(self.seed_effective_at)?;
        self.orders.values().try_fold(Decimal::ZERO, |sum, order| {
            order
                .facts
                .projects_remainder("INVALID_RESERVATION_ORDER")?;
            add(sum, mul(order.facts.remaining, Decimal::TEN)?)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{d, fixture};
    use super::*;
    #[test]
    fn closed_seed_and_context_reject_injection() {
        let (mut seed, _, _) = fixture();
        seed.cash = d("1000");
        seed.positions.clear();
        seed.orders.clear();
        let config = Config::frozen();
        for at in [0, 999] {
            let mut seed = seed.clone();
            seed.effective_at = at;
            assert!(ScenarioAccount::from_golden_cancel_seed(&seed, &config).is_ok());
        }
        for mutate in [
            |c: &mut Config| c.price = d("11"),
            |c: &mut Config| c.maximum = d("11"),
            |c: &mut Config| c.interval.to = Some(1001),
        ] {
            let mut bad = config.clone();
            mutate(&mut bad);
            assert_eq!(
                ScenarioAccount::from_golden_cancel_seed(&seed, &bad),
                Err("INVALID_GOLDEN_CANCEL_CONTEXT")
            );
        }
        for field in 0..4 {
            let mut bad = seed.clone();
            match field {
                0 => bad.cash = d("999"),
                1 => bad.positions = fixture().0.positions,
                2 => bad.orders = fixture().0.orders,
                _ => bad.config_id.clear(),
            };
            assert_eq!(
                ScenarioAccount::from_golden_cancel_seed(&bad, &config),
                Err("INVALID_GOLDEN_CANCEL_SEED")
            );
        }
    }
}
