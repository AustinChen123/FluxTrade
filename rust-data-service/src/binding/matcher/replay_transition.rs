//! Incomplete, in-memory GT-02 flat-opening kernel, not a replay/public API.
//! No margin, reservation, risk action, persistence, or delivery semantics.
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

use ring::digest::{digest, SHA256};
use rust_decimal::Decimal;

use super::{settlement, FeeModel, Order, PyMatchingEngine, SettlementModel};

mod synthetic_risk;

type Fault = &'static str;
type Hash = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq)]
struct AccountKey {
    venue: String,
    environment: String,
    account: String,
    subaccount: Option<String>,
}

fn identity(value: &str) -> bool {
    !value.is_empty() && value.trim() == value
}

impl AccountKey {
    fn validate(&self) -> Result<(), Fault> {
        if [&self.venue, &self.environment, &self.account]
            .into_iter()
            .all(|s| identity(s))
            && self.subaccount.as_deref().is_none_or(identity)
        {
            Ok(())
        } else {
            Err("INVALID_ACCOUNT_KEY")
        }
    }
}

#[derive(Clone, Debug)]
struct ExternalExecutionKey {
    account: AccountKey,
    namespace: String,
    product: String,
    external_id: String,
}

/// Frozen event inputs, not executable candidates or caller-authoritative fees.
#[derive(Clone, Debug)]
struct Template {
    key: ExternalExecutionKey,
    order_id: String,
    side: String,
    price: Decimal,
    quantity: Decimal,
    is_taker: bool,
    fee_asset: Option<String>,
    reported_fee: Decimal,
    effective_at: i64,
    spec: String,
    rule: String,
}

impl Template {
    fn hashes(&self) -> (Hash, Hash) {
        let account = &self.key.account;
        let mut fields = vec![
            Some(account.venue.clone()),
            Some(account.environment.clone()),
            Some(account.account.clone()),
            account.subaccount.clone(),
            Some(self.key.namespace.clone()),
            Some(self.key.product.clone()),
            Some(self.key.external_id.clone()),
        ];
        let execution_id = hash_fields(&fields);
        fields.extend([
            Some(self.order_id.clone()),
            Some(self.side.clone()),
            Some(self.price.normalize().to_string()),
            Some(self.quantity.normalize().to_string()),
            Some(if self.is_taker { "TAKER" } else { "MAKER" }.into()),
            self.fee_asset.clone(),
            Some(self.reported_fee.normalize().to_string()),
            Some(self.effective_at.to_string()),
        ]);
        (execution_id, hash_fields(&fields))
    }
}

// Explicit null tag plus byte lengths prevent optional/delimiter collisions.
fn hash_fields(fields: &[Option<String>]) -> Hash {
    let mut bytes = Vec::new();
    for field in fields {
        match field {
            None => bytes.push(0),
            Some(value) => {
                bytes.push(1);
                bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
        }
    }
    digest(&SHA256, &bytes)
        .as_ref()
        .try_into()
        .expect("SHA-256 length")
}

#[derive(Clone, Debug)]
struct Candidate {
    template: Template,
    account_version: u64,
    order_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IncompleteInternalReceipt {
    execution_id: Hash,
    payload_digest: Hash,
    event_id: String,
    order_id: String,
    account_before: u64,
    account_after: u64,
    order_before: u64,
    order_after: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum IncompleteInternalResult {
    Committed(IncompleteInternalReceipt),
    Duplicate(IncompleteInternalReceipt),
    Rejected {
        event_id: String,
        order_id: String,
        reason: String,
    },
}

enum Decision {
    Accept,
    Reject(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Gate {
    Running,
    Failed(Fault),
}

/// One engine is the sole financial owner; maps here contain metadata only.
struct ReplayAccountAggregate {
    engine: PyMatchingEngine,
    key: AccountKey,
    state_version: u64,
    order_versions: BTreeMap<String, u64>,
    specs: BTreeMap<String, String>,
    rule: String,
    receipts: BTreeMap<Hash, IncompleteInternalReceipt>,
    gate: Gate,
}

impl ReplayAccountAggregate {
    fn new(
        engine: PyMatchingEngine,
        key: AccountKey,
        specs: BTreeMap<String, String>,
        rule: String,
    ) -> Result<Self, Fault> {
        key.validate()?;
        validate_engine(&engine)?;
        if !engine.account.positions.is_empty()
            || !identity(&rule)
            || specs.is_empty()
            || specs.iter().any(|(k, v)| !identity(k) || !identity(v))
        {
            return Err("UNSUPPORTED_INITIAL_STATE");
        }
        let mut order_versions = BTreeMap::new();
        let mut position_identities = BTreeMap::new();
        for order in &engine.account.open_orders {
            validate_order(order)?;
            let tuple = (order.strategy_id.as_str(), order.product_id.as_str());
            let key = settlement::position_key(tuple.0, tuple.1);
            if position_identities
                .insert(key, tuple)
                .is_some_and(|old| old != tuple)
            {
                return Err("UNSUPPORTED_POSITION_KEY_ALIAS");
            }
            if !specs.contains_key(&order.product_id)
                || order_versions.insert(order.id.clone(), 0).is_some()
            {
                return Err("UNKNOWN_SPEC_OR_DUPLICATE_ORDER");
            }
        }
        Ok(Self {
            engine,
            key,
            state_version: 0,
            order_versions,
            specs,
            rule,
            receipts: BTreeMap::new(),
            gate: Gate::Running,
        })
    }

    fn draft(&self) -> Self {
        let e = &self.engine;
        Self {
            engine: PyMatchingEngine {
                account: e.account.clone(),
                maker_fee: e.maker_fee,
                taker_fee: e.taker_fee,
                market_slippage_bps: e.market_slippage_bps,
                market_slippage_price_tick: e.market_slippage_price_tick,
                contract_multiplier: e.contract_multiplier,
                fee_model: e.fee_model,
                settlement_model: e.settlement_model,
                rejections: e.rejections.clone(),
                warnings: e.warnings.clone(),
                scaled_price_tick: e.scaled_price_tick,
                scaled_volume_step: e.scaled_volume_step,
            },
            key: self.key.clone(),
            state_version: self.state_version,
            order_versions: self.order_versions.clone(),
            specs: self.specs.clone(),
            rule: self.rule.clone(),
            receipts: self.receipts.clone(),
            gate: self.gate.clone(),
        }
    }

    fn duplicate(&self, template: &Template) -> Result<Option<IncompleteInternalReceipt>, Fault> {
        let (id, payload) = template.hashes();
        match self.receipts.get(&id) {
            Some(receipt) if receipt.payload_digest != payload => Err("EXECUTION_ID_CONFLICT"),
            receipt => Ok(receipt.cloned()),
        }
    }

    fn materialize(&self, template: &Template) -> Result<Candidate, Fault> {
        Ok(Candidate {
            template: template.clone(),
            account_version: self.state_version,
            order_version: *self
                .order_versions
                .get(&template.order_id)
                .ok_or("UNKNOWN_ORDER")?,
        })
    }

    fn validate(&self, c: &Candidate) -> Result<usize, Fault> {
        let t = &c.template;
        t.key.account.validate()?;
        validate_engine(&self.engine)?;
        if t.key.account != self.key || !identity(&t.key.namespace) || !identity(&t.key.external_id)
        {
            return Err("IDENTITY_MISMATCH");
        }
        if self.specs.get(&t.key.product) != Some(&t.spec) || t.rule != self.rule {
            return Err("UNKNOWN_SPEC_OR_RULE");
        }
        if c.account_version != self.state_version
            || self.order_versions.get(&t.order_id) != Some(&c.order_version)
        {
            return Err("STALE_VERSION");
        }
        let index = self
            .engine
            .account
            .open_orders
            .iter()
            .position(|o| o.id == t.order_id)
            .ok_or("UNKNOWN_ORDER")?;
        let order = &self.engine.account.open_orders[index];
        validate_order(order)?;
        if order.product_id != t.key.product
            || order.side != t.side
            || t.quantity != order.quantity
            || t.price != order.price
            || t.reported_fee != Decimal::ZERO
            || t.fee_asset.is_some()
            || t.effective_at < 0
        {
            return Err("UNSUPPORTED_EXECUTION");
        }
        if self
            .engine
            .account
            .positions
            .contains_key(&settlement::position_key(
                &order.strategy_id,
                &order.product_id,
            ))
        {
            return Err("POSITION_NOT_ABSENT");
        }
        Ok(index)
    }

    fn prepare(
        &self,
        event: &str,
        c: &Candidate,
    ) -> Result<(Self, IncompleteInternalReceipt), Fault> {
        let mut draft = self.draft();
        let index = draft.validate(c)?;
        let order = draft.engine.account.open_orders[index].clone();
        // Check the exact legacy calculation's intermediate products, even at fee=0.
        if draft.engine.fee_model == FeeModel::PercentageNotional {
            c.template
                .price
                .checked_mul(order.quantity)
                .and_then(|v| v.checked_mul(draft.engine.contract_multiplier))
                .ok_or("ARITHMETIC_OVERFLOW")?;
        }
        let fee = settlement::calculate_fee(
            &draft.engine,
            c.template.price,
            order.quantity,
            c.template.is_taker,
        );
        if fee != Decimal::ZERO {
            return Err("NONZERO_ENGINE_FEE");
        }
        let charged = settlement::settle_fill(
            &mut draft.engine,
            &order,
            c.template.price,
            c.template.is_taker,
            fee,
        )
        .map_err(|_| "SETTLEMENT_ERROR")?;
        if charged != Decimal::ZERO || draft.engine.account.balance != self.engine.account.balance {
            return Err("SETTLEMENT_BALANCE_CHANGED");
        }
        draft.engine.account.open_orders.remove(index);
        draft.state_version = draft
            .state_version
            .checked_add(1)
            .ok_or("VERSION_OVERFLOW")?;
        let order_after = c.order_version.checked_add(1).ok_or("VERSION_OVERFLOW")?;
        draft.order_versions.insert(order.id.clone(), order_after);
        let (execution_id, payload_digest) = c.template.hashes();
        let receipt = IncompleteInternalReceipt {
            execution_id,
            payload_digest,
            event_id: event.into(),
            order_id: order.id,
            account_before: c.account_version,
            account_after: draft.state_version,
            order_before: c.order_version,
            order_after,
        };
        draft.receipts.insert(execution_id, receipt.clone());
        Ok((draft, receipt))
    }

    fn commit_candidate(
        &mut self,
        event: &str,
        c: &Candidate,
    ) -> Result<IncompleteInternalResult, Fault> {
        if self.gate != Gate::Running {
            return Err("RUN_FAILED");
        }
        let prepared = catch_unwind(AssertUnwindSafe(|| {
            if let Some(r) = self.duplicate(&c.template)? {
                return Ok(Err(r));
            }
            self.prepare(event, c).map(Ok)
        }))
        .unwrap_or(Err("INTERNAL_PANIC"));
        match prepared {
            Ok(Err(receipt)) => Ok(IncompleteInternalResult::Duplicate(receipt)),
            Ok(Ok((draft, receipt))) => {
                *self = draft; // The only publication point, including dedup and versions.
                Ok(IncompleteInternalResult::Committed(receipt))
            }
            Err(error) => {
                self.gate = Gate::Failed(error);
                Err(error)
            }
        }
    }

    fn event(
        &mut self,
        event: &str,
        templates: &[Template],
        mut decide: impl FnMut(&Self, &Candidate) -> Result<Decision, Fault>,
        mut stability: impl FnMut(&Self, &IncompleteInternalReceipt) -> Result<(), Fault>,
    ) -> Result<Vec<IncompleteInternalResult>, Fault> {
        if self.gate != Gate::Running {
            return Err("RUN_FAILED");
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            if !identity(event) {
                return Err("INVALID_EVENT");
            }
            let mut results = Vec::new();
            for template in templates {
                // Dedup precedes materialization/order existence and any policy read.
                if let Some(receipt) = self.duplicate(template)? {
                    results.push(IncompleteInternalResult::Duplicate(receipt));
                    continue;
                }
                let candidate = self.materialize(template)?;
                self.validate(&candidate)?;
                match decide(self, &candidate)? {
                    Decision::Reject(reason) => results.push(IncompleteInternalResult::Rejected {
                        event_id: event.into(),
                        order_id: template.order_id.clone(),
                        reason,
                    }),
                    Decision::Accept => {
                        let result = self.commit_candidate(event, &candidate)?;
                        if let IncompleteInternalResult::Committed(receipt) = &result {
                            // Mandatory fixture gate, NOT a risk evaluator or action executor.
                            stability(self, receipt)?;
                        }
                        results.push(result);
                    }
                }
            }
            Ok(results)
        }))
        .unwrap_or(Err("INTERNAL_PANIC"));
        if let Err(error) = result {
            self.gate = Gate::Failed(error);
        }
        result
    }
}

fn validate_engine(e: &PyMatchingEngine) -> Result<(), Fault> {
    if e.settlement_model != SettlementModel::Derivatives
        || e.account.spot_ledger.is_some()
        || e.account.balance < Decimal::ZERO
        || e.maker_fee != Decimal::ZERO
        || e.taker_fee != Decimal::ZERO
        || e.contract_multiplier <= Decimal::ZERO
        || e.account.spot_cost_basis != Decimal::ZERO
        || e.account.spot_realized_pnl != Decimal::ZERO
        || e.market_slippage_bps != Decimal::ZERO
        || e.market_slippage_price_tick.is_some()
        || e.scaled_price_tick.is_some()
        || e.scaled_volume_step.is_some()
        || !e.rejections.is_empty()
        || !e.warnings.is_empty()
    {
        Err("UNSUPPORTED_ENGINE")
    } else {
        Ok(())
    }
}

fn validate_order(o: &Order) -> Result<(), Fault> {
    if !identity(&o.id)
        || !identity(&o.product_id)
        || !identity(&o.strategy_id)
        || !matches!(o.side.as_str(), "LONG" | "SHORT")
        || o.order_type != "LIMIT"
        || o.price <= Decimal::ZERO
        || o.quantity <= Decimal::ZERO
        || o.trigger_price.is_some()
        || o.trailing_distance.is_some()
        || o.linked_order_id.is_some()
    {
        Err("UNSUPPORTED_ORDER")
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::matcher::AccountState;
    use crate::binding::models::Position;
    use std::cell::RefCell;

    fn fixture() -> (ReplayAccountAggregate, Vec<Template>) {
        let key = AccountKey {
            venue: "fixture".into(),
            environment: "test".into(),
            account: "A".into(),
            subaccount: None,
        };
        let mut engine = PyMatchingEngine {
            account: AccountState::new(Decimal::from(1000), None),
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            market_slippage_bps: Decimal::ZERO,
            market_slippage_price_tick: None,
            contract_multiplier: Decimal::ONE,
            fee_model: FeeModel::PercentageNotional,
            settlement_model: SettlementModel::Derivatives,
            rejections: vec![],
            warnings: vec![],
            scaled_price_tick: None,
            scaled_volume_step: None,
        };
        let templates = ["A", "B"]
            .into_iter()
            .map(|id| {
                engine.account.open_orders.push(Order {
                    id: id.into(),
                    product_id: "P".into(),
                    strategy_id: id.into(),
                    side: "LONG".into(),
                    order_type: "LIMIT".into(),
                    price: Decimal::TEN,
                    quantity: Decimal::ONE,
                    timestamp: 0,
                    trigger_price: None,
                    trailing_distance: None,
                    linked_order_id: None,
                });
                Template {
                    key: ExternalExecutionKey {
                        account: key.clone(),
                        namespace: "synthetic-trade-v1".into(),
                        product: "P".into(),
                        external_id: id.into(),
                    },
                    order_id: id.into(),
                    side: "LONG".into(),
                    price: Decimal::TEN,
                    quantity: Decimal::ONE,
                    is_taker: false,
                    fee_asset: None,
                    reported_fee: Decimal::ZERO,
                    effective_at: 1,
                    spec: "s1".into(),
                    rule: "v1".into(),
                }
            })
            .collect();
        (
            ReplayAccountAggregate::new(
                engine,
                key,
                BTreeMap::from([("P".into(), "s1".into())]),
                "v1".into(),
            )
            .unwrap(),
            templates,
        )
    }

    fn snapshot(a: &ReplayAccountAggregate) -> String {
        format!(
            "{:?}",
            (
                a.engine.account.balance,
                a.engine
                    .account
                    .positions
                    .iter()
                    .collect::<BTreeMap<_, _>>(),
                &a.engine.account.open_orders,
                a.state_version,
                &a.order_versions,
                &a.receipts,
                &a.key,
                &a.specs,
                &a.rule
            )
        )
    }

    fn no_risk(a: &ReplayAccountAggregate, r: &IncompleteInternalReceipt) -> Result<(), Fault> {
        assert_eq!(a.state_version, r.account_after);
        assert_eq!(a.engine.account.balance, Decimal::from(1000));
        Ok(())
    }

    fn run(
        a: &mut ReplayAccountAggregate,
        t: &[Template],
    ) -> Result<Vec<IncompleteInternalResult>, Fault> {
        a.event("C", t, |_, _| Ok(Decision::Accept), no_risk)
    }

    #[test]
    fn gt02_rules_read_fresh_state_and_stability_precedes_next_candidate() {
        for limit in [1, 2] {
            let mut repeats = Vec::new();
            for _ in 0..2 {
                let (mut a, mut t) = fixture();
                a.rule = format!("v{limit}");
                t.iter_mut().for_each(|t| t.rule = a.rule.clone());
                let trace = RefCell::new(Vec::new());
                let results = a
                    .event(
                        "C",
                        &t,
                        |a, c| {
                            trace.borrow_mut().push(format!(
                                "read:{}:{}:{}:{}",
                                c.template.order_id,
                                c.account_version,
                                c.order_version,
                                a.engine.account.positions.len()
                            ));
                            assert_eq!(c.account_version, a.state_version);
                            Ok(if a.state_version < limit {
                                Decision::Accept
                            } else {
                                Decision::Reject("EVENT_EXECUTION_LIMIT".into())
                            })
                        },
                        |a, r| {
                            no_risk(a, r)?;
                            trace
                                .borrow_mut()
                                .push(format!("stable:{}", a.state_version));
                            Ok(())
                        },
                    )
                    .unwrap();
                let mut expected = vec!["read:A:0:0:0", "stable:1", "read:B:1:0:1"];
                if limit == 2 {
                    expected.push("stable:2");
                }
                assert_eq!(*trace.borrow(), expected);
                assert_eq!(a.state_version, limit);
                assert_eq!(a.receipts.len(), limit as usize);
                assert_eq!(a.engine.account.open_orders.len(), (2 - limit) as usize);
                assert!(
                    matches!(&results[0], IncompleteInternalResult::Committed(r) if r.account_before == 0 && r.account_after == 1 && r.order_before == 0 && r.order_after == 1)
                );
                if limit == 1 {
                    assert_eq!(
                        results[1],
                        IncompleteInternalResult::Rejected {
                            event_id: "C".into(),
                            order_id: "B".into(),
                            reason: "EVENT_EXECUTION_LIMIT".into()
                        }
                    );
                } else {
                    assert!(
                        matches!(&results[1], IncompleteInternalResult::Committed(r) if r.account_before == 1 && r.account_after == 2)
                    );
                }
                for position in a.engine.account.positions.values() {
                    assert_eq!(
                        (position.quantity, position.entry_price),
                        (Decimal::ONE, Decimal::TEN)
                    );
                }
                repeats.push((snapshot(&a), results));
            }
            assert_eq!(repeats[0], repeats[1]);
        }
    }

    #[test]
    fn stability_failure_preserves_commit_and_never_materializes_second() {
        let (mut a, mut t) = fixture();
        t[1].order_id = "unknown-would-fail-materialization".into();
        let mut reads = 0;
        assert_eq!(
            a.event(
                "C",
                &t,
                |_, _| {
                    reads += 1;
                    Ok(Decision::Accept)
                },
                |_, _| Err("STABILITY_FAILED")
            ),
            Err("STABILITY_FAILED")
        );
        assert_eq!((reads, a.state_version, a.receipts.len()), (1, 1, 1));
        assert_eq!(a.gate, Gate::Failed("STABILITY_FAILED"));
        let before = snapshot(&a);
        assert_eq!(run(&mut a, &t), Err("RUN_FAILED"));
        assert_eq!(snapshot(&a), before);
    }

    #[test]
    fn duplicate_ignores_removed_order_and_stale_versions_but_money_changes_conflict() {
        let changes: &[fn(&mut Template)] = &[
            |t| t.price += Decimal::ONE,
            |t| t.quantity += Decimal::ONE,
            |t| t.reported_fee = Decimal::ONE,
            |t| t.fee_asset = Some("USD".into()),
            |t| t.effective_at += 1,
            |t| t.side = "SHORT".into(),
            |t| t.is_taker = true,
            |t| t.order_id = "different".into(),
        ];
        for change in changes {
            let (mut a, mut t) = fixture();
            let c = a.materialize(&t[0]).unwrap();
            let results = run(&mut a, &t).unwrap();
            let IncompleteInternalResult::Committed(original) = &results[0] else {
                panic!()
            };
            let before = snapshot(&a);
            assert_eq!(
                a.commit_candidate("later-event", &c).unwrap(),
                IncompleteInternalResult::Duplicate(original.clone())
            );
            assert_eq!(snapshot(&a), before);
            change(&mut t[0]);
            assert_eq!(run(&mut a, &t[..1]), Err("EXECUTION_ID_CONFLICT"));
            assert_eq!(snapshot(&a), before);
            assert_eq!(a.gate, Gate::Failed("EXECUTION_ID_CONFLICT"));
        }
    }

    #[test]
    fn canonical_hashes_scope_identity_and_preserve_field_boundaries() {
        let (a, t) = fixture();
        let mut scaled = t[0].clone();
        scaled.price = Decimal::new(1000, 2);
        scaled.quantity = Decimal::new(100, 2);
        scaled.reported_fee = Decimal::new(0, 5);
        assert_eq!(scaled.hashes(), t[0].hashes());
        for product in ["P", "Q"] {
            for account in ["A", "B"] {
                let mut b = a.draft();
                b.key.account = account.into();
                let mut input = t[0].clone();
                input.key.account = b.key.clone();
                input.key.product = product.into();
                b.specs.insert(product.into(), "s1".into());
                b.engine.account.open_orders[0].product_id = product.into();
                run(&mut b, &[input.clone()]).unwrap();
                assert_eq!(b.state_version, 1);
                assert_eq!(
                    input.hashes().0 == t[0].hashes().0,
                    product == "P" && account == "A"
                );
            }
        }
        assert_ne!(
            hash_fields(&[Some("a:b".into()), Some("c".into())]),
            hash_fields(&[Some("a".into()), Some("b:c".into())])
        );
        assert_ne!(hash_fields(&[None]), hash_fields(&[Some("null".into())]));
    }

    #[test]
    fn initial_admission_rejects_unsupported_state_without_mutation() {
        let mutations: &[fn(&mut PyMatchingEngine)] = &[
            |e| e.account.balance = -Decimal::ONE,
            |e| e.maker_fee = Decimal::ONE,
            |e| e.taker_fee = Decimal::ONE,
            |e| e.settlement_model = SettlementModel::CashSpot,
            |e| e.contract_multiplier = Decimal::ZERO,
            |e| e.account.open_orders.push(e.account.open_orders[0].clone()),
            |e| e.account.open_orders[0].id = " A ".into(),
            |e| e.account.open_orders[0].side = "buy".into(),
            |e| e.account.open_orders[0].price = Decimal::ZERO,
            |e| e.account.open_orders[0].quantity = -Decimal::ONE,
            |e| e.account.open_orders[0].quantity = Decimal::ZERO,
            |e| e.account.open_orders[0].price = -Decimal::ONE,
            |e| e.account.open_orders[0].id.clear(),
            |e| e.account.open_orders[0].strategy_id.clear(),
            |e| e.market_slippage_bps = Decimal::ONE,
            |e| e.market_slippage_price_tick = Some(Decimal::ONE),
            |e| e.scaled_price_tick = Some(Decimal::ONE),
            |e| e.scaled_volume_step = Some(Decimal::ONE),
            |e| e.account.spot_cost_basis = Decimal::ONE,
            |e| e.account.spot_realized_pnl = Decimal::ONE,
            |e| e.rejections.push(Default::default()),
            |e| e.warnings.push(Default::default()),
            |e| e.account.open_orders[0].order_type = "MARKET".into(),
            |e| e.account.open_orders[0].order_type = "STOP_LOSS".into(),
            |e| e.account.open_orders[0].order_type = "TAKE_PROFIT".into(),
            |e| e.account.open_orders[0].order_type = "TRAILING_STOP".into(),
            |e| e.account.open_orders[0].trigger_price = Some(Decimal::ONE),
            |e| e.account.open_orders[0].trailing_distance = Some(Decimal::ONE),
            |e| e.account.open_orders[0].linked_order_id = Some("OCO".into()),
            |e| e.account.open_orders[0].product_id = "unknown".into(),
        ];
        for mutate in mutations {
            let (mut a, _) = fixture();
            mutate(&mut a.engine);
            let before = snapshot(&a);
            assert!(ReplayAccountAggregate::new(
                a.draft().engine,
                a.key.clone(),
                a.specs.clone(),
                a.rule.clone()
            )
            .is_err());
            assert_eq!(snapshot(&a), before);
        }
    }

    #[test]
    fn candidate_fault_matrix_preserves_prior_commit_and_drops_current_draft() {
        let mutations: &[fn(&mut ReplayAccountAggregate, &mut Candidate)] = &[
            |_, c| c.account_version = 0,
            |_, c| c.order_version = 7,
            |_, c| c.template.order_id = "missing".into(),
            |_, c| c.template.spec = "unknown".into(),
            |_, c| c.template.rule = "unknown".into(),
            |_, c| c.template.key.account.account = "B".into(),
            |_, c| c.template.quantity = Decimal::new(5, 1),
            |_, c| c.template.quantity = Decimal::from(2),
            |_, c| c.template.price = Decimal::ZERO,
            |_, c| c.template.reported_fee = Decimal::ONE,
            |_, c| c.template.effective_at = -1,
            |_, c| c.template.key.namespace.clear(),
            |_, c| c.template.key.external_id.clear(),
            |_, c| c.template.key.account.subaccount = Some(String::new()),
            |a, _| a.engine.account.open_orders[0].strategy_id = "A".into(),
            |a, _| a.engine.account.balance = -Decimal::ONE,
            |a, _| a.engine.taker_fee = Decimal::ONE,
            |a, c| {
                a.state_version = u64::MAX;
                c.account_version = u64::MAX;
            },
            |a, c| {
                a.order_versions.insert("B".into(), u64::MAX);
                c.order_version = u64::MAX;
            },
            |a, c| {
                a.engine.account.open_orders[0].price = Decimal::MAX;
                c.template.price = Decimal::MAX;
                a.engine.contract_multiplier = Decimal::from(2);
            },
        ];
        for mutate in mutations {
            let (mut a, t) = fixture();
            run(&mut a, &t[..1]).unwrap();
            let mut c = a.materialize(&t[1]).unwrap();
            mutate(&mut a, &mut c);
            let before = snapshot(&a);
            assert!(a.commit_candidate("C", &c).is_err());
            assert_eq!(snapshot(&a), before);
            assert!(matches!(a.gate, Gate::Failed(_)));
            assert_eq!(a.receipts.len(), 1);
        }
    }

    #[test]
    fn nonempty_positions_and_internal_panics_fail_closed() {
        let (mut a, t) = fixture();
        a.engine.account.positions.insert(
            "A:P".into(),
            Position {
                product_id: "P".into(),
                strategy_id: "A".into(),
                side: "LONG".into(),
                quantity: Decimal::ONE,
                entry_price: Decimal::TEN,
                unrealized_pnl: Decimal::ZERO,
            },
        );
        assert!(ReplayAccountAggregate::new(
            a.draft().engine,
            a.key.clone(),
            a.specs.clone(),
            a.rule.clone()
        )
        .is_err());
        let before = snapshot(&a);
        assert_eq!(run(&mut a, &t), Err("POSITION_NOT_ABSENT"));
        assert_eq!(snapshot(&a), before);
        let (mut a, t) = fixture();
        let before = snapshot(&a);
        assert_eq!(
            a.event("C", &t, |_, _| panic!("internal decision failure"), no_risk),
            Err("INTERNAL_PANIC")
        );
        assert_eq!(snapshot(&a), before);
    }

    #[test]
    fn zero_balance_short_taker_and_per_contract_are_supported_without_fee_authority() {
        let (mut a, mut t) = fixture();
        a.engine.account.balance = Decimal::ZERO;
        a.engine.fee_model = FeeModel::PerContract;
        a.engine.account.open_orders[0].side = "SHORT".into();
        t[0].side = "SHORT".into();
        t[0].is_taker = true;
        a.event(
            "C",
            &t[..1],
            |_, _| Ok(Decision::Accept),
            |a, _| {
                assert_eq!(a.engine.account.balance, Decimal::ZERO);
                assert_eq!(a.engine.account.positions["A:P"].side, "SHORT");
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn actual_legacy_arithmetic_panic_in_gate_is_contained_after_commit() {
        let (mut a, t) = fixture();
        let mut reads = 0;
        let result = a.event(
            "C",
            &t,
            |_, _| {
                reads += 1;
                Ok(Decision::Accept)
            },
            |a, _| {
                // Fault injection uses the real legacy operation, not a fake settlement.
                let mut fault = a.draft();
                fault.engine.contract_multiplier = Decimal::from(2);
                settlement::calculate_fee(&fault.engine, Decimal::MAX, Decimal::ONE, false);
                Ok(())
            },
        );
        assert_eq!(result, Err("INTERNAL_PANIC"));
        assert_eq!((reads, a.state_version, a.receipts.len()), (1, 1, 1));
        assert_eq!(a.engine.account.balance, Decimal::from(1000));
        assert_eq!(a.engine.account.open_orders[0].id, "B");
        assert_eq!(a.gate, Gate::Failed("INTERNAL_PANIC"));
    }

    #[test]
    fn prepared_settlement_is_invisible_until_swap_and_product_dedup_is_separate() {
        let (mut a, mut t) = fixture();
        let candidate = a.materialize(&t[0]).unwrap();
        let before = snapshot(&a);
        let (draft, receipt) = a.prepare("C", &candidate).unwrap();
        assert_eq!(draft.engine.account.positions["A:P"].quantity, Decimal::ONE);
        assert_eq!(draft.engine.account.open_orders.len(), 1);
        assert_eq!(draft.receipts.len(), 1);
        assert_eq!(receipt.account_after, 1);
        drop(draft);
        assert_eq!(snapshot(&a), before);
        a.specs.insert("Q".into(), "s1".into());
        a.engine.account.open_orders[1].product_id = "Q".into();
        t[1].key.product = "Q".into();
        t[1].key.external_id = t[0].key.external_id.clone();
        run(&mut a, &t).unwrap();
        assert_eq!((a.state_version, a.receipts.len()), (2, 2));
        assert_ne!(t[0].hashes().0, t[1].hashes().0);
    }

    #[test]
    fn invalid_initial_identity_and_versions_are_not_defaulted() {
        for field in 0..6 {
            let (mut a, _) = fixture();
            match field {
                0 => a.key.account.clear(),
                1 => a.key.venue = " venue ".into(),
                2 => a.key.subaccount = Some(String::new()),
                3 => a.rule.clear(),
                4 => a.specs.clear(),
                _ => {
                    a.specs.insert("P".into(), String::new());
                }
            }
            assert!(ReplayAccountAggregate::new(a.engine, a.key, a.specs, a.rule).is_err());
        }
    }

    #[test]
    fn initialization_rejects_distinct_tuple_aliases_without_mutation() {
        for (left, right, alias) in [
            (("A:B", "C"), ("A", "B:C"), true),
            (("A", "C"), ("B", "C"), false),
            (("A", "C"), ("A", "C"), false),
        ] {
            let (mut a, _) = fixture();
            for (order, tuple) in a.engine.account.open_orders.iter_mut().zip([left, right]) {
                order.strategy_id = tuple.0.into();
                order.product_id = tuple.1.into();
                a.specs.insert(tuple.1.into(), "s1".into());
            }
            let before = snapshot(&a);
            let draft = a.draft();
            let result =
                ReplayAccountAggregate::new(draft.engine, draft.key, draft.specs, draft.rule);
            assert_eq!(
                result.err(),
                alias.then_some("UNSUPPORTED_POSITION_KEY_ALIAS")
            );
            assert_eq!(snapshot(&a), before);
        }
    }
}
