//! Single private admission writer with closed capacity and BTC/ETH policies.
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[cfg(test)]
pub(super) fn admit_execution_fixture(
    owner: &mut ScenarioAccount,
    side: Side,
    quantity: Decimal,
    price: Decimal,
) -> String {
    let intent = fixture_intent(owner, "execution-fixture", side, quantity, price);
    let reply = owner
        .admit(&Envelope {
            event_id: "admission",
            effective_at: 500,
            intent: &intent,
        })
        .unwrap();
    assert_eq!(reply.kind, ReplyKind::Accepted);
    reply.result.order_id.unwrap()
}

#[cfg(test)]
pub(super) fn fixture_intent(
    owner: &ScenarioAccount,
    id: &str,
    side: Side,
    quantity: Decimal,
    price: Decimal,
) -> OrderIntent {
    OrderIntent {
        intent_id: id.into(),
        client_order_id: "client".into(),
        account_key: owner.key.clone(),
        config_id: owner.config_id.clone(),
        product: if matches!(owner.profile, ProfileContext::GoldenCancel(_)) {
            ProfileProduct::Pa
        } else {
            ProfileProduct::BtcEth(Product::Btc)
        },
        strategy_id: "strategy".into(),
        side,
        order_type: OrderType::Limit,
        quantity,
        limit_price: Some(price),
        reduce_only: false,
        requested_at: 500,
    }
}

mod btc_policy;
#[cfg(test)]
pub(super) mod inspection_tests;
pub(super) mod wire;

#[cfg(test)]
pub(super) fn fixture_admit(
    owner: &mut ScenarioAccount,
    event_id: &str,
    effective_at: i64,
    intent: &OrderIntent,
) -> Result<(&'static str, AdmissionResult), Fault> {
    owner
        .admit(&Envelope {
            event_id,
            effective_at,
            intent,
        })
        .map(|r| {
            (
                match r.kind {
                    ReplyKind::Accepted => "accepted",
                    ReplyKind::Rejected => "rejected",
                    ReplyKind::Duplicate => "duplicate",
                },
                r.result,
            )
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OrderType {
    Limit,
    Market,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrderIntent {
    intent_id: String,
    client_order_id: String,
    account_key: AccountKey,
    config_id: String,
    product: ProfileProduct,
    strategy_id: String,
    side: Side,
    order_type: OrderType,
    quantity: Decimal,
    limit_price: Option<Decimal>,
    reduce_only: bool,
    requested_at: i64,
}

impl OrderIntent {
    #[cfg(test)]
    pub(super) fn historical_market_fixture(
        owner: &ScenarioAccount,
        intent_id: String,
        client_order_id: String,
        product_id: &'static str,
        side: Side,
        quantity: Decimal,
        limit_price: Option<Decimal>,
    ) -> Self {
        Self {
            intent_id,
            client_order_id,
            account_key: owner.key.clone(),
            config_id: owner.config_id.clone(),
            product: ProfileProduct::BtcEth(Product(product_id.into())),
            strategy_id: "historical-market-test".into(),
            side,
            order_type: OrderType::Market,
            quantity,
            limit_price,
            reduce_only: false,
            requested_at: 1_000,
        }
    }

    pub(super) fn wire_product(&self) -> &ProfileProduct {
        &self.product
    }
    pub(super) fn account_key(&self) -> &AccountKey {
        &self.account_key
    }
    pub(super) fn preflight_identity(&self, owner: &ScenarioAccount) -> Result<(), Fault> {
        if owner
            .intent_results
            .get(&self.intent_id)
            .is_some_and(|receipt| receipt.canonical_payload_digest != self.digest())
        {
            return Err("IDEMPOTENCY_KEY_CONFLICT");
        }
        Ok(())
    }
    pub(super) fn group_identity(&self) -> (&str, Hash) {
        (&self.intent_id, self.digest())
    }
    fn digest(&self) -> Hash {
        hash_fields(&[
            Some("scenario-intent-v1".into()),
            Some(self.intent_id.clone()),
            Some(self.client_order_id.clone()),
            Some(self.account_key.venue.clone()),
            Some(self.account_key.environment.clone()),
            Some(self.account_key.account.clone()),
            self.account_key.subaccount.clone(),
            Some(self.config_id.clone()),
            Some(self.product.canonical_id().into()),
            Some(self.strategy_id.clone()),
            Some(
                match self.side {
                    Side::Long => "LONG",
                    Side::Short => "SHORT",
                }
                .into(),
            ),
            Some(
                match self.order_type {
                    OrderType::Limit => "LIMIT",
                    OrderType::Market => "MARKET",
                    OrderType::Unsupported => "UNSUPPORTED",
                }
                .into(),
            ),
            Some(self.quantity.normalize().to_string()),
            self.limit_price.map(|v| v.normalize().to_string()),
            Some(self.reduce_only.to_string()),
            Some(self.requested_at.to_string()),
        ])
    }

    fn order_id(&self) -> String {
        let digest = hash_fields(&[
            Some("scenario-order-v1".into()),
            Some(format!("{:02x?}", self.digest())),
        ]);
        format!("scenario-order-v1:{digest:02x?}")
    }
}

struct Envelope<'a> {
    event_id: &'a str,
    effective_at: i64,
    intent: &'a OrderIntent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Accepted,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Evaluation {
    MinCash(Decimal),
    GoldenCancel(Decimal),
    GoldenCapacity(capacity::CapacityProjection),
    BtcEth(btc_policy::Evidence),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AdmissionResult {
    accepted_order: Option<SeedOrder>,
    intent_id: String,
    canonical_payload_digest: Hash,
    outcome: Outcome,
    reason_code: Option<Fault>,
    order_id: Option<String>,
    account_version_before: u64,
    account_version_after: u64,
    order_version_before: Option<u64>,
    order_version_after: Option<u64>,
    spec_version: String,
    rule_data_version: String,
    reservation_after: Option<Evaluation>,
    evaluation: Evaluation,
    created_at_event_id: String,
    order_type: OrderType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplyKind {
    Accepted,
    Rejected,
    Duplicate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Reply {
    kind: ReplyKind,
    result: AdmissionResult,
}

impl ScenarioAccount {
    /// Projects only owner-held working orders and proves historical admission identity.
    pub(super) fn historical_working_orders(
        &self,
    ) -> Result<Vec<super::historical::WorkingOrderMeta>, Fault> {
        const HISTORICAL: &str = "HISTORICAL_ORDER_V1";
        if !matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some())
        {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        }
        if self.seed_orders.len() != self.seed_intents.len()
            || self
                .seed_orders
                .iter()
                .any(|id| !self.orders.contains_key(id))
        {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        }
        let seed_sequences = self
            .seed_orders
            .iter()
            .enumerate()
            .map(|(index, id)| {
                Ok((
                    id.as_str(),
                    i64::try_from(index + 1).map_err(|_| "NATIVE_INVARIANT")?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Fault>>()?;
        let mut projection = Vec::new();
        for (order_id, order) in &self.orders {
            if !order
                .facts
                .projects_remainder("INVALID_HISTORICAL_ORDER_SNAPSHOT")?
                || matches!(
                    order.cancel,
                    risk_transition::cancel::State::EffectiveCanceled(_)
                )
            {
                continue;
            }
            let facts = &order.facts;
            if let Some(sequence) = seed_sequences.get(order_id.as_str()) {
                if order.created_at != self.seed_effective_at
                    || facts.order_id != *order_id
                    || !self.seed_intents.contains(&facts.intent_id)
                    || facts.product.btc().is_err()
                {
                    return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
                }
                projection.push(super::historical::WorkingOrderMeta {
                    order_id: order_id.clone(),
                    product: facts.product.btc()?.clone(),
                    order_version: order.version,
                    status: facts.status.clone(),
                    remaining: facts.remaining,
                    accepted_at: self.seed_effective_at,
                    accepted_source_sequence: *sequence,
                    kind: super::historical::OrderKind::Limit,
                    side: facts.side,
                    limit_price: Some(facts.price),
                    risk_cancel_pending: matches!(
                        order.cancel,
                        risk_transition::cancel::State::Requested(ref request)
                            if matches!(request.reason, risk_transition::cancel::Reason::RiskShortfall | risk_transition::cancel::Reason::MmrBreach)
                    ),
                });
                continue;
            }
            let admission = self
                .intent_results
                .get(&facts.intent_id)
                .ok_or("INVALID_HISTORICAL_ORDER_SNAPSHOT")?;
            let accepted = admission
                .accepted_order
                .as_ref()
                .ok_or("INVALID_HISTORICAL_ORDER_SNAPSHOT")?;
            if admission.outcome != Outcome::Accepted
                || admission.order_id.as_deref() != Some(order_id.as_str())
                || accepted.intent_id != facts.intent_id
                || accepted.order_id != facts.order_id
                || accepted.client_id != facts.client_id
                || accepted.product != facts.product
                || accepted.side != facts.side
                || accepted.price != facts.price
                || accepted.original != facts.original
            {
                return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
            }
            let event_id = &admission.created_at_event_id;
            let (stamp, _) = self
                .transition
                .events
                .get(event_id)
                .ok_or("INVALID_HISTORICAL_ORDER_SNAPSHOT")?;
            if stamp.event_id != *event_id
                || stamp.ordering_contract_id != HISTORICAL
                || self.transition.event_kinds.get(event_id) != Some(&source::Kind::Intent)
                || stamp.source_sequence.is_none()
                || stamp.effective_at != order.created_at
            {
                return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
            }
            projection.push(super::historical::WorkingOrderMeta {
                order_id: order_id.clone(),
                product: facts.product.btc()?.clone(),
                order_version: order.version,
                status: facts.status.clone(),
                remaining: facts.remaining,
                accepted_at: order.created_at,
                accepted_source_sequence: stamp
                    .source_sequence
                    .ok_or("INVALID_HISTORICAL_ORDER_SNAPSHOT")?,
                kind: match admission.order_type() {
                    OrderType::Limit => super::historical::OrderKind::Limit,
                    OrderType::Market => super::historical::OrderKind::Market,
                    OrderType::Unsupported => return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT"),
                },
                side: facts.side,
                limit_price: (admission.order_type() == OrderType::Limit).then_some(facts.price),
                risk_cancel_pending: matches!(
                    order.cancel,
                    risk_transition::cancel::State::Requested(ref request)
                        if matches!(request.reason, risk_transition::cancel::Reason::RiskShortfall | risk_transition::cancel::Reason::MmrBreach)
                ),
            });
        }
        projection.sort_by(|left, right| {
            (
                left.accepted_at,
                left.accepted_source_sequence,
                &left.order_id,
            )
                .cmp(&(
                    right.accepted_at,
                    right.accepted_source_sequence,
                    &right.order_id,
                ))
        });
        Ok(projection)
    }

    pub(super) fn encode_inspection_intents(
        &self,
        e: &mut risk_transition::cancel::identity::Encoding,
    ) -> Result<(), Fault> {
        use inspection::{number, order_facts};
        number(e, self.intent_results.len())?;
        for (key, r) in &self.intent_results {
            e.text(key);
            e.text(&r.intent_id);
            e.hash(r.canonical_payload_digest);
            e.text(match r.outcome {
                Outcome::Accepted => "ACCEPTED",
                Outcome::Rejected => "REJECTED",
            });
            e.optional_text(r.reason_code);
            e.optional_text(r.order_id.as_deref());
            number(e, r.account_version_before)?;
            number(e, r.account_version_after)?;
            for version in [r.order_version_before, r.order_version_after] {
                e.presence(version.is_some());
                if let Some(v) = version {
                    number(e, v)?;
                }
            }
            e.text(&r.spec_version);
            e.text(&r.rule_data_version);
            e.text(&r.created_at_event_id);
            if r.order_type == OrderType::Market {
                e.text("MARKET");
            }
            e.presence(r.accepted_order.is_some());
            if let Some(order) = &r.accepted_order {
                order_facts(e, order)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrepareStage {
    Validated,
    OrderDrafted,
    ResultDrafted,
}

struct Prepared {
    draft: ScenarioAccount,
    result: AdmissionResult,
}

impl ScenarioAccount {
    pub(super) fn group_admit(
        &mut self,
        stamp: &risk_transition::cancel::Stamp,
        intent: &OrderIntent,
    ) -> Result<(Option<Fault>, String), Fault> {
        let result = self
            .admit_stamped_checked(
                &Envelope {
                    event_id: &stamp.event_id,
                    effective_at: stamp.effective_at,
                    intent,
                },
                stamp,
                |_| Ok(()),
            )?
            .result;
        Ok((result.reason_code, result.created_at_event_id))
    }
    fn admit(&mut self, envelope: &Envelope<'_>) -> Result<Reply, Fault> {
        self.admit_checked(envelope, |_| Ok(()))
    }

    // The hook can only interrupt preparation; it cannot access/publish the owner.
    fn admit_checked(
        &mut self,
        envelope: &Envelope<'_>,
        hook: impl FnMut(PrepareStage) -> Result<(), Fault>,
    ) -> Result<Reply, Fault> {
        let stamp = source::stamp(envelope.event_id, envelope.effective_at, 60);
        self.admit_stamped_checked(envelope, &stamp, hook)
    }

    fn admit_stamped_checked(
        &mut self,
        envelope: &Envelope<'_>,
        stamp: &risk_transition::cancel::Stamp,
        hook: impl FnMut(PrepareStage) -> Result<(), Fault>,
    ) -> Result<Reply, Fault> {
        if self.seed_intents.contains(&envelope.intent.intent_id) {
            return Err(self.source_failure("SEED_IDENTITY_CONFLICT"));
        }
        if let Some(original) = self.intent_results.get(&envelope.intent.intent_id) {
            if original.canonical_payload_digest == envelope.intent.digest() {
                return Ok(Reply {
                    kind: ReplyKind::Duplicate,
                    result: original.clone(),
                });
            }
            return Err(self.source_failure("IDEMPOTENCY_KEY_CONFLICT"));
        }
        self.source_boundary(stamp, envelope.intent.digest(), source::Kind::Intent)
            .map_err(|f| self.source_failure(f))?;
        let historical = stamp.ordering_contract_id == "HISTORICAL_ORDER_V1"
            && matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some());
        let prepared = catch_unwind(AssertUnwindSafe(|| {
            self.prepare_admission(envelope, historical, hook)
        }))
        .map_err(|_| "ADMISSION_PANIC")
        .and_then(|result| result);
        let mut prepared = match prepared {
            Ok(prepared) => prepared,
            Err(fault) => {
                return Err(self.source_failure(fault));
            }
        };
        let kind = match prepared.result.outcome {
            Outcome::Accepted => {
                prepared.draft.publish_source(
                    stamp,
                    envelope.intent.digest(),
                    source::Kind::Intent,
                    true,
                );
                *self = prepared.draft;
                ReplyKind::Accepted
            }
            Outcome::Rejected => {
                self.intent_results
                    .insert(prepared.result.intent_id.clone(), prepared.result.clone());
                self.publish_source(stamp, envelope.intent.digest(), source::Kind::Intent, false);
                ReplyKind::Rejected
            }
        };
        Ok(Reply {
            kind,
            result: prepared.result,
        })
    }

    fn prepare_admission(
        &self,
        envelope: &Envelope<'_>,
        historical: bool,
        mut hook: impl FnMut(PrepareStage) -> Result<(), Fault>,
    ) -> Result<Prepared, Fault> {
        let intent = envelope.intent;
        let mut draft = self.clone();
        intent.account_key.validate()?;
        if intent.account_key != self.key {
            return Err("ACCOUNT_MISMATCH");
        }
        if intent.config_id != self.config_id {
            return Err("CONFIG_MISMATCH");
        }
        if [
            &intent.intent_id,
            &intent.client_order_id,
            &intent.strategy_id,
            &intent.config_id,
            envelope.event_id,
        ]
        .iter()
        .any(|id| !identity(id))
        {
            return Err("INVALID_ADMISSION_IDENTITY");
        }
        let order_id = intent.order_id();
        draft.guard_new_identity(NewIdentity::Order(&order_id))?;
        if draft.orders.contains_key(&order_id) {
            return Err("ORDER_ID_CONFLICT");
        }
        self.validate_context(envelope.effective_at)?;
        if intent.order_type == OrderType::Unsupported
            || (intent.order_type == OrderType::Market && !historical)
        {
            return Err("UNSUPPORTED_ORDER_TYPE");
        }
        let price = match intent.order_type {
            OrderType::Limit => intent.limit_price.ok_or("LIMIT_PRICE_REQUIRED")?,
            OrderType::Market => {
                if intent.limit_price.is_some() {
                    return Err("INVALID_BTC_INTENT");
                }
                super::historical::admission_reference_price(
                    self,
                    intent.product.btc()?,
                    intent.side,
                    envelope.effective_at,
                )?
            }
            OrderType::Unsupported => return Err("UNSUPPORTED_ORDER_TYPE"),
        };
        let (spec_version, rule_data_version) = match &self.profile {
            ProfileContext::P1O03 => {
                if intent.product != ProfileProduct::Pa
                    || intent.reduce_only
                    || intent.quantity <= Decimal::ZERO
                    || price <= Decimal::ZERO
                    || !aligned(intent.quantity, Decimal::ONE)
                    || !aligned(price, Decimal::ONE)
                {
                    return Err("INVALID_CAPACITY_CANDIDATE");
                }
                ("gt03-spec-v1".into(), "gt03-rule-v1".into())
            }
            ProfileContext::GoldenCancel(config) => {
                config.validate(envelope.effective_at)?;
                if intent.product != ProfileProduct::Pa
                    || intent.side != Side::Long
                    || intent.quantity != Decimal::TEN
                    || price != Decimal::TEN
                    || intent.reduce_only
                    || !self.orders.is_empty()
                {
                    return Err("INVALID_GOLDEN_CANCEL_INTENT");
                }
                ("gt03-spec-v1".into(), "gt03-rule-v1".into())
            }
            ProfileContext::EventLimit(_) => return Err("UNSUPPORTED_ADMISSION_PROFILE"),
            ProfileContext::GoldenCapacity(config) => {
                if intent.product != ProfileProduct::Pa {
                    return Err("PROFILE_MISMATCH");
                }
                if intent.reduce_only {
                    return Err("UNSUPPORTED_ADMISSION_ROLE");
                }
                (
                    config.spec_version.clone(),
                    config.rule_data_version.clone(),
                )
            }
            ProfileContext::BtcEthScenario { scenario, .. } => {
                let (spec, tier) =
                    scenario.resolve(intent.product.btc()?, envelope.effective_at)?;
                if intent.quantity < spec.minimum
                    || !aligned(intent.quantity, spec.lot)
                    || price <= Decimal::ZERO
                    || !aligned(price, spec.tick)
                {
                    return Err("INVALID_BTC_INTENT");
                }
                (spec.version.clone(), tier.version.clone())
            }
        };
        hook(PrepareStage::Validated)?;
        let below_floor = matches!(
            self.profile,
            ProfileContext::BtcEthScenario {
                min_cash_profile: true,
                ..
            }
        ) && self.cash < Decimal::from(995);
        if !below_floor {
            draft.orders.insert(
                order_id.clone(),
                RestingOrder {
                    created_at: envelope.effective_at,
                    version: 1,
                    cancel: risk_transition::cancel::State::None,
                    facts: SeedOrder {
                        intent_id: intent.intent_id.clone(),
                        order_id: order_id.clone(),
                        client_id: intent.client_order_id.clone(),
                        strategy_id: intent.strategy_id.clone(),
                        product: intent.product.clone(),
                        side: intent.side,
                        price,
                        reduce_only: intent.reduce_only,
                        original: intent.quantity,
                        filled: Decimal::ZERO,
                        canceled: Decimal::ZERO,
                        remaining: intent.quantity,
                        status: "OPEN".into(),
                    },
                },
            );
        }
        hook(PrepareStage::OrderDrafted)?;
        let (evaluation, reason_code) = if below_floor {
            (Evaluation::MinCash(self.cash), Some("MIN_CASH"))
        } else {
            match &self.profile {
                ProfileContext::GoldenCancel(_) => (
                    Evaluation::GoldenCancel(draft.golden_cancel_reservation()?),
                    None,
                ),
                ProfileContext::EventLimit(_) => return Err("UNSUPPORTED_ADMISSION_PROFILE"),
                ProfileContext::GoldenCapacity(_) | ProfileContext::P1O03 => {
                    let projection = self.capacity_projection(&capacity::Candidate {
                        product: intent.product.clone(),
                        quantity: intent.quantity,
                        price,
                    })?;
                    let reason =
                        (projection.required > projection.threshold).then_some("CAPACITY_EXCEEDED");
                    (Evaluation::GoldenCapacity(projection), reason)
                }
                ProfileContext::BtcEthScenario { .. } => {
                    let (evidence, reason) = btc_policy::evaluate(self, &draft, intent, price)?;
                    (Evaluation::BtcEth(evidence), reason)
                }
            }
        };
        let accepted = reason_code.is_none();
        if accepted {
            draft.state_version = self
                .state_version
                .checked_add(1)
                .ok_or("VERSION_OVERFLOW")?;
        }
        let result = AdmissionResult {
            accepted_order: accepted.then(|| draft.orders[&order_id].facts.clone()),
            intent_id: intent.intent_id.clone(),
            canonical_payload_digest: intent.digest(),
            outcome: if accepted {
                Outcome::Accepted
            } else {
                Outcome::Rejected
            },
            reason_code,
            order_id: accepted.then_some(order_id),
            account_version_before: self.state_version,
            account_version_after: draft.state_version,
            order_version_before: None,
            order_version_after: accepted.then_some(1),
            spec_version,
            rule_data_version,
            reservation_after: accepted.then(|| evaluation.clone()),
            evaluation,
            created_at_event_id: envelope.event_id.into(),
            order_type: intent.order_type,
        };
        draft
            .intent_results
            .insert(intent.intent_id.clone(), result.clone());
        hook(PrepareStage::ResultDrafted)?;
        Ok(Prepared { draft, result })
    }
}

impl AdmissionResult {
    pub(super) fn order_type(&self) -> OrderType {
        self.order_type
    }

    pub(super) fn delivery_order(&self, event: &str) -> Option<&SeedOrder> {
        (self.created_at_event_id == event)
            .then_some(self.accepted_order.as_ref())
            .flatten()
    }

    pub(super) fn created_at_event_id(&self) -> &str {
        &self.created_at_event_id
    }

    pub(super) fn order_id(&self) -> Option<&str> {
        self.order_id.as_deref()
    }
}

impl ScenarioAccount {
    pub(super) fn admitted_order_type(&self, facts: &SeedOrder) -> OrderType {
        self.intent_results
            .get(&facts.intent_id)
            .map(AdmissionResult::order_type)
            .unwrap_or(OrderType::Limit)
    }
}

#[cfg(test)]
#[path = "configured_leverage_tests.rs"]
mod configured_leverage_tests;
#[cfg(test)]
mod golden_cancel_tests;
#[cfg(test)]
mod min_cash_tests;
#[cfg(test)]
mod tests;
