use super::*;

impl ScenarioAccount {
    pub(in crate::binding::matcher::replay_transition::synthetic_risk::scenario_account) fn historical_order_binding(
        &self,
        event_id: &str,
        source_sequence: i64,
    ) -> Result<Option<(String, String, String, String)>, Fault> {
        const HISTORICAL: &str = "HISTORICAL_ORDER_V1";
        if !matches!(self.profile, ProfileContext::BtcEthScenario { ref scenario, .. } if scenario.configured.is_some())
        {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        }
        let Some((stamp, _)) = self.transition.events.get(event_id) else {
            return Ok(None);
        };
        if stamp.event_id != event_id
            || stamp.ordering_contract_id != HISTORICAL
            || stamp.source_sequence != Some(source_sequence)
            || self.transition.event_kinds.get(event_id) != Some(&source::Kind::Intent)
        {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        }
        let mut matches = self
            .intent_results
            .values()
            .filter(|result| result.created_at_event_id() == event_id);
        let Some(result) = matches.next() else {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        };
        if matches.next().is_some() {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        }
        let Some(order) = result.delivery_order(event_id) else {
            return Ok(None);
        };
        let order_id = result
            .order_id()
            .ok_or("INVALID_HISTORICAL_ORDER_SNAPSHOT")?;
        if order.order_id != order_id || order.client_id.is_empty() || order.strategy_id.is_empty()
        {
            return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
        }
        Ok(Some((
            order.client_id.clone(),
            order.order_id.clone(),
            order.product.canonical_id().to_owned(),
            order.strategy_id.clone(),
        )))
    }

    fn historical_meta_matches(
        &self,
        meta: &WorkingOrderMeta,
        facts: &SeedOrder,
    ) -> Result<bool, Fault> {
        let (kind, limit_price) = match self.admitted_order_type(facts) {
            admission::OrderType::Limit => (OrderKind::Limit, Some(facts.price)),
            admission::OrderType::Market => (OrderKind::Market, None),
            admission::OrderType::Unsupported => return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT"),
        };
        Ok(
            facts.projects_remainder("INVALID_HISTORICAL_ORDER_SNAPSHOT")?
                && meta.status == facts.status
                && meta.remaining == facts.remaining
                && meta.side == facts.side
                && meta.product == *facts.product.btc()?
                && meta.kind == kind
                && meta.limit_price == limit_price,
        )
    }

    fn historical_cancel_projection_matches(
        &self,
        meta: &WorkingOrderMeta,
        order: &RestingOrder,
    ) -> Result<bool, Fault> {
        use risk_transition::cancel::State;

        let cancel_pending = |reason: risk_transition::cancel::Reason| {
            matches!(
                reason,
                risk_transition::cancel::Reason::RiskShortfall
                    | risk_transition::cancel::Reason::MmrBreach
            )
        };
        match &order.cancel {
            State::None => Ok(order.version == meta.order_version
                && self.historical_meta_matches(meta, &order.facts)?
                && !meta.risk_cancel_pending),
            State::Requested(request) => Ok((order.version == meta.order_version
                || order.version == meta.order_version.saturating_add(1))
                && self.historical_meta_matches(meta, &order.facts)?
                && meta.risk_cancel_pending == cancel_pending(request.reason)),
            State::EffectiveCanceled(effect) => {
                let Some(receipt) = self
                    .cancel_facts
                    .historical_effect_receipt(&effect.action_id)
                else {
                    return Ok(false);
                };
                Ok(
                    receipt.outcome == risk_transition::cancel::Outcome::EffectiveCanceled
                        && receipt.target_order_id == meta.order_id
                        && receipt.order_version_after == order.version
                        && receipt.order_version_before >= meta.order_version
                        && receipt.order_version_before <= meta.order_version.saturating_add(1)
                        && self.historical_meta_matches(meta, &receipt.before)?
                        && meta.risk_cancel_pending == cancel_pending(receipt.reason)
                        && receipt.after == order.facts,
                )
            }
            _ => Ok(false),
        }
    }

    fn activate_historical_spec_boundary(&mut self, raw_time_ms: i64) -> Result<(), Fault> {
        let effective_at = effective_time(raw_time_ms, 0)?;
        let previous_at = self.transition.context_at.unwrap_or(self.seed_effective_at);
        let (scenario, marks) = self.btc_context()?;
        if scenario.configured.is_none() {
            return Ok(());
        }
        let mut changes = Vec::new();
        for product in scenario.products() {
            let (old_spec, old_tier) = scenario.resolve(&product, previous_at)?;
            let (spec, tier) = scenario.resolve(&product, effective_at)?;
            if spec.interval.from != effective_at || old_spec == spec {
                continue;
            }
            // P3C5B admits the normal spec boundary only; tier activation is a
            // separate context transition and remains outside this slice.
            if old_tier != tier {
                return Err("UNSUPPORTED_CONTEXT_TRANSITION");
            }
            changes.push((
                product,
                old_spec.version.clone(),
                spec.version.clone(),
                effective_at,
            ));
        }
        if changes.is_empty() {
            return Ok(());
        }
        let after = owner_context_id(
            scenario,
            marks,
            effective_at,
            self.seed_effective_at,
            &self.config_id,
        )?;
        let event_id = format!("P3-SPEC-ACTIVATION-{raw_time_ms}");
        self.activate_context(&context::Input {
            account_key: self.key.clone(),
            stamp: source::stamp(&event_id, effective_at, 10),
            expected_before: self.valuation_context_id,
            expected_after: after,
            rows: context::Rows::Specs(changes),
        })?;
        Ok(())
    }

    /// Advances one admitted product-local OHLC4 segment through the existing owner.
    /// The metadata is only a matching snapshot; settlement and immediate risk remain
    /// exclusively in `execute_stamped`.
    pub(in crate::binding::matcher::replay_transition::synthetic_risk::scenario_account) fn historical_market_step(
        &mut self,
        input: &NodeInput,
    ) -> Result<StepResult, Fault> {
        let model = Model::parse(&input.model_id, &input.model_version)?;
        if input.step_index > 3 || input.market_slippage_bps < Decimal::ZERO {
            return Err("INVALID_HISTORICAL_INPUT");
        }
        let products = {
            let (scenario, _) = self.btc_context()?;
            scenario.products()
        };
        if input.bars.len() != products.len() {
            return Err("INVALID_HISTORICAL_INPUT");
        }
        let raw_time_ms = input
            .bars
            .first()
            .ok_or("INVALID_HISTORICAL_INPUT")?
            .bar_open_ms
            .checked_add(STEP_MS * i64::from(input.step_index))
            .ok_or("INVALID_HISTORICAL_BAR")?;
        let at = effective_time(raw_time_ms, phase_for_step(input.step_index)?)?;
        if input.bars.iter().zip(&products).any(|(bar, product)| {
            &bar.product != product
                || bar.bar_open_ms != input.bars[0].bar_open_ms
                || bar.bar_duration_ms != BAR_MS
                || !bar.confirmed
        }) {
            return Err("INVALID_HISTORICAL_INPUT");
        }
        let mut seen = BTreeSet::new();
        for meta in &input.working_orders {
            if !seen.insert(meta.order_id.as_str()) {
                return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
            }
            let order = self
                .orders
                .get(&meta.order_id)
                .ok_or("INVALID_HISTORICAL_ORDER_SNAPSHOT")?;
            if meta.accepted_source_sequence < 0
                || meta.accepted_at != order.created_at
                || !self.historical_cancel_projection_matches(meta, order)?
            {
                return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
            }
        }
        let segment_boundary = segment_start(
            input
                .bars
                .first()
                .ok_or("INVALID_HISTORICAL_INPUT")?
                .bar_open_ms,
            input.step_index,
        )?;
        for (id, order) in &self.orders {
            let eligible_at_start = match segment_boundary {
                Some(boundary) => order.created_at <= boundary,
                None => order.created_at < at,
            };
            if order
                .facts
                .projects_remainder("INVALID_HISTORICAL_ORDER_SNAPSHOT")?
                && !matches!(
                    order.cancel,
                    risk_transition::cancel::State::EffectiveCanceled(_)
                )
                && eligible_at_start
                && !seen.contains(id.as_str())
            {
                return Err("INVALID_HISTORICAL_ORDER_SNAPSHOT");
            }
        }
        let mut draft = self.clone();
        // VERSION_ACTIVATION is phase 0; step 3 closes this raw boundary at
        // phase 4. Apply only the configured spec transition due at this time.
        if input.step_index == 3 && !self.is_terminal() {
            draft.activate_historical_spec_boundary(raw_time_ms)?;
        }
        let (scenario, _) = draft.btc_context()?;
        let mut steps = Vec::with_capacity(products.len());
        for (ordinal, (bar, product)) in input.bars.iter().zip(&products).enumerate() {
            let (spec, _) = scenario.resolve(product, at)?;
            steps.push(product_steps(
                bar,
                model,
                ordinal,
                spec.lot,
                spec.tick,
                spec.minimum,
                input.step_index,
            )?);
        }
        if self.is_terminal() {
            let raw_time_ms = steps.first().ok_or("INVALID_HISTORICAL_INPUT")?.raw_time_ms;
            return Ok(StepResult {
                raw_time_ms,
                effective_at: at,
                products: steps
                    .into_iter()
                    .map(|step| ProductStepResult {
                        product_id: product_id(&step.product).into(),
                        capacity: step.capacity,
                        discarded_volume: step.discarded_volume,
                        fills: Vec::new(),
                    })
                    .collect(),
            });
        }
        let old_at = draft
            .transition
            .context_at
            .unwrap_or(draft.seed_effective_at);
        if at < old_at {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }

        // Stage marks and context on a private owner draft. A failed admission never
        // publishes partial source or account state.
        let (_, current_marks) = draft.btc_context()?;
        if at == old_at
            && steps.iter().any(|step| {
                current_marks
                    .iter()
                    .find(|mark| {
                        mark.product == step.product && mark.valid_from <= at && at < mark.valid_to
                    })
                    .is_none_or(|mark| mark.price != step.mark_price)
            })
        {
            return Err("UNSUPPORTED_CONTEXT_TRANSITION");
        }
        let mut next_marks = current_marks.to_vec();
        for step in &steps {
            for mark in next_marks.iter_mut().filter(|mark| {
                mark.product == step.product && mark.valid_from < at && at < mark.valid_to
            }) {
                mark.valid_to = at;
            }
            next_marks.retain(|mark| !(mark.product == step.product && mark.valid_from == at));
            next_marks.push(Mark {
                product: step.product.clone(),
                price: step.mark_price,
                valid_from: at,
                valid_to: i64::MAX,
            });
        }
        validate_marks(&next_marks)?;
        if at > old_at {
            if let ProfileContext::BtcEthScenario { marks, .. } = &mut draft.profile {
                *marks = next_marks.clone();
            } else {
                return Err("PROFILE_MISMATCH");
            }
            let (scenario, _) = draft.btc_context()?;
            let before = draft.valuation_context_id;
            let after = owner_context_id(
                scenario,
                &next_marks,
                at,
                draft.seed_effective_at,
                &draft.config_id,
            )?;
            let mut row_markers = Vec::with_capacity(products.len());
            for product in &products {
                let mark = next_marks
                    .iter()
                    .find(|mark| {
                        mark.product == *product && mark.valid_from <= at && at < mark.valid_to
                    })
                    .ok_or("MARK_COVERAGE_MISSING")?;
                row_markers.push((product.clone(), mark.valid_from, mark.valid_to, mark.price));
            }
            let context_event = {
                let mut encoding =
                    risk_transition::cancel::identity::Encoding::new("P3_MARK_CONTEXT_V1");
                encoding.hash(input.run_contract_hash);
                encoding.integer(input.bars[0].bar_open_ms);
                encoding.integer(i64::from(input.step_index));
                for step in &steps {
                    encoding.hash(step.source_row_hash);
                    encoding.hash(step.mark_source_row_hash);
                }
                let hash = encoding.finish();
                format!("P3-MARK-{:02x?}", hash)
            };
            let context = context::Input {
                account_key: draft.key.clone(),
                stamp: source::stamp(&context_event, at, 20),
                expected_before: before,
                expected_after: after,
                rows: context::Rows::Marks(row_markers),
            };
            draft.activate_context(&context)?;
        }

        let raw_time_ms = steps.first().ok_or("INVALID_HISTORICAL_INPUT")?.raw_time_ms;
        let mut results = Vec::with_capacity(steps.len());
        let mut ordinal = 30;
        for step in steps {
            let mut remaining_capacity = step.capacity;
            let mut fills = Vec::new();
            let mut filled_this_segment = BTreeSet::new();
            loop {
                let live_orders: Vec<_> = input
                    .working_orders
                    .iter()
                    .filter(|meta| !filled_this_segment.contains(meta.order_id.as_str()))
                    .filter_map(|meta| draft.orders.get(&meta.order_id).map(|order| (meta, order)))
                    .filter(|(_, order)| {
                        order.facts.remaining > Decimal::ZERO
                            && !matches!(
                                order.cancel,
                                risk_transition::cancel::State::EffectiveCanceled(_)
                            )
                    })
                    .map(|(meta, order)| WorkingOrderMeta {
                        order_id: meta.order_id.clone(),
                        product: meta.product.clone(),
                        order_version: order.version,
                        status: order.facts.status.clone(),
                        remaining: order.facts.remaining,
                        accepted_at: order.created_at,
                        accepted_source_sequence: meta.accepted_source_sequence,
                        kind: meta.kind,
                        side: order.facts.side,
                        limit_price: meta.limit_price,
                        risk_cancel_pending: matches!(
                            order.cancel,
                            risk_transition::cancel::State::Requested(ref request)
                                if matches!(request.reason, risk_transition::cancel::Reason::RiskShortfall | risk_transition::cancel::Reason::MmrBreach)
                        ),
                    })
                    .collect();
                let candidates = product_candidates(
                    &step,
                    &live_orders,
                    remaining_capacity,
                    model,
                    input.run_contract_hash,
                    input.market_slippage_bps,
                    |id| {
                        Ok(i64::try_from(
                            draft
                                .execution_receipts
                                .values()
                                .filter(|r| r.historical_order_id() == id)
                                .count(),
                        )
                        .map_err(|_| "DECIMAL_OVERFLOW")?
                            + 1)
                    },
                )?;
                let Some(candidate) = candidates.first() else {
                    break;
                };
                let prepared = execution::historical_candidate(
                    &draft,
                    &candidate.order_id,
                    candidate.price,
                    candidate.quantity,
                    step.effective_at,
                    candidate.execution_id,
                )?;
                let detecting_event_id = prepared.event_id().to_owned();
                let stamp = source::stamp(&detecting_event_id, step.effective_at, ordinal);
                match draft.execute_stamped(&prepared, &stamp, |_| Ok(()))? {
                    Reply::Committed { receipt, .. } => {
                        let (quantity, price, execution_id) = receipt.historical_fill();
                        remaining_capacity = remaining_capacity
                            .checked_sub(quantity)
                            .ok_or("DECIMAL_OVERFLOW")?;
                        fills.push(HistoricalFill {
                            order_id: candidate.order_id.clone(),
                            quantity,
                            price,
                            execution_id,
                            source_event_id: receipt.source_event().to_owned(),
                            occurrence_index: 0,
                        });
                    }
                    Reply::Duplicate(_) | Reply::Rejected(_) => {}
                }
                filled_this_segment.insert(candidate.order_id.clone());
                let risk_effects: Vec<_> = draft
                    .orders
                    .iter()
                    .filter_map(|(order_id, order)| match &order.cancel {
                        risk_transition::cancel::State::Requested(request)
                            if request.detecting_event_id == detecting_event_id
                                && matches!(
                                    request.reason,
                                    risk_transition::cancel::Reason::RiskShortfall
                                        | risk_transition::cancel::Reason::MmrBreach
                                ) =>
                        {
                            Some((
                                request.detecting_event_id.clone(),
                                order_id.clone(),
                                request.reason,
                            ))
                        }
                        _ => None,
                    })
                    .collect();
                if !risk_effects.is_empty() {
                    let mut encoding =
                        risk_transition::cancel::identity::Encoding::new("P3_RISK_EFFECT_V1");
                    encoding.hash(candidate.execution_id);
                    for (_, order_id, reason) in &risk_effects {
                        encoding.text(order_id);
                        encoding.text(reason.name());
                    }
                    let effect_id = encoding.finish();
                    let event_id = format!("P3-RISK-EFFECT-{:02x?}", effect_id);
                    let effect_stamp = source::stamp(
                        &event_id,
                        step.effective_at,
                        ordinal.checked_add(1).ok_or("VERSION_OVERFLOW")?,
                    );
                    draft.effect_cancel(&risk_transition::cancel::EffectInput {
                        stamp: effect_stamp,
                        effects: risk_effects,
                    })?;
                    ordinal = ordinal.checked_add(2).ok_or("VERSION_OVERFLOW")?;
                } else {
                    ordinal = ordinal.checked_add(1).ok_or("VERSION_OVERFLOW")?;
                }
                if draft.is_terminal() {
                    break;
                }
            }
            results.push(ProductStepResult {
                product_id: product_id(&step.product).into(),
                capacity: step.capacity,
                discarded_volume: step.discarded_volume,
                fills,
            });
        }
        *self = draft;
        Ok(StepResult {
            raw_time_ms,
            effective_at: at,
            products: results,
        })
    }
}
