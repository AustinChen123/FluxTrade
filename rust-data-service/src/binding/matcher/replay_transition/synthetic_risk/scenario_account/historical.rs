//! Private deterministic historical OHLC matching; financial writes stay in `ScenarioAccount`.
use super::*;
use num_bigint_dig::BigInt;
use num_traits::ToPrimitive;
use rust_decimal::Decimal;

use super::super::{add, exact_div, mul};
use super::execution::commit::Reply;

const BAR_MS: i64 = 60_000;
const STEP_MS: i64 = 20_000;
const RAW_TIME_LIMIT: i64 = 1_i64 << 59;

fn effective_time(raw_time_ms: i64, phase: i64) -> Result<i64, Fault> {
    if !(0..RAW_TIME_LIMIT).contains(&raw_time_ms) || !(0..16).contains(&phase) {
        return Err("INVALID_HISTORICAL_TIME");
    }
    raw_time_ms
        .checked_mul(16)
        .and_then(|time| time.checked_add(phase))
        .ok_or("INVALID_HISTORICAL_TIME")
}

fn phase_for_step(step_index: u8) -> Result<i64, Fault> {
    match step_index {
        0 => Ok(7),     // NEXT_BAR_OPEN
        1 | 2 => Ok(8), // MARKET_SEGMENT_ENDPOINT
        3 => Ok(4),     // PREVIOUS_BAR_CLOSE
        _ => Err("INVALID_HISTORICAL_TIME"),
    }
}

fn segment_start(bar_open_ms: i64, step_index: u8) -> Result<Option<i64>, Fault> {
    let (offset, phase) = match step_index {
        0 => return Ok(None),
        1 => (0, 7),
        2 => (STEP_MS, 8),
        3 => (STEP_MS * 2, 8),
        _ => return Err("INVALID_HISTORICAL_TIME"),
    };
    let raw = bar_open_ms
        .checked_add(offset)
        .ok_or("INVALID_HISTORICAL_TIME")?;
    Ok(Some(effective_time(raw, phase)?))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Model {
    OpenHighLowClose,
    OpenLowHighClose,
}

impl Model {
    pub(super) fn parse(id: &str, version: &str) -> Result<Self, Fault> {
        if version != "1" {
            return Err("UNSUPPORTED_HISTORICAL_MODEL");
        }
        match id {
            "OHLC4_OPEN_HIGH_LOW_CLOSE_V1" => Ok(Self::OpenHighLowClose),
            "OHLC4_OPEN_LOW_HIGH_CLOSE_V1" => Ok(Self::OpenLowHighClose),
            _ => Err("UNSUPPORTED_HISTORICAL_MODEL"),
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::OpenHighLowClose => "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
            Self::OpenLowHighClose => "OHLC4_OPEN_LOW_HIGH_CLOSE_V1",
        }
    }

    fn path(self, prices: [Decimal; 4]) -> [Decimal; 4] {
        match self {
            Self::OpenHighLowClose => [prices[0], prices[1], prices[2], prices[3]],
            Self::OpenLowHighClose => [prices[0], prices[2], prices[1], prices[3]],
        }
    }
}

/// One product's paired, already-admitted trade and mark bar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct BarPair {
    pub product: Product,
    pub bar_open_ms: i64,
    pub bar_duration_ms: i64,
    pub trade_ohlc: [Decimal; 4],
    pub mark_ohlc: [Decimal; 4],
    pub volume_contracts: Decimal,
    pub confirmed: bool,
    pub source_row_hash: Hash,
}

/// Metadata paired with an owner working-order snapshot by the P3 caller.
/// Owner identity, state and version are revalidated by the account entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct WorkingOrderMeta {
    pub order_id: String,
    pub product: Product,
    pub order_version: u64,
    pub status: String,
    pub remaining: Decimal,
    pub accepted_at: i64,
    pub accepted_source_sequence: i64,
    pub kind: OrderKind,
    pub side: Side,
    pub limit_price: Option<Decimal>,
    pub risk_cancel_pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct NodeInput {
    pub model_id: String,
    pub model_version: String,
    pub run_contract_hash: Hash,
    pub step_index: u8,
    pub market_slippage_bps: Decimal,
    pub bars: Vec<BarPair>,
    pub working_orders: Vec<WorkingOrderMeta>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OrderKind {
    Limit,
    Market,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProductStep {
    product: Product,
    product_ordinal: usize,
    step_index: u8,
    raw_time_ms: i64,
    effective_at: i64,
    segment_start_at: Option<i64>,
    trade_price: Decimal,
    mark_price: Decimal,
    previous_trade_price: Option<Decimal>,
    capacity: Decimal,
    discarded_volume: Decimal,
    source_row_hash: Hash,
    price_tick: Decimal,
    quantity_step: Decimal,
    minimum_quantity: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Fraction {
    numerator: Decimal,
    denominator: Decimal,
}

impl Fraction {
    fn zero() -> Self {
        Self {
            numerator: Decimal::ZERO,
            denominator: Decimal::ONE,
        }
    }

    fn compare(self, other: Self) -> std::cmp::Ordering {
        let scaled = |numerator: Decimal, denominator: Decimal| {
            (
                BigInt::from(numerator.mantissa()) * BigInt::from(10).pow(denominator.scale()),
                BigInt::from(denominator.mantissa()) * BigInt::from(10).pow(numerator.scale()),
            )
        };
        let (left_num, left_den) = scaled(self.numerator, self.denominator);
        let (right_num, right_den) = scaled(other.numerator, other.denominator);
        (left_num * right_den).cmp(&(right_num * left_den))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Candidate {
    product_ordinal: usize,
    order_id: String,
    side: Side,
    quantity: Decimal,
    price: Decimal,
    trigger: Fraction,
    accepted_at: i64,
    accepted_source_sequence: i64,
    cumulative_fill_index: i64,
    execution_id: Hash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct HistoricalFill {
    pub order_id: String,
    pub quantity: Decimal,
    pub price: Decimal,
    pub execution_id: Hash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StepResult {
    pub raw_time_ms: i64,
    pub effective_at: i64,
    pub products: Vec<ProductStepResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProductStepResult {
    pub product_id: String,
    pub capacity: Decimal,
    pub discarded_volume: Decimal,
    pub fills: Vec<HistoricalFill>,
}

fn product_steps(
    bar: &BarPair,
    model: Model,
    product_ordinal: usize,
    quantity_step: Decimal,
    price_tick: Decimal,
    minimum_quantity: Decimal,
    step_index: u8,
) -> Result<ProductStep, Fault> {
    if bar.bar_open_ms < 0
        || bar.bar_duration_ms != BAR_MS
        || step_index > 3
        || !bar.confirmed
        || bar.volume_contracts < Decimal::ZERO
        || [quantity_step, price_tick, minimum_quantity]
            .iter()
            .any(|v| *v <= Decimal::ZERO)
        || bar.trade_ohlc.iter().any(|v| *v <= Decimal::ZERO)
        || bar.mark_ohlc.iter().any(|v| *v <= Decimal::ZERO)
    {
        return Err("INVALID_HISTORICAL_BAR");
    }
    let valid_ohlc = |[open, high, low, close]: [Decimal; 4]| {
        low <= open.min(close) && high >= open.max(close) && high >= low
    };
    if !valid_ohlc(bar.trade_ohlc) || !valid_ohlc(bar.mark_ohlc) {
        return Err("INVALID_HISTORICAL_BAR");
    }
    let trade = model.path(bar.trade_ohlc);
    let mark = model.path(bar.mark_ohlc);
    let numerator =
        BigInt::from(bar.volume_contracts.mantissa()) * BigInt::from(10).pow(quantity_step.scale());
    let denominator =
        BigInt::from(quantity_step.mantissa()) * BigInt::from(10).pow(bar.volume_contracts.scale());
    if denominator <= BigInt::from(0) {
        return Err("INVALID_HISTORICAL_BAR");
    }
    let whole_lots = (numerator / denominator)
        .to_u64()
        .ok_or("DECIMAL_OVERFLOW")?;
    let base = whole_lots / 4;
    let remainder = whole_lots % 4;
    let mut capacities = [Decimal::ZERO; 4];
    for (index, capacity) in capacities.iter_mut().enumerate() {
        let lots = base + u64::from((index as u64) < remainder);
        *capacity = quantity_step
            .checked_mul(Decimal::from(lots))
            .ok_or("DECIMAL_OVERFLOW")?;
    }
    let used = quantity_step
        .checked_mul(Decimal::from(whole_lots))
        .ok_or("DECIMAL_OVERFLOW")?;
    let discarded_volume = bar
        .volume_contracts
        .checked_sub(used)
        .ok_or("DECIMAL_OVERFLOW")?;
    let raw_time_ms = bar
        .bar_open_ms
        .checked_add(STEP_MS * i64::from(step_index))
        .ok_or("INVALID_HISTORICAL_BAR")?;
    let effective_at = effective_time(raw_time_ms, phase_for_step(step_index)?)?;
    let segment_start_at = segment_start(bar.bar_open_ms, step_index)?;
    Ok(ProductStep {
        product: bar.product.clone(),
        product_ordinal,
        step_index,
        raw_time_ms,
        effective_at,
        segment_start_at,
        trade_price: trade[usize::from(step_index)],
        mark_price: mark[usize::from(step_index)],
        previous_trade_price: step_index
            .checked_sub(1)
            .map(|index| trade[usize::from(index)]),
        capacity: capacities[usize::from(step_index)],
        discarded_volume,
        source_row_hash: bar.source_row_hash,
        price_tick,
        quantity_step,
        minimum_quantity,
    })
}

fn limit_candidate(step: &ProductStep, order: &WorkingOrderMeta) -> Option<(Decimal, Fraction)> {
    let limit = order.limit_price?;
    let previous = step.previous_trade_price;
    match (order.kind, order.side, previous) {
        (OrderKind::Limit, Side::Long, None) if limit >= step.trade_price => {
            Some((step.trade_price, Fraction::zero()))
        }
        (OrderKind::Limit, Side::Short, None) if limit <= step.trade_price => {
            Some((step.trade_price, Fraction::zero()))
        }
        (OrderKind::Limit, Side::Long, Some(p0)) if limit >= p0 => Some((p0, Fraction::zero())),
        (OrderKind::Limit, Side::Short, Some(p0)) if limit <= p0 => Some((p0, Fraction::zero())),
        (OrderKind::Limit, Side::Long, Some(p0)) if step.trade_price <= limit && limit < p0 => {
            Some((
                limit,
                Fraction {
                    numerator: p0 - limit,
                    denominator: p0 - step.trade_price,
                },
            ))
        }
        (OrderKind::Limit, Side::Short, Some(p0)) if step.trade_price >= limit && limit > p0 => {
            Some((
                limit,
                Fraction {
                    numerator: limit - p0,
                    denominator: step.trade_price - p0,
                },
            ))
        }
        _ => None,
    }
}

fn market_price(step: &ProductStep, side: Side, slippage_bps: Decimal) -> Result<Decimal, Fault> {
    let multiplier = match side {
        Side::Long => add(Decimal::from(10_000), slippage_bps)?,
        Side::Short => add(Decimal::from(10_000), -slippage_bps)?,
    };
    let raw = mul(
        step.trade_price,
        exact_div(multiplier, Decimal::from(10_000))?,
    )?;
    if raw <= Decimal::ZERO {
        return Err("INVALID_HISTORICAL_COST");
    }
    round_to_tick(raw, step.price_tick, side == Side::Long)
}

fn round_to_tick(value: Decimal, tick: Decimal, upward: bool) -> Result<Decimal, Fault> {
    let numerator = BigInt::from(value.mantissa()) * BigInt::from(10).pow(tick.scale());
    let denominator = BigInt::from(tick.mantissa()) * BigInt::from(10).pow(value.scale());
    if denominator <= BigInt::from(0) {
        return Err("INVALID_HISTORICAL_SPEC");
    }
    let mut units = &numerator / &denominator;
    if upward && !(&numerator % &denominator).eq(&BigInt::from(0)) {
        units += 1;
    }
    let mantissa = units * BigInt::from(tick.mantissa());
    Decimal::try_from_i128_with_scale(mantissa.to_i128().ok_or("DECIMAL_OVERFLOW")?, tick.scale())
        .map_err(|_| "DECIMAL_OVERFLOW")
}

fn derived_execution_id(
    run_contract_hash: Hash,
    model: Model,
    source_row_hash: Hash,
    step_index: u8,
    owner_order_id: &str,
    cumulative_fill_index: i64,
) -> Hash {
    let mut encoding = risk_transition::cancel::identity::Encoding::new("P3_EXECUTION_V1");
    encoding.hash(run_contract_hash);
    encoding.text(model.id());
    encoding.hash(source_row_hash);
    encoding.integer(i64::from(step_index));
    encoding.text(owner_order_id);
    encoding.integer(cumulative_fill_index);
    encoding.finish()
}

fn product_candidates(
    step: &ProductStep,
    orders: &[WorkingOrderMeta],
    segment_capacity: Decimal,
    model: Model,
    run_contract_hash: Hash,
    slippage_bps: Decimal,
    mut cumulative_fill_index: impl FnMut(&str) -> Result<i64, Fault>,
) -> Result<Vec<Candidate>, Fault> {
    if slippage_bps < Decimal::ZERO {
        return Err("INVALID_HISTORICAL_COST");
    }
    let mut eligible = Vec::new();
    for order in orders {
        if order.product != step.product {
            continue;
        }
        let can_participate = match step.segment_start_at {
            None => order.accepted_at < step.effective_at,
            Some(boundary) => order.accepted_at <= boundary,
        };
        if !can_participate
            || order.accepted_source_sequence < 0
            || order.remaining <= Decimal::ZERO
            || order.risk_cancel_pending
        {
            continue;
        }
        let (price, trigger) = match order.kind {
            OrderKind::Market => (
                market_price(step, order.side, slippage_bps)?,
                Fraction::zero(),
            ),
            OrderKind::Limit => match limit_candidate(step, order) {
                Some(value) => value,
                None => continue,
            },
        };
        eligible.push((order, price, trigger));
    }
    for index in 1..eligible.len() {
        let mut cursor = index;
        while cursor > 0 {
            let (left_order, _, left_trigger) = eligible[cursor - 1];
            let (right_order, _, right_trigger) = eligible[cursor];
            let ordering = left_trigger
                .compare(right_trigger)
                .then_with(|| left_order.accepted_at.cmp(&right_order.accepted_at))
                .then_with(|| {
                    left_order
                        .accepted_source_sequence
                        .cmp(&right_order.accepted_source_sequence)
                })
                .then_with(|| left_order.order_id.cmp(&right_order.order_id));
            if ordering != std::cmp::Ordering::Greater {
                break;
            }
            eligible.swap(cursor - 1, cursor);
            cursor -= 1;
        }
    }
    let mut remaining_capacity = segment_capacity;
    let mut result = Vec::new();
    for (order, price, trigger) in eligible {
        if remaining_capacity <= Decimal::ZERO {
            break;
        }
        let quantity = order.remaining.min(remaining_capacity);
        if quantity < step.minimum_quantity || !aligned(quantity, step.quantity_step) {
            continue;
        }
        let fill_index = cumulative_fill_index(&order.order_id)?;
        let execution_id = derived_execution_id(
            run_contract_hash,
            model,
            step.source_row_hash,
            step.step_index,
            &order.order_id,
            fill_index,
        );
        remaining_capacity = remaining_capacity
            .checked_sub(quantity)
            .ok_or("DECIMAL_OVERFLOW")?;
        result.push(Candidate {
            product_ordinal: step.product_ordinal,
            order_id: order.order_id.clone(),
            side: order.side,
            quantity,
            price,
            trigger,
            accepted_at: order.accepted_at,
            accepted_source_sequence: order.accepted_source_sequence,
            cumulative_fill_index: fill_index,
            execution_id,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests;

mod owner;
