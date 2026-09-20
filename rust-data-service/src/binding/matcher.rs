use ::pyo3::prelude::*;
use num_bigint_dig::BigInt;
use num_traits::{Pow, ToPrimitive};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FeeModel {
    PercentageNotional,
    PerContract,
}

impl FeeModel {
    fn parse(value: &str) -> PyResult<Self> {
        match value {
            "percentage_notional" => Ok(Self::PercentageNotional),
            "per_contract" => Ok(Self::PerContract),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported fee_model: {value}"
            ))),
        }
    }
}

use crate::binding::models::{Candlestick, FillEvent, Order, Position};
use crate::binding::scaled::ScaledCandlestick;
use crate::binding::spot_ledger::{CashSpotLedger, CashSpotSettlement, SpotFeeAsset};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SettlementModel {
    Derivatives,
    CashSpot,
}

impl SettlementModel {
    fn parse(value: &str) -> PyResult<Self> {
        match value {
            "derivatives" => Ok(Self::Derivatives),
            "cash_spot" => Ok(Self::CashSpot),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported settlement_model: {value}"
            ))),
        }
    }
}

struct ExitCandidate {
    order: Order,
    fill_price: Decimal,
    fee: Decimal,
    position_key: String,
    protected_side: String,
}

#[pyclass]
pub struct PyMatchingEngine {
    pub balance: Decimal,
    #[pyo3(get)]
    pub positions: HashMap<String, Position>,
    #[pyo3(get)]
    pub open_orders: Vec<Order>,
    maker_fee: Decimal,
    taker_fee: Decimal,
    market_slippage_bps: Decimal,
    market_slippage_price_tick: Option<Decimal>,
    contract_multiplier: Decimal,
    fee_model: FeeModel,
    settlement_model: SettlementModel,
    spot_ledger: Option<CashSpotLedger>,
    spot_cost_basis: Decimal,
    spot_realized_pnl: Decimal,
    rejections: Vec<HashMap<String, String>>,
    warnings: Vec<HashMap<String, String>>,
    scaled_price_tick: Option<Decimal>,
    scaled_volume_step: Option<Decimal>,
}

#[pymethods]
impl PyMatchingEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (initial_balance, maker_fee="0".to_string(), taker_fee="0".to_string(), contract_multiplier="1".to_string(), fee_model="percentage_notional".to_string(), settlement_model="derivatives".to_string(), base_asset="".to_string(), quote_asset="".to_string(), spot_fee_asset="quote".to_string(), market_slippage_bps="0".to_string(), market_slippage_price_tick=None))]
    fn new(
        initial_balance: String,
        maker_fee: String,
        taker_fee: String,
        contract_multiplier: String,
        fee_model: String,
        settlement_model: String,
        base_asset: String,
        quote_asset: String,
        spot_fee_asset: String,
        market_slippage_bps: String,
        market_slippage_price_tick: Option<String>,
    ) -> PyResult<Self> {
        let initial_balance = parse_decimal(&initial_balance, "initial_balance")?;
        let contract_multiplier = parse_decimal(&contract_multiplier, "contract_multiplier")?;
        let market_slippage_bps = parse_decimal(&market_slippage_bps, "market_slippage_bps")?;
        if market_slippage_bps < Decimal::ZERO {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "market_slippage_bps must be at least 0",
            ));
        }
        let market_slippage_price_tick = market_slippage_price_tick
            .map(|value| parse_decimal(&value, "market_slippage_price_tick"))
            .transpose()?;
        let has_valid_slippage_tick =
            market_slippage_price_tick.is_some_and(|tick| tick > Decimal::ZERO);
        if market_slippage_bps > Decimal::ZERO && !has_valid_slippage_tick {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "positive market_slippage_bps requires a positive market_slippage_price_tick",
            ));
        }
        if contract_multiplier <= Decimal::ZERO {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "contract_multiplier must be positive",
            ));
        }
        let fee_model = FeeModel::parse(&fee_model)?;
        let settlement_model = SettlementModel::parse(&settlement_model)?;
        if settlement_model == SettlementModel::CashSpot && contract_multiplier != Decimal::ONE {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cash_spot contract_multiplier must equal 1",
            ));
        }
        if settlement_model == SettlementModel::CashSpot
            && fee_model != FeeModel::PercentageNotional
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cash_spot settlement requires percentage_notional fee_model",
            ));
        }
        let spot_ledger = if settlement_model == SettlementModel::CashSpot {
            let fee_asset = SpotFeeAsset::parse(&spot_fee_asset)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
            Some(
                CashSpotLedger::new(initial_balance, base_asset, quote_asset, fee_asset)
                    .map_err(pyo3::exceptions::PyValueError::new_err)?,
            )
        } else {
            None
        };
        Ok(PyMatchingEngine {
            balance: initial_balance,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: parse_decimal(&maker_fee, "maker_fee")?,
            taker_fee: parse_decimal(&taker_fee, "taker_fee")?,
            market_slippage_bps,
            market_slippage_price_tick,
            contract_multiplier,
            fee_model,
            settlement_model,
            spot_ledger,
            spot_cost_basis: Decimal::ZERO,
            spot_realized_pnl: Decimal::ZERO,
            rejections: Vec::new(),
            warnings: Vec::new(),
            scaled_price_tick: None,
            scaled_volume_step: None,
        })
    }

    #[getter]
    fn balance(&self) -> String {
        match &self.spot_ledger {
            Some(ledger) => ledger.quote_available().to_string(),
            None => self.balance.to_string(),
        }
    }

    fn submit_order(&mut self, order: Order) -> PyResult<String> {
        if let Some(ledger) = self.spot_ledger.as_mut() {
            if matches!(order.order_type.as_str(), "MARKET" | "LIMIT") {
                let fee_rate = if order.order_type == "MARKET" {
                    self.taker_fee
                } else {
                    self.maker_fee
                };
                let warning = ledger
                    .reserve(&order, order.price, fee_rate)
                    .map_err(pyo3::exceptions::PyValueError::new_err)?;
                if let Some(reason) = warning {
                    self.warnings.push(HashMap::from([
                        ("order_id".to_string(), order.id.clone()),
                        ("product_id".to_string(), order.product_id.clone()),
                        ("strategy_id".to_string(), order.strategy_id.clone()),
                        ("reason".to_string(), reason),
                    ]));
                }
            } else if order.side == "SHORT" {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "cash_spot conditional order cannot protect a short position",
                ));
            }
        }
        let id = order.id.clone();
        self.open_orders.push(order);
        Ok(id)
    }

    fn get_positions(&self) -> HashMap<String, Position> {
        self.positions.clone()
    }

    fn drain_rejections(&mut self) -> Vec<HashMap<String, String>> {
        std::mem::take(&mut self.rejections)
    }

    fn drain_warnings(&mut self) -> Vec<HashMap<String, String>> {
        std::mem::take(&mut self.warnings)
    }

    fn get_asset_balance(&self, asset: &str, balance_type: &str) -> PyResult<String> {
        let Some(ledger) = &self.spot_ledger else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "asset balances require cash_spot settlement",
            ));
        };
        let value = if asset == ledger.base_asset {
            match balance_type {
                "total" => ledger.base_total(),
                "available" => ledger.base_available(),
                "reserved" => ledger.base_reserved(),
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "balance_type must be total, available, or reserved",
                    ))
                }
            }
        } else if asset == ledger.quote_asset {
            match balance_type {
                "total" => ledger.quote_total(),
                "available" => ledger.quote_available(),
                "reserved" => ledger.quote_reserved(),
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "balance_type must be total, available, or reserved",
                    ))
                }
            }
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported cash_spot asset: {asset}"
            )));
        };
        Ok(value.to_string())
    }

    fn apply_external_funding(&mut self, asset: &str, amount: String) -> PyResult<String> {
        let amount = parse_decimal(&amount, "external_funding_amount")?;
        let ledger = self.spot_ledger.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "external funding requires cash_spot settlement",
            )
        })?;
        ledger
            .credit_quote(asset, amount)
            .map(|balance| balance.to_string())
            .map_err(pyo3::exceptions::PyValueError::new_err)
    }

    fn cash_spot_account_snapshot(&self, mark_price: String) -> PyResult<HashMap<String, String>> {
        let Some(ledger) = &self.spot_ledger else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cash_spot account snapshot requires cash_spot settlement",
            ));
        };
        let mark_price = parse_decimal(&mark_price, "mark_price")?;
        if mark_price <= Decimal::ZERO {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cash_spot mark_price must be positive",
            ));
        }
        let unrealized_pnl = ledger.base_total() * mark_price - self.spot_cost_basis;
        Ok(HashMap::from([
            ("base_asset".to_string(), ledger.base_asset.clone()),
            ("quote_asset".to_string(), ledger.quote_asset.clone()),
            ("fee_asset".to_string(), ledger.fee_asset_name().to_string()),
            ("base_total".to_string(), ledger.base_total().to_string()),
            (
                "base_available".to_string(),
                ledger.base_available().to_string(),
            ),
            (
                "base_reserved".to_string(),
                ledger.base_reserved().to_string(),
            ),
            ("quote_total".to_string(), ledger.quote_total().to_string()),
            (
                "quote_available".to_string(),
                ledger.quote_available().to_string(),
            ),
            (
                "quote_reserved".to_string(),
                ledger.quote_reserved().to_string(),
            ),
            ("cost_basis".to_string(), self.spot_cost_basis.to_string()),
            (
                "realized_pnl".to_string(),
                self.spot_realized_pnl.to_string(),
            ),
            ("unrealized_pnl".to_string(), unrealized_pnl.to_string()),
            (
                "total_equity".to_string(),
                (ledger.quote_total() + ledger.base_total() * mark_price).to_string(),
            ),
        ]))
    }

    /// Get position for a specific strategy and product.
    fn get_position(&self, strategy_id: &str, product_id: &str) -> Option<Position> {
        let key = format!("{strategy_id}:{product_id}");
        self.positions.get(&key).cloned()
    }

    fn on_candle(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        self.process_candle_logic(candle)
    }

    fn set_scaled_precision(&mut self, price_tick: String, volume_step: String) -> PyResult<()> {
        let parsed_price_tick = parse_decimal(&price_tick, "price_tick")?;
        let parsed_volume_step = parse_decimal(&volume_step, "volume_step")?;
        if parsed_price_tick <= Decimal::ZERO {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "price_tick must be positive",
            ));
        }
        if parsed_volume_step <= Decimal::ZERO {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "volume_step must be positive",
            ));
        }
        self.scaled_price_tick = Some(parsed_price_tick);
        self.scaled_volume_step = Some(parsed_volume_step);
        Ok(())
    }

    fn on_scaled_candle(&mut self, candle: ScaledCandlestick) -> PyResult<Vec<FillEvent>> {
        let price_tick = self.scaled_price_tick.ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "scaled precision is not configured; call set_scaled_precision() first",
            )
        })?;
        let volume_step = self.scaled_volume_step.ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "scaled precision is not configured; call set_scaled_precision() first",
            )
        })?;
        self.process_candle_logic(Candlestick {
            product_id: candle.product_id,
            timeframe: candle.timeframe,
            timestamp: candle.timestamp,
            open: Decimal::from(candle.open_units) * price_tick,
            high: Decimal::from(candle.high_units) * price_tick,
            low: Decimal::from(candle.low_units) * price_tick,
            close: Decimal::from(candle.close_units) * price_tick,
            volume: Decimal::from(candle.volume_units) * volume_step,
        })
    }

    fn on_matching_tick(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        self.process_candle_logic(candle)
    }

    fn cancel_order(&mut self, order_id: String) -> bool {
        let before = self.open_orders.len();
        self.open_orders.retain(|o| o.id != order_id);
        let cancelled = self.open_orders.len() < before;
        if cancelled {
            if let Some(ledger) = self.spot_ledger.as_mut() {
                ledger.release(&order_id);
            }
        }
        cancelled
    }
}

impl PyMatchingEngine {
    fn market_fill_price(&self, reference_price: Decimal, side: &str) -> Result<Decimal, String> {
        if self.market_slippage_bps == Decimal::ZERO {
            return Ok(reference_price);
        }
        let price_tick = self.market_slippage_price_tick.ok_or_else(|| {
            "positive market_slippage_bps requires a positive market_slippage_price_tick"
                .to_string()
        })?;
        let ten = BigInt::from(10_u8);
        let basis_scale = ten.pow(self.market_slippage_bps.scale());
        let basis = BigInt::from(10_000_u32) * basis_scale;
        let bps_mantissa = BigInt::from(self.market_slippage_bps.mantissa());
        let price_factor = match side {
            "LONG" => &basis + bps_mantissa,
            "SHORT" => &basis - bps_mantissa,
            _ => return Err("market order side must be LONG or SHORT".to_string()),
        };
        let numerator =
            BigInt::from(reference_price.mantissa()) * price_factor * ten.pow(price_tick.scale());
        if numerator <= BigInt::from(0_u8) {
            return Err("market slippage produced a non-positive fill price".to_string());
        }
        let denominator =
            ten.pow(reference_price.scale()) * basis * BigInt::from(price_tick.mantissa());
        let rounded_units = if side == "LONG" {
            (&numerator + &denominator - BigInt::from(1_u8)) / &denominator
        } else {
            &numerator / &denominator
        };
        if rounded_units <= BigInt::from(0_u8) {
            return Err(
                "market slippage produced a non-positive tick-rounded fill price".to_string(),
            );
        }
        let mut rounded_mantissa = rounded_units * BigInt::from(price_tick.mantissa());
        let mut rounded_scale = price_tick.scale();
        while rounded_scale > 0 && (&rounded_mantissa % &ten) == BigInt::from(0_u8) {
            rounded_mantissa /= &ten;
            rounded_scale -= 1;
        }
        if rounded_mantissa > BigInt::from(Decimal::MAX.mantissa()) {
            return Err("market slippage arithmetic overflow".to_string());
        }
        let rounded_mantissa = rounded_mantissa
            .to_i128()
            .ok_or_else(|| "market slippage arithmetic overflow".to_string())?;
        Ok(Decimal::from_i128_with_scale(
            rounded_mantissa,
            rounded_scale,
        ))
    }

    fn process_candle_logic(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        let mut fills: Vec<FillEvent> = Vec::new();
        let mut remaining_orders: Vec<Order> = Vec::new();
        // Collect IDs of orders cancelled by OCO during this candle
        let mut cancelled_ids: HashSet<String> = HashSet::new();
        let mut closed_position_sides: HashMap<String, String> = HashMap::new();

        // Entry/exit orders establish the position state that protections act on.
        let mut market_orders: Vec<Order> = Vec::new();
        let mut conditional_orders: Vec<Order> = Vec::new();
        let mut limit_orders: Vec<Order> = Vec::new();

        for order in self.open_orders.drain(..) {
            match order.order_type.as_str() {
                "MARKET" => market_orders.push(order),
                "STOP_LOSS" | "TAKE_PROFIT" | "TRAILING_STOP" => conditional_orders.push(order),
                "LIMIT" => limit_orders.push(order),
                _ => remaining_orders.push(order),
            }
        }
        conditional_orders.sort_by_key(Self::conditional_priority);

        // 1. Process Market Orders (taker fee, fill at open)
        for order in market_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if order.product_id != candle.product_id {
                remaining_orders.push(order);
                continue;
            }

            let fill_price = match self.market_fill_price(candle.open, &order.side) {
                Ok(price) => price,
                Err(reason) => {
                    self.reject_order(&order, candle.timestamp, reason);
                    continue;
                }
            };
            let fee = if self.settlement_model == SettlementModel::CashSpot {
                match self.settle_spot_order(&order, fill_price, true) {
                    Ok(settlement) => {
                        self.update_spot_position(&order, fill_price, settlement);
                        settlement.fee
                    }
                    Err(reason) => {
                        self.reject_order(&order, candle.timestamp, reason);
                        continue;
                    }
                }
            } else {
                let fee = self.calculate_fee(fill_price, order.quantity, true);
                self.update_position(&order, fill_price);
                let charged_fee = std::cmp::min(fee, self.balance);
                self.balance -= charged_fee;
                fee
            };

            let fill = FillEvent {
                order_id: order.id.clone(),
                product_id: order.product_id.clone(),
                strategy_id: order.strategy_id.clone(),
                price: fill_price,
                quantity: order.quantity,
                fee,
                timestamp: candle.timestamp,
                fill_type: "MARKET".to_string(),
            };
            self.cancel_linked(&order, &mut cancelled_ids);
            fills.push(fill);
        }

        let (closing_limit_orders, opening_limit_orders): (Vec<_>, Vec<_>) = limit_orders
            .into_iter()
            .partition(|order| self.reduces_current_position(order));

        // 2. Fill entries before evaluating their protection on the same candle.
        for order in opening_limit_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if let Some(order) =
                self.match_limit_order(order, &candle, &mut fills, &mut cancelled_ids)
            {
                remaining_orders.push(order);
            }
        }

        let pending_entries: HashSet<(String, String, String)> = remaining_orders
            .iter()
            .filter(|order| matches!(order.order_type.as_str(), "MARKET" | "LIMIT"))
            .map(|order| {
                (
                    order.strategy_id.clone(),
                    order.product_id.clone(),
                    order.side.clone(),
                )
            })
            .collect();
        let mut selected_exit_candidates: Vec<ExitCandidate> = Vec::new();
        let mut selected_exit_indexes: HashMap<String, usize> = HashMap::new();

        // 3. Evaluate conditional exits without mutating the position.
        for mut order in conditional_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if order.product_id != candle.product_id {
                remaining_orders.push(order);
                continue;
            }

            let position_key = Self::position_key(&order.strategy_id, &order.product_id);
            let Some(protected_side) = self
                .positions
                .get(&position_key)
                .filter(|position| position.side == order.side && position.quantity > Decimal::ZERO)
                .map(|position| position.side.clone())
            else {
                let protects_pending_entry = pending_entries.contains(&(
                    order.strategy_id.clone(),
                    order.product_id.clone(),
                    order.side.clone(),
                ));
                if protects_pending_entry {
                    remaining_orders.push(order);
                }
                continue;
            };

            let fill_price = match self.check_conditional_trigger(&mut order, &candle) {
                Some(price) => price,
                None => {
                    remaining_orders.push(order);
                    continue;
                }
            };
            let fee = self.calculate_fee(fill_price, order.quantity, true);
            Self::consider_exit_candidate(
                ExitCandidate {
                    order,
                    fill_price,
                    fee,
                    position_key,
                    protected_side,
                },
                &mut selected_exit_candidates,
                &mut selected_exit_indexes,
                &mut remaining_orders,
            );
        }

        // 4. Compare reachable explicit limit exits against protections.
        for order in closing_limit_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if order.product_id != candle.product_id || !Self::limit_order_matches(&order, &candle)
            {
                remaining_orders.push(order);
                continue;
            }
            let position_key = Self::position_key(&order.strategy_id, &order.product_id);
            let Some(protected_side) = self
                .positions
                .get(&position_key)
                .filter(|position| position.quantity > Decimal::ZERO && position.side != order.side)
                .map(|position| position.side.clone())
            else {
                remaining_orders.push(order);
                continue;
            };
            let fill_price = order.price;
            let fee = self.calculate_fee(fill_price, order.quantity, false);
            Self::consider_exit_candidate(
                ExitCandidate {
                    order,
                    fill_price,
                    fee,
                    position_key,
                    protected_side,
                },
                &mut selected_exit_candidates,
                &mut selected_exit_indexes,
                &mut remaining_orders,
            );
        }

        for candidate in selected_exit_candidates {
            let fee = if self.settlement_model == SettlementModel::CashSpot {
                match self.settle_spot_order(&candidate.order, candidate.fill_price, true) {
                    Ok(settlement) => {
                        self.update_spot_position(
                            &candidate.order,
                            candidate.fill_price,
                            settlement,
                        );
                        settlement.fee
                    }
                    Err(reason) => {
                        self.reject_order(&candidate.order, candle.timestamp, reason);
                        continue;
                    }
                }
            } else {
                self.update_position(&candidate.order, candidate.fill_price);
                let charged_fee = std::cmp::min(candidate.fee, self.balance);
                self.balance -= charged_fee;
                candidate.fee
            };
            fills.push(FillEvent {
                order_id: candidate.order.id.clone(),
                product_id: candidate.order.product_id.clone(),
                strategy_id: candidate.order.strategy_id.clone(),
                price: candidate.fill_price,
                quantity: candidate.order.quantity,
                fee,
                timestamp: candle.timestamp,
                fill_type: candidate.order.order_type.clone(),
            });
            let still_protected_position =
                self.positions
                    .get(&candidate.position_key)
                    .is_some_and(|position| {
                        position.side == candidate.protected_side
                            && position.quantity > Decimal::ZERO
                    });
            if !still_protected_position {
                closed_position_sides.insert(candidate.position_key, candidate.protected_side);
            }
            self.cancel_linked(&candidate.order, &mut cancelled_ids);
        }

        remaining_orders.retain(|order| {
            if cancelled_ids.contains(&order.id) {
                return false;
            }
            let position_key = Self::position_key(&order.strategy_id, &order.product_id);
            if matches!(
                order.order_type.as_str(),
                "STOP_LOSS" | "TAKE_PROFIT" | "TRAILING_STOP"
            ) {
                let protects_current_position =
                    self.positions.get(&position_key).is_some_and(|position| {
                        position.side == order.side && position.quantity > Decimal::ZERO
                    });
                let protects_pending_entry = pending_entries.contains(&(
                    order.strategy_id.clone(),
                    order.product_id.clone(),
                    order.side.clone(),
                ));
                return protects_current_position || protects_pending_entry;
            }
            match closed_position_sides.get(&position_key) {
                Some(protected_side) if matches!(order.order_type.as_str(), "MARKET" | "LIMIT") => {
                    order.side == *protected_side
                }
                _ => true,
            }
        });
        self.open_orders = remaining_orders;
        if let Some(ledger) = self.spot_ledger.as_mut() {
            for cancelled_id in cancelled_ids {
                ledger.release(&cancelled_id);
            }
        }
        Ok(fills)
    }

    fn consider_exit_candidate(
        candidate: ExitCandidate,
        selected: &mut Vec<ExitCandidate>,
        indexes: &mut HashMap<String, usize>,
        remaining_orders: &mut Vec<Order>,
    ) {
        let Some(index) = indexes.get(&candidate.position_key).copied() else {
            indexes.insert(candidate.position_key.clone(), selected.len());
            selected.push(candidate);
            return;
        };
        if Self::candidate_is_worse(&candidate, &selected[index]) {
            let previous = std::mem::replace(&mut selected[index], candidate);
            remaining_orders.push(previous.order);
        } else {
            remaining_orders.push(candidate.order);
        }
    }

    fn candidate_is_worse(candidate: &ExitCandidate, current: &ExitCandidate) -> bool {
        let worse_price = if candidate.protected_side == "LONG" {
            candidate.fill_price < current.fill_price
        } else {
            candidate.fill_price > current.fill_price
        };
        worse_price || (candidate.fill_price == current.fill_price && candidate.fee > current.fee)
    }

    fn reduces_current_position(&self, order: &Order) -> bool {
        let position_key = Self::position_key(&order.strategy_id, &order.product_id);
        self.positions.get(&position_key).is_some_and(|position| {
            position.quantity > Decimal::ZERO && position.side != order.side
        })
    }

    fn match_limit_order(
        &mut self,
        order: Order,
        candle: &Candlestick,
        fills: &mut Vec<FillEvent>,
        cancelled_ids: &mut HashSet<String>,
    ) -> Option<Order> {
        if order.product_id != candle.product_id {
            return Some(order);
        }
        if !Self::limit_order_matches(&order, candle) {
            return Some(order);
        }

        let fee = if self.settlement_model == SettlementModel::CashSpot {
            match self.settle_spot_order(&order, order.price, false) {
                Ok(settlement) => {
                    self.update_spot_position(&order, order.price, settlement);
                    settlement.fee
                }
                Err(reason) => {
                    self.reject_order(&order, candle.timestamp, reason);
                    return None;
                }
            }
        } else {
            let fee = self.calculate_fee(order.price, order.quantity, false);
            self.update_position(&order, order.price);
            let charged_fee = std::cmp::min(fee, self.balance);
            self.balance -= charged_fee;
            fee
        };
        fills.push(FillEvent {
            order_id: order.id.clone(),
            product_id: order.product_id.clone(),
            strategy_id: order.strategy_id.clone(),
            price: order.price,
            quantity: order.quantity,
            fee,
            timestamp: candle.timestamp,
            fill_type: "LIMIT".to_string(),
        });
        self.cancel_linked(&order, cancelled_ids);
        None
    }

    fn limit_order_matches(order: &Order, candle: &Candlestick) -> bool {
        if order.side == "LONG" {
            candle.low <= order.price
        } else {
            candle.high >= order.price
        }
    }

    /// Lower values execute first when multiple conditional legs touch in one bar.
    fn conditional_priority(order: &Order) -> u8 {
        match order.order_type.as_str() {
            "STOP_LOSS" => 0,
            "TRAILING_STOP" => 1,
            "TAKE_PROFIT" => 2,
            _ => 3,
        }
    }

    fn check_conditional_trigger(
        &self,
        order: &mut Order,
        candle: &Candlestick,
    ) -> Option<Decimal> {
        match order.order_type.as_str() {
            "STOP_LOSS" => {
                let trigger_price = order.trigger_price.unwrap_or(order.price);
                return Self::stop_trigger_fill(&order.side, trigger_price, candle);
            }
            "TAKE_PROFIT" => {
                let trigger_price = order.trigger_price.unwrap_or(order.price);
                if order.side == "LONG" {
                    if candle.high >= trigger_price {
                        return Some(trigger_price);
                    }
                } else if candle.low <= trigger_price {
                    return Some(trigger_price);
                }
            }
            "TRAILING_STOP" => {
                if let Some(trigger_price) = order.trigger_price {
                    if let Some(fill_price) =
                        Self::stop_trigger_fill(&order.side, trigger_price, candle)
                    {
                        return Some(fill_price);
                    }
                }
                Self::update_trailing_stop(order, candle);
                let updated_trigger = order.trigger_price?;
                return Self::intrabar_stop_trigger(&order.side, updated_trigger, candle);
            }
            _ => {}
        }
        None
    }

    fn stop_trigger_fill(
        side: &str,
        trigger_price: Decimal,
        candle: &Candlestick,
    ) -> Option<Decimal> {
        if side == "LONG" {
            if candle.open <= trigger_price {
                return Some(candle.open);
            }
            if candle.low <= trigger_price {
                return Some(trigger_price);
            }
        } else {
            if candle.open >= trigger_price {
                return Some(candle.open);
            }
            if candle.high >= trigger_price {
                return Some(trigger_price);
            }
        }
        None
    }

    fn intrabar_stop_trigger(
        side: &str,
        trigger_price: Decimal,
        candle: &Candlestick,
    ) -> Option<Decimal> {
        if (side == "LONG" && candle.low <= trigger_price)
            || (side == "SHORT" && candle.high >= trigger_price)
        {
            return Some(trigger_price);
        }
        None
    }

    fn update_trailing_stop(order: &mut Order, candle: &Candlestick) {
        let distance = match order.trailing_distance {
            Some(d) => d,
            None => return,
        };
        if order.side == "LONG" {
            let new_trigger = candle.high - distance;
            if order
                .trigger_price
                .is_none_or(|current_trigger| new_trigger > current_trigger)
            {
                order.trigger_price = Some(new_trigger);
            }
        } else {
            let new_trigger = candle.low + distance;
            if order
                .trigger_price
                .is_none_or(|current_trigger| new_trigger < current_trigger)
            {
                order.trigger_price = Some(new_trigger);
            }
        }
    }

    /// Mark the linked order (OCO counterpart) for cancellation.
    fn cancel_linked(
        &self,
        filled_order: &Order,
        cancelled_ids: &mut std::collections::HashSet<String>,
    ) {
        if let Some(ref linked_id) = filled_order.linked_order_id {
            cancelled_ids.insert(linked_id.clone());
        }
    }

    fn calculate_fee(&self, price: Decimal, quantity: Decimal, is_taker: bool) -> Decimal {
        let fee = if is_taker {
            self.taker_fee
        } else {
            self.maker_fee
        };
        if let Some(ledger) = &self.spot_ledger {
            return match ledger.fee_asset {
                SpotFeeAsset::Base => quantity * fee,
                SpotFeeAsset::Quote => price * quantity * fee,
            };
        }
        match self.fee_model {
            FeeModel::PercentageNotional => price * quantity * self.contract_multiplier * fee,
            FeeModel::PerContract => quantity * fee,
        }
    }

    fn settle_spot_order(
        &mut self,
        order: &Order,
        fill_price: Decimal,
        is_taker: bool,
    ) -> Result<CashSpotSettlement, String> {
        let fee_rate = if is_taker {
            self.taker_fee
        } else {
            self.maker_fee
        };
        let mut settlement_order = order.clone();
        if matches!(
            order.order_type.as_str(),
            "STOP_LOSS" | "TAKE_PROFIT" | "TRAILING_STOP"
        ) {
            if order.side != "LONG" {
                return Err("cash_spot conditional order cannot close a short position".to_string());
            }
            settlement_order.side = "SHORT".to_string();
        }
        if settlement_order.side == "SHORT" {
            let ledger = self
                .spot_ledger
                .as_ref()
                .ok_or_else(|| "cash_spot ledger is unavailable".to_string())?;
            let fee = match ledger.fee_asset {
                SpotFeeAsset::Base => order.quantity * fee_rate,
                SpotFeeAsset::Quote => fill_price * order.quantity * fee_rate,
            };
            let required_position = order.quantity
                + if ledger.fee_asset == SpotFeeAsset::Base {
                    fee
                } else {
                    Decimal::ZERO
                };
            let position_key = Self::position_key(&order.strategy_id, &order.product_id);
            let available_position = self
                .positions
                .get(&position_key)
                .filter(|position| position.side == "LONG")
                .map_or(Decimal::ZERO, |position| position.quantity);
            if required_position > available_position {
                return Err(format!(
                    "cash_spot insufficient strategy position at fill: required={required_position} available={available_position}"
                ));
            }
        }
        self.spot_ledger
            .as_mut()
            .ok_or_else(|| "cash_spot ledger is unavailable".to_string())?
            .settle(&settlement_order, fill_price, fee_rate)
    }

    fn reject_order(&mut self, order: &Order, timestamp: i64, reason: String) {
        if let Some(ledger) = self.spot_ledger.as_mut() {
            ledger.release(&order.id);
        }
        self.rejections.push(HashMap::from([
            ("order_id".to_string(), order.id.clone()),
            ("product_id".to_string(), order.product_id.clone()),
            ("strategy_id".to_string(), order.strategy_id.clone()),
            ("timestamp".to_string(), timestamp.to_string()),
            ("reason".to_string(), reason),
        ]));
    }

    fn update_spot_position(
        &mut self,
        order: &Order,
        fill_price: Decimal,
        settlement: CashSpotSettlement,
    ) {
        let key = Self::position_key(&order.strategy_id, &order.product_id);
        let mut position = self.positions.remove(&key).unwrap_or(Position {
            product_id: order.product_id.clone(),
            strategy_id: order.strategy_id.clone(),
            side: "FLAT".to_string(),
            quantity: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
        });

        if settlement.base_delta > Decimal::ZERO {
            let acquired_cost = -settlement.quote_delta;
            let prior_cost = position.quantity * position.entry_price;
            let new_quantity = position.quantity + settlement.base_delta;
            position.side = "LONG".to_string();
            position.quantity = new_quantity;
            position.entry_price = (prior_cost + acquired_cost) / new_quantity;
            self.spot_cost_basis += acquired_cost;
            self.positions.insert(key, position);
            return;
        }

        let reduction = -settlement.base_delta;
        let removed_cost = position.entry_price * reduction;
        let proceeds = settlement.quote_delta;
        self.spot_cost_basis -= removed_cost;
        self.spot_realized_pnl += proceeds - removed_cost;
        position.quantity -= reduction;
        if position.quantity > Decimal::ZERO {
            self.positions.insert(key, position);
        }

        debug_assert!(fill_price > Decimal::ZERO);
    }

    fn position_key(strategy_id: &str, product_id: &str) -> String {
        format!("{strategy_id}:{product_id}")
    }

    fn update_position(&mut self, order: &Order, fill_price: Decimal) {
        let key = Self::position_key(&order.strategy_id, &order.product_id);
        let mut pos = self.positions.remove(&key).unwrap_or(Position {
            product_id: order.product_id.clone(),
            strategy_id: order.strategy_id.clone(),
            side: "FLAT".to_string(),
            quantity: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
        });

        let is_closing_order = matches!(
            order.order_type.as_str(),
            "STOP_LOSS" | "TAKE_PROFIT" | "TRAILING_STOP"
        );

        if is_closing_order {
            self.close_position(&mut pos, order, fill_price);
        } else {
            self.apply_position_change(&mut pos, order, fill_price);
        }

        if pos.quantity > Decimal::ZERO && pos.side != "FLAT" {
            self.positions.insert(key, pos);
        }
    }

    /// Close position for conditional orders (SL/TP/Trailing).
    fn close_position(&mut self, pos: &mut Position, order: &Order, fill_price: Decimal) {
        if pos.quantity.is_zero() || pos.side == "FLAT" {
            return;
        }

        let close_qty = order.quantity.min(pos.quantity);
        let price_diff = if pos.side == "LONG" {
            fill_price - pos.entry_price
        } else {
            pos.entry_price - fill_price
        };
        let realized_pnl = price_diff * close_qty * self.contract_multiplier;
        self.balance += realized_pnl;

        let remaining = pos.quantity - close_qty;
        if remaining > Decimal::ZERO {
            pos.quantity = remaining;
        } else {
            pos.side = "FLAT".to_string();
            pos.quantity = Decimal::ZERO;
            pos.entry_price = Decimal::ZERO;
        }
    }

    /// Apply position change for MARKET/LIMIT orders (open, increase, reduce, flip).
    fn apply_position_change(&mut self, pos: &mut Position, order: &Order, fill_price: Decimal) {
        if pos.quantity.is_zero() || pos.side == "FLAT" {
            pos.side = order.side.clone();
            pos.quantity = order.quantity;
            pos.entry_price = fill_price;
        } else if pos.side == order.side {
            let total_cost = pos.quantity * pos.entry_price + order.quantity * fill_price;
            let new_qty = pos.quantity + order.quantity;
            pos.entry_price = total_cost / new_qty;
            pos.quantity = new_qty;
        } else {
            let close_qty = order.quantity.min(pos.quantity);
            let price_diff = if pos.side == "LONG" {
                fill_price - pos.entry_price
            } else {
                pos.entry_price - fill_price
            };
            let realized_pnl = price_diff * close_qty * self.contract_multiplier;
            self.balance += realized_pnl;

            let remaining = pos.quantity - close_qty;
            let excess = order.quantity - close_qty;

            if remaining > Decimal::ZERO {
                pos.quantity = remaining;
            } else if excess > Decimal::ZERO {
                pos.side = order.side.clone();
                pos.quantity = excess;
                pos.entry_price = fill_price;
            } else {
                pos.side = "FLAT".to_string();
                pos.quantity = Decimal::ZERO;
                pos.entry_price = Decimal::ZERO;
            }
        }
    }
}

fn parse_decimal(s: &str, field: &str) -> PyResult<Decimal> {
    Decimal::from_str(s).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid decimal for '{field}': {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::models::{Candlestick, Order, Position};
    use rust_decimal_macros::dec;

    const PRODUCT: &str = "BINANCE:BTCUSDT-PERP";
    const TF: &str = "1m";
    const STRATEGY: &str = "test_strategy";

    fn pos_key(strategy_id: &str, product_id: &str) -> String {
        format!("{strategy_id}:{product_id}")
    }

    fn make_engine(balance: Decimal) -> PyMatchingEngine {
        PyMatchingEngine {
            balance,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: dec!(0.0002),
            taker_fee: dec!(0.0006),
            market_slippage_bps: Decimal::ZERO,
            market_slippage_price_tick: None,
            contract_multiplier: Decimal::ONE,
            fee_model: FeeModel::PercentageNotional,
            settlement_model: SettlementModel::Derivatives,
            spot_ledger: None,
            spot_cost_basis: Decimal::ZERO,
            spot_realized_pnl: Decimal::ZERO,
            rejections: Vec::new(),
            warnings: Vec::new(),
            scaled_price_tick: None,
            scaled_volume_step: None,
        }
    }

    fn make_candle(open: Decimal, high: Decimal, low: Decimal, close: Decimal) -> Candlestick {
        Candlestick {
            product_id: PRODUCT.to_string(),
            timeframe: TF.to_string(),
            timestamp: 1000,
            open,
            high,
            low,
            close,
            volume: dec!(100),
        }
    }

    fn make_order(id: &str, side: &str, order_type: &str, price: Decimal, qty: Decimal) -> Order {
        Order {
            id: id.to_string(),
            product_id: PRODUCT.to_string(),
            strategy_id: STRATEGY.to_string(),
            side: side.to_string(),
            order_type: order_type.to_string(),
            price,
            quantity: qty,
            timestamp: 900,
            trigger_price: None,
            trailing_distance: None,
            linked_order_id: None,
        }
    }

    fn make_position(
        product_id: &str,
        strategy_id: &str,
        side: &str,
        qty: Decimal,
        entry: Decimal,
    ) -> Position {
        Position {
            product_id: product_id.to_string(),
            strategy_id: strategy_id.to_string(),
            side: side.to_string(),
            quantity: qty,
            entry_price: entry,
            unrealized_pnl: Decimal::ZERO,
        }
    }

    // ── Market Orders ──

    #[test]
    fn test_market_order_long_fills_at_open() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("m1", "LONG", "MARKET", Decimal::ZERO, dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(50000));
        assert_eq!(fills[0].fill_type, "MARKET");

        let key = pos_key(STRATEGY, PRODUCT);
        let pos = engine.positions.get(&key).unwrap();
        assert_eq!(pos.side, "LONG");
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.entry_price, dec!(50000));
        assert_eq!(pos.strategy_id, STRATEGY);
    }

    #[test]
    fn market_slippage_rounds_adversely_without_clamping_to_candle_range() {
        for (side, expected) in [("LONG", dec!(100.05)), ("SHORT", dec!(99.95))] {
            let mut engine = make_engine(dec!(100000));
            engine.market_slippage_bps = dec!(1);
            engine.market_slippage_price_tick = Some(dec!(0.05));
            engine.open_orders.push(make_order(
                "slipped",
                side,
                "MARKET",
                Decimal::ZERO,
                dec!(1),
            ));

            let fills = engine
                .process_candle_logic(make_candle(dec!(100), dec!(100), dec!(100), dec!(100)))
                .unwrap();

            assert_eq!(fills[0].price, expected);
        }
    }

    #[test]
    fn market_slippage_preserves_smallest_representable_positive_bps() {
        for (side, expected) in [("LONG", dec!(1.01)), ("SHORT", dec!(0.99))] {
            let mut engine = make_engine(dec!(100000));
            engine.market_slippage_bps = dec!(0.0000000000000000000000000001);
            engine.market_slippage_price_tick = Some(dec!(0.01));
            engine.open_orders.push(make_order(
                "smallest-bps",
                side,
                "MARKET",
                Decimal::ZERO,
                dec!(1),
            ));

            let fills = engine
                .process_candle_logic(make_candle(dec!(1), dec!(1), dec!(1), dec!(1)))
                .unwrap();

            assert_eq!(fills[0].price, expected);
        }
    }

    #[test]
    fn market_slippage_does_not_change_limit_order_price() {
        let mut engine = make_engine(dec!(100000));
        engine.market_slippage_bps = dec!(10);
        engine.market_slippage_price_tick = Some(dec!(0.01));
        engine
            .open_orders
            .push(make_order("limit", "LONG", "LIMIT", dec!(99), dec!(1)));

        let fills = engine
            .process_candle_logic(make_candle(dec!(100), dec!(101), dec!(98), dec!(100)))
            .unwrap();

        assert_eq!(fills[0].price, dec!(99));
    }

    #[test]
    fn market_slippage_rejects_only_invalid_order_and_preserves_candle_progress() {
        let mut engine = make_engine(dec!(100000));
        engine.market_slippage_bps = dec!(1);
        engine.market_slippage_price_tick = Some(dec!(1));
        engine.open_orders.push(make_order(
            "filled-before-rejection",
            "LONG",
            "MARKET",
            Decimal::ZERO,
            dec!(1),
        ));
        engine.open_orders.push(make_order(
            "invalid-rounded-price",
            "SHORT",
            "MARKET",
            Decimal::ZERO,
            dec!(1),
        ));
        engine.open_orders.push(make_order(
            "filled-after-rejection",
            "LONG",
            "MARKET",
            Decimal::ZERO,
            dec!(1),
        ));
        let mut other_product =
            make_order("other-product", "LONG", "MARKET", Decimal::ZERO, dec!(1));
        other_product.product_id = "OTHER".to_string();
        engine.open_orders.push(other_product);

        let fills = engine
            .process_candle_logic(make_candle(dec!(1), dec!(1), dec!(1), dec!(1)))
            .unwrap();

        assert_eq!(
            fills
                .iter()
                .map(|fill| fill.order_id.as_str())
                .collect::<Vec<_>>(),
            vec!["filled-before-rejection", "filled-after-rejection"]
        );
        assert_eq!(engine.open_orders.len(), 1);
        assert_eq!(engine.open_orders[0].id, "other-product");
        let rejections = engine.drain_rejections();
        assert_eq!(rejections.len(), 1);
        assert_eq!(rejections[0]["order_id"], "invalid-rounded-price");
        assert!(rejections[0]["reason"].contains("non-positive tick-rounded fill price"));
        let position = engine.positions.get(&pos_key(STRATEGY, PRODUCT)).unwrap();
        assert_eq!(position.side, "LONG");
        assert_eq!(position.quantity, dec!(2));
        assert_eq!(position.entry_price, dec!(2));
    }

    #[test]
    fn market_slippage_overflow_rejects_orders_without_dropping_other_products() {
        let mut engine = make_engine(dec!(100000));
        engine.market_slippage_bps = dec!(1);
        engine.market_slippage_price_tick = Some(dec!(0.01));
        engine.open_orders.push(make_order(
            "overflow",
            "LONG",
            "MARKET",
            Decimal::ZERO,
            dec!(1),
        ));
        let mut other_product =
            make_order("other-product", "LONG", "MARKET", Decimal::ZERO, dec!(1));
        other_product.product_id = "OTHER".to_string();
        engine.open_orders.push(other_product);

        let fills = engine
            .process_candle_logic(make_candle(
                Decimal::MAX,
                Decimal::MAX,
                Decimal::MAX,
                Decimal::MAX,
            ))
            .unwrap();

        assert!(fills.is_empty());
        assert_eq!(engine.open_orders.len(), 1);
        assert_eq!(engine.open_orders[0].id, "other-product");
        assert!(engine.positions.is_empty());
        let rejections = engine.drain_rejections();
        assert_eq!(rejections.len(), 1);
        assert_eq!(rejections[0]["order_id"], "overflow");
        assert!(rejections[0]["reason"].contains("arithmetic overflow"));
    }

    #[test]
    fn test_scaled_candle_market_order_matches_decimal_candle() {
        let mut decimal_engine = make_engine(dec!(100000));
        decimal_engine.market_slippage_bps = dec!(10);
        decimal_engine.market_slippage_price_tick = Some(dec!(0.01));
        decimal_engine
            .open_orders
            .push(make_order("m1", "LONG", "MARKET", Decimal::ZERO, dec!(1)));
        let decimal_fills = decimal_engine
            .process_candle_logic(make_candle(
                dec!(50000),
                dec!(51000),
                dec!(49000),
                dec!(50500),
            ))
            .unwrap();

        let mut scaled_engine = make_engine(dec!(100000));
        scaled_engine.market_slippage_bps = dec!(10);
        scaled_engine.market_slippage_price_tick = Some(dec!(0.01));
        scaled_engine
            .open_orders
            .push(make_order("m1", "LONG", "MARKET", Decimal::ZERO, dec!(1)));
        scaled_engine.scaled_price_tick = Some(dec!(0.01));
        scaled_engine.scaled_volume_step = Some(dec!(0.001));
        let scaled_fills = scaled_engine
            .on_scaled_candle(ScaledCandlestick {
                product_id: PRODUCT.to_string(),
                timeframe: TF.to_string(),
                timestamp: 1000,
                open_units: 5_000_000,
                high_units: 5_100_000,
                low_units: 4_900_000,
                close_units: 5_050_000,
                volume_units: 100_000,
            })
            .unwrap();

        assert_eq!(scaled_fills.len(), decimal_fills.len());
        assert_eq!(decimal_fills[0].price, dec!(50050));
        assert_eq!(scaled_fills[0].price, decimal_fills[0].price);
        assert_eq!(scaled_fills[0].fee, decimal_fills[0].fee);
        assert_eq!(scaled_engine.balance, decimal_engine.balance);
    }

    #[test]
    fn test_market_order_short_fills_at_open() {
        let mut engine = make_engine(dec!(100000));
        engine.open_orders.push(make_order(
            "m2",
            "SHORT",
            "MARKET",
            Decimal::ZERO,
            dec!(0.5),
        ));

        let candle = make_candle(dec!(48000), dec!(49000), dec!(47000), dec!(48500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(48000));

        let key = pos_key(STRATEGY, PRODUCT);
        let pos = engine.positions.get(&key).unwrap();
        assert_eq!(pos.side, "SHORT");
        assert_eq!(pos.quantity, dec!(0.5));
    }

    #[test]
    fn test_market_order_taker_fee_deducted() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("m3", "LONG", "MARKET", Decimal::ZERO, dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        let expected_fee = dec!(50000) * dec!(1) * dec!(0.0006);
        assert_eq!(fills[0].fee, expected_fee);
        assert!(engine.balance < dec!(100000));
    }

    #[test]
    fn test_percentage_fee_uses_contract_multiplier() {
        let mut engine = make_engine(dec!(100000));
        engine.contract_multiplier = dec!(2);
        engine.maker_fee = dec!(0.001);
        engine.taker_fee = dec!(0.002);

        assert_eq!(engine.calculate_fee(dec!(100), dec!(3), false), dec!(0.6));
        assert_eq!(engine.calculate_fee(dec!(100), dec!(3), true), dec!(1.2));
    }

    #[test]
    fn test_per_contract_fee_ignores_price_and_multiplier() {
        let mut engine = make_engine(dec!(100000));
        engine.fee_model = FeeModel::PerContract;
        engine.contract_multiplier = dec!(2);
        engine.maker_fee = dec!(1.25);
        engine.taker_fee = dec!(1.75);

        assert_eq!(engine.calculate_fee(dec!(100), dec!(3), false), dec!(3.75));
        assert_eq!(engine.calculate_fee(dec!(100), dec!(3), true), dec!(5.25));
    }

    #[test]
    fn test_unknown_fee_model_is_rejected() {
        let error = PyMatchingEngine::new(
            "100000".to_string(),
            "0".to_string(),
            "0".to_string(),
            "1".to_string(),
            "unknown".to_string(),
            "derivatives".to_string(),
            "".to_string(),
            "".to_string(),
            "quote".to_string(),
            "0".to_string(),
            None,
        )
        .err()
        .expect("unknown fee model must fail");
        assert!(error.to_string().contains("unsupported fee_model"));
    }

    #[test]
    fn test_market_order_different_product_not_filled() {
        let mut engine = make_engine(dec!(100000));
        let mut order = make_order("m4", "LONG", "MARKET", Decimal::ZERO, dec!(1));
        order.product_id = "BINANCE:ETHUSDT-PERP".to_string();
        engine.open_orders.push(order);

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 0);
        assert_eq!(engine.open_orders.len(), 1);
    }

    // ── Limit Orders ──

    #[test]
    fn test_limit_order_long_fills_when_low_touches_price() {
        let mut engine = make_engine(dec!(100000));
        let mut order = make_order("l1", "LONG", "LIMIT", dec!(49500), dec!(1));
        order.price = dec!(49500);
        engine.open_orders.push(order);

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(49500));
        assert_eq!(fills[0].fill_type, "LIMIT");
    }

    #[test]
    fn test_limit_order_long_not_filled_when_low_above_price() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("l2", "LONG", "LIMIT", dec!(48000), dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 0);
        assert_eq!(engine.open_orders.len(), 1);
    }

    #[test]
    fn test_limit_order_short_fills_when_high_touches_price() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("l3", "SHORT", "LIMIT", dec!(50500), dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(50500));
    }

    #[test]
    fn test_limit_order_uses_maker_fee() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("l4", "LONG", "LIMIT", dec!(49500), dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        let expected_fee = dec!(49500) * dec!(1) * dec!(0.0002);
        assert_eq!(fills[0].fee, expected_fee);
    }

    #[test]
    fn test_limit_entry_activates_protection_on_same_candle() {
        let mut engine = make_engine(dec!(100000));
        let entry = make_order("entry", "LONG", "LIMIT", dec!(100), dec!(1));
        let mut stop = make_order("stop", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        stop.trigger_price = Some(dec!(90));
        stop.linked_order_id = Some("take_profit".to_string());
        let mut take_profit =
            make_order("take_profit", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        take_profit.trigger_price = Some(dec!(110));
        take_profit.linked_order_id = Some("stop".to_string());
        engine.open_orders.extend([entry, stop, take_profit]);

        let fills = engine
            .process_candle_logic(make_candle(dec!(105), dec!(115), dec!(85), dec!(100)))
            .unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].fill_type, "LIMIT");
        assert_eq!(fills[1].fill_type, "STOP_LOSS");
        assert!(engine.positions.is_empty());
        assert!(engine.open_orders.is_empty());
    }

    #[test]
    fn test_unfilled_limit_entry_keeps_protection_pending() {
        let mut engine = make_engine(dec!(100000));
        let entry = make_order("entry", "LONG", "LIMIT", dec!(90), dec!(1));
        let mut take_profit =
            make_order("take_profit", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        take_profit.trigger_price = Some(dec!(110));
        engine.open_orders.extend([entry, take_profit]);

        let fills = engine
            .process_candle_logic(make_candle(dec!(100), dec!(115), dec!(95), dec!(105)))
            .unwrap();

        assert!(fills.is_empty());
        assert_eq!(engine.open_orders.len(), 2);
    }

    // ── Stop Loss ──

    #[test]
    fn test_stop_loss_long_triggers_when_low_hits() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key.clone(),
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        let mut sl = make_order("sl1", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(49000));
        engine.open_orders.push(sl);

        let candle = make_candle(dec!(50000), dec!(50500), dec!(48500), dec!(49200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "STOP_LOSS");
        assert_eq!(fills[0].price, dec!(49000));

        assert!(!engine.positions.contains_key(&key));

        // PnL: (49000 - 50000) * 1 = -1000
        let expected_balance = dec!(100000) - dec!(1000) - fills[0].fee;
        assert_eq!(engine.balance, expected_balance);
    }

    #[test]
    fn test_stop_loss_short_triggers_when_high_hits() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "SHORT", dec!(1), dec!(50000)),
        );

        let mut sl = make_order("sl2", "SHORT", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(51000));
        engine.open_orders.push(sl);

        let candle = make_candle(dec!(50200), dec!(51500), dec!(49800), dec!(50800));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "STOP_LOSS");

        // PnL: (50000 - 51000) * 1 = -1000
        let expected_balance = dec!(100000) - dec!(1000) - fills[0].fee;
        assert_eq!(engine.balance, expected_balance);
    }

    #[test]
    fn test_stop_loss_not_triggered_when_price_doesnt_reach() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        let mut sl = make_order("sl3", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(48000));
        engine.open_orders.push(sl);

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 0);
        assert_eq!(engine.open_orders.len(), 1);
    }

    // ── Take Profit ──

    #[test]
    fn test_take_profit_long_triggers_when_high_hits() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        let mut tp = make_order("tp1", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        tp.trigger_price = Some(dec!(52000));
        engine.open_orders.push(tp);

        let candle = make_candle(dec!(50500), dec!(52500), dec!(50000), dec!(52000));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "TAKE_PROFIT");

        // PnL: (52000 - 50000) * 1 = +2000
        let expected_balance = dec!(100000) + dec!(2000) - fills[0].fee;
        assert_eq!(engine.balance, expected_balance);
    }

    #[test]
    fn test_take_profit_short_triggers_when_low_hits() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "SHORT", dec!(1), dec!(50000)),
        );

        let mut tp = make_order("tp2", "SHORT", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        tp.trigger_price = Some(dec!(48000));
        engine.open_orders.push(tp);

        let candle = make_candle(dec!(49000), dec!(49500), dec!(47500), dec!(48200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);

        // PnL: (50000 - 48000) * 1 = +2000
        let expected_balance = dec!(100000) + dec!(2000) - fills[0].fee;
        assert_eq!(engine.balance, expected_balance);
    }

    // ── Trailing Stop ──

    #[test]
    fn test_trailing_stop_long_updates_and_triggers() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        let mut ts = make_order("ts1", "LONG", "TRAILING_STOP", Decimal::ZERO, dec!(1));
        ts.trigger_price = Some(dec!(49000));
        ts.trailing_distance = Some(dec!(1000));
        engine.open_orders.push(ts);

        // Candle 1: high=52000 → new trigger = 52000 - 1000 = 51000
        let c1 = make_candle(dec!(51500), dec!(52000), dec!(51200), dec!(51800));
        let fills = engine.process_candle_logic(c1).unwrap();
        assert_eq!(fills.len(), 0);

        let updated_trigger = engine.open_orders[0].trigger_price.unwrap();
        assert_eq!(updated_trigger, dec!(51000));

        // Candle 2: low=50500 <= 51000 → triggers
        let c2 = make_candle(dec!(51200), dec!(51500), dec!(50500), dec!(50800));
        let fills = engine.process_candle_logic(c2).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "TRAILING_STOP");
    }

    #[test]
    fn test_trailing_stop_short_updates_and_triggers() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "SHORT", dec!(1), dec!(50000)),
        );

        let mut ts = make_order("ts2", "SHORT", "TRAILING_STOP", Decimal::ZERO, dec!(1));
        ts.trigger_price = Some(dec!(51000));
        ts.trailing_distance = Some(dec!(1000));
        engine.open_orders.push(ts);

        // Candle 1: low=48000 → new trigger = 48000 + 1000 = 49000
        let c1 = make_candle(dec!(48500), dec!(48800), dec!(48000), dec!(48300));
        let fills = engine.process_candle_logic(c1).unwrap();
        assert_eq!(fills.len(), 0);

        let updated_trigger = engine.open_orders[0].trigger_price.unwrap();
        assert_eq!(updated_trigger, dec!(49000));

        // Candle 2: high=49500 >= 49000 → triggers
        let c2 = make_candle(dec!(48800), dec!(49500), dec!(48500), dec!(49200));
        let fills = engine.process_candle_logic(c2).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "TRAILING_STOP");
    }

    #[test]
    fn test_trailing_stop_only_moves_in_favorable_direction() {
        let mut engine = make_engine(dec!(100000));

        let mut ts = make_order("ts3", "LONG", "TRAILING_STOP", Decimal::ZERO, dec!(1));
        ts.trigger_price = Some(dec!(49000));
        ts.trailing_distance = Some(dec!(1000));
        engine.open_orders.push(ts);

        // high=49500 → new_trigger = 49500 - 1000 = 48500 < 49000 → should NOT move down
        let c = make_candle(dec!(49000), dec!(49500), dec!(48800), dec!(49200));
        PyMatchingEngine::update_trailing_stop(&mut engine.open_orders[0], &c);

        assert_eq!(engine.open_orders[0].trigger_price.unwrap(), dec!(49000));
    }

    // ── OCO (One-Cancels-Other) ──

    #[test]
    fn test_oco_sl_triggers_cancels_tp() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        let mut sl = make_order("sl_oco", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(49000));
        sl.linked_order_id = Some("tp_oco".to_string());
        engine.open_orders.push(sl);

        let mut tp = make_order("tp_oco", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        tp.trigger_price = Some(dec!(52000));
        tp.linked_order_id = Some("sl_oco".to_string());
        engine.open_orders.push(tp);

        let candle = make_candle(dec!(50000), dec!(50500), dec!(48500), dec!(49200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, "sl_oco");
        assert!(engine.open_orders.is_empty());
    }

    #[test]
    fn test_oco_tp_triggers_cancels_sl() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        let mut sl = make_order("sl_oco2", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(48000));
        sl.linked_order_id = Some("tp_oco2".to_string());
        engine.open_orders.push(sl);

        let mut tp = make_order("tp_oco2", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        tp.trigger_price = Some(dec!(51000));
        tp.linked_order_id = Some("sl_oco2".to_string());
        engine.open_orders.push(tp);

        let candle = make_candle(dec!(50500), dec!(51500), dec!(50000), dec!(51200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, "tp_oco2");
        assert!(engine.open_orders.is_empty());
    }

    #[test]
    fn test_oco_trigger_matrix_uses_worst_case_fill() {
        let cases = [
            ("LONG", dec!(50500), dec!(49500), None),
            ("LONG", dec!(50500), dec!(48500), Some("STOP_LOSS")),
            ("LONG", dec!(51500), dec!(49500), Some("TAKE_PROFIT")),
            ("LONG", dec!(51500), dec!(48500), Some("STOP_LOSS")),
            ("SHORT", dec!(50500), dec!(49500), None),
            ("SHORT", dec!(51500), dec!(49500), Some("STOP_LOSS")),
            ("SHORT", dec!(50500), dec!(48500), Some("TAKE_PROFIT")),
            ("SHORT", dec!(51500), dec!(48500), Some("STOP_LOSS")),
        ];

        for (side, high, low, expected_fill_type) in cases {
            for reverse_submission_order in [false, true] {
                let mut engine = make_engine(dec!(100000));
                engine.positions.insert(
                    pos_key(STRATEGY, PRODUCT),
                    make_position(PRODUCT, STRATEGY, side, dec!(1), dec!(50000)),
                );

                let (sl_trigger, tp_trigger) = if side == "LONG" {
                    (dec!(49000), dec!(51000))
                } else {
                    (dec!(51000), dec!(49000))
                };
                let mut sl = make_order("matrix_sl", side, "STOP_LOSS", Decimal::ZERO, dec!(1));
                sl.trigger_price = Some(sl_trigger);
                sl.linked_order_id = Some("matrix_tp".to_string());
                let mut tp = make_order("matrix_tp", side, "TAKE_PROFIT", Decimal::ZERO, dec!(1));
                tp.trigger_price = Some(tp_trigger);
                tp.linked_order_id = Some("matrix_sl".to_string());

                if reverse_submission_order {
                    engine.open_orders.extend([tp, sl]);
                } else {
                    engine.open_orders.extend([sl, tp]);
                }

                let candle = make_candle(dec!(50000), high, low, dec!(50000));
                let fills = engine.process_candle_logic(candle).unwrap();

                match expected_fill_type {
                    Some(expected) => {
                        assert_eq!(fills.len(), 1, "side={side} high={high} low={low}");
                        assert_eq!(fills[0].fill_type, expected);
                        assert!(engine.open_orders.is_empty());
                    }
                    None => {
                        assert!(fills.is_empty(), "side={side} high={high} low={low}");
                        assert_eq!(engine.open_orders.len(), 2);
                    }
                }
            }
        }
    }

    #[test]
    fn test_stop_loss_gap_uses_worse_open_price() {
        let cases = [
            ("LONG", dec!(49000), dec!(48000), dec!(50000), dec!(47000)),
            ("SHORT", dec!(51000), dec!(52000), dec!(53000), dec!(50000)),
        ];

        for (side, trigger, open, high, low) in cases {
            let mut engine = make_engine(dec!(100000));
            engine.positions.insert(
                pos_key(STRATEGY, PRODUCT),
                make_position(PRODUCT, STRATEGY, side, dec!(1), dec!(50000)),
            );
            let mut stop = make_order("gap_sl", side, "STOP_LOSS", Decimal::ZERO, dec!(1));
            stop.trigger_price = Some(trigger);
            engine.open_orders.push(stop);

            let fills = engine
                .process_candle_logic(make_candle(open, high, low, open))
                .unwrap();

            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].price, open);
        }
    }

    #[test]
    fn test_market_exit_discards_protection_before_it_can_reopen_position() {
        let mut engine = make_engine(dec!(100000));
        engine.positions.insert(
            pos_key(STRATEGY, PRODUCT),
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(100)),
        );
        let exit = make_order("exit", "SHORT", "MARKET", Decimal::ZERO, dec!(1));
        let mut stop = make_order("stop", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        stop.trigger_price = Some(dec!(90));
        stop.linked_order_id = Some("take_profit".to_string());
        let mut take_profit =
            make_order("take_profit", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        take_profit.trigger_price = Some(dec!(110));
        take_profit.linked_order_id = Some("stop".to_string());
        engine.open_orders.extend([stop, take_profit, exit]);

        let fills = engine
            .process_candle_logic(make_candle(dec!(100), dec!(115), dec!(85), dec!(100)))
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "MARKET");
        assert!(engine.positions.is_empty());
        assert!(engine.open_orders.is_empty());
    }

    #[test]
    fn test_worst_exit_candidate_matrix() {
        let cases = [
            (
                "LONG",
                "STOP_LOSS",
                dec!(90),
                dec!(105),
                "STOP_LOSS",
                dec!(90),
            ),
            (
                "LONG",
                "TAKE_PROFIT",
                dec!(110),
                dec!(105),
                "LIMIT",
                dec!(105),
            ),
            (
                "LONG",
                "TRAILING_STOP",
                dec!(90),
                dec!(105),
                "TRAILING_STOP",
                dec!(90),
            ),
            (
                "LONG",
                "TRAILING_STOP",
                dec!(110),
                dec!(95),
                "LIMIT",
                dec!(95),
            ),
            (
                "SHORT",
                "STOP_LOSS",
                dec!(110),
                dec!(95),
                "STOP_LOSS",
                dec!(110),
            ),
            (
                "SHORT",
                "TAKE_PROFIT",
                dec!(90),
                dec!(95),
                "LIMIT",
                dec!(95),
            ),
            (
                "SHORT",
                "TRAILING_STOP",
                dec!(110),
                dec!(95),
                "TRAILING_STOP",
                dec!(110),
            ),
            (
                "SHORT",
                "TRAILING_STOP",
                dec!(90),
                dec!(105),
                "LIMIT",
                dec!(105),
            ),
            (
                "LONG",
                "TAKE_PROFIT",
                dec!(105),
                dec!(105),
                "TAKE_PROFIT",
                dec!(105),
            ),
        ];

        for (position_side, protection_type, trigger, limit_price, expected_type, expected_price) in
            cases
        {
            let mut engine = make_engine(dec!(100000));
            engine.positions.insert(
                pos_key(STRATEGY, PRODUCT),
                make_position(PRODUCT, STRATEGY, position_side, dec!(1), dec!(100)),
            );
            let exit_side = if position_side == "LONG" {
                "SHORT"
            } else {
                "LONG"
            };
            let exit = make_order("exit", exit_side, "LIMIT", limit_price, dec!(1));
            let mut protection = make_order(
                "protection",
                position_side,
                protection_type,
                Decimal::ZERO,
                dec!(1),
            );
            protection.trigger_price = Some(trigger);
            if protection_type == "TRAILING_STOP" {
                protection.trailing_distance = Some(dec!(10));
            }
            engine.open_orders.extend([exit, protection]);

            let fills = engine
                .process_candle_logic(make_candle(dec!(100), dec!(115), dec!(85), dec!(100)))
                .unwrap();

            assert_eq!(
                fills.len(),
                1,
                "side={position_side} type={protection_type}"
            );
            assert_eq!(fills[0].fill_type, expected_type);
            assert_eq!(fills[0].price, expected_price);
            assert!(engine.positions.is_empty());
            assert!(engine.open_orders.is_empty());
        }
    }

    #[test]
    fn test_filled_limit_exit_discards_untouched_protection() {
        let mut engine = make_engine(dec!(100000));
        engine.positions.insert(
            pos_key(STRATEGY, PRODUCT),
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(100)),
        );
        let exit = make_order("exit", "SHORT", "LIMIT", dec!(110), dec!(1));
        let mut stop = make_order("stop", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        stop.trigger_price = Some(dec!(90));
        engine.open_orders.extend([exit, stop]);

        let fills = engine
            .process_candle_logic(make_candle(dec!(100), dec!(115), dec!(95), dec!(110)))
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "LIMIT");
        assert!(engine.positions.is_empty());
        assert!(engine.open_orders.is_empty());
    }

    #[test]
    fn test_trailing_stop_prefers_existing_trigger_before_same_bar_update() {
        let cases = [
            (
                "LONG",
                dec!(49000),
                dec!(1000),
                make_candle(dec!(50000), dec!(52000), dec!(48000), dec!(50000)),
            ),
            (
                "SHORT",
                dec!(51000),
                dec!(1000),
                make_candle(dec!(50000), dec!(52000), dec!(48000), dec!(50000)),
            ),
        ];

        for (side, trigger, distance, candle) in cases {
            let mut engine = make_engine(dec!(100000));
            engine.positions.insert(
                pos_key(STRATEGY, PRODUCT),
                make_position(PRODUCT, STRATEGY, side, dec!(1), dec!(50000)),
            );
            let mut trailing = make_order(
                "trailing_old",
                side,
                "TRAILING_STOP",
                Decimal::ZERO,
                dec!(1),
            );
            trailing.trigger_price = Some(trigger);
            trailing.trailing_distance = Some(distance);
            engine.open_orders.push(trailing);

            let fills = engine.process_candle_logic(candle).unwrap();

            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].price, trigger);
        }
    }

    #[test]
    fn test_trailing_stop_can_trigger_after_same_bar_favorable_move() {
        let cases = [
            (
                "LONG",
                dec!(49000),
                dec!(51000),
                make_candle(dec!(50000), dec!(52000), dec!(50000), dec!(50500)),
            ),
            (
                "SHORT",
                dec!(51000),
                dec!(49000),
                make_candle(dec!(50000), dec!(50000), dec!(48000), dec!(49500)),
            ),
        ];

        for (side, old_trigger, expected_fill, candle) in cases {
            let mut engine = make_engine(dec!(100000));
            engine.positions.insert(
                pos_key(STRATEGY, PRODUCT),
                make_position(PRODUCT, STRATEGY, side, dec!(1), dec!(50000)),
            );
            let mut trailing = make_order(
                "trailing_new",
                side,
                "TRAILING_STOP",
                Decimal::ZERO,
                dec!(1),
            );
            trailing.trigger_price = Some(old_trigger);
            trailing.trailing_distance = Some(dec!(1000));
            engine.open_orders.push(trailing);

            let fills = engine.process_candle_logic(candle).unwrap();

            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].price, expected_fill);
        }
    }

    #[test]
    fn test_trailing_stop_without_initial_trigger_supports_both_sides() {
        let cases = [
            (
                "LONG",
                dec!(105),
                make_candle(dec!(100), dec!(115), dec!(100), dec!(105)),
            ),
            (
                "SHORT",
                dec!(95),
                make_candle(dec!(100), dec!(100), dec!(85), dec!(95)),
            ),
        ];

        for (side, expected_fill, candle) in cases {
            let mut engine = make_engine(dec!(100000));
            engine.positions.insert(
                pos_key(STRATEGY, PRODUCT),
                make_position(PRODUCT, STRATEGY, side, dec!(1), dec!(100)),
            );
            let mut trailing = make_order(
                "trailing_none",
                side,
                "TRAILING_STOP",
                Decimal::ZERO,
                dec!(1),
            );
            trailing.trailing_distance = Some(dec!(10));
            engine.open_orders.push(trailing);

            let fills = engine.process_candle_logic(candle).unwrap();

            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].price, expected_fill);
        }
    }

    // ── Position Management ──

    #[test]
    fn test_position_add_to_existing_averages_cost() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("add1", "LONG", "MARKET", Decimal::ZERO, dec!(1)));

        let c1 = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        engine.process_candle_logic(c1).unwrap();

        engine
            .open_orders
            .push(make_order("add2", "LONG", "MARKET", Decimal::ZERO, dec!(1)));

        let c2 = make_candle(dec!(52000), dec!(53000), dec!(51000), dec!(52500));
        engine.process_candle_logic(c2).unwrap();

        let key = pos_key(STRATEGY, PRODUCT);
        let pos = engine.positions.get(&key).unwrap();
        assert_eq!(pos.quantity, dec!(2));
        // avg: (50000*1 + 52000*1) / 2 = 51000
        assert_eq!(pos.entry_price, dec!(51000));
    }

    #[test]
    fn test_position_partial_close_reduces_quantity() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key.clone(),
            make_position(PRODUCT, STRATEGY, "LONG", dec!(2), dec!(50000)),
        );

        let mut sl = make_order("pc1", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(49000));
        engine.open_orders.push(sl);

        let candle = make_candle(dec!(50000), dec!(50500), dec!(48500), dec!(49200));
        engine.process_candle_logic(candle).unwrap();

        let pos = engine.positions.get(&key).unwrap();
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.side, "LONG");
    }

    #[test]
    fn test_position_flip_long_to_short() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key.clone(),
            make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(50000)),
        );

        engine.open_orders.push(make_order(
            "flip1",
            "SHORT",
            "MARKET",
            Decimal::ZERO,
            dec!(2),
        ));

        let candle = make_candle(dec!(48000), dec!(49000), dec!(47000), dec!(48500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);

        let pos = engine.positions.get(&key).unwrap();
        assert_eq!(pos.side, "SHORT");
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.entry_price, dec!(48000));

        // Realized PnL from closing long: (48000 - 50000) * 1 = -2000
        let expected_balance = dec!(100000) - dec!(2000) - fills[0].fee;
        assert_eq!(engine.balance, expected_balance);
    }

    // ── Cancel Order ──

    #[test]
    fn test_cancel_order_removes_from_open_orders() {
        let mut engine = make_engine(dec!(100000));
        engine
            .open_orders
            .push(make_order("c1", "LONG", "LIMIT", dec!(49000), dec!(1)));
        engine
            .open_orders
            .push(make_order("c2", "SHORT", "LIMIT", dec!(51000), dec!(1)));

        let removed = engine.cancel_order("c1".to_string());
        assert!(removed);
        assert_eq!(engine.open_orders.len(), 1);
        assert_eq!(engine.open_orders[0].id, "c2");
    }

    #[test]
    fn test_cancel_nonexistent_order_returns_false() {
        let mut engine = make_engine(dec!(100000));
        let removed = engine.cancel_order("nonexistent".to_string());
        assert!(!removed);
    }

    // ── Priority: Market > Conditional > Limit ──

    #[test]
    fn test_order_priority_market_before_conditional() {
        let mut engine = make_engine(dec!(100000));

        engine
            .open_orders
            .push(make_order("mkt", "LONG", "MARKET", Decimal::ZERO, dec!(1)));
        let mut sl = make_order("cond", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(0.5));
        sl.trigger_price = Some(dec!(49500));
        engine.open_orders.push(sl);

        let key = pos_key(STRATEGY, PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, STRATEGY, "LONG", dec!(0.5), dec!(50000)),
        );

        let candle = make_candle(dec!(50000), dec!(50500), dec!(49000), dec!(49500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].fill_type, "MARKET");
        assert_eq!(fills[1].fill_type, "STOP_LOSS");
    }

    // ── Edge Cases ──

    #[test]
    fn test_no_orders_returns_empty_fills() {
        let mut engine = make_engine(dec!(100000));
        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn test_protection_on_flat_is_discarded_without_fill_or_fee() {
        let mut engine = make_engine(dec!(100000));
        let mut sl = make_order("flat_sl", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(49000));
        engine.open_orders.push(sl);

        let candle = make_candle(dec!(50000), dec!(50500), dec!(48000), dec!(49200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert!(fills.is_empty());
        assert!(engine.open_orders.is_empty());
        assert_eq!(engine.balance, dec!(100000));
    }

    #[test]
    fn test_zero_fee_engine() {
        let mut engine = PyMatchingEngine {
            balance: dec!(100000),
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            market_slippage_bps: Decimal::ZERO,
            market_slippage_price_tick: None,
            contract_multiplier: Decimal::ONE,
            fee_model: FeeModel::PercentageNotional,
            settlement_model: SettlementModel::Derivatives,
            spot_ledger: None,
            spot_cost_basis: Decimal::ZERO,
            spot_realized_pnl: Decimal::ZERO,
            rejections: Vec::new(),
            warnings: Vec::new(),
            scaled_price_tick: None,
            scaled_volume_step: None,
        };
        engine
            .open_orders
            .push(make_order("zf1", "LONG", "MARKET", Decimal::ZERO, dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills[0].fee, Decimal::ZERO);
        assert_eq!(engine.balance, dec!(100000));
    }

    #[test]
    fn test_contract_multiplier_applies_to_conditional_close() {
        let mut engine = make_engine(dec!(100000));
        engine.maker_fee = Decimal::ZERO;
        engine.taker_fee = Decimal::ZERO;
        engine.contract_multiplier = dec!(2);
        let mut position = make_position(PRODUCT, STRATEGY, "LONG", dec!(1), dec!(100));
        let order = make_order("mnq_tp", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));

        engine.close_position(&mut position, &order, dec!(110));

        assert_eq!(engine.balance, dec!(100020));
    }

    #[test]
    fn test_contract_multiplier_applies_to_opposite_side_reduction() {
        let mut engine = make_engine(dec!(100000));
        engine.maker_fee = Decimal::ZERO;
        engine.taker_fee = Decimal::ZERO;
        engine.contract_multiplier = dec!(2);
        let mut position = make_position(PRODUCT, STRATEGY, "SHORT", dec!(2), dec!(100));
        let order = make_order("mnq_reduce", "LONG", "MARKET", Decimal::ZERO, dec!(1));

        engine.apply_position_change(&mut position, &order, dec!(90));

        assert_eq!(engine.balance, dec!(100020));
        assert_eq!(position.quantity, dec!(1));
    }

    #[test]
    fn test_non_positive_contract_multiplier_is_rejected() {
        for multiplier in ["0", "-1"] {
            let error = PyMatchingEngine::new(
                "100000".to_string(),
                "0".to_string(),
                "0".to_string(),
                multiplier.to_string(),
                "percentage_notional".to_string(),
                "derivatives".to_string(),
                "".to_string(),
                "".to_string(),
                "quote".to_string(),
                "0".to_string(),
                None,
            )
            .err()
            .expect("non-positive multiplier must fail");
            assert!(error
                .to_string()
                .contains("contract_multiplier must be positive"));
        }
    }

    // ── Multi-Strategy Position Isolation ──

    #[test]
    fn test_two_strategies_independent_positions_same_product() {
        let mut engine = make_engine(dec!(100000));

        // Strategy A goes LONG
        let mut order_a = make_order("a1", "LONG", "MARKET", Decimal::ZERO, dec!(1));
        order_a.strategy_id = "strategy_a".to_string();
        engine.open_orders.push(order_a);

        // Strategy B goes SHORT on the same product
        let mut order_b = make_order("b1", "SHORT", "MARKET", Decimal::ZERO, dec!(0.5));
        order_b.strategy_id = "strategy_b".to_string();
        engine.open_orders.push(order_b);

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].strategy_id, "strategy_a");
        assert_eq!(fills[1].strategy_id, "strategy_b");

        // Verify independent positions
        let key_a = pos_key("strategy_a", PRODUCT);
        let key_b = pos_key("strategy_b", PRODUCT);

        let pos_a = engine.positions.get(&key_a).unwrap();
        assert_eq!(pos_a.side, "LONG");
        assert_eq!(pos_a.quantity, dec!(1));
        assert_eq!(pos_a.strategy_id, "strategy_a");

        let pos_b = engine.positions.get(&key_b).unwrap();
        assert_eq!(pos_b.side, "SHORT");
        assert_eq!(pos_b.quantity, dec!(0.5));
        assert_eq!(pos_b.strategy_id, "strategy_b");
    }

    #[test]
    fn test_closing_one_strategy_position_doesnt_affect_other() {
        let mut engine = make_engine(dec!(100000));
        let key_a = pos_key("strategy_a", PRODUCT);
        let key_b = pos_key("strategy_b", PRODUCT);

        // Both strategies have LONG positions
        engine.positions.insert(
            key_a.clone(),
            make_position(PRODUCT, "strategy_a", "LONG", dec!(1), dec!(50000)),
        );
        engine.positions.insert(
            key_b.clone(),
            make_position(PRODUCT, "strategy_b", "LONG", dec!(2), dec!(48000)),
        );

        // Close strategy A's position via SL
        let mut sl_a = make_order("sl_a", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl_a.strategy_id = "strategy_a".to_string();
        sl_a.trigger_price = Some(dec!(49000));
        engine.open_orders.push(sl_a);

        let candle = make_candle(dec!(50000), dec!(50500), dec!(48500), dec!(49200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].strategy_id, "strategy_a");

        // Strategy A's position is closed
        assert!(!engine.positions.contains_key(&key_a));

        // Strategy B's position is untouched
        let pos_b = engine.positions.get(&key_b).unwrap();
        assert_eq!(pos_b.side, "LONG");
        assert_eq!(pos_b.quantity, dec!(2));
        assert_eq!(pos_b.entry_price, dec!(48000));
    }

    #[test]
    fn test_multi_strategy_shared_balance() {
        let mut engine = make_engine(dec!(100000));
        let key_a = pos_key("strategy_a", PRODUCT);
        let key_b = pos_key("strategy_b", PRODUCT);

        // Strategy A: LONG 1 BTC @ 50000
        engine.positions.insert(
            key_a.clone(),
            make_position(PRODUCT, "strategy_a", "LONG", dec!(1), dec!(50000)),
        );
        // Strategy B: SHORT 1 BTC @ 50000
        engine.positions.insert(
            key_b.clone(),
            make_position(PRODUCT, "strategy_b", "SHORT", dec!(1), dec!(50000)),
        );

        // Strategy A closes with TP at 52000 (+2000 PnL)
        let mut tp_a = make_order("tp_a", "LONG", "TAKE_PROFIT", Decimal::ZERO, dec!(1));
        tp_a.strategy_id = "strategy_a".to_string();
        tp_a.trigger_price = Some(dec!(52000));
        engine.open_orders.push(tp_a);

        // Strategy B closes with SL at 52000 (-2000 PnL)
        let mut sl_b = make_order("sl_b", "SHORT", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl_b.strategy_id = "strategy_b".to_string();
        sl_b.trigger_price = Some(dec!(52000));
        engine.open_orders.push(sl_b);

        let candle = make_candle(dec!(51000), dec!(52500), dec!(50500), dec!(52000));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 2);

        // Both positions closed
        assert!(!engine.positions.contains_key(&key_a));
        assert!(!engine.positions.contains_key(&key_b));

        // Net PnL = +2000 - 2000 = 0, minus fees
        let total_fees = fills[0].fee + fills[1].fee;
        assert_eq!(engine.balance, dec!(100000) - total_fees);
    }

    #[test]
    fn test_get_position_method() {
        let mut engine = make_engine(dec!(100000));
        let key = pos_key("my_strategy", PRODUCT);
        engine.positions.insert(
            key,
            make_position(PRODUCT, "my_strategy", "LONG", dec!(1), dec!(50000)),
        );

        // Found
        let pos = engine.get_position("my_strategy", PRODUCT);
        assert!(pos.is_some());
        let pos = pos.unwrap();
        assert_eq!(pos.side, "LONG");
        assert_eq!(pos.quantity, dec!(1));

        // Not found — wrong strategy
        assert!(engine.get_position("other_strategy", PRODUCT).is_none());

        // Not found — wrong product
        assert!(engine
            .get_position("my_strategy", "OTHER_PRODUCT")
            .is_none());
    }

    #[test]
    fn test_positions_property_uses_composite_keys() {
        let mut engine = make_engine(dec!(100000));

        // Open positions for two strategies
        let mut order_a = make_order("a1", "LONG", "MARKET", Decimal::ZERO, dec!(1));
        order_a.strategy_id = "alpha".to_string();
        engine.open_orders.push(order_a);

        let mut order_b = make_order("b1", "SHORT", "MARKET", Decimal::ZERO, dec!(1));
        order_b.strategy_id = "beta".to_string();
        engine.open_orders.push(order_b);

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        engine.process_candle_logic(candle).unwrap();

        let positions = engine.get_positions();
        assert_eq!(positions.len(), 2);
        assert!(positions.contains_key(&format!("alpha:{PRODUCT}")));
        assert!(positions.contains_key(&format!("beta:{PRODUCT}")));
    }

    fn make_spot_engine(
        initial_quote: Decimal,
        maker_fee: Decimal,
        taker_fee: Decimal,
        fee_asset: &str,
    ) -> PyMatchingEngine {
        PyMatchingEngine::new(
            initial_quote.to_string(),
            maker_fee.to_string(),
            taker_fee.to_string(),
            "1".to_string(),
            "percentage_notional".to_string(),
            "cash_spot".to_string(),
            "BTC".to_string(),
            "USDT".to_string(),
            fee_asset.to_string(),
            "0".to_string(),
            None,
        )
        .unwrap()
    }

    fn make_spot_order(
        id: &str,
        side: &str,
        order_type: &str,
        reference_price: Decimal,
        quantity: Decimal,
    ) -> Order {
        let mut order = make_order(id, side, order_type, reference_price, quantity);
        order.product_id = "BINANCE:BTCUSDT-SPOT".to_string();
        order
    }

    fn make_spot_candle(open: Decimal) -> Candlestick {
        let mut candle = make_candle(open, open, open, open);
        candle.product_id = "BINANCE:BTCUSDT-SPOT".to_string();
        candle
    }

    #[test]
    fn spot_quote_fee_acceptance_sequence_preserves_assets_and_cost_basis() {
        let mut engine = make_spot_engine(dec!(100), dec!(0.001), dec!(0.001), "quote");
        let buy = make_spot_order("buy", "LONG", "MARKET", dec!(50000), dec!(0.001));
        engine.submit_order(buy).unwrap();

        assert_eq!(engine.get_asset_balance("USDT", "total").unwrap(), "100");
        assert_eq!(
            engine.get_asset_balance("USDT", "reserved").unwrap(),
            "50.050000"
        );
        assert_eq!(engine.balance(), "49.950000");

        let fills = engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fee, dec!(0.050000));
        assert_eq!(
            engine.get_asset_balance("USDT", "total").unwrap(),
            "49.950000"
        );
        assert_eq!(engine.get_asset_balance("BTC", "total").unwrap(), "0.001");

        let sell = make_spot_order("sell", "SHORT", "MARKET", dec!(60000), dec!(0.0005));
        engine.submit_order(sell).unwrap();
        let fills = engine
            .process_candle_logic(make_spot_candle(dec!(60000)))
            .unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fee, dec!(0.0300000));

        let snapshot = engine
            .cash_spot_account_snapshot("60000".to_string())
            .unwrap();
        assert_eq!(snapshot["quote_total"], "79.9200000");
        assert_eq!(snapshot["base_total"], "0.0005");
        assert_eq!(snapshot["cost_basis"], "25.0250000");
        assert_eq!(snapshot["realized_pnl"], "4.9450000");
        assert_eq!(snapshot["total_equity"], "109.9200000");
    }

    #[test]
    fn spot_market_slippage_uses_adjusted_price_for_principal_fee_and_pnl() {
        let mut engine = make_spot_engine(dec!(100), Decimal::ZERO, dec!(0.001), "quote");
        engine.market_slippage_bps = dec!(10);
        engine.market_slippage_price_tick = Some(dec!(0.01));
        engine
            .submit_order(make_spot_order(
                "buy",
                "LONG",
                "MARKET",
                dec!(50000),
                dec!(0.001),
            ))
            .unwrap();

        let buy_fills = engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap();
        assert_eq!(buy_fills[0].price, dec!(50050));
        assert_eq!(buy_fills[0].fee, dec!(0.05005));
        assert_eq!(
            Decimal::from_str(&engine.get_asset_balance("USDT", "total").unwrap()).unwrap(),
            dec!(49.89995)
        );

        engine
            .submit_order(make_spot_order(
                "sell",
                "SHORT",
                "MARKET",
                dec!(50000),
                dec!(0.001),
            ))
            .unwrap();
        let sell_fills = engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap();
        assert_eq!(sell_fills[0].price, dec!(49950));
        assert_eq!(sell_fills[0].fee, dec!(0.04995));
        assert_eq!(
            Decimal::from_str(&engine.get_asset_balance("USDT", "total").unwrap()).unwrap(),
            dec!(99.80000)
        );
        assert_eq!(engine.spot_realized_pnl, dec!(-0.20000));
    }

    #[test]
    fn spot_market_slippage_rejects_whole_buy_when_adjusted_cost_is_unfunded() {
        let mut engine = make_spot_engine(dec!(50.10), Decimal::ZERO, dec!(0.001), "quote");
        engine.market_slippage_bps = dec!(10);
        engine.market_slippage_price_tick = Some(dec!(0.01));
        engine
            .submit_order(make_spot_order(
                "buy",
                "LONG",
                "MARKET",
                dec!(50000),
                dec!(0.001),
            ))
            .unwrap();

        let fills = engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap();

        assert!(fills.is_empty());
        assert_eq!(
            Decimal::from_str(&engine.get_asset_balance("USDT", "total").unwrap()).unwrap(),
            dec!(50.10)
        );
        assert_eq!(
            Decimal::from_str(&engine.get_asset_balance("USDT", "reserved").unwrap()).unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            Decimal::from_str(&engine.get_asset_balance("BTC", "total").unwrap()).unwrap(),
            Decimal::ZERO
        );
        let rejections = engine.drain_rejections();
        assert_eq!(rejections.len(), 1);
        assert!(rejections[0]["reason"].contains("insufficient available USDT at fill"));
    }

    #[test]
    fn spot_gap_rejection_is_full_and_releases_reservation() {
        let mut engine = make_spot_engine(dec!(100), Decimal::ZERO, dec!(0.001), "quote");
        let buy = make_spot_order("gap", "LONG", "MARKET", dec!(90000), dec!(0.001));
        engine.submit_order(buy).unwrap();

        let fills = engine
            .process_candle_logic(make_spot_candle(dec!(110000)))
            .unwrap();

        assert!(fills.is_empty());
        assert!(engine.positions.is_empty());
        assert_eq!(engine.get_asset_balance("USDT", "total").unwrap(), "100");
        assert_eq!(
            engine.get_asset_balance("USDT", "reserved").unwrap(),
            "0.000000"
        );
        let rejections = engine.drain_rejections();
        assert_eq!(rejections.len(), 1);
        assert!(rejections[0]["reason"].contains("insufficient available USDT at fill"));
    }

    #[test]
    fn spot_pending_orders_reserve_and_cancel_releases_once() {
        let mut engine = make_spot_engine(dec!(100), Decimal::ZERO, Decimal::ZERO, "quote");
        let first = make_spot_order("first", "LONG", "LIMIT", dec!(60), dec!(1));
        engine.submit_order(first).unwrap();
        let second = make_spot_order("second", "LONG", "LIMIT", dec!(50), dec!(1));

        engine.submit_order(second).unwrap();
        let warnings = engine.drain_warnings();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]["reason"].contains("insufficient_available_at_submission"));
        assert_eq!(engine.balance(), "40");
        assert!(engine.cancel_order("first".to_string()));
        assert_eq!(engine.balance(), "100");
        assert!(!engine.cancel_order("first".to_string()));
        assert_eq!(engine.balance(), "100");
        assert!(engine.cancel_order("second".to_string()));
    }

    #[test]
    fn cash_spot_external_funding_preserves_existing_reservations() {
        let mut engine = make_spot_engine(dec!(100), Decimal::ZERO, Decimal::ZERO, "quote");
        let pending = make_spot_order("pending", "LONG", "LIMIT", dec!(60), dec!(1));
        engine.submit_order(pending).unwrap();

        let total = engine
            .apply_external_funding("USDT", "50".to_string())
            .unwrap();

        assert_eq!(total, "150");
        assert_eq!(engine.get_asset_balance("USDT", "total").unwrap(), "150");
        assert_eq!(engine.get_asset_balance("USDT", "reserved").unwrap(), "60");
        assert_eq!(engine.get_asset_balance("USDT", "available").unwrap(), "90");
    }

    #[test]
    fn external_funding_validation_matrix_fails_closed() {
        let mut spot = make_spot_engine(dec!(100), Decimal::ZERO, Decimal::ZERO, "quote");
        assert!(spot
            .apply_external_funding("BTC", "1".to_string())
            .unwrap_err()
            .to_string()
            .contains("supports quote asset USDT only"));
        assert!(spot
            .apply_external_funding("USDT", "0".to_string())
            .unwrap_err()
            .to_string()
            .contains("amount must be positive"));
        assert!(spot
            .apply_external_funding("USDT", "-1".to_string())
            .unwrap_err()
            .to_string()
            .contains("amount must be positive"));
        assert!(spot
            .apply_external_funding("USDT", "not-a-decimal".to_string())
            .unwrap_err()
            .to_string()
            .contains("Invalid decimal for 'external_funding_amount'"));
        assert_eq!(spot.get_asset_balance("USDT", "total").unwrap(), "100");

        let mut max_balance = make_spot_engine(Decimal::MAX, Decimal::ZERO, Decimal::ZERO, "quote");
        assert!(max_balance
            .apply_external_funding("USDT", "1".to_string())
            .unwrap_err()
            .to_string()
            .contains("would overflow quote balance"));
        assert_eq!(
            max_balance.get_asset_balance("USDT", "total").unwrap(),
            Decimal::MAX.to_string()
        );

        let mut derivatives = make_engine(dec!(100));
        assert!(derivatives
            .apply_external_funding("USDT", "1".to_string())
            .unwrap_err()
            .to_string()
            .contains("requires cash_spot settlement"));
    }

    #[test]
    fn cash_spot_sell_rejects_unfunded_inventory_without_locking() {
        let mut engine = make_spot_engine(dec!(100), Decimal::ZERO, Decimal::ZERO, "quote");
        let naked_sell = make_spot_order("naked", "SHORT", "MARKET", dec!(50000), dec!(0.001));
        engine.submit_order(naked_sell).unwrap();
        assert_eq!(engine.drain_warnings().len(), 1);
        assert!(engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap()
            .is_empty());
        assert_eq!(engine.drain_rejections().len(), 1);

        let buy = make_spot_order("buy", "LONG", "MARKET", dec!(50000), dec!(0.001));
        engine.submit_order(buy).unwrap();
        engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap();
        let oversell = make_spot_order("oversell", "SHORT", "MARKET", dec!(50000), dec!(0.002));
        engine.submit_order(oversell).unwrap();
        assert_eq!(engine.drain_warnings().len(), 1);
        assert!(engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap()
            .is_empty());
        assert_eq!(engine.drain_rejections().len(), 1);
        assert_eq!(engine.get_asset_balance("BTC", "total").unwrap(), "0.001");
    }

    #[test]
    fn spot_base_fee_reduces_received_asset_without_quote_drift() {
        let mut engine = make_spot_engine(dec!(100), Decimal::ZERO, dec!(0.001), "base");
        let buy = make_spot_order("buy", "LONG", "MARKET", dec!(50000), dec!(0.001));
        engine.submit_order(buy).unwrap();
        let fills = engine
            .process_candle_logic(make_spot_candle(dec!(50000)))
            .unwrap();

        assert_eq!(fills[0].fee, dec!(0.000001));
        assert_eq!(engine.get_asset_balance("USDT", "total").unwrap(), "50.000");
        assert_eq!(
            engine.get_asset_balance("BTC", "total").unwrap(),
            "0.000999"
        );
        let position = engine
            .get_position(STRATEGY, "BINANCE:BTCUSDT-SPOT")
            .unwrap();
        assert_eq!(position.quantity, dec!(0.000999));
    }
}
