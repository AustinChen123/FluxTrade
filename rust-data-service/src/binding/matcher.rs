use ::pyo3::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

use crate::binding::models::{Candlestick, FillEvent, Order, Position};

#[pyclass]
pub struct PyMatchingEngine {
    pub balance: Decimal,
    #[pyo3(get)]
    pub positions: HashMap<String, Position>,
    #[pyo3(get)]
    pub open_orders: Vec<Order>,
    maker_fee: Decimal,
    taker_fee: Decimal,
}

#[pymethods]
impl PyMatchingEngine {
    #[new]
    #[pyo3(signature = (initial_balance, maker_fee="0".to_string(), taker_fee="0".to_string()))]
    fn new(initial_balance: String, maker_fee: String, taker_fee: String) -> PyResult<Self> {
        Ok(PyMatchingEngine {
            balance: parse_decimal(&initial_balance, "initial_balance")?,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: parse_decimal(&maker_fee, "maker_fee")?,
            taker_fee: parse_decimal(&taker_fee, "taker_fee")?,
        })
    }

    #[getter]
    fn balance(&self) -> String {
        self.balance.to_string()
    }

    fn submit_order(&mut self, order: Order) -> PyResult<String> {
        let id = order.id.clone();
        self.open_orders.push(order);
        Ok(id)
    }

    fn get_positions(&self) -> HashMap<String, Position> {
        self.positions.clone()
    }

    /// Get position for a specific strategy and product.
    fn get_position(&self, strategy_id: &str, product_id: &str) -> Option<Position> {
        let key = format!("{strategy_id}:{product_id}");
        self.positions.get(&key).cloned()
    }

    fn on_candle(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        self.process_candle_logic(candle)
    }

    fn on_matching_tick(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        self.process_candle_logic(candle)
    }

    fn cancel_order(&mut self, order_id: String) -> bool {
        let before = self.open_orders.len();
        self.open_orders.retain(|o| o.id != order_id);
        self.open_orders.len() < before
    }
}

impl PyMatchingEngine {
    fn process_candle_logic(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        let mut fills: Vec<FillEvent> = Vec::new();
        let mut remaining_orders: Vec<Order> = Vec::new();
        // Collect IDs of orders cancelled by OCO during this candle
        let mut cancelled_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Update trailing stops before matching
        self.update_trailing_stops(&candle);

        // Partition orders by type for priority: Market > SL/TP/Trailing > Limit
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

        // 1. Process Market Orders (taker fee, fill at open)
        for order in market_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if order.product_id != candle.product_id {
                remaining_orders.push(order);
                continue;
            }

            let fill_price = candle.open;
            let fee = self.calculate_fee(fill_price, order.quantity, true);

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
            self.update_position(&order, fill_price);
            let fee = std::cmp::min(fee, self.balance);
            self.balance -= fee;
            self.cancel_linked(&order, &mut cancelled_ids);
            fills.push(fill);
        }

        // 2. Process Conditional Orders (SL/TP/Trailing — taker fee)
        for order in conditional_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if order.product_id != candle.product_id {
                remaining_orders.push(order);
                continue;
            }

            let trigger = match self.check_conditional_trigger(&order, &candle) {
                Some(price) => price,
                None => {
                    remaining_orders.push(order);
                    continue;
                }
            };

            let fee = self.calculate_fee(trigger, order.quantity, true);
            let fill = FillEvent {
                order_id: order.id.clone(),
                product_id: order.product_id.clone(),
                strategy_id: order.strategy_id.clone(),
                price: trigger,
                quantity: order.quantity,
                fee,
                timestamp: candle.timestamp,
                fill_type: order.order_type.clone(),
            };
            self.update_position(&order, trigger);
            let fee = std::cmp::min(fee, self.balance);
            self.balance -= fee;
            self.cancel_linked(&order, &mut cancelled_ids);
            fills.push(fill);
        }

        // 3. Process Limit Orders (maker fee)
        for order in limit_orders {
            if cancelled_ids.contains(&order.id) {
                continue;
            }
            if order.product_id != candle.product_id {
                remaining_orders.push(order);
                continue;
            }

            let matched = if order.side == "LONG" {
                candle.low <= order.price
            } else {
                candle.high >= order.price
            };

            if matched {
                let fee = self.calculate_fee(order.price, order.quantity, false);
                let fill = FillEvent {
                    order_id: order.id.clone(),
                    product_id: order.product_id.clone(),
                    strategy_id: order.strategy_id.clone(),
                    price: order.price,
                    quantity: order.quantity,
                    fee,
                    timestamp: candle.timestamp,
                    fill_type: "LIMIT".to_string(),
                };
                self.update_position(&order, order.price);
                let fee = std::cmp::min(fee, self.balance);
                self.balance -= fee;
                self.cancel_linked(&order, &mut cancelled_ids);
                fills.push(fill);
            } else {
                remaining_orders.push(order);
            }
        }

        // Filter out cancelled orders from remaining
        remaining_orders.retain(|o| !cancelled_ids.contains(&o.id));
        self.open_orders = remaining_orders;
        Ok(fills)
    }

    /// Check if a conditional order triggers on this candle.
    /// Returns the fill price if triggered, None otherwise.
    fn check_conditional_trigger(&self, order: &Order, candle: &Candlestick) -> Option<Decimal> {
        let trigger_price = order.trigger_price.unwrap_or(order.price);

        match order.order_type.as_str() {
            "STOP_LOSS" => {
                if order.side == "LONG" {
                    if candle.low <= trigger_price {
                        return Some(trigger_price);
                    }
                } else if candle.high >= trigger_price {
                    return Some(trigger_price);
                }
            }
            "TAKE_PROFIT" => {
                if order.side == "LONG" {
                    if candle.high >= trigger_price {
                        return Some(trigger_price);
                    }
                } else if candle.low <= trigger_price {
                    return Some(trigger_price);
                }
            }
            "TRAILING_STOP" => {
                if order.side == "LONG" {
                    if candle.low <= trigger_price {
                        return Some(trigger_price);
                    }
                } else if candle.high >= trigger_price {
                    return Some(trigger_price);
                }
            }
            _ => {}
        }
        None
    }

    /// Update trailing stop trigger prices based on candle high/low.
    fn update_trailing_stops(&mut self, candle: &Candlestick) {
        for order in &mut self.open_orders {
            if order.order_type != "TRAILING_STOP" || order.product_id != candle.product_id {
                continue;
            }
            let distance = match order.trailing_distance {
                Some(d) => d,
                None => continue,
            };
            let current_trigger = order.trigger_price.unwrap_or(order.price);

            if order.side == "LONG" {
                let new_trigger = candle.high - distance;
                if new_trigger > current_trigger {
                    order.trigger_price = Some(new_trigger);
                }
            } else {
                let new_trigger = candle.low + distance;
                if new_trigger < current_trigger {
                    order.trigger_price = Some(new_trigger);
                }
            }
        }
    }

    /// Mark the linked order (OCO counterpart) for cancellation.
    fn cancel_linked(&self, filled_order: &Order, cancelled_ids: &mut std::collections::HashSet<String>) {
        if let Some(ref linked_id) = filled_order.linked_order_id {
            cancelled_ids.insert(linked_id.clone());
        }
    }

    fn calculate_fee(&self, price: Decimal, quantity: Decimal, is_taker: bool) -> Decimal {
        let rate = if is_taker {
            self.taker_fee
        } else {
            self.maker_fee
        };
        price * quantity * rate
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
        let realized_pnl = price_diff * close_qty;
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
            let realized_pnl = price_diff * close_qty;
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

    fn make_position(product_id: &str, strategy_id: &str, side: &str, qty: Decimal, entry: Decimal) -> Position {
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
        engine.update_trailing_stops(&c);

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
    fn test_close_position_on_flat_does_nothing() {
        let mut engine = make_engine(dec!(100000));
        let mut sl = make_order("flat_sl", "LONG", "STOP_LOSS", Decimal::ZERO, dec!(1));
        sl.trigger_price = Some(dec!(49000));
        engine.open_orders.push(sl);

        let candle = make_candle(dec!(50000), dec!(50500), dec!(48000), dec!(49200));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        let fee = fills[0].fee;
        assert_eq!(engine.balance, dec!(100000) - fee);
    }

    #[test]
    fn test_zero_fee_engine() {
        let mut engine = PyMatchingEngine {
            balance: dec!(100000),
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
        };
        engine
            .open_orders
            .push(make_order("zf1", "LONG", "MARKET", Decimal::ZERO, dec!(1)));

        let candle = make_candle(dec!(50000), dec!(51000), dec!(49000), dec!(50500));
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills[0].fee, Decimal::ZERO);
        assert_eq!(engine.balance, dec!(100000));
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
        assert!(engine.get_position("my_strategy", "OTHER_PRODUCT").is_none());
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
}
