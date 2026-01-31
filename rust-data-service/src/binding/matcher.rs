use ::pyo3::prelude::*;
use std::collections::HashMap;
use crate::binding::models::{Candlestick, Order, FillEvent, Position};

#[pyclass]
pub struct PyMatchingEngine {
    #[pyo3(get)]
    pub balance: f64,
    #[pyo3(get)]
    pub positions: HashMap<String, Position>,
    #[pyo3(get)]
    pub open_orders: Vec<Order>,
    maker_fee: f64,
    taker_fee: f64,
}

#[pymethods]
impl PyMatchingEngine {
    #[new]
    #[pyo3(signature = (initial_balance, maker_fee=0.0, taker_fee=0.0))]
    fn new(initial_balance: f64, maker_fee: f64, taker_fee: f64) -> Self {
        PyMatchingEngine {
            balance: initial_balance,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee,
            taker_fee,
        }
    }

    fn submit_order(&mut self, order: Order) -> PyResult<String> {
        let id = order.id.clone();
        self.open_orders.push(order);
        Ok(id)
    }

    fn get_balance(&self) -> f64 {
        self.balance
    }

    fn get_positions(&self) -> HashMap<String, Position> {
        self.positions.clone()
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
        let mut cancelled_ids: Vec<String> = Vec::new();

        // Update trailing stops before matching
        self.update_trailing_stops(&candle);

        // Partition orders by type for priority: Market > SL/TP/Trailing > Limit
        let mut market_orders: Vec<Order> = Vec::new();
        let mut conditional_orders: Vec<Order> = Vec::new();
        let mut limit_orders: Vec<Order> = Vec::new();

        for order in self.open_orders.drain(..) {
            match order.order_type.as_str() {
                "MARKET" => market_orders.push(order),
                "STOP_LOSS" | "TAKE_PROFIT" | "TRAILING_STOP" => {
                    conditional_orders.push(order)
                }
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
                price: fill_price,
                quantity: order.quantity,
                fee,
                timestamp: candle.timestamp,
                fill_type: "MARKET".to_string(),
            };
            self.update_position(&order, fill_price);
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
                price: trigger,
                quantity: order.quantity,
                fee,
                timestamp: candle.timestamp,
                fill_type: order.order_type.clone(),
            };
            self.update_position(&order, trigger);
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
                    price: order.price,
                    quantity: order.quantity,
                    fee,
                    timestamp: candle.timestamp,
                    fill_type: "LIMIT".to_string(),
                };
                self.update_position(&order, order.price);
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
    fn check_conditional_trigger(&self, order: &Order, candle: &Candlestick) -> Option<f64> {
        let trigger_price = order.trigger_price.unwrap_or(order.price);

        match order.order_type.as_str() {
            "STOP_LOSS" => {
                // SL for LONG position: triggers when price drops to trigger
                // SL for SHORT position: triggers when price rises to trigger
                if order.side == "LONG" {
                    // This is a sell SL — closing a long
                    // Triggers when low <= trigger_price
                    if candle.low <= trigger_price {
                        return Some(trigger_price);
                    }
                } else {
                    // This is a buy SL — closing a short
                    // Triggers when high >= trigger_price
                    if candle.high >= trigger_price {
                        return Some(trigger_price);
                    }
                }
            }
            "TAKE_PROFIT" => {
                if order.side == "LONG" {
                    // Sell TP — closing a long when price rises
                    if candle.high >= trigger_price {
                        return Some(trigger_price);
                    }
                } else {
                    // Buy TP — closing a short when price drops
                    if candle.low <= trigger_price {
                        return Some(trigger_price);
                    }
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
    /// Called before matching so triggers reflect the current candle's movement.
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
                // Trailing SL for long: trail below price, move up only
                let new_trigger = candle.high - distance;
                if new_trigger > current_trigger {
                    order.trigger_price = Some(new_trigger);
                }
            } else {
                // Trailing SL for short: trail above price, move down only
                let new_trigger = candle.low + distance;
                if new_trigger < current_trigger {
                    order.trigger_price = Some(new_trigger);
                }
            }
        }
    }

    /// Mark the linked order (OCO counterpart) for cancellation.
    fn cancel_linked(&self, filled_order: &Order, cancelled_ids: &mut Vec<String>) {
        if let Some(ref linked_id) = filled_order.linked_order_id {
            if !cancelled_ids.contains(linked_id) {
                cancelled_ids.push(linked_id.clone());
            }
        }
    }

    fn calculate_fee(&self, price: f64, quantity: f64, is_taker: bool) -> f64 {
        let rate = if is_taker { self.taker_fee } else { self.maker_fee };
        price * quantity * rate
    }

    fn update_position(&mut self, order: &Order, fill_price: f64) {
        // Take position out to avoid double mutable borrow on self
        let mut pos = self.positions.remove(&order.product_id).unwrap_or(Position {
            product_id: order.product_id.clone(),
            side: "FLAT".to_string(),
            quantity: 0.0,
            entry_price: 0.0,
            unrealized_pnl: 0.0,
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

        // Put position back (or remove if flat)
        if pos.quantity > 1e-9 && pos.side != "FLAT" {
            self.positions.insert(order.product_id.clone(), pos);
        }
    }

    /// Close position for conditional orders (SL/TP/Trailing).
    fn close_position(&mut self, pos: &mut Position, order: &Order, fill_price: f64) {
        if pos.quantity < 1e-9 || pos.side == "FLAT" {
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
        if remaining > 1e-9 {
            pos.quantity = remaining;
        } else {
            pos.side = "FLAT".to_string();
            pos.quantity = 0.0;
            pos.entry_price = 0.0;
        }
    }

    /// Apply position change for MARKET/LIMIT orders (open, increase, reduce, flip).
    fn apply_position_change(&mut self, pos: &mut Position, order: &Order, fill_price: f64) {
        if pos.quantity < 1e-9 || pos.side == "FLAT" {
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

            if remaining > 1e-9 {
                pos.quantity = remaining;
            } else if excess > 1e-9 {
                pos.side = order.side.clone();
                pos.quantity = excess;
                pos.entry_price = fill_price;
            } else {
                pos.side = "FLAT".to_string();
                pos.quantity = 0.0;
                pos.entry_price = 0.0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::models::{Candlestick, Order, Position};

    const PRODUCT: &str = "BINANCE:BTCUSDT-PERP";
    const TF: &str = "1m";

    fn make_engine(balance: f64) -> PyMatchingEngine {
        PyMatchingEngine {
            balance,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: 0.0002,
            taker_fee: 0.0006,
        }
    }

    fn make_candle(open: f64, high: f64, low: f64, close: f64) -> Candlestick {
        Candlestick {
            product_id: PRODUCT.to_string(),
            timeframe: TF.to_string(),
            timestamp: 1000,
            open,
            high,
            low,
            close,
            volume: 100.0,
        }
    }

    fn make_order(id: &str, side: &str, order_type: &str, price: f64, qty: f64) -> Order {
        Order {
            id: id.to_string(),
            product_id: PRODUCT.to_string(),
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

    // ── Market Orders ──

    #[test]
    fn test_market_order_long_fills_at_open() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("m1", "LONG", "MARKET", 0.0, 1.0));

        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 50_000.0);
        assert_eq!(fills[0].fill_type, "MARKET");

        let pos = engine.positions.get(PRODUCT).unwrap();
        assert_eq!(pos.side, "LONG");
        assert!((pos.quantity - 1.0).abs() < 1e-9);
        assert!((pos.entry_price - 50_000.0).abs() < 1e-9);
    }

    #[test]
    fn test_market_order_short_fills_at_open() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("m2", "SHORT", "MARKET", 0.0, 0.5));

        let candle = make_candle(48_000.0, 49_000.0, 47_000.0, 48_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 48_000.0);

        let pos = engine.positions.get(PRODUCT).unwrap();
        assert_eq!(pos.side, "SHORT");
        assert!((pos.quantity - 0.5).abs() < 1e-9);
    }

    #[test]
    fn test_market_order_taker_fee_deducted() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("m3", "LONG", "MARKET", 0.0, 1.0));

        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        let expected_fee = 50_000.0 * 1.0 * 0.0006;
        assert!((fills[0].fee - expected_fee).abs() < 1e-9);
        assert!(engine.balance < 100_000.0);
    }

    #[test]
    fn test_market_order_different_product_not_filled() {
        let mut engine = make_engine(100_000.0);
        let mut order = make_order("m4", "LONG", "MARKET", 0.0, 1.0);
        order.product_id = "BINANCE:ETHUSDT-PERP".to_string();
        engine.open_orders.push(order);

        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 0);
        assert_eq!(engine.open_orders.len(), 1);
    }

    // ── Limit Orders ──

    #[test]
    fn test_limit_order_long_fills_when_low_touches_price() {
        let mut engine = make_engine(100_000.0);
        let mut order = make_order("l1", "LONG", "LIMIT", 49_500.0, 1.0);
        order.price = 49_500.0;
        engine.open_orders.push(order);

        // low=49_000 <= 49_500 → should fill
        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 49_500.0);
        assert_eq!(fills[0].fill_type, "LIMIT");
    }

    #[test]
    fn test_limit_order_long_not_filled_when_low_above_price() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("l2", "LONG", "LIMIT", 48_000.0, 1.0));

        // low=49_000 > 48_000 → should NOT fill
        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 0);
        assert_eq!(engine.open_orders.len(), 1);
    }

    #[test]
    fn test_limit_order_short_fills_when_high_touches_price() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("l3", "SHORT", "LIMIT", 50_500.0, 1.0));

        // high=51_000 >= 50_500 → should fill
        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 50_500.0);
    }

    #[test]
    fn test_limit_order_uses_maker_fee() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("l4", "LONG", "LIMIT", 49_500.0, 1.0));

        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        let expected_fee = 49_500.0 * 1.0 * 0.0002; // maker
        assert!((fills[0].fee - expected_fee).abs() < 1e-9);
    }

    // ── Stop Loss ──

    #[test]
    fn test_stop_loss_long_triggers_when_low_hits() {
        let mut engine = make_engine(100_000.0);
        // Open a LONG position first
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut sl = make_order("sl1", "LONG", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(49_000.0);
        engine.open_orders.push(sl);

        // low=48_500 <= 49_000 → triggers
        let candle = make_candle(50_000.0, 50_500.0, 48_500.0, 49_200.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "STOP_LOSS");
        assert_eq!(fills[0].price, 49_000.0);

        // Position should be closed (FLAT removed from map)
        assert!(!engine.positions.contains_key(PRODUCT));

        // PnL: (49_000 - 50_000) * 1.0 = -1_000
        let expected_balance = 100_000.0 - 1_000.0 - fills[0].fee;
        assert!((engine.balance - expected_balance).abs() < 1e-6);
    }

    #[test]
    fn test_stop_loss_short_triggers_when_high_hits() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "SHORT".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut sl = make_order("sl2", "SHORT", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(51_000.0);
        engine.open_orders.push(sl);

        // high=51_500 >= 51_000 → triggers
        let candle = make_candle(50_200.0, 51_500.0, 49_800.0, 50_800.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "STOP_LOSS");

        // PnL: (50_000 - 51_000) * 1.0 = -1_000
        let expected_balance = 100_000.0 - 1_000.0 - fills[0].fee;
        assert!((engine.balance - expected_balance).abs() < 1e-6);
    }

    #[test]
    fn test_stop_loss_not_triggered_when_price_doesnt_reach() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut sl = make_order("sl3", "LONG", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(48_000.0);
        engine.open_orders.push(sl);

        // low=49_000 > 48_000 → NOT triggered
        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 0);
        assert_eq!(engine.open_orders.len(), 1);
    }

    // ── Take Profit ──

    #[test]
    fn test_take_profit_long_triggers_when_high_hits() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut tp = make_order("tp1", "LONG", "TAKE_PROFIT", 0.0, 1.0);
        tp.trigger_price = Some(52_000.0);
        engine.open_orders.push(tp);

        // high=52_500 >= 52_000 → triggers
        let candle = make_candle(50_500.0, 52_500.0, 50_000.0, 52_000.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "TAKE_PROFIT");

        // PnL: (52_000 - 50_000) * 1.0 = +2_000
        let expected_balance = 100_000.0 + 2_000.0 - fills[0].fee;
        assert!((engine.balance - expected_balance).abs() < 1e-6);
    }

    #[test]
    fn test_take_profit_short_triggers_when_low_hits() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "SHORT".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut tp = make_order("tp2", "SHORT", "TAKE_PROFIT", 0.0, 1.0);
        tp.trigger_price = Some(48_000.0);
        engine.open_orders.push(tp);

        // low=47_500 <= 48_000 → triggers
        let candle = make_candle(49_000.0, 49_500.0, 47_500.0, 48_200.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);

        // PnL: (50_000 - 48_000) * 1.0 = +2_000
        let expected_balance = 100_000.0 + 2_000.0 - fills[0].fee;
        assert!((engine.balance - expected_balance).abs() < 1e-6);
    }

    // ── Trailing Stop ──

    #[test]
    fn test_trailing_stop_long_updates_and_triggers() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut ts = make_order("ts1", "LONG", "TRAILING_STOP", 0.0, 1.0);
        ts.trigger_price = Some(49_000.0); // initial trigger
        ts.trailing_distance = Some(1_000.0);
        engine.open_orders.push(ts);

        // Candle 1: high=52_000 → new trigger = 52_000 - 1_000 = 51_000
        // low=51_200 > 51_000 → does NOT trigger on same candle
        let c1 = make_candle(51_500.0, 52_000.0, 51_200.0, 51_800.0);
        let fills = engine.process_candle_logic(c1).unwrap();
        assert_eq!(fills.len(), 0);

        let updated_trigger = engine.open_orders[0].trigger_price.unwrap();
        assert!((updated_trigger - 51_000.0).abs() < 1e-9);

        // Candle 2: low=50_500 <= 51_000 → triggers
        let c2 = make_candle(51_200.0, 51_500.0, 50_500.0, 50_800.0);
        let fills = engine.process_candle_logic(c2).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "TRAILING_STOP");
    }

    #[test]
    fn test_trailing_stop_short_updates_and_triggers() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "SHORT".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut ts = make_order("ts2", "SHORT", "TRAILING_STOP", 0.0, 1.0);
        ts.trigger_price = Some(51_000.0); // initial trigger
        ts.trailing_distance = Some(1_000.0);
        engine.open_orders.push(ts);

        // Candle 1: low=48_000 → new trigger = 48_000 + 1_000 = 49_000
        // high=48_800 < 49_000 → does NOT trigger on same candle
        let c1 = make_candle(48_500.0, 48_800.0, 48_000.0, 48_300.0);
        let fills = engine.process_candle_logic(c1).unwrap();
        assert_eq!(fills.len(), 0);

        let updated_trigger = engine.open_orders[0].trigger_price.unwrap();
        assert!((updated_trigger - 49_000.0).abs() < 1e-9);

        // Candle 2: high=49_500 >= 49_000 → triggers
        let c2 = make_candle(48_800.0, 49_500.0, 48_500.0, 49_200.0);
        let fills = engine.process_candle_logic(c2).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_type, "TRAILING_STOP");
    }

    #[test]
    fn test_trailing_stop_only_moves_in_favorable_direction() {
        let mut engine = make_engine(100_000.0);

        let mut ts = make_order("ts3", "LONG", "TRAILING_STOP", 0.0, 1.0);
        ts.trigger_price = Some(49_000.0);
        ts.trailing_distance = Some(1_000.0);
        engine.open_orders.push(ts);

        // high=49_500 → new_trigger = 49_500 - 1_000 = 48_500 < 49_000 → should NOT move down
        let c = make_candle(49_000.0, 49_500.0, 48_800.0, 49_200.0);
        engine.update_trailing_stops(&c);

        assert!((engine.open_orders[0].trigger_price.unwrap() - 49_000.0).abs() < 1e-9);
    }

    // ── OCO (One-Cancels-Other) ──

    #[test]
    fn test_oco_sl_triggers_cancels_tp() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut sl = make_order("sl_oco", "LONG", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(49_000.0);
        sl.linked_order_id = Some("tp_oco".to_string());
        engine.open_orders.push(sl);

        let mut tp = make_order("tp_oco", "LONG", "TAKE_PROFIT", 0.0, 1.0);
        tp.trigger_price = Some(52_000.0);
        tp.linked_order_id = Some("sl_oco".to_string());
        engine.open_orders.push(tp);

        // low=48_500 triggers SL, TP should be cancelled
        let candle = make_candle(50_000.0, 50_500.0, 48_500.0, 49_200.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, "sl_oco");
        assert!(engine.open_orders.is_empty()); // TP cancelled
    }

    #[test]
    fn test_oco_tp_triggers_cancels_sl() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut sl = make_order("sl_oco2", "LONG", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(48_000.0);
        sl.linked_order_id = Some("tp_oco2".to_string());
        engine.open_orders.push(sl);

        let mut tp = make_order("tp_oco2", "LONG", "TAKE_PROFIT", 0.0, 1.0);
        tp.trigger_price = Some(51_000.0);
        tp.linked_order_id = Some("sl_oco2".to_string());
        engine.open_orders.push(tp);

        // high=51_500 triggers TP, SL should be cancelled
        let candle = make_candle(50_500.0, 51_500.0, 50_000.0, 51_200.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, "tp_oco2");
        assert!(engine.open_orders.is_empty());
    }

    // ── Position Management ──

    #[test]
    fn test_position_add_to_existing_averages_cost() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("add1", "LONG", "MARKET", 0.0, 1.0));

        let c1 = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        engine.process_candle_logic(c1).unwrap();

        engine.open_orders.push(make_order("add2", "LONG", "MARKET", 0.0, 1.0));

        let c2 = make_candle(52_000.0, 53_000.0, 51_000.0, 52_500.0);
        engine.process_candle_logic(c2).unwrap();

        let pos = engine.positions.get(PRODUCT).unwrap();
        assert!((pos.quantity - 2.0).abs() < 1e-9);
        // avg: (50_000*1 + 52_000*1) / 2 = 51_000
        assert!((pos.entry_price - 51_000.0).abs() < 1e-6);
    }

    #[test]
    fn test_position_partial_close_reduces_quantity() {
        let mut engine = make_engine(100_000.0);
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 2.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        let mut sl = make_order("pc1", "LONG", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(49_000.0);
        engine.open_orders.push(sl);

        let candle = make_candle(50_000.0, 50_500.0, 48_500.0, 49_200.0);
        engine.process_candle_logic(candle).unwrap();

        let pos = engine.positions.get(PRODUCT).unwrap();
        assert!((pos.quantity - 1.0).abs() < 1e-9);
        assert_eq!(pos.side, "LONG");
    }

    #[test]
    fn test_position_flip_long_to_short() {
        let mut engine = make_engine(100_000.0);
        // Start with LONG 1.0 @ 50_000
        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        // Place SHORT 2.0 → close 1.0 LONG, open 1.0 SHORT
        engine.open_orders.push(make_order("flip1", "SHORT", "MARKET", 0.0, 2.0));

        let candle = make_candle(48_000.0, 49_000.0, 47_000.0, 48_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);

        let pos = engine.positions.get(PRODUCT).unwrap();
        assert_eq!(pos.side, "SHORT");
        assert!((pos.quantity - 1.0).abs() < 1e-9);
        assert!((pos.entry_price - 48_000.0).abs() < 1e-9);

        // Realized PnL from closing long: (48_000 - 50_000) * 1.0 = -2_000
        let expected_balance = 100_000.0 - 2_000.0 - fills[0].fee;
        assert!((engine.balance - expected_balance).abs() < 1e-6);
    }

    // ── Cancel Order ──

    #[test]
    fn test_cancel_order_removes_from_open_orders() {
        let mut engine = make_engine(100_000.0);
        engine.open_orders.push(make_order("c1", "LONG", "LIMIT", 49_000.0, 1.0));
        engine.open_orders.push(make_order("c2", "SHORT", "LIMIT", 51_000.0, 1.0));

        let removed = engine.cancel_order("c1".to_string());
        assert!(removed);
        assert_eq!(engine.open_orders.len(), 1);
        assert_eq!(engine.open_orders[0].id, "c2");
    }

    #[test]
    fn test_cancel_nonexistent_order_returns_false() {
        let mut engine = make_engine(100_000.0);
        let removed = engine.cancel_order("nonexistent".to_string());
        assert!(!removed);
    }

    // ── Priority: Market > Conditional > Limit ──

    #[test]
    fn test_order_priority_market_before_conditional() {
        let mut engine = make_engine(100_000.0);

        // Submit both a market and a conditional order
        engine.open_orders.push(make_order("mkt", "LONG", "MARKET", 0.0, 1.0));
        let mut sl = make_order("cond", "LONG", "STOP_LOSS", 0.0, 0.5);
        sl.trigger_price = Some(49_500.0);
        engine.open_orders.push(sl);

        engine.positions.insert(PRODUCT.to_string(), Position {
            product_id: PRODUCT.to_string(),
            side: "LONG".to_string(),
            quantity: 0.5,
            entry_price: 50_000.0,
            unrealized_pnl: 0.0,
        });

        // Both should trigger: low=49_000 <= trigger 49_500
        let candle = make_candle(50_000.0, 50_500.0, 49_000.0, 49_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].fill_type, "MARKET"); // market first
        assert_eq!(fills[1].fill_type, "STOP_LOSS"); // conditional second
    }

    // ── Edge Cases ──

    #[test]
    fn test_no_orders_returns_empty_fills() {
        let mut engine = make_engine(100_000.0);
        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn test_close_position_on_flat_does_nothing() {
        let mut engine = make_engine(100_000.0);
        // No position, submit SL → should fill but close_position is no-op
        let mut sl = make_order("flat_sl", "LONG", "STOP_LOSS", 0.0, 1.0);
        sl.trigger_price = Some(49_000.0);
        engine.open_orders.push(sl);

        let candle = make_candle(50_000.0, 50_500.0, 48_000.0, 49_200.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert_eq!(fills.len(), 1);
        // Balance unchanged (no position to realize PnL from)
        let fee = fills[0].fee;
        assert!((engine.balance - (100_000.0 - fee)).abs() < 1e-6);
    }

    #[test]
    fn test_zero_fee_engine() {
        let mut engine = PyMatchingEngine {
            balance: 100_000.0,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            maker_fee: 0.0,
            taker_fee: 0.0,
        };
        engine.open_orders.push(make_order("zf1", "LONG", "MARKET", 0.0, 1.0));

        let candle = make_candle(50_000.0, 51_000.0, 49_000.0, 50_500.0);
        let fills = engine.process_candle_logic(candle).unwrap();

        assert!((fills[0].fee - 0.0).abs() < 1e-12);
        assert!((engine.balance - 100_000.0).abs() < 1e-9);
    }
}
