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
