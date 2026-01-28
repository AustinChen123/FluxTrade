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
}

#[pymethods]
impl PyMatchingEngine {
    #[new]
    fn new(initial_balance: f64) -> Self {
        PyMatchingEngine {
            balance: initial_balance,
            positions: HashMap::new(),
            open_orders: Vec::new(),
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
}

impl PyMatchingEngine {
    fn process_candle_logic(&mut self, candle: Candlestick) -> PyResult<Vec<FillEvent>> {
        let mut fills = Vec::new();
        let mut remaining_orders = Vec::new();

        // Separate orders into Market and Limit buckets to ensure priority
        // Market orders must be processed first.
        let (market_orders, limit_orders): (Vec<Order>, Vec<Order>) = 
            self.open_orders.drain(..)
            .partition(|o| o.order_type == "MARKET");

        // 1. Process Market Orders
        for order in market_orders {
            let mut matched = false;
            let mut fill_price = 0.0;

            if order.product_id == candle.product_id {
                // Market orders fill immediately at Open price
                matched = true;
                fill_price = candle.open;
            }

            if matched {
                let fill = FillEvent {
                    order_id: order.id.clone(),
                    product_id: order.product_id.clone(),
                    price: fill_price,
                    quantity: order.quantity,
                    fee: 0.0,
                    timestamp: candle.timestamp,
                };
                self.update_position(&order, fill_price);
                fills.push(fill);
            } else {
                remaining_orders.push(order);
            }
        }

        // 2. Process Limit Orders
        for order in limit_orders {
            let mut matched = false;
            let mut fill_price = 0.0;

            if order.product_id == candle.product_id {
                // Check LIMIT orders
                if order.order_type == "LIMIT" {
                    if order.side == "LONG" {
                        // Buy if Low <= Limit Price
                        if candle.low <= order.price {
                            matched = true;
                            fill_price = order.price;
                        }
                    } else if order.side == "SHORT" {
                        // Sell if High >= Limit Price
                        if candle.high >= order.price {
                            matched = true;
                            fill_price = order.price;
                        }
                    }
                } 
            }

            if matched {
                // Create Fill Event
                let fill = FillEvent {
                    order_id: order.id.clone(),
                    product_id: order.product_id.clone(),
                    price: fill_price,
                    quantity: order.quantity,
                    fee: 0.0, // Fee logic can be added later
                    timestamp: candle.timestamp,
                };
                
                // Update Internal State
                self.update_position(&order, fill_price);
                fills.push(fill);
            } else {
                remaining_orders.push(order);
            }
        }

        self.open_orders = remaining_orders;
        Ok(fills)
    }

    fn update_position(&mut self, order: &Order, fill_price: f64) {
        // Get or Create Position
        let entry = self.positions.entry(order.product_id.clone()).or_insert(Position {
            product_id: order.product_id.clone(),
            side: "FLAT".to_string(),
            quantity: 0.0,
            entry_price: 0.0,
            unrealized_pnl: 0.0,
        });

        // Netting Logic
        if entry.quantity == 0.0 || entry.side == "FLAT" {
            // New Position
            entry.side = order.side.clone();
            entry.quantity = order.quantity;
            entry.entry_price = fill_price;
        } else if entry.side == order.side {
            // Increase Position (Weighted Average Price)
            let total_cost = entry.quantity * entry.entry_price + order.quantity * fill_price;
            let new_qty = entry.quantity + order.quantity;
            entry.entry_price = total_cost / new_qty;
            entry.quantity = new_qty;
        } else {
            // Close / Reduce Position
            let close_qty = if order.quantity >= entry.quantity {
                entry.quantity
            } else {
                order.quantity
            };

            // Calculate PnL for the closed portion
            let price_diff = if entry.side == "LONG" {
                fill_price - entry.entry_price
            } else {
                entry.entry_price - fill_price // Short: Entry - Exit
            };
            
            let realized_pnl = price_diff * close_qty;
            self.balance += realized_pnl;

            // Update Quantity
            let remaining_qty = entry.quantity - close_qty;
            let excess_order_qty = order.quantity - close_qty;

            if remaining_qty > 1e-9 {
                // Partial Close
                entry.quantity = remaining_qty;
            } else if excess_order_qty > 1e-9 {
                // Flip Position (Reverse)
                entry.side = order.side.clone();
                entry.quantity = excess_order_qty;
                entry.entry_price = fill_price;
            } else {
                // Flat
                entry.side = "FLAT".to_string();
                entry.quantity = 0.0;
                entry.entry_price = 0.0;
            }
        }
    }
}
