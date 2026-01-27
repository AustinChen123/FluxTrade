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
        let mut fills = Vec::new();
        let mut remaining_orders = Vec::new();

        // Iterate through all open orders
        // Note: In a real HFT engine, this would be an OrderBook (B-Tree or similar)
        // For backtesting iteration, a Vec is acceptable for < 10k orders.
        let orders: Vec<Order> = self.open_orders.drain(..).collect();
        for order in orders {
            let mut matched = false;
            let mut fill_price = 0.0;

            if order.product_id == candle.product_id {
                // Check LIMIT orders
                if order.order_type == "LIMIT" {
                    if order.side == "LONG" {
                        // Buy if Low <= Limit Price
                        // Fill at Limit Price (Conservative backtest assumption) or Open if Open < Limit?
                        // Standard conservative: Fill at Limit Price.
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
                // TODO: MARKET orders (assume fill at Open or Close)
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
}

impl PyMatchingEngine {
    fn update_position(&mut self, order: &Order, fill_price: f64) {
        // Get or Create Position
        let entry = self.positions.entry(order.product_id.clone()).or_insert(Position {
            product_id: order.product_id.clone(),
            side: "FLAT".to_string(),
            quantity: 0.0,
            entry_price: 0.0,
            unrealized_pnl: 0.0,
        });

        // Simplified Position Logic (Netting Mode)
        // If side matches or position is flat, add quantity
        if entry.quantity == 0.0 || entry.side == order.side {
            let total_cost = entry.quantity * entry.entry_price + order.quantity * fill_price;
            let new_qty = entry.quantity + order.quantity;
            
            entry.side = order.side.clone();
            entry.entry_price = total_cost / new_qty;
            entry.quantity = new_qty;
        } else {
            // Closing position logic would go here
            // For V2 prototype, we just accumulate for now to prove data flow
            // Real implementation needs to handle reducing quantity and realizing PnL
        }
    }
}
