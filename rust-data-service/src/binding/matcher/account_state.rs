use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::binding::models::{Order, Position};
use crate::binding::spot_ledger::CashSpotLedger;

/// Sole storage owner of the legacy matcher's mutable account state.
/// Matching and settlement retain their existing mutation responsibilities.
#[derive(Clone)]
pub(super) struct AccountState {
    pub(super) balance: Decimal,
    pub(super) positions: HashMap<String, Position>,
    pub(super) open_orders: Vec<Order>,
    pub(super) spot_ledger: Option<CashSpotLedger>,
    pub(super) spot_cost_basis: Decimal,
    pub(super) spot_realized_pnl: Decimal,
}

impl AccountState {
    pub(super) fn new(balance: Decimal, spot_ledger: Option<CashSpotLedger>) -> Self {
        Self {
            balance,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            spot_ledger,
            spot_cost_basis: Decimal::ZERO,
            spot_realized_pnl: Decimal::ZERO,
        }
    }
}
