use rust_decimal::Decimal;

use super::{FeeModel, PyMatchingEngine, SettlementModel};
use crate::binding::models::{Order, Position};
use crate::binding::spot_ledger::{CashSpotSettlement, SpotFeeAsset};

pub(super) fn settle_fill(
    engine: &mut PyMatchingEngine,
    order: &Order,
    fill_price: Decimal,
    is_taker: bool,
    calculated_fee: Decimal,
) -> Result<Decimal, String> {
    if engine.settlement_model == SettlementModel::CashSpot {
        let expected_fee = calculate_fee(engine, fill_price, order.quantity, is_taker);
        if calculated_fee != expected_fee {
            return Err(format!(
                "cash_spot settlement fee mismatch: calculated={calculated_fee} expected={expected_fee}"
            ));
        }
        let settlement = settle_spot_order(engine, order, fill_price, is_taker)?;
        debug_assert_eq!(settlement.fee, calculated_fee);
        update_spot_position(engine, order, fill_price, settlement);
        return Ok(calculated_fee);
    }

    update_position(engine, order, fill_price);
    let charged_fee = std::cmp::min(calculated_fee, engine.balance);
    engine.balance -= charged_fee;
    Ok(calculated_fee)
}

pub(super) fn calculate_fee(
    engine: &PyMatchingEngine,
    price: Decimal,
    quantity: Decimal,
    is_taker: bool,
) -> Decimal {
    let fee = if is_taker {
        engine.taker_fee
    } else {
        engine.maker_fee
    };
    if let Some(ledger) = &engine.spot_ledger {
        return match ledger.fee_asset {
            SpotFeeAsset::Base => quantity * fee,
            SpotFeeAsset::Quote => price * quantity * fee,
        };
    }
    match engine.fee_model {
        FeeModel::PercentageNotional => price * quantity * engine.contract_multiplier * fee,
        FeeModel::PerContract => quantity * fee,
    }
}

fn settle_spot_order(
    engine: &mut PyMatchingEngine,
    order: &Order,
    fill_price: Decimal,
    is_taker: bool,
) -> Result<CashSpotSettlement, String> {
    let fee_rate = if is_taker {
        engine.taker_fee
    } else {
        engine.maker_fee
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
        let ledger = engine
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
        let position_key = position_key(&order.strategy_id, &order.product_id);
        let available_position = engine
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
    engine
        .spot_ledger
        .as_mut()
        .ok_or_else(|| "cash_spot ledger is unavailable".to_string())?
        .settle(&settlement_order, fill_price, fee_rate)
}

fn update_spot_position(
    engine: &mut PyMatchingEngine,
    order: &Order,
    fill_price: Decimal,
    settlement: CashSpotSettlement,
) {
    let key = position_key(&order.strategy_id, &order.product_id);
    let mut position = engine.positions.remove(&key).unwrap_or(Position {
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
        engine.spot_cost_basis += acquired_cost;
        engine.positions.insert(key, position);
        return;
    }

    let reduction = -settlement.base_delta;
    let removed_cost = position.entry_price * reduction;
    let proceeds = settlement.quote_delta;
    engine.spot_cost_basis -= removed_cost;
    engine.spot_realized_pnl += proceeds - removed_cost;
    position.quantity -= reduction;
    if position.quantity > Decimal::ZERO {
        engine.positions.insert(key, position);
    }

    debug_assert!(fill_price > Decimal::ZERO);
}

pub(super) fn position_key(strategy_id: &str, product_id: &str) -> String {
    format!("{strategy_id}:{product_id}")
}

fn update_position(engine: &mut PyMatchingEngine, order: &Order, fill_price: Decimal) {
    let key = position_key(&order.strategy_id, &order.product_id);
    let mut position = engine.positions.remove(&key).unwrap_or(Position {
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
        close_position(engine, &mut position, order, fill_price);
    } else {
        apply_position_change(engine, &mut position, order, fill_price);
    }

    if position.quantity > Decimal::ZERO && position.side != "FLAT" {
        engine.positions.insert(key, position);
    }
}

fn close_position(
    engine: &mut PyMatchingEngine,
    position: &mut Position,
    order: &Order,
    fill_price: Decimal,
) {
    if position.quantity.is_zero() || position.side == "FLAT" {
        return;
    }

    let close_quantity = order.quantity.min(position.quantity);
    let price_difference = if position.side == "LONG" {
        fill_price - position.entry_price
    } else {
        position.entry_price - fill_price
    };
    let realized_pnl = price_difference * close_quantity * engine.contract_multiplier;
    engine.balance += realized_pnl;

    let remaining = position.quantity - close_quantity;
    if remaining > Decimal::ZERO {
        position.quantity = remaining;
    } else {
        position.side = "FLAT".to_string();
        position.quantity = Decimal::ZERO;
        position.entry_price = Decimal::ZERO;
    }
}

fn apply_position_change(
    engine: &mut PyMatchingEngine,
    position: &mut Position,
    order: &Order,
    fill_price: Decimal,
) {
    if position.quantity.is_zero() || position.side == "FLAT" {
        position.side = order.side.clone();
        position.quantity = order.quantity;
        position.entry_price = fill_price;
    } else if position.side == order.side {
        let total_cost = position.quantity * position.entry_price + order.quantity * fill_price;
        let new_quantity = position.quantity + order.quantity;
        position.entry_price = total_cost / new_quantity;
        position.quantity = new_quantity;
    } else {
        let close_quantity = order.quantity.min(position.quantity);
        let price_difference = if position.side == "LONG" {
            fill_price - position.entry_price
        } else {
            position.entry_price - fill_price
        };
        let realized_pnl = price_difference * close_quantity * engine.contract_multiplier;
        engine.balance += realized_pnl;

        let remaining = position.quantity - close_quantity;
        let excess = order.quantity - close_quantity;

        if remaining > Decimal::ZERO {
            position.quantity = remaining;
        } else if excess > Decimal::ZERO {
            position.side = order.side.clone();
            position.quantity = excess;
            position.entry_price = fill_price;
        } else {
            position.side = "FLAT".to_string();
            position.quantity = Decimal::ZERO;
            position.entry_price = Decimal::ZERO;
        }
    }
}
