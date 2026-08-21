use super::{
    ledger::{InstrumentPositionSnapshot, OrderSnapshot},
    ledger_runtime,
    order_command::ExitPosition,
    order_runtime::OrderRuntimeHandle,
};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::{collections::HashSet, time::Duration};
use tokio::time::sleep;

const MAX_MITIGATION_ROUNDS: usize = 6;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct EmergencyMitigationResult {
    pub(crate) cancelled_orders: usize,
    pub(crate) exited_positions: usize,
}

pub(crate) async fn mitigate(profile: &str, account_id: &str) -> Result<EmergencyMitigationResult> {
    let mut cancelled_baskets = HashSet::new();
    let mut exited_instruments = HashSet::new();
    let mut action_failures = Vec::new();
    let mut snapshot_failures = Vec::new();
    let mut last_remote_state = "authoritative state unavailable".to_string();

    for attempt in 0..MAX_MITIGATION_ROUNDS {
        let snapshot = match load_exact_snapshot(
            profile,
            account_id,
            "Rithmic emergency current snapshot failed",
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                snapshot_failures.push(format!("current snapshot: {error}"));
                if attempt + 1 < MAX_MITIGATION_ROUNDS {
                    sleep(Duration::from_secs(1)).await;
                }
                continue;
            }
        };
        let current_working_orders = working_orders(&snapshot);
        let current_open_positions = open_positions(&snapshot);
        last_remote_state = format!(
            "working_orders_remain={} positions_remain={}",
            !current_working_orders.is_empty(),
            !current_open_positions.is_empty(),
        );
        if current_working_orders.is_empty() && current_open_positions.is_empty() {
            return Ok(EmergencyMitigationResult {
                cancelled_orders: cancelled_baskets.len(),
                exited_positions: exited_instruments.len(),
            });
        }

        // Cancel first, then refresh the authoritative ledger. An entry may
        // fill while its cancel is in flight, so positions from the pre-cancel
        // snapshot are never treated as the final flatten set.
        let refreshed_snapshot = if current_working_orders.is_empty() {
            snapshot
        } else {
            if let Err(error) = apply_actions(
                profile,
                account_id,
                &current_working_orders,
                &[],
                &mut cancelled_baskets,
                &mut exited_instruments,
                &mut action_failures,
            ) {
                action_failures.push(format!("cancel phase runtime failed: {error}"));
            }
            match load_exact_snapshot(
                profile,
                account_id,
                "Rithmic emergency post-cancel snapshot failed",
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    snapshot_failures.push(format!("post-cancel snapshot: {error}"));
                    if attempt + 1 < MAX_MITIGATION_ROUNDS {
                        sleep(Duration::from_secs(1)).await;
                    }
                    continue;
                }
            }
        };

        let refreshed_working_orders = working_orders(&refreshed_snapshot);
        let refreshed_open_positions = open_positions(&refreshed_snapshot);
        last_remote_state = format!(
            "working_orders_remain={} positions_remain={}",
            !refreshed_working_orders.is_empty(),
            !refreshed_open_positions.is_empty(),
        );
        if refreshed_working_orders.is_empty() && refreshed_open_positions.is_empty() {
            return Ok(EmergencyMitigationResult {
                cancelled_orders: cancelled_baskets.len(),
                exited_positions: exited_instruments.len(),
            });
        }

        if let Err(error) = apply_actions(
            profile,
            account_id,
            &refreshed_working_orders,
            &refreshed_open_positions,
            &mut cancelled_baskets,
            &mut exited_instruments,
            &mut action_failures,
        ) {
            action_failures.push(format!("flatten phase runtime failed: {error}"));
        }

        match load_exact_snapshot(
            profile,
            account_id,
            "Rithmic emergency verification snapshot failed",
        )
        .await
        {
            Ok(verification) => {
                let working_orders_remain = !working_orders(&verification).is_empty();
                let positions_remain = !open_positions(&verification).is_empty();
                if !working_orders_remain && !positions_remain {
                    return Ok(EmergencyMitigationResult {
                        cancelled_orders: cancelled_baskets.len(),
                        exited_positions: exited_instruments.len(),
                    });
                }
                last_remote_state = format!(
                    "working_orders_remain={working_orders_remain} \
                     positions_remain={positions_remain}"
                );
            }
            Err(error) => {
                snapshot_failures.push(format!("verification snapshot: {error}"));
            }
        }
        if attempt + 1 < MAX_MITIGATION_ROUNDS {
            sleep(Duration::from_secs(1)).await;
        }
    }

    anyhow::bail!(
        "Rithmic emergency mitigation did not reach flat state: {}; \
         snapshot_failures={}; action_failures={}",
        last_remote_state,
        snapshot_failures.join(" | "),
        action_failures.join(" | ")
    )
}

async fn load_exact_snapshot(
    profile: &str,
    account_id: &str,
    failure_context: &str,
) -> Result<ledger_runtime::RemoteLedgerSnapshot> {
    let snapshot = ledger_runtime::run(profile, Some(account_id))
        .await
        .with_context(|| failure_context.to_string())?;
    ensure!(
        snapshot.account.identity.account_id == account_id,
        "Rithmic emergency account identity mismatch"
    );
    Ok(snapshot)
}

fn working_orders(snapshot: &ledger_runtime::RemoteLedgerSnapshot) -> Vec<&OrderSnapshot> {
    snapshot
        .orders
        .iter()
        .filter(|order| order_may_be_working(order))
        .collect()
}

fn open_positions(
    snapshot: &ledger_runtime::RemoteLedgerSnapshot,
) -> Vec<&InstrumentPositionSnapshot> {
    snapshot
        .positions
        .iter()
        .filter(|position| position.net_quantity != 0)
        .collect()
}

fn apply_actions(
    profile: &str,
    account_id: &str,
    working_orders: &[&OrderSnapshot],
    open_positions: &[&InstrumentPositionSnapshot],
    cancelled_baskets: &mut HashSet<String>,
    exited_instruments: &mut HashSet<(String, String)>,
    action_failures: &mut Vec<String>,
) -> Result<()> {
    let runtime = OrderRuntimeHandle::start(profile.to_string(), Some(account_id.to_string()))
        .context("Rithmic emergency ORDER runtime failed")?;
    for order in working_orders {
        match runtime.cancel(order.basket_id.clone()) {
            Ok(()) => {
                cancelled_baskets.insert(order.basket_id.clone());
            }
            Err(error) => {
                action_failures.push(format!("cancel basket {} failed: {error}", order.basket_id));
            }
        }
    }
    for position in open_positions {
        let instrument = (position.exchange.clone(), position.symbol.clone());
        match runtime.exit_position(ExitPosition {
            exchange: instrument.0.clone(),
            symbol: instrument.1.clone(),
            window_name: None,
        }) {
            Ok(()) => {
                exited_instruments.insert(instrument);
            }
            Err(error) => action_failures.push(format!(
                "exit {}/{} failed: {error}",
                position.exchange, position.symbol,
            )),
        }
    }
    Ok(())
}

fn order_may_be_working(order: &OrderSnapshot) -> bool {
    let notification = order
        .notification_type
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();
    let status = order.status.trim().to_ascii_lowercase();
    let quantity = order.quantity;
    let filled = order.filled_quantity.unwrap_or(Decimal::ZERO);

    if matches!(notification.as_str(), "CANCEL" | "REJECT")
        || matches!(
            status.as_str(),
            "cancel" | "canceled" | "cancelled" | "reject" | "rejected" | "expired" | "failed"
        )
    {
        return !(quantity > Decimal::ZERO && filled < quantity);
    }
    if quantity > Decimal::ZERO && filled == quantity {
        return !(notification == "FILL"
            || matches!(
                status.as_str(),
                "complete" | "completed" | "filled" | "closed"
            ));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::rithmic::ledger::{Account, AccountIdentity, TransactionType};
    use rust_decimal_macros::dec;

    fn account_identity() -> AccountIdentity {
        AccountIdentity {
            fcm_id: "FCM".to_string(),
            ib_id: "IB".to_string(),
            account_id: "ACCOUNT".to_string(),
        }
    }

    fn order(
        status: &str,
        notification_type: Option<&str>,
        quantity: Decimal,
        filled_quantity: Option<Decimal>,
    ) -> OrderSnapshot {
        OrderSnapshot {
            account: account_identity(),
            client_order_id: Some("client".to_string()),
            window_name: None,
            originator_window_name: None,
            basket_id: "basket".to_string(),
            original_basket_id: None,
            linked_basket_ids: None,
            exchange_order_id: None,
            exchange: "CME".to_string(),
            symbol: "MNQU6".to_string(),
            status: status.to_string(),
            notification_type: notification_type.map(str::to_string),
            completion_reason: None,
            report_text: None,
            transaction_type: TransactionType::Buy,
            quantity,
            price: None,
            trigger_price: None,
            price_type: None,
            bracket_type: None,
            filled_quantity,
            unfilled_quantity: None,
            average_fill_price: None,
            timestamp_ms: None,
        }
    }

    fn position(net_quantity: i32) -> InstrumentPositionSnapshot {
        InstrumentPositionSnapshot {
            account: account_identity(),
            exchange: "CME".to_string(),
            symbol: "MNQU6".to_string(),
            net_quantity,
            average_open_fill_price: Some(dec!(20000)),
            open_pnl: Some(Decimal::ZERO),
            day_pnl: Some(Decimal::ZERO),
            timestamp_ms: Some(1),
        }
    }

    fn snapshot(
        orders: Vec<OrderSnapshot>,
        positions: Vec<InstrumentPositionSnapshot>,
    ) -> ledger_runtime::RemoteLedgerSnapshot {
        ledger_runtime::RemoteLedgerSnapshot {
            account: Account {
                identity: account_identity(),
                name: None,
                currency: Some("USD".to_string()),
            },
            orders,
            order_history: Vec::new(),
            fills: Vec::new(),
            positions,
            account_summary: None,
        }
    }

    #[test]
    fn working_order_classifier_covers_terminal_and_ambiguous_states() {
        let cases = [
            ("OPEN", None, dec!(1), None, true),
            ("OPEN", None, dec!(2), Some(dec!(1)), true),
            ("FILLED", Some("FILL"), dec!(1), Some(dec!(1)), false),
            ("COMPLETE", None, dec!(1), Some(dec!(1)), false),
            ("CANCELLED", Some("CANCEL"), dec!(1), None, false),
            ("REJECTED", Some("REJECT"), dec!(1), None, false),
            ("UNKNOWN", None, dec!(1), Some(dec!(1)), true),
            ("CANCELLED", Some("CANCEL"), Decimal::ZERO, None, true),
        ];

        for (status, notification, quantity, filled, expected) in cases {
            assert_eq!(
                order_may_be_working(&order(status, notification, quantity, filled)),
                expected,
                "status={status} notification={notification:?}"
            );
        }
    }

    #[test]
    fn post_cancel_snapshot_exposes_position_created_by_late_fill() {
        let before_cancel = snapshot(vec![order("OPEN", None, dec!(1), None)], Vec::new());
        let after_cancel = snapshot(
            vec![order("FILLED", Some("FILL"), dec!(1), Some(dec!(1)))],
            vec![position(1)],
        );

        assert_eq!(working_orders(&before_cancel).len(), 1);
        assert!(open_positions(&before_cancel).is_empty());
        assert!(working_orders(&after_cancel).is_empty());
        assert_eq!(open_positions(&after_cancel).len(), 1);
    }
}
