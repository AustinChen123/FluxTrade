use super::{
    codec,
    ledger::{AccountIdentity, TransactionType},
    protocol,
};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

const EXCHANGE_ORDER_NOTIFICATION: i32 = 352;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderEvent {
    pub(crate) account: AccountIdentity,
    pub(crate) client_order_id: Option<String>,
    pub(crate) window_name: Option<String>,
    pub(crate) originator_window_name: Option<String>,
    pub(crate) basket_id: String,
    pub(crate) original_basket_id: Option<String>,
    pub(crate) linked_basket_ids: Option<String>,
    pub(crate) exchange_order_id: Option<String>,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) status: String,
    pub(crate) notification_type: String,
    pub(crate) transaction_type: TransactionType,
    pub(crate) quantity: Option<Decimal>,
    pub(crate) price: Option<Decimal>,
    pub(crate) trigger_price: Option<Decimal>,
    pub(crate) price_type: Option<String>,
    pub(crate) bracket_type: Option<String>,
    pub(crate) last_fill_quantity: Option<Decimal>,
    pub(crate) last_fill_price: Option<Decimal>,
    pub(crate) cumulative_filled_quantity: Option<Decimal>,
    pub(crate) cumulative_average_price: Option<Decimal>,
    pub(crate) timestamp_ms: Option<i64>,
}

pub(crate) fn decode_order_event(
    payload: &[u8],
    expected_account: &AccountIdentity,
) -> Result<OrderEvent> {
    validate_account(expected_account)?;
    ensure_template(payload)?;
    let response: protocol::ExchangeOrderNotification = codec::decode(payload)?;
    ensure!(
        response.is_snapshot != Some(true),
        "Rithmic live order event is a snapshot"
    );
    let account = account_identity(response.fcm_id, response.ib_id, response.account_id)?;
    ensure!(
        account == *expected_account,
        "Rithmic live order event account mismatch"
    );
    let notify_type = protocol::exchange_order_notification::NotifyType::try_from(
        response
            .notify_type
            .context("missing Rithmic order notify type")?,
    )
    .context("unknown Rithmic order notify type")?;
    let cumulative_filled_quantity =
        optional_nonnegative_quantity(response.total_fill_size, "total fill size")?;
    let unfilled_quantity =
        optional_nonnegative_quantity(response.total_unfilled_size, "total unfilled size")?;
    let quantity = optional_positive_quantity(response.quantity, "order quantity")?;
    let status = classify_status(
        notify_type,
        response.status.as_deref(),
        quantity,
        cumulative_filled_quantity,
        unfilled_quantity,
    )?;
    Ok(OrderEvent {
        account,
        client_order_id: optional_text(response.user_tag),
        window_name: optional_text(response.window_name),
        originator_window_name: optional_text(response.originator_window_name),
        basket_id: required_text(response.basket_id, "basket ID")?,
        original_basket_id: optional_text(response.original_basket_id),
        linked_basket_ids: optional_text(response.linked_basket_ids),
        exchange_order_id: optional_text(response.exchange_order_id),
        exchange: required_text(response.exchange, "exchange")?,
        symbol: required_text(response.symbol, "symbol")?,
        status,
        notification_type: notify_type.as_str_name().to_ascii_lowercase(),
        transaction_type: transaction_type(response.transaction_type)?,
        quantity,
        price: optional_nonnegative_decimal(response.price, "order price")?,
        trigger_price: optional_nonnegative_decimal(response.trigger_price, "trigger price")?,
        price_type: optional_exchange_price_type(response.price_type)?,
        bracket_type: optional_exchange_bracket_type(response.bracket_type)?,
        last_fill_quantity: optional_positive_quantity(response.fill_size, "fill size")?,
        last_fill_price: optional_nonnegative_decimal(response.fill_price, "fill price")?,
        cumulative_filled_quantity,
        cumulative_average_price: optional_nonnegative_decimal(
            response.avg_fill_price,
            "average fill price",
        )?,
        timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
    })
}

pub(crate) fn is_order_event(template_id: i32) -> bool {
    template_id == EXCHANGE_ORDER_NOTIFICATION
}

pub(crate) fn notification_is_snapshot(payload: &[u8]) -> Result<bool> {
    ensure_template(payload)?;
    let response: protocol::ExchangeOrderNotification = codec::decode(payload)?;
    Ok(response.is_snapshot == Some(true))
}

fn classify_status(
    notify_type: protocol::exchange_order_notification::NotifyType,
    raw_status: Option<&str>,
    quantity: Option<Decimal>,
    cumulative_filled: Option<Decimal>,
    unfilled: Option<Decimal>,
) -> Result<String> {
    use protocol::exchange_order_notification::NotifyType;
    if let (Some(filled), Some(quantity)) = (cumulative_filled, quantity) {
        ensure!(
            filled <= quantity,
            "Rithmic cumulative fill exceeds order quantity"
        );
    }
    if let (Some(unfilled), Some(quantity)) = (unfilled, quantity) {
        ensure!(
            unfilled <= quantity,
            "Rithmic unfilled size exceeds order quantity"
        );
    }
    let status = match notify_type {
        NotifyType::Fill => {
            let quantity = quantity.context("Rithmic fill event omitted order quantity")?;
            let filled = cumulative_filled
                .filter(|filled| *filled > Decimal::ZERO)
                .context("Rithmic fill event omitted positive cumulative fill size")?;
            if let Some(unfilled) = unfilled {
                ensure!(
                    filled + unfilled == quantity,
                    "Rithmic fill totals do not match order quantity"
                );
            }
            if filled == quantity {
                "filled"
            } else {
                "partially_filled"
            }
        }
        NotifyType::Cancel => {
            ensure_terminal_fill_is_incomplete("cancel", quantity, cumulative_filled)?;
            "cancelled"
        }
        NotifyType::Reject => {
            ensure_terminal_fill_is_incomplete("reject", quantity, cumulative_filled)?;
            "rejected"
        }
        NotifyType::NotCancelled => "cancel_rejected",
        NotifyType::Status | NotifyType::Modify | NotifyType::Trigger | NotifyType::Generic => {
            if raw_status.map(str::trim).is_none_or(str::is_empty)
                && notify_type != NotifyType::Generic
            {
                return classify_fill_progress(quantity, cumulative_filled);
            }
            return normalize_status(raw_status, quantity, cumulative_filled);
        }
        NotifyType::NotModified => "modify_rejected",
    };
    Ok(status.to_string())
}

fn ensure_terminal_fill_is_incomplete(
    notification: &str,
    quantity: Option<Decimal>,
    cumulative_filled: Option<Decimal>,
) -> Result<()> {
    if let Some(filled) = cumulative_filled.filter(|filled| *filled > Decimal::ZERO) {
        let quantity = quantity.with_context(|| {
            format!("Rithmic {notification} event with fills omitted order quantity")
        })?;
        ensure!(
            filled < quantity,
            "Rithmic {notification} event conflicts with complete fill"
        );
    }
    Ok(())
}

fn classify_fill_progress(
    quantity: Option<Decimal>,
    cumulative_filled: Option<Decimal>,
) -> Result<String> {
    match cumulative_filled {
        None => Ok("open".to_string()),
        Some(filled) if filled.is_zero() => Ok("open".to_string()),
        Some(filled) => {
            let quantity =
                quantity.context("Rithmic order event with fills omitted order quantity")?;
            if filled == quantity {
                Ok("filled".to_string())
            } else {
                Ok("partially_filled".to_string())
            }
        }
    }
}

fn normalize_status(
    raw_status: Option<&str>,
    quantity: Option<Decimal>,
    cumulative_filled: Option<Decimal>,
) -> Result<String> {
    let normalized = raw_status
        .map(str::trim)
        .filter(|status| !status.is_empty())
        .context("Rithmic order event omitted status")?
        .to_ascii_lowercase()
        .replace([' ', '-'], "_");
    let status = match normalized.as_str() {
        "open" | "open_pending" | "submitted" | "accepted" => match cumulative_filled {
            None => "open",
            Some(filled) if filled.is_zero() => "open",
            Some(filled) if quantity.is_some_and(|quantity| filled < quantity) => {
                "partially_filled"
            }
            Some(_) => anyhow::bail!("Rithmic open status conflicts with cumulative fill"),
        },
        "partial" | "partially_filled" | "partiallyfilled" => {
            let quantity = quantity.context("Rithmic partial status omitted order quantity")?;
            ensure!(
                cumulative_filled
                    .is_some_and(|filled| { filled > Decimal::ZERO && filled < quantity }),
                "Rithmic partial status conflicts with cumulative fill"
            );
            "partially_filled"
        }
        "complete" | "completed" | "filled" => {
            let quantity = quantity.context("Rithmic complete status omitted order quantity")?;
            ensure!(
                cumulative_filled == Some(quantity),
                "Rithmic complete status is not fully filled"
            );
            "filled"
        }
        "cancel" | "canceled" | "cancelled" => "cancelled",
        "reject" | "rejected" => "rejected",
        other => anyhow::bail!("unsupported Rithmic order status {other}"),
    };
    Ok(status.to_string())
}

fn ensure_template(payload: &[u8]) -> Result<()> {
    let actual = codec::template_id(payload)?;
    ensure!(
        actual == EXCHANGE_ORDER_NOTIFICATION,
        "unexpected Rithmic template {actual}, expected {EXCHANGE_ORDER_NOTIFICATION}"
    );
    Ok(())
}

fn validate_account(account: &AccountIdentity) -> Result<()> {
    required_text(Some(account.fcm_id.clone()), "fcm ID")?;
    required_text(Some(account.ib_id.clone()), "IB ID")?;
    required_text(Some(account.account_id.clone()), "account ID")?;
    Ok(())
}

fn account_identity(
    fcm_id: Option<String>,
    ib_id: Option<String>,
    account_id: Option<String>,
) -> Result<AccountIdentity> {
    Ok(AccountIdentity {
        fcm_id: required_text(fcm_id, "fcm ID")?,
        ib_id: required_text(ib_id, "IB ID")?,
        account_id: required_text(account_id, "account ID")?,
    })
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic {field}"))
}

fn optional_text(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn transaction_type(value: Option<i32>) -> Result<TransactionType> {
    match value {
        Some(value)
            if value == protocol::exchange_order_notification::TransactionType::Buy as i32 =>
        {
            Ok(TransactionType::Buy)
        }
        Some(value)
            if value == protocol::exchange_order_notification::TransactionType::Sell as i32 =>
        {
            Ok(TransactionType::Sell)
        }
        Some(value)
            if value == protocol::exchange_order_notification::TransactionType::Ss as i32 =>
        {
            Ok(TransactionType::ShortSell)
        }
        Some(_) => anyhow::bail!("unknown Rithmic order transaction type"),
        None => anyhow::bail!("missing Rithmic order transaction type"),
    }
}

fn optional_exchange_price_type(value: Option<i32>) -> Result<Option<String>> {
    use protocol::exchange_order_notification::PriceType;
    value
        .map(|value| {
            let value = PriceType::try_from(value).context("unknown Rithmic order price type")?;
            Ok(match value {
                PriceType::Limit => "limit",
                PriceType::Market => "market",
                PriceType::StopLimit => "stop_limit",
                PriceType::StopMarket => "stop_market",
            }
            .to_string())
        })
        .transpose()
}

fn optional_exchange_bracket_type(value: Option<i32>) -> Result<Option<String>> {
    use protocol::exchange_order_notification::BracketType;
    value
        .map(|value| {
            let value =
                BracketType::try_from(value).context("unknown Rithmic order bracket type")?;
            Ok(match value {
                BracketType::StopOnly => "stop_only",
                BracketType::TargetOnly => "target_only",
                BracketType::TargetAndStop => "target_and_stop",
                BracketType::StopOnlyStatic => "stop_only_static",
                BracketType::TargetOnlyStatic => "target_only_static",
                BracketType::TargetAndStopStatic => "target_and_stop_static",
            }
            .to_string())
        })
        .transpose()
}

fn optional_positive_quantity(value: Option<i32>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(|value| {
            ensure!(value > 0, "invalid Rithmic {field}");
            Ok(Decimal::from(value))
        })
        .transpose()
}

fn optional_nonnegative_quantity(value: Option<i32>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(|value| {
            ensure!(value >= 0, "invalid Rithmic {field}");
            Ok(Decimal::from(value))
        })
        .transpose()
}

fn optional_nonnegative_decimal(value: Option<f64>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(|value| {
            ensure!(value.is_finite() && value >= 0.0, "invalid Rithmic {field}");
            Decimal::from_str(&value.to_string())
                .with_context(|| format!("invalid Rithmic {field}"))
        })
        .transpose()
}

fn optional_epoch_millis(ssboe: Option<i32>, usecs: Option<i32>) -> Result<Option<i64>> {
    match (ssboe, usecs) {
        (None, None) => Ok(None),
        (Some(ssboe), usecs) => {
            ensure!(ssboe >= 0, "invalid Rithmic order ssboe");
            let usecs = usecs.unwrap_or_default();
            ensure!(
                (0..1_000_000).contains(&usecs),
                "invalid Rithmic order usecs"
            );
            Ok(Some(i64::from(ssboe) * 1_000 + i64::from(usecs) / 1_000))
        }
        (None, Some(_)) => anyhow::bail!("Rithmic order usecs requires ssboe"),
    }
}
