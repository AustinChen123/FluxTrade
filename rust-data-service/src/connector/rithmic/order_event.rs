use super::{
    codec,
    ledger::{AccountIdentity, TransactionType},
    order_field_policy::{
        optional_nonnegative_decimal_with_diagnostic, optional_quantity_with_diagnostic,
        NumericFieldContext, QuantityFieldDiagnostic,
    },
    protocol,
};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;

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
    pub(crate) raw_status: Option<String>,
    pub(crate) notification_type: String,
    pub(crate) transaction_type: TransactionType,
    pub(crate) quantity: Option<Decimal>,
    pub(crate) unfilled_quantity: Option<Decimal>,
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
    let raw_notify_type = response
        .notify_type
        .context("missing Rithmic order notify type")?;
    let notify_type = protocol::exchange_order_notification::NotifyType::try_from(raw_notify_type)
        .context("unknown Rithmic order notify type")?;
    let numeric_context = NumericFieldContext::new(
        EXCHANGE_ORDER_NOTIFICATION,
        "live",
        Some(raw_notify_type),
        Some(notify_type.as_str_name()),
        response.basket_id.as_deref(),
        response.exchange_order_id.as_deref(),
    );
    let cumulative_filled_quantity = optional_quantity_with_diagnostic(
        response.total_fill_size,
        "total fill size",
        "total_fill_size",
        false,
        numeric_context.clone(),
    )?;
    let unfilled_quantity = optional_quantity_with_diagnostic(
        response.total_unfilled_size,
        "total unfilled size",
        "total_unfilled_size",
        false,
        numeric_context.clone(),
    )?;
    let quantity = optional_quantity_with_diagnostic(
        response.quantity,
        "order quantity",
        "quantity",
        true,
        numeric_context.clone(),
    )?
    .or_else(|| {
        // Only an explicitly complete Fill proves the missing original quantity.
        (notify_type == protocol::exchange_order_notification::NotifyType::Fill
            && unfilled_quantity == Some(Decimal::ZERO))
        .then_some(cumulative_filled_quantity)
        .flatten()
        .filter(|filled| *filled > Decimal::ZERO)
    });
    let status = classify_status(
        notify_type,
        response.status.as_deref(),
        quantity,
        cumulative_filled_quantity,
        unfilled_quantity,
        numeric_context.clone(),
        RawOrderQuantities {
            quantity: response.quantity,
            filled: response.total_fill_size,
            unfilled: response.total_unfilled_size,
        },
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
        raw_status: response.status,
        notification_type: notify_type.as_str_name().to_ascii_lowercase(),
        transaction_type: transaction_type(response.transaction_type)?,
        quantity,
        unfilled_quantity,
        price: optional_nonnegative_decimal_with_diagnostic(
            response.price,
            "order price",
            "price",
            numeric_context.clone(),
        )?,
        trigger_price: optional_nonnegative_decimal_with_diagnostic(
            response.trigger_price,
            "trigger price",
            "trigger_price",
            numeric_context.clone(),
        )?,
        price_type: optional_exchange_price_type(response.price_type)?,
        bracket_type: optional_exchange_bracket_type(response.bracket_type)?,
        last_fill_quantity: optional_quantity_with_diagnostic(
            response.fill_size,
            "fill size",
            "fill_size",
            true,
            numeric_context.clone(),
        )?,
        last_fill_price: optional_nonnegative_decimal_with_diagnostic(
            response.fill_price,
            "fill price",
            "fill_price",
            numeric_context.clone(),
        )?,
        cumulative_filled_quantity,
        cumulative_average_price: optional_nonnegative_decimal_with_diagnostic(
            response.avg_fill_price,
            "average fill price",
            "avg_fill_price",
            numeric_context,
        )?,
        timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
    })
}

pub(crate) fn is_order_event(template_id: i32) -> bool {
    template_id == EXCHANGE_ORDER_NOTIFICATION
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalid_price_diagnostic_for_binding_test() -> anyhow::Error {
    let expected_account = AccountIdentity {
        fcm_id: "FCM".to_string(),
        ib_id: "IB".to_string(),
        account_id: "ACCOUNT_ID_SECRET".to_string(),
    };
    let payload = codec::encode(&protocol::ExchangeOrderNotification {
        template_id: EXCHANGE_ORDER_NOTIFICATION,
        notify_type: Some(protocol::exchange_order_notification::NotifyType::Status as i32),
        is_snapshot: Some(false),
        fcm_id: Some("FCM".to_string()),
        ib_id: Some("IB".to_string()),
        account_id: Some("ACCOUNT_ID_SECRET".to_string()),
        basket_id: Some("BASKET_ID_SECRET_123".to_string()),
        exchange_order_id: Some("ORDER_ID_SECRET_456".to_string()),
        exchange: Some("CME".to_string()),
        symbol: Some("NQU6".to_string()),
        status: Some("OPEN".to_string()),
        transaction_type: Some(protocol::exchange_order_notification::TransactionType::Buy as i32),
        quantity: Some(1),
        price: Some(-1.0),
        ..Default::default()
    })
    .expect("encode invalid price fixture");
    decode_order_event(&payload, &expected_account).expect_err("invalid price is rejected")
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalid_quantity_diagnostic_for_binding_test() -> anyhow::Error {
    let expected_account = AccountIdentity {
        fcm_id: "FCM".to_string(),
        ib_id: "IB".to_string(),
        account_id: "ACCOUNT_ID_SECRET".to_string(),
    };
    let payload = codec::encode(&protocol::ExchangeOrderNotification {
        template_id: EXCHANGE_ORDER_NOTIFICATION,
        notify_type: Some(protocol::exchange_order_notification::NotifyType::Status as i32),
        is_snapshot: Some(false),
        fcm_id: Some("FCM".to_string()),
        ib_id: Some("IB".to_string()),
        account_id: Some("ACCOUNT_ID_SECRET".to_string()),
        basket_id: Some("BASKET_ID_SECRET_123".to_string()),
        exchange_order_id: Some("ORDER_ID_SECRET_456".to_string()),
        exchange: Some("CME".to_string()),
        symbol: Some("NQU6".to_string()),
        status: Some("OPEN".to_string()),
        transaction_type: Some(protocol::exchange_order_notification::TransactionType::Buy as i32),
        quantity: Some(0),
        ..Default::default()
    })
    .expect("encode invalid quantity fixture");
    decode_order_event(&payload, &expected_account).expect_err("invalid quantity is rejected")
}

pub(crate) fn notification_is_snapshot(payload: &[u8]) -> Result<bool> {
    ensure_template(payload)?;
    let response: protocol::ExchangeOrderNotification = codec::decode(payload)?;
    Ok(response.is_snapshot == Some(true))
}

struct RawOrderQuantities {
    quantity: Option<i32>,
    filled: Option<i32>,
    unfilled: Option<i32>,
}

fn classify_status(
    notify_type: protocol::exchange_order_notification::NotifyType,
    raw_status: Option<&str>,
    quantity: Option<Decimal>,
    cumulative_filled: Option<Decimal>,
    unfilled: Option<Decimal>,
    context: NumericFieldContext,
    raw: RawOrderQuantities,
) -> Result<String> {
    use protocol::exchange_order_notification::NotifyType;
    if let (Some(filled), Some(quantity)) = (cumulative_filled, quantity) {
        if filled > quantity {
            return Err(
                anyhow::anyhow!("Rithmic cumulative fill exceeds order quantity").context(
                    QuantityFieldDiagnostic::new(
                        context.clone(),
                        "total_fill_size",
                        raw.filled,
                        "exceeds_order_quantity",
                    ),
                ),
            );
        }
    }
    if let (Some(unfilled), Some(quantity)) = (unfilled, quantity) {
        if unfilled > quantity {
            return Err(
                anyhow::anyhow!("Rithmic unfilled size exceeds order quantity").context(
                    QuantityFieldDiagnostic::new(
                        context.clone(),
                        "total_unfilled_size",
                        raw.unfilled,
                        "exceeds_order_quantity",
                    ),
                ),
            );
        }
    }
    if quantity.is_none()
        && cumulative_filled.is_some_and(|filled| filled > Decimal::ZERO)
        && matches!(
            notify_type,
            NotifyType::Fill | NotifyType::Cancel | NotifyType::Reject
        )
    {
        return Ok("quantity_unresolved".to_string());
    }
    let status = match notify_type {
        NotifyType::Fill => {
            let quantity = quantity.ok_or_else(|| {
                anyhow::anyhow!("Rithmic fill event omitted order quantity").context(
                    QuantityFieldDiagnostic::new(
                        context.clone(),
                        "quantity",
                        raw.quantity,
                        "missing_required",
                    ),
                )
            })?;
            let filled = cumulative_filled
                .filter(|filled| *filled > Decimal::ZERO)
                .ok_or_else(|| {
                    anyhow::anyhow!("Rithmic fill event omitted positive cumulative fill size")
                        .context(QuantityFieldDiagnostic::new(
                            context.clone(),
                            "total_fill_size",
                            raw.filled,
                            "missing_or_zero_required",
                        ))
                })?;
            if let Some(unfilled) = unfilled {
                if filled + unfilled != quantity {
                    return Err(
                        anyhow::anyhow!("Rithmic fill totals do not match order quantity").context(
                            QuantityFieldDiagnostic::new(
                                context.clone(),
                                "total_fill_size",
                                raw.filled,
                                "fill_totals_mismatch",
                            ),
                        ),
                    );
                }
            }
            if filled == quantity {
                "filled"
            } else {
                "partially_filled"
            }
        }
        NotifyType::Cancel => {
            ensure_terminal_fill_is_incomplete(
                "cancel",
                quantity,
                cumulative_filled,
                &context,
                raw,
            )?;
            "cancelled"
        }
        NotifyType::Reject => {
            ensure_terminal_fill_is_incomplete(
                "reject",
                quantity,
                cumulative_filled,
                &context,
                raw,
            )?;
            "rejected"
        }
        NotifyType::NotCancelled => "cancel_rejected",
        NotifyType::Status | NotifyType::Modify | NotifyType::Trigger | NotifyType::Generic => {
            if raw_status.map(str::trim).is_none_or(str::is_empty)
                && notify_type != NotifyType::Generic
            {
                return classify_fill_progress(quantity, cumulative_filled);
            }
            return normalize_status(raw_status, quantity, cumulative_filled, &context, raw);
        }
        NotifyType::NotModified => "modify_rejected",
    };
    Ok(status.to_string())
}

fn ensure_terminal_fill_is_incomplete(
    notification: &str,
    quantity: Option<Decimal>,
    cumulative_filled: Option<Decimal>,
    context: &NumericFieldContext,
    raw: RawOrderQuantities,
) -> Result<()> {
    if let Some(filled) = cumulative_filled.filter(|filled| *filled > Decimal::ZERO) {
        let quantity = quantity.ok_or_else(|| {
            anyhow::anyhow!("Rithmic {notification} event with fills omitted order quantity")
                .context(QuantityFieldDiagnostic::new(
                    context.clone(),
                    "quantity",
                    raw.quantity,
                    "missing_required",
                ))
        })?;
        if filled >= quantity {
            return Err(anyhow::anyhow!(
                "Rithmic {notification} event conflicts with complete fill"
            )
            .context(QuantityFieldDiagnostic::new(
                context.clone(),
                "total_fill_size",
                raw.filled,
                "complete_fill_conflict",
            )));
        }
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
            let Some(quantity) = quantity else {
                return Ok("quantity_unresolved".to_string());
            };
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
    context: &NumericFieldContext,
    raw: RawOrderQuantities,
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
            Some(filled) if quantity.is_none() && filled > Decimal::ZERO => "quantity_unresolved",
            Some(_) => {
                return Err(
                    anyhow::anyhow!("Rithmic open status conflicts with cumulative fill").context(
                        QuantityFieldDiagnostic::new(
                            context.clone(),
                            "total_fill_size",
                            raw.filled,
                            "open_status_conflict",
                        ),
                    ),
                );
            }
        },
        "partial" | "partially_filled" | "partiallyfilled" => {
            let Some(quantity) = quantity else {
                if !cumulative_filled.is_some_and(|filled| filled > Decimal::ZERO) {
                    return Err(anyhow::anyhow!(
                        "Rithmic partial status conflicts with cumulative fill"
                    )
                    .context(QuantityFieldDiagnostic::new(
                        context.clone(),
                        "total_fill_size",
                        raw.filled,
                        "partial_status_conflict",
                    )));
                }
                return Ok("quantity_unresolved".to_string());
            };
            if !cumulative_filled.is_some_and(|filled| filled > Decimal::ZERO && filled < quantity)
            {
                return Err(anyhow::anyhow!(
                    "Rithmic partial status conflicts with cumulative fill"
                )
                .context(QuantityFieldDiagnostic::new(
                    context.clone(),
                    "total_fill_size",
                    raw.filled,
                    "partial_status_conflict",
                )));
            }
            "partially_filled"
        }
        "complete" | "completed" | "filled" => {
            let Some(quantity) = quantity else {
                if !cumulative_filled.is_some_and(|filled| filled > Decimal::ZERO) {
                    return Err(
                        anyhow::anyhow!("Rithmic complete status is not fully filled").context(
                            QuantityFieldDiagnostic::new(
                                context.clone(),
                                "total_fill_size",
                                raw.filled,
                                "complete_status_not_filled",
                            ),
                        ),
                    );
                }
                return Ok("quantity_unresolved".to_string());
            };
            if cumulative_filled != Some(quantity) {
                return Err(
                    anyhow::anyhow!("Rithmic complete status is not fully filled").context(
                        QuantityFieldDiagnostic::new(
                            context.clone(),
                            "total_fill_size",
                            raw.filled,
                            "complete_status_not_filled",
                        ),
                    ),
                );
            }
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
