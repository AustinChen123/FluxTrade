use super::{
    codec, protocol,
    session::{classify_response_codes, ensure_success, ResponseDisposition},
};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

const LOGIN_INFO_REQUEST: i32 = 300;
const LOGIN_INFO_RESPONSE: i32 = 301;
const ACCOUNT_LIST_REQUEST: i32 = 302;
const ACCOUNT_LIST_RESPONSE: i32 = 303;
const SHOW_ORDERS_REQUEST: i32 = 320;
const SHOW_ORDERS_RESPONSE: i32 = 321;
const SHOW_ORDER_HISTORY_REQUEST: i32 = 322;
const SHOW_ORDER_HISTORY_RESPONSE: i32 = 323;
const RITHMIC_ORDER_NOTIFICATION: i32 = 351;
const EXCHANGE_ORDER_NOTIFICATION: i32 = 352;
const SHOW_FILL_HISTORY_REQUEST: i32 = 3512;
const SHOW_FILL_HISTORY_RESPONSE: i32 = 3513;
const PNL_POSITION_SNAPSHOT_REQUEST: i32 = 402;
const PNL_POSITION_SNAPSHOT_RESPONSE: i32 = 403;
const INSTRUMENT_PNL_POSITION_UPDATE: i32 = 450;
const ACCOUNT_PNL_POSITION_UPDATE: i32 = 451;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UserType {
    Admin,
    Fcm,
    Ib,
    Trader,
}

impl UserType {
    fn from_protocol(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Admin),
            1 => Ok(Self::Fcm),
            2 => Ok(Self::Ib),
            3 => Ok(Self::Trader),
            _ => anyhow::bail!("unknown Rithmic user type"),
        }
    }

    fn account_list_value(self) -> Result<i32> {
        match self {
            Self::Fcm => Ok(1),
            Self::Ib => Ok(2),
            Self::Trader => Ok(3),
            Self::Admin => anyhow::bail!("Rithmic admin cannot request a trading account list"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LoginInfo {
    pub(crate) fcm_id: String,
    pub(crate) ib_id: String,
    pub(crate) user_type: UserType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountIdentity {
    pub(crate) fcm_id: String,
    pub(crate) ib_id: String,
    pub(crate) account_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Account {
    pub(crate) identity: AccountIdentity,
    pub(crate) name: Option<String>,
    pub(crate) currency: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AccountListEvent {
    Account(Account),
    Completed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransactionType {
    Buy,
    Sell,
    ShortSell,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OrderSnapshot {
    pub(crate) account: AccountIdentity,
    pub(crate) client_order_id: Option<String>,
    pub(crate) basket_id: String,
    pub(crate) original_basket_id: Option<String>,
    pub(crate) linked_basket_ids: Option<String>,
    pub(crate) exchange_order_id: Option<String>,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) status: String,
    pub(crate) notification_type: Option<String>,
    pub(crate) completion_reason: Option<String>,
    pub(crate) report_text: Option<String>,
    pub(crate) transaction_type: TransactionType,
    pub(crate) quantity: Decimal,
    pub(crate) price: Option<Decimal>,
    pub(crate) trigger_price: Option<Decimal>,
    pub(crate) price_type: Option<String>,
    pub(crate) bracket_type: Option<String>,
    pub(crate) filled_quantity: Option<Decimal>,
    pub(crate) unfilled_quantity: Option<Decimal>,
    pub(crate) average_fill_price: Option<Decimal>,
    pub(crate) timestamp_ms: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OrderSnapshotEvent {
    Snapshot(Box<OrderSnapshot>),
    RequestCompleted,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OrderHistoryEvent {
    Notification(Box<OrderSnapshot>),
    RequestCompleted,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FillSnapshot {
    pub(crate) account: AccountIdentity,
    pub(crate) basket_id: String,
    pub(crate) exchange_order_id: Option<String>,
    pub(crate) fill_id: String,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) transaction_type: String,
    pub(crate) fill_quantity: Decimal,
    pub(crate) fill_price: Decimal,
    pub(crate) cumulative_filled_quantity: Option<Decimal>,
    pub(crate) cumulative_average_price: Option<Decimal>,
    pub(crate) timestamp_ms: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FillHistoryEvent {
    Fill(Box<FillSnapshot>),
    RequestCompleted,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InstrumentPositionSnapshot {
    pub(crate) account: AccountIdentity,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) net_quantity: i32,
    pub(crate) average_open_fill_price: Option<Decimal>,
    pub(crate) open_pnl: Option<Decimal>,
    pub(crate) day_pnl: Option<Decimal>,
    pub(crate) timestamp_ms: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AccountSummarySnapshot {
    pub(crate) account: AccountIdentity,
    pub(crate) account_balance: Option<Decimal>,
    pub(crate) cash_on_hand: Option<Decimal>,
    pub(crate) available_buying_power: Option<Decimal>,
    pub(crate) day_pnl: Option<Decimal>,
    pub(crate) net_quantity: Option<i32>,
    pub(crate) timestamp_ms: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PnlSnapshotEvent {
    Instrument(InstrumentPositionSnapshot),
    Account(AccountSummarySnapshot),
    RequestCompleted,
}

pub(crate) fn login_info_request(request_key: &str) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    codec::encode(&protocol::RequestLoginInfo {
        template_id: LOGIN_INFO_REQUEST,
        user_msg: vec![request_key.to_string()],
    })
}

pub(crate) fn decode_login_info(payload: &[u8], request_key: &str) -> Result<LoginInfo> {
    validate_request_key(request_key)?;
    ensure_template(payload, LOGIN_INFO_RESPONSE)?;
    let response: protocol::ResponseLoginInfo = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    ensure_success(&response.rp_code)?;

    Ok(LoginInfo {
        fcm_id: required_text(response.fcm_id, "fcm_id")?,
        ib_id: required_text(response.ib_id, "ib_id")?,
        user_type: UserType::from_protocol(
            response.user_type.context("missing Rithmic user type")?,
        )?,
    })
}

pub(crate) fn account_list_request(request_key: &str, login_info: &LoginInfo) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    let fcm_id = required_text(Some(login_info.fcm_id.clone()), "fcm_id")?;
    let ib_id = required_text(Some(login_info.ib_id.clone()), "ib_id")?;
    codec::encode(&protocol::RequestAccountList {
        template_id: ACCOUNT_LIST_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(fcm_id),
        ib_id: Some(ib_id),
        user_type: Some(login_info.user_type.account_list_value()?),
    })
}

pub(crate) fn decode_account_list_event(
    payload: &[u8],
    request_key: &str,
) -> Result<AccountListEvent> {
    validate_request_key(request_key)?;
    ensure_template(payload, ACCOUNT_LIST_RESPONSE)?;
    let response: protocol::ResponseAccountList = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;

    match classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)? {
        ResponseDisposition::Succeeded => return Ok(AccountListEvent::Completed),
        ResponseDisposition::Failed(codes) => {
            anyhow::bail!("Rithmic account-list response failed: {}", codes.join(","))
        }
        ResponseDisposition::Processing => {}
    }

    Ok(AccountListEvent::Account(Account {
        identity: AccountIdentity {
            fcm_id: required_text(response.fcm_id, "fcm_id")?,
            ib_id: required_text(response.ib_id, "ib_id")?,
            account_id: required_text(response.account_id, "account_id")?,
        },
        name: optional_text(response.account_name),
        currency: optional_text(response.account_currency),
    }))
}

pub(crate) fn show_orders_request(request_key: &str, account: &AccountIdentity) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    codec::encode(&protocol::RequestShowOrders {
        template_id: SHOW_ORDERS_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
    })
}

pub(crate) fn decode_order_snapshot_event(
    payload: &[u8],
    request_key: &str,
    expected_account: &AccountIdentity,
) -> Result<OrderSnapshotEvent> {
    validate_request_key(request_key)?;
    validate_account(expected_account)?;
    match codec::template_id(payload)? {
        SHOW_ORDERS_RESPONSE => {
            let response: protocol::ResponseShowOrders = codec::decode(payload)?;
            ensure_request_key(&response.user_msg, request_key)?;
            ensure_success(&response.rp_code)?;
            Ok(OrderSnapshotEvent::RequestCompleted)
        }
        EXCHANGE_ORDER_NOTIFICATION => {
            let response: protocol::ExchangeOrderNotification = codec::decode(payload)?;
            Ok(OrderSnapshotEvent::Snapshot(Box::new(
                order_snapshot_from_notification(response, expected_account, true)?,
            )))
        }
        template_id => anyhow::bail!("unsupported Rithmic order snapshot template {template_id}"),
    }
}

pub(crate) fn is_order_snapshot_response(template_id: i32) -> bool {
    template_id == SHOW_ORDERS_RESPONSE
}

fn order_snapshot_from_notification(
    response: protocol::ExchangeOrderNotification,
    expected_account: &AccountIdentity,
    require_snapshot: bool,
) -> Result<OrderSnapshot> {
    if require_snapshot {
        ensure!(
            response.is_snapshot == Some(true),
            "Rithmic order response is not a snapshot"
        );
    }
    let account = account_identity(response.fcm_id, response.ib_id, response.account_id)?;
    ensure!(
        account == *expected_account,
        "Rithmic order response account mismatch"
    );
    Ok(OrderSnapshot {
        account,
        client_order_id: optional_text(response.user_tag),
        basket_id: required_text(response.basket_id, "basket_id")?,
        original_basket_id: optional_text(response.original_basket_id),
        linked_basket_ids: optional_text(response.linked_basket_ids),
        exchange_order_id: optional_text(response.exchange_order_id),
        exchange: required_text(response.exchange, "exchange")?,
        symbol: required_text(response.symbol, "symbol")?,
        status: required_text(response.status, "status")?,
        notification_type: response
            .notify_type
            .map(exchange_notification_type)
            .transpose()?,
        completion_reason: None,
        report_text: optional_text(response.report_text),
        transaction_type: transaction_type(response.transaction_type)?,
        quantity: positive_quantity(response.quantity, "order quantity")?,
        price: optional_nonnegative_decimal(response.price, "order price")?,
        trigger_price: optional_nonnegative_decimal(response.trigger_price, "trigger price")?,
        price_type: exchange_price_type(response.price_type)?,
        bracket_type: exchange_bracket_type(response.bracket_type)?,
        filled_quantity: optional_quantity(response.total_fill_size, "total_fill_size")?,
        unfilled_quantity: optional_quantity(response.total_unfilled_size, "total_unfilled_size")?,
        average_fill_price: optional_nonnegative_decimal(
            response.avg_fill_price,
            "average fill price",
        )?,
        timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
    })
}

pub(crate) fn show_order_history_request(
    request_key: &str,
    account: &AccountIdentity,
    basket_id: &str,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let basket_id = required_text(Some(basket_id.to_string()), "basket_id")?;
    codec::encode(&protocol::RequestShowOrderHistory {
        template_id: SHOW_ORDER_HISTORY_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        basket_id: Some(basket_id),
    })
}

pub(crate) fn decode_order_history_event(
    payload: &[u8],
    request_key: &str,
    expected_account: &AccountIdentity,
    expected_basket_id: &str,
) -> Result<OrderHistoryEvent> {
    validate_request_key(request_key)?;
    validate_account(expected_account)?;
    match codec::template_id(payload)? {
        SHOW_ORDER_HISTORY_RESPONSE => {
            let response: protocol::ResponseShowOrderHistory = codec::decode(payload)?;
            ensure_request_key(&response.user_msg, request_key)?;
            ensure_success(&response.rp_code)?;
            Ok(OrderHistoryEvent::RequestCompleted)
        }
        EXCHANGE_ORDER_NOTIFICATION => {
            let response: protocol::ExchangeOrderNotification = codec::decode(payload)?;
            let order = order_snapshot_from_notification(response, expected_account, false)?;
            ensure!(
                order.basket_id == expected_basket_id,
                "Rithmic order-history basket ID mismatch"
            );
            Ok(OrderHistoryEvent::Notification(Box::new(order)))
        }
        RITHMIC_ORDER_NOTIFICATION => {
            let response: protocol::RithmicOrderNotification = codec::decode(payload)?;
            let order = rithmic_order_snapshot(response, expected_account)?;
            ensure!(
                order.basket_id == expected_basket_id,
                "Rithmic order-history basket ID mismatch"
            );
            Ok(OrderHistoryEvent::Notification(Box::new(order)))
        }
        template_id => anyhow::bail!("unsupported Rithmic order-history template {template_id}"),
    }
}

fn rithmic_order_snapshot(
    response: protocol::RithmicOrderNotification,
    expected_account: &AccountIdentity,
) -> Result<OrderSnapshot> {
    let account = account_identity(response.fcm_id, response.ib_id, response.account_id)?;
    ensure!(
        account == *expected_account,
        "Rithmic order-history account mismatch"
    );
    Ok(OrderSnapshot {
        account,
        client_order_id: optional_text(response.user_tag),
        basket_id: required_text(response.basket_id, "basket_id")?,
        original_basket_id: optional_text(response.original_basket_id),
        linked_basket_ids: optional_text(response.linked_basket_ids),
        exchange_order_id: optional_text(response.exchange_order_id),
        exchange: required_text(response.exchange, "exchange")?,
        symbol: required_text(response.symbol, "symbol")?,
        status: required_text(response.status, "status")?,
        notification_type: Some(rithmic_notification_type(
            response
                .notify_type
                .context("missing Rithmic order-history notify type")?,
        )?),
        completion_reason: optional_text(response.completion_reason),
        report_text: optional_text(response.report_text),
        transaction_type: transaction_type_value(response.transaction_type)?,
        quantity: positive_quantity(response.quantity, "order quantity")?,
        price: optional_nonnegative_decimal(response.price, "order price")?,
        trigger_price: optional_nonnegative_decimal(response.trigger_price, "trigger price")?,
        price_type: rithmic_price_type(response.price_type)?,
        bracket_type: rithmic_bracket_type(response.bracket_type)?,
        filled_quantity: optional_quantity(response.total_fill_size, "total_fill_size")?,
        unfilled_quantity: optional_quantity(response.total_unfilled_size, "total_unfilled_size")?,
        average_fill_price: optional_nonnegative_decimal(
            response.avg_fill_price,
            "average fill price",
        )?,
        timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
    })
}

pub(crate) fn show_fill_history_request(
    request_key: &str,
    account: &AccountIdentity,
    start_index: i32,
    finish_index: i32,
    max_record_count: i32,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    ensure!(start_index >= 0, "invalid Rithmic fill-history start index");
    ensure!(
        finish_index >= start_index,
        "invalid Rithmic fill-history finish index"
    );
    ensure!(
        (1..=10_000).contains(&max_record_count),
        "invalid Rithmic fill-history max record count"
    );
    codec::encode(&protocol::RequestShowFillHistory {
        template_id: SHOW_FILL_HISTORY_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        index_format: Some("ssboe".to_string()),
        start_index: Some(start_index),
        finish_index: Some(finish_index),
        max_record_count: Some(max_record_count),
    })
}

pub(crate) fn decode_fill_history_event(
    payload: &[u8],
    request_key: &str,
    expected_account: &AccountIdentity,
) -> Result<FillHistoryEvent> {
    validate_request_key(request_key)?;
    validate_account(expected_account)?;
    ensure_template(payload, SHOW_FILL_HISTORY_RESPONSE)?;
    let response: protocol::ResponseShowFillHistory = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;

    match classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)? {
        ResponseDisposition::Succeeded => return Ok(FillHistoryEvent::RequestCompleted),
        ResponseDisposition::Failed(codes) => {
            anyhow::bail!("Rithmic fill-history response failed: {}", codes.join(","))
        }
        ResponseDisposition::Processing => {}
    }
    let account = account_identity(response.fcm_id, response.ib_id, response.account_id)?;
    ensure!(
        account == *expected_account,
        "Rithmic fill-history account mismatch"
    );
    let fill_quantity = positive_u64_quantity(response.fill_size, "fill_size")?;
    let fill_price = required_nonnegative_decimal(response.fill_price, "fill price")?;
    Ok(FillHistoryEvent::Fill(Box::new(FillSnapshot {
        account,
        basket_id: required_text(response.basket_id, "basket_id")?,
        exchange_order_id: optional_text(response.exchange_order_id),
        fill_id: required_text(response.fill_id, "fill_id")?,
        exchange: required_text(response.exchange, "exchange")?,
        symbol: required_text(response.symbol, "symbol")?,
        transaction_type: required_text(response.transaction_type, "transaction_type")?,
        fill_quantity,
        fill_price,
        cumulative_filled_quantity: optional_u64_quantity(
            response.total_fill_size,
            "total_fill_size",
        )?,
        cumulative_average_price: optional_nonnegative_decimal(
            response.avg_fill_price,
            "average fill price",
        )?,
        timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
    })))
}

pub(crate) fn pnl_position_snapshot_request(
    request_key: &str,
    account: &AccountIdentity,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    codec::encode(&protocol::RequestPnLPositionSnapshot {
        template_id: PNL_POSITION_SNAPSHOT_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
    })
}

pub(crate) fn decode_pnl_snapshot_event(
    payload: &[u8],
    request_key: &str,
    expected_account: &AccountIdentity,
) -> Result<PnlSnapshotEvent> {
    validate_request_key(request_key)?;
    validate_account(expected_account)?;
    match codec::template_id(payload)? {
        PNL_POSITION_SNAPSHOT_RESPONSE => {
            let response: protocol::ResponsePnLPositionSnapshot = codec::decode(payload)?;
            ensure_request_key(&response.user_msg, request_key)?;
            ensure_success(&response.rp_code)?;
            Ok(PnlSnapshotEvent::RequestCompleted)
        }
        INSTRUMENT_PNL_POSITION_UPDATE => {
            let response: protocol::InstrumentPnLPositionUpdate = codec::decode(payload)?;
            ensure_snapshot(response.is_snapshot, "instrument PnL")?;
            let account = account_identity(response.fcm_id, response.ib_id, response.account_id)?;
            ensure!(
                account == *expected_account,
                "Rithmic instrument position account mismatch"
            );
            Ok(PnlSnapshotEvent::Instrument(InstrumentPositionSnapshot {
                account,
                exchange: required_text(response.exchange, "exchange")?,
                symbol: required_text(response.symbol, "symbol")?,
                net_quantity: response
                    .net_quantity
                    .context("missing Rithmic net_quantity")?,
                average_open_fill_price: optional_nonnegative_decimal(
                    response.avg_open_fill_price,
                    "average open fill price",
                )?,
                open_pnl: optional_decimal_text(response.open_position_pnl, "open PnL")?,
                day_pnl: optional_decimal(response.day_pnl, "day PnL")?,
                timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
            }))
        }
        ACCOUNT_PNL_POSITION_UPDATE => {
            let response: protocol::AccountPnLPositionUpdate = codec::decode(payload)?;
            ensure_snapshot(response.is_snapshot, "account PnL")?;
            let account = account_identity(response.fcm_id, response.ib_id, response.account_id)?;
            ensure!(
                account == *expected_account,
                "Rithmic account summary account mismatch"
            );
            Ok(PnlSnapshotEvent::Account(AccountSummarySnapshot {
                account,
                account_balance: optional_decimal_text(
                    response.account_balance,
                    "account balance",
                )?,
                cash_on_hand: optional_decimal_text(response.cash_on_hand, "cash on hand")?,
                available_buying_power: optional_decimal_text(
                    response.available_buying_power,
                    "available buying power",
                )?,
                day_pnl: optional_decimal_text(response.day_pnl, "day PnL")?,
                net_quantity: response.net_quantity,
                timestamp_ms: optional_epoch_millis(response.ssboe, response.usecs)?,
            }))
        }
        template_id => anyhow::bail!("unsupported Rithmic PnL snapshot template {template_id}"),
    }
}

fn validate_request_key(request_key: &str) -> Result<()> {
    ensure!(
        !request_key.trim().is_empty(),
        "Rithmic ledger request key must not be empty"
    );
    Ok(())
}

fn ensure_request_key(user_msg: &[String], request_key: &str) -> Result<()> {
    ensure!(
        user_msg.first().is_some_and(|value| value == request_key),
        "Rithmic ledger response request key mismatch"
    );
    Ok(())
}

fn ensure_template(payload: &[u8], expected: i32) -> Result<()> {
    ensure!(
        codec::template_id(payload)? == expected,
        "unexpected Rithmic ledger response template"
    );
    Ok(())
}

fn ensure_snapshot(is_snapshot: Option<bool>, response: &str) -> Result<()> {
    ensure!(
        is_snapshot == Some(true),
        "Rithmic {response} response is not a snapshot"
    );
    Ok(())
}

fn validate_account(account: &AccountIdentity) -> Result<()> {
    required_text(Some(account.fcm_id.clone()), "fcm_id")?;
    required_text(Some(account.ib_id.clone()), "ib_id")?;
    required_text(Some(account.account_id.clone()), "account_id")?;
    Ok(())
}

fn account_identity(
    fcm_id: Option<String>,
    ib_id: Option<String>,
    account_id: Option<String>,
) -> Result<AccountIdentity> {
    Ok(AccountIdentity {
        fcm_id: required_text(fcm_id, "fcm_id")?,
        ib_id: required_text(ib_id, "ib_id")?,
        account_id: required_text(account_id, "account_id")?,
    })
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic ledger {field}"))
}

fn optional_text(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn transaction_type(value: Option<i32>) -> Result<TransactionType> {
    transaction_type_value(value)
}

fn transaction_type_value(value: Option<i32>) -> Result<TransactionType> {
    match value {
        Some(1) => Ok(TransactionType::Buy),
        Some(2) => Ok(TransactionType::Sell),
        Some(3) => Ok(TransactionType::ShortSell),
        Some(_) => anyhow::bail!("unknown Rithmic order transaction type"),
        None => anyhow::bail!("missing Rithmic order transaction type"),
    }
}

fn exchange_price_type(value: Option<i32>) -> Result<Option<String>> {
    value
        .map(|value| {
            protocol::exchange_order_notification::PriceType::try_from(value)
                .context("unknown Rithmic exchange-order price type")
                .map(|value| value.as_str_name().to_ascii_lowercase())
        })
        .transpose()
}

fn rithmic_price_type(value: Option<i32>) -> Result<Option<String>> {
    value
        .map(|value| {
            protocol::rithmic_order_notification::PriceType::try_from(value)
                .context("unknown Rithmic order-history price type")
                .map(|value| value.as_str_name().to_ascii_lowercase())
        })
        .transpose()
}

fn exchange_bracket_type(value: Option<i32>) -> Result<Option<String>> {
    value
        .map(|value| {
            protocol::exchange_order_notification::BracketType::try_from(value)
                .context("unknown Rithmic exchange-order bracket type")
                .map(|value| value.as_str_name().to_ascii_lowercase())
        })
        .transpose()
}

fn rithmic_bracket_type(value: Option<i32>) -> Result<Option<String>> {
    value
        .map(|value| {
            protocol::rithmic_order_notification::BracketType::try_from(value)
                .context("unknown Rithmic order-history bracket type")
                .map(|value| value.as_str_name().to_ascii_lowercase())
        })
        .transpose()
}

fn exchange_notification_type(value: i32) -> Result<String> {
    protocol::exchange_order_notification::NotifyType::try_from(value)
        .context("unknown Rithmic exchange-order notify type")
        .map(|value| value.as_str_name().to_string())
}

fn rithmic_notification_type(value: i32) -> Result<String> {
    protocol::rithmic_order_notification::NotifyType::try_from(value)
        .context("unknown Rithmic order-history notify type")
        .map(|value| value.as_str_name().to_string())
}

fn positive_quantity(value: Option<i32>, field: &str) -> Result<Decimal> {
    let value = value.with_context(|| format!("missing Rithmic {field}"))?;
    ensure!(value > 0, "invalid Rithmic {field}");
    Ok(Decimal::from(value))
}

fn optional_quantity(value: Option<i32>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(|value| {
            ensure!(value >= 0, "invalid Rithmic {field}");
            Ok(Decimal::from(value))
        })
        .transpose()
}

fn positive_u64_quantity(value: Option<u64>, field: &str) -> Result<Decimal> {
    let value = value.with_context(|| format!("missing Rithmic {field}"))?;
    ensure!(value > 0, "invalid Rithmic {field}");
    Ok(Decimal::from(value))
}

fn optional_u64_quantity(value: Option<u64>, _field: &str) -> Result<Option<Decimal>> {
    Ok(value.map(Decimal::from))
}

fn required_nonnegative_decimal(value: Option<f64>, field: &str) -> Result<Decimal> {
    optional_nonnegative_decimal(value, field)?.with_context(|| format!("missing Rithmic {field}"))
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

fn optional_decimal(value: Option<f64>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(|value| {
            ensure!(value.is_finite(), "invalid Rithmic {field}");
            Decimal::from_str(&value.to_string())
                .with_context(|| format!("invalid Rithmic {field}"))
        })
        .transpose()
}

fn optional_decimal_text(value: Option<String>, field: &str) -> Result<Option<Decimal>> {
    optional_text(value)
        .map(|value| Decimal::from_str(&value).with_context(|| format!("invalid Rithmic {field}")))
        .transpose()
}

fn optional_epoch_millis(ssboe: Option<i32>, usecs: Option<i32>) -> Result<Option<i64>> {
    match (ssboe, usecs) {
        (None, None) => Ok(None),
        (Some(ssboe), usecs) => {
            ensure!(ssboe >= 0, "invalid Rithmic ledger ssboe");
            let usecs = usecs.unwrap_or_default();
            ensure!(
                (0..1_000_000).contains(&usecs),
                "invalid Rithmic ledger usecs"
            );
            Ok(Some(i64::from(ssboe) * 1_000 + i64::from(usecs) / 1_000))
        }
        (None, Some(_)) => anyhow::bail!("Rithmic ledger usecs requires ssboe"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn account() -> AccountIdentity {
        AccountIdentity {
            fcm_id: "FCM".to_string(),
            ib_id: "IB".to_string(),
            account_id: "ACCOUNT".to_string(),
        }
    }

    #[test]
    fn read_only_request_templates_and_fields_are_exact() {
        let login_payload = login_info_request("ledger").unwrap();
        let login: protocol::RequestLoginInfo = codec::decode(&login_payload).unwrap();
        assert_eq!(login.template_id, LOGIN_INFO_REQUEST);
        assert_eq!(login.user_msg, ["ledger"]);

        let account_payload = account_list_request(
            "ledger",
            &LoginInfo {
                fcm_id: "FCM".to_string(),
                ib_id: "IB".to_string(),
                user_type: UserType::Trader,
            },
        )
        .unwrap();
        let account_request: protocol::RequestAccountList =
            codec::decode(&account_payload).unwrap();
        assert_eq!(account_request.template_id, ACCOUNT_LIST_REQUEST);
        assert_eq!(account_request.user_type, Some(3));

        let order_payload = show_orders_request("orders", &account()).unwrap();
        let order_request: protocol::RequestShowOrders = codec::decode(&order_payload).unwrap();
        assert_eq!(order_request.template_id, SHOW_ORDERS_REQUEST);
        assert_eq!(order_request.account_id.as_deref(), Some("ACCOUNT"));

        let history_payload =
            show_order_history_request("history", &account(), "basket-1").unwrap();
        let history_request: protocol::RequestShowOrderHistory =
            codec::decode(&history_payload).unwrap();
        assert_eq!(history_request.template_id, SHOW_ORDER_HISTORY_REQUEST);
        assert_eq!(history_request.basket_id.as_deref(), Some("basket-1"));

        let fills_payload =
            show_fill_history_request("fills", &account(), 1_700_000_000, 1_700_000_100, 10_000)
                .unwrap();
        let fills_request: protocol::RequestShowFillHistory =
            codec::decode(&fills_payload).unwrap();
        assert_eq!(fills_request.template_id, SHOW_FILL_HISTORY_REQUEST);
        assert_eq!(fills_request.index_format.as_deref(), Some("ssboe"));
        assert_eq!(fills_request.max_record_count, Some(10_000));

        let pnl_payload = pnl_position_snapshot_request("pnl", &account()).unwrap();
        let pnl_request: protocol::RequestPnLPositionSnapshot =
            codec::decode(&pnl_payload).unwrap();
        assert_eq!(pnl_request.template_id, PNL_POSITION_SNAPSHOT_REQUEST);
        assert_eq!(pnl_request.account_id.as_deref(), Some("ACCOUNT"));
    }

    #[test]
    fn order_history_requires_matching_basket_and_explicit_completion() {
        let notification = codec::encode(&protocol::ExchangeOrderNotification {
            template_id: EXCHANGE_ORDER_NOTIFICATION,
            is_snapshot: Some(false),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some("basket-1".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            status: Some("CANCELLED".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            total_fill_size: Some(0),
            total_unfilled_size: Some(0),
            ..Default::default()
        })
        .unwrap();
        let OrderHistoryEvent::Notification(order) =
            decode_order_history_event(&notification, "history", &account(), "basket-1").unwrap()
        else {
            panic!("expected order-history notification");
        };
        assert_eq!(order.status, "CANCELLED");
        assert!(
            decode_order_history_event(&notification, "history", &account(), "other-basket",)
                .is_err()
        );

        let completed = codec::encode(&protocol::ResponseShowOrderHistory {
            template_id: SHOW_ORDER_HISTORY_RESPONSE,
            user_msg: vec!["history".to_string()],
            rp_code: vec!["0".to_string()],
        })
        .unwrap();
        assert_eq!(
            decode_order_history_event(&completed, "history", &account(), "basket-1").unwrap(),
            OrderHistoryEvent::RequestCompleted
        );
    }

    #[test]
    fn order_notification_type_matrices_are_complete() {
        let rithmic_names = [
            "ORDER_RCVD_FROM_CLNT",
            "MODIFY_RCVD_FROM_CLNT",
            "CANCEL_RCVD_FROM_CLNT",
            "OPEN_PENDING",
            "MODIFY_PENDING",
            "CANCEL_PENDING",
            "ORDER_RCVD_BY_EXCH_GTWY",
            "MODIFY_RCVD_BY_EXCH_GTWY",
            "CANCEL_RCVD_BY_EXCH_GTWY",
            "ORDER_SENT_TO_EXCH",
            "MODIFY_SENT_TO_EXCH",
            "CANCEL_SENT_TO_EXCH",
            "OPEN",
            "MODIFIED",
            "COMPLETE",
            "MODIFICATION_FAILED",
            "CANCELLATION_FAILED",
            "TRIGGER_PENDING",
            "GENERIC",
            "LINK_ORDERS_FAILED",
        ];
        for (notify_type, expected) in (1..=20).zip(rithmic_names) {
            let payload = codec::encode(&protocol::RithmicOrderNotification {
                template_id: RITHMIC_ORDER_NOTIFICATION,
                notify_type: Some(notify_type),
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                basket_id: Some("basket-1".to_string()),
                exchange: Some("CME".to_string()),
                symbol: Some("NQU6".to_string()),
                status: Some("COMPLETE".to_string()),
                completion_reason: Some("reason".to_string()),
                report_text: Some("report".to_string()),
                transaction_type: Some(1),
                quantity: Some(1),
                price: Some(20_001.25),
                trigger_price: Some(19_999.25),
                price_type: Some(
                    protocol::rithmic_order_notification::PriceType::StopMarket as i32,
                ),
                bracket_type: Some(
                    protocol::rithmic_order_notification::BracketType::StopOnlyStatic as i32,
                ),
                total_fill_size: Some(0),
                total_unfilled_size: Some(0),
                ..Default::default()
            })
            .unwrap();
            let OrderHistoryEvent::Notification(order) =
                decode_order_history_event(&payload, "history", &account(), "basket-1").unwrap()
            else {
                panic!("expected order-history notification");
            };
            assert_eq!(order.notification_type.as_deref(), Some(expected));
            assert_eq!(order.status, "COMPLETE");
            assert_eq!(order.completion_reason.as_deref(), Some("reason"));
            assert_eq!(order.report_text.as_deref(), Some("report"));
            assert_eq!(order.price, Some(dec!(20001.25)));
            assert_eq!(order.trigger_price, Some(dec!(19999.25)));
            assert_eq!(order.price_type.as_deref(), Some("stop_market"));
            assert_eq!(order.bracket_type.as_deref(), Some("stop_only_static"));
        }

        for (notify_type, expected) in [
            "STATUS",
            "MODIFY",
            "CANCEL",
            "TRIGGER",
            "FILL",
            "REJECT",
            "NOT_MODIFIED",
            "NOT_CANCELLED",
            "GENERIC",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                exchange_notification_type((notify_type + 1) as i32).unwrap(),
                expected
            );
        }
        assert!(rithmic_notification_type(21).is_err());
        assert!(exchange_notification_type(10).is_err());
    }

    #[test]
    fn fill_history_follows_processing_then_completion_semantics() {
        let fill = codec::encode(&protocol::ResponseShowFillHistory {
            template_id: SHOW_FILL_HISTORY_RESPONSE,
            user_msg: vec!["fills".to_string()],
            rq_handler_rp_code: vec!["0".to_string()],
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some("basket-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            fill_id: Some("fill-1".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            transaction_type: Some("BUY".to_string()),
            fill_size: Some(1),
            fill_price: Some(20_000.25),
            total_fill_size: Some(1),
            avg_fill_price: Some(20_000.25),
            ssboe: Some(1_700_000_000),
            usecs: Some(123_000),
            ..Default::default()
        })
        .unwrap();
        let FillHistoryEvent::Fill(fill) =
            decode_fill_history_event(&fill, "fills", &account()).unwrap()
        else {
            panic!("expected fill-history row");
        };
        assert_eq!(fill.fill_quantity, dec!(1));
        assert_eq!(fill.fill_price, dec!(20000.25));
        assert_eq!(fill.timestamp_ms, Some(1_700_000_000_123));

        let completed = codec::encode(&protocol::ResponseShowFillHistory {
            template_id: SHOW_FILL_HISTORY_RESPONSE,
            user_msg: vec!["fills".to_string()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_fill_history_event(&completed, "fills", &account()).unwrap(),
            FillHistoryEvent::RequestCompleted
        );
    }

    #[test]
    fn recovery_request_boundary_matrix_fails_closed() {
        for (start, finish, max_count, succeeds) in [
            (0, 0, 1, true),
            (1, 2, 10_000, true),
            (-1, 2, 1, false),
            (2, 1, 1, false),
            (1, 2, 0, false),
            (1, 2, 10_001, false),
        ] {
            assert_eq!(
                show_fill_history_request("fills", &account(), start, finish, max_count).is_ok(),
                succeeds
            );
        }
        assert!(show_order_history_request("history", &account(), " ").is_err());
    }

    #[test]
    fn account_discovery_follows_the_documented_n_plus_one_response_pattern() {
        let login = codec::encode(&protocol::ResponseLoginInfo {
            template_id: LOGIN_INFO_RESPONSE,
            user_msg: vec!["ledger".to_string()],
            rp_code: vec!["0".to_string()],
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            user_type: Some(3),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_login_info(&login, "ledger").unwrap(),
            LoginInfo {
                fcm_id: "FCM".to_string(),
                ib_id: "IB".to_string(),
                user_type: UserType::Trader,
            }
        );

        let item = codec::encode(&protocol::ResponseAccountList {
            template_id: ACCOUNT_LIST_RESPONSE,
            user_msg: vec!["ledger".to_string()],
            rq_handler_rp_code: vec!["0".to_string()],
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            account_name: Some("Primary".to_string()),
            account_currency: Some("USD".to_string()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_account_list_event(&item, "ledger").unwrap(),
            AccountListEvent::Account(Account {
                identity: account(),
                name: Some("Primary".to_string()),
                currency: Some("USD".to_string()),
            })
        );

        let end = codec::encode(&protocol::ResponseAccountList {
            template_id: ACCOUNT_LIST_RESPONSE,
            user_msg: vec!["ledger".to_string()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_account_list_event(&end, "ledger").unwrap(),
            AccountListEvent::Completed
        );
    }

    #[test]
    fn working_order_snapshot_preserves_external_orders_and_decimal_values() {
        let payload = codec::encode(&protocol::ExchangeOrderNotification {
            template_id: EXCHANGE_ORDER_NOTIFICATION,
            is_snapshot: Some(true),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some("basket-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("MNQU6".to_string()),
            status: Some("OPEN".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(2),
            price: Some(21_001.25),
            trigger_price: Some(20_999.25),
            price_type: Some(protocol::exchange_order_notification::PriceType::StopMarket as i32),
            bracket_type: Some(
                protocol::exchange_order_notification::BracketType::StopOnlyStatic as i32,
            ),
            total_fill_size: Some(1),
            total_unfilled_size: Some(1),
            avg_fill_price: Some(21_000.25),
            ssboe: Some(1_800_000_000),
            usecs: Some(123_000),
            ..Default::default()
        })
        .unwrap();

        let OrderSnapshotEvent::Snapshot(snapshot) =
            decode_order_snapshot_event(&payload, "orders", &account()).unwrap()
        else {
            panic!("expected order snapshot");
        };
        assert_eq!(snapshot.client_order_id, None);
        assert_eq!(snapshot.quantity, dec!(2));
        assert_eq!(snapshot.price, Some(dec!(21001.25)));
        assert_eq!(snapshot.trigger_price, Some(dec!(20999.25)));
        assert_eq!(snapshot.price_type.as_deref(), Some("stop_market"));
        assert_eq!(snapshot.bracket_type.as_deref(), Some("stop_only_static"));
        assert_eq!(snapshot.filled_quantity, Some(dec!(1)));
        assert_eq!(snapshot.unfilled_quantity, Some(dec!(1)));
        assert_eq!(snapshot.average_fill_price, Some(dec!(21000.25)));
        assert_eq!(snapshot.timestamp_ms, Some(1_800_000_000_123));
    }

    #[test]
    fn pnl_snapshots_keep_signed_quantities_and_decimal_money() {
        let position = codec::encode(&protocol::InstrumentPnLPositionUpdate {
            template_id: INSTRUMENT_PNL_POSITION_UPDATE,
            is_snapshot: Some(true),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("MNQU6".to_string()),
            net_quantity: Some(-2),
            avg_open_fill_price: Some(21_000.25),
            open_position_pnl: Some("-12.50".to_string()),
            day_pnl: Some(25.75),
            ..Default::default()
        })
        .unwrap();
        let PnlSnapshotEvent::Instrument(position) =
            decode_pnl_snapshot_event(&position, "pnl", &account()).unwrap()
        else {
            panic!("expected instrument snapshot");
        };
        assert_eq!(position.net_quantity, -2);
        assert_eq!(position.average_open_fill_price, Some(dec!(21000.25)));
        assert_eq!(position.open_pnl, Some(dec!(-12.50)));
        assert_eq!(position.day_pnl, Some(dec!(25.75)));

        let summary = codec::encode(&protocol::AccountPnLPositionUpdate {
            template_id: ACCOUNT_PNL_POSITION_UPDATE,
            is_snapshot: Some(true),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            account_balance: Some("25000.00".to_string()),
            available_buying_power: Some("24000.00".to_string()),
            day_pnl: Some("-10.25".to_string()),
            ..Default::default()
        })
        .unwrap();
        let PnlSnapshotEvent::Account(summary) =
            decode_pnl_snapshot_event(&summary, "pnl", &account()).unwrap()
        else {
            panic!("expected account snapshot");
        };
        assert_eq!(summary.account_balance, Some(dec!(25000.00)));
        assert_eq!(summary.available_buying_power, Some(dec!(24000.00)));
        assert_eq!(summary.day_pnl, Some(dec!(-10.25)));
    }

    #[test]
    fn snapshot_boundary_matrix_fails_closed() {
        let wrong_account = AccountIdentity {
            account_id: "OTHER".to_string(),
            ..account()
        };
        let valid_order = protocol::ExchangeOrderNotification {
            template_id: EXCHANGE_ORDER_NOTIFICATION,
            is_snapshot: Some(true),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some("basket-1".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("MNQU6".to_string()),
            status: Some("OPEN".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            ..Default::default()
        };

        for payload in [
            codec::encode(&protocol::ExchangeOrderNotification {
                is_snapshot: Some(false),
                ..valid_order.clone()
            })
            .unwrap(),
            codec::encode(&protocol::ExchangeOrderNotification {
                quantity: Some(0),
                ..valid_order.clone()
            })
            .unwrap(),
            codec::encode(&protocol::ExchangeOrderNotification {
                transaction_type: Some(99),
                ..valid_order.clone()
            })
            .unwrap(),
        ] {
            assert!(decode_order_snapshot_event(&payload, "orders", &account()).is_err());
        }
        let payload = codec::encode(&valid_order).unwrap();
        assert!(decode_order_snapshot_event(&payload, "orders", &wrong_account).is_err());

        let non_snapshot_position = codec::encode(&protocol::InstrumentPnLPositionUpdate {
            template_id: INSTRUMENT_PNL_POSITION_UPDATE,
            is_snapshot: Some(false),
            ..Default::default()
        })
        .unwrap();
        assert!(decode_pnl_snapshot_event(&non_snapshot_position, "pnl", &account()).is_err());
    }

    #[test]
    fn zero_average_prices_are_valid_for_unfilled_orders_and_flat_positions() {
        let order = codec::encode(&protocol::ExchangeOrderNotification {
            template_id: EXCHANGE_ORDER_NOTIFICATION,
            is_snapshot: Some(true),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some("basket-1".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("MNQU6".to_string()),
            status: Some("OPEN".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            avg_fill_price: Some(0.0),
            ..Default::default()
        })
        .unwrap();
        let OrderSnapshotEvent::Snapshot(order) =
            decode_order_snapshot_event(&order, "orders", &account()).unwrap()
        else {
            panic!("expected order snapshot");
        };
        assert_eq!(order.average_fill_price, Some(Decimal::ZERO));

        let position = codec::encode(&protocol::InstrumentPnLPositionUpdate {
            template_id: INSTRUMENT_PNL_POSITION_UPDATE,
            is_snapshot: Some(true),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("MNQU6".to_string()),
            net_quantity: Some(0),
            avg_open_fill_price: Some(0.0),
            ..Default::default()
        })
        .unwrap();
        let PnlSnapshotEvent::Instrument(position) =
            decode_pnl_snapshot_event(&position, "pnl", &account()).unwrap()
        else {
            panic!("expected instrument snapshot");
        };
        assert_eq!(position.average_open_fill_price, Some(Decimal::ZERO));
    }

    #[test]
    fn request_completion_and_error_matrix() {
        for (codes, succeeds) in [
            (vec!["0".to_string()], true),
            (vec!["9".to_string()], false),
            (vec![], false),
        ] {
            let orders = codec::encode(&protocol::ResponseShowOrders {
                template_id: SHOW_ORDERS_RESPONSE,
                user_msg: vec!["orders".to_string()],
                rp_code: codes.clone(),
            })
            .unwrap();
            assert_eq!(
                decode_order_snapshot_event(&orders, "orders", &account()).is_ok(),
                succeeds
            );

            let pnl = codec::encode(&protocol::ResponsePnLPositionSnapshot {
                template_id: PNL_POSITION_SNAPSHOT_RESPONSE,
                user_msg: vec!["pnl".to_string()],
                rp_code: codes,
            })
            .unwrap();
            assert_eq!(
                decode_pnl_snapshot_event(&pnl, "pnl", &account()).is_ok(),
                succeeds
            );
        }
    }
}
