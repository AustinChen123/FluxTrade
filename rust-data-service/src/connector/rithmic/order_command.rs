use super::{
    codec,
    ledger::{AccountIdentity, UserType},
    order::{
        ensure_request_key, ensure_template, optional_text, required_text, validate_account,
        validate_request_key,
    },
    protocol,
    session::{classify_response_codes, ResponseDisposition},
};
use anyhow::{ensure, Context, Result};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use std::str::FromStr;

pub(super) const NEW_ORDER_REQUEST: i32 = 312;
pub(super) const NEW_ORDER_RESPONSE: i32 = 313;
pub(super) const BRACKET_ORDER_REQUEST: i32 = 330;
pub(super) const BRACKET_ORDER_RESPONSE: i32 = 331;
pub(super) const MODIFY_ORDER_REQUEST: i32 = 314;
pub(super) const MODIFY_ORDER_RESPONSE: i32 = 315;
pub(super) const CANCEL_ORDER_REQUEST: i32 = 316;
pub(super) const CANCEL_ORDER_RESPONSE: i32 = 317;
pub(super) const EXIT_POSITION_REQUEST: i32 = 3504;
pub(super) const EXIT_POSITION_RESPONSE: i32 = 3505;
pub(super) const REJECT: i32 = 75;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderType {
    Market,
    Limit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NewOrder {
    pub(crate) client_order_id: String,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) quantity: Decimal,
    pub(crate) price: Option<Decimal>,
    pub(crate) side: OrderSide,
    pub(crate) order_type: OrderType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BracketOrder {
    pub(crate) entry: NewOrder,
    pub(crate) stop_ticks: Option<i32>,
    pub(crate) target_ticks: Option<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtectionLeg {
    StopLoss,
    TakeProfit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProtectionModification {
    pub(crate) basket_id: String,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) quantity: Decimal,
    pub(crate) leg: ProtectionLeg,
    pub(crate) price: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExitPosition {
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) window_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderAck {
    pub(crate) client_order_id: String,
    pub(crate) basket_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MutationResponse {
    pub(crate) disposition: ResponseDisposition,
    pub(crate) basket_id: Option<String>,
}

pub(crate) fn new_order_request(
    request_key: &str,
    account: &AccountIdentity,
    trade_route: &str,
    order: &NewOrder,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let trade_route = required_text(Some(trade_route.to_string()), "trade route")?;
    let fields = validate_wire_order(order)?;
    let transaction_type = match order.side {
        OrderSide::Buy => protocol::request_new_order::TransactionType::Buy as i32,
        OrderSide::Sell => protocol::request_new_order::TransactionType::Sell as i32,
    };
    let price_type = match order.order_type {
        OrderType::Market => protocol::request_new_order::PriceType::Market as i32,
        OrderType::Limit => protocol::request_new_order::PriceType::Limit as i32,
    };
    codec::encode(&protocol::RequestNewOrder {
        template_id: NEW_ORDER_REQUEST,
        user_msg: vec![request_key.to_string()],
        user_tag: Some(fields.client_order_id),
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        symbol: Some(fields.symbol),
        exchange: Some(fields.exchange),
        quantity: Some(fields.quantity),
        price: fields.price,
        transaction_type: Some(transaction_type),
        duration: Some(protocol::request_new_order::Duration::Day as i32),
        price_type: Some(price_type),
        trade_route: Some(trade_route),
        manual_or_auto: Some(protocol::request_new_order::OrderPlacement::Auto as i32),
        ..Default::default()
    })
}

pub(crate) fn bracket_order_request(
    request_key: &str,
    account: &AccountIdentity,
    user_type: UserType,
    trade_route: &str,
    order: &BracketOrder,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let trade_route = required_text(Some(trade_route.to_string()), "trade route")?;
    let fields = validate_wire_order(&order.entry)?;
    let stop_ticks = positive_ticks(order.stop_ticks, "stop ticks")?;
    let target_ticks = positive_ticks(order.target_ticks, "target ticks")?;
    ensure!(
        stop_ticks.is_some() || target_ticks.is_some(),
        "Rithmic bracket order requires stop or target ticks"
    );
    let bracket_type = match (stop_ticks, target_ticks) {
        (Some(_), Some(_)) => protocol::request_bracket_order::BracketType::TargetAndStopStatic,
        (Some(_), None) => protocol::request_bracket_order::BracketType::StopOnlyStatic,
        (None, Some(_)) => protocol::request_bracket_order::BracketType::TargetOnlyStatic,
        (None, None) => unreachable!("validated bracket has at least one protective leg"),
    } as i32;
    let transaction_type = match order.entry.side {
        OrderSide::Buy => protocol::request_bracket_order::TransactionType::Buy as i32,
        OrderSide::Sell => protocol::request_bracket_order::TransactionType::Sell as i32,
    };
    let price_type = match order.entry.order_type {
        OrderType::Market => protocol::request_bracket_order::PriceType::Market as i32,
        OrderType::Limit => protocol::request_bracket_order::PriceType::Limit as i32,
    };
    let user_type = match user_type {
        UserType::Admin => protocol::request_bracket_order::UserType::Admin as i32,
        UserType::Fcm => protocol::request_bracket_order::UserType::Fcm as i32,
        UserType::Ib => protocol::request_bracket_order::UserType::Ib as i32,
        UserType::Trader => protocol::request_bracket_order::UserType::Trader as i32,
    };

    codec::encode(&protocol::RequestBracketOrder {
        template_id: BRACKET_ORDER_REQUEST,
        user_msg: vec![request_key.to_string()],
        user_tag: Some(fields.client_order_id),
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        symbol: Some(fields.symbol),
        exchange: Some(fields.exchange),
        quantity: Some(fields.quantity),
        price: fields.price,
        transaction_type: Some(transaction_type),
        duration: Some(protocol::request_bracket_order::Duration::Day as i32),
        price_type: Some(price_type),
        trade_route: Some(trade_route),
        manual_or_auto: Some(protocol::request_bracket_order::OrderPlacement::Auto as i32),
        user_type: Some(user_type),
        bracket_type: Some(bracket_type),
        target_quantity: target_ticks.map(|_| fields.quantity).into_iter().collect(),
        target_ticks: target_ticks.into_iter().collect(),
        stop_quantity: stop_ticks.map(|_| fields.quantity).into_iter().collect(),
        stop_ticks: stop_ticks.into_iter().collect(),
        ..Default::default()
    })
}

pub(crate) fn modify_order_request(
    request_key: &str,
    account: &AccountIdentity,
    modification: &ProtectionModification,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let basket_id = required_text(Some(modification.basket_id.clone()), "basket ID")?;
    let exchange = required_text(Some(modification.exchange.clone()), "exchange")?;
    let symbol = required_text(Some(modification.symbol.clone()), "symbol")?;
    let quantity = decimal_quantity_to_i32(modification.quantity)?;
    let price = decimal_price_to_f64(modification.price)?;
    let (price_type, limit_price, trigger_price) = match modification.leg {
        ProtectionLeg::StopLoss => (
            protocol::request_modify_order::PriceType::StopMarket as i32,
            None,
            Some(price),
        ),
        ProtectionLeg::TakeProfit => (
            protocol::request_modify_order::PriceType::Limit as i32,
            Some(price),
            None,
        ),
    };
    codec::encode(&protocol::RequestModifyOrder {
        template_id: MODIFY_ORDER_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        basket_id: Some(basket_id),
        symbol: Some(symbol),
        exchange: Some(exchange),
        quantity: Some(quantity),
        price: limit_price,
        trigger_price,
        price_type: Some(price_type),
        manual_or_auto: Some(protocol::request_modify_order::OrderPlacement::Auto as i32),
        ..Default::default()
    })
}

pub(crate) fn decode_new_order_response(
    payload: &[u8],
    request_key: &str,
    expected_client_order_id: &str,
) -> Result<MutationResponse> {
    validate_request_key(request_key)?;
    ensure_template(payload, NEW_ORDER_RESPONSE)?;
    let response: protocol::ResponseNewOrder = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    if let Some(user_tag) = optional_text(response.user_tag) {
        ensure!(
            user_tag == expected_client_order_id,
            "Rithmic new-order client ID mismatch"
        );
    }
    Ok(MutationResponse {
        disposition: classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)?,
        basket_id: optional_text(response.basket_id),
    })
}

pub(crate) fn decode_bracket_order_response(
    payload: &[u8],
    request_key: &str,
    expected_client_order_id: &str,
) -> Result<MutationResponse> {
    validate_request_key(request_key)?;
    ensure_template(payload, BRACKET_ORDER_RESPONSE)?;
    let response: protocol::ResponseBracketOrder = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    if let Some(user_tag) = optional_text(response.user_tag) {
        ensure!(
            user_tag == expected_client_order_id,
            "Rithmic bracket-order client ID mismatch"
        );
    }
    Ok(MutationResponse {
        disposition: classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)?,
        basket_id: optional_text(response.basket_id),
    })
}

pub(crate) fn decode_modify_order_response(
    payload: &[u8],
    request_key: &str,
    expected_basket_id: &str,
) -> Result<MutationResponse> {
    validate_request_key(request_key)?;
    ensure_template(payload, MODIFY_ORDER_RESPONSE)?;
    let response: protocol::ResponseModifyOrder = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    let basket_id = optional_text(response.basket_id);
    if let Some(basket_id) = basket_id.as_deref() {
        ensure!(
            basket_id == expected_basket_id,
            "Rithmic modify-order basket ID mismatch"
        );
    }
    Ok(MutationResponse {
        disposition: classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)?,
        basket_id,
    })
}

pub(crate) fn cancel_order_request(
    request_key: &str,
    account: &AccountIdentity,
    basket_id: &str,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let basket_id = required_text(Some(basket_id.to_string()), "basket ID")?;
    codec::encode(&protocol::RequestCancelOrder {
        template_id: CANCEL_ORDER_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        basket_id: Some(basket_id),
        manual_or_auto: Some(protocol::request_cancel_order::OrderPlacement::Auto as i32),
        ..Default::default()
    })
}

pub(crate) fn decode_cancel_order_response(
    payload: &[u8],
    request_key: &str,
    expected_basket_id: &str,
) -> Result<MutationResponse> {
    validate_request_key(request_key)?;
    ensure_template(payload, CANCEL_ORDER_RESPONSE)?;
    let response: protocol::ResponseCancelOrder = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    let basket_id = optional_text(response.basket_id);
    if let Some(basket_id) = basket_id.as_deref() {
        ensure!(
            basket_id == expected_basket_id,
            "Rithmic cancel-order basket ID mismatch"
        );
    }
    Ok(MutationResponse {
        disposition: classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)?,
        basket_id,
    })
}

pub(crate) fn exit_position_request(
    request_key: &str,
    account: &AccountIdentity,
    position: &ExitPosition,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let exchange = required_text(Some(position.exchange.clone()), "exchange")?;
    let symbol = required_text(Some(position.symbol.clone()), "symbol")?;
    let window_name = position
        .window_name
        .clone()
        .map(|value| required_text(Some(value), "window_name"))
        .transpose()?;
    codec::encode(&protocol::RequestExitPosition {
        template_id: EXIT_POSITION_REQUEST,
        user_msg: vec![request_key.to_string()],
        window_name,
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        symbol: Some(symbol),
        exchange: Some(exchange),
        manual_or_auto: Some(protocol::request_exit_position::OrderPlacement::Auto as i32),
        ..Default::default()
    })
}

pub(crate) fn decode_exit_position_response(
    payload: &[u8],
    request_key: &str,
    expected: &ExitPosition,
) -> Result<ResponseDisposition> {
    validate_request_key(request_key)?;
    ensure_template(payload, EXIT_POSITION_RESPONSE)?;
    let response: protocol::ResponseExitPosition = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    if let Some(exchange) = optional_text(response.exchange) {
        ensure!(
            exchange.eq_ignore_ascii_case(&expected.exchange),
            "Rithmic exit-position exchange mismatch"
        );
    }
    if let Some(symbol) = optional_text(response.symbol) {
        ensure!(
            symbol == expected.symbol,
            "Rithmic exit-position symbol mismatch"
        );
    }
    classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)
}

pub(crate) fn decode_request_reject(payload: &[u8]) -> Result<(String, String)> {
    ensure_template(payload, REJECT)?;
    let response: protocol::Reject = codec::decode(payload)?;
    let request_key = response
        .user_msg
        .first()
        .cloned()
        .context("Rithmic reject omitted request key")?;
    let code = response
        .rp_code
        .first()
        .cloned()
        .context("Rithmic reject omitted response code")?;
    Ok((request_key, code))
}

pub(crate) fn is_new_order_response(template_id: i32) -> bool {
    template_id == NEW_ORDER_RESPONSE
}

pub(crate) fn is_bracket_order_response(template_id: i32) -> bool {
    template_id == BRACKET_ORDER_RESPONSE
}

pub(crate) fn is_modify_order_response(template_id: i32) -> bool {
    template_id == MODIFY_ORDER_RESPONSE
}

pub(crate) fn is_cancel_order_response(template_id: i32) -> bool {
    template_id == CANCEL_ORDER_RESPONSE
}

pub(crate) fn is_exit_position_response(template_id: i32) -> bool {
    template_id == EXIT_POSITION_RESPONSE
}

pub(crate) fn is_reject(template_id: i32) -> bool {
    template_id == REJECT
}

fn decimal_quantity_to_i32(value: Decimal) -> Result<i32> {
    ensure!(
        value > Decimal::ZERO,
        "Rithmic order quantity must be positive"
    );
    ensure!(
        value.fract().is_zero(),
        "Rithmic order quantity must be an integer"
    );
    value
        .to_i32()
        .context("Rithmic order quantity exceeds protocol range")
}

fn decimal_price_to_f64(value: Decimal) -> Result<f64> {
    ensure!(
        value > Decimal::ZERO,
        "Rithmic order price must be positive"
    );
    let wire = value
        .to_string()
        .parse::<f64>()
        .context("Rithmic order price exceeds protocol range")?;
    ensure!(wire.is_finite(), "Rithmic order price must be finite");
    let round_trip = Decimal::from_str(&wire.to_string())
        .context("Rithmic order price cannot round-trip through vendor wire")?;
    ensure!(
        round_trip == value.normalize(),
        "Rithmic order price loses precision at vendor wire boundary"
    );
    Ok(wire)
}

struct WireOrderFields {
    client_order_id: String,
    exchange: String,
    symbol: String,
    quantity: i32,
    price: Option<f64>,
}

fn validate_wire_order(order: &NewOrder) -> Result<WireOrderFields> {
    let client_order_id = required_text(Some(order.client_order_id.clone()), "client order ID")?;
    let exchange = required_text(Some(order.exchange.clone()), "exchange")?;
    let symbol = required_text(Some(order.symbol.clone()), "symbol")?;
    let quantity = decimal_quantity_to_i32(order.quantity)?;
    let price = match order.order_type {
        OrderType::Market => {
            ensure!(
                order.price.is_none(),
                "Rithmic market order cannot include price"
            );
            None
        }
        OrderType::Limit => Some(decimal_price_to_f64(
            order.price.context("Rithmic limit order requires price")?,
        )?),
    };
    Ok(WireOrderFields {
        client_order_id,
        exchange,
        symbol,
        quantity,
        price,
    })
}

fn positive_ticks(value: Option<i32>, field: &str) -> Result<Option<i32>> {
    value
        .map(|value| {
            ensure!(value > 0, "invalid Rithmic {field}");
            Ok(value)
        })
        .transpose()
}
