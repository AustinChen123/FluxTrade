use super::{
    codec,
    ledger::{AccountIdentity, TransactionType},
    protocol,
    session::{classify_response_codes, ensure_success, ResponseDisposition},
};
use anyhow::{ensure, Context, Result};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use std::str::FromStr;

const TRADE_ROUTES_REQUEST: i32 = 310;
const TRADE_ROUTES_RESPONSE: i32 = 311;
const SUBSCRIBE_ORDER_UPDATES_REQUEST: i32 = 308;
const SUBSCRIBE_ORDER_UPDATES_RESPONSE: i32 = 309;
const NEW_ORDER_REQUEST: i32 = 312;
const NEW_ORDER_RESPONSE: i32 = 313;
const CANCEL_ORDER_REQUEST: i32 = 316;
const CANCEL_ORDER_RESPONSE: i32 = 317;
const EXCHANGE_ORDER_NOTIFICATION: i32 = 352;
const REJECT: i32 = 75;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TradeRoute {
    pub(crate) exchange: String,
    pub(crate) route: String,
    pub(crate) is_default: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TradeRouteEvent {
    Route(TradeRoute),
    Completed,
}

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
pub(crate) struct OrderAck {
    pub(crate) client_order_id: String,
    pub(crate) basket_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MutationResponse {
    pub(crate) disposition: ResponseDisposition,
    pub(crate) basket_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderEvent {
    pub(crate) account: AccountIdentity,
    pub(crate) client_order_id: Option<String>,
    pub(crate) basket_id: String,
    pub(crate) exchange_order_id: Option<String>,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) status: String,
    pub(crate) transaction_type: TransactionType,
    pub(crate) quantity: Decimal,
    pub(crate) last_fill_quantity: Option<Decimal>,
    pub(crate) last_fill_price: Option<Decimal>,
    pub(crate) cumulative_filled_quantity: Option<Decimal>,
    pub(crate) cumulative_average_price: Option<Decimal>,
    pub(crate) timestamp_ms: Option<i64>,
}

pub(crate) fn trade_routes_request(request_key: &str) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    codec::encode(&protocol::RequestTradeRoutes {
        template_id: TRADE_ROUTES_REQUEST,
        user_msg: vec![request_key.to_string()],
        subscribe_for_updates: Some(false),
    })
}

pub(crate) fn decode_trade_route_event(
    payload: &[u8],
    request_key: &str,
) -> Result<TradeRouteEvent> {
    validate_request_key(request_key)?;
    ensure_template(payload, TRADE_ROUTES_RESPONSE)?;
    let response: protocol::ResponseTradeRoutes = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    match classify_response_codes(&response.rq_handler_rp_code, &response.rp_code)? {
        ResponseDisposition::Succeeded => return Ok(TradeRouteEvent::Completed),
        ResponseDisposition::Failed(codes) => {
            anyhow::bail!("Rithmic trade-route response failed: {}", codes.join(","))
        }
        ResponseDisposition::Processing => {}
    }
    let status = required_text(response.status, "trade route status")?;
    ensure!(
        status.eq_ignore_ascii_case("up"),
        "Rithmic trade route is not up"
    );
    Ok(TradeRouteEvent::Route(TradeRoute {
        exchange: required_text(response.exchange, "trade route exchange")?,
        route: required_text(response.trade_route, "trade route")?,
        is_default: response.is_default.unwrap_or(false),
    }))
}

pub(crate) fn subscribe_order_updates_request(
    request_key: &str,
    account: &AccountIdentity,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    codec::encode(&protocol::RequestSubscribeForOrderUpdates {
        template_id: SUBSCRIBE_ORDER_UPDATES_REQUEST,
        user_msg: vec![request_key.to_string()],
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
    })
}

pub(crate) fn decode_subscribe_order_updates_response(
    payload: &[u8],
    request_key: &str,
) -> Result<()> {
    validate_request_key(request_key)?;
    ensure_template(payload, SUBSCRIBE_ORDER_UPDATES_RESPONSE)?;
    let response: protocol::ResponseSubscribeForOrderUpdates = codec::decode(payload)?;
    ensure_request_key(&response.user_msg, request_key)?;
    ensure_success(&response.rp_code)
}

pub(crate) fn new_order_request(
    request_key: &str,
    account: &AccountIdentity,
    trade_route: &str,
    order: &NewOrder,
) -> Result<Vec<u8>> {
    validate_request_key(request_key)?;
    validate_account(account)?;
    let client_order_id = required_text(Some(order.client_order_id.clone()), "client order ID")?;
    let exchange = required_text(Some(order.exchange.clone()), "exchange")?;
    let symbol = required_text(Some(order.symbol.clone()), "symbol")?;
    let trade_route = required_text(Some(trade_route.to_string()), "trade route")?;
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
        user_tag: Some(client_order_id),
        fcm_id: Some(account.fcm_id.clone()),
        ib_id: Some(account.ib_id.clone()),
        account_id: Some(account.account_id.clone()),
        symbol: Some(symbol),
        exchange: Some(exchange),
        quantity: Some(quantity),
        price,
        transaction_type: Some(transaction_type),
        duration: Some(protocol::request_new_order::Duration::Day as i32),
        price_type: Some(price_type),
        trade_route: Some(trade_route),
        manual_or_auto: Some(protocol::request_new_order::OrderPlacement::Auto as i32),
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

pub(crate) fn decode_order_event(
    payload: &[u8],
    expected_account: &AccountIdentity,
) -> Result<OrderEvent> {
    validate_account(expected_account)?;
    ensure_template(payload, EXCHANGE_ORDER_NOTIFICATION)?;
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
    let status = classify_status(
        notify_type,
        response.status.as_deref(),
        cumulative_filled_quantity,
        unfilled_quantity,
    )?;
    Ok(OrderEvent {
        account,
        client_order_id: optional_text(response.user_tag),
        basket_id: required_text(response.basket_id, "basket ID")?,
        exchange_order_id: optional_text(response.exchange_order_id),
        exchange: required_text(response.exchange, "exchange")?,
        symbol: required_text(response.symbol, "symbol")?,
        status,
        transaction_type: transaction_type(response.transaction_type)?,
        quantity: positive_quantity(response.quantity, "order quantity")?,
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

pub(crate) fn template_id(payload: &[u8]) -> Result<i32> {
    codec::template_id(payload)
}

pub(crate) fn is_new_order_response(template_id: i32) -> bool {
    template_id == NEW_ORDER_RESPONSE
}

pub(crate) fn is_cancel_order_response(template_id: i32) -> bool {
    template_id == CANCEL_ORDER_RESPONSE
}

pub(crate) fn is_order_event(template_id: i32) -> bool {
    template_id == EXCHANGE_ORDER_NOTIFICATION
}

pub(crate) fn notification_is_snapshot(payload: &[u8]) -> Result<bool> {
    ensure_template(payload, EXCHANGE_ORDER_NOTIFICATION)?;
    let response: protocol::ExchangeOrderNotification = codec::decode(payload)?;
    Ok(response.is_snapshot == Some(true))
}

pub(crate) fn is_reject(template_id: i32) -> bool {
    template_id == REJECT
}

fn classify_status(
    notify_type: protocol::exchange_order_notification::NotifyType,
    raw_status: Option<&str>,
    cumulative_filled: Option<Decimal>,
    unfilled: Option<Decimal>,
) -> Result<String> {
    use protocol::exchange_order_notification::NotifyType;
    let status = match notify_type {
        NotifyType::Fill => {
            ensure!(
                cumulative_filled.is_some(),
                "Rithmic fill event omitted cumulative fill size"
            );
            if unfilled == Some(Decimal::ZERO) {
                "filled"
            } else {
                "partially_filled"
            }
        }
        NotifyType::Cancel => "cancelled",
        NotifyType::Reject => "rejected",
        NotifyType::NotCancelled => "cancel_rejected",
        NotifyType::Status | NotifyType::Modify | NotifyType::Trigger | NotifyType::Generic => {
            return normalize_status(raw_status);
        }
        NotifyType::NotModified => "modify_rejected",
    };
    Ok(status.to_string())
}

fn normalize_status(raw_status: Option<&str>) -> Result<String> {
    let normalized = raw_status
        .map(str::trim)
        .filter(|status| !status.is_empty())
        .context("Rithmic order event omitted status")?
        .to_ascii_lowercase()
        .replace([' ', '-'], "_");
    let status = match normalized.as_str() {
        "open" | "open_pending" | "submitted" | "accepted" => "open",
        "partial" | "partially_filled" | "partiallyfilled" => "partially_filled",
        "complete" | "completed" | "filled" => "filled",
        "cancel" | "canceled" | "cancelled" => "cancelled",
        "reject" | "rejected" => "rejected",
        other => anyhow::bail!("unsupported Rithmic order status {other}"),
    };
    Ok(status.to_string())
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

fn validate_request_key(request_key: &str) -> Result<()> {
    ensure!(
        !request_key.trim().is_empty(),
        "Rithmic order request key must not be empty"
    );
    Ok(())
}

fn ensure_request_key(user_msg: &[String], request_key: &str) -> Result<()> {
    ensure!(
        user_msg.first().is_some_and(|value| value == request_key),
        "Rithmic order response request key mismatch"
    );
    Ok(())
}

fn ensure_template(payload: &[u8], expected: i32) -> Result<()> {
    ensure!(
        codec::template_id(payload)? == expected,
        "unexpected Rithmic order response template"
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

fn positive_quantity(value: Option<i32>, field: &str) -> Result<Decimal> {
    let value = value.with_context(|| format!("missing Rithmic {field}"))?;
    ensure!(value > 0, "invalid Rithmic {field}");
    Ok(Decimal::from(value))
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

    fn order(order_type: OrderType) -> NewOrder {
        NewOrder {
            client_order_id: "client-1".to_string(),
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            quantity: dec!(1),
            price: (order_type == OrderType::Limit).then_some(dec!(20000.25)),
            side: OrderSide::Buy,
            order_type,
        }
    }

    #[test]
    fn order_request_templates_and_decimal_boundary_are_exact() {
        let payload =
            new_order_request("new-1", &account(), "route", &order(OrderType::Limit)).unwrap();
        let request: protocol::RequestNewOrder = codec::decode(&payload).unwrap();
        assert_eq!(request.template_id, NEW_ORDER_REQUEST);
        assert_eq!(request.user_msg, ["new-1"]);
        assert_eq!(request.user_tag.as_deref(), Some("client-1"));
        assert_eq!(request.quantity, Some(1));
        assert_eq!(request.price, Some(20000.25));
        assert_eq!(request.trade_route.as_deref(), Some("route"));

        let cancel = cancel_order_request("cancel-1", &account(), "basket-1").unwrap();
        let cancel: protocol::RequestCancelOrder = codec::decode(&cancel).unwrap();
        assert_eq!(cancel.template_id, CANCEL_ORDER_REQUEST);
        assert_eq!(cancel.basket_id.as_deref(), Some("basket-1"));
    }

    #[test]
    fn trade_route_service_state_matrix_fails_closed() {
        for (status, succeeds) in [
            (Some("UP"), true),
            (Some("up"), true),
            (Some("DOWN"), false),
            (Some("open"), false),
            (None, false),
        ] {
            let payload = codec::encode(&protocol::ResponseTradeRoutes {
                template_id: TRADE_ROUTES_RESPONSE,
                user_msg: vec!["routes".to_string()],
                rq_handler_rp_code: vec!["0".to_string()],
                exchange: Some("CME".to_string()),
                trade_route: Some("globex".to_string()),
                status: status.map(str::to_string),
                is_default: Some(true),
                ..Default::default()
            })
            .unwrap();

            assert_eq!(
                decode_trade_route_event(&payload, "routes").is_ok(),
                succeeds,
                "status={status:?}",
            );
        }
    }

    #[test]
    fn order_request_validation_matrix_fails_closed() {
        for (quantity, price, order_type, succeeds) in [
            (dec!(1), None, OrderType::Market, true),
            (dec!(1), Some(dec!(20000.25)), OrderType::Limit, true),
            (dec!(0), None, OrderType::Market, false),
            (dec!(-1), None, OrderType::Market, false),
            (dec!(1.5), None, OrderType::Market, false),
            (dec!(1), Some(dec!(1)), OrderType::Market, false),
            (dec!(1), None, OrderType::Limit, false),
        ] {
            let mut candidate = order(order_type);
            candidate.quantity = quantity;
            candidate.price = price;
            assert_eq!(
                new_order_request("new", &account(), "route", &candidate).is_ok(),
                succeeds
            );
        }
    }

    #[test]
    fn response_identity_matrix_is_strict() {
        let response = |request_key: &str, user_tag: &str, basket_id: &str, code: &str| {
            codec::encode(&protocol::ResponseNewOrder {
                template_id: NEW_ORDER_RESPONSE,
                user_msg: vec![request_key.to_string()],
                user_tag: Some(user_tag.to_string()),
                basket_id: Some(basket_id.to_string()),
                rp_code: vec![code.to_string()],
                ..Default::default()
            })
            .unwrap()
        };
        assert_eq!(
            decode_new_order_response(
                &response("new", "client-1", "basket-1", "0"),
                "new",
                "client-1"
            )
            .unwrap(),
            MutationResponse {
                disposition: ResponseDisposition::Succeeded,
                basket_id: Some("basket-1".to_string()),
            }
        );
        assert!(decode_new_order_response(
            &response("other", "client-1", "basket-1", "0"),
            "new",
            "client-1"
        )
        .is_err());
        assert!(decode_new_order_response(
            &response("new", "other", "basket-1", "0"),
            "new",
            "client-1"
        )
        .is_err());
        assert_eq!(
            decode_new_order_response(
                &response("new", "client-1", "basket-1", "9"),
                "new",
                "client-1"
            )
            .unwrap(),
            MutationResponse {
                disposition: ResponseDisposition::Failed(vec!["9".to_string()]),
                basket_id: Some("basket-1".to_string()),
            }
        );

        let processing = codec::encode(&protocol::ResponseNewOrder {
            template_id: NEW_ORDER_RESPONSE,
            user_msg: vec!["new".to_string()],
            user_tag: Some("client-1".to_string()),
            basket_id: Some("basket-1".to_string()),
            rq_handler_rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_new_order_response(&processing, "new", "client-1").unwrap(),
            MutationResponse {
                disposition: ResponseDisposition::Processing,
                basket_id: Some("basket-1".to_string()),
            }
        );
    }

    #[test]
    fn cancel_response_phase_and_identity_are_explicit() {
        let response = |handler: &[&str], terminal: &[&str], basket_id: Option<&str>| {
            codec::encode(&protocol::ResponseCancelOrder {
                template_id: CANCEL_ORDER_RESPONSE,
                user_msg: vec!["cancel".to_string()],
                basket_id: basket_id.map(str::to_string),
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                ..Default::default()
            })
            .unwrap()
        };
        assert_eq!(
            decode_cancel_order_response(
                &response(&["0"], &[], Some("basket-1")),
                "cancel",
                "basket-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Processing,
        );
        assert_eq!(
            decode_cancel_order_response(&response(&[], &["0"], None), "cancel", "basket-1")
                .unwrap()
                .disposition,
            ResponseDisposition::Succeeded,
        );
        assert!(decode_cancel_order_response(
            &response(&[], &["0"], Some("other")),
            "cancel",
            "basket-1",
        )
        .is_err());
    }

    #[test]
    fn live_event_state_matrix_uses_notify_type_and_fill_totals() {
        use protocol::exchange_order_notification::NotifyType;
        for (notify_type, raw_status, filled, unfilled, expected) in [
            (NotifyType::Status, "OPEN", 0, 1, "open"),
            (NotifyType::Fill, "OPEN", 1, 1, "partially_filled"),
            (NotifyType::Fill, "COMPLETE", 2, 0, "filled"),
            (NotifyType::Cancel, "COMPLETE", 0, 0, "cancelled"),
            (NotifyType::Reject, "COMPLETE", 0, 0, "rejected"),
            (NotifyType::NotCancelled, "OPEN", 0, 1, "cancel_rejected"),
        ] {
            let payload = codec::encode(&protocol::ExchangeOrderNotification {
                template_id: EXCHANGE_ORDER_NOTIFICATION,
                notify_type: Some(notify_type as i32),
                is_snapshot: Some(false),
                user_tag: Some("client-1".to_string()),
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                basket_id: Some("basket-1".to_string()),
                exchange: Some("CME".to_string()),
                symbol: Some("NQU6".to_string()),
                status: Some(raw_status.to_string()),
                transaction_type: Some(
                    protocol::exchange_order_notification::TransactionType::Buy as i32,
                ),
                quantity: Some(2),
                total_fill_size: Some(filled),
                total_unfilled_size: Some(unfilled),
                fill_size: (notify_type == NotifyType::Fill).then_some(1),
                fill_price: (notify_type == NotifyType::Fill).then_some(20000.25),
                avg_fill_price: (filled > 0).then_some(20000.25),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(
                decode_order_event(&payload, &account()).unwrap().status,
                expected
            );
        }
    }

    #[test]
    fn live_event_rejects_wrong_account_snapshot_and_incomplete_fill() {
        let base = protocol::ExchangeOrderNotification {
            template_id: EXCHANGE_ORDER_NOTIFICATION,
            notify_type: Some(protocol::exchange_order_notification::NotifyType::Fill as i32),
            is_snapshot: Some(false),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some("basket-1".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            status: Some("OPEN".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            total_fill_size: Some(1),
            total_unfilled_size: Some(0),
            fill_size: Some(1),
            fill_price: Some(20000.25),
            ..Default::default()
        };
        for invalid in [
            protocol::ExchangeOrderNotification {
                is_snapshot: Some(true),
                ..base.clone()
            },
            protocol::ExchangeOrderNotification {
                account_id: Some("OTHER".to_string()),
                ..base.clone()
            },
            protocol::ExchangeOrderNotification {
                total_fill_size: None,
                ..base.clone()
            },
        ] {
            let payload = codec::encode(&invalid).unwrap();
            assert!(decode_order_event(&payload, &account()).is_err());
        }
    }
}
