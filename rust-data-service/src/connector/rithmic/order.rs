use super::{
    codec,
    ledger::AccountIdentity,
    protocol,
    session::{classify_response_codes, ensure_success, ResponseDisposition},
};
use anyhow::{ensure, Context, Result};

const TRADE_ROUTES_REQUEST: i32 = 310;
const TRADE_ROUTES_RESPONSE: i32 = 311;
const SUBSCRIBE_ORDER_UPDATES_REQUEST: i32 = 308;
const SUBSCRIBE_ORDER_UPDATES_RESPONSE: i32 = 309;

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

pub(crate) fn template_id(payload: &[u8]) -> Result<i32> {
    codec::template_id(payload)
}

pub(super) fn validate_request_key(request_key: &str) -> Result<()> {
    ensure!(
        !request_key.trim().is_empty(),
        "Rithmic order request key must not be empty"
    );
    Ok(())
}

pub(super) fn ensure_request_key(user_msg: &[String], request_key: &str) -> Result<()> {
    ensure!(
        user_msg.first().is_some_and(|value| value == request_key),
        "Rithmic order response request key mismatch"
    );
    Ok(())
}

pub(super) fn ensure_template(payload: &[u8], expected: i32) -> Result<()> {
    ensure!(
        codec::template_id(payload)? == expected,
        "unexpected Rithmic order response template"
    );
    Ok(())
}

pub(super) fn validate_account(account: &AccountIdentity) -> Result<()> {
    required_text(Some(account.fcm_id.clone()), "fcm ID")?;
    required_text(Some(account.ib_id.clone()), "IB ID")?;
    required_text(Some(account.account_id.clone()), "account ID")?;
    Ok(())
}

pub(super) fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic {field}"))
}

pub(super) fn optional_text(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::super::ledger::UserType;
    use super::super::order_command::*;
    use super::super::order_event::decode_order_event;
    use super::*;
    use rust_decimal_macros::dec;

    const EXCHANGE_ORDER_NOTIFICATION: i32 = 352;

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

    fn error_message<T: std::fmt::Debug>(result: Result<T>) -> String {
        result.unwrap_err().to_string()
    }

    #[test]
    fn live_order_event_decoder_has_one_module_owner() {
        let request_source = include_str!("order.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        let event_source = include_str!("order_event.rs");
        let runtime_source = include_str!("order_runtime.rs");
        let pending_source = include_str!("order_pending.rs");
        let binding_source = include_str!("../../binding/rithmic_order.rs");

        for symbol in [
            "struct OrderEvent",
            "fn decode_order_event",
            "fn classify_status",
            "fn notification_is_snapshot",
        ] {
            assert!(!request_source.contains(symbol), "duplicate {symbol}");
            assert!(event_source.contains(symbol), "missing {symbol}");
        }
        assert!(!request_source.contains("ExchangeOrderNotification"));
        assert!(runtime_source.contains("order_event::decode_order_event"));
        assert!(runtime_source.contains("order_event::notification_is_snapshot"));
        assert!(pending_source.contains("order_event::OrderEvent"));
        assert!(binding_source.contains("order_event::OrderEvent"));
    }

    #[test]
    fn order_command_codec_has_one_module_owner() {
        let route_source = include_str!("order.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        let command_source = include_str!("order_command.rs");
        let runtime_source = include_str!("order_runtime.rs");
        let pending_source = include_str!("order_pending.rs");
        let emergency_source = include_str!("emergency.rs");
        let binding_source = include_str!("../../binding/rithmic_order.rs");

        for symbol in [
            "enum OrderSide",
            "enum OrderType",
            "struct NewOrder",
            "struct BracketOrder",
            "enum ProtectionLeg",
            "struct ProtectionModification",
            "struct ExitPosition",
            "struct OrderAck",
            "struct MutationResponse",
            "fn new_order_request",
            "fn bracket_order_request",
            "fn modify_order_request",
            "fn decode_new_order_response",
            "fn decode_bracket_order_response",
            "fn decode_modify_order_response",
            "fn cancel_order_request",
            "fn decode_cancel_order_response",
            "fn exit_position_request",
            "fn decode_exit_position_response",
            "fn decode_request_reject",
            "fn is_new_order_response",
            "fn is_bracket_order_response",
            "fn is_modify_order_response",
            "fn is_cancel_order_response",
            "fn is_exit_position_response",
            "fn is_reject",
            "fn decimal_quantity_to_i32",
            "fn decimal_price_to_f64",
            "struct WireOrderFields",
            "fn validate_wire_order",
            "fn positive_ticks",
            "const NEW_ORDER_REQUEST",
            "const NEW_ORDER_RESPONSE",
            "const BRACKET_ORDER_REQUEST",
            "const BRACKET_ORDER_RESPONSE",
            "const MODIFY_ORDER_REQUEST",
            "const MODIFY_ORDER_RESPONSE",
            "const CANCEL_ORDER_REQUEST",
            "const CANCEL_ORDER_RESPONSE",
            "const EXIT_POSITION_REQUEST",
            "const EXIT_POSITION_RESPONSE",
            "const REJECT",
        ] {
            assert!(!route_source.contains(symbol), "duplicate {symbol}");
            assert!(command_source.contains(symbol), "missing {symbol}");
        }
        for helper in [
            "validate_request_key",
            "ensure_request_key",
            "ensure_template",
            "validate_account",
            "required_text",
            "optional_text",
        ] {
            assert!(route_source.contains(&format!("fn {helper}")));
            assert!(!command_source.contains(&format!("fn {helper}")));
        }
        assert!(!route_source.contains("order_command"));
        assert!(runtime_source.contains("order_command::new_order_request"));
        assert!(pending_source.contains("order_command::decode_new_order_response"));
        assert!(emergency_source.contains("order_command::ExitPosition"));
        assert!(binding_source.contains("order_command::{"));
    }

    #[test]
    fn command_request_validation_precedence_is_exact() {
        let empty_account = AccountIdentity {
            fcm_id: String::new(),
            ib_id: String::new(),
            account_id: String::new(),
        };
        let mut invalid_order = order(OrderType::Limit);
        invalid_order.client_order_id.clear();
        invalid_order.exchange.clear();
        invalid_order.symbol.clear();
        invalid_order.quantity = dec!(0);
        invalid_order.price = None;

        assert_eq!(
            error_message(new_order_request("", &empty_account, "", &invalid_order)),
            "Rithmic order request key must not be empty",
        );
        assert_eq!(
            error_message(new_order_request("key", &empty_account, "", &invalid_order)),
            "missing Rithmic fcm ID",
        );
        let mut partial_account = empty_account.clone();
        partial_account.fcm_id = "FCM".to_string();
        assert_eq!(
            error_message(new_order_request(
                "key",
                &partial_account,
                "",
                &invalid_order,
            )),
            "missing Rithmic IB ID",
        );
        partial_account.ib_id = "IB".to_string();
        assert_eq!(
            error_message(new_order_request(
                "key",
                &partial_account,
                "",
                &invalid_order,
            )),
            "missing Rithmic account ID",
        );
        assert_eq!(
            error_message(new_order_request("key", &account(), "", &invalid_order)),
            "missing Rithmic trade route",
        );
        assert_eq!(
            error_message(new_order_request(
                "key",
                &account(),
                "route",
                &invalid_order
            )),
            "missing Rithmic client order ID",
        );
        invalid_order.client_order_id = "client".to_string();
        assert_eq!(
            error_message(new_order_request(
                "key",
                &account(),
                "route",
                &invalid_order,
            )),
            "missing Rithmic exchange",
        );
        invalid_order.exchange = "CME".to_string();
        assert_eq!(
            error_message(new_order_request(
                "key",
                &account(),
                "route",
                &invalid_order,
            )),
            "missing Rithmic symbol",
        );
        invalid_order.symbol = "NQU6".to_string();
        assert_eq!(
            error_message(new_order_request(
                "key",
                &account(),
                "route",
                &invalid_order,
            )),
            "Rithmic order quantity must be positive",
        );
        invalid_order.quantity = dec!(1);
        assert_eq!(
            error_message(new_order_request(
                "key",
                &account(),
                "route",
                &invalid_order,
            )),
            "Rithmic limit order requires price",
        );
        invalid_order.price = Some(dec!(0));
        assert_eq!(
            error_message(bracket_order_request(
                "key",
                &account(),
                UserType::Trader,
                "route",
                &BracketOrder {
                    entry: invalid_order,
                    stop_ticks: Some(0),
                    target_ticks: None,
                },
            )),
            "Rithmic order price must be positive",
        );

        let mut modification = ProtectionModification {
            basket_id: String::new(),
            exchange: String::new(),
            symbol: String::new(),
            quantity: dec!(0),
            leg: ProtectionLeg::StopLoss,
            price: dec!(0),
        };
        assert_eq!(
            error_message(modify_order_request("key", &account(), &modification)),
            "missing Rithmic basket ID",
        );
        modification.basket_id = "basket".to_string();
        assert_eq!(
            error_message(modify_order_request("key", &account(), &modification)),
            "missing Rithmic exchange",
        );
        modification.exchange = "CME".to_string();
        assert_eq!(
            error_message(modify_order_request("key", &account(), &modification)),
            "missing Rithmic symbol",
        );
        modification.symbol = "NQU6".to_string();
        assert_eq!(
            error_message(modify_order_request("key", &account(), &modification)),
            "Rithmic order quantity must be positive",
        );
        modification.quantity = dec!(1);
        assert_eq!(
            error_message(modify_order_request("key", &account(), &modification)),
            "Rithmic order price must be positive",
        );

        assert_eq!(
            error_message(cancel_order_request("key", &account(), "")),
            "missing Rithmic basket ID",
        );
        let mut position = ExitPosition {
            exchange: String::new(),
            symbol: String::new(),
            window_name: Some(String::new()),
        };
        assert_eq!(
            error_message(exit_position_request("key", &account(), &position)),
            "missing Rithmic exchange",
        );
        position.exchange = "CME".to_string();
        assert_eq!(
            error_message(exit_position_request("key", &account(), &position)),
            "missing Rithmic symbol",
        );
        position.symbol = "NQU6".to_string();
        assert_eq!(
            error_message(exit_position_request("key", &account(), &position)),
            "missing Rithmic window_name",
        );
    }

    #[test]
    fn command_response_validation_precedence_is_exact() {
        let wrong_template = codec::encode(&protocol::ResponseNewOrder {
            template_id: BRACKET_ORDER_RESPONSE,
            user_msg: vec!["other".to_string()],
            user_tag: Some("other-client".to_string()),
            rp_code: vec!["9".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            error_message(decode_new_order_response(&wrong_template, "", "client-1")),
            "Rithmic order request key must not be empty",
        );
        assert_eq!(
            error_message(decode_new_order_response(
                &wrong_template,
                "new",
                "client-1"
            )),
            "unexpected Rithmic order response template",
        );

        let response = |request_key: &str, client_order_id: &str| {
            codec::encode(&protocol::ResponseNewOrder {
                template_id: NEW_ORDER_RESPONSE,
                user_msg: vec![request_key.to_string()],
                user_tag: Some(client_order_id.to_string()),
                rp_code: vec!["9".to_string()],
                ..Default::default()
            })
            .unwrap()
        };
        assert_eq!(
            error_message(decode_new_order_response(
                &response("other", "other-client"),
                "new",
                "client-1",
            )),
            "Rithmic order response request key mismatch",
        );
        assert_eq!(
            error_message(decode_new_order_response(
                &response("new", "other-client"),
                "new",
                "client-1",
            )),
            "Rithmic new-order client ID mismatch",
        );
        assert_eq!(
            error_message(decode_trade_route_event(&wrong_template, "")),
            "Rithmic order request key must not be empty",
        );
        assert_eq!(
            error_message(decode_subscribe_order_updates_response(
                &wrong_template,
                "subscribe",
            )),
            "unexpected Rithmic order response template",
        );

        let reject = codec::encode(&protocol::Reject {
            template_id: REJECT,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            error_message(decode_request_reject(&reject)),
            "Rithmic reject omitted request key",
        );
        let reject_without_code = codec::encode(&protocol::Reject {
            template_id: REJECT,
            user_msg: vec!["request".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            error_message(decode_request_reject(&reject_without_code)),
            "Rithmic reject omitted response code",
        );
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
    fn bracket_request_maps_static_protection_matrix() {
        use protocol::request_bracket_order::BracketType;

        for (stop_ticks, target_ticks, bracket_type) in [
            (Some(8), None, BracketType::StopOnlyStatic),
            (None, Some(12), BracketType::TargetOnlyStatic),
            (Some(8), Some(12), BracketType::TargetAndStopStatic),
        ] {
            let payload = bracket_order_request(
                "bracket-1",
                &account(),
                UserType::Trader,
                "route",
                &BracketOrder {
                    entry: order(OrderType::Limit),
                    stop_ticks,
                    target_ticks,
                },
            )
            .unwrap();
            let request: protocol::RequestBracketOrder = codec::decode(&payload).unwrap();

            assert_eq!(request.template_id, BRACKET_ORDER_REQUEST);
            assert_eq!(request.user_msg, ["bracket-1"]);
            assert_eq!(request.user_tag.as_deref(), Some("client-1"));
            assert_eq!(request.quantity, Some(1));
            assert_eq!(request.price, Some(20000.25));
            assert_eq!(request.trade_route.as_deref(), Some("route"));
            assert_eq!(request.user_type, Some(3));
            assert_eq!(request.bracket_type, Some(bracket_type as i32));
            assert_eq!(
                request.stop_ticks,
                stop_ticks.into_iter().collect::<Vec<_>>()
            );
            assert_eq!(
                request.stop_quantity,
                stop_ticks.map(|_| 1).into_iter().collect::<Vec<_>>()
            );
            assert_eq!(
                request.target_ticks,
                target_ticks.into_iter().collect::<Vec<_>>()
            );
            assert_eq!(
                request.target_quantity,
                target_ticks.map(|_| 1).into_iter().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn bracket_request_validation_matrix_fails_closed() {
        for (stop_ticks, target_ticks, succeeds) in [
            (Some(1), None, true),
            (None, Some(1), true),
            (Some(1), Some(1), true),
            (None, None, false),
            (Some(0), None, false),
            (Some(-1), None, false),
            (None, Some(0), false),
            (None, Some(-1), false),
        ] {
            assert_eq!(
                bracket_order_request(
                    "bracket",
                    &account(),
                    UserType::Trader,
                    "route",
                    &BracketOrder {
                        entry: order(OrderType::Market),
                        stop_ticks,
                        target_ticks,
                    },
                )
                .is_ok(),
                succeeds,
                "stop_ticks={stop_ticks:?} target_ticks={target_ticks:?}",
            );
        }
    }

    #[test]
    fn modify_protection_request_maps_stop_and_target_fields() {
        use protocol::request_modify_order::PriceType;

        for (leg, price_type, price, trigger_price) in [
            (
                ProtectionLeg::StopLoss,
                PriceType::StopMarket,
                None,
                Some(19999.0),
            ),
            (
                ProtectionLeg::TakeProfit,
                PriceType::Limit,
                Some(20003.0),
                None,
            ),
        ] {
            let payload = modify_order_request(
                "modify-1",
                &account(),
                &ProtectionModification {
                    basket_id: "child-1".to_string(),
                    exchange: "CME".to_string(),
                    symbol: "NQU6".to_string(),
                    quantity: dec!(1),
                    leg,
                    price: if leg == ProtectionLeg::StopLoss {
                        dec!(19999.0)
                    } else {
                        dec!(20003.0)
                    },
                },
            )
            .unwrap();
            let request: protocol::RequestModifyOrder = codec::decode(&payload).unwrap();

            assert_eq!(request.template_id, 314);
            assert_eq!(request.user_msg, ["modify-1"]);
            assert_eq!(request.basket_id.as_deref(), Some("child-1"));
            assert_eq!(request.quantity, Some(1));
            assert_eq!(request.price_type, Some(price_type as i32));
            assert_eq!(request.price, price);
            assert_eq!(request.trigger_price, trigger_price);
        }
    }

    #[test]
    fn exit_position_request_and_response_validate_instrument_identity() {
        let position = ExitPosition {
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            window_name: Some("exit-window-1".to_string()),
        };
        let payload = exit_position_request("exit-1", &account(), &position).unwrap();
        let request: protocol::RequestExitPosition = codec::decode(&payload).unwrap();
        assert_eq!(request.template_id, EXIT_POSITION_REQUEST);
        assert_eq!(request.user_msg, ["exit-1"]);
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT"));
        assert_eq!(request.exchange.as_deref(), Some("CME"));
        assert_eq!(request.symbol.as_deref(), Some("NQU6"));
        assert_eq!(request.window_name.as_deref(), Some("exit-window-1"));
        assert_eq!(
            request.manual_or_auto,
            Some(protocol::request_exit_position::OrderPlacement::Auto as i32)
        );
        let uncorrelated = ExitPosition {
            window_name: None,
            ..position.clone()
        };
        let payload = exit_position_request("exit-2", &account(), &uncorrelated).unwrap();
        let request: protocol::RequestExitPosition = codec::decode(&payload).unwrap();
        assert_eq!(request.window_name, None);

        for invalid in ["", " "] {
            let candidate = ExitPosition {
                window_name: Some(invalid.to_string()),
                ..position.clone()
            };
            assert!(exit_position_request("exit", &account(), &candidate).is_err());
        }

        let response = |request_key: &str, exchange: &str, symbol: &str, code: &str| {
            codec::encode(&protocol::ResponseExitPosition {
                template_id: EXIT_POSITION_RESPONSE,
                user_msg: vec![request_key.to_string()],
                rp_code: vec![code.to_string()],
                exchange: Some(exchange.to_string()),
                symbol: Some(symbol.to_string()),
                ..Default::default()
            })
            .unwrap()
        };
        assert_eq!(
            decode_exit_position_response(
                &response("exit-1", "CME", "NQU6", "0"),
                "exit-1",
                &position,
            )
            .unwrap(),
            ResponseDisposition::Succeeded
        );
        let response_without_optional_identity = codec::encode(&protocol::ResponseExitPosition {
            template_id: EXIT_POSITION_RESPONSE,
            user_msg: vec!["exit-1".to_string()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_exit_position_response(
                &response_without_optional_identity,
                "exit-1",
                &position,
            )
            .unwrap(),
            ResponseDisposition::Succeeded
        );
        assert_eq!(
            error_message(decode_exit_position_response(
                &response("other", "OTHER", "ESU6", "9"),
                "exit-1",
                &position,
            )),
            "Rithmic order response request key mismatch",
        );
        assert_eq!(
            error_message(decode_exit_position_response(
                &response("exit-1", "OTHER", "ESU6", "9"),
                "exit-1",
                &position,
            )),
            "Rithmic exit-position exchange mismatch",
        );
        assert_eq!(
            error_message(decode_exit_position_response(
                &response("exit-1", "CME", "ESU6", "9"),
                "exit-1",
                &position,
            )),
            "Rithmic exit-position symbol mismatch",
        );
        assert_eq!(
            decode_exit_position_response(
                &response("exit-1", "CME", "NQU6", "9"),
                "exit-1",
                &position,
            )
            .unwrap(),
            ResponseDisposition::Failed(vec!["9".to_string()])
        );
    }

    #[test]
    fn exit_position_request_rejects_incomplete_instrument_identity() {
        for (exchange, symbol, succeeds) in [
            ("CME", "NQU6", true),
            ("", "NQU6", false),
            ("CME", "", false),
            (" ", "NQU6", false),
            ("CME", " ", false),
        ] {
            assert_eq!(
                exit_position_request(
                    "exit",
                    &account(),
                    &ExitPosition {
                        exchange: exchange.to_string(),
                        symbol: symbol.to_string(),
                        window_name: None,
                    },
                )
                .is_ok(),
                succeeds
            );
        }
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
    fn bracket_response_phase_and_identity_are_strict() {
        let response = |request_key: &str, user_tag: &str, handler: &[&str], terminal: &[&str]| {
            codec::encode(&protocol::ResponseBracketOrder {
                template_id: BRACKET_ORDER_RESPONSE,
                user_msg: vec![request_key.to_string()],
                user_tag: Some(user_tag.to_string()),
                basket_id: Some("basket-1".to_string()),
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                ..Default::default()
            })
            .unwrap()
        };

        assert_eq!(
            decode_bracket_order_response(
                &response("bracket", "client-1", &["0"], &[]),
                "bracket",
                "client-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Processing,
        );
        assert_eq!(
            decode_bracket_order_response(
                &response("bracket", "client-1", &[], &["0"]),
                "bracket",
                "client-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Succeeded,
        );
        assert_eq!(
            error_message(decode_bracket_order_response(
                &response("other", "other", &[], &["9"]),
                "bracket",
                "client-1",
            )),
            "Rithmic order response request key mismatch",
        );
        assert_eq!(
            error_message(decode_bracket_order_response(
                &response("bracket", "other", &[], &["9"]),
                "bracket",
                "client-1",
            )),
            "Rithmic bracket-order client ID mismatch",
        );
    }

    #[test]
    fn modify_response_phase_and_basket_identity_are_strict() {
        let response =
            |request_key: &str, basket_id: Option<&str>, handler: &[&str], terminal: &[&str]| {
                codec::encode(&protocol::ResponseModifyOrder {
                    template_id: 315,
                    user_msg: vec![request_key.to_string()],
                    basket_id: basket_id.map(str::to_string),
                    rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                    rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                    ..Default::default()
                })
                .unwrap()
            };

        assert_eq!(
            decode_modify_order_response(
                &response("modify", Some("child-1"), &["0"], &[]),
                "modify",
                "child-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Processing,
        );
        assert_eq!(
            decode_modify_order_response(
                &response("modify", Some("child-1"), &[], &["0"]),
                "modify",
                "child-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Succeeded,
        );
        assert_eq!(
            error_message(decode_modify_order_response(
                &response("other", Some("other-child"), &[], &["9"]),
                "modify",
                "child-1",
            )),
            "Rithmic order response request key mismatch",
        );
        assert_eq!(
            error_message(decode_modify_order_response(
                &response("modify", Some("other-child"), &[], &["9"]),
                "modify",
                "child-1",
            )),
            "Rithmic modify-order basket ID mismatch",
        );
    }

    #[test]
    fn cancel_response_phase_and_identity_are_explicit() {
        let response =
            |request_key: &str, handler: &[&str], terminal: &[&str], basket_id: Option<&str>| {
                codec::encode(&protocol::ResponseCancelOrder {
                    template_id: CANCEL_ORDER_RESPONSE,
                    user_msg: vec![request_key.to_string()],
                    basket_id: basket_id.map(str::to_string),
                    rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                    rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                    ..Default::default()
                })
                .unwrap()
            };
        assert_eq!(
            decode_cancel_order_response(
                &response("cancel", &["0"], &[], Some("basket-1")),
                "cancel",
                "basket-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Processing,
        );
        assert_eq!(
            decode_cancel_order_response(
                &response("cancel", &[], &["0"], None),
                "cancel",
                "basket-1",
            )
            .unwrap()
            .disposition,
            ResponseDisposition::Succeeded,
        );
        assert_eq!(
            error_message(decode_cancel_order_response(
                &response("other", &[], &["9"], Some("other")),
                "cancel",
                "basket-1",
            )),
            "Rithmic order response request key mismatch",
        );
        assert_eq!(
            error_message(decode_cancel_order_response(
                &response("cancel", &[], &["9"], Some("other")),
                "cancel",
                "basket-1",
            )),
            "Rithmic cancel-order basket ID mismatch",
        );
    }

    #[test]
    fn live_event_state_matrix_uses_notify_type_and_fill_totals() {
        use protocol::exchange_order_notification::NotifyType;
        for (notify_type, raw_status, filled, unfilled, expected) in [
            (NotifyType::Status, "OPEN", 0, 2, "open"),
            (NotifyType::Status, "OPEN", 1, 1, "partially_filled"),
            (NotifyType::Modify, "OPEN", 0, 2, "open"),
            (NotifyType::Trigger, "OPEN", 0, 2, "open"),
            (NotifyType::Generic, "OPEN", 0, 2, "open"),
            (NotifyType::Fill, "OPEN", 1, 1, "partially_filled"),
            (NotifyType::Fill, "COMPLETE", 2, 0, "filled"),
            (NotifyType::Cancel, "COMPLETE", 0, 0, "cancelled"),
            (NotifyType::Cancel, "COMPLETE", 1, 1, "cancelled"),
            (NotifyType::Reject, "COMPLETE", 0, 0, "rejected"),
            (NotifyType::Reject, "COMPLETE", 1, 1, "rejected"),
            (NotifyType::NotModified, "OPEN", 0, 2, "modify_rejected"),
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
    fn sparse_live_events_only_require_quantity_for_fill_semantics() {
        use protocol::exchange_order_notification::NotifyType;
        let event = |notify_type, status: Option<&str>, filled: Option<i32>| {
            codec::encode(&protocol::ExchangeOrderNotification {
                template_id: EXCHANGE_ORDER_NOTIFICATION,
                notify_type: Some(notify_type as i32),
                is_snapshot: Some(false),
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                basket_id: Some("basket-1".to_string()),
                exchange: Some("CME".to_string()),
                symbol: Some("NQU6".to_string()),
                status: status.map(str::to_string),
                transaction_type: Some(
                    protocol::exchange_order_notification::TransactionType::Buy as i32,
                ),
                total_fill_size: filled,
                ..Default::default()
            })
            .unwrap()
        };

        for (notify_type, status, filled, expected) in [
            (NotifyType::Status, Some("OPEN"), Some(0), "open"),
            (NotifyType::Status, None, Some(0), "open"),
            (NotifyType::Modify, None, None, "open"),
            (NotifyType::Trigger, None, Some(0), "open"),
            (NotifyType::Cancel, None, Some(0), "cancelled"),
            (NotifyType::Reject, None, Some(0), "rejected"),
            (NotifyType::NotModified, None, Some(0), "modify_rejected"),
            (NotifyType::NotCancelled, None, Some(0), "cancel_rejected"),
        ] {
            let decoded =
                decode_order_event(&event(notify_type, status, filled), &account()).unwrap();
            assert_eq!(decoded.status, expected);
            assert_eq!(decoded.quantity, None);
        }

        for payload in [
            event(NotifyType::Fill, Some("COMPLETE"), Some(1)),
            event(NotifyType::Status, Some("PARTIAL"), Some(1)),
            event(NotifyType::Status, Some("COMPLETE"), Some(1)),
            event(NotifyType::Status, None, Some(1)),
            event(NotifyType::Generic, None, Some(0)),
            event(NotifyType::Cancel, None, Some(1)),
            event(NotifyType::Reject, None, Some(1)),
        ] {
            assert!(decode_order_event(&payload, &account()).is_err());
        }
    }

    #[test]
    fn missing_status_uses_complete_fill_progress_when_quantity_is_present() {
        use protocol::exchange_order_notification::NotifyType;
        for (filled, expected) in [(1, "partially_filled"), (2, "filled")] {
            let payload = codec::encode(&protocol::ExchangeOrderNotification {
                template_id: EXCHANGE_ORDER_NOTIFICATION,
                notify_type: Some(NotifyType::Status as i32),
                is_snapshot: Some(false),
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                basket_id: Some("basket-1".to_string()),
                exchange: Some("CME".to_string()),
                symbol: Some("NQU6".to_string()),
                transaction_type: Some(
                    protocol::exchange_order_notification::TransactionType::Buy as i32,
                ),
                quantity: Some(2),
                total_fill_size: Some(filled),
                ..Default::default()
            })
            .unwrap();

            assert_eq!(
                decode_order_event(&payload, &account()).unwrap().status,
                expected,
            );
        }
    }

    #[test]
    fn live_event_preserves_bracket_identity_and_decimal_prices() {
        let payload = codec::encode(&protocol::ExchangeOrderNotification {
            template_id: EXCHANGE_ORDER_NOTIFICATION,
            notify_type: Some(protocol::exchange_order_notification::NotifyType::Status as i32),
            is_snapshot: Some(false),
            user_tag: Some("client-1".to_string()),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            window_name: Some("exit-window".to_string()),
            originator_window_name: Some("origin-window".to_string()),
            basket_id: Some("child-1".to_string()),
            original_basket_id: Some("parent-1".to_string()),
            linked_basket_ids: Some("child-2".to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            status: Some("OPEN".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Sell as i32,
            ),
            quantity: Some(1),
            price: Some(20002.25),
            trigger_price: Some(19998.25),
            price_type: Some(protocol::exchange_order_notification::PriceType::StopMarket as i32),
            bracket_type: Some(
                protocol::exchange_order_notification::BracketType::TargetAndStopStatic as i32,
            ),
            total_fill_size: Some(0),
            total_unfilled_size: Some(1),
            ..Default::default()
        })
        .unwrap();

        let event = decode_order_event(&payload, &account()).unwrap();
        assert_eq!(event.window_name.as_deref(), Some("exit-window"));
        assert_eq!(
            event.originator_window_name.as_deref(),
            Some("origin-window")
        );
        assert_eq!(event.original_basket_id.as_deref(), Some("parent-1"));
        assert_eq!(event.linked_basket_ids.as_deref(), Some("child-2"));
        assert_eq!(event.price, Some(dec!(20002.25)));
        assert_eq!(event.trigger_price, Some(dec!(19998.25)));
        assert_eq!(event.price_type.as_deref(), Some("stop_market"));
        assert_eq!(
            event.bracket_type.as_deref(),
            Some("target_and_stop_static")
        );
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

    #[test]
    fn live_event_fill_totals_fail_closed() {
        use protocol::exchange_order_notification::NotifyType;
        let event = |notify_type, status: &str, filled, unfilled| {
            codec::encode(&protocol::ExchangeOrderNotification {
                template_id: EXCHANGE_ORDER_NOTIFICATION,
                notify_type: Some(notify_type as i32),
                is_snapshot: Some(false),
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                basket_id: Some("basket-1".to_string()),
                exchange: Some("CME".to_string()),
                symbol: Some("NQU6".to_string()),
                status: Some(status.to_string()),
                transaction_type: Some(
                    protocol::exchange_order_notification::TransactionType::Buy as i32,
                ),
                quantity: Some(2),
                total_fill_size: Some(filled),
                total_unfilled_size: Some(unfilled),
                ..Default::default()
            })
            .unwrap()
        };

        for payload in [
            event(NotifyType::Status, "COMPLETE", 0, 0),
            event(NotifyType::Fill, "OPEN", 0, 2),
            event(NotifyType::Fill, "OPEN", 1, 0),
            event(NotifyType::Fill, "OPEN", 3, 0),
            event(NotifyType::Cancel, "COMPLETE", 2, 0),
            event(NotifyType::Reject, "COMPLETE", 2, 0),
        ] {
            assert!(decode_order_event(&payload, &account()).is_err());
        }
    }
}
