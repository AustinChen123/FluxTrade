use super::{
    ledger::{self, AccountIdentity, OrderSnapshot, UserType},
    order::TradeRoute,
    order_command::{self, BracketOrder, ExitPosition, NewOrder, OrderAck, ProtectionModification},
    order_pending::{Pending, SubmitKind},
    transport::RithmicConnection,
};
use anyhow::{bail, ensure, Result};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as std_mpsc,
    },
    time::Instant,
};

pub(super) type Reply<T> = std_mpsc::SyncSender<Result<T>>;

pub(super) enum Command {
    Submit {
        order: NewOrder,
        deadline: Instant,
        reply: Reply<OrderAck>,
    },
    SubmitBracket {
        order: BracketOrder,
        deadline: Instant,
        reply: Reply<OrderAck>,
    },
    Modify {
        modification: ProtectionModification,
        deadline: Instant,
        reply: Reply<()>,
    },
    Cancel {
        basket_id: String,
        deadline: Instant,
        reply: Reply<()>,
    },
    ExitPosition {
        position: ExitPosition,
        deadline: Instant,
        reply: Reply<()>,
    },
    Lookup {
        client_order_id: String,
        exchange: String,
        symbol: String,
        deadline: Instant,
        reply: Reply<Option<OrderSnapshot>>,
    },
    Shutdown,
}

impl Command {
    pub(super) fn is_submission(&self) -> bool {
        matches!(
            self,
            Self::Submit { .. }
                | Self::SubmitBracket { .. }
                | Self::Modify { .. }
                | Self::ExitPosition { .. }
        )
    }
}

pub(super) async fn begin_command(
    connection: &mut RithmicConnection,
    account: &AccountIdentity,
    user_type: UserType,
    routes: &[TradeRoute],
    sequence: &AtomicU64,
    command: Command,
) -> Result<Option<Pending>> {
    match command {
        Command::Submit {
            order: new_order,
            deadline,
            reply,
        } => {
            if Instant::now() >= deadline {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic order command expired")));
                return Ok(None);
            }
            let route = match select_trade_route(routes, &new_order.exchange) {
                Ok(route) => route,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return Ok(None);
                }
            };
            let request_key = request_key("new", sequence);
            let payload =
                match order_command::new_order_request(&request_key, account, route, &new_order) {
                    Ok(payload) => payload,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return Ok(None);
                    }
                };
            let client_order_id = new_order.client_order_id;
            if let Err(error) = connection.send_payload(payload).await {
                let _ = reply.send(Err(anyhow::anyhow!(
                    "Rithmic new-order result is ambiguous: {error}"
                )));
                return Err(error);
            }
            Ok(Some(Pending::Submit {
                kind: SubmitKind::Plain,
                request_key,
                client_order_id,
                basket_id: None,
                deadline,
                reply,
            }))
        }
        Command::SubmitBracket {
            order: bracket_order,
            deadline,
            reply,
        } => {
            if Instant::now() >= deadline {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic bracket command expired")));
                return Ok(None);
            }
            let route = match select_trade_route(routes, &bracket_order.entry.exchange) {
                Ok(route) => route,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return Ok(None);
                }
            };
            let request_key = request_key("bracket", sequence);
            let payload = match order_command::bracket_order_request(
                &request_key,
                account,
                user_type,
                route,
                &bracket_order,
            ) {
                Ok(payload) => payload,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return Ok(None);
                }
            };
            let client_order_id = bracket_order.entry.client_order_id;
            if let Err(error) = connection.send_payload(payload).await {
                let _ = reply.send(Err(anyhow::anyhow!(
                    "Rithmic bracket-order result is ambiguous: {error}"
                )));
                return Err(error);
            }
            Ok(Some(Pending::Submit {
                kind: SubmitKind::Bracket,
                request_key,
                client_order_id,
                basket_id: None,
                deadline,
                reply,
            }))
        }
        Command::Modify {
            modification,
            deadline,
            reply,
        } => {
            if Instant::now() >= deadline {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic modify command expired")));
                return Ok(None);
            }
            let request_key = request_key("modify", sequence);
            let payload =
                match order_command::modify_order_request(&request_key, account, &modification) {
                    Ok(payload) => payload,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return Ok(None);
                    }
                };
            if let Err(error) = connection.send_payload(payload).await {
                let _ = reply.send(Err(anyhow::anyhow!(
                    "Rithmic modify-order result is ambiguous: {error}"
                )));
                return Err(error);
            }
            Ok(Some(Pending::Modify {
                request_key,
                modification,
                response_accepted: false,
                event_seen: false,
                deadline,
                reply,
            }))
        }
        Command::Cancel {
            basket_id,
            deadline,
            reply,
        } => {
            if Instant::now() >= deadline {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic cancel command expired")));
                return Ok(None);
            }
            let request_key = request_key("cancel", sequence);
            let payload =
                match order_command::cancel_order_request(&request_key, account, &basket_id) {
                    Ok(payload) => payload,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return Ok(None);
                    }
                };
            if let Err(error) = connection.send_payload(payload).await {
                let _ = reply.send(Err(anyhow::anyhow!(
                    "Rithmic cancel result is ambiguous: {error}"
                )));
                return Err(error);
            }
            Ok(Some(Pending::Cancel {
                request_key,
                basket_id,
                response_accepted: false,
                terminal_seen: false,
                deadline,
                reply,
            }))
        }
        Command::ExitPosition {
            position,
            deadline,
            reply,
        } => {
            if Instant::now() >= deadline {
                let _ = reply.send(Err(anyhow::anyhow!(
                    "Rithmic exit-position command expired"
                )));
                return Ok(None);
            }
            let request_key = request_key("exit-position", sequence);
            let payload =
                match order_command::exit_position_request(&request_key, account, &position) {
                    Ok(payload) => payload,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return Ok(None);
                    }
                };
            if let Err(error) = connection.send_payload(payload).await {
                let _ = reply.send(Err(anyhow::anyhow!(
                    "Rithmic exit-position result is ambiguous: {error}"
                )));
                return Err(error);
            }
            Ok(Some(Pending::ExitPosition {
                request_key,
                position,
                deadline,
                reply,
            }))
        }
        Command::Lookup {
            client_order_id,
            exchange,
            symbol,
            deadline,
            reply,
        } => {
            if Instant::now() >= deadline {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic lookup command expired")));
                return Ok(None);
            }
            if let Err(error) = validate_lookup_identity(&client_order_id, &exchange, &symbol) {
                let _ = reply.send(Err(error));
                return Ok(None);
            }
            let request_key = request_key("lookup", sequence);
            let payload = ledger::show_orders_request(&request_key, account)?;
            if let Err(error) = connection.send_payload(payload).await {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic order lookup failed: {error}")));
                return Err(error);
            }
            Ok(Some(Pending::Lookup {
                request_key,
                client_order_id,
                exchange,
                symbol,
                matches: Vec::new(),
                deadline,
                reply,
            }))
        }
        Command::Shutdown => Ok(None),
    }
}

pub(super) fn select_trade_route<'a>(routes: &'a [TradeRoute], exchange: &str) -> Result<&'a str> {
    let matching: Vec<_> = routes
        .iter()
        .filter(|route| route.exchange.eq_ignore_ascii_case(exchange))
        .collect();
    ensure!(
        !matching.is_empty(),
        "no open Rithmic trade route for exchange"
    );
    let defaults: Vec<_> = matching.iter().filter(|route| route.is_default).collect();
    match (defaults.as_slice(), matching.as_slice()) {
        ([route], _) => Ok(route.route.as_str()),
        ([], [route]) => Ok(route.route.as_str()),
        _ => bail!("ambiguous Rithmic trade route for exchange"),
    }
}

fn request_key(prefix: &str, sequence: &AtomicU64) -> String {
    format!(
        "fluxtrade-order-{prefix}-{}",
        sequence.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) fn reject_command(command: Command, message: &str) {
    match command {
        Command::Submit { reply, .. } | Command::SubmitBracket { reply, .. } => {
            let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
        }
        Command::Modify { reply, .. } | Command::Cancel { reply, .. } => {
            let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
        }
        Command::ExitPosition { reply, .. } => {
            let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
        }
        Command::Lookup { reply, .. } => {
            let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
        }
        Command::Shutdown => {}
    }
}

fn validate_lookup_identity(client_order_id: &str, exchange: &str, symbol: &str) -> Result<()> {
    ensure!(
        ![client_order_id, exchange, symbol]
            .iter()
            .any(|value| value.trim().is_empty()),
        "Rithmic lookup identity must not be empty"
    );
    Ok(())
}
