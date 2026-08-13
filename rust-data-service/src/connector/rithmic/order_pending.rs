use super::{
    ledger::{self, AccountIdentity, OrderSnapshot, OrderSnapshotEvent},
    order::{self, ExitPosition, OrderAck, ProtectionModification},
    order_event::OrderEvent,
    session::ResponseDisposition,
};
use anyhow::{bail, ensure, Context, Result};
use std::{sync::mpsc as std_mpsc, time::Instant};

#[derive(Clone, Copy)]
pub(super) enum SubmitKind {
    Plain,
    Bracket,
}

impl SubmitKind {
    fn is_response(self, template_id: i32) -> bool {
        match self {
            Self::Plain => order::is_new_order_response(template_id),
            Self::Bracket => order::is_bracket_order_response(template_id),
        }
    }
}

pub(super) enum Pending {
    Submit {
        kind: SubmitKind,
        request_key: String,
        client_order_id: String,
        basket_id: Option<String>,
        deadline: Instant,
        reply: std_mpsc::SyncSender<Result<OrderAck>>,
    },
    Cancel {
        request_key: String,
        basket_id: String,
        response_accepted: bool,
        terminal_seen: bool,
        deadline: Instant,
        reply: std_mpsc::SyncSender<Result<()>>,
    },
    Modify {
        request_key: String,
        modification: ProtectionModification,
        response_accepted: bool,
        event_seen: bool,
        deadline: Instant,
        reply: std_mpsc::SyncSender<Result<()>>,
    },
    ExitPosition {
        request_key: String,
        position: ExitPosition,
        deadline: Instant,
        reply: std_mpsc::SyncSender<Result<()>>,
    },
    Lookup {
        request_key: String,
        client_order_id: String,
        exchange: String,
        symbol: String,
        matches: Vec<OrderSnapshot>,
        deadline: Instant,
        reply: std_mpsc::SyncSender<Result<Option<OrderSnapshot>>>,
    },
}

pub(super) fn handle_response(
    payload: &[u8],
    template_id: i32,
    account: &AccountIdentity,
    pending: &mut Option<Pending>,
) -> Result<()> {
    if ledger::is_order_snapshot_response(template_id) {
        return complete_lookup(pending, payload, account);
    }
    if order::is_reject(template_id) {
        let (request_key, code) = order::decode_request_reject(payload)?;
        reject_matching_pending(pending, &request_key, &code);
        return Ok(());
    }
    match pending.take() {
        Some(Pending::Submit {
            kind,
            request_key,
            client_order_id,
            mut basket_id,
            deadline,
            reply,
        }) if kind.is_response(template_id) => {
            let response = match kind {
                SubmitKind::Plain => {
                    order::decode_new_order_response(payload, &request_key, &client_order_id)
                }
                SubmitKind::Bracket => {
                    order::decode_bracket_order_response(payload, &request_key, &client_order_id)
                }
            };
            match response {
                Ok(response) => {
                    if let Err(error) = merge_basket_id(&mut basket_id, response.basket_id) {
                        let _ =
                            reply.send(Err(error.context("Rithmic new-order result is ambiguous")));
                        return Ok(());
                    }
                    match response.disposition {
                        ResponseDisposition::Processing => {
                            *pending = Some(Pending::Submit {
                                kind,
                                request_key,
                                client_order_id,
                                basket_id,
                                deadline,
                                reply,
                            });
                        }
                        ResponseDisposition::Succeeded => {
                            let result = basket_id
                                .context("Rithmic new-order response omitted basket ID")
                                .map(|basket_id| OrderAck {
                                    client_order_id,
                                    basket_id,
                                })
                                .context("Rithmic new-order result is ambiguous");
                            let _ = reply.send(result);
                        }
                        ResponseDisposition::Failed(codes) => {
                            let _ = reply.send(Err(anyhow::anyhow!(
                                "Rithmic new-order response failed: {}",
                                codes.join(",")
                            )));
                        }
                    }
                }
                Err(error) => {
                    let _ = reply.send(Err(error.context("Rithmic new-order result is ambiguous")));
                }
            }
        }
        Some(Pending::Cancel {
            request_key,
            basket_id,
            mut response_accepted,
            terminal_seen,
            deadline,
            reply,
        }) if order::is_cancel_order_response(template_id) => {
            match order::decode_cancel_order_response(payload, &request_key, &basket_id) {
                Ok(response) => match response.disposition {
                    ResponseDisposition::Processing => {
                        *pending = Some(Pending::Cancel {
                            request_key,
                            basket_id,
                            response_accepted,
                            terminal_seen,
                            deadline,
                            reply,
                        });
                    }
                    ResponseDisposition::Succeeded => {
                        response_accepted = true;
                        complete_or_restore_cancel(
                            pending,
                            request_key,
                            basket_id,
                            response_accepted,
                            terminal_seen,
                            deadline,
                            reply,
                        );
                    }
                    ResponseDisposition::Failed(codes) => {
                        let _ = reply.send(Err(anyhow::anyhow!(
                            "Rithmic cancel-order response failed: {}",
                            codes.join(",")
                        )));
                    }
                },
                Err(error) => {
                    let _ = reply.send(Err(
                        error.context("Rithmic cancel-order result is ambiguous")
                    ));
                }
            }
        }
        Some(Pending::Modify {
            request_key,
            modification,
            mut response_accepted,
            event_seen,
            deadline,
            reply,
        }) if order::is_modify_order_response(template_id) => {
            match order::decode_modify_order_response(
                payload,
                &request_key,
                &modification.basket_id,
            ) {
                Ok(response) => match response.disposition {
                    ResponseDisposition::Processing => {
                        *pending = Some(Pending::Modify {
                            request_key,
                            modification,
                            response_accepted,
                            event_seen,
                            deadline,
                            reply,
                        });
                    }
                    ResponseDisposition::Succeeded => {
                        response_accepted = true;
                        complete_or_restore_modify(
                            pending,
                            request_key,
                            modification,
                            response_accepted,
                            event_seen,
                            deadline,
                            reply,
                        );
                    }
                    ResponseDisposition::Failed(codes) => {
                        let message = if event_seen {
                            format!(
                                "Rithmic modify-order result is ambiguous: event succeeded but response failed: {}",
                                codes.join(",")
                            )
                        } else {
                            format!("Rithmic modify-order response failed: {}", codes.join(","))
                        };
                        let _ = reply.send(Err(anyhow::anyhow!(message)));
                    }
                },
                Err(error) => {
                    let _ = reply.send(Err(
                        error.context("Rithmic modify-order result is ambiguous")
                    ));
                }
            }
        }
        Some(Pending::ExitPosition {
            request_key,
            position,
            deadline,
            reply,
        }) if order::is_exit_position_response(template_id) => {
            match order::decode_exit_position_response(payload, &request_key, &position) {
                Ok(ResponseDisposition::Processing) => {
                    *pending = Some(Pending::ExitPosition {
                        request_key,
                        position,
                        deadline,
                        reply,
                    });
                }
                Ok(ResponseDisposition::Succeeded) => {
                    let _ = reply.send(Ok(()));
                }
                Ok(ResponseDisposition::Failed(codes)) => {
                    let _ = reply.send(Err(anyhow::anyhow!(
                        "Rithmic exit-position response failed: {}",
                        codes.join(",")
                    )));
                }
                Err(error) => {
                    let _ = reply.send(Err(
                        error.context("Rithmic exit-position result is ambiguous")
                    ));
                }
            }
        }
        current => *pending = current,
    }
    Ok(())
}

fn merge_basket_id(current: &mut Option<String>, candidate: Option<String>) -> Result<()> {
    let Some(candidate) = candidate else {
        return Ok(());
    };
    if let Some(current) = current.as_deref() {
        ensure!(current == candidate, "Rithmic response basket ID changed");
    } else {
        *current = Some(candidate);
    }
    Ok(())
}

pub(super) fn update_pending_from_snapshot(
    pending: &mut Option<Pending>,
    payload: &[u8],
    account: &AccountIdentity,
) -> Result<()> {
    let Some(Pending::Lookup {
        request_key,
        client_order_id,
        exchange,
        symbol,
        mut matches,
        deadline,
        reply,
    }) = pending.take()
    else {
        bail!("unexpected Rithmic order snapshot notification");
    };
    let event = ledger::decode_order_snapshot_event(payload, &request_key, account)?;
    let OrderSnapshotEvent::Snapshot(snapshot) = event else {
        bail!("Rithmic order snapshot completed on notification template");
    };
    if snapshot.client_order_id.as_deref() == Some(client_order_id.as_str()) {
        ensure!(
            snapshot.exchange.eq_ignore_ascii_case(&exchange) && snapshot.symbol == symbol,
            "Rithmic lookup client ID matched a different instrument"
        );
        ensure!(
            matches.is_empty(),
            "duplicate Rithmic lookup client order ID"
        );
        matches.push(*snapshot);
    }
    *pending = Some(Pending::Lookup {
        request_key,
        client_order_id,
        exchange,
        symbol,
        matches,
        deadline,
        reply,
    });
    Ok(())
}

pub(super) fn complete_lookup(
    pending: &mut Option<Pending>,
    payload: &[u8],
    account: &AccountIdentity,
) -> Result<()> {
    let Some(Pending::Lookup {
        request_key,
        mut matches,
        reply,
        ..
    }) = pending.take()
    else {
        bail!("unexpected Rithmic order snapshot completion");
    };
    ensure!(
        ledger::decode_order_snapshot_event(payload, &request_key, account)?
            == OrderSnapshotEvent::RequestCompleted,
        "Rithmic lookup did not complete"
    );
    let _ = reply.send(Ok(matches.pop()));
    Ok(())
}

pub(super) fn update_pending_from_event(
    pending: &mut Option<Pending>,
    event: &OrderEvent,
) -> Result<()> {
    let Some(current) = pending.take() else {
        return Ok(());
    };
    match current {
        Pending::Modify {
            request_key,
            modification,
            response_accepted,
            event_seen,
            deadline,
            reply,
        } => {
            if event.basket_id != modification.basket_id {
                *pending = Some(Pending::Modify {
                    request_key,
                    modification,
                    response_accepted,
                    event_seen,
                    deadline,
                    reply,
                });
            } else if event.status == "modify_rejected" {
                let _ = reply.send(Err(anyhow::anyhow!("Rithmic modify was rejected")));
            } else if event.notification_type == "modify" {
                match validate_modify_event(event, &modification) {
                    Ok(()) => complete_or_restore_modify(
                        pending,
                        request_key,
                        modification,
                        response_accepted,
                        true,
                        deadline,
                        reply,
                    ),
                    Err(error) => {
                        let _ = reply.send(Err(error.context(
                            "Rithmic modify-order result is ambiguous: conflicting modify event",
                        )));
                    }
                }
            } else {
                *pending = Some(Pending::Modify {
                    request_key,
                    modification,
                    response_accepted,
                    event_seen,
                    deadline,
                    reply,
                });
            }
            Ok(())
        }
        Pending::Cancel {
            request_key,
            basket_id,
            response_accepted,
            terminal_seen,
            deadline,
            reply,
        } => {
            if event.basket_id != basket_id {
                *pending = Some(Pending::Cancel {
                    request_key,
                    basket_id,
                    response_accepted,
                    terminal_seen,
                    deadline,
                    reply,
                });
                return Ok(());
            }
            match event.status.as_str() {
                "cancelled" => complete_or_restore_cancel(
                    pending,
                    request_key,
                    basket_id,
                    response_accepted,
                    true,
                    deadline,
                    reply,
                ),
                "cancel_rejected" => {
                    let _ = reply.send(Err(anyhow::anyhow!("Rithmic cancel was rejected")));
                }
                "filled" | "rejected" => {
                    let _ = reply.send(Err(anyhow::anyhow!(
                        "Rithmic order became terminal before cancellation"
                    )));
                }
                _ => {
                    *pending = Some(Pending::Cancel {
                        request_key,
                        basket_id,
                        response_accepted,
                        terminal_seen,
                        deadline,
                        reply,
                    });
                }
            }
            Ok(())
        }
        other => {
            *pending = Some(other);
            Ok(())
        }
    }
}

fn complete_or_restore_modify(
    pending: &mut Option<Pending>,
    request_key: String,
    modification: ProtectionModification,
    response_accepted: bool,
    event_seen: bool,
    deadline: Instant,
    reply: std_mpsc::SyncSender<Result<()>>,
) {
    if response_accepted && event_seen {
        let _ = reply.send(Ok(()));
    } else {
        *pending = Some(Pending::Modify {
            request_key,
            modification,
            response_accepted,
            event_seen,
            deadline,
            reply,
        });
    }
}

fn validate_modify_event(event: &OrderEvent, modification: &ProtectionModification) -> Result<()> {
    ensure!(
        event.exchange.eq_ignore_ascii_case(&modification.exchange)
            && event.symbol == modification.symbol,
        "Rithmic modify event instrument mismatch"
    );
    ensure!(
        event.quantity == Some(modification.quantity),
        "Rithmic modify event quantity mismatch"
    );
    match modification.leg {
        order::ProtectionLeg::StopLoss => {
            ensure!(
                event.price_type.as_deref() == Some("stop_market")
                    && event.trigger_price == Some(modification.price),
                "Rithmic stop modification event mismatch"
            );
        }
        order::ProtectionLeg::TakeProfit => {
            ensure!(
                event.price_type.as_deref() == Some("limit")
                    && event.price == Some(modification.price),
                "Rithmic target modification event mismatch"
            );
        }
    }
    Ok(())
}

pub(super) fn complete_or_restore_cancel(
    pending: &mut Option<Pending>,
    request_key: String,
    basket_id: String,
    response_accepted: bool,
    terminal_seen: bool,
    deadline: Instant,
    reply: std_mpsc::SyncSender<Result<()>>,
) {
    if response_accepted && terminal_seen {
        let _ = reply.send(Ok(()));
    } else {
        *pending = Some(Pending::Cancel {
            request_key,
            basket_id,
            response_accepted,
            terminal_seen,
            deadline,
            reply,
        });
    }
}

fn reject_matching_pending(pending: &mut Option<Pending>, request_key: &str, code: &str) {
    let matches = match pending.as_ref() {
        Some(Pending::Submit {
            request_key: expected,
            ..
        })
        | Some(Pending::Cancel {
            request_key: expected,
            ..
        })
        | Some(Pending::Modify {
            request_key: expected,
            ..
        })
        | Some(Pending::ExitPosition {
            request_key: expected,
            ..
        })
        | Some(Pending::Lookup {
            request_key: expected,
            ..
        }) => expected == request_key,
        None => false,
    };
    if matches {
        let message = format!("Rithmic rejected order request with code {code}");
        fail_pending(pending, &message);
    }
}

pub(super) fn pending_expired(pending: &Pending) -> bool {
    let deadline = match pending {
        Pending::Submit { deadline, .. }
        | Pending::Cancel { deadline, .. }
        | Pending::Modify { deadline, .. }
        | Pending::ExitPosition { deadline, .. }
        | Pending::Lookup { deadline, .. } => deadline,
    };
    Instant::now() >= *deadline
}

pub(super) fn fail_pending(pending: &mut Option<Pending>, message: &str) {
    if let Some(pending) = pending.take() {
        match pending {
            Pending::Submit { reply, .. } => {
                let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
            }
            Pending::Cancel { reply, .. } => {
                let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
            }
            Pending::Modify { reply, .. } => {
                let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
            }
            Pending::ExitPosition { reply, .. } => {
                let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
            }
            Pending::Lookup { reply, .. } => {
                let _ = reply.send(Err(anyhow::anyhow!(message.to_string())));
            }
        }
    }
}
