use super::{
    config,
    ledger::{self, AccountIdentity, OrderSnapshot, UserType},
    ledger_runtime::{discover_order_account_with_login, next_payload, wait_for_heartbeat},
    order::{self, TradeRoute, TradeRouteEvent},
    order_command::{self, BracketOrder, ExitPosition, NewOrder, OrderAck, ProtectionModification},
    order_event::{self, OrderEvent},
    order_pending::{self, fail_pending, pending_expired, Pending, SubmitKind},
    profile_lock::ProfileLease,
    session::Plant,
    transport::{self, ConnectionEvent, RithmicConnection},
};
use anyhow::{bail, ensure, Context, Result};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc as std_mpsc, Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::warn;

#[cfg(test)]
use super::order_pending::{
    complete_lookup, complete_or_restore_cancel, update_pending_from_event,
    update_pending_from_snapshot,
};

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(45);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_INITIAL: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const TRADE_ROUTES_KEY: &str = "fluxtrade-order-routes";
const SUBSCRIBE_KEY: &str = "fluxtrade-order-subscribe";

type Reply<T> = std_mpsc::SyncSender<Result<T>>;

enum Command {
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
    fn is_submission(&self) -> bool {
        matches!(
            self,
            Self::Submit { .. }
                | Self::SubmitBracket { .. }
                | Self::Modify { .. }
                | Self::ExitPosition { .. }
        )
    }
}

pub(crate) struct OrderRuntimeHandle {
    commands: mpsc::Sender<Command>,
    events: std_mpsc::Receiver<Result<OrderEvent>>,
    connected: Arc<AtomicBool>,
    // Monotonic counter incremented on every successful (re)connect. Lets the
    // Python owned-order recovery detect a reconnect without missing a fast
    // disconnect/reconnect flap the way a momentary `connected` bool would.
    generation: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
}

impl OrderRuntimeHandle {
    pub(crate) fn start(profile: String, account_id: Option<String>) -> Result<Self> {
        let _lease = ProfileLease::acquire(&profile)?;
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = std_mpsc::channel();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);
        let connected = Arc::new(AtomicBool::new(false));
        let thread_connected = Arc::clone(&connected);
        let generation = Arc::new(AtomicU64::new(0));
        let thread_generation = Arc::clone(&generation);
        let thread = thread::Builder::new()
            .name("rithmic-order-runtime".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(run(
                        profile,
                        account_id,
                        _lease,
                        command_rx,
                        event_tx,
                        thread_connected,
                        thread_generation,
                        ready_tx,
                    )),
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.into()));
                    }
                }
            })
            .context("failed to start Rithmic order runtime thread")?;

        match ready_rx.recv_timeout(STARTUP_TIMEOUT) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = thread.join();
                return Err(error);
            }
            Err(error) => {
                let _ = command_tx.blocking_send(Command::Shutdown);
                let _ = thread.join();
                return Err(error).context("Rithmic order runtime startup timed out");
            }
        }
        Ok(Self {
            commands: command_tx,
            events: event_rx,
            connected,
            generation,
            thread: Some(thread),
        })
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Number of successful (re)connects so far. A strictly increasing value
    /// between two observations means the session reconnected in between.
    pub(crate) fn connection_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn submit(&self, order: NewOrder) -> Result<OrderAck> {
        self.request(|deadline, reply| Command::Submit {
            order,
            deadline,
            reply,
        })
    }

    pub(crate) fn submit_bracket(&self, order: BracketOrder) -> Result<OrderAck> {
        self.request(|deadline, reply| Command::SubmitBracket {
            order,
            deadline,
            reply,
        })
    }

    pub(crate) fn modify(&self, modification: ProtectionModification) -> Result<()> {
        self.request(|deadline, reply| Command::Modify {
            modification,
            deadline,
            reply,
        })
    }

    pub(crate) fn cancel(&self, basket_id: String) -> Result<()> {
        self.request(|deadline, reply| Command::Cancel {
            basket_id,
            deadline,
            reply,
        })
    }

    pub(crate) fn exit_position(&self, position: ExitPosition) -> Result<()> {
        self.request(|deadline, reply| Command::ExitPosition {
            position,
            deadline,
            reply,
        })
    }

    pub(crate) fn lookup(
        &self,
        client_order_id: String,
        exchange: String,
        symbol: String,
    ) -> Result<Option<OrderSnapshot>> {
        self.request(|deadline, reply| Command::Lookup {
            client_order_id,
            exchange,
            symbol,
            deadline,
            reply,
        })
    }

    pub(crate) fn try_next_event(&self) -> Result<Option<OrderEvent>> {
        match self.events.try_recv() {
            Ok(event) => event.map(Some),
            Err(std_mpsc::TryRecvError::Empty) => Ok(None),
            Err(std_mpsc::TryRecvError::Disconnected) => {
                bail!("Rithmic order event stream disconnected")
            }
        }
    }

    fn request<T>(&self, command: impl FnOnce(Instant, Reply<T>) -> Command) -> Result<T> {
        ensure!(self.is_connected(), "Rithmic order runtime is disconnected");
        let deadline = Instant::now() + COMMAND_TIMEOUT;
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.commands
            .blocking_send(command(deadline, reply_tx))
            .map_err(|_| anyhow::anyhow!("Rithmic order runtime stopped"))?;
        reply_rx
            .recv_timeout(COMMAND_TIMEOUT)
            .context("Rithmic order command timed out")?
    }
}

impl Drop for OrderRuntimeHandle {
    fn drop(&mut self) {
        let _ = self.commands.blocking_send(Command::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    profile: String,
    account_id: Option<String>,
    _lease: ProfileLease,
    commands: mpsc::Receiver<Command>,
    events: std_mpsc::Sender<Result<OrderEvent>>,
    connected: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    ready: Reply<()>,
) {
    run_with_connector(
        _lease,
        commands,
        events,
        connected,
        generation,
        ready,
        move || {
            let profile = profile.clone();
            let account_id = account_id.clone();
            async move { connect_and_prepare(&profile, account_id.as_deref()).await }
        },
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_with_connector<C, F>(
    _lease: ProfileLease,
    mut commands: mpsc::Receiver<Command>,
    events: std_mpsc::Sender<Result<OrderEvent>>,
    connected: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    ready: Reply<()>,
    mut connect: C,
) where
    C: FnMut() -> F,
    F: Future<
        Output = Result<(
            RithmicConnection,
            AccountIdentity,
            UserType,
            Vec<TradeRoute>,
        )>,
    >,
{
    let mut ready = Some(ready);
    let mut backoff = RECONNECT_INITIAL;
    loop {
        match connect().await {
            Ok((mut connection, account, user_type, routes)) => {
                connected.store(true, Ordering::Release);
                let submissions_allowed = generation.fetch_add(1, Ordering::Release) == 0;
                backoff = RECONNECT_INITIAL;
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Ok(()));
                }
                if !run_connected(
                    &mut connection,
                    &account,
                    user_type,
                    &routes,
                    &mut commands,
                    &events,
                    submissions_allowed,
                )
                .await
                {
                    connected.store(false, Ordering::Release);
                    return;
                }
                connected.store(false, Ordering::Release);
            }
            Err(error) => {
                connected.store(false, Ordering::Release);
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error));
                    return;
                }
                warn!(%error, "Rithmic order runtime disconnected; reconnecting");
            }
        }

        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            command = commands.recv() => {
                match command {
                    Some(Command::Shutdown) | None => return,
                    Some(command) => reject_command(command, "Rithmic order runtime is reconnecting"),
                }
            }
        }
        backoff = backoff.saturating_mul(2).min(RECONNECT_MAX);
    }
}

async fn connect_and_prepare(
    profile: &str,
    account_id: Option<&str>,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    let runtime = config::load(profile, Plant::Order)?;
    connect_and_prepare_runtime(runtime, account_id).await
}

async fn connect_and_prepare_runtime(
    runtime: config::RuntimeConfig,
    account_id: Option<&str>,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    let mut connection = transport::connect(&runtime.url, runtime.login, RESPONSE_TIMEOUT).await?;
    wait_for_heartbeat(&mut connection, "ORDER").await?;
    let (account, login_info) =
        discover_order_account_with_login(&mut connection, account_id).await?;
    let account = account.identity;

    connection
        .send_payload(order::trade_routes_request(TRADE_ROUTES_KEY)?)
        .await?;
    let routes = collect_trade_routes(&mut connection).await?;
    ensure!(!routes.is_empty(), "Rithmic returned no open trade routes");

    connection
        .send_payload(order::subscribe_order_updates_request(
            SUBSCRIBE_KEY,
            &account,
        )?)
        .await?;
    let payload = tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(&mut connection))
        .await
        .context("Rithmic order-update subscription timed out")??;
    order::decode_subscribe_order_updates_response(&payload, SUBSCRIBE_KEY)?;
    Ok((connection, account, login_info.user_type, routes))
}

async fn collect_trade_routes(connection: &mut RithmicConnection) -> Result<Vec<TradeRoute>> {
    let mut routes = Vec::new();
    loop {
        let payload = tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(connection))
            .await
            .context("Rithmic trade-route request timed out")??;
        match order::decode_trade_route_event(&payload, TRADE_ROUTES_KEY)? {
            TradeRouteEvent::Route(route) => routes.push(route),
            TradeRouteEvent::Completed => return Ok(routes),
        }
    }
}

async fn run_connected(
    connection: &mut RithmicConnection,
    account: &AccountIdentity,
    user_type: UserType,
    routes: &[TradeRoute],
    commands: &mut mpsc::Receiver<Command>,
    events: &std_mpsc::Sender<Result<OrderEvent>>,
    submissions_allowed: bool,
) -> bool {
    let sequence = AtomicU64::new(1);
    let mut pending = None;
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Shutdown) | None => {
                    fail_pending(&mut pending, "Rithmic order runtime stopped");
                    return false;
                }
                Some(command) => {
                    if !submissions_allowed && command.is_submission() {
                        reject_command(
                            command,
                            "Rithmic order submission is blocked until reconciliation",
                        );
                        continue;
                    }
                    if pending.is_some() {
                        reject_command(command, "Rithmic order runtime is busy");
                        continue;
                    }
                    match begin_command(
                        connection,
                        account,
                        user_type,
                        routes,
                        &sequence,
                        command,
                    )
                    .await
                    {
                        Ok(next) => pending = next,
                        Err(error) => {
                            warn!(%error, "Rithmic order command write failed");
                            fail_pending(&mut pending, "Rithmic order command result is ambiguous");
                            return true;
                        }
                    }
                }
            },
            event = connection.next_event() => match event {
                Ok(ConnectionEvent::HeartbeatConfirmed) => {}
                Ok(ConnectionEvent::Payload(payload)) => {
                    if let Err(error) = handle_payload(payload, account, &mut pending, events) {
                        warn!(%error, "invalid Rithmic order payload; stopping runtime");
                        fail_pending(&mut pending, "Rithmic order command result is ambiguous");
                        let _ = events.send(Err(error.context(
                            "Rithmic order stream failed protocol validation",
                        )));
                        return false;
                    }
                }
                Err(error) => {
                    warn!(%error, "Rithmic order connection lost; reconnecting");
                    fail_pending(&mut pending, "Rithmic order command result is ambiguous");
                    return true;
                }
            },
            () = tokio::time::sleep(Duration::from_millis(100)), if pending.is_some() => {
                if pending.as_ref().is_some_and(pending_expired) {
                    fail_pending(&mut pending, "Rithmic order command timed out; result is ambiguous");
                    return true;
                }
            },
        }
    }
}

async fn begin_command(
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

fn handle_payload(
    payload: Vec<u8>,
    account: &AccountIdentity,
    pending: &mut Option<Pending>,
    events: &std_mpsc::Sender<Result<OrderEvent>>,
) -> Result<()> {
    let template_id = order::template_id(&payload)?;
    if order_event::is_order_event(template_id) {
        if order_event::notification_is_snapshot(&payload)? {
            return order_pending::update_pending_from_snapshot(pending, &payload, account);
        }
        let event = order_event::decode_order_event(&payload, account)?;
        order_pending::update_pending_from_event(pending, &event)?;
        events
            .send(Ok(event))
            .map_err(|_| anyhow::anyhow!("Rithmic order event receiver stopped"))?;
        return Ok(());
    }
    order_pending::handle_response(&payload, template_id, account, pending)
}
fn select_trade_route<'a>(routes: &'a [TradeRoute], exchange: &str) -> Result<&'a str> {
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

fn reject_command(command: Command, message: &str) {
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

#[cfg(test)]
mod tests {
    use super::super::ledger::TransactionType;
    use super::super::{codec, config::RuntimeConfig, protocol, session::LoginParameters};
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use rust_decimal_macros::dec;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message, WebSocketStream};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_increments_generation_after_full_order_startup() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut unexpected_template = None;
            for attempt in 0..2 {
                let mut socket = serve_order_startup(&listener).await;
                if attempt == 0 {
                    drop(socket);
                } else if let Ok(Some(Ok(Message::Binary(payload)))) =
                    timeout(Duration::from_secs(2), socket.next()).await
                {
                    unexpected_template = Some(codec::template_id(&payload).unwrap());
                }
            }
            unexpected_template
        });

        let login = LoginParameters::new(
            "test-user".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            Plant::Order,
        )
        .unwrap();
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = std_mpsc::channel();
        let connected = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(0));
        let (ready_tx, _ready_rx) = std_mpsc::sync_channel(1);
        let lease = ProfileLease::acquire("order-generation-loopback").unwrap();
        let runtime_generation = Arc::clone(&generation);
        let runtime = tokio::spawn(run_with_connector(
            lease,
            command_rx,
            event_tx,
            Arc::clone(&connected),
            runtime_generation,
            ready_tx,
            move || {
                let runtime = RuntimeConfig {
                    url: url.clone(),
                    login: login.clone(),
                };
                async move { connect_and_prepare_runtime(runtime, Some("ACCOUNT")).await }
            },
        ));

        timeout(Duration::from_secs(4), async {
            while generation.load(Ordering::Acquire) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let (submit_tx, submit_rx) = std_mpsc::sync_channel(1);
        command_tx
            .send(Command::Submit {
                order: NewOrder {
                    client_order_id: "blocked-after-reconnect".to_string(),
                    ..test_order()
                },
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: submit_tx,
            })
            .await
            .unwrap();
        let error = submit_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("blocked until reconciliation"));

        command_tx.send(Command::Shutdown).await.unwrap();
        timeout(Duration::from_secs(2), runtime)
            .await
            .unwrap()
            .unwrap();
        let unexpected_template = server.await.unwrap();

        assert_eq!(generation.load(Ordering::Acquire), 2);
        assert!(!connected.load(Ordering::Acquire));
        assert_eq!(unexpected_template, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protocol_validation_stops_runtime_and_fails_pending_without_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut socket = serve_order_startup(&listener).await;
            let command = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_template(command, 312);
            send(
                &mut socket,
                codec::encode(&protocol::ExchangeOrderNotification {
                    template_id: 352,
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
                    ..Default::default()
                })
                .unwrap(),
            )
            .await;
        });

        let login = LoginParameters::new(
            "test-user".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            Plant::Order,
        )
        .unwrap();
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = std_mpsc::channel();
        let connected = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(0));
        let (ready_tx, _ready_rx) = std_mpsc::sync_channel(1);
        let lease = ProfileLease::acquire("order-protocol-validation-loopback").unwrap();
        let runtime_generation = Arc::clone(&generation);
        let runtime = tokio::spawn(run_with_connector(
            lease,
            command_rx,
            event_tx,
            Arc::clone(&connected),
            runtime_generation,
            ready_tx,
            move || {
                let runtime = RuntimeConfig {
                    url: url.clone(),
                    login: login.clone(),
                };
                async move { connect_and_prepare_runtime(runtime, Some("ACCOUNT")).await }
            },
        ));

        timeout(Duration::from_secs(3), async {
            while generation.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let (submit_tx, submit_rx) = std_mpsc::sync_channel(1);
        command_tx
            .send(Command::Submit {
                order: test_order(),
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: submit_tx,
            })
            .await
            .unwrap();

        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap_err()
                .to_string(),
            "Rithmic order command result is ambiguous"
        );
        let error = event_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Rithmic order stream failed protocol validation"
        );
        assert!(format!("{error:#}").contains("missing Rithmic order notify type"));
        timeout(Duration::from_secs(2), runtime)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        assert_eq!(generation.load(Ordering::Acquire), 1);
        assert!(!connected.load(Ordering::Acquire));
    }

    #[test]
    fn pending_transition_implementation_has_one_module_owner() {
        let actor = include_str!("order_runtime.rs");
        let owner = include_str!("order_pending.rs");
        for name in [
            "handle_response",
            "merge_basket_id",
            "update_pending_from_snapshot",
            "complete_lookup",
            "update_pending_from_event",
            "complete_or_restore_modify",
            "validate_modify_event",
            "complete_or_restore_cancel",
            "reject_matching_pending",
            "pending_expired",
            "fail_pending",
        ] {
            let signature = format!("fn {name}(");
            assert!(owner.contains(&signature), "missing owner for {name}");
            assert!(
                !actor.contains(&signature),
                "duplicate actor owner for {name}"
            );
        }
    }

    #[test]
    fn reconnect_gate_blocks_only_submit_commands() {
        let (submit_tx, _) = std_mpsc::sync_channel(1);
        let (bracket_tx, _) = std_mpsc::sync_channel(1);
        let (modify_tx, _) = std_mpsc::sync_channel(1);
        let (cancel_tx, _) = std_mpsc::sync_channel(1);
        let (lookup_tx, _) = std_mpsc::sync_channel(1);
        let commands = [
            Command::Submit {
                order: test_order(),
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: submit_tx,
            },
            Command::SubmitBracket {
                order: BracketOrder {
                    entry: test_order(),
                    stop_ticks: Some(8),
                    target_ticks: Some(12),
                },
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: bracket_tx,
            },
            Command::Modify {
                modification: ProtectionModification {
                    basket_id: "child-1".to_string(),
                    exchange: "CME".to_string(),
                    symbol: "NQU6".to_string(),
                    quantity: dec!(1),
                    leg: order_command::ProtectionLeg::StopLoss,
                    price: dec!(19999.0),
                },
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: modify_tx,
            },
            Command::Cancel {
                basket_id: "basket-1".to_string(),
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: cancel_tx,
            },
            Command::Lookup {
                client_order_id: "client-1".to_string(),
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: lookup_tx,
            },
            Command::Shutdown,
        ];

        assert_eq!(
            commands.map(|command| command.is_submission()),
            [true, true, true, false, false, false]
        );
    }

    async fn serve_order_startup(listener: &TcpListener) -> WebSocketStream<TcpStream> {
        let mut socket = serve_handshake(listener).await;
        send(&mut socket, heartbeat_response()).await;

        assert_template(socket.next().await.unwrap().unwrap(), 300);
        send(
            &mut socket,
            codec::encode(&protocol::ResponseLoginInfo {
                template_id: 301,
                user_msg: vec!["fluxtrade-ledger-login".to_string()],
                rp_code: vec!["0".to_string()],
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                user_type: Some(3),
                ..Default::default()
            })
            .unwrap(),
        )
        .await;

        assert_template(socket.next().await.unwrap().unwrap(), 302);
        send(
            &mut socket,
            codec::encode(&protocol::ResponseAccountList {
                template_id: 303,
                user_msg: vec!["fluxtrade-ledger-accounts".to_string()],
                rq_handler_rp_code: vec!["0".to_string()],
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                ..Default::default()
            })
            .unwrap(),
        )
        .await;
        send(
            &mut socket,
            codec::encode(&protocol::ResponseAccountList {
                template_id: 303,
                user_msg: vec!["fluxtrade-ledger-accounts".to_string()],
                rp_code: vec!["0".to_string()],
                ..Default::default()
            })
            .unwrap(),
        )
        .await;

        assert_template(socket.next().await.unwrap().unwrap(), 310);
        send(
            &mut socket,
            codec::encode(&protocol::ResponseTradeRoutes {
                template_id: 311,
                user_msg: vec![TRADE_ROUTES_KEY.to_string()],
                rq_handler_rp_code: vec!["0".to_string()],
                exchange: Some("CME".to_string()),
                trade_route: Some("route".to_string()),
                status: Some("UP".to_string()),
                is_default: Some(true),
                ..Default::default()
            })
            .unwrap(),
        )
        .await;
        send(
            &mut socket,
            codec::encode(&protocol::ResponseTradeRoutes {
                template_id: 311,
                user_msg: vec![TRADE_ROUTES_KEY.to_string()],
                rp_code: vec!["0".to_string()],
                ..Default::default()
            })
            .unwrap(),
        )
        .await;

        assert_template(socket.next().await.unwrap().unwrap(), 308);
        send(
            &mut socket,
            codec::encode(&protocol::ResponseSubscribeForOrderUpdates {
                template_id: 309,
                user_msg: vec![SUBSCRIBE_KEY.to_string()],
                rp_code: vec!["0".to_string()],
            })
            .unwrap(),
        )
        .await;
        socket
    }

    async fn serve_handshake(listener: &TcpListener) -> WebSocketStream<TcpStream> {
        let (stream, _) = listener.accept().await.unwrap();
        let mut discovery = accept_async(stream).await.unwrap();
        assert_template(discovery.next().await.unwrap().unwrap(), 16);
        send(
            &mut discovery,
            codec::encode(&protocol::ResponseRithmicSystemInfo {
                template_id: 17,
                rp_code: vec!["0".to_string()],
                system_name: vec!["test-system".to_string()],
                ..Default::default()
            })
            .unwrap(),
        )
        .await;

        let (stream, _) = listener.accept().await.unwrap();
        let mut login = accept_async(stream).await.unwrap();
        assert_template(login.next().await.unwrap().unwrap(), 10);
        send(
            &mut login,
            codec::encode(&protocol::ResponseLogin {
                template_id: 11,
                rp_code: vec!["0".to_string()],
                heartbeat_interval: Some(30.0),
                ..Default::default()
            })
            .unwrap(),
        )
        .await;
        assert_template(login.next().await.unwrap().unwrap(), 18);
        login
    }

    fn heartbeat_response() -> Vec<u8> {
        codec::encode(&protocol::ResponseHeartbeat {
            template_id: 19,
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap()
    }

    async fn send(socket: &mut WebSocketStream<TcpStream>, payload: Vec<u8>) {
        socket.send(Message::Binary(payload.into())).await.unwrap();
    }

    fn assert_template(message: Message, expected: i32) {
        let Message::Binary(payload) = message else {
            panic!("expected binary Rithmic message");
        };
        assert_eq!(codec::template_id(&payload).unwrap(), expected);
    }

    #[derive(Clone, Copy)]
    enum PendingActorFailure {
        Shutdown,
        Timeout,
        ConnectionLoss,
    }

    async fn assert_actor_pending_failure(
        trigger: PendingActorFailure,
        expected_reason: &str,
        expected_generation: u64,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut socket = serve_order_startup(&listener).await;
            let command = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_template(command, 312);
            match trigger {
                PendingActorFailure::Shutdown => {
                    let _ = timeout(Duration::from_secs(2), socket.next()).await;
                    return;
                }
                PendingActorFailure::Timeout => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                PendingActorFailure::ConnectionLoss => {}
            }
            drop(socket);
            let mut socket = serve_order_startup(&listener).await;
            let _ = timeout(Duration::from_secs(2), socket.next()).await;
        });

        let login = LoginParameters::new(
            "test-user".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            Plant::Order,
        )
        .unwrap();
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = std_mpsc::channel();
        let connected = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(0));
        let (ready_tx, _ready_rx) = std_mpsc::sync_channel(1);
        let profile = match trigger {
            PendingActorFailure::Shutdown => "order-pending-shutdown-loopback",
            PendingActorFailure::Timeout => "order-pending-timeout-loopback",
            PendingActorFailure::ConnectionLoss => "order-pending-loss-loopback",
        };
        let lease = ProfileLease::acquire(profile).unwrap();
        let runtime_generation = Arc::clone(&generation);
        let runtime = tokio::spawn(run_with_connector(
            lease,
            command_rx,
            event_tx,
            Arc::clone(&connected),
            runtime_generation,
            ready_tx,
            move || {
                let runtime = RuntimeConfig {
                    url: url.clone(),
                    login: login.clone(),
                };
                async move { connect_and_prepare_runtime(runtime, Some("ACCOUNT")).await }
            },
        ));

        timeout(Duration::from_secs(3), async {
            while generation.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let (submit_tx, submit_rx) = std_mpsc::sync_channel(1);
        command_tx
            .send(Command::Submit {
                order: test_order(),
                deadline: match trigger {
                    PendingActorFailure::Timeout => Instant::now() + Duration::from_millis(20),
                    _ => Instant::now() + COMMAND_TIMEOUT,
                },
                reply: submit_tx,
            })
            .await
            .unwrap();
        if matches!(trigger, PendingActorFailure::Shutdown) {
            command_tx.send(Command::Shutdown).await.unwrap();
        }
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap_err()
                .to_string(),
            expected_reason
        );

        if !matches!(trigger, PendingActorFailure::Shutdown) {
            timeout(Duration::from_secs(4), async {
                while generation.load(Ordering::Acquire) < expected_generation {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            command_tx.send(Command::Shutdown).await.unwrap();
        }
        timeout(Duration::from_secs(2), runtime)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        assert_eq!(generation.load(Ordering::Acquire), expected_generation);
        assert!(!connected.load(Ordering::Acquire));
    }

    fn test_order() -> NewOrder {
        NewOrder {
            client_order_id: "client-1".to_string(),
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            quantity: dec!(1),
            price: Some(dec!(20000.25)),
            side: order_command::OrderSide::Buy,
            order_type: order_command::OrderType::Limit,
        }
    }

    fn route(exchange: &str, name: &str, is_default: bool) -> TradeRoute {
        TradeRoute {
            exchange: exchange.to_string(),
            route: name.to_string(),
            is_default,
        }
    }

    fn event(status: &str, basket_id: &str) -> OrderEvent {
        OrderEvent {
            account: AccountIdentity {
                fcm_id: "FCM".to_string(),
                ib_id: "IB".to_string(),
                account_id: "ACCOUNT".to_string(),
            },
            client_order_id: Some("client-1".to_string()),
            window_name: None,
            originator_window_name: None,
            basket_id: basket_id.to_string(),
            original_basket_id: None,
            linked_basket_ids: None,
            exchange_order_id: None,
            exchange: "CME".to_string(),
            symbol: "MNQU6".to_string(),
            status: status.to_string(),
            notification_type: "status".to_string(),
            transaction_type: TransactionType::Buy,
            quantity: Some(dec!(1)),
            price: None,
            trigger_price: None,
            price_type: None,
            bracket_type: None,
            last_fill_quantity: None,
            last_fill_price: None,
            cumulative_filled_quantity: None,
            cumulative_average_price: None,
            timestamp_ms: None,
        }
    }

    fn live_order_event_payload(
        notify_type: protocol::exchange_order_notification::NotifyType,
        basket_id: &str,
    ) -> Vec<u8> {
        codec::encode(&protocol::ExchangeOrderNotification {
            template_id: 352,
            notify_type: Some(notify_type as i32),
            is_snapshot: Some(false),
            user_tag: Some("client-1".to_string()),
            fcm_id: Some("FCM".to_string()),
            ib_id: Some("IB".to_string()),
            account_id: Some("ACCOUNT".to_string()),
            basket_id: Some(basket_id.to_string()),
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            status: Some("OPEN".to_string()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            total_fill_size: Some(0),
            total_unfilled_size: Some(1),
            ..Default::default()
        })
        .unwrap()
    }

    fn pending_cancel() -> (Option<Pending>, std_mpsc::Receiver<Result<()>>) {
        let (tx, rx) = std_mpsc::sync_channel(1);
        (
            Some(Pending::Cancel {
                request_key: "cancel-1".to_string(),
                basket_id: "basket-1".to_string(),
                response_accepted: false,
                terminal_seen: false,
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: tx,
            }),
            rx,
        )
    }

    fn pending_submit() -> (Option<Pending>, std_mpsc::Receiver<Result<OrderAck>>) {
        let (tx, rx) = std_mpsc::sync_channel(1);
        (
            Some(Pending::Submit {
                kind: SubmitKind::Plain,
                request_key: "new-1".to_string(),
                client_order_id: "client-1".to_string(),
                basket_id: None,
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: tx,
            }),
            rx,
        )
    }

    fn pending_bracket() -> (Option<Pending>, std_mpsc::Receiver<Result<OrderAck>>) {
        let (tx, rx) = std_mpsc::sync_channel(1);
        (
            Some(Pending::Submit {
                kind: SubmitKind::Bracket,
                request_key: "bracket-1".to_string(),
                client_order_id: "client-1".to_string(),
                basket_id: None,
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: tx,
            }),
            rx,
        )
    }

    fn pending_modify() -> (Option<Pending>, std_mpsc::Receiver<Result<()>>) {
        let (tx, rx) = std_mpsc::sync_channel(1);
        (
            Some(Pending::Modify {
                request_key: "modify-1".to_string(),
                modification: ProtectionModification {
                    basket_id: "child-1".to_string(),
                    exchange: "CME".to_string(),
                    symbol: "MNQU6".to_string(),
                    quantity: dec!(1),
                    leg: order_command::ProtectionLeg::StopLoss,
                    price: dec!(19999),
                },
                response_accepted: false,
                event_seen: false,
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: tx,
            }),
            rx,
        )
    }

    fn pending_exit_position() -> (Option<Pending>, std_mpsc::Receiver<Result<()>>) {
        let (tx, rx) = std_mpsc::sync_channel(1);
        (
            Some(Pending::ExitPosition {
                request_key: "exit-position-1".to_string(),
                position: ExitPosition {
                    exchange: "CME".to_string(),
                    symbol: "NQU6".to_string(),
                    window_name: Some("exit-window-1".to_string()),
                },
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: tx,
            }),
            rx,
        )
    }

    fn pending_lookup() -> (
        Option<Pending>,
        std_mpsc::Receiver<Result<Option<OrderSnapshot>>>,
    ) {
        let (tx, rx) = std_mpsc::sync_channel(1);
        (
            Some(Pending::Lookup {
                request_key: "lookup-1".to_string(),
                client_order_id: "client-1".to_string(),
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                matches: Vec::new(),
                deadline: Instant::now() + COMMAND_TIMEOUT,
                reply: tx,
            }),
            rx,
        )
    }

    fn assert_pending_failure<T>(
        mut pending: Option<Pending>,
        receiver: std_mpsc::Receiver<Result<T>>,
        expected: &str,
    ) {
        fail_pending(&mut pending, expected);
        assert!(pending.is_none());
        let Err(error) = receiver.recv().unwrap() else {
            panic!("pending command unexpectedly succeeded");
        };
        assert_eq!(error.to_string(), expected);
    }

    #[test]
    fn actor_transitions_pending_before_reporting_event_receiver_failure() {
        use protocol::exchange_order_notification::NotifyType;

        let account = event("open", "basket-1").account;
        let (event_tx, event_rx) = std_mpsc::channel();
        drop(event_rx);

        let (mut pending, reply_rx) = pending_cancel();
        if let Some(Pending::Cancel {
            response_accepted, ..
        }) = pending.as_mut()
        {
            *response_accepted = true;
        }
        let error = handle_payload(
            live_order_event_payload(NotifyType::Cancel, "basket-1"),
            &account,
            &mut pending,
            &event_tx,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "Rithmic order event receiver stopped");
        assert!(pending.is_none());
        assert!(reply_rx.recv().unwrap().is_ok());

        let (mut pending, reply_rx) = pending_cancel();
        let error = handle_payload(
            live_order_event_payload(NotifyType::Status, "unrelated-basket"),
            &account,
            &mut pending,
            &event_tx,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "Rithmic order event receiver stopped");
        assert!(matches!(pending, Some(Pending::Cancel { .. })));
        fail_pending(&mut pending, "Rithmic order command result is ambiguous");
        assert_eq!(
            reply_rx.recv().unwrap().unwrap_err().to_string(),
            "Rithmic order command result is ambiguous"
        );
    }

    #[test]
    fn matching_reject_fails_pending_while_foreign_reject_preserves_it() {
        let reject = |request_key: &str| {
            codec::encode(&protocol::Reject {
                template_id: 75,
                user_msg: vec![request_key.to_string()],
                rp_code: vec!["ORDER_REJECTED".to_string()],
            })
            .unwrap()
        };
        let account = event("open", "basket-1").account;
        let (event_tx, _event_rx) = std_mpsc::channel();
        let (mut pending, reply_rx) = pending_submit();

        handle_payload(reject("foreign-request"), &account, &mut pending, &event_tx).unwrap();
        assert!(matches!(pending, Some(Pending::Submit { .. })));
        assert!(matches!(
            reply_rx.try_recv(),
            Err(std_mpsc::TryRecvError::Empty)
        ));

        handle_payload(reject("new-1"), &account, &mut pending, &event_tx).unwrap();
        assert!(pending.is_none());
        assert_eq!(
            reply_rx.recv().unwrap().unwrap_err().to_string(),
            "Rithmic rejected order request with code ORDER_REJECTED"
        );
    }

    #[test]
    fn every_pending_variant_preserves_actor_failure_reasons() {
        for expected in [
            "Rithmic order runtime stopped",
            "Rithmic order command timed out; result is ambiguous",
            "Rithmic order command result is ambiguous",
        ] {
            let (pending, receiver) = pending_submit();
            assert_pending_failure(pending, receiver, expected);
            let (pending, receiver) = pending_cancel();
            assert_pending_failure(pending, receiver, expected);
            let (pending, receiver) = pending_modify();
            assert_pending_failure(pending, receiver, expected);
            let (pending, receiver) = pending_exit_position();
            assert_pending_failure(pending, receiver, expected);
            let (pending, receiver) = pending_lookup();
            assert_pending_failure(pending, receiver, expected);
        }
    }

    #[test]
    fn every_pending_variant_uses_its_own_deadline() {
        fn assert_deadline(mut pending: Option<Pending>) {
            assert!(!pending_expired(pending.as_ref().unwrap()));
            match pending.as_mut().unwrap() {
                Pending::Submit { deadline, .. }
                | Pending::Cancel { deadline, .. }
                | Pending::Modify { deadline, .. }
                | Pending::ExitPosition { deadline, .. }
                | Pending::Lookup { deadline, .. } => {
                    *deadline = Instant::now() - Duration::from_millis(1);
                }
            }
            assert!(pending_expired(pending.as_ref().unwrap()));
        }

        assert_deadline(pending_submit().0);
        assert_deadline(pending_cancel().0);
        assert_deadline(pending_modify().0);
        assert_deadline(pending_exit_position().0);
        assert_deadline(pending_lookup().0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_pending_failure_disposition_matrix_is_exact() {
        assert_actor_pending_failure(
            PendingActorFailure::Shutdown,
            "Rithmic order runtime stopped",
            1,
        )
        .await;
        assert_actor_pending_failure(
            PendingActorFailure::Timeout,
            "Rithmic order command timed out; result is ambiguous",
            2,
        )
        .await;
        assert_actor_pending_failure(
            PendingActorFailure::ConnectionLoss,
            "Rithmic order command result is ambiguous",
            2,
        )
        .await;
    }

    #[test]
    fn route_selection_fails_closed_for_missing_or_ambiguous_routes() {
        assert_eq!(
            select_trade_route(&[route("CME", "only", false)], "cme").unwrap(),
            "only"
        );
        assert_eq!(
            select_trade_route(
                &[
                    route("CME", "secondary", false),
                    route("CME", "primary", true),
                ],
                "CME",
            )
            .unwrap(),
            "primary"
        );
        assert!(select_trade_route(&[], "CME").is_err());
        assert!(select_trade_route(
            &[route("CME", "one", false), route("CME", "two", false)],
            "CME"
        )
        .is_err());
        assert!(select_trade_route(
            &[route("CME", "one", true), route("CME", "two", true)],
            "CME"
        )
        .is_err());
    }

    #[test]
    fn exit_position_waits_for_terminal_response_and_fails_closed() {
        let response = |handler: &[&str], terminal: &[&str]| {
            codec::encode(&protocol::ResponseExitPosition {
                template_id: 3505,
                user_msg: vec!["exit-position-1".to_string()],
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                exchange: Some("CME".to_string()),
                symbol: Some("NQU6".to_string()),
            })
            .unwrap()
        };
        let account = event("open", "basket-1").account;
        let (events, _event_rx) = std_mpsc::channel();

        let (mut pending, rx) = pending_exit_position();
        handle_payload(response(&["0"], &[]), &account, &mut pending, &events).unwrap();
        assert!(matches!(pending, Some(Pending::ExitPosition { .. })));
        assert!(matches!(rx.try_recv(), Err(std_mpsc::TryRecvError::Empty)));
        handle_payload(response(&[], &["0"]), &account, &mut pending, &events).unwrap();
        assert!(pending.is_none());
        assert!(rx.recv().unwrap().is_ok());

        let (mut pending, rx) = pending_exit_position();
        handle_payload(response(&[], &["9"]), &account, &mut pending, &events).unwrap();
        assert!(pending.is_none());
        assert!(rx.recv().unwrap().unwrap_err().to_string().contains("9"));
    }

    #[test]
    fn cancel_completion_matrix_requires_response_and_terminal_event() {
        for (response_first, expected) in [(true, true), (false, true)] {
            let (mut pending, rx) = pending_cancel();
            if response_first {
                if let Some(Pending::Cancel {
                    request_key,
                    basket_id,
                    reply,
                    ..
                }) = pending.take()
                {
                    complete_or_restore_cancel(
                        &mut pending,
                        request_key,
                        basket_id,
                        true,
                        false,
                        Instant::now() + COMMAND_TIMEOUT,
                        reply,
                    );
                }
                assert!(rx.try_recv().is_err());
                update_pending_from_event(&mut pending, &event("cancelled", "basket-1")).unwrap();
            } else {
                update_pending_from_event(&mut pending, &event("cancelled", "basket-1")).unwrap();
                assert!(rx.try_recv().is_err());
                if let Some(Pending::Cancel {
                    request_key,
                    basket_id,
                    reply,
                    terminal_seen,
                    ..
                }) = pending.take()
                {
                    complete_or_restore_cancel(
                        &mut pending,
                        request_key,
                        basket_id,
                        true,
                        terminal_seen,
                        Instant::now() + COMMAND_TIMEOUT,
                        reply,
                    );
                }
            }
            assert_eq!(rx.recv().unwrap().is_ok(), expected);
            assert!(pending.is_none());
        }
    }

    #[test]
    fn submit_waits_for_terminal_response_and_preserves_basket_id() {
        let response = |handler: &[&str], terminal: &[&str], basket_id: Option<&str>| {
            codec::encode(&protocol::ResponseNewOrder {
                template_id: 313,
                user_msg: vec!["new-1".to_string()],
                user_tag: Some("client-1".to_string()),
                basket_id: basket_id.map(str::to_string),
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                ..Default::default()
            })
            .unwrap()
        };
        let (mut pending, rx) = pending_submit();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "basket-1").account;

        handle_payload(
            response(&["0"], &[], Some("basket-1")),
            &account,
            &mut pending,
            &events,
        )
        .unwrap();
        assert!(rx.try_recv().is_err());
        assert!(matches!(pending, Some(Pending::Submit { .. })));

        handle_payload(response(&[], &["0"], None), &account, &mut pending, &events).unwrap();
        assert_eq!(rx.recv().unwrap().unwrap().basket_id, "basket-1");
        assert!(pending.is_none());
    }

    #[test]
    fn bracket_submit_uses_the_same_terminal_response_lifecycle() {
        let response = |handler: &[&str], terminal: &[&str], basket_id: Option<&str>| {
            codec::encode(&protocol::ResponseBracketOrder {
                template_id: 331,
                user_msg: vec!["bracket-1".to_string()],
                user_tag: Some("client-1".to_string()),
                basket_id: basket_id.map(str::to_string),
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                ..Default::default()
            })
            .unwrap()
        };
        let (mut pending, rx) = pending_bracket();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "basket-1").account;

        handle_payload(
            response(&["0"], &[], Some("basket-1")),
            &account,
            &mut pending,
            &events,
        )
        .unwrap();
        assert!(rx.try_recv().is_err());
        assert!(matches!(pending, Some(Pending::Submit { .. })));

        handle_payload(response(&[], &["0"], None), &account, &mut pending, &events).unwrap();
        assert_eq!(rx.recv().unwrap().unwrap().basket_id, "basket-1");
        assert!(pending.is_none());
    }

    #[test]
    fn modify_completion_requires_success_response_and_modify_event_in_either_order() {
        let response = codec::encode(&protocol::ResponseModifyOrder {
            template_id: 315,
            user_msg: vec!["modify-1".to_string()],
            basket_id: Some("child-1".to_string()),
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "child-1").account;

        for response_first in [true, false] {
            let (mut pending, rx) = pending_modify();
            let mut modify_event = event("open", "child-1");
            modify_event.notification_type = "modify".to_string();
            modify_event.price_type = Some("stop_market".to_string());
            modify_event.trigger_price = Some(dec!(19999));
            if response_first {
                handle_payload(response.clone(), &account, &mut pending, &events).unwrap();
                assert!(rx.try_recv().is_err());
                update_pending_from_event(&mut pending, &modify_event).unwrap();
            } else {
                update_pending_from_event(&mut pending, &modify_event).unwrap();
                assert!(rx.try_recv().is_err());
                handle_payload(response.clone(), &account, &mut pending, &events).unwrap();
            }
            assert!(rx.recv().unwrap().is_ok());
            assert!(pending.is_none());
        }
    }

    #[test]
    fn modify_rejection_completes_without_waiting_for_event() {
        let response = codec::encode(&protocol::ResponseModifyOrder {
            template_id: 315,
            user_msg: vec!["modify-1".to_string()],
            basket_id: Some("child-1".to_string()),
            rp_code: vec!["9".to_string()],
            ..Default::default()
        })
        .unwrap();
        let (mut pending, rx) = pending_modify();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "child-1").account;

        handle_payload(response, &account, &mut pending, &events).unwrap();

        assert!(rx.recv().unwrap().is_err());
        assert!(pending.is_none());
    }

    #[test]
    fn modify_rejects_a_conflicting_event_as_ambiguous() {
        let (mut pending, rx) = pending_modify();
        let mut modify_event = event("open", "child-1");
        modify_event.notification_type = "modify".to_string();
        modify_event.price_type = Some("stop_market".to_string());
        modify_event.trigger_price = Some(dec!(19998));

        update_pending_from_event(&mut pending, &modify_event).unwrap();

        assert!(rx
            .recv()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        assert!(pending.is_none());
    }

    #[test]
    fn modify_rejects_event_and_response_contradiction_as_ambiguous() {
        let response = codec::encode(&protocol::ResponseModifyOrder {
            template_id: 315,
            user_msg: vec!["modify-1".to_string()],
            basket_id: Some("child-1".to_string()),
            rp_code: vec!["9".to_string()],
            ..Default::default()
        })
        .unwrap();
        let (mut pending, rx) = pending_modify();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "child-1").account;
        let mut modify_event = event("open", "child-1");
        modify_event.notification_type = "modify".to_string();
        modify_event.price_type = Some("stop_market".to_string());
        modify_event.trigger_price = Some(dec!(19999));

        update_pending_from_event(&mut pending, &modify_event).unwrap();
        handle_payload(response, &account, &mut pending, &events).unwrap();

        assert!(rx
            .recv()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        assert!(pending.is_none());
    }

    #[test]
    fn submit_rejects_conflicting_basket_ids_as_ambiguous() {
        let response = |handler: &[&str], terminal: &[&str], basket_id: &str| {
            codec::encode(&protocol::ResponseNewOrder {
                template_id: 313,
                user_msg: vec!["new-1".to_string()],
                user_tag: Some("client-1".to_string()),
                basket_id: Some(basket_id.to_string()),
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                ..Default::default()
            })
            .unwrap()
        };
        let (mut pending, rx) = pending_submit();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "basket-1").account;

        handle_payload(
            response(&["0"], &[], "basket-1"),
            &account,
            &mut pending,
            &events,
        )
        .unwrap();
        handle_payload(
            response(&[], &["0"], "basket-2"),
            &account,
            &mut pending,
            &events,
        )
        .unwrap();

        assert!(rx
            .recv()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        assert!(pending.is_none());
    }

    #[test]
    fn cancel_processing_response_does_not_complete_request() {
        let response = |handler: &[&str], terminal: &[&str]| {
            codec::encode(&protocol::ResponseCancelOrder {
                template_id: 317,
                user_msg: vec!["cancel-1".to_string()],
                basket_id: Some("basket-1".to_string()),
                rq_handler_rp_code: handler.iter().map(|code| (*code).to_string()).collect(),
                rp_code: terminal.iter().map(|code| (*code).to_string()).collect(),
                ..Default::default()
            })
            .unwrap()
        };
        let (mut pending, rx) = pending_cancel();
        let (events, _) = std_mpsc::channel();
        let account = event("open", "basket-1").account;

        handle_payload(response(&["0"], &[]), &account, &mut pending, &events).unwrap();
        assert!(rx.try_recv().is_err());
        handle_payload(response(&[], &["0"]), &account, &mut pending, &events).unwrap();
        assert!(rx.try_recv().is_err());
        update_pending_from_event(&mut pending, &event("cancelled", "basket-1")).unwrap();
        assert!(rx.recv().unwrap().is_ok());
        assert!(pending.is_none());
    }

    #[test]
    fn unrelated_or_nonterminal_events_do_not_complete_cancel() {
        let (mut pending, rx) = pending_cancel();
        for candidate in [event("open", "basket-1"), event("cancelled", "other")] {
            update_pending_from_event(&mut pending, &candidate).unwrap();
            assert!(rx.try_recv().is_err());
            assert!(pending.is_some());
        }
    }

    #[test]
    fn live_events_do_not_destroy_an_inflight_lookup() {
        let (mut pending, rx) = pending_lookup();

        update_pending_from_event(&mut pending, &event("open", "basket-1")).unwrap();

        assert!(matches!(pending, Some(Pending::Lookup { .. })));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn cancel_rejection_is_terminal_failure() {
        let (mut pending, rx) = pending_cancel();
        update_pending_from_event(&mut pending, &event("cancel_rejected", "basket-1")).unwrap();
        assert!(rx.recv().unwrap().is_err());
        assert!(pending.is_none());
    }

    #[test]
    fn fill_while_cancel_is_pending_is_terminal_failure() {
        let (mut pending, rx) = pending_cancel();

        update_pending_from_event(&mut pending, &event("filled", "basket-1")).unwrap();

        assert!(rx.recv().unwrap().is_err());
        assert!(pending.is_none());
    }

    #[test]
    fn lookup_requires_one_exact_client_and_instrument_match() {
        let snapshot = |client_order_id: &str, symbol: &str| {
            codec::encode(&protocol::ExchangeOrderNotification {
                template_id: 352,
                is_snapshot: Some(true),
                user_tag: Some(client_order_id.to_string()),
                fcm_id: Some("FCM".to_string()),
                ib_id: Some("IB".to_string()),
                account_id: Some("ACCOUNT".to_string()),
                basket_id: Some("basket-1".to_string()),
                exchange: Some("CME".to_string()),
                symbol: Some(symbol.to_string()),
                status: Some("OPEN".to_string()),
                transaction_type: Some(
                    protocol::exchange_order_notification::TransactionType::Buy as i32,
                ),
                quantity: Some(1),
                total_fill_size: Some(0),
                total_unfilled_size: Some(1),
                ..Default::default()
            })
            .unwrap()
        };
        let completed = codec::encode(&protocol::ResponseShowOrders {
            template_id: 321,
            user_msg: vec!["lookup-1".to_string()],
            rp_code: vec!["0".to_string()],
        })
        .unwrap();

        let (mut pending, rx) = pending_lookup();
        update_pending_from_snapshot(
            &mut pending,
            &snapshot("other", "NQU6"),
            &event("open", "basket-1").account,
        )
        .unwrap();
        update_pending_from_snapshot(
            &mut pending,
            &snapshot("client-1", "NQU6"),
            &event("open", "basket-1").account,
        )
        .unwrap();
        complete_lookup(&mut pending, &completed, &event("open", "basket-1").account).unwrap();
        assert_eq!(rx.recv().unwrap().unwrap().unwrap().basket_id, "basket-1");
        assert!(pending.is_none());

        let (mut wrong_instrument, _) = pending_lookup();
        assert!(update_pending_from_snapshot(
            &mut wrong_instrument,
            &snapshot("client-1", "ESZ6"),
            &event("open", "basket-1").account,
        )
        .is_err());

        let (mut duplicate, _) = pending_lookup();
        let payload = snapshot("client-1", "NQU6");
        update_pending_from_snapshot(&mut duplicate, &payload, &event("open", "basket-1").account)
            .unwrap();
        assert!(update_pending_from_snapshot(
            &mut duplicate,
            &payload,
            &event("open", "basket-1").account,
        )
        .is_err());
    }
}
