use super::{
    codec,
    session::{
        is_fatal_session_error, is_retryable_session_error, LoginParameters, RithmicSession,
    },
};
use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, TimeZone, Timelike, Utc, Weekday};
use futures_util::{SinkExt, StreamExt};
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep_until, timeout, Instant};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{info, warn};

type RithmicSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type MaintenanceGuard = Arc<dyn Fn() -> bool + Send + Sync>;
const MAINTENANCE_POLL_DELAY: Duration = Duration::from_secs(15 * 60);
const POST_MAINTENANCE_VERIFICATION_ATTEMPTS: u8 = 3;
const DAILY_MAINTENANCE_START_MINUTE: u32 = 17 * 60 + 15;
const DAILY_MAINTENANCE_END_MINUTE: u32 = 17 * 60 + 50;
const SUNDAY_VERIFICATION_START_MINUTE: u32 = 12 * 60 + 15;

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct RetryableTransportFailure {
    #[source]
    source: anyhow::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("Rithmic maintenance active; provider I/O suppressed")]
struct MaintenanceSuppressed;

fn mark_retryable_transport(error: anyhow::Error) -> anyhow::Error {
    RetryableTransportFailure { source: error }.into()
}

pub(crate) fn is_retryable_transport_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|source| source.downcast_ref::<RetryableTransportFailure>().is_some())
}

pub(crate) fn is_retryable_connection_error(error: &anyhow::Error) -> bool {
    is_retryable_transport_error(error)
        || is_retryable_session_error(error)
        || error
            .chain()
            .any(|source| source.downcast_ref::<MaintenanceSuppressed>().is_some())
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct StableConnectionObserved {
    #[source]
    source: anyhow::Error,
}

fn mark_stable_connection(error: anyhow::Error, observed: bool) -> anyhow::Error {
    if observed {
        StableConnectionObserved { source: error }.into()
    } else {
        error
    }
}

pub(crate) fn error_after_stable_connection(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|source| source.downcast_ref::<StableConnectionObserved>().is_some())
}

#[cfg(test)]
pub(crate) fn mark_test_stable_connection(error: anyhow::Error) -> anyhow::Error {
    mark_stable_connection(error, true)
}

pub(crate) fn maintenance_retry_delay(now: DateTime<Utc>) -> Option<Duration> {
    maintenance_active(now).then_some(MAINTENANCE_POLL_DELAY)
}

pub(crate) fn maintenance_active(now: DateTime<Utc>) -> bool {
    let eastern_offset_hours = if eastern_daylight_saving_active(now) {
        -4
    } else {
        -5
    };
    let eastern = now + ChronoDuration::hours(eastern_offset_hours);
    let minute_of_day = eastern.hour() * 60 + eastern.minute();
    let daily_maintenance =
        (DAILY_MAINTENANCE_START_MINUTE..DAILY_MAINTENANCE_END_MINUTE).contains(&minute_of_day);
    match eastern.weekday() {
        Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu => daily_maintenance,
        Weekday::Fri => minute_of_day >= DAILY_MAINTENANCE_START_MINUTE,
        Weekday::Sat => true,
        Weekday::Sun => minute_of_day < SUNDAY_VERIFICATION_START_MINUTE,
    }
}

fn eastern_daylight_saving_active(now: DateTime<Utc>) -> bool {
    let year = now.year();
    let march_first = Utc.with_ymd_and_hms(year, 3, 1, 0, 0, 0).unwrap();
    let november_first = Utc.with_ymd_and_hms(year, 11, 1, 0, 0, 0).unwrap();
    let second_sunday_march = 1 + days_until_sunday(march_first.weekday()) + 7;
    let first_sunday_november = 1 + days_until_sunday(november_first.weekday());
    let starts = Utc
        .with_ymd_and_hms(year, 3, second_sunday_march, 7, 0, 0)
        .unwrap();
    let ends = Utc
        .with_ymd_and_hms(year, 11, first_sunday_november, 6, 0, 0)
        .unwrap();
    now >= starts && now < ends
}

fn days_until_sunday(weekday: Weekday) -> u32 {
    (7 - weekday.num_days_from_sunday()) % 7
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PayloadFailureKind {
    HandlerLockPayload,
    HandlerLockPreparation,
    ResetQueue,
    FrontMonthValidation,
    RolloverRequired,
    MarketDecode,
    MinuteBarInvariant,
    SubscriptionRejected,
    CandleQueueFull,
    CandleQueueClosed,
}

#[derive(Debug)]
pub(crate) struct PayloadFailure {
    kind: PayloadFailureKind,
    template_id: Option<i32>,
    payload_len: Option<usize>,
}

impl PayloadFailure {
    pub(crate) fn new(kind: PayloadFailureKind) -> Self {
        Self {
            kind,
            template_id: None,
            payload_len: None,
        }
    }

    pub(crate) fn attach_transport(&mut self, template_id: Option<i32>, payload_len: usize) {
        self.template_id = template_id;
        self.payload_len = Some(payload_len);
    }

    pub(crate) fn operation(&self) -> &'static str {
        use PayloadFailureKind::*;
        match self.kind {
            HandlerLockPreparation | ResetQueue => "prepare_connection",
            _ => "handle_payload",
        }
    }
    pub(crate) fn stage(&self) -> &'static str {
        use PayloadFailureKind::*;
        match self.kind {
            HandlerLockPayload | HandlerLockPreparation => "handler_lock",
            ResetQueue => "startup_reset",
            FrontMonthValidation | RolloverRequired => "front_month",
            MarketDecode => "market_decode",
            MinuteBarInvariant => "minute_bar",
            SubscriptionRejected => "subscription",
            CandleQueueFull | CandleQueueClosed => "candle_handoff",
        }
    }
    pub(crate) fn stable_error_code(&self) -> &'static str {
        use PayloadFailureKind::*;
        match self.kind {
            HandlerLockPayload | HandlerLockPreparation => "handler_lock_poisoned",
            ResetQueue => "reset_queue_unavailable",
            FrontMonthValidation => "front_month_validation_failed",
            RolloverRequired => "rollover_required",
            MarketDecode => "malformed_market_payload",
            MinuteBarInvariant => "minute_bar_invariant",
            SubscriptionRejected => "subscription_rejected",
            CandleQueueFull => "candle_queue_full",
            CandleQueueClosed => "candle_queue_closed",
        }
    }
    pub(crate) fn disposition(&self) -> &'static str {
        use PayloadFailureKind::*;
        match self.kind {
            FrontMonthValidation | RolloverRequired | SubscriptionRejected => "controlled_halt",
            _ => "fatal_service_exit",
        }
    }
    pub(crate) fn state_effect(&self) -> &'static str {
        use PayloadFailureKind::*;
        match self.kind {
            HandlerLockPayload => "mutation_unknown",
            HandlerLockPreparation => "preparation_state_unknown",
            ResetQueue => "builder_reset_downstream_not_notified",
            CandleQueueFull | CandleQueueClosed => "builder_advanced_candle_not_handed_off",
            _ => "none",
        }
    }
    pub(crate) fn safe_cause(&self) -> &'static str {
        use PayloadFailureKind::*;
        match self.kind {
            HandlerLockPayload | HandlerLockPreparation => "live handler lock unavailable",
            ResetQueue => "aggregation reset handoff failed",
            FrontMonthValidation => "front-month validation failed",
            RolloverRequired => "configured contract is not front month",
            MarketDecode => "market payload validation failed",
            MinuteBarInvariant => "minute-bar invariant failed",
            SubscriptionRejected => "market-data subscription rejected",
            CandleQueueFull => "candle forwarding queue full",
            CandleQueueClosed => "candle forwarding queue closed",
        }
    }
    pub(crate) fn template_id(&self) -> Option<i32> {
        self.template_id
    }
    pub(crate) fn payload_len(&self) -> Option<usize> {
        self.payload_len
    }
}

impl std::fmt::Display for PayloadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "component=rithmic task=live_market_data operation={} stage={} template_id={} payload_len={} stable_error_code={} disposition={} state_effect={} safe_cause={}", self.operation(), self.stage(), self.template_id.map_or_else(|| "unknown".to_string(), |value| value.to_string()), self.payload_len.map_or_else(|| "unknown".to_string(), |value| value.to_string()), self.stable_error_code(), self.disposition(), self.state_effect(), self.safe_cause())
    }
}

impl std::error::Error for PayloadFailure {}

pub(crate) fn is_controlled_halt(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<PayloadFailure>()
            .is_some_and(|failure| failure.disposition() == "controlled_halt")
    })
}

#[derive(Debug, PartialEq)]
enum IncomingMessage {
    Payload(Vec<u8>),
    ReplyPong(Vec<u8>),
    Ignore,
    Closed,
}

pub(crate) struct RithmicConnection {
    socket: RithmicSocket,
    session: RithmicSession,
    response_timeout: Duration,
    heartbeat_deadline: Instant,
    awaiting_heartbeat: bool,
    maintenance_active: MaintenanceGuard,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ConnectionEvent {
    HeartbeatConfirmed,
    Payload(Vec<u8>),
}

#[derive(Clone, Copy)]
pub(crate) struct ReconnectPolicy {
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl ReconnectPolicy {
    pub(crate) fn new(initial_backoff: Duration, max_backoff: Duration) -> Result<Self> {
        ensure!(
            !initial_backoff.is_zero(),
            "Rithmic reconnect initial_backoff must be positive"
        );
        ensure!(
            initial_backoff <= max_backoff,
            "Rithmic reconnect max_backoff must not be below initial_backoff"
        );
        Ok(Self {
            initial_backoff,
            max_backoff,
        })
    }
}

pub(crate) struct ReconnectRuntimeConfig<N> {
    response_timeout: Duration,
    policy: ReconnectPolicy,
    maintenance_now: N,
    maintenance_poll_delay: Duration,
}

impl<N> ReconnectRuntimeConfig<N> {
    pub(crate) fn new(
        response_timeout: Duration,
        policy: ReconnectPolicy,
        maintenance_now: N,
    ) -> Self {
        Self {
            response_timeout,
            policy,
            maintenance_now,
            maintenance_poll_delay: MAINTENANCE_POLL_DELAY,
        }
    }

    #[cfg(test)]
    fn with_maintenance_poll_delay(mut self, delay: Duration) -> Self {
        self.maintenance_poll_delay = delay;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionPreparation {
    Startup,
    Retry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PayloadOutcome {
    Handled,
    RuntimeVerified,
}

#[derive(Default)]
pub(crate) struct MaintenanceRecoveryBudget {
    verification_required: bool,
    failed_attempts: u8,
}

impl MaintenanceRecoveryBudget {
    pub(crate) fn observe_maintenance(&mut self) {
        self.verification_required = true;
        self.failed_attempts = 0;
    }

    pub(crate) fn connection_stable(&mut self) {
        self.verification_required = false;
        self.failed_attempts = 0;
    }

    pub(crate) fn transport_failure_exhausted(&mut self) -> bool {
        if !self.verification_required {
            return false;
        }
        self.failed_attempts = self.failed_attempts.saturating_add(1);
        self.failed_attempts >= POST_MAINTENANCE_VERIFICATION_ATTEMPTS
    }
}

pub(crate) async fn run_with_reconnect<F, P, N>(
    url: &str,
    login: LoginParameters,
    startup_payloads: Vec<Vec<u8>>,
    mut prepare_connection: P,
    mut handle_payload: F,
    runtime: ReconnectRuntimeConfig<N>,
) -> Result<()>
where
    F: FnMut(Vec<u8>) -> Result<PayloadOutcome>,
    P: FnMut(ConnectionPreparation) -> Result<()>,
    N: Fn() -> DateTime<Utc> + Send + Sync + 'static,
{
    let ReconnectRuntimeConfig {
        response_timeout,
        policy,
        maintenance_now,
        maintenance_poll_delay,
    } = runtime;
    let maintenance_now = Arc::new(maintenance_now);
    let mut backoffs = ReconnectBackoffs::new(policy);
    let mut maintenance_recovery = MaintenanceRecoveryBudget::default();
    let mut stable_connection_observed = false;

    loop {
        if maintenance_active(maintenance_now()) {
            maintenance_recovery.observe_maintenance();
            info!(
                delay_seconds = maintenance_poll_delay.as_secs_f64(),
                "Rithmic maintenance active; provider connection suppressed"
            );
            tokio::time::sleep(maintenance_poll_delay).await;
            continue;
        }
        let guard_clock = Arc::clone(&maintenance_now);
        let connection_guard: MaintenanceGuard =
            Arc::new(move || maintenance_active(guard_clock()));
        let retry_cause = match connect_with_maintenance_guard(
            url,
            login.clone(),
            response_timeout,
            connection_guard,
        )
        .await
        {
            Ok(mut connection) => {
                let mut startup_pending = true;
                let mut heartbeat_confirmations = 0_u32;
                'connected: loop {
                    if maintenance_active(maintenance_now()) {
                        info!("Rithmic maintenance started; closing active provider session");
                        break 'connected RetryCause::Maintenance;
                    }
                    match connection
                        .next_event_guarded(|| maintenance_active(maintenance_now()))
                        .await
                    {
                        Ok(ConnectionEvent::HeartbeatConfirmed) => {
                            heartbeat_confirmations = heartbeat_confirmations.saturating_add(1);
                            if startup_pending {
                                if maintenance_active(maintenance_now()) {
                                    break 'connected RetryCause::Maintenance;
                                }
                                prepare_connection(ConnectionPreparation::Startup)
                                    .context("Rithmic startup preparation failed")?;
                                for payload in &startup_payloads {
                                    if maintenance_active(maintenance_now()) {
                                        break 'connected RetryCause::Maintenance;
                                    }
                                    let template_id = codec::template_id(payload)
                                        .context("invalid Rithmic startup payload")?;
                                    if let Err(error) =
                                        connection.send_payload(payload.clone()).await
                                    {
                                        warn!(%error, "Rithmic startup write failed; reconnecting");
                                        break 'connected RetryCause::Transport;
                                    }
                                    info!(
                                        template_id,
                                        payload_len = payload.len(),
                                        "Rithmic startup payload sent"
                                    );
                                }
                                startup_pending = false;
                            }
                            if heartbeat_establishes_stability(heartbeat_confirmations) {
                                backoffs.connection_stable();
                            }
                        }
                        Ok(ConnectionEvent::Payload(payload)) => {
                            match handle_payload_with_diagnostics(payload, &mut handle_payload) {
                                Ok(outcome) => observe_payload_outcome(
                                    outcome,
                                    &mut stable_connection_observed,
                                    &mut maintenance_recovery,
                                ),
                                Err(error) => {
                                    return Err(mark_stable_connection(
                                        error,
                                        stable_connection_observed,
                                    ));
                                }
                            }
                        }
                        Err(error) => {
                            if maintenance_active(maintenance_now()) {
                                break 'connected RetryCause::Maintenance;
                            }
                            let classified = classify_connection_error(
                                error,
                                "fatal Rithmic session failure",
                                "Rithmic connection lost; reconnecting",
                            );
                            match classified {
                                Ok(cause) => break 'connected cause,
                                Err(error) => {
                                    return Err(mark_stable_connection(
                                        error,
                                        stable_connection_observed,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            Err(_error) if maintenance_active(maintenance_now()) => RetryCause::Maintenance,
            Err(error) => match classify_connection_error(
                error,
                "fatal Rithmic handshake failure",
                "Rithmic connection failed; reconnecting",
            ) {
                Ok(cause) => cause,
                Err(error) => {
                    return Err(mark_stable_connection(error, stable_connection_observed));
                }
            },
        };

        if retry_cause == RetryCause::Maintenance {
            prepare_connection(ConnectionPreparation::Retry)
                .context("Rithmic maintenance preparation failed")?;
            continue;
        }

        let delay = backoffs.next_delay(retry_cause);
        prepare_connection(ConnectionPreparation::Retry)
            .context("Rithmic retry preparation failed")?;
        if maintenance_recovery.transport_failure_exhausted() {
            warn!(
                attempts = POST_MAINTENANCE_VERIFICATION_ATTEMPTS,
                runtime_state = "not_ready",
                provider_io = "quiescent",
                "Rithmic post-maintenance verification exhausted; provider I/O quiescent until service restart"
            );
            std::future::pending::<()>().await;
            unreachable!("pending maintenance recovery unexpectedly completed");
        }
        tokio::time::sleep(delay).await;
    }
}

fn observe_payload_outcome(
    outcome: PayloadOutcome,
    stable_connection_observed: &mut bool,
    maintenance_recovery: &mut MaintenanceRecoveryBudget,
) {
    if outcome == PayloadOutcome::RuntimeVerified {
        *stable_connection_observed = true;
        maintenance_recovery.connection_stable();
    }
}

fn handle_payload_with_diagnostics<F>(
    payload: Vec<u8>,
    handle_payload: &mut F,
) -> Result<PayloadOutcome>
where
    F: FnMut(Vec<u8>) -> Result<PayloadOutcome>,
{
    let payload_len = payload.len();
    let template_id = codec::template_id(&payload).ok();
    match handle_payload(payload) {
        Ok(outcome) => Ok(outcome),
        Err(mut error) => {
            if let Some(failure) = error.downcast_mut::<PayloadFailure>() {
                failure.attach_transport(template_id, payload_len);
                return Err(error);
            }
            Err(error.context(format!(
                "Rithmic payload handler failed: template_id={} payload_len={payload_len}",
                template_id.map_or_else(|| "unknown".to_string(), |value| value.to_string())
            )))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RetryCause {
    Transport,
    Maintenance,
}

struct ReconnectBackoffs {
    policy: ReconnectPolicy,
    transport: Duration,
}

impl ReconnectBackoffs {
    fn new(policy: ReconnectPolicy) -> Self {
        Self {
            transport: policy.initial_backoff,
            policy,
        }
    }

    fn connection_stable(&mut self) {
        self.transport = self.policy.initial_backoff;
    }

    fn next_delay(&mut self, cause: RetryCause) -> Duration {
        let backoff = match cause {
            RetryCause::Transport => &mut self.transport,
            RetryCause::Maintenance => return Duration::ZERO,
        };
        let delay = *backoff;
        *backoff = next_backoff(*backoff, self.policy.max_backoff);
        delay
    }
}

fn classify_connection_error(
    error: anyhow::Error,
    fatal_context: &str,
    retry_message: &str,
) -> Result<RetryCause> {
    if is_fatal_session_error(&error) {
        return Err(error.context(fatal_context.to_string()));
    }
    if !is_retryable_connection_error(&error) {
        return Err(error.context(fatal_context.to_string()));
    }
    warn!(%error, "{retry_message}");
    Ok(RetryCause::Transport)
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

pub(crate) fn heartbeat_establishes_stability(confirmations: u32) -> bool {
    confirmations >= 2
}

pub(crate) async fn connect(
    url: &str,
    login: LoginParameters,
    response_timeout: Duration,
) -> Result<RithmicConnection> {
    connect_with_maintenance_guard(url, login, response_timeout, Arc::new(|| false)).await
}

pub(crate) async fn connect_with_maintenance_guard(
    url: &str,
    login: LoginParameters,
    response_timeout: Duration,
    maintenance_active: MaintenanceGuard,
) -> Result<RithmicConnection> {
    let mut session = RithmicSession::new(login);

    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    let (mut discovery, _) = match timeout(response_timeout, connect_async(url)).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => return Err(mark_retryable_transport(error.into())),
        Err(error) => {
            return Err(mark_retryable_transport(
                anyhow::Error::new(error).context("Rithmic system-info connection timed out"),
            ));
        }
    };
    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    send_binary(
        &mut discovery,
        session.begin_system_info()?,
        response_timeout,
    )
    .await?;
    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    let response = receive_binary_guarded(
        &mut discovery,
        response_timeout,
        maintenance_active.as_ref(),
    )
    .await?;
    session.reject_terminal(&response)?;
    session.accept_system_info(&response)?;
    drop(discovery);

    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    let (mut socket, _) = match timeout(response_timeout, connect_async(url)).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => return Err(mark_retryable_transport(error.into())),
        Err(error) => {
            return Err(mark_retryable_transport(
                anyhow::Error::new(error).context("Rithmic login connection timed out"),
            ));
        }
    };
    session.mark_reconnected()?;
    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    send_binary(&mut socket, session.begin_login()?, response_timeout).await?;
    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    let response =
        receive_binary_guarded(&mut socket, response_timeout, maintenance_active.as_ref()).await?;
    session.reject_terminal(&response)?;
    let initial_heartbeat = session.accept_login(&response)?;
    ensure_provider_io_allowed(maintenance_active.as_ref())?;
    send_binary(&mut socket, initial_heartbeat, response_timeout).await?;

    Ok(RithmicConnection {
        socket,
        session,
        response_timeout,
        heartbeat_deadline: deadline_after(response_timeout)?,
        awaiting_heartbeat: true,
        maintenance_active,
    })
}

fn ensure_provider_io_allowed<N>(maintenance_active: &N) -> Result<()>
where
    N: Fn() -> bool + ?Sized,
{
    if maintenance_active() {
        return Err(MaintenanceSuppressed.into());
    }
    Ok(())
}

impl RithmicConnection {
    pub(crate) async fn send_payload(&mut self, payload: Vec<u8>) -> Result<()> {
        ensure_provider_io_allowed(self.maintenance_active.as_ref())?;
        send_binary(&mut self.socket, payload, self.response_timeout).await
    }

    pub(crate) async fn next_event(&mut self) -> Result<ConnectionEvent> {
        self.next_event_guarded(|| false).await
    }

    pub(crate) async fn next_event_guarded<N>(
        &mut self,
        maintenance_active: N,
    ) -> Result<ConnectionEvent>
    where
        N: Fn() -> bool,
    {
        let connection_guard = Arc::clone(&self.maintenance_active);
        self.next_event_guarded_inner(move || connection_guard() || maintenance_active())
            .await
    }

    async fn next_event_guarded_inner<N>(
        &mut self,
        maintenance_active: N,
    ) -> Result<ConnectionEvent>
    where
        N: Fn() -> bool,
    {
        loop {
            ensure_provider_io_allowed(&maintenance_active)?;
            let message = tokio::select! {
                message = self.socket.next() => Some(message),
                () = sleep_until(self.heartbeat_deadline) => None,
            };
            ensure_provider_io_allowed(&maintenance_active)?;

            if let Some(message) = message {
                let message = match message {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => {
                        return Err(mark_retryable_transport(error.into()));
                    }
                    None => {
                        return Err(mark_retryable_transport(anyhow::anyhow!(
                            "Rithmic connection ended"
                        )));
                    }
                };
                match classify_message(message)? {
                    IncomingMessage::Payload(payload) => {
                        if self.session.accept_control(&payload)? {
                            ensure!(
                                self.awaiting_heartbeat,
                                "Rithmic sent an unexpected heartbeat response"
                            );
                            self.awaiting_heartbeat = false;
                            self.heartbeat_deadline =
                                deadline_after(self.session.heartbeat_interval()?)?;
                            return Ok(ConnectionEvent::HeartbeatConfirmed);
                        }
                        return Ok(ConnectionEvent::Payload(payload));
                    }
                    IncomingMessage::ReplyPong(payload) => {
                        ensure!(
                            !maintenance_active(),
                            "Rithmic maintenance started before Pong write"
                        );
                        await_write(
                            self.socket.send(Message::Pong(payload.into())),
                            self.response_timeout,
                            "Rithmic Pong write",
                        )
                        .await?;
                    }
                    IncomingMessage::Ignore => {}
                    IncomingMessage::Closed => {
                        return Err(mark_retryable_transport(anyhow::anyhow!(
                            "Rithmic connection closed"
                        )));
                    }
                }
                continue;
            }

            if self.awaiting_heartbeat {
                return Err(mark_retryable_transport(anyhow::anyhow!(
                    "Rithmic heartbeat response timed out"
                )));
            }
            ensure!(
                !maintenance_active(),
                "Rithmic maintenance started before heartbeat write"
            );
            send_binary(
                &mut self.socket,
                self.session.heartbeat()?,
                self.response_timeout,
            )
            .await?;
            self.awaiting_heartbeat = true;
            self.heartbeat_deadline = deadline_after(self.response_timeout)?;
        }
    }

    #[cfg(test)]
    fn state(&self) -> super::session::SessionState {
        self.session.state()
    }
}

fn deadline_after(delay: Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(delay)
        .context("Rithmic timer delay exceeds Instant range")
}

async fn send_binary(socket: &mut RithmicSocket, payload: Vec<u8>, wait: Duration) -> Result<()> {
    await_write(
        socket.send(Message::Binary(payload.into())),
        wait,
        "Rithmic binary write",
    )
    .await
}

async fn receive_binary(socket: &mut RithmicSocket, wait: Duration) -> Result<Vec<u8>> {
    receive_binary_guarded(socket, wait, &|| false).await
}

async fn receive_binary_guarded<N>(
    socket: &mut RithmicSocket,
    wait: Duration,
    maintenance_active: &N,
) -> Result<Vec<u8>>
where
    N: Fn() -> bool + ?Sized,
{
    match timeout(
        wait,
        receive_protocol_payload_guarded(socket, wait, maintenance_active),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => Err(mark_retryable_transport(
            anyhow::Error::new(error).context("Rithmic response timed out"),
        )),
    }
}

async fn receive_protocol_payload(socket: &mut RithmicSocket, wait: Duration) -> Result<Vec<u8>> {
    receive_protocol_payload_guarded(socket, wait, &|| false).await
}

async fn receive_protocol_payload_guarded<N>(
    socket: &mut RithmicSocket,
    wait: Duration,
    maintenance_active: &N,
) -> Result<Vec<u8>>
where
    N: Fn() -> bool + ?Sized,
{
    while let Some(message) = socket.next().await {
        let message = message.map_err(|error| mark_retryable_transport(error.into()))?;
        match classify_message(message)? {
            IncomingMessage::Payload(payload) => return Ok(payload),
            IncomingMessage::ReplyPong(payload) => {
                ensure_provider_io_allowed(maintenance_active)?;
                await_write(
                    socket.send(Message::Pong(payload.into())),
                    wait,
                    "Rithmic Pong write",
                )
                .await?
            }
            IncomingMessage::Ignore => {}
            IncomingMessage::Closed => {
                return Err(mark_retryable_transport(anyhow::anyhow!(
                    "Rithmic connection closed"
                )));
            }
        }
    }
    Err(mark_retryable_transport(anyhow::anyhow!(
        "Rithmic connection ended"
    )))
}

async fn await_write<F, E>(write: F, wait: Duration, operation: &str) -> Result<()>
where
    F: Future<Output = std::result::Result<(), E>>,
    E: Error + Send + Sync + 'static,
{
    match timeout(wait, write).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(mark_retryable_transport(anyhow::Error::new(error))),
        Err(error) => Err(mark_retryable_transport(
            anyhow::Error::new(error).context(format!("{operation} timed out")),
        )),
    }
}

fn classify_message(message: Message) -> Result<IncomingMessage> {
    match message {
        Message::Binary(payload) => Ok(IncomingMessage::Payload(payload.to_vec())),
        Message::Ping(payload) => Ok(IncomingMessage::ReplyPong(payload.to_vec())),
        Message::Pong(_) => Ok(IncomingMessage::Ignore),
        Message::Close(_) => Ok(IncomingMessage::Closed),
        _ => bail!("Rithmic sent a non-binary protocol message"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::session::{Plant, RithmicSession, SessionState};
    use super::super::{codec, protocol};
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    fn utc(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn retry_and_stability_markers_remain_independent() {
        let retryable = mark_retryable_transport(anyhow::anyhow!("socket unavailable"));
        assert!(is_retryable_transport_error(&retryable));
        assert!(!error_after_stable_connection(&retryable));

        let stable = mark_stable_connection(retryable, true);
        assert!(is_retryable_transport_error(&stable));
        assert!(error_after_stable_connection(&stable));
    }

    fn outside_maintenance() -> DateTime<Utc> {
        utc("2026-09-21T12:00:00Z")
    }

    #[test]
    fn maintenance_state_matrix_covers_daylight_and_standard_time() {
        for (timestamp, expected) in [
            ("2026-09-14T21:14:59Z", false),
            ("2026-09-14T21:15:00Z", true),
            ("2026-09-14T21:49:59Z", true),
            ("2026-09-14T21:50:00Z", false),
            ("2026-09-18T21:14:59Z", false),
            ("2026-09-18T21:15:00Z", true),
            ("2026-09-19T12:00:00Z", true),
            ("2026-09-20T16:14:59Z", true),
            ("2026-09-20T16:15:00Z", false),
            ("2026-12-14T22:14:59Z", false),
            ("2026-12-14T22:15:00Z", true),
            ("2026-12-14T22:50:00Z", false),
            ("2026-12-18T22:14:59Z", false),
            ("2026-12-18T22:15:00Z", true),
            ("2026-12-20T17:14:59Z", true),
            ("2026-12-20T17:15:00Z", false),
        ] {
            assert_eq!(maintenance_active(utc(timestamp)), expected, "{timestamp}");
        }
    }

    #[test]
    fn maintenance_retry_never_requests_provider_io() {
        assert_eq!(
            maintenance_retry_delay(utc("2026-09-19T12:00:00Z")),
            Some(Duration::from_secs(900))
        );
        assert_eq!(maintenance_retry_delay(utc("2026-09-21T12:00:00Z")), None);
    }

    #[test]
    fn maintenance_suppression_is_explicitly_retryable() {
        let error = ensure_provider_io_allowed(&|| true).unwrap_err();

        assert!(error.downcast_ref::<MaintenanceSuppressed>().is_some());
        assert!(is_retryable_connection_error(&error));
        assert!(!is_fatal_session_error(&error));
    }

    #[test]
    fn only_provider_verified_payload_clears_maintenance_recovery_episode() {
        let mut recovery = MaintenanceRecoveryBudget::default();
        let mut stable = false;
        recovery.observe_maintenance();

        observe_payload_outcome(PayloadOutcome::Handled, &mut stable, &mut recovery);
        assert!(!stable);
        assert!(!recovery.transport_failure_exhausted());
        assert!(!recovery.transport_failure_exhausted());
        assert!(recovery.transport_failure_exhausted());

        recovery.observe_maintenance();
        observe_payload_outcome(PayloadOutcome::RuntimeVerified, &mut stable, &mut recovery);
        assert!(stable);
        assert!(!recovery.transport_failure_exhausted());
        assert!(!recovery.verification_required);
    }

    #[test]
    fn post_maintenance_recovery_budget_quiesces_after_three_failures() {
        let mut budget = MaintenanceRecoveryBudget::default();

        assert!(!budget.transport_failure_exhausted());
        budget.observe_maintenance();
        assert!(!budget.transport_failure_exhausted());
        assert!(!budget.transport_failure_exhausted());
        assert!(budget.transport_failure_exhausted());
    }

    #[test]
    fn cold_start_transport_failures_do_not_consume_post_maintenance_budget() {
        let mut budget = MaintenanceRecoveryBudget::default();

        for _ in 0..10 {
            assert!(!budget.transport_failure_exhausted());
        }
        assert!(!budget.verification_required);
        assert_eq!(budget.failed_attempts, 0);
    }

    #[tokio::test]
    async fn runtime_observes_maintenance_then_quiesces_after_three_real_connect_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        drop(listener);
        let checks = Arc::new(AtomicUsize::new(0));
        let runtime_checks = Arc::clone(&checks);
        let attempts = Arc::new(AtomicUsize::new(0));
        let runtime_attempts = Arc::clone(&attempts);
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1)).unwrap();

        let runtime = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                vec![],
                move |_| {
                    runtime_attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_| Ok(PayloadOutcome::Handled),
                ReconnectRuntimeConfig::new(Duration::from_millis(50), policy, move || {
                    if runtime_checks.fetch_add(1, Ordering::SeqCst) == 0 {
                        utc("2026-09-19T12:00:00Z")
                    } else {
                        outside_maintenance()
                    }
                })
                .with_maintenance_poll_delay(Duration::from_millis(1)),
            )
            .await
        });

        timeout(Duration::from_secs(2), async {
            while attempts.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(!runtime.is_finished());
        runtime.abort();
    }

    #[tokio::test]
    async fn cold_start_keeps_retrying_after_three_real_connect_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        drop(listener);
        let attempts = Arc::new(AtomicUsize::new(0));
        let runtime_attempts = Arc::clone(&attempts);
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1)).unwrap();

        let runtime = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                vec![],
                move |_| {
                    runtime_attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_| Ok(PayloadOutcome::Handled),
                ReconnectRuntimeConfig::new(Duration::from_millis(50), policy, outside_maintenance),
            )
            .await
        });

        timeout(Duration::from_secs(1), async {
            while attempts.load(Ordering::SeqCst) < 4 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(!runtime.is_finished());
        runtime.abort();
    }

    #[tokio::test]
    async fn maintenance_after_handshake_runs_retry_preparation_before_quiescing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let maintenance = Arc::new(AtomicBool::new(false));
        let server_maintenance = Arc::clone(&maintenance);
        let server = tokio::spawn(async move {
            let _socket = serve_handshake(&listener, 30.0).await;
            server_maintenance.store(true, Ordering::Release);
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let clock_maintenance = Arc::clone(&maintenance);
        let preparations = Arc::new(Mutex::new(Vec::new()));
        let observed_preparations = Arc::clone(&preparations);
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1)).unwrap();
        let runtime = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                vec![],
                move |preparation| {
                    observed_preparations.lock().unwrap().push(preparation);
                    Ok(())
                },
                |_| Ok(PayloadOutcome::Handled),
                ReconnectRuntimeConfig::new(Duration::from_secs(1), policy, move || {
                    if clock_maintenance.load(Ordering::Acquire) {
                        utc("2026-09-19T12:00:00Z")
                    } else {
                        outside_maintenance()
                    }
                })
                .with_maintenance_poll_delay(Duration::from_secs(60)),
            )
            .await
        });

        timeout(Duration::from_secs(2), async {
            while preparations.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            *preparations.lock().unwrap(),
            [ConnectionPreparation::Retry]
        );
        assert!(!runtime.is_finished());
        runtime.abort();
        server.abort();
    }

    #[test]
    fn stable_connection_clears_post_maintenance_recovery_budget() {
        let mut budget = MaintenanceRecoveryBudget::default();

        budget.observe_maintenance();
        assert!(!budget.transport_failure_exhausted());
        budget.connection_stable();
        assert!(!budget.transport_failure_exhausted());
        assert!(!budget.verification_required);
        assert_eq!(budget.failed_attempts, 0);
    }

    #[test]
    fn websocket_message_classification_matrix() {
        assert_eq!(
            classify_message(Message::Binary(vec![1].into())).unwrap(),
            IncomingMessage::Payload(vec![1])
        );
        assert_eq!(
            classify_message(Message::Ping(vec![2].into())).unwrap(),
            IncomingMessage::ReplyPong(vec![2])
        );
        assert_eq!(
            classify_message(Message::Pong(vec![3].into())).unwrap(),
            IncomingMessage::Ignore
        );
        assert_eq!(
            classify_message(Message::Close(None)).unwrap(),
            IncomingMessage::Closed
        );
        assert!(classify_message(Message::Text("invalid".into())).is_err());
    }

    #[tokio::test]
    async fn websocket_write_timeout_is_bounded() {
        let write = std::future::pending::<std::io::Result<()>>();

        let error = await_write(write, Duration::from_millis(1), "test write")
            .await
            .unwrap_err();

        assert!(error.to_string().contains("test write timed out"));
    }

    #[tokio::test]
    async fn schedules_heartbeats_and_returns_non_control_payloads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());

        let server = tokio::spawn(async move {
            let mut socket = serve_handshake(&listener, 0.05).await;
            socket.send(Message::Pong(Vec::new().into())).await.unwrap();
            socket
                .send(Message::Binary(
                    codec::encode(&protocol::ResponseHeartbeat {
                        template_id: 19,
                        rp_code: vec!["0".to_string()],
                        ..Default::default()
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(45)).await;
            socket.send(Message::Ping(vec![1].into())).await.unwrap();
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Pong(_)
            ));
            assert_template(socket.next().await.unwrap().unwrap(), 18);
            socket
                .send(Message::Binary(
                    codec::encode(&protocol::ResponseHeartbeat {
                        template_id: 19,
                        rp_code: vec!["0".to_string()],
                        ..Default::default()
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Binary(
                    codec::encode(&protocol::RequestLogout {
                        template_id: 12,
                        ..Default::default()
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
        });

        let mut connection = connect(&url, login(), Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(connection.state(), SessionState::Active);
        assert_eq!(
            connection.next_event().await.unwrap(),
            ConnectionEvent::HeartbeatConfirmed
        );
        assert_eq!(
            connection.next_event().await.unwrap(),
            ConnectionEvent::HeartbeatConfirmed
        );
        let ConnectionEvent::Payload(payload) = connection.next_event().await.unwrap() else {
            panic!("expected Rithmic payload event");
        };
        assert_eq!(codec::template_id(&payload).unwrap(), 12);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn established_connection_suppresses_all_provider_io_when_maintenance_starts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let _socket = serve_handshake(&listener, 30.0).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let maintenance = Arc::new(AtomicBool::new(false));
        let guard_state = Arc::clone(&maintenance);
        let guard: MaintenanceGuard = Arc::new(move || guard_state.load(Ordering::Acquire));
        let mut connection =
            connect_with_maintenance_guard(&url, login(), Duration::from_secs(1), guard)
                .await
                .unwrap();

        maintenance.store(true, Ordering::Release);
        let payload = codec::encode(&protocol::RequestMarketDataUpdate {
            template_id: 100,
            ..Default::default()
        })
        .unwrap();

        assert!(connection.send_payload(payload).await.is_err());
        assert!(connection.next_event().await.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn missing_heartbeat_response_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let _socket = serve_handshake(&listener, 30.0).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let mut connection = connect(&url, login(), Duration::from_millis(20))
            .await
            .unwrap();
        assert!(connection.next_event().await.is_err());
        server.abort();
    }

    #[test]
    fn reconnect_policy_rejects_invalid_backoff() {
        assert!(ReconnectPolicy::new(Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(ReconnectPolicy::new(Duration::from_secs(2), Duration::from_secs(1)).is_err());
        assert_eq!(
            next_backoff(Duration::from_secs(1), Duration::from_secs(10)),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(8), Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        assert!(!heartbeat_establishes_stability(1));
        assert!(heartbeat_establishes_stability(2));
    }

    #[test]
    fn deadline_range_matrix_fails_closed() {
        assert!(deadline_after(Duration::ZERO).is_ok());
        assert!(deadline_after(Duration::from_secs(30)).is_ok());
        assert!(deadline_after(Duration::MAX).is_err());
    }

    #[test]
    fn reconnect_backoff_state_matrix() {
        let policy = ReconnectPolicy::new(Duration::from_secs(1), Duration::from_secs(8)).unwrap();
        let mut backoffs = ReconnectBackoffs::new(policy);

        assert_eq!(
            backoffs.next_delay(RetryCause::Transport),
            Duration::from_secs(1)
        );
        assert_eq!(
            backoffs.next_delay(RetryCause::Transport),
            Duration::from_secs(2)
        );
        backoffs.connection_stable();
        assert_eq!(
            backoffs.next_delay(RetryCause::Transport),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn connection_error_classification_matrix() {
        let mut session = RithmicSession::new(login());
        session.begin_system_info().unwrap();
        let fatal_response = codec::encode(&protocol::ResponseRithmicSystemInfo {
            template_id: 17,
            rp_code: vec!["9".to_string()],
            system_name: vec!["test-system".to_string()],
            ..Default::default()
        })
        .unwrap();
        let fatal = session.accept_system_info(&fatal_response).unwrap_err();

        assert!(classify_connection_error(fatal, "fatal", "retry").is_err());
        assert_eq!(
            classify_connection_error(
                mark_retryable_transport(anyhow::anyhow!("network")),
                "fatal",
                "retry"
            )
            .unwrap(),
            RetryCause::Transport
        );
        assert!(classify_connection_error(
            anyhow::anyhow!("unknown protocol failure"),
            "fatal",
            "retry"
        )
        .is_err());
    }

    #[tokio::test]
    async fn reconnects_after_transport_failure_and_forwards_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut socket = serve_handshake(&listener, 30.0).await;
            send_heartbeat_response(&mut socket).await;
            assert_template(socket.next().await.unwrap().unwrap(), 100);
            send_test_payload(&mut socket, 12).await;
            drop(socket);

            let mut socket = serve_handshake(&listener, 30.0).await;
            send_heartbeat_response(&mut socket).await;
            assert_template(socket.next().await.unwrap().unwrap(), 100);
            send_test_payload(&mut socket, 13).await;
        });
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(10)).unwrap();
        let payloads = Arc::new(Mutex::new(Vec::new()));
        let handler_payloads = Arc::clone(&payloads);
        let preparations = Arc::new(Mutex::new(0_u32));
        let startup_preparations = Arc::clone(&preparations);
        let retries = Arc::new(Mutex::new(0_u32));
        let retry_preparations = Arc::clone(&retries);
        let startup_payload = codec::encode(&protocol::RequestMarketDataUpdate {
            template_id: 100,
            ..Default::default()
        })
        .unwrap();
        let supervisor = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                vec![startup_payload],
                move |preparation| {
                    match preparation {
                        ConnectionPreparation::Startup => {
                            *startup_preparations.lock().unwrap() += 1
                        }
                        ConnectionPreparation::Retry => *retry_preparations.lock().unwrap() += 1,
                    }
                    Ok(())
                },
                move |payload| {
                    handler_payloads.lock().unwrap().push(payload);
                    Ok(PayloadOutcome::Handled)
                },
                ReconnectRuntimeConfig::new(Duration::from_secs(1), policy, outside_maintenance),
            )
            .await
        });

        timeout(Duration::from_secs(2), async {
            while payloads.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let templates: Vec<_> = payloads
            .lock()
            .unwrap()
            .iter()
            .map(|payload| codec::template_id(payload).unwrap())
            .collect();
        assert_eq!(templates, [12, 13]);
        assert_eq!(*preparations.lock().unwrap(), 2);
        assert!(*retries.lock().unwrap() >= 1);

        supervisor.abort();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn burst_payloads_are_handled_inline_without_backpressure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut socket = serve_handshake(&listener, 30.0).await;
            send_heartbeat_response(&mut socket).await;
            assert_template(socket.next().await.unwrap().unwrap(), 100);
            send_test_payload(&mut socket, 12).await;
            send_test_payload(&mut socket, 13).await;
            send_test_payload(&mut socket, 14).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(10)).unwrap();
        let payloads = Arc::new(Mutex::new(Vec::new()));
        let handler_payloads = Arc::clone(&payloads);
        let startup_payload = codec::encode(&protocol::RequestMarketDataUpdate {
            template_id: 100,
            ..Default::default()
        })
        .unwrap();
        let supervisor = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                vec![startup_payload],
                |_| Ok(()),
                move |payload| {
                    handler_payloads.lock().unwrap().push(payload);
                    Ok(PayloadOutcome::Handled)
                },
                ReconnectRuntimeConfig::new(Duration::from_secs(1), policy, outside_maintenance),
            )
            .await
        });

        timeout(Duration::from_secs(1), async {
            while payloads.lock().unwrap().len() < 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let templates: Vec<_> = payloads
            .lock()
            .unwrap()
            .iter()
            .map(|payload| codec::template_id(payload).unwrap())
            .collect();
        assert_eq!(templates, [12, 13, 14]);

        supervisor.abort();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn handler_failure_is_terminal_and_stops_following_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut socket = serve_handshake(&listener, 30.0).await;
            send_heartbeat_response(&mut socket).await;
            socket
                .send(Message::Binary(
                    codec::encode(&protocol::Reject {
                        template_id: 75,
                        user_msg: vec!["subscription".to_string()],
                        rp_code: vec!["permission-denied".to_string()],
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            send_test_payload(&mut socket, 12).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(10)).unwrap();
        let templates = Arc::new(Mutex::new(Vec::new()));
        let handler_templates = Arc::clone(&templates);
        let supervisor = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                vec![],
                |_| Ok(()),
                move |payload| {
                    let template_id = codec::template_id(&payload)?;
                    handler_templates.lock().unwrap().push(template_id);
                    if template_id == 75 {
                        return Err(
                            PayloadFailure::new(PayloadFailureKind::SubscriptionRejected).into(),
                        );
                    }
                    Ok(PayloadOutcome::Handled)
                },
                ReconnectRuntimeConfig::new(Duration::from_secs(1), policy, outside_maintenance),
            )
            .await
        });

        let error = timeout(Duration::from_secs(1), supervisor)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<PayloadFailure>()
                .unwrap()
                .template_id(),
            Some(75)
        );
        assert_eq!(*templates.lock().unwrap(), [75]);
        server.await.unwrap();
    }

    #[test]
    fn payload_handler_failure_adds_safe_envelope_without_provider_text() {
        let payload = codec::encode(&protocol::Reject {
            template_id: 75,
            user_msg: vec!["do-not-log-this".to_string()],
            rp_code: vec!["also-sensitive".to_string()],
        })
        .unwrap();
        let mut handler = |_| Err(PayloadFailure::new(PayloadFailureKind::MarketDecode).into());

        let error = handle_payload_with_diagnostics(payload, &mut handler).unwrap_err();
        let failure = error.downcast_ref::<PayloadFailure>().unwrap();
        assert_eq!(failure.template_id(), Some(75));
        assert!(failure.payload_len().unwrap() > 0);
        assert_eq!(failure.stage(), "market_decode");
        assert_eq!(failure.state_effect(), "none");
        let formatted = failure.to_string();
        assert!(!formatted.contains("do-not-log-this"));
        assert!(!formatted.contains("also-sensitive"));
    }

    fn login() -> LoginParameters {
        LoginParameters::new(
            "test-user".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            Plant::Ticker,
        )
        .unwrap()
    }

    async fn serve_handshake(
        listener: &TcpListener,
        heartbeat_interval: f64,
    ) -> WebSocketStream<TcpStream> {
        let (stream, _) = listener.accept().await.unwrap();
        let mut discovery = accept_async(stream).await.unwrap();
        assert_template(discovery.next().await.unwrap().unwrap(), 16);
        discovery
            .send(Message::Pong(Vec::new().into()))
            .await
            .unwrap();
        discovery.send(Message::Ping(vec![1].into())).await.unwrap();
        discovery
            .send(Message::Binary(
                codec::encode(&protocol::ResponseRithmicSystemInfo {
                    template_id: 17,
                    rp_code: vec!["0".to_string()],
                    system_name: vec!["test-system".to_string()],
                    ..Default::default()
                })
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
        assert!(matches!(
            discovery.next().await.unwrap().unwrap(),
            Message::Pong(_)
        ));

        let (stream, _) = listener.accept().await.unwrap();
        let mut login = accept_async(stream).await.unwrap();
        assert_template(login.next().await.unwrap().unwrap(), 10);
        login
            .send(Message::Binary(
                codec::encode(&protocol::ResponseLogin {
                    template_id: 11,
                    rp_code: vec!["0".to_string()],
                    heartbeat_interval: Some(heartbeat_interval),
                    ..Default::default()
                })
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
        assert_template(login.next().await.unwrap().unwrap(), 18);
        login
    }

    async fn send_heartbeat_response(socket: &mut WebSocketStream<TcpStream>) {
        socket
            .send(Message::Binary(
                codec::encode(&protocol::ResponseHeartbeat {
                    template_id: 19,
                    rp_code: vec!["0".to_string()],
                    ..Default::default()
                })
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
    }

    async fn send_test_payload(socket: &mut WebSocketStream<TcpStream>, template_id: i32) {
        socket
            .send(Message::Binary(
                codec::encode(&protocol::RequestLogout {
                    template_id,
                    ..Default::default()
                })
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
    }

    fn assert_template(message: Message, expected: i32) {
        let Message::Binary(payload) = message else {
            panic!("expected binary Rithmic message");
        };
        assert_eq!(codec::template_id(&payload).unwrap(), expected);
    }
}
