use super::session::{is_fatal_session_error, LoginParameters, RithmicSession};
use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep_until, timeout, Instant};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::warn;

type RithmicSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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

pub(crate) async fn run_with_reconnect(
    url: &str,
    login: LoginParameters,
    response_timeout: Duration,
    policy: ReconnectPolicy,
    startup_payloads: Vec<Vec<u8>>,
    payload_tx: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    let mut backoffs = ReconnectBackoffs::new(policy);
    let mut pending_payload = None;

    loop {
        if let Some(payload) = pending_payload.take() {
            payload_tx
                .send(payload)
                .await
                .context("Rithmic payload consumer closed")?;
            backoffs.payload_delivered();
        }

        let retry_cause = match connect(url, login.clone(), response_timeout).await {
            Ok(mut connection) => {
                let mut startup_pending = true;
                let mut heartbeat_confirmations = 0_u32;
                'connected: loop {
                    match connection.next_event().await {
                        Ok(ConnectionEvent::HeartbeatConfirmed) => {
                            heartbeat_confirmations = heartbeat_confirmations.saturating_add(1);
                            if startup_pending {
                                for payload in &startup_payloads {
                                    if let Err(error) =
                                        connection.send_payload(payload.clone()).await
                                    {
                                        warn!(%error, "Rithmic startup write failed; reconnecting");
                                        break 'connected RetryCause::Transport;
                                    }
                                }
                                startup_pending = false;
                            }
                            if heartbeat_establishes_stability(heartbeat_confirmations) {
                                backoffs.connection_stable();
                            }
                        }
                        Ok(ConnectionEvent::Payload(payload)) => {
                            match forward_payload(&payload_tx, payload) {
                                PayloadDelivery::Delivered => {
                                    backoffs.payload_delivered();
                                }
                                PayloadDelivery::Full(payload) => {
                                    warn!("Rithmic payload channel is full; reconnecting");
                                    pending_payload = Some(payload);
                                    break 'connected RetryCause::Backpressure;
                                }
                                PayloadDelivery::Closed(_) => {
                                    bail!("Rithmic payload consumer closed");
                                }
                            }
                        }
                        Err(error) => {
                            break 'connected classify_connection_error(
                                error,
                                "fatal Rithmic session failure",
                                "Rithmic connection lost; reconnecting",
                            )?;
                        }
                    }
                }
            }
            Err(error) => classify_connection_error(
                error,
                "fatal Rithmic handshake failure",
                "Rithmic connection failed; reconnecting",
            )?,
        };

        let delay = backoffs.next_delay(retry_cause);
        tokio::time::sleep(delay).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RetryCause {
    Transport,
    Backpressure,
}

struct ReconnectBackoffs {
    policy: ReconnectPolicy,
    transport: Duration,
    backpressure: Duration,
}

impl ReconnectBackoffs {
    fn new(policy: ReconnectPolicy) -> Self {
        Self {
            transport: policy.initial_backoff,
            backpressure: policy.initial_backoff,
            policy,
        }
    }

    fn connection_stable(&mut self) {
        self.transport = self.policy.initial_backoff;
    }

    fn payload_delivered(&mut self) {
        self.backpressure = self.policy.initial_backoff;
    }

    fn next_delay(&mut self, cause: RetryCause) -> Duration {
        let backoff = match cause {
            RetryCause::Transport => &mut self.transport,
            RetryCause::Backpressure => &mut self.backpressure,
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
    warn!(%error, "{retry_message}");
    Ok(RetryCause::Transport)
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

fn heartbeat_establishes_stability(confirmations: u32) -> bool {
    confirmations >= 2
}

#[derive(Debug, PartialEq)]
enum PayloadDelivery {
    Delivered,
    Full(Vec<u8>),
    Closed(Vec<u8>),
}

fn forward_payload(payload_tx: &mpsc::Sender<Vec<u8>>, payload: Vec<u8>) -> PayloadDelivery {
    match payload_tx.try_send(payload) {
        Ok(()) => PayloadDelivery::Delivered,
        Err(mpsc::error::TrySendError::Full(payload)) => PayloadDelivery::Full(payload),
        Err(mpsc::error::TrySendError::Closed(payload)) => PayloadDelivery::Closed(payload),
    }
}

pub(crate) async fn connect(
    url: &str,
    login: LoginParameters,
    response_timeout: Duration,
) -> Result<RithmicConnection> {
    let mut session = RithmicSession::new(login);

    let (mut discovery, _) = timeout(response_timeout, connect_async(url))
        .await
        .context("Rithmic system-info connection timed out")??;
    send_binary(
        &mut discovery,
        session.begin_system_info()?,
        response_timeout,
    )
    .await?;
    let response = receive_binary(&mut discovery, response_timeout).await?;
    session.reject_terminal(&response)?;
    session.accept_system_info(&response)?;
    drop(discovery);

    let (mut socket, _) = timeout(response_timeout, connect_async(url))
        .await
        .context("Rithmic login connection timed out")??;
    session.mark_reconnected()?;
    send_binary(&mut socket, session.begin_login()?, response_timeout).await?;
    let response = receive_binary(&mut socket, response_timeout).await?;
    session.reject_terminal(&response)?;
    let initial_heartbeat = session.accept_login(&response)?;
    send_binary(&mut socket, initial_heartbeat, response_timeout).await?;

    Ok(RithmicConnection {
        socket,
        session,
        response_timeout,
        heartbeat_deadline: Instant::now() + response_timeout,
        awaiting_heartbeat: true,
    })
}

impl RithmicConnection {
    pub(crate) async fn send_payload(&mut self, payload: Vec<u8>) -> Result<()> {
        send_binary(&mut self.socket, payload, self.response_timeout).await
    }

    pub(crate) async fn next_event(&mut self) -> Result<ConnectionEvent> {
        loop {
            let message = tokio::select! {
                message = self.socket.next() => Some(message),
                () = sleep_until(self.heartbeat_deadline) => None,
            };

            if let Some(message) = message {
                let message = message.context("Rithmic connection ended")??;
                match classify_message(message)? {
                    IncomingMessage::Payload(payload) => {
                        if self.session.accept_control(&payload)? {
                            ensure!(
                                self.awaiting_heartbeat,
                                "Rithmic sent an unexpected heartbeat response"
                            );
                            self.awaiting_heartbeat = false;
                            self.heartbeat_deadline =
                                Instant::now() + self.session.heartbeat_interval()?;
                            return Ok(ConnectionEvent::HeartbeatConfirmed);
                        }
                        return Ok(ConnectionEvent::Payload(payload));
                    }
                    IncomingMessage::ReplyPong(payload) => {
                        await_write(
                            self.socket.send(Message::Pong(payload.into())),
                            self.response_timeout,
                            "Rithmic Pong write",
                        )
                        .await?;
                    }
                    IncomingMessage::Ignore => {}
                    IncomingMessage::Closed => bail!("Rithmic connection closed"),
                }
                continue;
            }

            if self.awaiting_heartbeat {
                bail!("Rithmic heartbeat response timed out");
            }
            send_binary(
                &mut self.socket,
                self.session.heartbeat()?,
                self.response_timeout,
            )
            .await?;
            self.awaiting_heartbeat = true;
            self.heartbeat_deadline = Instant::now() + self.response_timeout;
        }
    }

    #[cfg(test)]
    fn state(&self) -> super::session::SessionState {
        self.session.state()
    }
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
    timeout(wait, receive_protocol_payload(socket, wait))
        .await
        .context("Rithmic response timed out")?
}

async fn receive_protocol_payload(socket: &mut RithmicSocket, wait: Duration) -> Result<Vec<u8>> {
    while let Some(message) = socket.next().await {
        match classify_message(message?)? {
            IncomingMessage::Payload(payload) => return Ok(payload),
            IncomingMessage::ReplyPong(payload) => {
                await_write(
                    socket.send(Message::Pong(payload.into())),
                    wait,
                    "Rithmic Pong write",
                )
                .await?
            }
            IncomingMessage::Ignore => {}
            IncomingMessage::Closed => bail!("Rithmic connection closed"),
        }
    }
    bail!("Rithmic connection ended")
}

async fn await_write<F, E>(write: F, wait: Duration, operation: &str) -> Result<()>
where
    F: Future<Output = std::result::Result<(), E>>,
    E: Error + Send + Sync + 'static,
{
    timeout(wait, write)
        .await
        .with_context(|| format!("{operation} timed out"))??;
    Ok(())
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
    use super::*;
    use crate::connector::rithmic::session::{Plant, RithmicSession, SessionState};
    use crate::connector::rithmic::{codec, protocol};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

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
    fn reconnect_backoff_state_matrix() {
        let policy = ReconnectPolicy::new(Duration::from_secs(1), Duration::from_secs(8)).unwrap();
        let mut backoffs = ReconnectBackoffs::new(policy);

        assert_eq!(
            backoffs.next_delay(RetryCause::Transport),
            Duration::from_secs(1)
        );
        backoffs.payload_delivered();
        assert_eq!(
            backoffs.next_delay(RetryCause::Transport),
            Duration::from_secs(2)
        );
        assert_eq!(
            backoffs.next_delay(RetryCause::Backpressure),
            Duration::from_secs(1)
        );
        assert_eq!(
            backoffs.next_delay(RetryCause::Backpressure),
            Duration::from_secs(2)
        );
        backoffs.payload_delivered();
        assert_eq!(
            backoffs.next_delay(RetryCause::Backpressure),
            Duration::from_secs(1)
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
            classify_connection_error(anyhow::anyhow!("network"), "fatal", "retry").unwrap(),
            RetryCause::Transport
        );
    }

    #[test]
    fn payload_channel_state_matrix_is_fail_closed() {
        let (tx, mut rx) = mpsc::channel(1);
        assert_eq!(forward_payload(&tx, vec![1]), PayloadDelivery::Delivered);
        assert_eq!(
            forward_payload(&tx, vec![2]),
            PayloadDelivery::Full(vec![2])
        );
        assert_eq!(rx.try_recv().unwrap(), vec![1]);

        drop(rx);
        assert_eq!(
            forward_payload(&tx, vec![3]),
            PayloadDelivery::Closed(vec![3])
        );
    }

    #[tokio::test]
    async fn reconnects_after_transport_failure_and_forwards_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            drop(serve_handshake(&listener, 30.0).await);

            let mut socket = serve_handshake(&listener, 30.0).await;
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
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(10)).unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let supervisor = tokio::spawn(async move {
            run_with_reconnect(&url, login(), Duration::from_secs(1), policy, vec![], tx).await
        });

        let payload = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(codec::template_id(&payload).unwrap(), 12);

        supervisor.abort();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn temporary_backpressure_reconnects_and_recovers_delivery() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (overflow_tx, overflow_rx) = tokio::sync::oneshot::channel();
        let (reconnected_tx, reconnected_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first = serve_handshake(&listener, 30.0).await;
            send_heartbeat_response(&mut first).await;
            assert_template(first.next().await.unwrap().unwrap(), 100);
            send_test_payload(&mut first, 12).await;
            send_test_payload(&mut first, 13).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            overflow_tx.send(()).unwrap();

            let mut second = serve_handshake(&listener, 30.0).await;
            send_heartbeat_response(&mut second).await;
            assert_template(second.next().await.unwrap().unwrap(), 100);
            reconnected_tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            send_test_payload(&mut second, 14).await;
        });
        let policy =
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(10)).unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let startup_payload = codec::encode(&protocol::RequestMarketDataUpdate {
            template_id: 100,
            ..Default::default()
        })
        .unwrap();
        let supervisor = tokio::spawn(async move {
            run_with_reconnect(
                &url,
                login(),
                Duration::from_secs(1),
                policy,
                vec![startup_payload],
                tx,
            )
            .await
        });

        timeout(Duration::from_secs(2), overflow_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(codec::template_id(&rx.recv().await.unwrap()).unwrap(), 12);
        assert_eq!(
            codec::template_id(
                &timeout(Duration::from_secs(1), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            13
        );
        timeout(Duration::from_secs(2), reconnected_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            codec::template_id(
                &timeout(Duration::from_secs(1), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            14
        );

        supervisor.abort();
        server.await.unwrap();
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
