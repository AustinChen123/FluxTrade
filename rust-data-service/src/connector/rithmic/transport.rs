use super::session::{LoginParameters, RithmicSession};
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
    payload_tx: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    let mut backoff = policy.initial_backoff;

    loop {
        match connect(url, login.clone(), response_timeout).await {
            Ok(mut connection) => loop {
                match connection.next_event().await {
                    Ok(ConnectionEvent::HeartbeatConfirmed) => {
                        backoff = policy.initial_backoff;
                    }
                    Ok(ConnectionEvent::Payload(payload)) => {
                        forward_payload(&payload_tx, payload)?;
                    }
                    Err(error) => {
                        warn!(%error, "Rithmic connection lost; reconnecting");
                        break;
                    }
                }
            },
            Err(error) => warn!(%error, "Rithmic connection failed; reconnecting"),
        }

        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, policy.max_backoff);
    }
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

fn forward_payload(payload_tx: &mpsc::Sender<Vec<u8>>, payload: Vec<u8>) -> Result<()> {
    match payload_tx.try_send(payload) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => bail!("Rithmic payload channel is full"),
        Err(mpsc::error::TrySendError::Closed(_)) => bail!("Rithmic payload consumer closed"),
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
    session.accept_system_info(&response)?;
    drop(discovery);

    let (mut socket, _) = timeout(response_timeout, connect_async(url))
        .await
        .context("Rithmic login connection timed out")??;
    session.mark_reconnected()?;
    send_binary(&mut socket, session.begin_login()?, response_timeout).await?;
    let response = receive_binary(&mut socket, response_timeout).await?;
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
    use crate::connector::rithmic::session::{Plant, SessionState};
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
    }

    #[test]
    fn payload_channel_state_matrix_is_fail_closed() {
        let (tx, mut rx) = mpsc::channel(1);
        forward_payload(&tx, vec![1]).unwrap();
        assert!(forward_payload(&tx, vec![2])
            .unwrap_err()
            .to_string()
            .contains("channel is full"));
        assert_eq!(rx.try_recv().unwrap(), vec![1]);

        drop(rx);
        assert!(forward_payload(&tx, vec![3])
            .unwrap_err()
            .to_string()
            .contains("consumer closed"));
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
            run_with_reconnect(&url, login(), Duration::from_secs(1), policy, tx).await
        });

        let payload = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(codec::template_id(&payload).unwrap(), 12);

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

    fn assert_template(message: Message, expected: i32) {
        let Message::Binary(payload) = message else {
            panic!("expected binary Rithmic message");
        };
        assert_eq!(codec::template_id(&payload).unwrap(), expected);
    }
}
