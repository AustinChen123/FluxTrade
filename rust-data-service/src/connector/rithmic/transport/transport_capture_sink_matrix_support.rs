use super::super::super::{codec, protocol};
use super::super::tests::{login, send_heartbeat_response, serve_handshake};
use super::super::{
    connect, ConnectionEvent, Duration, Message, RithmicConnection, TestWriterState,
};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SocketOutcome {
    Ok,
    WriteError,
    Timeout,
    Cancel,
}

impl SocketOutcome {
    pub(super) fn all() -> [Self; 4] {
        [Self::Ok, Self::WriteError, Self::Timeout, Self::Cancel]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriterState {
    Healthy,
    Blocked,
    Full,
    Dead,
    WriteFailing,
}

impl WriterState {
    pub(super) fn all() -> [Self; 5] {
        [
            Self::Healthy,
            Self::Blocked,
            Self::Full,
            Self::Dead,
            Self::WriteFailing,
        ]
    }

    pub(super) fn test_state(self) -> TestWriterState {
        match self {
            Self::Healthy => TestWriterState::Healthy,
            Self::Blocked | Self::Full => TestWriterState::Blocked,
            Self::Dead => TestWriterState::Dead,
            Self::WriteFailing => TestWriterState::WriteFailing,
        }
    }
}

pub(super) fn outbound_payload() -> Vec<u8> {
    protocol::RequestNewOrder {
        template_id: 312,
        user_tag: Some("capture-matrix-client".into()),
        symbol: Some("MNQZ6".into()),
        quantity: Some(1),
        price: Some(20000.0),
        ..Default::default()
    }
    .encode_to_vec()
}

pub(super) fn inbound_payload() -> Vec<u8> {
    codec::encode(&protocol::ExchangeOrderNotification {
        template_id: 352,
        notify_type: Some(protocol::exchange_order_notification::NotifyType::Status as i32),
        is_snapshot: Some(false),
        fcm_id: Some("FCM".into()),
        ib_id: Some("IB".into()),
        account_id: Some("ACCOUNT".into()),
        user_tag: Some("matrix-client".into()),
        exchange: Some("CME".into()),
        symbol: Some("MNQZ6".into()),
        status: Some("OPEN".into()),
        quantity: Some(1),
        total_fill_size: Some(0),
        total_unfilled_size: Some(1),
        price: Some(20000.0),
        ..Default::default()
    })
    .unwrap()
}

pub(super) async fn connection_with_server(
    outcome: SocketOutcome,
    outbound: bool,
    receive_payload: Option<Vec<u8>>,
) -> (RithmicConnection, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (close_tx, close_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = serve_handshake(&listener, 30.0).await;
        send_heartbeat_response(&mut socket).await;
        if !outbound && outcome == SocketOutcome::Ok {
            socket
                .send(Message::Binary(receive_payload.unwrap().into()))
                .await
                .unwrap();
        }
        match outcome {
            SocketOutcome::Ok if outbound => {
                let message = socket.next().await.unwrap().unwrap();
                assert!(matches!(message, Message::Binary(_)));
            }
            SocketOutcome::WriteError => {
                socket.send(Message::Close(None)).await.unwrap();
                let _ = close_tx.send(());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    });
    let mut connection = connect(&url, login(), Duration::from_secs(30))
        .await
        .unwrap();
    if outbound {
        assert_eq!(
            connection.next_event().await.unwrap(),
            ConnectionEvent::HeartbeatConfirmed
        );
        if outcome == SocketOutcome::WriteError {
            close_rx.await.unwrap();
            assert!(matches!(
                connection.socket.next().await.unwrap(),
                Ok(Message::Close(_))
            ));
            let _ = connection.socket.close(None).await;
        }
    } else {
        assert_eq!(
            connection.next_event().await.unwrap(),
            ConnectionEvent::HeartbeatConfirmed
        );
        if outcome == SocketOutcome::WriteError {
            close_rx.await.unwrap();
        }
    }
    (connection, server)
}
