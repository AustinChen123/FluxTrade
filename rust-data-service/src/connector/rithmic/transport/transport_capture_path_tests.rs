use super::super::{codec, protocol};
use super::tests::{login, send_heartbeat_response, serve_handshake};
use super::*;
use prost::Message as ProstMessage;
use tokio::net::TcpListener;

#[tokio::test]
async fn actual_receive_path_matrix_preserves_payload_with_capture_sink_states() {
    let payloads = [
        codec::encode(&protocol::ExchangeOrderNotification {
            template_id: 352,
            notify_type: Some(protocol::exchange_order_notification::NotifyType::Status as i32),
            is_snapshot: Some(false),
            fcm_id: Some("FCM".into()),
            ib_id: Some("IB".into()),
            account_id: Some("ACCOUNT".into()),
            basket_id: Some("basket-1".into()),
            user_tag: Some("client-1".into()),
            exchange: Some("CME".into()),
            symbol: Some("MNQU6".into()),
            status: Some("OPEN".into()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            total_fill_size: Some(0),
            total_unfilled_size: Some(1),
            price: Some(1.25),
            ..Default::default()
        })
        .unwrap(),
        codec::encode(&protocol::ExchangeOrderNotification {
            template_id: 352,
            notify_type: Some(protocol::exchange_order_notification::NotifyType::Status as i32),
            is_snapshot: Some(false),
            fcm_id: Some("FCM".into()),
            ib_id: Some("IB".into()),
            account_id: Some("ACCOUNT".into()),
            basket_id: Some("basket-1".into()),
            user_tag: Some("client-1".into()),
            exchange: Some("CME".into()),
            symbol: Some("MNQU6".into()),
            status: Some("OPEN".into()),
            transaction_type: Some(
                protocol::exchange_order_notification::TransactionType::Buy as i32,
            ),
            quantity: Some(1),
            total_fill_size: Some(0),
            total_unfilled_size: Some(1),
            price: Some(-1.25),
            ..Default::default()
        })
        .unwrap(),
        vec![0x80],
        codec::encode(&protocol::RequestLogout {
            template_id: 999,
            ..Default::default()
        })
        .unwrap(),
    ];
    for (payload_index, payload) in payloads.into_iter().enumerate() {
        for sink_state in [
            "healthy",
            "writer_blocked",
            "blocked_full",
            "dead",
            "disabled",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server_payload = payload.clone();
            let server = tokio::spawn(async move {
                let mut socket = serve_handshake(&listener, 30.0).await;
                send_heartbeat_response(&mut socket).await;
                socket
                    .send(Message::Binary(server_payload.into()))
                    .await
                    .unwrap();
            });
            let mut connection = connect(&url, login(), Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(
                connection.next_event().await.unwrap(),
                ConnectionEvent::HeartbeatConfirmed
            );

            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let mut receiver = Some(receiver);
            let blocked_writer = if sink_state == "writer_blocked" {
                let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
                let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
                let writer_receiver = receiver.take().unwrap();
                let writer = std::thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    writer_receiver.try_recv().ok()
                });
                ready_rx.recv().unwrap();
                Some((release_tx, writer))
            } else {
                None
            };
            let capture = if sink_state == "disabled" {
                super::super::capture::CaptureHandle::disabled(
                    sender,
                    super::super::capture::CounterHandle::default(),
                )
            } else {
                super::super::capture::CaptureHandle::from_sender(
                    sender,
                    super::super::capture::CounterHandle::default(),
                )
            };
            if sink_state == "blocked_full" {
                capture.attempt(
                    super::super::capture::Direction::Inbound,
                    Some(352),
                    1,
                    8,
                    b"queued",
                );
            }
            if sink_state == "dead" {
                drop(receiver.take());
            }

            let receive = connection.next_event_guarded_with_capture(|| false, &capture);
            let event = if sink_state == "writer_blocked" {
                tokio::time::timeout(Duration::from_millis(500), receive)
                    .await
                    .expect("receive path waited for blocked capture writer")
            } else {
                receive.await
            };
            if let Some((release, writer)) = blocked_writer {
                release.send(()).unwrap();
                let record = writer.join().unwrap();
                assert_eq!(record.is_some(), payload_index < 2);
            }
            if payload_index == 2 {
                let error = event.unwrap_err();
                assert!(error.to_string().contains("invalid varint"));
            } else {
                assert_eq!(
                    event.unwrap(),
                    ConnectionEvent::Payload(payload.clone()),
                    "payload={payload_index} sink={sink_state}"
                );
                if payload_index < 2 {
                    let account = super::super::ledger::AccountIdentity {
                        fcm_id: "FCM".into(),
                        ib_id: "IB".into(),
                        account_id: "ACCOUNT".into(),
                    };
                    let business_decode =
                        super::super::order_event::decode_order_event(&payload, &account);
                    assert_eq!(
                            business_decode.is_ok(),
                            payload_index == 0,
                            "business decode after receive/capture: payload={payload_index} sink={sink_state}"
                        );
                }
            }
            let eligible = codec::capture_template_id(&payload).is_some_and(|id| {
                super::super::capture::eligible_template(
                    super::super::capture::Direction::Inbound,
                    Some(id),
                )
            });
            let counts = capture.counts();
            if eligible {
                let active = sink_state != "disabled";
                let prefill = u64::from(sink_state == "blocked_full");
                assert_eq!(
                    counts.0,
                    if active { prefill + 1 } else { 0 },
                    "payload={payload_index} sink={sink_state}"
                );
                assert_eq!(
                    counts.1,
                    u64::from(sink_state != "dead" && active),
                    "payload={payload_index} sink={sink_state}"
                );
                if sink_state == "blocked_full" {
                    assert_eq!(counts.2, 1);
                }
            } else {
                assert_eq!(counts.0, u64::from(sink_state == "blocked_full"));
            }
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn production_send_payload_enqueues_capture_before_socket_result() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = serve_handshake(&listener, 30.0).await;
        send_heartbeat_response(&mut socket).await;
        drop(socket);
    });
    let mut connection = connect(&url, login(), Duration::from_secs(1))
        .await
        .unwrap();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let capture = super::super::capture::CaptureHandle::from_sender(
        sender,
        super::super::capture::CounterHandle::default(),
    );
    let payload = super::super::protocol::RequestNewOrder {
        template_id: 312,
        user_tag: Some("private-client-tag".into()),
        symbol: Some("MNQZ6".into()),
        quantity: Some(2),
        price: Some(20001.25),
        ..Default::default()
    }
    .encode_to_vec();
    let _ = connection
        .send_payload_with_capture(payload, &capture)
        .await;
    assert!(receiver.recv_timeout(Duration::from_secs(1)).is_ok());
    assert_eq!(capture.counts().0, 1);
    assert_eq!(capture.counts().1, 1);
    server.await.unwrap();
}
