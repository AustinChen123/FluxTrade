use super::super::capture::{CaptureHandle, CounterHandle};
use super::super::{
    ledger::AccountIdentity,
    order_pending::{Pending, SubmitKind},
    order_runtime, protocol,
};
use super::tests::{login, send_heartbeat_response, serve_handshake};
use super::*;
use futures_util::SinkExt;
use tokio::net::TcpListener;

#[tokio::test]
async fn blocked_capture_writer_does_not_change_business_or_pending_disposition() {
    if env!("CARGO_CRATE_NAME") != "fluxtrade_core" {
        return;
    }
    let account = AccountIdentity {
        fcm_id: "FCM".into(),
        ib_id: "IB".into(),
        account_id: "ACCOUNT".into(),
    };
    let valid = codec::encode(&protocol::ExchangeOrderNotification {
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
        transaction_type: Some(protocol::exchange_order_notification::TransactionType::Buy as i32),
        quantity: Some(1),
        total_fill_size: Some(0),
        total_unfilled_size: Some(1),
        price: Some(1.25),
        ..Default::default()
    })
    .unwrap();
    let invalid = codec::encode(&protocol::ExchangeOrderNotification {
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
        transaction_type: Some(protocol::exchange_order_notification::TransactionType::Buy as i32),
        quantity: Some(1),
        total_fill_size: Some(0),
        total_unfilled_size: Some(1),
        price: Some(-1.25),
        ..Default::default()
    })
    .unwrap();
    let unknown = codec::encode(&protocol::RequestLogout {
        template_id: 999,
        ..Default::default()
    })
    .unwrap();
    let payloads = [
        ("valid", valid, true),
        ("semantic_invalid", invalid.clone(), true),
        ("semantic_invalid_no_pending", invalid, false),
        ("unknown_template", unknown.clone(), true),
        ("unknown_template_no_pending", unknown, false),
        ("malformed_envelope", vec![0x80], true),
        ("malformed_envelope_no_pending", vec![0x80], false),
    ];

    #[derive(Clone, Copy, Debug)]
    enum MatrixState {
        Disabled,
        Healthy,
        Blocked,
        Full,
        Dead,
    }
    for state in [
        MatrixState::Disabled,
        MatrixState::Healthy,
        MatrixState::Blocked,
        MatrixState::Full,
        MatrixState::Dead,
    ] {
        for (case, payload, pending_present) in payloads.clone() {
            let disposition_case = case.strip_suffix("_no_pending").unwrap_or(case);
            let root = tempfile::tempdir().unwrap();
            let (capture_handle, owner, prefill) = match state {
                MatrixState::Disabled => {
                    let (sender, _receiver) = std::sync::mpsc::sync_channel(1);
                    (
                        CaptureHandle::disabled(sender, CounterHandle::default()),
                        None,
                        0,
                    )
                }
                MatrixState::Healthy
                | MatrixState::Blocked
                | MatrixState::Full
                | MatrixState::Dead => {
                    let writer_state = match state {
                        MatrixState::Healthy => TestWriterState::Healthy,
                        MatrixState::Blocked | MatrixState::Full => TestWriterState::Blocked,
                        MatrixState::Dead => TestWriterState::Dead,
                        MatrixState::Disabled => unreachable!(),
                    };
                    let owner = super::test_owner_writer(root.path(), writer_state);
                    if matches!(state, MatrixState::Full) {
                        owner.capture.attempt(
                            CaptureDirection::Inbound,
                            Some(352),
                            1,
                            998,
                            b"prefill",
                        );
                        (owner.capture.clone(), Some(owner), 1)
                    } else {
                        (owner.capture.clone(), Some(owner), 0)
                    }
                }
            };
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
            let mut connection = connect(&url, login(), Duration::from_secs(30))
                .await
                .unwrap();
            assert_eq!(
                connection.next_event().await.unwrap(),
                ConnectionEvent::HeartbeatConfirmed
            );
            let receive = connection.next_event_guarded_with_capture(|| false, &capture_handle);
            let received = tokio::time::timeout(Duration::from_millis(500), receive)
                .await
                .expect("business receive waited for blocked capture writer");

            let (events, event_rx) = std::sync::mpsc::channel();
            let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
            let pending_key = format!("pending-{case}");
            let mut pending = pending_present.then_some(Pending::Submit {
                kind: SubmitKind::Plain,
                request_key: pending_key.clone(),
                client_order_id: "client-pending".into(),
                basket_id: Some("basket-pending".into()),
                deadline: std::time::Instant::now() + Duration::from_secs(30),
                reply: reply_tx,
            });
            if disposition_case == "malformed_envelope" {
                assert!(received.is_err());
                assert!(order_runtime::handle_payload(
                    payload.clone(),
                    &account,
                    &mut pending,
                    &events,
                )
                .is_err());
            } else {
                assert_eq!(received.unwrap(), ConnectionEvent::Payload(payload.clone()));
                let disposition =
                    order_runtime::handle_payload(payload.clone(), &account, &mut pending, &events);
                match disposition_case {
                    "valid" => {
                        assert!(disposition.is_ok());
                        assert!(event_rx.try_recv().is_ok());
                    }
                    "semantic_invalid" => {
                        assert!(disposition.is_err());
                        assert!(event_rx.try_recv().is_err());
                    }
                    "unknown_template" => {
                        assert!(disposition.is_ok());
                        assert!(event_rx.try_recv().is_err());
                    }
                    _ => unreachable!(),
                }
            }
            if pending_present {
                match pending.as_ref().expect("pending state was removed") {
                    Pending::Submit { request_key, .. } => assert_eq!(request_key, &pending_key),
                    _ => panic!("pending state changed for {case}"),
                }
            } else {
                assert!(pending.is_none(), "no-pending state changed for {case}");
            }
            assert!(
                reply_rx.try_recv().is_err(),
                "pending reply changed for {case}"
            );
            let eligible = if matches!(state, MatrixState::Disabled) {
                0
            } else {
                u64::from(disposition_case == "valid" || disposition_case == "semantic_invalid")
            };
            let attempted = capture_handle.counts().0;
            assert_eq!(attempted, prefill + eligible);
            drop(capture_handle);
            if let Some(owner) = owner {
                let health = owner.capture.health_counts();
                if matches!(state, MatrixState::Blocked | MatrixState::Full) {
                    assert_eq!(health.1, 0, "writer crossed its release gate for {case}");
                }
                assert_eq!(health.4, matches!(state, MatrixState::Dead));

                let (jsonl, final_health) = owner.release_and_join();
                let expected_written = match state {
                    MatrixState::Dead => 0,
                    MatrixState::Full => 1,
                    _ => attempted,
                };
                assert_eq!(final_health.1, expected_written);
                assert_eq!(
                    final_health.2,
                    if matches!(state, MatrixState::Dead | MatrixState::Full) {
                        eligible
                    } else {
                        0
                    },
                    "unexpected dropped count for {state:?}/{case}"
                );
                let text = std::fs::read_to_string(jsonl).unwrap();
                assert_eq!(text.lines().count(), expected_written as usize);
                if expected_written != 0 && !matches!(state, MatrixState::Full) {
                    assert!(text.contains("allowlisted"));
                    assert!(!text.contains("ACCOUNT"));
                }
            }
            server.await.unwrap();
        }
    }
}
