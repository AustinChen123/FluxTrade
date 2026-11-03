#[path = "transport_capture_sink_matrix_support.rs"]
mod matrix_support;
use super::super::capture::before_outbound_write;
use super::{test_owner_writer, CaptureDirection as Direction, ConnectionEvent};
use matrix_support::*;
use std::fs;
use std::future::Future;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn real_loopback_socket_to_owner_jsonl_matrix_covers_both_directions() {
    // Both the cdylib and service binary include this private transport module.
    // Keep one unique execution of each socket/writer cell under cargo test.
    if env!("CARGO_CRATE_NAME") != "fluxtrade_core" {
        return;
    }
    let payload = inbound_payload();
    for outcome in SocketOutcome::all() {
        for writer_state in WriterState::all() {
            run_outbound_cell(outcome, writer_state).await;
            run_receive_cell(outcome, writer_state, &payload).await;
        }
    }
}

async fn run_outbound_cell(outcome: SocketOutcome, writer_state: WriterState) {
    let root = tempfile::tempdir().unwrap();
    let owner = test_owner_writer(root.path(), writer_state.test_state());
    if writer_state == WriterState::Full {
        owner
            .capture
            .attempt(Direction::Outbound, Some(312), 1, 700, b"prefill");
    }
    let (mut connection, server) = connection_with_server(outcome, true, None).await;
    let connection_id = connection.capture_connection_id;
    let payload = outbound_payload();
    assert!(payload.len() <= 256 * 1024);
    assert_eq!(
        super::super::codec::capture_template_id(&payload),
        Some(312)
    );
    if outcome == SocketOutcome::Cancel {
        let capture = &owner.capture;
        let mut future = Box::pin(before_outbound_write(
            capture,
            Some(312),
            payload,
            connection_id,
            |_| std::future::pending::<()>(),
        ));
        let pending = std::future::poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await;
        assert!(
            pending,
            "controlled outbound write boundary must remain pending"
        );
        drop(future);
    } else if outcome == SocketOutcome::Timeout {
        let timed_out = before_outbound_write(
            &owner.capture,
            Some(312),
            payload,
            connection_id,
            |_| async {
                tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
                    .await
                    .is_err()
            },
        )
        .await;
        assert!(timed_out, "controlled outbound write must report timeout");
    } else {
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            connection.send_payload_with_capture(payload, &owner.capture),
        )
        .await
        .expect("outbound send waited for capture writer");
        match outcome {
            SocketOutcome::Ok => assert!(result.is_ok()),
            SocketOutcome::WriteError => assert!(
                result.is_err(),
                "socket outcome {outcome:?} unexpectedly succeeded with writer {writer_state:?}"
            ),
            SocketOutcome::Timeout => unreachable!(),
            SocketOutcome::Cancel => unreachable!(),
        }
    }
    let expected_attempts = 1 + u64::from(writer_state == WriterState::Full);
    assert_eq!(owner.capture.counts().0, expected_attempts);
    if writer_state == WriterState::Full {
        assert_eq!(owner.capture.counts().2, 1);
    }
    let (path, health) = owner.release_and_join();
    server.await.unwrap();
    assert_eq!(
        health.2,
        u64::from(
            writer_state == WriterState::Full
                || matches!(writer_state, WriterState::Dead | WriterState::WriteFailing)
        )
    );
    assert_eq!(
        health.3,
        u64::from(matches!(
            writer_state,
            WriterState::Dead | WriterState::WriteFailing
        ))
    );
    assert_jsonl_rows(
        path,
        writer_state,
        true,
        Some(if writer_state == WriterState::Full {
            700
        } else {
            connection_id
        }),
    );
}

async fn run_receive_cell(outcome: SocketOutcome, writer_state: WriterState, payload: &[u8]) {
    let root = tempfile::tempdir().unwrap();
    let owner = test_owner_writer(root.path(), writer_state.test_state());
    let captures_payload = outcome == SocketOutcome::Ok;
    if writer_state == WriterState::Full && captures_payload {
        owner
            .capture
            .attempt(Direction::Inbound, Some(352), 1, 700, b"prefill");
    }
    let (mut connection, server) =
        connection_with_server(outcome, false, captures_payload.then(|| payload.to_vec())).await;
    let connection_id = connection.capture_connection_id;
    match outcome {
        SocketOutcome::Ok => {
            let receive = connection.next_event_guarded_with_capture(|| false, &owner.capture);
            let event = tokio::time::timeout(Duration::from_millis(500), receive)
                .await
                .expect("receive waited for capture writer")
                .unwrap();
            assert_eq!(event, ConnectionEvent::Payload(payload.to_vec()));
        }
        SocketOutcome::WriteError => {
            assert!(connection
                .next_event_guarded_with_capture(|| false, &owner.capture)
                .await
                .is_err());
        }
        SocketOutcome::Timeout => {
            connection.heartbeat_deadline = Instant::now() + Duration::from_millis(5);
            assert!(connection
                .next_event_guarded_with_capture(|| false, &owner.capture)
                .await
                .is_err());
        }
        SocketOutcome::Cancel => {
            let mut future =
                Box::pin(connection.next_event_guarded_with_capture(|| false, &owner.capture));
            let pending = std::future::poll_fn(|context| match future.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(true),
                Poll::Ready(_) => Poll::Ready(false),
            })
            .await;
            assert!(pending);
            drop(future);
        }
    }
    let expected_attempts = u64::from(captures_payload)
        + u64::from(writer_state == WriterState::Full && captures_payload);
    assert_eq!(owner.capture.counts().0, expected_attempts);
    if writer_state == WriterState::Full && captures_payload {
        assert_eq!(owner.capture.counts().2, 1);
    }
    let (path, health) = owner.release_and_join();
    server.await.unwrap();
    assert_eq!(
        health.2,
        u64::from(
            captures_payload
                && (writer_state == WriterState::Full
                    || matches!(writer_state, WriterState::Dead | WriterState::WriteFailing))
        )
    );
    assert_eq!(
        health.3,
        u64::from(
            captures_payload
                && matches!(writer_state, WriterState::Dead | WriterState::WriteFailing)
        )
    );
    assert_jsonl_rows(
        path,
        writer_state,
        captures_payload,
        captures_payload.then_some(if writer_state == WriterState::Full {
            700
        } else {
            connection_id
        }),
    );
}

fn assert_jsonl_rows(
    path: std::path::PathBuf,
    state: WriterState,
    attempted: bool,
    expected_connection_id: Option<u64>,
) {
    let text = fs::read_to_string(path).unwrap_or_default();
    let expected = if attempted
        && matches!(
            state,
            WriterState::Healthy | WriterState::Blocked | WriterState::Full
        ) {
        1
    } else {
        0
    };
    assert_eq!(
        text.lines().count(),
        expected,
        "writer={state:?} attempted={attempted}"
    );
    if let Some(row) = text.lines().next() {
        let value: serde_json::Value = serde_json::from_str(row).unwrap();
        assert_eq!(
            value["capture_status"],
            if state == WriterState::Full {
                "metadata_only"
            } else {
                "allowlisted"
            }
        );
        assert_eq!(value["connection_id"].as_u64(), expected_connection_id);
        if state != WriterState::Full {
            assert!(!value["content"]["fields"].as_array().unwrap().is_empty());
        }
        assert!(matches!(
            value["direction"].as_str(),
            Some("inbound" | "outbound")
        ));
    }
}
