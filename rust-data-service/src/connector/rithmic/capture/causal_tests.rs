use super::{before_action, before_outbound_write, test_owner_writer, Direction, TestWriterState};
use std::fs;

#[derive(Clone, Copy)]
enum SocketOutcome {
    Ok,
    WriteError,
    Timeout,
    Cancel,
}

impl SocketOutcome {
    fn all() -> [Self; 4] {
        [Self::Ok, Self::WriteError, Self::Timeout, Self::Cancel]
    }

    fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::WriteError => "write_error",
            Self::Timeout => "timeout",
            Self::Cancel => "cancel",
        }
    }

    fn result(self) -> Result<(), &'static str> {
        match self {
            Self::Ok => Ok(()),
            Self::WriteError => Err("socket write error"),
            Self::Timeout => Err("socket timeout"),
            Self::Cancel => Err("socket future cancelled"),
        }
    }
}

#[derive(Clone, Copy)]
enum WriterState {
    Healthy,
    Blocked,
    Full,
    Dead,
    WriteFailing,
}

impl WriterState {
    fn all() -> [Self; 5] {
        [
            Self::Healthy,
            Self::Blocked,
            Self::Full,
            Self::Dead,
            Self::WriteFailing,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Blocked => "blocked",
            Self::Full => "full",
            Self::Dead => "dead",
            Self::WriteFailing => "write_failing",
        }
    }

    fn test_state(self) -> TestWriterState {
        match self {
            Self::Healthy => TestWriterState::Healthy,
            Self::Blocked | Self::Full => TestWriterState::Blocked,
            Self::Dead => TestWriterState::Dead,
            Self::WriteFailing => TestWriterState::WriteFailing,
        }
    }
}

#[tokio::test]
async fn production_capture_hooks_preserve_socket_results_across_owner_jsonl_matrix() {
    if env!("CARGO_CRATE_NAME") != "fluxtrade_core" {
        return;
    }
    let payload = [0x08, 0xb8, 0x02, 0x10, 0x01];
    for direction in [Direction::Outbound, Direction::Inbound] {
        for socket in SocketOutcome::all() {
            for writer_state in WriterState::all() {
                let root = tempfile::tempdir().unwrap();
                let owner = test_owner_writer(root.path(), writer_state.test_state());
                let receive_payload =
                    matches!(direction, Direction::Outbound) || matches!(socket, SocketOutcome::Ok);
                if matches!(writer_state, WriterState::Full) && receive_payload {
                    owner.capture.attempt(direction, Some(312), 5, 99, &payload);
                }
                let before = owner.capture.counts();
                let result = match direction {
                    Direction::Outbound => {
                        before_outbound_write(
                            &owner.capture,
                            Some(312),
                            payload.to_vec(),
                            99,
                            move |_| async move { socket.result() },
                        )
                        .await
                    }
                    Direction::Inbound if receive_payload => before_action(
                        &owner.capture,
                        Direction::Inbound,
                        Some(352),
                        payload.len(),
                        99,
                        &payload,
                        || socket.result(),
                    ),
                    Direction::Inbound => socket.result(),
                };
                assert_eq!(
                    result,
                    socket.result(),
                    "direction={direction:?} socket={} writer={}",
                    socket.name(),
                    writer_state.name()
                );
                assert_eq!(
                    owner.capture.counts().0,
                    before.0 + u64::from(receive_payload),
                    "socket failure before receive payload must not create a record"
                );
                if matches!(writer_state, WriterState::Full) && receive_payload {
                    assert_eq!(owner.capture.counts().2, 1);
                }
                let (path, health) = owner.release_and_join();
                assert_eq!(
                    health.2,
                    u64::from(
                        receive_payload
                            && matches!(
                                writer_state,
                                WriterState::Full | WriterState::Dead | WriterState::WriteFailing
                            )
                    )
                );
                let jsonl = fs::read_to_string(path).unwrap_or_default();
                let expected_records = match writer_state {
                    WriterState::Healthy | WriterState::Blocked if receive_payload => 1,
                    WriterState::Full if receive_payload => 1,
                    WriterState::Dead | WriterState::WriteFailing => 0,
                    _ => 0,
                };
                assert_eq!(
                    jsonl.lines().count(),
                    expected_records,
                    "direction={direction:?} socket={} writer={}",
                    socket.name(),
                    writer_state.name()
                );
                if expected_records != 0 {
                    let record: serde_json::Value =
                        serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();
                    assert_eq!(
                        record["direction"],
                        match direction {
                            Direction::Inbound => "inbound",
                            Direction::Outbound => "outbound",
                        }
                    );
                    assert_eq!(record["connection_id"], 99);
                    assert_eq!(record["capture_status"], "metadata_only");
                }
            }
        }
    }
}
