use super::*;

#[test]
fn records_never_include_payload_content_or_identity_values() {
    let (bytes, _) = format_record(
        Record {
            direction: Direction::Inbound,
            template_id: Some(352),
            payload_len: 64,
            payload: None,
            connection_id: 9,
            sequence: 7,
            timestamp_ms: 123,
        },
        None,
    );
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["template_id"], 352);
    assert_eq!(json["payload_len"], 64);
    assert_eq!(json["content"]["fields"], serde_json::json!([]));
    assert!(bytes.len() <= MAX_RECORD_BYTES);
}

#[test]
fn capture_attempt_is_nonblocking_and_bounded() {
    let (sender, _receiver) = mpsc::sync_channel(1);
    let capture = CaptureHandle::from_sender(sender, CounterHandle::default());
    let start = std::time::Instant::now();
    capture.attempt(Direction::Outbound, Some(312), 128, 1, &[]);
    capture.attempt(Direction::Outbound, Some(312), 128, 1, &[]);
    let (attempted, enqueued, full) = capture.counts();
    assert!(start.elapsed() < std::time::Duration::from_millis(100));
    assert_eq!(attempted, 2);
    assert_eq!(enqueued, 1);
    assert_eq!(full, 1);
}

#[test]
fn only_reviewed_application_template_ids_are_eligible() {
    for id in [313, 315, 317, 321, 331, 351, 352, 403, 450, 451, 3505] {
        assert!(eligible_template(Direction::Inbound, Some(id)));
    }
    for id in [308, 312, 314, 316, 320, 330, 3504, 402] {
        assert!(eligible_template(Direction::Outbound, Some(id)));
    }
    for id in [10, 11, 17, 18, 300, 301, 302, 303, 322, 323, 3512, 3513] {
        assert!(!eligible_template(Direction::Inbound, Some(id)));
        assert!(!eligible_template(Direction::Outbound, Some(id)));
    }
    assert!(!eligible_template(Direction::Inbound, None));
}

#[test]
fn eligible_capture_attempt_precedes_consumer_even_when_queue_is_full() {
    let (sender, _receiver) = mpsc::sync_channel(1);
    let capture = CaptureHandle::from_sender(sender, CounterHandle::default());
    let mut observed = false;
    let result = before_action(&capture, Direction::Inbound, Some(352), 128, 4, &[], || {
        observed = true;
        let (attempted, enqueued, _) = capture.counts();
        assert_eq!((attempted, enqueued), (1, 1));
        "consumer-result"
    });
    assert!(observed);
    assert_eq!(result, "consumer-result");

    let start = std::time::Instant::now();
    before_action(&capture, Direction::Inbound, Some(352), 128, 4, &[], || {
        assert_eq!(capture.counts(), (2, 1, 1));
    });
    assert!(start.elapsed() < std::time::Duration::from_millis(100));
}

#[test]
fn receive_disposition_matrix_is_unchanged_with_healthy_full_and_dead_sink() {
    let classes = [
        (Some(352), "valid_decode"),
        (Some(352), "semantic_invalid"),
        (None, "malformed_envelope"),
        (Some(999), "well_formed_unknown"),
    ];
    for (sink_state, full) in [
        ("healthy", false),
        ("blocked_full", true),
        ("dead", false),
        ("disabled", false),
    ] {
        for (template, disposition) in classes {
            let (sender, receiver) = mpsc::sync_channel(1);
            if full {
                sender
                    .try_send(Record {
                        direction: Direction::Inbound,
                        template_id: Some(352),
                        payload_len: 1,
                        payload: None,
                        connection_id: 1,
                        sequence: 1,
                        timestamp_ms: 1,
                    })
                    .unwrap();
            }
            let capture = if sink_state == "disabled" {
                CaptureHandle::disabled(sender, CounterHandle::default())
            } else {
                CaptureHandle::from_sender(sender, CounterHandle::default())
            };
            if sink_state == "dead" {
                drop(receiver);
            }
            let called = std::sync::atomic::AtomicBool::new(false);
            let result = before_action(
                &capture,
                Direction::Inbound,
                template,
                17,
                4,
                b"test",
                || {
                    called.store(true, Ordering::Relaxed);
                    disposition
                },
            );
            assert!(called.load(Ordering::Relaxed), "{sink_state}/{disposition}");
            assert_eq!(result, disposition, "{sink_state}/{disposition}");
            if eligible_template(Direction::Inbound, template) && sink_state != "disabled" {
                assert_eq!(capture.counts().0, 1, "{sink_state}/{disposition}");
                if full {
                    assert_eq!(capture.counts().2, 1, "{sink_state}/{disposition}");
                }
            } else {
                assert_eq!(capture.counts().0, 0, "{sink_state}/{disposition}");
            }
        }
    }
}

#[test]
fn key_absence_keeps_metadata_record_and_reports_key_unavailable() {
    let (bytes, status) = format_record(
        Record {
            direction: Direction::Inbound,
            template_id: Some(352),
            payload_len: 64,
            payload: Some(vec![1, 2, 3]),
            connection_id: 9,
            sequence: 7,
            timestamp_ms: 123,
        },
        None,
    );
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, "key_unavailable_or_oversize");
    assert_eq!(json["capture_status"], "metadata_only");
    assert_eq!(json["content"]["fields"], serde_json::json!([]));
    assert!(String::from_utf8(bytes).unwrap().len() <= MAX_RECORD_BYTES);
}

#[tokio::test]
async fn outbound_write_error_timeout_and_cancellation_follow_the_transport_attempt() {
    for outcome in ["written", "write_error", "timeout"] {
        let (sender, receiver) = mpsc::sync_channel(1);
        let capture = CaptureHandle::from_sender(sender, CounterHandle::default());
        let write = async move {
            match outcome {
                "written" => "written",
                "write_error" => "write_error",
                _ => {
                    let result = tokio::time::timeout(
                        std::time::Duration::from_millis(1),
                        std::future::pending::<()>(),
                    )
                    .await;
                    assert!(result.is_err());
                    "timeout"
                }
            }
        };
        let result =
            before_outbound_write(&capture, Some(308), b"attempt".to_vec(), 9, |_| write).await;
        assert_eq!(result, outcome);
        assert_eq!(capture.counts(), (1, 1, 0));
        let record = receiver.try_recv().unwrap();
        assert_eq!(record.direction.as_str(), "outbound");
        assert_eq!(record.template_id, Some(308));
        assert_eq!(record.payload.as_deref(), Some(&b"attempt"[..]));
    }

    let (sender, receiver) = mpsc::sync_channel(1);
    let capture = CaptureHandle::from_sender(sender, CounterHandle::default());
    let cancelled = tokio::spawn(async move {
        before_outbound_write(&capture, Some(312), b"cancelled-write".to_vec(), 10, |_| {
            std::future::pending::<()>()
        })
        .await;
    });
    tokio::task::yield_now().await;
    let record = receiver
        .try_recv()
        .expect("write cancellation must follow an already-enqueued attempt");
    assert_eq!(record.direction.as_str(), "outbound");
    assert_eq!(record.template_id, Some(312));
    assert_eq!(record.payload.as_deref(), Some(&b"cancelled-write"[..]));
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
}

#[test]
fn held_initializer_publishes_only_after_release() {
    const CHILD: &str = "F4_CAPTURE_HELD_INITIALIZER_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "held_initializer_publishes_only_after_release",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "held initializer regression failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let initializer = std::thread::spawn(move || {
        initialize_process_capture_with(|| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let (sender, _receiver) = std::sync::mpsc::sync_channel(1);
            CaptureHandle::disabled(sender, CounterHandle::default())
        });
    });
    started_rx.recv().unwrap();
    let before_release = process_capture() as *const CaptureHandle;
    assert!(process_capture().health_counts().4);
    release_tx.send(()).unwrap();
    initializer.join().unwrap();
    let published = process_capture() as *const CaptureHandle;
    assert_ne!(published, before_release);
    assert_eq!(published, process_capture() as *const CaptureHandle);
}

#[test]
fn key_loader_accepts_only_private_bounded_bytes_and_valid_version() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let directory = tempfile::tempdir().unwrap();
    let key_path = directory.path().join("capture.key");
    std::fs::write(&key_path, [0x71; 32]).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(super::CaptureKey::load_from(Some(&key_path), Some("v2")).is_some());

    let missing = directory.path().join("missing.key");
    assert!(super::CaptureKey::load_from(Some(&missing), Some("v2")).is_none());
    assert!(super::CaptureKey::load_from(None, Some("v2")).is_none());
    assert!(super::CaptureKey::load_from(Some(&key_path), Some("bad/version")).is_none());
    assert!(super::CaptureKey::load_from(Some(&key_path), Some("")).is_none());

    std::fs::write(&key_path, [0x71; 31]).unwrap();
    assert!(super::CaptureKey::load_from(Some(&key_path), Some("v2")).is_none());
    std::fs::write(&key_path, [0x71; 4097]).unwrap();
    assert!(super::CaptureKey::load_from(Some(&key_path), Some("v2")).is_none());

    std::fs::write(&key_path, [0x71; 32]).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(super::CaptureKey::load_from(Some(&key_path), Some("v2")).is_none());
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let alias = directory.path().join("alias.key");
    symlink(&key_path, &alias).unwrap();
    assert!(super::CaptureKey::load_from(Some(&alias), Some("v2")).is_none());
}
