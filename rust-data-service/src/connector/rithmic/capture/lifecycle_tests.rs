use super::super::capture::{
    initialize_process_capture_with, process_capture, shutdown_process_capture, test_owner_writer,
    CaptureHandle, TestWriterState,
};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::os::unix::fs::PermissionsExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async, tungstenite::protocol::Message as WsMessage, WebSocketStream,
};

async fn handshake(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut discovery = accept_async(stream).await.unwrap();
    assert_template(discovery.next().await.unwrap().unwrap(), 16);
    discovery
        .send(WsMessage::Pong(Vec::new().into()))
        .await
        .unwrap();
    discovery
        .send(WsMessage::Ping(vec![1].into()))
        .await
        .unwrap();
    discovery
        .send(WsMessage::Binary(
            super::super::protocol::ResponseRithmicSystemInfo {
                template_id: 17,
                rp_code: vec!["0".into()],
                system_name: vec!["test-system".into()],
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        discovery.next().await.unwrap().unwrap(),
        WsMessage::Pong(_)
    ));

    let (stream, _) = listener.accept().await.unwrap();
    let mut login = accept_async(stream).await.unwrap();
    assert_template(login.next().await.unwrap().unwrap(), 10);
    login
        .send(WsMessage::Binary(
            super::super::protocol::ResponseLogin {
                template_id: 11,
                rp_code: vec!["0".into()],
                heartbeat_interval: Some(30.0),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        ))
        .await
        .unwrap();
    assert_template(login.next().await.unwrap().unwrap(), 18);
    login
}

fn assert_template(message: WsMessage, expected: i32) {
    let WsMessage::Binary(payload) = message else {
        panic!("expected binary Rithmic payload");
    };
    assert_eq!(
        super::super::codec::template_id(&payload).unwrap(),
        expected
    );
}

#[tokio::test]
async fn process_owner_captures_loopback_order_pnl_reconnect_while_blocked() {
    const ISOLATED_RUN: &str = "FLUXTRADE_CAPTURE_SINGLETON_TEST_CHILD";
    if std::env::var_os(ISOLATED_RUN).is_none() {
        for state in ["blocked", "dead"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "process_owner_captures_loopback_order_pnl_reconnect_while_blocked",
                    "--nocapture",
                ])
                .env(ISOLATED_RUN, state)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated process-owner {state} lifecycle test failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _environment_guard = ENV_LOCK.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let capture_root = directory.path().join("capture");
    std::fs::create_dir(&capture_root).unwrap();
    std::fs::set_permissions(&capture_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let key_path = directory.path().join("capture.key");
    std::fs::write(&key_path, [0x51; 32]).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::env::set_var("FLUXTRADE_RITHMIC_CAPTURE_KEY_FILE", &key_path);
    std::env::set_var("FLUXTRADE_RITHMIC_CAPTURE_KEY_VERSION", "lifecycle_test");

    let owner_state =
        if std::env::var_os(ISOLATED_RUN).as_deref() == Some(std::ffi::OsStr::new("dead")) {
            TestWriterState::Dead
        } else {
            TestWriterState::Blocked
        };
    let mut owner = test_owner_writer(&capture_root, owner_state);
    let failed_init = std::panic::catch_unwind(|| {
        initialize_process_capture_with(|| panic!("injected initializer failure"));
    });
    assert!(failed_init.is_err());
    assert!(process_capture().health_counts().4);
    initialize_process_capture_with(|| owner.capture.clone());
    let capture = process_capture().clone();
    let published_address = process_capture() as *const CaptureHandle as usize;
    let accessors: Vec<_> = (0..8)
        .map(|_| std::thread::spawn(|| process_capture() as *const CaptureHandle as usize))
        .collect();
    for accessor in accessors {
        assert_eq!(accessor.join().unwrap(), published_address);
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut first = handshake(&listener).await;
        first
            .send(WsMessage::Binary(
                super::super::protocol::ResponseHeartbeat {
                    template_id: 19,
                    rp_code: vec!["0".into()],
                    ..Default::default()
                }
                .encode_to_vec()
                .into(),
            ))
            .await
            .unwrap();
        assert_template(first.next().await.unwrap().unwrap(), 312);
        first
            .send(WsMessage::Binary(
                super::super::protocol::InstrumentPnLPositionUpdate {
                    template_id: 450,
                    day_pnl: Some(-8.75),
                    net_quantity: Some(1),
                    ..Default::default()
                }
                .encode_to_vec()
                .into(),
            ))
            .await
            .unwrap();
        drop(first);

        let mut reconnected = handshake(&listener).await;
        reconnected
            .send(WsMessage::Binary(
                super::super::protocol::ResponseHeartbeat {
                    template_id: 19,
                    rp_code: vec!["0".into()],
                    ..Default::default()
                }
                .encode_to_vec()
                .into(),
            ))
            .await
            .unwrap();
        assert_template(reconnected.next().await.unwrap().unwrap(), 312);
    });

    let login = super::super::session::LoginParameters::new(
        "test-user".into(),
        "test-password".into(),
        "test-system".into(),
        "FluxTrade".into(),
        "0.1.0".into(),
        super::super::session::Plant::Order,
    )
    .unwrap();
    let mut connection = super::connect_with_capture(
        &url,
        login.clone(),
        std::time::Duration::from_secs(1),
        capture.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        connection.next_event().await.unwrap(),
        super::ConnectionEvent::HeartbeatConfirmed
    );
    let order = super::super::protocol::RequestNewOrder {
        template_id: 312,
        symbol: Some("MNQZ6".into()),
        quantity: Some(1),
        price: Some(20_000.0),
        ..Default::default()
    }
    .encode_to_vec();
    connection.send_payload(order.clone()).await.unwrap();
    let pnl = connection.next_event().await.unwrap();
    assert!(matches!(pnl, super::ConnectionEvent::Payload(payload)
        if super::super::codec::template_id(&payload).unwrap() == 450));
    drop(connection);

    let mut reconnected = super::connect_with_capture(
        &url,
        login,
        std::time::Duration::from_secs(1),
        capture.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        reconnected.next_event().await.unwrap(),
        super::ConnectionEvent::HeartbeatConfirmed
    );
    reconnected.send_payload(order).await.unwrap();
    assert_eq!(
        capture.counts(),
        if matches!(owner_state, TestWriterState::Dead) {
            (3, 0, 0)
        } else {
            (3, 1, 2)
        }
    );
    assert_eq!(capture.health_counts().1, 0);
    assert_eq!(
        capture.health_counts().4,
        matches!(owner_state, TestWriterState::Dead)
    );
    assert_eq!(
        process_capture() as *const CaptureHandle as usize,
        published_address
    );

    let shutdown_started = std::time::Instant::now();
    shutdown_process_capture();
    assert!(shutdown_started.elapsed() < std::time::Duration::from_millis(100));
    assert_eq!(
        process_capture() as *const CaptureHandle as usize,
        published_address
    );
    assert_eq!(
        capture.counts(),
        if matches!(owner_state, TestWriterState::Dead) {
            (3, 0, 0)
        } else {
            (3, 1, 2)
        }
    );

    if matches!(owner_state, TestWriterState::Dead) {
        let (_path, health) = owner.release_and_join();
        assert_eq!(health.1, 0);
        assert_eq!(health.2, 3);
        assert_eq!(health.3, 3);
    } else {
        owner.release_without_join();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while capture.health_counts().1 == 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(capture.health_counts().1, 1);
        assert_eq!(capture.health_counts().2, 2);
        let content = std::fs::read_to_string(&owner.jsonl_path).unwrap();
        let records: Vec<serde_json::Value> = content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["template_id"], 312);
        assert!(content.contains("\"key_version\":\"test\""));
    }
    server.await.unwrap();
    drop(reconnected);
}
