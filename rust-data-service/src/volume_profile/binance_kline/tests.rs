use super::super::work_policy::{Action, Failure};
use super::*;
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const DAY: i64 = 86_400_000;
fn day() -> Window {
    Window::new(0, DAY).unwrap()
}

async fn local(response: Vec<u8>, hold: bool) -> (super::super::transport::Outcome, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/klines", listener.local_addr().unwrap());
    let mut transport = Transport::new(Duration::from_millis(100)).unwrap();
    transport.endpoint = reqwest::Url::parse(&endpoint).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        tx.send(String::from_utf8(request).unwrap()).unwrap();
        let _ = socket.write_all(&response).await;
        if hold {
            std::future::pending::<()>().await;
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let outcome = transport.fetch(day(), DAY).await?;
        let request = rx.await?;
        anyhow::Ok((outcome, request))
    })
    .await;
    task.abort();
    let _ = task.await;
    let (outcome, request) = result.expect("loopback fetch/capture deadline").unwrap();
    outcome.validate().unwrap();
    (outcome, request)
}

#[tokio::test]
async fn exact_query_headers_status_and_retry_after_matrix() {
    assert_eq!(
        Transport::new(Duration::from_secs(3))
            .unwrap()
            .endpoint
            .as_str(),
        ENDPOINT
    );
    for timeout in [Duration::ZERO, Duration::from_secs(4)] {
        assert!(Transport::new(timeout).is_err());
    }
    for (status, headers, expected) in [
        (200, "", Action::Success),
        (429, "Retry-After: 0\r\n", Action::RetryAfter(0)),
        (429, "Retry-After: 3\r\n", Action::RetryAfter(3)),
        (429, "", Action::Stop),
        (429, "Retry-After: -1\r\n", Action::Stop),
        (429, "Retry-After: 1.5\r\n", Action::Stop),
        (429, "Retry-After: 01\r\n", Action::Stop),
        (429, "Retry-After: 18446744073709551616\r\n", Action::Stop),
        (429, "Retry-After: 1\r\nRetry-After: 2\r\n", Action::Stop),
        (
            429,
            "Retry-After: 1\r\nRetry-After: 1\r\n",
            Action::RetryAfter(1),
        ),
        (418, "Retry-After: 3\r\n", Action::Stop),
        (451, "", Action::Stop),
        (400, "", Action::Stop),
        (403, "", Action::Stop),
        (500, "", Action::Retry),
        (503, "", Action::Retry),
        (
            302,
            "Location: http://127.0.0.1:1/forbidden\r\n",
            Action::Stop,
        ),
    ] {
        let (outcome, request) = local(
            format!("HTTP/1.1 {status} Test\r\n{headers}Content-Length: 2\r\n\r\n[]").into_bytes(),
            false,
        )
        .await;
        assert_eq!(outcome.disposition.action, expected, "{status} {headers}");
        assert_eq!(outcome.disposition.status, Some(status));
        let target = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = reqwest::Url::parse(&format!("http://localhost{target}")).unwrap();
        let pairs: Vec<_> = url.query_pairs().collect();
        assert_eq!(pairs.len(), 5);
        assert_eq!(
            pairs.into_iter().collect::<BTreeMap<_, _>>(),
            [
                ("symbol".into(), "BTCUSDT".into()),
                ("interval".into(), "1d".into()),
                ("startTime".into(), "0".into()),
                ("endTime".into(), "86399999".into()),
                ("limit".into(), "1".into())
            ]
            .into()
        );
        let lower = request.to_ascii_lowercase();
        for header in [
            "authorization:",
            "proxy-authorization:",
            "cookie:",
            "x-mbx-apikey:",
        ] {
            assert!(!lower.contains(header));
        }
    }
}

#[tokio::test]
async fn body_caps_timeouts_connection_and_invalid_day() {
    let (closed, _) = local(
        b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n[]".to_vec(),
        false,
    )
    .await;
    assert_eq!(closed.body.as_deref(), Some(b"[]".as_slice()));
    let (invalid_header, _) = local(
        b"HTTP/1.1 429 Test\r\nRetry-After: \xff\r\nContent-Length: 0\r\n\r\n".to_vec(),
        false,
    )
    .await;
    assert_eq!(invalid_header.disposition.action, Action::Stop);
    let (truncated, _) = local(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc".to_vec(),
        false,
    )
    .await;
    assert_eq!(
        (truncated.failure, truncated.response_bytes),
        (Some(Failure::Connection), 3)
    );
    for size in [RAW_LIMIT, RAW_LIMIT + 1] {
        let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        response.extend_from_slice(format!("{size:x}\r\n").as_bytes());
        response.extend(vec![b'x'; size]);
        response.extend_from_slice(b"\r\n0\r\n\r\n");
        let (outcome, _) = local(response, false).await;
        assert_eq!(outcome.response_bytes, size as u64);
        assert_eq!(
            outcome.failure,
            if size == RAW_LIMIT {
                None
            } else {
                Some(Failure::Protocol)
            }
        );
        assert_eq!(outcome.body.is_some(), size == RAW_LIMIT);
    }
    let (outcome, _) = local(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            RAW_LIMIT + 1
        )
        .into_bytes(),
        false,
    )
    .await;
    assert_eq!(
        (outcome.failure, outcome.response_bytes),
        (Some(Failure::Protocol), 0)
    );
    for (response, status, bytes) in [
        (b"".as_slice(), None, 0),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc".as_slice(),
            Some(200),
            3,
        ),
    ] {
        let (outcome, _) = local(response.to_vec(), true).await;
        assert_eq!(
            (
                outcome.failure,
                outcome.disposition.status,
                outcome.response_bytes
            ),
            (Some(Failure::Timeout), status, bytes)
        );
        assert_eq!(outcome.disposition.action, Action::Retry);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut transport = Transport::new(Duration::from_millis(100)).unwrap();
    transport.endpoint =
        reqwest::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    for (window, now) in [
        (Window::new(1, DAY + 1).unwrap(), DAY + 1),
        (day(), DAY - 1),
        (Window::new(0, DAY - 1).unwrap(), DAY),
    ] {
        assert!(transport.fetch(window, now).await.is_err());
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    drop(listener);
    let failed = transport.fetch(day(), DAY).await.unwrap();
    assert_eq!(failed.failure, Some(Failure::Connection));
    assert_eq!(failed.disposition.action, Action::Retry);
}

#[test]
fn proxy_environment_is_ignored_in_isolated_process() {
    const MARKER: &str = "FLUXTRADE_TEST_KLINE_PROXY_CHILD";
    if std::env::var_os(MARKER).is_some() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (outcome, _) = local(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]".to_vec(),
                false,
            )
            .await;
            assert_eq!(outcome.disposition.action, Action::Success);
        });
        return;
    }
    use std::io::{Read, Seek};
    let mut diagnostic = tempfile::tempfile().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "volume_profile::binance_kline::tests::proxy_environment_is_ignored_in_isolated_process"])
        .env(MARKER, "1").env("HTTP_PROXY", "http://127.0.0.1:1").env("http_proxy", "http://127.0.0.1:1")
        .env("ALL_PROXY", "http://127.0.0.1:1").env("all_proxy", "http://127.0.0.1:1")
        .env("NO_PROXY", "").env("no_proxy", "")
        .stdout(diagnostic.try_clone().unwrap()).stderr(diagnostic.try_clone().unwrap())
        .spawn().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let result = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Err(error) => break Err(format!("child wait failed: {error}")),
            Ok(None) if std::time::Instant::now() >= deadline => {
                break Err("proxy child deadline".into())
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
        }
    };
    if result.is_err() {
        let _ = child.kill();
    }
    let reaped = child.wait();
    diagnostic.rewind().unwrap();
    let mut bytes = Vec::new();
    diagnostic.take(4096).read_to_end(&mut bytes).unwrap();
    assert!(
        result.as_ref().is_ok_and(|status| status.success()) && reaped.is_ok(),
        "proxy child {result:?}; {}",
        String::from_utf8_lossy(&bytes)
    );
}
