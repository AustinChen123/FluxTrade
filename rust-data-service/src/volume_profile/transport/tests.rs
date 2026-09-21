use super::*;
use std::collections::BTreeMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use Action::{Retry, RetryAfter, Stop, Success};

async fn local(
    response: &[u8],
    hold_ms: u64,
    cap: usize,
) -> (Transport, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let bytes = response.to_vec();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            assert!(request.len() < 8192);
        }
        let _ = socket.write_all(&bytes).await;
        tokio::time::sleep(Duration::from_millis(hold_ms)).await;
        String::from_utf8(request).unwrap()
    });
    let mut transport = Transport::new(Duration::from_millis(50), cap).unwrap();
    transport.endpoint = Url::parse(&format!("http://{address}/api/v3/aggTrades")).unwrap();
    (transport, server)
}

#[tokio::test]
async fn exact_query_and_no_credentials() {
    for request in [
        Request::First {
            start_time: 100,
            end_time: 199,
        },
        Request::Next { from_id: u64::MAX },
    ] {
        let (transport, server) =
            local(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]", 0, 2).await;
        let outcome = transport.fetch(&request).await;
        assert_eq!(outcome.body, Some(b"[]".to_vec()));
        assert_eq!(outcome.response_bytes, 2);
        assert_eq!(outcome.failure, None);
        let wire = server.await.unwrap();
        let target = wire.split_whitespace().nth(1).unwrap();
        let url = Url::parse(&format!("http://127.0.0.1{target}")).unwrap();
        let mut expected = BTreeMap::from([
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("limit".into(), "1000".into()),
        ]);
        match request {
            Request::First { .. } => {
                expected.insert("startTime".into(), "100".into());
                expected.insert("endTime".into(), "199".into());
            }
            Request::Next { .. } => {
                expected.insert("fromId".into(), u64::MAX.to_string());
            }
        }
        assert_eq!(url.path(), "/api/v3/aggTrades");
        assert_eq!(url.query_pairs().count(), expected.len());
        assert_eq!(
            url.query_pairs().into_owned().collect::<BTreeMap<_, _>>(),
            expected
        );
        for forbidden in
            "authorization: proxy-authorization: x-mbx-apikey: cookie:".split_whitespace()
        {
            assert!(!wire.to_ascii_lowercase().contains(forbidden));
        }
    }
    let production = Transport::new(Duration::from_secs(1), 1).unwrap();
    assert_eq!(production.endpoint.as_str(), ENDPOINT);
    assert!(Transport::new(Duration::ZERO, 1).is_err());
    assert!(Transport::new(Duration::from_secs(1), 0).is_err());
}

#[tokio::test]
async fn bounded_body_matrix() {
    for (wire, success) in [
        ("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n[]", true),
        (
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n[]\r\n0\r\n\r\n",
            true,
        ),
        (
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
            false,
        ),
        ("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc", false),
        ("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n", false),
    ] {
        let (transport, server) = local(wire.as_bytes(), 0, 2).await;
        let outcome = transport.fetch(&Request::Next { from_id: 1 }).await;
        assert_eq!(outcome.disposition.status, Some(200));
        assert_eq!(outcome.body.is_some(), success);
        assert_eq!(outcome.failure, (!success).then_some(Failure::Protocol));
        assert_eq!(
            outcome.disposition.action,
            if success { Success } else { Stop }
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn http_policy_and_all_retry_after_values() {
    for (status, headers, expected) in [
        (429, "Retry-After: 2\r\n", RetryAfter(2)),
        (429, "Retry-After: 0\r\n", Action::RetryAfter(0)),
        (429, "Retry-After: 2\r\nRetry-After: 2\r\n", RetryAfter(2)),
        (429, "Retry-After: 2\r\nRetry-After: 3\r\n", Action::Stop),
        (429, "", Action::Stop),
        (429, "Retry-After: bad\r\n", Action::Stop),
        (418, "Retry-After: 2\r\n", Action::Stop),
        (451, "", Action::Stop),
        (400, "", Action::Stop),
        (500, "", Action::Retry),
        (302, "Location: http://127.0.0.1:1/\r\n", Action::Stop),
    ] {
        let wire = format!("HTTP/1.1 {status} Test\r\n{headers}Content-Length: 99999999\r\n\r\n");
        let (transport, server) = local(wire.as_bytes(), 0, 2).await;
        let result = transport.fetch(&Request::Next { from_id: 1 }).await;
        assert_eq!(result.disposition.status, Some(status));
        assert_eq!(result.disposition.action, expected);
        assert_eq!(result.failure, None);
        assert!(result.body.is_none());
        server.await.unwrap();
    }
    let (transport, server) = local(b"HTTP/1.1 429 Test\r\nRetry-After: \xff\r\n\r\n", 0, 2).await;
    let result = transport.fetch(&Request::Next { from_id: 1 }).await;
    assert_eq!(result.disposition.action, Stop);
    assert_eq!(result.failure, None);
    server.await.unwrap();
}

#[tokio::test]
async fn header_body_timeout_and_connection_failure_are_not_success() {
    for (body, status, hold, bytes) in [
        ("", None, 150, 0),
        ("", Some(200), 150, 0),
        ("[", Some(200), 150, 1),
        ("[", Some(200), 0, 1),
    ] {
        let wire = format!("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{body}");
        let wire = if status.is_some() {
            wire.as_bytes()
        } else {
            b""
        };
        let (transport, server) = local(wire, hold, 2).await;
        let result = transport.fetch(&Request::Next { from_id: 1 }).await;
        assert_eq!(result.disposition.status, status);
        assert_eq!(result.disposition.action, Retry);
        assert!(result.body.is_none());
        assert_eq!(result.response_bytes, bytes);
        assert_eq!(
            result.failure,
            Some(if hold == 0 {
                Failure::Connection
            } else {
                Failure::Timeout
            })
        );
        server.await.unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    drop(listener);
    let mut transport = Transport::new(Duration::from_millis(50), 2).unwrap();
    transport.endpoint = endpoint;
    let result = transport.fetch(&Request::Next { from_id: 1 }).await;
    assert_eq!(result.disposition.status, None);
    assert_eq!(result.failure, Some(Failure::Connection));
    assert_eq!(result.disposition.action, Retry);
    assert!(result.body.is_none());
}
