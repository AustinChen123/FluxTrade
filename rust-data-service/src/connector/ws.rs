use anyhow::{bail, Result};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

#[allow(dead_code)]
pub struct WebSocketManager {
    pub url: String,
    pub max_retries: Option<u32>,
    pub initial_backoff: Duration,
}

impl WebSocketManager {
    #[allow(dead_code)]
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            max_retries: None, // Default to infinite retries
            initial_backoff: Duration::from_secs(1),
        }
    }

    #[allow(dead_code)]
    pub async fn connect_with_retry<F, Fut, C, Cut>(
        &self,
        mut on_connect: C,
        mut on_message: F,
    ) -> Result<()>
    where
        F: FnMut(Message) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
        C: FnMut(
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        ) -> Cut,
        Cut: std::future::Future<
            Output = Result<(
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                >,
                Result<()>,
            )>,
        >,
    {
        let mut retries = 0;
        let mut backoff = self.initial_backoff;

        loop {
            match self
                .connect_and_loop(&mut on_connect, &mut on_message)
                .await
            {
                Ok(_) => {
                    info!("Connection closed gracefully");
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    error!(
                        "WebSocket error (attempt {}): {}",
                        retries, e
                    );

                    if let Some(max) = self.max_retries {
                        if retries >= max {
                            bail!(
                                "Max retries reached for WebSocket connection to {}",
                                self.url
                            );
                        }
                    }

                    warn!("HealthStatus: Disconnected/Paused. Reconnecting in {:?}", backoff);
                    sleep(backoff).await;
                    if backoff < Duration::from_secs(60) {
                        backoff *= 2; // Exponential backoff up to 60s cap
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn connect_and_loop<F, Fut, C, Cut>(
        &self,
        on_connect: &mut C,
        on_message: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Message) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
        C: FnMut(
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        ) -> Cut,
        Cut: std::future::Future<
            Output = Result<(
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                >,
                Result<()>,
            )>,
        >,
    {
        let ws_stream = connect_async(&self.url).await?.0;
        info!("Successfully connected to {}", self.url);

        // Run connection hook
        let (ws_stream, res) = on_connect(ws_stream).await?;
        res?;

        let (_write, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            let message = message?;

            match message {
                Message::Ping(payload) => {
                    on_message(Message::Pong(payload)).await?;
                }
                Message::Close(_) => {
                    return Ok(());
                }
                _ => {
                    on_message(message).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn test_websocket_manager_connect() {
        // 1. Start a mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{}", addr);

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                if let Ok(mut ws_stream) = accept_async(stream).await {
                    ws_stream.send(Message::Text("hello".into())).await.unwrap();
                    sleep(Duration::from_millis(100)).await;
                }
            }
        });

        // 2. Use manager to connect
        let manager = WebSocketManager::new(&url);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(async move {
            manager
                .connect_with_retry(
                    |ws| async move { Ok((ws, Ok(()))) },
                    |msg| {
                        let tx = tx.clone();
                        async move {
                            if let Message::Text(text) = msg {
                                tx.send(text).await.unwrap();
                            }
                            Ok(())
                        }
                    },
                )
                .await
        });

        // 3. Verify message received
        let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap();
        assert_eq!(msg.unwrap(), "hello");

        handle.abort();
    }

    #[tokio::test]
    async fn test_websocket_manager_retry() {
        let url = "ws://127.0.0.1:1"; // Non-existent port to force failure
        let mut manager = WebSocketManager::new(url);
        manager.max_retries = Some(2);
        manager.initial_backoff = Duration::from_millis(10);

        let result = manager
            .connect_with_retry(|ws| async move { Ok((ws, Ok(()))) }, |_| async { Ok(()) })
            .await;

        // Should fail after 2 retries
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Max retries reached"));
    }
}
