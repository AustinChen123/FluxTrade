mod aggregator;
mod connector;
mod model;
mod publisher;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, Level};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let url = "wss://ws.backpack.exchange/";
    info!("Connecting to Backpack WS: {}", url);

    let (mut ws_stream, _) = timeout(Duration::from_secs(10), connect_async(url)).await??;
    info!("Connected successfully!");

    // Correcting to "method": "SUBSCRIBE" based on search results
    let sub = json!({
        "method": "SUBSCRIBE",
        "params": ["trade.SOL_USDC", "kline.1m.SOL_USDC"]
    });

    info!("Sending subscription request: {}", sub);
    ws_stream
        .send(Message::Text(sub.to_string().into()))
        .await?;

    info!("Waiting for messages (30s timeout)...");

    let mut count = 0;
    loop {
        match timeout(Duration::from_secs(30), ws_stream.next()).await {
            Ok(Some(msg)) => {
                let msg = msg?;
                if let Message::Text(text) = msg {
                    info!("Received Data: {}", text);
                    count += 1;
                }
                if count >= 3 {
                    break;
                }
            }
            Ok(None) => {
                info!("Stream closed by server");
                break;
            }
            Err(_) => {
                error!("Timeout waiting for message.");
                break;
            }
        }
    }

    Ok(())
}
