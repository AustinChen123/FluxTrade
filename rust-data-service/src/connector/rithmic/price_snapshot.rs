use super::{
    config,
    market::{self, MarketDataEvent, SubscriptionAction},
    session::Plant,
    transport::{self, ConnectionEvent},
};
use anyhow::{ensure, Context, Result};
use std::time::Duration;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn run(
    profile: &str,
    exchange: &str,
    symbol: &str,
    wait: Duration,
) -> Result<market::LastTradeUpdate> {
    ensure!(
        !wait.is_zero(),
        "Rithmic price snapshot timeout must be positive"
    );
    let request = market::last_trade_request(exchange, symbol, SubscriptionAction::Subscribe)?;
    let runtime = config::load(profile, Plant::Ticker)?;

    tokio::time::timeout(wait, async {
        let mut connection =
            transport::connect(&runtime.url, runtime.login, RESPONSE_TIMEOUT).await?;
        wait_for_heartbeat(&mut connection).await?;
        connection.send_payload(request).await?;

        loop {
            match connection.next_event().await? {
                ConnectionEvent::HeartbeatConfirmed => {}
                ConnectionEvent::Payload(payload) => {
                    if let Some(trade) = accept_event(
                        market::decode_market_data_event(&payload)?,
                        exchange,
                        symbol,
                    )? {
                        return Ok(trade);
                    }
                }
            }
        }
    })
    .await
    .context("Rithmic price snapshot timed out")?
}

async fn wait_for_heartbeat(connection: &mut transport::RithmicConnection) -> Result<()> {
    let event = connection.next_event().await?;
    ensure!(
        event == ConnectionEvent::HeartbeatConfirmed,
        "Rithmic TICKER payload arrived before heartbeat confirmation"
    );
    Ok(())
}

fn accept_event(
    event: MarketDataEvent,
    expected_exchange: &str,
    expected_symbol: &str,
) -> Result<Option<market::LastTradeUpdate>> {
    match event {
        MarketDataEvent::SubscriptionAccepted
        | MarketDataEvent::LastTradeCleared
        | MarketDataEvent::LastTradeUnchanged => Ok(None),
        MarketDataEvent::LastTrade(trade) => {
            ensure!(
                trade.exchange == expected_exchange && trade.symbol == expected_symbol,
                "Rithmic price snapshot instrument identity mismatch"
            );
            Ok(Some(trade))
        }
        MarketDataEvent::Rejected { response_codes, .. } => anyhow::bail!(
            "Rithmic price snapshot subscription was rejected: {}",
            response_codes.join(",")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rithmic_ledger::market::{Aggressor, LastTradeUpdate};
    use rust_decimal_macros::dec;

    #[test]
    fn snapshot_event_matrix_fails_closed() {
        for event in [
            MarketDataEvent::SubscriptionAccepted,
            MarketDataEvent::LastTradeCleared,
            MarketDataEvent::LastTradeUnchanged,
        ] {
            assert!(accept_event(event, "CME", "NQU6").unwrap().is_none());
        }

        let trade = LastTradeUpdate {
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            price: dec!(29784.75),
            quantity: dec!(1),
            aggressor: Some(Aggressor::Buy),
            timestamp: 1_800_000_001_000,
            is_snapshot: true,
        };
        assert_eq!(
            accept_event(MarketDataEvent::LastTrade(trade), "CME", "NQU6")
                .unwrap()
                .unwrap()
                .price,
            dec!(29784.75)
        );

        let wrong_instrument = LastTradeUpdate {
            exchange: "CME".to_string(),
            symbol: "ESU6".to_string(),
            price: dec!(7000),
            quantity: dec!(1),
            aggressor: None,
            timestamp: 1_800_000_001_000,
            is_snapshot: true,
        };
        assert!(accept_event(MarketDataEvent::LastTrade(wrong_instrument), "CME", "NQU6").is_err());

        assert!(accept_event(
            MarketDataEvent::Rejected {
                user_messages: vec![],
                response_codes: vec!["permission-denied".to_string()],
            },
            "CME",
            "NQU6",
        )
        .is_err());
    }
}
