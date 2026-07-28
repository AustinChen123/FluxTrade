use super::{
    config, front_month,
    session::Plant,
    transport::{self, ConnectionEvent},
};
use anyhow::{ensure, Context, Result};
use std::time::Duration;

const REQUEST_KEY: &str = "front-month";
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn run(
    profile: &str,
    root_symbol: &str,
    exchange: &str,
    exclusive_session: bool,
    wait: Duration,
) -> Result<String> {
    ensure!(
        exclusive_session,
        "exclusive_session=true is required after stopping other Rithmic TICKER clients"
    );
    ensure!(
        !wait.is_zero(),
        "Rithmic front-month timeout must be positive"
    );
    let request = front_month::request(REQUEST_KEY, root_symbol, exchange)?;
    let runtime = config::load(profile, Plant::Ticker)?;

    tokio::time::timeout(wait, async {
        let mut connection =
            transport::connect(&runtime.url, runtime.login, RESPONSE_TIMEOUT).await?;
        let event = connection.next_event().await?;
        ensure!(
            event == ConnectionEvent::HeartbeatConfirmed,
            "Rithmic TICKER payload arrived before heartbeat confirmation"
        );
        connection.send_payload(request).await?;

        loop {
            match connection.next_event().await? {
                ConnectionEvent::HeartbeatConfirmed => {}
                ConnectionEvent::Payload(payload) => {
                    return front_month::decode_response(
                        &payload,
                        REQUEST_KEY,
                        root_symbol,
                        exchange,
                    );
                }
            }
        }
    })
    .await
    .context("Rithmic front-month request timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn preflight_rejects_unsafe_or_incomplete_requests_before_credentials() {
        for result in [
            run("profile", "NQ", "CME", false, Duration::from_secs(1)).await,
            run("profile", "NQ", "CME", true, Duration::ZERO).await,
            run("profile", "", "CME", true, Duration::from_secs(1)).await,
            run("profile", "NQ", "", true, Duration::from_secs(1)).await,
        ] {
            assert!(result.is_err());
        }
    }
}
