use super::{
    config,
    ledger::{AccountIdentity, UserType},
    ledger_runtime::{discover_order_account_with_login, next_payload, wait_for_heartbeat},
    order::{self, TradeRoute, TradeRouteEvent},
    session::Plant,
    transport::{self, RithmicConnection},
};
use anyhow::{ensure, Context, Result};
use std::time::Duration;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const TRADE_ROUTES_KEY: &str = "fluxtrade-order-routes";
pub(super) const SUBSCRIBE_KEY: &str = "fluxtrade-order-subscribe";

pub(super) async fn connect_and_prepare(
    profile: &str,
    account_id: Option<&str>,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    let runtime = config::load(profile, Plant::Order)?;
    connect_and_prepare_runtime(runtime, account_id).await
}

pub(super) async fn connect_and_prepare_runtime(
    runtime: config::RuntimeConfig,
    account_id: Option<&str>,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    let mut connection = transport::connect(&runtime.url, runtime.login, RESPONSE_TIMEOUT).await?;
    wait_for_heartbeat(&mut connection, "ORDER").await?;
    let (account, login_info) =
        discover_order_account_with_login(&mut connection, account_id).await?;
    let account = account.identity;

    connection
        .send_payload(order::trade_routes_request(TRADE_ROUTES_KEY)?)
        .await?;
    let routes = collect_trade_routes(&mut connection).await?;
    ensure!(!routes.is_empty(), "Rithmic returned no open trade routes");

    connection
        .send_payload(order::subscribe_order_updates_request(
            SUBSCRIBE_KEY,
            &account,
        )?)
        .await?;
    let payload = tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(&mut connection))
        .await
        .context("Rithmic order-update subscription timed out")??;
    order::decode_subscribe_order_updates_response(&payload, SUBSCRIBE_KEY)?;
    Ok((connection, account, login_info.user_type, routes))
}

async fn collect_trade_routes(connection: &mut RithmicConnection) -> Result<Vec<TradeRoute>> {
    let mut routes = Vec::new();
    loop {
        let payload = tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(connection))
            .await
            .context("Rithmic trade-route request timed out")??;
        match order::decode_trade_route_event(&payload, TRADE_ROUTES_KEY)? {
            TradeRouteEvent::Route(route) => routes.push(route),
            TradeRouteEvent::Completed => return Ok(routes),
        }
    }
}
