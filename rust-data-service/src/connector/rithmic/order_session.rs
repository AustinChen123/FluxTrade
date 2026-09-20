use super::{
    config,
    ledger::{AccountIdentity, UserType},
    ledger_runtime::{
        discover_order_account_with_login, is_retryable_snapshot_error, next_payload,
        wait_for_heartbeat,
    },
    order::{self, TradeRoute, TradeRouteEvent},
    session::Plant,
    transport::{self, MaintenanceGuard, RithmicConnection},
};
use anyhow::Result;
use chrono::Utc;
use std::{sync::Arc, time::Duration};

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const TRADE_ROUTES_KEY: &str = "fluxtrade-order-routes";
pub(super) const SUBSCRIBE_KEY: &str = "fluxtrade-order-subscribe";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderSessionDisposition {
    Retryable,
    Fatal,
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct OrderSessionFailure {
    disposition: OrderSessionDisposition,
    #[source]
    source: anyhow::Error,
}

fn mark_order_session_failure(
    error: anyhow::Error,
    disposition: OrderSessionDisposition,
) -> anyhow::Error {
    OrderSessionFailure {
        disposition,
        source: error,
    }
    .into()
}

fn classify_order_session_failure(error: anyhow::Error) -> anyhow::Error {
    let disposition = if transport::is_retryable_connection_error(&error)
        || is_retryable_snapshot_error(&error)
    {
        OrderSessionDisposition::Retryable
    } else {
        OrderSessionDisposition::Fatal
    };
    mark_order_session_failure(error, disposition)
}

pub(super) fn retryable_order_session_failure(error: anyhow::Error) -> anyhow::Error {
    mark_order_session_failure(error, OrderSessionDisposition::Retryable)
}

pub(super) fn is_retryable_order_session_error(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<OrderSessionFailure>()
            .is_some_and(|failure| failure.disposition == OrderSessionDisposition::Retryable)
    })
}

pub(super) async fn connect_and_prepare(
    profile: &str,
    account_id: Option<&str>,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    let runtime = config::load(profile, Plant::Order)
        .map_err(|error| mark_order_session_failure(error, OrderSessionDisposition::Fatal))?;
    connect_and_prepare_runtime_guarded(
        runtime,
        account_id,
        Arc::new(|| transport::maintenance_active(Utc::now())),
    )
    .await
}

#[cfg(test)]
pub(super) async fn connect_and_prepare_runtime(
    runtime: config::RuntimeConfig,
    account_id: Option<&str>,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    connect_and_prepare_runtime_guarded(runtime, account_id, Arc::new(|| false)).await
}

async fn connect_and_prepare_runtime_guarded(
    runtime: config::RuntimeConfig,
    account_id: Option<&str>,
    maintenance_active: MaintenanceGuard,
) -> Result<(
    RithmicConnection,
    AccountIdentity,
    UserType,
    Vec<TradeRoute>,
)> {
    let mut connection = transport::connect_with_maintenance_guard(
        &runtime.url,
        runtime.login,
        RESPONSE_TIMEOUT,
        maintenance_active,
    )
    .await
    .map_err(classify_order_session_failure)?;
    wait_for_heartbeat(&mut connection, "ORDER")
        .await
        .map_err(classify_order_session_failure)?;
    let (account, login_info) = discover_order_account_with_login(&mut connection, account_id)
        .await
        .map_err(classify_order_session_failure)?;
    let account = account.identity;

    connection
        .send_payload(
            order::trade_routes_request(TRADE_ROUTES_KEY)
                .map_err(classify_order_session_failure)?,
        )
        .await
        .map_err(classify_order_session_failure)?;
    let routes = collect_trade_routes(&mut connection)
        .await
        .map_err(classify_order_session_failure)?;
    if routes.is_empty() {
        return Err(mark_order_session_failure(
            anyhow::anyhow!("Rithmic returned no open trade routes"),
            OrderSessionDisposition::Fatal,
        ));
    }

    connection
        .send_payload(
            order::subscribe_order_updates_request(SUBSCRIBE_KEY, &account)
                .map_err(classify_order_session_failure)?,
        )
        .await
        .map_err(classify_order_session_failure)?;
    let payload = match tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(&mut connection)).await
    {
        Ok(payload) => payload.map_err(classify_order_session_failure)?,
        Err(error) => {
            return Err(retryable_order_session_failure(
                anyhow::Error::new(error).context("Rithmic order-update subscription timed out"),
            ));
        }
    };
    order::decode_subscribe_order_updates_response(&payload, SUBSCRIBE_KEY)
        .map_err(classify_order_session_failure)?;
    Ok((connection, account, login_info.user_type, routes))
}

async fn collect_trade_routes(connection: &mut RithmicConnection) -> Result<Vec<TradeRoute>> {
    let mut routes = Vec::new();
    loop {
        let payload = match tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(connection)).await {
            Ok(payload) => payload?,
            Err(error) => {
                return Err(retryable_order_session_failure(
                    anyhow::Error::new(error).context("Rithmic trade-route request timed out"),
                ));
            }
        };
        match order::decode_trade_route_event(&payload, TRADE_ROUTES_KEY)? {
            TradeRouteEvent::Route(route) => routes.push(route),
            TradeRouteEvent::Completed => return Ok(routes),
        }
    }
}
