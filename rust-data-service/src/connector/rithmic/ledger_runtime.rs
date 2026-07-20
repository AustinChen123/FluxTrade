use super::{
    config,
    ledger::{
        self, Account, AccountIdentity, AccountListEvent, AccountSummarySnapshot,
        InstrumentPositionSnapshot, LoginInfo, OrderSnapshot, OrderSnapshotEvent, PnlSnapshotEvent,
    },
    session::Plant,
    transport::{self, ConnectionEvent, RithmicConnection},
};
use anyhow::{ensure, Context, Result};
use std::{collections::HashSet, time::Duration};

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);
const LOGIN_INFO_KEY: &str = "fluxtrade-ledger-login";
const ACCOUNT_LIST_KEY: &str = "fluxtrade-ledger-accounts";
const ORDER_SNAPSHOT_KEY: &str = "fluxtrade-ledger-orders";
const PNL_SNAPSHOT_KEY: &str = "fluxtrade-ledger-pnl";

#[derive(Debug)]
/// ORDER and PNL snapshots are complete individually but not atomic across plants.
pub(crate) struct RemoteLedgerSnapshot {
    pub(crate) account: Account,
    pub(crate) orders: Vec<OrderSnapshot>,
    pub(crate) positions: Vec<InstrumentPositionSnapshot>,
    pub(crate) account_summary: Option<AccountSummarySnapshot>,
}

pub(crate) async fn run(profile: &str, account_id: Option<&str>) -> Result<RemoteLedgerSnapshot> {
    let account_id = account_id
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty());

    let order_runtime = config::load(profile, Plant::Order)?;
    let mut order_connection =
        transport::connect(&order_runtime.url, order_runtime.login, RESPONSE_TIMEOUT).await?;
    wait_for_heartbeat(&mut order_connection, "ORDER").await?;

    order_connection
        .send_payload(ledger::login_info_request(LOGIN_INFO_KEY)?)
        .await?;
    let login_info = tokio::time::timeout(SNAPSHOT_TIMEOUT, async {
        let payload = next_payload(&mut order_connection).await?;
        ledger::decode_login_info(&payload, LOGIN_INFO_KEY)
    })
    .await
    .context("Rithmic login-info snapshot timed out")??;

    order_connection
        .send_payload(ledger::account_list_request(ACCOUNT_LIST_KEY, &login_info)?)
        .await?;
    let account = tokio::time::timeout(
        SNAPSHOT_TIMEOUT,
        collect_account(&mut order_connection, &login_info, account_id),
    )
    .await
    .context("Rithmic account-list snapshot timed out")??;

    order_connection
        .send_payload(ledger::show_orders_request(
            ORDER_SNAPSHOT_KEY,
            &account.identity,
        )?)
        .await?;
    let orders = tokio::time::timeout(
        SNAPSHOT_TIMEOUT,
        collect_orders(&mut order_connection, &account.identity),
    )
    .await
    .context("Rithmic order snapshot timed out")??;
    drop(order_connection);

    let pnl_runtime = config::load(profile, Plant::Pnl)?;
    let mut pnl_connection =
        transport::connect(&pnl_runtime.url, pnl_runtime.login, RESPONSE_TIMEOUT).await?;
    wait_for_heartbeat(&mut pnl_connection, "PNL").await?;
    pnl_connection
        .send_payload(ledger::pnl_position_snapshot_request(
            PNL_SNAPSHOT_KEY,
            &account.identity,
        )?)
        .await?;
    let (positions, account_summary) = tokio::time::timeout(
        SNAPSHOT_TIMEOUT,
        collect_pnl(&mut pnl_connection, &account.identity),
    )
    .await
    .context("Rithmic PnL snapshot timed out")??;

    Ok(RemoteLedgerSnapshot {
        account,
        orders,
        positions,
        account_summary,
    })
}

async fn wait_for_heartbeat(connection: &mut RithmicConnection, plant: &str) -> Result<()> {
    let event = tokio::time::timeout(RESPONSE_TIMEOUT, connection.next_event())
        .await
        .with_context(|| format!("Rithmic {plant} heartbeat timed out"))??;
    ensure!(
        event == ConnectionEvent::HeartbeatConfirmed,
        "Rithmic {plant} payload arrived before heartbeat confirmation"
    );
    Ok(())
}

async fn next_payload(connection: &mut RithmicConnection) -> Result<Vec<u8>> {
    loop {
        match connection.next_event().await? {
            ConnectionEvent::HeartbeatConfirmed => {}
            ConnectionEvent::Payload(payload) => return Ok(payload),
        }
    }
}

async fn collect_account(
    connection: &mut RithmicConnection,
    login_info: &LoginInfo,
    requested_account_id: Option<&str>,
) -> Result<Account> {
    let mut selected = None;
    loop {
        let payload = next_payload(connection).await?;
        if accept_account_event(
            &mut selected,
            ledger::decode_account_list_event(&payload, ACCOUNT_LIST_KEY)?,
            login_info,
            requested_account_id,
        )? {
            return selected.context("Rithmic ledger account was not found");
        }
    }
}

async fn collect_orders(
    connection: &mut RithmicConnection,
    account: &AccountIdentity,
) -> Result<Vec<OrderSnapshot>> {
    let mut orders = Vec::new();
    let mut basket_ids = HashSet::new();
    loop {
        let payload = next_payload(connection).await?;
        if accept_order_event(
            &mut orders,
            &mut basket_ids,
            ledger::decode_order_snapshot_event(&payload, ORDER_SNAPSHOT_KEY, account)?,
        )? {
            return Ok(orders);
        }
    }
}

async fn collect_pnl(
    connection: &mut RithmicConnection,
    account: &AccountIdentity,
) -> Result<(
    Vec<InstrumentPositionSnapshot>,
    Option<AccountSummarySnapshot>,
)> {
    let mut positions = Vec::new();
    let mut instruments = HashSet::new();
    let mut account_summary = None;
    loop {
        let payload = next_payload(connection).await?;
        if accept_pnl_event(
            &mut positions,
            &mut instruments,
            &mut account_summary,
            ledger::decode_pnl_snapshot_event(&payload, PNL_SNAPSHOT_KEY, account)?,
        )? {
            return Ok((positions, account_summary));
        }
    }
}

fn accept_account_event(
    selected: &mut Option<Account>,
    event: AccountListEvent,
    login_info: &LoginInfo,
    requested_account_id: Option<&str>,
) -> Result<bool> {
    match event {
        AccountListEvent::Account(account) => {
            ensure_login_identity(&account.identity, login_info)?;
            if requested_account_id
                .is_none_or(|account_id| account.identity.account_id == account_id)
            {
                ensure!(
                    selected.is_none(),
                    "multiple Rithmic ledger accounts require --account-id"
                );
                *selected = Some(account);
            }
            Ok(false)
        }
        AccountListEvent::Completed => Ok(true),
    }
}

fn accept_order_event(
    orders: &mut Vec<OrderSnapshot>,
    basket_ids: &mut HashSet<String>,
    event: OrderSnapshotEvent,
) -> Result<bool> {
    match event {
        OrderSnapshotEvent::Snapshot(order) => {
            ensure!(
                basket_ids.insert(order.basket_id.clone()),
                "duplicate Rithmic order snapshot basket ID"
            );
            orders.push(*order);
            Ok(false)
        }
        OrderSnapshotEvent::RequestCompleted => Ok(true),
    }
}

fn accept_pnl_event(
    positions: &mut Vec<InstrumentPositionSnapshot>,
    instruments: &mut HashSet<(String, String)>,
    account_summary: &mut Option<AccountSummarySnapshot>,
    event: PnlSnapshotEvent,
) -> Result<bool> {
    match event {
        PnlSnapshotEvent::Instrument(position) => {
            ensure!(
                instruments.insert((position.exchange.clone(), position.symbol.clone())),
                "duplicate Rithmic instrument position snapshot"
            );
            positions.push(position);
            Ok(false)
        }
        PnlSnapshotEvent::Account(summary) => {
            ensure!(
                account_summary.replace(summary).is_none(),
                "duplicate Rithmic account summary snapshot"
            );
            Ok(false)
        }
        PnlSnapshotEvent::RequestCompleted => Ok(true),
    }
}

fn ensure_login_identity(account: &AccountIdentity, login_info: &LoginInfo) -> Result<()> {
    ensure!(
        account.fcm_id == login_info.fcm_id && account.ib_id == login_info.ib_id,
        "Rithmic account-list identity mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn login_info() -> LoginInfo {
        LoginInfo {
            fcm_id: "FCM".to_string(),
            ib_id: "IB".to_string(),
            user_type: ledger::UserType::Trader,
        }
    }

    fn identity(fcm_id: &str, ib_id: &str) -> AccountIdentity {
        AccountIdentity {
            fcm_id: fcm_id.to_string(),
            ib_id: ib_id.to_string(),
            account_id: "ACCOUNT".to_string(),
        }
    }

    fn account() -> Account {
        Account {
            identity: identity("FCM", "IB"),
            name: None,
            currency: None,
        }
    }

    fn order() -> OrderSnapshot {
        OrderSnapshot {
            account: identity("FCM", "IB"),
            client_order_id: None,
            basket_id: "BASKET".to_string(),
            exchange_order_id: None,
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            status: "OPEN".to_string(),
            transaction_type: ledger::TransactionType::Buy,
            quantity: Decimal::ONE,
            filled_quantity: None,
            unfilled_quantity: Some(Decimal::ONE),
            average_fill_price: None,
            timestamp_ms: None,
        }
    }

    fn position() -> InstrumentPositionSnapshot {
        InstrumentPositionSnapshot {
            account: identity("FCM", "IB"),
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            net_quantity: 1,
            average_open_fill_price: Some(Decimal::ONE),
            open_pnl: None,
            day_pnl: None,
            timestamp_ms: None,
        }
    }

    fn summary() -> AccountSummarySnapshot {
        AccountSummarySnapshot {
            account: identity("FCM", "IB"),
            account_balance: Some(Decimal::ONE),
            cash_on_hand: None,
            available_buying_power: None,
            day_pnl: None,
            net_quantity: Some(1),
            timestamp_ms: None,
        }
    }

    #[test]
    fn account_identity_matrix_fails_closed() {
        let login = login_info();
        for (fcm_id, ib_id, accepted) in [
            ("FCM", "IB", true),
            ("OTHER", "IB", false),
            ("FCM", "OTHER", false),
            ("OTHER", "OTHER", false),
        ] {
            assert_eq!(
                ensure_login_identity(&identity(fcm_id, ib_id), &login).is_ok(),
                accepted
            );
        }
    }

    #[test]
    fn account_collection_requires_completion_and_rejects_duplicate_selection() {
        let mut selected = None;
        assert!(!accept_account_event(
            &mut selected,
            AccountListEvent::Account(account()),
            &login_info(),
            Some("ACCOUNT"),
        )
        .unwrap());
        assert!(accept_account_event(
            &mut selected,
            AccountListEvent::Completed,
            &login_info(),
            Some("ACCOUNT"),
        )
        .unwrap());
        assert!(accept_account_event(
            &mut selected,
            AccountListEvent::Account(account()),
            &login_info(),
            Some("ACCOUNT"),
        )
        .is_err());

        let mut inferred = None;
        assert!(!accept_account_event(
            &mut inferred,
            AccountListEvent::Account(account()),
            &login_info(),
            None,
        )
        .unwrap());
        assert!(accept_account_event(
            &mut inferred,
            AccountListEvent::Account(account()),
            &login_info(),
            None,
        )
        .is_err());
    }

    #[test]
    fn order_collection_requires_completion_and_rejects_duplicate_baskets() {
        let mut orders = Vec::new();
        let mut basket_ids = HashSet::new();
        assert!(!accept_order_event(
            &mut orders,
            &mut basket_ids,
            OrderSnapshotEvent::Snapshot(Box::new(order())),
        )
        .unwrap());
        assert!(accept_order_event(
            &mut orders,
            &mut basket_ids,
            OrderSnapshotEvent::RequestCompleted,
        )
        .unwrap());
        assert!(accept_order_event(
            &mut orders,
            &mut basket_ids,
            OrderSnapshotEvent::Snapshot(Box::new(order())),
        )
        .is_err());
    }

    #[test]
    fn pnl_collection_requires_completion_and_rejects_duplicate_rows() {
        let mut positions = Vec::new();
        let mut instruments = HashSet::new();
        let mut account_summary = None;
        assert!(!accept_pnl_event(
            &mut positions,
            &mut instruments,
            &mut account_summary,
            PnlSnapshotEvent::Instrument(position()),
        )
        .unwrap());
        assert!(!accept_pnl_event(
            &mut positions,
            &mut instruments,
            &mut account_summary,
            PnlSnapshotEvent::Account(summary()),
        )
        .unwrap());
        assert!(accept_pnl_event(
            &mut positions,
            &mut instruments,
            &mut account_summary,
            PnlSnapshotEvent::RequestCompleted,
        )
        .unwrap());
        assert!(accept_pnl_event(
            &mut positions,
            &mut instruments,
            &mut account_summary,
            PnlSnapshotEvent::Instrument(position()),
        )
        .is_err());
        assert!(accept_pnl_event(
            &mut positions,
            &mut HashSet::new(),
            &mut account_summary,
            PnlSnapshotEvent::Account(summary()),
        )
        .is_err());
    }
}
