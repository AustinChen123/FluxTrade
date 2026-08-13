#[path = "connector/rithmic/codec.rs"]
mod codec;

#[path = "connector/rithmic/config.rs"]
mod config;

#[path = "connector/rithmic/ledger.rs"]
pub(crate) mod ledger;

#[path = "connector/rithmic/ledger_runtime.rs"]
pub(crate) mod ledger_runtime;

#[path = "connector/rithmic/last_trade_snapshot.rs"]
pub(crate) mod last_trade_snapshot;

#[path = "connector/rithmic/order.rs"]
pub(crate) mod order;

#[path = "connector/rithmic/order_command.rs"]
pub(crate) mod order_command;

#[path = "connector/rithmic/order_dispatch.rs"]
mod order_dispatch;

#[path = "connector/rithmic/order_event.rs"]
pub(crate) mod order_event;

#[path = "connector/rithmic/order_pending.rs"]
mod order_pending;

#[path = "connector/rithmic/order_runtime.rs"]
pub(crate) mod order_runtime;

#[path = "connector/rithmic/order_session.rs"]
mod order_session;

#[path = "connector/rithmic/market.rs"]
#[allow(dead_code)]
pub(crate) mod market;

#[path = "connector/rithmic/profile_lock.rs"]
pub(crate) mod profile_lock;

#[allow(dead_code, clippy::enum_variant_names, clippy::tabs_in_doc_comments)]
pub(crate) mod protocol {
    include!(concat!(env!("OUT_DIR"), "/rti.rs"));
}

#[path = "connector/rithmic/session.rs"]
#[allow(dead_code)]
mod session;

#[path = "connector/rithmic/transport.rs"]
#[allow(dead_code)]
mod transport;
