#[path = "connector/rithmic/codec.rs"]
mod codec;

#[path = "connector/rithmic/config.rs"]
mod config;

#[path = "connector/rithmic/ledger.rs"]
pub(crate) mod ledger;

#[path = "connector/rithmic/ledger_runtime.rs"]
pub(crate) mod ledger_runtime;

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
