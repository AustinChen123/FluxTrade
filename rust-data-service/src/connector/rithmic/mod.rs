#[allow(dead_code)]
mod bar;

#[allow(dead_code)]
mod codec;

#[allow(dead_code)]
mod config;

#[allow(dead_code)]
mod front_month;

#[allow(dead_code)]
mod history;

pub(crate) mod history_runtime;

#[allow(dead_code)]
mod ledger;

pub(crate) mod ledger_runtime;

#[allow(dead_code)]
mod order;

#[allow(dead_code)]
pub(crate) mod order_runtime;

mod profile_lock;

pub(crate) mod live;

#[allow(dead_code)]
mod market;

#[allow(dead_code, clippy::enum_variant_names, clippy::tabs_in_doc_comments)]
pub(crate) mod protocol {
    include!(concat!(env!("OUT_DIR"), "/rti.rs"));
}

#[allow(dead_code)]
mod session;

#[allow(dead_code)]
mod transport;

#[cfg(test)]
mod tests {
    use super::protocol::{RequestHeartbeat, RequestLogin};

    #[test]
    fn generated_v2_protocol_contains_login_and_heartbeat_messages() {
        let login = RequestLogin {
            template_id: 10,
            ..Default::default()
        };
        let heartbeat = RequestHeartbeat {
            template_id: 18,
            ..Default::default()
        };

        assert_eq!(login.template_id, 10);
        assert_eq!(heartbeat.template_id, 18);
    }
}
