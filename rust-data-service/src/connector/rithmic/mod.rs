#[allow(dead_code)]
mod codec;

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
