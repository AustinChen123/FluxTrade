use anyhow::{ensure, Context, Result};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct TemplateEnvelope {
    #[prost(int32, required, tag = "154467")]
    template_id: i32,
}

pub(super) fn encode<M: Message>(message: &M) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(message.encoded_len());
    message.encode(&mut payload)?;
    Ok(payload)
}

pub(super) fn decode<M: Message + Default>(payload: &[u8]) -> Result<M> {
    M::decode(payload).context("invalid Rithmic protobuf payload")
}

pub(super) fn template_id(payload: &[u8]) -> Result<i32> {
    let template_id = TemplateEnvelope::decode(payload)?.template_id;
    ensure!(template_id > 0, "Rithmic payload omitted template_id");
    Ok(template_id)
}

#[cfg(test)]
mod tests {
    use super::super::protocol::RequestHeartbeat;
    use super::*;

    #[test]
    fn websocket_payload_matches_raw_protobuf_wire_format() {
        let request = RequestHeartbeat {
            template_id: 18,
            ..Default::default()
        };
        let payload = encode(&request).unwrap();

        assert_eq!(payload, [0x98, 0xb6, 0x4b, 0x12]);
        assert_eq!(template_id(&payload).unwrap(), 18);
        assert_eq!(decode::<RequestHeartbeat>(&payload).unwrap(), request);
    }

    #[test]
    fn malformed_protobuf_payload_matrix_is_rejected() {
        let cases = [vec![], vec![0x80], vec![0x98, 0xb6, 0x4b]];

        for payload in cases {
            assert!(
                template_id(&payload).is_err(),
                "accepted payload: {payload:?}"
            );
        }
    }
}
