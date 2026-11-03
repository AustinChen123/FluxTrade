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

/// Reads only the template-id scalar needed to classify a transport frame.
/// This does not construct or decode a generated protocol message.
pub(super) fn capture_template_id(payload: &[u8]) -> Option<i32> {
    const MAX_CAPTURE_SCAN_BYTES: usize = 256 * 1024;
    if payload.len() > MAX_CAPTURE_SCAN_BYTES {
        return None;
    }
    fn varint(bytes: &[u8], offset: &mut usize) -> Option<u64> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = *bytes.get(*offset)?;
            *offset += 1;
            if shift == 63 && byte > 1 {
                return None;
            }
            value |= u64::from(byte & 0x7f).checked_shl(shift)?;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    let mut offset = 0;
    let mut template_id = None;
    while offset < payload.len() {
        let key = varint(payload, &mut offset)?;
        let field = key >> 3;
        let wire = (key & 7) as u8;
        if field == 154_467 {
            if wire != 0 {
                return None;
            }
            let value = i32::try_from(varint(payload, &mut offset)?).ok()?;
            if template_id.replace(value).is_some() {
                return None;
            }
            continue;
        }
        match wire {
            0 => {
                varint(payload, &mut offset)?;
            }
            1 => offset = offset.checked_add(8)?,
            2 => {
                let length = usize::try_from(varint(payload, &mut offset)?).ok()?;
                offset = offset.checked_add(length)?;
            }
            5 => offset = offset.checked_add(4)?,
            _ => return None,
        }
        if offset > payload.len() {
            return None;
        }
    }
    template_id.filter(|value| *value > 0)
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

    #[test]
    fn capture_classifier_refuses_payload_before_scanning_over_budget_bytes() {
        // A valid message over the production scan budget must be rejected before
        // skipping its large unknown field body. Without the budget this is Some(18).
        let mut payload = vec![0x98, 0xb6, 0x4b, 0x12, 0xc2, 0x3e];
        let length = 256 * 1024;
        let mut remaining = length as u64;
        while remaining >= 0x80 {
            payload.push((remaining as u8) | 0x80);
            remaining >>= 7;
        }
        payload.push(remaining as u8);
        payload.resize(payload.len() + length, 0);
        assert!(payload.len() > 256 * 1024);
        assert_eq!(capture_template_id(&payload), None);
    }

    #[test]
    fn capture_classifier_reads_template_scalar_without_generated_decode() {
        let mut payload = vec![0x08, 0x01];
        payload.extend_from_slice(&[0x98, 0xb6, 0x4b, 0x12]);
        assert_eq!(capture_template_id(&payload), Some(18));
        assert_eq!(capture_template_id(&[0x08, 0x01]), None);
        assert_eq!(capture_template_id(&[0x98, 0xb6, 0x4b]), None);
        let mut duplicate = payload;
        duplicate.extend_from_slice(&[0x98, 0xb6, 0x4b, 0x13]);
        assert_eq!(capture_template_id(&duplicate), None);
    }
}
