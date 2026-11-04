use super::{
    policy::{field, FieldKind},
    CaptureKey, FieldCapture,
};
use ring::hmac;
use serde_json::json;

pub(super) fn capture_fields(template: i32, payload: &[u8], key: &CaptureKey) -> FieldCapture {
    let mut result = FieldCapture {
        fields: Vec::new(),
        omitted_field_count: 0,
        decode_status: "ok",
    };
    let mut cursor = 0;
    let mut body_template_id = None;
    while cursor < payload.len() {
        let (wire_key, next) = match varint(payload, cursor) {
            Some(value) => value,
            None => return malformed(result),
        };
        cursor = next;
        let number = wire_key >> 3;
        let wire = (wire_key & 7) as u8;
        if number == 0 || number > 536_870_911 {
            return malformed(result);
        }
        let number = number as u32;
        let parsed = (|| -> Option<(Vec<u8>, u64, usize)> {
            match wire {
                0 => varint(payload, cursor)
                    .map(|(value, next)| (value.to_le_bytes().to_vec(), value, next)),
                1 if cursor + 8 <= payload.len() => {
                    let bytes = payload[cursor..cursor + 8].to_vec();
                    Some((
                        bytes.clone(),
                        u64::from_le_bytes(bytes.try_into().ok()?),
                        cursor + 8,
                    ))
                }
                2 => {
                    let (size, value_start) = varint(payload, cursor)?;
                    let size = usize::try_from(size).ok()?;
                    let value_end = value_start.checked_add(size)?;
                    (value_end <= payload.len()).then(|| {
                        (
                            payload[value_start..value_end].to_vec(),
                            size as u64,
                            value_end,
                        )
                    })
                }
                5 if cursor + 4 <= payload.len() => {
                    let bytes = payload[cursor..cursor + 4].to_vec();
                    Some((
                        bytes.clone(),
                        u32::from_le_bytes(bytes.try_into().ok()?) as u64,
                        cursor + 4,
                    ))
                }
                _ => None,
            }
        })();
        let Some((raw, integer, next)) = parsed else {
            return malformed(result);
        };
        cursor = next;
        if number == 154467 {
            if wire != 0 || body_template_id.replace(integer as i32).is_some() {
                return malformed(result);
            }
            if matches!(integer, 10 | 11) {
                result.fields.clear();
                result.omitted_field_count = 0;
                result.decode_status = "auth_excluded";
                return result;
            }
            if integer as i32 != template {
                return malformed(result);
            }
            continue;
        }
        let Some(kind) = field(template, number) else {
            result.omitted_field_count += 1;
            continue;
        };
        let value = match kind {
            FieldKind::Identity(name) if wire == 2 => {
                let signature = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key.bytes), &raw);
                json!({"name": name, "present": true, "hmac_sha256": hex(signature.as_ref())})
            }
            FieldKind::SafeText(name) if wire == 2 && safe_token(&raw) => {
                json!({"name": name, "present": true, "value": String::from_utf8_lossy(&raw)})
            }
            FieldKind::Boolean(name) if wire == 0 => {
                json!({"name": name, "present": true, "value": integer != 0})
            }
            FieldKind::Integer(name) if wire == 0 => {
                json!({"name": name, "present": true, "value": integer as i64})
            }
            FieldKind::Enum(name) if wire == 0 => {
                json!({"name": name, "present": true, "value": integer as i64})
            }
            FieldKind::Float(name) if wire == 1 => {
                let value = f64::from_bits(integer);
                json!({"name": name, "present": true, "raw_bits": format!("0x{integer:016x}"), "decimal": format!("{value:?}")})
            }
            _ => {
                result.omitted_field_count += 1;
                continue;
            }
        };
        if result.fields.len() >= 128 {
            result.omitted_field_count += 1;
        } else {
            result.fields.push(value);
        }
    }
    if body_template_id != Some(template) {
        return malformed(result);
    }
    result
}

fn malformed(mut result: FieldCapture) -> FieldCapture {
    result.decode_status = "malformed";
    result.fields.clear();
    result
}

fn safe_token(raw: &[u8]) -> bool {
    !raw.is_empty()
        && raw.len() <= 32
        && raw
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'!'))
        && std::str::from_utf8(raw).is_ok()
}

fn varint(bytes: &[u8], mut cursor: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(cursor)?;
        cursor += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some((value, cursor));
        }
    }
    None
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(TABLE[(byte >> 4) as usize] as char);
        out.push(TABLE[(byte & 0xf) as usize] as char);
    }
    out
}
