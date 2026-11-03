//! Local reviewed field allowlists for generated Rithmic prost types.
//! Field numbers/names mirror fields already consumed by the ORDER and PNL
//! paths. Unknown values and unclassified strings are always omitted.

use super::CaptureKey;
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
pub(super) struct FieldCapture {
    pub(super) fields: Vec<Value>,
    pub(super) omitted_field_count: usize,
    pub(super) decode_status: &'static str,
}

impl FieldCapture {
    pub(super) fn metadata_only() -> Self {
        Self {
            fields: Vec::new(),
            omitted_field_count: 0,
            decode_status: "key_unavailable_or_oversize",
        }
    }
}

#[path = "field_map/policy.rs"]
mod policy;
#[path = "field_map/projection.rs"]
mod projection;
#[cfg(test)]
#[path = "field_map/tests.rs"]
mod tests;

pub(super) fn capture_fields(template: i32, payload: &[u8], key: &CaptureKey) -> FieldCapture {
    projection::capture_fields(template, payload, key)
}
