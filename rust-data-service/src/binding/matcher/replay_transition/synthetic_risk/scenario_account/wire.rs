//! Strict private wire primitives, not an operation or financial owner.
use super::*;
use serde::de::{MapAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::fmt::{self, Write};

pub(super) mod profiles;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Json {
    Null,
    Bool(bool),
    Number(String),
    Text(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

struct Pairs(Vec<(String, Box<RawValue>)>);
impl<'de> Deserialize<'de> for Pairs {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Pairs;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Pairs, M::Error> {
                let mut rows = Vec::new();
                while let Some(row) = map.next_entry()? {
                    rows.push(row);
                }
                Ok(Pairs(rows))
            }
        }
        d.deserialize_map(ObjectVisitor)
    }
}

pub(super) fn decode(input: &str) -> Result<Json, Fault> {
    // RawValue validates syntax without rounding numbers or collapsing object keys.
    let raw: Box<RawValue> = serde_json::from_str(input).map_err(|_| "INVALID_JSON")?;
    Json::from_raw(&raw, 0)
}

impl Json {
    fn from_raw(raw: &RawValue, parent_depth: usize) -> Result<Self, Fault> {
        let s = raw.get();
        let depth = parent_depth + usize::from(matches!(s.as_bytes()[0], b'{' | b'['));
        if depth > 128 {
            return Err("INVALID_SCHEMA");
        }
        Ok(match s.as_bytes()[0] {
            b'{' => {
                let pairs: Pairs = serde_json::from_str(s).map_err(|_| "INVALID_SCHEMA")?;
                let mut rows = BTreeMap::new();
                for (key, value) in pairs.0 {
                    if rows.contains_key(&key) {
                        return Err("INVALID_SCHEMA");
                    }
                    rows.insert(key, Self::from_raw(&value, depth)?);
                }
                Self::Object(rows)
            }
            b'[' => {
                let rows: Vec<Box<RawValue>> =
                    serde_json::from_str(s).map_err(|_| "INVALID_SCHEMA")?;
                Self::Array(
                    rows.iter()
                        .map(|v| Self::from_raw(v, depth))
                        .collect::<Result<_, _>>()?,
                )
            }
            b'"' => Self::Text(serde_json::from_str(s).map_err(|_| "INVALID_SCHEMA")?),
            b'n' => Self::Null,
            b't' | b'f' => Self::Bool(s == "true"),
            _ => Self::Number(s.into()),
        })
    }
    pub(super) fn object(
        &self,
        required: &[&str],
        optional: &[&str],
    ) -> Result<&BTreeMap<String, Self>, Fault> {
        let Self::Object(rows) = self else {
            return Err("INVALID_SCHEMA");
        };
        if required.iter().any(|key| !rows.contains_key(*key))
            || rows
                .keys()
                .any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
        {
            return Err("INVALID_SCHEMA");
        }
        Ok(rows)
    }
    pub(super) fn text(&self) -> Result<&str, Fault> {
        match self {
            Self::Text(s) => Ok(s),
            _ => Err("INVALID_SCHEMA"),
        }
    }
    pub(super) fn id(&self) -> Result<String, Fault> {
        let s = self.text()?;
        if !identity(s) {
            return Err("INVALID_SCHEMA");
        }
        Ok(s.into())
    }
    pub(super) fn integer(&self, nonnegative: bool) -> Result<i64, Fault> {
        let Self::Number(s) = self else {
            return Err("INVALID_SCHEMA");
        };
        let value = s.parse::<i64>().map_err(|_| "INVALID_SCHEMA")?;
        if nonnegative && value < 0 {
            return Err("INVALID_SCHEMA");
        }
        Ok(value)
    }
    pub(super) fn decimal(&self) -> Result<Decimal, Fault> {
        let s = self.text()?;
        let value = Decimal::from_str_exact(s).map_err(|_| "INVALID_SCHEMA")?;
        if value.normalize().to_string() != s {
            return Err("INVALID_SCHEMA");
        }
        Ok(value)
    }
    pub(super) fn hash(&self) -> Result<Hash, Fault> {
        let s = self.text()?;
        if s.len() != 64
            || !s
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("INVALID_SCHEMA");
        }
        let mut hash = [0; 32];
        for (i, byte) in hash.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| "INVALID_SCHEMA")?;
        }
        Ok(hash)
    }
    pub(super) fn account(&self) -> Result<AccountKey, Fault> {
        let rows = self.object(&["venue", "environment", "account"], &["subaccount"])?;
        Ok(AccountKey {
            venue: rows["venue"].id()?,
            environment: rows["environment"].id()?,
            account: rows["account"].id()?,
            subaccount: optional(rows, "subaccount").map(Self::id).transpose()?,
        })
    }
    pub(super) fn canonical(&self) -> Result<String, Fault> {
        Ok(match self {
            Self::Null => return Err("NATIVE_INVARIANT"),
            Self::Bool(b) => b.to_string(),
            Self::Number(_) => self
                .integer(false)
                .map_err(|_| "NATIVE_INVARIANT")?
                .to_string(),
            Self::Text(s) => quote(s),
            Self::Array(rows) => format!(
                "[{}]",
                rows.iter()
                    .map(Self::canonical)
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",")
            ),
            Self::Object(rows) => format!(
                "{{{}}}",
                rows.iter()
                    .map(|(k, v)| Ok(format!("{}:{}", quote(k), v.canonical()?)))
                    .collect::<Result<Vec<_>, Fault>>()?
                    .join(",")
            ),
        })
    }
}

pub(super) fn optional<'a>(rows: &'a BTreeMap<String, Json>, key: &str) -> Option<&'a Json> {
    rows.get(key).filter(|value| **value != Json::Null)
}
fn quote(s: &str) -> String {
    let mut out = String::from("\"");
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\0'..='\u{1f}' => write!(out, "\\u{:04x}", ch as u32).expect("String write"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}
