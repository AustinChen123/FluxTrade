//! Independent test oracle: raw bytes, never the production Encoding.
use super::*;
pub(crate) struct Raw(pub Vec<u8>);
impl Raw {
    pub(crate) fn text(&mut self, value: &str) {
        self.num(value.len() as i64);
        self.0.extend(value.as_bytes());
    }
    pub(crate) fn num(&mut self, value: i64) {
        self.0.extend(value.to_be_bytes());
    }
    pub(crate) fn optional(&mut self, value: Option<&str>) {
        self.0.push(u8::from(value.is_some()));
        if let Some(value) = value {
            self.text(value);
        }
    }
    pub(crate) fn decimals(&mut self, values: &[Decimal]) {
        for value in values {
            self.text(&value.normalize().to_string());
        }
    }
    pub(crate) fn order(&mut self, o: &SeedOrder) {
        for s in [
            o.intent_id.as_str(),
            &o.order_id,
            &o.client_id,
            &o.strategy_id,
            o.product.canonical_id(),
            if o.side == Side::Long {
                "LONG"
            } else {
                "SHORT"
            },
        ] {
            self.text(s);
        }
        self.decimals(&[o.price]);
        self.0.push(u8::from(o.reduce_only));
        self.decimals(&[o.original, o.filled, o.canceled, o.remaining]);
        self.text(&o.status);
    }
    pub(crate) fn finish(self) -> Hash {
        ring::digest::digest(&ring::digest::SHA256, &self.0)
            .as_ref()
            .try_into()
            .unwrap()
    }
}
