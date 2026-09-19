use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::binding::models::Order;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpotFeeAsset {
    Base,
    Quote,
}

impl SpotFeeAsset {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "base" => Ok(Self::Base),
            "quote" => Ok(Self::Quote),
            _ => Err(format!("unsupported spot_fee_asset: {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReservedAsset {
    Base,
    Quote,
}

#[derive(Clone, Debug)]
struct Reservation {
    asset: ReservedAsset,
    amount: Decimal,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CashSpotSettlement {
    pub(crate) fee: Decimal,
    pub(crate) base_delta: Decimal,
    pub(crate) quote_delta: Decimal,
}

#[derive(Debug)]
pub(crate) struct CashSpotLedger {
    pub(crate) base_asset: String,
    pub(crate) quote_asset: String,
    pub(crate) fee_asset: SpotFeeAsset,
    base_total: Decimal,
    quote_total: Decimal,
    base_reserved: Decimal,
    quote_reserved: Decimal,
    reservations: HashMap<String, Reservation>,
}

impl CashSpotLedger {
    pub(crate) fn new(
        initial_quote: Decimal,
        base_asset: String,
        quote_asset: String,
        fee_asset: SpotFeeAsset,
    ) -> Result<Self, String> {
        if initial_quote < Decimal::ZERO {
            return Err("cash_spot initial balance cannot be negative".to_string());
        }
        if base_asset.trim().is_empty() || quote_asset.trim().is_empty() {
            return Err("cash_spot base_asset and quote_asset are required".to_string());
        }
        if base_asset == quote_asset {
            return Err("cash_spot base_asset and quote_asset must differ".to_string());
        }
        Ok(Self {
            base_asset,
            quote_asset,
            fee_asset,
            base_total: Decimal::ZERO,
            quote_total: initial_quote,
            base_reserved: Decimal::ZERO,
            quote_reserved: Decimal::ZERO,
            reservations: HashMap::new(),
        })
    }

    pub(crate) fn reserve(
        &mut self,
        order: &Order,
        reference_price: Decimal,
        fee_rate: Decimal,
    ) -> Result<Option<String>, String> {
        if self.reservations.contains_key(&order.id) {
            return Err("cash_spot duplicate order reservation".to_string());
        }
        if order.quantity <= Decimal::ZERO {
            return Err("cash_spot order quantity must be positive".to_string());
        }
        if reference_price <= Decimal::ZERO {
            return Err("cash_spot order reference price must be positive".to_string());
        }

        let (asset, amount) = match order.side.as_str() {
            "LONG" => {
                let notional = reference_price * order.quantity;
                let fee = self.fee(reference_price, order.quantity, fee_rate);
                let required = notional
                    + if self.fee_asset == SpotFeeAsset::Quote {
                        fee
                    } else {
                        Decimal::ZERO
                    };
                (ReservedAsset::Quote, required)
            }
            "SHORT" => {
                let fee = self.fee(reference_price, order.quantity, fee_rate);
                let required = order.quantity
                    + if self.fee_asset == SpotFeeAsset::Base {
                        fee
                    } else {
                        Decimal::ZERO
                    };
                (ReservedAsset::Base, required)
            }
            _ => return Err("cash_spot order side must be LONG or SHORT".to_string()),
        };

        let available = match asset {
            ReservedAsset::Base => self.base_available(),
            ReservedAsset::Quote => self.quote_available(),
        };
        if amount > available {
            return Ok(Some(format!(
                "cash_spot_insufficient_available_at_submission: asset={} required={amount} available={available}",
                match asset {
                    ReservedAsset::Base => &self.base_asset,
                    ReservedAsset::Quote => &self.quote_asset,
                }
            )));
        }

        match asset {
            ReservedAsset::Base => self.base_reserved += amount,
            ReservedAsset::Quote => self.quote_reserved += amount,
        }
        self.reservations
            .insert(order.id.clone(), Reservation { asset, amount });
        Ok(None)
    }

    pub(crate) fn credit_quote(&mut self, asset: &str, amount: Decimal) -> Result<Decimal, String> {
        if asset != self.quote_asset {
            return Err(format!(
                "cash_spot external funding supports quote asset {} only",
                self.quote_asset
            ));
        }
        if amount <= Decimal::ZERO {
            return Err("cash_spot external funding amount must be positive".to_string());
        }
        self.quote_total = self
            .quote_total
            .checked_add(amount)
            .ok_or_else(|| "cash_spot external funding would overflow quote balance".to_string())?;
        Ok(self.quote_total)
    }

    pub(crate) fn settle(
        &mut self,
        order: &Order,
        fill_price: Decimal,
        fee_rate: Decimal,
    ) -> Result<CashSpotSettlement, String> {
        if fill_price <= Decimal::ZERO {
            self.release(&order.id);
            return Err("cash_spot fill price must be positive".to_string());
        }
        let fee = self.fee(fill_price, order.quantity, fee_rate);
        let reservation = self.reservations.remove(&order.id);
        if let Some(reservation) = reservation.as_ref() {
            self.release_amount(reservation);
        }

        let notional = fill_price * order.quantity;
        let (required_base, required_quote, base_delta, quote_delta) = match order.side.as_str() {
            "LONG" => {
                let received_base = order.quantity
                    - if self.fee_asset == SpotFeeAsset::Base {
                        fee
                    } else {
                        Decimal::ZERO
                    };
                let spent_quote = notional
                    + if self.fee_asset == SpotFeeAsset::Quote {
                        fee
                    } else {
                        Decimal::ZERO
                    };
                (Decimal::ZERO, spent_quote, received_base, -spent_quote)
            }
            "SHORT" => {
                let spent_base = order.quantity
                    + if self.fee_asset == SpotFeeAsset::Base {
                        fee
                    } else {
                        Decimal::ZERO
                    };
                let received_quote = notional
                    - if self.fee_asset == SpotFeeAsset::Quote {
                        fee
                    } else {
                        Decimal::ZERO
                    };
                (spent_base, Decimal::ZERO, -spent_base, received_quote)
            }
            _ => return Err("cash_spot order side must be LONG or SHORT".to_string()),
        };

        if required_base > self.base_available() {
            return Err(format!(
                "cash_spot insufficient available {} at fill: required={required_base} available={}",
                self.base_asset,
                self.base_available()
            ));
        }
        if required_quote > self.quote_available() {
            return Err(format!(
                "cash_spot insufficient available {} at fill: required={required_quote} available={}",
                self.quote_asset,
                self.quote_available()
            ));
        }
        if order.side == "LONG" && base_delta <= Decimal::ZERO {
            return Err("cash_spot base fee consumes entire buy quantity".to_string());
        }
        if order.side == "SHORT" && quote_delta < Decimal::ZERO {
            return Err("cash_spot quote fee exceeds sell proceeds".to_string());
        }

        self.base_total += base_delta;
        self.quote_total += quote_delta;
        Ok(CashSpotSettlement {
            fee,
            base_delta,
            quote_delta,
        })
    }

    pub(crate) fn release(&mut self, order_id: &str) {
        if let Some(reservation) = self.reservations.remove(order_id) {
            self.release_amount(&reservation);
        }
    }

    pub(crate) fn base_total(&self) -> Decimal {
        self.base_total
    }

    pub(crate) fn quote_total(&self) -> Decimal {
        self.quote_total
    }

    pub(crate) fn base_available(&self) -> Decimal {
        self.base_total - self.base_reserved
    }

    pub(crate) fn quote_available(&self) -> Decimal {
        self.quote_total - self.quote_reserved
    }

    pub(crate) fn base_reserved(&self) -> Decimal {
        self.base_reserved
    }

    pub(crate) fn quote_reserved(&self) -> Decimal {
        self.quote_reserved
    }

    pub(crate) fn fee_asset_name(&self) -> &str {
        match self.fee_asset {
            SpotFeeAsset::Base => &self.base_asset,
            SpotFeeAsset::Quote => &self.quote_asset,
        }
    }

    fn fee(&self, price: Decimal, quantity: Decimal, fee_rate: Decimal) -> Decimal {
        match self.fee_asset {
            SpotFeeAsset::Base => quantity * fee_rate,
            SpotFeeAsset::Quote => price * quantity * fee_rate,
        }
    }

    fn release_amount(&mut self, reservation: &Reservation) {
        match reservation.asset {
            ReservedAsset::Base => self.base_reserved -= reservation.amount,
            ReservedAsset::Quote => self.quote_reserved -= reservation.amount,
        }
    }
}
