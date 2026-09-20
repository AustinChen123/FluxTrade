//! Pure, venue-neutral fixed-grid profiles. No collection or publication claims.
//!
//! Inputs and results use the rust_decimal domain (96-bit coefficient, scale
//! <= 28). Exact integer intermediates reject unrepresentable results rather
//! than rounding. A failed add leaves the profile unchanged. Callers own source
//! deduplication/completeness; aggregate_count counts accepted input records.
//! The caller/ingress must use the existing product registry to verify that
//! grid.unit equals the product's quote asset. This module validates canonical
//! ID syntax, not registry membership or quote-asset semantics.
pub mod binance_spot;
pub mod checkpoint;
#[cfg(unix)]
pub mod directory;
mod exact;
pub mod work_policy;

use std::collections::BTreeMap;

use rust_decimal::Decimal;

pub const ALGORITHM_VERSION: &str = "vp-v1";
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid canonical product identity")]
    Product,
    #[error("invalid grid or unaligned coarse grid")]
    Grid,
    #[error("invalid half-open time window")]
    Window,
    #[error("trade identity or timestamp does not match profile")]
    TradeScope,
    #[error("price and quantity must be positive")]
    TradeValue,
    #[error("result cannot be represented exactly")]
    Arithmetic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grid {
    origin: Decimal,
    step: Decimal,
    unit: String,
}

impl Grid {
    pub fn new(origin: Decimal, step: Decimal, unit: impl Into<String>) -> Result<Self> {
        let unit = unit.into();
        if step <= Decimal::ZERO
            || unit.is_empty()
            || !unit
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
            return Err(Error::Grid);
        }
        Ok(Self { origin, step, unit })
    }

    pub fn origin(&self) -> Decimal {
        self.origin
    }
    pub fn step(&self) -> Decimal {
        self.step
    }
    pub fn unit(&self) -> &str {
        &self.unit
    }

    pub fn index(&self, price: Decimal) -> Result<i64> {
        exact::index(price, self.origin, self.step)
    }

    pub fn edges(&self, index: i64) -> Result<(Decimal, Decimal)> {
        exact::edges(self.origin, self.step, index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    start_ms: i64,
    end_ms: i64,
}

impl Window {
    pub fn new(start_ms: i64, end_ms: i64) -> Result<Self> {
        if start_ms < 0 || start_ms >= end_ms {
            return Err(Error::Window);
        }
        Ok(Self { start_ms, end_ms })
    }
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }
    pub fn end_ms(&self) -> i64 {
        self.end_ms
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Volume {
    pub base_volume: Decimal,
    pub quote_volume: Decimal,
    pub aggregate_count: u64,
}

impl Volume {
    fn add(&self, rhs: &Self) -> Result<Self> {
        Ok(Self {
            base_volume: exact::add(self.base_volume, rhs.base_volume)?,
            quote_volume: exact::add(self.quote_volume, rhs.quote_volume)?,
            aggregate_count: self
                .aggregate_count
                .checked_add(rhs.aggregate_count)
                .ok_or(Error::Arithmetic)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointOfControl {
    pub index: i64,
    pub low: Decimal,
    pub high_exclusive: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeProfile {
    product_id: String,
    grid: Grid,
    window: Window,
    bins: BTreeMap<i64, Volume>,
    totals: Volume,
}

impl VolumeProfile {
    pub fn new(product_id: impl Into<String>, grid: Grid, window: Window) -> Result<Self> {
        let product_id = product_id.into();
        crate::model::validate_product_id(&product_id).map_err(|_| Error::Product)?;
        Ok(Self {
            product_id,
            grid,
            window,
            bins: BTreeMap::new(),
            totals: Volume::default(),
        })
    }

    pub fn product_id(&self) -> &str {
        &self.product_id
    }
    pub fn grid(&self) -> &Grid {
        &self.grid
    }
    pub fn window(&self) -> Window {
        self.window
    }
    pub fn bins(&self) -> &BTreeMap<i64, Volume> {
        &self.bins
    }
    pub fn totals(&self) -> &Volume {
        &self.totals
    }

    pub fn add(
        &mut self,
        product_id: &str,
        timestamp_ms: i64,
        price: Decimal,
        quantity: Decimal,
    ) -> Result<()> {
        if product_id != self.product_id
            || timestamp_ms < self.window.start_ms
            || timestamp_ms >= self.window.end_ms
        {
            return Err(Error::TradeScope);
        }
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(Error::TradeValue);
        }
        let index = self.grid.index(price)?;
        self.grid.edges(index)?;
        let volume = Volume {
            base_volume: quantity,
            quote_volume: exact::mul(price, quantity)?,
            aggregate_count: 1,
        };
        let bin = self
            .bins
            .get(&index)
            .cloned()
            .unwrap_or_default()
            .add(&volume)?;
        let totals = self.totals.add(&volume)?;
        self.bins.insert(index, bin);
        self.totals = totals;
        Ok(())
    }

    /// Base-volume maximum; strict comparison preserves the lowest-index tie.
    pub fn poc(&self) -> Result<Option<PointOfControl>> {
        let mut best: Option<(i64, &Volume)> = None;
        for (&index, volume) in &self.bins {
            if best.is_none_or(|(_, current)| volume.base_volume > current.base_volume) {
                best = Some((index, volume));
            }
        }
        best.map(|(index, _)| {
            let (low, high_exclusive) = self.grid.edges(index)?;
            Ok(PointOfControl {
                index,
                low,
                high_exclusive,
            })
        })
        .transpose()
    }

    /// Coarsen on the same origin/unit only; never distribute coarse volume.
    pub fn coarsen(&self, grid: Grid) -> Result<Self> {
        if grid.origin != self.grid.origin
            || grid.unit != self.grid.unit
            || grid.step < self.grid.step
            || !exact::multiple(grid.step, self.grid.step)
        {
            return Err(Error::Grid);
        }
        let mut result = Self::new(self.product_id.clone(), grid, self.window)?;
        for (&index, volume) in &self.bins {
            let (low, _) = self.grid.edges(index)?;
            let target = result.grid.index(low)?;
            result.grid.edges(target)?;
            let merged = result
                .bins
                .get(&target)
                .cloned()
                .unwrap_or_default()
                .add(volume)?;
            result.bins.insert(target, merged);
        }
        result.totals = self.totals.clone();
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
