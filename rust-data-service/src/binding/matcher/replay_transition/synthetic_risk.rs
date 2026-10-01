//! Private SCENARIO_ONLY / NOT_HISTORICAL_ADMISSIBLE replay kernel.
//! Covers 4A valuation and 4B account, reservation, admission, and execution;
//! no public API, 4C actions, persistence, scheduling, or delivery semantics.
use num_bigint_dig::BigInt;
use num_traits::{Pow, ToPrimitive, Zero};
use rust_decimal::Decimal;

use super::Fault;

mod configuration;
use configuration::{frozen_spec, frozen_tiers, interval, ConfiguredProduct};
mod scenario_account;
pub(crate) use scenario_account::register_python;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Product(std::borrow::Cow<'static, str>);

#[allow(non_upper_case_globals)]
impl Product {
    const Btc: Self = Self(std::borrow::Cow::Borrowed("BTC-USDT-SWAP"));
    const Eth: Self = Self(std::borrow::Cow::Borrowed("ETH-USDT-SWAP"));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Interval {
    from: i64,
    to: Option<i64>,
}

impl Interval {
    fn contains(self, at: i64) -> bool {
        at >= self.from && self.to.is_none_or(|end| at < end)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Spec {
    product: Product,
    version: String,
    interval: Interval,
    contract_value: Decimal,
    multiplier: Decimal,
    tick: Decimal,
    lot: Decimal,
    minimum: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Tier {
    minimum: Decimal,
    maximum: Decimal,
    mmr: Decimal,
    imr: Decimal,
    max_leverage: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TierVersion {
    product: Product,
    version: String,
    interval: Interval,
    tiers: Vec<Tier>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FrozenScenario {
    configured: Option<Vec<ConfiguredProduct>>,
    leverage: Decimal,
    specs: Vec<Spec>,
    tiers: Vec<TierVersion>,
}

impl FrozenScenario {
    fn resolve(&self, product: &Product, at: i64) -> Result<(&Spec, &TierVersion), Fault> {
        let spec = self
            .specs
            .iter()
            .find(|v| &v.product == product && v.interval.contains(at));
        let tier = self
            .tiers
            .iter()
            .find(|v| &v.product == product && v.interval.contains(at));
        match (spec, tier) {
            (Some(s), Some(t)) => Ok((s, t)),
            _ => Err("VERSION_COVERAGE_MISSING"),
        }
    }

    fn evaluate(
        &self,
        input: &ValuationInput,
        marks: &[Mark],
    ) -> Result<IncompleteValuation, Fault> {
        if input.effective_at < 0 {
            return Err("INVALID_EFFECTIVE_TIME");
        }
        validate_marks(marks)?;
        let mut products = Vec::new();
        let mut equity_terms = vec![input.cash];
        let mut mmr = Decimal::ZERO;
        let mut exposure = Decimal::ZERO;
        for position in &input.positions {
            if products
                .iter()
                .any(|v: &ProductValuation| v.product == position.product)
            {
                return Err("DUPLICATE_NET_POSITION");
            }
            let (spec, tier_version) = self.resolve(&position.product, input.effective_at)?;
            if position.contracts <= Decimal::ZERO
                || position.lots.is_empty()
                || position.contracts.checked_rem(spec.lot) != Some(Decimal::ZERO)
            {
                return Err("INVALID_POSITION");
            }
            let mark = marks
                .iter()
                .find(|m| {
                    m.product == position.product
                        && m.valid_from <= input.effective_at
                        && input.effective_at < m.valid_to
                })
                .ok_or("MARK_COVERAGE_MISSING")?;
            let (tier_index, tier) = if self.configured.is_some() {
                configured_tier(
                    self,
                    &position.product,
                    input.effective_at,
                    position.contracts,
                )?
                .ok_or("UNSUPPORTED_POSITION_TIER")?
            } else {
                tier_version
                    .tiers
                    .iter()
                    .enumerate()
                    .find(|(_, t)| {
                        position.contracts >= t.minimum && position.contracts <= t.maximum
                    })
                    .ok_or("UNSUPPORTED_POSITION_TIER")?
            };
            let base = mul(
                mul(position.contracts, spec.contract_value)?,
                spec.multiplier,
            )?;
            let value = mul(base, mark.price)?;
            let mut lot_contracts = Decimal::ZERO;
            let mut lot_upl = Vec::new();
            for lot in &position.lots {
                if lot.contracts <= Decimal::ZERO || lot.entry <= Decimal::ZERO {
                    return Err("INVALID_POSITION_LOT");
                }
                lot_contracts = add(lot_contracts, lot.contracts)?;
                let lot_base = mul(mul(lot.contracts, spec.contract_value)?, spec.multiplier)?;
                let difference = match position.side {
                    Side::Long => add(mark.price, -lot.entry),
                    Side::Short => add(lot.entry, -mark.price),
                }?;
                lot_upl.push(mul(lot_base, difference)?);
            }
            if lot_contracts != position.contracts {
                return Err("POSITION_LOT_QUANTITY_MISMATCH");
            }
            let upl = signed_sum(&lot_upl)?;
            let contribution = mul(value, tier.mmr)?;
            let margin = if self.configured.is_some() {
                exact_div(value, self.leverage)?
            } else {
                exact(value.mantissa(), value.scale() + 1)?
            };
            equity_terms.push(upl);
            mmr = add(mmr, contribution)?;
            exposure = add(exposure, margin)?;
            products.push(ProductValuation {
                product: position.product.clone(),
                spec_version: spec.version.clone(),
                tier_version: tier_version.version.clone(),
                tier: tier_index + 1,
                base_quantity: base,
                notional: value,
                unrealized_pnl: upl,
                maintenance_margin: contribution,
                exposure_margin: margin,
            });
        }
        products.sort_by(|a, b| a.product.cmp(&b.product));
        let equity = signed_sum(&equity_terms)?;
        Ok(IncompleteValuation {
            account_version: input.account_version,
            effective_at: input.effective_at,
            risk: maintenance_state(!products.is_empty(), equity, mmr),
            products,
            equity,
            maintenance_margin: mmr,
            exposure_margin: exposure,
            available_margin: add(equity, -exposure)?,
        })
    }
}

fn configured_tier<'a>(
    scenario: &'a FrozenScenario,
    product: &Product,
    at: i64,
    contracts: Decimal,
) -> Result<Option<(usize, &'a Tier)>, Fault> {
    if contracts < Decimal::ZERO {
        return Err("UNSUPPORTED_POSITION_TIER");
    }
    if contracts == Decimal::ZERO {
        return Ok(None);
    }
    let (_, version) = scenario.resolve(product, at)?;
    let (index, tier) = version
        .tiers
        .iter()
        .enumerate()
        .find(|(_, row)| contracts >= row.minimum && contracts <= row.maximum)
        .ok_or("UNSUPPORTED_POSITION_TIER")?;
    if scenario.leverage > tier.max_leverage {
        return Err("LEVERAGE_TIER_CONFLICT");
    }
    Ok(Some((index, tier)))
}

fn mul(left: Decimal, right: Decimal) -> Result<Decimal, Fault> {
    let (left, right) = (left.normalize(), right.normalize());
    exact(
        left.mantissa()
            .checked_mul(right.mantissa())
            .ok_or("DECIMAL_OVERFLOW")?,
        left.scale() + right.scale(),
    )
}

// Exact signed accumulation for lot UPL and cash plus product UPL.
fn signed_sum(terms: &[Decimal]) -> Result<Decimal, Fault> {
    if terms.is_empty() {
        return Err("INVALID_VALUATION_TERMS");
    }
    let mut scale = terms.iter().map(Decimal::scale).max().unwrap_or(0);
    let mut mantissa = BigInt::zero();
    for term in terms {
        mantissa += BigInt::from(term.mantissa()) * 10_i128.pow(scale - term.scale());
    }
    while scale > 0 && (&mantissa % 10_u32).is_zero() {
        mantissa /= 10_u32;
        scale -= 1;
    }
    exact(mantissa.to_i128().ok_or("DECIMAL_OVERFLOW")?, scale)
}

// Integer mantissas only guard representability; all outputs remain Decimal.
fn exact(mut mantissa: i128, mut scale: u32) -> Result<Decimal, Fault> {
    while scale > 0 && mantissa % 10 == 0 {
        mantissa /= 10;
        scale -= 1;
    }
    if scale > Decimal::MAX_SCALE {
        return Err("DECIMAL_PRECISION_LOSS");
    }
    Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|_| "DECIMAL_OVERFLOW")
}

fn exact_div(numerator: Decimal, denominator: Decimal) -> Result<Decimal, Fault> {
    if denominator == Decimal::ZERO {
        return Err("DECIMAL_DIVISION_BY_ZERO");
    }
    if numerator == Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }
    let mut top = BigInt::from(numerator.mantissa()) * BigInt::from(10).pow(denominator.scale());
    let mut bottom = BigInt::from(denominator.mantissa()) * BigInt::from(10).pow(numerator.scale());
    if bottom < BigInt::zero() {
        top = -top;
        bottom = -bottom;
    }
    let divisor = bigint_gcd(top.clone(), bottom.clone());
    top /= &divisor;
    bottom /= divisor;
    let two = BigInt::from(2);
    let five = BigInt::from(5);
    let mut twos = 0;
    let mut fives = 0;
    while (&bottom % &two).is_zero() {
        bottom /= &two;
        twos += 1;
    }
    while (&bottom % &five).is_zero() {
        bottom /= &five;
        fives += 1;
    }
    if bottom != BigInt::from(1) {
        return Err("DECIMAL_PRECISION_LOSS");
    }
    let scale = twos.max(fives);
    if scale > Decimal::MAX_SCALE {
        return Err("DECIMAL_PRECISION_LOSS");
    }
    top *= two.pow(scale - twos);
    top *= five.pow(scale - fives);
    exact(top.to_i128().ok_or("DECIMAL_OVERFLOW")?, scale)
}

fn bigint_gcd(mut left: BigInt, mut right: BigInt) -> BigInt {
    if left < BigInt::zero() {
        left = -left;
    }
    if right < BigInt::zero() {
        right = -right;
    }
    while !right.is_zero() {
        let remainder = &left % &right;
        left = right;
        right = remainder;
    }
    left
}

fn add(left: Decimal, right: Decimal) -> Result<Decimal, Fault> {
    let (left, right) = (left.normalize(), right.normalize());
    let scale = left.scale().max(right.scale());
    let aligned = |v: Decimal| {
        v.mantissa()
            .checked_mul(10_i128.pow(scale - v.scale()))
            .ok_or("DECIMAL_OVERFLOW")
    };
    exact(
        aligned(left)?
            .checked_add(aligned(right)?)
            .ok_or("DECIMAL_OVERFLOW")?,
        scale,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Mark {
    product: Product,
    price: Decimal,
    valid_from: i64,
    valid_to: i64,
}

fn validate_marks(marks: &[Mark]) -> Result<(), Fault> {
    for (i, mark) in marks.iter().enumerate() {
        if mark.price <= Decimal::ZERO || mark.valid_from < 0 || mark.valid_to <= mark.valid_from {
            return Err("INVALID_MARK");
        }
        if marks[..i].iter().any(|prior| {
            prior.product == mark.product
                && prior.valid_from < mark.valid_to
                && mark.valid_from < prior.valid_to
        }) {
            return Err("OVERLAPPING_MARKS");
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Long,
    Short,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NetPosition {
    product: Product,
    side: Side,
    contracts: Decimal,
    lots: Vec<ValuationLot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValuationLot {
    contracts: Decimal,
    entry: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValuationInput {
    account_version: u64,
    effective_at: i64,
    cash: Decimal,
    positions: Vec<NetPosition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProductValuation {
    product: Product,
    spec_version: String,
    tier_version: String,
    tier: usize,
    base_quantity: Decimal,
    notional: Decimal,
    unrealized_pnl: Decimal,
    maintenance_margin: Decimal,
    exposure_margin: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaintenanceState {
    Safe,
    Breach,
}

fn maintenance_state(has_position: bool, equity: Decimal, mmr: Decimal) -> MaintenanceState {
    if has_position && equity <= mmr {
        MaintenanceState::Breach
    } else {
        MaintenanceState::Safe
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IncompleteValuation {
    account_version: u64,
    effective_at: i64,
    products: Vec<ProductValuation>,
    equity: Decimal,
    maintenance_margin: Decimal,
    exposure_margin: Decimal,
    available_margin: Decimal,
    risk: MaintenanceState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn config() -> FrozenScenario {
        let mut specs = Vec::new();
        let mut tiers = Vec::new();
        for product in [Product::Btc, Product::Eth] {
            for second in [false, true] {
                specs.push(frozen_spec(&product, second));
                tiers.push(frozen_tiers(&product, second));
            }
        }
        FrozenScenario::new(d("10"), specs, tiers).unwrap()
    }

    fn position(product: Product, quantity: &str, entry: &str) -> NetPosition {
        NetPosition {
            product,
            side: Side::Long,
            contracts: d(quantity),
            lots: vec![ValuationLot {
                contracts: d(quantity),
                entry: d(entry),
            }],
        }
    }

    fn input(cash: &str, positions: Vec<NetPosition>) -> ValuationInput {
        ValuationInput {
            account_version: 7,
            effective_at: 500,
            cash: d(cash),
            positions,
        }
    }

    fn mark(product: Product, price: &str, from: i64, to: i64) -> Mark {
        Mark {
            product,
            price: d(price),
            valid_from: from,
            valid_to: to,
        }
    }

    #[test]
    fn spec_and_tier_half_open_resolution_never_activates_preloads_early() {
        let config = config();
        for product in [Product::Btc, Product::Eth] {
            for (at, spec_id, tier_id, btc_tick, eth_tick) in [
                (0, "spec-v1", "tier-v1", "0.1", "0.01"),
                (999, "spec-v1", "tier-v1", "0.1", "0.01"),
                (1000, "spec-v1", "tier-v2", "0.1", "0.01"),
                (1001, "spec-v1", "tier-v2", "0.1", "0.01"),
                (1999, "spec-v1", "tier-v2", "0.1", "0.01"),
                (2000, "spec-v2", "tier-v2", "1", "0.1"),
                (2001, "spec-v2", "tier-v2", "1", "0.1"),
            ] {
                let (spec, tier) = config.resolve(&product, at).unwrap();
                assert_eq!((&*spec.version, &*tier.version), (spec_id, tier_id));
                assert_eq!(
                    spec.tick,
                    d(if product == Product::Btc {
                        btc_tick
                    } else {
                        eth_tick
                    })
                );
            }
            assert_eq!(
                config.resolve(&product, -1),
                Err("VERSION_COVERAGE_MISSING")
            );
        }
    }

    #[test]
    fn malformed_or_unsupported_frozen_inputs_are_all_or_nothing() {
        let mutations: &[fn(&mut FrozenScenario)] = &[
            |c| c.specs[1].interval.from = 2001,
            |c| c.specs[1].interval.from = 1999,
            |c| c.specs[0].interval.to = Some(0),
            |c| c.specs[1].version = "spec-v3".into(),
            |c| c.specs.push(c.specs[1].clone()),
            |c| {
                c.specs.remove(1);
            },
            |c| c.specs[1].contract_value = d("0.02"),
            |c| c.specs[1].multiplier = d("2"),
            |c| c.specs[1].lot = d("0.1"),
            |c| c.specs[1].minimum = d("0.1"),
            |c| c.specs[1].tick = d("2"),
            |c| c.specs[1].product = Product::Eth,
            |c| c.tiers[1].interval.from = 1001,
            |c| c.tiers[1].interval.from = 999,
            |c| c.tiers[0].interval.to = Some(-1),
            |c| c.tiers[1].version = "tier-v3".into(),
            |c| c.tiers.push(c.tiers[1].clone()),
            |c| {
                c.tiers.remove(1);
            },
            |c| c.tiers[1].tiers[0].mmr = d("0.9"),
            |c| c.tiers[1].tiers[0].minimum = d("1"),
            |c| c.tiers[1].tiers[0].maximum = d("0"),
            |c| c.tiers[1].tiers[0].imr = d("0"),
            |c| c.tiers[1].tiers[2].max_leverage = d("9"),
            |c| c.leverage = d("100"),
            |c| c.leverage = d("0"),
            |c| c.leverage = d("5"),
        ];
        for mutate in mutations {
            let mut data = config();
            mutate(&mut data);
            let before = data.clone();
            assert!(
                FrozenScenario::new(data.leverage, data.specs.clone(), data.tiers.clone()).is_err()
            );
            assert_eq!(data, before);
        }
    }

    #[test]
    fn mark_validity_boundaries_contiguity_and_product_are_causal() {
        let config = config();
        let mut state = input("1000", vec![position(Product::Btc, "1", "50000")]);
        let first = mark(Product::Btc, "50000", 100, 200);
        for (at, valid) in [(99, false), (100, true), (199, true), (200, false)] {
            state.effective_at = at;
            assert_eq!(config.evaluate(&state, &[first.clone()]).is_ok(), valid);
        }
        state.effective_at = 200;
        let next = mark(Product::Btc, "51000", 200, 300);
        let value = config
            .evaluate(&state, &[next.clone(), first.clone()])
            .unwrap();
        assert_eq!(value.equity, d("1010"));
        assert_eq!(value.maintenance_margin, d("2.04"));
        for invalid in [
            vec![],
            vec![mark(Product::Eth, "50000", 100, 300)],
            vec![mark(Product::Btc, "50000", 201, 300)],
            vec![first.clone(), mark(Product::Btc, "51000", 201, 300)],
            vec![first.clone(), mark(Product::Btc, "51000", 199, 300)],
            vec![mark(Product::Btc, "50000", 200, 200)],
            vec![mark(Product::Btc, "50000", -1, 300)],
            vec![mark(Product::Btc, "0", 100, 300)],
            vec![mark(Product::Btc, "-1", 100, 300)],
        ] {
            let before = (state.clone(), invalid.clone());
            assert!(config.evaluate(&state, &invalid).is_err());
            assert_eq!((state.clone(), invalid), before);
        }
        state.effective_at = 199;
        assert_eq!(
            config.evaluate(&state, &[first, next]).unwrap().equity,
            d("1000")
        );
    }

    #[test]
    fn two_product_golden_valuation_uses_leverage_not_tier_imr() {
        let config = config();
        let mut short = position(Product::Eth, "3", "2000");
        short.side = Side::Short;
        let state = input("1000", vec![position(Product::Btc, "2", "50000"), short]);
        let marks = vec![
            mark(Product::Btc, "51000", 0, 3000),
            mark(Product::Eth, "1900", 0, 3000),
        ];
        let before = (config.clone(), state.clone(), marks.clone());
        let result = config.evaluate(&state, &marks).unwrap();
        assert_eq!(
            (
                result.equity,
                result.maintenance_margin,
                result.exposure_margin,
                result.available_margin
            ),
            (d("1050"), d("6.36"), d("159"), d("891"))
        );
        assert_eq!(
            (result.account_version, result.effective_at, result.risk),
            (7, 500, MaintenanceState::Safe)
        );
        for (row, base, notional, upl, mmr, margin) in [
            (&result.products[0], "0.02", "1020", "20", "4.08", "102"),
            (&result.products[1], "0.3", "570", "30", "2.28", "57"),
        ] {
            assert_eq!(
                (
                    row.base_quantity,
                    row.notional,
                    row.unrealized_pnl,
                    row.maintenance_margin,
                    row.exposure_margin
                ),
                (d(base), d(notional), d(upl), d(mmr), d(margin))
            );
        }
        assert_eq!((config.clone(), state.clone(), marks.clone()), before);
        assert_eq!(config.evaluate(&state, &marks).unwrap(), result);
        let mut changed = state.clone();
        changed.cash = d("9999");
        assert_eq!(result.equity, d("1050"));
        assert_ne!(config.evaluate(&changed, &marks).unwrap(), result);
        assert_eq!(
            config.evaluate(&state, &marks[..1]),
            Err("MARK_COVERAGE_MISSING")
        );
    }

    #[test]
    fn active_tier_and_exact_mmr_boundary_match_independent_anchors() {
        let config = config();
        let marks = [mark(Product::Btc, "50000", 0, 3000)];
        let mut state = input("2100", vec![position(Product::Btc, "1000", "50000")]);
        for (at, expected, risk) in [
            (999, "2000", MaintenanceState::Safe),
            (1000, "2250", MaintenanceState::Breach),
            (1001, "2250", MaintenanceState::Breach),
        ] {
            state.effective_at = at;
            let value = config.evaluate(&state, &marks).unwrap();
            assert_eq!((value.maintenance_margin, value.risk), (d(expected), risk));
            assert_eq!(
                (value.equity, value.exposure_margin),
                (d("2100"), d("50000"))
            );
        }
        for (cash, risk) in [
            ("1.99", MaintenanceState::Breach),
            ("2", MaintenanceState::Breach),
            ("2.01", MaintenanceState::Safe),
        ] {
            let value = config
                .evaluate(
                    &input(cash, vec![position(Product::Btc, "1", "50000")]),
                    &marks,
                )
                .unwrap();
            assert_eq!(
                (value.equity, value.maintenance_margin, value.risk),
                (d(cash), d("2"), risk)
            );
        }
        let flat = config.evaluate(&input("-1", vec![]), &[]).unwrap();
        assert_eq!(
            (
                flat.equity,
                flat.available_margin,
                flat.maintenance_margin,
                flat.exposure_margin,
                flat.risk
            ),
            (d("-1"), d("-1"), d("0"), d("0"), MaintenanceState::Safe)
        );
    }

    #[test]
    fn tier_size_boundaries_and_bad_positions_never_partially_accept() {
        let config = config();
        for (product, quantity, tier, mmr) in [
            (Product::Btc, "1000", 1, "2000"),
            (Product::Btc, "1000.01", 2, "2500.025"),
            (Product::Btc, "5000", 2, "12500"),
            (Product::Btc, "5000.01", 3, "18750.0375"),
            (Product::Btc, "20000", 3, "75000"),
            (Product::Eth, "5000", 1, "100000"),
            (Product::Eth, "5000.01", 2, "125000.25"),
            (Product::Eth, "10000", 2, "250000"),
            (Product::Eth, "10000.01", 3, "375000.375"),
            (Product::Eth, "25000", 3, "937500"),
        ] {
            let state = input("1000", vec![position(product.clone(), quantity, "50000")]);
            let value = config
                .evaluate(&state, &[mark(product, "50000", 0, 3000)])
                .unwrap();
            assert_eq!(
                (value.products[0].tier, value.maintenance_margin),
                (tier, d(mmr))
            );
        }
        for quantity in ["0", "-1", "0.001", "1000.005", "25000.01"] {
            let state = input(
                "1000",
                vec![
                    position(Product::Btc, "1", "50000"),
                    position(Product::Eth, quantity, "2000"),
                ],
            );
            let before = state.clone();
            assert!(config
                .evaluate(
                    &state,
                    &[
                        mark(Product::Btc, "50000", 0, 3000),
                        mark(Product::Eth, "2000", 0, 3000)
                    ]
                )
                .is_err());
            assert_eq!(state, before);
        }
        let duplicate = input("1000", vec![position(Product::Btc, "1", "50000"); 2]);
        assert_eq!(
            config.evaluate(&duplicate, &[mark(Product::Btc, "50000", 0, 3000)]),
            Err("DUPLICATE_NET_POSITION")
        );
        let extreme = input("1000", vec![position(Product::Eth, "25000", "1")]);
        assert_eq!(
            config.evaluate(
                &extreme,
                &[Mark {
                    price: Decimal::MAX,
                    ..mark(Product::Eth, "1", 0, 3000)
                }]
            ),
            Err("DECIMAL_OVERFLOW")
        );
    }

    #[test]
    fn scenario_loss_anchor_and_tick_activation_do_not_rewrite_units() {
        let config = config();
        let mut state = input("3000", vec![position(Product::Btc, "1001", "50000")]);
        let loss = config
            .evaluate(&state, &[mark(Product::Btc, "49900", 0, 3000)])
            .unwrap();
        assert_eq!(
            (
                loss.products[0].unrealized_pnl,
                loss.equity,
                loss.maintenance_margin,
                loss.risk
            ),
            (
                d("-1001"),
                d("1999"),
                d("2497.495"),
                MaintenanceState::Breach
            )
        );
        let marks = [mark(Product::Btc, "50000", 0, 3000)];
        for at in [1999, 2000, 2001] {
            state.effective_at = at;
            let before = state.clone();
            let value = config.evaluate(&state, &marks).unwrap();
            assert_eq!(
                (
                    value.products[0].base_quantity,
                    value.equity,
                    value.maintenance_margin
                ),
                (d("10.01"), d("3000"), d("2752.75"))
            );
            assert_eq!(state, before);
        }
    }

    #[test]
    fn precision_loss_and_overflow_reject_without_partial_valuation() {
        let config = config();
        let tiny = d("0.0000000000000000000000000001");
        assert_eq!(mul(tiny, d("0.01")), Err("DECIMAL_PRECISION_LOSS"));
        assert_eq!(
            exact(tiny.mantissa(), tiny.scale() + 1),
            Err("DECIMAL_PRECISION_LOSS")
        );
        assert_eq!(add(Decimal::MAX, d("0.1")), Err("DECIMAL_OVERFLOW"));
        assert_eq!(mul(d("0.2"), d("0.5")), Ok(d("0.1")));
        for (cash, price, fault) in [
            (d("1000"), tiny, "DECIMAL_PRECISION_LOSS"),
            (Decimal::MAX, d("51000"), "DECIMAL_OVERFLOW"),
        ] {
            let mut state = input("1000", vec![position(Product::Btc, "1", "50000")]);
            state.cash = cash;
            let marks = [Mark {
                price,
                ..mark(Product::Btc, "1", 0, 3000)
            }];
            let before = (config.clone(), state.clone(), marks.clone());
            assert_eq!(config.evaluate(&state, &marks), Err(fault));
            assert_eq!((config.clone(), state, marks), before);
        }
    }

    #[test]
    fn signed_equity_sum_is_permutation_invariant_without_transient_overflow() {
        let tiny = d("0.0000000000000000000000000001");
        for (terms, expected) in [
            ([Decimal::MAX, d("1"), d("-1")], Decimal::MAX),
            ([Decimal::MAX, d("0.1"), d("-0.1")], Decimal::MAX),
            ([Decimal::MAX, tiny, -Decimal::MAX], tiny),
            ([d("1.20"), d("-1.199"), d("0.009")], d("0.01")),
        ] {
            for [i, j, k] in [
                [0, 1, 2],
                [0, 2, 1],
                [1, 0, 2],
                [1, 2, 0],
                [2, 0, 1],
                [2, 1, 0],
            ] {
                assert_eq!(signed_sum(&[terms[i], terms[j], terms[k]]), Ok(expected));
            }
        }
        assert_eq!(signed_sum(&[Decimal::MAX, d("1")]), Err("DECIMAL_OVERFLOW"));
        let config = config();
        let mut btc = position(Product::Btc, "1", "50100");
        btc.side = Side::Short;
        let eth = position(Product::Eth, "0.1", "50100");
        let marks = [
            mark(Product::Btc, "50000", 0, 3000),
            mark(Product::Eth, "50000", 0, 3000),
        ];
        for positions in [vec![btc.clone(), eth.clone()], vec![eth, btc]] {
            let mut state = input("0", positions);
            state.cash = Decimal::MAX;
            let before = (state.clone(), marks.clone());
            let value = config.evaluate(&state, &marks).unwrap();
            assert_eq!(
                (
                    value.equity,
                    value.maintenance_margin,
                    value.exposure_margin,
                    value.available_margin
                ),
                (
                    Decimal::MAX,
                    d("4"),
                    d("100"),
                    d("79228162514264337593543950235")
                )
            );
            assert_eq!((state, marks.clone()), before);
        }
    }
}
