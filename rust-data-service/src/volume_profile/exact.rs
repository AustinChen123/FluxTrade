//! Decimal arithmetic that rejects precision loss as well as overflow.
use num_bigint_dig::BigInt;
use num_traits::{Pow, Signed, ToPrimitive, Zero};
use rust_decimal::Decimal;

use super::{Error, Result};

fn coefficient(value: Decimal, scale: u32) -> BigInt {
    BigInt::from(value.mantissa()) * BigInt::from(10_u8).pow(scale - value.scale())
}

fn decimal(mut value: BigInt, mut scale: u32) -> Result<Decimal> {
    let ten = BigInt::from(10_u8);
    while scale > 0 && (&value % &ten).is_zero() {
        value /= &ten;
        scale -= 1;
    }
    if scale > Decimal::MAX_SCALE || value.abs() > BigInt::from(Decimal::MAX.mantissa()) {
        return Err(Error::Arithmetic);
    }
    Ok(Decimal::from_i128_with_scale(
        value.to_i128().ok_or(Error::Arithmetic)?,
        scale,
    ))
}

pub(super) fn add(a: Decimal, b: Decimal) -> Result<Decimal> {
    let scale = a.scale().max(b.scale());
    decimal(coefficient(a, scale) + coefficient(b, scale), scale)
}

pub(super) fn mul(a: Decimal, b: Decimal) -> Result<Decimal> {
    decimal(
        BigInt::from(a.mantissa()) * BigInt::from(b.mantissa()),
        a.scale() + b.scale(),
    )
}

/// Keep cancellation exact even when index * step exceeds the Decimal domain.
pub(super) fn edges(origin: Decimal, step: Decimal, index: i64) -> Result<(Decimal, Decimal)> {
    let scale = origin.scale().max(step.scale());
    let step = coefficient(step, scale);
    let low = coefficient(origin, scale) + BigInt::from(index) * &step;
    let high = &low + step;
    Ok((decimal(low, scale)?, decimal(high, scale)?))
}

/// Floor without a rounded Decimal division near bin boundaries.
pub(super) fn index(price: Decimal, origin: Decimal, step: Decimal) -> Result<i64> {
    let scale = price.scale().max(origin.scale()).max(step.scale());
    let numerator = coefficient(price, scale) - coefficient(origin, scale);
    let denominator = coefficient(step, scale);
    let mut quotient = &numerator / &denominator;
    if numerator.is_negative() && !(&numerator % &denominator).is_zero() {
        quotient -= 1;
    }
    quotient.to_i64().ok_or(Error::Arithmetic)
}

pub(super) fn multiple(value: Decimal, step: Decimal) -> bool {
    let scale = value.scale().max(step.scale());
    (coefficient(value, scale) % coefficient(step, scale)).is_zero()
}
