use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

pub(crate) fn optional_nonnegative_decimal(
    value: Option<f64>,
    field: &str,
) -> Result<Option<Decimal>> {
    value
        .map(|value| {
            ensure!(value.is_finite() && value >= 0.0, "invalid Rithmic {field}");
            Decimal::from_str(&value.to_string())
                .with_context(|| format!("invalid Rithmic {field}"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::optional_nonnegative_decimal;
    use rust_decimal::Decimal;

    #[test]
    fn optional_nonnegative_decimal_preserves_existing_acceptance_and_error_text() {
        for value in [
            None,
            Some(0.0),
            Some(-0.0),
            Some(0.25),
            Some(12.5),
            Some(f64::from_bits(1)),
        ] {
            assert!(optional_nonnegative_decimal(value, "price").is_ok());
        }

        assert_eq!(
            optional_nonnegative_decimal(Some(0.25), "price")
                .unwrap()
                .unwrap(),
            Decimal::new(25, 2)
        );
        for value in [
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ] {
            assert_eq!(
                optional_nonnegative_decimal(value, "price")
                    .unwrap_err()
                    .to_string(),
                "invalid Rithmic price"
            );
        }
        for value in [Some(1e29), Some(f64::MAX)] {
            assert_eq!(
                optional_nonnegative_decimal(value, "price")
                    .unwrap_err()
                    .to_string(),
                "invalid Rithmic price"
            );
        }
    }
}
