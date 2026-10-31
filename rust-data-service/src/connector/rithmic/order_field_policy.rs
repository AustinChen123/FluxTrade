use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::error::Error;
use std::fmt::{Display, Formatter};
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

#[derive(Clone, Debug)]
pub(crate) struct NumericFieldContext {
    pub(crate) template_id: i32,
    pub(crate) source: &'static str,
    pub(crate) notify_type: Option<i32>,
    pub(crate) notify_name: Option<&'static str>,
    redacted_identity: String,
}

impl NumericFieldContext {
    pub(crate) fn new(
        template_id: i32,
        source: &'static str,
        notify_type: Option<i32>,
        notify_name: Option<&'static str>,
        basket_id: Option<&str>,
        order_id: Option<&str>,
    ) -> Self {
        Self {
            template_id,
            source,
            notify_type,
            notify_name,
            redacted_identity: format!(
                "basket#{} order#{}",
                masked_correlation(basket_id),
                masked_correlation(order_id),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NumericFailureClass {
    Negative,
    Nonfinite,
    DecimalUnrepresentable,
}

impl Display for NumericFailureClass {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Negative => "negative",
            Self::Nonfinite => "nonfinite",
            Self::DecimalUnrepresentable => "decimal_unrepresentable",
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NumericFieldDiagnostic {
    template_id: i32,
    source: &'static str,
    notify_type: Option<i32>,
    notify_name: Option<&'static str>,
    field: String,
    raw_bits: u64,
    value: f64,
    failure: NumericFailureClass,
    redacted_identity: String,
}

#[derive(Clone, Debug)]
pub(crate) struct QuantityFieldDiagnostic {
    context: NumericFieldContext,
    field: String,
    value: Option<i32>,
    failure: &'static str,
}

impl QuantityFieldDiagnostic {
    pub(crate) fn new(
        context: NumericFieldContext,
        field: &str,
        value: Option<i32>,
        failure: &'static str,
    ) -> Self {
        Self {
            context,
            field: field.to_string(),
            value,
            failure,
        }
    }
}

impl Display for QuantityFieldDiagnostic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "rithmic_numeric_validation template={} context={} notify_type={}",
            self.context.template_id,
            self.context.source,
            self.context
                .notify_type
                .map(|v| v.to_string())
                .unwrap_or_else(|| "absent".into())
        )?;
        if let Some(name) = self.context.notify_name {
            write!(formatter, "({name})")?;
        }
        write!(
            formatter,
            " field={} present={} raw_i32={} failure={} identity={}",
            self.field,
            self.value.is_some(),
            self.value
                .map(|v| v.to_string())
                .unwrap_or_else(|| "N/A".into()),
            self.failure,
            self.context.redacted_identity
        )
    }
}

impl Error for QuantityFieldDiagnostic {}

pub(crate) fn optional_quantity_with_diagnostic(
    value: Option<i32>,
    field: &str,
    diagnostic_field: &str,
    positive: bool,
    context: NumericFieldContext,
) -> Result<Option<Decimal>> {
    let result = value
        .map(|raw| {
            ensure!(
                if positive { raw > 0 } else { raw >= 0 },
                "invalid Rithmic {field}"
            );
            Ok(Decimal::from(raw))
        })
        .transpose();
    result.map_err(|error: anyhow::Error| {
        let raw = value.expect("only a present integer can fail quantity validation");
        let failure = if raw < 0 { "negative" } else { "zero" };
        error.context(QuantityFieldDiagnostic::new(
            context,
            diagnostic_field,
            Some(raw),
            failure,
        ))
    })
}

impl NumericFieldDiagnostic {
    fn new(
        value: f64,
        field: &str,
        context: NumericFieldContext,
        failure: NumericFailureClass,
    ) -> Self {
        Self {
            template_id: context.template_id,
            source: context.source,
            notify_type: context.notify_type,
            notify_name: context.notify_name,
            field: field.to_string(),
            raw_bits: value.to_bits(),
            value,
            failure,
            redacted_identity: context.redacted_identity,
        }
    }
}

impl Display for NumericFieldDiagnostic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "rithmic_numeric_validation template={} context={} notify_type={}",
            self.template_id,
            self.source,
            self.notify_type
                .map(|value| value.to_string())
                .unwrap_or_else(|| "absent".to_string()),
        )?;
        if let Some(name) = self.notify_name {
            write!(formatter, "({name})")?;
        }
        write!(
            formatter,
            " field={} present=true raw_bits=0x{:016x} value={} failure={} identity={}",
            self.field, self.raw_bits, self.value, self.failure, self.redacted_identity
        )
    }
}

impl Error for NumericFieldDiagnostic {}

pub(crate) fn optional_nonnegative_decimal_with_diagnostic(
    value: Option<f64>,
    field: &str,
    diagnostic_field: &str,
    context: NumericFieldContext,
) -> Result<Option<Decimal>> {
    optional_nonnegative_decimal(value, field).with_context(|| {
        let value = value.expect("a missing optional value cannot fail validation");
        let failure = if !value.is_finite() {
            NumericFailureClass::Nonfinite
        } else if value < 0.0 {
            NumericFailureClass::Negative
        } else {
            NumericFailureClass::DecimalUnrepresentable
        };
        NumericFieldDiagnostic::new(value, diagnostic_field, context, failure)
    })
}

fn masked_correlation(value: Option<&str>) -> String {
    let Some(value) = value else {
        return "absent".to_string();
    };
    let character_count = value.chars().count();
    if character_count <= 2 {
        return format!("masked:length={character_count}");
    }
    let suffix: String = value
        .chars()
        .rev()
        .take(2)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let safe_suffix = if suffix
        .chars()
        .all(|character| character.is_ascii_alphanumeric())
    {
        suffix
    } else {
        "**".to_string()
    };
    format!("suffix={safe_suffix}:length={character_count}")
}

#[cfg(test)]
mod tests {
    use super::{
        masked_correlation, optional_nonnegative_decimal,
        optional_nonnegative_decimal_with_diagnostic, NumericFailureClass, NumericFieldContext,
        NumericFieldDiagnostic,
    };
    use rust_decimal::Decimal;

    #[test]
    fn shared_numeric_policy_has_no_proto_dependency() {
        let source = include_str!("order_field_policy.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(!source.contains("protocol::"));
        assert!(!source.contains("prost::"));
    }

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

    #[test]
    fn diagnostic_formatter_reports_raw_numeric_details_without_identity_leaks() {
        let value = f64::NEG_INFINITY;
        let diagnostic = optional_nonnegative_decimal_with_diagnostic(
            Some(value),
            "order price",
            "price",
            NumericFieldContext::new(
                352,
                "live",
                Some(3),
                Some("FILL"),
                Some("BASKET_ID_SECRET_123"),
                Some("ORDER_ID_SECRET_456"),
            ),
        )
        .unwrap_err();
        let typed = diagnostic
            .downcast_ref::<NumericFieldDiagnostic>()
            .expect("numeric rejection carries typed diagnostic");
        assert_eq!(typed.failure, NumericFailureClass::Nonfinite);
        let text = format!("{diagnostic:#}");
        assert!(text.contains("template=352 context=live notify_type=3(FILL)"));
        assert!(text.contains("field=price present=true"));
        assert!(text.contains(&format!("raw_bits=0x{:016x}", value.to_bits())));
        assert!(text.contains("value=-inf failure=nonfinite"));
        assert!(!text.contains("BASKET_ID_SECRET_123"));
        assert!(!text.contains("ORDER_ID_SECRET_456"));
        assert!(!text.contains("ACCOUNT_ID_SECRET"));
    }

    #[test]
    fn correlation_masker_never_panics_or_emits_short_or_control_identifiers() {
        let alphabet = [
            'a', '7', '界', 'é', '\n', '\r', '\t', '\0', '\u{7f}', '\u{2028}',
        ];
        let mut samples = vec![String::new()];
        for first in alphabet {
            samples.push(first.to_string());
            for second in alphabet {
                samples.push(format!("{first}{second}"));
                for third in alphabet {
                    samples.push(format!("{first}{second}{third}"));
                }
            }
        }
        samples.extend([
            "\u{1f680}a".to_string(),
            "\u{1f680}ab".to_string(),
            "éa-secret".to_string(),
            "secret-\n".to_string(),
            "x\r\ny".to_string(),
        ]);

        for sample in samples {
            let masked = masked_correlation(Some(&sample));
            assert!(!masked
                .chars()
                .any(
                    |character| ['\n', '\r', '\t', '\0', '\u{7f}', '\u{2028}'].contains(&character)
                ));
            if !sample.is_empty() && sample.chars().count() <= 2 {
                assert_eq!(masked, format!("masked:length={}", sample.chars().count()));
            }
        }

        let arbitrary_utf8 = [
            "",
            "a",
            "ab",
            "界",
            "éa",
            "🔒x",
            "\n\r",
            "x\0y",
            "e\u{301}",
            "\u{2028}abcdef",
            "abcdef\u{2029}",
            "🧑🏽‍💻",
        ];
        for sample in arbitrary_utf8 {
            let masked = masked_correlation(Some(sample));
            let context =
                NumericFieldContext::new(352, "live", None, None, Some(sample), Some(sample));
            let diagnostic =
                NumericFieldDiagnostic::new(-1.0, "price", context, NumericFailureClass::Negative)
                    .to_string();
            assert!(diagnostic.contains(&format!("identity=basket#{masked} order#{masked}")));
            if !sample.is_empty() && sample.chars().count() <= 2 {
                assert_eq!(masked, format!("masked:length={}", sample.chars().count()));
            }
            assert!(!diagnostic.contains('\n'));
            assert!(!diagnostic.contains('\r'));
            assert!(!diagnostic.contains('\0'));
        }
    }
}
