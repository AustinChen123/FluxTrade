use ::pyo3::prelude::*;

/// Candle payload for scaled-integer PyO3 boundary experiments.
///
/// Units are interpreted by a session-level precision spec on the Python side.
/// This class deliberately avoids Decimal parsing and string getters so the
/// experiment can isolate PyO3 transport overhead.
#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScaledCandlestick {
    #[pyo3(get, set)]
    pub product_id: String,
    #[pyo3(get, set)]
    pub timeframe: String,
    #[pyo3(get, set)]
    pub timestamp: i64,
    #[pyo3(get, set)]
    pub open_units: i64,
    #[pyo3(get, set)]
    pub high_units: i64,
    #[pyo3(get, set)]
    pub low_units: i64,
    #[pyo3(get, set)]
    pub close_units: i64,
    #[pyo3(get, set)]
    pub volume_units: i64,
}

#[pymethods]
impl ScaledCandlestick {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        product_id,
        timeframe,
        timestamp,
        open_units,
        high_units,
        low_units,
        close_units,
        volume_units
    ))]
    fn new(
        product_id: String,
        timeframe: String,
        timestamp: i64,
        open_units: i64,
        high_units: i64,
        low_units: i64,
        close_units: i64,
        volume_units: i64,
    ) -> PyResult<Self> {
        if open_units <= 0 || high_units <= 0 || low_units <= 0 || close_units <= 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "price units must be positive",
            ));
        }
        if volume_units < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "volume_units must be non-negative",
            ));
        }
        Ok(Self {
            product_id,
            timeframe,
            timestamp,
            open_units,
            high_units,
            low_units,
            close_units,
            volume_units,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ScaledCandlestick;

    #[test]
    fn scaled_candlestick_keeps_integer_units() {
        let candle = ScaledCandlestick::new(
            "BINANCE:BTCUSDT-PERP".to_string(),
            "5m".to_string(),
            1_700_000_000_000,
            1_045_237,
            1_045_240,
            1_045_220,
            1_045_235,
            12_500,
        )
        .unwrap();

        assert_eq!(candle.open_units, 1_045_237);
        assert_eq!(candle.close_units, 1_045_235);
        assert_eq!(candle.volume_units, 12_500);
    }

    #[test]
    fn scaled_candlestick_rejects_invalid_units() {
        let result = ScaledCandlestick::new(
            "BINANCE:BTCUSDT-PERP".to_string(),
            "5m".to_string(),
            1_700_000_000_000,
            0,
            1,
            1,
            1,
            0,
        );

        assert!(result.is_err());
    }
}
