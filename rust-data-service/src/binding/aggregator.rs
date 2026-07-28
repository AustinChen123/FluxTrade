use crate::aggregator::CandleAggregator;
use crate::binding::models::Candlestick as PyCandlestick;
use crate::model::Candlestick;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass(name = "CandleAggregator")]
pub struct PyCandleAggregator {
    inner: CandleAggregator,
}

#[pymethods]
impl PyCandleAggregator {
    #[new]
    fn new() -> Self {
        Self {
            inner: CandleAggregator::new(),
        }
    }

    fn add_candle(
        &mut self,
        candle: PyRef<'_, PyCandlestick>,
        target_timeframe: &str,
    ) -> PyResult<Option<PyCandlestick>> {
        self.inner
            .add_candle(
                &Candlestick {
                    product_id: candle.product_id.clone(),
                    timeframe: candle.timeframe.clone(),
                    timestamp: candle.timestamp,
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    volume: candle.volume,
                },
                target_timeframe,
            )
            .map(|completed| completed.map(PyCandlestick::from))
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn reset_product(&mut self, product_id: &str) {
        self.inner.reset_product(product_id);
    }

    #[staticmethod]
    fn can_aggregate(source_timeframe: &str, target_timeframe: &str) -> PyResult<bool> {
        CandleAggregator::can_aggregate(source_timeframe, target_timeframe)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

impl From<Candlestick> for PyCandlestick {
    fn from(candle: Candlestick) -> Self {
        Self {
            product_id: candle.product_id,
            timeframe: candle.timeframe,
            timestamp: candle.timestamp,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
        }
    }
}
