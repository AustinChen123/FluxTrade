use super::{codec, protocol};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

const TIME_BAR_REPLAY_REQUEST: i32 = 202;
const TIME_BAR_REPLAY_RESPONSE: i32 = 203;

#[derive(Debug, PartialEq)]
pub(crate) struct HistoryMinuteBar {
    pub exchange: String,
    pub symbol: String,
    pub end_timestamp: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

#[derive(Debug, PartialEq)]
pub(crate) enum HistoryEvent {
    Bar(HistoryMinuteBar),
    PageEnded { next_start: Option<i32> },
}

pub(crate) fn minute_bar_replay_request(
    request_key: &str,
    exchange: &str,
    symbol: &str,
    start_index: i32,
    finish_index: i32,
) -> Result<Vec<u8>> {
    ensure!(
        !request_key.trim().is_empty(),
        "Rithmic history request key must not be empty"
    );
    ensure!(
        !exchange.trim().is_empty(),
        "Rithmic exchange must not be empty"
    );
    ensure!(
        !symbol.trim().is_empty(),
        "Rithmic symbol must not be empty"
    );
    ensure!(
        start_index >= 0,
        "Rithmic history start index must not be negative"
    );
    ensure!(
        finish_index > start_index,
        "Rithmic history finish index must follow start index"
    );

    codec::encode(&protocol::RequestTimeBarReplay {
        template_id: TIME_BAR_REPLAY_REQUEST,
        user_msg: vec![request_key.to_string()],
        symbol: Some(symbol.to_string()),
        exchange: Some(exchange.to_string()),
        bar_type: Some(protocol::request_time_bar_replay::BarType::MinuteBar as i32),
        bar_type_period: Some(1),
        start_index: Some(start_index),
        finish_index: Some(finish_index),
        user_max_count: None,
        direction: None,
        time_order: Some(protocol::request_time_bar_replay::TimeOrder::Forwards as i32),
        resume_bars: Some(false),
    })
}

pub(crate) struct HistoryPageDecoder {
    request_key: String,
    finish_index: i32,
    last_marker: Option<i32>,
}

impl HistoryPageDecoder {
    pub(crate) fn new(request_key: String, finish_index: i32) -> Result<Self> {
        ensure!(
            !request_key.trim().is_empty(),
            "Rithmic history request key must not be empty"
        );
        ensure!(
            finish_index >= 0,
            "Rithmic history finish index must not be negative"
        );
        Ok(Self {
            request_key,
            finish_index,
            last_marker: None,
        })
    }

    pub(crate) fn decode(&mut self, payload: &[u8]) -> Result<HistoryEvent> {
        ensure!(
            codec::template_id(payload)? == TIME_BAR_REPLAY_RESPONSE,
            "unexpected Rithmic history response template"
        );
        let response: protocol::ResponseTimeBarReplay = codec::decode(payload)?;
        ensure!(
            response.user_msg.first() == Some(&self.request_key),
            "Rithmic history response request key mismatch"
        );

        if response.rp_code.first().is_some_and(|code| code == "0")
            || (response.rp_code.is_empty() && response.rq_handler_rp_code.is_empty())
        {
            let next_start = self
                .last_marker
                .filter(|marker| *marker < self.finish_index)
                .and_then(|marker| marker.checked_add(1));
            return Ok(HistoryEvent::PageEnded { next_start });
        }
        ensure!(
            response
                .rq_handler_rp_code
                .first()
                .is_some_and(|code| code == "0"),
            "Rithmic history response failed"
        );

        let marker = response.marker.context("missing Rithmic history marker")?;
        ensure!(
            marker >= 0 && marker <= self.finish_index,
            "invalid Rithmic history marker"
        );
        ensure!(
            self.last_marker.is_none_or(|last| marker > last),
            "Rithmic history marker did not advance"
        );
        ensure!(
            response.r#type == Some(protocol::response_time_bar_replay::BarType::MinuteBar as i32),
            "Rithmic history response is not a minute bar"
        );
        ensure!(
            response.period.as_deref() == Some("1"),
            "Rithmic history period is not one minute"
        );

        let open = decimal(response.open_price, "open")?;
        let high = decimal(response.high_price, "high")?;
        let low = decimal(response.low_price, "low")?;
        let close = decimal(response.close_price, "close")?;
        ensure!(
            high >= open && high >= close && low <= open && low <= close,
            "invalid Rithmic history OHLC"
        );
        self.last_marker = Some(marker);

        Ok(HistoryEvent::Bar(HistoryMinuteBar {
            exchange: required_text(response.exchange, "exchange")?,
            symbol: required_text(response.symbol, "symbol")?,
            end_timestamp: i64::from(marker) * 1_000,
            open,
            high,
            low,
            close,
            volume: Decimal::from(response.volume.context("missing Rithmic history volume")?),
        }))
    }
}

fn decimal(value: Option<f64>, field: &str) -> Result<Decimal> {
    let value = value.with_context(|| format!("missing Rithmic history {field}"))?;
    ensure!(
        value.is_finite() && value > 0.0,
        "invalid Rithmic history {field}"
    );
    Decimal::from_str(&value.to_string()).context("invalid Rithmic history price")
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic history {field}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn request_and_page_lifecycle_matrix() {
        let payload = minute_bar_replay_request("page", "CME", "NQU6", 100, 300).unwrap();
        let request: protocol::RequestTimeBarReplay = codec::decode(&payload).unwrap();
        assert_eq!(request.template_id, TIME_BAR_REPLAY_REQUEST);
        assert_eq!(request.bar_type_period, Some(1));
        assert_eq!(request.start_index, Some(100));
        assert_eq!(request.finish_index, Some(300));

        let mut decoder = HistoryPageDecoder::new("page".to_string(), 300).unwrap();
        let bar = response(vec!["working"], vec!["0"], Some(120));
        assert!(matches!(
            decoder.decode(&bar).unwrap(),
            HistoryEvent::Bar(_)
        ));
        let page_end = response(vec!["0"], vec!["0"], None);
        assert_eq!(
            decoder.decode(&page_end).unwrap(),
            HistoryEvent::PageEnded {
                next_start: Some(121)
            }
        );
    }

    #[test]
    fn completed_range_and_invalid_response_matrix() {
        let mut decoder = HistoryPageDecoder::new("page".to_string(), 120).unwrap();
        assert!(matches!(
            decoder
                .decode(&response(vec!["working"], vec!["0"], Some(120)))
                .unwrap(),
            HistoryEvent::Bar(_)
        ));
        assert_eq!(
            decoder
                .decode(&response(vec!["0"], vec!["0"], None))
                .unwrap(),
            HistoryEvent::PageEnded { next_start: None }
        );

        for payload in [
            response(vec!["error"], vec!["9"], None),
            response(vec!["9"], vec![], None),
            codec::encode(&protocol::ResponseTimeBarReplay {
                template_id: 204,
                user_msg: vec!["page".to_string()],
                ..Default::default()
            })
            .unwrap(),
            codec::encode(&protocol::ResponseTimeBarReplay {
                template_id: TIME_BAR_REPLAY_RESPONSE,
                user_msg: vec!["other".to_string()],
                ..Default::default()
            })
            .unwrap(),
        ] {
            assert!(HistoryPageDecoder::new("page".to_string(), 120)
                .unwrap()
                .decode(&payload)
                .is_err());
        }
    }

    fn response(rp_code: Vec<&str>, handler_code: Vec<&str>, marker: Option<i32>) -> Vec<u8> {
        codec::encode(&protocol::ResponseTimeBarReplay {
            template_id: TIME_BAR_REPLAY_RESPONSE,
            user_msg: vec!["page".to_string()],
            rq_handler_rp_code: handler_code.into_iter().map(str::to_string).collect(),
            rp_code: rp_code.into_iter().map(str::to_string).collect(),
            symbol: marker.map(|_| "NQU6".to_string()),
            exchange: marker.map(|_| "CME".to_string()),
            r#type: marker.map(|_| protocol::response_time_bar_replay::BarType::MinuteBar as i32),
            period: marker.map(|_| "1".to_string()),
            marker,
            volume: marker.map(|_| 10),
            open_price: marker.map(|_| 100.0),
            high_price: marker.map(|_| 101.0),
            low_price: marker.map(|_| 99.0),
            close_price: marker.map(|_| 100.5),
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn decoded_bar_preserves_decimal_and_end_timestamp() {
        let mut decoder = HistoryPageDecoder::new("page".to_string(), 300).unwrap();
        let HistoryEvent::Bar(bar) = decoder
            .decode(&response(vec!["working"], vec!["0"], Some(120)))
            .unwrap()
        else {
            panic!("expected history bar");
        };
        assert_eq!(bar.end_timestamp, 120_000);
        assert_eq!(bar.open, dec!(100));
        assert_eq!(bar.close, dec!(100.5));
        assert_eq!(bar.volume, dec!(10));
    }
}
