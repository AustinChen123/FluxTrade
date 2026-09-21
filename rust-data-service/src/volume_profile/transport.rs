//! Public Binance spot transport only. No credentials, redirects, proxies or retries.
use std::time::Duration;

use anyhow::{ensure, Result};
use reqwest::{header::RETRY_AFTER, Client, Url};

use super::binance_spot::{Request, ENDPOINT, PAGE_LIMIT, SYMBOL};
use super::work_policy::{self, Action, Disposition, Failure};

#[derive(Debug)]
pub struct Outcome {
    pub disposition: Disposition,
    pub failure: Option<Failure>,
    pub body: Option<Vec<u8>>,
    /// Body bytes actually yielded by the HTTP client, including an excess chunk.
    pub response_bytes: u64,
}

impl Outcome {
    /// Verify shape, byte accounting and owner-derived disposition before use.
    pub fn validate(&self) -> Result<()> {
        let expected = if let Some(kind) = self.failure {
            ensure!(self.body.is_none(), "failure outcome contains body");
            work_policy::failure(kind, self.disposition.status)
        } else {
            let status = self
                .disposition
                .status
                .ok_or_else(|| anyhow::anyhow!("HTTP outcome missing status"))?;
            let expected = match self.disposition.action {
                Action::RetryAfter(seconds) => work_policy::http(status, &[&seconds.to_string()]),
                _ => work_policy::http(status, &[]),
            };
            if self.disposition.action == Action::Success {
                let body = self
                    .body
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("success missing body"))?;
                ensure!(
                    self.response_bytes == body.len() as u64,
                    "success byte count mismatch"
                );
            } else {
                ensure!(self.body.is_none(), "non-success contains body");
            }
            expected
        };
        ensure!(
            self.disposition == expected,
            "disposition conflicts with owner policy"
        );
        Ok(())
    }
}

pub struct Transport {
    client: Client,
    endpoint: Url,
    limit: usize,
}

impl Transport {
    pub fn new(timeout: Duration, response_limit: usize) -> Result<Self> {
        ensure!(!timeout.is_zero(), "positive limits required");
        ensure!(response_limit > 0, "positive limits required");
        ensure!(response_limit < usize::MAX, "response limit overflow");
        let client = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        Ok(Self {
            client,
            endpoint: Url::parse(ENDPOINT)?,
            limit: response_limit,
        })
    }

    pub async fn fetch(&self, request: &Request) -> Outcome {
        let mut params = vec![
            ("symbol", SYMBOL.to_string()),
            ("limit", PAGE_LIMIT.to_string()),
        ];
        match request {
            Request::First {
                start_time,
                end_time,
            } => {
                params.push(("startTime", start_time.to_string()));
                params.push(("endTime", end_time.to_string()));
            }
            Request::Next { from_id } => params.push(("fromId", from_id.to_string())),
        }
        bounded_request(
            self.client.get(self.endpoint.clone()).query(&params),
            self.limit,
        )
        .await
    }
}

/// Shared bounded HTTP mechanics; provider owners retain endpoint/query policy.
pub(crate) async fn bounded_request(request: reqwest::RequestBuilder, limit: usize) -> Outcome {
    let mut response = match request.send().await {
        Ok(response) => response,
        Err(error) => return transport_error(error, None, 0),
    };
    let code = response.status().as_u16();
    let status = Some(code);
    let values: Vec<_> = response
        .headers()
        .get_all(RETRY_AFTER)
        .iter()
        .map(|v| v.to_str().unwrap_or(""))
        .collect();
    let disposition = work_policy::http(code, &values);
    if disposition.action != Action::Success {
        return Outcome {
            disposition,
            failure: None,
            body: None,
            response_bytes: 0,
        };
    }
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return failed(Failure::Protocol, status, 0);
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let total = (body.len() as u64).checked_add(chunk.len() as u64);
                match total {
                    Some(size) if size <= limit as u64 => body.extend_from_slice(&chunk),
                    _ => return failed(Failure::Protocol, status, total.unwrap_or(u64::MAX)),
                }
            }
            Ok(None) => {
                return Outcome {
                    disposition,
                    failure: None,
                    response_bytes: body.len() as u64,
                    body: Some(body),
                }
            }
            Err(error) => return transport_error(error, status, body.len() as u64),
        }
    }
}

fn failed(kind: Failure, status: Option<u16>, response_bytes: u64) -> Outcome {
    Outcome {
        disposition: work_policy::failure(kind, status),
        failure: Some(kind),
        body: None,
        response_bytes,
    }
}

fn transport_error(error: reqwest::Error, status: Option<u16>, bytes: u64) -> Outcome {
    let kind = if error.is_timeout() {
        Failure::Timeout
    } else {
        Failure::Connection
    };
    failed(kind, status, bytes)
}

#[cfg(test)]
mod tests;
