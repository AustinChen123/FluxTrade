//! One trusted-root/job fetch invocation, no scheduling or cleanup.
//! Bounds: four attempts, 256 KiB client-delivered bodies, 30 seconds elapsed;
//! 3-second transport attempts and 250/500/1000ms retry delays.
use anyhow::{ensure, Result};
use async_trait::async_trait;
use rustix::fd::AsFd;
use std::future::Future;

use super::{
    binance_kline,
    kline_evidence::{self, Identity},
    mvp,
    transport::Outcome,
    work_policy::{Action, Budget, BudgetError, Failure, Limits, RetrySchedule},
    Window,
};

#[async_trait]
pub trait Source {
    async fn fetch(&mut self, day: Window, now_ms: i64) -> Result<Outcome>;
}
#[async_trait]
impl Source for binance_kline::Transport {
    async fn fetch(&mut self, day: Window, now_ms: i64) -> Result<Outcome> {
        binance_kline::Transport::fetch(self, day, now_ms).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Reason {
    Complete,
    Existing,
    Deferred,
    Failed,
    RetriesExhausted,
    Budget,
}

#[derive(Debug)]
pub struct Report {
    reason: Reason,
    status: Option<u16>,
    failure: Option<Failure>,
    requests: u64,
    response_bytes: u64,
    retry_after: Option<u64>,
    observed_at_ms: Option<i64>,
}
impl Report {
    pub fn exit_code(&self) -> u8 {
        match self.reason {
            Reason::Complete | Reason::Existing => 0,
            Reason::Failed => 1,
            _ => 75,
        }
    }
    pub fn json(&self, identity: &Identity) -> serde_json::Value {
        let failure = self.failure.map(|f| match f {
            Failure::Timeout => "TIMEOUT",
            Failure::Connection => "CONNECTION",
            Failure::Decode => "DECODE",
            Failure::Protocol => "PROTOCOL",
        });
        serde_json::json!({"reason":self.reason,"status":self.status,"failure":failure,
            "requests":self.requests,"response_bytes":self.response_bytes,"retry_after_seconds":self.retry_after,
            "observed_at_ms":self.observed_at_ms,"job_id":identity.job_id(),"start_ms":identity.start_ms(),
            "end_ms":identity.end_ms(),"product_id":"BINANCE:BTCUSDT-SPOT","config_sha256":mvp::config_hex()})
    }
}

pub async fn run<S, W, F>(
    root: &impl AsFd,
    identity: &Identity,
    source: &mut S,
    mut wall_ms: impl FnMut() -> Result<i64>,
    mut elapsed_ms: impl FnMut() -> u64,
    mut wait: W,
) -> Result<Report>
where
    S: Source,
    W: FnMut(u64) -> F,
    F: Future<Output = ()>,
{
    let mut report = Report {
        reason: Reason::Existing,
        status: None,
        failure: None,
        requests: 0,
        response_bytes: 0,
        retry_after: None,
        observed_at_ms: None,
    };
    if let Some(existing) = kline_evidence::recover_optional(root, identity)? {
        report.observed_at_ms = Some(existing.observed_at_ms());
        return Ok(report);
    }
    let day = Window::new(identity.start_ms(), identity.end_ms())?;
    let mut previous_elapsed = 0;
    let mut elapsed = || -> Result<u64> {
        let current = elapsed_ms();
        ensure!(current >= previous_elapsed, "elapsed clock regressed");
        previous_elapsed = current;
        Ok(current)
    };
    let mut previous_wall = day.end_ms();
    let mut budget = Budget::new(Limits {
        requests: 4,
        response_bytes: 4 * binance_kline::RAW_LIMIT as u64,
        elapsed_ms: 30_000,
    })
    .map_err(|_| anyhow::anyhow!("invalid fetch budget"))?;
    let schedule = RetrySchedule::new(3, 250, 4000)?;
    let mut ordinal = 0;
    loop {
        let before = wall_ms()?;
        ensure!(
            before >= previous_wall && before <= 253_402_300_799_999,
            "invalid source clock"
        );
        previous_wall = before;
        if !admitted(budget.admit(elapsed()?))? {
            report.reason = Reason::Budget;
            break;
        }
        let outcome = source.fetch(day, before).await?;
        let exhausted = !admitted(budget.account(outcome.response_bytes, elapsed()?))?;
        report.requests = budget.requests();
        report.response_bytes = budget.response_bytes();
        outcome.validate()?;
        report.status = outcome.disposition.status;
        report.failure = outcome.failure;
        match outcome.disposition.action {
            Action::Success => {
                let raw = outcome.body.expect("validated success body");
                if binance_kline::parse(&raw, day).is_err() {
                    report.reason = Reason::Failed;
                    report.failure = Some(Failure::Decode);
                    break;
                }
                let observed = wall_ms()?;
                ensure!(
                    observed >= before && observed <= 253_402_300_799_999,
                    "invalid source clock"
                );
                let evidence = kline_evidence::persist(root, identity.clone(), &raw, observed)?;
                report.observed_at_ms = Some(evidence.observed_at_ms());
                report.reason = Reason::Complete;
                break;
            }
            Action::RetryAfter(seconds) => {
                report.reason = Reason::Deferred;
                report.retry_after = Some(seconds);
                break;
            }
            Action::Stop => {
                report.reason = Reason::Failed;
                break;
            }
            Action::Retry => {
                let Some(delay) = schedule.delay_ms(ordinal) else {
                    report.reason = Reason::RetriesExhausted;
                    break;
                };
                if exhausted || !admitted(budget.admit_delay(elapsed()?, delay))? {
                    report.reason = Reason::Budget;
                    break;
                }
                ordinal += 1;
                wait(delay).await;
            }
        }
    }
    Ok(report)
}

fn admitted(result: std::result::Result<(), BudgetError>) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(BudgetError::Exhausted) => Ok(false),
        Err(_) => anyhow::bail!("fetch budget integrity failure"),
    }
}

#[cfg(test)]
mod tests;
