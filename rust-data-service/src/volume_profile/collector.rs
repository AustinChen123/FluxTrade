//! One bounded round for a single trusted-root/job worker; no scheduler or sleep.
use super::binance_spot::Request;
use super::checkpoint::CheckpointProgress;
use super::store::Store;
use super::transport::{Outcome, Transport};
use super::work_policy::{Action, Budget, BudgetError, Disposition, Failure, RetrySchedule};
use anyhow::{ensure, Result};
use async_trait::async_trait;
use std::future::Future;

#[async_trait]
pub trait Source {
    async fn fetch(&mut self, request: &Request) -> Outcome;
}
#[async_trait]
impl Source for Transport {
    async fn fetch(&mut self, request: &Request) -> Outcome {
        Transport::fetch(self, request).await
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reason {
    Complete,
    Budget,
    RetriesExhausted(Disposition, Option<Failure>),
    Deferred(u64),
    Failed(Disposition, Option<Failure>),
}
#[derive(Debug, PartialEq, Eq)]
pub struct Report {
    pub reason: Reason,
    pub requests: u64,
    /// Client-delivered body bytes, not network wire bytes.
    pub response_bytes: u64,
}

/// Each invocation requires a fresh Budget and its own monotonic elapsed origin.
/// The injected clock/wait own no cursor; recovery uses only the Store.
pub async fn round<S, W, F>(
    store: &Store,
    source: &mut S,
    budget: &mut Budget,
    retries: RetrySchedule,
    mut wait: W,
    mut elapsed_ms: impl FnMut() -> u64,
) -> Result<Report>
where
    S: Source,
    W: FnMut(u64) -> F,
    F: Future<Output = ()>,
{
    ensure!(budget.is_fresh(), "round requires fresh budget");
    let (manifest, mut recovered) = store.recover()?;
    let mut sequence = u64::try_from(manifest.entries().len())?;
    let mut ordinal = 0;
    let reason = loop {
        if recovered.pages.is_stopped() {
            break Reason::Complete;
        }
        let request = recovered.pages.request()?;
        match budget.admit(elapsed_ms()) {
            Ok(()) => {}
            Err(BudgetError::Exhausted) => break Reason::Budget,
            Err(error) => anyhow::bail!("budget admission: {error:?}"),
        }
        let outcome = source.fetch(&request).await;
        let exhausted = match budget.account(outcome.response_bytes, elapsed_ms()) {
            Ok(()) => false,
            Err(BudgetError::Exhausted) => true,
            Err(error) => anyhow::bail!("budget accounting: {error:?}"),
        };
        let action = outcome.disposition.action;
        outcome.validate()?;
        match action {
            Action::Success => {
                let body = outcome.body.unwrap();
                let (_, progress) = recovered.pages.accept(&body)?;
                store.append(sequence, &body, CheckpointProgress::try_from(progress)?)?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("sequence overflow"))?;
                ordinal = 0;
                if recovered.pages.is_stopped() {
                    break Reason::Complete;
                }
                if exhausted {
                    break Reason::Budget;
                }
            }
            Action::RetryAfter(seconds) => break Reason::Deferred(seconds),
            Action::Stop => break Reason::Failed(outcome.disposition, outcome.failure),
            Action::Retry => {
                if exhausted {
                    break Reason::Budget;
                }
                let Some(delay) = retries.delay_ms(ordinal) else {
                    break Reason::RetriesExhausted(outcome.disposition, outcome.failure);
                };
                ordinal += 1;
                match budget.admit_delay(elapsed_ms(), delay) {
                    Ok(()) => {}
                    Err(BudgetError::Exhausted) => break Reason::Budget,
                    Err(error) => anyhow::bail!("budget delay: {error:?}"),
                }
                wait(delay).await;
            }
        }
    };
    Ok(Report {
        reason,
        requests: budget.requests(),
        response_bytes: budget.response_bytes(),
    })
}

#[cfg(test)]
mod tests;
