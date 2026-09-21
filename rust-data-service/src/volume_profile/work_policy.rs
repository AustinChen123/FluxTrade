//! Pure single-worker work policy. Callers own I/O, bounded retry counts/backoff,
//! and monotonic elapsed milliseconds measured from the start of the job.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Success,
    RetryAfter(u64),
    Retry,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disposition {
    pub status: Option<u16>,
    pub action: Action,
}

/// Header values must be canonical decimal seconds, not HTTP dates or lists.
/// Identical duplicate values are harmless; conflicting values fail closed.
/// Zero is valid HTTP delay-seconds (RFC 9110 section 10.2.3).
pub fn http(status: u16, retry_after: &[&str]) -> Disposition {
    let action = match status {
        200..=299 => Action::Success,
        429 => match retry_after.first() {
            Some(value) if retry_after.iter().all(|v| v == value) => {
                let canonical = *value == "0"
                    || (value.starts_with(|c: char| ('1'..='9').contains(&c))
                        && value.bytes().all(|c| c.is_ascii_digit()));
                match value.parse::<u64>().ok().filter(|_| canonical) {
                    Some(seconds) => Action::RetryAfter(seconds),
                    None => Action::Stop,
                }
            }
            _ => Action::Stop,
        },
        500..=599 => Action::Retry,
        _ => Action::Stop,
    };
    Disposition {
        status: Some(status),
        action,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Timeout,
    Connection,
    Decode,
    Protocol,
}

pub fn failure(error: Failure, status: Option<u16>) -> Disposition {
    Disposition {
        status,
        action: match error {
            Failure::Timeout | Failure::Connection => match status {
                Some(code) if !(200..=299).contains(&code) => http(code, &[]).action,
                _ => Action::Retry,
            },
            Failure::Decode | Failure::Protocol => Action::Stop,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub requests: u64,
    pub response_bytes: u64,
    pub elapsed_ms: u64,
}

/// Pure retry delays in milliseconds. Ordinal zero is the first retry, not the
/// initial attempt. No sleeping or wall-clock deadline arithmetic is performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrySchedule {
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

impl RetrySchedule {
    pub fn new(max_retries: u32, base_delay_ms: u64, max_delay_ms: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_retries > 0 && base_delay_ms > 0,
            "positive retry limits required"
        );
        anyhow::ensure!(base_delay_ms <= max_delay_ms, "retry base exceeds cap");
        Ok(Self {
            max_retries,
            base_delay_ms,
            max_delay_ms,
        })
    }

    /// Saturate to the configured cap before arithmetic can wrap. At most 64
    /// doublings are needed even when the caller supplies a very large ordinal.
    pub fn delay_ms(&self, ordinal: u32) -> Option<u64> {
        if ordinal >= self.max_retries {
            return None;
        }
        let mut delay = self.base_delay_ms;
        for _ in 0..ordinal.min(64) {
            delay = delay
                .checked_mul(2)
                .unwrap_or(self.max_delay_ms)
                .min(self.max_delay_ms);
            if delay == self.max_delay_ms {
                break;
            }
        }
        Some(delay)
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 200,
            response_bytes: 25 * 1024 * 1024,
            elapsed_ms: 300_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetError {
    InvalidLimits,
    Exhausted,
    Overflow,
    InFlight,
    NoAttempt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Budget {
    limits: Limits,
    requests: u64,
    bytes: u64,
    in_flight: bool,
    stopped: bool,
}

impl Budget {
    pub fn new(limits: Limits) -> Result<Self, BudgetError> {
        if limits.requests == 0 || limits.response_bytes == 0 || limits.elapsed_ms == 0 {
            return Err(BudgetError::InvalidLimits);
        }
        Ok(Self {
            limits,
            requests: 0,
            bytes: 0,
            in_flight: false,
            stopped: false,
        })
    }
    pub fn requests(&self) -> u64 {
        self.requests
    }
    pub fn response_bytes(&self) -> u64 {
        self.bytes
    }

    pub fn is_fresh(&self) -> bool {
        self.requests == 0 && self.bytes == 0 && !self.in_flight && !self.stopped
    }

    /// Admit a delay only between attempts and strictly inside the time budget.
    /// The caller supplies the latest monotonic elapsed time; no attempt is charged.
    pub fn admit_delay(&mut self, elapsed_ms: u64, delay_ms: u64) -> Result<(), BudgetError> {
        if self.in_flight {
            return Err(BudgetError::InFlight);
        }
        if self.stopped
            || elapsed_ms
                .checked_add(delay_ms)
                .is_none_or(|end| end >= self.limits.elapsed_ms)
        {
            self.stopped = true;
            return Err(BudgetError::Exhausted);
        }
        Ok(())
    }

    /// Charge an attempt before I/O. Never refund failed or cancelled attempts.
    pub fn admit(&mut self, elapsed_ms: u64) -> Result<(), BudgetError> {
        if self.in_flight {
            return Err(BudgetError::InFlight);
        }
        if self.stopped
            || self.requests >= self.limits.requests
            || self.bytes >= self.limits.response_bytes
            || elapsed_ms >= self.limits.elapsed_ms
        {
            self.stopped = true;
            return Err(BudgetError::Exhausted);
        }
        self.requests = self.requests.checked_add(1).ok_or(BudgetError::Overflow)?;
        self.in_flight = true;
        Ok(())
    }

    /// Settle exactly one admitted attempt, including zero-byte transport failures.
    /// Exhaustion is reported after accounting; overflow permanently stops work.
    pub fn account(&mut self, response_bytes: u64, elapsed_ms: u64) -> Result<(), BudgetError> {
        if !self.in_flight {
            return Err(BudgetError::NoAttempt);
        }
        self.in_flight = false;
        self.bytes = match self.bytes.checked_add(response_bytes) {
            Some(total) => total,
            None => {
                self.stopped = true;
                return Err(BudgetError::Overflow);
            }
        };
        self.stopped = self.requests >= self.limits.requests
            || self.bytes >= self.limits.response_bytes
            || elapsed_ms >= self.limits.elapsed_ms;
        if self.stopped {
            Err(BudgetError::Exhausted)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;
