use super::*;

#[test]
fn freshness_and_delay_admission_boundaries() {
    let mut b = budget();
    assert!(b.is_fresh());
    for (elapsed, delay) in [(0, 0), (0, 99), (98, 1)] {
        assert_eq!(b.admit_delay(elapsed, delay), Ok(()));
        assert!(b.is_fresh());
        assert_eq!((b.requests(), b.response_bytes()), (0, 0));
    }
    b.admit(0).unwrap();
    assert!(!b.is_fresh());
    assert_eq!(b.admit_delay(0, 1), Err(BudgetError::InFlight));
    assert_eq!((b.requests(), b.response_bytes()), (1, 0));
    b.account(1, 1).unwrap();
    assert!(!b.is_fresh());
    b.admit_delay(1, 1).unwrap();
    assert_eq!((b.requests(), b.response_bytes()), (1, 1));
    for (elapsed, delay) in [(99, 1), (99, 2), (100, 0), (0, u64::MAX), (1, u64::MAX)] {
        let mut b = budget();
        assert_eq!(b.admit_delay(elapsed, delay), Err(BudgetError::Exhausted));
        assert!(!b.is_fresh());
        assert_eq!((b.requests(), b.response_bytes()), (0, 0));
        assert_eq!(b.admit_delay(0, 0), Err(BudgetError::Exhausted));
        assert_eq!(b.admit(0), Err(BudgetError::Exhausted));
    }
    let mut b = budget();
    assert_eq!(b.admit(100), Err(BudgetError::Exhausted));
    assert!(!b.is_fresh());
    assert_eq!(b.admit_delay(0, 0), Err(BudgetError::Exhausted));
}

#[test]
fn retry_schedule_validation_cap_and_overflow() {
    for (count, base, cap) in [(0, 1, 1), (1, 0, 1), (1, 1, 0), (1, 2, 1)] {
        assert!(RetrySchedule::new(count, base, cap).is_err());
    }
    let schedule = RetrySchedule::new(5, 3, 10).unwrap();
    assert_eq!(
        (0..=5).map(|n| schedule.delay_ms(n)).collect::<Vec<_>>(),
        vec![Some(3), Some(6), Some(10), Some(10), Some(10), None]
    );
    assert_eq!(schedule.delay_ms(u32::MAX), None);
    let fixed = RetrySchedule::new(1, 7, 7).unwrap();
    assert_eq!(fixed.delay_ms(0), Some(7));
    assert_eq!(fixed.delay_ms(1), None);
    for base in [1, u64::MAX / 2 + 1, u64::MAX] {
        let large = RetrySchedule::new(u32::MAX, base, u64::MAX).unwrap();
        assert_eq!(large.delay_ms(0), Some(base));
        assert_eq!(large.delay_ms(64), Some(u64::MAX));
        assert_eq!(large.delay_ms(u32::MAX - 1), Some(u64::MAX));
        assert_eq!(large.delay_ms(u32::MAX), None);
        assert_eq!(
            std::time::Duration::from_millis(large.delay_ms(64).unwrap()).as_millis(),
            u128::from(u64::MAX)
        );
    }
}

#[test]
fn http_status_and_retry_after_matrix() {
    for status in 100..=599 {
        let expected = match status {
            200..=299 => Action::Success,
            429 => Action::RetryAfter(2),
            500..=599 => Action::Retry,
            _ => Action::Stop,
        };
        assert_eq!(http(status, &["2"]).status, Some(status));
        assert_eq!(http(status, &["2"]).action, expected);
    }
    for headers in [
        vec![],
        vec![""],
        vec!["-1"],
        vec!["1.5"],
        vec!["+1"],
        vec!["01"],
        vec![" 1"],
        vec!["1 "],
        vec!["1,1"],
        vec!["18446744073709551616"],
        vec!["tomorrow"],
        vec!["1", "2"],
        vec!["1", "bad"],
    ] {
        assert_eq!(http(429, &headers).action, Action::Stop);
    }
    for (headers, seconds) in [
        (vec!["0"], 0),
        (vec!["2", "2"], 2),
        (vec!["18446744073709551615"], u64::MAX),
    ] {
        assert_eq!(http(429, &headers).action, Action::RetryAfter(seconds));
    }
    for error in [
        Failure::Timeout,
        Failure::Connection,
        Failure::Decode,
        Failure::Protocol,
    ] {
        for status in [None, Some(200), Some(418), Some(429), Some(451), Some(500)] {
            let result = failure(error, status);
            let expected = match (error, status) {
                (Failure::Decode | Failure::Protocol, _) => Action::Stop,
                (_, Some(418 | 429 | 451)) => Action::Stop,
                (_, None | Some(200 | 500)) => Action::Retry,
                _ => Action::Stop,
            };
            assert_eq!(result.status, status);
            assert_eq!(result.action, expected, "{error:?} x {status:?}");
        }
    }
}

fn budget() -> Budget {
    Budget::new(Limits {
        requests: 2,
        response_bytes: 10,
        elapsed_ms: 100,
    })
    .unwrap()
}

#[test]
fn limits_admission_and_attempt_lifecycle() {
    let defaults = Limits::default();
    assert_eq!(defaults.requests, 200);
    assert_eq!(defaults.response_bytes, 26_214_400);
    assert_eq!(defaults.elapsed_ms, 300_000);
    for (requests, response_bytes, elapsed_ms) in [(0, 1, 1), (1, 0, 1), (1, 1, 0)] {
        let limits = Limits {
            requests,
            response_bytes,
            elapsed_ms,
        };
        assert_eq!(Budget::new(limits), Err(BudgetError::InvalidLimits));
    }
    let mut b = budget();
    assert_eq!(b.account(0, 0), Err(BudgetError::NoAttempt));
    b.admit(0).unwrap();
    assert_eq!(b.admit(0), Err(BudgetError::InFlight));
    b.account(0, 1).unwrap();
    assert_eq!(b.account(0, 1), Err(BudgetError::NoAttempt));
    b.admit(1).unwrap();
    assert_eq!(b.account(0, 2), Err(BudgetError::Exhausted));
    assert_eq!(b.admit(2), Err(BudgetError::Exhausted));
    assert_eq!(b.requests(), 2);
}

#[test]
fn exact_and_one_over_byte_time_limits_preserve_charged_attempts() {
    for bytes in [9, 10, 11] {
        for elapsed in [99, 100, 101] {
            let mut b = budget();
            b.admit(0).unwrap();
            let exhausted = bytes >= 10 || elapsed >= 100;
            assert_eq!(
                b.account(bytes, elapsed),
                if exhausted {
                    Err(BudgetError::Exhausted)
                } else {
                    Ok(())
                }
            );
            assert_eq!((b.requests(), b.response_bytes()), (1, bytes));
            assert_eq!(b.admit(elapsed).is_err(), exhausted);
        }
    }
    for elapsed in [100, 101] {
        let mut b = budget();
        assert_eq!(b.admit(elapsed), Err(BudgetError::Exhausted));
        assert_eq!(b.requests(), 0);
    }
}

#[test]
fn byte_overflow_stops_without_wrapping_or_refunding_attempt() {
    let mut b = Budget::new(Limits {
        response_bytes: u64::MAX,
        ..Limits::default()
    })
    .unwrap();
    b.admit(0).unwrap();
    b.account(u64::MAX - 1, 1).unwrap();
    b.admit(1).unwrap();
    assert_eq!(b.account(2, 2), Err(BudgetError::Overflow));
    assert_eq!((b.requests(), b.response_bytes()), (2, u64::MAX - 1));
    assert_eq!(b.admit(2), Err(BudgetError::Exhausted));
}
