use super::*;
use crate::volume_profile::{Grid, Window};

fn profile() -> VolumeProfile {
    let mut p = VolumeProfile::new(
        PRODUCT_ID,
        Grid::new(Decimal::ZERO, Decimal::TEN, "USDT").unwrap(),
        Window::new(0, DAY).unwrap(),
    )
    .unwrap();
    p.add(PRODUCT_ID, 0, Decimal::from(5), Decimal::from(2))
        .unwrap();
    p.add(PRODUCT_ID, 2 * HOUR, Decimal::from(25), Decimal::ONE)
        .unwrap();
    p
}

#[test]
fn legitimate_655_bins_exceed_old_byte_envelope() {
    let mut p = VolumeProfile::new(
        PRODUCT_ID,
        Grid::new(Decimal::ZERO, Decimal::TEN, "USDT").unwrap(),
        Window::new(0, DAY).unwrap(),
    )
    .unwrap();
    for index in 0..655_i64 {
        p.add(
            PRODUCT_ID,
            0,
            Decimal::from((10_000_000_000 + index) * 10 + 1),
            Decimal::ONE,
        )
        .unwrap();
    }
    let evidence = (0..24)
        .map(|hour| {
            HourEvidence::new(
                hour * HOUR,
                "a".repeat(64),
                1,
                if hour == 0 { Some(1) } else { None },
                if hour == 0 { Some(655) } else { None },
                if hour == 0 { 655 } else { 0 },
            )
            .unwrap()
        })
        .collect();
    let wire = handoff(&p, evidence).unwrap();
    assert!(655 * 9 > 4096); // Each bin object has four keys and four scalar values.
    let bytes = wire.to_bytes().unwrap();
    assert!(bytes.len() <= MAX_BYTES);
    assert!(bytes.len() > 65_536);
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), wire.wire);
    let oversized = Handoff {
        wire: json!({"padding": "x".repeat(MAX_BYTES)}),
    };
    assert!(oversized.to_bytes().is_err());
}
fn hours() -> Vec<HourEvidence> {
    (0..24)
        .map(|i| {
            let id = if i == 0 {
                Some(100)
            } else if i == 2 {
                Some(101)
            } else {
                None
            };
            HourEvidence::new(i * HOUR, "a".repeat(64), 1, id, id, u64::from(id.is_some())).unwrap()
        })
        .collect()
}
fn handoff(p: &VolumeProfile, hours: Vec<HourEvidence>) -> Result<Handoff> {
    Handoff::new(
        "daily-job".into(),
        "c".repeat(64),
        GRID_ID.into(),
        p,
        hours,
        Reconciliation::new(
            p,
            "b".repeat(64),
            p.totals().base_volume,
            p.totals().quote_volume,
            if p.totals().aggregate_count == 0 {
                0
            } else {
                5
            },
        )?,
        DAY,
    )
}

#[test]
fn shared_python_fixture_is_byte_and_digest_identical() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../python-strategy/tests/fixtures/profile_handoff_v1.json"
    ))
    .unwrap();
    let wire = handoff(&profile(), hours()).unwrap();
    assert_eq!(
        wire.to_bytes().unwrap(),
        fixture["wire_bytes"].as_str().unwrap().as_bytes()
    );
    assert_eq!(
        serde_json::to_vec(&wire.wire["content"]).unwrap(),
        fixture["content_bytes"].as_str().unwrap().as_bytes()
    );
    assert_eq!(wire.wire["content_sha256"], fixture["content_sha256"]);
    let mut stdout = wire.to_bytes().unwrap();
    stdout.push(b'\n');
    assert_eq!(
        sha(&stdout),
        "f6630bf75baedaa34b750b167eb0fdda296b14b4c945ec00f2e0f36aa2576ca7"
    );
}

#[test]
fn coverage_ids_and_counts_fail_closed() {
    let p = profile();
    for kind in 0..5 {
        let mut h = hours();
        match kind {
            0 => {
                h.remove(1);
            }
            1 => h.swap(0, 1),
            2 => {
                h[2] =
                    HourEvidence::new(2 * HOUR, "a".repeat(64), 1, Some(100), Some(100), 1).unwrap()
            }
            3 => {
                h[2] =
                    HourEvidence::new(2 * HOUR, "a".repeat(64), 1, Some(102), Some(102), 1).unwrap()
            }
            _ => h[2] = HourEvidence::new(2 * HOUR, "a".repeat(64), 1, None, None, 0).unwrap(),
        }
        assert!(handoff(&p, h).is_err());
    }
    assert!(HourEvidence::new(0, "a".repeat(64), 0, None, None, 0).is_err());
    assert!(HourEvidence::new(0, "A".repeat(64), 1, None, None, 0).is_err());
    assert!(HourEvidence::new(0, "a".repeat(64), 1, Some(1), Some(1), 2).is_err());
    assert!(HourEvidence::new(0, "a".repeat(64), 1, Some(1), None, 0).is_err());
    assert!(
        Reconciliation::new(&p, "b".repeat(64), Decimal::ONE, p.totals().quote_volume, 5).is_err()
    );
    assert!(Reconciliation::new(
        &p,
        "b".repeat(64),
        p.totals().base_volume,
        p.totals().quote_volume,
        0
    )
    .is_err());
}

#[test]
fn empty_requires_official_zero_and_identity_is_bounded() {
    let p = VolumeProfile::new(
        PRODUCT_ID,
        Grid::new(Decimal::ZERO, Decimal::TEN, "USDT").unwrap(),
        Window::new(0, DAY).unwrap(),
    )
    .unwrap();
    let h: Vec<_> = (0..24)
        .map(|i| HourEvidence::new(i * HOUR, "a".repeat(64), 1, None, None, 0).unwrap())
        .collect();
    assert!(handoff(&p, h.clone()).is_ok());
    assert!(Reconciliation::new(&p, "b".repeat(64), Decimal::ZERO, Decimal::ZERO, 1).is_err());
    let r = Reconciliation::new(&p, "b".repeat(64), Decimal::ZERO, Decimal::ZERO, 0).unwrap();
    for (job, config, available) in [
        ("../bad", "c".repeat(64), DAY),
        ("job", "C".repeat(64), DAY),
        ("job", "c".repeat(64), DAY - 1),
    ] {
        assert!(Handoff::new(
            job.into(),
            config,
            GRID_ID.into(),
            &p,
            h.clone(),
            r.clone(),
            available
        )
        .is_err());
    }
    let mut wire = handoff(&p, h).unwrap();
    wire.wire["extra"] = json!("x".repeat(MAX_BYTES));
    assert!(wire.to_bytes().is_err());
}

#[test]
fn mvp_grid_and_exact_hour_id_span_are_required() {
    assert!(HourEvidence::new(0, "a".repeat(64), 1, Some(101), Some(102), 1).is_err());
    let empty = |origin, step, unit| {
        VolumeProfile::new(
            PRODUCT_ID,
            Grid::new(origin, step, unit).unwrap(),
            Window::new(0, DAY).unwrap(),
        )
        .unwrap()
    };
    let good = empty(Decimal::ZERO, Decimal::TEN, "USDT");
    let hours: Vec<_> = (0..24)
        .map(|i| HourEvidence::new(i * HOUR, "a".repeat(64), 1, None, None, 0).unwrap())
        .collect();
    let evidence =
        Reconciliation::new(&good, "b".repeat(64), Decimal::ZERO, Decimal::ZERO, 0).unwrap();
    for (grid_id, origin, step, unit) in [
        ("g1", Decimal::ZERO, Decimal::TEN, "USDT"),
        (GRID_ID, Decimal::ONE, Decimal::TEN, "USDT"),
        (GRID_ID, Decimal::ZERO, Decimal::ONE, "USDT"),
        (GRID_ID, Decimal::ZERO, Decimal::TEN, "USDC"),
    ] {
        let profile = empty(origin, step, unit);
        assert!(Handoff::new(
            "job".into(),
            "c".repeat(64),
            grid_id.into(),
            &profile,
            hours.clone(),
            evidence.clone(),
            DAY
        )
        .is_err());
    }
}
