use super::*;
use crate::volume_profile::binance_spot::Pages;
use crate::volume_profile::checkpoint::{recover, PageEntry};
use serde_json::json;
use serde_json::Value;

fn inputs(empty: bool) -> Vec<(Manifest, Recovered)> {
    (0..24).map(|i| {
        let window = Window::new(i * HOUR, (i + 1) * HOUR).unwrap();
        let identity = Identity::new(hourly_staging_job_id("daily", i * HOUR).unwrap(), window, GRID_ID.into(), [3; 32]).unwrap();
        let raw = if empty { b"[]".to_vec() } else { serde_json::to_vec(&json!([
            {"a": i + 1, "p": "10", "q": "1", "T": i * HOUR, "f": i * 2, "l": i * 2 + 1, "m": false, "M": true}
        ])).unwrap() };
        let mut pages = Pages::new(window);
        let (_, progress) = pages.accept(&raw).unwrap();
        let mut entries = vec![PageEntry::new(0, &raw, progress.try_into().unwrap())];
        if !empty { entries.push(PageEntry::new(1, b"[]", CheckpointProgress::EmptyPage)); }
        let manifest = Manifest::new(identity.clone(), entries);
        let rebuilt = recover(&manifest, &identity, |name| Ok(if name.contains("00000000000000000000") { raw.clone() } else { b"[]".to_vec() })).unwrap();
        (manifest, rebuilt)
    }).collect()
}
fn replace_hour(index: i64, raw: &[u8]) -> Result<(Manifest, Recovered)> {
    let window = Window::new(index * HOUR, (index + 1) * HOUR)?;
    let identity = Identity::new(
        hourly_staging_job_id("daily", index * HOUR)?,
        window,
        GRID_ID.into(),
        [3; 32],
    )?;
    let mut pages = Pages::new(window);
    let (_, progress) = pages.accept(raw)?;
    let mut entries = vec![PageEntry::new(0, raw, progress.try_into()?)];
    if !pages.is_stopped() {
        entries.push(PageEntry::new(1, b"[]", CheckpointProgress::EmptyPage));
    }
    let manifest = Manifest::new(identity.clone(), entries);
    let recovered = recover(&manifest, &identity, |name| {
        Ok(if name == manifest.entries()[0].name() {
            raw.to_vec()
        } else {
            b"[]".to_vec()
        })
    })?;
    Ok((manifest, recovered))
}
fn kline(empty: bool) -> Vec<u8> {
    serde_json::to_vec(&json!([[
        0,
        "10",
        "10",
        "10",
        "10",
        if empty { "0" } else { "24" },
        DAY - 1,
        if empty { "0" } else { "240" },
        if empty { 0 } else { 48 },
        "0",
        "0",
        "0"
    ]]))
    .unwrap()
}
fn run(inputs: &[(Manifest, Recovered)], raw: &[u8]) -> Result<Handoff> {
    assemble(
        "daily",
        Window::new(0, DAY).unwrap(),
        [3; 32],
        inputs,
        raw,
        DAY,
    )
}
fn wire(handoff: Handoff) -> Value {
    serde_json::from_slice(&handoff.to_bytes().unwrap()).unwrap()
}

#[test]
fn deterministic_mapping_manifest_digest_and_daily_sums() {
    let inputs = inputs(false);
    let result = wire(run(&inputs, &kline(false)).unwrap());
    assert_eq!(
        hourly_staging_job_id("daily", 0).unwrap(),
        "d4fa1b4aefde9f869816eae9f06037eea6b28229b1b93ca396b21119acc535c7"
    );
    assert_eq!(
        sha(&serde_json::to_vec(&inputs[0].0).unwrap()),
        "fde31657cf31de1d5cb995ba7d957d1bbe03fe5354ebffd314263537e9790e2f"
    );
    assert_eq!(
        result["hours"][0]["manifest_sha256"],
        "fde31657cf31de1d5cb995ba7d957d1bbe03fe5354ebffd314263537e9790e2f"
    );
    assert_eq!(result["hours"][0]["page_count"], 2);
    assert_eq!(
        result["content"]["bins"],
        json!([{"bin_index": 1, "base_volume": "24", "quote_volume": "240", "aggregate_count": 24}])
    );
    assert_eq!(
        result["reconciliation"]["official_constituent_trade_count"],
        48
    );
    assert_eq!(result["reconciliation"]["actual_aggregate_trade_count"], 24);
    assert_eq!(
        result["reconciliation"]["response_sha256"],
        sha(&kline(false))
    );
    for (job, start) in [("../bad", 0), ("daily", -HOUR), ("daily", 1), ("", 0)] {
        assert!(hourly_staging_job_id(job, start).is_err());
    }
    assert_ne!(
        hourly_staging_job_id("daily", 0).unwrap(),
        hourly_staging_job_id("daily", HOUR).unwrap()
    );
}

#[test]
fn identity_coverage_and_terminal_manifest_matrix() {
    for (field, value) in [
        ("job_id", json!("other")),
        ("start_ms", json!(HOUR)),
        ("end_ms", json!(2 * HOUR)),
        ("grid_id", json!("g1")),
        ("config_sha256", json!(vec![4; 32])),
        ("schema", json!(2)),
        ("product_id", json!("BINANCE:BTCUSDT-PERP")),
        ("algorithm", json!("vp-v2")),
    ] {
        let mut inputs = inputs(false);
        let mut manifest = serde_json::to_value(&inputs[0].0).unwrap();
        manifest["identity"][field] = value;
        inputs[0].0 = serde_json::from_value(manifest).unwrap();
        assert!(run(&inputs, &kline(false)).is_err());
    }
    for kind in 0..6 {
        let mut inputs = inputs(false);
        match kind {
            0 => {
                inputs.remove(0);
            }
            1 => inputs.swap(0, 1),
            2 => {
                let manifest = Manifest::new(inputs[0].0.identity().clone(), vec![]);
                let recovered =
                    recover(&manifest, manifest.identity(), |_| unreachable!()).unwrap();
                inputs[0] = (manifest, recovered);
            }
            3 => inputs[0].0 = Manifest::new(inputs[0].0.identity().clone(), vec![]),
            4 => {
                inputs[0].0 = Manifest::new(
                    inputs[0].0.identity().clone(),
                    vec![PageEntry::new(1, b"[]", CheckpointProgress::EmptyPage)],
                )
            }
            _ => {
                inputs[0].0 = Manifest::new(
                    inputs[0].0.identity().clone(),
                    vec![PageEntry::new(0, b"[]", CheckpointProgress::Next(2))],
                )
            }
        }
        assert!(run(&inputs, &kline(false)).is_err());
    }
}

#[test]
fn trade_hour_id_gap_and_overflow_reject_without_changing_inputs() {
    let trade = json!({"a": 1, "p": "10", "q": "1", "T": 0, "f": 0, "l": 1, "m": false, "M": true});
    assert!(replace_hour(1, &serde_json::to_vec(&json!([trade.clone()])).unwrap()).is_err());
    let mut gap = trade.clone();
    gap["a"] = json!(3);
    assert!(replace_hour(
        0,
        &serde_json::to_vec(&json!([trade.clone(), gap.clone()])).unwrap()
    )
    .is_err());
    for kind in 0..3 {
        let mut inputs = inputs(false);
        let mut changed = trade.clone();
        let hour = if kind == 0 { 1 } else { 0 };
        if kind == 0 {
            changed = gap.clone();
            changed["T"] = json!(HOUR);
        }
        if kind == 1 {
            changed["T"] = json!(HOUR);
        } // Recovery crops an out-of-hour trade.
        if kind == 2 {
            changed["p"] = json!(Decimal::MAX.to_string());
            changed["q"] = json!("2");
        }
        inputs[hour] =
            replace_hour(hour as i64, &serde_json::to_vec(&json!([changed])).unwrap()).unwrap();
        let before = inputs[0].1.trades().to_vec();
        assert!(run(&inputs, &kline(false)).is_err());
        assert_eq!(inputs[0].1.trades(), before);
    }
    assert!(run(&inputs(false), &kline(false)).is_ok());
}

#[test]
fn recovery_is_bound_to_the_exact_manifest_that_was_verified() {
    let empty = inputs(true);
    assert!(run(&empty, &kline(true)).is_ok());
    let nonempty = inputs(false);
    assert!(run(&nonempty, &kline(false)).is_ok());
    let mismatched: Vec<_> = empty
        .into_iter()
        .zip(nonempty)
        .map(|((manifest, _), (_, recovered))| (manifest, recovered))
        .collect();
    let error = run(&mismatched, &kline(false)).unwrap_err();
    assert_eq!(error.to_string(), "manifest/recovery binding mismatch");
}

#[test]
fn official_kline_shape_types_window_and_exact_reconciliation() {
    let input = inputs(false);
    for (field, value) in [
        (0, json!(1)),
        (6, json!(DAY)),
        (5, json!(24)),
        (7, json!(240)),
        (8, json!(true)),
        (8, json!(-1)),
        (8, json!(0)),
        (5, json!("23")),
        (7, json!("241")),
    ] {
        let mut raw: Value = serde_json::from_slice(&kline(false)).unwrap();
        raw[0][field] = value;
        assert!(run(&input, &serde_json::to_vec(&raw).unwrap()).is_err());
    }
    for field in [1, 2, 3, 4, 5, 7, 9, 10, 11] {
        for bad in [
            json!("NaN"),
            json!("-1"),
            json!(1),
            json!("1e3"),
            json!("79228162514264337593543950336"),
        ] {
            let mut raw: Value = serde_json::from_slice(&kline(false)).unwrap();
            raw[0][field] = bad;
            assert!(run(&input, &serde_json::to_vec(&raw).unwrap()).is_err());
        }
    }
    let row = serde_json::from_slice::<Value>(&kline(false)).unwrap()[0].clone();
    for raw in [
        json!([]),
        json!([row.clone(), row.clone()]),
        json!([[]]),
        json!({}),
    ] {
        assert!(run(&input, &serde_json::to_vec(&raw).unwrap()).is_err());
    }
    let mut raw: Value = serde_json::from_slice(&kline(false)).unwrap();
    raw[0][8] = json!(1); // Diagnostic constituent count is not aggregate record count.
    assert!(run(&input, &serde_json::to_vec(&raw).unwrap()).is_ok());
    assert!(run(&inputs(true), &kline(true)).is_ok());
    raw = serde_json::from_slice(&kline(true)).unwrap();
    raw[0][8] = json!(1);
    assert!(run(&inputs(true), &serde_json::to_vec(&raw).unwrap()).is_err());
    assert!(run(&input, &vec![b' '; 65_537]).is_err());
}
