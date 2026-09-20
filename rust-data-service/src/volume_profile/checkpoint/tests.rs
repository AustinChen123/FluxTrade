use super::*;
use serde_json::json;

fn identity() -> Identity {
    Identity::new(
        "job-1".into(),
        Window::new(100, 200).unwrap(),
        "grid-10".into(),
        [7; 32],
    )
    .unwrap()
}
fn body(ids: &[u64]) -> Vec<u8> {
    serde_json::to_vec(
        &ids.iter()
            .map(|id| json!({"a":id,"p":"10","q":"1","T":100,"f":id,"l":id,"m":true,"M":true}))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}
fn fixture() -> (Manifest, Vec<Vec<u8>>) {
    let bodies = vec![body(&[0, 1]), body(&[1, 2]), b"[]".to_vec()];
    let states = [
        CheckpointProgress::Next(2),
        CheckpointProgress::Next(3),
        CheckpointProgress::EmptyPage,
    ];
    let entries = bodies
        .iter()
        .zip(states)
        .enumerate()
        .map(|(i, (b, p))| PageEntry::new(i as u64, b, p))
        .collect();
    (Manifest::new(identity(), entries), bodies)
}
fn replay(m: &Manifest, bodies: &[Vec<u8>]) -> Result<Recovered> {
    recover(m, &identity(), |name| {
        let index = m.entries.iter().position(|e| e.name() == name).unwrap();
        Ok(bodies
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("missing page"))?
            .clone())
    })
}

#[test]
fn roundtrip_replay_equals_uninterrupted_and_ignores_orphan() {
    let (manifest, mut bodies) = fixture();
    let restored: Manifest =
        serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap();
    let mut live = Pages::new(Window::new(100, 200).unwrap());
    let mut trades = Vec::new();
    for b in &bodies {
        trades.extend(live.accept(b).unwrap().0);
    }
    bodies.push(b"orphan corrupt tail".to_vec());
    let result = replay(&restored, &bodies).unwrap();
    assert_eq!(result.pages, live);
    assert_eq!(result.trades, trades);
    assert_eq!(result.trades.len(), 3);
    let mut partial = manifest.clone();
    partial.entries.truncate(2);
    assert_eq!(
        replay(&partial, &bodies).unwrap().pages.request().unwrap(),
        Request::Next { from_id: 3 }
    );
    let empty = Manifest::new(identity(), vec![]);
    assert!(
        recover(&empty, &identity(), |_| panic!("unreferenced loader"))
            .unwrap()
            .trades
            .is_empty()
    );
}

#[test]
fn wrong_identity_rejected_before_loading() {
    let (manifest, _) = fixture();
    for (field, value) in [
        ("schema", json!(2)),
        ("algorithm", json!("other")),
        ("job_id", json!("other")),
        ("product_id", json!("BINANCE:BTCUSDT-PERP")),
        ("start_ms", json!(99)),
        ("end_ms", json!(201)),
        ("grid_id", json!("other")),
        ("config_sha256", json!(vec![0; 32])),
    ] {
        let mut wire = serde_json::to_value(&manifest).unwrap();
        wire["identity"][field] = value;
        let bad: Manifest = serde_json::from_value(wire).unwrap();
        assert!(recover(&bad, &identity(), |_| panic!("identity must precede I/O")).is_err());
    }
    for id in ["", "../escape", "a/b", "a\\b", ".", "a b"] {
        assert!(Identity::new(
            id.into(),
            Window::new(1, 2).unwrap(),
            "grid".into(),
            [0; 32]
        )
        .is_err());
    }
}

#[test]
fn corruption_order_and_progress_matrix() {
    let (manifest, bodies) = fixture();
    assert!(replay(&manifest, &bodies[..2]).is_err());
    for changed in [body(&[0, 2]), bodies[0][..bodies[0].len() - 1].to_vec()] {
        let mut bad = bodies.clone();
        bad[0] = changed;
        assert!(replay(&manifest, &bad).is_err());
    }
    for order in [[1, 0, 2], [0, 0, 2], [0, 2, 1]] {
        let mut bad = manifest.clone();
        bad.entries = order.iter().map(|&i| manifest.entries[i].clone()).collect();
        assert!(replay(&bad, &bodies).is_err());
    }
    for state in [
        CheckpointProgress::Next(99),
        CheckpointProgress::WindowEnd,
        CheckpointProgress::EmptyPage,
    ] {
        let mut bad = manifest.clone();
        bad.entries[0].progress = state;
        assert!(replay(&bad, &bodies).is_err());
    }
    let mut bad = manifest.clone();
    bad.entries
        .push(PageEntry::new(3, b"[]", CheckpointProgress::EmptyPage));
    assert!(replay(&bad, &bodies).is_err());
    let mut conflict = bodies.clone();
    let mut row: serde_json::Value = serde_json::from_slice(&conflict[1]).unwrap();
    row[0]["q"] = json!("2");
    conflict[1] = serde_json::to_vec(&row).unwrap();
    let mut bad = manifest;
    bad.entries[1] = PageEntry::new(1, &conflict[1], CheckpointProgress::Next(3));
    assert!(replay(&bad, &conflict).is_err());
}
