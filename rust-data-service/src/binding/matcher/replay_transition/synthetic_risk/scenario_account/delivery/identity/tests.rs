use super::super::super::tests::{d, fixture};
use super::*;
fn hex(hash: Hash) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}
fn fixture_delivery() -> Delivery {
    let (seed, _, _) = fixture();
    let t = Transport {
        route: "WS",
        operation: "ORDER",
        client: "C".into(),
        order: None,
        code: "0".into(),
        message: None,
        product: None,
        side: None,
        price: None,
        size: None,
    };
    Delivery {
        account: seed.key,
        projection: Projection {
            schema_version: "delivery_projection_v1".into(),
            reference: Reference {
                namespace: "SOURCE",
                fact_id: "A".into(),
            },
            kind: "TRANSPORT_ACK",
            occurrence: 1,
            sequence: 2,
            visible_at: 3,
            continuation: None,
            transport: Some(t.clone()),
        },
        delivery_id: [0; 32],
        payload_digest: [0; 32],
        body: Body::Source(Payload::Transport(t)),
        snapshot_version: None,
        snapshot_as_of: None,
    }
}
#[test]
fn canonical_vectors_and_every_projection_field_are_sensitive() {
    // Independent Python hashlib/struct vectors: UTF-8 text prefixed by >q length;
    // account subaccount absent; all transport options absent; no production encoder.
    let base = fixture_delivery();
    let p = &base.projection;
    assert_eq!(p.validate(), Ok(()));
    assert_eq!(
        hex(p.id(&base.account)),
        "23f5e46def89ec5982d3c6de0a3766bddacab58f44d3767c6a086280faa95619"
    );
    assert_eq!(
        hex(p.request_digest(&base.account)),
        "db7c6ec4352ccdd95c39bf6bceb02cb9cf429fedde1b972fa38e9b36262d35be"
    );
    assert_eq!(
        hex(base.digest().unwrap()),
        "061a140ee2e4cd66ce81f17812276f37855bb9991765ea1586c200b97d3693aa"
    );
    for field in 0..23 {
        let mut x = base.clone();
        let p = &mut x.projection;
        match field {
            0 => x.account.venue.push('x'),
            1 => x.account.environment.push('x'),
            2 => x.account.account.push('x'),
            3 => x.account.subaccount = Some("S".into()),
            4 => p.schema_version.push('x'),
            5 => p.reference.namespace = "SNAPSHOT",
            6 => p.reference.fact_id.push('x'),
            7 => p.kind = "EXECUTION_FACT",
            8 => p.occurrence += 1,
            9 => p.sequence += 1,
            10 => p.visible_at += 1,
            11 => p.continuation = Some("Q".into()),
            12 => p.transport = None,
            n => {
                let t = p.transport.as_mut().unwrap();
                match n {
                    13 => t.route = "REST",
                    14 => t.operation = "CANCEL",
                    15 => t.client.push('x'),
                    16 => t.order = Some("O".into()),
                    17 => t.code.push('x'),
                    18 => t.message = Some("M".into()),
                    19 => t.product = Some("P_A".into()),
                    20 => t.side = Some(Side::Short),
                    21 => t.price = Some(d("1")),
                    _ => t.size = Some(d("2")),
                }
            }
        }
        assert_ne!(
            x.projection.request_digest(&x.account),
            base.projection.request_digest(&base.account)
        );
        assert_eq!(
            x.projection.id(&x.account) != base.projection.id(&base.account),
            matches!(field, 0..=3 | 5..=8)
        );
        if field >= 13 {
            x.body = Body::Source(Payload::Transport(x.projection.transport.clone().unwrap()));
        }
        assert_eq!(x.digest() != base.digest(), !matches!(field, 4 | 12));
    }
    for field in 0..4 {
        let mut x = base.clone();
        match field {
            0 => x.snapshot_version = Some(7),
            1 => x.snapshot_as_of = Some(8),
            2 => x.delivery_id = [1; 32],
            _ => x.payload_digest = [1; 32],
        }
        assert_eq!(x.digest() != base.digest(), field < 2);
    }
}

#[test]
fn present_options_match_independently_computed_vectors() {
    let mut x = fixture_delivery();
    x.account.subaccount = Some("S".into());
    x.projection.continuation = Some("Q".into());
    let t = x.projection.transport.as_mut().unwrap();
    t.order = Some("O".into());
    t.message = Some("M".into());
    t.product = Some("P_A".into());
    t.side = Some(Side::Short);
    t.price = Some(d("1.00"));
    t.size = Some(d("2.0"));
    x.body = Body::Source(Payload::Transport(t.clone()));
    x.snapshot_version = Some(7);
    x.snapshot_as_of = Some(8);
    assert_eq!(
        hex(x.projection.request_digest(&x.account)),
        "a68b838e261e5bba3fdefeb57c96097a14c272e34e29d233596365705776ecfc"
    );
    assert_eq!(
        hex(x.digest().unwrap()),
        "4c8509b5e0ccf86eed354cebd6097f543fb0f96efeba41268e1274517c8176a0"
    );
}
#[test]
fn structural_errors_are_pure_and_times_have_no_owner_clock() {
    let (seed, config, marks) = fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let before = owner.clone();
    for field in 0..10 {
        let mut p = fixture_delivery().projection;
        match field {
            0 => p.schema_version.clear(),
            1 => p.reference.namespace = "BAD",
            2 => p.reference.fact_id = " ".into(),
            3 => p.kind = "BAD",
            4 => p.occurrence = -1,
            5 => p.sequence = -1,
            6 => p.visible_at = -1,
            7 => p.continuation = Some(" ".into()),
            8 => p.kind = "EXECUTION_FACT",
            _ => p.reference.namespace = "LIQUIDATION",
        }
        assert_eq!(p.validate(), Err("INVALID_SCHEMA"));
        assert_eq!(owner, before);
    }
    let mut derived_historical_ack = fixture_delivery().projection;
    derived_historical_ack.transport = None;
    assert_eq!(derived_historical_ack.validate(), Ok(()));
    for time in [0, i64::MAX] {
        let mut p = fixture_delivery().projection;
        p.visible_at = time;
        assert_eq!(p.validate(), Ok(()));
    }
    assert_eq!(owner, before);
}
