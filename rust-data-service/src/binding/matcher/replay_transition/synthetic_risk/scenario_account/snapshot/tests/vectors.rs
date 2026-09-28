use super::*;
use ring::digest::{digest, SHA256};

// Independent protocol oracle: no production Encoding or Decimal formatter.
fn text(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as i64).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}
fn texts(bytes: &mut Vec<u8>, values: &[&str]) {
    for value in values {
        text(bytes, value);
    }
}
fn integer(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn check(payload: Payload, bytes: Vec<u8>) {
    let mut actual = Encoding::new("vector");
    payload.encode(&mut actual).unwrap();
    let mut expected = Vec::new();
    text(&mut expected, "vector");
    expected.extend(bytes.clone());
    assert_eq!(
        actual.finish().as_slice(),
        digest(&SHA256, &expected).as_ref()
    );
    for version in [None, Some(7)] {
        let fact = Fact {
            snapshot_id: "S".into(),
            request_digest: [9; 32],
            kind: payload.kind(),
            captured_account_version: version,
            snapshot_as_of: 600,
            continuation_id: version.map(|_| "Q".into()),
            payload: payload.clone(),
            payload_digest: [0; 32],
        };
        let mut expected = Vec::new();
        texts(
            &mut expected,
            &["SCENARIO_SNAPSHOT_PAYLOAD_V1", "SNAPSHOT", "S"],
        );
        expected.extend([9; 32]);
        let kind = match payload {
            Payload::MarketGolden => "MARKET",
            Payload::Earn(_) => "EARN",
            Payload::Trading(..) => "TRADING",
            Payload::Positions(_) => "POSITIONS",
            Payload::OpenOrders(_) => "OPEN_ORDERS",
            Payload::Failure(k) => match k {
                Kind::Earn => "EARN",
                Kind::Trading => "TRADING",
                Kind::Positions => "POSITIONS",
                Kind::OpenOrders => "OPEN_ORDERS",
                _ => unreachable!(),
            },
        };
        text(&mut expected, kind);
        expected.push(u8::from(version.is_some()));
        if let Some(v) = version {
            integer(&mut expected, v);
        }
        integer(&mut expected, 600);
        expected.push(u8::from(version.is_some()));
        if version.is_some() {
            text(&mut expected, "Q");
        }
        expected.extend(&bytes);
        assert_eq!(
            fact.digest().unwrap().as_slice(),
            digest(&SHA256, &expected).as_ref()
        );
    }
}
#[test]
fn every_payload_variant_has_an_independent_typed_byte_vector() {
    let mut market = Vec::new();
    text(&mut market, "MARKET_SNAPSHOT");
    integer(&mut market, 1);
    texts(
        &mut market,
        &["P_A", "10", "1", "1", "1", "1", "0.1", "live"],
    );
    integer(&mut market, 1);
    check(Payload::MarketGolden, market);
    for (payload, fields) in [
        (
            Payload::Earn(d("0.000")),
            vec!["EARN_SNAPSHOT", "SUCCESS", "0"],
        ),
        (
            Payload::Trading(d("100.00"), d("-2.020")),
            vec!["TRADING_SNAPSHOT", "SUCCESS", "100", "-2.02"],
        ),
    ] {
        let mut b = Vec::new();
        texts(&mut b, &fields);
        check(payload, b);
    }
    for (kind, token) in [
        (Kind::Earn, "EARN_SNAPSHOT"),
        (Kind::Trading, "TRADING_SNAPSHOT"),
        (Kind::Positions, "POSITION_SNAPSHOT"),
        (Kind::OpenOrders, "OPEN_ORDER_SNAPSHOT"),
    ] {
        let mut b = Vec::new();
        texts(&mut b, &[token, "FAILURE", "SYNTHETIC_FAILURE"]);
        check(Payload::Failure(kind), b);
    }
    for empty in [true, false] {
        let mut b = Vec::new();
        texts(&mut b, &["POSITION_SNAPSHOT", "SUCCESS"]);
        integer(&mut b, i64::from(!empty));
        let rows = if empty {
            vec![]
        } else {
            texts(&mut b, &["BTC-USDT-SWAP", "cross", "-3"]);
            b.push(1);
            text(&mut b, "50001");
            b.push(1);
            text(&mut b, "1500.03");
            vec![payload::PositionRow {
                product: "BTC-USDT-SWAP".into(),
                contracts: d("-3.00"),
                mark: d("50001.0"),
                notional: d("1500.030"),
            }]
        };
        check(Payload::Positions(rows), b);
    }
    let (seed, _, _) = fixture();
    for state in 0..3 {
        let mut b = Vec::new();
        texts(&mut b, &["OPEN_ORDER_SNAPSHOT", "SUCCESS"]);
        integer(&mut b, i64::from(state != 0));
        let rows = if state == 0 {
            vec![]
        } else {
            let mut facts = seed.orders[0].clone();
            facts.filled = d(if state == 1 { "0" } else { "1" });
            facts.side = if state == 1 { Side::Long } else { Side::Short };
            texts(
                &mut b,
                &[
                    "O1",
                    "C1",
                    "BTC-USDT-SWAP",
                    if state == 1 {
                        "live"
                    } else {
                        "partially_filled"
                    },
                    if state == 1 { "buy" } else { "sell" },
                    "50000.1",
                    "2",
                    if state == 1 { "0" } else { "1" },
                ],
            );
            integer(&mut b, 600);
            vec![payload::OrderRow {
                facts,
                created_at: 600,
            }]
        };
        check(Payload::OpenOrders(rows), b);
    }
}
