use super::*;

#[test]
fn transport_bytes_lock_optional_markers_and_decimal_normalization() {
    let (seed, _, _) = fixture();
    for present in [false, true] {
        let mut t = transport(&seed.orders[0]);
        t.message = present.then(|| "observed".into());
        t.price = present.then(|| d("50000.100"));
        if !present {
            t.order = None;
            t.product = None;
            t.side = None;
            t.size = None;
        }
        let mut bytes = Vec::new();
        let text = |b: &mut Vec<u8>, s: &str| {
            b.extend_from_slice(&(s.len() as i64).to_be_bytes());
            b.extend_from_slice(s.as_bytes());
        };
        for s in ["vector", "TRANSPORT_ACK", "REST", "ORDER", "C1"] {
            text(&mut bytes, s);
        }
        bytes.push(u8::from(present));
        if present {
            text(&mut bytes, "O1");
        }
        text(&mut bytes, "0");
        for s in ["observed", "BTC-USDT-SWAP", "sell", "50000.1", "2"] {
            bytes.push(u8::from(present));
            if present {
                text(&mut bytes, s);
            }
        }
        let mut e = Encoding::new("vector");
        Payload::Transport(t).encode(&mut e).unwrap();
        assert_eq!(
            e.finish().as_slice(),
            ring::digest::digest(&ring::digest::SHA256, &bytes).as_ref()
        );
    }
}
