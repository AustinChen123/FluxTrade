use super::*;
use serde_json::{json, Value};

fn trade(id: u64, time: i64) -> Value {
    json!({"a":id,"p":"10.25","q":"0.2","T":time,"f":id,"l":id,"m":true,"M":true})
}
fn page(rows: Vec<Value>) -> Vec<u8> {
    serde_json::to_vec(&rows).unwrap()
}
fn pages() -> Pages {
    Pages::new(Window::new(100, 200).unwrap())
}

#[test]
fn inclusive_requests_preserve_same_ms_and_crop_half_open_end() {
    let mut p = pages();
    assert_eq!(
        p.request().unwrap(),
        Request::First {
            start_time: 100,
            end_time: 199
        }
    );
    let (rows, next) = p.accept(&page(vec![trade(0, 100), trade(1, 100)])).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].price.to_string(), "10.25");
    assert_eq!(rows[0].quantity.to_string(), "0.2");
    assert_eq!(next, Progress::Continue(Request::Next { from_id: 2 }));
    assert_eq!(p.request().unwrap(), Request::Next { from_id: 2 });
    let (rows, end) = p
        .accept(&page(vec![
            trade(1, 100),
            trade(2, 100),
            trade(3, 199),
            trade(4, 200),
            trade(5, 201),
        ]))
        .unwrap();
    assert_eq!(rows.iter().map(|t| t.id).collect::<Vec<_>>(), vec![2, 3]);
    assert_eq!(end, Progress::WindowEnd);
    assert!(p.request().is_err());
    assert!(p.accept(b"[]").is_err());
}

#[test]
fn full_short_empty_and_boundary_only_pages() {
    let mut p = pages();
    let (_, next) = p
        .accept(&page((0..1000).map(|id| trade(id, 100)).collect()))
        .unwrap();
    assert_eq!(next, Progress::Continue(Request::Next { from_id: 1000 }));
    assert_eq!(p.accept(b"[]").unwrap(), (vec![], Progress::EmptyPage));
    assert_eq!(pages().accept(b"[]").unwrap().1, Progress::EmptyPage);
    assert_eq!(
        pages().accept(&page(vec![trade(u64::MAX, 200)])).unwrap(),
        (vec![], Progress::WindowEnd)
    );
    assert!(pages()
        .accept(&page((0..1001).map(|id| trade(id, 100)).collect()))
        .is_err());
}

#[test]
fn invalid_pages_never_advance_or_partially_apply() {
    let mut p = pages();
    p.accept(&page(vec![trade(10, 100), trade(11, 101)]))
        .unwrap();
    let mut conflict = trade(11, 101);
    conflict["q"] = json!("0.3");
    for rows in [
        vec![conflict, trade(12, 101)],
        vec![trade(13, 102)],
        vec![trade(12, 100)],
        vec![trade(11, 101)],
        vec![trade(12, 101), trade(12, 101)],
        vec![trade(12, 102), trade(11, 101)],
        vec![trade(12, 99)],
        vec![trade(12, 101), trade(14, 102)],
    ] {
        let before = p.clone();
        assert!(p.accept(&page(rows)).is_err());
        assert_eq!(p, before);
    }
    assert!(pages().accept(&page(vec![trade(u64::MAX, 100)])).is_err());
}

#[test]
fn malformed_and_nonpositive_wire_values_fail_closed() {
    for field in ["a", "p", "q", "T", "f", "l", "m", "M"] {
        let mut row = trade(0, 100);
        row.as_object_mut().unwrap().remove(field);
        assert!(pages().accept(&page(vec![row])).is_err());
    }
    for field in ["p", "q"] {
        for value in [
            json!(0),
            json!(true),
            Value::Null,
            json!(""),
            json!("0"),
            json!("-1"),
            json!("NaN"),
            json!("1e2"),
            json!("1_0"),
            json!("1.2.3"),
            json!("0.00000000000000000000000000001"),
            json!("79228162514264337593543950336"),
        ] {
            let mut row = trade(0, 100);
            row[field] = value;
            assert!(pages().accept(&page(vec![row])).is_err());
        }
    }
    for (field, value) in [
        ("a", json!(-1)),
        ("T", json!(-1)),
        ("f", json!(2)),
        ("m", json!(1)),
    ] {
        let mut row = trade(0, 100);
        row[field] = value;
        assert!(pages().accept(&page(vec![row])).is_err());
    }
    for body in [b"{".as_slice(), b"null", b"{}"] {
        assert!(pages().accept(body).is_err());
    }
}
