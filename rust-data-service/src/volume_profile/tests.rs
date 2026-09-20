use super::*;
use rust_decimal_macros::dec;

const SPOT: &str = "BINANCE:BTCUSDT-SPOT";
const PERP: &str = "BINANCE:BTCUSDT-PERP";

fn grid(step: Decimal) -> Grid {
    Grid::new(dec!(0), step, "USDT").unwrap()
}
fn profile(step: Decimal) -> VolumeProfile {
    VolumeProfile::new(SPOT, grid(step), Window::new(100, 200).unwrap()).unwrap()
}

#[test]
fn canonical_identity_does_not_collide() {
    let mut spot = profile(dec!(10));
    let mut perp = VolumeProfile::new(PERP, grid(dec!(10)), spot.window()).unwrap();
    assert_ne!(spot.product_id(), perp.product_id());
    assert_eq!(
        spot.add(PERP, 100, dec!(10), dec!(1)),
        Err(Error::TradeScope)
    );
    assert_eq!(
        perp.add(SPOT, 100, dec!(10), dec!(1)),
        Err(Error::TradeScope)
    );
    spot.add(SPOT, 100, dec!(10), dec!(1)).unwrap();
    perp.add(PERP, 100, dec!(10), dec!(2)).unwrap();
    assert_eq!(spot.totals().base_volume, dec!(1));
    assert_eq!(perp.totals().base_volume, dec!(2));
    assert!(VolumeProfile::new("BINANCE:BTC/USDT-SPOT", grid(dec!(10)), spot.window()).is_err());
}

#[test]
fn half_open_price_and_time_edges() {
    let mut value = profile(dec!(10));
    for (price, expected) in [
        (dec!(0.001), 0),
        (dec!(9.999999999999999999999999999), 0),
        (dec!(10), 1),
        (dec!(20), 2),
    ] {
        assert_eq!(value.grid().index(price).unwrap(), expected);
        value.add(SPOT, 100, price, dec!(1)).unwrap();
    }
    value.add(SPOT, 199, dec!(10), dec!(1)).unwrap();
    for time in [99, 200, i64::MAX] {
        let before = value.clone();
        assert_eq!(
            value.add(SPOT, time, dec!(10), dec!(1)),
            Err(Error::TradeScope)
        );
        assert_eq!(value, before);
    }
    assert_eq!(value.bins()[&0].base_volume, dec!(2));
    assert_eq!(value.grid().edges(1).unwrap(), (dec!(10), dec!(20)));
    let shifted = Grid::new(dec!(100), dec!(10), "USDT").unwrap();
    assert_eq!(shifted.index(dec!(89.99)).unwrap(), -2);
    assert_eq!(shifted.index(dec!(90)).unwrap(), -1);
    assert_eq!(shifted.index(dec!(99.99)).unwrap(), -1);
    assert_eq!(shifted.index(dec!(100)).unwrap(), 0);
}

#[test]
fn empty_sparse_bins_and_poc_ties_use_base_volume() {
    let mut value = profile(dec!(10));
    assert!(value.bins().is_empty());
    assert_eq!(value.totals(), &Volume::default());
    assert_eq!(value.poc().unwrap(), None);
    value.add(SPOT, 100, dec!(101), dec!(2)).unwrap();
    value.add(SPOT, 100, dec!(11), dec!(2)).unwrap();
    assert!(!value.bins().contains_key(&2));
    assert_eq!(
        value.poc().unwrap(),
        Some(PointOfControl {
            index: 1,
            low: dec!(10),
            high_exclusive: dec!(20)
        })
    );
    assert_eq!(value.totals().quote_volume, dec!(224));
}

#[test]
fn batch_chunk_and_coarse_conservation_matrix() {
    let trades = [
        (dec!(1), dec!(0.1)),
        (dec!(49.99), dec!(2)),
        (dec!(50), dec!(3)),
        (dec!(100), dec!(4)),
        (dec!(199.99), dec!(5)),
        (dec!(200), dec!(6)),
    ];
    let mut batch = profile(dec!(10));
    for (p, q) in trades {
        batch.add(SPOT, 100, p, q).unwrap();
    }
    for size in 1..=trades.len() {
        let mut chunked = profile(dec!(10));
        for chunk in trades.chunks(size) {
            for &(p, q) in chunk {
                chunked.add(SPOT, 100, p, q).unwrap();
            }
        }
        assert_eq!(chunked, batch);
    }
    for step in [dec!(10), dec!(50), dec!(100), dec!(200)] {
        let mut direct = profile(step);
        for (p, q) in trades {
            direct.add(SPOT, 100, p, q).unwrap();
        }
        let coarse = batch.coarsen(grid(step)).unwrap();
        assert_eq!(coarse, direct);
        let sum = coarse
            .bins()
            .values()
            .try_fold(Volume::default(), |sum, bin| sum.add(bin))
            .unwrap();
        assert_eq!(&sum, coarse.totals());
        assert_eq!(sum.base_volume, dec!(20.1));
        assert_eq!(sum.quote_volume, dec!(2850.03));
        assert_eq!(sum.aggregate_count, 6);
    }
}

#[test]
fn invalid_inputs_and_unaligned_grids_are_rejected() {
    for step in [dec!(0), dec!(-1)] {
        assert!(Grid::new(dec!(0), step, "USDT").is_err());
    }
    for unit in ["", "usdt", "USD T"] {
        assert!(Grid::new(dec!(0), dec!(10), unit).is_err());
    }
    for (start, end) in [(-1, 1), (0, 0), (2, 1)] {
        assert!(Window::new(start, end).is_err());
    }
    let mut value = profile(dec!(10));
    for (p, q) in [
        (dec!(0), dec!(1)),
        (dec!(-1), dec!(1)),
        (dec!(1), dec!(0)),
        (dec!(1), dec!(-1)),
    ] {
        assert_eq!(value.add(SPOT, 100, p, q), Err(Error::TradeValue));
        assert!(value.bins().is_empty());
    }
    for invalid in [
        grid(dec!(5)),
        grid(dec!(15)),
        Grid::new(dec!(1), dec!(50), "USDT").unwrap(),
        Grid::new(dec!(0), dec!(50), "USD").unwrap(),
    ] {
        assert_eq!(value.coarsen(invalid), Err(Error::Grid));
    }
}

#[test]
fn decimal_limits_fail_atomically_without_rounding() {
    let mut value = profile(dec!(10));
    value.add(SPOT, 100, dec!(1), dec!(1)).unwrap();
    let before = value.clone();
    for (p, q) in [
        (dec!(0.0000000000000000000000000001), dec!(0.1)),
        (dec!(2), Decimal::MAX),
        (dec!(1), Decimal::MAX),
    ] {
        assert_eq!(value.add(SPOT, 100, p, q), Err(Error::Arithmetic));
        assert_eq!(value, before);
    }
    assert_eq!(exact::add(Decimal::MAX, dec!(0.1)), Err(Error::Arithmetic));
    assert_eq!(
        exact::mul(dec!(0.00000000000001), dec!(0.00000000000001)).unwrap(),
        dec!(0.0000000000000000000000000001)
    );
    assert_eq!(exact::add(Decimal::MAX, dec!(0)).unwrap(), Decimal::MAX);
    assert_eq!(exact::mul(Decimal::MAX, dec!(1)).unwrap(), Decimal::MAX);
    assert_eq!(grid(dec!(1)).index(Decimal::MAX), Err(Error::Arithmetic));
    assert_eq!(
        Grid::new(Decimal::MAX, dec!(1), "USDT").unwrap().edges(0),
        Err(Error::Arithmetic)
    );
    let max_count = Volume {
        aggregate_count: u64::MAX,
        ..Volume::default()
    };
    assert_eq!(
        max_count.add(&Volume {
            aggregate_count: 1,
            ..Volume::default()
        }),
        Err(Error::Arithmetic)
    );
}

#[test]
fn negative_indices_coarsen_by_floor_not_truncation() {
    let fine = Grid::new(dec!(100), dec!(10), "USDT").unwrap();
    let coarse = Grid::new(dec!(100), dec!(50), "USDT").unwrap();
    let window = Window::new(100, 200).unwrap();
    let mut value = VolumeProfile::new(SPOT, fine, window).unwrap();
    let mut direct = VolumeProfile::new(SPOT, coarse.clone(), window).unwrap();
    for p in [dec!(1), dec!(49), dec!(50), dec!(99), dec!(100)] {
        value.add(SPOT, 100, p, dec!(1)).unwrap();
        direct.add(SPOT, 100, p, dec!(1)).unwrap();
    }
    assert_eq!(value.coarsen(coarse).unwrap(), direct);
}

#[test]
fn totals_overflow_does_not_insert_a_new_bin() {
    let mut value = profile(dec!(10));
    value.add(SPOT, 100, dec!(1), Decimal::MAX).unwrap();
    let before = value.clone();
    assert_eq!(
        value.add(SPOT, 100, dec!(11), dec!(1)),
        Err(Error::Arithmetic)
    );
    assert_eq!(value, before);
    assert!(!value.bins().contains_key(&1));
}

#[test]
fn later_trades_do_not_mutate_an_observed_prefix() {
    let mut value = profile(dec!(10));
    value.add(SPOT, 100, dec!(11), dec!(1)).unwrap();
    let prefix = value.clone();
    value.add(SPOT, 199, dec!(101), dec!(3)).unwrap();
    assert_eq!(prefix.poc().unwrap().unwrap().index, 1);
    assert_eq!(prefix.totals().aggregate_count, 1);
    assert_eq!(value.poc().unwrap().unwrap().index, 10);
    assert_eq!(value.totals().aggregate_count, 2);
}

#[test]
fn representable_edges_survive_out_of_range_intermediate_products() {
    let origin = dec!(-79228162514264337593543950335);
    let step = dec!(47536897508558602556126370201);
    let low = dec!(15845632502852867518708790067);
    let high = dec!(63382530011411470074835160268);
    let coarse = Grid::new(origin, step, "USDT").unwrap();
    assert_eq!(coarse.edges(2).unwrap(), (low, high));
    let window = Window::new(100, 200).unwrap();
    let mut direct = VolumeProfile::new(SPOT, coarse.clone(), window).unwrap();
    direct.add(SPOT, 100, low, dec!(1)).unwrap();
    assert_eq!(direct.poc().unwrap().unwrap().index, 2);
    assert_eq!(direct.totals().quote_volume, low);

    let fine = Grid::new(origin, low, "USDT").unwrap();
    let mut source = VolumeProfile::new(SPOT, fine, window).unwrap();
    source.add(SPOT, 100, low, dec!(1)).unwrap();
    assert_eq!(source.coarsen(coarse).unwrap(), direct);

    // The next bin's final upper boundary genuinely exceeds Decimal::MAX.
    let before = direct.clone();
    assert_eq!(direct.grid().edges(3), Err(Error::Arithmetic));
    assert_eq!(direct.add(SPOT, 100, high, dec!(1)), Err(Error::Arithmetic));
    assert_eq!(direct, before);
}
