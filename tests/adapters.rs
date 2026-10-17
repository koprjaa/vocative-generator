// Project: vocative-generator
// File: adapters.rs
// Description: Equivalents of all adaptive band, movement, interval and count tests.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use std::time::{Duration, Instant};
use vocative_generator::adapters::*;
fn counter(initial: f64) -> AdaptiveValue {
    let mut v = AdaptiveValue::new(initial, 1.0, 100.0, &WORKER_BANDS, "Test", true);
    v.interval = Duration::ZERO;
    v
}
#[test]
fn all_band_boundaries() {
    for (bands, rates, factors) in [
        (
            &DELAY_BANDS,
            [1.0, 0.95, 0.94, 0.8, 0.79, 0.0],
            [0.85, 0.85, 1.0, 1.0, 1.8, 1.8],
        ),
        (
            &WORKER_BANDS,
            [1.0, 0.9, 0.89, 0.8, 0.79, 0.0],
            [1.1, 1.1, 1.0, 1.0, 0.8, 0.8],
        ),
        (
            &BATCH_BANDS,
            [1.0, 0.95, 0.94, 0.75, 0.74, 0.0],
            [1.1, 1.1, 1.0, 1.0, 0.8, 0.8],
        ),
    ] {
        let mut value = counter(10.0);
        value.bands = bands.to_vec();
        for (r, f) in rates.into_iter().zip(factors) {
            assert_eq!(value.factor_for(r), f);
        }
        assert_eq!(bands.last().unwrap().0, 0.0);
        assert!(bands.windows(2).all(|w| w[0].0 >= w[1].0));
        assert!(bands.iter().any(|(_, f)| *f == 1.0));
    }
}
#[test]
fn movement() {
    for (initial, rate, expected) in [
        (30.0, 1.0, 33.0),
        (30.0, 0.0, 24.0),
        (30.0, 0.85, 30.0),
        (99.0, 1.0, 100.0),
        (5.0, 1.0, 6.0),
        (3.0, 0.0, 2.0),
    ] {
        let mut v = counter(initial);
        v.adjust(Some(rate));
        assert_eq!(v.value, expected);
    }
    let mut v = counter(2.0);
    for _ in 0..5 {
        v.adjust(Some(0.0));
    }
    assert_eq!(v.value, 1.0);
    let mut d = make_delay(1.0, 0.1, 5.0);
    d.interval = Duration::ZERO;
    d.adjust(Some(1.0));
    assert!((d.value - 0.85).abs() < 1e-12);
}
#[test]
fn interval_gate_and_reset() {
    let mut v = counter(30.0);
    v.interval = Duration::from_secs(3600);
    v.adjust(Some(1.0));
    assert_eq!(v.value, 30.0);
    v.last_adjustment = Instant::now() - Duration::from_secs(7200);
    v.adjust(Some(1.0));
    v.adjust(Some(1.0));
    assert_eq!(v.value, 33.0);
}
#[test]
fn recorded_counts() {
    let mut v = counter(30.0);
    for _ in 0..19 {
        v.record(true);
    }
    v.record(false);
    v.adjust(None);
    assert_eq!(v.value, 33.0);
    assert_eq!((v.successes, v.errors), (0, 0));
    let mut v = counter(30.0);
    for _ in 0..10 {
        v.record(false);
    }
    v.adjust(None);
    assert_eq!(v.value, 24.0);
    let mut v = counter(30.0);
    let before = v.last_adjustment;
    v.adjust(None);
    assert_eq!(v.value, 30.0);
    assert_eq!(v.last_adjustment, before);
    for _ in 0..10 {
        v.record(false);
    }
    v.adjust(Some(1.0));
    assert_eq!(v.value, 33.0);
}
#[test]
fn factory_types_bounds_and_opposite_directions() {
    let mut delay = make_delay(0.5, 0.01, 5.0);
    assert!(!delay.integer);
    assert_eq!((delay.minimum, delay.maximum), (0.01, 5.0));
    let mut workers = make_workers(30, 10, 100);
    assert!(workers.integer);
    assert!(make_batch_size(30, 10, 100).integer);
    delay.interval = Duration::ZERO;
    workers.interval = Duration::ZERO;
    delay.adjust(Some(1.0));
    workers.adjust(Some(1.0));
    assert!(delay.value < 0.5);
    assert!(workers.value > 30.0);
}
