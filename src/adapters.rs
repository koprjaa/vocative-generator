// Project: vocative-generator
// File: adapters.rs
// Description: Interval gated adaptive delay and batch sizing.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use crate::log;
use std::time::{Duration, Instant};
pub const DELAY_BANDS: [(f64, f64); 3] = [(0.95, 0.85), (0.8, 1.0), (0.0, 1.8)];
pub const WORKER_BANDS: [(f64, f64); 3] = [(0.9, 1.1), (0.8, 1.0), (0.0, 0.8)];
pub const BATCH_BANDS: [(f64, f64); 3] = [(0.95, 1.1), (0.75, 1.0), (0.0, 0.8)];
#[derive(Clone)]
pub struct AdaptiveValue {
    pub value: f64,
    pub minimum: f64,
    pub maximum: f64,
    pub bands: Vec<(f64, f64)>,
    pub name: String,
    pub integer: bool,
    pub interval: Duration,
    pub last_adjustment: Instant,
    pub successes: usize,
    pub errors: usize,
}
impl AdaptiveValue {
    pub fn new(
        initial: f64,
        minimum: f64,
        maximum: f64,
        bands: &[(f64, f64)],
        name: &str,
        integer: bool,
    ) -> Self {
        Self {
            value: initial,
            minimum,
            maximum,
            bands: bands.into(),
            name: name.into(),
            integer,
            interval: Duration::from_secs(3),
            last_adjustment: Instant::now(),
            successes: 0,
            errors: 0,
        }
    }
    pub fn record(&mut self, success: bool) {
        if success {
            self.successes += 1;
        } else {
            self.errors += 1;
        }
    }
    pub fn factor_for(&self, rate: f64) -> f64 {
        self.bands
            .iter()
            .find(|(t, _)| rate >= *t)
            .map(|(_, f)| *f)
            .unwrap_or(1.0)
    }
    pub fn adjust(&mut self, rate: Option<f64>) {
        if self.last_adjustment.elapsed() < self.interval {
            return;
        }
        let rate = match rate {
            Some(r) => r,
            None if self.successes + self.errors > 0 => {
                self.successes as f64 / (self.successes + self.errors) as f64
            }
            _ => return,
        };
        let old = self.value;
        let factor = self.factor_for(rate);
        if factor != 1.0 {
            let mut next = old * factor;
            if self.integer {
                next = next.trunc();
                if next == old.trunc() {
                    next = old.trunc() + if factor > 1.0 { 1.0 } else { -1.0 };
                }
            }
            self.value = next.clamp(self.minimum, self.maximum);
        }
        self.successes = 0;
        self.errors = 0;
        self.last_adjustment = Instant::now();
        if old != self.value {
            let format = |v: f64| {
                if self.integer {
                    format!("{v:.0}")
                } else {
                    format!("{v:.3}")
                }
            };
            log(
                "AdaptiveValue",
                "INFO",
                format!(
                    "{} adjusted: {} -> {} (success: {:.1}%)",
                    self.name,
                    format(old),
                    format(self.value),
                    rate * 100.0
                ),
            );
        }
    }
}
pub fn make_delay(initial: f64, min: f64, max: f64) -> AdaptiveValue {
    AdaptiveValue::new(initial, min, max, &DELAY_BANDS, "Delay", false)
}
pub fn make_workers(initial: usize, min: usize, max: usize) -> AdaptiveValue {
    AdaptiveValue::new(
        initial as f64,
        min as f64,
        max as f64,
        &WORKER_BANDS,
        "Workers",
        true,
    )
}
pub fn make_batch_size(initial: usize, min: usize, max: usize) -> AdaptiveValue {
    AdaptiveValue::new(
        initial as f64,
        min as f64,
        max as f64,
        &BATCH_BANDS,
        "Batch size",
        true,
    )
}
