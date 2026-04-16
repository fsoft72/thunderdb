//! Harness driver: setup → warmup → samples → verdict.

use crate::common::fairness::HarnessConfig;
use std::time::Instant;

pub struct Harness {
    pub config: HarnessConfig,
}

impl Harness {
    /// Build a [`Harness`] by reading configuration from environment variables and CLI args.
    pub fn from_env() -> Self {
        let args: Vec<String> = std::env::args().collect();
        Self { config: HarnessConfig::from_env_and_args(&args) }
    }

    /// Collect timings from N iterations of the closure.
    /// Returns (median_ns, p95_ns, outlier_count) after dropping min+max.
    pub(crate) fn collect_samples<F: FnMut()>(samples: usize, mut f: F) -> (u128, u128, usize) {
        let mut timings: Vec<u128> = Vec::with_capacity(samples);
        for _ in 0..samples {
            let t0 = Instant::now();
            f();
            timings.push(t0.elapsed().as_nanos());
        }
        Self::reduce(timings)
    }

    /// Reduce a raw timing vector to (median_ns, p95_ns, outlier_count).
    ///
    /// The inner slice (min and max dropped) is used for the median when n >= 3.
    /// Outliers are defined as samples more than 5× the median.
    pub(crate) fn reduce(mut timings: Vec<u128>) -> (u128, u128, usize) {
        let n = timings.len();
        timings.sort_unstable();
        // p95 before dropping
        let p95 = timings[((n as f64) * 0.95).ceil() as usize - 1];
        let median = if n >= 3 {
            let inner = &timings[1..n - 1];
            inner[inner.len() / 2]
        } else {
            timings[n / 2]
        };
        // Count outliers >5× median
        let outliers = timings.iter().filter(|&&t| t > median.saturating_mul(5)).count();
        (median, p95, outliers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reduce_11_samples_drops_min_max() {
        let samples: Vec<u128> = vec![1, 100, 100, 100, 100, 100, 100, 100, 100, 100, 1000];
        let (median, _p95, _out) = Harness::reduce(samples);
        assert_eq!(median, 100);
    }

    #[test]
    fn reduce_detects_outlier() {
        let samples: Vec<u128> = vec![10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 100];
        let (_median, _p95, out) = Harness::reduce(samples);
        assert_eq!(out, 1);
    }

    #[test]
    fn reduce_p95() {
        let samples: Vec<u128> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let (_median, p95, _out) = Harness::reduce(samples);
        assert_eq!(p95, 11);
    }
}
