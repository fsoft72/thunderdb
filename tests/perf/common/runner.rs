//! Harness driver: setup → warmup → samples → verdict.

use crate::common::fairness::{HarnessConfig, Tier, Durability, CacheState};
use crate::common::fixtures::{Fixtures, drop_fixtures};
use crate::common::report::{BenchResult, EngineTiming, CellReport, HarnessReport, Summary};
use crate::common::scenario::Scenario;
use crate::common::verdict::{Verdict, classify_ratio};
use std::panic::AssertUnwindSafe;
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

impl Harness {
    /// Run one scenario in one (tier, mode, cache) cell. Returns a BenchResult.
    pub(crate) fn run_one(
        &self, scenario: &Scenario,
        tier: Tier, mode: Durability, cache: CacheState,
    ) -> BenchResult {
        // DURABLE is stubbed — Thunder returns Unsupported.
        if mode == Durability::Durable {
            return BenchResult {
                scenario: scenario.name.into(), group: scenario.group.into(),
                thunder: None, sqlite: None, ratio: None,
                verdict: Verdict::Unsupported,
            };
        }

        // Build fixtures
        let fixtures_result = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.setup)(tier, mode)));
        let mut fixtures = match fixtures_result {
            Ok(f) => f,
            Err(_) => return BenchResult {
                scenario: scenario.name.into(), group: scenario.group.into(),
                thunder: None, sqlite: None, ratio: None,
                verdict: Verdict::Failure("setup panicked".into()),
            },
        };

        // Warmup: 3 iterations, results discarded
        for _ in 0..3 {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.thunder)(&mut fixtures)));
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.sqlite)(&fixtures)));
        }

        // Sample Thunder
        let samples = self.config.sample_count;
        let thunder_panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            Harness::collect_samples(samples, || (scenario.thunder)(&mut fixtures))
        }));
        let thunder = match thunder_panic {
            Ok((median, p95, out)) => Some(EngineTiming {
                median_ns: median, p95_ns: p95, sample_count: samples, dropped_outliers: out
            }),
            Err(_) => None,
        };

        // Sample SQLite
        let sqlite_panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            Harness::collect_samples(samples, || (scenario.sqlite)(&fixtures))
        }));
        let sqlite = match sqlite_panic {
            Ok((median, p95, out)) => Some(EngineTiming {
                median_ns: median, p95_ns: p95, sample_count: samples, dropped_outliers: out
            }),
            Err(_) => None,
        };

        // Correctness check
        let assert_result = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.assert)(&mut fixtures)));

        let verdict = match (&thunder, &sqlite, &assert_result) {
            (_, _, Err(_)) => Verdict::Failure("assert panicked".into()),
            (_, _, Ok(Err(msg))) => Verdict::Failure(msg.clone()),
            (None, _, _) => Verdict::Failure("thunder panicked".into()),
            (_, None, _) => Verdict::Failure("sqlite panicked".into()),
            (Some(t), Some(s), _) => {
                let ratio = t.median_ns as f64 / s.median_ns as f64;
                classify_ratio(ratio)
            }
        };

        let ratio = thunder.as_ref().zip(sqlite.as_ref())
            .map(|(t, s)| t.median_ns as f64 / s.median_ns as f64);

        drop_fixtures(fixtures);
        let _ = cache;  // COLD wiring added in Task 16

        BenchResult {
            scenario: scenario.name.into(), group: scenario.group.into(),
            thunder, sqlite, ratio, verdict,
        }
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

    use crate::common::scenario::Scenario;
    use crate::common::fixtures::build_blog_fixtures;
    use crate::common::verdict::Verdict;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn run_one_tie_when_equal_timing() {
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
            cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
        }};
        let s = Scenario::new("equal_sleep", "test")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|_f| sleep(Duration::from_millis(2)))
            .sqlite(|_f| sleep(Duration::from_millis(2)))
            .assert(|_f| Ok(()))
            .build();
        let r = h.run_one(&s, Tier::Small, Durability::Fast, CacheState::Warm);
        assert_eq!(r.verdict, Verdict::Tie);
    }

    #[test]
    fn run_one_win_when_thunder_faster() {
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
            cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
        }};
        let s = Scenario::new("thunder_faster", "test")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|_f| sleep(Duration::from_millis(1)))
            .sqlite(|_f| sleep(Duration::from_millis(10)))
            .assert(|_f| Ok(()))
            .build();
        let r = h.run_one(&s, Tier::Small, Durability::Fast, CacheState::Warm);
        assert_eq!(r.verdict, Verdict::Win);
    }
}
