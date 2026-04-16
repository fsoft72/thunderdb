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

        // Sample Thunder (reopen between samples if COLD)
        let samples = self.config.sample_count;
        let thunder_panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut timings = Vec::with_capacity(samples);
            for _ in 0..samples {
                if cache == CacheState::Cold {
                    let _ = crate::common::fixtures::reopen_handles(&mut fixtures);
                }
                let t0 = Instant::now();
                (scenario.thunder)(&mut fixtures);
                timings.push(t0.elapsed().as_nanos());
            }
            Harness::reduce(timings)
        }));
        let thunder = match thunder_panic {
            Ok((median, p95, out)) => Some(EngineTiming {
                median_ns: median, p95_ns: p95, sample_count: samples, dropped_outliers: out
            }),
            Err(_) => None,
        };

        // Sample SQLite (reopen between samples if COLD)
        let sqlite_panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut timings = Vec::with_capacity(samples);
            for _ in 0..samples {
                if cache == CacheState::Cold {
                    let _ = crate::common::fixtures::reopen_handles(&mut fixtures);
                }
                let t0 = Instant::now();
                (scenario.sqlite)(&fixtures);
                timings.push(t0.elapsed().as_nanos());
            }
            Harness::reduce(timings)
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

        BenchResult {
            scenario: scenario.name.into(), group: scenario.group.into(),
            thunder, sqlite, ratio, verdict,
        }
    }
}

impl Harness {
    /// Run all scenarios across the full (tier × mode × cache) matrix and return a HarnessReport.
    pub fn run_scenarios(&self, scenarios: &[Scenario]) -> HarnessReport {
        let mut cells = Vec::new();
        for tier in &self.config.tiers {
            for mode in &self.config.durabilities {
                for cache in &self.config.cache_states {
                    let mut results = Vec::new();
                    for s in scenarios {
                        results.push(self.run_one(s, *tier, *mode, *cache));
                    }
                    cells.push(CellReport { tier: *tier, mode: *mode, cache: *cache, results });
                }
            }
        }
        let mut report = HarnessReport {
            schema_version: 1,
            git_sha: collect_git_sha(),
            rustc_version: collect_rustc_version(),
            started_at: chrono_like_timestamp(),
            cells,
            summary: Summary { win: 0, tie: 0, loss: 0, unsupported: 0, failure: 0 },
        };
        report.compute_summary();
        report
    }
}

impl Harness {
    /// End-to-end: run all scenarios, load baseline if present, print scoreboard,
    /// optionally promote baseline, write JSON artifact, return report.
    pub fn run(&self, scenarios: &[Scenario], baseline_path: &std::path::Path, artifact_dir: &std::path::Path) -> HarnessReport {
        let report = self.run_scenarios(scenarios);
        let baseline = crate::common::baseline::load_baseline(baseline_path).ok().flatten();
        let idx = baseline.as_ref().map(|b| crate::common::baseline::index_baseline(b));
        println!("{}", report.to_terminal_with_baseline(idx.as_ref()));
        let _ = report.write_to(artifact_dir);
        if self.config.update_baseline {
            if report.summary.failure > 0 {
                eprintln!("--update-baseline refused: run contains Failures");
            } else {
                crate::common::baseline::save_baseline(&report, baseline_path).expect("save baseline");
                eprintln!("Baseline promoted: {}", baseline_path.display());
            }
        }
        report
    }
}

/// Retrieve the current git commit SHA via a subprocess call.
fn collect_git_sha() -> String {
    std::process::Command::new("git").args(["rev-parse", "HEAD"]).output()
        .ok().and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// Retrieve the rustc version string via a subprocess call.
fn collect_rustc_version() -> String {
    std::process::Command::new("rustc").arg("--version").output()
        .ok().and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// Return a unix-epoch-based timestamp string (no chrono dependency needed).
fn chrono_like_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    format!("unix:{}", secs)
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

    #[test]
    fn run_one_failure_when_thunder_panics() {
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
            cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
        }};
        let s = Scenario::new("crash", "test")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|_f| panic!("boom"))
            .sqlite(|_f| {})
            .assert(|_f| Ok(()))
            .build();
        let r = h.run_one(&s, Tier::Small, Durability::Fast, CacheState::Warm);
        assert!(matches!(r.verdict, Verdict::Failure(_)), "got {:?}", r.verdict);
    }

    #[test]
    fn run_one_failure_when_assert_disagrees() {
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
            cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
        }};
        let s = Scenario::new("wrong_answer", "test")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|_f| {})
            .sqlite(|_f| {})
            .assert(|_f| Err("mismatch".into()))
            .build();
        let r = h.run_one(&s, Tier::Small, Durability::Fast, CacheState::Warm);
        assert!(matches!(r.verdict, Verdict::Failure(ref m) if m == "mismatch"), "got {:?}", r.verdict);
    }

    #[test]
    fn run_one_durable_is_unsupported() {
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Durable],
            cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
        }};
        let s = Scenario::new("any", "test")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|_f| {})
            .sqlite(|_f| {})
            .assert(|_f| Ok(()))
            .build();
        let r = h.run_one(&s, Tier::Small, Durability::Durable, CacheState::Warm);
        assert_eq!(r.verdict, Verdict::Unsupported);
    }

    #[test]
    fn cold_cache_reopens_between_samples() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
            cache_states: vec![CacheState::Cold], sample_count: 3, update_baseline: false,
        }};
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let s = Scenario::new("cold_probe", "test")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(move |_f| { calls2.fetch_add(1, Ordering::SeqCst); })
            .sqlite(|_f| {})
            .assert(|_f| Ok(()))
            .build();
        let r = h.run_one(&s, Tier::Small, Durability::Fast, CacheState::Cold);
        // 3 warmup + 3 sample calls to thunder = 6
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        assert!(!matches!(r.verdict, Verdict::Failure(_) | Verdict::Unsupported),
            "unexpected verdict {:?}", r.verdict);
    }

    #[test]
    fn run_scenarios_iterates_matrix() {
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small],
            durabilities: vec![Durability::Fast, Durability::Durable],
            cache_states: vec![CacheState::Warm],
            sample_count: 3, update_baseline: false,
        }};
        let s = vec![
            Scenario::new("a", "read")
                .setup(|t, m| build_blog_fixtures(t, m))
                .thunder(|_f| {}).sqlite(|_f| {}).assert(|_f| Ok(())).build(),
        ];
        let r = h.run_scenarios(&s);
        assert_eq!(r.cells.len(), 2);
        assert_eq!(r.cells[0].results.len(), 1);
        assert_eq!(r.summary.unsupported, 1);
    }

    #[test]
    fn run_end_to_end_writes_artifact() {
        let dir = std::env::temp_dir().join("thunderdb_run_e2e_test");
        let _ = std::fs::remove_dir_all(&dir);
        let baseline_path = dir.join("baseline.json");
        let artifact_dir = dir.join("artifacts");
        let h = Harness { config: HarnessConfig {
            tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
            cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
        }};
        let s = vec![
            Scenario::new("e2e", "read")
                .setup(|t, m| build_blog_fixtures(t, m))
                .thunder(|_f| {}).sqlite(|_f| {}).assert(|_f| Ok(())).build(),
        ];
        let report = h.run(&s, &baseline_path, &artifact_dir);
        assert_eq!(report.cells.len(), 1);
        let entries: Vec<_> = std::fs::read_dir(&artifact_dir).unwrap()
            .filter_map(|e| e.ok()).collect();
        assert_eq!(entries.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
