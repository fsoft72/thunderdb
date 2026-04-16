//! Benchmark harness shared library.
//!
//! See docs/superpowers/specs/2026-04-16-benchmark-harness-design.md.

#![allow(dead_code)]

pub mod verdict;
pub mod fairness;
pub mod cache;
pub mod report;
pub mod baseline;
pub mod fixtures;
pub mod scenario;
pub mod runner;

pub use verdict::{Verdict, classify_ratio};
pub use fairness::{Tier, Durability, CacheState, HarnessConfig};
pub use fixtures::{Fixtures, FIXTURE_SEED, USER_COUNT, TOPICS, build_blog_fixtures, drop_fixtures,
                   comments_for_post, total_comments};
pub use report::{BenchResult, EngineTiming, HarnessReport, CellReport, Summary};
pub use baseline::{load_baseline, save_baseline, index_baseline, delta_pct, BaselineKey};
pub use scenario::{Scenario, ScenarioBuilder};
pub use runner::Harness;
