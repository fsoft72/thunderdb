//! Benchmark harness shared library.
//!
//! See `docs/superpowers/specs/2026-04-16-benchmark-harness-design.md`.

#![allow(dead_code)] // submodules fill in incrementally

pub mod verdict;
pub use verdict::{classify_ratio, Verdict};

pub mod fairness;
pub use fairness::{Tier, Durability, CacheState, HarnessConfig};

pub mod cache;

pub mod report;
pub use report::{BenchResult, EngineTiming, HarnessReport, CellReport, Summary};

pub mod baseline;
pub use baseline::{load_baseline, save_baseline, index_baseline, delta_pct, BaselineKey};

pub mod fixtures;
pub use fixtures::{Fixtures, FIXTURE_SEED, USER_COUNT, TOPICS, build_blog_fixtures, drop_fixtures, comments_for_post, total_comments};

pub mod scenario;
pub use scenario::{Scenario, ScenarioBuilder};
