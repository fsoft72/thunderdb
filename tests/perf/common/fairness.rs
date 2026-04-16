//! Fairness dimensions: Tier × Durability × CacheState.

use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier { Small, Medium, Large }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Durability { Fast, Durable }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState { Warm, Cold }

impl Tier {
    pub fn post_count(self) -> usize {
        match self {
            Tier::Small => 10_000,
            Tier::Medium => 100_000,
            Tier::Large => 1_000_000,
        }
    }

    pub fn label(self) -> &'static str {
        match self { Tier::Small => "SMALL", Tier::Medium => "MEDIUM", Tier::Large => "LARGE" }
    }

    pub fn parse_set(s: &str) -> Vec<Tier> {
        match s {
            "medium" => vec![Tier::Medium],
            "large" => vec![Tier::Large],
            "all" => vec![Tier::Small, Tier::Medium, Tier::Large],
            _ => vec![Tier::Small],
        }
    }
}

impl Durability {
    pub fn label(self) -> &'static str {
        match self { Durability::Fast => "FAST", Durability::Durable => "DURABLE" }
    }

    pub fn parse_set(s: &str) -> Vec<Durability> {
        match s {
            "durable" => vec![Durability::Durable],
            "both" => vec![Durability::Fast, Durability::Durable],
            _ => vec![Durability::Fast],
        }
    }
}

impl CacheState {
    pub fn label(self) -> &'static str {
        match self { CacheState::Warm => "WARM", CacheState::Cold => "COLD" }
    }

    pub fn parse_set(s: &str) -> Vec<CacheState> {
        match s {
            "cold" => vec![CacheState::Cold],
            "both" => vec![CacheState::Warm, CacheState::Cold],
            _ => vec![CacheState::Warm],
        }
    }
}

/// Full combination of dimensions requested for a run.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub tiers: Vec<Tier>,
    pub durabilities: Vec<Durability>,
    pub cache_states: Vec<CacheState>,
    pub sample_count: usize,
    pub update_baseline: bool,
}

impl HarnessConfig {
    /// Build from env vars and cli args. Env var names:
    /// THUNDERDB_TIER, THUNDERDB_DURABILITY, THUNDERDB_CACHE.
    /// CLI args supported: --update-baseline, --quick (these libtest rejects
    /// as unknown; use env vars THUNDERDB_UPDATE_BASELINE=1 and THUNDERDB_QUICK=1
    /// when running via `cargo test`).
    pub fn from_env_and_args(args: &[String]) -> Self {
        let tiers = Tier::parse_set(&env::var("THUNDERDB_TIER").unwrap_or_default());
        let durabilities = Durability::parse_set(&env::var("THUNDERDB_DURABILITY").unwrap_or_default());
        let cache_states = CacheState::parse_set(&env::var("THUNDERDB_CACHE").unwrap_or_default());
        let update_baseline = args.iter().any(|a| a == "--update-baseline")
            || env::var("THUNDERDB_UPDATE_BASELINE").map(|v| v == "1" || v == "true").unwrap_or(false);
        let quick = args.iter().any(|a| a == "--quick")
            || env::var("THUNDERDB_QUICK").map(|v| v == "1" || v == "true").unwrap_or(false);
        let sample_count = if quick { 3 } else { 11 };
        Self { tiers, durabilities, cache_states, sample_count, update_baseline }
    }

    pub fn cells(&self) -> impl Iterator<Item = (Tier, Durability, CacheState)> + '_ {
        self.tiers.iter().flat_map(move |&t| {
            self.durabilities.iter().flat_map(move |&d| {
                self.cache_states.iter().map(move |&c| (t, d, c))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_default_small() {
        assert_eq!(Tier::parse_set("small"), vec![Tier::Small]);
    }
    #[test]
    fn tier_all() {
        assert_eq!(Tier::parse_set("all"), vec![Tier::Small, Tier::Medium, Tier::Large]);
    }
    #[test]
    fn tier_invalid_falls_back_to_small() {
        assert_eq!(Tier::parse_set("xyz"), vec![Tier::Small]);
    }
    #[test]
    fn durability_both() {
        assert_eq!(Durability::parse_set("both"), vec![Durability::Fast, Durability::Durable]);
    }
    #[test]
    fn durability_single_fast() {
        assert_eq!(Durability::parse_set("fast"), vec![Durability::Fast]);
    }
    #[test]
    fn cache_both() {
        assert_eq!(CacheState::parse_set("both"), vec![CacheState::Warm, CacheState::Cold]);
    }
    #[test]
    fn cache_cold_only() {
        assert_eq!(CacheState::parse_set("cold"), vec![CacheState::Cold]);
    }
    #[test]
    fn post_counts() {
        assert_eq!(Tier::Small.post_count(), 10_000);
        assert_eq!(Tier::Medium.post_count(), 100_000);
        assert_eq!(Tier::Large.post_count(), 1_000_000);
    }

    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let saved: Vec<_> = vars.iter().map(|(k, _)| (k.to_string(), env::var(k).ok())).collect();
        for (k, v) in vars {
            match v { Some(val) => env::set_var(k, val), None => env::remove_var(k) }
        }
        f();
        for (k, prev) in saved {
            match prev { Some(val) => env::set_var(&k, val), None => env::remove_var(&k) }
        }
    }

    #[test]
    fn harness_config_defaults() {
        with_env(&[
            ("THUNDERDB_TIER", None), ("THUNDERDB_DURABILITY", None), ("THUNDERDB_CACHE", None),
        ], || {
            let c = HarnessConfig::from_env_and_args(&[]);
            assert_eq!(c.tiers, vec![Tier::Small]);
            assert_eq!(c.durabilities, vec![Durability::Fast]);
            assert_eq!(c.cache_states, vec![CacheState::Warm]);
            assert_eq!(c.sample_count, 11);
            assert!(!c.update_baseline);
        });
    }

    #[test]
    fn harness_config_full_matrix() {
        with_env(&[
            ("THUNDERDB_TIER", Some("all")),
            ("THUNDERDB_DURABILITY", Some("both")),
            ("THUNDERDB_CACHE", Some("both")),
        ], || {
            let c = HarnessConfig::from_env_and_args(&[]);
            assert_eq!(c.cells().count(), 12);
        });
    }

    #[test]
    fn harness_config_quick_flag() {
        with_env(&[], || {
            let c = HarnessConfig::from_env_and_args(&["--quick".to_string()]);
            assert_eq!(c.sample_count, 3);
        });
    }

    #[test]
    fn harness_config_update_baseline_flag() {
        with_env(&[], || {
            let c = HarnessConfig::from_env_and_args(&["--update-baseline".to_string()]);
            assert!(c.update_baseline);
        });
    }
}
