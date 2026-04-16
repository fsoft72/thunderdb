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
}
