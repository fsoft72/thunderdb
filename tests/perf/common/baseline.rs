//! Baseline: committed reference report for regression detection.

use crate::common::report::HarnessReport;
use std::collections::HashMap;
use std::io;
use std::path::Path;

/// Fast lookup key for a BenchResult across runs.
pub type BaselineKey = (String, String, String, String); // (scenario, tier_label, mode_label, cache_label)

/// Load a baseline report from disk. Missing file → empty report.
/// Schema-version mismatch → Err.
pub fn load_baseline(path: &Path) -> io::Result<Option<HarnessReport>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)?;
    let report: HarnessReport = serde_json::from_str(&content)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if report.schema_version != 1 {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
            format!("baseline schema version {} != 1", report.schema_version)));
    }
    Ok(Some(report))
}

/// Save a report as the new baseline (overwrites).
pub fn save_baseline(report: &HarnessReport, path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, json)
}

/// Build a fast-lookup index of a baseline's BenchResults.
pub fn index_baseline(report: &HarnessReport) -> HashMap<BaselineKey, u128> {
    let mut out = HashMap::new();
    for cell in &report.cells {
        for r in &cell.results {
            if let Some(t) = &r.thunder {
                let key = (r.scenario.clone(), cell.tier.label().into(), cell.mode.label().into(), cell.cache.label().into());
                out.insert(key, t.median_ns);
            }
        }
    }
    out
}

/// Compute percent delta of current vs baseline median.
/// Returns None if the baseline doesn't contain this key.
pub fn delta_pct(index: &HashMap<BaselineKey, u128>, key: &BaselineKey, current_ns: u128) -> Option<f64> {
    index.get(key).map(|&b| ((current_ns as f64 - b as f64) / b as f64) * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::fairness::{Tier, Durability, CacheState};
    use crate::common::verdict::Verdict;
    use crate::common::report::{BenchResult, CellReport, EngineTiming, Summary};

    fn sample() -> HarnessReport {
        HarnessReport {
            schema_version: 1,
            git_sha: "x".into(), rustc_version: "x".into(), started_at: "x".into(),
            cells: vec![CellReport {
                tier: Tier::Small, mode: Durability::Fast, cache: CacheState::Warm,
                results: vec![BenchResult {
                    scenario: "foo".into(), group: "read".into(),
                    thunder: Some(EngineTiming { median_ns: 100, p95_ns: 110, sample_count: 11, dropped_outliers: 0 }),
                    sqlite:  Some(EngineTiming { median_ns: 200, p95_ns: 220, sample_count: 11, dropped_outliers: 0 }),
                    ratio: Some(0.5), verdict: Verdict::Win,
                }],
            }],
            summary: Summary { win: 1, tie: 0, loss: 0, unsupported: 0, failure: 0 },
        }
    }

    #[test]
    fn load_missing_returns_none() {
        let r = load_baseline(Path::new("/tmp/no_such_baseline_9876543.json")).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn save_then_load_round_trip() {
        let path = std::env::temp_dir().join("thunderdb_baseline_test.json");
        let _ = std::fs::remove_file(&path);
        save_baseline(&sample(), &path).unwrap();
        let loaded = load_baseline(&path).unwrap().unwrap();
        assert_eq!(loaded, sample());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn delta_detects_regression() {
        let idx = index_baseline(&sample());
        let key = ("foo".into(), "SMALL".into(), "FAST".into(), "WARM".into());
        let d = delta_pct(&idx, &key, 110).unwrap();
        assert!((d - 10.0).abs() < 0.01, "expected +10%, got {}", d);
    }

    #[test]
    fn delta_missing_key_is_none() {
        let idx = index_baseline(&sample());
        let key = ("ghost".into(), "SMALL".into(), "FAST".into(), "WARM".into());
        assert!(delta_pct(&idx, &key, 100).is_none());
    }
}
