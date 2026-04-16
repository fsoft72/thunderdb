//! Result types, JSON serialization, terminal rendering, baseline diff.

use crate::common::fairness::{Tier, Durability, CacheState};
use crate::common::verdict::Verdict;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EngineTiming {
    pub median_ns: u128,
    pub p95_ns: u128,
    pub sample_count: usize,
    pub dropped_outliers: usize,
}

impl EngineTiming {
    /// Return the median as a `Duration`.
    pub fn median(&self) -> Duration { Duration::from_nanos(self.median_ns as u64) }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchResult {
    pub scenario: String,
    pub group: String,
    pub thunder: Option<EngineTiming>,
    pub sqlite: Option<EngineTiming>,
    pub ratio: Option<f64>,
    pub verdict: Verdict,
}

impl Serialize for Tier {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.label())
    }
}
impl<'de> Deserialize<'de> for Tier {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s: String = String::deserialize(d)?;
        match s.as_str() {
            "SMALL" => Ok(Tier::Small),
            "MEDIUM" => Ok(Tier::Medium),
            "LARGE" => Ok(Tier::Large),
            _ => Err(serde::de::Error::custom(format!("unknown tier {}", s))),
        }
    }
}
impl Serialize for Durability {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.label())
    }
}
impl<'de> Deserialize<'de> for Durability {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s: String = String::deserialize(d)?;
        match s.as_str() {
            "FAST" => Ok(Durability::Fast),
            "DURABLE" => Ok(Durability::Durable),
            _ => Err(serde::de::Error::custom(format!("unknown durability {}", s))),
        }
    }
}
impl Serialize for CacheState {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.label())
    }
}
impl<'de> Deserialize<'de> for CacheState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s: String = String::deserialize(d)?;
        match s.as_str() {
            "WARM" => Ok(CacheState::Warm),
            "COLD" => Ok(CacheState::Cold),
            _ => Err(serde::de::Error::custom(format!("unknown cache state {}", s))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CellReport {
    pub tier: Tier,
    pub mode: Durability,
    pub cache: CacheState,
    pub results: Vec<BenchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Summary {
    pub win: usize,
    pub tie: usize,
    pub loss: usize,
    pub unsupported: usize,
    pub failure: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HarnessReport {
    pub schema_version: u32,
    pub git_sha: String,
    pub rustc_version: String,
    pub started_at: String,
    pub cells: Vec<CellReport>,
    pub summary: Summary,
}

impl HarnessReport {
    /// Recompute summary counts from all cell results.
    pub fn compute_summary(&mut self) {
        let mut s = Summary { win: 0, tie: 0, loss: 0, unsupported: 0, failure: 0 };
        for cell in &self.cells {
            for r in &cell.results {
                match &r.verdict {
                    Verdict::Win => s.win += 1,
                    Verdict::Tie => s.tie += 1,
                    Verdict::Loss => s.loss += 1,
                    Verdict::Unsupported => s.unsupported += 1,
                    Verdict::Failure(_) => s.failure += 1,
                }
            }
        }
        self.summary = s;
    }
}

impl HarnessReport {
    /// Render a human-readable scoreboard. Stable format: header, per-cell
    /// table, aggregate footer.
    pub fn to_terminal(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        for cell in &self.cells {
            writeln!(&mut out, "\n=== vs SQLite: (tier={}, mode={}, cache={}) ===\n",
                cell.tier.label(), cell.mode.label(), cell.cache.label()).unwrap();
            writeln!(&mut out, " {:<40} {:>10} {:>10} {:>8}   {}",
                "Scenario", "Thunder", "SQLite", "Ratio", "Verdict").unwrap();
            writeln!(&mut out, " {}", "-".repeat(80)).unwrap();
            let mut w = 0; let mut t = 0; let mut l = 0; let mut u = 0; let mut f = 0;
            for r in &cell.results {
                let thunder = r.thunder.as_ref().map(|t| format_duration_ns(t.median_ns)).unwrap_or_else(|| "n/a".into());
                let sqlite = r.sqlite.as_ref().map(|s| format_duration_ns(s.median_ns)).unwrap_or_else(|| "n/a".into());
                let ratio = r.ratio.map(|x| format!("{:.2}x", x)).unwrap_or_else(|| "—".into());
                let verdict = match &r.verdict {
                    Verdict::Win => { w += 1; "Win".to_string() }
                    Verdict::Tie => { t += 1; "Tie".to_string() }
                    Verdict::Loss => { l += 1; "Loss".to_string() }
                    Verdict::Unsupported => { u += 1; "Unsupported".to_string() }
                    Verdict::Failure(msg) => { f += 1; format!("Failure: {}", msg) }
                };
                writeln!(&mut out, " {:<40} {:>10} {:>10} {:>8}   {}",
                    r.scenario, thunder, sqlite, ratio, verdict).unwrap();
            }
            writeln!(&mut out, " {}", "-".repeat(80)).unwrap();
            writeln!(&mut out, " Summary: {} Win, {} Tie, {} Loss, {} Unsupported, {} Failure",
                w, t, l, u, f).unwrap();
        }

        writeln!(&mut out, "\n=== Aggregate across all cells ===").unwrap();
        writeln!(&mut out, " OVERALL: {} Win, {} Tie, {} Loss, {} Unsupported, {} Failure  → {}",
            self.summary.win, self.summary.tie, self.summary.loss,
            self.summary.unsupported, self.summary.failure,
            if self.summary.loss > 0 || self.summary.failure > 0 { "FAIL" } else { "PASS" }).unwrap();

        out
    }
}

impl HarnessReport {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    pub fn write_to(&self, dir: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
        std::fs::create_dir_all(dir)?;
        let fname = format!("{}.json", self.started_at.replace(":", "-"));
        let path = dir.join(fname);
        std::fs::write(&path, self.to_json())?;
        Ok(path)
    }

    /// Print scoreboard to stdout and exit with 0 (pass) or 1 (any Loss or Failure).
    pub fn exit_with_verdict(self) -> ! {
        println!("{}", self.to_terminal());
        let code = if self.summary.loss > 0 || self.summary.failure > 0 { 1 } else { 0 };
        std::process::exit(code);
    }
}

/// Format nanoseconds as a human-readable duration string.
/// Thresholds: <1µs → ns, <1ms → µs, <1s → ms, else → s.
fn format_duration_ns(ns: u128) -> String {
    if ns < 1_000 { format!("{}ns", ns) }
    else if ns < 1_000_000 { format!("{}µs", ns / 1_000) }
    else if ns < 1_000_000_000 { format!("{}ms", ns / 1_000_000) }
    else { format!("{:.2}s", ns as f64 / 1e9) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> HarnessReport {
        HarnessReport {
            schema_version: 1,
            git_sha: "abc123".into(),
            rustc_version: "1.78.0".into(),
            started_at: "2026-04-16T14:22:00Z".into(),
            cells: vec![CellReport {
                tier: Tier::Small, mode: Durability::Fast, cache: CacheState::Warm,
                results: vec![BenchResult {
                    scenario: "test".into(), group: "read".into(),
                    thunder: Some(EngineTiming { median_ns: 100, p95_ns: 110, sample_count: 11, dropped_outliers: 0 }),
                    sqlite:  Some(EngineTiming { median_ns: 200, p95_ns: 220, sample_count: 11, dropped_outliers: 0 }),
                    ratio: Some(0.5),
                    verdict: Verdict::Win,
                }],
            }],
            summary: Summary { win: 1, tie: 0, loss: 0, unsupported: 0, failure: 0 },
        }
    }

    #[test]
    fn json_round_trip() {
        let r = sample_report();
        let json = serde_json::to_string(&r).unwrap();
        let parsed: HarnessReport = serde_json::from_str(&json).unwrap();
        assert_eq!(r, parsed);
    }

    #[test]
    fn compute_summary_counts() {
        let mut r = sample_report();
        r.cells[0].results.push(BenchResult {
            scenario: "test2".into(), group: "read".into(),
            thunder: None, sqlite: None, ratio: None, verdict: Verdict::Unsupported,
        });
        r.compute_summary();
        assert_eq!(r.summary.win, 1);
        assert_eq!(r.summary.unsupported, 1);
    }

    #[test]
    fn terminal_contains_scenario_name_and_ratio() {
        let out = sample_report().to_terminal();
        assert!(out.contains("test"));
        assert!(out.contains("0.5"));
        assert!(out.contains("Win"));
    }

    #[test]
    fn terminal_has_cell_header() {
        let out = sample_report().to_terminal();
        assert!(out.contains("tier=SMALL"));
        assert!(out.contains("mode=FAST"));
        assert!(out.contains("cache=WARM"));
    }

    #[test]
    fn terminal_has_summary_line() {
        let out = sample_report().to_terminal();
        assert!(out.contains("Summary:"));
    }

    #[test]
    fn format_duration() {
        assert_eq!(format_duration_ns(500), "500ns");
        assert_eq!(format_duration_ns(1_500), "1µs");
        assert_eq!(format_duration_ns(2_500_000), "2ms");
    }

    #[test]
    fn write_to_creates_file() {
        let dir = std::env::temp_dir().join("thunderdb_write_to_test");
        let _ = std::fs::remove_dir_all(&dir);
        let path = sample_report().write_to(&dir).unwrap();
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"scenario\": \"test\""));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
