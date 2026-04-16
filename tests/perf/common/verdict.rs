//! Verdict classification: Win / Tie / Loss / Unsupported / Failure.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Verdict {
    Win,
    Tie,
    Loss,
    Unsupported,
    Failure(String),
}

/// Classify a ratio (thunder_median / sqlite_median) per the band rule:
/// - `< 0.95` → Win
/// - `[0.95, 1.05]` → Tie
/// - `> 1.05` → Loss
pub fn classify_ratio(ratio: f64) -> Verdict {
    if ratio < 0.95 {
        Verdict::Win
    } else if ratio <= 1.05 {
        Verdict::Tie
    } else {
        Verdict::Loss
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_win() {
        assert_eq!(classify_ratio(0.5), Verdict::Win);
        assert_eq!(classify_ratio(0.94), Verdict::Win);
    }

    #[test]
    fn tie_band_lower() {
        assert_eq!(classify_ratio(0.95), Verdict::Tie);
        assert_eq!(classify_ratio(1.0), Verdict::Tie);
        assert_eq!(classify_ratio(1.05), Verdict::Tie);
    }

    #[test]
    fn loss_beyond_band() {
        assert_eq!(classify_ratio(1.051), Verdict::Loss);
        assert_eq!(classify_ratio(2.0), Verdict::Loss);
    }
}
