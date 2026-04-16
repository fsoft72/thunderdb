//! Scenario: one benchmark end-to-end.

use crate::common::fairness::{Tier, Durability};
use crate::common::fixtures::Fixtures;

pub type SetupFn = Box<dyn Fn(Tier, Durability) -> Fixtures + Send + Sync>;
pub type ThunderFn = Box<dyn Fn(&mut Fixtures) + Send + Sync>;
pub type SqliteFn  = Box<dyn Fn(&Fixtures) + Send + Sync>;
pub type AssertFn  = Box<dyn Fn(&mut Fixtures) -> Result<(), String> + Send + Sync>;

pub struct Scenario {
    pub name: &'static str,
    pub group: &'static str,
    pub setup: SetupFn,
    pub thunder: ThunderFn,
    pub sqlite: SqliteFn,
    pub assert: AssertFn,
}

pub struct ScenarioBuilder {
    name: &'static str,
    group: &'static str,
    setup: Option<SetupFn>,
    thunder: Option<ThunderFn>,
    sqlite: Option<SqliteFn>,
    assert: Option<AssertFn>,
}

impl Scenario {
    /// Create a new [`ScenarioBuilder`] for a scenario with the given name and group.
    pub fn new(name: &'static str, group: &'static str) -> ScenarioBuilder {
        ScenarioBuilder { name, group, setup: None, thunder: None, sqlite: None, assert: None }
    }
}

impl ScenarioBuilder {
    /// Set the setup function that builds [`Fixtures`] for the given tier and durability.
    pub fn setup<F: Fn(Tier, Durability) -> Fixtures + Send + Sync + 'static>(mut self, f: F) -> Self { self.setup = Some(Box::new(f)); self }

    /// Set the function that runs the Thunder benchmark operation.
    pub fn thunder<F: Fn(&mut Fixtures) + Send + Sync + 'static>(mut self, f: F) -> Self { self.thunder = Some(Box::new(f)); self }

    /// Set the function that runs the SQLite benchmark operation.
    pub fn sqlite<F: Fn(&Fixtures) + Send + Sync + 'static>(mut self, f: F) -> Self { self.sqlite = Some(Box::new(f)); self }

    /// Set the assertion function that validates correctness after the benchmark run.
    pub fn assert<F: Fn(&mut Fixtures) -> Result<(), String> + Send + Sync + 'static>(mut self, f: F) -> Self { self.assert = Some(Box::new(f)); self }

    /// Consume the builder and return a fully-configured [`Scenario`].
    /// Panics if any required closure (setup, thunder, sqlite, assert) is missing.
    pub fn build(self) -> Scenario {
        Scenario {
            name: self.name, group: self.group,
            setup: self.setup.expect("scenario missing setup"),
            thunder: self.thunder.expect("scenario missing thunder"),
            sqlite: self.sqlite.expect("scenario missing sqlite"),
            assert: self.assert.expect("scenario missing assert"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::fixtures::build_blog_fixtures;

    #[test]
    fn builder_composes_scenario() {
        let s = Scenario::new("dummy", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|_f| {})
            .sqlite(|_f| {})
            .assert(|_f| Ok(()))
            .build();
        assert_eq!(s.name, "dummy");
        assert_eq!(s.group, "read");
    }

    #[test]
    #[should_panic(expected = "scenario missing thunder")]
    fn builder_panics_when_incomplete() {
        let _ = Scenario::new("broken", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .sqlite(|_f| {})
            .assert(|_f| Ok(()))
            .build();
    }
}
