//! Test fixtures: deterministic blog dataset shared across scenarios.

use crate::common::fairness::{Tier, Durability};
use rusqlite::Connection;
use std::path::PathBuf;
use thunderdb::Database;

/// Reserved seed for any fixture that needs randomness.
/// Base blog fixture is index-derived and doesn't use it.
pub const FIXTURE_SEED: u64 = 0xD811_1DB5_EED5_5EED;

pub const USER_COUNT: usize = 5;
pub const TOPICS: [&str; 5] = ["rust", "database", "performance", "testing", "design"];

pub struct Fixtures {
    pub tier: Tier,
    pub mode: Durability,
    pub thunder_dir: PathBuf,
    pub sqlite_path: PathBuf,
    thunder: Option<Database>,
    sqlite: Option<Connection>,
}

impl Fixtures {
    /// Return a reference to the ThunderDB database handle.
    pub fn thunder(&self) -> &Database {
        self.thunder.as_ref().expect("thunder handle closed")
    }

    /// Return a mutable reference to the ThunderDB database handle.
    pub fn thunder_mut(&mut self) -> &mut Database {
        self.thunder.as_mut().expect("thunder handle closed")
    }

    /// Return a reference to the SQLite connection handle.
    pub fn sqlite(&self) -> &Connection {
        self.sqlite.as_ref().expect("sqlite handle closed")
    }

    /// Harness-internal: close and reopen handles for COLD cache.
    pub(crate) fn take_handles(&mut self) -> (Option<Database>, Option<Connection>) {
        (self.thunder.take(), self.sqlite.take())
    }

    /// Harness-internal: restore handles after a COLD cache cycle.
    pub(crate) fn set_handles(&mut self, t: Database, s: Connection) {
        self.thunder = Some(t);
        self.sqlite = Some(s);
    }
}

/// Construct a Fixtures instance from pre-opened handles and path metadata.
pub(crate) fn make_fixtures(
    tier: Tier, mode: Durability,
    thunder_dir: PathBuf, sqlite_path: PathBuf,
    thunder: Database, sqlite: Connection,
) -> Fixtures {
    Fixtures { tier, mode, thunder_dir, sqlite_path, thunder: Some(thunder), sqlite: Some(sqlite) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_work() {
        let tmp = std::env::temp_dir().join("thunderdb_fixture_accessor_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let thunder_dir = tmp.join("thunder");
        let sqlite_path = tmp.join("sqlite.db");
        let thunder = Database::open(&thunder_dir).unwrap();
        let sqlite = Connection::open(&sqlite_path).unwrap();
        let f = make_fixtures(Tier::Small, Durability::Fast, thunder_dir, sqlite_path, thunder, sqlite);
        let _ = f.thunder();
        let _ = f.sqlite();
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    #[should_panic(expected = "thunder handle closed")]
    fn thunder_after_take_panics() {
        let tmp = std::env::temp_dir().join("thunderdb_fixture_take_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let thunder = Database::open(tmp.join("t")).unwrap();
        let sqlite = Connection::open(tmp.join("s.db")).unwrap();
        let mut f = make_fixtures(Tier::Small, Durability::Fast, tmp.join("t"), tmp.join("s.db"), thunder, sqlite);
        let _ = f.take_handles();
        let _ = f.thunder();  // panics
    }
}
