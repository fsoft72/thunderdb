//! Test fixtures: deterministic blog dataset shared across scenarios.

use crate::common::fairness::{Tier, Durability};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use thunderdb::Database;

/// Which storage engine a [`Snapshot`] captures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Thunder,
    Sqlite,
}

/// Filesystem snapshot of a single engine's on-disk state. Used to reset
/// mutated databases to a pristine baseline between write-benchmark samples.
/// The temp directory is cleaned on `Drop`.
pub struct Snapshot {
    engine: Engine,
    snapshot_dir: PathBuf,
    /// Live file/dir paths that were captured, relative to snapshot_dir.
    entries: Vec<PathBuf>,
}

impl Snapshot {
    /// Capture a pristine copy of the engine files at `live_path` into a new temp dir.
    ///
    /// Thunder: `live_path` is a directory; every file inside is copied recursively.
    /// SQLite: `live_path` is the main `.db` file; `-wal` and `-shm` companions are
    /// captured when present.
    pub fn capture(engine: Engine, live_path: &Path) -> std::io::Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let uniq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let snap_dir = std::env::temp_dir().join(format!("thunderdb_snap_{}_{}", std::process::id(), uniq));
        std::fs::create_dir_all(&snap_dir)?;
        let mut entries = Vec::new();
        match engine {
            Engine::Thunder => copy_dir_recursive(live_path, &snap_dir, Path::new(""), &mut entries)?,
            Engine::Sqlite => {
                let main_name: PathBuf = live_path.file_name().unwrap().into();
                copy_file_if_exists(live_path, &snap_dir.join(&main_name), &mut entries, main_name)?;
                for suffix in &["-wal", "-shm"] {
                    let mut live = live_path.as_os_str().to_os_string();
                    live.push(suffix);
                    let live = PathBuf::from(live);
                    let name: PathBuf = live.file_name().unwrap().into();
                    copy_file_if_exists(&live, &snap_dir.join(&name), &mut entries, name)?;
                }
            }
        }
        Ok(Snapshot { engine, snapshot_dir: snap_dir, entries })
    }

    /// Restore pristine files to `live_path`, replacing any current content.
    pub fn restore(&self, live_path: &Path) -> std::io::Result<()> {
        match self.engine {
            Engine::Thunder => {
                if live_path.exists() { std::fs::remove_dir_all(live_path)?; }
                std::fs::create_dir_all(live_path)?;
                for rel in &self.entries {
                    let src = self.snapshot_dir.join(rel);
                    let dst = live_path.join(rel);
                    if let Some(parent) = dst.parent() { std::fs::create_dir_all(parent)?; }
                    std::fs::copy(&src, &dst)?;
                }
            }
            Engine::Sqlite => {
                let parent = live_path.parent().unwrap_or(Path::new("."));
                for suffix in &["", "-wal", "-shm"] {
                    let mut p = live_path.as_os_str().to_os_string();
                    p.push(suffix);
                    let p = PathBuf::from(p);
                    if p.exists() { std::fs::remove_file(&p)?; }
                }
                for rel in &self.entries {
                    let src = self.snapshot_dir.join(rel);
                    let dst = parent.join(rel);
                    std::fs::copy(&src, &dst)?;
                }
            }
        }
        Ok(())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.snapshot_dir);
    }
}

fn copy_file_if_exists(src: &Path, dst: &Path, entries: &mut Vec<PathBuf>, rel: PathBuf) -> std::io::Result<()> {
    if src.exists() {
        std::fs::copy(src, dst)?;
        entries.push(rel);
    }
    Ok(())
}

fn copy_dir_recursive(src_root: &Path, dst_root: &Path, rel: &Path, entries: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let src = src_root.join(rel);
    for entry in std::fs::read_dir(&src)? {
        let entry = entry?;
        let name = entry.file_name();
        let child_rel = rel.join(&name);
        let meta = entry.metadata()?;
        if meta.is_dir() {
            std::fs::create_dir_all(dst_root.join(&child_rel))?;
            copy_dir_recursive(src_root, dst_root, &child_rel, entries)?;
        } else {
            let dst = dst_root.join(&child_rel);
            if let Some(parent) = dst.parent() { std::fs::create_dir_all(parent)?; }
            std::fs::copy(entry.path(), dst)?;
            entries.push(child_rel);
        }
    }
    Ok(())
}

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
    thunder_snap: Option<Snapshot>,
    sqlite_snap: Option<Snapshot>,
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

    /// Capture pristine snapshots of both engine file sets.
    /// Must be called AFTER the fixture is fully built and BEFORE any measured
    /// mutation. Closes and reopens handles around the capture so on-disk files
    /// are flushed and self-consistent.
    pub fn snapshot_all(&mut self) -> std::io::Result<()> {
        let (t, s) = self.take_handles();
        drop(t); drop(s);
        // Drop any prior snapshots before capturing new ones so the old temp
        // directories are cleaned immediately (important for the selftest that
        // checks Drop cleanup).
        self.thunder_snap = None;
        self.sqlite_snap = None;
        self.thunder_snap = Some(Snapshot::capture(Engine::Thunder, &self.thunder_dir)?);
        self.sqlite_snap  = Some(Snapshot::capture(Engine::Sqlite,  &self.sqlite_path)?);
        let t = Database::open(&self.thunder_dir)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        let s = Connection::open(&self.sqlite_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        self.set_handles(t, s);
        Ok(())
    }

    /// Restore pristine files to both engines from the snapshots taken by
    /// `snapshot_all`. Handles are closed, files replaced, handles reopened.
    /// Panics if `snapshot_all` has not been called.
    pub fn restore_all(&mut self) -> std::io::Result<()> {
        let (t, s) = self.take_handles();
        drop(t); drop(s);
        self.thunder_snap.as_ref().expect("snapshot_all not called").restore(&self.thunder_dir)?;
        self.sqlite_snap.as_ref().expect("snapshot_all not called").restore(&self.sqlite_path)?;
        let t = Database::open(&self.thunder_dir)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        let s = Connection::open(&self.sqlite_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        self.set_handles(t, s);
        Ok(())
    }
}

/// Construct a Fixtures instance from pre-opened handles and path metadata.
pub(crate) fn make_fixtures(
    tier: Tier, mode: Durability,
    thunder_dir: PathBuf, sqlite_path: PathBuf,
    thunder: Database, sqlite: Connection,
) -> Fixtures {
    Fixtures {
        tier, mode, thunder_dir, sqlite_path,
        thunder: Some(thunder), sqlite: Some(sqlite),
        thunder_snap: None, sqlite_snap: None,
    }
}

/// Deterministic per-post comment count (2-4).
pub fn comments_for_post(post_idx: usize) -> usize { 2 + (post_idx % 3) }

/// Total expected comments across all posts for a given tier.
pub fn total_comments(tier: Tier) -> usize {
    (1..=tier.post_count()).map(comments_for_post).sum()
}

/// Build the blog dataset on both engines and return the fixtures.
///
/// Thunder: users (id index), blog_posts (id, author_id, title indices),
/// comments (post_id, author_id indices).
/// SQLite: same schema with matching indices; pragmas per `mode`.
pub fn build_blog_fixtures(tier: Tier, mode: Durability) -> Fixtures {
    use rusqlite::params;
    use thunderdb::{DirectDataAccess, Value};
    use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};

    // Unique per-call suffix so parallel tests with the same (tier, mode) don't collide.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}_{}_{}_{}",
        std::process::id(),
        tier.label(), mode.label(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    );
    let base = std::env::temp_dir().join(format!("thunderdb_perf_{}", unique));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let thunder_dir = base.join("thunder");
    let sqlite_path = base.join("sqlite.db");

    // ── Thunder ──
    let mut tdb = Database::open(&thunder_dir).expect("open thunderdb");

    // Users
    let users: Vec<Vec<Value>> = (1..=USER_COUNT)
        .map(|i| vec![
            Value::Int32(i as i32),
            Value::varchar(format!("user_{}", i)),
            Value::varchar(format!("user_{}@example.com", i)),
        ]).collect();
    tdb.insert_batch("users", users).unwrap();
    {
        let tbl = tdb.get_table_mut("users").unwrap();
        tbl.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "email".into(), data_type: "VARCHAR".into() },
        ]}).unwrap();
        tbl.create_index("id").unwrap();
    }

    // Posts
    let post_count = tier.post_count();
    let posts: Vec<Vec<Value>> = (1..=post_count)
        .map(|i| {
            let author_id = (i % USER_COUNT) + 1;
            let topic = TOPICS[i % TOPICS.len()];
            vec![
                Value::Int32(i as i32),
                Value::Int32(author_id as i32),
                Value::varchar(format!("Post about {} #{}", topic, i)),
                Value::varchar(format!(
                    "This is post {} discussing {} in depth. ThunderDB makes {} easy.",
                    i, topic, topic)),
            ]
        }).collect();
    tdb.insert_batch("blog_posts", posts).unwrap();
    {
        let tbl = tdb.get_table_mut("blog_posts").unwrap();
        tbl.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "title".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "content".into(), data_type: "VARCHAR".into() },
        ]}).unwrap();
        tbl.create_index("id").unwrap();
        tbl.create_index("author_id").unwrap();
        tbl.create_index("title").unwrap();
    }

    // Comments
    let mut comment_rows = Vec::new();
    let mut cid = 1i32;
    for p in 1..=post_count {
        for c in 0..comments_for_post(p) {
            let commenter = ((p + c) % USER_COUNT) + 1;
            comment_rows.push(vec![
                Value::Int32(cid),
                Value::Int32(p as i32),
                Value::Int32(commenter as i32),
                Value::varchar(format!("Comment {} on post {}", c + 1, p)),
            ]);
            cid += 1;
        }
    }
    tdb.insert_batch("comments", comment_rows).unwrap();
    {
        let tbl = tdb.get_table_mut("comments").unwrap();
        tbl.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "post_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "text".into(), data_type: "VARCHAR".into() },
        ]}).unwrap();
        tbl.create_index("post_id").unwrap();
        tbl.create_index("author_id").unwrap();
    }

    // ── SQLite ──
    let sdb = Connection::open(&sqlite_path).unwrap();
    match mode {
        Durability::Fast => {
            sdb.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        }
        Durability::Durable => {
            sdb.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;").unwrap();
        }
    }

    sdb.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
         CREATE TABLE blog_posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
         CREATE INDEX idx_posts_author ON blog_posts(author_id);
         CREATE INDEX idx_posts_title ON blog_posts(title);
         CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, author_id INTEGER NOT NULL, text TEXT NOT NULL);
         CREATE INDEX idx_comments_post ON comments(post_id);
         CREATE INDEX idx_comments_author ON comments(author_id);"
    ).unwrap();

    {
        let mut st = sdb.prepare("INSERT INTO users (id, name, email) VALUES (?1, ?2, ?3)").unwrap();
        for i in 1..=USER_COUNT {
            st.execute(params![i as i32, format!("user_{}", i), format!("user_{}@example.com", i)]).unwrap();
        }
    }
    {
        let tx = sdb.unchecked_transaction().unwrap();
        {
            let mut st = tx.prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)").unwrap();
            for i in 1..=post_count {
                let author = (i % USER_COUNT) + 1;
                let topic = TOPICS[i % TOPICS.len()];
                st.execute(params![
                    i as i32, author as i32,
                    format!("Post about {} #{}", topic, i),
                    format!("This is post {} discussing {} in depth. ThunderDB makes {} easy.", i, topic, topic),
                ]).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    {
        let tx = sdb.unchecked_transaction().unwrap();
        {
            let mut st = tx.prepare("INSERT INTO comments (id, post_id, author_id, text) VALUES (?1, ?2, ?3, ?4)").unwrap();
            let mut cid = 1i32;
            for p in 1..=post_count {
                for c in 0..comments_for_post(p) {
                    let commenter = ((p + c) % USER_COUNT) + 1;
                    st.execute(params![
                        cid, p as i32, commenter as i32,
                        format!("Comment {} on post {}", c + 1, p),
                    ]).unwrap();
                    cid += 1;
                }
            }
        }
        tx.commit().unwrap();
    }

    make_fixtures(tier, mode, thunder_dir, sqlite_path, tdb, sdb)
}

/// Build the same blog schema as `build_blog_fixtures` but with zero rows in
/// posts and comments. Users table is left empty as well. Intended for INSERT
/// benchmarks that measure population cost starting from an empty DB.
pub fn build_empty_fixtures(tier: Tier, mode: Durability) -> Fixtures {
    use std::sync::atomic::{AtomicU64, Ordering};
    use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}_{}_{}_empty_{}",
        std::process::id(), tier.label(), mode.label(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    );
    let base = std::env::temp_dir().join(format!("thunderdb_perf_{}", unique));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let thunder_dir = base.join("thunder");
    let sqlite_path = base.join("sqlite.db");

    let mut tdb = Database::open(&thunder_dir).expect("open thunderdb");
    for (table, cols) in [
        ("users", vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "email".into(), data_type: "VARCHAR".into() },
        ]),
        ("blog_posts", vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "title".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "content".into(), data_type: "VARCHAR".into() },
        ]),
        ("comments", vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "post_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "text".into(), data_type: "VARCHAR".into() },
        ]),
    ] {
        let tbl = tdb.get_or_create_table(table).unwrap();
        tbl.set_schema(TableSchema { columns: cols }).unwrap();
        tbl.create_index("id").unwrap();
    }

    let sdb = Connection::open(&sqlite_path).unwrap();
    match mode {
        Durability::Fast => { sdb.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); }
        Durability::Durable => { sdb.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;").unwrap(); }
    }
    sdb.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
         CREATE TABLE blog_posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
         CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, author_id INTEGER NOT NULL, text TEXT NOT NULL);"
    ).unwrap();

    make_fixtures(tier, mode, thunder_dir, sqlite_path, tdb, sdb)
}

/// Build a single-table dataset (`blog_posts_q`) tuned for SP4a query
/// scenarios: nullable `category` (~10% NULL), nullable `published_at`
/// (~30% NULL), indexed `slug`, non-indexed `body`, non-indexed `views`.
///
/// Both Thunder and SQLite are populated with identical row contents so
/// per-scenario `.assert` callbacks can compare row counts byte-for-byte.
///
/// Determinism: row `i` (1-based) chooses
/// - `author_id = (i % 50) + 1`
/// - `category = if i % 10 == 0 { NULL } else { CATEGORIES[i % 5] }`
/// - `published_at = if i % 10 < 3 { NULL } else { 1_700_000_000 + i as i64 }`
/// - `views = ((i * 2654435761) % 1_000_000) as i64`  (Knuth multiplicative hash)
/// - `slug = format!("post-{:08x}", i)`              (unique, lowercase ascii)
pub fn build_blog_posts_q_fixtures(tier: Tier, mode: Durability) -> Fixtures {
    use rusqlite::params;
    use thunderdb::{DirectDataAccess, Value};
    use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};

    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}_{}_{}_q_{}",
        std::process::id(), tier.label(), mode.label(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    );
    let base = std::env::temp_dir().join(format!("thunderdb_perf_{}", unique));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let thunder_dir = base.join("thunder");
    let sqlite_path = base.join("sqlite.db");

    const CATEGORIES: [&str; 5] = ["news", "review", "tutorial", "opinion", "guide"];
    let post_count = tier.post_count();

    // ── Thunder ──
    let mut tdb = Database::open(&thunder_dir).expect("open thunderdb");

    let posts: Vec<Vec<Value>> = (1..=post_count).map(|i| {
        let author_id = (i as i64 % 50) + 1;
        let category: Value = if i % 10 == 0 {
            Value::Null
        } else {
            Value::varchar(CATEGORIES[i % CATEGORIES.len()].to_string())
        };
        let published_at: Value = if i % 10 < 3 {
            Value::Null
        } else {
            Value::Int64(1_700_000_000 + i as i64)
        };
        let views = ((i as i64).wrapping_mul(2654435761)).rem_euclid(1_000_000);
        vec![
            Value::Int64(i as i64),
            Value::Int64(author_id),
            Value::varchar(format!("Post about topic #{}", i)),
            Value::varchar(format!("post-{:08x}", i)),
            Value::varchar(format!("This is the body of post {}.  Topic discussion follows for several sentences.", i)),
            category,
            published_at,
            Value::Int64(views),
        ]
    }).collect();

    tdb.insert_batch("blog_posts_q", posts).unwrap();
    {
        let tbl = tdb.get_table_mut("blog_posts_q").unwrap();
        tbl.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(),           data_type: "INT64".into() },
            ColumnInfo { name: "author_id".into(),    data_type: "INT64".into() },
            ColumnInfo { name: "title".into(),        data_type: "VARCHAR".into() },
            ColumnInfo { name: "slug".into(),         data_type: "VARCHAR".into() },
            ColumnInfo { name: "body".into(),         data_type: "VARCHAR".into() },
            ColumnInfo { name: "category".into(),     data_type: "VARCHAR".into() },
            ColumnInfo { name: "published_at".into(), data_type: "INT64".into() },
            ColumnInfo { name: "views".into(),        data_type: "INT64".into() },
        ]}).unwrap();
        tbl.create_index("id").unwrap();
        tbl.create_index("author_id").unwrap();
        tbl.create_index("title").unwrap();
        tbl.create_index("slug").unwrap();
    }

    // ── SQLite ──
    let sdb = Connection::open(&sqlite_path).unwrap();
    match mode {
        Durability::Fast => {
            sdb.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        }
        Durability::Durable => {
            sdb.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;").unwrap();
        }
    }
    sdb.execute_batch(
        "CREATE TABLE blog_posts_q (
            id INTEGER PRIMARY KEY,
            author_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            slug TEXT NOT NULL,
            body TEXT NOT NULL,
            category TEXT,
            published_at INTEGER,
            views INTEGER NOT NULL
         );
         CREATE INDEX idx_q_author ON blog_posts_q(author_id);
         CREATE INDEX idx_q_title  ON blog_posts_q(title);
         CREATE INDEX idx_q_slug   ON blog_posts_q(slug);"
    ).unwrap();

    {
        let tx = sdb.unchecked_transaction().unwrap();
        {
            let mut st = tx.prepare(
                "INSERT INTO blog_posts_q (id, author_id, title, slug, body, category, published_at, views)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)").unwrap();
            for i in 1..=post_count {
                let author_id = (i as i64 % 50) + 1;
                let category: Option<String> = if i % 10 == 0 {
                    None
                } else {
                    Some(CATEGORIES[i % CATEGORIES.len()].to_string())
                };
                let published_at: Option<i64> = if i % 10 < 3 {
                    None
                } else {
                    Some(1_700_000_000 + i as i64)
                };
                let views = ((i as i64).wrapping_mul(2654435761)).rem_euclid(1_000_000);
                st.execute(params![
                    i as i64, author_id,
                    format!("Post about topic #{}", i),
                    format!("post-{:08x}", i),
                    format!("This is the body of post {}.  Topic discussion follows for several sentences.", i),
                    category, published_at, views,
                ]).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    make_fixtures(tier, mode, thunder_dir, sqlite_path, tdb, sdb)
}

/// Close and reopen both engine handles. Used between COLD samples.
pub(crate) fn reopen_handles(f: &mut Fixtures) -> std::io::Result<()> {
    let (_t, _s) = f.take_handles();
    drop(_t);
    drop(_s);

    // Thunder: all *.bin data files.
    for p in crate::common::cache::collect_data_files(&f.thunder_dir) {
        let _ = crate::common::cache::posix_fadvise_dontneed(&p);
    }

    // SQLite: main db + WAL/SHM companions (fair COLD measurement).
    // Missing files (non-WAL journal mode, DELETE mode post-commit) skip silently.
    let _ = crate::common::cache::posix_fadvise_dontneed(&f.sqlite_path);
    for suffix in &["-wal", "-shm"] {
        let companion = {
            let mut s = f.sqlite_path.clone().into_os_string();
            s.push(suffix);
            std::path::PathBuf::from(s)
        };
        if companion.exists() {
            let _ = crate::common::cache::posix_fadvise_dontneed(&companion);
        }
    }

    let t = Database::open(&f.thunder_dir).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    let s = Connection::open(&f.sqlite_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    f.set_handles(t, s);
    Ok(())
}

/// Clean up tmp directories created by `build_blog_fixtures` (best-effort).
///
/// Clones paths before dropping the struct so handles are closed first
/// (important on Windows), then removes files and tries to remove the
/// parent base directory if it is now empty.
pub fn drop_fixtures(f: Fixtures) {
    let thunder_dir = f.thunder_dir.clone();
    let sqlite_path = f.sqlite_path.clone();
    drop(f);
    let _ = std::fs::remove_dir_all(&thunder_dir);
    let _ = std::fs::remove_file(&sqlite_path);
    if let Some(parent) = thunder_dir.parent() {
        let _ = std::fs::remove_dir(parent);
    }
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

    #[test]
    fn small_fixture_has_correct_row_counts() {
        let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);
        use thunderdb::DirectDataAccess;
        let users = f.thunder_mut().count("users", vec![]).unwrap();
        let posts = f.thunder_mut().count("blog_posts", vec![]).unwrap();
        let comments = f.thunder_mut().count("comments", vec![]).unwrap();
        assert_eq!(users, USER_COUNT);
        assert_eq!(posts, Tier::Small.post_count());
        assert_eq!(comments, total_comments(Tier::Small));

        let s_users: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
        let s_posts: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
        let s_comments: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM comments", [], |r| r.get(0)).unwrap();
        assert_eq!(s_users as usize, USER_COUNT);
        assert_eq!(s_posts as usize, Tier::Small.post_count());
        assert_eq!(s_comments as usize, total_comments(Tier::Small));

        drop_fixtures(f);
    }

    #[test]
    fn comments_for_post_distribution() {
        assert_eq!(comments_for_post(1), 3);
        assert_eq!(comments_for_post(2), 4);
        assert_eq!(comments_for_post(3), 2);
    }

    #[test]
    fn engine_enum_has_variants() {
        let e = Engine::Thunder;
        match e {
            Engine::Thunder => {}
            Engine::Sqlite => {}
        }
    }

    #[test]
    fn empty_fixture_has_zero_rows() {
        let mut f = build_empty_fixtures(Tier::Small, Durability::Fast);
        use thunderdb::DirectDataAccess;
        assert_eq!(f.thunder_mut().count("blog_posts", vec![]).unwrap(), 0);
        let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
        assert_eq!(s, 0);
        drop_fixtures(f);
    }

    #[test]
    fn snapshot_and_restore_roundtrip_blog_fixture() {
        use rusqlite::params;
        use thunderdb::DirectDataAccess;

        let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);
        f.snapshot_all().unwrap();

        // Mutate both engines after snapshot.
        f.thunder_mut().delete("blog_posts", vec![]).unwrap();
        let _ = f.sqlite().execute("DELETE FROM blog_posts", params![]).unwrap();

        // Restore.
        f.restore_all().unwrap();

        // Row counts match the pristine fixture.
        let t_posts = f.thunder_mut().count("blog_posts", vec![]).unwrap();
        let s_posts: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
        assert_eq!(t_posts, Tier::Small.post_count());
        assert_eq!(s_posts as usize, Tier::Small.post_count());

        drop_fixtures(f);
    }
}
