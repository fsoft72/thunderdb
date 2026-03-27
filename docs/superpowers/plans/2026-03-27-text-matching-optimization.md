# Text Matching Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve ThunderDB's text matching performance through three optimizations: memchr SIMD matching, column-offset row format, and B-tree prefix indexing on VARCHAR columns.

**Architecture:** memchr replaces std string methods in LikePattern for SIMD-accelerated matching. Row serialization adds a u16 offset array per row for O(1) column access. DataFile gains a callback-based scan that filters on raw bytes before full deserialization. The benchmark adds title indexes to exercise the existing prefix LIKE index path.

**Tech Stack:** Rust, memchr crate, existing ThunderDB B-tree infrastructure.

---

### Task 1: Add memchr dependency and update LikePattern Contains variant

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/index/like.rs`

- [ ] **Step 1: Add memchr to Cargo.toml**

In `Cargo.toml`, add `memchr` to `[dependencies]` (after `serde_json`):

```toml
memchr = "2"
```

- [ ] **Step 2: Change Contains variant to struct with cached Finder**

In `src/index/like.rs`, replace the `Contains` variant and add manual `PartialEq` / `Clone`:

Replace the entire `LikePattern` enum definition (lines 9-25) with:

```rust
/// LIKE pattern type
#[derive(Debug)]
pub enum LikePattern {
    /// Prefix match: 'abc%' matches strings starting with "abc"
    Prefix(String),

    /// Suffix match: '%abc' matches strings ending with "abc"
    Suffix(String),

    /// Contains: '%abc%' matches strings containing "abc"
    /// Pre-builds a memchr::memmem::Finder for SIMD-accelerated search.
    Contains {
        needle: String,
        finder: memchr::memmem::Finder<'static>,
    },

    /// Exact match: 'abc' (no wildcards)
    Exact(String),

    /// Complex pattern with multiple % and _ wildcards
    Complex(String),
}
```

Remove `#[derive(Clone, PartialEq)]` from the enum (already done above) and add manual impls after the enum:

```rust
impl PartialEq for LikePattern {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Prefix(a), Self::Prefix(b)) => a == b,
            (Self::Suffix(a), Self::Suffix(b)) => a == b,
            (Self::Contains { needle: a, .. }, Self::Contains { needle: b, .. }) => a == b,
            (Self::Exact(a), Self::Exact(b)) => a == b,
            (Self::Complex(a), Self::Complex(b)) => a == b,
            _ => false,
        }
    }
}

impl Clone for LikePattern {
    fn clone(&self) -> Self {
        match self {
            Self::Prefix(s) => Self::Prefix(s.clone()),
            Self::Suffix(s) => Self::Suffix(s.clone()),
            Self::Contains { needle, .. } => Self::new_contains(needle.clone()),
            Self::Exact(s) => Self::Exact(s.clone()),
            Self::Complex(s) => Self::Complex(s.clone()),
        }
    }
}
```

- [ ] **Step 3: Add `new_contains` constructor and update `parse()`**

Add a constructor method to `LikePattern` impl block (before `parse`):

```rust
/// Build a Contains variant with a pre-constructed SIMD Finder.
///
/// # Safety
/// The Finder borrows from the needle's heap buffer. String heap data
/// has a stable address — moving the struct does not move the heap
/// allocation — so the Finder's internal pointer stays valid for the
/// lifetime of this enum variant.
fn new_contains(needle: String) -> Self {
    let ptr = needle.as_bytes().as_ptr();
    let len = needle.as_bytes().len();
    // SAFETY: needle is heap-allocated, lives alongside finder in the
    // same enum variant, and is never mutated after construction.
    let static_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let finder = memchr::memmem::Finder::new(static_bytes);
    LikePattern::Contains { needle, finder }
}
```

In `parse()`, replace line 54:
```rust
Ok(LikePattern::Contains(content.to_string()))
```
with:
```rust
Ok(LikePattern::new_contains(content.to_string()))
```

- [ ] **Step 4: Update `matches_string()` to use memchr and byte-level ops**

Replace the `matches_string` method (lines 89-97) with:

```rust
/// Check if a string matches this pattern
fn matches_string(&self, s: &str) -> bool {
    match self {
        LikePattern::Exact(pattern) => s.as_bytes() == pattern.as_bytes(),
        LikePattern::Prefix(prefix) => s.as_bytes().starts_with(prefix.as_bytes()),
        LikePattern::Suffix(suffix) => s.as_bytes().ends_with(suffix.as_bytes()),
        LikePattern::Contains { finder, .. } => finder.find(s.as_bytes()).is_some(),
        LikePattern::Complex(pattern) => self.matches_complex(s, pattern),
    }
}
```

- [ ] **Step 5: Update `get_prefix()` to handle new Contains variant**

In the `get_prefix()` method (line 176-182), the `Contains` match arm needs updating because the variant is now a struct. The existing `_ => None` arm already covers it, so no change needed — but verify by reading the code.

- [ ] **Step 6: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass (the existing like.rs tests cover prefix, suffix, contains, exact, complex, and edge cases).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/index/like.rs
git commit -m "Use memchr SIMD for LIKE matching, cache Finder in Contains variant"
```

---

### Task 2: Add column offset array to Row serialization

**Files:**
- Modify: `src/storage/row.rs`

- [ ] **Step 1: Write test for new row format round-trip**

Add to the `tests` module in `src/storage/row.rs`:

```rust
#[test]
fn test_row_format_with_offsets() {
    // 4-column row similar to blog_posts: id, author_id, title, content
    let row = Row::new(
        42,
        vec![
            Value::Int32(1),
            Value::Int32(5),
            Value::varchar("Post about rust #1".to_string()),
            Value::varchar("This is a long content field with lots of text".to_string()),
        ],
    );

    let bytes = row.to_bytes();

    // Verify header: row_id(8) + col_count(4) + 4 offsets(8) = 20 bytes before values
    let col_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    assert_eq!(col_count, 4);

    // Verify round-trip
    let decoded = Row::from_bytes(&bytes).unwrap();
    assert_eq!(row, decoded);
}

#[test]
fn test_value_at_single_column() {
    let row = Row::new(
        1,
        vec![
            Value::Int32(100),
            Value::varchar("hello".to_string()),
            Value::Int64(999),
        ],
    );

    let bytes = row.to_bytes();

    // Extract each column individually
    let v0 = Row::value_at(&bytes, 0).unwrap();
    assert_eq!(v0, Value::Int32(100));

    let v1 = Row::value_at(&bytes, 1).unwrap();
    assert_eq!(v1, Value::varchar("hello".to_string()));

    let v2 = Row::value_at(&bytes, 2).unwrap();
    assert_eq!(v2, Value::Int64(999));

    // Out of bounds
    assert!(Row::value_at(&bytes, 3).is_err());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib test_row_format_with_offsets test_value_at_single_column 2>&1 | tail -10`
Expected: FAIL — `value_at` does not exist, offset layout doesn't match.

- [ ] **Step 3: Rewrite `write_to()` with offset array**

Replace the `write_to` method in `src/storage/row.rs` (lines 20-38) with:

```rust
    /// Write row to a writer using the column-offset format.
    ///
    /// Format: [row_id:8][col_count:4][off0:2]...[offN-1:2][val0]...[valN-1]
    ///
    /// Each offset is a u16 LE relative to the start of the values area.
    pub fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<usize> {
        let col_count = self.values.len();

        // Serialize all values into a temporary buffer to compute offsets
        let mut values_buf = Vec::with_capacity(col_count * 8);
        let mut offsets: Vec<u16> = Vec::with_capacity(col_count);

        for value in &self.values {
            offsets.push(values_buf.len() as u16);
            value.write_to(&mut values_buf)?;
        }

        // Write header
        writer.write_all(&self.row_id.to_le_bytes())?;          // 8 bytes
        writer.write_all(&(col_count as u32).to_le_bytes())?;   // 4 bytes

        // Write offset array
        for off in &offsets {
            writer.write_all(&off.to_le_bytes())?;               // 2 bytes each
        }

        // Write values
        writer.write_all(&values_buf)?;

        Ok(8 + 4 + col_count * 2 + values_buf.len())
    }
```

- [ ] **Step 4: Rewrite `to_bytes()` to use write_to**

Replace the `to_bytes` method (lines 48-52) with:

```rust
    /// Serialize row to bytes
    ///
    /// Format:
    /// - Row ID: [8 bytes, u64 little-endian]
    /// - Value count: [4 bytes, u32 little-endian]
    /// - Offset array: [col_count × 2 bytes, u16 little-endian each]
    /// - Values: [serialized values concatenated]
    ///
    /// Total length prefix is NOT included (managed by DataFile)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.values.len() * 10);
        self.write_to(&mut bytes).unwrap();
        bytes
    }
```

- [ ] **Step 5: Rewrite `from_bytes()` to read offset array**

Replace the `from_bytes` method (lines 57-93) with:

```rust
    /// Deserialize row from bytes (column-offset format)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(Error::Serialization(
                "Insufficient bytes for row header".to_string(),
            ));
        }

        let row_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let value_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;

        let offsets_end = 12 + value_count * 2;
        if bytes.len() < offsets_end {
            return Err(Error::Serialization(
                "Insufficient bytes for offset array".to_string(),
            ));
        }

        // Read offset array (not strictly needed for full deserialization,
        // but we skip past it to reach the values area)
        let values_start = offsets_end;

        let mut values = Vec::with_capacity(value_count);
        let mut cursor = values_start;

        for i in 0..value_count {
            if cursor >= bytes.len() {
                return Err(Error::Serialization(format!(
                    "Unexpected end of data while reading value {} of {}",
                    i + 1,
                    value_count
                )));
            }
            let (value, consumed) = Value::from_bytes(&bytes[cursor..])?;
            values.push(value);
            cursor += consumed;
        }

        Ok(Self { row_id, values })
    }
```

- [ ] **Step 6: Add `value_at()` static method**

Add after `from_bytes()` in `src/storage/row.rs`:

```rust
    /// Extract a single column value from raw row bytes without full deserialization.
    ///
    /// Uses the column-offset array to jump directly to the target column.
    /// Does not allocate a Row or deserialize other columns.
    pub fn value_at(bytes: &[u8], col_idx: usize) -> Result<Value> {
        if bytes.len() < 12 {
            return Err(Error::Serialization(
                "Insufficient bytes for row header".to_string(),
            ));
        }

        let value_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;

        if col_idx >= value_count {
            return Err(Error::Serialization(format!(
                "Column index {} out of bounds (row has {} columns)",
                col_idx, value_count
            )));
        }

        let offsets_start = 12;
        let values_area_start = offsets_start + value_count * 2;

        // Read the offset for the target column
        let off_pos = offsets_start + col_idx * 2;
        if bytes.len() < off_pos + 2 {
            return Err(Error::Serialization(
                "Insufficient bytes for offset entry".to_string(),
            ));
        }
        let col_offset = u16::from_le_bytes(bytes[off_pos..off_pos + 2].try_into().unwrap()) as usize;

        let value_pos = values_area_start + col_offset;
        if value_pos >= bytes.len() {
            return Err(Error::Serialization(
                "Column offset points past end of row data".to_string(),
            ));
        }

        let (value, _) = Value::from_bytes(&bytes[value_pos..])?;
        Ok(value)
    }
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test --lib row::tests 2>&1 | tail -10`
Expected: all row tests pass (including the two new ones and all existing round-trip tests).

- [ ] **Step 8: Commit**

```bash
git add src/storage/row.rs
git commit -m "Add column-offset array to row format with value_at() for O(1) column access"
```

---

### Task 3: Add callback-based filtered scan to DataFile and TableEngine

**Files:**
- Modify: `src/storage/data_file.rs`
- Modify: `src/storage/table_engine.rs`

- [ ] **Step 1: Add `scan_rows_callback()` to DataFile**

Add after the existing `scan_rows()` method (around line 470) in `src/storage/data_file.rs`:

```rust
    /// Scan active rows, applying a callback filter on raw bytes.
    ///
    /// For each active row, passes the raw byte buffer to `predicate`.
    /// If it returns `true`, the row is fully deserialized and included.
    /// If `false`, the row is skipped without deserialization.
    pub fn scan_rows_callback<F>(
        &mut self,
        limit: Option<usize>,
        predicate: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let max_rows = limit.unwrap_or(usize::MAX);
        let mut results = Vec::new();
        let mut row_buffer = Vec::with_capacity(1024);

        match &mut self.backend {
            DataFileBackend::File(writer) => {
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();
                file.seek(SeekFrom::Start(0))?;

                let mut reader = BufReader::with_capacity(256 * 1024, &*file);
                loop {
                    if results.len() >= max_rows {
                        break;
                    }
                    let mut marker = [0u8; 1];
                    match reader.read_exact(&mut marker) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e.into()),
                    }

                    let mut length_buf = [0u8; 4];
                    reader.read_exact(&mut length_buf)?;
                    let length = u32::from_le_bytes(length_buf) as usize;

                    if marker[0] == TOMBSTONE_MARKER {
                        std::io::copy(
                            &mut reader.by_ref().take(length as u64),
                            &mut std::io::sink(),
                        )?;
                    } else {
                        if row_buffer.len() < length {
                            row_buffer.resize(length, 0);
                        }
                        reader.read_exact(&mut row_buffer[..length])?;
                        if predicate(&row_buffer[..length]) {
                            results.push(Row::from_bytes(&row_buffer[..length])?);
                        }
                    }
                }
            }
            DataFileBackend::Memory(data) => {
                let mut cursor = 0usize;
                while cursor < data.len() {
                    if results.len() >= max_rows {
                        break;
                    }
                    if cursor + 1 + 4 > data.len() {
                        break;
                    }
                    let marker = data[cursor];
                    cursor += 1;

                    let length = u32::from_le_bytes(
                        data[cursor..cursor + 4].try_into().unwrap(),
                    ) as usize;
                    cursor += 4;

                    if cursor + length > data.len() {
                        break;
                    }
                    if marker != TOMBSTONE_MARKER {
                        if predicate(&data[cursor..cursor + length]) {
                            results.push(Row::from_bytes(&data[cursor..cursor + length])?);
                        }
                    }
                    cursor += length;
                }
            }
        }

        Ok(results)
    }
```

- [ ] **Step 2: Add `scan_all_filtered()` to TableEngine**

Add after `scan_all_limited()` (around line 454) in `src/storage/table_engine.rs`:

```rust
    /// Scan active rows with a callback predicate on raw bytes.
    ///
    /// The predicate receives the raw serialized row bytes (without the
    /// data-file marker/length envelope). Use `Row::value_at()` inside the
    /// predicate to extract individual columns for filtering.
    pub fn scan_all_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        self.data_file.scan_rows_callback(None, predicate)
    }
```

- [ ] **Step 3: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass (new methods are additive, no existing code changed).

- [ ] **Step 4: Commit**

```bash
git add src/storage/data_file.rs src/storage/table_engine.rs
git commit -m "Add callback-based filtered scan to DataFile and TableEngine"
```

---

### Task 4: Wire filtered scan into Database::scan_with_limit()

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Update the no-index scan path to use callback filtering**

In `src/lib.rs`, in the `scan_with_limit()` method, replace the block at lines 353-363:

```rust
            } else {
                // No index: push limit into scan when no filters need post-processing
                if filters.is_empty() {
                    let scan_limit = limit.map(|l| l + offset.unwrap_or(0));
                    table_engine.scan_all_limited(scan_limit)?
                } else {
                    table_engine.scan_all()?
                }
            };
            (source, filters)
```

with:

```rust
            } else {
                if filters.is_empty() {
                    // No filters: push limit into scan
                    let scan_limit = limit.map(|l| l + offset.unwrap_or(0));
                    table_engine.scan_all_limited(scan_limit)?
                } else {
                    // Filtered scan: use callback to filter on raw bytes
                    // before full deserialization
                    let filter_col_indices: Vec<Option<usize>> = filters
                        .iter()
                        .map(|f| column_mapping.get(&f.column).copied())
                        .collect();

                    let rows = table_engine.scan_all_filtered(|raw_bytes| {
                        for (filter, col_idx) in filters.iter().zip(filter_col_indices.iter()) {
                            if let Some(idx) = col_idx {
                                match Row::value_at(raw_bytes, *idx) {
                                    Ok(val) => {
                                        if !filter.matches(&val) {
                                            return false;
                                        }
                                    }
                                    Err(_) => return false,
                                }
                            } else {
                                return false;
                            }
                        }
                        true
                    })?;
                    return Ok(apply_pagination(rows, limit, offset));
                }
            };
            (source, filters)
```

- [ ] **Step 2: Add `apply_pagination` helper**

Add a private helper function in `src/lib.rs` (outside the impl block, near the top-level helpers):

```rust
/// Apply offset and limit to a pre-filtered result set.
fn apply_pagination(rows: Vec<Row>, limit: Option<usize>, offset: Option<usize>) -> Vec<Row> {
    let offset_val = offset.unwrap_or(0);
    let limit_val = limit.unwrap_or(usize::MAX);

    rows.into_iter()
        .skip(offset_val)
        .take(limit_val)
        .collect()
}
```

- [ ] **Step 3: Add Row import if not already present**

Ensure `use crate::storage::Row;` is available in `src/lib.rs`. Check the existing imports — `Row` is likely already imported via `pub use`.

- [ ] **Step 4: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass. The filtered scan path is now used for all no-index scan queries with filters.

- [ ] **Step 5: Run the benchmark to verify improvement**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1 | tail -20`
Expected: LIKE queries on title/content should show improvement (fewer full deserializations).

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs
git commit -m "Use callback-based filtered scan for no-index queries with filters"
```

---

### Task 5: Add title index to benchmark and verify prefix LIKE index path

**Files:**
- Modify: `tests/integration/thunderdb_vs_sqlite_bench.rs`

- [ ] **Step 1: Add title index in ThunderDB setup**

In `tests/integration/thunderdb_vs_sqlite_bench.rs`, in `setup_thunderdb()`, after the existing `table.create_index("author_id").unwrap();` (around line 135), add:

```rust
        table.create_index("title").unwrap();
```

- [ ] **Step 2: Add title index in SQLite setup**

In `setup_sqlite()`, add after the existing `CREATE INDEX idx_comments_author` line:

```sql
CREATE INDEX idx_posts_title ON blog_posts(title);
```

(Append to the `execute_batch` string that creates the tables and indexes.)

- [ ] **Step 3: Run the benchmark in release mode**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1 | tail -20`
Expected: benchmark 2 (LIKE prefix on title) should show a large improvement for ThunderDB because it now uses the B-tree index instead of a full scan.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/thunderdb_vs_sqlite_bench.rs
git commit -m "Add title index to benchmark for prefix LIKE via B-tree range scan"
```

---

### Task 6: Fix any broken tests and update CHANGES.md

**Files:**
- Modify: `CHANGES.md`
- Possibly modify: test files if any tests relied on old row format byte layout

- [ ] **Step 1: Run full test suite**

Run: `cargo test 2>&1 | tail -10`
Expected: all tests pass. If any test hardcodes row byte offsets or sizes (e.g., `test_row_insufficient_bytes`), it will need updating due to the new offset array.

- [ ] **Step 2: Fix any failing tests**

If `test_row_insufficient_bytes` fails (it constructs raw bytes with `2u32.to_le_bytes()` for value_count but no offset array), update it to include the offset array or adjust the expected error.

- [ ] **Step 3: Update CHANGES.md**

Add a new entry at the top of `CHANGES.md`:

```markdown
## 2026-03-27 - Text matching performance optimizations

- **memchr SIMD LIKE matching**: `LikePattern` now uses `memchr::memmem::Finder` for `Contains` patterns with pre-built SIMD lookup tables; `Prefix`/`Suffix` use byte-slice comparison
- **Column-offset row format**: Row serialization includes a u16 offset array per row, enabling O(1) single-column access via `Row::value_at()` without deserializing the entire row
- **Callback-based filtered scan**: `DataFile::scan_rows_callback()` and `TableEngine::scan_all_filtered()` apply filters on raw bytes before full deserialization, avoiding unnecessary `Value`/`SmallString` construction for non-matching rows
- **Prefix LIKE via B-tree index**: Benchmark now creates indexes on `title` column, exercising the existing `prefix_search_by_index` path for `LIKE 'prefix%'` queries
- Added `memchr` as direct dependency
```

- [ ] **Step 4: Run full test suite one more time**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass.

- [ ] **Step 5: Run benchmark comparison**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1`
Expected: visible improvement in LIKE-related benchmarks compared to the baseline.

- [ ] **Step 6: Commit**

```bash
git add CHANGES.md
git commit -m "Update CHANGES.md with text matching optimizations"
```
