# Query Optimizer Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve query execution performance through filter cost reordering, optimized COUNT, and partial deserialization on indexed paths.

**Architecture:** Filter cost estimation drives reordering before evaluation. COUNT uses index-only counting when possible, falling back to a callback counter that avoids row materialization. Indexed fetch paths gain a predicate callback that filters on raw bytes before deserializing, using the existing `Row::value_at()` column-offset infrastructure.

**Tech Stack:** Rust, existing ThunderDB B-tree and callback scan infrastructure.

---

### Task 1: Add Filter::estimated_cost() and sort filters before evaluation

**Files:**
- Modify: `src/query/filter.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Add `estimated_cost()` method to Filter**

In `src/query/filter.rs`, add this method to the `Filter` impl block (after the `matches` method, around line 78):

```rust
    /// Estimated evaluation cost for filter reordering.
    ///
    /// Lower cost = cheaper to evaluate. Filters are sorted by cost so that
    /// cheap checks (null, integer equality) short-circuit before expensive
    /// ones (LIKE, IN with large lists).
    pub fn estimated_cost(&self) -> u8 {
        match &self.operator {
            Operator::IsNull | Operator::IsNotNull => 1,
            Operator::Equals(v) | Operator::NotEquals(v) => {
                if matches!(v, Value::Varchar(_)) { 6 } else { 2 }
            }
            Operator::GreaterThan(_)
            | Operator::GreaterThanOrEqual(_)
            | Operator::LessThan(_)
            | Operator::LessThanOrEqual(_) => 3,
            Operator::Between(_, _) => 4,
            Operator::In(_) | Operator::NotIn(_) => 5,
            Operator::Like(_) | Operator::NotLike(_) => {
                if let Some(ref pat) = self.cached_like {
                    if pat.can_use_index() { 7 } else { 8 }
                } else {
                    8
                }
            }
        }
    }
```

- [ ] **Step 2: Sort filters in scan_with_limit before the post-filter loop**

In `src/lib.rs`, in `scan_with_limit()`, find the line (around line 397):

```rust
        // Single-pass: filter + offset + limit with early termination
```

Add this line just before it:

```rust
        let mut active_filters = active_filters;
        active_filters.sort_by_key(|f| f.estimated_cost());
```

- [ ] **Step 3: Sort filters in the callback scan path too**

In `src/lib.rs`, in the callback scan path (around line 359), before building `filter_col_indices`, add:

```rust
                    let mut filters = filters;
                    filters.sort_by_key(|f| f.estimated_cost());
```

Note: `filters` is already owned (`Vec<Filter>`) so we can sort in place. Adjust the subsequent code to use the sorted `filters`.

- [ ] **Step 4: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass. Filter reordering does not change semantics — AND filters produce the same result regardless of evaluation order.

- [ ] **Step 5: Commit**

```bash
git add src/query/filter.rs src/lib.rs
git commit -m "Add filter cost estimation and sort filters before evaluation"
```

---

### Task 2: Add count_rows_callback to DataFile and count_filtered to TableEngine

**Files:**
- Modify: `src/storage/data_file.rs`
- Modify: `src/storage/table_engine.rs`

- [ ] **Step 1: Add `count_rows_callback()` to DataFile**

In `src/storage/data_file.rs`, add after the `scan_rows_callback` method:

```rust
    /// Count active rows matching a predicate without deserializing.
    ///
    /// Like `scan_rows_callback` but only counts matches instead of
    /// collecting deserialized rows. Zero allocations for row data.
    pub fn count_rows_callback<F>(&mut self, predicate: F) -> Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        let mut count = 0usize;
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
                            count += 1;
                        }
                    }
                }
            }
            DataFileBackend::Memory(data) => {
                let mut cursor = 0usize;
                while cursor < data.len() {
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
                    if marker != TOMBSTONE_MARKER && predicate(&data[cursor..cursor + length]) {
                        count += 1;
                    }
                    cursor += length;
                }
            }
        }

        Ok(count)
    }
```

- [ ] **Step 2: Add `count_filtered()` to TableEngine**

In `src/storage/table_engine.rs`, add after `scan_all_filtered`:

```rust
    /// Count active rows matching a predicate without deserializing.
    pub fn count_filtered<F>(&mut self, predicate: F) -> Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        self.data_file.count_rows_callback(predicate)
    }
```

- [ ] **Step 3: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass. New methods are additive.

- [ ] **Step 4: Commit**

```bash
git add src/storage/data_file.rs src/storage/table_engine.rs
git commit -m "Add count_rows_callback and count_filtered for zero-alloc counting"
```

---

### Task 3: Rewrite Database::count() with index-only and callback paths

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Rewrite the `count()` method**

In `src/lib.rs`, replace the `count()` method (lines 481-489) with:

```rust
    fn count(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
        let table_engine = self.get_table_mut(table)?;

        // Fast path: no filters → O(1) from RAT
        if filters.is_empty() {
            return Ok(table_engine.active_row_count());
        }

        let column_mapping = table_engine.build_column_mapping();
        let all_stats = table_engine.index_manager().all_stats();
        let stats_ref = if all_stats.is_empty() { None } else { Some(all_stats) };

        // Path A: try index-only count (no data file I/O)
        let mut remaining = Vec::new();
        let multi = multi_index_scan(
            &filters,
            table_engine.index_manager(),
            stats_ref,
            &mut remaining,
        );
        if let Some(row_ids) = multi {
            if remaining.is_empty() {
                return Ok(row_ids.len());
            }
        }

        // Single index path
        let indexed_columns = table_engine.index_manager().indexed_columns().to_vec();
        if let Some((col, op)) = choose_index(&filters, &indexed_columns, stats_ref) {
            if let Some(row_ids) = table_engine.index_manager().query_row_ids(&col, &op) {
                if filters.len() == 1 {
                    return Ok(row_ids.len());
                }
            }
        }

        // Path B: callback count (scan but no Row allocation)
        let filter_col_indices: Vec<Option<usize>> = filters
            .iter()
            .map(|f| {
                if let Some(&idx) = column_mapping.get(&f.column) {
                    Some(idx)
                } else if f.column.starts_with("col") {
                    f.column[3..].parse::<usize>().ok()
                } else {
                    None
                }
            })
            .collect();

        table_engine.count_filtered(|raw_bytes| {
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
        })
    }
```

- [ ] **Step 2: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass. COUNT behavior is unchanged — same results, faster execution.

- [ ] **Step 3: Run benchmark to verify COUNT improvement**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1 | grep "COUNT"`
Expected: benchmark 11 (COUNT WHERE author_id=2) should show dramatic improvement.

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "Optimize count() with index-only and callback paths"
```

---

### Task 4: Add read_raw() to DataFile and get_by_ids_filtered() to TableEngine

**Files:**
- Modify: `src/storage/data_file.rs`
- Modify: `src/storage/table_engine.rs`

- [ ] **Step 1: Add `read_raw()` to DataFile**

In `src/storage/data_file.rs`, add after the existing `read_row()` method:

```rust
    /// Read raw row bytes without deserializing.
    ///
    /// Like `read_row()` but returns the raw byte buffer instead of
    /// calling `Row::from_bytes()`. Returns `None` for tombstoned rows.
    pub fn read_raw(&mut self, offset: u64, length: u32) -> Result<Option<Vec<u8>>> {
        let total_to_read = 1 + 4 + length as usize;

        match &mut self.backend {
            DataFileBackend::File(writer) => {
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();
                file.seek(SeekFrom::Start(offset))?;

                if self.read_buffer.len() < total_to_read {
                    self.read_buffer.resize(total_to_read, 0);
                }

                file.read_exact(&mut self.read_buffer[..total_to_read])?;

                if self.read_buffer[0] == TOMBSTONE_MARKER {
                    return Ok(None);
                }

                let mut length_buf = [0u8; 4];
                length_buf.copy_from_slice(&self.read_buffer[1..5]);
                let stored_length = u32::from_le_bytes(length_buf);

                if stored_length != length {
                    return Err(Error::Storage(format!(
                        "Length mismatch at offset {}: expected {}, found {}",
                        offset, length, stored_length
                    )));
                }

                Ok(Some(self.read_buffer[5..total_to_read].to_vec()))
            }
            DataFileBackend::Memory(data) => {
                let start = offset as usize;
                let end = start + total_to_read;

                if end > data.len() {
                    return Err(Error::Storage(format!(
                        "Read out of bounds: {} > {}",
                        end,
                        data.len()
                    )));
                }

                let slice = &data[start..end];

                if slice[0] == TOMBSTONE_MARKER {
                    return Ok(None);
                }

                let mut length_buf = [0u8; 4];
                length_buf.copy_from_slice(&slice[1..5]);
                let stored_length = u32::from_le_bytes(length_buf);

                if stored_length != length {
                    return Err(Error::Storage(format!(
                        "Length mismatch at offset {}: expected {}, found {}",
                        offset, length, stored_length
                    )));
                }

                Ok(Some(slice[5..].to_vec()))
            }
        }
    }
```

- [ ] **Step 2: Add `get_by_ids_filtered()` to TableEngine**

In `src/storage/table_engine.rs`, add after the existing `get_by_ids()` method:

```rust
    /// Get multiple rows by ID, filtering on raw bytes before deserializing.
    ///
    /// Like `get_by_ids()` but applies a predicate to the raw row bytes.
    /// Only rows where `predicate` returns `true` are deserialized.
    /// Use `Row::value_at()` inside the predicate for column-level checks.
    pub fn get_by_ids_filtered<F>(
        &mut self,
        row_ids: &[u64],
        predicate: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(row_ids.len());
        for &row_id in row_ids {
            if let Some((offset, length)) = self.rat.get(row_id) {
                entries.push((offset, length));
            }
        }

        entries.sort_unstable_by_key(|&(offset, _)| offset);

        let mut rows = Vec::with_capacity(entries.len());
        for (offset, length) in entries {
            if let Some(raw) = self.data_file.read_raw(offset, length)? {
                if predicate(&raw) {
                    rows.push(Row::from_bytes(&raw)?);
                }
            }
        }
        Ok(rows)
    }
```

- [ ] **Step 3: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass. New methods are additive.

- [ ] **Step 4: Commit**

```bash
git add src/storage/data_file.rs src/storage/table_engine.rs
git commit -m "Add read_raw and get_by_ids_filtered for partial deserialization on indexed paths"
```

---

### Task 5: Wire get_by_ids_filtered into scan_with_limit indexed paths

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Refactor multi-index path to use filtered fetch**

In `src/lib.rs`, in `scan_with_limit()`, replace lines 325-327:

```rust
        let (source_rows, active_filters) = if let Some(row_ids) = multi_result {
            let rows = table_engine.get_by_ids(&row_ids)?;
            (rows, remaining_filters)
```

with:

```rust
        let (source_rows, active_filters) = if let Some(row_ids) = multi_result {
            if remaining_filters.is_empty() {
                let rows = table_engine.get_by_ids(&row_ids)?;
                (rows, remaining_filters)
            } else {
                let mut remaining_filters = remaining_filters;
                remaining_filters.sort_by_key(|f| f.estimated_cost());
                let rem_col_indices: Vec<Option<usize>> = remaining_filters
                    .iter()
                    .map(|f| {
                        if let Some(&idx) = column_mapping.get(&f.column) {
                            Some(idx)
                        } else if f.column.starts_with("col") {
                            f.column[3..].parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .collect();
                let rows = table_engine.get_by_ids_filtered(&row_ids, |raw_bytes| {
                    for (filter, col_idx) in remaining_filters.iter().zip(rem_col_indices.iter()) {
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
                (rows, vec![])
            }
```

- [ ] **Step 2: Refactor single-index path to use query_row_ids + filtered fetch**

In `src/lib.rs`, replace the single-index match block (lines 331-351) with:

```rust
            let source = if let Some((col, op)) = choose_index(&filters, &indexed_columns, stats_ref) {
                if let Some(row_ids) = table_engine.index_manager().query_row_ids(&col, &op) {
                    // Collect remaining filters (everything except the indexed one)
                    let mut remaining: Vec<Filter> = filters
                        .iter()
                        .filter(|f| f.column != col || f.operator != op)
                        .cloned()
                        .collect();

                    if remaining.is_empty() {
                        table_engine.get_by_ids(&row_ids)?
                    } else {
                        remaining.sort_by_key(|f| f.estimated_cost());
                        let rem_col_indices: Vec<Option<usize>> = remaining
                            .iter()
                            .map(|f| {
                                if let Some(&idx) = column_mapping.get(&f.column) {
                                    Some(idx)
                                } else if f.column.starts_with("col") {
                                    f.column[3..].parse::<usize>().ok()
                                } else {
                                    None
                                }
                            })
                            .collect();
                        table_engine.get_by_ids_filtered(&row_ids, |raw_bytes| {
                            for (filter, col_idx) in remaining.iter().zip(rem_col_indices.iter()) {
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
                        })?
                    }
                } else {
                    // Index query failed, fall back to the old path
                    match op {
                        Operator::Equals(val) => table_engine.search_by_index(&col, &val)?,
                        Operator::Between(start, end) => table_engine.range_search_by_index(&col, &start, &end)?,
                        Operator::GreaterThan(val) => table_engine.greater_than_by_index(&col, &val, false)?,
                        Operator::GreaterThanOrEqual(val) => table_engine.greater_than_by_index(&col, &val, true)?,
                        Operator::LessThan(val) => table_engine.less_than_by_index(&col, &val, false)?,
                        Operator::LessThanOrEqual(val) => table_engine.less_than_by_index(&col, &val, true)?,
                        Operator::Like(pattern) => {
                            use crate::index::LikePattern;
                            if let Ok(lp) = LikePattern::parse(&pattern) {
                                if let Some(prefix) = lp.get_prefix() {
                                    table_engine.prefix_search_by_index(&col, prefix)?
                                } else {
                                    table_engine.scan_all()?
                                }
                            } else {
                                table_engine.scan_all()?
                            }
                        }
                        _ => table_engine.scan_all()?,
                    }
                }
```

Note: the `remaining` filters have already been applied in the predicate, so we pass `vec![]` as `active_filters` when filtered fetch was used. Adjust the `(source, filters)` tuple accordingly — when `remaining` was consumed, return `(source, vec![])`.

- [ ] **Step 3: Run all tests**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass.

- [ ] **Step 4: Run benchmark**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1 | tail -20`
Expected: improvements on indexed paths (benchmarks 4, 8, 9, 11).

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "Wire filtered fetch into indexed scan paths for partial deserialization"
```

---

### Task 6: Update CHANGES.md and run final benchmark

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Run full test suite**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass.

- [ ] **Step 2: Update CHANGES.md**

Add at the top of `CHANGES.md`:

```markdown
## 2026-03-27 - Query optimizer improvements

- **Filter cost-based reordering**: Filters sorted by estimated cost before evaluation — cheap checks (null, integer equality) short-circuit before expensive ones (LIKE, IN)
- **Optimized COUNT with filters**: Index-only counting when all filters are indexable (zero data file I/O); callback counting for non-indexed filters (no row materialization)
- **Partial deserialization on indexed paths**: `get_by_ids_filtered()` applies remaining filters via `Row::value_at()` on raw bytes before full deserialization, avoiding unnecessary column construction
- **New methods**: `DataFile::read_raw()`, `DataFile::count_rows_callback()`, `TableEngine::get_by_ids_filtered()`, `TableEngine::count_filtered()`, `Filter::estimated_cost()`
```

- [ ] **Step 3: Run benchmark in release mode**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1`
Expected: visible improvements across COUNT and indexed benchmarks.

- [ ] **Step 4: Commit**

```bash
git add CHANGES.md
git commit -m "Update CHANGES.md with query optimizer improvements"
```
