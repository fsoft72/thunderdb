# SP3b: Write-Path Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse per-row page I/O to per-page I/O for bulk UPDATE/DELETE, add in-place slot overwrite, and skip B-tree index ops when indexed column values are unchanged.

**Architecture:** Five layers top-to-bottom: `Page` gets in-place overwrite; `PagedTable` gets `update_batch`/`delete_batch` that group mutations by page_id (one read+write per page); `IndexManager` gets `delete_rows_batch` mirroring existing `insert_rows_batch`; `TableEngine` wires the layers with smart index skip (skip index update when indexed col value unchanged and row stayed in-place); `lib.rs` `update()`/`delete()` switch from per-row loops to batch calls.

**Tech Stack:** Rust, existing `Page`/`PagedTable`/`IndexManager`/`TableEngine` abstractions, existing `toast` module, `BTreeMap` for page grouping.

---

## File Map

| File | Changes |
|---|---|
| `src/storage/page.rs` | Add `Page::update_row_inplace` |
| `src/storage/paged_table.rs` | Add `BatchUpdateOutcome` enum, `update_batch`, `delete_batch` |
| `src/index/manager.rs` | Add `IndexManager::delete_rows_batch` |
| `src/storage/table_engine.rs` | Add `TableEngine::update_batch`, `delete_batch` |
| `src/lib.rs` | Replace per-row loops in `update()` / `delete()` |
| `tests/perf/vs_sqlite_write.rs` | Rerun benchmarks; promote new baseline |
| `CHANGES.md` | SP3b entry |

---

## Task 1: `Page::update_row_inplace`

**Files:**
- Modify: `src/storage/page.rs` (add method to `impl Page`, add tests to `#[cfg(test)]` module)

---

- [ ] **Step 1: Write the failing tests**

Add inside the existing `#[cfg(test)]` block at the bottom of `src/storage/page.rs`:

```rust
#[test]
fn test_update_inplace_fits() {
    let mut page = Page::new(1);
    let slot = page.insert_row(b"hello world").unwrap();
    let updated = page.update_row_inplace(slot, b"hi!");
    assert!(updated);
    assert_eq!(page.get_row(slot).unwrap(), b"hi!" as &[u8]);
    assert_eq!(page.active_count(), 1); // unchanged
}

#[test]
fn test_update_inplace_exact_size() {
    let mut page = Page::new(1);
    let slot = page.insert_row(b"abcde").unwrap();
    let updated = page.update_row_inplace(slot, b"ABCDE");
    assert!(updated);
    assert_eq!(page.get_row(slot).unwrap(), b"ABCDE" as &[u8]);
}

#[test]
fn test_update_inplace_too_large() {
    let mut page = Page::new(1);
    let slot = page.insert_row(b"hi").unwrap();
    let updated = page.update_row_inplace(slot, b"this is much larger than two bytes");
    assert!(!updated);
    // original data untouched
    assert_eq!(page.get_row(slot).unwrap(), b"hi" as &[u8]);
}

#[test]
fn test_update_inplace_freed_slot() {
    let mut page = Page::new(1);
    let slot = page.insert_row(b"data").unwrap();
    page.delete_row(slot);
    assert!(!page.update_row_inplace(slot, b"new"));
}

#[test]
fn test_update_inplace_out_of_bounds() {
    let mut page = Page::new(1);
    assert!(!page.update_row_inplace(99, b"data"));
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cargo test -p thunderdb test_update_inplace 2>&1 | grep -E "FAILED|error\[|^error"
```

Expected: compile error — `update_row_inplace` not found.

- [ ] **Step 3: Implement `Page::update_row_inplace`**

Add this method inside `impl Page` in `src/storage/page.rs`, after `delete_row`:

```rust
/// Overwrite a slot's data in-place when `new_data` fits within the existing slot length.
///
/// Returns `true` if updated; `false` if `new_data.len()` exceeds the slot length,
/// the slot is freed, or `slot_index` is out of bounds.
/// Dead bytes at the tail are abandoned (no page compaction).
pub fn update_row_inplace(&mut self, slot_index: u16, new_data: &[u8]) -> bool {
    if slot_index >= self.header.slot_count {
        return false;
    }

    let slot_pos = PAGE_HEADER_SIZE + slot_index as usize * SLOT_SIZE;
    let offset = u16::from_le_bytes(
        self.data[slot_pos..slot_pos + 2].try_into().unwrap(),
    );

    if offset == INVALID_SLOT {
        return false;
    }

    let old_len = u16::from_le_bytes(
        self.data[slot_pos + 2..slot_pos + 4].try_into().unwrap(),
    ) as usize;

    if new_data.len() > old_len {
        return false;
    }

    let start = offset as usize;
    self.data[start..start + new_data.len()].copy_from_slice(new_data);
    self.data[slot_pos + 2..slot_pos + 4]
        .copy_from_slice(&(new_data.len() as u16).to_le_bytes());

    true
}
```

- [ ] **Step 4: Verify tests pass**

```bash
cargo test -p thunderdb test_update_inplace 2>&1 | tail -5
```

Expected: `5 passed`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/page.rs
git commit -m "feat(page): add update_row_inplace for in-place slot overwrite"
```

---

## Task 2: `PagedTable::delete_batch`

**Files:**
- Modify: `src/storage/paged_table.rs` (add method + tests)

---

- [ ] **Step 1: Write the failing tests**

Add inside the `#[cfg(test)]` block at the bottom of `src/storage/paged_table.rs`:

```rust
#[test]
fn test_delete_batch_basic() {
    let path = temp_path("test_delete_batch_basic.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();

    let mut ctids = Vec::new();
    for i in 0..10i32 {
        ctids.push(pt.insert_row(&[Value::Int32(i)]).unwrap());
    }
    assert_eq!(pt.active_row_count(), 10);

    let deleted = pt.delete_batch(&ctids[..5]).unwrap();
    assert_eq!(deleted, 5);
    assert_eq!(pt.active_row_count(), 5);

    for ctid in &ctids[..5] {
        assert!(pt.get_row(*ctid).unwrap().is_none());
    }
    for ctid in &ctids[5..] {
        assert!(pt.get_row(*ctid).unwrap().is_some());
    }
    cleanup(&path);
}

#[test]
fn test_delete_batch_across_pages() {
    let path = temp_path("test_delete_batch_pages.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();

    let mut ctids = Vec::new();
    for i in 0..200i32 {
        ctids.push(pt.insert_row(&[Value::Int32(i), Value::varchar(format!("row_{:0>60}", i))]).unwrap());
    }

    let to_delete: Vec<Ctid> = ctids.iter().step_by(2).copied().collect();
    let deleted = pt.delete_batch(&to_delete).unwrap();
    assert_eq!(deleted, 100);
    assert_eq!(pt.active_row_count(), 100);
    cleanup(&path);
}

#[test]
fn test_delete_batch_empty() {
    let path = temp_path("test_delete_batch_empty.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();
    let deleted = pt.delete_batch(&[]).unwrap();
    assert_eq!(deleted, 0);
    cleanup(&path);
}

#[test]
fn test_delete_batch_double_delete() {
    let path = temp_path("test_delete_batch_double.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();
    let ctid = pt.insert_row(&[Value::Int32(1)]).unwrap();
    // Delete same ctid twice in one batch — second should be no-op
    let deleted = pt.delete_batch(&[ctid, ctid]).unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(pt.active_row_count(), 0);
    cleanup(&path);
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cargo test -p thunderdb test_delete_batch 2>&1 | grep -E "FAILED|error\[|^error"
```

Expected: compile error — `delete_batch` not found.

- [ ] **Step 3: Implement `PagedTable::delete_batch`**

Add this method inside `impl PagedTable` in `src/storage/paged_table.rs`, after `update_row`:

```rust
/// Delete multiple rows grouped by page_id — one read+write per page.
///
/// Returns the number of rows actually deleted (already-freed slots are skipped).
pub fn delete_batch(&mut self, ctids: &[Ctid]) -> Result<usize> {
    if ctids.is_empty() {
        return Ok(0);
    }

    // Group slot indices by page_id in sorted order for sequential I/O
    let mut by_page: std::collections::BTreeMap<u32, Vec<u16>> =
        std::collections::BTreeMap::new();
    for ctid in ctids {
        by_page.entry(ctid.page_id).or_default().push(ctid.slot_index);
    }

    let mut total_deleted = 0usize;

    for (page_id, slots) in by_page {
        if page_id >= self.page_file.page_count() {
            continue;
        }

        let mut page = self.page_file.read_page(page_id)?;
        let mut page_deleted = 0usize;

        for slot in slots {
            // Extract raw bytes before deleting the slot (needed for TOAST cleanup)
            let raw = page.get_row(slot).map(|b| b.to_vec());

            if let Some(ref raw_bytes) = raw {
                // Free any overflow pages referenced by TOAST pointers.
                // free_toast_data writes only to overflow pages (not this data page),
                // so `page` remains valid after the call.
                toast::free_toast_data(raw_bytes, &mut self.page_file)?;
            }

            if page.delete_row(slot) {
                page_deleted += 1;
            }
        }

        if page_deleted > 0 {
            self.page_file.write_page(&page)?;
            self.page_file.update_fsm(page_id, page.free_space())?;
            self.active_count -= page_deleted as u64;
            total_deleted += page_deleted;
        }
    }

    Ok(total_deleted)
}
```

- [ ] **Step 4: Verify tests pass**

```bash
cargo test -p thunderdb test_delete_batch 2>&1 | tail -5
```

Expected: `4 passed`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/paged_table.rs
git commit -m "feat(paged_table): add delete_batch — one page read+write per page"
```

---

## Task 3: `PagedTable::update_batch`

**Files:**
- Modify: `src/storage/paged_table.rs` (add `BatchUpdateOutcome` enum + `update_batch` method + tests)

---

- [ ] **Step 1: Write the failing tests**

Add inside the `#[cfg(test)]` block at the bottom of `src/storage/paged_table.rs`:

```rust
#[test]
fn test_update_batch_all_inplace() {
    let path = temp_path("test_update_batch_inplace.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();

    let mut ctids = Vec::new();
    for i in 0..5i32 {
        ctids.push(pt.insert_row(&[Value::Int32(i), Value::Int32(100)]).unwrap());
    }

    // Same-size update: Int32 -> Int32, all should be in-place
    let mutations: Vec<(Ctid, Vec<Value>)> = ctids.iter().enumerate()
        .map(|(i, &ctid)| (ctid, vec![Value::Int32(i as i32 * 10), Value::Int32(200)]))
        .collect();
    let outcomes = pt.update_batch(&mutations).unwrap();

    for (i, outcome) in outcomes.iter().enumerate() {
        match outcome {
            BatchUpdateOutcome::InPlace(ctid) => assert_eq!(*ctid, ctids[i]),
            BatchUpdateOutcome::Relocated(_) => panic!("expected in-place for row {}", i),
        }
    }
    assert_eq!(pt.active_row_count(), 5);

    // Verify updated values
    for (i, &ctid) in ctids.iter().enumerate() {
        let row = pt.get_row(ctid).unwrap().unwrap();
        assert_eq!(row.values[0], Value::Int32(i as i32 * 10));
        assert_eq!(row.values[1], Value::Int32(200));
    }
    cleanup(&path);
}

#[test]
fn test_update_batch_all_relocate() {
    let path = temp_path("test_update_batch_relocate.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();

    let ctid = pt.insert_row(&[Value::Int32(1)]).unwrap();

    // New row is larger than old: forces relocation
    let large_val = "x".repeat(500);
    let mutations = vec![(ctid, vec![Value::Int32(1), Value::varchar(large_val.clone())])];
    let outcomes = pt.update_batch(&mutations).unwrap();

    match outcomes[0] {
        BatchUpdateOutcome::Relocated(new_ctid) => {
            assert_ne!(new_ctid, ctid);
            let row = pt.get_row(new_ctid).unwrap().unwrap();
            assert_eq!(row.values[1], Value::varchar(large_val));
        }
        BatchUpdateOutcome::InPlace(_) => panic!("expected relocation"),
    }
    assert_eq!(pt.active_row_count(), 1);
    cleanup(&path);
}

#[test]
fn test_update_batch_empty() {
    let path = temp_path("test_update_batch_empty.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();
    let outcomes = pt.update_batch(&[]).unwrap();
    assert!(outcomes.is_empty());
    cleanup(&path);
}

#[test]
fn test_update_batch_active_count_stable() {
    let path = temp_path("test_update_batch_count.pages");
    cleanup(&path);
    let mut pt = PagedTable::open(&path).unwrap();

    let mut ctids = Vec::new();
    for i in 0..10i32 {
        ctids.push(pt.insert_row(&[Value::Int32(i)]).unwrap());
    }
    assert_eq!(pt.active_row_count(), 10);

    let mutations: Vec<(Ctid, Vec<Value>)> = ctids.iter()
        .map(|&ctid| (ctid, vec![Value::Int32(999)]))
        .collect();
    pt.update_batch(&mutations).unwrap();

    assert_eq!(pt.active_row_count(), 10); // must be stable
    cleanup(&path);
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cargo test -p thunderdb test_update_batch 2>&1 | grep -E "FAILED|error\[|^error"
```

Expected: compile error — `BatchUpdateOutcome` and `update_batch` not found.

- [ ] **Step 3: Implement `BatchUpdateOutcome` and `update_batch`**

Add the enum just before `impl PagedTable` in `src/storage/paged_table.rs`:

```rust
/// Result of a single row mutation in `update_batch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchUpdateOutcome {
    /// Row was overwritten in its existing slot; ctid is unchanged.
    InPlace(Ctid),
    /// Row did not fit in the old slot; old slot was freed and a new one allocated.
    Relocated(Ctid),
}
```

Add this method inside `impl PagedTable`, after `delete_batch`:

```rust
/// Update multiple rows grouped by page_id — one read+write per page.
///
/// Tries in-place overwrite (new serialized bytes ≤ old slot length and
/// ≤ TOAST_THRESHOLD). Rows that do not fit are deleted from their page
/// and batch-inserted at the end via `insert_batch`.
///
/// Returns one `BatchUpdateOutcome` per input mutation, in the same order.
pub fn update_batch(
    &mut self,
    mutations: &[(Ctid, Vec<Value>)],
) -> Result<Vec<BatchUpdateOutcome>> {
    if mutations.is_empty() {
        return Ok(Vec::new());
    }

    let mut outcomes: Vec<Option<BatchUpdateOutcome>> = vec![None; mutations.len()];

    // Group mutation indices by page_id (sorted for sequential I/O)
    let mut by_page: std::collections::BTreeMap<u32, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, (ctid, _)) in mutations.iter().enumerate() {
        by_page.entry(ctid.page_id).or_default().push(i);
    }

    let mut relocate_indices: Vec<usize> = Vec::new();

    for (page_id, indices) in &by_page {
        let page_id = *page_id;
        if page_id >= self.page_file.page_count() {
            // Page doesn't exist; all mutations for it must relocate
            for &i in indices {
                relocate_indices.push(i);
            }
            continue;
        }

        let mut page = self.page_file.read_page(page_id)?;
        let mut n_deleted = 0usize;

        for &i in indices {
            let (ctid, new_values) = &mutations[i];
            let new_bytes = serialize_row_for_page(new_values);

            // Determine old slot length from the in-memory page
            let old_slot_len = page.get_row(ctid.slot_index)
                .map(|b| b.len())
                .unwrap_or(0);

            // In-place is eligible when the new serialized row fits in the old slot
            // and does not need TOAST (keeps the data page self-contained).
            let can_inplace = old_slot_len > 0
                && new_bytes.len() <= old_slot_len
                && new_bytes.len() <= TOAST_THRESHOLD;

            // Free old TOAST overflow pages before overwriting or deleting the slot.
            // free_toast_data only writes to overflow pages, leaving `page` valid.
            if let Some(raw) = page.get_row(ctid.slot_index).map(|b| b.to_vec()) {
                toast::free_toast_data(&raw, &mut self.page_file)?;
            }

            if can_inplace {
                page.update_row_inplace(ctid.slot_index, &new_bytes);
                outcomes[i] = Some(BatchUpdateOutcome::InPlace(*ctid));
            } else {
                page.delete_row(ctid.slot_index);
                n_deleted += 1;
                relocate_indices.push(i);
            }
        }

        self.page_file.write_page(&page)?;
        self.page_file.update_fsm(page_id, page.free_space())?;
        if n_deleted > 0 {
            self.active_count -= n_deleted as u64;
        }
    }

    // Batch-insert all rows that could not be updated in-place
    if !relocate_indices.is_empty() {
        let reloc_values: Vec<Vec<Value>> = relocate_indices.iter()
            .map(|&i| mutations[i].1.clone())
            .collect();
        let new_ctids = self.insert_batch(&reloc_values)?;
        for (&i, &new_ctid) in relocate_indices.iter().zip(new_ctids.iter()) {
            outcomes[i] = Some(BatchUpdateOutcome::Relocated(new_ctid));
        }
    }

    Ok(outcomes.into_iter().map(|o| o.unwrap()).collect())
}
```

- [ ] **Step 4: Verify tests pass**

```bash
cargo test -p thunderdb test_update_batch 2>&1 | tail -5
```

Expected: `4 passed`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/paged_table.rs
git commit -m "feat(paged_table): add update_batch — in-place overwrite + page-level batching"
```

---

## Task 4: `IndexManager::delete_rows_batch`

**Files:**
- Modify: `src/index/manager.rs` (add method + tests)

---

- [ ] **Step 1: Write the failing tests**

Add inside the `#[cfg(test)]` block at the bottom of `src/index/manager.rs`:

```rust
#[test]
fn test_delete_rows_batch_basic() {
    let mut mgr = create_test_manager("batch_del_basic");
    let mapping = create_column_mapping();

    mgr.create_index("id").unwrap();
    mgr.create_index("age").unwrap();

    for i in 1u64..=5 {
        let row = create_test_row(i, i as i32 * 10, "User", i as i32 * 5);
        mgr.insert_row(&row, &mapping).unwrap();
    }

    // Delete rows 2 and 4
    let deletions: Vec<(u64, Vec<Value>)> = vec![
        (2, vec![Value::Int32(20), Value::varchar("User"), Value::Int32(10)]),
        (4, vec![Value::Int32(40), Value::varchar("User"), Value::Int32(20)]),
    ];
    mgr.delete_rows_batch(&deletions, &mapping).unwrap();

    assert!(mgr.search("id", &Value::Int32(20)).unwrap().is_empty());
    assert!(mgr.search("id", &Value::Int32(40)).unwrap().is_empty());
    assert_eq!(mgr.search("id", &Value::Int32(10)).unwrap(), vec![1]);
    assert_eq!(mgr.search("id", &Value::Int32(30)).unwrap(), vec![3]);
}

#[test]
fn test_delete_rows_batch_empty() {
    let mut mgr = create_test_manager("batch_del_empty");
    let mapping = create_column_mapping();
    mgr.create_index("id").unwrap();
    // Should not panic or error on empty input
    mgr.delete_rows_batch(&[], &mapping).unwrap();
}

#[test]
fn test_delete_rows_batch_all_rows() {
    let mut mgr = create_test_manager("batch_del_all");
    let mapping = create_column_mapping();
    mgr.create_index("id").unwrap();

    let mut rows_to_delete = Vec::new();
    for i in 1u64..=10 {
        let row = create_test_row(i, i as i32, "User", 20);
        mgr.insert_row(&row, &mapping).unwrap();
        rows_to_delete.push((i, vec![Value::Int32(i as i32), Value::varchar("User"), Value::Int32(20)]));
    }

    mgr.delete_rows_batch(&rows_to_delete, &mapping).unwrap();

    for i in 1i32..=10 {
        assert!(mgr.search("id", &Value::Int32(i)).unwrap().is_empty());
    }
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cargo test -p thunderdb test_delete_rows_batch 2>&1 | grep -E "FAILED|error\[|^error"
```

Expected: compile error — `delete_rows_batch` not found.

- [ ] **Step 3: Implement `IndexManager::delete_rows_batch`**

Add this method inside `impl IndexManager` in `src/index/manager.rs`, after `delete_row`:

```rust
/// Remove multiple rows from all relevant indices in batch.
///
/// Per indexed column: collects all (value, row_id) pairs, sorts by value
/// for B-tree traversal locality, then deletes in sorted order.
/// Mirrors `insert_rows_batch`.
///
/// # Arguments
/// * `deletions` - (row_id, old_values) pairs to remove
/// * `column_mapping` - maps column names to positions in old_values
pub fn delete_rows_batch(
    &mut self,
    deletions: &[(u64, Vec<Value>)],
    column_mapping: &HashMap<String, usize>,
) -> Result<()> {
    if deletions.is_empty() {
        return Ok(());
    }

    for column_name in &self.indexed_columns.clone() {
        if let Some(&col_idx) = column_mapping.get(column_name) {
            let mut entries: Vec<(Value, u64)> = Vec::with_capacity(deletions.len());
            for (row_id, values) in deletions {
                if let Some(value) = values.get(col_idx) {
                    entries.push((value.clone(), *row_id));
                }
            }

            // Sort by key for B-tree traversal locality (mirrors insert_rows_batch)
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            if let Some(index) = self.indices.get_mut(column_name) {
                for (value, row_id) in &entries {
                    index.delete(value, row_id);
                }
            }

            if let Some(stats) = self.stats_cache.get_mut(column_name) {
                for _ in &entries {
                    stats.record_delete();
                }
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 4: Verify tests pass**

```bash
cargo test -p thunderdb test_delete_rows_batch 2>&1 | tail -5
```

Expected: `3 passed`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/index/manager.rs
git commit -m "feat(index): add delete_rows_batch — sorted batch B-tree deletion"
```

---

## Task 5: `TableEngine::delete_batch`

**Files:**
- Modify: `src/storage/table_engine.rs` (add method + tests)

---

- [ ] **Step 1: Write the failing tests**

Add inside the `#[cfg(test)]` block at the bottom of `src/storage/table_engine.rs`:

```rust
#[test]
fn test_engine_delete_batch_no_index() {
    let mut table = create_test_table("te_delete_batch_noidx");
    let mut ids = Vec::new();
    for i in 0..10i32 {
        ids.push(table.insert_row(vec![Value::Int32(i)]).unwrap());
    }

    let deletions: Vec<(u64, Vec<Value>)> = ids[..5].iter()
        .enumerate()
        .map(|(i, &id)| (id, vec![Value::Int32(i as i32)]))
        .collect();
    let count = table.delete_batch(&deletions).unwrap();

    assert_eq!(count, 5);
    assert_eq!(table.active_row_count(), 5);
    for &id in &ids[..5] {
        assert!(table.get_by_id(id).unwrap().is_none());
    }
}

#[test]
fn test_engine_delete_batch_with_index() {
    let mut table = create_test_table("te_delete_batch_idx");

    // Set schema so IndexManager can find columns by name
    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
        ],
    }).unwrap();
    table.create_index("id").unwrap();

    let mut ids = Vec::new();
    let mut values_map = Vec::new();
    for i in 0..5i32 {
        let vals = vec![Value::Int32(i), Value::varchar(format!("user_{}", i))];
        ids.push(table.insert_row(vals.clone()).unwrap());
        values_map.push(vals);
    }

    // Delete rows 1 and 3
    let deletions = vec![
        (ids[1], values_map[1].clone()),
        (ids[3], values_map[3].clone()),
    ];
    let count = table.delete_batch(&deletions).unwrap();
    assert_eq!(count, 2);
    assert_eq!(table.active_row_count(), 3);

    // Index must no longer return deleted rows
    let found = table.search_by_index("id", &Value::Int32(1)).unwrap();
    assert!(found.is_empty());
    let found = table.search_by_index("id", &Value::Int32(3)).unwrap();
    assert!(found.is_empty());

    // Surviving rows still in index
    let found = table.search_by_index("id", &Value::Int32(0)).unwrap();
    assert_eq!(found.len(), 1);
}

#[test]
fn test_engine_delete_batch_empty() {
    let mut table = create_test_table("te_delete_batch_empty");
    let count = table.delete_batch(&[]).unwrap();
    assert_eq!(count, 0);
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cargo test -p thunderdb test_engine_delete_batch 2>&1 | grep -E "FAILED|error\[|^error"
```

Expected: compile error — `delete_batch` not found on `TableEngine`.

- [ ] **Step 3: Implement `TableEngine::delete_batch`**

Add this method inside `impl TableEngine` in `src/storage/table_engine.rs`, after `delete_by_id`:

```rust
/// Delete multiple rows in batch.
///
/// Uses `PagedTable::delete_batch` for page-level I/O efficiency, then
/// removes all index entries via `IndexManager::delete_rows_batch`.
///
/// # Arguments
/// * `deletions` - (row_id, old_values) pairs; old_values are used for index cleanup
///
/// # Returns
/// Number of rows actually deleted
pub fn delete_batch(&mut self, deletions: &[(u64, Vec<Value>)]) -> Result<usize> {
    if deletions.is_empty() {
        return Ok(0);
    }

    let ctids: Vec<crate::storage::page::Ctid> = deletions.iter()
        .map(|(row_id, _)| crate::storage::page::Ctid::from_u64(*row_id))
        .collect();

    let deleted = self.paged_table.delete_batch(&ctids)?;

    if deleted > 0 && !self.index_manager.indexed_columns().is_empty() {
        let mapping = self.build_column_mapping();
        self.index_manager.delete_rows_batch(deletions, &mapping)?;
    }

    Ok(deleted)
}
```

- [ ] **Step 4: Verify tests pass**

```bash
cargo test -p thunderdb test_engine_delete_batch 2>&1 | tail -5
```

Expected: `3 passed`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/table_engine.rs
git commit -m "feat(table_engine): add delete_batch — page-level batch + index cleanup"
```

---

## Task 6: `TableEngine::update_batch`

**Files:**
- Modify: `src/storage/table_engine.rs` (add method + tests)

---

- [ ] **Step 1: Write the failing tests**

Add inside the `#[cfg(test)]` block at the bottom of `src/storage/table_engine.rs`:

```rust
#[test]
fn test_engine_update_batch_no_index() {
    let mut table = create_test_table("te_update_batch_noidx");

    let mut ids = Vec::new();
    for i in 0..5i32 {
        ids.push(table.insert_row(vec![Value::Int32(i), Value::Int32(0)]).unwrap());
    }

    let updates: Vec<(u64, Vec<Value>, Vec<Value>)> = ids.iter().enumerate()
        .map(|(i, &id)| (id,
            vec![Value::Int32(i as i32), Value::Int32(0)],
            vec![Value::Int32(i as i32), Value::Int32(99)],
        ))
        .collect();

    let count = table.update_batch(&updates).unwrap();
    assert_eq!(count, 5);
    assert_eq!(table.active_row_count(), 5);

    for (i, &id) in ids.iter().enumerate() {
        // row_id may have changed if relocated; scan to verify values
        let _ = i; // suppress unused warning
        let _ = id;
    }
    // Verify via scan
    let rows = table.scan_all().unwrap();
    let mut vals: Vec<i32> = rows.iter().map(|r| {
        if let Value::Int32(v) = r.values[1] { v } else { panic!() }
    }).collect();
    vals.sort();
    assert!(vals.iter().all(|&v| v == 99));
}

#[test]
fn test_engine_update_batch_with_index_skip() {
    let mut table = create_test_table("te_update_batch_skip");

    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "content".to_string(), data_type: "VARCHAR".to_string() },
        ],
    }).unwrap();
    table.create_index("id").unwrap();

    let mut ids = Vec::new();
    for i in 0..5i32 {
        ids.push(table.insert_row(vec![Value::Int32(i), Value::varchar("old")]).unwrap());
    }

    // Update non-indexed `content` column only — id unchanged
    let updates: Vec<(u64, Vec<Value>, Vec<Value>)> = ids.iter().enumerate()
        .map(|(i, &id)| (id,
            vec![Value::Int32(i as i32), Value::varchar("old")],
            vec![Value::Int32(i as i32), Value::varchar("new")],
        ))
        .collect();

    let count = table.update_batch(&updates).unwrap();
    assert_eq!(count, 5);

    // Index still returns correct rows (id values unchanged, index must be intact)
    for i in 0..5i32 {
        let found = table.search_by_index("id", &Value::Int32(i)).unwrap();
        assert_eq!(found.len(), 1, "id {} should still be indexed", i);
    }
}

#[test]
fn test_engine_update_batch_indexed_col_changes() {
    let mut table = create_test_table("te_update_batch_idxchange");

    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
        ],
    }).unwrap();
    table.create_index("id").unwrap();

    let id = table.insert_row(vec![Value::Int32(1)]).unwrap();

    // Update the indexed column: old=1, new=999
    let updates = vec![(id,
        vec![Value::Int32(1)],
        vec![Value::Int32(999)],
    )];
    table.update_batch(&updates).unwrap();

    // Old index entry must be gone
    assert!(table.search_by_index("id", &Value::Int32(1)).unwrap().is_empty());
    // New index entry must exist
    assert_eq!(table.search_by_index("id", &Value::Int32(999)).unwrap().len(), 1);
}

#[test]
fn test_engine_update_batch_empty() {
    let mut table = create_test_table("te_update_batch_empty");
    let count = table.update_batch(&[]).unwrap();
    assert_eq!(count, 0);
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cargo test -p thunderdb test_engine_update_batch 2>&1 | grep -E "FAILED|error\[|^error"
```

Expected: compile error — `update_batch` not found on `TableEngine`.

- [ ] **Step 3: Implement `TableEngine::update_batch`**

Add this method inside `impl TableEngine` in `src/storage/table_engine.rs`, after `delete_batch`:

```rust
/// Update multiple rows in batch with smart index skip.
///
/// Uses `PagedTable::update_batch` for page-level I/O efficiency.
/// Index maintenance is skipped for rows where all indexed column values
/// are unchanged and the row was updated in-place (same row_id).
///
/// # Arguments
/// * `updates` - (row_id, old_values, new_values) triples
///
/// # Returns
/// Number of rows processed
pub fn update_batch(
    &mut self,
    updates: &[(u64, Vec<Value>, Vec<Value>)],
) -> Result<usize> {
    if updates.is_empty() {
        return Ok(0);
    }

    let mutations: Vec<(crate::storage::page::Ctid, Vec<Value>)> = updates.iter()
        .map(|(row_id, _, new_values)| {
            (crate::storage::page::Ctid::from_u64(*row_id), new_values.clone())
        })
        .collect();

    let outcomes = self.paged_table.update_batch(&mutations)?;

    if !self.index_manager.indexed_columns().is_empty() {
        let mapping = self.build_column_mapping();
        let indexed_cols: Vec<String> = self.index_manager.indexed_columns().to_vec();

        let mut index_deletes: Vec<(u64, Vec<Value>)> = Vec::new();
        let mut index_inserts: Vec<crate::storage::Row> = Vec::new();

        for (i, (row_id, old_values, new_values)) in updates.iter().enumerate() {
            let outcome = &outcomes[i];
            let new_row_id = match outcome {
                crate::storage::paged_table::BatchUpdateOutcome::InPlace(ctid) => ctid.to_u64(),
                crate::storage::paged_table::BatchUpdateOutcome::Relocated(ctid) => ctid.to_u64(),
            };
            let is_inplace = matches!(
                outcome,
                crate::storage::paged_table::BatchUpdateOutcome::InPlace(_)
            );

            // Skip index maintenance only when: in-place AND no indexed column changed
            let needs_index_update = !is_inplace || indexed_cols.iter().any(|col_name| {
                if let Some(&col_idx) = mapping.get(col_name) {
                    old_values.get(col_idx) != new_values.get(col_idx)
                } else {
                    false
                }
            });

            if needs_index_update {
                index_deletes.push((*row_id, old_values.clone()));
                index_inserts.push(crate::storage::Row::new(new_row_id, new_values.clone()));
            }
        }

        if !index_deletes.is_empty() {
            self.index_manager.delete_rows_batch(&index_deletes, &mapping)?;
            self.index_manager.insert_rows_batch(&index_inserts, &mapping)?;
        }
    }

    Ok(updates.len())
}
```

- [ ] **Step 4: Verify tests pass**

```bash
cargo test -p thunderdb test_engine_update_batch 2>&1 | tail -5
```

Expected: `4 passed`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/table_engine.rs
git commit -m "feat(table_engine): add update_batch — in-place + batch + smart index skip"
```

---

## Task 7: Wire `lib.rs` — replace per-row loops with batch calls

**Files:**
- Modify: `src/lib.rs` (lines ~609-674, `update()` and `delete()` private functions)

---

- [ ] **Step 1: Locate the current per-row loops**

Read `src/lib.rs` lines 609-674. They should look like:

```rust
fn update(&mut self, table: &str, filters: Vec<Filter>, updates: Vec<(String, Value)>) -> Result<usize> {
    let rows = self.scan(table, filters)?;
    if rows.is_empty() { return Ok(0); }
    let table_engine = self.get_table_mut(table)?;
    let column_mapping = table_engine.build_column_mapping();
    let count = rows.len();
    for row in rows {
        // per-row _update_row_with_old call
        table_engine._update_row_with_old(row.row_id, old_values, new_values)?;
    }
    Ok(count)
}

fn delete(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
    let rows = self.scan(table, filters)?;
    if rows.is_empty() { return Ok(0); }
    let count = rows.len();
    let table_engine = self.get_table_mut(table)?;
    for row in rows {
        table_engine._delete_with_old_values(row.row_id, row.values)?;
    }
    Ok(count)
}
```

- [ ] **Step 2: Write integration test before changing anything**

Find the existing integration test file or add a new test function in the `#[cfg(test)]` block of `src/lib.rs`. Add:

```rust
#[test]
fn test_batch_update_via_sql() {
    let mut db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT, val INT)").unwrap();
    for i in 0..20i32 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 0)", i)).unwrap();
    }
    // Update all rows
    db.execute("UPDATE t SET val = 99").unwrap();
    let rows = db.query("SELECT val FROM t").unwrap();
    assert_eq!(rows.len(), 20);
    assert!(rows.iter().all(|r| r.values[0] == Value::Int32(99)));
}

#[test]
fn test_batch_delete_via_sql() {
    let mut db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE t2 (id INT, val INT)").unwrap();
    for i in 0..20i32 {
        db.execute(&format!("INSERT INTO t2 VALUES ({}, {})", i, i)).unwrap();
    }
    db.execute("DELETE FROM t2 WHERE val >= 10").unwrap();
    let rows = db.query("SELECT id FROM t2").unwrap();
    assert_eq!(rows.len(), 10);
}
```

- [ ] **Step 3: Run tests to confirm they pass with current code (baseline)**

```bash
cargo test -p thunderdb test_batch_update_via_sql test_batch_delete_via_sql 2>&1 | tail -5
```

Expected: `2 passed` (these test current behavior — they should already work).

- [ ] **Step 4: Replace `update()` per-row loop**

In `src/lib.rs`, replace the body of the `update()` function (the for loop and surrounding boilerplate) with the batch call. The full replacement for the `update` function body:

```rust
fn update(
    &mut self,
    table: &str,
    filters: Vec<Filter>,
    updates: Vec<(String, Value)>,
) -> Result<usize> {
    let rows = self.scan(table, filters)?;

    if rows.is_empty() {
        return Ok(0);
    }

    let table_engine = self.get_table_mut(table)?;
    let column_mapping = table_engine.build_column_mapping();

    let batch: Vec<(u64, Vec<Value>, Vec<Value>)> = rows.into_iter().map(|row| {
        let old_values = row.values.clone();
        let mut new_values = row.values;

        for (col_name, new_val) in &updates {
            let col_idx = if let Some(&idx) = column_mapping.get(col_name) {
                Some(idx)
            } else if col_name.starts_with("col") {
                col_name[3..].parse::<usize>().ok()
            } else {
                None
            };

            if let Some(idx) = col_idx {
                if idx < new_values.len() {
                    new_values[idx] = new_val.clone();
                }
            }
        }

        (row.row_id, old_values, new_values)
    }).collect();

    let count = batch.len();
    table_engine.update_batch(&batch)?;
    Ok(count)
}
```

- [ ] **Step 5: Replace `delete()` per-row loop**

In `src/lib.rs`, replace the body of the `delete()` function with:

```rust
fn delete(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
    let rows = self.scan(table, filters)?;

    if rows.is_empty() {
        return Ok(0);
    }

    let count = rows.len();
    let table_engine = self.get_table_mut(table)?;

    let deletions: Vec<(u64, Vec<Value>)> = rows.into_iter()
        .map(|r| (r.row_id, r.values))
        .collect();

    table_engine.delete_batch(&deletions)?;
    Ok(count)
}
```

- [ ] **Step 6: Verify integration tests pass**

```bash
cargo test -p thunderdb test_batch_update_via_sql test_batch_delete_via_sql 2>&1 | tail -5
```

Expected: `2 passed`.

- [ ] **Step 7: Run full test suite**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/lib.rs
git commit -m "perf(lib): replace per-row update/delete loops with batch calls"
```

---

## Task 8: Benchmark gate and rebaseline

**Files:**
- Run: `tests/perf/vs_sqlite_write.rs`
- Modify: `perf/baseline-write.json` (promote new baseline)
- Modify: `CHANGES.md`

---

- [ ] **Step 1: Run the write benchmark suite against current baseline**

```bash
cargo test -p thunderdb --test vs_sqlite_write -- --nocapture 2>&1 | tee /tmp/sp3b_bench_first.txt
```

Check output. W5/W7/W9 ratios should be dramatically lower than SP3 baseline (139x, 94x, 98x).

- [ ] **Step 2: If any previously-Winning scenario regressed to Loss, investigate before proceeding**

```bash
grep -E "Loss|Win|Tie" /tmp/sp3b_bench_first.txt
```

All W1-W4 wins must be preserved. W6/W8 should improve or hold. W5/W7/W9 should improve significantly.

- [ ] **Step 3: Promote new baseline**

```bash
SP3_PROMOTE_BASELINE=1 cargo test -p thunderdb --test vs_sqlite_write -- --nocapture 2>&1 | tail -5
```

Expected output includes: `baseline promoted`.

- [ ] **Step 4: Verify new baseline is committed**

```bash
git diff --stat perf/baseline-write.json
```

Expected: file changed with new timing values.

- [ ] **Step 5: Add SP3b entry to CHANGES.md**

Add at the top of `CHANGES.md`, above the SP3 entry:

```markdown
## 2026-04-24 - SP3b: Thunder write-path optimization

Fourth sub-project in the "faster than SQLite in all benchmarks" program.
Addresses W5/W7/W9 losses from SP3 (139x, 94x, 98x) by eliminating per-row
page I/O and per-row index maintenance.

- **`Page::update_row_inplace`**: overwrites a slot in-place when the new serialized
  row fits within the existing slot length. Dead bytes at tail abandoned (no compaction).
  Returns `false` to signal relocation needed.

- **`PagedTable::update_batch`**: groups mutations by `page_id`, reads each data page
  once, applies all in-place overwrites, then batch-inserts rows that needed relocation.
  One `write_page` + `update_fsm` per data page touched.

- **`PagedTable::delete_batch`**: groups ctids by `page_id`, reads each data page once,
  frees TOAST and deletes all slots, writes back once. O(pages) not O(rows).

- **`IndexManager::delete_rows_batch`**: sorts `(old_value, row_id)` pairs per indexed
  column before B-tree deletion — same cache-locality trick as existing `insert_rows_batch`.

- **`TableEngine::update_batch`**: wires page-level batch with smart index skip.
  Skips all index ops for rows where the row stayed in-place AND no indexed column
  value changed (common case: updating non-indexed columns like `content`).

- **`TableEngine::delete_batch`**: wires `PagedTable::delete_batch` with
  `IndexManager::delete_rows_batch`.

- **`lib.rs` `update()` / `delete()`**: replaced per-row loops with batch calls.
  The rows collection pass was already done before mutations — no structural change needed.

**Updated FAST/WARM ratios (SMALL tier, 11 samples):** _(fill in after benchmark run)_

Spec: `docs/superpowers/specs/2026-04-24-sp3b-write-path-optimization-design.md`
Plan: `docs/superpowers/plans/2026-04-24-sp3b-write-path-optimization.md`
```

- [ ] **Step 6: Fill in the actual benchmark numbers**

Replace `_(fill in after benchmark run)_` in `CHANGES.md` with the actual ratio table from the benchmark output (same format as SP3 entry in CHANGES.md).

- [ ] **Step 7: Run full test suite one final time**

```bash
cargo test -p thunderdb 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 8: Commit everything**

```bash
git add perf/baseline-write.json CHANGES.md docs/superpowers/plans/2026-04-24-sp3b-write-path-optimization.md
git commit -m "SP3b: write-path optimization — batch update/delete + in-place + index skip"
```

---

## Self-Review Checklist

- [x] **`Page::update_row_inplace`** — Task 1 ✓
- [x] **`PagedTable::delete_batch`** — Task 2 ✓
- [x] **`PagedTable::update_batch` + `BatchUpdateOutcome`** — Task 3 ✓
- [x] **`IndexManager::delete_rows_batch`** — Task 4 ✓
- [x] **`TableEngine::delete_batch`** — Task 5 ✓
- [x] **`TableEngine::update_batch` with smart index skip** — Task 6 ✓
- [x] **`lib.rs` batch wiring** — Task 7 ✓
- [x] **Benchmark gate + rebaseline + CHANGES.md** — Task 8 ✓
- [x] **`_update_row_with_old` and `_delete_with_old_values` retained** — spec says keep them; Task 7 only changes `update()`/`delete()` internal fns, not public API
- [x] **Type consistency** — `BatchUpdateOutcome` defined in Task 3, imported via full path in Task 6 (`crate::storage::paged_table::BatchUpdateOutcome`)
