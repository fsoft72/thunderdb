# SQL JOIN Support — Design Spec

## Scope

Add INNER, LEFT, and RIGHT JOIN to the SQL layer with:

- Multi-table chaining (`FROM a JOIN b ON ... JOIN c ON ...`)
- Table aliases (`FROM users u JOIN posts p ON u.id = p.author_id`)
- Equality ON conditions only (`left_col = right_col`)
- Dot-qualified column references (`u.name`, `p.title`) required when ambiguous, optional when unambiguous
- WHERE filter pushdown to per-table scans where possible

## What's NOT in scope

- FULL OUTER JOIN
- Non-equality ON conditions (use WHERE for those)
- Subqueries in FROM
- CROSS JOIN / implicit comma joins
- Changes to QueryBuilder, QueryPlan, DirectDataAccess, or storage layer

Single-table queries continue through the existing code path unchanged.

## Approach

All JOIN logic lives in the REPL's `execute_select()` method (Approach A). The parser produces a new AST structure; the REPL detects joins and takes a separate execution path. The existing single-table query pipeline is untouched.

---

## 1. Parser Changes

### 1.1 New Tokens (`src/parser/token.rs`)

Add keywords: `JOIN`, `INNER`, `LEFT`, `RIGHT`, `OUTER`.

`ON` already exists from CREATE INDEX work. `INNER`, `LEFT`, `RIGHT`, `OUTER` are added as keyword tokens. Plain `JOIN` is equivalent to `INNER JOIN`.

### 1.2 New AST Types (`src/parser/ast.rs`)

```rust
/// Table reference with optional alias
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

/// Column reference, optionally qualified with table/alias
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

/// JOIN type
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// FROM clause — single table or chain of joins
pub enum FromClause {
    /// Single table: FROM users u
    Table(TableRef),
    /// Join: FROM users u JOIN posts p ON u.id = p.author_id
    Join {
        left: Box<FromClause>,
        join_type: JoinType,
        right: TableRef,
        on_left: ColumnRef,
        on_right: ColumnRef,
    },
}
```

### 1.3 SelectStatement Changes

Replace `from: String` with `from: FromClause`. Add a helper `from_table_name()` that returns the table name for single-table queries (backward compat).

### 1.4 Expression Changes

Add `Expression::QualifiedColumn(String, String)` for `table.column` references in WHERE, ORDER BY, and SELECT clauses.

### 1.5 SelectColumn Changes

Add `SelectColumn::QualifiedColumn(String, String)` for `SELECT u.name`.

### 1.6 Parsing Flow

`parse_select()` calls `parse_from_clause()`:

1. Parse `FROM table [alias]` into `FromClause::Table`
2. Loop while current token is `JOIN`, `INNER`, `LEFT`, or `RIGHT`:
   - Determine `JoinType` (default INNER; LEFT/RIGHT consume optional OUTER)
   - Consume `JOIN`
   - Parse right table + optional alias
   - Consume `ON`
   - Parse `left_col = right_col` (both may be dot-qualified)
   - Wrap current `FromClause` into `FromClause::Join`
3. Return the built-up `FromClause`

Dot-qualified columns: in `parse_expression()` and `parse_select_columns()`, when an identifier is followed by `.` and another identifier, produce `QualifiedColumn(table, column)` instead of `Column(name)`.

---

## 2. Execution Flow (REPL layer)

When `execute_select` detects `FromClause::Join`, it takes this path:

### 2.1 Flatten Join Chain

Walk the `FromClause` tree to produce:

```
base_table: TableRef
joins: Vec<{join_type, right: TableRef, on_left: ColumnRef, on_right: ColumnRef}>
```

### 2.2 Build Merged Column Mapping

Before scanning, read each table's schema and build a `HashMap<String, usize>` mapping:

- Qualified entries: `"alias.column" -> position` for every column
- Bare entries: `"column" -> position` only if the column name is unique across all joined tables
- If a bare name is ambiguous (exists in 2+ tables), it is NOT added — any reference to it without qualification will produce an error

This mapping is used for all column resolution: ON conditions, WHERE, ORDER BY, SELECT projection.

### 2.3 Partition WHERE Filters

Analyze each filter in the WHERE clause:

- If the filter references columns from only one table (by alias/name), tag it as a **pushdown filter** for that table's scan
- If the filter references columns from multiple tables or uses an ambiguous bare name, keep it as a **post-join filter**

Convert qualified column references to bare column names for pushdown filters (since the per-table scan uses the single-table column mapping).

### 2.4 Scan Each Table

For each table in the join chain, call `self.database.scan(table_name, pushdown_filters)`. This leverages existing indexes and reduces rows before joining.

### 2.5 Hash Join

Process joins sequentially through the chain. For each join step:

**INNER JOIN**:
1. Build `HashMap<Value, Vec<&Row>>` from the right table keyed on the ON column
2. For each left row, look up the left ON column value in the map
3. For each match, emit a merged row (left.values ++ right.values)
4. No match = row dropped

**LEFT JOIN**:
Same as INNER, but when no match is found, emit left.values ++ [Null, Null, ...] (right column count nulls).

**RIGHT JOIN**:
Swap left and right tables, perform a LEFT JOIN, then reorder the merged row columns so left table columns come first. Alternatively: track which right rows were matched; after the inner loop, emit unmatched right rows as [Null, ...] ++ right.values.

**Row merging**: Concatenate `left.values` and `right.values` into a new `Row`. Use the left row's `row_id` (meaningless post-join but struct requires it).

### 2.6 Post-Join Operations

On the merged rows, apply in order:

1. Post-join WHERE filters (using merged column mapping)
2. ORDER BY (using merged column mapping)
3. LIMIT/OFFSET
4. Column projection (SELECT columns, using merged column mapping)

Steps 2-4 reuse existing `QueryPlan` methods by passing the merged column mapping.

---

## 3. Column Resolution

### 3.1 Resolution Algorithm

Given a column reference (from SELECT, WHERE, ORDER BY, ON):

1. **Qualified** (`u.name`): look up `"u.name"` in merged mapping. Error if not found.
2. **Bare** (`name`): look up `"name"` in merged mapping. If not found, error. The mapping was built to exclude ambiguous bare names, so if it's present it's unambiguous.

### 3.2 Ambiguity Error

```sql
SELECT id FROM users u JOIN posts p ON u.id = p.author_id
```

If both `users` and `posts` have an `id` column, bare `id` is not in the mapping. Error: `Column 'id' is ambiguous; qualify with table name or alias`.

### 3.3 Schema-less Tables

If a table has no schema, fall back to positional `col0`, `col1`, etc. Dot-qualified: `u.col0`. These are always unambiguous since they include position.

---

## 4. Files Changed

| File | Changes |
|------|---------|
| `src/parser/token.rs` | Add JOIN, INNER, LEFT, RIGHT, OUTER tokens |
| `src/parser/ast.rs` | Add FromClause, TableRef, ColumnRef, JoinType enums; QualifiedColumn in Expression and SelectColumn; modify SelectStatement |
| `src/parser/parser.rs` | Add parse_from_clause(), update parse_expression() and parse_select_columns() for dot-qualified columns |
| `src/parser/executor.rs` | Update select_to_query() to handle new FromClause (single-table path unchanged) |
| `src/parser/validator.rs` | No changes needed (structural validation done by parser) |
| `src/repl/mod.rs` | Add join execution path in execute_select(): flatten, build mapping, partition filters, scan, hash-join, post-join ops |
| `src/query/direct.rs` | Update apply_filters() to handle QualifiedColumn in expressions (or convert at REPL layer) |

**Not changed**: `src/query/builder.rs`, `src/lib.rs` (DirectDataAccess), storage layer, index system.

---

## 5. Testing

### 5.1 Parser Tests

- `CREATE INDEX` still works (ON token shared)
- `SELECT * FROM users` (single table, no regression)
- `SELECT * FROM users u` (alias)
- `SELECT * FROM users u JOIN posts p ON u.id = p.author_id` (INNER)
- `SELECT * FROM users u LEFT JOIN posts p ON u.id = p.author_id` (LEFT)
- `SELECT * FROM users u RIGHT JOIN posts p ON u.id = p.author_id` (RIGHT)
- `SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id` (qualified columns)
- Multi-table chain: three-way join

### 5.2 Integration Tests

- Update the blog benchmark SQL test to use JOINs
- INNER JOIN: posts with their authors (every post has an author)
- LEFT JOIN: users with their posts (some users may have no posts)
- RIGHT JOIN: posts with their commenters
- Multi-table: users JOIN posts JOIN comments
- WHERE filter pushdown: verify indexed scan kicks in
- Ambiguous column error: SELECT id from a two-table join where both have id
- COUNT(*) with JOIN

### 5.3 Existing Tests

- All 264 unit tests must continue passing (single-table path untouched)
- All existing integration tests must pass
