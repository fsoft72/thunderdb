// REPL interface - Phase 5
//
// Interactive command-line interface for queries

pub mod commands;
pub mod formatter;

use crate::error::Result;
use crate::parser::ast::{FromClause, JoinType, ColumnRef, TableRef};
use crate::parser::{Statement, Executor};
use crate::query::DirectDataAccess;
use crate::repl::commands::{parse_special_command, SpecialCommand};
use crate::repl::formatter::format_results;
use crate::Database;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

#[cfg(feature = "repl")]
use rustyline::error::ReadlineError;
#[cfg(feature = "repl")]
use rustyline::DefaultEditor;

/// REPL state
pub struct Repl<'a> {
    prompt: String,
    history_file: String,
    database: &'a mut Database,
    #[cfg(feature = "repl")]
    editor: DefaultEditor,
}

/// Flattened join step
struct JoinStep {
    join_type: JoinType,
    right: TableRef,
    on_left: ColumnRef,
    on_right: ColumnRef,
}

/// Flatten a FromClause join tree into base table + ordered join steps
fn flatten_joins(from: &FromClause) -> (TableRef, Vec<JoinStep>) {
    match from {
        FromClause::Table(t) => (t.clone(), vec![]),
        FromClause::Join { left, join_type, right, on_left, on_right } => {
            let (base, mut steps) = flatten_joins(left);
            steps.push(JoinStep {
                join_type: join_type.clone(),
                right: right.clone(),
                on_left: on_left.clone(),
                on_right: on_right.clone(),
            });
            (base, steps)
        }
    }
}

/// Build column mapping for joined tables.
/// Maps both "alias.col" and bare "col" (if unambiguous) to position in merged row.
fn build_join_column_mapping(
    tables: &[(String, Option<String>, Vec<String>)],
) -> HashMap<String, usize> {
    let mut mapping = HashMap::new();
    let mut bare_count: HashMap<String, usize> = HashMap::new();
    let mut bare_pos: HashMap<String, usize> = HashMap::new();
    let mut offset = 0usize;

    for (name, alias, columns) in tables {
        let qualifier = alias.as_deref().unwrap_or(name.as_str());
        for (i, col) in columns.iter().enumerate() {
            let pos = offset + i;
            mapping.insert(format!("{}.{}", qualifier, col), pos);
            *bare_count.entry(col.clone()).or_insert(0) += 1;
            bare_pos.insert(col.clone(), pos);
        }
        offset += columns.len();
    }

    for (col, count) in &bare_count {
        if *count == 1 {
            if let Some(&pos) = bare_pos.get(col) {
                mapping.insert(col.clone(), pos);
            }
        }
    }

    mapping
}

/// Partition WHERE filters into per-table pushdowns and post-join filters.
fn partition_filters(
    where_clause: &Option<crate::parser::ast::Expression>,
    tables: &[(String, Option<String>, Vec<String>)],
) -> crate::error::Result<(HashMap<String, Vec<crate::query::Filter>>, Vec<crate::query::Filter>)> {
    use crate::parser::Executor;
    use crate::query::Filter;

    let mut pushdowns: HashMap<String, Vec<Filter>> = HashMap::new();
    let mut post_join: Vec<Filter> = Vec::new();

    let filters = if let Some(expr) = where_clause {
        Executor::expression_to_filters(expr)?
    } else {
        return Ok((pushdowns, post_join));
    };

    for filter in filters {
        if let Some(dot_pos) = filter.column.find('.') {
            let qualifier = &filter.column[..dot_pos];
            let bare_col = &filter.column[dot_pos + 1..];
            let table_name = tables.iter()
                .find(|(name, alias, _)| {
                    alias.as_deref() == Some(qualifier) || name == qualifier
                })
                .map(|(name, _, _)| name.clone());

            if let Some(name) = table_name {
                pushdowns.entry(name).or_default()
                    .push(Filter::new(bare_col.to_string(), filter.operator.clone()));
            } else {
                post_join.push(filter);
            }
        } else {
            let mut found_table = None;
            let mut ambiguous = false;
            for (name, _, columns) in tables {
                if columns.contains(&filter.column) {
                    if found_table.is_some() {
                        ambiguous = true;
                        break;
                    }
                    found_table = Some(name.clone());
                }
            }
            if ambiguous || found_table.is_none() {
                post_join.push(filter);
            } else {
                pushdowns.entry(found_table.unwrap()).or_default().push(filter);
            }
        }
    }

    Ok((pushdowns, post_join))
}

/// Perform a hash join between left and right row sets.
///
/// Uses BTreeMap since Value implements Ord but not Hash.
fn hash_join(
    left_rows: &[crate::storage::Row],
    right_rows: &[crate::storage::Row],
    left_col_idx: usize,
    right_col_idx: usize,
    join_type: &JoinType,
    left_col_count: usize,
    right_col_count: usize,
) -> Vec<crate::storage::Row> {
    use crate::storage::{Row, Value};

    let mut right_map: BTreeMap<Value, Vec<usize>> = BTreeMap::new();
    for (i, row) in right_rows.iter().enumerate() {
        if let Some(val) = row.values.get(right_col_idx) {
            right_map.entry(val.clone()).or_default().push(i);
        }
    }

    let mut results = Vec::new();
    let mut right_matched = vec![false; right_rows.len()];

    for left_row in left_rows {
        let left_val = left_row.values.get(left_col_idx);
        let matches = left_val.and_then(|v| right_map.get(v));

        if let Some(indices) = matches {
            for &ri in indices {
                right_matched[ri] = true;
                let mut values = left_row.values.clone();
                values.extend(right_rows[ri].values.clone());
                results.push(Row { row_id: left_row.row_id, values });
            }
        } else if matches!(join_type, JoinType::Left) {
            let mut values = left_row.values.clone();
            values.extend(std::iter::repeat(Value::Null).take(right_col_count));
            results.push(Row { row_id: left_row.row_id, values });
        }
    }

    if matches!(join_type, JoinType::Right) {
        for (i, matched) in right_matched.iter().enumerate() {
            if !matched {
                let mut values: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                values.extend(right_rows[i].values.clone());
                results.push(Row { row_id: right_rows[i].row_id, values });
            }
        }
    }

    results
}

impl<'a> Repl<'a> {
    /// Create a new REPL
    pub fn new(database: &'a mut Database) -> Result<Self> {
        let prompt = database.config().repl.prompt.clone();
        let history_file = database.config().repl.history_file.clone();

        #[cfg(feature = "repl")]
        let editor = DefaultEditor::new().map_err(|e| {
            crate::error::Error::Config(format!("Failed to create editor: {}", e))
        })?;

        Ok(Self {
            prompt,
            history_file,
            database,
            #[cfg(feature = "repl")]
            editor,
        })
    }

    /// Run the REPL loop
    #[cfg(feature = "repl")]
    pub fn run(&mut self) -> Result<()> {
        // Load history
        let _ = self.editor.load_history(&self.history_file);

        println!("ThunderDB v{}", crate::VERSION);
        println!("Type .help for help, .exit to quit");
        println!();

        let mut multi_line_buffer = String::new();

        loop {
            // Determine prompt based on whether we're in multi-line mode
            let prompt = if multi_line_buffer.is_empty() {
                &self.prompt
            } else {
                "      ...> "
            };

            match self.editor.readline(prompt) {
                Ok(line) => {
                    let line = line.trim();

                    // Skip empty lines
                    if line.is_empty() && multi_line_buffer.is_empty() {
                        continue;
                    }

                    // Add to history
                    let _ = self.editor.add_history_entry(line);

                    // Check for special commands (only at start of input)
                    if multi_line_buffer.is_empty() && line.starts_with('.') {
                        if let Some(cmd) = parse_special_command(line) {
                            match cmd {
                                SpecialCommand::Exit => {
                                    println!("Goodbye!");
                                    break;
                                }
                                SpecialCommand::Help => {
                                    self.show_help();
                                }
                                SpecialCommand::Tables => {
                                    self.show_tables();
                                }
                                SpecialCommand::Schema(table) => {
                                    self.show_schema(&table);
                                }
                                SpecialCommand::Stats(table) => {
                                    self.show_stats(&table);
                                }
                                SpecialCommand::Save => {
                                    match self.database.save() {
                                        Ok(()) => println!("Database saved successfully to: {}", self.database.config().storage.data_dir),
                                        Err(e) => eprintln!("Error saving database: {}", e),
                                    }
                                }
                            }
                            continue;
                        }
                    }

                    // Accumulate multi-line input
                    if !line.is_empty() {
                        if !multi_line_buffer.is_empty() {
                            multi_line_buffer.push(' ');
                        }
                        multi_line_buffer.push_str(line);
                    }

                    // Check if we have a complete statement (ends with semicolon)
                    if multi_line_buffer.ends_with(';') {
                        let sql = multi_line_buffer.trim_end_matches(';').trim();

                        if !sql.is_empty() {
                            self.execute_sql(sql);
                        }

                        multi_line_buffer.clear();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // Ctrl-C
                    println!("^C");
                    multi_line_buffer.clear();
                }
                Err(ReadlineError::Eof) => {
                    // Ctrl-D
                    println!("Goodbye!");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        // Save history
        let _ = self.editor.save_history(&self.history_file);

        Ok(())
    }

    /// Run REPL without rustyline (fallback)
    #[cfg(not(feature = "repl"))]
    pub fn run(&mut self) -> Result<()> {
        println!("ThunderDB v{}", crate::VERSION);
        println!("REPL feature not enabled. Please compile with --features repl");
        Ok(())
    }

    /// Execute SQL statement
    pub fn execute_sql(&mut self, sql: &str) {
        let sql = sql.trim();
        if sql.is_empty() || sql.starts_with("--") {
            return;
        }

        let start = Instant::now();

        match self.database.parse_sql_cached(sql) {
            Ok(stmt) => {
                let result = self.execute_statement(&stmt);
                let elapsed = start.elapsed();

                match result {
                    Ok(()) => {
                        // Success message already printed by execute_statement
                    }
                    Err(e) => {
                        // Special case: ignore TableNotFound during DROP TABLE for idempotency
                        if let Statement::DropTable(_) = stmt {
                            if let crate::error::Error::TableNotFound(_) = e {
                                // Silently ignore
                                return;
                            }
                        }
                        eprintln!("Error: {}", e);
                    }
                }

                let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
                if elapsed_ms >= 1.0 {
                    println!("({:.2}ms)", elapsed_ms);
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }

        println!();
    }

    /// Execute a parsed statement
    fn execute_statement(&mut self, stmt: &Statement) -> Result<()> {
        match stmt {
            Statement::Select(select) => {
                // COUNT(*) short-circuit: use the fast count path
                if select.is_count_star() {
                    if select.from.is_single_table() {
                        let filters = Executor::get_where_filters(&select.where_clause)?;
                        let count = self.database.count(select.from.base_table_name(), filters)?;
                        println!("COUNT(*)");
                        println!("--------");
                        println!("{}", count);
                        println!("1 row(s)");
                    } else {
                        // For joins, execute the join and count results
                        let rows = self.execute_join(select)?;
                        println!("COUNT(*)");
                        println!("--------");
                        println!("{}", rows.len());
                        println!("1 row(s)");
                    }
                    return Ok(());
                }

                let rows = self.execute_select(select)?;

                // Get column names
                let column_names = if select.is_select_star() {
                    if !select.from.is_single_table() {
                        // For joins, build column names from all table schemas
                        let (base, steps) = flatten_joins(&select.from);
                        let mut names = Vec::new();
                        let mut add_table_cols = |db: &crate::Database, table_name: &str, alias: Option<&str>| {
                            let prefix = alias.unwrap_or(table_name);
                            if let Some(t) = db.get_table(table_name) {
                                if let Some(schema) = t.schema() {
                                    for col in &schema.columns {
                                        names.push(format!("{}.{}", prefix, col.name));
                                    }
                                    return;
                                }
                            }
                            names.push(format!("{}.?", prefix));
                        };
                        add_table_cols(&self.database, &base.name, base.alias.as_deref());
                        for step in &steps {
                            add_table_cols(&self.database, &step.right.name, step.right.alias.as_deref());
                        }
                        names
                    } else {
                        // Existing single-table logic
                        if let Some(table) = self.database.get_table(select.from.base_table_name()) {
                            if let Some(schema) = table.schema() {
                                schema.columns.iter().map(|c| c.name.clone()).collect()
                            } else if let Some(first_row) = rows.first() {
                                (0..first_row.values.len())
                                    .map(|i| format!("col{}", i))
                                    .collect()
                            } else {
                                vec![]
                            }
                        } else if let Some(first_row) = rows.first() {
                            (0..first_row.values.len())
                                .map(|i| format!("col{}", i))
                                .collect()
                        } else {
                            vec![]
                        }
                    }
                } else {
                    select.get_column_names()
                };

                // Format and display results
                if rows.is_empty() {
                    println!("No rows returned");
                } else {
                    let formatted = format_results(&rows, &column_names);
                    print!("{}", formatted);
                    println!("{} row(s)", rows.len());
                }

                Ok(())
            }
            Statement::Insert(insert) => {
                let values = Executor::get_insert_values(insert);
                let row_id = self.database.insert_row(&insert.table, values)?;
                println!("Inserted row with ID: {}", row_id);
                Ok(())
            }
            Statement::Update(update) => {
                let filters = Executor::get_where_filters(&update.where_clause)?;
                let updates = Executor::get_update_assignments(update)?;
                match self.database.update(&update.table, filters, updates) {
                    Ok(count) => println!("Updated {} row(s)", count),
                    Err(e) => eprintln!("Error: {}", e),
                }
                Ok(())
            }
            Statement::Delete(delete) => {
                let filters = Executor::get_where_filters(&delete.where_clause)?;
                let count = self.database.delete(&delete.table, filters)?;
                println!("Deleted {} row(s)", count);
                Ok(())
            }
            Statement::ShowTables => {
                self.show_tables();
                Ok(())
            }
            Statement::ShowDatabases => {
                self.show_databases();
                Ok(())
            }
            Statement::Use(db_name) => {
                self.switch_database(db_name)?;
                Ok(())
            }
            Statement::CreateTable(create) => {
                let table_engine = self.database.get_or_create_table(&create.name)?;
                let columns: Vec<crate::storage::table_engine::ColumnInfo> = create.columns.iter().map(|c| {
                    crate::storage::table_engine::ColumnInfo {
                        name: c.name.clone(),
                        data_type: format!("{:?}", c.data_type).to_uppercase(),
                    }
                }).collect();
                table_engine.set_schema(crate::storage::table_engine::TableSchema { columns })?;
                self.database.clear_statement_cache();
                println!("Table created: {}", create.name);
                Ok(())
            }
            Statement::CreateIndex(create_index) => {
                let table_engine = self.database.get_table_mut(&create_index.table)?;
                table_engine.create_index(&create_index.column)?;
                println!("Index created: {} on {}.{}", create_index.index_name, create_index.table, create_index.column);
                Ok(())
            }
            Statement::DropTable(table) => {
                self.database.drop_table(&table)?;
                self.database.clear_statement_cache();
                println!("Table dropped: {}", table);
                Ok(())
            }
        }
    }

    /// Execute a SELECT with JOIN
    fn execute_join(&mut self, select: &crate::parser::SelectStatement) -> crate::error::Result<Vec<crate::storage::Row>> {
        use crate::query::DirectDataAccess;
        use crate::parser::ast::SelectColumn;

        let (base_table, join_steps) = flatten_joins(&select.from);

        // Collect table metadata: (name, alias, column_names)
        let mut table_info: Vec<(String, Option<String>, Vec<String>)> = Vec::new();

        let get_columns = |db: &crate::Database, name: &str| -> Vec<String> {
            if let Some(t) = db.get_table(name) {
                if let Some(schema) = t.schema() {
                    return schema.columns.iter().map(|c| c.name.clone()).collect();
                }
            }
            vec![]
        };

        let base_cols = get_columns(&self.database, &base_table.name);
        table_info.push((base_table.name.clone(), base_table.alias.clone(), base_cols));

        for step in &join_steps {
            let cols = get_columns(&self.database, &step.right.name);
            table_info.push((step.right.name.clone(), step.right.alias.clone(), cols));
        }

        let column_mapping = build_join_column_mapping(&table_info);

        let (pushdowns, post_join_filters) = partition_filters(&select.where_clause, &table_info)?;

        // Scan base table
        let base_filters = pushdowns.get(&base_table.name).cloned().unwrap_or_default();
        let mut current_rows = self.database.scan(&base_table.name, base_filters)?;
        let mut current_col_count = table_info[0].2.len();

        // Execute each join step
        for (i, step) in join_steps.iter().enumerate() {
            let right_info = &table_info[i + 1];
            let right_filters = pushdowns.get(&step.right.name).cloned().unwrap_or_default();
            let right_rows = self.database.scan(&step.right.name, right_filters)?;
            let right_col_count = right_info.2.len();

            // Resolve ON column indices
            let left_key = if let Some(ref tbl) = step.on_left.table {
                format!("{}.{}", tbl, step.on_left.column)
            } else {
                step.on_left.column.clone()
            };

            let left_col_idx = column_mapping.get(&left_key)
                .copied()
                .ok_or_else(|| crate::error::Error::Query(
                    format!("Column '{}' not found in join", left_key)
                ))?;

            // Right column index is relative to the right table (not merged row)
            let right_col_idx = right_info.2.iter()
                .position(|c| c == &step.on_right.column)
                .ok_or_else(|| crate::error::Error::Query(
                    format!("Column '{}' not found in table '{}'", step.on_right.column, step.right.name)
                ))?;

            current_rows = hash_join(
                &current_rows, &right_rows,
                left_col_idx, right_col_idx,
                &step.join_type,
                current_col_count, right_col_count,
            );
            current_col_count += right_col_count;
        }

        // Apply post-join WHERE filters
        if !post_join_filters.is_empty() {
            current_rows.retain(|row| {
                crate::query::direct::apply_filters(row, &post_join_filters, &column_mapping)
            });
        }

        // Apply ORDER BY
        if let Some(ref order_by) = select.order_by {
            if let Some(&col_idx) = column_mapping.get(&order_by.column) {
                let desc = order_by.direction == crate::parser::ast::OrderDirection::Desc;
                current_rows.sort_by(|a, b| {
                    let va = a.values.get(col_idx);
                    let vb = b.values.get(col_idx);
                    let cmp = match (va, vb) {
                        (Some(a), Some(b)) => a.cmp(b),
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (None, None) => std::cmp::Ordering::Equal,
                    };
                    if desc { cmp.reverse() } else { cmp }
                });
            }
        }

        // Apply LIMIT/OFFSET
        if let Some(offset) = select.offset {
            current_rows = current_rows.into_iter().skip(offset).collect();
        }
        if let Some(limit) = select.limit {
            current_rows.truncate(limit);
        }

        // Apply SELECT projection
        if !select.is_select_star() && !select.is_count_star() {
            let select_indices: Vec<usize> = select.columns.iter().filter_map(|col| {
                match col {
                    SelectColumn::Column(name) => column_mapping.get(name).copied(),
                    SelectColumn::QualifiedColumn(table, col) => {
                        let key = format!("{}.{}", table, col);
                        column_mapping.get(&key).copied()
                    }
                    SelectColumn::ColumnWithAlias(name, _) => column_mapping.get(name).copied(),
                    _ => None,
                }
            }).collect();

            if !select_indices.is_empty() {
                current_rows = current_rows.into_iter().map(|row| {
                    let new_values: Vec<crate::storage::Value> = select_indices.iter()
                        .filter_map(|&idx| row.values.get(idx).cloned())
                        .collect();
                    crate::storage::Row { row_id: row.row_id, values: new_values }
                }).collect();
            }
        }

        Ok(current_rows)
    }

    /// Execute a SELECT statement
    ///
    /// Handles ORDER BY, column projection, LIMIT, and OFFSET.
    /// When ORDER BY is present, all matching rows are fetched first so
    /// they can be sorted before pagination is applied.
    fn execute_select(&mut self, select: &crate::parser::SelectStatement) -> Result<Vec<crate::storage::Row>> {
        // Dispatch to join path if FROM clause contains joins
        if !select.from.is_single_table() {
            return self.execute_join(select);
        }

        let query = Executor::select_to_query(select);
        let has_ordering = query.get_order_by().is_some();
        let has_projection = query.get_columns().is_some();
        let plan = query.build();

        // When ORDER BY is active we need ALL matching rows before sorting,
        // so skip limit/offset during the scan and apply them after sorting.
        let (scan_limit, scan_offset) = if has_ordering {
            (None, None)
        } else {
            (plan.limit, plan.offset)
        };

        let filters = plan.filters.clone();
        let mut rows = self.database.scan_with_limit(
            &plan.table, filters, scan_limit, scan_offset,
        )?;

        // Build column mapping for ordering / projection (uses cached Arc)
        if has_ordering || has_projection {
            let column_mapping = self.database.get_table_mut(&plan.table)
                .map(|t| t.build_column_mapping())
                .unwrap_or_default();

            rows = plan.apply_ordering(rows, &column_mapping);

            if has_ordering {
                rows = plan.apply_pagination(rows);
            }

            rows = plan.apply_projection(rows, &column_mapping);
        }

        Ok(rows)
    }

    /// Show help message
    pub fn show_help(&self) {
        println!("ThunderDB Commands:");
        println!();
        println!("  SQL Commands:");
        println!("    SELECT * FROM table WHERE condition;");
        println!("    INSERT INTO table VALUES (value1, value2, ...);");
        println!("    UPDATE table SET column = value WHERE condition;");
        println!("    DELETE FROM table WHERE condition;");
        println!("    CREATE TABLE table (col1 type, col2 type, ...);");
        println!("    DROP TABLE table;");
        println!();
        println!("  Special Commands:");
        println!("    .help              Show this help message");
        println!("    .tables            List all tables");
        println!("    .schema [table]    Show table schema");
        println!("    .stats [table]     Show table statistics");
        println!("    .save              Save database to disk (when in memory mode)");
        println!("    .exit, .quit       Exit the REPL");
        println!();
        println!("  Tips:");
        println!("    - SQL statements must end with semicolon (;)");
        println!("    - Use Ctrl-C to cancel current input");
        println!("    - Use Ctrl-D or .exit to quit");
        println!("    - Multi-line input is supported");
        println!();
    }

    /// Show tables list
    pub fn show_tables(&mut self) {
        let tables = self.database.list_tables();
        if tables.is_empty() {
            println!("No tables found");
        } else {
            println!("Tables in current database:");
            for table in tables {
                println!("  {}", table);
            }
        }
    }

    /// Show databases list
    pub fn show_databases(&self) {
        println!("Databases:");
        println!("  Currently using: {}", self.database.config().storage.data_dir);
        println!();
        println!("Note: To switch databases, use: USE <database_name>");
        println!("      This will open/create a database in ./data/<database_name>");
    }

    /// Switch to a different database
    fn switch_database(&mut self, db_name: &str) -> Result<()> {
        println!("Database switching in REPL requires restart.");
        println!("Restart with database path: ./data/{}", db_name);
        println!();
        println!("Or manually specify path when opening database.");
        Ok(())
    }

    /// Show table schema
    pub fn show_schema(&mut self, table: &Option<String>) {
        if let Some(table_name) = table {
            match self.database.get_table_mut(table_name) {
                Ok(table_engine) => {
                    if let Some(schema) = table_engine.schema() {
                        println!("Schema for table: {}", table_name);
                        println!("+----------------------+----------------------+");
                        println!("| Column               | Type                 |");
                        println!("+----------------------+----------------------+");
                        for col in &schema.columns {
                            println!("| {:<20} | {:<20} |", col.name, col.data_type);
                        }
                        println!("+----------------------+----------------------+");
                    } else {
                        println!("No schema defined for table: {}", table_name);
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        } else {
            println!("Usage: .schema <table_name>");
        }
        println!();
    }

    /// Show table statistics
    pub fn show_stats(&mut self, table: &Option<String>) {
        if let Some(table_name) = table {
            match self.database.get_table_mut(table_name) {
                Ok(table_engine) => {
                    let stats = table_engine.stats();
                    println!("Statistics for table: {}", table_name);
                    println!("  Total rows:  {}", stats.total_rows);
                    println!("  Active rows: {}", stats.active_rows);
                    println!("  Data file:   {} bytes", stats.data_file_size);
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        } else {
            println!("Usage: .stats <table_name>");
        }
        println!();
    }

    /// Save the database
    pub fn save(&mut self) -> Result<()> {
        self.database.save()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repl_creation() {
        let mut db = Database::open("/tmp/thunderdb_repl_test").unwrap();
        let result = Repl::new(&mut db);
        assert!(result.is_ok());
        std::fs::remove_dir_all("/tmp/thunderdb_repl_test").ok();
    }
}
