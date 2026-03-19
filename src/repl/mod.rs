// REPL interface - Phase 5
//
// Interactive command-line interface for queries

pub mod commands;
pub mod formatter;

use crate::error::Result;
use crate::parser::{Statement, Executor};
use crate::query::DirectDataAccess;
use crate::repl::commands::{parse_special_command, SpecialCommand};
use crate::repl::formatter::format_results;
use crate::Database;
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
                let rows = self.execute_select(select)?;

                // Get column names
                let column_names = if select.is_select_star() {
                    // Try to get from table schema
                    if let Some(table) = self.database.get_table(&select.from) {
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
            Statement::DropTable(table) => {
                self.database.drop_table(&table)?;
                self.database.clear_statement_cache();
                println!("Table dropped: {}", table);
                Ok(())
            }
        }
    }

    /// Execute a SELECT statement
    ///
    /// Handles ORDER BY, column projection, LIMIT, and OFFSET.
    /// When ORDER BY is present, all matching rows are fetched first so
    /// they can be sorted before pagination is applied.
    fn execute_select(&mut self, select: &crate::parser::SelectStatement) -> Result<Vec<crate::storage::Row>> {
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

        // Build column mapping for ordering / projection
        if has_ordering || has_projection {
            let mut column_mapping = std::collections::HashMap::new();
            if let Some(table) = self.database.get_table(&plan.table) {
                if let Some(schema) = table.schema() {
                    for (i, col) in schema.columns.iter().enumerate() {
                        column_mapping.insert(col.name.clone(), i);
                    }
                }
            }

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
