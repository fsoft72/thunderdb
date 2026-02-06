// REPL interface - Phase 5
//
// Interactive command-line interface for queries

pub mod commands;
pub mod formatter;

use crate::error::Result;
use crate::parser::{parse_sql, Statement, Executor};
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
    fn execute_sql(&mut self, sql: &str) {
        let start = Instant::now();

        match parse_sql(sql) {
            Ok(stmt) => {
                let result = self.execute_statement(&stmt);
                let elapsed = start.elapsed();

                match result {
                    Ok(()) => {
                        // Success message already printed by execute_statement
                    }
                    Err(e) => {
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
                    // For now, use generic column names
                    if let Some(first_row) = rows.first() {
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
            Statement::Update(_update) => {
                println!("UPDATE not yet implemented");
                Ok(())
            }
            Statement::Delete(delete) => {
                let filters = Executor::get_where_filters(&delete.where_clause)?;
                let count = self.database.delete(&delete.table, filters)?;
                println!("Deleted {} row(s)", count);
                Ok(())
            }
        }
    }

    /// Execute a SELECT statement
    fn execute_select(&mut self, select: &crate::parser::SelectStatement) -> Result<Vec<crate::storage::Row>> {
        let query = Executor::select_to_query(select);
        let filters = query.get_filters().to_vec();
        let limit = query.get_limit();
        let offset = query.get_offset();

        self.database.scan_with_limit(&select.from, filters, limit, offset)
    }

    /// Show help message
    fn show_help(&self) {
        println!("ThunderDB Commands:");
        println!();
        println!("  SQL Commands:");
        println!("    SELECT * FROM table WHERE condition;");
        println!("    INSERT INTO table VALUES (value1, value2, ...);");
        println!("    UPDATE table SET column = value WHERE condition;");
        println!("    DELETE FROM table WHERE condition;");
        println!();
        println!("  Special Commands:");
        println!("    .help              Show this help message");
        println!("    .tables            List all tables");
        println!("    .schema [table]    Show table schema");
        println!("    .stats [table]     Show table statistics");
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
    fn show_tables(&self) {
        let tables = self.database.list_tables();
        if tables.is_empty() {
            println!("No tables found");
        } else {
            println!("Tables:");
            for table in tables {
                println!("  - {}", table);
            }
        }
        println!();
    }

    /// Show table schema
    fn show_schema(&self, table: &Option<String>) {
        if let Some(table_name) = table {
            println!("Schema for table: {}", table_name);
            println!("  (Schema display not yet implemented)");
        } else {
            println!("Usage: .schema <table_name>");
        }
        println!();
    }

    /// Show table statistics
    fn show_stats(&self, table: &Option<String>) {
        if let Some(table_name) = table {
            println!("Statistics for table: {}", table_name);
            println!("  (Statistics display not yet implemented)");
        } else {
            println!("Usage: .stats <table_name>");
        }
        println!();
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
