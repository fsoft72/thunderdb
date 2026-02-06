use thunderdb::{Database, Result};
use std::env;
use std::fs;

#[cfg(feature = "repl")]
use thunderdb::repl::Repl;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    // Open database (default to ./data)
    let mut db = Database::open("./data")?;

    if args.len() > 1 {
        // Run from file
        let file_path = &args[1];
        let content = fs::read_to_string(file_path)?;

        #[cfg(feature = "repl")]
        {
            let mut repl = Repl::new(&mut db)?;
            let mut buffer = String::new();

            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with("--") {
                    continue;
                }

                buffer.push_str(line);
                buffer.push(' ');

                if buffer.trim().ends_with(';') || buffer.trim().starts_with('.') {
                    let is_special = buffer.trim().starts_with('.');
                    let sql = if is_special {
                        buffer.trim()
                    } else {
                        buffer.trim().trim_end_matches(';').trim()
                    };

                    if !sql.is_empty() {
                        if !is_special {
                            println!("Executing: {};", sql);
                        } else {
                            println!("Executing: {}", sql);
                        }

                        if is_special {
                            // Handle special commands
                            if let Some(cmd) = thunderdb::repl::commands::parse_special_command(sql) {
                                match cmd {
                                    thunderdb::repl::commands::SpecialCommand::Exit => break,
                                    thunderdb::repl::commands::SpecialCommand::Help => repl.show_help(),
                                    thunderdb::repl::commands::SpecialCommand::Tables => repl.show_tables(),
                                    thunderdb::repl::commands::SpecialCommand::Schema(t) => repl.show_schema(&t),
                                    thunderdb::repl::commands::SpecialCommand::Stats(t) => repl.show_stats(&t),
                                }
                            }
                        } else {
                            repl.execute_sql(sql);
                        }
                    }
                    buffer.clear();
                }
            }
        }
        #[cfg(not(feature = "repl"))]
        {
            println!("REPL feature (required for execution) not enabled.");
        }
    } else {
        // Run REPL
        #[cfg(feature = "repl")]
        {
            let mut repl = Repl::new(&mut db)?;
            repl.run()?;
        }

        #[cfg(not(feature = "repl"))]
        {
            println!("ThunderDB v{} - By Fabio Rotondo - OS3 srl - https://github.com/fsoft72", thunderdb::VERSION);
            println!("Database opened at: {}", db.config().storage.data_dir);
            println!();
            println!("REPL feature not enabled. Compile with --features repl to enable.");
        }
    }

    Ok(())
}
