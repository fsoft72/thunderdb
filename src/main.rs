use thunderdb::{Database, Result};
use std::env;
use std::fs;

#[cfg(feature = "repl")]
use thunderdb::repl::Repl;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut in_memory = false;
    let mut data_dir = "./data".to_string();
    let mut file_to_run = None;

    // Parse arguments
    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--memory" {
            in_memory = true;
        } else if arg == "--data-dir" && i + 1 < args.len() {
            i += 1;
            data_dir = args[i].clone();
        } else if arg.starts_with("--") {
            eprintln!("Unknown option: {}", arg);
            std::process::exit(1);
        } else if file_to_run.is_none() {
            file_to_run = Some(arg.clone());
        }
        i += 1;
    }

    // Open database
    let mut db = if in_memory {
        if data_dir == "./data" && !std::path::Path::new("./data").exists() {
            // No explicit data dir provided and default doesn't exist, use pure in-memory
            Database::open_in_memory()?
        } else {
            // Load from disk but stay in memory
            let mut database = Database::open(&data_dir)?;
            database.config_mut().storage.in_memory = true;
            database
        }
    } else {
        Database::open(&data_dir)?
    };

    if db.config().storage.in_memory {
        println!("ThunderDB operating in MEMORY mode.");
        if db.config().storage.data_dir != ":memory:" {
            println!("Database loaded from: {}", db.config().storage.data_dir);
        }
    } else {
        println!("ThunderDB operating in DISK mode at: {}", db.config().storage.data_dir);
    }
    println!();

    if let Some(file_path) = file_to_run {
        // Run from file
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
                                    thunderdb::repl::commands::SpecialCommand::Save => {
                                        if let Err(e) = repl.save() {
                                            eprintln!("Error saving database: {}", e);
                                        }
                                    }
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
            if db.config().storage.in_memory {
                println!("Operating in MEMORY mode.");
            }
            println!();
            println!("REPL feature not enabled. Compile with --features repl to enable.");
        }
    }

    // Explicit save on shutdown if in memory mode
    if db.config().storage.in_memory {
        println!("Saving database to disk before shutdown...");
        match db.save() {
            Ok(()) => println!("Database saved successfully."),
            Err(e) => eprintln!("Error saving database: {}", e),
        }
    }

    Ok(())
}
