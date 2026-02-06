use thunderdb::{Database, Result};

fn main() -> Result<()> {
    println!("ThunderDB v{}", thunderdb::VERSION);
    println!("Type .help for help, .exit to quit");
    println!();

    // For now, just open a database and show it works
    let db = Database::open("./data")?;
    println!("Database opened successfully at: {}", db.config().storage.data_dir);
    println!();
    println!("REPL not yet implemented - coming in Phase 5!");

    Ok(())
}
