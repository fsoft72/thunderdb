use thunderdb::{Database, Result};

#[cfg(feature = "repl")]
use thunderdb::repl::Repl;

fn main() -> Result<()> {
    // Open database
    let mut db = Database::open("./data")?;

    #[cfg(feature = "repl")]
    {
        // Run REPL
        let mut repl = Repl::new(&mut db)?;
        repl.run()?;
    }

    #[cfg(not(feature = "repl"))]
    {
        println!("ThunderDB v{}", thunderdb::VERSION);
        println!("Database opened at: {}", db.config().storage.data_dir);
        println!();
        println!("REPL feature not enabled. Compile with --features repl to enable.");
    }

    Ok(())
}
