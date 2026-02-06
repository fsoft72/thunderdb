#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;
#[cfg(feature = "wasm")]
use crate::Database;

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub struct ThunderDBWasm {
    db: Database,
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl ThunderDBWasm {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        let db = Database::open_in_memory().unwrap();
        Self { db }
    }

    pub fn query(&mut self, sql: &str) -> String {
        // Use the database to list tables as a simple test of integration
        let tables = self.db.list_tables();
        format!("Executing SQL: {}. Current tables: {:?}", sql, tables)
    }

    pub fn version(&self) -> String {
        crate::VERSION.to_string()
    }
}
