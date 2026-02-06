#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;
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
        // In WASM, we might want to use a virtual file system or just memory.
        // For now, let's use a dummy path. 
        // Note: Real persistence in WASM requires more complex setup (IndexedDB, etc.)
        let db = Database::open("/tmp/thunderdb").unwrap();
        Self { db }
    }

    pub fn query(&mut self, sql: &str) -> String {
        // Simple placeholder for SQL execution in WASM
        format!("Executing SQL: {}", sql)
    }

    pub fn version(&self) -> String {
        crate::VERSION.to_string()
    }
}
