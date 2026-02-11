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
        use crate::query::DirectDataAccess;
        use crate::storage::Value;

        let sql = sql.trim();
        if sql.to_uppercase().starts_with("CREATE TABLE") {
            // Simplified create table for demo
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 3 {
                let table_name = parts[2].trim_end_matches('(').trim_end_matches(';');
                match self.db.get_or_create_table(table_name) {
                    Ok(_) => return format!("Table '{}' created successfully (in-memory).", table_name),
                    Err(e) => return format!("Error creating table: {}", e),
                }
            }
        } else if sql.to_uppercase().starts_with("INSERT INTO") {
            // Simplified insert: INSERT INTO table VALUES (val1, val2, ...)
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 3 {
                let table_name = parts[2];
                // Very crude extraction of values
                if let Some(start_idx) = sql.find('(') {
                    if let Some(end_idx) = sql.rfind(')') {
                        let val_str = &sql[start_idx+1..end_idx];
                        let vals: Vec<Value> = val_str.split(',')
                            .map(|v| Value::varchar(v.trim().trim_matches('\'').to_string()))
                            .collect();
                        
                        match self.db.insert_row(table_name, vals) {
                            Ok(id) => return format!("Row inserted with ID: {}", id),
                            Err(e) => return format!("Error inserting row: {}", e),
                        }
                    }
                }
            }
        } else if sql.to_uppercase().starts_with("SELECT") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            // SELECT * FROM table
            if parts.len() >= 4 && parts[2].to_uppercase() == "FROM" {
                let table_name = parts[3].trim_end_matches(';');
                match self.db.scan(table_name, vec![]) {
                    Ok(rows) => {
                        if rows.is_empty() {
                            return "No rows found.".to_string();
                        }
                        let mut result = format!("Found {} rows:\n", rows.len());
                        for row in rows {
                            result.push_str(&format!("ID {}: {:?}\n", row.row_id, row.values));
                        }
                        return result;
                    },
                    Err(e) => return format!("Error scanning table: {}", e),
                }
            }
        }

        format!("Query received: {}. (Currently supporting simple CREATE TABLE, INSERT, and SELECT * FROM)", sql)
    }

    pub fn version(&self) -> String {
        crate::VERSION.to_string()
    }
}
