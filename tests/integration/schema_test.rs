use thunderdb::{Database, Value, DirectDataAccess};
use thunderdb::storage::table_engine::{TableSchema, ColumnInfo};
use std::fs;

#[test]
fn test_schema_enforcement_behavior() {
    let data_dir = "/tmp/thunderdb_test_schema";
    let _ = fs::remove_dir_all(data_dir);
    let mut db = Database::open(data_dir).unwrap();
    
    let table_name = "users";
    let table = db.get_or_create_table(table_name).unwrap();
    
    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
        ]
    }).unwrap();
    
    // Currently, it should NOT fail because we don't enforce schema on insert_row yet
    let result = db.insert_row(table_name, vec![Value::Int32(1)]); 
    assert!(result.is_ok(), "Expected it to succeed because schema is not enforced yet");
    
    let result = db.insert_row(table_name, vec![Value::Int32(2), Value::Varchar("Alice".to_string()), Value::Int32(100)]);
    assert!(result.is_ok(), "Expected it to succeed even with extra columns");
    
    fs::remove_dir_all(data_dir).ok();
}
