use thunderdb::{Database, Value, DirectDataAccess, Error};
use std::fs;

#[test]
fn test_table_not_found() {
    let data_dir = "/tmp/thunderdb_test_errors";
    let _ = fs::remove_dir_all(data_dir);
    let mut db = Database::open(data_dir).unwrap();
    
    // Try to get a non-existent table
    let result = db.get_by_id("non_existent", 1);
    assert!(matches!(result, Err(Error::TableNotFound(_))));
    
    // Try to scan a non-existent table
    let result = db.scan("non_existent", vec![]);
    assert!(matches!(result, Err(Error::TableNotFound(_))));
    
    // Try to drop a non-existent table
    let result = db.drop_table("non_existent");
    assert!(matches!(result, Err(Error::TableNotFound(_))));
    
    fs::remove_dir_all(data_dir).ok();
}

#[test]
fn test_insert_into_new_table() {
    let mut db = Database::open("/tmp/thunderdb_test_auto_create").unwrap();
    
    // insert_row should automatically create the table
    let result = db.insert_row("new_table", vec![Value::Int32(1)]);
    assert!(result.is_ok());
    
    assert!(db.list_tables().contains(&"new_table".to_string()));
    
    fs::remove_dir_all("/tmp/thunderdb_test_auto_create").ok();
}
