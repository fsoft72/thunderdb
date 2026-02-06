use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator};
use thunderdb::storage::table_engine::{TableSchema, ColumnInfo};
use std::fs;

fn setup_db(name: &str) -> Database {
    let path = format!("/tmp/thunderdb_test_{}", name);
    let _ = fs::remove_dir_all(&path);
    Database::open(&path).expect("Failed to open database")
}

#[test]
fn test_index_extended_operators() {
    let mut db = setup_db("extended_index");
    let table_name = "data";
    
    let table = db.get_or_create_table(table_name).unwrap();
    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "val".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
        ]
    }).unwrap();
    table.index_manager_mut().create_index("val").unwrap();
    table.index_manager_mut().create_index("name").unwrap();
    
    for i in 1..=10 {
        db.insert_row(table_name, vec![Value::Int32(i), Value::Varchar(format!("item_{:02}", i))]).unwrap();
    }
    
    // Test GreaterThan
    let results = db.scan(table_name, vec![Filter::new("val", Operator::GreaterThan(Value::Int32(7)))]).unwrap();
    assert_eq!(results.len(), 3); // 8, 9, 10
    
    // Test LessThanOrEqual
    let results = db.scan(table_name, vec![Filter::new("val", Operator::LessThanOrEqual(Value::Int32(3)))]).unwrap();
    assert_eq!(results.len(), 3); // 1, 2, 3
    
    // Test LIKE Prefix
    let results = db.scan(table_name, vec![Filter::new("name", Operator::Like("item_0%".to_string()))]).unwrap();
    assert_eq!(results.len(), 9); // item_01 to item_09
    
    let results = db.scan(table_name, vec![Filter::new("name", Operator::Like("item_1%".to_string()))]).unwrap();
    assert_eq!(results.len(), 1); // item_10

    // Test NULL values in index
    db.insert_row(table_name, vec![Value::Null, Value::Varchar("null_val".to_string())]).unwrap();
    let results = db.scan(table_name, vec![Filter::new("val", Operator::IsNull)]).unwrap();
    // Note: IsNull does not use index currently based on choose_index
    assert_eq!(results.len(), 1);
    
    // Test Equals(Null) - if supported
    let results = db.scan(table_name, vec![Filter::new("val", Operator::Equals(Value::Null))]).unwrap();
    assert_eq!(results.len(), 1);
    
    fs::remove_dir_all("/tmp/thunderdb_test_extended_index").ok();
}
