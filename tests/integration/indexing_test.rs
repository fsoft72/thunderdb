use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator};
use thunderdb::storage::table_engine::{TableSchema, ColumnInfo};
use std::fs;

fn setup_db(name: &str) -> Database {
    let path = format!("/tmp/thunderdb_test_{}", name);
    let _ = fs::remove_dir_all(&path);
    Database::open(&path).expect("Failed to open database")
}

#[test]
fn test_index_creation_and_persistence() {
    let data_dir = "/tmp/thunderdb_test_index_persist";
    let _ = fs::remove_dir_all(data_dir);
    
    let table_name = "users";
    
    // 1. Create DB, table, and index
    {
        let mut db = Database::open(data_dir).unwrap();
        let table = db.get_or_create_table(table_name).unwrap();
        
        table.set_schema(TableSchema {
            columns: vec![
                ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
                ColumnInfo { name: "age".to_string(), data_type: "INT".to_string() },
            ]
        }).unwrap();
        
        table.index_manager_mut().create_index("age").unwrap();
        
        db.insert_row(table_name, vec![Value::Int32(1), Value::Int32(25)]).unwrap();
        db.insert_row(table_name, vec![Value::Int32(2), Value::Int32(30)]).unwrap();
        
        // Flush to disk
        db.get_table_mut(table_name).unwrap().flush().unwrap();
        db.get_table_mut(table_name).unwrap().index_manager().flush().unwrap();
    }
    
    // 2. Reopen DB and verify index works
    {
        let mut db = Database::open(data_dir).unwrap();
        
        // Search using index
        let results = db.scan(table_name, vec![
            Filter::new("age", Operator::Equals(Value::Int32(30)))
        ]).unwrap();
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::Int32(2));
        
        // Verify index is present
        let table = db.get_table_mut(table_name).unwrap();
        assert!(table.index_manager().has_index("age"));
    }
    
    fs::remove_dir_all(data_dir).ok();
}

#[test]
fn test_index_range_queries() {
    let mut db = setup_db("index_range");
    let table_name = "products";
    
    let table = db.get_or_create_table(table_name).unwrap();
    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "price".to_string(), data_type: "INT".to_string() },
        ]
    }).unwrap();
    table.index_manager_mut().create_index("price").unwrap();
    
    for i in 1..=10 {
        db.insert_row(table_name, vec![Value::Int32(i), Value::Int32(i * 10)]).unwrap();
    }
    
    // Test BETWEEN
    let results = db.scan(table_name, vec![
        Filter::new("price", Operator::Between(Value::Int32(30), Value::Int32(60)))
    ]).unwrap();
    assert_eq!(results.len(), 4); // 30, 40, 50, 60
    
    // Test GreaterThan (Note: choose_index currently only supports Equals and Between for indices)
    // If it falls back to scan, it should still be correct.
    let results = db.scan(table_name, vec![
        Filter::new("price", Operator::GreaterThan(Value::Int32(80)))
    ]).unwrap();
    assert_eq!(results.len(), 2); // 90, 100
}

#[test]
fn test_multiple_filters_with_index() {
    let mut db = setup_db("multi_filter_index");
    let table_name = "employees";
    
    let table = db.get_or_create_table(table_name).unwrap();
    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "dept".to_string(), data_type: "VARCHAR".to_string() },
            ColumnInfo { name: "salary".to_string(), data_type: "INT".to_string() },
        ]
    }).unwrap();
    table.index_manager_mut().create_index("dept").unwrap();
    
    db.insert_row(table_name, vec![Value::Int32(1), Value::varchar("IT".to_string()), Value::Int32(5000)]).unwrap();
    db.insert_row(table_name, vec![Value::Int32(2), Value::varchar("IT".to_string()), Value::Int32(6000)]).unwrap();
    db.insert_row(table_name, vec![Value::Int32(3), Value::varchar("HR".to_string()), Value::Int32(5500)]).unwrap();
    
    // Combined filter: dept='IT' (indexed) AND salary > 5500 (not indexed)
    let results = db.scan(table_name, vec![
        Filter::new("dept", Operator::Equals(Value::varchar("IT".to_string()))),
        Filter::new("salary", Operator::GreaterThan(Value::Int32(5500)))
    ]).unwrap();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], Value::Int32(2));
}

#[test]
fn test_index_with_updates_and_deletes() {
    let mut db = setup_db("index_updates");
    let table_name = "tasks";
    
    let table = db.get_or_create_table(table_name).unwrap();
    table.set_schema(TableSchema {
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
            ColumnInfo { name: "status".to_string(), data_type: "VARCHAR".to_string() },
        ]
    }).unwrap();
    table.index_manager_mut().create_index("status").unwrap();
    
    db.insert_row(table_name, vec![Value::Int32(1), Value::varchar("todo".to_string())]).unwrap();
    db.insert_row(table_name, vec![Value::Int32(2), Value::varchar("todo".to_string())]).unwrap();
    
    // Verify initial state
    assert_eq!(db.count(table_name, vec![Filter::new("status", Operator::Equals(Value::varchar("todo".to_string())))]).unwrap(), 2);
    
    // Update row 1 to 'done'
    db.update(table_name, 
        vec![Filter::new("id", Operator::Equals(Value::Int32(1)))],
        vec![("status".to_string(), Value::varchar("done".to_string()))]
    ).unwrap();
    
    // Verify index reflected update
    assert_eq!(db.count(table_name, vec![Filter::new("status", Operator::Equals(Value::varchar("todo".to_string())))]).unwrap(), 1);
    assert_eq!(db.count(table_name, vec![Filter::new("status", Operator::Equals(Value::varchar("done".to_string())))]).unwrap(), 1);
    
    // Delete row 2
    db.delete(table_name, vec![Filter::new("id", Operator::Equals(Value::Int32(2)))]).unwrap();
    
    // Verify index reflected deletion
    assert_eq!(db.count(table_name, vec![Filter::new("status", Operator::Equals(Value::varchar("todo".to_string())))]).unwrap(), 0);
}
