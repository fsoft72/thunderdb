use thunderdb::{Database, Result, Error, DirectDataAccess};

#[test]
fn test_select_from_non_existent_table_errors() -> Result<()> {
    let temp_dir = "/tmp/thunderdb_non_existent_test";
    let _ = std::fs::remove_dir_all(temp_dir);
    
    let mut db = Database::open(temp_dir)?;
    
    // This should fail because 'pippo' does not exist
    let result = db.scan("pippo", vec![]);
    
    match result {
        Err(Error::TableNotFound(name)) => assert_eq!(name, "pippo"),
        Err(e) => panic!("Expected TableNotFound error, got {:?}", e),
        Ok(_) => panic!("Expected error for non-existent table, but it succeeded"),
    }
    
            let _ = std::fs::remove_dir_all(temp_dir);
    
            Ok(())
    
        }
    
        
    
        #[test]
    
        fn test_list_tables_sees_disk_tables() -> Result<()> {
    
            let temp_dir = "/tmp/thunderdb_list_tables_test";
    
            let _ = std::fs::remove_dir_all(temp_dir);
    
            
    
            // 1. Create a table and close DB
    
            {
    
                let mut db = Database::open(temp_dir)?;
    
                db.insert_row("table1", vec![Value::Int32(1)])?;
    
            }
    
            
    
            // 2. Reopen DB, table1 should be visible in list_tables even before any operation
    
            {
    
                let db = Database::open(temp_dir)?;
    
                let tables = db.list_tables();
    
                assert!(tables.contains(&"table1".to_string()));
    
            }
    
            
    
            let _ = std::fs::remove_dir_all(temp_dir);
    
            Ok(())
    
        }
    
        
    
    
    
    #[test]
    
    fn test_insert_into_non_existent_table_creates_it() -> Result<()> {
    
        let temp_dir = "/tmp/thunderdb_insert_create_test";
    
        let _ = std::fs::remove_dir_all(temp_dir);
    
        
    
        let mut db = Database::open(temp_dir)?;
    
        
    
        // This should succeed and create the table
    
        let row_id = db.insert_row("new_table", vec![Value::Int32(123)])?;
    
        assert_eq!(row_id, 1);
    
        
    
        // Now SELECT should also succeed
    
        let rows = db.scan("new_table", vec![])?;
    
        assert_eq!(rows.len(), 1);
    
        
    
        let _ = std::fs::remove_dir_all(temp_dir);
    
        Ok(())
    
    }
    
    
    
    use thunderdb::Value;
    
    