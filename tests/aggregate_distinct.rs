//! Correctness tests for Database::aggregate and Database::distinct.

use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
use thunderdb::{Aggregate, Database, DirectDataAccess, Filter, Operator, Value};

/// Open a fresh Database with the given schema in a unique temp directory.
///
/// Uses the standard chicken-and-egg fixture pattern: insert a placeholder row
/// to materialize the table, override the schema, then delete all rows.
fn open_with_schema(cols: Vec<(&str, &str)>) -> Database {
    let dir = std::env::temp_dir().join(format!(
        "thunderdb_aggdist_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = Database::open(&dir).unwrap();
    // Insert one dummy row to force table creation, then overwrite schema.
    let mut placeholder: Vec<Value> = (0..cols.len()).map(|_| Value::Int64(0)).collect();
    placeholder[0] = Value::Int64(1);
    db.insert_batch("t", vec![placeholder]).unwrap();
    {
        let tbl = db.get_table_mut("t").unwrap();
        tbl.set_schema(TableSchema {
            columns: cols
                .iter()
                .map(|(n, ty)| ColumnInfo {
                    name: (*n).into(),
                    data_type: (*ty).into(),
                })
                .collect(),
        })
        .unwrap();
    }
    db.delete("t", vec![]).unwrap(); // empty the table
    db
}

#[test]
fn aggregate_count_star_empty() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    let r = db
        .aggregate("t", vec![], vec![Aggregate::Count], vec![])
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].keys, vec![]);
    assert_eq!(r[0].aggs, vec![Value::Int64(0)]);
}

#[test]
fn aggregate_sum_empty_is_null_not_zero() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    let r = db
        .aggregate("t", vec![], vec![Aggregate::Sum("v".into())], vec![])
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r[0].aggs,
        vec![Value::Null],
        "SUM of empty must be NULL (SQLite parity)"
    );
}

#[test]
fn aggregate_avg_min_max_skip_nulls() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    db.insert_batch(
        "t",
        vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Null],
            vec![Value::Int64(3), Value::Int64(30)],
        ],
    )
    .unwrap();

    let r = db
        .aggregate(
            "t",
            vec![],
            vec![
                Aggregate::Avg("v".into()),
                Aggregate::Min("v".into()),
                Aggregate::Max("v".into()),
            ],
            vec![],
        )
        .unwrap();

    assert_eq!(r.len(), 1);
    assert_eq!(r[0].aggs[0], Value::Float64(20.0));
    assert_eq!(r[0].aggs[1], Value::Int64(10));
    assert_eq!(r[0].aggs[2], Value::Int64(30));
}

#[test]
fn aggregate_group_by_with_null_key() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("k", "VARCHAR")]);
    db.insert_batch(
        "t",
        vec![
            vec![Value::Int64(1), Value::varchar("a")],
            vec![Value::Int64(2), Value::varchar("a")],
            vec![Value::Int64(3), Value::Null],
            vec![Value::Int64(4), Value::varchar("b")],
        ],
    )
    .unwrap();

    let mut r = db
        .aggregate("t", vec!["k".into()], vec![Aggregate::Count], vec![])
        .unwrap();
    r.sort_by_key(|row| format!("{:?}", row.keys));

    assert_eq!(r.len(), 3, "two non-null groups + one NULL group");
    let total: i64 = r
        .iter()
        .map(|row| match row.aggs[0] {
            Value::Int64(n) => n,
            _ => panic!("expected Int64 count"),
        })
        .sum();
    assert_eq!(total, 4);
}

#[test]
fn distinct_low_card_with_filter() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("k", "VARCHAR")]);
    db.insert_batch(
        "t",
        vec![
            vec![Value::Int64(1), Value::varchar("a")],
            vec![Value::Int64(2), Value::varchar("a")],
            vec![Value::Int64(3), Value::varchar("b")],
            vec![Value::Int64(4), Value::varchar("c")],
        ],
    )
    .unwrap();

    let mut d = db
        .distinct(
            "t",
            vec!["k".into()],
            vec![Filter::new(
                "id",
                Operator::GreaterThan(Value::Int64(1)),
            )],
        )
        .unwrap();
    d.sort();
    assert_eq!(d.len(), 3);
    assert_eq!(d[0], vec![Value::varchar("a")]);
    assert_eq!(d[1], vec![Value::varchar("b")]);
    assert_eq!(d[2], vec![Value::varchar("c")]);
}

#[test]
fn aggregate_sum_on_non_int_column_errors_at_plan_time() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("name", "VARCHAR")]);
    db.insert_batch(
        "t",
        vec![vec![Value::Int64(1), Value::varchar("hello")]],
    )
    .unwrap();

    let r = db.aggregate("t", vec![], vec![Aggregate::Sum("name".into())], vec![]);
    assert!(r.is_err(), "SUM on VARCHAR should error at plan time");
    let msg = format!("{:?}", r.unwrap_err());
    assert!(
        msg.contains("SUM/AVG requires INT64") && msg.contains("name"),
        "expected typed error mentioning column name, got: {}",
        msg
    );
}

#[test]
fn aggregate_avg_on_non_int_column_errors_at_plan_time() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("name", "VARCHAR")]);
    db.insert_batch(
        "t",
        vec![vec![Value::Int64(1), Value::varchar("hello")]],
    )
    .unwrap();

    let r = db.aggregate("t", vec![], vec![Aggregate::Avg("name".into())], vec![]);
    assert!(r.is_err(), "AVG on VARCHAR should error at plan time");
    let msg = format!("{:?}", r.unwrap_err());
    assert!(
        msg.contains("SUM/AVG requires INT64") && msg.contains("name"),
        "expected typed error mentioning column name, got: {}",
        msg
    );
}

#[test]
fn distinct_unknown_column_errors() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    let r = db.distinct("t", vec!["nope".into()], vec![]);
    assert!(r.is_err(), "DISTINCT on unknown column should error");
    let msg = format!("{:?}", r.unwrap_err());
    assert!(
        msg.contains("nope"),
        "error should name the unknown column, got: {}",
        msg
    );
}
