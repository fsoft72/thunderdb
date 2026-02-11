// Result formatting - Step 5.3

use crate::storage::{Row, Value};
use std::fmt::Write as FmtWrite;

/// Format query results in tabular format
pub fn format_results(rows: &[Row], column_names: &[String]) -> String {
    if rows.is_empty() {
        return "No rows returned".to_string();
    }

    let mut output = String::new();

    // Calculate column widths
    let mut widths = vec![0; column_names.len()];

    // Width from column names
    for (i, name) in column_names.iter().enumerate() {
        widths[i] = name.len();
    }

    // Width from data (with max length limit)
    const MAX_DISPLAY_WIDTH: usize = 50;
    for row in rows {
        for (i, value) in row.values.iter().enumerate() {
            if i < widths.len() {
                let len = format_value_display(value, MAX_DISPLAY_WIDTH).len();
                widths[i] = widths[i].max(len);
            }
        }
    }

    // Cap widths at reasonable maximum
    for width in &mut widths {
        *width = (*width).min(MAX_DISPLAY_WIDTH);
    }

    // Draw top border
    write_separator(&mut output, &widths);

    // Write header
    output.push('|');
    for (i, name) in column_names.iter().enumerate() {
        write!(
            &mut output,
            " {:<width$} |",
            truncate_string(name, widths[i]),
            width = widths[i]
        )
        .unwrap();
    }
    output.push('\n');

    // Draw header separator
    write_separator(&mut output, &widths);

    // Write data rows
    for row in rows {
        output.push('|');
        for (i, value) in row.values.iter().enumerate() {
            if i < widths.len() {
                let display = format_value_display(value, widths[i]);
                write!(&mut output, " {:<width$} |", display, width = widths[i]).unwrap();
            }
        }
        output.push('\n');
    }

    // Draw bottom border
    write_separator(&mut output, &widths);

    output
}

/// Write a separator line
fn write_separator(output: &mut String, widths: &[usize]) {
    output.push('+');
    for &width in widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');
}

/// Format a value for display
fn format_value_display(value: &Value, max_width: usize) -> String {
    let s = match value {
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => format!("{:.2}", v),
        Value::Float64(v) => format!("{:.2}", v),
        Value::Varchar(v) => v.as_str().to_string(),
        Value::Timestamp(v) => v.to_string(),
        Value::Null => "NULL".to_string(),
    };

    truncate_string(&s, max_width)
}

/// Truncate a string to a maximum width with ellipsis
fn truncate_string(s: &str, max_width: usize) -> String {
    if s.len() <= max_width {
        s.to_string()
    } else if max_width <= 3 {
        "...".to_string()
    } else {
        let mut truncated = s.chars().take(max_width - 3).collect::<String>();
        truncated.push_str("...");
        truncated
    }
}

/// Format execution summary
pub fn format_summary(row_count: usize, elapsed_ms: f64) -> String {
    format!("{} row(s) ({:.2}ms)", row_count, elapsed_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_empty() {
        let rows: Vec<Row> = vec![];
        let columns = vec!["id".to_string(), "name".to_string()];
        let result = format_results(&rows, &columns);
        assert_eq!(result, "No rows returned");
    }

    #[test]
    fn test_format_single_row() {
        let rows = vec![Row::new(
            1,
            vec![Value::Int32(1), Value::varchar("Alice".to_string())],
        )];
        let columns = vec!["id".to_string(), "name".to_string()];
        let result = format_results(&rows, &columns);

        assert!(result.contains("id"));
        assert!(result.contains("name"));
        assert!(result.contains("1"));
        assert!(result.contains("Alice"));
    }

    #[test]
    fn test_format_multiple_rows() {
        let rows = vec![
            Row::new(
                1,
                vec![Value::Int32(1), Value::varchar("Alice".to_string())],
            ),
            Row::new(
                2,
                vec![Value::Int32(2), Value::varchar("Bob".to_string())],
            ),
            Row::new(
                3,
                vec![Value::Int32(3), Value::varchar("Charlie".to_string())],
            ),
        ];
        let columns = vec!["id".to_string(), "name".to_string()];
        let result = format_results(&rows, &columns);

        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
        assert!(result.contains("Charlie"));
    }

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("hello", 10), "hello");
        assert_eq!(truncate_string("hello world", 5), "he...");
        assert_eq!(truncate_string("hello", 3), "...");
        assert_eq!(truncate_string("hello", 5), "hello");
    }

    #[test]
    fn test_format_value_display() {
        assert_eq!(format_value_display(&Value::Int32(42), 10), "42");
        assert_eq!(format_value_display(&Value::Null, 10), "NULL");
        assert_eq!(
            format_value_display(&Value::varchar("test".to_string()), 10),
            "test"
        );
        assert_eq!(
            format_value_display(&Value::varchar("very long string".to_string()), 5),
            "ve..."
        );
    }

    #[test]
    fn test_format_summary() {
        assert_eq!(format_summary(0, 1.23), "0 row(s) (1.23ms)");
        assert_eq!(format_summary(1, 2.5), "1 row(s) (2.50ms)");
        assert_eq!(format_summary(100, 15.678), "100 row(s) (15.68ms)");
    }

    #[test]
    fn test_format_with_null() {
        let rows = vec![Row::new(
            1,
            vec![Value::Int32(1), Value::Null, Value::varchar("test".to_string())],
        )];
        let columns = vec!["id".to_string(), "middle".to_string(), "name".to_string()];
        let result = format_results(&rows, &columns);

        assert!(result.contains("NULL"));
    }

    #[test]
    fn test_format_with_floats() {
        let rows = vec![Row::new(
            1,
            vec![Value::Float64(3.14159), Value::Float32(2.718)],
        )];
        let columns = vec!["pi".to_string(), "e".to_string()];
        let result = format_results(&rows, &columns);

        assert!(result.contains("3.14"));
        assert!(result.contains("2.72"));
    }
}
