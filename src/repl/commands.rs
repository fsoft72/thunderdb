// Special REPL commands - Step 5.2

/// Special command types
#[derive(Debug, Clone, PartialEq)]
pub enum SpecialCommand {
    /// .exit or .quit - Exit REPL
    Exit,
    /// .help - Show help
    Help,
    /// .tables - List all tables
    Tables,
    /// .schema [table] - Show table schema
    Schema(Option<String>),
    /// .stats [table] - Show table statistics
    Stats(Option<String>),
    /// .save - Save database to disk (when in memory mode)
    Save,
}

/// Parse a special command (starts with .)
pub fn parse_special_command(line: &str) -> Option<SpecialCommand> {
    let line = line.trim();

    if !line.starts_with('.') {
        return None;
    }

    let parts: Vec<&str> = line[1..].split_whitespace().collect();

    if parts.is_empty() {
        return None;
    }

    let cmd = parts[0].to_lowercase();

    match cmd.as_str() {
        "exit" | "quit" => Some(SpecialCommand::Exit),
        "help" => Some(SpecialCommand::Help),
        "save" => Some(SpecialCommand::Save),
        "tables" => Some(SpecialCommand::Tables),
        "schema" => {
            let table = parts.get(1).map(|s| s.to_string());
            Some(SpecialCommand::Schema(table))
        }
        "stats" => {
            let table = parts.get(1).map(|s| s.to_string());
            Some(SpecialCommand::Stats(table))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exit() {
        assert_eq!(parse_special_command(".exit"), Some(SpecialCommand::Exit));
        assert_eq!(parse_special_command(".quit"), Some(SpecialCommand::Exit));
        assert_eq!(parse_special_command(".EXIT"), Some(SpecialCommand::Exit));
    }

    #[test]
    fn test_parse_help() {
        assert_eq!(parse_special_command(".help"), Some(SpecialCommand::Help));
        assert_eq!(parse_special_command(".HELP"), Some(SpecialCommand::Help));
    }

    #[test]
    fn test_parse_tables() {
        assert_eq!(parse_special_command(".tables"), Some(SpecialCommand::Tables));
    }

    #[test]
    fn test_parse_schema() {
        assert_eq!(
            parse_special_command(".schema users"),
            Some(SpecialCommand::Schema(Some("users".to_string())))
        );
        assert_eq!(
            parse_special_command(".schema"),
            Some(SpecialCommand::Schema(None))
        );
    }

    #[test]
    fn test_parse_stats() {
        assert_eq!(
            parse_special_command(".stats users"),
            Some(SpecialCommand::Stats(Some("users".to_string())))
        );
        assert_eq!(
            parse_special_command(".stats"),
            Some(SpecialCommand::Stats(None))
        );
    }

    #[test]
    fn test_parse_invalid() {
        assert_eq!(parse_special_command("SELECT * FROM users"), None);
        assert_eq!(parse_special_command(".unknown"), None);
        assert_eq!(parse_special_command(""), None);
    }
}
