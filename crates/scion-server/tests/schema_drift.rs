//! The Postgres schema keeps the tables and columns of the SQLite schema.
//! Both backends hold the same rows, so a column that one has and the
//! other lacks is a bug that the queries of the other backend hit at
//! runtime. This test reads both sets of migrations and compares the
//! tables, their columns, and the coarse kind of each column.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One column of a table: its coarse kind, which the two dialects spell
/// differently.
#[derive(Debug, PartialEq, Eq)]
struct Column {
    kind: &'static str,
}

/// The schema: table name, then its columns with their kinds.
type Schema = BTreeMap<String, Vec<(String, Column)>>;

#[derive(Clone, Copy)]
enum Dialect {
    Sqlite,
    Postgres,
}

/// Reads every `.sql` file of `dir`, oldest first, and applies its
/// statements: tables appear, gain columns, and rename.
fn read_schema(dir: &Path, dialect: Dialect) -> Schema {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    paths.sort();
    let mut schema = Schema::new();
    for path in paths {
        let sql = std::fs::read_to_string(&path).unwrap();
        for statement in sql.split(';') {
            apply(statement, dialect, &mut schema);
        }
    }
    schema
}

/// Applies one SQL statement to `schema`.
fn apply(statement: &str, dialect: Dialect, schema: &mut Schema) {
    // A comment belongs to no statement.
    let sql: String = statement
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let statement = sql.trim();
    if let Some(rest) = statement.strip_prefix("CREATE TABLE") {
        let open = rest.find('(').unwrap();
        let name = rest[..open]
            .split_whitespace()
            .last()
            .unwrap()
            .trim_matches('"')
            .to_owned();
        let end = matching_paren(rest, open);
        schema.insert(name, columns_of(&rest[open + 1..end], dialect));
    } else if let Some(rest) = statement.strip_prefix("ALTER TABLE ") {
        let mut words = rest.split_whitespace();
        let table = words.next().unwrap().trim_matches('"').to_owned();
        let action = words.collect::<Vec<_>>().join(" ");
        if let Some(new) = action.strip_prefix("RENAME TO ") {
            let new = new.trim().trim_matches('"').to_owned();
            let columns = schema.remove(&table).unwrap();
            schema.insert(new, columns);
        } else {
            let rest = action.strip_prefix("ADD ").unwrap_or(&action);
            let rest = rest.strip_prefix("COLUMN ").unwrap_or(rest);
            let mut words = rest.split_whitespace();
            let column = words.next().unwrap().trim_matches('"').to_owned();
            let kind = words.next().unwrap();
            let kind = match dialect {
                Dialect::Sqlite => sqlite_kind(kind),
                Dialect::Postgres => postgres_kind(kind),
            };
            schema
                .entry(table)
                .or_default()
                .push((column, Column { kind }));
        }
    }
}

/// The index of the parenthesis that closes the one at `open`.
fn matching_paren(s: &str, open: usize) -> usize {
    let mut depth = 0usize;
    for (j, c) in s[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return open + j;
                }
            }
            _ => {}
        }
    }
    panic!("unclosed parenthesis in {s:?}")
}

/// Splits the body of a `CREATE TABLE` on the commas that no parenthesis
/// of a constraint or a foreign key hides, then keeps the column lines.
fn columns_of(body: &str, dialect: Dialect) -> Vec<(String, Column)> {
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut defs = Vec::new();
    for (i, c) in body.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                defs.push(&body[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    defs.push(&body[start..]);
    defs.iter()
        .filter_map(|def| {
            let mut words = def.split_whitespace();
            let name = words.next()?.trim_matches('"');
            if !is_column_name(name) {
                return None; // a constraint line
            }
            let kind = words.next()?;
            if !is_type(kind) {
                return None; // a constraint line, such as REFERENCES
            }
            let kind = match dialect {
                Dialect::Sqlite => sqlite_kind(kind),
                Dialect::Postgres => postgres_kind(kind),
            };
            Some((name.to_owned(), Column { kind }))
        })
        .collect()
}

/// Whether `name` names a column, not a constraint.
fn is_column_name(name: &str) -> bool {
    !matches!(
        name,
        "FOREIGN" | "CHECK" | "UNIQUE" | "PRIMARY" | "CONSTRAINT"
    ) && name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

/// Whether `kind` is a type of either dialect, not a keyword of a
/// constraint, such as `REFERENCES`.
fn is_type(kind: &str) -> bool {
    matches!(
        kind,
        "INTEGER" | "INT" | "TEXT" | "BLOB" | "REAL" | "BIGINT" | "BYTEA" | "DOUBLE" | "BOOLEAN"
    )
}

/// The coarse kind of a SQLite type.
fn sqlite_kind(kind: &str) -> &'static str {
    match kind {
        "INTEGER" | "INT" => "int",
        "TEXT" => "text",
        "BLOB" => "bytes",
        "REAL" => "float",
        other => panic!("unknown SQLite type {other:?}"),
    }
}

/// The coarse kind of a Postgres type.
fn postgres_kind(kind: &str) -> &'static str {
    match kind {
        "BIGINT" | "INTEGER" | "INT" => "int",
        "TEXT" => "text",
        "BYTEA" => "bytes",
        "DOUBLE" | "REAL" => "float",
        "BOOLEAN" => "bool",
        other => panic!("unknown Postgres type {other:?}"),
    }
}

#[test]
fn the_postgres_schema_holds_the_tables_and_columns_of_the_sqlite_schema() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let sqlite = read_schema(&dir, Dialect::Sqlite);
    let postgres = read_schema(&dir.join("postgres"), Dialect::Postgres);
    assert!(!sqlite.is_empty(), "the SQLite migrations hold tables");
    assert!(!postgres.is_empty(), "the Postgres migrations hold tables");

    let mut ok = true;
    for (name, columns) in &sqlite {
        let Some(pg_columns) = postgres.get(name) else {
            println!("table {name} is missing from the Postgres schema");
            ok = false;
            continue;
        };
        for (column, _) in columns {
            if !pg_columns.iter().any(|(n, _)| n == column) {
                println!("column {name}.{column} is missing from the Postgres schema");
                ok = false;
            }
        }
        for (column, _) in pg_columns {
            if !columns.iter().any(|(n, _)| n == column) {
                println!("column {name}.{column} is missing from the SQLite schema");
                ok = false;
            }
        }
    }
    for name in postgres.keys() {
        if !sqlite.contains_key(name) {
            println!("table {name} is missing from the SQLite schema");
            ok = false;
        }
    }
    assert!(ok, "the schemas drift");
}
