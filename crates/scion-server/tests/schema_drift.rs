//! The Postgres schema holds the tables and columns of the SQLite schema,
//! with the same shape: a column that one has and the other lacks, a type
//! that differs, or a key, a foreign key, or an index on one side only is
//! a bug that the queries of the other backend hit at runtime. This test
//! reads both sets of migrations and compares what each builds.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One column of a table: its coarse kind, which the two dialects spell
/// differently, and its constraints.
#[derive(Debug, PartialEq, Eq, Default)]
struct Column {
    kind: &'static str,
    not_null: bool,
    primary_key: bool,
    unique: bool,
    /// The table a `REFERENCES` names, if the column has one.
    references: Option<String>,
}

/// The schema: table name, then its columns with their shape.
type Schema = BTreeMap<String, Vec<(String, Column)>>;

#[derive(Clone, Copy)]
enum Dialect {
    Sqlite,
    Postgres,
}

/// Reads every `.sql` file of `dir`, oldest first, and applies its
/// statements: tables appear, gain columns, and rename.
fn read_schema(dir: &Path, dialect: Dialect) -> (Schema, Vec<String>) {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    paths.sort();
    let mut schema = Schema::new();
    let mut indexes = Vec::new();
    for path in paths {
        let sql = std::fs::read_to_string(&path).unwrap();
        for statement in sql.split(';') {
            apply(statement, dialect, &mut schema, &mut indexes);
        }
    }
    (schema, indexes)
}

/// Applies one SQL statement to `schema`.
fn apply(statement: &str, dialect: Dialect, schema: &mut Schema, indexes: &mut Vec<String>) {
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
            let name = column_name_of(rest);
            let _ = words.next();
            let kind = words.next().unwrap();
            let constraints = words.collect::<Vec<_>>().join(" ");
            let column = column_of(&constraints, kind, dialect);
            schema.entry(table).or_default().push((name, column));
        }
    } else if let Some(rest) = statement.strip_prefix("CREATE INDEX ") {
        indexes.push(index_name(rest));
    } else if let Some(rest) = statement.strip_prefix("CREATE UNIQUE INDEX ") {
        indexes.push(index_name(rest));
    }
}

/// The name an index definition starts with.
fn index_name(rest: &str) -> String {
    let rest = rest.trim_start();
    let rest = rest.strip_prefix("IF NOT EXISTS ").unwrap_or(rest);
    rest.split_whitespace()
        .next()
        .unwrap()
        .trim_matches('"')
        .to_owned()
}

/// The column of an `ADD COLUMN` line: the words after the name are its
/// type, then its constraints.
fn column_name_of(rest: &str) -> String {
    rest.split_whitespace()
        .next()
        .unwrap()
        .trim_matches('"')
        .to_owned()
}

fn column_of(constraints: &str, kind: &str, dialect: Dialect) -> Column {
    let kind = match dialect {
        Dialect::Sqlite => sqlite_kind(kind),
        Dialect::Postgres => postgres_kind(kind),
    };
    let references = constraints
        .split_whitespace()
        .zip(constraints.split_whitespace().skip(1))
        .find(|(prev, _)| *prev == "REFERENCES")
        .map(|(_, table)| table.trim_matches(',').trim_matches('"').to_owned());
    Column {
        kind,
        not_null: constraints.contains("NOT NULL"),
        primary_key: constraints.contains("PRIMARY KEY"),
        unique: constraints.contains("UNIQUE"),
        references,
    }
}

/// The columns of a `CREATE TABLE` body, constraints included.
fn columns_of(body: &str, dialect: Dialect) -> Vec<(String, Column)> {
    let defs = split_defs(body);
    let mut columns: Vec<(String, Column)> = Vec::new();
    let mut table_primary_key: Option<Vec<String>> = None;
    for def in defs {
        let words: Vec<&str> = def.split_whitespace().collect();
        let Some(first) = words.first() else {
            continue;
        };
        let name = first.trim_matches('"');
        if matches!(
            name,
            "FOREIGN" | "CHECK" | "UNIQUE" | "PRIMARY" | "CONSTRAINT"
        ) {
            apply_table_constraint(def, &mut columns, &mut table_primary_key);
            continue;
        }
        let Some(kind) = words.get(1) else {
            continue;
        };
        if !is_type(kind) {
            continue; // a constraint line, such as `REFERENCES`
        }
        let constraints = words[2..].join(" ");
        columns.push((name.to_owned(), column_of(&constraints, kind, dialect)));
    }
    // A table-level `PRIMARY KEY (a, b)` marks its columns.
    if let Some(keys) = table_primary_key {
        for (name, column) in &mut columns {
            if keys.contains(name) {
                column.primary_key = true;
            }
        }
    }
    columns
}

/// Applies one table-level constraint to `columns`.
fn apply_table_constraint(
    def: &str,
    columns: &mut [(String, Column)],
    table_primary_key: &mut Option<Vec<String>>,
) {
    let names = |rest: &str| -> Vec<String> {
        let open = rest.find('(').unwrap();
        let close = matching_paren(rest, open);
        rest[open + 1..close]
            .split(',')
            .map(|n| n.trim().trim_matches('"').to_owned())
            .collect()
    };
    if let Some(rest) = def.strip_prefix("PRIMARY KEY") {
        *table_primary_key = Some(names(rest));
    } else if let Some(rest) = def.strip_prefix("UNIQUE") {
        for name in names(rest) {
            if let Some((_, column)) = columns.iter_mut().find(|(n, _)| *n == name) {
                column.unique = true;
            }
        }
    } else if let Some(rest) = def.strip_prefix("FOREIGN KEY") {
        let columns_of_key = names(rest);
        if let Some(table) = def.split("REFERENCES").nth(1).and_then(|t| {
            t.split_whitespace()
                .next()
                .map(|t| t.trim_matches('"').to_owned())
        }) {
            for name in columns_of_key {
                if let Some((_, column)) = columns.iter_mut().find(|(n, _)| *n == name) {
                    column.references = Some(table.clone());
                }
            }
        }
    }
}

/// Splits the body of a `CREATE TABLE` on the commas that no parenthesis
/// of a constraint or a foreign key hides.
fn split_defs(body: &str) -> Vec<&str> {
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
    defs
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
    let (sqlite, sqlite_indexes) = read_schema(&dir, Dialect::Sqlite);
    let (postgres, postgres_indexes) = read_schema(&dir.join("postgres"), Dialect::Postgres);
    assert!(!sqlite.is_empty(), "the SQLite migrations hold tables");
    assert!(!postgres.is_empty(), "the Postgres migrations hold tables");

    let mut ok = true;
    let mut say = |message: String, ok: &mut bool| {
        println!("{message}");
        *ok = false;
    };
    for (name, columns) in &sqlite {
        let Some(pg_columns) = postgres.get(name) else {
            say(
                format!("table {name} is missing from the Postgres schema"),
                &mut ok,
            );
            continue;
        };
        for (column, shape) in columns {
            let Some(pg_shape) = pg_columns.iter().find(|(n, _)| n == column).map(|(_, c)| c)
            else {
                say(
                    format!("column {name}.{column} is missing from the Postgres schema"),
                    &mut ok,
                );
                continue;
            };
            if pg_shape != shape {
                say(
                    format!(
                        "column {name}.{column} is {shape:?} in SQLite, {pg_shape:?} in Postgres"
                    ),
                    &mut ok,
                );
            }
        }
        for (column, _) in pg_columns {
            if !columns.iter().any(|(n, _)| n == column) {
                say(
                    format!("column {name}.{column} is missing from the SQLite schema"),
                    &mut ok,
                );
            }
        }
    }
    for name in postgres.keys() {
        if !sqlite.contains_key(name) {
            say(
                format!("table {name} is missing from the SQLite schema"),
                &mut ok,
            );
        }
    }
    for index in &sqlite_indexes {
        if !postgres_indexes.contains(index) {
            say(
                format!("index {index} is missing from the Postgres schema"),
                &mut ok,
            );
        }
    }
    for index in &postgres_indexes {
        if !sqlite_indexes.contains(index) {
            say(
                format!("index {index} is missing from the SQLite schema"),
                &mut ok,
            );
        }
    }
    assert!(ok, "the schemas drift");
}
