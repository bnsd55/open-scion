//! The Postgres schema holds the tables and columns of the SQLite schema,
//! with the same shape: a column that one has and the other lacks, a type
//! that differs, or a key, a foreign key, or an index on one side only is
//! a bug that the queries of the other backend hit at runtime. This test
//! reads both sets of migrations and compares what each builds.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// One column of a table: its coarse kind, which the two dialects spell
/// differently, and the foreign key it carries, if any.
#[derive(Debug, PartialEq, Eq, Default)]
struct Column {
    kind: &'static str,
    not_null: bool,
    /// The `table(column)` the column references, if it has one.
    references: Option<String>,
    /// The literal a `DEFAULT` names, if the column has one.
    default: Option<String>,
}

/// One table: its columns, and the constraints that name several of
/// them. A key of two columns is one group, not two flags: a schema
/// that splits a group, or joins two, drifts.
#[derive(Debug, PartialEq, Eq, Default)]
struct Table {
    columns: Vec<(String, Column)>,
    /// The columns of the primary key, in order.
    primary_key: Vec<String>,
    /// The column groups of every UNIQUE, in no order.
    uniques: BTreeSet<BTreeSet<String>>,
    /// The expressions of every CHECK, normalized, in no order. The two
    /// dialects spell the same check with different functions, which
    /// `normalize_check` folds.
    checks: BTreeSet<String>,
}

/// The schema: table name, then its shape.
type Schema = BTreeMap<String, Table>;

/// One index: its name, the table it orders, the columns it orders by,
/// whether it is UNIQUE, and its partial filter, which the two dialects
/// spell the same way.
#[derive(Debug, PartialEq, Eq)]
struct Index {
    table: String,
    columns: Vec<String>,
    unique: bool,
    filter: String,
}

#[derive(Clone, Copy)]
enum Dialect {
    Sqlite,
    Postgres,
}

/// Reads every `.sql` file of `dir`, oldest first, and applies its
/// statements: tables appear, gain columns, and rename.
fn read_schema(dir: &Path, dialect: Dialect) -> (Schema, BTreeMap<String, Index>) {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    paths.sort();
    let mut schema = Schema::new();
    let mut indexes = BTreeMap::new();
    for path in paths {
        let sql = std::fs::read_to_string(&path).unwrap();
        for statement in sql.split(';') {
            apply(statement, dialect, &mut schema, &mut indexes);
        }
    }
    (schema, indexes)
}

/// Applies one SQL statement to `schema`.
fn apply(
    statement: &str,
    dialect: Dialect,
    schema: &mut Schema,
    indexes: &mut BTreeMap<String, Index>,
) {
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
        schema.insert(name, table_of(&rest[open + 1..end], dialect));
    } else if let Some(rest) = statement.strip_prefix("ALTER TABLE ") {
        let mut words = rest.split_whitespace();
        let table = words.next().unwrap().trim_matches('"').to_owned();
        let action = words.collect::<Vec<_>>().join(" ");
        if let Some(new) = action.strip_prefix("RENAME TO ") {
            let new = new.trim().trim_matches('"').to_owned();
            let table = schema.remove(&table).unwrap();
            schema.insert(new, table);
        } else {
            let rest = action.strip_prefix("ADD ").unwrap_or(&action);
            let rest = rest.strip_prefix("COLUMN ").unwrap_or(rest);
            let name = column_name_of(rest);
            let mut words = rest.split_whitespace();
            let _ = words.next();
            let kind = words.next().unwrap();
            let constraints = words.collect::<Vec<_>>().join(" ");
            let (column, check) = column_of(&constraints, kind, dialect);
            let table = schema.entry(table).or_default();
            if let Some(check) = check {
                table.checks.insert(check);
            }
            table.columns.push((name, column));
        }
    } else if let Some(rest) = statement
        .strip_prefix("CREATE INDEX ")
        .or_else(|| statement.strip_prefix("CREATE UNIQUE INDEX "))
    {
        let unique = statement.starts_with("CREATE UNIQUE INDEX");
        let rest = rest.trim_start();
        let rest = rest.strip_prefix("IF NOT EXISTS ").unwrap_or(rest);
        let mut words = rest.split_whitespace();
        let name = words.next().unwrap().trim_matches('"').to_owned();
        let table = words
            .nth(1)
            .expect("an index names its table")
            .trim_matches('"')
            .to_owned();
        let open = rest.find('(').expect("an index lists its columns");
        let close = matching_paren(rest, open);
        let columns = rest[open + 1..close]
            .split(',')
            .map(|c| c.trim().trim_matches('"').to_owned())
            .collect();
        let filter = rest[close + 1..]
            .trim()
            .trim_start_matches("WHERE")
            .trim()
            .to_owned();
        indexes.insert(
            name,
            Index {
                table,
                columns,
                unique,
                filter,
            },
        );
    }
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

/// The columns of a `CREATE TABLE` body, constraints included, as one
/// table.
fn table_of(body: &str, dialect: Dialect) -> Table {
    let mut table = Table::default();
    for def in split_defs(body) {
        let def = def.trim();
        let words: Vec<&str> = def.split_whitespace().collect();
        let Some(first) = words.first() else {
            continue;
        };
        let name = first.trim_matches('"');
        if matches!(
            name,
            "FOREIGN" | "CHECK" | "UNIQUE" | "PRIMARY" | "CONSTRAINT"
        ) {
            apply_table_constraint(def, &mut table);
            continue;
        }
        let Some(kind) = words.get(1) else {
            continue;
        };
        if !is_type(kind) {
            continue; // a constraint line, such as `REFERENCES`
        }
        let constraints = words[2..].join(" ");
        let (column, check) = column_of(&constraints, kind, dialect);
        if let Some(check) = check {
            table.checks.insert(check);
        }
        if constraints.contains("PRIMARY KEY") {
            table.primary_key.push(name.to_owned());
        }
        if constraints.contains("UNIQUE") {
            table.uniques.insert(BTreeSet::from([name.to_owned()]));
        }
        table.columns.push((name.to_owned(), column));
    }
    table
}

/// Applies one table-level constraint to `table`.
fn apply_table_constraint(def: &str, table: &mut Table) {
    let names = |rest: &str| -> Vec<String> {
        let open = rest.find('(').unwrap();
        let close = matching_paren(rest, open);
        rest[open + 1..close]
            .split(',')
            .map(|n| n.trim().trim_matches('"').to_owned())
            .collect()
    };
    // A named constraint keeps only its shape: `CONSTRAINT name UNIQUE
    // (a, b)` is the `UNIQUE (a, b)` after it.
    let rest = def.strip_prefix("CONSTRAINT ").unwrap_or(def);
    let rest = match rest.split_once(' ') {
        Some((first, tail)) if !matches!(first, "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK") => tail,
        _ => rest,
    };
    if let Some(rest) = rest.strip_prefix("PRIMARY KEY") {
        table.primary_key = names(rest);
    } else if let Some(rest) = rest.strip_prefix("UNIQUE") {
        table.uniques.insert(names(rest).into_iter().collect());
    } else if let Some(rest) = rest.strip_prefix("CHECK") {
        let open = rest.find('(').unwrap();
        let close = matching_paren(rest, open);
        table.checks.insert(normalize_check(&rest[open + 1..close]));
    } else if let Some(rest) = rest.strip_prefix("FOREIGN KEY") {
        let from = names(rest);
        // `FOREIGN KEY (a, b) REFERENCES t (x, y)`: each column of the
        // key names the column of the target it pairs with.
        let Some((_from, columns)) = rest.split_once("REFERENCES") else {
            return;
        };
        // The target table is the words before its column list.
        let target = columns
            .split('(')
            .next()
            .unwrap_or("")
            .split_whitespace()
            .last()
            .unwrap_or("")
            .trim_matches('"')
            .to_owned();
        let target_open = columns.find('(').expect("a target names its columns");
        let target_close = matching_paren(columns, target_open);
        let to: Vec<String> = columns[target_open + 1..target_close]
            .split(',')
            .map(|n| n.trim().trim_matches('"').to_owned())
            .collect();
        for (from, to) in from.into_iter().zip(to) {
            if let Some((_, column)) = table.columns.iter_mut().find(|(n, _)| *n == from) {
                column.references = Some(format!("{target}({to})"));
            }
        }
    }
}

/// The check of a column definition, normalized, if it has one.
fn check_of(constraints: &str) -> Option<String> {
    let at = constraints.find("CHECK")?;
    let rest = &constraints[at..];
    let open = rest.find('(')? + at;
    let close = matching_paren(constraints, open);
    Some(normalize_check(&constraints[open + 1..close]))
}

/// The same check in both dialects: one text, without whitespace, with
/// the functions the dialects spell differently folded together, and
/// every letter lower.
fn normalize_check(expr: &str) -> String {
    expr.to_lowercase()
        .replace("octet_length(", "length(")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

fn column_of(constraints: &str, kind: &str, dialect: Dialect) -> (Column, Option<String>) {
    let kind = match dialect {
        Dialect::Sqlite => sqlite_kind(kind),
        Dialect::Postgres => postgres_kind(kind),
    };
    let words: Vec<&str> = constraints.split_whitespace().collect();
    let references = words.iter().position(|w| *w == "REFERENCES").map(|i| {
        let table = words[i + 1].trim_matches(',').trim_matches('"');
        // `REFERENCES t (c)`: the column the reference names, which
        // the parenthesis after the table holds.
        match words.get(i + 2) {
            Some(col) => format!(
                "{table}({})",
                col.trim_start_matches('(')
                    .trim_end_matches([')', ','])
                    .trim_matches('"')
            ),
            None => table.to_owned(),
        }
    });
    // A `GENERATED` column has no default: its value comes from the
    // sequence, which SQLite spells as a bare `INTEGER PRIMARY KEY`.
    let default = if constraints.contains("GENERATED") {
        None
    } else {
        words
            .iter()
            .position(|w| *w == "DEFAULT")
            // A zero blob is `x''` in SQLite and `''::bytea` in Postgres:
            // both spell the empty byte string.
            .map(|i| {
                let raw = words[i + 1];
                if raw.starts_with("x'") || raw.ends_with("::bytea") {
                    "empty bytes".to_owned()
                } else {
                    raw.trim_matches('\'').to_owned()
                }
            })
    };
    (
        Column {
            kind,
            not_null: constraints.contains("NOT NULL"),
            references,
            default,
        },
        check_of(constraints),
    )
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

/// The diffs of one table's shape between the two dialects: its
/// columns, its primary key, its UNIQUE groups, and its CHECKs.
fn table_diffs(name: &str, table: &Table, pg_table: &Table) -> Vec<String> {
    let mut diffs = Vec::new();
    for (column, shape) in &table.columns {
        let Some(pg_shape) = pg_table
            .columns
            .iter()
            .find(|(n, _)| n == column)
            .map(|(_, c)| c)
        else {
            diffs.push(format!(
                "column {name}.{column} is missing from the Postgres schema"
            ));
            continue;
        };
        if pg_shape != shape {
            diffs.push(format!(
                "column {name}.{column} is {shape:?} in SQLite, {pg_shape:?} in Postgres"
            ));
        }
    }
    for (column, _) in &pg_table.columns {
        if !table.columns.iter().any(|(n, _)| n == column) {
            diffs.push(format!(
                "column {name}.{column} is missing from the SQLite schema"
            ));
        }
    }
    if table.primary_key != pg_table.primary_key {
        diffs.push(format!(
            "table {name} has the primary key {:?} in SQLite, {:?} in Postgres",
            table.primary_key, pg_table.primary_key
        ));
    }
    if table.uniques != pg_table.uniques {
        diffs.push(format!(
            "table {name} has the UNIQUE groups {:?} in SQLite, {:?} in Postgres",
            table.uniques, pg_table.uniques
        ));
    }
    if table.checks != pg_table.checks {
        diffs.push(format!(
            "table {name} has the CHECKs {:?} in SQLite, {:?} in Postgres",
            table.checks, pg_table.checks
        ));
    }
    diffs
}

#[test]
fn the_postgres_schema_holds_the_tables_and_columns_of_the_sqlite_schema() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let (sqlite, sqlite_indexes) = read_schema(&dir, Dialect::Sqlite);
    let (postgres, postgres_indexes) = read_schema(&dir.join("postgres"), Dialect::Postgres);
    assert!(!sqlite.is_empty(), "the SQLite migrations hold tables");
    assert!(!postgres.is_empty(), "the Postgres migrations hold tables");

    let mut ok = true;
    let say = |message: String, ok: &mut bool| {
        println!("{message}");
        *ok = false;
    };
    for (name, table) in &sqlite {
        let Some(pg_table) = postgres.get(name) else {
            say(
                format!("table {name} is missing from the Postgres schema"),
                &mut ok,
            );
            continue;
        };
        for diff in table_diffs(name, table, pg_table) {
            say(diff, &mut ok);
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
    for (name, index) in &sqlite_indexes {
        match postgres_indexes.get(name) {
            Some(pg_index) if pg_index != index => say(
                format!("index {name} is {index:?} in SQLite, {pg_index:?} in Postgres"),
                &mut ok,
            ),
            None => {
                println!("index {name} is missing from the Postgres schema");
                ok = false;
            }
            Some(_) => {}
        }
    }
    for name in postgres_indexes.keys() {
        if !sqlite_indexes.contains_key(name) {
            println!("index {name} is missing from the SQLite schema");
            ok = false;
        }
    }
    assert!(ok, "the schemas drift");
}
