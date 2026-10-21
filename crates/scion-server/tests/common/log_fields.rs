//! Parse the server's plain log lines into level, message, and fields,
//! so assertions name fields instead of substrings: `contains("7")`
//! matches a timestamp, and `contains("peer=Some(127.0.0.1")` matches
//! whatever other line happens to carry that prefix.
//!
//! The lines the server writes with `tracing_subscriber::fmt` (and
//! `NO_COLOR=1`) look like
//!
//! ```text
//! 2026-10-21T06:58:07.191633Z  WARN call{rpc=/scion.v1.Scion/Log peer="127.0.0.1:54321"}: scion_proto::runtime::listen: closing a connection: its stalled replies hold too much held=4194305 limit=4194304 peer=Some(127.0.0.1:54321)
//! ```
//!
//! Timestamp, level (five columns), an optional span with its own
//! fields, target, message, then fields. A field value is bare
//! (`listen=127.0.0.1:8080`), quoted (`reason="missing or wrong
//! token"`), or a Debug value with parentheses
//! (`peer=Some(127.0.0.1:54321)`). Lines that do not fit — the
//! binary's stdout output, cargo chatter — parse to nothing and are
//! skipped by [`lines`].

/// One parsed log line. The borrow lives as long as the log text.
#[derive(Debug)]
pub(crate) struct LogLine<'a> {
    /// `INFO`, `WARN`, `ERROR`, `DEBUG`, or `TRACE`. Present for the
    /// suites that want it; the field asserts today do not read it.
    #[allow(dead_code)]
    pub(crate) level: &'a str,
    /// The event's message: everything between the target and the
    /// first field.
    pub(crate) message: &'a str,
    fields: Vec<(&'a str, &'a str)>,
}

impl<'a> LogLine<'a> {
    /// The value of the field named `key`, outer quotes stripped, or
    /// `None` when the line names no such field. An event field wins
    /// over a span field of the same name: the event's is the more
    /// specific.
    pub(crate) fn field(&self, key: &str) -> Option<&'a str> {
        let (_, value) = self.fields.iter().rev().find(|(k, _)| *k == key)?;
        Some(value.strip_prefix('"').unwrap_or(value))
            .map(|value| value.strip_suffix('"').unwrap_or(value))
    }
}

/// Splits on spaces that are not inside double quotes, keeping each
/// token's byte offset in `rest`.
fn split_outside_quotes(rest: &str) -> Vec<(usize, &str)> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    for (index, c) in rest.char_indices() {
        match c {
            '"' => quoted = !quoted,
            ' ' if !quoted => {
                if index > start {
                    parts.push((start, &rest[start..index]));
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    if start < rest.len() {
        parts.push((start, &rest[start..]));
    }
    parts
}

/// A field token names a key with an ASCII-identifier head: `key=`,
/// not a message word.
fn is_field_token(token: &str) -> bool {
    let Some(eq) = token.find('=') else {
        return false;
    };
    let key = &token[..eq];
    !key.is_empty()
        && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && key
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

/// Parses one log line, or `None` when the line is not a log line.
/// A line can carry a span before the target
/// (`WARN call{rpc=...}: target: message`); the span's fields fold
/// into the line's fields, and the event's fields win.
fn parse_line(line: &str) -> Option<LogLine<'_>> {
    // The timestamp, then the level. The default format right-aligns
    // the level in five columns, so short levels carry leading spaces.
    let rest = line.trim_start();
    let (_stamp, rest) = rest.split_once(' ')?;
    let rest = rest.trim_start();
    let (level, rest) = rest.split_once(' ')?;
    if !matches!(level, "INFO" | "WARN" | "ERROR" | "DEBUG" | "TRACE") {
        return None;
    }
    // A span, if any: a one-word name, then `{fields}`, then ": ".
    let mut fields = Vec::new();
    let rest = match rest.find("}: ") {
        Some(close) if rest[..close].contains('{') => {
            let span = &rest[..close];
            let (name, span_fields) = span.split_once('{')?;
            if name.contains(' ') {
                return None;
            }
            for (_, part) in split_outside_quotes(span_fields) {
                let (key, value) = part.split_once('=')?;
                fields.push((key, value));
            }
            &rest[close + 3..]
        }
        _ => rest,
    };
    // The target ends at the first ": ".
    let (target, rest) = rest.split_once(": ")?;
    if target.is_empty() || target.contains(' ') {
        return None;
    }
    // The message runs to the first field token; the fields follow.
    // The span's fields above do not start the run.
    let parts = split_outside_quotes(rest);
    let mut field_start = rest.len();
    let mut seen_field = false;
    for (offset, part) in &parts {
        if !seen_field {
            if !is_field_token(part) {
                continue;
            }
            seen_field = true;
            field_start = *offset;
        }
        let (key, value) = part.split_once('=')?;
        fields.push((key, value));
    }
    let message = rest[..field_start].trim_end();
    if message.is_empty() {
        return None;
    }
    Some(LogLine {
        level,
        message,
        fields,
    })
}

/// Parses every log line in `text`, skipping the lines that do not
/// parse.
pub(crate) fn lines(text: &str) -> Vec<LogLine<'_>> {
    text.lines().filter_map(parse_line).collect()
}

/// The first line whose message starts with `message`: the line the
/// assertion is about, with its fields.
pub(crate) fn find_line<'a>(text: &'a str, message: &str) -> Option<LogLine<'a>> {
    lines(text)
        .into_iter()
        .find(|line| line.message.starts_with(message))
}
