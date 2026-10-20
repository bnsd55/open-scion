//! The protocol v1 surface, as the machine form of stability.md's 1.x
//! promise: the calls of `scion.v1.Scion` and `scion.v1.Objects`, with
//! the field numbers of their messages, are additive-only in 1.x. The
//! golden names what 1.x freezes; the check reads the checked-in proto,
//! so no doc edit can make a removed or renumbered field pass. Adding a
//! call or a field passes: the golden is a floor, not a ceiling —
//! removing one is serving a v2, not changing v1
//! (docs/compatibility.md).

use std::collections::BTreeSet;

const PROTO: &str = include_str!("../proto/scion.proto");
const GOLDEN: &str = include_str!("protocol_surface_v1.golden");

#[test]
fn the_v1_surface_holds_everything_the_golden_names() {
    let actual: BTreeSet<String> = surface(PROTO).into_iter().collect();
    let golden: Vec<&str> = GOLDEN
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    assert!(
        !golden.is_empty(),
        "the golden must name the surface it freezes"
    );
    let missing: Vec<&str> = golden
        .iter()
        .copied()
        .filter(|line| !actual.contains(*line))
        .collect();
    assert!(
        missing.is_empty(),
        "the 1.x protocol surface shrank or renumbered; an independent v1 client \
         would break. Restore these, or serve a v2 beside v1 \
         (docs/compatibility.md): {missing:?}"
    );
}

/// Every call (`Service.rpc`) and every field
/// (`Message.field:number:type`) the proto names, as one line each.
fn surface(proto: &str) -> Vec<String> {
    let mut out = Vec::new();
    // The block the line sits in: the current service and message. The
    // proto has no nesting deeper than these two.
    let mut service = String::new();
    let mut message = String::new();
    for line in proto.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("service ") {
            rest.trim_end_matches('{').trim().clone_into(&mut service);
        } else if let Some(rest) = line.strip_prefix("message ") {
            rest.trim_end_matches('{').trim().clone_into(&mut message);
        } else if let Some(rest) = line.strip_prefix("rpc ") {
            let name = rest.split('(').next().unwrap_or("").trim().to_owned();
            out.push(format!("{service}.{name}"));
        } else if let Some(field) = field_of(line, &message) {
            out.push(field);
        }
    }
    out.sort();
    out.dedup();
    out
}

/// One field line of a message: `type name = number;` (possibly
/// `repeated type name`), or `map<k, v> name = number;`.
fn field_of(line: &str, message: &str) -> Option<String> {
    if message.is_empty() || line.starts_with("//") || line.starts_with("message ") {
        return None;
    }
    let line = line.trim().trim_end_matches(';').trim();
    let (left, number) = line.split_once('=')?;
    let number = number.trim();
    let mut parts = left.split_whitespace();
    let mut kind = parts.next()?.to_owned();
    if kind == "repeated" {
        kind = format!("repeated {}", parts.next()?);
    }
    let name = parts.next()?;
    Some(format!("{message}.{name}:{number}:{kind}"))
}
