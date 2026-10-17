//! Every relative link in the markdown of the repo points at a file that
//! exists, and every `#anchor` names a heading of the file it points at.
//! The checks follow GitHub's slug rules, so a link that renders as a link
//! on GitHub also resolves here. No network: `http(s)` links are skipped.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

/// The markdown files whose links the test checks.
fn documents() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = ["README.md", "CHANGELOG.md"]
        .iter()
        .map(|name| Path::new(ROOT).join(name))
        .collect();
    let docs = Path::new(ROOT).join("docs");
    let mut listed: Vec<PathBuf> = fs::read_dir(docs)
        .expect("docs/ exists")
        .map(|entry| entry.expect("a docs entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    listed.sort();
    files.append(&mut listed);
    files
}

/// The heading slugs of one markdown file, by GitHub's rules: lowercase,
/// punctuation dropped, spaces as hyphens, duplicates suffixed `-1`, `-2`.
fn slugs(markdown: &str) -> Vec<String> {
    let mut headings: Vec<String> = Vec::new();
    let mut in_fence = false;
    for line in markdown.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || !trimmed.starts_with('#') {
            continue;
        }
        let hashes = trimmed.chars().take_while(|c| *c == '#').count();
        let rest = &trimmed[hashes..];
        if !rest.starts_with(' ') {
            continue;
        }
        headings.push(slug(rest.trim()));
    }
    // GitHub dedupes repeated slugs; keep every variant so any of the
    // rendered forms resolves.
    let mut seen: HashMap<String, usize> = HashMap::new();
    headings
        .into_iter()
        .map(|slug| {
            let count = seen.entry(slug.clone()).or_insert(0);
            let suffix = if *count == 0 {
                String::new()
            } else {
                format!("-{count}")
            };
            *count += 1;
            format!("{slug}{suffix}")
        })
        .collect()
}

/// GitHub's slug of one heading's text.
fn slug(text: &str) -> String {
    let mut out = String::new();
    let mut last_space = false;
    for c in text.chars() {
        if c.is_whitespace() {
            if !last_space {
                out.push('-');
            }
            last_space = true;
            continue;
        }
        last_space = false;
        if c == '`' || c == '*' || c == '_' {
            // Formatting is dropped, but a bare `_` or `*` word mark is
            // punctuation on GitHub too.
            continue;
        }
        if c.is_alphanumeric() || c == '-' || c == '_' {
            out.extend(c.to_lowercase());
        }
    }
    out
}

/// One inline link's target, or a reference definition's target.
fn targets(markdown: &str) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let bytes = markdown.as_bytes();
    let mut in_fence = false;
    for (i, line) in markdown.lines().enumerate() {
        let _ = i;
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        // Inline links: ](target)
        let mut rest = line;
        while let Some(open) = rest.find("](") {
            rest = &rest[open + 2..];
            let mut depth = 1;
            let end = rest
                .char_indices()
                .find(|(_, c)| match c {
                    '(' => {
                        depth += 1;
                        false
                    }
                    ')' => {
                        depth -= 1;
                        depth == 0
                    }
                    _ => false,
                })
                .map(|(n, _)| n);
            let Some(end) = end else { break };
            found.push(rest[..end].to_owned());
            rest = &rest[end + 1..];
        }
        // Reference definitions: [text]: target
        if let Some(colon) = line.find("]:") {
            if line.starts_with('[') {
                let target = line[colon + 2..].trim();
                if !target.is_empty() {
                    found.push(target.to_owned());
                }
            }
        }
        let _ = bytes;
    }
    found
}

#[test]
fn every_markdown_link_resolves() {
    let root = Path::new(ROOT);
    let mut broken: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for document in documents() {
        let markdown = fs::read_to_string(&document)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", document.display()));
        let dir = document.parent().expect("a document has a parent");
        for target in targets(&markdown) {
            // Strip an optional URL title: [text](file "title").
            let target = target.split_whitespace().next().unwrap_or("");
            if target.is_empty()
                || target.starts_with("http://")
                || target.starts_with("https://")
                || target.starts_with("mailto:")
            {
                continue;
            }
            checked += 1;
            let (path, anchor) = match target.split_once('#') {
                Some((file, anchor)) => (file, Some(anchor)),
                None => (target, None),
            };
            // A target with no file part names a heading of this file.
            let resolved = if path.is_empty() {
                document.clone()
            } else {
                dir.join(target)
            };
            if !(resolved.is_file() || resolved.is_dir()) {
                broken.push(format!(
                    "{} -> {} (no file)",
                    document.strip_prefix(root).unwrap().display(),
                    target
                ));
                continue;
            }
            let Some(anchor) = anchor else { continue };
            let text = fs::read_to_string(&resolved)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", resolved.display()));
            let heading = slugs(&text);
            if !heading.iter().any(|slug| slug == anchor) {
                broken.push(format!(
                    "{} -> {}#{anchor} (headings: {})",
                    document.strip_prefix(root).unwrap().display(),
                    target,
                    heading.join(", ")
                ));
            }
        }
    }
    assert!(checked > 0, "the documents hold links");
    assert!(
        broken.is_empty(),
        "{} of {checked} links are broken:\n{}",
        broken.len(),
        broken.join("\n")
    );
}
