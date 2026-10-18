//! Every relative link in the markdown of the repo points at a file that
//! exists, and every `#anchor` names a heading of the file it points at.
//! The checks follow GitHub's slug rules, so a link that renders as a link
//! on GitHub also resolves here. No network: `http(s)` links are skipped.
//! The test also pins every RPC of `proto/scion.proto` to a section of
//! `docs/protocol-v1.md`, so the protocol doc cannot fall behind the proto.

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
    let mut stack = vec![PathBuf::from(ROOT).join("docs")];
    let mut listed: Vec<PathBuf> = Vec::new();
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("docs/ exists") {
            let path = entry.expect("a docs entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "md") {
                listed.push(path);
            }
        }
    }
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
    let mut in_fence = false;
    for line in markdown.lines() {
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
        if let Some(colon) = line.find("]:")
            && line.starts_with('[')
        {
            let target = line[colon + 2..].trim();
            if !target.is_empty() {
                found.push(target.to_owned());
            }
        }
    }
    found
}

/// The links of `documents` (absolute paths) that do not resolve, against
/// the repo at `root`. `checked` counts the links the checker looked at.
fn broken_links(documents: &[PathBuf], root: &Path) -> (Vec<String>, usize) {
    let mut broken: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for document in documents {
        let markdown = fs::read_to_string(document)
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
    (broken, checked)
}

#[test]
fn every_markdown_link_resolves() {
    let root = Path::new(ROOT);
    let (broken, checked) = broken_links(&documents(), root);
    assert!(checked > 0, "the documents hold links");
    assert!(
        broken.is_empty(),
        "{} of {checked} links are broken:\n{}",
        broken.len(),
        broken.join("\n")
    );
}

/// Every RPC that `proto/scion.proto` declares has a section in
/// `docs/protocol-v1.md`: a `### ` heading that names it. Modeled on the
/// SHORTCUT table test of `query.rs`: a new, renamed or removed RPC must
/// update the protocol doc in the same PR, or this fails.
#[test]
fn the_protocol_doc_has_a_section_for_every_rpc() {
    let proto = fs::read_to_string(Path::new(ROOT).join("proto/scion.proto"))
        .expect("proto/scion.proto exists");
    let doc = fs::read_to_string(Path::new(ROOT).join("docs/protocol-v1.md"))
        .expect("docs/protocol-v1.md exists");
    let headings: Vec<String> = doc
        .lines()
        .filter(|line| line.starts_with("### "))
        .map(|line| line["### ".len()..].trim().to_owned())
        .collect();
    assert!(!headings.is_empty(), "the protocol doc has sections");
    let mut missing = Vec::new();
    for line in proto
        .lines()
        .filter(|line| line.trim_start().starts_with("rpc "))
    {
        let rest = line.trim().trim_start_matches("rpc ").trim_start();
        let name = rest
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()
            .expect("an rpc line holds a name");
        let named = headings.iter().any(|heading| {
            heading
                .split(|c: char| !c.is_alphanumeric())
                .any(|word| word == name)
        });
        if !named {
            missing.push(name.to_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "these RPCs have no section in docs/protocol-v1.md: {}",
        missing.join(", ")
    );
}

#[test]
fn a_broken_link_is_reported() {
    // One tree: `a.md` links to a file that is not there, and to a heading
    // that `b.md` does not have. The checker must name both.
    let root = tempfile::tempdir().expect("a temp tree");
    fs::write(
        root.path().join("a.md"),
        "[file](missing.md)\n[anchor](b.md#no)\n",
    )
    .unwrap();
    fs::write(root.path().join("b.md"), "# Real\n").unwrap();
    let documents = vec![root.path().join("a.md"), root.path().join("b.md")];
    let (broken, checked) = broken_links(&documents, root.path());
    assert_eq!(checked, 2);
    assert_eq!(broken.len(), 2, "{}", broken.join("\n"));
    assert!(broken[0].contains("a.md -> missing.md (no file)"));
    assert!(broken[1].contains("b.md#no"));
}

#[cfg(test)]
mod checker {
    use super::{slug, targets};

    #[test]
    fn a_slug_follows_the_github_rules() {
        assert_eq!(slug("6. What to watch"), "6-what-to-watch");
        // Punctuation and formatting drop; digits, letters, `-` and `_` stay.
        // Each space maps to one hyphen, and a kept hyphen stays.
        assert_eq!(slug("The `Meta` trait (v1)"), "the-meta-trait-v1");
        assert_eq!(slug("server --data <dir>"), "server---data-dir");
        assert_eq!(slug("todo"), "todo");
    }

    #[test]
    fn a_duplicate_heading_takes_the_next_suffix() {
        // The test's own list holds duplicates of one heading, so its second
        // occurrence is what GitHub renders as `x-1`.
        let first = slug("Blobs");
        assert_eq!(first, "blobs");
        assert_eq!(format!("{first}-1"), "blobs-1");
    }

    #[test]
    fn targets_finds_inline_and_reference_links() {
        let markdown = "[a](b.md) ![img](c.png \"t\")\n\n[d]: e.md#f\n";
        assert_eq!(targets(markdown), vec!["b.md", "c.png \"t\"", "e.md#f"]);
    }
}
