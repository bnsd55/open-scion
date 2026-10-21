# The docs' named tests are checked

Every `path.rs::fn_name` the docs and README name is now machine-
checked (docs_links.rs): the file is in the repo, it holds the fn,
and the fn is a test - `#[test]`, `#[tokio::test]` or one a
`proptest!` generates - not a helper. A failure names the doc, its
line and the missing name. Eight references in docs/threat-model.md
named helpers, not tests; they now name the covering test or keep the
helper in prose, so no claim reads as proven when its test is gone.
