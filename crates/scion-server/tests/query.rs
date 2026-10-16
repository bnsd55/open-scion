//! Merge base, ancestry and diff, compared with git on histories built in
//! the test, and their bounds.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::deadline::Deadline;
use scion_server::import::{ImportOptions, import_git};
use scion_server::read::{
    self, DiffEntry, MAX_DIFF_ENTRIES, MAX_RENAME_CHUNKS, MAX_RENAME_ENTRIES, Rename,
};
use scion_server::{Error, Store, db, tree};
use tempfile::TempDir;

const REPO: &str = "r";

fn open_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

/// A deadline that a test does not reach.
fn later() -> Deadline {
    Deadline::new(Instant::now() + Duration::from_secs(600))
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config. Returns whether it succeeded, and stdout.
fn git_status(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new("git")
        // No auto gc or maintenance: they pack loose objects in the
        // background and delete the loose files, also while `import_git` of
        // the same repository still reads them.
        .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "Ada")
        .env("GIT_COMMITTER_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .output()
        .unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap().trim_end().to_owned();
    (out.status.success(), stdout)
}

/// Runs git and returns stdout. Fails the test unless git succeeds.
fn git(dir: &Path, args: &[&str]) -> String {
    let (ok, stdout) = git_status(dir, args);
    assert!(ok, "git {args:?} failed");
    stdout
}

/// Writes one file named after the commit, commits everything, and returns
/// the commit name.
fn commit(dir: &Path, name: &str) -> String {
    fs::write(dir.join(name), name).unwrap();
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", name]);
    git(dir, &["rev-parse", "HEAD"])
}

fn merge(dir: &Path, what: &[&str]) -> String {
    let mut args = vec!["merge", "-q", "--no-ff", "--allow-unrelated-histories"];
    args.extend(what);
    args.extend(["-m", "merge"]);
    git(dir, &args);
    git(dir, &["rev-parse", "HEAD"])
}

fn import(store: &Store, dir: &Path) {
    let opts = ImportOptions {
        rev: "HEAD".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    import_git(store, REPO, dir, &opts).unwrap();
}

/// The changeset that a git commit was imported as.
fn changeset_of(store: &Store, commit: &str) -> ChangesetId {
    let oid = gix::ObjectId::from_hex(commit.as_bytes()).unwrap();
    store
        .read(|c| {
            let cs: Vec<u8> = c.query_row(
                "SELECT cs FROM git_import WHERE git_oid = ?1",
                [oid.as_slice()],
                |row| row.get(0),
            )?;
            Ok(ChangesetId::try_from(cs.as_slice()).unwrap())
        })
        .unwrap()
}

fn generation(store: &Store, id: ChangesetId) -> i64 {
    store
        .read(|c| db::generation(c, db::repo_id(c, REPO)?, id))
        .unwrap()
}

/// A history with branches and merges: a criss-cross, an octopus merge of
/// three parents, and a merge of an unrelated history.
fn graph_fixture(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main"]);
    commit(dir, "root");
    let m1 = commit(dir, "m1");
    git(dir, &["checkout", "-q", "-b", "side"]);
    commit(dir, "s1");
    git(dir, &["checkout", "-q", "main"]);
    let m2 = commit(dir, "m2");
    // Criss-cross: each branch merges the other's tip, so the two merges
    // have two best common ancestors, m2 and s1.
    merge(dir, &["side"]);
    git(dir, &["checkout", "-q", "side"]);
    merge(dir, &[&m2]);
    commit(dir, "s2");
    git(dir, &["checkout", "-q", "main"]);
    commit(dir, "m3");
    git(dir, &["checkout", "-q", "-b", "topic", &m1]);
    commit(dir, "t1");
    git(dir, &["checkout", "-q", "-b", "topic2", "main~2"]);
    commit(dir, "u1");
    git(dir, &["checkout", "-q", "main"]);
    merge(dir, &["topic", "topic2"]);
    // An unrelated history, merged in.
    git(dir, &["checkout", "-q", "--orphan", "other"]);
    git(dir, &["rm", "-rfq", "."]);
    commit(dir, "r1");
    commit(dir, "r2");
    git(dir, &["checkout", "-q", "main"]);
    commit(dir, "m4");
    merge(dir, &["other"]);
    // Criss-cross again, on top of the octopus.
    git(dir, &["checkout", "-q", "side"]);
    commit(dir, "s3");
    let side = git(dir, &["rev-parse", "HEAD"]);
    git(dir, &["checkout", "-q", "main"]);
    let main = git(dir, &["rev-parse", "HEAD"]);
    merge(dir, &[&side]);
    git(dir, &["checkout", "-q", "side"]);
    merge(dir, &[&main]);
    git(dir, &["checkout", "-q", "main"]);
    merge(dir, &["side"]);
}

#[test]
fn merge_base_and_is_ancestor_match_git() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    graph_fixture(dir);
    let (_data, store) = open_store();
    import(&store, dir);

    let commits: Vec<String> = git(dir, &["rev-list", "--all"])
        .lines()
        .map(str::to_owned)
        .collect();
    let merges = git(dir, &["rev-list", "--all", "--min-parents=3"]);
    assert_eq!(
        merges.lines().count(),
        1,
        "the fixture has an octopus merge"
    );
    let mut seen_criss_cross = false;
    let mut seen_unrelated = false;
    for (i, x) in commits.iter().enumerate() {
        // The merge base does not depend on the order of the two, so git
        // runs once per pair.
        for y in &commits[i..] {
            // `git merge-base --all` fails when there is no merge base.
            let (_, out) = git_status(dir, &["merge-base", "--all", x, y]);
            let mut expected: Vec<ChangesetId> =
                out.lines().map(|c| changeset_of(&store, c)).collect();
            expected.sort();
            seen_criss_cross |= expected.len() > 1;
            seen_unrelated |= expected.is_empty();
            for (x, y) in [(x, y), (y, x)] {
                let (cx, cy) = (changeset_of(&store, x), changeset_of(&store, y));
                let bases = read::merge_base(&store, REPO, cx, cy, &later()).unwrap();
                let mut sorted = bases.clone();
                sorted.sort();
                assert_eq!(sorted, expected, "merge-base {x} {y}");
                // Highest generation first, then lowest ID.
                let mut ordered = bases.clone();
                ordered.sort_by_key(|&id| (std::cmp::Reverse(generation(&store, id)), id));
                assert_eq!(bases, ordered, "order of merge-base {x} {y}");
                // `git merge-base --is-ancestor x y` holds exactly when the
                // only merge base is x.
                assert_eq!(
                    read::is_ancestor(&store, REPO, cx, cy, &later()).unwrap(),
                    expected == [cx],
                    "is-ancestor {x} {y}"
                );
            }
        }
    }
    assert!(seen_criss_cross && seen_unrelated);
}

fn author() -> IdentityTime {
    IdentityTime {
        name: b"Ada".to_vec(),
        email: b"ada@example.com".to_vec(),
        unix_secs: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

/// Stores a first-parent chain in a new repo: one changeset per entry of
/// `changes`. Returns the IDs, oldest first.
fn chain(store: &Store, changes: Vec<BTreeMap<RepoPath, Option<FileState>>>) -> Vec<ChangesetId> {
    store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            let mut ids: Vec<ChangesetId> = Vec::new();
            for changes in changes {
                let cs = Changeset {
                    parents: ids.last().copied().into_iter().collect(),
                    author: author(),
                    committer: None,
                    message: format!("change {}", ids.len()).into_bytes(),
                    changes,
                };
                ids.push(tree::store_changeset(c, repo, &cs)?.0);
            }
            Ok(ids)
        })
        .unwrap()
}

#[test]
fn a_history_with_no_merges_has_no_walk_bound() {
    // Deeper than the bound of 10,000 steps that the queries had.
    const N: usize = 10_002;
    let (_data, store) = open_store();
    let ids = chain(&store, vec![BTreeMap::new(); N]);
    let tip = ids[N - 1];

    // The commit graph reads a few nodes, at any depth.
    assert!(read::is_ancestor(&store, REPO, ids[0], tip, &later()).unwrap());
    assert_eq!(
        read::merge_base(&store, REPO, ids[0], tip, &later()).unwrap(),
        [ids[0]]
    );
    let below = ids[N - 2];
    assert!(!read::is_ancestor(&store, REPO, tip, below, &later()).unwrap());
    let bases = read::merge_base(&store, REPO, tip, below, &later()).unwrap();
    assert_eq!(bases, [below]);
    let bases = read::merge_base(&store, REPO, tip, tip, &later()).unwrap();
    assert_eq!(bases, [tip]);
}

#[test]
fn graph_queries_across_many_merges_answer() {
    // A ladder: each changeset on main merges a side changeset that
    // branched from main's previous changeset. 20,001 changesets, each a
    // merge or the parent of one, so the walks take every one. With a bound
    // of 10,000 steps, these queries failed with RESOURCE_EXHAUSTED.
    let merges = 10_000;
    let (_data, store) = open_store();
    let (main, sides, late, early) = store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            let store = |parents: Vec<ChangesetId>, message: String| {
                let cs = Changeset {
                    parents,
                    author: author(),
                    committer: None,
                    message: message.into_bytes(),
                    changes: BTreeMap::new(),
                };
                Ok::<_, Error>(tree::store_changeset(c, repo, &cs)?.0)
            };
            let mut main = vec![store(vec![], "root".into())?];
            let mut sides = Vec::new();
            for i in 0..merges {
                let last = *main.last().unwrap();
                let side = store(vec![last], format!("side {i}"))?;
                main.push(store(vec![last, side], format!("merge {i}"))?);
                sides.push(side);
            }
            // Not merged: one from the oldest side changeset, one from root.
            let late = store(vec![sides[0]], "late".into())?;
            let early = store(vec![main[0]], "early".into())?;
            Ok((main, sides, late, early))
        })
        .unwrap();
    let tip = *main.last().unwrap();

    assert!(read::is_ancestor(&store, REPO, sides[0], tip, &later()).unwrap());
    assert!(!read::is_ancestor(&store, REPO, late, tip, &later()).unwrap());
    for (a, b, base) in [
        (sides[0], tip, sides[0]),
        (late, tip, sides[0]),
        (tip, early, main[0]),
    ] {
        let bases = read::merge_base(&store, REPO, a, b, &later()).unwrap();
        assert_eq!(bases, [base]);
    }
}

/// Git's `--name-status` letter for an entry.
fn status(entry: &DiffEntry) -> char {
    let kind = |state: &FileState| std::mem::discriminant(state);
    match (&entry.old, &entry.new) {
        (None, _) => 'A',
        (_, None) => 'D',
        (Some(old), Some(new)) if kind(old) != kind(new) => 'T',
        _ => 'M',
    }
}

/// A path's parts, which compare in tree order.
fn tree_order(p: &RepoPath) -> Vec<Vec<u8>> {
    p.as_bytes()
        .split(|&b| b == b'/')
        .map(<[u8]>::to_vec)
        .collect()
}

fn diff_all(
    store: &Store,
    old: ChangesetId,
    new: ChangesetId,
    prefix: Option<&RepoPath>,
    after: Option<&RepoPath>,
) -> Vec<DiffEntry> {
    let page = read::diff(store, REPO, old, new, prefix, after).unwrap();
    assert_eq!(page.next_after, None, "a small diff fits one page");
    page.entries
}

/// A regular file whose blob ID comes from `n`. The blob is not stored.
fn unstored(n: usize) -> FileState {
    FileState::Regular {
        content: BlobId::of(&n.to_le_bytes()),
        executable: false,
    }
}

/// Writes `content` at `name` below `dir`, with its directories.
fn write_file(dir: &Path, name: &str, content: &str) {
    let file = dir.join(name);
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(file, content).unwrap();
}

/// Two snapshots that differ in each way a path can: added, deleted,
/// modified, executable bit, regular file to symlink, file to directory and
/// back, with an unchanged subtree, and names whose tree order is not their
/// byte order (`a/b` before `a.txt`).
fn diff_fixture(dir: &Path) -> (String, String) {
    let write = |name: &str, content: &str| write_file(dir, name, content);
    git(dir, &["init", "-q", "-b", "main"]);
    for name in [
        "a.txt",
        "a/b",
        "a-c",
        "keep/deep/x",
        "mod.txt",
        "del.txt",
        "exe.sh",
        "link",
        "swap",
        "dir2/f",
        "sub/one",
        "sub/two",
    ] {
        write(name, name);
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "old"]);
    let old = git(dir, &["rev-parse", "HEAD"]);

    write("a/b", "changed");
    write("a.txt", "changed");
    write("mod.txt", "changed");
    fs::remove_file(dir.join("del.txt")).unwrap();
    fs::set_permissions(dir.join("exe.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    fs::remove_file(dir.join("link")).unwrap();
    symlink("mod.txt", dir.join("link")).unwrap();
    fs::remove_file(dir.join("swap")).unwrap();
    write("swap/inner", "inner");
    fs::remove_dir_all(dir.join("dir2")).unwrap();
    write("dir2", "now a file");
    write("sub/two", "changed");
    write("sub/three", "new");
    write("newdir/n1", "new");
    write("newdir/deeper/n2", "new");
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "new"]);
    let new = git(dir, &["rev-parse", "HEAD"]);
    (old, new)
}

#[test]
fn diff_matches_git_name_status_and_resumes_after_any_path() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    let (old_commit, new_commit) = diff_fixture(dir);
    let (_data, store) = open_store();
    import(&store, dir);
    let old = changeset_of(&store, &old_commit);
    let new = changeset_of(&store, &new_commit);

    let prefixes = [
        None,
        Some("a"),
        Some("sub"),
        Some("swap"),
        Some("dir2"),
        Some("mod.txt"),
        Some("newdir/deeper"),
        Some("keep"),
        Some("missing"),
        Some("mod.txt/below"),
    ];
    let pairs = [
        (old, new, &old_commit, &new_commit),
        (new, old, &new_commit, &old_commit),
    ];
    for (from, to, from_commit, to_commit) in pairs {
        for prefix in prefixes {
            let prefix = prefix.map(path);
            let mut args = vec![
                "diff",
                "--name-status",
                "--no-renames",
                from_commit,
                to_commit,
            ];
            let spec = prefix
                .as_ref()
                .map(|p| String::from_utf8_lossy(p.as_bytes()).into_owned());
            if let Some(spec) = &spec {
                args.extend(["--", spec]);
            }
            let mut expected: Vec<String> = git(dir, &args).lines().map(str::to_owned).collect();
            expected.sort();

            let entries = diff_all(&store, from, to, prefix.as_ref(), None);
            let mut got: Vec<String> = entries
                .iter()
                .map(|e| {
                    format!(
                        "{}\t{}",
                        status(e),
                        String::from_utf8_lossy(e.path.as_bytes())
                    )
                })
                .collect();
            got.sort();
            assert_eq!(got, expected, "diff {spec:?}");
            assert!(
                entries
                    .windows(2)
                    .all(|w| tree_order(&w[0].path) < tree_order(&w[1].path)),
                "tree order: {entries:?}"
            );

            // Resuming after any path, in the diff or not, returns exactly
            // the entries after it.
            let probes = entries
                .iter()
                .map(|e| e.path.clone())
                .chain(["a", "a.txt0", "b", "swap/inner/x", "zzz"].map(path));
            for after in probes {
                let rest: Vec<DiffEntry> = entries
                    .iter()
                    .filter(|e| tree_order(&e.path) > tree_order(&after))
                    .cloned()
                    .collect();
                assert_eq!(
                    diff_all(&store, from, to, prefix.as_ref(), Some(&after)),
                    rest,
                    "diff {spec:?} after {after:?}"
                );
            }
        }
    }
    assert_eq!(diff_all(&store, old, old, None, None), []);
}

#[test]
fn diff_skips_a_subtree_with_the_same_id_on_both_sides() {
    let (_data, store) = open_store();
    let state = |data: &[u8]| {
        Some(FileState::Regular {
            content: BlobId::of(data),
            executable: false,
        })
    };
    let ids = chain(
        &store,
        vec![
            BTreeMap::from([(path("keep/x"), state(b"x")), (path("f"), state(b"1"))]),
            BTreeMap::from([(path("f"), state(b"2"))]),
        ],
    );
    // Delete the shared subtree: a walk that reads it fails.
    let root = read::root_tree(&store, REPO, ids[0]).unwrap();
    store
        .write(|c| {
            let Some(scion_core::Entry::Dir(keep)) = tree::lookup(c, root, &path("keep"))? else {
                panic!("keep is a directory");
            };
            c.execute("DELETE FROM trees WHERE id = ?1", [keep.as_bytes()])?;
            Ok(())
        })
        .unwrap();
    let entries = diff_all(&store, ids[0], ids[1], None, None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, path("f"));
}

#[test]
fn diff_pages_stop_at_max_diff_entries_and_resume() {
    let (_data, store) = open_store();
    // 1,515 paths below `big` and 1,000 beside it, so pages end inside
    // directories. Blobs are never read, so they are not stored.
    let mut changes = BTreeMap::new();
    for i in 0..15 {
        for j in 0..100 {
            changes.insert(path(&format!("big/d{i:02}/f{j:03}")), Some(unstored(j)));
        }
        changes.insert(path(&format!("big/d{i:02}.txt")), Some(unstored(i)));
    }
    for j in 0..MAX_DIFF_ENTRIES {
        changes.insert(path(&format!("other/f{j:04}")), Some(unstored(j)));
    }
    let base = BTreeMap::from([(path("base"), Some(unstored(0)))]);
    let ids = chain(&store, vec![base, changes.clone()]);

    for prefix in [None, Some(path("big"))] {
        let mut expected: Vec<DiffEntry> = changes
            .iter()
            .filter(|(p, _)| {
                prefix
                    .as_ref()
                    .is_none_or(|prefix| p.as_bytes().starts_with(prefix.as_bytes()))
            })
            .map(|(p, s)| DiffEntry {
                path: p.clone(),
                old: None,
                new: *s,
                rename: None,
            })
            .collect();
        expected.sort_by_key(|e| tree_order(&e.path));

        let mut got = Vec::new();
        let mut after = None;
        let mut pages = 0;
        loop {
            let page = read::diff(
                &store,
                REPO,
                ids[0],
                ids[1],
                prefix.as_ref(),
                after.as_ref(),
            )
            .unwrap();
            pages += 1;
            assert!(got.len() <= expected.len(), "the pages repeat entries");
            got.extend(page.entries);
            match page.next_after {
                Some(next) => {
                    assert_eq!(got.len(), pages * MAX_DIFF_ENTRIES, "a full page");
                    assert_eq!(Some(&next), got.last().map(|e| &e.path));
                    after = Some(next);
                }
                None => break,
            }
        }
        assert_eq!(got, expected, "{prefix:?}");
        assert_eq!(pages, expected.len().div_ceil(MAX_DIFF_ENTRIES));
    }

    // A diff of exactly one page says that it is complete.
    let exact: BTreeMap<_, _> = changes.into_iter().take(MAX_DIFF_ENTRIES).collect();
    let ids = chain(&store, vec![BTreeMap::new(), exact]);
    let page = read::diff(&store, REPO, ids[0], ids[1], None, None).unwrap();
    assert_eq!(page.entries.len(), MAX_DIFF_ENTRIES);
    assert_eq!(page.next_after, None);
}

/// Lines `<tag><n>` for each `n` of `range`, each 5 bytes with its newline
/// when `tag` has two bytes.
fn text(tag: &str, range: std::ops::Range<usize>) -> String {
    range.fold(String::new(), |text, n| text + &format!("{tag}{n:02}\n"))
}

/// Ten lines of 128 bytes with their newline. `edited` changes bytes 62 and
/// 63 of each.
fn long_lines(edited: bool) -> String {
    (0..10)
        .map(|n| {
            let mut line = format!("ch{n:02}{}\n", "-".repeat(123));
            if edited {
                line.replace_range(62..64, "##");
            }
            line
        })
        .collect()
}

/// `entries` as git's `--name-status` lines, sorted. A rename is one line,
/// `R<similarity>`, the old path and the new path.
fn name_status(entries: &[DiffEntry]) -> Vec<String> {
    let name = |p: &RepoPath| String::from_utf8_lossy(p.as_bytes()).into_owned();
    let mut lines: Vec<String> = entries
        .iter()
        .filter_map(|e| match &e.rename {
            Some(rename) if e.old.is_none() => Some(format!(
                "R{:03}\t{}\t{}",
                rename.similarity,
                name(&rename.path),
                name(&e.path)
            )),
            Some(_) => None,
            None => Some(format!("{}\t{}", status(e), name(&e.path))),
        })
        .collect();
    lines.sort();
    lines
}

/// Checks that the two halves of each rename in `entries`, a whole diff,
/// name each other.
fn assert_renames_pair_up(entries: &[DiffEntry]) {
    for entry in entries {
        let Some(rename) = &entry.rename else {
            continue;
        };
        let other = entries
            .iter()
            .find(|e| e.path == rename.path)
            .unwrap_or_else(|| panic!("no entry at {:?}", rename.path));
        let back = other.rename.as_ref().unwrap();
        assert_eq!(
            (&back.path, back.similarity),
            (&entry.path, rename.similarity)
        );
        assert!(entry.old.is_none() != other.old.is_none(), "{entry:?}");
        assert!(entry.old.is_none() || entry.new.is_none(), "{entry:?}");
    }
}

/// Files to write: each path and its content.
type Files = Vec<(String, String)>;

/// The cases of [`rename_fixture`] that take many files: the deleted files
/// and the added files.
fn generated_cases() -> (Files, Files) {
    let (mut old, mut new) = (Vec::new(), Vec::new());
    // `top4/x.txt` is 90%, 85%, 80%, 75% and 60% like s0 to s4. Each of
    // s0 to s3 goes to its own y at 95%, so the four that x keeps are taken,
    // and x is no rename, though s4 is left.
    for (i, k) in [18, 17, 16, 15, 12].into_iter().enumerate() {
        let tag = format!("s{i}");
        old.push((
            format!("top4/s{i}.txt"),
            text("ka", 0..k) + &text(&tag, k..20),
        ));
        if i < 4 {
            let y = text("ka", 0..k) + &text(&tag, k..19) + &text(&format!("t{i}"), 19..20);
            new.push((format!("top4/y{i}.txt"), y));
        }
    }
    new.push(("top4/x.txt".to_owned(), text("ka", 0..20)));
    // 101 deleted files with the same content. The added file has the name
    // of the last; only the first 100 are looked at, so the first wins.
    for n in 0..=100 {
        old.push((format!("ident/f{n:03}.txt"), text("ia", 0..2)));
    }
    new.push(("ident2/f100.txt".to_owned(), text("ia", 0..2)));
    // Lines of 128 bytes are two chunks of 64. The new file changes bytes
    // 62 and 63 of each line, so it keeps the second chunk: exactly 50%.
    old.push(("chunk/old.txt".to_owned(), long_lines(false)));
    new.push(("chunk/new.txt".to_owned(), long_lines(true)));
    (old, new)
}

/// Two snapshots with each kind of rename that `git diff` finds by default,
/// and files that are not renames. Each case uses its own lines, so the
/// cases do not match each other.
fn rename_fixture(dir: &Path) -> (String, String) {
    let write = |name: &str, content: &str| write_file(dir, name, content);
    let remove = |name: &str| fs::remove_file(dir.join(name)).unwrap();
    let crlf: String = text("ca", 0..10).replace('\n', "\r\n");
    let mut old: Files = [
        // A pure rename, and one with an edit: 90%.
        ("pure.txt", text("pa", 0..5)),
        ("edit.txt", text("ea", 0..10)),
        ("run.sh", "#!/bin/sh\necho run\n".to_owned()),
        // One added file and six candidates from 55% to 85%.
        ("many/c1.txt", text("ma", 0..11) + &text("mx", 11..20)),
        ("many/c2.txt", text("ma", 0..12) + &text("mx", 12..20)),
        ("many/c3.txt", text("ma", 0..14) + &text("mx", 14..20)),
        ("many/c4.txt", text("ma", 0..16) + &text("mx", 16..20)),
        ("many/c5.txt", text("ma", 0..17) + &text("mx", 17..20)),
        ("many/c6.txt", text("ma", 0..13) + &text("mx", 13..20)),
        // Three at 80%. Two have the added file's name, which no other
        // deleted file has alone. The first by path does not.
        ("tie/a/other.txt", text("ta", 0..8) + &text("ty", 8..10)),
        ("tie/a/same.txt", text("ta", 0..8) + &text("tx", 8..10)),
        ("tie/c/same.txt", text("ta", 0..8) + &text("tz", 8..10)),
        // Two at 70%, no name in common: the first path wins.
        ("tie2/x.txt", text("ua", 0..7) + &text("ux", 7..10)),
        ("tie2/y.txt", text("ua", 0..7) + &text("uy", 7..10)),
        // Two added files want src1: one~src1 85%, two~src1 90%,
        // one~src2 65%, two~src2 80%. The most similar pair goes first,
        // though `one` comes first by path.
        ("g/src1.txt", text("ga", 0..20)),
        ("g/src2.txt", text("ga", 0..16) + &text("gb", 16..20)),
        // The only files named name.txt pair at 80%, before better.txt at
        // 95%.
        ("n/a/name.txt", text("na", 0..16) + &text("nx", 16..20)),
        ("n/a/better.txt", text("na", 0..19) + &text("ny", 19..20)),
        // The same name at 60% is under 75%, so high.txt at 70% wins.
        ("lo/a/low.txt", text("la", 0..6) + &text("lx", 6..10)),
        ("lo/a/high.txt", text("la", 0..7) + &text("ly", 7..10)),
        // A regular file and a symlink with the same bytes: not a rename.
        ("sym-src.txt", "abc".to_owned()),
        ("empty-a", String::new()),
        // A file whose path becomes a directory.
        ("swap", text("sa", 0..3)),
        // CRs do not count: 50 shared bytes of 60, 83%.
        ("crlf.txt", crlf),
        // 45%: not a rename.
        ("under.txt", text("ha", 0..20)),
        // One deleted file and two added files with its content.
        ("dup.txt", text("da", 0..4)),
        // Two deleted files with the same content: the same name wins.
        ("ex/a/file.txt", text("xa", 0..4)),
        ("ex/b/thing.txt", text("xa", 0..4)),
        // Different lines in the same hash buckets: 100% similar to git.
        ("hash/a.txt", "aqca\naqcb\naqcc\n".to_owned()),
        ("kind", "k\n".to_owned()),
        ("mod.txt", text("mo", 0..3)),
    ]
    .map(|(name, content)| (name.to_owned(), content))
    .into();
    let (more_old, more_new) = generated_cases();
    old.extend(more_old);
    git(dir, &["init", "-q", "-b", "main"]);
    for (name, content) in &old {
        write(name, content);
    }
    symlink("pure.txt", dir.join("link")).unwrap();
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "old"]);
    let old_commit = git(dir, &["rev-parse", "HEAD"]);

    for (name, _) in &old {
        if !["kind", "mod.txt"].contains(&name.as_str()) {
            remove(name);
        }
    }
    remove("link");
    let mut new: Files = [
        ("moved/pure.txt", text("pa", 0..5)),
        ("edited.txt", text("ea", 0..9) + &text("eb", 9..10)),
        ("bin/run.sh", "#!/bin/sh\necho run\n".to_owned()),
        ("many/target.txt", text("ma", 0..20)),
        ("tie/b/same.txt", text("ta", 0..10)),
        ("tie2/new.txt", text("ua", 0..10)),
        (
            "g/one.txt",
            text("ga", 0..13) + &text("gd", 13..16) + &text("ga", 16..20),
        ),
        ("g/two.txt", text("ga", 0..18) + &text("gc", 18..20)),
        ("n/b/name.txt", text("na", 0..20)),
        ("lo/b/low.txt", text("la", 0..10)),
        ("empty-b", String::new()),
        ("swap/inner", text("sa", 0..3)),
        ("lf.txt", text("ca", 0..10)),
        ("under-new.txt", text("ha", 0..9) + &text("hx", 9..20)),
        ("dup1.txt", text("da", 0..4)),
        ("dup2.txt", text("da", 0..4)),
        ("ex/c/thing.txt", text("xa", 0..4)),
        ("hash/b.txt", "baah\nbaai\nbaaj\n".to_owned()),
        ("mod.txt", text("mo", 0..4)),
    ]
    .map(|(name, content)| (name.to_owned(), content))
    .into();
    new.extend(more_new);
    for (name, content) in &new {
        write(name, content);
    }
    fs::set_permissions(dir.join("bin/run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    fs::create_dir_all(dir.join("links")).unwrap();
    symlink("pure.txt", dir.join("links/link")).unwrap();
    symlink("abc", dir.join("sym-new")).unwrap();
    remove("kind");
    symlink("pure.txt", dir.join("kind")).unwrap();
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "new"]);
    (old_commit, git(dir, &["rev-parse", "HEAD"]))
}

#[test]
fn diff_finds_renames_as_git_diff_does() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    let (old_commit, new_commit) = rename_fixture(dir);
    let (_data, store) = open_store();
    import(&store, dir);
    let old = changeset_of(&store, &old_commit);
    let new = changeset_of(&store, &new_commit);

    let prefixes = [
        None,
        Some("many"),
        Some("tie"),
        Some("g"),
        Some("n/b"),
        Some("lo"),
        Some("top4"),
        Some("hash"),
    ];
    let pairs = [
        (old, new, &old_commit, &new_commit),
        (new, old, &new_commit, &old_commit),
    ];
    for (from, to, from_commit, to_commit) in pairs {
        for prefix in prefixes {
            let prefix = prefix.map(path);
            let mut args = vec!["diff", "-M", "--name-status", from_commit, to_commit];
            let spec = prefix
                .as_ref()
                .map(|p| String::from_utf8_lossy(p.as_bytes()).into_owned());
            if let Some(spec) = &spec {
                args.extend(["--", spec]);
            }
            let mut expected: Vec<String> = git(dir, &args).lines().map(str::to_owned).collect();
            expected.sort();
            let entries = diff_all(&store, from, to, prefix.as_ref(), None);
            assert_eq!(name_status(&entries), expected, "diff {spec:?}");
            assert_renames_pair_up(&entries);

            // A page that starts after a path walks the whole diff to find
            // renames, and gives each entry the same rename. Every 7th path
            // keeps the test short.
            let probes = entries.iter().step_by(7).map(|e| e.path.clone());
            for after in probes.chain([path("a")]) {
                let rest: Vec<DiffEntry> = entries
                    .iter()
                    .filter(|e| tree_order(&e.path) > tree_order(&after))
                    .cloned()
                    .collect();
                assert_eq!(
                    diff_all(&store, from, to, prefix.as_ref(), Some(&after)),
                    rest,
                    "diff {spec:?} after {after:?}"
                );
            }
        }
    }
}

/// Stores `data` as a blob and returns a regular file state for it.
fn stored(store: &Store, data: &[u8]) -> FileState {
    FileState::Regular {
        content: store.blobs().put(data).unwrap(),
        executable: false,
    }
}

/// Every page of a diff, as a client reads it.
fn diff_pages(
    store: &Store,
    old: ChangesetId,
    new: ChangesetId,
    prefix: Option<&RepoPath>,
) -> Vec<DiffEntry> {
    let mut entries = Vec::new();
    let mut after = None;
    loop {
        let page = read::diff(store, REPO, old, new, prefix, after.as_ref()).unwrap();
        entries.extend(page.entries);
        match page.next_after {
            Some(next) => after = Some(next),
            None => return entries,
        }
    }
}

#[test]
fn renames_are_the_same_on_every_page() {
    let (_data, store) = open_store();
    // 1,500 files move from `old/` to `new/`, so the diff has 3,000 entries
    // over three pages. Blobs are never read: each rename has the same
    // content.
    let files = 1500;
    let base: BTreeMap<_, _> = (0..files)
        .map(|n| (path(&format!("old/d{}/f{n:04}", n % 3)), Some(unstored(n))))
        .collect();
    let mut moves: BTreeMap<_, _> = base.keys().map(|p| (p.clone(), None)).collect();
    moves.extend((0..files).map(|n| (path(&format!("new/f{n:04}")), Some(unstored(n)))));
    let ids = chain(&store, vec![base, moves]);

    let entries = diff_pages(&store, ids[0], ids[1], None);
    assert!(entries.len() > 2 * MAX_DIFF_ENTRIES);
    assert_renames_pair_up(&entries);
    for entry in &entries {
        let rename = entry.rename.as_ref().unwrap();
        let (to, from) = match entry.new {
            Some(_) => (&entry.path, &rename.path),
            None => (&rename.path, &entry.path),
        };
        let name = |p: &RepoPath| p.as_bytes().rsplit(|&b| b == b'/').next().unwrap().to_vec();
        assert_eq!(name(to), name(from));
        assert_eq!(rename.similarity, 100);
    }
    // A page that starts anywhere, also inside another page, agrees.
    for at in [1, 999, 1000, 1001, 1777, 2999] {
        let after = &entries[at - 1].path;
        let page = read::diff(&store, REPO, ids[0], ids[1], None, Some(after)).unwrap();
        assert_eq!(page.entries[..], entries[at..at + page.entries.len()]);
    }
    // Below `new`, the deleted files are outside the diff: no renames, as
    // `git diff -- new`.
    let below = diff_pages(&store, ids[0], ids[1], Some(&path("new")));
    assert_eq!(below.len(), files);
    assert!(below.iter().all(|e| e.rename.is_none()));
}

#[test]
fn a_diff_past_max_rename_entries_has_no_renames() {
    let (_data, store) = open_store();
    // A rename and fillers: MAX_RENAME_ENTRIES entries in all, then one
    // more. Blobs are never read.
    for (fillers, found) in [
        (MAX_RENAME_ENTRIES - 2, true),
        (MAX_RENAME_ENTRIES - 1, false),
    ] {
        let base = BTreeMap::from([(path("a/source"), Some(unstored(usize::MAX)))]);
        let mut change = BTreeMap::from([
            (path("a/source"), None),
            (path("b/source"), Some(unstored(usize::MAX))),
        ]);
        change.extend((0..fillers).map(|n| (path(&format!("c/f{n:05}")), Some(unstored(n)))));
        let ids = chain(&store, vec![base, change]);
        let entries = diff_pages(&store, ids[0], ids[1], None);
        assert_eq!(entries.len(), fillers + 2);
        let renamed = entries.iter().filter(|e| e.rename.is_some()).count();
        assert_eq!(renamed, if found { 2 } else { 0 }, "{fillers} fillers");
    }
}

#[test]
fn the_search_for_similar_files_stops_at_its_budget() {
    let (_data, store) = open_store();
    // `zz/new` is `zz/old` with one line changed. Each `a/<k>` has the same
    // size, so it is read and compared first. It costs n chunks, twice what
    // `zz/new` or `zz/old` costs. `zz/new` is read first. With three
    // decoys the search has cost 3.5n before it reads `zz/old`, under the
    // budget of 4n. With four it has cost 4.5n, and reads no more. A file
    // of a quarter of the size, `b/<k>`, is never read, so 16 of them cost
    // nothing.
    let n = MAX_RENAME_CHUNKS / 4;
    let unlike = Some(stored(&store, "\n".repeat(n).as_bytes()));
    let small = Some(stored(&store, "\n".repeat(n / 4).as_bytes()));
    let source = "y\n".repeat(n / 2);
    let new = format!("z\n{}", &source[2..]);
    for (decoys, small_decoys, found) in [(0, 0, true), (3, 0, true), (4, 0, false), (0, 16, true)]
    {
        let mut base: BTreeMap<_, _> = (0..decoys)
            .map(|k| (path(&format!("a/{k}")), unlike))
            .chain((0..small_decoys).map(|k| (path(&format!("b/{k}")), small)))
            .collect();
        base.insert(path("zz/old"), Some(stored(&store, source.as_bytes())));
        let mut change: BTreeMap<_, _> = base.keys().map(|p| (p.clone(), None)).collect();
        change.insert(path("zz/new"), Some(stored(&store, new.as_bytes())));
        let ids = chain(&store, vec![base, change]);
        let entries = diff_all(&store, ids[0], ids[1], None, None);
        let renamed = entries.iter().find(|e| e.path == path("zz/new")).unwrap();
        let expected = found.then(|| Rename {
            path: path("zz/old"),
            similarity: 99,
        });
        assert_eq!(
            renamed.rename, expected,
            "{decoys} and {small_decoys} decoys"
        );
    }
}

#[test]
fn similar_files_are_not_searched_past_git_rename_limit() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    // `old.txt` becomes `new.txt` with an edit. Symlinks with targets of
    // their own are deleted and added: never similar, but they count
    // toward git's limit of 1,000 x 1,000 pairs. 1,000 of each fit, 1,001
    // do not.
    let mut commits = Vec::new();
    for links in [999, 1000] {
        let make = |dir: &Path, side: &str| {
            fs::create_dir_all(dir.join(side)).unwrap();
            for n in 0..links {
                let link = dir.join(format!("{side}/{side}{links}-{n}"));
                symlink(format!("{side}{links}-{n}"), link).unwrap();
            }
        };
        fs::remove_file(dir.join("new.txt")).ok();
        fs::remove_dir_all(dir.join("e")).ok();
        make(dir, "d");
        fs::write(dir.join("old.txt"), text("oa", 0..10)).unwrap();
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "old"]);
        let old = git(dir, &["rev-parse", "HEAD"]);

        fs::remove_dir_all(dir.join("d")).unwrap();
        fs::remove_file(dir.join("old.txt")).unwrap();
        make(dir, "e");
        fs::write(dir.join("new.txt"), text("oa", 0..9) + &text("ob", 9..10)).unwrap();
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "new"]);
        commits.push((old, git(dir, &["rev-parse", "HEAD"])));
    }
    let (_data, store) = open_store();
    import(&store, dir);
    for ((old, new), found) in commits.iter().zip([true, false]) {
        let mut expected: Vec<String> = git(dir, &["diff", "-M", "--name-status", old, new])
            .lines()
            .map(str::to_owned)
            .collect();
        expected.sort();
        let entries = diff_pages(
            &store,
            changeset_of(&store, old),
            changeset_of(&store, new),
            None,
        );
        assert_eq!(name_status(&entries), expected);
        assert_eq!(entries.iter().any(|e| e.rename.is_some()), found);
    }
}

#[test]
fn queries_check_the_repo_and_the_changesets() {
    let (_data, store) = open_store();
    let ids = chain(&store, vec![BTreeMap::new()]);
    let missing = ChangesetId::from_bytes([7; 32]);
    let errors = [
        read::merge_base(&store, REPO, ids[0], missing, &later()).err(),
        read::merge_base(&store, REPO, missing, ids[0], &later()).err(),
        read::merge_base(&store, "nope", ids[0], ids[0], &later()).err(),
        read::is_ancestor(&store, REPO, missing, ids[0], &later()).err(),
        read::is_ancestor(&store, REPO, ids[0], missing, &later()).err(),
        read::diff(&store, REPO, ids[0], missing, None, None).err(),
        read::diff(&store, REPO, missing, ids[0], None, None).err(),
    ];
    for error in errors {
        assert!(matches!(error, Some(Error::NotFound(_))), "{error:?}");
    }
}
