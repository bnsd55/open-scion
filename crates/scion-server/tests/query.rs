//! Merge base, ancestry and diff, compared with git on histories built in
//! the test, and their bounds.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::Command;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::import::{ImportOptions, import_git};
use scion_server::read::{self, DiffEntry, MAX_DIFF_ENTRIES, MAX_WALK};
use scion_server::{Error, Store, db, tree};
use tempfile::TempDir;

const REPO: &str = "r";

fn open_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config. Returns whether it succeeded, and stdout.
fn git_status(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new("git")
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
    for x in &commits {
        for y in &commits {
            let (cx, cy) = (changeset_of(&store, x), changeset_of(&store, y));
            // `git merge-base --all` fails when there is no merge base.
            let (_, out) = git_status(dir, &["merge-base", "--all", x, y]);
            let mut expected: Vec<ChangesetId> =
                out.lines().map(|c| changeset_of(&store, c)).collect();
            expected.sort();
            let bases = read::merge_base(&store, REPO, cx, cy).unwrap();
            let mut sorted = bases.clone();
            sorted.sort();
            assert_eq!(sorted, expected, "merge-base {x} {y}");
            // Highest generation first, then lowest ID.
            let mut ordered = bases.clone();
            ordered.sort_by_key(|&id| (std::cmp::Reverse(generation(&store, id)), id));
            assert_eq!(bases, ordered, "order of merge-base {x} {y}");
            seen_criss_cross |= bases.len() > 1;
            seen_unrelated |= bases.is_empty();

            let (yes, _) = git_status(dir, &["merge-base", "--is-ancestor", x, y]);
            assert_eq!(
                read::is_ancestor(&store, REPO, cx, cy).unwrap(),
                yes,
                "is-ancestor {x} {y}"
            );
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
fn graph_walks_stop_at_max_walk() {
    let (_data, store) = open_store();
    let ids = chain(&store, vec![BTreeMap::new(); MAX_WALK + 2]);
    let tip = ids[MAX_WALK + 1];

    // From the tip down to ids[1] is MAX_WALK changesets to read.
    assert!(read::is_ancestor(&store, REPO, ids[1], tip).unwrap());
    let deeper = read::is_ancestor(&store, REPO, ids[0], tip);
    assert!(
        matches!(deeper, Err(Error::ResourceExhausted(_))),
        "{deeper:?}"
    );
    // The merge base walk also takes the base itself from the queue.
    assert_eq!(
        read::merge_base(&store, REPO, ids[2], tip).unwrap(),
        [ids[2]]
    );
    let deeper = read::merge_base(&store, REPO, ids[1], tip);
    assert!(
        matches!(deeper, Err(Error::ResourceExhausted(_))),
        "{deeper:?}"
    );

    // Generations prune both walks: a descendant is never below its
    // ancestor's generation, and the walk stops at the first base.
    let below = ids[MAX_WALK];
    assert!(!read::is_ancestor(&store, REPO, tip, below).unwrap());
    assert_eq!(read::merge_base(&store, REPO, tip, below).unwrap(), [below]);
    assert_eq!(read::merge_base(&store, REPO, tip, tip).unwrap(), [tip]);
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

/// Two snapshots that differ in each way a path can: added, deleted,
/// modified, executable bit, regular file to symlink, file to directory and
/// back, with an unchanged subtree, and names whose tree order is not their
/// byte order (`a/b` before `a.txt`).
fn diff_fixture(dir: &Path) -> (String, String) {
    let write = |name: &str, content: &str| {
        let file = dir.join(name);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, content).unwrap();
    };
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
fn diff_pages_stop_at_max_diff_entries_and_resume() {
    let (_data, store) = open_store();
    // 1,515 paths below `big` and 1,000 beside it, so pages end inside
    // directories. Blobs are never read, so they are not stored.
    let state = |n: usize| {
        Some(FileState::Regular {
            content: BlobId::of(&n.to_le_bytes()),
            executable: false,
        })
    };
    let mut changes = BTreeMap::new();
    for i in 0..15 {
        for j in 0..100 {
            changes.insert(path(&format!("big/d{i:02}/f{j:03}")), state(j));
        }
        changes.insert(path(&format!("big/d{i:02}.txt")), state(i));
    }
    for j in 0..MAX_DIFF_ENTRIES {
        changes.insert(path(&format!("other/f{j:04}")), state(j));
    }
    let base = BTreeMap::from([(path("base"), state(0))]);
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

#[test]
fn queries_check_the_repo_and_the_changesets() {
    let (_data, store) = open_store();
    let ids = chain(&store, vec![BTreeMap::new()]);
    let missing = ChangesetId::from_bytes([7; 32]);
    let errors = [
        read::merge_base(&store, REPO, ids[0], missing).err(),
        read::merge_base(&store, REPO, missing, ids[0]).err(),
        read::merge_base(&store, "nope", ids[0], ids[0]).err(),
        read::is_ancestor(&store, REPO, missing, ids[0]).err(),
        read::is_ancestor(&store, REPO, ids[0], missing).err(),
        read::diff(&store, REPO, ids[0], missing, None, None).err(),
        read::diff(&store, REPO, missing, ids[0], None, None).err(),
    ];
    for error in errors {
        assert!(matches!(error, Some(Error::NotFound(_))), "{error:?}");
    }
}
