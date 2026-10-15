//! Path history (`Log` with a path) and blame, with their bounds.
#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::Command;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::blame::{
    self, Blame, BlameRange, MAX_BLAME_BYTES, MAX_BLAME_DIFF_LINES, MAX_BLAME_VERSIONS,
};
use scion_server::import::{ImportOptions, import_git};
use scion_server::read::{self, MAX_SCAN};
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
/// config, and returns stdout. Fails the test unless git succeeds.
fn git(dir: &Path, args: &[&str]) -> String {
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
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim_end().to_owned()
}

/// Writes the files in order (`None` deletes a file or a directory),
/// commits everything, and returns the commit name.
fn commit(dir: &Path, message: &str, files: &[(&str, Option<&str>)]) -> String {
    for (name, content) in files {
        let file = dir.join(name);
        match content {
            Some(content) => {
                fs::create_dir_all(file.parent().unwrap()).unwrap();
                fs::write(file, content).unwrap();
            }
            None if file.is_dir() => fs::remove_dir_all(file).unwrap(),
            None => fs::remove_file(file).unwrap(),
        }
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", message]);
    git(dir, &["rev-parse", "HEAD"])
}

fn import(store: &Store, dir: &Path) -> ChangesetId {
    let opts = ImportOptions {
        rev: "HEAD".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    import_git(store, REPO, dir, &opts).unwrap().tip
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

fn path_log(store: &Store, from: ChangesetId, p: &str) -> (Vec<ChangesetId>, Option<ChangesetId>) {
    let page = read::log_page(store, REPO, from, Some(&path(p)), 1000).unwrap();
    (
        page.entries.into_iter().map(|(id, _)| id).collect(),
        page.next,
    )
}

#[test]
fn path_log_follows_files_directories_deletes_and_first_parents() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    let add = commit(
        dir,
        "add",
        &[
            ("a.txt", Some("1\n")),
            ("dir/x", Some("x\n")),
            ("dir/sub/y", Some("y\n")),
            ("other", Some("o\n")),
            ("swap", Some("s\n")),
        ],
    );
    let edit_a = commit(dir, "edit a", &[("a.txt", Some("2\n"))]);
    // `swap` becomes a directory here, and a file again in `readd_a`.
    let edit_y = commit(
        dir,
        "edit y",
        &[
            ("dir/sub/y", Some("y2\n")),
            ("swap", None),
            ("swap/inner", Some("i\n")),
        ],
    );
    let delete_a = commit(dir, "delete a", &[("a.txt", None)]);
    let readd_a = commit(
        dir,
        "re-add a",
        &[
            ("a.txt", Some("3\n")),
            ("swap", None),
            ("swap", Some("s2\n")),
        ],
    );
    git(dir, &["checkout", "-q", "-b", "side"]);
    commit(
        dir,
        "side",
        &[("dir/x", Some("x2\n")), ("side.txt", Some("s\n"))],
    );
    git(dir, &["checkout", "-q", "main"]);
    commit(dir, "edit other", &[("other", Some("o2\n"))]);
    git(dir, &["merge", "-q", "--no-ff", "side", "-m", "merge side"]);
    let merge = git(dir, &["rev-parse", "HEAD"]);
    // Names that start with a path but are not below it.
    let siblings = commit(
        dir,
        "siblings",
        &[("dir/subway", Some("s\n")), ("a.txt.bak", Some("b\n"))],
    );

    let (_data, store) = open_store();
    let tip = import(&store, dir);
    let ids = |commits: &[&str]| -> Vec<ChangesetId> {
        commits.iter().map(|c| changeset_of(&store, c)).collect()
    };
    // First parent only: the side commit never appears, the merge does when
    // its diff against its first parent changes the path.
    let cases: [(&str, &[&str]); 7] = [
        ("a.txt", &[&readd_a, &delete_a, &edit_a, &add]),
        ("swap", &[&readd_a, &edit_y, &add]),
        ("swap/inner", &[&readd_a, &edit_y]),
        ("dir", &[&siblings, &merge, &edit_y, &add]),
        ("dir/sub", &[&edit_y, &add]),
        ("dir/x", &[&merge, &add]),
        ("side.txt", &[&merge]),
    ];
    for (p, expected) in cases {
        assert_eq!(path_log(&store, tip, p), (ids(expected), None), "{p}");
    }
    // A limit stops the walk after the last entry it returns.
    let page = read::log_page(&store, REPO, tip, Some(&path("a.txt")), 2).unwrap();
    let delete = changeset_of(&store, &delete_a);
    let parent = read::get_changeset(&store, REPO, delete).unwrap().parents[0];
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.next, Some(parent));
}

fn author() -> IdentityTime {
    IdentityTime {
        name: b"Ada".to_vec(),
        email: b"ada@example.com".to_vec(),
        unix_secs: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

fn regular(content: BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

/// Stores a first-parent chain in a new repo: one changeset per entry of
/// `puts`. Returns the IDs, oldest first.
fn chain(store: &Store, puts: Vec<Vec<(&str, FileState)>>) -> Vec<ChangesetId> {
    store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            let mut ids: Vec<ChangesetId> = Vec::new();
            for puts in puts {
                let cs = Changeset {
                    parents: ids.last().copied().into_iter().collect(),
                    author: author(),
                    committer: None,
                    message: format!("change {}", ids.len()).into_bytes(),
                    changes: puts.into_iter().map(|(p, s)| (path(p), Some(s))).collect(),
                };
                ids.push(tree::store_changeset(c, repo, &cs)?.0);
            }
            Ok(ids)
        })
        .unwrap()
}

/// `count` lines from line `start`, from line `origin` of `id`.
fn range(start: usize, count: usize, id: Option<ChangesetId>, origin: u32) -> BlameRange {
    BlameRange {
        start_line: u32::try_from(start).unwrap(),
        line_count: u32::try_from(count).unwrap(),
        changeset: id,
        origin_line: origin,
    }
}

fn blame(store: &Store, cs: ChangesetId, p: &str) -> Blame {
    blame::blame(store, REPO, cs, &path(p)).unwrap()
}

#[test]
fn walks_stop_after_max_scan_changesets_and_resume() {
    let (_data, store) = open_store();
    let f = store.blobs().put(b"one\ntwo\n").unwrap();
    // `f` changes only in the root, then MAX_SCAN changesets change `g`.
    // Their content is never read, so it is not stored.
    let mut changes = vec![vec![("f", regular(f)), ("g", regular(BlobId::of(b"")))]];
    changes.extend((0..MAX_SCAN).map(|i| vec![("g", regular(BlobId::of(&i.to_le_bytes())))]));
    let ids = chain(&store, changes);
    let (root, tip) = (ids[0], *ids.last().unwrap());

    assert_eq!(path_log(&store, tip, "f"), (vec![], Some(root)));
    assert_eq!(path_log(&store, root, "f"), (vec![root], None));

    let first = blame(&store, tip, "f");
    assert_eq!(first.ranges, [range(1, 2, None, 1)]);
    assert_eq!(first.next, Some(root));
    let rest = blame(&store, root, "f");
    assert_eq!(rest.ranges, [range(1, 2, Some(root), 1)]);
    assert_eq!(rest.next, None);
    assert_eq!(rest.authors, [(root, author())]);
}

#[test]
fn blame_stops_after_max_versions_and_resumes() {
    let (_data, store) = open_store();
    // Each version adds one line at the top, so in the newest file line k
    // came from changeset `ids[len - k]`, where it was line 1.
    let mut text = String::new();
    let changes = (1..=MAX_BLAME_VERSIONS + 1)
        .map(|n| {
            text.insert_str(0, &format!("line {n}\n"));
            vec![("f", regular(store.blobs().put(text.as_bytes()).unwrap()))]
        })
        .collect();
    let ids = chain(&store, changes);

    let first = blame(&store, *ids.last().unwrap(), "f");
    // The first call diffs the newest MAX_BLAME_VERSIONS versions. The last
    // line is older: line 1 of the file at the first changeset.
    let len = ids.len();
    let mut expected: Vec<BlameRange> = (1..len)
        .map(|k| range(k, 1, Some(ids[len - k]), 1))
        .collect();
    expected.push(range(len, 1, None, 1));
    assert_eq!(first.ranges, expected);
    assert_eq!(first.next, Some(ids[0]));
    assert_eq!(first.authors.len(), MAX_BLAME_VERSIONS);

    let rest = blame(&store, ids[0], "f");
    assert_eq!(rest.ranges, [range(1, 1, Some(ids[0]), 1)]);
    assert_eq!(rest.next, None);
}

#[test]
fn blame_gives_every_line_to_the_change_after_a_version_it_cannot_diff() {
    let (_data, store) = open_store();
    let binary = store.blobs().put(b"a\n\0\n").unwrap();
    let text = store.blobs().put(b"a\nb\n").unwrap();
    let edit = store.blobs().put(b"a\nb\nc\n").unwrap();
    let ids = chain(
        &store,
        vec![
            vec![("f", regular(binary))],
            vec![("f", regular(text))],
            vec![("f", regular(edit))],
        ],
    );
    // A line diff would keep "a" from the first version; a binary version
    // is not diffed, so "a" belongs to the change that made the file text.
    let result = blame(&store, ids[2], "f");
    assert_eq!(
        result.ranges,
        [range(1, 2, Some(ids[1]), 1), range(3, 1, Some(ids[2]), 3)]
    );
    assert_eq!(result.next, None, "every line has a changeset");
}

#[test]
fn a_change_that_only_deletes_lines_is_not_named() {
    let (_data, store) = open_store();
    let put = |data: &[u8]| regular(store.blobs().put(data).unwrap());
    let ids = chain(
        &store,
        vec![vec![("f", put(b"a\nb\nc\n"))], vec![("f", put(b"a\nc\n"))]],
    );
    let result = blame(&store, ids[1], "f");
    assert_eq!(
        result.ranges,
        [range(1, 1, Some(ids[0]), 1), range(2, 1, Some(ids[0]), 3),]
    );
    assert_eq!(result.authors, [(ids[0], author())]);
}

#[test]
fn blame_refuses_what_it_cannot_diff() {
    let (_data, store) = open_store();
    let at_limit = vec![b'x'; MAX_BLAME_BYTES];
    let over_limit = vec![b'x'; MAX_BLAME_BYTES + 1];
    let put = |data: &[u8]| regular(store.blobs().put(data).unwrap());
    let ids = chain(
        &store,
        vec![vec![
            ("at-limit", put(&at_limit)),
            ("over-limit", put(&over_limit)),
            ("binary", put(b"text\0more\n")),
            ("dir/file", put(b"x\n")),
            (
                "link",
                FileState::Symlink {
                    target: BlobId::of(b"at-limit"),
                },
            ),
        ]],
    );
    let cs = ids[0];
    assert_eq!(
        blame(&store, cs, "at-limit").ranges,
        [range(1, 1, Some(cs), 1)]
    );
    for p in ["over-limit", "binary", "dir", "link"] {
        let result = blame::blame(&store, REPO, cs, &path(p));
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{p}: {result:?}"
        );
    }
    let missing = blame::blame(&store, REPO, cs, &path("missing"));
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
}

#[test]
fn blame_and_path_log_do_not_follow_renames() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    let add = commit(dir, "add", &[("old.txt", Some("1\n2\n3\n"))]);
    let rename = commit(
        dir,
        "rename",
        &[("old.txt", None), ("new.txt", Some("1\n2\n3\n"))],
    );
    let (_data, store) = open_store();
    let tip = import(&store, dir);
    let rename_id = changeset_of(&store, &rename);
    // SHORTCUT (#69): git follows the rename and gives the lines to `add`.
    // Here the rename takes them.
    let porcelain = git(dir, &["blame", "--first-parent", "--porcelain", "new.txt"]);
    assert!(porcelain.starts_with(&add), "{porcelain}");
    assert_eq!(
        blame(&store, tip, "new.txt").ranges,
        [range(1, 3, Some(rename_id), 1)]
    );
    assert_eq!(path_log(&store, tip, "new.txt"), (vec![rename_id], None));
}

#[test]
fn blame_of_an_empty_file_has_no_ranges() {
    let (_data, store) = open_store();
    let empty = store.blobs().put(b"").unwrap();
    let ids = chain(&store, vec![vec![("f", regular(empty))]]);
    let result = blame(&store, ids[0], "f");
    assert_eq!(result.ranges, Vec::<BlameRange>::new());
    assert_eq!(result.next, None);
}

#[test]
fn blame_stops_when_its_diff_budget_runs_out() {
    let (_data, store) = open_store();
    // Each version adds one line at the top of about 300,000 lines. One diff
    // costs about 600,000 lines, so a second one would pass
    // MAX_BLAME_DIFF_LINES. Lines count, not their content: repeated lines
    // like these are the slow case for a diff.
    let n = 300_000;
    assert!(4 * n > MAX_BLAME_DIFF_LINES && 2 * n < MAX_BLAME_DIFF_LINES);
    let mut text = "\n".repeat(n);
    let mut changes = vec![vec![(
        "f",
        regular(store.blobs().put(text.as_bytes()).unwrap()),
    )]];
    for k in 2..=3 {
        text.insert_str(0, &format!("{k}\n"));
        changes.push(vec![(
            "f",
            regular(store.blobs().put(text.as_bytes()).unwrap()),
        )]);
    }
    let ids = chain(&store, changes);

    let first = blame(&store, ids[2], "f");
    // One diff: the newest line. The rest is still in the file at ids[1],
    // which the call did not diff.
    assert_eq!(
        first.ranges,
        [range(1, 1, Some(ids[2]), 1), range(2, n + 1, None, 1)]
    );
    assert_eq!(first.next, Some(ids[1]));
    let rest = blame(&store, ids[1], "f");
    assert_eq!(
        rest.ranges,
        [range(1, 1, Some(ids[1]), 1), range(2, n, Some(ids[0]), 1)]
    );
    assert_eq!(rest.next, None);

    // The first diff of a call runs even when it alone is over the budget,
    // so every call makes progress.
    let big = 2 * n;
    assert!(2 * big > MAX_BLAME_DIFF_LINES);
    let old = "\n".repeat(big);
    let new = format!("x\n{old}");
    let ids = chain(
        &store,
        vec![
            vec![("big", regular(store.blobs().put(old.as_bytes()).unwrap()))],
            vec![("big", regular(store.blobs().put(new.as_bytes()).unwrap()))],
        ],
    );
    let result = blame(&store, ids[1], "big");
    assert_eq!(
        result.ranges,
        [range(1, 1, Some(ids[1]), 1), range(2, big, Some(ids[0]), 1)]
    );
    assert_eq!(result.next, None);
}
