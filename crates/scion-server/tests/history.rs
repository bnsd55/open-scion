//! Path history (`Log` with a path) and blame, with their bounds.
#![cfg(unix)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::blame::{
    self, Blame, BlameRange, MAX_BLAME_BYTES, MAX_BLAME_DIFF_LINES, MAX_BLAME_LINES,
    MAX_BLAME_VERSIONS, MAX_RENAME_BYTES, MAX_RENAME_SOURCES,
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
/// `puts`, which puts files or deletes them (`None`). Returns the IDs,
/// oldest first.
fn chain<S: Into<Option<FileState>>>(store: &Store, puts: Vec<Vec<(&str, S)>>) -> Vec<ChangesetId> {
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
                    changes: puts.into_iter().map(|(p, s)| (path(p), s.into())).collect(),
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
    assert_eq!(first.next, Some((root, path("f"))));
    let rest = blame(&store, root, "f");
    assert_eq!(rest.ranges, [range(1, 2, Some(root), 1)]);
    assert_eq!(rest.next, None);
    assert_eq!(rest.changesets, [(root, author(), path("f"))]);
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
    assert_eq!(first.next, Some((ids[0], path("f"))));
    assert_eq!(first.changesets.len(), MAX_BLAME_VERSIONS);

    let rest = blame(&store, ids[0], "f");
    assert_eq!(rest.ranges, [range(1, 1, Some(ids[0]), 1)]);
    assert_eq!(rest.next, None);
}

#[test]
fn blame_gives_every_line_to_the_change_after_a_version_it_cannot_diff() {
    let (_data, store) = open_store();
    let too_long = format!("a\nb\n{}", "\n".repeat(MAX_BLAME_LINES));
    let over_limit = store.blobs().put(too_long.as_bytes()).unwrap();
    let text = store.blobs().put(b"a\nb\n").unwrap();
    let edit = store.blobs().put(b"a\nb\nc\n").unwrap();
    let ids = chain(
        &store,
        vec![
            vec![("f", regular(over_limit))],
            vec![("f", regular(text))],
            vec![("f", regular(edit))],
        ],
    );
    // A line diff would keep "a" and "b" from the first version. It has
    // more lines than blame reads, so they belong to the change after it.
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
    assert_eq!(result.changesets, [(ids[0], author(), path("f"))]);
}

#[test]
fn blame_refuses_what_it_cannot_diff() {
    let (_data, store) = open_store();
    let put = |data: &[u8]| regular(store.blobs().put(data).unwrap());
    let lines = |n: usize| "\n".repeat(n).into_bytes();
    let ids = chain(
        &store,
        vec![vec![
            ("bytes-at-limit", put(&vec![b'x'; MAX_BLAME_BYTES])),
            ("bytes-over-limit", put(&vec![b'x'; MAX_BLAME_BYTES + 1])),
            ("lines-at-limit", put(&lines(MAX_BLAME_LINES))),
            ("lines-over-limit", put(&lines(MAX_BLAME_LINES + 1))),
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
        blame(&store, cs, "bytes-at-limit").ranges,
        [range(1, 1, Some(cs), 1)]
    );
    assert_eq!(
        blame(&store, cs, "lines-at-limit").ranges,
        [range(1, MAX_BLAME_LINES, Some(cs), 1)]
    );
    for p in ["bytes-over-limit", "lines-over-limit", "dir", "link"] {
        let result = blame::blame(&store, REPO, cs, &path(p));
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{p}: {result:?}"
        );
    }
    let missing = blame::blame(&store, REPO, cs, &path("missing"));
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
}

/// Blames `p` at `cs` to the end, and continues where a call stops, as a
/// client does. Each line as its changeset, its line there and the path
/// there.
fn blame_lines(store: &Store, cs: ChangesetId, p: &str) -> Vec<(ChangesetId, u32, RepoPath)> {
    let mut at = (cs, path(p));
    let mut paths = HashMap::new();
    // Each line's changeset once known, and until then its line at `at`.
    let mut lines: Vec<(Option<ChangesetId>, u32)> = Vec::new();
    loop {
        let result = blame::blame(store, REPO, at.0, &at.1).unwrap();
        paths.extend(result.changesets.into_iter().map(|(id, _, p)| (id, p)));
        let here: Vec<_> = result
            .ranges
            .iter()
            .flat_map(|r| (0..r.line_count).map(move |n| (r.changeset, r.origin_line + n)))
            .collect();
        if lines.is_empty() {
            lines = here;
        } else {
            for line in lines.iter_mut().filter(|(id, _)| id.is_none()) {
                *line = here[line.1 as usize - 1];
            }
        }
        match result.next {
            Some(next) => at = next,
            None => break,
        }
    }
    lines
        .into_iter()
        .map(|(id, origin)| {
            let id = id.unwrap();
            (id, origin, paths[&id].clone())
        })
        .collect()
}

/// `git blame --first-parent` of `file`, in the form of [`blame_lines`].
fn git_blame_lines(store: &Store, dir: &Path, file: &str) -> Vec<(ChangesetId, u32, RepoPath)> {
    // `--porcelain`, not `--line-porcelain`, which is slow on large files.
    let out = git(dir, &["blame", "--first-parent", "--porcelain", file]);
    let mut lines = Vec::new();
    // Each commit's changeset, and its path as git last named it. git names
    // the path the first time it shows a commit, and each time if the
    // commit has lines from more than one path.
    let mut commits: HashMap<String, (ChangesetId, RepoPath)> = HashMap::new();
    // Each line starts with "<commit> <origin line> <line> [<count>]", and
    // its text comes after a tab.
    let mut header: Option<(String, u32)> = None;
    for line in out.lines() {
        if line.starts_with('\t') {
            let (commit, origin) = header.take().unwrap();
            let (id, name) = commits[&commit].clone();
            lines.push((id, origin, name));
        } else if let Some((commit, _)) = &header {
            if let Some(name) = line.strip_prefix("filename ") {
                commits.insert(commit.clone(), (changeset_of(store, commit), path(name)));
            }
        } else {
            let fields: Vec<&str> = line.split(' ').collect();
            header = Some((fields[0].to_owned(), fields[1].parse().unwrap()));
        }
    }
    lines
}

/// A rename case: a commit message, the files the commit deletes, with
/// their text, and the file it adds.
type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], (&'a str, &'a str));

/// Commits the files that `cases` delete, all at once. Then commits each
/// case on its own: it deletes its files and adds its file.
fn commit_cases(dir: &Path, cases: &[Case<'_>]) {
    let sources: Vec<_> = cases
        .iter()
        .flat_map(|(_, deleted, _)| deleted.iter().map(|&(p, text)| (p, Some(text))))
        .collect();
    commit(dir, "add the sources", &sources);
    for &(message, deleted, (added, text)) in cases {
        let mut files: Vec<_> = deleted.iter().map(|&(p, _)| (p, None)).collect();
        files.push((added, Some(text)));
        commit(dir, message, &files);
    }
}

/// Imports the git history in `dir`, and checks that the blame of each of
/// `files` at the tip matches `git blame --first-parent`: the changeset,
/// line and path of each line.
fn assert_blame_matches_git(dir: &Path, files: &[&str]) -> (TempDir, Store, ChangesetId) {
    let (data, store) = open_store();
    let tip = import(&store, dir);
    for file in files {
        let expected = git_blame_lines(&store, dir, file);
        assert_eq!(blame_lines(&store, tip, file), expected, "{file}");
    }
    (data, store, tip)
}

#[test]
fn blame_follows_renames_as_git_blame_first_parent_does() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    commit_cases(
        dir,
        &[
            (
                "pure rename",
                &[("pure.txt", "p1\np2\np3\n")],
                ("renamed.txt", "p1\np2\np3\n"),
            ),
            (
                "rename with an edit, 77% similar",
                &[("edit.txt", "e1\ne2\ne3\ne4\ne5\ne6\ne7\ne8\ne9\ne10\n")],
                (
                    "edited.txt",
                    "e1\ne2\ne3\ne4\ne5\ne6\ne7\ne8\ne9\nchanged\n",
                ),
            ),
            (
                "rename into a new directory, with an edit",
                &[("top.txt", "t1\nt2\nt3\n")],
                ("new/dir/top.txt", "t1\nt2\nt3\nt4\n"),
            ),
            (
                "a delete and an unrelated add",
                &[("gone.txt", "g1\ng2\ng3\n")],
                ("fresh.txt", "f1\nf2\nf3\n"),
            ),
        ],
    );
    commit(dir, "add swap", &[("swap", Some("s1\ns2\ns3\n"))]);
    commit(
        dir,
        "swap becomes a directory",
        &[("swap", None), ("swap/inner", Some("s1\ns2\ns3\ns4\n"))],
    );
    commit(
        dir,
        "and a file again",
        &[("swap", None), ("swap", Some("s0\ns1\ns2\ns3\ns4\n"))],
    );
    std::os::unix::fs::symlink("nowhere", dir.join("link")).unwrap();
    commit(
        dir,
        "add a symlink",
        &[("link-source.txt", Some("k1\nk2\n"))],
    );
    // A type change, not a rename.
    commit(
        dir,
        "the symlink becomes a regular file",
        &[
            ("link-source.txt", None),
            ("link", None),
            ("link", Some("k1\nk2\n")),
        ],
    );
    let files = [
        "renamed.txt",
        "edited.txt",
        "new/dir/top.txt",
        "fresh.txt",
        "swap",
        "link",
    ];
    let (_data, store, tip) = assert_blame_matches_git(dir, &files);
    // `Log` with a path does not follow renames, as `git log` without
    // `--follow`.
    let (log, next) = path_log(&store, tip, "renamed.txt");
    assert_eq!((log.len(), next), (1, None));
}

#[test]
fn blame_picks_the_rename_source_as_git_blame_does() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    // One line of 192 bytes in three 64-byte chunks, then "a".
    let long = |end: &str| {
        format!(
            "{}{}{}\na\n",
            "x".repeat(64),
            "y".repeat(64),
            end.repeat(64)
        )
    };
    // 20 lines of 4 bytes. The new file keeps 9 of them: 45%.
    let line = |n: usize, kept: bool| format!("{}{n:02}\n", if kept { 'l' } else { 'X' });
    let twenty: String = (1..=20).map(|n| line(n, true)).collect();
    let nine_of_twenty: String = (1..=20).map(|n| line(n, n <= 9)).collect();
    // 10 CRLF lines of 4 bytes, 5 kept: 50% of the bytes, but a CR does
    // not count, so 37.5%.
    let crlf_line = |n: usize, other: char| format!("{}{n}\r\n", if n < 5 { 'k' } else { other });
    let crlf = |other| -> String { (0..10).map(|n| crlf_line(n, other)).collect() };
    let cases: [Case<'_>; 9] = [
        (
            "two sources with the same content: the same name wins",
            &[("tie/d/w.txt", "w1\nw2\n"), ("tie/e/v.txt", "w1\nw2\n")],
            ("tie/f/v.txt", "w1\nw2\n"),
        ),
        (
            "the one source with the same name wins at 80%, over 90%",
            &[
                ("name/a/o.txt", "1\n2\n3\n4\n5\n6\n7\n8\n9\nT\n"),
                ("name/b/z.txt", "1\n2\n3\n4\n5\n6\n7\n8\nW\nX\n"),
            ],
            ("name/c/z.txt", "1\n2\n3\n4\n5\n6\n7\n8\n9\nD\n"),
        ),
        (
            "two sources at 60%: the same name wins",
            &[
                ("even/a/o.txt", "1\n2\n3\n4\n5\n6\nQ\nR\nS\nT\n"),
                ("even/b/z.txt", "1\n2\n3\n4\n5\n6\nU\nV\nW\nX\n"),
            ],
            ("even/c/z.txt", "1\n2\n3\n4\n5\n6\nA\nB\nC\nD\n"),
        ),
        (
            "three sources at 60%, two with the same name: the first of them wins",
            &[
                ("twice/a/o.txt", "1\n2\n3\n4\n5\n6\nQ\nR\nS\nT\n"),
                ("twice/b/z.txt", "1\n2\n3\n4\n5\n6\nU\nV\nW\nX\n"),
                ("twice/c/z.txt", "1\n2\n3\n4\n5\n6\nK\nL\nM\nN\n"),
            ],
            ("twice/d/z.txt", "1\n2\n3\n4\n5\n6\nA\nB\nC\nD\n"),
        ),
        (
            "line ends change: a CR before a newline does not count",
            &[("crlf.txt", "aaa\r\nbbb\r\nccc\r\nddd\r\neee\nfff\n")],
            ("lf.txt", "aaa\nbbb\nccc\nddd\neee\nfff\n"),
        ),
        (
            "CRLF on both sides: 50% of the bytes, under 50% without the CRs",
            &[("crlf-half.txt", &crlf('o'))],
            ("crlf-half-new.txt", &crlf('n')),
        ),
        (
            "a long line counts in chunks of 64 bytes",
            &[("long.txt", &long("z"))],
            ("long-new.txt", &long("Q")),
        ),
        (
            "exactly 50% similar",
            &[(
                "half.txt",
                "aaa\nbbb\nccc\nddd\neee\nfff\nggg\nhhh\niii\njjj\n",
            )],
            (
                "half-new.txt",
                "aaa\nbbb\nccc\nddd\neee\nFFF\nGGG\nHHH\nIII\nJJJ\n",
            ),
        ),
        (
            "45% similar",
            &[("under.txt", &twenty)],
            ("under-new.txt", &nine_of_twenty),
        ),
    ];
    commit_cases(dir, &cases);
    let files = cases.map(|(_, _, (added, _))| added);
    assert_blame_matches_git(dir, &files);
}

#[test]
fn a_rename_search_reads_every_file_the_old_limit_read() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    // Nine deleted files of 1 MiB, in 8-byte lines: 1,179,648 chunks, more
    // than one call's budget. `new` is `big/8`, the last by path, with one
    // line changed. git compares all nine and follows `big/8`.
    let file = |k: usize| format!("{k}aaaaaa\n").repeat((1 << 20) / 8);
    let sources: Vec<(String, String)> = (0..9).map(|k| (format!("big/{k}"), file(k))).collect();
    let sources: Vec<(&str, &str)> = sources
        .iter()
        .map(|(p, text)| (p.as_str(), text.as_str()))
        .collect();
    let new = file(8).replacen("8aaaaaa\n", "changed\n", 1);
    commit_cases(
        dir,
        &[("rename one of nine large files", &sources, ("new", &new))],
    );
    assert_blame_matches_git(dir, &["new"]);
}

#[test]
fn binary_files_are_blamed_as_git_blame_does() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    // A NUL byte makes a file binary to git, but `git blame` still diffs it
    // line by line.
    commit(
        dir,
        "add",
        &[
            ("bin", Some("a\nb\n\0c\nd\n")),
            ("mixed", Some("a\nb\n\0c\n")),
        ],
    );
    commit(
        dir,
        "edit, and mixed becomes text",
        &[
            ("bin", Some("a\nb\n\0c\nd\ne\n")),
            ("mixed", Some("a\nb\nc\n")),
        ],
    );
    // In a binary file, a CR before a newline counts when git scores a
    // rename. These are 86% similar as text, but 3% as binary.
    let lines = |count: usize, end: &str| -> String {
        (1..=count)
            .map(|n| format!("k{n:04}{end}"))
            .collect::<Vec<_>>()
            .concat()
    };
    let (crlf, lf) = (
        format!("\0\n{}", lines(9, "\r\n")),
        format!("\0\n{}", lines(9, "\n")),
    );
    // git looks for a NUL byte in the first 8,000 bytes only. 1,142 lines
    // of 7 bytes, then a NUL byte at offset 7,999 (binary: under 1% similar)
    // or 8,000 (text: 86% similar).
    let nul_at = |offset: usize, end: &str| {
        format!("{}{}\0\n", lines(1142, end), &"abcdef"[..offset - 7994])
    };
    commit_cases(
        dir,
        &[
            (
                "rename a binary file, and change its line ends",
                &[("crlf.bin", &crlf)],
                ("lf.bin", &lf),
            ),
            (
                "the same, with a NUL byte at offset 7,999",
                &[("nul-7999.bin", &nul_at(7999, "\r\n"))],
                ("nul-7999-lf.bin", &nul_at(7999, "\n")),
            ),
            (
                "the same, with a NUL byte at offset 8,000",
                &[("nul-8000.bin", &nul_at(8000, "\r\n"))],
                ("nul-8000-lf.bin", &nul_at(8000, "\n")),
            ),
        ],
    );
    let files = [
        "bin",
        "mixed",
        "lf.bin",
        "nul-7999-lf.bin",
        "nul-8000-lf.bin",
    ];
    assert_blame_matches_git(dir, &files);
}

#[test]
fn blame_continues_at_the_old_path_after_a_rename() {
    let (_data, store) = open_store();
    // Each changeset adds one line at the top. The one at `before` also
    // renames `a` to `f`. One call diffs MAX_BLAME_VERSIONS versions, so it
    // passes the rename and stops on the history of `a`.
    let before = MAX_BLAME_VERSIONS / 2 + 10;
    let mut text = String::new();
    let changes = (0..=before + MAX_BLAME_VERSIONS / 2)
        .map(|n| {
            text.insert_str(0, &format!("line {n}\n"));
            let put = Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
            match n.cmp(&before) {
                Ordering::Less => vec![("a", put)],
                Ordering::Equal => vec![("a", None), ("f", put)],
                Ordering::Greater => vec![("f", put)],
            }
        })
        .collect();
    let ids = chain(&store, changes);
    let tip = *ids.last().unwrap();

    let (next, next_path) = blame(&store, tip, "f").next.unwrap();
    assert!(ids[..before].contains(&next), "{next}");
    assert_eq!(next_path, path("a"));
    // Line k came from changeset `len - k`, where it was line 1.
    let len = ids.len();
    let expected: Vec<_> = (1..=len)
        .map(|k| {
            let at = if len - k < before { "a" } else { "f" };
            (ids[len - k], 1, path(at))
        })
        .collect();
    assert_eq!(blame_lines(&store, tip, "f"), expected);
}

#[test]
fn a_rename_search_counts_against_the_diff_budget() {
    let (_data, store) = open_store();
    // `ids[1]` renames `a` to `f` and adds a line, so finding the source
    // compares about 2n chunks, and the diff after it about 2n lines. The
    // diff of `ids[2]` costs about 2n lines first. Without the search, the
    // call fits in its budget.
    let n = 200_000;
    assert!(4 * n + 4 <= MAX_BLAME_DIFF_LINES && 6 * n > MAX_BLAME_DIFF_LINES);
    let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let empty = "\n".repeat(n);
    let ids = chain(
        &store,
        vec![
            vec![("a", put(&empty))],
            vec![("a", None), ("f", put(&format!("1\n{empty}")))],
            vec![("f", put(&format!("2\n1\n{empty}")))],
        ],
    );
    let first = blame(&store, ids[2], "f");
    assert_eq!(
        first.ranges,
        [range(1, 1, Some(ids[2]), 1), range(2, n + 1, None, 1)]
    );
    assert_eq!(first.next, Some((ids[1], path("f"))));
    let rest = blame(&store, ids[1], "f");
    assert_eq!(
        rest.ranges,
        [range(1, 1, Some(ids[1]), 1), range(2, n, Some(ids[0]), 1)]
    );
    assert_eq!(rest.changesets[1], (ids[0], author(), path("a")));
}

#[test]
fn a_long_line_costs_once_per_64_bytes() {
    let (_data, store) = open_store();
    // 2,000 lines of 6,400 bytes cost 200,000 each side. Each version adds
    // one short line at the top, so each diff costs about 400,000, and a
    // call stops before its third diff. Counted by lines, all would fit.
    let n = 2_000;
    let pieces = n * 100;
    assert!(4 * pieces < MAX_BLAME_DIFF_LINES && 6 * pieces > MAX_BLAME_DIFF_LINES);
    let mut text = format!("{}\n", "x".repeat(6_399)).repeat(n);
    let mut changes = vec![vec![(
        "f",
        regular(store.blobs().put(text.as_bytes()).unwrap()),
    )]];
    for k in 1..=3 {
        text.insert_str(0, &format!("{k}\n"));
        changes.push(vec![(
            "f",
            regular(store.blobs().put(text.as_bytes()).unwrap()),
        )]);
    }
    let ids = chain(&store, changes);
    assert_eq!(blame(&store, ids[3], "f").next, Some((ids[1], path("f"))));
}

#[test]
fn a_rename_search_stops_after_its_bound() {
    let (_data, store) = open_store();
    // Files of MAX_BLAME_BYTES, in lines of 64 bytes. `new` is a rename of
    // `old/7` with one line changed. `old/0` to `old/6` come first by path
    // and are not like it. After seven of them, the search has read more
    // than MAX_RENAME_BYTES, so it stops.
    const { assert!(6 * MAX_BLAME_BYTES <= MAX_RENAME_BYTES && 7 * MAX_BLAME_BYTES > MAX_RENAME_BYTES) };
    let lines = MAX_BLAME_BYTES / 64;
    let line = |byte: char| format!("{}\n", byte.to_string().repeat(63));
    let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let names: Vec<String> = (0..=7).map(|k| format!("old/{k}")).collect();
    let unlike = put(&line('a').repeat(lines));
    let mut root: Vec<_> = names[..7].iter().map(|p| (p.as_str(), unlike)).collect();
    root.push((names[7].as_str(), put(&line('n').repeat(lines))));
    let mut rename: Vec<_> = names.iter().map(|p| (p.as_str(), None)).collect();
    let new = format!("{}{}", line('c'), line('n').repeat(lines - 1));
    rename.push(("new", put(&new)));
    let ids = chain(&store, vec![root, rename]);
    assert_eq!(
        blame(&store, ids[1], "new").ranges,
        [range(1, lines, Some(ids[1]), 1)]
    );
}

#[test]
fn a_mode_change_or_a_rename_with_no_edit_costs_nothing() {
    let (_data, store) = open_store();
    // `ids[1]` makes `a` executable, and `ids[2]` renames it to `f`, both
    // with the same content. A diff of either would cost about 2n lines,
    // and the call would stop after the first diff.
    let n = 300_000;
    assert!(4 * n > MAX_BLAME_DIFF_LINES && 2 * n < MAX_BLAME_DIFF_LINES);
    let empty = "\n".repeat(n);
    let blob = store.blobs().put(empty.as_bytes()).unwrap();
    let executable = FileState::Regular {
        content: blob,
        executable: true,
    };
    let edit = store.blobs().put(format!("x\n{empty}").as_bytes()).unwrap();
    let ids = chain(
        &store,
        vec![
            vec![("a", Some(regular(blob)))],
            vec![("a", Some(executable))],
            vec![("a", None), ("f", Some(executable))],
            vec![("f", Some(regular(edit)))],
        ],
    );
    let result = blame(&store, ids[3], "f");
    assert_eq!(
        result.ranges,
        [range(1, 1, Some(ids[3]), 1), range(2, n, Some(ids[0]), 1)]
    );
    assert_eq!(result.next, None);
}

#[test]
fn a_file_read_and_refused_costs_its_size() {
    let (_data, store) = open_store();
    // `ids[0]` has more lines than blame reads. It is read, then refused,
    // and that costs about 16,000. With the diff of `ids[2]` before it, the
    // call passes its budget and stops at `ids[1]`.
    let n = 520_000;
    let refused = MAX_BLAME_LINES.div_ceil(64);
    assert!(2 * n < MAX_BLAME_DIFF_LINES && 2 * n + 1 + refused > MAX_BLAME_DIFF_LINES);
    let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let empty = "\n".repeat(n);
    let ids = chain(
        &store,
        vec![
            vec![("f", put(&"\n".repeat(MAX_BLAME_LINES + 1)))],
            vec![("f", put(&empty))],
            vec![("f", put(&format!("x\n{empty}")))],
        ],
    );
    assert_eq!(blame(&store, ids[2], "f").next, Some((ids[1], path("f"))));
}

#[test]
fn a_changeset_with_many_deletes_is_searched_only_for_the_same_content_or_name() {
    let (_data, store) = open_store();
    let put = |data: &str| Some(regular(store.blobs().put(data.as_bytes()).unwrap()));
    let ten = "1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n";
    let edited = "1\n2\n3\n4\n5\n6\n7\n8\n9\nX\n";
    // Small files, too small to be like any blamed file.
    let fillers: Vec<String> = (0..4 * MAX_RENAME_SOURCES)
        .map(|n| format!("filler/{n}"))
        .collect();
    let mut fillers = fillers.iter().map(String::as_str);
    let mut root: Vec<_> = fillers.clone().map(|f| (f, put("f\n"))).collect();
    root.extend(["a.txt", "b.txt", "c/same.txt", "e.txt"].map(|p| (p, put(ten))));
    // Each changeset deletes a source and fillers, and adds one file.
    let mut step = |source: &'static str, deletes: usize, added: &'static str, text: &str| {
        let mut changes: Vec<_> = fillers
            .by_ref()
            .take(deletes - 1)
            .map(|f| (f, None))
            .collect();
        changes.extend([(source, None), (added, put(text))]);
        changes
    };
    let changes = vec![
        root,
        step("a.txt", MAX_RENAME_SOURCES, "a2.txt", edited),
        step("b.txt", MAX_RENAME_SOURCES + 1, "b2.txt", edited),
        step("c/same.txt", MAX_RENAME_SOURCES + 1, "d/same.txt", edited),
        step("e.txt", MAX_RENAME_SOURCES + 1, "e2.txt", ten),
    ];
    let ids = chain(&store, changes);
    let tip = ids[4];
    let renamed = |p: &str, at: usize, source: &str| {
        let result = blame(&store, tip, p);
        assert_eq!(
            result.ranges,
            [
                range(1, 9, Some(ids[0]), 1),
                range(10, 1, Some(ids[at]), 10)
            ],
            "{p}"
        );
        assert_eq!(result.changesets[1], (ids[0], author(), path(source)));
    };
    // Up to MAX_RENAME_SOURCES deletes, every deleted file is compared.
    renamed("a2.txt", 1, "a.txt");
    // Over it, a file that is only similar, under another name, is not.
    assert_eq!(
        blame(&store, tip, "b2.txt").ranges,
        [range(1, 10, Some(ids[2]), 1)]
    );
    // The one deleted file with the same name still is, and the same
    // content is always found.
    renamed("d/same.txt", 3, "c/same.txt");
    let exact = blame(&store, tip, "e2.txt");
    assert_eq!(exact.ranges, [range(1, 10, Some(ids[0]), 1)]);
    assert_eq!(exact.changesets, [(ids[0], author(), path("e.txt"))]);
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
    assert_eq!(first.next, Some((ids[1], path("f"))));
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
