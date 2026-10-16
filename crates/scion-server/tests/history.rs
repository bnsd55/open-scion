//! Path history (`Log` with a path) and blame, with their bounds.
#![cfg(unix)]

mod common;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::blame::{
    self, Blame, BlameRange, Line, MAX_BLAME_DIFF_LINES, MAX_BLAME_VERSIONS, RenameSearch, Resume,
};
use scion_server::deadline::Deadline;
use scion_server::import::{ImportOptions, import_git};
use scion_server::memory::Bytes;
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

fn later() -> Deadline {
    Deadline::new(Instant::now() + Duration::from_secs(600))
}

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config, and returns stdout. Fails the test unless git succeeds.
fn git(dir: &Path, args: &[&str]) -> String {
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
    common::spawn_guard::prime_gix_lookup();
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
    let page = read::log_page(store, REPO, from, Some(&path(p)), 1000, &later()).unwrap();
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
    let page = read::log_page(&store, REPO, tip, Some(&path("a.txt")), 2, &later()).unwrap();
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
    blame::blame(
        store,
        REPO,
        cs,
        &path(p),
        &Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap()
}

/// A resume that goes on with `next` and the search, at line 0.
/// Whether a reply's resume goes on at `cs` and `p`.
fn goes_on(resume: Option<&Resume>, cs: ChangesetId, p: &str) -> bool {
    resume.and_then(|r| r.next.as_ref()) == Some(&(cs, path(p)))
}

/// Blames with a pool of `cap` bytes.
fn blame_bounded(
    store: &Store,
    cs: ChangesetId,
    p: &str,
    cap: usize,
) -> std::result::Result<Blame, scion_server::error::Error> {
    blame::blame(
        store,
        REPO,
        cs,
        &path(p),
        &Resume::default(),
        &Bytes::new(cap),
        None,
    )
}

#[test]
fn a_path_log_reads_past_max_scan_changesets_and_blame_resumes() {
    let (_data, store) = open_store();
    let f = store.blobs().put(b"one\ntwo\n").unwrap();
    // `f` changes only in the root, then MAX_SCAN changesets change `g`.
    // Their content is never read, so it is not stored.
    let mut changes = vec![vec![("f", regular(f)), ("g", regular(BlobId::of(b"")))]];
    changes.extend((0..MAX_SCAN).map(|i| vec![("g", regular(BlobId::of(&i.to_le_bytes())))]));
    let ids = chain(&store, changes);
    let (root, tip) = (ids[0], *ids.last().unwrap());

    // A path log reads the path history, so one call finds the root.
    assert_eq!(path_log(&store, tip, "f"), (vec![root], None));

    let first = blame(&store, tip, "f");
    assert_eq!(first.ranges, [range(1, 2, None, 1)]);
    assert!(goes_on(first.resume.as_ref(), root, "f"));
    let rest = blame(&store, root, "f");
    assert_eq!(rest.ranges, [range(1, 2, Some(root), 1)]);
    assert_eq!(rest.resume, None);
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
    assert!(goes_on(first.resume.as_ref(), ids[0], "f"));
    assert_eq!(first.changesets.len(), MAX_BLAME_VERSIONS);

    let rest = blame(&store, ids[0], "f");
    assert_eq!(rest.ranges, [range(1, 1, Some(ids[0]), 1)]);
    assert_eq!(rest.resume, None);
}

#[test]
fn blame_gives_every_line_to_the_change_after_a_version_it_cannot_diff() {
    let (_data, store) = open_store();
    // One million empty lines: the version's text is small, but a diff of
    // it holds about 128 bytes per line, far past a pool of 2 MiB.
    let big = store.blobs().put("\n".repeat(1 << 20).as_bytes()).unwrap();
    let text = store.blobs().put(b"a\nb\n").unwrap();
    let edit = store.blobs().put(b"a\nb\nc\n").unwrap();
    let ids = chain(
        &store,
        vec![
            vec![("f", regular(big))],
            vec![("f", regular(text))],
            vec![("f", regular(edit))],
        ],
    );
    // A line diff would keep "a" and "b" from the first version. Its diff
    // does not fit the budget, so the version counts as absent, and its
    // lines belong to the change after it.
    let result = blame_bounded(&store, ids[2], "f", 2 << 20).unwrap();
    assert_eq!(
        result.ranges,
        [range(1, 2, Some(ids[1]), 1), range(3, 1, Some(ids[2]), 3)]
    );
    assert_eq!(result.resume, None, "every line has a changeset");
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
    // Files of the old limits: 16 MiB in one line, and a million lines.
    // There are no size or line limits any more: both blame as one range.
    let ids = chain(
        &store,
        vec![vec![
            ("16mib", put(&vec![b'x'; 16 << 20])),
            ("million-lines", put(&vec![b'\n'; 1 << 20])),
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
        blame(&store, cs, "16mib").ranges,
        [range(1, 1, Some(cs), 1)]
    );
    assert_eq!(
        blame(&store, cs, "million-lines").ranges,
        [range(1, 1 << 20, Some(cs), 1)]
    );
    for p in ["dir", "link"] {
        let result = blame::blame(
            &store,
            REPO,
            cs,
            &path(p),
            &Resume::default(),
            &Bytes::new(1 << 30),
            None,
        );
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{p}: {result:?}"
        );
    }
    let missing = blame::blame(
        &store,
        REPO,
        cs,
        &path("missing"),
        &Resume::default(),
        &Bytes::new(1 << 30),
        None,
    );
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
}

#[test]
fn blame_refuses_a_file_whose_share_is_past_the_whole_budget() {
    let (_data, store) = open_store();
    let f = store.blobs().put(&[b'x'; 100]).unwrap();
    let ids = chain(&store, vec![vec![("f", regular(f))]]);
    // The text alone is 100 bytes; with its rows and a diff's state it is
    // about 12,800. A pool of 64 bytes cannot hold it.
    let result = blame_bounded(&store, ids[0], "f", 64);
    assert!(
        matches!(result, Err(Error::ResourceExhausted(_))),
        "{result:?}"
    );
}

/// Blames `p` at `cs` to the end, and continues where a call stops, as a
/// client does. Each line as its changeset, its line there and the path
/// there.
fn blame_lines(store: &Store, cs: ChangesetId, p: &str) -> Vec<(ChangesetId, u32, RepoPath)> {
    blame_calls(store, cs, p).0
}

/// [`blame_lines`], and the calls it took.
fn blame_calls(
    store: &Store,
    cs: ChangesetId,
    p: &str,
) -> (Vec<(ChangesetId, u32, RepoPath)>, usize) {
    let mut resume = Resume::default();
    let mut paths = HashMap::new();
    // Each line's changeset and line at it, once known, and until then its
    // line in the file at the walk's position.
    let mut lines: Vec<(Option<ChangesetId>, u32)> = Vec::new();
    let mut calls = 0;
    loop {
        calls += 1;
        let result = blame::blame(
            store,
            REPO,
            cs,
            &path(p),
            &resume,
            &Bytes::new(1 << 30),
            None,
        )
        .unwrap();
        paths.extend(result.changesets.into_iter().map(|(id, _, p)| (id, p)));
        // Each reply covers a window of the file: its first range names
        // the window's first line.
        let at_line = result
            .ranges
            .first()
            .map(|r| r.start_line as usize - 1)
            .expect("a reply has ranges");
        let here: Vec<(Option<ChangesetId>, u32)> = result
            .ranges
            .iter()
            .flat_map(|r| (0..r.line_count).map(move |n| (r.changeset, r.origin_line + n)))
            .collect();
        if at_line == lines.len() {
            lines.extend(here);
        } else {
            // The same window again, from a call that went deeper into the
            // history: its reply maps the lines still without a changeset.
            assert!(at_line < lines.len(), "the server skipped lines");
            for (line, source) in lines.iter_mut().enumerate().skip(at_line) {
                if source.0.is_some() {
                    continue;
                }
                *source = here[line - at_line];
            }
        }
        match result.resume {
            Some(next) => {
                // The next call gets the cursor the reply carried, and the
                // server reads it back the way a resumed call does.
                eprintln!(
                    "DEBUG resume: after_line={} counted={} window={} next_path={:?} searched={} best={:?} first_lines={:?}",
                    next.after_line,
                    next.counted,
                    next.window.len(),
                    next.next.as_ref().map(|(_, p)| p.as_bytes()),
                    next.search.searched,
                    next.search.best,
                    &next.window[..next.window.len().min(6)]
                );
                let bytes = blame::encode_cursor(&next, REPO, cs, &path(p));
                let (decoded, hold) = blame::decode_cursor(
                    store,
                    REPO,
                    &bytes,
                    cs,
                    &path(p),
                    &Bytes::new(1 << 30),
                    None,
                )
                .unwrap();
                drop(hold);
                resume = decoded;
            }
            None => break,
        }
    }
    let lines = lines
        .into_iter()
        .map(|(id, origin)| {
            let id = id.unwrap();
            (id, origin, paths[&id].clone())
        })
        .collect();
    (lines, calls)
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
fn a_rename_search_past_one_call_matches_git() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    // Nine deleted files of 1 MiB, in 8-byte lines: 1,179,648 chunks, more
    // than one call's budget, so the search goes on in the next call. `new`
    // is `big/8`, the last by path, with one line changed. git compares all
    // nine and follows `big/8`.
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

    let resume = blame(&store, tip, "f").resume.expect("the call stopped");
    let Some((next, next_path)) = resume.next else {
        panic!("the walk stopped");
    };
    assert!(ids[..before].contains(&next), "{next}");
    // The walk follows the rename to the old path.
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
    // The walk stopped at `ids[1]`, where the search for the rename source
    // ran to its end.
    assert_eq!(first.resume.unwrap().next, Some((ids[1], path("f"))));
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
    // The walk stopped at `ids[1]` after one diff; the reply carries the
    // lines it named and where the rest sit.
    let resume = blame(&store, ids[3], "f").resume.expect("the call stopped");
    assert!(goes_on(Some(&resume), ids[1], "f"));
    assert_eq!(resume.counted, 2_003);
    assert_eq!(resume.window.len(), 2_003);
    assert_eq!(resume.window[0], Line::Named(ids[3]));
    assert_eq!(resume.window[1], Line::Named(ids[2]));
    assert_eq!(resume.window[2], Line::Unnamed(0));
}

#[test]
fn a_rename_search_goes_on_where_a_call_stopped() {
    let (_data, store) = open_store();
    // Files of 16 MiB, in lines of 64 bytes: n chunks each, so a call's
    // budget is four of them. `new` is a rename of `old/7` with one
    // line changed. `old/1` is 60% like it, the rest are not like it.
    // A call reads `new`, then deleted files while its budget lasts, and
    // reads the best file so far again when it goes on. `lib/new`, first by
    // path, has the name of `new`, so step 2 of the first call reads it. The
    // calls that go on do not run step 2 again.
    let n = MAX_BLAME_DIFF_LINES / 4;
    assert_eq!(4 * n, MAX_BLAME_DIFF_LINES);
    let line = |byte: char| format!("{}\n", byte.to_string().repeat(63));
    let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let mut names: Vec<String> = (0..=7).map(|k| format!("old/{k}")).collect();
    names.push("lib/new".into());
    let unlike = put(&line('a').repeat(n));
    let mut root: Vec<_> = names.iter().map(|p| (p.as_str(), unlike)).collect();
    let sixty = line('n').repeat(n * 3 / 5) + &line('b').repeat(n - n * 3 / 5);
    root[1].1 = put(&sixty);
    root[7].1 = put(&line('n').repeat(n));
    let mut rename: Vec<_> = names.iter().map(|p| (p.as_str(), None)).collect();
    let new = format!("{}{}", line('c'), line('n').repeat(n - 1));
    rename.push(("new", put(&new)));
    let edit = vec![("new", put("edited\n"))];
    let ids = chain(&store, vec![root, rename, edit]);

    let mut resume = Resume::default();
    for searched in [3, 5, 7] {
        let result = blame::blame(
            &store,
            REPO,
            ids[1],
            &path("new"),
            &resume,
            &Bytes::new(1 << 30),
            None,
        )
        .unwrap();
        assert_eq!(result.ranges, [range(1, n, None, 1)]);
        let next = result.resume.expect("the search stopped");
        assert!(goes_on(Some(&next), ids[1], "new"));
        let expected = RenameSearch {
            searched,
            best: Some(path("old/1")),
        };
        assert_eq!(next.search, expected);
        resume = next;
    }
    let last = blame::blame(
        &store,
        REPO,
        ids[1],
        &path("new"),
        &resume,
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    assert_eq!(last.resume, None);
    assert_eq!(last.ranges[0], range(1, 1, Some(ids[1]), 1));
    assert!(last.ranges[1..].iter().all(|r| r.changeset == Some(ids[0])));
    assert_eq!(last.changesets[1], (ids[0], author(), path("old/7")));
    assert_eq!(blame_calls(&store, ids[1], "new").1, 4);

    // A search that cannot have stopped there is refused: past the deleted
    // files, a best file not searched yet or not deleted, at a changeset
    // with no parent, and where the file was there before.
    let bad = [
        (ids[1], "new", 10, None),
        (ids[1], "new", 3, Some("old/3")),
        (ids[1], "new", 3, Some("old")),
        (ids[0], "old/7", 1, None),
        (ids[2], "new", 1, None),
    ];
    // A real reply's resume, with its search replaced by a bad one.
    let stopped = blame::blame(
        &store,
        REPO,
        ids[1],
        &path("new"),
        &Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap()
    .resume
    .expect("the search stopped");
    for (cs, p, searched, best) in bad {
        let mut resume = stopped.clone();
        resume.next = Some((cs, path(p)));
        resume.search = RenameSearch {
            searched,
            best: best.map(path),
        };
        let result = blame::blame(
            &store,
            REPO,
            ids[1],
            &path("new"),
            &resume,
            &Bytes::new(1 << 30),
            None,
        );
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{p} at {searched}: {result:?}"
        );
    }
}

#[test]
fn a_rename_search_moves_on_every_call() {
    let (_data, store) = open_store();
    // Files of half a call's budget, in lines of 16 bytes: one chunk each.
    // A call reads `new`, then deleted files. A call that goes on with a
    // best file reads `new` and that file again, which spends its budget,
    // and still looks at one more file. `old/1` is 60% like `new`, and `new`
    // is a rename of `old/2` with one line changed.
    let n = MAX_BLAME_DIFF_LINES / 2;
    let line = |byte: char| format!("{}\n", byte.to_string().repeat(15));
    let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let sixty = line('n').repeat(n * 3 / 5) + &line('b').repeat(n - n * 3 / 5);
    let root = vec![
        ("old/0", put(&line('a').repeat(n))),
        ("old/1", put(&sixty)),
        ("old/2", put(&line('n').repeat(n))),
    ];
    let new = line('c') + &line('n').repeat(n - 1);
    let rename = vec![
        ("old/0", None),
        ("old/1", None),
        ("old/2", None),
        ("new", put(&new)),
    ];
    let ids = chain(&store, vec![root, rename]);
    let mut resume = Resume::default();
    for (searched, best) in [(1, None), (2, Some("old/1"))] {
        let result = blame::blame(
            &store,
            REPO,
            ids[1],
            &path("new"),
            &resume,
            &Bytes::new(1 << 30),
            None,
        )
        .unwrap();
        let search = RenameSearch {
            searched,
            best: best.map(path),
        };
        let next = result.resume.expect("the search stopped");
        assert!(goes_on(Some(&next), ids[1], "new"));
        assert_eq!(next.search, search);
        resume = next;
    }
    let last = blame::blame(
        &store,
        REPO,
        ids[1],
        &path("new"),
        &resume,
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    assert_eq!(last.resume, None);
    assert_eq!(last.changesets[1], (ids[0], author(), path("old/2")));
}

#[test]
fn a_rename_search_at_a_later_step_uses_the_budget_left() {
    // `ids[2]` adds a line to `new`, a diff that costs about 300,000. Before
    // that, `ids[1]` renamed `old` to `new` with one line changed. The
    // search and its diff do not fit in the budget left, so the call stops
    // at `ids[1]` and says where the search stopped. With no other deleted
    // file the search ends, so the next call only reads `old` again. With
    // five files of the same size before `old`, of one line repeated, so
    // not like `new`, it stops before `old`.
    let n: u32 = 150_000;
    let lines =
        |from: u32| (from..from + n).fold(String::new(), |text, i| text + &format!("{i:07}\n"));
    let old = lines(0);
    let new = old.replacen("0000000\n", "changed\n", 1);
    let ended = RenameSearch {
        searched: 1,
        best: Some(path("old")),
    };
    let before_old = RenameSearch {
        searched: 4,
        best: None,
    };
    for (decoys, stopped) in [(0, ended), (5, before_old)] {
        let (_data, store) = open_store();
        let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
        let names: Vec<String> = (0..decoys).map(|k| format!("a/{k}")).collect();
        let mut root: Vec<_> = names
            .iter()
            .map(|p| (p.as_str(), put(&format!("{p:<7}\n").repeat(150_000))))
            .collect();
        root.push(("old", put(&old)));
        let mut rename: Vec<_> = root.iter().map(|&(p, _)| (p, None)).collect();
        rename.push(("new", put(&new)));
        let edit = vec![("new", put(&format!("{new}extra00\n")))];
        let ids = chain(&store, vec![root, rename, edit]);
        let first = blame(&store, ids[2], "new");
        let next = first.resume.expect("the search stopped");
        assert!(goes_on(Some(&next), ids[1], "new"), "{decoys}");
        assert_eq!(next.search, stopped, "{decoys}");
        let (got, calls) = blame_calls(&store, ids[2], "new");
        let mut expected = vec![(ids[1], 1, path("new"))];
        expected.extend((2..=n).map(|i| (ids[0], i, path("old"))));
        expected.push((ids[2], n + 1, path("new")));
        assert_eq!((got, calls), (expected, 2), "{decoys}");
    }
}

#[test]
fn a_rename_search_counts_the_file_sizes_it_reads() {
    let (_data, store) = open_store();
    // 17,000 deleted files of a few bytes, each its own blob: too small to
    // be like `new`, so the search reads only their sizes. At 64 each, that
    // is more than one call's budget, so the search goes on in a second
    // call. `old`, first by path, is the rename.
    let names: Vec<String> = (0..17_000).map(|k| format!("t/{k:05}")).collect();
    let mut batch = store.blobs().batch();
    let mut root: Vec<_> = names
        .iter()
        .map(|p| (p.as_str(), Some(regular(batch.put(p.as_bytes()).unwrap()))))
        .collect();
    batch.sync().unwrap();
    let put = |text: &str| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let old = numbered(0..2000);
    root.push(("old", put(&old)));
    let mut rename: Vec<_> = root.iter().map(|&(p, _)| (p, None)).collect();
    rename.push(("new", put(&old.replacen("n000000\n", "changed\n", 1))));
    let ids = chain(&store, vec![root, rename]);
    let first = blame(&store, ids[1], "new");
    let Some(Resume { search, .. }) = first.resume else {
        panic!("the search stopped");
    };
    assert!(search.searched < names.len(), "{search:?}");
    assert_eq!(search.best, Some(path("old")));
    let (got, calls) = blame_calls(&store, ids[1], "new");
    assert_eq!(got[1], (ids[0], 2, path("old")));
    assert_eq!(calls, 2);
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
    assert_eq!(result.resume, None);
}

/// Lines `n<number>` for each number of `range`, 8 bytes each with their
/// newline.
fn numbered(range: std::ops::Range<usize>) -> String {
    range.fold(String::new(), |text, n| text + &format!("n{n:06}\n"))
}

/// A file of 2,000 lines of 8 bytes: the first `shared` lines of
/// `numbered(0..2000)`, then lines of file `k`'s own.
fn like_numbered(k: usize, shared: usize) -> String {
    (shared..2000).fold(numbered(0..shared), |text, n| {
        text + &format!("{k:03x}{n:04x}\n")
    })
}

/// Commits `count` files `f/<k>` of [`like_numbered`], with `shared(k)`
/// lines in common with `new.txt`, and `others`. Then deletes them all and
/// adds `new.txt`, `numbered(0..2000)`. Checks that blame of `new.txt`
/// matches git. Returns the calls it took.
fn assert_many_deletes_match_git(
    count: usize,
    shared: impl Fn(usize) -> usize,
    others: &[(&str, String)],
) -> usize {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    let mut files: Vec<(String, String)> = (0..count)
        .map(|k| (format!("f/{k:04}"), like_numbered(k, shared(k))))
        .collect();
    files.extend(
        others
            .iter()
            .map(|(p, text)| ((*p).to_owned(), text.clone())),
    );
    let deleted: Vec<(&str, &str)> = files
        .iter()
        .map(|(p, text)| (p.as_str(), text.as_str()))
        .collect();
    let new = numbered(0..2000);
    commit_cases(dir, &[("delete many files", &deleted, ("new.txt", &new))]);
    let (_data, store) = open_store();
    let tip = import(&store, dir);
    let expected = git_blame_lines(&store, dir, "new.txt");
    let (got, calls) = blame_calls(&store, tip, "new.txt");
    assert_eq!(got, expected);
    calls
}

#[test]
fn a_changeset_that_deletes_150_files_is_searched_as_git_blame_does() {
    // `f/0120` shares 80% of `new.txt`. `lib/new.txt` has its name, and no
    // other deleted file has, but it shares 60%: under 75%, so it only
    // competes on similarity. The others share 45% to 68%. Chunks whose
    // hashes collide add to that, as in git.
    let shared = |k: usize| if k == 120 { 1600 } else { 900 + k * 3 };
    let same_name = [("lib/new.txt", like_numbered(4000, 1200))];
    assert_many_deletes_match_git(150, shared, &same_name);
}

#[test]
fn a_changeset_that_deletes_1000_files_is_searched_over_calls_as_git_blame_does() {
    // `f/0100` shares 95% of `new.txt`, `f/0700` 92.5%, the others 45% to
    // 65%. Chunks whose hashes collide add to that, as in git: up to about
    // 75% here. The search reads all 1,000 files, more than one call's
    // budget, so later calls go on with `f/0100` as the best so far and keep
    // it.
    let shared = |k: usize| match k {
        100 => 1900,
        700 => 1850,
        _ => 900 + k * 37 % 400,
    };
    let calls = assert_many_deletes_match_git(1000, shared, &[]);
    assert!(calls > 2, "{calls} calls");
}

#[test]
fn blame_of_an_empty_file_has_no_ranges() {
    let (_data, store) = open_store();
    let empty = store.blobs().put(b"").unwrap();
    let ids = chain(&store, vec![vec![("f", regular(empty))]]);
    let result = blame(&store, ids[0], "f");
    assert_eq!(result.ranges, Vec::<BlameRange>::new());
    assert_eq!(result.resume, None);
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
    assert!(goes_on(first.resume.as_ref(), ids[1], "f"));
    let rest = blame(&store, ids[1], "f");
    assert_eq!(
        rest.ranges,
        [range(1, 1, Some(ids[1]), 1), range(2, n, Some(ids[0]), 1)]
    );
    assert_eq!(rest.resume, None);

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
    assert_eq!(result.resume, None);
}

/// A chain of changesets in the repo `name`, as [`chain`] does in `r`.
fn chain_in<S: Into<Option<FileState>>>(
    store: &Store,
    name: &str,
    puts: Vec<Vec<(&str, S)>>,
) -> Vec<ChangesetId> {
    assert_ne!(name, REPO);
    store
        .write(|c| {
            let repo = db::create_repo(c, name)?;
            let mut ids: Vec<ChangesetId> = Vec::new();
            for puts in puts {
                let cs = Changeset {
                    parents: ids.last().copied().into_iter().collect(),
                    author: author(),
                    committer: None,
                    message: format!("{name} change {}", ids.len()).into_bytes(),
                    changes: puts.into_iter().map(|(p, s)| (path(p), s.into())).collect(),
                };
                ids.push(tree::store_changeset(c, repo, &cs)?.0);
            }
            Ok(ids)
        })
        .unwrap()
}

/// Cursor bytes for `resume`, made for a blame of `p` at `cs` in `r`.
fn cursor_bytes(resume: &Resume, cs: ChangesetId, p: &str) -> Vec<u8> {
    blame::encode_cursor(resume, REPO, cs, &path(p))
}

#[test]
fn a_cursor_names_only_changesets_of_its_repo() {
    let (_data, store) = open_store();
    let put = |data: &[u8]| regular(store.blobs().put(data).unwrap());
    let ids = chain(
        &store,
        vec![vec![("f", put(b"a\n"))], vec![("f", put(b"b\n"))]],
    );
    // Another repo, with a changeset `r` does not have.
    let other = chain_in(&store, "other", vec![vec![("f", put(b"a\n"))]]);

    // A cursor whose walk goes on at the other repo's changeset: its
    // digest is right, its changeset is not.
    let walk = Resume {
        next: Some((other[0], path("f"))),
        search: RenameSearch::default(),
        after_line: 0,
        counted: 2,
        window: vec![Line::Unnamed(0), Line::Unnamed(1)],
    };
    let bytes = cursor_bytes(&walk, ids[1], "f");
    let err = blame::decode_cursor(
        &store,
        REPO,
        &bytes,
        ids[1],
        &path("f"),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");

    // The same for a line the cursor says was named by it.
    let named = Resume {
        next: Some((ids[1], path("f"))),
        search: RenameSearch::default(),
        after_line: 0,
        counted: 2,
        window: vec![Line::Named(other[0]), Line::Unnamed(0)],
    };
    let bytes = cursor_bytes(&named, ids[1], "f");
    let err = blame::decode_cursor(
        &store,
        REPO,
        &bytes,
        ids[1],
        &path("f"),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
}

#[test]
fn a_cursor_with_past_bounds_is_refused_before_it_allocates() {
    let (_data, store) = open_store();
    let put = |data: &[u8]| regular(store.blobs().put(data).unwrap());
    let ids = chain(
        &store,
        vec![vec![("f", put(b"a\n"))], vec![("f", put(b"b\n"))]],
    );
    // A cursor with an empty window: its header ends at the table count.
    let resume = Resume {
        next: Some((ids[1], path("f"))),
        search: RenameSearch::default(),
        after_line: 0,
        counted: 2,
        window: vec![Line::Unnamed(0), Line::Unnamed(1)],
    };
    let good = cursor_bytes(&resume, ids[1], "f");
    // version, digest, walk flag, walk changeset, walk path, searched, best
    // flag, after_line, counted, table count: the run count follows.
    let head = good.len() - 4 - 2; // past the run count and the two runs
    assert_eq!(good[good.len() - 6], 0, "no table");

    // A run count of u32::MAX: refused, and it does not allocate for it.
    let mut made = good[..head].to_vec();
    made.truncate(good.len() - 6);
    made.extend_from_slice(&u32::MAX.to_be_bytes());
    let err = blame::decode_cursor(
        &store,
        REPO,
        &made,
        ids[1],
        &path("f"),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");

    // A cursor that does not fit the budget is refused too.
    let err = blame::decode_cursor(
        &store,
        REPO,
        &good,
        ids[1],
        &path("f"),
        &Bytes::new(10),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::ResourceExhausted(_)),
        "a cursor past the budget: {err:?}"
    );
}

#[test]
fn the_worst_cursor_stays_small() {
    // The most runs a window can hold: its lines alternate between the
    // changesets one call can walk, and every other line is still open.
    // The table holds each changeset once; a run costs a few bytes.
    let versions = MAX_BLAME_VERSIONS;
    let window: Vec<Line> = (0..blame::MAX_BLAME_WINDOW_LINES)
        .map(|i| {
            if i % 2 == 0 {
                let mut id = [0u8; 32];
                id[0] = u8::try_from(i % versions).unwrap_or(u8::MAX);
                id[1] = 0x42;
                Line::Named(ChangesetId::from_bytes(id))
            } else {
                Line::Unnamed(i)
            }
        })
        .collect();
    let resume = Resume {
        next: None,
        search: RenameSearch::default(),
        after_line: 0,
        counted: blame::MAX_BLAME_WINDOW_LINES,
        window,
    };
    let bytes = cursor_bytes(&resume, ChangesetId::from_bytes([7; 32]), "f");
    println!(
        "worst cursor: {} bytes for {} lines, {} runs",
        bytes.len(),
        blame::MAX_BLAME_WINDOW_LINES,
        blame::MAX_BLAME_WINDOW_LINES
    );
    assert!(bytes.len() < 8 << 20, "{} bytes", bytes.len());
}

#[test]
fn a_call_that_holds_budget_never_waits_for_more() {
    let (_data, store) = open_store();
    // The older version is long, the blamed one short: the call takes the
    // short text and the file's line share first, and the long older text
    // does not fit what is left. The call must fail at once with
    // `RESOURCE_EXHAUSTED`, not wait for bytes that only it can free: with
    // a waiting take and no deadline, this test would hang forever.
    let long = store.blobs().put("line\n".repeat(100).as_bytes()).unwrap();
    let put_long = Some(regular(long));
    let put_short = Some(regular(store.blobs().put(b"a\nb\n").unwrap()));
    let ids = chain(&store, vec![vec![("f", put_long)], vec![("f", put_short)]]);
    // 6 bytes of text + 2 lines x 128 bytes of state leave 338 of 600,
    // and the 500 bytes of the older version do not fit them. The cap is
    // above 500, so the version is not absent: the take is what misses.
    let err = blame::blame(
        &store,
        REPO,
        ids[1],
        &path("f"),
        &Resume::default(),
        &Bytes::new(600),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::ResourceExhausted(_)),
        "a held budget never waits: {err:?}"
    );
}
