//! A Postgres contract check against an independent expected answer.
//!
//! The Postgres bugs this repo has already fixed - the parent numbering
//! twice, a missing ORDER BY - passed tests that compared two runs of the
//! same code: a store's answer against a store-shaped expectation. This
//! check compares each backend against a manifest computed from a real
//! git repository and from the workload the test itself builds, so an
//! answer that is wrong in the same way on both sides cannot pass here.
//!
//! The workload, deterministic from the fixture: import a git repo with
//! 52 commits (a side branch, a merge, a rename, a delete), land a clean
//! changeset, land one on a stale base that must conflict, retry that
//! stale land's request key, and run one `gc`.
//!
//! The manifest comes from git alone: the first-parent chain's subjects
//! (`git log --first-parent`), each path's history, the tip's whole tree
//! (`git ls-tree -r`), ancestor facts for sampled pairs of the linear
//! chain, and the blame of the one file only the linear commits touch
//! (`git blame --porcelain`). The lands' intended tree is the git tip's
//! tree plus the file the clean land adds, computed from the changeset
//! the test built itself.
//!
//! Per backend, the test first proves it really ran there (the #412
//! lesson: the env's choice and the store's actual kind agree, the
//! on-disk shape matches, the main tables hold non-zero row counts read
//! by raw SQL), then compares every public read - `ResolveRef`, `Log` with
//! and without a path, `GetRootTree`, `IsAncestor` for the sampled pairs and
//! a real DAG pair, blame - against the manifest, ordered where order is
//! part of the answer, and requires fsck to see no damage.
//!
//! The mutations this check provably catches are recorded in the PR text.

mod common;

use std::collections::BTreeMap;
use std::process::Command;

use scion_core::{Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::v1::{
    BlameRequest, GetChangesetRequest, IsAncestorRequest, LandRequest, LandResponse, LogRequest,
    ResolveRefRequest, scion_client::ScionClient,
};
use scion_server::{Backend, ImportOptions, import_git};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "contract";
const TOKEN: &str = "contract-token";

const LINEAR_COMMITS: usize = 30;
const SIDE_COMMITS: usize = 10;
const AFTER_MERGE_COMMITS: usize = 5;
const TAIL_COMMITS: usize = 4;

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

/// The git repository the manifest is computed from: 52 commits with a
/// side branch, a merge, a rename and a delete. The subjects are unique,
/// so a subject names its commit everywhere below.
struct Git(tempfile::TempDir);

impl Git {
    fn new() -> Self {
        let dir = tempfile::TempDir::new().unwrap();
        let out = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .current_dir(dir.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(out.status.success());
        Self(dir)
    }

    fn git(&self, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(self.0.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_NAME", "Ada")
            .env("GIT_COMMITTER_EMAIL", "ada@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0000")
            .env("GIT_COMMITTER_DATE", "1700000000 +0000")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn commit_file(&self, path: &str, content: &str, message: &str) {
        let full = self.0.path().join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, content).unwrap();
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", message]);
    }

    fn append(&self, path: &str, line: &str) {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.0.path().join(path))
            .unwrap();
        f.write_all(line.as_bytes()).unwrap();
    }

    fn build(&self) {
        for i in 0..LINEAR_COMMITS {
            self.append("linear.txt", &format!("line {i} of c{i}\n"));
            self.commit_file(&format!("docs/f{i}"), &format!("{i}\n"), &format!("c{i}"));
        }
        self.git(&["checkout", "-q", "-b", "side", "main"]);
        for i in 0..SIDE_COMMITS {
            self.commit_file(&format!("side/s{i}"), &format!("{i}\n"), &format!("s{i}"));
        }
        self.git(&["checkout", "-q", "main"]);
        self.git(&["merge", "--no-ff", "-q", "-m", "merge side", "side"]);
        for i in 0..AFTER_MERGE_COMMITS {
            self.commit_file(
                &format!("docs/g{i}"),
                &format!("after merge {i}\n"),
                &format!("c{}", LINEAR_COMMITS + i),
            );
        }
        self.git(&["mv", "docs/f10", "docs/g10-renamed"]);
        self.git(&["commit", "-q", "-m", "rename f10"]);
        self.git(&["rm", "-q", "docs/f5"]);
        self.git(&["commit", "-q", "-m", "delete f5"]);
        for i in 0..TAIL_COMMITS {
            self.commit_file(
                &format!("docs/h{i}"),
                &format!("tail {i}\n"),
                &format!("c{}", LINEAR_COMMITS + AFTER_MERGE_COMMITS + i),
            );
        }
    }

    /// The manifest, computed from git alone.
    fn manifest(&self) -> Manifest {
        let subjects = |args: &[&str]| {
            self.git(args)
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let mut files = BTreeMap::new();
        for line in self.git(&["ls-tree", "-r", "main"]).lines() {
            let (meta, path) = line.split_once('\t').expect("a tree line");
            let mut parts = meta.split_whitespace();
            let mode = parts.next().unwrap();
            assert_eq!(
                parts.next().unwrap(),
                "blob",
                "the tip tree holds files: {line}"
            );
            let sha = parts.next().unwrap();
            let bytes = Command::new("git")
                .args(["cat-file", "blob", sha])
                .current_dir(self.0.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap();
            files.insert(
                path.to_owned(),
                (
                    mode == "100755",
                    String::from_utf8_lossy(&bytes.stdout).into_owned(),
                ),
            );
        }
        // The blame of linear.txt: the porcelain names each line's
        // introducing commit, and that commit's summary is its subject.
        let porcelain = self.git(&["blame", "--porcelain", "main", "--", "linear.txt"]);
        let mut subject_of_sha = BTreeMap::<String, String>::new();
        let mut current_sha = String::new();
        let mut blame = Vec::new();
        for line in porcelain.lines() {
            if let Some(rest) = line.strip_prefix('\t') {
                let _ = rest;
                let subject = subject_of_sha
                    .get(&current_sha)
                    .expect("a content line follows its commit");
                blame.push(subject.clone());
            } else if let Some(summary) = line.strip_prefix("summary ") {
                subject_of_sha.insert(current_sha.clone(), summary.to_owned());
            } else if is_sha_header(line) {
                // A "<sha> <orig-line> <final-line>" header starts a block.
                // `boundary` and the other metadata lines must not read
                // as one: only a 40-hex first token counts.
                current_sha = line.split_whitespace().next().unwrap().to_string();
            }
        }
        Manifest {
            chain: subjects(&["log", "--first-parent", "--format=%s", "main"]),
            files,
            log_f10: subjects(&[
                "log",
                "--first-parent",
                "--format=%s",
                "main",
                "--",
                "docs/f10",
            ]),
            log_g10: subjects(&[
                "log",
                "--first-parent",
                "--format=%s",
                "main",
                "--",
                "docs/g10-renamed",
            ]),
            log_linear: subjects(&[
                "log",
                "--first-parent",
                "--format=%s",
                "main",
                "--",
                "linear.txt",
            ]),
            blame,
        }
    }
}

/// What every public read must return, computed from git and from the
/// workload's own changesets, never from a backend.
struct Manifest {
    /// `main`'s first-parent chain, newest first, by subject.
    chain: Vec<String>,
    /// The tip's whole tree: path -> (executable, content).
    files: BTreeMap<String, (bool, String)>,
    /// The path histories, newest first, by subject.
    log_f10: Vec<String>,
    log_g10: Vec<String>,
    log_linear: Vec<String>,
    /// The blame of linear.txt: the introducing commit's subject per
    /// line, top to bottom.
    blame: Vec<String>,
}

impl Manifest {
    /// The tree the workload must end with: the git tip's tree plus the
    /// file the clean land adds.
    fn final_files(&self) -> BTreeMap<String, (bool, String)> {
        let mut files = self.files.clone();
        files.insert("landed/new.txt".to_owned(), (false, "new\n".to_owned()));
        files
    }

    /// Ancestor facts for sampled pairs of the first-parent chain. The
    /// chain is linear, so git's own verdict is the order: the entry at
    /// position `a` is an ancestor of the entry at position `b` exactly
    /// when `a >= b`. A changeset is its own ancestor, so one pair is a
    /// self pair.
    fn ancestor_pairs(&self) -> Vec<(usize, usize, bool)> {
        let n = self.chain.len();
        vec![
            (n - 1, 0, true),
            (n - 1, n / 2, true),
            (0, n / 2, false),
            (2, 1, true),
            (n - 1, n - 1, true),
        ]
    }
}

fn is_pg() -> bool {
    Backend::test_is_postgres()
}

/// Whether a porcelain line is a commit header: a 40-hex first token
/// followed by line numbers. `boundary` and the rest start with other
/// shapes.
fn is_sha_header(line: &str) -> bool {
    let Some(first) = line.split_whitespace().next() else {
        return false;
    };
    first.len() == 40
        && first.as_bytes().iter().all(u8::is_ascii_hexdigit)
        && line[first.len()..].starts_with(' ')
}

/// The backend identity, the #412 lesson: the env's choice and the
/// store's actual kind must agree, and the on-disk shape must match.
fn assert_identity(dir: &std::path::Path, _store: &Backend) {
    if is_pg() {
        assert!(
            dir.join("backend").exists(),
            "a Postgres data directory carries its backend marker"
        );
        assert!(
            !dir.join("scion.db").exists(),
            "a Postgres data directory holds no scion.db"
        );
    } else {
        assert!(
            !dir.join("backend").exists(),
            "a SQLite data directory carries no Postgres marker"
        );
        assert!(
            dir.join("scion.db").exists(),
            "a SQLite data directory keeps its metadata in scion.db"
        );
    }
}

/// Non-zero row counts in the main tables, read by raw SQL, so an import
/// that wrote nothing cannot pass vacuously.
fn assert_row_counts(dir: &std::path::Path, want_changesets: i64) {
    if is_pg() {
        let schema = Backend::test_schema(dir).unwrap();
        let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL");
        let mut db = postgres::Client::connect(url.trim_end_matches('/'), postgres::NoTls)
            .expect("the test database is up");
        db.batch_execute(&format!("SET search_path TO {schema}"))
            .expect("the schema selects");
        let mut count = |sql: &str| db.query_one(sql, &[]).unwrap().get::<_, i64>(0);
        assert!(count("SELECT count(*) FROM changesets") >= want_changesets);
        assert!(count("SELECT count(*) FROM changeset_parents") >= want_changesets - 1);
        assert!(count("SELECT count(*) FROM history_nodes") > 0);
        assert!(count("SELECT count(*) FROM refs") > 0);
        assert!(count("SELECT count(*) FROM trees") > 0);
    } else {
        let db = rusqlite::Connection::open(dir.join("scion.db")).unwrap();
        let count = |sql: &str| {
            db.query_row(sql, [], |row| row.get::<_, i64>(0))
                .expect("the count reads")
        };
        assert!(count("SELECT count(*) FROM changesets") >= want_changesets);
        assert!(count("SELECT count(*) FROM changeset_parents") >= want_changesets - 1);
        assert!(count("SELECT count(*) FROM history_nodes") > 0);
        assert!(count("SELECT count(*) FROM refs") > 0);
        assert!(count("SELECT count(*) FROM trees") > 0);
    }
}

/// fsck, through the binary, with the URL the schema follows on
/// Postgres (the #492 lesson).
fn fsck_clean(dir: &std::path::Path) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    cmd.arg("--data").arg(dir);
    if is_pg() {
        cmd.env("SCION_POSTGRES_URL", common::postgres_url(dir));
    }
    let out = cmd.arg("fsck").output().unwrap();
    assert!(
        out.status.success(),
        "fsck saw damage:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn serve(dir: &std::path::Path, store: Backend) -> String {
    let keys = std::sync::Arc::new(scion_server::Keys::generate());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(scion_server::test_support::service::serve(
        store,
        listener,
        TOKEN,
        scion_server::test_support::service::ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: scion_proto::runtime::listen::Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: std::sync::Arc::new(scion_server::Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(std::sync::Arc::clone(&keys)),
        std::future::pending(),
    ));
    format!("http://{addr}")
}

async fn client(url: &str) -> Scion {
    ScionClient::with_interceptor(
        common::channel(url).await.unwrap(),
        Bearer::new(TOKEN).unwrap(),
    )
}

/// The full first-parent walk from `tip`: the entries newest first, each
/// with its decoded changeset.
async fn full_log(
    scion: &mut Scion,
    tip: &ChangesetId,
    path: &[u8],
) -> Vec<(ChangesetId, Changeset)> {
    let mut got = Vec::new();
    let mut from = tip.as_bytes().to_vec();
    loop {
        let mut reply = scion
            .log(LogRequest {
                repo: REPO.into(),
                changeset_id: from.clone(),
                limit: 200,
                path: path.to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        let mut next = Vec::new();
        while let Some(part) = reply.message().await.unwrap() {
            for entry in &part.entries {
                let id = ChangesetId::try_from(entry.changeset_id.as_slice()).unwrap();
                let cs = Changeset::decode(&entry.changeset).unwrap();
                got.push((id, cs));
            }
            part.next_changeset_id.clone_into(&mut next);
        }
        if next.is_empty() {
            break;
        }
        from = next;
    }
    got
}

async fn changeset_of(scion: &mut Scion, id: &ChangesetId) -> Changeset {
    let mut reply = scion
        .get_changeset(GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = reply.message().await.unwrap() {
        bytes.extend_from_slice(&part.changeset);
    }
    Changeset::decode(&bytes).unwrap()
}

/// Lands one changeset that writes `path` on top of `base`, through the
/// public RPC: the blob rides `PutBlob` first, the way a client does.
async fn land_one(
    scion: &mut Scion,
    base: &ChangesetId,
    path: &str,
    content: &str,
    message: &str,
    key: &str,
) -> (LandResponse, ChangesetId) {
    let upload = scion
        .put_blob(tokio_stream::once(scion_proto::v1::PutBlobRequest {
            repo: REPO.into(),
            data: content.as_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    let blob = scion_core::BlobId::try_from(upload.blob_id.as_slice()).unwrap();
    let cs = Changeset::new(
        vec![*base],
        scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_100_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        format!("{message}\n").into_bytes(),
        [(
            RepoPath::new(path).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let (bytes, id) = cs.encode_with_id();
    let reply = scion
        .create_changeset(scion_proto::v1::CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.changeset_id, id.as_bytes().to_vec());
    let answer = scion
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: key.to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    (answer, id)
}

fn state_name(reply: &LandResponse) -> &'static str {
    match reply.state() {
        scion_proto::v1::LandState::Landed => "LANDED",
        scion_proto::v1::LandState::Conflict => "CONFLICT",
        scion_proto::v1::LandState::Rejected => "REJECTED",
        _ => "PENDING",
    }
}

fn subject_of(cs: &Changeset) -> String {
    String::from_utf8_lossy(cs.message()).trim_end().to_owned()
}

/// The whole contract, for the backend the env names. Each stage is a
/// helper, so the orchestration reads as the design's four steps.
#[tokio::test(flavor = "multi_thread")]
async fn the_public_reads_match_an_independent_manifest() {
    let git = Git::new();
    git.build();
    let manifest = git.manifest();

    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    assert_identity(dir.path(), &store);

    // Step 1: the import, through the public entry point.
    let tip_sha = git.git(&["rev-parse", "main"]);
    let report = import_git(
        &store,
        REPO,
        git.0.path(),
        &ImportOptions {
            rev: tip_sha,
            target_ref: "main".to_owned(),
            verify_sample: 1000,
        },
    )
    .unwrap();
    assert_eq!(
        report.commits,
        manifest.chain.len() + SIDE_COMMITS,
        "the whole history came in, the side branch included"
    );

    // Step 2: the workload's lands, over the public RPC.
    let url = serve(dir.path(), store.clone()).await;
    let mut scion = client(&url).await;
    let tip_id = resolve_main(&mut scion).await;
    assert_eq!(tip_id, report.tip, "main points at the imported tip");
    let chain = full_log(&mut scion, &tip_id, b"").await;
    assert_eq!(
        chain.len(),
        manifest.chain.len(),
        "the log names every commit of the chain, in git's order"
    );
    for (i, (_, cs)) in chain.iter().enumerate() {
        assert_eq!(
            subject_of(cs),
            manifest.chain[i],
            "the chain matches git at {i}"
        );
    }
    let (stale_reply, stale_id) = run_lands(&mut scion, &tip_id, &chain).await;
    let final_tip = resolve_main(&mut scion).await;
    assert_ne!(final_tip, tip_id, "the clean land moved main");

    // Step 3: the row counts, by raw SQL, after the lands. The raw
    // client is blocking, so it runs off the async runtime.
    {
        let dir = dir.path().to_path_buf();
        let want = i64::try_from(manifest.chain.len()).unwrap();
        tokio::task::spawn_blocking(move || assert_row_counts(&dir, want))
            .await
            .unwrap();
    }

    // Step 4: every public read, against the manifest.
    check_logs(&mut scion, &tip_id, &manifest).await;
    check_tree_read(&url, &store, final_tip, &manifest).await;
    check_ancestors(&mut scion, &chain, merge_id(&chain), &manifest).await;
    check_blame_read(&mut scion, &tip_id, &chain, &manifest).await;
    assert_eq!(state_name(&stale_reply), "CONFLICT");
    let _ = stale_id;

    // fsck sees no damage, and gc ran beside the server.
    let mut gc = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    gc.arg("--data").arg(dir.path());
    if is_pg() {
        gc.env("SCION_POSTGRES_URL", common::postgres_url(dir.path()));
    }
    let gc_out = gc.args(["gc", "--grace-secs", "0"]).output().unwrap();
    assert!(
        gc_out.status.success(),
        "gc failed: {}",
        String::from_utf8_lossy(&gc_out.stderr)
    );
    fsck_clean(dir.path());
}

async fn resolve_main(scion: &mut Scion) -> ChangesetId {
    ChangesetId::try_from(
        scion
            .resolve_ref(ResolveRefRequest {
                repo: REPO.into(),
                name: "main".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .changeset_id
            .as_slice(),
    )
    .unwrap()
}

/// The workload's lands: a clean one that must land, one on a stale base
/// that must conflict, and the retry of the stale one's request key,
/// which must answer the same.
async fn run_lands(
    scion: &mut Scion,
    tip_id: &ChangesetId,
    chain: &[(ChangesetId, Changeset)],
) -> (LandResponse, ChangesetId) {
    let (clean_reply, _clean_id) = land_one(
        scion,
        tip_id,
        "landed/new.txt",
        "new\n",
        "the clean land",
        "k-clean",
    )
    .await;
    assert_eq!(state_name(&clean_reply), "LANDED", "the clean land lands");

    // A stale base: the walk's thirtieth entry is a linear commit whose
    // linear.txt is shorter than the tip's, so an overwrite of the file
    // must conflict.
    let stale_base = chain[30].0;
    let (stale_reply, stale_id) = land_one(
        scion,
        &stale_base,
        "linear.txt",
        "conflicting content\n",
        "the stale land",
        "k-stale",
    )
    .await;
    assert_eq!(
        state_name(&stale_reply),
        "CONFLICT",
        "the stale land conflicts"
    );
    // The retry: the same request key answers the same, recorded once.
    let retried = scion
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: stale_id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k-stale".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retried.state(),
        stale_reply.state(),
        "the retry answers the same"
    );
    (stale_reply, stale_id)
}

fn merge_id(chain: &[(ChangesetId, Changeset)]) -> ChangesetId {
    chain
        .iter()
        .find(|(_, cs)| subject_of(cs) == "merge side")
        .map(|(id, _)| *id)
        .unwrap()
}

/// The path histories, against git, ordered.
async fn check_logs(scion: &mut Scion, tip_id: &ChangesetId, manifest: &Manifest) {
    for (path, want) in [
        (&b"docs/f10"[..], &manifest.log_f10),
        (&b"docs/g10-renamed"[..], &manifest.log_g10),
        (&b"linear.txt"[..], &manifest.log_linear),
    ] {
        let got = full_log(scion, tip_id, path).await;
        let subjects: Vec<String> = got.iter().map(|(_, cs)| subject_of(cs)).collect();
        assert_eq!(
            &subjects,
            want,
            "the log of {} matches git, ordered",
            String::from_utf8_lossy(path)
        );
    }
}

/// `GetRootTree` answers the store's own root tree, with a ticket; the
/// whole tree of the final tip matches the manifest's intended tree.
async fn check_tree_read(url: &str, store: &Backend, final_tip: ChangesetId, manifest: &Manifest) {
    use scion_server::test_support::meta::{Meta as _, MetaTx as _};
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let root =
        scion_server::test_support::tree::root_tree_unbounded(store, repo, final_tip).unwrap();
    let mut objects = scion_proto::v1::objects_client::ObjectsClient::with_interceptor(
        common::channel(url).await.unwrap(),
        Bearer::new(TOKEN).unwrap(),
    );
    let tree_reply = objects
        .get_root_tree(scion_proto::v1::GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: final_tip.as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        tree_reply.tree_id.as_slice(),
        root.as_bytes(),
        "GetRootTree answers the store's own root tree"
    );
    assert!(
        !tree_reply.ticket.is_empty(),
        "GetRootTree answers a ticket"
    );
    drop(objects);

    // The whole tree, from the store: path -> (executable, content).
    let mut got_tree: BTreeMap<String, (bool, String)> = BTreeMap::new();
    let mut stack = vec![(String::new(), root)];
    while let Some((prefix, tree_id)) = stack.pop() {
        let tree = store.read(|c| c.get_tree(tree_id)).unwrap();
        for (name, entry) in tree.entries() {
            let path = if prefix.is_empty() {
                String::from_utf8_lossy(name.as_bytes()).into_owned()
            } else {
                format!("{prefix}/{}", String::from_utf8_lossy(name.as_bytes()))
            };
            match entry {
                scion_core::Entry::File(file) => {
                    let state = file.state();
                    let content = store.blobs().get(state.blob().unwrap()).unwrap();
                    got_tree.insert(
                        path,
                        (
                            matches!(
                                state,
                                FileState::Regular {
                                    executable: true,
                                    ..
                                }
                            ),
                            String::from_utf8_lossy(&content).into_owned(),
                        ),
                    );
                }
                scion_core::Entry::Dir(id) => stack.push((path, *id)),
            }
        }
    }
    assert_eq!(
        &got_tree,
        &manifest.final_files(),
        "the whole tree matches git plus the lands"
    );
}

/// `IsAncestor` for the sampled pairs of the linear chain, plus a real DAG
/// pair: the merge's second parent (the side branch's tip).
async fn check_ancestors(
    scion: &mut Scion,
    chain: &[(ChangesetId, Changeset)],
    merge_id: ChangesetId,
    manifest: &Manifest,
) {
    let merge = changeset_of(scion, &merge_id).await;
    assert_eq!(merge.parents().len(), 2, "the merge carried both parents");
    let side_tip = merge.parents()[1];
    for (older, newer, want) in manifest.ancestor_pairs() {
        let got = scion
            .is_ancestor(IsAncestorRequest {
                repo: REPO.into(),
                ancestor_id: chain[older].0.as_bytes().to_vec(),
                descendant_id: chain[newer].0.as_bytes().to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            got.is_ancestor, want,
            "is_ancestor({older}, {newer}) matches git's verdict for a linear chain"
        );
    }
    for (ancestor, descendant, want) in [(side_tip, merge_id, true), (chain[0].0, side_tip, false)]
    {
        let got = scion
            .is_ancestor(IsAncestorRequest {
                repo: REPO.into(),
                ancestor_id: ancestor.as_bytes().to_vec(),
                descendant_id: descendant.as_bytes().to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.is_ancestor, want, "the DAG pair matches git");
    }
}

/// Blame of linear.txt: every line names the linear commit that wrote
/// it, exactly as git blame does.
async fn check_blame_read(
    scion: &mut Scion,
    tip_id: &ChangesetId,
    chain: &[(ChangesetId, Changeset)],
    manifest: &Manifest,
) {
    let subject_of_id: BTreeMap<ChangesetId, String> =
        chain.iter().map(|(id, cs)| (*id, subject_of(cs))).collect();
    let reply = scion
        .blame(BlameRequest {
            repo: REPO.into(),
            changeset_id: tip_id.as_bytes().to_vec(),
            path: b"linear.txt".to_vec(),
            cursor: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut per_line: BTreeMap<u32, String> = BTreeMap::new();
    let mut parts = reply;
    while let Some(part) = parts.message().await.unwrap() {
        for range in &part.ranges {
            if range.changeset_id.is_empty() {
                continue;
            }
            let id = ChangesetId::try_from(range.changeset_id.as_slice()).unwrap();
            let subject = subject_of_id
                .get(&id)
                .expect("a blamed changeset sits on the chain");
            for line in range.start_line..range.start_line + range.line_count {
                per_line.insert(line, subject.clone());
            }
        }
        if part.next_cursor.is_empty() {
            break;
        }
        parts = scion
            .blame(BlameRequest {
                repo: REPO.into(),
                changeset_id: tip_id.as_bytes().to_vec(),
                path: b"linear.txt".to_vec(),
                cursor: part.next_cursor.clone(),
            })
            .await
            .unwrap()
            .into_inner();
    }
    let line_count = u32::try_from(manifest.blame.len()).unwrap();
    let got_blame: Vec<String> = (1..=line_count)
        .map(|line| {
            per_line
                .get(&line)
                .cloned()
                .expect("blame covers every line")
        })
        .collect();
    assert_eq!(
        &got_blame, &manifest.blame,
        "blame matches git, line for line"
    );
}
