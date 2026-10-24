//! The exit codes `scn` promises in 1.x, as one table: 0 on success, 1
//! only for `is-ancestor`'s "no", 2 on any error. Every row of the
//! table is a line here, so a release that changes a code fails this
//! test before it ships (docs/stability.md, 1.x).

#![cfg(unix)]

use std::process::{Command, Output};

use scion_core::{Changeset, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::listen::Limits;
use scion_server::Quotas;
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::service;
use std::sync::Arc;
use tokio::net::TcpListener;

const TOKEN: &str = "secret";

/// The `scn` exit-code table.
#[test]
fn the_exit_codes_of_scn() {
    // 0: a success. `--help` needs no server.
    assert_eq!(
        scn(&["--help"]).status.code(),
        Some(0),
        "scn --help exits 0"
    );

    // 2: any error. An unknown flag is a command-line error; a command
    // that cannot reach its server is an error too, and both must read
    // as 2, never as the "no" of is-ancestor.
    let bad_flag = scn(&["--no-such-flag"]);
    assert_eq!(bad_flag.status.code(), Some(2), "an unknown flag exits 2");
    let dead = scn(&["--server", "http://127.0.0.1:1", "log"]);
    assert_eq!(
        dead.status.code(),
        Some(2),
        "a command that cannot reach the server exits 2"
    );

    // 1: only the "no" of `is-ancestor`.
    let (url, root, other, _runtime) = server_with_two_changesets();
    let (url, root, other) = (url.as_str(), root.as_str(), other.as_str());
    let yes = scn(&["--server", url, "--repo", "r", "is-ancestor", root, root]);
    assert_eq!(yes.status.code(), Some(0), "is-ancestor of itself is yes");
    let no = scn(&["--server", url, "--repo", "r", "is-ancestor", other, root]);
    // An unrelated changeset is not an ancestor: the "no" of the table.
    assert_eq!(no.status.code(), Some(1), "is-ancestor's no exits 1");
}

/// Runs the `scn` binary with `args` and a token, the way the CLI tests
/// do.
fn scn(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap()
}

/// A server with repo `r`, one root changeset on `main`, and a second
/// changeset on no ref: the two ends of `is-ancestor`'s table. Returns
/// the URL, the two IDs in hex, and the keep-alive runtime.
fn server_with_two_changesets() -> (String, String, String, tokio::runtime::Runtime) {
    let dir = tempfile::tempdir().unwrap();
    let store = scion_server::test_support::store::Store::open(dir.path()).unwrap();
    let blob = store.blobs().put(b"exit codes\n").unwrap();
    let root = Changeset::new(
        vec![],
        IdentityTime::new(b"e".to_vec(), b"e@x".to_vec(), 0, 0).expect("identity fits"),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let (_, root_id) = root.encode_with_id();
    let root_hex = root_id.to_string();
    let other = Changeset::new(
        vec![],
        IdentityTime::new(b"f".to_vec(), b"f@x".to_vec(), 0, 0).expect("identity fits"),
        None,
        b"unrelated\n".to_vec(),
        std::collections::BTreeMap::new(),
    )
    .expect("changeset fits the format");
    let (_, other_id) = other.encode_with_id();
    let other_hex = other_id.to_string();
    store
        .write(|c| {
            let repo = c.create_repo("r")?;
            c.insert_changeset(repo, &root)?;
            c.insert_changeset(repo, &other)?;
            assert!(c.cas_ref(repo, "main", None, root_id)?);
            Ok(())
        })
        .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    runtime.spawn(service::serve(
        from_store(store),
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    // The data directory must outlive the server: the temp dir leaks
    // for the length of the test process, and the runtime's drop at the
    // test's end stops the server first.
    std::mem::forget(dir);
    (url, root_hex, other_hex, runtime)
}
