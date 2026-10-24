//! scn's JSON keeps non-UTF-8 bytes: a name or a message that is not
//! valid UTF-8 prints its lossy text in the field a script already
//! reads, with the exact bytes beside it in a sibling `<field>_b64`
//! (standard base64, with padding), so a script gets the real bytes
//! back and two different names never print as one. A value that is
//! valid UTF-8 prints exactly as before, with no sibling field — the
//! golden (`json_golden.rs`) pins that shape.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;

use base64::Engine as _;
use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::listen::Limits;
use scion_server::test_support::meta::RepoId;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::service;
use scion_server::{Backend, Quotas};
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "secret";
const REPO: &str = "r";

/// The bytes of the fixture: a name, an author and a message whose bytes
/// are not valid UTF-8 (a `0x80` byte), and one name that is plain.
const RAW_NAME: &[u8] = b"raw\xff.txt";
const RAW_MESSAGE: &[u8] = b"a \xff message";
const RAW_AUTHOR: &[u8] = b"Ad\xffy";
const PLAIN_NAME: &[u8] = b"plain.txt";
/// A name whose base64 encoding needs padding: 13 bytes are 4 groups of
/// 3 minus 2, so the encoding is 20 characters ending in one `=`.
const PADDED_NAME: &[u8] = b"thirteen-byt\x80";

struct Fixture {
    url: String,
    /// A read handle of the store the server serves, for the test's own
    /// lookups of the fixture's ids.
    store: Backend,
    repo: scion_server::test_support::meta::RepoId,
    home: TempDir,
    /// The changeset whose landing conflicts with `main`'s move of the
    /// raw path.
    clash: ChangesetId,
    /// The server runs on this runtime; dropped, the server stops.
    _runtime: tokio::runtime::Runtime,
}

/// Seeds the store: the root, the tip with the raw name/author/message,
/// the rename, and the conflicting land, with `main` at the rename and
/// `wip` at the tip.
fn seed(
    store: &Backend,
) -> (
    scion_server::test_support::meta::RepoId,
    ChangesetId,
    ChangesetId,
) {
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let plain_blob = store.blobs().put(b"plain\n").unwrap();
    let root = store
        .write(|c| {
            c.insert_changeset(
                repo,
                &changeset(
                    b"root",
                    BTreeMap::from([(RepoPath::new(PLAIN_NAME).unwrap(), plain_blob)]),
                ),
            )
        })
        .unwrap();
    let tip = seed_tip(store, repo, root);
    let moved = seed_rename(store, repo, tip);
    let clash = seed_clash(store, repo, tip);
    // `main` names the rename; `wip` names the tip, so the ls, show and
    // log reads see the raw name, message and author.
    store
        .write(|c| c.cas_ref(repo, "main", None, moved))
        .unwrap();
    store.write(|c| c.cas_ref(repo, "wip", None, tip)).unwrap();
    (repo, tip, clash)
}

/// The tip: the raw name and the padded name, with the raw author and
/// message.
fn seed_tip(store: &Backend, repo: RepoId, root: ChangesetId) -> ChangesetId {
    let raw_blob = store.blobs().put(b"raw\n").unwrap();
    let pad_blob = store.blobs().put(b"pad\n").unwrap();
    store
        .write(|c| {
            c.insert_changeset(
                repo,
                &Changeset::new(
                    vec![root],
                    IdentityTime::new(
                        RAW_AUTHOR.to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .unwrap(),
                    None,
                    RAW_MESSAGE.to_vec(),
                    BTreeMap::from([
                        (
                            RepoPath::new(RAW_NAME).unwrap(),
                            Some(FileState::Regular {
                                content: raw_blob,
                                executable: false,
                            }),
                        ),
                        (
                            RepoPath::new(PADDED_NAME).unwrap(),
                            Some(FileState::Regular {
                                content: pad_blob,
                                executable: false,
                            }),
                        ),
                    ]),
                )
                .unwrap(),
            )
        })
        .unwrap()
}

/// The rename of the raw file to a plain one, same bytes: the diff
/// reports it with `old_path` = the raw bytes. `main` names it, which
/// also makes the clash conflict: the ref moves the raw path after it.
fn seed_rename(store: &Backend, repo: RepoId, tip: ChangesetId) -> ChangesetId {
    let moved_blob = store.blobs().put(b"raw\n").unwrap();
    store
        .write(|c| {
            c.insert_changeset(
                repo,
                &Changeset::new(
                    vec![tip],
                    IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_100,
                        0,
                    )
                    .unwrap(),
                    None,
                    b"the rename\n".to_vec(),
                    BTreeMap::from([
                        (RepoPath::new(RAW_NAME).unwrap(), None),
                        (
                            RepoPath::new(b"moved.txt").unwrap(),
                            Some(FileState::Regular {
                                content: moved_blob,
                                executable: false,
                            }),
                        ),
                    ]),
                )
                .unwrap(),
            )
        })
        .unwrap()
}

/// The conflicting land: off the tip, touching the raw path again.
fn seed_clash(store: &Backend, repo: RepoId, tip: ChangesetId) -> ChangesetId {
    let clash_blob = store.blobs().put(b"clash\n").unwrap();
    store
        .write(|c| {
            c.insert_changeset(
                repo,
                &Changeset::new(
                    vec![tip],
                    IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_200,
                        0,
                    )
                    .unwrap(),
                    None,
                    b"the clash\n".to_vec(),
                    BTreeMap::from([(
                        RepoPath::new(RAW_NAME).unwrap(),
                        Some(FileState::Regular {
                            content: clash_blob,
                            executable: false,
                        }),
                    )]),
                )
                .unwrap(),
            )
        })
        .unwrap()
}

impl Fixture {
    /// A store whose `main` names one changeset that puts the plain name
    /// and the raw names (the tree `ls` reads), with the raw author and
    /// message.
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        let (repo, _tip, clash) = seed(&store);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let read_handle = store.clone();
        runtime.spawn(service::serve(
            store,
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
        // The data directory must outlive the server, as json_golden's
        // fixture does.
        std::mem::forget(dir);
        let home = tempfile::tempdir().unwrap();
        Self {
            url,
            store: read_handle,
            repo,
            home,
            _runtime: runtime,
            clash,
        }
    }

    fn json(&self, args: &[&str]) -> serde_json::Value {
        let out = self.scn(args);
        assert!(
            out.status.success(),
            "scn {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_str(&String::from_utf8(out.stdout).unwrap()).unwrap()
    }

    /// The changeset whose message holds the raw bytes.
    fn tip(&self) -> ChangesetId {
        let ids = self
            .store
            .read(|c| c.repo_changeset_ids(self.repo))
            .unwrap();
        for id in &ids {
            let bodies = self
                .store
                .read(|c| c.stored_changeset_bodies(std::slice::from_ref(id)))
                .unwrap();
            let (id, body) = &bodies[0];
            if body.windows(RAW_MESSAGE.len()).any(|w| w == RAW_MESSAGE) {
                return ChangesetId::try_from(id.as_slice()).unwrap();
            }
        }
        panic!("no changeset with the raw message");
    }

    fn scn(&self, args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", REPO])
            .args(args)
            .env("SCION_TOKEN", TOKEN)
            .env("HOME", self.home.path())
            .output()
            .unwrap()
    }
}

/// A changeset on no parent that puts `name` -> `blob`.
fn changeset(message: &[u8], puts: BTreeMap<RepoPath, BlobId>) -> Changeset {
    Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .unwrap(),
        None,
        message.to_vec(),
        puts.into_iter()
            .map(|(name, blob)| {
                (
                    name,
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )
            })
            .collect(),
    )
    .unwrap()
}

/// The base64 of `bytes`, standard alphabet with padding.
fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// The sibling `<field>_b64` of `value` holds the exact bytes, decodes to
/// them, and the text field is the lossy form of the same bytes.
fn assert_b64(value: &serde_json::Value, field: &str, want: &[u8]) {
    let got = value[format!("{field}_b64")]
        .as_str()
        .unwrap_or_else(|| panic!("{field}_b64 missing: {value}"));
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(got)
            .unwrap(),
        want,
        "{field}_b64 does not decode to the bytes"
    );
    let text = value[field].as_str().unwrap();
    assert_eq!(
        text.as_bytes(),
        String::from_utf8_lossy(want).as_bytes(),
        "{field} is the lossy form of the bytes"
    );
}

#[test]
fn a_non_utf8_name_and_message_round_trip_through_the_json() {
    let fx = Fixture::start();

    // ls: the tree holds the raw names beside the plain one.
    let ls = fx.json(&["ls", "--json", "wip"]);
    let entries = ls.as_array().unwrap();
    let raw = entries
        .iter()
        .find(|e| e["name"].as_str() == Some(String::from_utf8_lossy(RAW_NAME).as_ref()))
        .expect("the raw name is in the listing");
    assert_b64(raw, "name", RAW_NAME);
    let padded = entries
        .iter()
        .find(|e| e["name"].as_str() == Some(String::from_utf8_lossy(PADDED_NAME).as_ref()))
        .expect("the padded name is in the listing");
    assert_b64(padded, "name", PADDED_NAME);
    // The padding case: 13 bytes encode to 20 characters ending in
    // exactly one `=`, and decode byte-exact.
    let encoded = padded["name_b64"].as_str().unwrap();
    assert_eq!(encoded.len() % 4, 0, "standard base64 with padding");
    assert!(encoded.ends_with('='));
    assert_eq!(b64(PADDED_NAME), encoded);

    // show: the message, the change's path and the author's name.
    let show = fx.json(&["show", "--json", "wip"]);
    assert_b64(&show, "message", RAW_MESSAGE);
    let changes = show["changes"].as_array().unwrap();
    let raw_change = changes
        .iter()
        .find(|c| c["path"].as_str() == Some(String::from_utf8_lossy(RAW_NAME).as_ref()))
        .expect("the raw path is in the changes");
    assert_b64(raw_change, "path", RAW_NAME);
    assert_b64(&show["author"], "name", RAW_AUTHOR);

    // log: the same author and message through the history view.
    let log = fx.json(&["log", "--json", "wip"]);
    let tip = log
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["message"].as_str() == Some(String::from_utf8_lossy(RAW_MESSAGE).as_ref()))
        .expect("the tip is in the log");
    assert_b64(&tip["author"], "name", RAW_AUTHOR);
    assert_b64(tip, "message", RAW_MESSAGE);
}

#[test]
fn a_valid_utf8_value_prints_exactly_as_before() {
    // The golden (json_golden.rs) pins the whole shape for valid UTF-8;
    // here the one field: a plain name carries no sibling, and the text
    // is the name.
    let fx = Fixture::start();
    let ls = fx.json(&["ls", "--json", "wip"]);
    let plain = ls
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"].as_str() == Some("plain.txt"))
        .unwrap();
    assert_eq!(plain["name"], "plain.txt");
    assert!(plain.get("name_b64").is_none());
}

/// status: the diff of the raw file's rename carries `old_path` with the
/// raw bytes, and `path` with the new plain name.
#[test]
fn a_rename_of_a_raw_name_round_trips_through_the_diff() {
    let fx = Fixture::start();
    let tip = fx.tip();
    // The rename changeset is main; the diff of the tip against main
    // holds one rename: the raw name to moved.txt.
    let diff = fx.json(&["diff", "--json", &tip.to_string(), "main"]);
    let renamed = diff
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"].as_str() == Some("moved.txt"))
        .expect("the rename is in the diff");
    // moved.txt is valid UTF-8: no sibling. old_path is not.
    assert!(renamed.get("path_b64").is_none());
    assert_b64(renamed, "old_path", RAW_NAME);
}

/// blame: the history of the moved file spans the raw name's changeset,
/// so a range's path and author carry the raw bytes.
#[test]
fn a_blame_over_a_raw_name_round_trips() {
    let fx = Fixture::start();
    let blame = fx.json(&["blame", "--json", "main:moved.txt"]);
    let raw_range = blame
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["path"].as_str() == Some(String::from_utf8_lossy(RAW_NAME).as_ref()))
        .expect("the raw name is in the blame");
    assert_b64(raw_range, "path", RAW_NAME);
    assert_b64(&raw_range["author"], "name", RAW_AUTHOR);
}

/// land: two changesets that touch the same raw path; `main` moves the
/// path between the second's base and its landing, so the land
/// conflicts, and `conflict_paths_b64` decodes to the raw bytes.
#[test]
fn a_land_conflict_names_the_raw_path_in_the_sibling_array() {
    let fx = Fixture::start();
    let out = fx.scn(&["land", "--json", &fx.clash.to_string()]);
    assert!(
        !out.status.success(),
        "the land conflicts: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let value: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out.stdout).unwrap()).unwrap();
    assert_eq!(value["state"], "LAND_STATE_CONFLICT");
    let paths = value["conflict_paths"].as_array().unwrap();
    assert_eq!(
        paths[0],
        String::from_utf8_lossy(RAW_NAME).as_ref(),
        "the conflict path is the lossy text"
    );
    let raw = value["conflict_paths_b64"]
        .as_array()
        .expect("the sibling array carries the bytes");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(raw[0].as_str().unwrap())
            .unwrap(),
        RAW_NAME
    );
}
