//! A blame cursor arrives from the client with every resumed call. Any
//! input must end in a parsed cursor or an error, never a panic; and the
//! format round-trips structurally: what the decoder read encodes and
//! decodes again to the same cursor. The encoder is canonical, but a
//! mutated input need not be, so the property compares the two decodes,
//! not the bytes.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::blame::{self, Resume};
use scion_server::test_support::memory::Bytes;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::{store::Store, tree}};
use std::sync::OnceLock;
use tempfile::TempDir;

const REPO: &str = "f";
const PATH: &str = "f";

/// The store the whole run shares: one SQLite file in one temp directory,
/// with a chain of three changesets that each put `f`. Built once per
/// process, because SQLite setup would otherwise eat the fuzzing time.
/// The directory stays in the static for the life of the process: keeping
/// it reachable keeps LeakSanitizer quiet and its cleanup never runs
/// while the fuzzer runs.
fn fixture() -> &'static (TempDir, Backend, ChangesetId) {
    static FIXTURE: OnceLock<(TempDir, Backend, ChangesetId)> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Backend::Sqlite(Store::open(dir.path()).expect("store"));
        let tip = store
            .write(|c| {
                let repo = c.create_repo(REPO)?;
                let mut ids: Vec<ChangesetId> = Vec::new();
                for (n, line) in [b"a\n".as_slice(), b"b\n", b"c\n"].iter().enumerate() {
                    let content = store.blobs().put(line).expect("blob");
                    let len = line.len() as u64;
                    let cs = Changeset {
                        parents: ids.last().copied().into_iter().collect(),
                        author: IdentityTime {
                            name: b"Ada".to_vec(),
                            email: b"ada@example.com".to_vec(),
                            unix_secs: 1_700_000_000 + n as i64,
                            tz_offset_minutes: 0,
                        },
                        committer: None,
                        message: format!("change {n}").into_bytes(),
                        changes: [(
                            RepoPath::new(PATH).unwrap(),
                            Some(FileState::Regular {
                                content,
                                executable: false,
                            }),
                        )]
                        .into_iter()
                        .collect(),
                    };
                    ids.push(tree::store_changeset(c, repo, &cs, &mut move |b| {
                        if b == content {
                            Ok(len)
                        } else {
                            Err(scion_server::Error::NotFound(format!("blob {b}")))
                        }
                    })?
                    .0);
                }
                Ok(ids)
            })
            .expect("the chain stores")
            .pop()
            .expect("three changesets");
        (dir, store, tip)
    })
}

fuzz_target!(|data: &[u8]| {
    let (_dir, store, tip) = fixture();
    let tip = *tip;
    let path = RepoPath::new(PATH).expect("a valid path");

    // The header (version and digest) of a real cursor, so the mutations
    // reach the fields instead of dying at the digest check.
    let base = blame::encode_cursor(&Resume::default(), REPO, tip, &path);
    let head = base.len().min(1 + 16);
    let mut bytes = base[..head].to_vec();
    bytes.extend_from_slice(data);

    let memory = Bytes::new(1 << 30);
    if let Ok((resume, _hold)) =
        blame::decode_cursor(store, REPO, &bytes, tip, &path, &memory, None)
    {
        // What the decoder read encodes and decodes again to the same
        // cursor: one round of encode+decode lands on a fixed point.
        let re = blame::encode_cursor(&resume, REPO, tip, &path);
        let again = blame::decode_cursor(store, REPO, &re, tip, &path, &memory, None)
            .expect("an encoded cursor decodes");
        assert_eq!(resume, again.0, "the cursor round-trips");
    }
});
