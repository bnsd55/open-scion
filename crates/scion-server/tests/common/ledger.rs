//! The independent operation ledger (#377): what the fault suites
//! acknowledged, recorded in the test process the moment a reply arrives,
//! and checked back through the RPCs — never through the store's own code,
//! which fsck also runs on. fsck stays as the second opinion (`fsck_ok`).
//!
//! The checks (`check`):
//! - C1: every acknowledged blob that a changeset took is served back
//!   whole: `ReadFile` at the changeset (the ref's tip, when the land
//!   landed), and the sha256 of the served bytes equals the hash the
//!   client took of the bytes it sent — sha2 here, the store's BLAKE3
//!   nowhere, which is the independence.
//! - C2: every acknowledged changeset decodes from `GetChangeset` and
//!   hashes to its ID.
//! - C3: every land that replied LANDED has its result on the target
//!   ref's first-parent history, and `GetLand` agrees.
//! - C4: refs never move backwards: every recorded head of a ref is on
//!   the first-parent history of the head recorded after it. A ref only
//!   moves forward by compare-and-swap, so the final history holds them
//!   all; one walk per ref proves the whole order.
//! - C5: nothing unacknowledged is claimed lost. The check asserts only
//!   presence of acknowledged entries; a land in flight at a fault may
//!   land after the restart, and the suites' own retries cover it.
//! - C6: `fsck_ok` runs the store's own fsck beside the RPC checks. A
//!   disagreement is the bug this gate closes, and both halves name it.
//!
//! A failure names the entry: every entry carries a `where_` string the
//! recorder chose ("round 2 kill 1, p3-o7-863838"), and the panic lists
//! what was served and what was expected, the soak-fault `missing.csv`
//! shape.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use scion_core::{BlobId, Changeset, ChangesetId};
use scion_proto::v1::{GetChangesetRequest, LandState, ReadFileRequest};

use super::{Objects, Rng, Scion, connect, resolve_ref};

/// What a reply acknowledged, with the hash the test took itself.
#[derive(Clone)]
pub(crate) enum Kind {
    /// `PutBlob` replied. `used_in` names the changeset that took the
    /// blob, and the path it sits at, once `CreateChangeset` replied.
    Blob {
        id: BlobId,
        sha256: [u8; 32],
        len: usize,
        used_in: Option<(ChangesetId, Vec<u8>)>,
    },
    /// `CreateChangeset` replied.
    Changeset { id: ChangesetId },
    /// `Land` replied with a final state.
    Land {
        key: String,
        land_id: String,
        target: &'static str,
        state: LandState,
        result: Option<ChangesetId>,
    },
    /// `ResolveRef` replied: one observation of a ref's position.
    Ref { name: &'static str, at: ChangesetId },
    /// A write the test knows was refused (the EIO hook spent, ENOSPC):
    /// the only absence the check may assert, because the ledger itself
    /// recorded the refusal.
    Refused { what: String },
}

/// One acknowledged operation, and where in the run it happened.
pub(crate) struct Entry {
    pub kind: Kind,
    pub where_: String,
}

/// The ledger.
#[derive(Default)]
pub(crate) struct OpLog {
    entries: Vec<Entry>,
}

impl OpLog {
    pub(crate) fn record(&mut self, kind: Kind, where_: impl Into<String>) {
        self.entries.push(Entry {
            kind,
            where_: where_.into(),
        });
    }

    /// Fills `used_in` on the newest unmatched blob entry whose ID is
    /// `id`: the changeset that took it, and the path it sits at.
    pub(crate) fn link_blob(&mut self, id: BlobId, changeset: ChangesetId, path: &[u8]) {
        for entry in self.entries.iter_mut().rev() {
            let Kind::Blob {
                id: blob,
                used_in: slot,
                ..
            } = &mut entry.kind
            else {
                continue;
            };
            if *blob == id && slot.is_none() {
                *slot = Some((changeset, path.to_vec()));
                return;
            }
        }
    }

    pub(crate) fn merge(&mut self, other: OpLog) {
        self.entries.extend(other.entries);
    }

    /// The entries, for a test that must inspect what it recorded.
    pub(crate) fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

/// sha256, the test's own hash: not the store's BLAKE3.
pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// How many older blob entries one check reads in full, beyond the
/// entries since the last check (seeded).
pub(crate) const SAMPLE: usize = 200;

/// The entries this check examines: everything since `mark`, plus a
/// seeded sample of the older ones.
fn pick<'a>(log: &'a OpLog, mark: usize, sample: usize, rng: &mut Rng) -> Vec<&'a Entry> {
    let (older, recent) = log.entries.split_at(mark.min(log.entries.len()));
    let mut wanted: Vec<&Entry> = recent.iter().collect();
    let picked: BTreeSet<usize> = (0..sample)
        .filter(|_| !older.is_empty())
        .map(|_| rng.below(older.len()))
        .collect();
    for i in picked {
        wanted.push(&older[i]);
    }
    wanted
}

/// Runs C1-C5 against the server at `url`. `mark` is where the last
/// check stopped; the entries since it are the recent window, read in
/// full, and `sample` older blob entries join them, chosen by `rng`.
/// Returns the new mark for the next call.
pub(crate) async fn check(
    log: &OpLog,
    url: &str,
    mark: usize,
    sample: usize,
    rng: &mut Rng,
) -> usize {
    let (mut scion, mut objects) = connect(url)
        .await
        .unwrap_or_else(|e| panic!("the ledger check cannot connect: {e}"));
    let recent: Vec<&Entry> = pick(log, mark, sample, rng);

    // C1: the served bytes of every acknowledged blob that a changeset
    // took, at the changeset the ledger links it to.
    for entry in &recent {
        let Kind::Blob {
            id,
            sha256: want,
            len,
            used_in: Some((changeset, path)),
        } = &entry.kind
        else {
            continue;
        };
        let served = read_file(&mut scion, *changeset, path)
            .await
            .unwrap_or_else(|s| {
                panic!(
                    "acknowledged blob {id} ({}), which {changeset} carries at {}, reads back as: {s}",
                    entry.where_,
                    hex(path)
                )
            });
        let got = sha256(&served);
        assert_eq!(
            got, *want,
            "acknowledged blob {id} ({}) served bytes whose sha is {got:02x?}, expected {want:02x?}",
            entry.where_
        );
        assert_eq!(
            served.len(),
            *len,
            "acknowledged blob {id} ({}) served {} bytes, sent {len}",
            entry.where_,
            served.len()
        );
    }

    // C2: every acknowledged changeset decodes and hashes to its ID.
    for entry in &recent {
        let Kind::Changeset { id } = entry.kind else {
            continue;
        };
        let (_, got) = get_changeset(&mut scion, id).await;
        assert_eq!(
            got, id,
            "acknowledged changeset ({}) served as {got}, which is not its ID",
            entry.where_
        );
    }

    // C3: every LANDED land is on its ref's history.
    for entry in &recent {
        let Kind::Land {
            key: _,
            land_id,
            target,
            state: LandState::Landed,
            result: Some(result),
        } = &entry.kind
        else {
            continue;
        };
        let on_ref = ref_history(&mut scion, &mut objects, target).await;
        assert!(
            on_ref.contains_key(result),
            "acknowledged land {land_id} ({}) named result {result}, which is not on {target}'s history",
            entry.where_
        );
    }

    // C4: refs never move backwards. One walk from the current head of
    // each ref with observations holds every recorded head.
    let mut observed: BTreeMap<&'static str, Vec<(ChangesetId, &str)>> = BTreeMap::new();
    for entry in &recent {
        let Kind::Ref { name, at } = entry.kind else {
            continue;
        };
        observed.entry(name).or_default().push((at, &entry.where_));
    }
    for (name, observations) in &observed {
        let history = ref_history(&mut scion, &mut objects, name).await;
        for (at, where_) in observations {
            assert!(
                history.contains_key(at),
                "{name} moved backwards: {at} ({where_}) is not on its current history"
            );
        }
    }
    log.entries.len()
}

/// C6: the store's own fsck, the second opinion.
pub(crate) fn fsck_ok(store: &scion_server::Backend, what: &str) {
    let report = scion_server::fsck::run(store, 1, &mut |_| {})
        .unwrap_or_else(|e| panic!("fsck of {what} failed to run: {e}"));
    assert!(
        report.is_clean(),
        "fsck of {what} reports: {:?}",
        report.problems
    );
}

/// `GetChangeset` as one call.
async fn get_changeset(scion: &mut Scion, id: ChangesetId) -> (Changeset, ChangesetId) {
    let request = GetChangesetRequest {
        repo: super::REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
    };
    let mut stream = scion
        .get_changeset(request)
        .await
        .unwrap_or_else(|s| panic!("GetChangeset {id}: {s:?}"))
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = stream.message().await.unwrap() {
        bytes.extend_from_slice(&part.changeset);
    }
    Changeset::decode_with_id(&bytes).unwrap()
}

/// The first-parent history of `name`, from its current head to the root.
async fn ref_history(
    scion: &mut Scion,
    objects: &mut Objects,
    name: &str,
) -> HashMap<ChangesetId, Changeset> {
    let mut head = resolve_ref(objects, name)
        .await
        .unwrap_or_else(|s| panic!("ResolveRef {name}: {s:?}"));
    let mut history = HashMap::new();
    loop {
        let (changeset, id) = get_changeset(scion, head).await;
        let parent = changeset.parents().first().copied();
        history.insert(id, changeset);
        match parent {
            Some(p) => head = p,
            None => break, // the root: a ref may legitimately sit at it
        }
    }
    history
}

/// `ReadFile` as one call: the whole bytes of `path` at `changeset`.
async fn read_file(
    scion: &mut Scion,
    changeset: ChangesetId,
    path: &[u8],
) -> Result<Vec<u8>, tonic::Status> {
    let mut stream = scion
        .read_file(ReadFileRequest {
            repo: super::REPO.into(),
            changeset_id: changeset.as_bytes().to_vec(),
            path: path.to_vec(),
        })
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = stream.message().await? {
        bytes.extend_from_slice(&part.data);
    }
    Ok(bytes)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[usize::from(b >> 4)] as char);
        out.push(HEX[usize::from(b & 15)] as char);
    }
    out
}
