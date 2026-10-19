//! Conformance: the server, a scion-cache in front of it and a scion-daemon
//! in front of the cache give the same answer to the same `Objects` call,
//! cold and warm. A tier serves only `Objects` (`ResolveRef`, `GetRootTree`
//! and `GetObjects`); the `Scion` service goes to the server.
//!
//! A client sees a `GetObjects` answer as the objects that it gets, asking
//! again after a reply that stops early, and the error that ends it. Each
//! object must follow the part rules and match its ID, so a tier that sends
//! another encoding is still the same answer, but one that sends other
//! bytes is not.
#![cfg(unix)]

use std::fs;
use std::path::Path;

use scion_core::{BlobId, ChangesetId, Entry, FileState, Tree, TreeId};
use scion_proto::MAX_OBJECT_REFS;
use scion_proto::runtime::object::{self, Assembler, Header};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef, ResolveRefRequest,
};
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::{Backend, test_support::tree};
use tonic::Code;

mod common;
use common::{Client, REPO, Tiers, blob_ref, changeset, connect, land, tree_ref};

/// What ends a `GetObjects` answer before its last object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    /// The status of a call.
    Call(Code),
    /// An error in the stream of a call.
    Stream(Code),
    /// An object that the client refuses: it breaks the part rules, or its
    /// bytes do not match its ID.
    Refused,
}

/// A `GetObjects` answer as a client sees it.
#[derive(Debug, PartialEq, Eq)]
struct Got {
    /// The objects, as kind and ID in hex, each checked against its ID.
    objects: Vec<(i32, String)>,
    end: Option<End>,
}

/// One call of the `Objects` service.
#[derive(Clone, Debug)]
enum Call {
    ResolveRef(ResolveRefRequest),
    GetRootTree(GetRootTreeRequest),
    GetObjects(GetObjectsRequest),
}

#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Id(Result<Vec<u8>, Code>),
    Objects(Got),
}

/// Whether `data` is the object `kind` and `id` name.
fn matches(kind: i32, id: &[u8], data: &[u8]) -> bool {
    match ObjectKind::try_from(kind) {
        Ok(ObjectKind::Tree) => {
            Tree::decode_with_id(data).is_ok_and(|(_, got)| got.as_bytes() == id)
        }
        Ok(ObjectKind::Blob) => BlobId::of(data).as_bytes() == id,
        _ => false,
    }
}

/// `GetObjects` as a client does it: asks again for the objects that a reply
/// did not send, and checks each object. Also returns the bytes of each
/// object.
async fn get_objects(client: &mut Client, request: &GetObjectsRequest) -> (Got, Vec<Vec<u8>>) {
    let zstd = request.accept.contains(&Encoding::Zstd.into());
    let (mut objects, mut bytes) = (Vec::new(), Vec::new());
    let end = 'calls: loop {
        let rest = GetObjectsRequest {
            refs: request.refs[objects.len()..].to_vec(),
            ..request.clone()
        };
        let mut stream = match client.get_objects(rest).await {
            Ok(reply) => reply.into_inner(),
            Err(status) => break Some(End::Call(status.code())),
        };
        let before = objects.len();
        loop {
            let first = match stream.message().await {
                Ok(Some(part)) => part,
                Ok(None) => break,
                Err(status) => break 'calls Some(End::Stream(status.code())),
            };
            let Ok(header) = Header::of(&first, zstd) else {
                break 'calls Some(End::Refused);
            };
            let mut parts = Assembler::new(header, first);
            while !parts.is_done() {
                match stream.message().await {
                    Ok(Some(part)) if parts.push(&part).is_ok() => {}
                    Ok(_) => break 'calls Some(End::Refused),
                    Err(status) => break 'calls Some(End::Stream(status.code())),
                }
            }
            let (header, data) = parts.finish();
            let Some(want) = request.refs.get(objects.len()) else {
                break 'calls Some(End::Refused);
            };
            let kind = i32::from(header.kind);
            let fits = kind == want.kind && header.id[..] == want.id[..];
            match object::decode(&header, data) {
                Ok(data) if fits && matches(kind, &want.id, &data) => bytes.push(data),
                _ => break 'calls Some(End::Refused),
            }
            let hex = BlobId::try_from(want.id.as_slice()).unwrap().to_string();
            objects.push((kind, hex));
        }
        // A reply that ends `OK` sends at least one object.
        if objects.len() == before {
            break Some(End::Refused);
        }
        if objects.len() == request.refs.len() {
            break None;
        }
    };
    (Got { objects, end }, bytes)
}

async fn ask(url: &str, call: &Call) -> Answer {
    let mut client = connect(url).await;
    let id = |reply: Result<Vec<u8>, tonic::Status>| Answer::Id(reply.map_err(|s| s.code()));
    match call.clone() {
        Call::ResolveRef(request) => {
            let reply = client.resolve_ref(request).await;
            id(reply.map(|r| r.into_inner().changeset_id))
        }
        Call::GetRootTree(request) => {
            let reply = client.get_root_tree(request).await;
            id(reply.map(|r| r.into_inner().tree_id))
        }
        Call::GetObjects(request) => Answer::Objects(get_objects(&mut client, &request).await.0),
    }
}

/// Asks the server, then the daemon (so its misses fill the cache), the
/// cache, and the daemon and the cache again. Every tier must give the
/// server's answer.
async fn same(tiers: &Tiers, call: &Call) -> Answer {
    let server = ask(&tiers.server, call).await;
    for (name, url) in [
        ("daemon", &tiers.socket),
        ("cache", &tiers.cache_url),
        ("daemon, warm", &tiers.socket),
        ("cache, warm", &tiers.cache_url),
    ] {
        assert_eq!(ask(url, call).await, server, "{name}: {call:?}");
    }
    server
}

fn resolve(repo: &str, name: &str) -> Call {
    Call::ResolveRef(ResolveRefRequest {
        repo: repo.into(),
        name: name.into(),
    })
}

fn root_tree(repo: &str, changeset_id: &[u8]) -> Call {
    Call::GetRootTree(GetRootTreeRequest {
        repo: repo.into(),
        changeset_id: changeset_id.to_vec(),
    })
}

fn objects(refs: &[ObjectRef], zstd: bool, ticket: &[u8]) -> Call {
    Call::GetObjects(GetObjectsRequest {
        refs: refs.to_vec(),
        accept: if zstd {
            vec![Encoding::Zstd.into()]
        } else {
            vec![]
        },
        ticket: ticket.to_vec(),
    })
}

/// Mints a ticket for the root tree of `changeset` of [`REPO`] on the
/// server: the read permission that the `GetObjects` calls below spend.
async fn mint(tiers: &Tiers, changeset: ChangesetId) -> Vec<u8> {
    let mut client = connect(&tiers.server).await;
    let reply = client
        .get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: changeset.as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!reply.ticket.is_empty(), "the server minted no ticket");
    reply.ticket
}

/// The ID of a successful `ResolveRef` or `GetRootTree` answer.
fn id_of(answer: &Answer) -> Vec<u8> {
    match answer {
        Answer::Id(Ok(id)) => id.clone(),
        other => panic!("{other:?}"),
    }
}

/// The trees and blobs of the snapshot of `changeset`, its root tree first.
/// All of it comes from the server: the tier answers for `ResolveRef` and
/// `GetRootTree` are compared in one place,
/// `every_tier_resolves_as_the_server_does`.
/// The trees and blobs of the snapshot of `changeset`, its root tree
/// first, each with its path from the root. All of it comes from the
/// server: the tier answers for `ResolveRef` and `GetRootTree` are
/// compared in one place, `every_tier_resolves_as_the_server_does`.
async fn snapshot(
    tiers: &Tiers,
    changeset: ChangesetId,
) -> (Vec<(TreeId, Vec<u8>)>, Vec<(BlobId, Vec<u8>)>) {
    let ticket = mint(tiers, changeset).await;
    let root = ask(&tiers.server, &root_tree(REPO, changeset.as_bytes())).await;
    let root = TreeId::try_from(id_of(&root).as_slice()).unwrap();
    let mut client = connect(&tiers.server).await;
    let (mut trees, mut blobs) = (vec![(root, Vec::new())], Vec::new());
    let mut next = 0;
    while next < trees.len() {
        let (id, path) = trees[next].clone();
        let request = GetObjectsRequest {
            refs: vec![tree_ref(id, &path)],
            accept: vec![],
            ticket: ticket.clone(),
        };
        let (got, data) = get_objects(&mut client, &request).await;
        assert_eq!(got.end, None);
        for (name, entry) in Tree::decode(&data[0]).unwrap().entries {
            let mut below = path.clone();
            if !below.is_empty() {
                below.push(b'/');
            }
            below.extend_from_slice(name.as_bytes());
            match entry {
                Entry::Dir(id) => trees.push((id, below)),
                Entry::File(file) => {
                    if let Some(blob) = file.state().blob() {
                        blobs.push((blob, below));
                    }
                }
            }
        }
        next += 1;
    }
    (trees, blobs)
}

/// Flips one byte in the middle of the file at `path`, so it keeps its
/// size. Returns the bytes it had.
fn damage(path: &Path) -> Vec<u8> {
    let good = fs::read(path).unwrap();
    let mut bad = good.clone();
    let middle = bad.len() / 2;
    bad[middle] ^= 0xff;
    fs::write(path, bad).unwrap();
    good
}

/// A server, cache and daemon whose repo `r` holds files of each kind and
/// size (`common::files`), and a repo `other` that does not have `r`'s
/// changesets.
fn fixture() -> (Tiers, ChangesetId) {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let first = land(&store, &changeset(None, &common::files(&store).0));
    let other = FileState::Regular {
        content: store.blobs().put(b"other\n").unwrap(),
        executable: false,
    };
    let other = changeset(None, &[("other.txt", other)]);
    store
        .write(|c| {
            let repo = c.create_repo("other")?;
            tree::store_changeset(c, repo, &other, &mut tree::lens_of(&store))
        })
        .unwrap();
    (Tiers::start(data, store), first)
}

/// How many objects a `GetObjects` answer holds, and what ended it.
fn shape(answer: &Answer) -> (usize, Option<End>) {
    let Answer::Objects(got) = answer else {
        panic!("{answer:?}");
    };
    (got.objects.len(), got.end)
}

#[test]
fn every_tier_refuses_to_resolve() {
    let (tiers, first) = fixture();
    let first_id = first.as_bytes().to_vec();
    tiers.runtime.block_on(async {
        // The server is the only resolver: scn gets a ref and the ticket of
        // its root tree there, and brings the ticket to a tier. A tier that
        // answered ResolveRef or GetRootTree would be a way to get a ticket
        // for any repo.
        assert_eq!(
            ask(&tiers.server, &resolve(REPO, "main")).await,
            Answer::Id(Ok(first_id.clone()))
        );
        for call in [
            resolve(REPO, "main"),
            resolve(REPO, "no-such-ref"),
            root_tree(REPO, &first_id),
            root_tree(REPO, &[0; 32]),
        ] {
            for (name, url) in [("daemon", &tiers.socket), ("cache", &tiers.cache_url)] {
                let answer = ask(url, &call).await;
                assert_eq!(
                    answer,
                    Answer::Id(Err(Code::Unimplemented)),
                    "{name}: {call:?}"
                );
            }
        }
    });
}

#[test]
fn every_tier_sends_the_objects_of_the_server() {
    let (tiers, first) = fixture();
    tiers.runtime.block_on(async {
        // Every object of the snapshot, a tree level at a time as a checkout
        // asks, then every blob in one call, with a ref that repeats. All of
        // it rides one ticket, as a checkout's calls do.
        let (trees, blobs) = snapshot(&tiers, first).await;
        assert_eq!((trees.len(), blobs.len()), (4, 8));
        let ticket = mint(&tiers, first).await;
        for zstd in [false, true] {
            for (tree, path) in &trees {
                let answer = same(&tiers, &objects(&[tree_ref(*tree, path)], zstd, &ticket)).await;
                assert_eq!(shape(&answer), (1, None));
            }
            let mut refs: Vec<ObjectRef> =
                blobs.iter().map(|(id, path)| blob_ref(*id, path)).collect();
            refs.push(refs[0].clone());
            let answer = same(&tiers, &objects(&refs, zstd, &ticket)).await;
            assert_eq!(shape(&answer), (refs.len(), None));
        }

        // A missing object ends the stream after the objects before it. A
        // tree asked for as a blob is missing too: the walk reaches the path
        // and finds a directory, not a file.
        let at = |(id, path): &(BlobId, Vec<u8>)| blob_ref(*id, path);
        let [a, b] = [at(&blobs[0]), at(&blobs[1])];
        let missing = blob_ref(BlobId::of(b"in no repo\n"), b"README.md");
        let tree_as_blob = blob_ref(BlobId::from_bytes(*trees[0].0.as_bytes()), &trees[0].1);
        let not_found = Some(End::Stream(Code::NotFound));
        for (refs, sent) in [
            (vec![a.clone(), missing.clone(), b.clone()], 1),
            (vec![missing.clone(), a.clone()], 0),
            (vec![a.clone(), tree_as_blob], 1),
        ] {
            for zstd in [false, true] {
                let answer = same(&tiers, &objects(&refs, zstd, &ticket)).await;
                assert_eq!(shape(&answer), (sent, not_found), "{refs:?}");
            }
        }

        // Every tier refuses what the server refuses, before it sends
        // anything. The repo of a read is the ticket's, so no call names
        // one; a call whose shape is bad is refused all the same.
        let short = ObjectRef {
            id: vec![1; 31],
            ..a.clone()
        };
        let kind = |kind: i32| ObjectRef { kind, ..a.clone() };
        for refs in [
            vec![],
            vec![a.clone(); MAX_OBJECT_REFS + 1],
            vec![a.clone(), short],
            vec![a.clone(), kind(ObjectKind::Unspecified.into())],
            vec![a.clone(), kind(7)],
        ] {
            let answer = same(&tiers, &objects(&refs, true, &ticket)).await;
            let refused = Some(End::Call(Code::InvalidArgument));
            assert_eq!(shape(&answer), (0, refused), "{refs:?}");
        }
        // A ticket that the tiers cannot read ends the call before the
        // first object. A ref outside the ticket's tree ends the stream
        // before the first object, with the same not-found as a missing
        // object.
        for (refs, ticket, end) in [
            (
                vec![a.clone()],
                Vec::new(),
                End::Call(Code::PermissionDenied),
            ),
            (
                vec![a.clone()],
                vec![9; 10],
                End::Call(Code::PermissionDenied),
            ),
            (
                vec![blob_ref(BlobId::of(b"in no repo\n"), b"nowhere")],
                ticket.clone(),
                End::Stream(Code::NotFound),
            ),
        ] {
            let answer = same(&tiers, &objects(&refs, true, &ticket)).await;
            assert_eq!(shape(&answer), (0, Some(end)), "{refs:?}");
        }
    });
}

#[test]
fn a_damaged_copy_on_the_daemon_is_fetched_again() {
    let (tiers, first) = fixture();
    tiers.runtime.block_on(async {
        let (trees, blobs) = snapshot(&tiers, first).await;
        let ticket = mint(&tiers, first).await;
        let refs: Vec<ObjectRef> = trees
            .iter()
            .map(|(id, path)| tree_ref(*id, path))
            .chain(blobs.iter().map(|(id, path)| blob_ref(*id, path)))
            .collect();
        let all = ask(&tiers.server, &objects(&refs, true, &ticket)).await;
        // Now the daemon holds every object.
        assert_eq!(
            ask(&tiers.socket, &objects(&refs, true, &ticket)).await,
            all
        );
        let calls = tiers.upstream_calls();

        // One byte of the daemon's copy of the root tree and of the large
        // blob changes.
        let [on_server, on_node] = tiers.blob_paths(blobs[1].0);
        damage(&on_node);
        damage(&tiers.daemon_tree_path(trees[0].0));
        for zstd in [true, false] {
            assert_eq!(
                ask(&tiers.socket, &objects(&refs, zstd, &ticket)).await,
                all
            );
        }
        // The daemon fetched both again, and keeps the server's file again.
        assert!(tiers.upstream_calls().0 > calls.0);
        let same_file = fs::read(on_node).unwrap() == fs::read(on_server).unwrap();
        assert!(same_file, "the daemon's file differs from the server's");
    });
}

#[test]
fn a_damaged_blob_on_the_server_never_passes_a_tier() {
    let (tiers, first) = fixture();
    tiers.runtime.block_on(async {
        let (_, blobs) = snapshot(&tiers, first).await;
        let ticket = mint(&tiers, first).await;
        // README.md, big.bin and the target of link, by name.
        let refs: Vec<ObjectRef> = [blobs[0].clone(), blobs[1].clone(), blobs[3].clone()]
            .iter()
            .map(|(id, path)| blob_ref(*id, path))
            .collect();
        let big = blobs[1].0;

        // One byte of the large blob's zstd file changes. Its frame header
        // still reads, so the server sends it, in its stored form.
        let path = &tiers.blob_paths(big)[0];
        let good = damage(path);
        for zstd in [false, true] {
            let call = objects(&refs, zstd, &ticket);
            // The server unpacks the blob for a RAW client and finds the
            // damage. It does not check what it sends ZSTD: the client does.
            let server = if zstd {
                End::Refused
            } else {
                End::Stream(Code::Internal)
            };
            let tier = End::Stream(Code::DataLoss);
            for (name, url, end) in [
                ("server", &tiers.server, server),
                ("daemon", &tiers.socket, tier),
                ("cache", &tiers.cache_url, tier),
            ] {
                let answer = ask(url, &call).await;
                assert_eq!(shape(&answer), (1, Some(end)), "{name}, zstd {zstd}");
            }
        }
        // No tier kept the bad blob. Once the server's copy is good again,
        // every tier asks for it again and sends it.
        assert!(!tiers.blob_paths(big)[1].exists());
        fs::write(path, &good).unwrap();
        let answer = same(&tiers, &objects(&refs, true, &ticket)).await;
        assert_eq!(shape(&answer), (3, None));
    });
}
