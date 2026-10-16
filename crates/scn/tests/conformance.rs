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
use std::path::PathBuf;

use scion_core::{BlobId, ChangesetId, Entry, FileState, Tree, TreeId};
use scion_proto::auth::Bearer;
use scion_proto::object::{self, Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef, ResolveRefRequest,
};
use scion_proto::{CHUNK_BYTES, MAX_OBJECT_REFS};
use scion_server::{Store, db, tree};
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

mod common;
use common::{REPO, TOKEN, Tiers, changeset, land, noise};

type Client = ObjectsClient<InterceptedService<Channel, Bearer>>;

async fn connect(url: &str) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
}

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

/// Asks the daemon (so its misses fill the cache), the cache, the server,
/// then the daemon and the cache again. Every answer must be the server's.
async fn same(tiers: &Tiers, call: &Call) -> Answer {
    let order = [
        ("daemon", &tiers.socket),
        ("cache", &tiers.cache_url),
        ("server", &tiers.server),
        ("daemon, warm", &tiers.socket),
        ("cache, warm", &tiers.cache_url),
    ];
    let mut answers = Vec::new();
    for (name, url) in order {
        answers.push((name, ask(url, call).await));
    }
    let server = answers.remove(2).1;
    for (name, answer) in answers {
        assert_eq!(answer, server, "{name}: {call:?}");
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

fn objects(repo: &str, refs: &[ObjectRef], zstd: bool) -> Call {
    Call::GetObjects(GetObjectsRequest {
        repo: repo.into(),
        refs: refs.to_vec(),
        accept: if zstd {
            vec![Encoding::Zstd.into()]
        } else {
            vec![]
        },
    })
}

fn tree_ref(id: TreeId) -> ObjectRef {
    ObjectRef {
        kind: ObjectKind::Tree.into(),
        id: id.as_bytes().to_vec(),
    }
}

fn blob_ref(id: BlobId) -> ObjectRef {
    ObjectRef {
        kind: ObjectKind::Blob.into(),
        id: id.as_bytes().to_vec(),
    }
}

/// The ID of a successful `ResolveRef` or `GetRootTree` answer.
fn id_of(answer: &Answer) -> Vec<u8> {
    match answer {
        Answer::Id(Ok(id)) => id.clone(),
        other => panic!("{other:?}"),
    }
}

/// The trees and blobs of the snapshot under `root`, read from the server.
async fn snapshot(tiers: &Tiers, root: TreeId) -> (Vec<TreeId>, Vec<BlobId>) {
    let mut client = connect(&tiers.server).await;
    let (mut trees, mut blobs) = (vec![root], Vec::new());
    let mut next = 0;
    while next < trees.len() {
        let request = GetObjectsRequest {
            repo: REPO.into(),
            refs: vec![tree_ref(trees[next])],
            accept: vec![],
        };
        let (got, data) = get_objects(&mut client, &request).await;
        assert_eq!(got.end, None);
        for entry in Tree::decode(&data[0]).unwrap().entries.into_values() {
            match entry {
                Entry::Dir(id) => trees.push(id),
                Entry::File(state) => blobs.extend(state.blob()),
            }
        }
        next += 1;
    }
    (trees, blobs)
}

/// A server, cache and daemon whose repo `r` holds files of each kind and
/// size, and a repo `other` that does not have `r`'s changesets.
fn fixture() -> (Tiers, ChangesetId) {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
    let put = |content: &[u8]| store.blobs().put(content).unwrap();
    let regular = |content: &[u8], executable| FileState::Regular {
        content: put(content),
        executable,
    };
    // Over three parts of 1 MiB, packed or not: noise, then text.
    let mut big = noise(3 * CHUNK_BYTES);
    big.extend((0..100_000).flat_map(|i| format!("line {i}\n").into_bytes()));
    let readme = regular(b"hello\n", false);
    let files = [
        ("README.md", readme),
        ("big.bin", regular(&big, false)),
        ("empty", regular(b"", false)),
        ("run.sh", regular(b"#!/bin/sh\necho hi\n", true)),
        ("src/lib.rs", regular(b"pub fn f() {}\n", false)),
        ("src/nested/deep/file.txt", regular(b"deep\n", false)),
        // The same blob as README.md.
        ("src/nested/copy.txt", readme),
        (
            "link",
            FileState::Symlink {
                target: put(b"README.md"),
            },
        ),
    ];
    let first = land(&store, &changeset(None, &files));
    let other = changeset(None, &[("other.txt", regular(b"other\n", false))]);
    store
        .write(|c| {
            let repo = db::create_repo(c, "other")?;
            tree::store_changeset(c, repo, &other)
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
fn every_tier_resolves_as_the_server_does() {
    let (tiers, first) = fixture();
    let first = first.as_bytes().to_vec();
    tiers.runtime.block_on(async {
        // A ref is the only answer that changes, so a tier never keeps it.
        assert_eq!(id_of(&same(&tiers, &resolve(REPO, "main")).await), first);
        for (repo, name, code) in [
            (REPO, "no-such-ref", Code::NotFound),
            ("gone", "main", Code::NotFound),
            ("", "main", Code::InvalidArgument),
            ("bad\u{1}", "main", Code::InvalidArgument),
            (REPO, "", Code::InvalidArgument),
        ] {
            let answer = same(&tiers, &resolve(repo, name)).await;
            assert_eq!(answer, Answer::Id(Err(code)), "{repo:?} {name:?}");
        }
        let new = FileState::Regular {
            content: tiers.store.blobs().put(b"hello, again\n").unwrap(),
            executable: false,
        };
        let parent = ChangesetId::try_from(first.as_slice()).unwrap();
        let second = changeset(Some(parent), &[("README.md", new)]);
        let second = land(&tiers.store, &second);
        let answer = same(&tiers, &resolve(REPO, "main")).await;
        assert_eq!(
            id_of(&answer),
            second.as_bytes(),
            "every tier follows the ref"
        );

        // A root tree is kept by repo and changeset: `other` never sees
        // `first`, before or after `r` asked for it.
        let not_found = Answer::Id(Err(Code::NotFound));
        assert_eq!(same(&tiers, &root_tree("other", &first)).await, not_found);
        id_of(&same(&tiers, &root_tree(REPO, &first)).await);
        assert_eq!(same(&tiers, &root_tree("other", &first)).await, not_found);
        for (repo, id, code) in [
            (REPO, vec![0; 32], Code::NotFound),
            ("gone", first.clone(), Code::NotFound),
            (REPO, vec![0; 31], Code::InvalidArgument),
            ("", first.clone(), Code::InvalidArgument),
        ] {
            let answer = same(&tiers, &root_tree(repo, &id)).await;
            assert_eq!(answer, Answer::Id(Err(code)), "{repo:?} {id:?}");
        }
    });
}

#[test]
fn every_tier_sends_the_objects_of_the_server() {
    let (tiers, first) = fixture();
    let missing = blob_ref(BlobId::of(b"in no repo\n"));
    tiers.runtime.block_on(async {
        let root = same(&tiers, &root_tree(REPO, first.as_bytes())).await;
        let root = TreeId::try_from(id_of(&root).as_slice()).unwrap();
        // Every object of the snapshot, a tree level at a time as a checkout
        // asks, then every blob in one call, with a ref that repeats.
        let (trees, blobs) = snapshot(&tiers, root).await;
        assert_eq!((trees.len(), blobs.len()), (4, 8));
        for zstd in [false, true] {
            for &tree in &trees {
                let answer = same(&tiers, &objects(REPO, &[tree_ref(tree)], zstd)).await;
                assert_eq!(shape(&answer), (1, None));
            }
            let mut refs: Vec<ObjectRef> = blobs.iter().copied().map(blob_ref).collect();
            refs.push(refs[0].clone());
            let answer = same(&tiers, &objects(REPO, &refs, zstd)).await;
            assert_eq!(shape(&answer), (refs.len(), None));
        }

        // A missing object ends the stream after the objects before it. A
        // tree asked for as a blob is missing too.
        let [a, b] = [blobs[0], blobs[1]].map(blob_ref);
        let tree_as_blob = blob_ref(BlobId::from_bytes(*root.as_bytes()));
        let not_found = Some(End::Stream(Code::NotFound));
        for (refs, sent) in [
            (vec![a.clone(), missing.clone(), b.clone()], 1),
            (vec![missing.clone(), a.clone()], 0),
            (vec![a.clone(), tree_as_blob], 1),
        ] {
            for zstd in [false, true] {
                let answer = same(&tiers, &objects(REPO, &refs, zstd)).await;
                assert_eq!(shape(&answer), (sent, not_found), "{refs:?}");
            }
        }

        // Every tier refuses what the server refuses, before it sends
        // anything.
        let short = ObjectRef {
            id: vec![1; 31],
            ..a.clone()
        };
        let kind = |kind: i32| ObjectRef { kind, ..a.clone() };
        for (repo, refs) in [
            (REPO, vec![]),
            (REPO, vec![a.clone(); MAX_OBJECT_REFS + 1]),
            (REPO, vec![a.clone(), short]),
            (REPO, vec![a.clone(), kind(ObjectKind::Unspecified.into())]),
            (REPO, vec![a.clone(), kind(7)]),
            ("", vec![a.clone()]),
            ("bad\u{1}", vec![a.clone()]),
        ] {
            let answer = same(&tiers, &objects(repo, &refs, true)).await;
            let refused = Some(End::Call(Code::InvalidArgument));
            assert_eq!(shape(&answer), (0, refused), "{repo:?} {refs:?}");
        }
        // A repo that nobody has, and an object that no tier holds: the call
        // fails at each tier. (A tier sends an object that it holds for any
        // valid repo name, #13.)
        let answer = same(&tiers, &objects("gone", &[missing], true)).await;
        assert_eq!(shape(&answer), (0, Some(End::Call(Code::NotFound))));
    });
}

/// The daemon's file of a tree.
fn daemon_tree_path(tiers: &Tiers, id: TreeId) -> PathBuf {
    let hex = id.to_string();
    let objects = tiers.dirs[1].path().join("data/objects/tree");
    objects.join(&hex[..2]).join(&hex[2..])
}

#[test]
fn a_damaged_copy_on_the_daemon_is_fetched_again() {
    let (tiers, first) = fixture();
    tiers.runtime.block_on(async {
        let root = same(&tiers, &root_tree(REPO, first.as_bytes())).await;
        let root = TreeId::try_from(id_of(&root).as_slice()).unwrap();
        let (trees, blobs) = snapshot(&tiers, root).await;
        let refs: Vec<ObjectRef> = trees
            .iter()
            .copied()
            .map(tree_ref)
            .chain(blobs.iter().copied().map(blob_ref))
            .collect();
        // The daemon now holds every object.
        let all = same(&tiers, &objects(REPO, &refs, true)).await;
        let calls = tiers.upstream_calls();

        // Damage one byte of the daemon's copy of a tree and of the large
        // blob, so each file keeps its size.
        let [on_server, on_node] = tiers.blob_paths(blobs[1]);
        for path in [&on_node, &daemon_tree_path(&tiers, root)] {
            let mut bytes = fs::read(path).unwrap();
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0xff;
            fs::write(path, bytes).unwrap();
        }
        for zstd in [true, false] {
            assert_eq!(ask(&tiers.socket, &objects(REPO, &refs, zstd)).await, all);
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
        let root = same(&tiers, &root_tree(REPO, first.as_bytes())).await;
        let root = TreeId::try_from(id_of(&root).as_slice()).unwrap();
        let (_, blobs) = snapshot(&tiers, root).await;
        // README.md, big.bin and the target of link, by name.
        let refs = [blobs[0], blobs[1], blobs[3]].map(blob_ref);
        let big = blobs[1];

        // One byte of the large blob's zstd file changes. Its frame header
        // still reads, so the server sends it, in its stored form.
        let path = &tiers.blob_paths(big)[0];
        let good = fs::read(path).unwrap();
        let mut bad = good.clone();
        let middle = bad.len() / 2;
        bad[middle] ^= 0xff;
        fs::write(path, &bad).unwrap();

        for zstd in [false, true] {
            let call = objects(REPO, &refs, zstd);
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
        let answer = same(&tiers, &objects(REPO, &refs, true)).await;
        assert_eq!(shape(&answer), (3, None));
    });
}
