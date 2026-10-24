//! `Log` paging, end to end: a reply that stops at its `limit` carries
//! `next_changeset_id`, and the next call resumes exactly there. The tests
//! walk a history page by page, with and without a path, and compare every
//! paged walk against one full walk of the same history: same entries, same
//! order, no gap, no repeat. A continuation that skipped or repeated one
//! entry would put the wrong entry at a seam (call k must resume at exactly
//! the entry the full walk holds at position k * page) or shift the rest,
//! and both end the walk with a wrong list.

use std::sync::Arc;

use scion_core::{Changeset, FileState, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::LogRequest;
use scion_proto::v1::scion_client::ScionClient;
use scion_server::Backend;
use scion_server::Metrics;
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::{service, store::Store, tree};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "secret";
/// Longer than one internal page of the reply's producer
/// (`read::MAX_SCAN` = 1,000), so one call fetches page after page.
const HISTORY: usize = 3 * 1000 + 1;

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

/// A repo whose chain of `count` changesets alternates the path it
/// changes: even i changes `d/f.txt`, odd i changes `other.txt`. The
/// message names i, so a test can tell which changeset an entry holds.
fn chain(count: usize) -> (tempfile::TempDir, Backend, Vec<scion_core::ChangesetId>) {
    let dir = tempfile::tempdir().unwrap();
    let store = from_store(Store::open(dir.path()).unwrap());
    let ids = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut ids = Vec::new();
            for i in 0..count {
                let content = vec![b'a' + u8::try_from(i % 26).unwrap(); i + 1];
                let regular = FileState::Regular {
                    content: store.blobs().put(&content).unwrap(),
                    executable: false,
                };
                let path: &[u8] = if i % 2 == 0 { b"d/f.txt" } else { b"other.txt" };
                let cs = Changeset::new(
                    ids.last().copied().into_iter().collect(),
                    scion_core::IdentityTime::new(b"a".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
                        .expect("identity fits the format"),
                    None,
                    format!("change {i}\n").into_bytes(),
                    [(RepoPath::new(path).unwrap(), Some(regular))].into(),
                )
                .expect("changeset fits the format");
                ids.push(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            Ok(ids)
        })
        .unwrap();
    (dir, store, ids)
}

async fn serve(store: Backend, dir: &std::path::Path) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
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

/// One `Log` call: the entry IDs of the reply with the i of each one's
/// message, and the reply's continuation.
async fn call(
    client: &mut Scion,
    from: &scion_core::ChangesetId,
    limit: u32,
    path: &[u8],
) -> (Vec<(usize, scion_core::ChangesetId)>, Vec<u8>) {
    let mut reply = client
        .log(LogRequest {
            repo: REPO.to_string(),
            changeset_id: from.as_bytes().to_vec(),
            limit,
            path: path.to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut got: Vec<(usize, scion_core::ChangesetId)> = Vec::new();
    let mut next = Vec::new();
    while let Some(part) = reply.message().await.unwrap() {
        for entry in &part.entries {
            let id = scion_core::ChangesetId::try_from(entry.changeset_id.as_slice()).unwrap();
            let cs = Changeset::decode(&entry.changeset).unwrap();
            // The reply streams newest first; a repeated ID inside one
            // reply is a repeat.
            assert_ne!(got.last().map(|(_, id)| id), Some(&id), "a repeat");
            let message = String::from_utf8(cs.message().to_vec()).unwrap();
            let i: usize = message
                .trim()
                .strip_prefix("change ")
                .expect("the message names its i")
                .parse()
                .unwrap();
            got.push((i, id));
        }
        next = part.next_changeset_id;
    }
    (got, next)
}

/// Walks from `tip` page by page, `page` entries per call, and fails
/// unless the walk covers exactly `expected` (positions in the full walk,
/// newest first): every call resumes at the right entry, and the whole
/// sequence matches, with no gap and no repeat.
async fn paged(
    client: &mut Scion,
    tip: &scion_core::ChangesetId,
    expected: &[usize],
    page: u32,
    path: &[u8],
) {
    let mut got: Vec<usize> = Vec::new();
    let mut from = *tip;
    // A walk that never advances (a continuation that repeats one entry)
    // would loop here; the bound turns it into a failure.
    let calls_max = expected.len() / page as usize + 8;
    for call_number in 0..calls_max {
        let (ids, next) = call(client, &from, page, path).await;
        // Call k resumes exactly at expected[k*page]: a continuation that
        // skipped or repeated one entry fails here, and any drift after it
        // fails the final comparison.
        if let Some(&first) = expected.get(got.len()) {
            assert_eq!(
                ids.first().map(|(i, _)| i),
                Some(&first),
                "call {call_number} resumed at the wrong entry"
            );
        }
        got.extend(ids.iter().map(|(i, _)| *i));
        assert!(got.len() <= expected.len(), "the paged walk ran long");
        if next.is_empty() {
            break;
        }
        from = scion_core::ChangesetId::try_from(next.as_slice()).unwrap();
    }
    assert_eq!(got, expected, "the paged walk differs from the full walk");
}

#[tokio::test]
async fn log_paging_matches_one_full_walk() {
    let (dir, store, ids) = chain(HISTORY);
    let tip = *ids.last().unwrap();
    let url = serve(store, dir.path()).await;
    let mut client = client(&url).await;

    // The reference: the full walk, assembled from calls at the maximum
    // the server scans. It must end the history with an empty
    // continuation.
    let mut full: Vec<(usize, scion_core::ChangesetId)> = Vec::new();
    let mut from = tip;
    for _ in 0..HISTORY / 1_000 + 2 {
        let (ids, next) = call(&mut client, &from, 1_000, b"").await;
        full.extend(ids);
        if next.is_empty() {
            break;
        }
        from = scion_core::ChangesetId::try_from(next.as_slice()).unwrap();
    }
    assert_eq!(full.len(), HISTORY, "the whole history in one walk");

    // Page by page, ending mid-page: every seam resumes at the entry the
    // full walk holds there, and the whole sequence matches.
    let positions: Vec<usize> = full.iter().map(|(i, _)| *i).collect();
    paged(&mut client, &tip, &positions, 7, b"").await;
    // A page size that divides the history: the last page ends on its edge.
    paged(&mut client, &tip, &positions, 10, b"").await;
    // One entry per call: every call ends on a seam.
    paged(&mut client, &tip, &positions, 1, b"").await;
    // A limit past one internal page: the reply fetches page after page
    // inside one call, and its continuation picks up mid-page.
    let (long, next) = call(&mut client, &tip, 2_500, b"").await;
    assert_eq!(long, full[..2_500], "the first 2,500 of the full walk");
    assert_eq!(
        next,
        full[2_500].1.as_bytes().to_vec(),
        "the continuation is the 2,501st entry"
    );
}

#[tokio::test]
async fn log_paging_with_a_path_matches_one_full_walk() {
    let (dir, store, ids) = chain(HISTORY);
    let tip = *ids.last().unwrap();
    let url = serve(store, dir.path()).await;
    let mut client = client(&url).await;

    // The reference: the full path walk, assembled the same way.
    let mut full: Vec<(usize, scion_core::ChangesetId)> = Vec::new();
    let mut from = tip;
    // A ceiling on the pages: a server that repeats its cursor must fail
    // the test, not hang it.
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= HISTORY, "the walk never left {from:?}");
        let (ids, next) = call(&mut client, &from, 1_000, b"d/f.txt").await;
        full.extend(ids);
        if next.is_empty() {
            break;
        }
        from = scion_core::ChangesetId::try_from(next.as_slice()).unwrap();
    }
    assert_eq!(full.len(), HISTORY.div_ceil(2), "only the even i");
    // An independent expectation: the i of the path entries, newest first.
    let mut want: Vec<usize> = (0..HISTORY).rev().filter(|i| i % 2 == 0).collect();
    want.retain(|i| full.iter().any(|(got, _)| got == i));
    assert_eq!(full.iter().map(|(i, _)| *i).collect::<Vec<_>>(), want);

    let positions: Vec<usize> = full.iter().map(|(i, _)| *i).collect();
    paged(&mut client, &tip, &positions, 7, b"d/f.txt").await;
    paged(&mut client, &tip, &positions, 4, b"d/f.txt").await;
}

#[path = "common/mod.rs"]
mod common;
