//! Pins the v1 byte format (`docs/format-v1.md`).
//!
//! Expected bytes are assembled by hand from the grammar, independent of the
//! encoder. If a test here fails, the format changed and every stored ID with it.

use std::collections::BTreeMap;

use scion_core::{
    BlobHasher, BlobId, Changeset, DecodeError, Entry, FileEntry, FileState, GitOid, IdentityTime,
    PathElem, PathError, RepoPath, Tree, TreeId,
};

/// Builds expected bytes straight from the grammar.
#[derive(Default)]
struct W(Vec<u8>);

impl W {
    fn u8(mut self, v: u8) -> Self {
        self.0.push(v);
        self
    }
    fn u32(mut self, v: u32) -> Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn raw(mut self, b: &[u8]) -> Self {
        self.0.extend_from_slice(b);
        self
    }
    fn bytes(self, b: &[u8]) -> Self {
        self.u32(u32::try_from(b.len()).unwrap()).raw(b)
    }
    fn identity(self, name: &[u8], email: &[u8], secs: i64, tz: i32) -> Self {
        self.bytes(name)
            .bytes(email)
            .raw(&secs.to_be_bytes())
            .raw(&tz.to_be_bytes())
    }
    /// A changeset header with no parents, no committer, and `n` changes to follow.
    fn header(n_changes: u32) -> Self {
        W::default()
            .u8(1)
            .u32(0)
            .identity(b"A", b"a@x", 0, 0)
            .u8(0)
            .bytes(b"m")
            .u32(n_changes)
    }
}

fn hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new()
        .update(domain)
        .update(bytes)
        .finalize()
        .as_bytes()
}

fn identity(name: &str, email: &str, unix_secs: i64, tz_offset_minutes: i32) -> IdentityTime {
    IdentityTime::new(name.into(), email.into(), unix_secs, tz_offset_minutes)
        .expect("identity fits the format")
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn regular(content: &[u8], executable: bool) -> FileState {
    FileState::Regular {
        content: BlobId::of(content),
        executable,
    }
}

fn root() -> Changeset {
    Changeset::new(
        vec![],
        identity("Ada Lovelace", "ada@example.com", 1_700_000_000, 120),
        Some(identity("Bob", "bob@example.com", 1_700_000_100, -300)),
        b"init\n".to_vec(),
        BTreeMap::from([
            (path("README.md"), Some(regular(b"# demo\n", false))),
            (path("bin/run"), Some(regular(b"#!/bin/sh\n", true))),
            (
                path("docs/link"),
                Some(FileState::Symlink {
                    target: BlobId::of(b"../README.md"),
                }),
            ),
            (
                path("vendor/lib"),
                Some(FileState::Gitlink {
                    commit: GitOid::Sha1([0xab; 20]),
                }),
            ),
        ]),
    )
    .expect("changeset fits the format")
}

fn child() -> Changeset {
    Changeset::new(
        vec![root().id()],
        identity("Ada Lovelace", "ada@example.com", 1_700_000_200, 120),
        None,
        b"fix\n".to_vec(),
        BTreeMap::from([
            (path("README.md"), None),
            (path("src/main.rs"), Some(regular(b"fn main() {}\n", false))),
            (
                path("vendor/lib"),
                Some(FileState::Gitlink {
                    commit: GitOid::Sha256([0xcd; 32]),
                }),
            ),
        ]),
    )
    .expect("changeset fits the format")
}

fn merge() -> Changeset {
    Changeset::new(
        vec![child().id(), root().id()],
        identity("Bob", "bob@example.com", 1_700_000_300, -300),
        None,
        vec![],
        BTreeMap::new(),
    )
    .expect("changeset fits the format")
}

fn tree() -> Tree {
    let name = |n: &str| PathElem::new(n).unwrap();
    Tree::new(BTreeMap::from([
        (
            name("README.md"),
            Entry::File(FileEntry::new(
                regular(b"# demo\n", false),
                b"# demo\n".len() as u64,
            )),
        ),
        (name("bin"), Entry::Dir(TreeId::from_bytes([0x11; 32]))),
        (
            name("link"),
            Entry::File(FileEntry::new(
                FileState::Symlink {
                    target: BlobId::of(b"README.md"),
                },
                b"README.md".len() as u64,
            )),
        ),
        (
            name("run.sh"),
            Entry::File(FileEntry::new(
                regular(b"#!/bin/sh\n", true),
                b"#!/bin/sh\n".len() as u64,
            )),
        ),
        (
            name("sub"),
            Entry::File(FileEntry::new(
                FileState::Gitlink {
                    commit: GitOid::Sha1([0x22; 20]),
                },
                0,
            )),
        ),
        (
            name("sub256"),
            Entry::File(FileEntry::new(
                FileState::Gitlink {
                    commit: GitOid::Sha256([0x33; 32]),
                },
                0,
            )),
        ),
    ]))
    .expect("tree fits the format")
}

#[test]
fn blob_ids() {
    assert_eq!(BlobId::of(b"").as_bytes(), &hash(b"scion\0blob\0v1\0", b""));
    assert_eq!(
        BlobId::of(b"").to_string(),
        "f35eedcfc54eb21715f61f3bb47f9e58b75ed941ad3a04ec8a45fb8163865d2d"
    );
    assert_eq!(
        BlobId::of(b"# demo\n").to_string(),
        "20d3380696d2b5af0b85ae1dc2595c086aecc40db3702c88607725cd8d930c2a"
    );
    let streamed = BlobId::of_reader(&b"# demo\n"[..]).unwrap();
    assert_eq!(streamed, BlobId::of(b"# demo\n"));
    let mut parts = BlobHasher::default();
    parts.update(b"# de");
    parts.update(b"");
    parts.update(b"mo\n");
    assert_eq!(parts.finalize(), BlobId::of(b"# demo\n"));
    assert_eq!(BlobHasher::default().finalize(), BlobId::of(b""));
}

#[test]
fn root_changeset_bytes() {
    let expected = W::default()
        .u8(1)
        .u32(0)
        .identity(b"Ada Lovelace", b"ada@example.com", 1_700_000_000, 120)
        .u8(1)
        .identity(b"Bob", b"bob@example.com", 1_700_000_100, -300)
        .bytes(b"init\n")
        .u32(4)
        .bytes(b"README.md")
        .u8(1)
        .raw(BlobId::of(b"# demo\n").as_bytes())
        .u8(0)
        .bytes(b"bin/run")
        .u8(1)
        .raw(BlobId::of(b"#!/bin/sh\n").as_bytes())
        .u8(1)
        .bytes(b"docs/link")
        .u8(2)
        .raw(BlobId::of(b"../README.md").as_bytes())
        .bytes(b"vendor/lib")
        .u8(3)
        .u8(1)
        .raw(&[0xab; 20])
        .0;
    let cs = root();
    assert_eq!(cs.encode(), expected);
    assert_eq!(
        cs.id().as_bytes(),
        &hash(b"scion\0changeset\0v1\0", &expected)
    );
    assert_eq!(
        cs.id().to_string(),
        "68aaac6287056ee56023fb6139c7c5b977d68345cc50d3d7ada2d8f4eec72c30"
    );
    assert_eq!(Changeset::decode(&expected).unwrap(), cs);
    let id = cs.id();
    assert_eq!(Changeset::decode_with_id(&expected).unwrap(), (cs, id));
}

#[test]
fn child_changeset_bytes() {
    let expected = W::default()
        .u8(1)
        .u32(1)
        .raw(root().id().as_bytes())
        .identity(b"Ada Lovelace", b"ada@example.com", 1_700_000_200, 120)
        .u8(0)
        .bytes(b"fix\n")
        .u32(3)
        .bytes(b"README.md")
        .u8(0)
        .bytes(b"src/main.rs")
        .u8(1)
        .raw(BlobId::of(b"fn main() {}\n").as_bytes())
        .u8(0)
        .bytes(b"vendor/lib")
        .u8(3)
        .u8(2)
        .raw(&[0xcd; 32])
        .0;
    let cs = child();
    assert_eq!(cs.encode(), expected);
    assert_eq!(
        cs.id().to_string(),
        "141e0659d73489d915d1a915797f8824e1302a3d42f95ea89252eb5de6944d41"
    );
    assert_eq!(Changeset::decode(&expected).unwrap(), cs);
    let id = cs.id();
    assert_eq!(Changeset::decode_with_id(&expected).unwrap(), (cs, id));
}

#[test]
fn merge_changeset_bytes() {
    let expected = W::default()
        .u8(1)
        .u32(2)
        .raw(child().id().as_bytes())
        .raw(root().id().as_bytes())
        .identity(b"Bob", b"bob@example.com", 1_700_000_300, -300)
        .u8(0)
        .bytes(b"")
        .u32(0)
        .0;
    let cs = merge();
    assert_eq!(cs.encode(), expected);
    assert_eq!(
        cs.id().to_string(),
        "b91ced501e84d9b9c17d4ad531272b55a9c136298b19ff3bf26980837ddb1c1b"
    );
    assert_eq!(Changeset::decode(&expected).unwrap(), cs);
    let id = cs.id();
    assert_eq!(Changeset::decode_with_id(&expected).unwrap(), (cs, id));
}

#[test]
fn tree_bytes() {
    let expected = W::default()
        .u8(1)
        .u32(6)
        .bytes(b"README.md")
        .u8(1)
        .raw(BlobId::of(b"# demo\n").as_bytes())
        .u8(0)
        .u64(b"# demo\n".len() as u64)
        .bytes(b"bin")
        .u8(4)
        .raw(&[0x11; 32])
        .bytes(b"link")
        .u8(2)
        .raw(BlobId::of(b"README.md").as_bytes())
        .u64(b"README.md".len() as u64)
        .bytes(b"run.sh")
        .u8(1)
        .raw(BlobId::of(b"#!/bin/sh\n").as_bytes())
        .u8(1)
        .u64(b"#!/bin/sh\n".len() as u64)
        .bytes(b"sub")
        .u8(3)
        .u8(1)
        .raw(&[0x22; 20])
        .bytes(b"sub256")
        .u8(3)
        .u8(2)
        .raw(&[0x33; 32])
        .0;
    let t = tree();
    assert_eq!(t.encode(), expected);
    assert_eq!(t.id().as_bytes(), &hash(b"scion\0tree\0v1\0", &expected));
    assert_eq!(
        t.id().to_string(),
        "aa0a80bc7aa457e6f83bc5ffb4369f6ee1ea5d187d8f6a1a8901d9efba315571"
    );
    assert_eq!(Tree::decode(&expected).unwrap(), t);
}

#[test]
fn empty_tree_bytes() {
    assert_eq!(Tree::default().encode(), [1, 0, 0, 0, 0]);
    assert_eq!(
        Tree::default().id().to_string(),
        "d73124f4dff0a07b7648eccde1c3bdd8c571775ac5546a4311ceef860d76226a"
    );
}

// ---- rejection ----

fn decode_cs(w: &W) -> Result<Changeset, DecodeError> {
    Changeset::decode(&w.0)
}

#[test]
fn rejects_every_truncation_and_trailing_byte() {
    for valid in [root().encode(), child().encode(), merge().encode()] {
        for len in 0..valid.len() {
            assert!(
                Changeset::decode(&valid[..len]).is_err(),
                "prefix {len} accepted"
            );
        }
    }
    let valid = tree().encode();
    for len in 0..valid.len() {
        assert!(
            Tree::decode(&valid[..len]).is_err(),
            "prefix {len} accepted"
        );
    }
    let mut cs = root().encode();
    cs.push(0);
    assert_eq!(Changeset::decode(&cs), Err(DecodeError::TrailingBytes));
    let mut t = tree().encode();
    t.push(0);
    assert_eq!(Tree::decode(&t), Err(DecodeError::TrailingBytes));
}

#[test]
fn rejects_bad_version() {
    for v in [0, 2, 255] {
        let mut cs = root().encode();
        cs[0] = v;
        assert_eq!(Changeset::decode(&cs), Err(DecodeError::BadVersion(v)));
        let mut t = tree().encode();
        t[0] = v;
        assert_eq!(Tree::decode(&t), Err(DecodeError::BadVersion(v)));
    }
}

#[test]
fn rejects_bad_tags_flags_and_algos() {
    let blob = [7u8; 32];
    assert_eq!(
        decode_cs(&W::header(1).bytes(b"a").u8(4).raw(&blob)),
        Err(DecodeError::BadTag(4))
    );
    assert_eq!(
        decode_cs(&W::header(1).bytes(b"a").u8(9)),
        Err(DecodeError::BadTag(9))
    );
    assert_eq!(
        decode_cs(&W::header(1).bytes(b"a").u8(1).raw(&blob).u8(2)),
        Err(DecodeError::BadFlag(2))
    );
    assert_eq!(
        decode_cs(&W::header(1).bytes(b"a").u8(3).u8(0)),
        Err(DecodeError::BadGitAlgo(0))
    );
    assert_eq!(
        decode_cs(&W::header(1).bytes(b"a").u8(3).u8(3)),
        Err(DecodeError::BadGitAlgo(3))
    );

    let committer_flag_2 = W::default().u8(1).u32(0).identity(b"A", b"a@x", 0, 0).u8(2);
    assert_eq!(decode_cs(&committer_flag_2), Err(DecodeError::BadFlag(2)));

    let delete_in_tree = W::default().u8(1).u32(1).bytes(b"a").u8(0);
    assert_eq!(Tree::decode(&delete_in_tree.0), Err(DecodeError::BadTag(0)));
}

#[test]
fn rejects_unsorted_and_duplicate_paths() {
    let unsorted = W::header(2).bytes(b"b").u8(0).bytes(b"a").u8(0);
    assert_eq!(decode_cs(&unsorted), Err(DecodeError::Unsorted));
    let duplicate = W::header(2).bytes(b"a").u8(0).bytes(b"a").u8(0);
    assert_eq!(decode_cs(&duplicate), Err(DecodeError::Unsorted));

    let dir = [9u8; 32];
    let unsorted_tree = W::default()
        .u8(1)
        .u32(2)
        .bytes(b"b")
        .u8(4)
        .raw(&dir)
        .bytes(b"a")
        .u8(4)
        .raw(&dir);
    assert_eq!(Tree::decode(&unsorted_tree.0), Err(DecodeError::Unsorted));
}

#[test]
fn rejects_too_many_changes_before_reading_them() {
    let two = W::header(2).bytes(b"a").u8(0).bytes(b"b").u8(0).0;
    assert_eq!(
        Changeset::decode_with_id_max_changes(&two, 2).unwrap(),
        Changeset::decode_with_id(&two).unwrap()
    );
    assert_eq!(
        Changeset::decode_with_id_max_changes(&two, 1),
        Err(DecodeError::TooManyChanges(1))
    );
    // The count says u32::MAX changes and no change follows. The limit error,
    // not UnexpectedEnd as in `rejects_lengths_past_the_end_without_allocating`,
    // shows that no change was read.
    assert_eq!(
        Changeset::decode_with_id_max_changes(&W::header(u32::MAX).0, 2),
        Err(DecodeError::TooManyChanges(2))
    );
}

#[test]
fn rejects_bad_paths() {
    let long = vec![b'a'; 4097];
    let deep = vec!["a"; 257].join("/");
    let cases: [(&[u8], PathError); 9] = [
        (b"", PathError::Empty),
        (b"/a", PathError::EmptyPart),
        (b"a/", PathError::EmptyPart),
        (b"a//b", PathError::EmptyPart),
        (b".", PathError::DotPart),
        (b"a/../b", PathError::DotPart),
        (b"a\0b", PathError::Nul),
        (&long, PathError::TooLong),
        (deep.as_bytes(), PathError::TooManyParts),
    ];
    for (bad, err) in cases {
        assert_eq!(
            decode_cs(&W::header(1).bytes(bad).u8(0)),
            Err(DecodeError::BadPath(err))
        );
    }
    let slash_name = W::default().u8(1).u32(1).bytes(b"a/b").u8(4).raw(&[9; 32]);
    assert_eq!(
        Tree::decode(&slash_name.0),
        Err(DecodeError::BadPath(PathError::Slash))
    );
}

#[test]
fn rejects_lengths_past_the_end_without_allocating() {
    let huge_parents = W::default().u8(1).u32(u32::MAX);
    assert_eq!(decode_cs(&huge_parents), Err(DecodeError::UnexpectedEnd));
    let huge_changes = W::header(u32::MAX);
    assert_eq!(decode_cs(&huge_changes), Err(DecodeError::UnexpectedEnd));
    let huge_message = W::default()
        .u8(1)
        .u32(0)
        .identity(b"A", b"a@x", 0, 0)
        .u8(0)
        .u32(u32::MAX);
    assert_eq!(decode_cs(&huge_message), Err(DecodeError::UnexpectedEnd));
}

#[test]
fn accepts_paths_at_the_limits_and_keeps_raw_bytes() {
    let longest = vec![b'a'; 4096];
    let deepest = vec!["a"; 256].join("/");
    let raw: [&[u8]; 6] = [
        &longest,
        deepest.as_bytes(),
        b".hidden/...",
        b"A",
        b"a",
        &[0xff, 0xfe],
    ];
    for p in raw {
        assert_eq!(RepoPath::new(p).unwrap().as_bytes(), p);
    }
}

#[test]
fn ids_parse_from_hex_and_bytes() {
    let id = root().id();
    assert_eq!(id.to_string().parse::<scion_core::ChangesetId>(), Ok(id));
    assert_eq!(
        scion_core::ChangesetId::try_from(id.as_bytes().as_slice()),
        Ok(id)
    );
    assert!("abc".parse::<scion_core::ChangesetId>().is_err());
    assert!(scion_core::ChangesetId::try_from([0u8; 31].as_slice()).is_err());
    let tree = tree();
    assert_eq!(
        Tree::decode_with_id(&tree.encode()),
        Ok((tree.clone(), tree.id()))
    );
}

/// The encoding is canonical for every entry kind: a tree encodes, decodes
/// and encodes again to the same bytes. The `FileEntry` fields are private
/// and `new` normalizes a gitlink's length to 0, so no two unequal values
/// can encode the same bytes.
#[test]
fn a_tree_of_every_kind_round_trips() {
    let name = |n: &str| PathElem::new(n).unwrap();
    let tree = tree();
    let bytes = tree.encode();
    let (decoded, id) = Tree::decode_with_id(&bytes).unwrap();
    assert_eq!(decoded, tree);
    assert_eq!(decoded.encode(), bytes);
    assert_eq!(decoded.id(), id);

    // A gitlink names no blob, so it encodes no length: `new` records 0
    // whatever the caller passes, and the decode of its bytes agrees.
    let gitlink = FileState::Gitlink {
        commit: GitOid::Sha1([0x44; 20]),
    };
    let plain = Tree::new(BTreeMap::from([(
        name("sub"),
        Entry::File(FileEntry::new(gitlink, 0)),
    )]))
    .expect("tree fits the format");
    let lied = Tree::new(BTreeMap::from([(
        name("sub"),
        Entry::File(FileEntry::new(gitlink, 5)),
    )]))
    .expect("tree fits the format");
    assert_eq!(plain, lied, "the length of a gitlink is not a field");
    assert_eq!(plain.encode(), lied.encode());
    assert_eq!(Tree::decode(&lied.encode()).unwrap(), plain);
}
