//! Property tests: every valid object round-trips, and the decoder accepts
//! exactly one encoding per object.

use proptest::collection::{btree_map, vec};
use proptest::option;
use proptest::prelude::*;
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileEntry, FileState, GitOid, IdentityTime, PathElem,
    RepoPath, Tree, TreeId,
};

fn part() -> impl Strategy<Value = Vec<u8>> {
    vec(
        any::<u8>().prop_filter("no NUL or '/'", |b| *b != 0 && *b != b'/'),
        1..6,
    )
    .prop_filter("not '.' or '..'", |p| p != b"." && p != b"..")
}

fn repo_path() -> impl Strategy<Value = RepoPath> {
    vec(part(), 1..4).prop_map(|parts| RepoPath::new(parts.join(&b'/')).unwrap())
}

fn file_state() -> impl Strategy<Value = FileState> {
    let blob = any::<[u8; 32]>().prop_map(BlobId::from_bytes);
    let git_oid = prop_oneof![
        any::<[u8; 20]>().prop_map(GitOid::Sha1),
        any::<[u8; 32]>().prop_map(GitOid::Sha256),
    ];
    prop_oneof![
        (blob.clone(), any::<bool>()).prop_map(|(content, executable)| FileState::Regular {
            content,
            executable
        }),
        blob.prop_map(|target| FileState::Symlink { target }),
        git_oid.prop_map(|commit| FileState::Gitlink { commit }),
    ]
}

fn identity() -> impl Strategy<Value = IdentityTime> {
    (
        vec(any::<u8>(), 0..8),
        vec(any::<u8>(), 0..8),
        any::<i64>(),
        any::<i32>(),
    )
        .prop_map(|(name, email, unix_secs, tz_offset_minutes)| IdentityTime {
            name,
            email,
            unix_secs,
            tz_offset_minutes,
        })
}

fn changeset() -> impl Strategy<Value = Changeset> {
    (
        vec(any::<[u8; 32]>().prop_map(ChangesetId::from_bytes), 0..3),
        identity(),
        option::of(identity()),
        vec(any::<u8>(), 0..16),
        btree_map(repo_path(), option::of(file_state()), 0..6),
    )
        .prop_map(|(parents, author, committer, message, changes)| Changeset {
            parents,
            author,
            committer,
            message,
            changes,
        })
}

fn tree() -> impl Strategy<Value = Tree> {
    let entry = prop_oneof![
        (file_state(), any::<u64>())
            .prop_map(|(state, len)| Entry::File(FileEntry::new(state, len))),
        any::<[u8; 32]>().prop_map(|b| Entry::Dir(TreeId::from_bytes(b))),
    ];
    btree_map(part().prop_map(|p| PathElem::new(p).unwrap()), entry, 0..6)
        .prop_map(|entries| Tree { entries })
}

proptest! {
    #[test]
    fn changeset_round_trips(cs in changeset()) {
        prop_assert_eq!(Changeset::decode(&cs.encode()).unwrap(), cs);
    }

    #[test]
    fn tree_round_trips(t in tree()) {
        prop_assert_eq!(Tree::decode(&t.encode()).unwrap(), t);
    }

    /// Change one byte of a valid encoding. If the result still decodes, it
    /// must be the canonical encoding of what it decodes to.
    #[test]
    fn mutated_changeset_is_rejected_or_canonical(cs in changeset(), at in any::<usize>(), byte in any::<u8>()) {
        let mut bytes = cs.encode();
        let at = at % bytes.len();
        bytes[at] = byte;
        if let Ok(decoded) = Changeset::decode(&bytes) {
            prop_assert_eq!(decoded.encode(), bytes);
        }
    }

    #[test]
    fn mutated_tree_is_rejected_or_canonical(t in tree(), at in any::<usize>(), byte in any::<u8>()) {
        let mut bytes = t.encode();
        let at = at % bytes.len();
        bytes[at] = byte;
        if let Ok(decoded) = Tree::decode(&bytes) {
            prop_assert_eq!(decoded.encode(), bytes);
        }
    }
}
