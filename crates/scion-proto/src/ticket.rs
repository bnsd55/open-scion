//! Read tickets: signed permission to read the objects of one root tree of
//! one repo, for one principal, for a short time. The server mints one with
//! every `GetRootTree` reply ([`sign`]); every `GetObjects` call carries
//! one, and no token. The server and every cache tier check it and then
//! walk each requested path from the ticket's root tree ([`Walk`]), so an
//! object ID is never a permission by itself.
//!
//! The bytes (version 1): a version byte, the expiry as u64 Unix seconds,
//! big-endian, the 32-byte root tree, the repo and the principal, each with
//! a u16 length, and the Ed25519 signature. The signature covers [`PREFIX`]
//! and every byte before it, and is checked with
//! [`VerifyingKey::verify_strict`], trying each configured public key (at
//! most 2, for a rotation; there is no key id). A parser takes exact
//! lengths and refuses extra bytes.
//!
//! The server keeps the signing key. The tiers hold public keys only: the
//! daemon runs as its own user, so a shared MAC key there would let that
//! user mint tickets for every repo.

use std::collections::HashMap;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

use scion_core::{Entry, PathElem, RepoPath, Tree};

use crate::v1::ObjectKind;

/// How long a ticket is good for, from its minting.
pub const TICKET_LIFETIME_SECS: u64 = 15 * 60;

/// How far a verifier's clock may be behind or ahead of the signer's.
pub const TICKET_SKEW_SECS: u64 = 60;

/// Domain separation, so bytes signed for another purpose never verify as a
/// ticket with the same key.
const PREFIX: &[u8] = b"scion\0ticket\0v1\0";
const VERSION: u8 = 1;
const SIGNATURE_BYTES: usize = 64;
const ROOT_TREE_BYTES: usize = 32;

/// Why a ticket is refused. The text goes to the caller, so a client can
/// tell a ticket it may refresh from one it must not use again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TicketError {
    /// Not version 1, not the exact format, or not signed by a known key.
    Bad(&'static str),
    /// Its expiry passed, with the clock skew added.
    Expired,
    /// Its expiry is so far ahead that no server could have minted it now.
    OutsideClockWindow,
}

impl std::fmt::Display for TicketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TicketError::Bad(why) => write!(f, "ticket refused: {why}"),
            TicketError::Expired => f.write_str("ticket expired"),
            TicketError::OutsideClockWindow => f.write_str("ticket outside the clock window"),
        }
    }
}

/// A checked read ticket: which principal may read the objects of which
/// root tree of which repo, until when.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ticket {
    /// Unix seconds after which the ticket is refused.
    pub expires: u64,
    /// The root tree whose objects the ticket reaches.
    pub root_tree: [u8; 32],
    /// The repo of that tree.
    pub repo: String,
    /// The principal that may use it.
    pub principal: String,
    /// The wire bytes that were parsed, so a tier relays the ticket as it
    /// came.
    pub bytes: Vec<u8>,
}

/// Signs a ticket for the objects of a root tree: `repo` and `principal`,
/// expiring [`TICKET_LIFETIME_SECS`] after `now`.
///
/// # Errors
///
/// If `repo` is not a repo name, or a name is too long for its field.
pub fn sign(
    key: &SigningKey,
    repo: &str,
    principal: &str,
    root_tree: [u8; 32],
    now: u64,
) -> Result<Vec<u8>, String> {
    crate::check::name("repo", repo)?;
    if principal.is_empty() {
        return Err("principal name is empty".into());
    }
    let (repo_len, principal_len) = (u16::try_from(repo.len()), u16::try_from(principal.len()));
    let (Ok(repo_len), Ok(principal_len)) = (repo_len, principal_len) else {
        return Err("a name is too long for its field".into());
    };
    let mut body = Vec::with_capacity(
        1 + 8 + ROOT_TREE_BYTES + 4 + repo_len as usize + principal_len as usize,
    );
    body.push(VERSION);
    body.extend_from_slice(&(now + TICKET_LIFETIME_SECS).to_be_bytes());
    body.extend_from_slice(&root_tree);
    body.extend_from_slice(&repo_len.to_be_bytes());
    body.extend_from_slice(repo.as_bytes());
    body.extend_from_slice(&principal_len.to_be_bytes());
    body.extend_from_slice(principal.as_bytes());
    let signature = key.sign(&signed_bytes(&body)).to_bytes();
    body.extend_from_slice(&signature);
    Ok(body)
}

/// Seconds since the Unix epoch: the clock that tickets live by.
#[must_use]
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// The bytes that a signature covers: [`PREFIX`] and then the ticket bytes
/// before its signature.
fn signed_bytes(body: &[u8]) -> Vec<u8> {
    let mut signed = Vec::with_capacity(PREFIX.len() + body.len());
    signed.extend_from_slice(PREFIX);
    signed.extend_from_slice(body);
    signed
}

/// Checks the format, the signature and the clock of `bytes`, and returns
/// the ticket. Each of `keys` verifies it strictly, and one match is
/// enough.
///
/// # Errors
///
/// [`TicketError`] for bytes that no key signed, that are past their
/// expiry, that claim an expiry no server could have minted, or that do not
/// parse exactly.
///
/// # Panics
///
/// Never: the length check before the split makes the last slice exactly
/// [`SIGNATURE_BYTES`] long.
pub fn parse(bytes: &[u8], keys: &[VerifyingKey], now: u64) -> Result<Ticket, TicketError> {
    let (body, signature_bytes) = split(bytes)?;
    let signature = Signature::from_bytes(signature_bytes.try_into().unwrap());
    if !keys
        .iter()
        .any(|key| key.verify_strict(&signed_bytes(body), &signature).is_ok())
    {
        return Err(TicketError::Bad("unknown key or bad signature"));
    }
    ticket_of(body, bytes, now)
}

/// A ticket without checking who signed it, for a hop that only forwards
/// tickets and never vouches for them. Everything else is checked, so the
/// walk from its root tree still gates what a forwarder serves.
///
/// # Errors
///
/// [`TicketError::Bad`] for a ticket that is too short, of another
/// version, or with trailing bytes; [`TicketError::Expired`] or
/// [`TicketError::OutsideClockWindow`] when its clock window is off.
pub fn parse_unverified(bytes: &[u8], now: u64) -> Result<Ticket, TicketError> {
    let (body, _) = split(bytes)?;
    ticket_of(body, bytes, now)
}

/// Splits the signature off, or refuses a ticket too short to have one.
fn split(bytes: &[u8]) -> Result<(&[u8], &[u8]), TicketError> {
    // Version, expiry, root tree, two lengths with their names, signature.
    const MIN_BYTES: usize = 1 + 8 + ROOT_TREE_BYTES + 2 + 2 + SIGNATURE_BYTES;
    if bytes.len() < MIN_BYTES {
        return Err(TicketError::Bad("too short"));
    }
    Ok(bytes.split_at(bytes.len() - SIGNATURE_BYTES))
}

/// Reads the fields of the body of a ticket and checks its clock window.
fn ticket_of(mut read: &[u8], bytes: &[u8], now: u64) -> Result<Ticket, TicketError> {
    if take(&mut read, 1)?[0] != VERSION {
        return Err(TicketError::Bad("version"));
    }
    let expires = u64::from_be_bytes(take(&mut read, 8)?.try_into().unwrap());
    let root_tree = take(&mut read, ROOT_TREE_BYTES)?.try_into().unwrap();
    let repo = take_name(&mut read)?;
    let principal = take_name(&mut read)?;
    if !read.is_empty() {
        return Err(TicketError::Bad("trailing bytes"));
    }
    if now > expires + TICKET_SKEW_SECS {
        return Err(TicketError::Expired);
    }
    if expires > now.saturating_add(TICKET_LIFETIME_SECS + TICKET_SKEW_SECS) {
        return Err(TicketError::OutsideClockWindow);
    }
    Ok(Ticket {
        expires,
        root_tree,
        repo: repo.to_owned(),
        principal: principal.to_owned(),
        bytes: bytes.to_owned(),
    })
}

/// Takes `n` bytes, or refuses a ticket cut short.
fn take<'a>(read: &mut &'a [u8], n: usize) -> Result<&'a [u8], TicketError> {
    if read.len() < n {
        return Err(TicketError::Bad("too short"));
    }
    let (front, rest) = read.split_at(n);
    *read = rest;
    Ok(front)
}

/// Reads a u16 length, then that many bytes as UTF-8.
fn take_name<'a>(read: &mut &'a [u8]) -> Result<&'a str, TicketError> {
    let bytes = take(read, 2)?;
    let len = usize::from(u16::from_be_bytes(bytes.try_into().unwrap()));
    let bytes = take(read, len)?;
    std::str::from_utf8(bytes).map_err(|_| TicketError::Bad("name"))
}

/// What the walk of one path found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Walk {
    /// The path reaches the kind and ID that were asked for.
    Reached,
    /// The walk needs this tree, which the reader does not hold. `path` is
    /// the tree's path from the root, for the fetch that follows.
    Missing {
        /// The tree that the reader does not hold.
        id: [u8; 32],
        /// Its path from the root tree, empty for the root.
        path: Vec<u8>,
    },
    /// The path does not reach that kind and ID: no such entry, a file or
    /// symlink on the way, or another object at the end.
    Absent,
}

/// The walks of one `GetObjects` call, from one ticket's root tree. Each
/// tree is decoded once and checked against its ID, and its decoded bytes
/// count against the call's reply budget.
pub struct Walker {
    root: [u8; 32],
    trees: HashMap<[u8; 32], Tree>,
    decoded: usize,
}

impl Walker {
    /// A walk from the root tree `root`.
    #[must_use]
    pub fn new(root_tree: [u8; 32]) -> Self {
        Self {
            root: root_tree,
            trees: HashMap::new(),
            decoded: 0,
        }
    }

    /// The bytes of the trees that the walks decoded so far.
    #[must_use]
    pub fn decoded_bytes(&self) -> usize {
        self.decoded
    }

    /// Whether the walk of `path` from the root tree ends at `kind` `id`.
    /// Each tree on the way is checked against its ID. A tree must be a
    /// directory entry named by the path; a blob the blob of a regular file
    /// or of a symlink entry there. A walk never goes through a file or a
    /// symlink, and a gitlink ends it.
    ///
    /// `read_tree` returns the stored `TreeV1` bytes of a tree, or `None` if
    /// the reader does not hold it.
    ///
    /// # Errors
    ///
    /// Whatever `read_tree` fails with.
    pub fn reach<E>(
        &mut self,
        read_tree: &mut impl FnMut([u8; 32]) -> Result<Option<Vec<u8>>, E>,
        kind: ObjectKind,
        id: &[u8; 32],
        path: &[u8],
    ) -> Result<Walk, E> {
        // The empty path names the root tree itself. The reader is still
        // asked for it: a tier that does not hold the root tree must fetch
        // it before it can serve anything.
        if path.is_empty() {
            let reaches = match self.tree(self.root, read_tree)? {
                Held::Found(_) => kind == ObjectKind::Tree && id == &self.root,
                Held::Missing => {
                    return Ok(Walk::Missing {
                        id: self.root,
                        path: Vec::new(),
                    });
                }
                Held::Broken => false,
            };
            return Ok(if reaches { Walk::Reached } else { Walk::Absent });
        }
        let Ok(repo_path) = RepoPath::new(path) else {
            return Ok(Walk::Absent);
        };
        let parts: Vec<&[u8]> = repo_path.as_bytes().split(|byte| *byte == b'/').collect();
        let mut at = self.root;
        // The path of the tree that `at` names, from the root.
        let mut walked: Vec<u8> = Vec::with_capacity(path.len());
        for (n, part) in parts.iter().enumerate() {
            let last = n + 1 == parts.len();
            let tree = match self.tree(at, read_tree)? {
                Held::Found(tree) => tree,
                Held::Missing => {
                    return Ok(Walk::Missing {
                        id: at,
                        path: walked.clone(),
                    });
                }
                Held::Broken => return Ok(Walk::Absent),
            };
            let Ok(elem) = PathElem::new((*part).to_vec()) else {
                return Ok(Walk::Absent);
            };
            let Some(entry) = tree.entries.get(&elem) else {
                return Ok(Walk::Absent);
            };
            match entry {
                Entry::Dir(tree_id) => {
                    if last {
                        let reaches = kind == ObjectKind::Tree && tree_id.as_bytes() == id;
                        return Ok(if reaches { Walk::Reached } else { Walk::Absent });
                    }
                    if n > 0 {
                        walked.push(b'/');
                    }
                    walked.extend_from_slice(part);
                    at.copy_from_slice(tree_id.as_bytes());
                }
                Entry::File(state) => {
                    if !last {
                        // A walk never goes through a file, a symlink or a
                        // gitlink.
                        return Ok(Walk::Absent);
                    }
                    let reaches = kind == ObjectKind::Blob
                        && state.blob().is_some_and(|blob| blob.as_bytes() == id);
                    return Ok(if reaches { Walk::Reached } else { Walk::Absent });
                }
            }
        }
        unreachable!("the loop returns at every entry")
    }

    /// The decoded tree `id`, checked against its ID: from the cache, or
    /// read with `read_tree` first.
    fn tree<'a, E>(
        &'a mut self,
        id: [u8; 32],
        read_tree: &mut impl FnMut([u8; 32]) -> Result<Option<Vec<u8>>, E>,
    ) -> Result<Held<'a>, E> {
        if !self.trees.contains_key(&id) {
            let Some(bytes) = read_tree(id)? else {
                return Ok(Held::Missing);
            };
            // The bytes are not the tree that the walk asked for: it is not
            // reachable, and nothing may be served for it.
            let tree = match Tree::decode_with_id(&bytes) {
                Ok((tree, tree_id)) if tree_id.as_bytes() == &id => tree,
                _ => return Ok(Held::Broken),
            };
            self.decoded += bytes.len();
            self.trees.insert(id, tree);
        }
        Ok(Held::Found(self.trees.get(&id).unwrap()))
    }
}

/// What the reader had for one tree of the walk.
enum Held<'a> {
    /// The decoded tree.
    Found(&'a Tree),
    /// The reader does not hold it.
    Missing,
    /// It holds bytes that are not that tree.
    Broken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_core::{BlobId, Entry, FileState, GitOid, PathElem, Tree, TreeId};

    #[test]
    fn a_signed_ticket_parses_for_its_life() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = sign(&key, "r", "alice", [1; 32], 1_000).unwrap();
        let keys = [VerifyingKey::from(&key)];
        // Good when minted, at the end of its life, and within the skew.
        for now in [
            1_000,
            1_000 + TICKET_LIFETIME_SECS,
            1_000 + TICKET_LIFETIME_SECS + TICKET_SKEW_SECS,
        ] {
            let ticket = parse(&bytes, &keys, now).unwrap();
            assert_eq!(ticket.repo, "r");
            assert_eq!(ticket.principal, "alice");
            assert_eq!(ticket.root_tree, [1; 32]);
            assert_eq!(ticket.expires, 1_000 + TICKET_LIFETIME_SECS);
        }
        // Past the expiry and the skew.
        let late = 1_000 + TICKET_LIFETIME_SECS + TICKET_SKEW_SECS + 1;
        assert_eq!(parse(&bytes, &keys, late), Err(TicketError::Expired));
    }

    #[test]
    fn an_unverified_ticket_still_keeps_its_shape_and_its_clock() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = sign(&key, "r", "alice", [1; 32], 1_000).unwrap();
        // The bytes are read the same way, signature or not.
        let ticket = parse_unverified(&bytes, 1_000).unwrap();
        assert_eq!((ticket.repo.as_str(), ticket.root_tree), ("r", [1; 32]));
        // Too short, expired, of another version: refused all the same.
        assert_eq!(
            parse_unverified(&bytes[..20], 1_000),
            Err(TicketError::Bad("too short"))
        );
        // The ticket lives 900 s; past its expiry plus the skew it is out.
        assert_eq!(
            parse_unverified(&bytes, 1_000 + TICKET_LIFETIME_SECS + TICKET_SKEW_SECS + 1),
            Err(TicketError::Expired)
        );
        let mut other = bytes.clone();
        other[0] = 99;
        assert_eq!(
            parse_unverified(&other, 1_000),
            Err(TicketError::Bad("version"))
        );
    }

    #[test]
    fn a_ticket_that_starts_too_late_is_outside_the_clock_window() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = sign(&key, "r", "alice", [1; 32], 1_000).unwrap();
        // A verifier more than the skew behind the signer refuses it.
        let behind = 1_000 - TICKET_SKEW_SECS - 1;
        assert_eq!(
            parse(&bytes, &[VerifyingKey::from(&key)], behind),
            Err(TicketError::OutsideClockWindow)
        );
        // A forged ticket that claims a long life: the same refusal.
        let mut long = bytes.clone();
        long[1..9].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            parse(&long, &[VerifyingKey::from(&key)], 1_000),
            Err(TicketError::Bad("unknown key or bad signature"))
        );
    }

    #[test]
    fn only_bytes_a_key_signed_parse() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let other = SigningKey::from_bytes(&[8; 32]);
        let bytes = sign(&key, "r", "alice", [1; 32], 1_000).unwrap();
        let keys = [VerifyingKey::from(&key), VerifyingKey::from(&other)];
        // One of two keys is enough.
        assert!(parse(&bytes, &keys, 1_000).is_ok());
        // The ticket of one key does not verify with the other alone.
        assert_eq!(
            parse(&bytes, &[VerifyingKey::from(&other)], 1_000),
            Err(TicketError::Bad("unknown key or bad signature"))
        );
        // A changed body breaks the signature.
        let mut flipped = bytes.clone();
        let at = 1 + 8 + 32 + 2;
        flipped[at] ^= 0xff;
        assert!(parse(&flipped, &keys, 1_000).is_err());
        // Extra bytes at the end, with a signature that covers them: the
        // parser takes exact lengths, so it refuses the trailing byte.
        let body = &bytes[..bytes.len() - SIGNATURE_BYTES];
        let mut extra = body.to_vec();
        extra.push(0);
        extra.extend_from_slice(&key.sign(&signed_bytes(&extra)).to_bytes());
        assert_eq!(
            parse(&extra, &keys, 1_000),
            Err(TicketError::Bad("trailing bytes"))
        );
        // A cut below the smallest ticket, and another version.
        assert_eq!(
            parse(&[0; 100], &keys, 1_000),
            Err(TicketError::Bad("too short"))
        );
        // Another version, signed as if it were real: still refused.
        let mut v2 = vec![2u8];
        v2.extend_from_slice(&bytes[1..bytes.len() - SIGNATURE_BYTES]);
        v2.extend_from_slice(&key.sign(&signed_bytes(&v2)).to_bytes());
        assert_eq!(parse(&v2, &keys, 1_000), Err(TicketError::Bad("version")));
        // No keys at all: nothing verifies.
        assert!(parse(&bytes, &[], 1_000).is_err());
    }

    /// A tree with one regular file `leaf`, inside `a`, plus a symlink and a
    /// gitlink at the root. Returns the trees and the blobs they name.
    fn trees() -> (Tree, Tree, BlobId, BlobId) {
        let leaf = Tree {
            entries: [(
                PathElem::new("leaf").unwrap(),
                Entry::File(FileState::Regular {
                    content: BlobId::of(b"file\n"),
                    executable: false,
                }),
            )]
            .into(),
        };
        let root = Tree {
            entries: [
                (PathElem::new("a").unwrap(), Entry::Dir(leaf.id())),
                (
                    PathElem::new("link").unwrap(),
                    Entry::File(FileState::Symlink {
                        target: BlobId::of(b"leaf"),
                    }),
                ),
                (
                    PathElem::new("sub").unwrap(),
                    Entry::File(FileState::Gitlink {
                        commit: GitOid::Sha1([0; 20]),
                    }),
                ),
            ]
            .into(),
        };
        (leaf, root, BlobId::of(b"file\n"), BlobId::of(b"leaf"))
    }

    /// Reads the trees of `all`, and holds nothing else.
    fn reader(all: &[(TreeId, Tree)]) -> impl FnMut([u8; 32]) -> Result<Option<Vec<u8>>, ()> + '_ {
        move |id| {
            Ok(all
                .iter()
                .find(|(tree_id, _)| tree_id.as_bytes() == &id)
                .map(|(_, tree)| tree.encode()))
        }
    }

    #[test]
    fn a_walk_reaches_only_what_its_path_names() {
        let (leaf, root, file, link) = trees();
        let (root_bytes, leaf_bytes) = (root.encode().len(), leaf.encode().len());
        let all = [(root.id(), root.clone()), (leaf.id(), leaf.clone())];
        let mut walk = Walker::new(*root.id().as_bytes());
        let mut read = reader(&all);

        // The empty path names the root tree, and only as a tree.
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Tree, root.id().as_bytes(), b""),
            Ok(Walk::Reached)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, root.id().as_bytes(), b""),
            Ok(Walk::Absent)
        );
        // The tree at `a`, and the blob of the file at `a/leaf`.
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Tree, leaf.id().as_bytes(), b"a"),
            Ok(Walk::Reached)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, file.as_bytes(), b"a/leaf"),
            Ok(Walk::Reached)
        );
        // A symlink names its target blob, but a walk never goes through
        // one, and a gitlink is never a blob.
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, link.as_bytes(), b"link"),
            Ok(Walk::Reached)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, link.as_bytes(), b"link/x"),
            Ok(Walk::Absent)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, &[0; 32], b"sub"),
            Ok(Walk::Absent)
        );
        // Another kind or ID at the end, and a path with no entry.
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, leaf.id().as_bytes(), b"a"),
            Ok(Walk::Absent)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Tree, &[0; 32], b"a/leaf"),
            Ok(Walk::Absent)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Tree, &[0; 32], b"no-such"),
            Ok(Walk::Absent)
        );
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, file.as_bytes(), b"a/b/leaf"),
            Ok(Walk::Absent)
        );
        // Each tree on the way was decoded once.
        assert_eq!(walk.decoded_bytes(), root_bytes + leaf_bytes);
    }

    #[test]
    fn a_walk_that_misses_a_tree_names_it_with_its_path() {
        let (leaf, root, file, _link) = trees();
        // The root is held, the tree at `a` is not.
        let all = [(root.id(), root.clone())];
        let mut walk = Walker::new(*root.id().as_bytes());
        let mut read = reader(&all);
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, file.as_bytes(), b"a/leaf"),
            Ok(Walk::Missing {
                id: *leaf.id().as_bytes(),
                path: b"a".to_vec()
            })
        );
        // A root tree that the reader does not hold, for the empty path.
        let mut other = Walker::new([9; 32]);
        let mut read = reader(&all);
        assert_eq!(
            other.reach(&mut read, ObjectKind::Tree, &[0; 32], b""),
            Ok(Walk::Missing {
                id: [9; 32],
                path: Vec::new()
            })
        );
    }

    #[test]
    fn a_walk_refuses_a_tree_that_does_not_match_its_id() {
        let (_leaf, root, file, _link) = trees();
        // The reader holds the root, but sends the bytes of another tree
        // for everything else.
        let root_id = *root.id().as_bytes();
        let root_bytes = root.encode();
        let mut read = move |id: [u8; 32]| -> Result<Option<Vec<u8>>, ()> {
            if id == root_id {
                Ok(Some(root_bytes.clone()))
            } else {
                // The wrong tree for this ID.
                Ok(Some(Tree::default().encode()))
            }
        };
        let mut walk = Walker::new(root_id);
        assert_eq!(
            walk.reach(&mut read, ObjectKind::Blob, file.as_bytes(), b"a/leaf"),
            Ok(Walk::Absent)
        );
    }
}
