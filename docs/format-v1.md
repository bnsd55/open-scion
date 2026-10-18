# Scion object format v1

This file is the contract. Object IDs are hashes of these bytes, so any change
here changes every ID. `crates/scion-core/tests/golden.rs` pins it.

## Primitives

```
u8, u32be, i32be, i64be   fixed width, big-endian
bytes(x)                  u32be(len(x)) || x
id                        32 raw bytes
```

## IDs

```
BlobId      = BLAKE3("scion\0blob\0v1\0"      || raw file bytes)
ChangesetId = BLAKE3("scion\0changeset\0v1\0" || ChangesetV1)
TreeId      = BLAKE3("scion\0tree\0v1\0"      || TreeV1)
```

The three ID kinds are distinct types in code and never mixed.

## ChangesetV1

```
ChangesetV1    = u8(1)
                 || u32be(n_parents) || ChangesetId*          in order, [0] = diff base
                 || IdentityTimeV1(author)
                 || u8(0 | 1) || [IdentityTimeV1(committer)]  1 = committer present
                 || bytes(message)
                 || u32be(n_changes) || PathChangeV1*         strictly increasing path bytes

IdentityTimeV1 = bytes(name) || bytes(email) || i64be(unix_secs) || i32be(tz_offset_minutes)

PathChangeV1   = bytes(path) || u8(tag) || payload
  tag 0 delete    -
  tag 1 regular   BlobId || u8(executable 0 | 1)
  tag 2 symlink   BlobId                                      blob holds the link target bytes
  tag 3 gitlink   GitOid                                      no blob
GitOid         = u8(1) || 20 bytes                            sha1
               | u8(2) || 32 bytes                            sha256
```

`changes` lists the files that differ from `parents[0]`. A changeset with no
parents is a root.

## TreeV1

```
TreeV1 = u8(1) || u32be(n_entries) || EntryV1*                strictly increasing name bytes
EntryV1 = bytes(name) || u8(tag) || payload
  tag 1 regular   BlobId || u8(executable 0 | 1) || u64be(len)
  tag 2 symlink   BlobId || u64be(len)
  tag 3 gitlink   GitOid
  tag 4 dir       TreeId
```

`len` is the byte length of the blob the entry names: the file's size for a
regular entry, the link target's length for a symlink. A gitlink names no
blob and carries no length; a directory's size is filesystem convention, so
its entry carries none. The tree ID covers `len`, so `len` cannot change
without changing the ID.

`TreeV1` is the public tree format that clients fetch. Which tree belongs to
which changeset is private derived data.

## Paths

A path is raw bytes split on `/`. Rejected:

- empty path, leading `/`, trailing `/`, empty part (`a//b`)
- a part equal to `.` or `..`
- any NUL byte
- more than 4096 bytes, or more than 256 parts

Case and Unicode forms are kept as they are. A tree entry name follows the same
rules and must not contain `/`.

## Decoder rules

The decoder accepts exactly one encoding per object. It rejects:

- trailing bytes after the object
- a version byte other than 1, an unknown tag, a flag byte other than 0 or 1,
  a gitlink algorithm byte other than 1 (sha1) or 2 (sha256)
- paths or names that are unsorted, duplicated, or break the path rules
- any length or count larger than the bytes that remain

So every accepted input re-encodes to the same bytes, and the ID of an
accepted input is the ID of its re-encoding.

The format checks syntax only. Whether a changeset applies to its parent tree
is checked when the change is applied. For example: a put at `a` and a put at
`a/b` in one changeset, a delete of a path that does not exist, or a delete in
a root.

A tree's `len` is checked where the blob is at hand. The builders of trees
(`CreateChangeset`, `land`, the git import) take it from the blob they just
stored, so a stored tree cannot disagree with its blobs. A reader that
fetches a blob checks the streamed length against the `len` its tree claimed
and rejects a mismatch.
