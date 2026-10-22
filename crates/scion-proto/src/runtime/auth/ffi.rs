//! The one module in the workspace where unsafe code is allowed, and
//! only the macOS POSIX-ACL calls of [`fd_acl`]: the platform's ACL
//! interface has no Rust binding, so the symbols are declared here and
//! wrapped in a safe function that returns owned data. Every unsafe
//! block carries the contract its call needs.

#![allow(unsafe_code)]

use std::os::raw::{c_int, c_void};

/// `ACL_TYPE_EXTENDED`, the only access-ACL kind macOS keeps on a
/// descriptor (`sys/acl.h`).
const ACL_TYPE_EXTENDED: c_int = 0x0100;
/// `ACL_FIRST_ENTRY`: the first walk position of [`acl_get_entry`]
/// (`sys/acl.h`).
const ACL_FIRST_ENTRY: c_int = 0;
/// `ACL_NEXT_ENTRY`: the next walk position, until the call fails
/// (`sys/acl.h`).
const ACL_NEXT_ENTRY: c_int = -1;
/// `ACL_EXTENDED_ALLOW`: the tag of an entry that grants something
/// (`sys/acl.h`).
const ACL_EXTENDED_ALLOW: c_int = 0x0001;
/// `ACL_EXTENDED_DENY`: the tag of an entry that only denies
/// (`sys/acl.h`).
const ACL_EXTENDED_DENY: c_int = 0x0002;
/// `ENOENT`: the errno `acl_get_fd_np` sets when the descriptor carries
/// no ACL at all.
const ENOENT: c_int = 2;
/// `EINVAL`: the errno `acl_get_entry` sets when `entry_id` is
/// `ACL_NEXT_ENTRY` and the last entry has already been returned — the
/// walk's end (`sys/acl.h`, the `acl_get_entry` man page).
const EINVAL: c_int = 22;

/// `ACL_UNDEFINED_ID`: the qualifier of an entry that names everyone
/// (`sys/acl.h`).
const ACL_UNDEFINED_ID: u32 = 0xffff_ffff;

unsafe extern "C" {
    fn acl_get_fd_np(fd: c_int, acl_type: c_int) -> *mut c_void;
    fn acl_get_entry(acl: *mut c_void, entry_id: c_int, entry_p: *mut *mut c_void) -> c_int;
    fn acl_get_tag_type(entry: *mut c_void, tag_type_p: *mut c_int) -> c_int;
    /// macOS names this call `acl_get_qualifier`: it returns the entry's
    /// qualifier as an owned `uid_t`/`guid_t`, or null when the entry
    /// names everyone (`sys/acl.h`; the `_tag_qualifier` spelling is
    /// FreeBSD's).
    fn acl_get_qualifier(entry: *mut c_void) -> *mut c_void;
    fn acl_free(obj_p: *mut c_void) -> c_int;
}

/// What the descriptor's extended ACL does.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Verdict {
    /// The descriptor carries no ACL at all.
    None,
    /// Every entry denies: nothing is granted, so the mode bits stay
    /// the whole story (the ambient home-folder shape of issue #399).
    DenyOnly,
    /// At least one entry grants, or a tag is one this check cannot
    /// name: the ACL reaches beyond the mode's bits. The text names
    /// the entry that granted.
    Granting(&'static str, Option<u32>),
}

/// The descriptor's extended-ACL verdict. A descriptor with no ACL
/// (`ENOENT`) is [`Verdict::None`]; every entry is walked and typed, a
/// walk or a type read that fails is [`Verdict::Granting`] — an ACL
/// this check cannot vouch for — and a single granting or unknown tag
/// makes the whole ACL [`Verdict::Granting`].
/// Names the kind of an entry the walk read, for the refusal message.
fn entry_kind(tag: c_int) -> &'static str {
    match tag {
        ACL_EXTENDED_ALLOW => "an allow entry",
        _ => "an entry of an unknown kind",
    }
}

/// The qualifier of the entry the walk read: the account it names,
/// when the entry names one. `None` when it names everyone, or when
/// the header hands back nothing to read.
fn entry_who(entry: *mut c_void) -> Option<u32> {
    // SAFETY: `entry` is the handle `acl_get_entry` returned from the
    // live `acl`; the call returns an owned qualifier the caller must
    // release with `acl_free`, or null when the entry names everyone.
    let who = unsafe { acl_get_qualifier(entry) };
    if who.is_null() {
        return None;
    }
    // SAFETY: `who` is the live pointer the call returned; it points at
    // one `u32` for as long as it is not freed.
    let named = unsafe { *(who.cast::<u32>()) };
    // SAFETY: the qualifier is released exactly once, and nothing reads
    // it afterwards.
    let freed = unsafe { acl_free(who) };
    (freed == 0 && named != ACL_UNDEFINED_ID).then_some(named)
}

/// What one `acl_get_entry` call did: returned an entry, ended the
/// walk, or failed. macOS signals the walk's end with -1 and `EINVAL`
/// (`entry_id` is `ACL_NEXT_ENTRY` and the last entry has already been
/// returned). The handle was obtained and the walk started at
/// `ACL_FIRST_ENTRY`, so `EINVAL` on a `ACL_NEXT_ENTRY` call can only
/// mean the end; any other failure must not pass for it.
///
/// `EINVAL` on the first call is a failure, not an empty ACL: measured
/// on macOS, an ACL added (`chmod +a`) and then removed (`chmod -a`)
/// leaves no handle at all — `acl_get_fd_np` answers `ENOENT`, the
/// no-ACL case — so a zero-entry ACL does not arise, and a first call
/// that fails has read nothing and cannot vouch for the rest.
fn walk_step(entry_id: c_int, got: c_int, errno: c_int) -> Step {
    if got == 0 {
        return Step::Entry;
    }
    if entry_id == ACL_NEXT_ENTRY && errno == EINVAL {
        return Step::End;
    }
    Step::Failed
}

/// One step of the `fd_acl` walk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    Entry,
    End,
    Failed,
}

/// One walked entry or the walk's end, as [`verdict_of`] sees it: the
/// step the call made, and — for an entry — the tag it typed with the
/// account it names, or `None` when the kind could not be read.
type Walked = (Step, Option<(c_int, Option<u32>)>);

/// The verdict a walk's steps carry: any entry that grants something
/// grants, a walk that failed grants (the check cannot vouch for what
/// it did not read), an entry whose kind could not be read grants, and
/// a walk that read every entry and found only deny entries denies
/// only. The one place the walk's decision lives.
fn verdict_of(steps: impl Iterator<Item = Walked>) -> Verdict {
    for (step, typed) in steps {
        match step {
            // A failed walk cannot vouch for the entries it did not
            // read, so the ACL is treated as granting: this check never
            // accepts what it cannot read whole.
            Step::Failed => return Verdict::Granting("an ACL whose walk failed", None),
            // The walk read every entry and typed them all; none grants.
            Step::End => return Verdict::DenyOnly,
            Step::Entry => match typed {
                None => return Verdict::Granting("an entry whose kind could not be read", None),
                Some((tag, who)) if tag != ACL_EXTENDED_DENY => {
                    return Verdict::Granting(entry_kind(tag), who);
                }
                Some((_, _)) => {}
            },
        }
    }
    Verdict::DenyOnly
}

pub(crate) fn fd_acl(fd: std::os::fd::RawFd) -> Verdict {
    // SAFETY: `acl_get_fd_np` reads the descriptor's ACL and returns a
    // handle the caller must release with `acl_free`; it writes nothing
    // the handle does not own. `fd` stays open for the duration of the
    // call, and `ACL_TYPE_EXTENDED` is the constant the header fixes.
    let acl = unsafe { acl_get_fd_np(fd, ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        // No ACL on the descriptor is the ENOENT case the man page names;
        // any other failure is an ACL this check cannot read.
        let no_acl = std::io::Error::last_os_error().raw_os_error() == Some(ENOENT);
        return if no_acl {
            Verdict::None
        } else {
            Verdict::Granting("an ACL that could not be read", None)
        };
    }
    // The FFI side of the walk: each call's step, and for an entry the
    // tag it typed and the account it names. The decision is the fold's
    // (`verdict_of`), not this loop's.
    let mut walked: Vec<Walked> = Vec::new();
    let mut entry_id = ACL_FIRST_ENTRY;
    loop {
        // SAFETY: `acl` is the handle `acl_get_fd_np` returned and is not
        // yet freed; `entry` becomes the out-pointer the header asks
        // for, taken with `&raw mut` so no borrow crosses the call; the
        // walk starts at `ACL_FIRST_ENTRY` and continues with
        // `ACL_NEXT_ENTRY` until the header says it is over.
        let mut entry: *mut c_void = std::ptr::null_mut();
        let got = unsafe { acl_get_entry(acl, entry_id, &raw mut entry) };
        // The errno the call just set: read it now, before anything
        // else can clobber it.
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        let step = walk_step(entry_id, got, errno);
        if step != Step::Entry {
            walked.push((step, None));
            break;
        }
        entry_id = ACL_NEXT_ENTRY;
        let mut tag: c_int = 0;
        // SAFETY: `entry` is the handle `acl_get_entry` just returned
        // from the live `acl`; `tag` is the out-pointer the header asks
        // for. Both stay alive for the duration of the call.
        let typed = unsafe { acl_get_tag_type(entry, &raw mut tag) };
        if typed != 0 {
            // An entry this check cannot type is one it cannot vouch
            // for.
            walked.push((Step::Entry, None));
            break;
        }
        // SAFETY: `entry` is the same live handle; the qualifier is
        // owned by the `acl` and released with it.
        let who = entry_who(entry);
        walked.push((Step::Entry, Some((tag, who))));
        if tag != ACL_EXTENDED_DENY {
            break;
        }
    }
    // SAFETY: `acl` is the same un-freed handle; `acl_free` releases it
    // exactly once on every path from here.
    let freed = unsafe { acl_free(acl) };
    if freed != 0 {
        return Verdict::Granting("an ACL that could not be released", None);
    }
    verdict_of(walked.into_iter())
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn a_call_that_returns_is_an_entry() {
        assert_eq!(walk_step(ACL_FIRST_ENTRY, 0, 0), Step::Entry);
        assert_eq!(walk_step(ACL_NEXT_ENTRY, 0, 0), Step::Entry);
    }

    #[test]
    fn the_walk_ends_on_einval_after_the_first_entry() {
        assert_eq!(walk_step(ACL_NEXT_ENTRY, -1, EINVAL), Step::End);
    }

    #[test]
    fn every_other_failure_fails_the_walk() {
        // An error on the first call is not the end: no entry was read.
        assert_eq!(walk_step(ACL_FIRST_ENTRY, -1, EINVAL), Step::Failed);
        // Any other errno is a failure, wherever the walk stands.
        assert_eq!(walk_step(ACL_NEXT_ENTRY, -1, ENOENT), Step::Failed);
        assert_eq!(walk_step(ACL_FIRST_ENTRY, -1, 0), Step::Failed);
    }

    #[test]
    fn a_failed_walk_grants_even_after_deny_entries() {
        let steps = vec![
            (Step::Entry, Some((ACL_EXTENDED_DENY, None))),
            (Step::Failed, None),
        ];
        assert_eq!(
            verdict_of(steps.into_iter()),
            Verdict::Granting("an ACL whose walk failed", None)
        );
    }

    #[test]
    fn a_walk_that_read_every_entry_and_found_only_deny_denies_only() {
        let steps = vec![
            (Step::Entry, Some((ACL_EXTENDED_DENY, Some(0)))),
            (Step::Entry, Some((ACL_EXTENDED_DENY, None))),
            (Step::End, None),
        ];
        assert_eq!(verdict_of(steps.into_iter()), Verdict::DenyOnly);
    }

    #[test]
    fn an_allow_entry_grants() {
        let steps = vec![(Step::Entry, Some((ACL_EXTENDED_ALLOW, Some(1))))];
        assert_eq!(
            verdict_of(steps.into_iter()),
            Verdict::Granting(entry_kind(ACL_EXTENDED_ALLOW), Some(1))
        );
    }

    #[test]
    fn an_entry_whose_kind_could_not_be_read_grants() {
        let steps = vec![
            (Step::Entry, Some((ACL_EXTENDED_DENY, None))),
            (Step::Entry, None),
        ];
        assert_eq!(
            verdict_of(steps.into_iter()),
            Verdict::Granting("an entry whose kind could not be read", None)
        );
    }
}
