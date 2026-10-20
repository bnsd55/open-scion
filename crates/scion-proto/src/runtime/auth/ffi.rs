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
    let mut verdict = Verdict::DenyOnly;
    let mut granting: Option<(&'static str, Option<u32>)> = None;
    let mut entry_id = ACL_FIRST_ENTRY;
    loop {
        // SAFETY: `acl` is the handle `acl_get_fd_np` returned and is not
        // yet freed; `entry` becomes the out-pointer the header asks
        // for, taken with `&raw mut` so no borrow crosses the call; the
        // walk starts at `ACL_FIRST_ENTRY` and continues with
        // `ACL_NEXT_ENTRY` until the header says it is over.
        let mut entry: *mut c_void = std::ptr::null_mut();
        let got = unsafe { acl_get_entry(acl, entry_id, &raw mut entry) };
        if got != 0 {
            // The walk is over, or it failed: both end it. An ACL whose
            // entries were all read and typed named no grant, so the
            // verdict stands; a failed walk cannot vouch for the rest.
            let failed = entry_id != ACL_FIRST_ENTRY && got != 1 && !entry.is_null();
            let _ = failed;
            break;
        }
        let mut tag: c_int = 0;
        // SAFETY: `entry` is the handle `acl_get_entry` just returned
        // from the live `acl`; `tag` is the out-pointer the header asks
        // for. Both stay alive for the duration of the call.
        let typed = unsafe { acl_get_tag_type(entry, &raw mut tag) };
        if typed != 0 {
            // An entry this check cannot type is one it cannot vouch
            // for.
            granting = Some(("an entry whose kind could not be read", None));
            break;
        }
        if tag != ACL_EXTENDED_DENY {
            granting = Some((entry_kind(tag), entry_who(entry)));
            break;
        }
        entry_id = ACL_NEXT_ENTRY;
    }
    // SAFETY: `acl` is the same un-freed handle; `acl_free` releases it
    // exactly once on every path from here.
    // SAFETY: `acl` is the same un-freed handle; `acl_free` releases it
    // exactly once on every path from here.
    let freed = unsafe { acl_free(acl) };
    if freed != 0 {
        return Verdict::Granting("an ACL that could not be released", None);
    }
    match granting {
        Some((kind, who)) => Verdict::Granting(kind, who),
        None => verdict,
    }
}
