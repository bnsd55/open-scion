//! The one module in the workspace where unsafe code is allowed, and
//! only for two things. On macOS, the POSIX-ACL calls of `fd_acl`: the
//! platform's ACL interface has no Rust binding, so the symbols are
//! declared here. On Linux with glibc, the account database through
//! NSS (`getpwent_r`, `getgrent_r`, `getgrouplist`, which the `libc`
//! crate binds there), for the proof that a group is its owner's alone.
//! Each is wrapped in a safe function that returns owned data, and every
//! unsafe block carries the contract its call needs.

#![allow(unsafe_code)]

#[cfg(target_os = "macos")]
use std::os::raw::{c_int, c_void};

#[cfg(target_os = "macos")]
/// `ACL_TYPE_EXTENDED`, the only access-ACL kind macOS keeps on a
/// descriptor (`sys/acl.h`).
const ACL_TYPE_EXTENDED: c_int = 0x0100;
#[cfg(target_os = "macos")]
/// `ACL_FIRST_ENTRY`: the first walk position of [`acl_get_entry`]
/// (`sys/acl.h`).
const ACL_FIRST_ENTRY: c_int = 0;
#[cfg(target_os = "macos")]
/// `ACL_NEXT_ENTRY`: the next walk position, until the call fails
/// (`sys/acl.h`).
const ACL_NEXT_ENTRY: c_int = -1;
#[cfg(target_os = "macos")]
/// `ACL_EXTENDED_ALLOW`: the tag of an entry that grants something
/// (`sys/acl.h`).
const ACL_EXTENDED_ALLOW: c_int = 0x0001;
#[cfg(target_os = "macos")]
/// `ACL_EXTENDED_DENY`: the tag of an entry that only denies
/// (`sys/acl.h`).
const ACL_EXTENDED_DENY: c_int = 0x0002;
#[cfg(target_os = "macos")]
/// `ENOENT`: the errno `acl_get_fd_np` sets when the descriptor carries
/// no ACL at all.
const ENOENT: c_int = 2;

#[cfg(target_os = "macos")]
/// `ACL_UNDEFINED_ID`: the qualifier of an entry that names everyone
/// (`sys/acl.h`).
const ACL_UNDEFINED_ID: u32 = 0xffff_ffff;

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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
    let verdict = Verdict::DenyOnly;
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

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) use nss::{group_entries, group_list, passwd_entries};

/// The account database through glibc's NSS, read whole into owned data
/// for the proof in `accounts`. glibc only: these are its reentrant
/// calls, and the `libc` crate binds them there.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod nss {
    use std::ffi::{CStr, CString, c_char, c_int};
    use std::mem::MaybeUninit;
    use std::sync::{Mutex, PoisonError};

    /// The largest buffer one entry may need. An entry that still answers
    /// `ERANGE` at this size is one the proof cannot read, so it refuses.
    const MAX_BUFFER: usize = 1 << 20;
    /// The most groups `getgrouplist` may give one account.
    const MAX_GROUPS: usize = 1 << 16;

    /// `getpwent_r` and `getgrent_r` move one cursor per process, which
    /// `setpwent` and `setgrent` rewind: one walk at a time, so two walks
    /// of this process never share a cursor.
    static WALK: Mutex<()> = Mutex::new(());

    /// One account: its name as bytes, its uid and its primary gid.
    pub(crate) struct Passwd {
        pub(crate) name: Vec<u8>,
        pub(crate) uid: u32,
        pub(crate) gid: u32,
    }

    /// One group: its name as bytes, its gid, and the names it lists.
    pub(crate) struct Group {
        pub(crate) name: Vec<u8>,
        pub(crate) gid: u32,
        pub(crate) members: Vec<Vec<u8>>,
    }

    /// What one call of a walk answered.
    enum Step<T> {
        Entry(T),
        End,
        /// `ERANGE`: the same entry again, with a larger buffer.
        Grow,
        Failed(c_int),
    }

    /// Calls `next` until the walk ends, with a buffer that doubles on
    /// `ERANGE` up to [`MAX_BUFFER`].
    fn walk<T>(call: &str, mut next: impl FnMut(&mut [u8]) -> Step<T>) -> Result<Vec<T>, String> {
        let mut entries = Vec::new();
        let mut buf = vec![0u8; 1024];
        loop {
            match next(&mut buf) {
                Step::Entry(entry) => entries.push(entry),
                Step::End => return Ok(entries),
                Step::Grow if buf.len() < MAX_BUFFER => buf.resize(buf.len() * 2, 0),
                Step::Grow => {
                    return Err(format!(
                        "{call} needs a buffer past {} KiB for one entry",
                        MAX_BUFFER / 1024
                    ));
                }
                Step::Failed(errno) => {
                    return Err(format!(
                        "{call} failed: {}",
                        std::io::Error::from_raw_os_error(errno)
                    ));
                }
            }
        }
    }

    /// Every account `getpwent_r` gives, in order.
    pub(crate) fn passwd_entries() -> Result<Vec<Passwd>, String> {
        let _walk = WALK.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: `setpwent` takes no argument and rewinds this process's
        // passwd cursor; `WALK` keeps every other walk of this process out
        // until `endpwent` below.
        unsafe { libc::setpwent() };
        let entries = walk("getpwent_r", |buf| {
            let mut entry = MaybeUninit::<libc::passwd>::uninit();
            let mut result: *mut libc::passwd = std::ptr::null_mut();
            // SAFETY: `getpwent_r` fills `entry`, writes the entry's
            // strings into `buf` and never past `buf.len()` bytes, and
            // sets `result` to `entry` or to null. All three outlive the
            // call, and `WALK` holds the cursor it moves.
            let rc = unsafe {
                libc::getpwent_r(
                    entry.as_mut_ptr(),
                    buf.as_mut_ptr().cast::<c_char>(),
                    buf.len(),
                    &raw mut result,
                )
            };
            match rc {
                0 if result.is_null() => Step::End,
                0 => {
                    // SAFETY: on success `result` points at `entry`, which
                    // the call filled; its strings live in `buf`, which is
                    // still borrowed here.
                    let pw = unsafe { &*result };
                    // SAFETY: `pw_name` is a NUL-terminated string inside
                    // `buf`, as glibc returns every entry.
                    let name = unsafe { CStr::from_ptr(pw.pw_name) }.to_bytes().to_vec();
                    Step::Entry(Passwd {
                        name,
                        uid: pw.pw_uid,
                        gid: pw.pw_gid,
                    })
                }
                libc::ENOENT => Step::End,
                libc::ERANGE => Step::Grow,
                errno => Step::Failed(errno),
            }
        });
        // SAFETY: `endpwent` takes no argument and closes the cursor that
        // `setpwent` opened; `WALK` is still held.
        unsafe { libc::endpwent() };
        entries
    }

    /// Every group `getgrent_r` gives, in order, with its member names.
    pub(crate) fn group_entries() -> Result<Vec<Group>, String> {
        let _walk = WALK.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: `setgrent` takes no argument and rewinds this process's
        // group cursor; `WALK` keeps every other walk of this process out
        // until `endgrent` below.
        unsafe { libc::setgrent() };
        let entries = walk("getgrent_r", |buf| {
            let mut entry = MaybeUninit::<libc::group>::uninit();
            let mut result: *mut libc::group = std::ptr::null_mut();
            // SAFETY: `getgrent_r` fills `entry`, writes the entry's
            // strings and its member array into `buf` and never past
            // `buf.len()` bytes, and sets `result` to `entry` or to null.
            // All three outlive the call, and `WALK` holds the cursor.
            let rc = unsafe {
                libc::getgrent_r(
                    entry.as_mut_ptr(),
                    buf.as_mut_ptr().cast::<c_char>(),
                    buf.len(),
                    &raw mut result,
                )
            };
            match rc {
                0 if result.is_null() => Step::End,
                0 => {
                    // SAFETY: on success `result` points at `entry`, which
                    // the call filled; its strings and member array live in
                    // `buf`, which is still borrowed here.
                    let gr = unsafe { &*result };
                    // SAFETY: `gr_name` is a NUL-terminated string inside
                    // `buf`.
                    let name = unsafe { CStr::from_ptr(gr.gr_name) }.to_bytes().to_vec();
                    let mut members = Vec::new();
                    let mut at = gr.gr_mem;
                    while !at.is_null() {
                        // SAFETY: `gr_mem` is an array of pointers inside
                        // `buf` that ends with a null pointer, and `at` has
                        // not passed that null.
                        let member = unsafe { *at };
                        if member.is_null() {
                            break;
                        }
                        // SAFETY: each member is a NUL-terminated string
                        // inside `buf`.
                        members.push(unsafe { CStr::from_ptr(member) }.to_bytes().to_vec());
                        // SAFETY: `member` was not the null that ends the
                        // array, so the next slot is still inside it.
                        at = unsafe { at.add(1) };
                    }
                    Step::Entry(Group {
                        name,
                        gid: gr.gr_gid,
                        members,
                    })
                }
                libc::ENOENT => Step::End,
                libc::ERANGE => Step::Grow,
                errno => Step::Failed(errno),
            }
        });
        // SAFETY: `endgrent` takes no argument and closes the cursor that
        // `setgrent` opened; `WALK` is still held.
        unsafe { libc::endgrent() };
        entries
    }

    /// The groups `getgrouplist` gives the account `name` whose primary
    /// group is `gid`: its primary group and every supplementary one that
    /// the `initgroups` sources name.
    pub(crate) fn group_list(name: &[u8], gid: u32) -> Result<Vec<u32>, String> {
        let name = CString::new(name).map_err(|_| "an account name holds a NUL byte".to_owned())?;
        let mut room: usize = 64;
        loop {
            let mut groups: Vec<libc::gid_t> = vec![0; room];
            let mut count = c_int::try_from(room).map_err(|_| "too many groups".to_owned())?;
            // SAFETY: `getgrouplist` writes at most `count` gids into
            // `groups`, which holds `room` of them, the value `count`
            // starts with, and sets `count` to the number it found; `name`
            // is NUL-terminated and outlives the call.
            let rc = unsafe {
                libc::getgrouplist(name.as_ptr(), gid, groups.as_mut_ptr(), &raw mut count)
            };
            let found = usize::try_from(count)
                .map_err(|_| "getgrouplist answered a negative count".to_owned())?;
            if rc >= 0 && found <= room {
                groups.truncate(found);
                return Ok(groups);
            }
            if found <= room || found > MAX_GROUPS {
                return Err(format!(
                    "getgrouplist could not list the groups of {} ({found} groups)",
                    String::from_utf8_lossy(name.as_bytes())
                ));
            }
            room = found;
        }
    }
}
