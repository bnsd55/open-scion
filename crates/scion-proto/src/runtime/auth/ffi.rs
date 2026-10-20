//! The one module in the workspace where unsafe code is allowed, and
//! only the macOS POSIX-ACL calls of [`fd_has_acl`]: the platform's ACL
//! interface has no Rust binding, so the three symbols are declared here
//! and wrapped in a safe function that returns owned data. Every unsafe
//! block carries the contract its call needs.

#![allow(unsafe_code)]

use std::os::raw::{c_int, c_void};

/// `ACL_TYPE_EXTENDED`, the only access-ACL kind macOS keeps on a
/// descriptor (`sys/acl.h`).
const ACL_TYPE_EXTENDED: c_int = 0x0100;
/// `ACL_FIRST_ENTRY`: the first walk position of [`acl_get_entry`]
/// (`sys/acl.h`).
const ACL_FIRST_ENTRY: c_int = 0;
/// `ENOENT`: the errno `acl_get_fd_np` sets when the descriptor carries
/// no ACL at all.
const ENOENT: c_int = 2;

unsafe extern "C" {
    fn acl_get_fd_np(fd: c_int, acl_type: c_int) -> *mut c_void;
    fn acl_get_entry(acl: *mut c_void, entry_id: c_int, entry_p: *mut *mut c_void) -> c_int;
    fn acl_free(obj_p: *mut c_void) -> c_int;
}

/// Whether the descriptor carries an extended ACL: one or more entries of
/// any kind answer yes, a descriptor with no ACL (`ENOENT`) answers no,
/// and a call that fails - an ACL that cannot be read is one this check
/// cannot vouch for - also answers yes.
///
/// The bytes behind the returned `acl_t` are owned by the call and stay
/// opaque: only `acl_get_entry` and `acl_free` ever touch them, so the
/// safe wrapper can return a plain `bool`.
pub(crate) fn fd_has_acl(fd: std::os::fd::RawFd) -> bool {
    // SAFETY: `acl_get_fd_np` reads the descriptor's ACL and returns a
    // handle the caller must release with `acl_free`; it writes nothing
    // the handle does not own. `fd` stays open for the duration of the
    // call, and `ACL_TYPE_EXTENDED` is the constant the header fixes.
    let acl = unsafe { acl_get_fd_np(fd, ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        // No ACL on the descriptor is the ENOENT case the man page names;
        // any other failure is an ACL this check cannot read.
        let no_acl = std::io::Error::last_os_error().raw_os_error() == Some(ENOENT);
        return !no_acl;
    }
    // SAFETY: `acl` is the handle `acl_get_fd_np` returned and is not yet
    // freed; `entry` becomes the out-pointer the header asks for, taken
    // with `&raw mut` so no borrow crosses the call; `ACL_FIRST_ENTRY`
    // starts the walk.
    let mut entry: *mut c_void = std::ptr::null_mut();
    let got = unsafe { acl_get_entry(acl, ACL_FIRST_ENTRY, &raw mut entry) };
    // SAFETY: `acl` is the same un-freed handle; `acl_free` releases it
    // exactly once on every path from here.
    let freed = unsafe { acl_free(acl) };
    if freed != 0 {
        return true;
    }
    got == 0
}
