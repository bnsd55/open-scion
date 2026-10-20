The check that keeps another user from swapping a path on its way had
two gaps. The sticky bit excused a group-writable directory as it
excuses the /tmp shape, so 01770 and 02770 passed: a sticky bit bounds
who may remove a name, not who may add one, so a group that can write
the directory can still plant and swap its own entries. A
group-writable ancestor is refused now whatever the sticky bit does;
the sticky bit keeps its one excuse, the shape where others can write
too (01777), where the others-write rule is the real wall. And the
checks read only the mode bits, not POSIX ACLs, so an access ACL that
grants another account what the mode does not passed everywhere: now
the checked read of a secret or a shared file refuses one whose access
ACL is not the exact minimal form (version 2, one USER_OBJ, one
GROUP_OBJ, one OTHER, undefined ids, perms equal to the mode's bits),
an ancestor directory with such an ACL or with any default ACL is
refused on the walk, and an ACL that cannot be read is refused with the
rest. On Linux only: macOS ACLs are unchecked, they need acl_get_fd_np,
an unsafe FFI call, and the workspace forbids unsafe_code.
