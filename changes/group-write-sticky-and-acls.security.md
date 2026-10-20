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
rest. On macOS the descriptor's extended ACL is read through one private
ffi module (acl_get_fd_np, the one place the workspace's unsafe-code
lint lets hold it): a descriptor with no ACL passes, one or more entries
refuses, a read that fails refuses. The workspace's unsafe_code lint
drops from forbid to deny, with the lint forbid at every crate root but
this one, and a test walks the crate's sources and refuses unsafe code
outside the ffi module. On this Mac the temp directories carry no ACLs
and the ambient deny-only ACLs of the system pass, as they grant
nothing; an ACL that grants anything is refused.
