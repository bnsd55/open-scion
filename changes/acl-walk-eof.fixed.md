The check that reads an extended ACL on macOS treated any
`acl_get_entry` failure as the end of the walk, so a walk that failed
partway answered "deny only" and the ACL passed.

Found in the review of #345. The walk now decides each call with a
pure function: -1 with `EINVAL` on a `ACL_NEXT_ENTRY` call is the
walk's end (the macOS contract); any other failure refuses the path
("an ACL whose walk failed").

macOS only; every other target keeps refusing group-writable paths.
