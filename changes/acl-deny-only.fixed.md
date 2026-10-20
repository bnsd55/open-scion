# The macOS deny-only ACL is read through, and the refusal names its entry (#399)

## Fixed

The check for extended ACLs on macOS (PR #369) read only the *first* ACL
entry: a descriptor carrying the ambient home-folder ACL —
`group:everyone deny delete`, one entry — was refused as if it granted.
Every macOS home folder carries that shape, so the server refused any
data directory under `$HOME` (issue #399, a real Mac).

The walk now reads **every** entry (`acl_get_entry`, first to last) and
types each one (`acl_get_tag_type`):

- every entry deny-only → **accepted**: a deny entry grants nobody
  anything, so the mode bits stay the whole story;
- any allow entry, unknown tag, unreadable entry, or ACL that cannot be
  read or released → **refused**.

The refusal names the entry that granted ("an allow entry for account
501 grants access") and the platform's fix tool: macOS `chmod -N`,
Linux `setfacl -b` (unchanged).

Tests (macOS, each able to fail, proven red against the previous code
first): a file under a `chmod +a "everyone deny delete"` directory is
read; a file under a parent with a deny-only ACL passes
`check_ancestors`; an `everyone allow read` entry is refused and the
refusal names `chmod -N`.

The claim of PR #369 — "deny-only ACLs pass" — is now true.
Closes #399. Exposed by #345.
