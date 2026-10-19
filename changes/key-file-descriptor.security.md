A ticket key was checked by path and then read through the same
unresolved path again: a directory on the checked path that another
user could change let them swap the file between the two steps, and a
key file owned by another user passed the checks at all, so a public
key they hold could be fed in as the old key of a rotation. The key
now opens once, through the path the ancestor check resolved and with
a symlink at the end refused, and the kind, owner (the user that runs
the server or root), and mode checks run on the open descriptor that
the key is read from. A symlinked data directory is refused by kind
now: a symlink's mode is not a rule (on macOS lchmod can set any mode
on one), so the mode check alone did not catch it.
