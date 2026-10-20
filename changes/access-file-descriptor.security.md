The access file of `--access` holds bearer tokens and rules, and was
read by plain path: no check of the directories above it, no symlink
refusal, no owner or mode check, so a file another user holds - and can
replace at will - was taken as the access policy. The TLS private keys
of `--tls-key` (scion-server) and `--tls-client-key` (scion-cache,
scion-daemon) were read by plain path too. All of them are read through
one checked descriptor now, the read the ticket keys gained in #354,
moved into `scion_proto::runtime::auth` so every binary shares it: the
directories above are checked, the file is opened once with
`O_NOFOLLOW`, and kind, owner (the account that runs the process or
root), and mode run on the descriptor the bytes are read from - a
secret is read by its owner alone. The ticket keys keep their
public-key rule (no group or others write) through the same core. A
symlinked data directory is refused by kind now, not by the link's
mode: on macOS lchmod can give a symlink any mode, so the mode check
alone did not catch one. The ELOOP message names a link at the file
precisely and a loop on the way as what it is.

Files the binaries read at start that were checked and left as they
are, with why: the TLS certificate and client-CA files (public
material, but not nothing: a swapped client CA would change whose
certificates the server accepts, so they stay a listed gap for a
follow-up decision), the `scn --put` content file (the caller's own
content, kind-checked already), and the `scn` mount session file (not
a secret).
