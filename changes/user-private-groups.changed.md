A path that its group can write no longer stops the server, the daemon or
`scn` when, on Linux with glibc, the group is proved to hold no account
but the path's owner: the private group that Debian, Ubuntu and Fedora
give every account along with umask 002. The proof reads
`/etc/nsswitch.conf` first (`passwd`, `group` and `initgroups` from
`files`, optionally followed by `systemd`, and nothing else), then one
snapshot of the account database through glibc's NSS; any doubt refuses.
It covers the directories above a socket, key or data directory, the
data directory itself, and every file read through the checked
descriptor that must stay whole (public keys, certificates, CAs,
snapshots). On macOS, musl and every other target a group-writable path
stays refused. CI runs the suite under umask 002 (#302).
