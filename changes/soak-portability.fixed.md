The soak scripts ran on a default Mac and failed at once: `timeout`
does not exist there (Homebrew installs it as gtimeout) and nothing
checked, so every client, import, gc and fsck call died with "timeout:
command not found" and the writers landed 0 (issue #399, Run A). The
scripts now resolve TIMEOUT once in common.sh - timeout, else
gtimeout, else a named error - and call it everywhere, including the
NFS mount script.

The slow client never held a stream on any shell: its drain used
`read -N`, which exists in neither dash (Ubuntu's /bin/sh) nor bash
3.2 (macOS's /bin/sh), and the error vanished behind `|| true`. The
drain is now dd, one 4 KiB block at a time with a pause between -
portable across dash, bash 3.2 and Linux bash - shared by both soak
scripts in common.sh, and each drain logs bytes read and seconds
held. A drain that reads fewer bytes than the file has and does not
end inside a kill window fails the run: the reader quit while the
server was still serving. The old reader makes that check red, which
is the proof it works.
