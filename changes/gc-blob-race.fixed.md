The write path re-checks each blob's file inside its commit, so the
collector's sweep cannot leave a stored changeset naming a file it took.
PutBlob adds the repo's row on every store, not only when an earlier read
said the row was there, so a reply of ok leaves the blob named.
