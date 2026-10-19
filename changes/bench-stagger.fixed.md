The benchmark's job starts are staggered 20 ms apart so a burst cannot
overflow a server's accept queue, and a run with SCION_KEEP_LOGS set keeps
every server and batch log instead of losing them with the temp directory.
