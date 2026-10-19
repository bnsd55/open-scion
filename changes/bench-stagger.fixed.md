Job starts are staggered 10 ms apart so a burst cannot overflow a server's
accept queue, a run with SCION_KEEP_LOGS set keeps every server and batch
log plus the nstat listen-overflow counters around every batch instead of
losing them with the temp directory, and the stagger carries a SHORTCUT note
naming its ceiling and its upgrade path.
