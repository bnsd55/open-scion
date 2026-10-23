A metadata write the database refuses names its SQLSTATE, and the
store answers by the code. A full disk is one of them and only one:
53100, which errcode_for_file_access gives every ENOSPC the database
hits, temp files included (elog.c). The mapping pins it to the fixed
`ResourceExhausted` shortage reply the blob path already promises.

The codes beside it stay internal on purpose, each with its reason
and each pinned by a unit test so a later widening cannot widen
silently: 53200 is out of memory, where a disk reply lies; 53000 is
the class's catch-all, too vague to promise a disk answer; 53400 is
a configured cap - a temp_file_limit or the replication slots - the
same refusal every time, so a retry-later reply would tell the
client to retry what cannot succeed; and 58030 is an io_error, a
failing device, which is the read-only-disk row's internal.
