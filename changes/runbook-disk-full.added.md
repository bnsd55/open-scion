The disk-full runbook (docs/operations.md, "Disk full"): the two alerts'
first move is the gc routine - a dry run, then a real run - then fsck,
then grow the volume if gc collected too little; a write that failed
out-of-space leaves nothing to repair, and writes resume with no restart.
The Proof test runs those steps in order on a real full disk (tmpfs):
fill, refusal, the gc dry run and real run through the actual command,
fsck clean, then a write that lands again with no restart and a clean
reopen.
