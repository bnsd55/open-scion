scn no longer panics when the reader of its stdout leaves: a pipeline
like `scn blame main:README.md | head -1` closed the pipe at the first
line, and the next print panicked with "failed printing to stdout".
Every stdout write of scn now goes through one writer, and a broken
pipe ends the command with status 0 and no message — a closed stdout
is a normal end for a CLI, the reader chose to stop, and with
`set -o pipefail` a 141 exit would fail every benign `| head`. Any
other stdout error still fails the command. A test pipes `scn blame`
into a reader that leaves after one line and asserts the quiet exit.
The release smoke reads scn's output from files instead of pipes, so
its checks no longer race against a reader that leaves mid-write.
