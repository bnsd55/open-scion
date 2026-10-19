//! scn's one stdout writer, and the closed pipe that ends it.
//!
//! A CLI's stdout is often a pipe whose reader stops early:
//! `scn blame main:README.md | head -1` closes the pipe after the
//! first line, and scn's next write then fails with
//! `io::ErrorKind::BrokenPipe`. That is a normal end for the command,
//! not a failure — the reader got what it wanted. The default
//! `println!` panics on the error ("failed printing to stdout"), so
//! every stdout write of scn goes through this module, and a broken
//! pipe exits 0 with no message: scn's own work succeeded, and the
//! reader chose to stop reading. Not 141: the process is not killed
//! by SIGPIPE, it stops on its own, and a `set -o pipefail` shell
//! would turn every benign `| head` into a failure. `unsafe` is
//! forbidden in the workspace, so SIGPIPE keeps its default
//! disposition; the mapping happens on the error, not on the signal.
//! Any other stdout error still fails the command.

use std::io::{self, Write};

/// True when `err` is a closed pipe: the reader of scn's stdout went
/// away, which is how `| head` and `grep -q` end a pipeline.
pub(crate) fn broken_pipe(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}

#[cold]
fn stop_quietly() -> ! {
    std::process::exit(0)
}

/// Prints one line to stdout. The `outln!` macro calls this and
/// propagates the error with `?`; never call it directly, so every
/// print site reads like a `println!`. A broken pipe ends the process
/// quietly; any other error returns, and the caller fails the command
/// the usual way.
pub(crate) fn line(args: std::fmt::Arguments<'_>) -> io::Result<()> {
    match io::stdout().write_fmt(args) {
        Ok(()) => Ok(()),
        Err(e) if broken_pipe(&e) => stop_quietly(),
        Err(e) => Err(e),
    }
}

/// Writes `args` to a stdout writer that the caller holds, a lock or a
/// `BufWriter`. A broken pipe stops the process quietly; any other
/// error returns, and the caller fails the command the usual way.
pub(crate) fn write<W: Write>(out: &mut W, args: std::fmt::Arguments<'_>) -> io::Result<()> {
    match out.write_fmt(args) {
        Ok(()) => Ok(()),
        Err(e) if broken_pipe(&e) => stop_quietly(),
        Err(e) => Err(e),
    }
}

/// Writes raw bytes to a stdout writer, `scn cat`'s streamed chunks.
/// Same broken-pipe answer as [`write`](fn@write).
pub(crate) fn write_all<W: Write>(out: &mut W, buf: &[u8]) -> io::Result<()> {
    match out.write_all(buf) {
        Ok(()) => Ok(()),
        Err(e) if broken_pipe(&e) => stop_quietly(),
        Err(e) => Err(e),
    }
}

/// Flushes a stdout writer with the same broken-pipe answer as
/// [`write`](fn@write): a `BufWriter` holds whole lines back, so the reader can
/// leave while the buffer still holds them.
pub(crate) fn flush<W: Write>(out: &mut W) -> io::Result<()> {
    match out.flush() {
        Ok(()) => Ok(()),
        Err(e) if broken_pipe(&e) => stop_quietly(),
        Err(e) => Err(e),
    }
}

/// stdout's `println!` of scn: it prints through the module's `line`
/// function and propagates a non-broken-pipe error with `?`, so a closed
/// pipe ends the command quietly and any other stdout error fails it the
/// usual way. Like `println!`, each call prints one newline after its
/// arguments.
#[macro_export]
macro_rules! outln {
    () => {
        $crate::out::line(format_args!("\n"))?
    };
    ($($arg:tt)*) => {
        $crate::out::line(format_args!("{}\n", format_args!($($arg)*)))?
    };
}
