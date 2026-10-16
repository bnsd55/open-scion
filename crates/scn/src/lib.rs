//! The library of `scn`: the pieces of the client that the tests drive,
//! and that the `scn` binary calls.
// SHORTCUT: these items were crate-private behind the `scn` binary, and
// their docs hold no `# Errors` sections; relaxed until the docs are
// written out (https://github.com/bnsd55/open-scion/issues/11).
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]

pub mod client;
pub mod cmd;
pub mod materialize;
pub mod vfs;
