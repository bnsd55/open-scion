Every server error that wraps another error returns it from
`source()`. `Error::Git` stops discarding its cause: it carries the
import's context and keeps the git library's own error as the source,
which type is no longer part of the API; `Error::Apply` forwards to the
apply error again, which `#[error(transparent)]` had dropped. The
internal-error log line walks the whole source chain, and a test fails
if a wrapping error's source goes missing.
