The storage engines come out of the server's public API. Error::Db,
Error::Pg and Error::Busy fold into one Error::Storage, which carries a
class (busy, out of space, or a failure of the engine's own) and keeps
the engine's own error as the source, where logs and tests read it. A
major bump of an embedded engine is then a private change, and callers
match classes, not engine internals. The gRPC codes and reply texts of
every class are unchanged.
