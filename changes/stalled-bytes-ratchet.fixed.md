A streamed reply that stalled on a client that stopped reading now gives
its byte estimate back when its stream ends, instead of holding it until
the connection closes. A connection that serves many stalled replies no
longer ratchets toward its stalled-bytes close limit for bytes it no
longer holds. The new scion_stalled_reply_bytes gauge shows what the
stalled replies of the process hold now.
