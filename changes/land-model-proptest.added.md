The land model check becomes a randomized script: a proptest strategy
generates a sequence of operations against the real `scion-server` —
lands from four callers on two refs, retries with the same request key,
twins of one key in flight together, calls dropped in flight, a SIGKILL
(plain, or at a pending land), the collector beside the live server,
and bursts of up to eight lands on one ref at once — and a plain
in-memory model checks the result: each ref's first-parent history is
one chain, every acknowledged land is in it exactly once, no land is
applied twice, no key's state regresses, a kill kept every land that
had answered, and `fsck` sees no damage. On a failure, proptest shrinks
the script to the smallest one that still fails. A checked-in seed
corpus of seven scripts runs first and reads like sentences; CI runs
one random case per pull request, and the nightly `land-model` job
raises it to eight.

A conflict reply is judged against the head the server saw when it
decided, which each call records beside its reply (a batch's reply
records the head after the batch — an upper bound, so a pass there is
weaker, never a false accusation) — a bogus conflict whose path a
later land moved cannot slip through. A CONFLICT with no paths is
allowed when the detail says why, as the protocol provides.
