A quota test no longer depends on the runner's TCP buffer sizes: its
unread reply is 64 MiB, about 4x the pipeline a probe must lose to, so
the slot is still held when the test asserts it.
