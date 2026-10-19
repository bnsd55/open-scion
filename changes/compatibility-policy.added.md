A written compatibility policy: which component pairs work at versions
N and N-1 (the previous release, including the previous minor), what
"works" means per pair, and what a break needs in 0.x — a minor that
changes a wire, ticket or snapshot contract serves or reads the old
version beside the new one and drops it no earlier than the next
minor, while the store's migrations stay forward-only.
