The operator's alert rules are versioned with the code they watch
(deploy/prometheus/openscion-alerts.yml)

Nineteen alerts over the metrics the server, the cache and the gc
collector export today - the server is down, RPC error rate, internal
errors, land errors and replans, latency p99, disk free (two levels),
blob sync saturation, the view memo bypassed, stalled replies, the
Postgres pool and transaction age and collector key, the tier's miss
rate, the gc run going stale, and Objects refusals. Severities are
docs/support.md's levels, and each alert's annotation says the
operator's first move, linked to the operations.md section that has
it. The Postgres group is true only for the Postgres shape.

ci.yml gains a promtool job: the pinned Prometheus release tarball
(v3.13.4, the current LTS line, its sha256 in the job) checks the
rules and runs openscion-alerts-test.yml, where every alert fires on
a crafted series and does not fire just below its threshold. The
daemon exports no metrics; an alert on it needs the gap closed first.
