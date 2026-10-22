# The metric surface's golden

The `/metrics` registry joins the machine-checked surfaces: a
checked-in golden (`crates/scion-proto/tests/metrics_v1.golden`) pins
every `scion_*` name the registry renders, its `# TYPE`, and its
labels and label values - names, types and labels are the surface,
the sample values are this run's counts and pin nothing. The test
exercises the registry end to end first (one request per RPC path,
one cancelled, one failed, every gauge and histogram), because
histograms render only once they counted. The stalled gauge hides
until a reply stalls, so its name, type and label set pin in a unit
test beside the render. A diff in the golden is a surface change: a
removed name or a changed type is a break, never a re-pin; re-pin an
added name on purpose with `SCION_UPDATE_METRICS_GOLDEN=1`.
