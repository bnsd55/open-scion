The checkout benchmark publishes its first results: one bench.yml run
on main, the medians with min and max for the five states, and the
ratios between the tools. A cold checkout through the daemon is 9.3 to
22.1 times faster than git's clones and moves hundreds of times less;
git's mirror clone stays 1.7 times faster than the warm daemon, and the
numbers say why: the server side is idle, the gap is 50 fresh client
processes.
