A dry run of the release walk fixed the guide where it read wrong: the
milestone check passes while the release's own tracking issue is open,
the unreleased note's fate on release day is stated, the version step
says the first release checks the five versions equal instead of
bumping them, the release notes name the command that extracts the
version's changelog section alone, and the tag check runs locally
before the push, not only on the remote after it.
