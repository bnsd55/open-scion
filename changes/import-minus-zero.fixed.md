An import refused nothing when a git commit's date carried git's
unknown-offset zone `-0000`: the sign check folded the minus into a
zero and the commit imported with a `+0000` date, silently rewriting
history. Such a commit is refused now, on the author and the
committer line alike; every real zone still imports with its offset.
