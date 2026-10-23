A `Log` page checks its deadline before its first entry and then once
every 16 entries, not before each one. A check reads the clock, a system
call where the clocksource has no user-space read (HPET), and on such a
host the check per entry made `scn log --path` of a 674-entry history in
tokio about 5% slower: 88.1 ms against 83.3 ms, with 777 clock reads in
the server against 149. A call that expires or goes still stops within
16 entries.
