# Support

What a reported problem gets, at what speed, and for how long. This is
the support policy the v0.4 gate asks for ([ROADMAP.md](ROADMAP.md)).
It describes what the project does today and names the pieces that
start with v0.4.

## Severity levels

| Level | It means | Example from this project |
|---|---|---|
| Critical | Data loss or a security boundary crossed, on the paths the docs promise | A read-rule bypass: a token whose access file denies a path can still read blobs under it, or a mount reads another principal's blobs |
| High | Data the user acknowledged is gone or wrong, or a crash a client can trigger | A lost acknowledged land: `Land` returned success, the caller switched work to the new head, and the next restart shows the old ref |
| Medium | A feature answers wrong or slowly under its documented limits, with a workaround | A blame call stops at its rename bound and reports fewer lines than the doc's example shows |
| Low | Cosmetic or documentation-only, no wrong answer | A status code's message text names the wrong limit |

## What each level gets

Today, with one supported line (the latest commit on `main`, per
[SECURITY.md](../SECURITY.md)), every fix lands on `main` and rides the
next release.

From v0.4 the split is: the current minor (release N) gets fixes for
all four levels — bugs, security problems, and data loss. The previous
minor (N-1) gets only critical and high security and data-loss fixes,
until it leaves the N-1 window described in
[compatibility.md](compatibility.md): clients, servers, caches and
daemons of version N and N-1 must work together, so a security or data
problem in N-1 is fixed in N-1 while that window is open. Medium and
low problems are fixed on the current minor only, and ride the normal
release train.

## How support ends

From v0.4, a release announces the end of its support in its release
notes: the notes say which release stops taking fixes, and the N-1
window closes when the next minor lands, per
[compatibility.md](compatibility.md). The version-matrix job
([.github/workflows/version-matrix.yml](../.github/workflows/
version-matrix.yml)) keeps the N-1 pairs tested while the window
is open.

## What 0.x means

There is no long-term-support minor in 0.x: every minor can change
what [stability.md](stability.md) does not pin, and only the latest
minor takes the full fix list. The first release that could carry an
LTS promise is v1.0, per the roadmap.
