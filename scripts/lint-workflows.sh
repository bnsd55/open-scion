#!/bin/sh
# actionlint over .github/workflows/ (#436): a workflow that cannot parse
# must fail a check on the PR that carries it, not break main's next
# push-triggered run (#416 broke bench.yml with an unquoted colon, #419
# shipped a coverage.yml its pinned tool refused, and neither run caught
# either).
#
# The script needs: curl (or a PATH actionlint), shasum or sha256sum.
# It fetches a pinned actionlint release (its sha256 is pinned here and
# checked before it runs), unless the PATH already has one — a PATH
# actionlint of a different version still counts: the check is the
# workflow set, not the tool's build.
set -eu

# The pins. Bump a version together with its sha256 line. The version is
# the latest release, checked live when the pin is bumped.
ACTIONLINT_VERSION=1.7.12
ACTIONLINT_LINUX_AMD64=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
ACTIONLINT_DARWIN_ARM64=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f

note() { printf '%s %s\n' "lint-workflows:" "$*"; }
die() { printf '%s %s\n' "lint-workflows:" "$*" >&2; exit 1; }

# A pinned actionlint, downloaded only if the PATH has none.
ACTIONLINT=$(command -v actionlint || true)
if [ -z "$ACTIONLINT" ]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) asset=linux_amd64; want=$ACTIONLINT_LINUX_AMD64 ;;
    Darwin-arm64) asset=darwin_arm64; want=$ACTIONLINT_DARWIN_ARM64 ;;
    *) die "no pinned actionlint for $(uname -s)-$(uname -m)" ;;
  esac
  dir=${TMPDIR:-/tmp}/actionlint-$ACTIONLINT_VERSION-bin
  mkdir -p "$dir"
  tarball=$dir/actionlint.tar.gz
  if [ ! -x "$dir/actionlint" ]; then
    url="https://github.com/rhysd/actionlint/releases/download/v$ACTIONLINT_VERSION/actionlint_${ACTIONLINT_VERSION}_${asset}.tar.gz"
    curl -sL -o "$tarball" "$url"
    if command -v sha256sum > /dev/null; then
      got=$(sha256sum "$tarball" | cut -d' ' -f1)
    else
      got=$(shasum -a 256 "$tarball" | cut -d' ' -f1)
    fi
    [ "$got" = "$want" ] || die "actionlint $asset sha256 $got is not the pinned $want"
    tar xzf "$tarball" -C "$dir"
  fi
  ACTIONLINT=$dir/actionlint
fi

cd "$(dirname "$0")/.."
[ -d .github/workflows ] || die "no .github/workflows to lint"
note "actionlint $($ACTIONLINT -version | head -1 | cut -d' ' -f2) over .github/workflows/"
"$ACTIONLINT" -color .github/workflows/*.yml

# Every workflow carries a top-level permissions: block: a workflow
# without one runs its token with the default write rights, and
# Scorecard's Token-Permissions check scores the whole of main 0 for it
# (#518 shipped pg-disk-full.yml without one).
for workflow in .github/workflows/*.yml; do
  grep -q '^permissions:' "$workflow" || die "$workflow has no top-level permissions: block"
done

# Every services image carries a digest pin: a tag can move to new
# bytes, a digest cannot (production-audit.md section 9). The pin is
# `image: <name>:<tag>@sha256:<digest of the multi-arch index>`; the
# matrix jobs carry the digest in their include entries. An `image:`
# line inside a `services:` block without `@sha256:` fails here, on
# the PR that carries it.
for workflow in .github/workflows/*.yml; do
  awk -v w="$workflow" '
    /^[^ #]/            { in_services = 0 }   # a top-level key leaves the job
    /^  [^ ]/           { in_services = 0 }   # a job id or a sibling of one
    /^    [^ ]/         { if ($0 !~ /^    services: *$/) in_services = 0 }  # a sibling of services
    /^    services: *$/ { in_services = 1 }
    in_services && /^[ ]*image: / && $0 !~ /@sha256:/ {
      printf "lint-workflows: %s: a services image without a digest pin: %s\n", w, $0
      bad = 1
    }
    END { exit bad }
  ' "$workflow" || die "$workflow has a services image without a digest pin"
done

# One action, one version, everywhere: where an action's pin is a
# released version (its comment names `vX.Y.Z`), the same owner/repo
# must appear with one SHA under .github/, and one SHA must carry one
# version comment. A second version-pinned SHA of the same action is an
# upgrade that missed a file - the backup-n1.yml upload pinned a SHA
# that does not exist (a one-character typo of the real v4.6.2), and no
# run saw it because the workflow had never run. Actions whose SHA pins
# a branch on purpose (dtolnay's toolchain branches: stable, nightly,
# the MSRV) carry a ref comment, not a version, and pass. Offline: the
# pins and the comments are only compared against each other, never
# against the network.
python3 - <<'PYEOF'
import glob, re, sys

pins: dict[str, set[str]] = {}
comments: dict[str, set[str]] = {}
for path in sorted(glob.glob(".github/workflows/*.yml")):
    for line in open(path):
        m = re.search(r"uses:\s*([^\s@]+)@([0-9a-f]{40})(?:\s*#\s*(\S+))?", line)
        if not m:
            continue
        action, sha, comment = m.group(1), m.group(2), m.group(3)
        pins.setdefault(action, set()).add(sha)
        comments.setdefault(f"{action}@{sha}", set()).add(comment)

bad = 0
versioned = {
    action: {sha for sha in shas if any(c and c.startswith("v") and c[1].isdigit() for c in comments[f"{action}@{sha}"])}
    for action, shas in pins.items()
}
for action, shas in sorted(versioned.items()):
    if len(shas) > 1:
        for sha in sorted(shas):
            where = [
                p
                for p in sorted(glob.glob(".github/workflows/*.yml"))
                if any(
                    m.group(1) == action and m.group(2) == sha
                    for line in open(p)
                    if (m := re.search(r"uses:\s*([^\s@]+)@([0-9a-f]{40})", line))
                )
            ]
            print(
                f"lint-workflows: {action} runs with two version pins; "
                f"{sha} in {','.join(where)}"
            )
        bad = 1
for key, cs in sorted(comments.items()):
    real = {c for c in cs if c}
    if len(real) > 1:
        print(f"lint-workflows: {key} carries the comments {sorted(real)}")
        bad = 1
sys.exit(bad)
PYEOF
