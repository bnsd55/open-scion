#!/usr/bin/env python3
"""Enforce the pinned Scorecard policy (.github/scorecard-policy.yml).

Usage: check-scorecard-policy.py RESULTS.JSON POLICY.YML

Fails when a floored check scores below its floor and no unexpired
exception covers it, when a floored check is missing from the results
entirely (Scorecard skipped it or it errored), and when any exception
has expired. Score -1 is Scorecard's "could not run" and fails like
any other score below a floor.
"""

import datetime
import json
import sys

import yaml


def today():
    return datetime.date.today()



def scores_from_json(path):
    """check name -> score, from Scorecard's JSON results."""
    with open(path) as f:
        return {check["name"]: check["score"]
                for check in json.load(f)["checks"]}


def main():
    scores = scores_from_json(sys.argv[1])
    with open(sys.argv[2]) as f:
        policy = yaml.safe_load(f)
    failures = []
    for check, floor in sorted(policy["floors"].items()):
        if check not in scores:
            failures.append(
                f"{check}: missing from the results (skipped or errored), "
                f"so its floor {floor} is unverified")
            continue
        score = scores[check]
        if score >= floor:
            continue
        label = ("could not run (-1)" if score == -1
                 else f"score {score}")
        waived = [
            exc for exc in policy.get("exceptions", [])
            if exc["check"] == check
            and datetime.date.fromisoformat(exc["expires"]) >= today()
        ]
        if waived:
            print(f"{check}: {label} is below its floor {floor}, waived by "
                  f"an exception of {waived[0]['owner']} until "
                  f"{waived[0]['expires']}")
        else:
            failures.append(
                f"{check}: {label} is below its floor {floor}, and no "
                f"unexpired exception covers it")
    for exc in policy.get("exceptions", []):
        if datetime.date.fromisoformat(exc["expires"]) < today():
            failures.append(
                f"{exc['check']}: the exception expired "
                f"{exc['expires']} but its check still needs it")
    if failures:
        for line in failures:
            print(f"policy violation: {line}", file=sys.stderr)
        return 1
    print(f"policy holds: {len(policy['floors'])} floors checked against "
          f"{len(scores)} scored checks, no violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
