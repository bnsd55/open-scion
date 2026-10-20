#!/usr/bin/env python3
"""Enforce the pinned Scorecard policy (.github/scorecard-policy.yml).

Usage: check-scorecard-policy.py RESULTS.SARIF POLICY.YML

Fails when a floored check scores below its floor and no unexpired
exception covers it, and when any exception has expired. The SARIF
reports only checks with findings; a floored check the SARIF does not
name counts as 10.
"""

import datetime
import json
import re
import sys

import yaml


def scores_from_sarif(path):
    """check name -> score, from the Scorecard SARIF's results."""
    with open(path) as f:
        runs = json.load(f)["runs"]
    out = {}
    for run in runs:
        rules = run.get("tool", {}).get("driver", {}).get("rules", [])
        for result in run.get("results", []):
            rule = {}
            if result.get("ruleIndex") is not None and 0 <= result[
                    "ruleIndex"] < len(rules):
                rule = rules[result["ruleIndex"]]
            name = result.get("ruleId") or rule.get("name")
            match = re.search(r"score is (\d+)",
                              result.get("message", {}).get("text", ""))
            if name and match:
                out[name] = int(match.group(1))
    return out


def today():
    return datetime.date.today()


def main():
    scores = scores_from_sarif(sys.argv[1])
    with open(sys.argv[2]) as f:
        policy = yaml.safe_load(f)
    failures = []
    for check, floor in sorted(policy["floors"].items()):
        score = scores.get(check, 10)
        if score >= floor:
            continue
        waived = [
            exc for exc in policy.get("exceptions", [])
            if exc["check"] == check
            and datetime.date.fromisoformat(exc["expires"]) >= today()
        ]
        if waived:
            print(f"{check}: score {score} < floor {floor}, waived by an "
                  f"exception of {waived[0]['owner']} "
                  f"until {waived[0]['expires']}")
        else:
            failures.append(
                f"{check}: score {score} is below its floor {floor}, and "
                f"no unexpired exception covers it")
    for exc in policy.get("exceptions", []):
        if datetime.date.fromisoformat(exc["expires"]) < today():
            failures.append(
                f"{exc['check']}: the exception expired "
                f"{exc['expires']} but its check still needs it")
    if failures:
        for line in failures:
            print(f"policy violation: {line}", file=sys.stderr)
        return 1
    print(f"policy holds: {len(policy['floors'])} floors checked, "
          f"{len(scores)} checks scored, no violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
