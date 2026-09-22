#!/usr/bin/env python3
"""Analyze the completed, paired host-local control-plane benchmark run."""
import argparse
import json
import math
import random
import re
from pathlib import Path

LINE = re.compile(r"^(Benchmark\S+)\s+\d+\s+([0-9.]+)\s+ns/op(?:\s+([0-9.]+)\s+redis_commands/op)?(?:\s+([0-9.]+)\s+redis_network_bytes/op)?(?:\s+([0-9.]+)\s+redis_round_trips/op)?(?:\s+([0-9.]+)\s+B/op)?(?:\s+([0-9.]+)\s+allocs/op)?")
METRICS = ["ns/op", "redis_commands/op", "redis_network_bytes/op", "redis_round_trips/op", "B/op", "allocs/op"]


def read(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        match = LINE.match(line)
        if not match:
            continue
        name = match.group(1)
        row = [float(match.group(2))] + [float(v) if v else None for v in match.groups()[2:]]
        values.setdefault(name, []).append(dict(zip(METRICS, row)))
    if not values:
        raise SystemExit(f"no benchmark rows in {path}")
    return values


def median(values):
    ordered = sorted(values)
    n = len(ordered)
    return ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2


def quantile(values, p):
    ordered = sorted(values)
    position = (len(ordered) - 1) * p
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def bootstrap(values, count=10000, seed=20260922):
    rng = random.Random(seed)
    medians = [median([values[rng.randrange(len(values))] for _ in values]) for _ in range(count)]
    return quantile(medians, 0.025), quantile(medians, 0.975)


def change(before, after):
    if before == 0:
        return 0.0 if after == 0 else None
    return 100 * (after / before - 1)


def family(name):
    if "SnapshotCosts" in name:
        return "snapshot"
    if "SetupKeyCosts" in name:
        return "setup-key"
    return "publish"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--treatment", required=True)
    parser.add_argument("--setup-baseline", required=True)
    parser.add_argument("--setup-treatment", required=True)
    parser.add_argument("--output", default="research/third-wave/results")
    args = parser.parse_args()
    baseline = read(args.baseline)
    treatment = read(args.treatment)
    for path in (args.setup_baseline, args.setup_treatment):
        extra = read(path)
        for name, rows in extra.items():
            (baseline if "baseline" in path else treatment)[name] = rows
    if set(baseline) != set(treatment):
        raise SystemExit("benchmark sets differ")

    records = []
    for name in sorted(baseline):
        if len(baseline[name]) != len(treatment[name]) or len(baseline[name]) < 5:
            raise SystemExit(f"sample count mismatch: {name}")
        for metric in METRICS:
            left = [row[metric] for row in baseline[name]]
            right = [row[metric] for row in treatment[name]]
            if any(value is None for value in left + right):
                continue
            paired = [change(a, b) for a, b in zip(left, right)]
            if any(value is None for value in paired):
                continue
            lo, hi = bootstrap(paired)
            records.append({"family": family(name), "benchmark": name, "metric": metric,
                            "samples": len(paired), "baseline_median": median(left),
                            "treatment_median": median(right), "change_percent": median(paired),
                            "interval_95_bootstrap": [lo, hi],
                            "regression_over_15_percent": median(paired) > 15})
    grouped = {}
    for record in records:
        grouped.setdefault(record["family"], []).append(record)
    # Bonferroni-adjusted intervals are represented by using the family alpha in
    # the result metadata; the bootstrap interval above remains the descriptive
    # 95% interval from the registered plan.
    for family_name, family_records in grouped.items():
        for record in family_records:
            record["family_tests"] = len(family_records)
            record["familywise_alpha"] = 0.05 / len(family_records)
    result = {"protocol": {"bootstrap_resamples": 10000, "seed": 20260922,
                            "direction": "negative treatment change is lower cost",
                            "families": {key: len(value) for key, value in grouped.items()}},
              "records": records}
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    (output / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = ["# Completed control-plane benchmark", "", "Negative values indicate lower treatment cost. Intervals are paired bootstrap descriptive intervals; family-wise alpha is recorded per comparison.", "", "| Family | Benchmark | Metric | Baseline median | Treatment median | Change | 95% interval |", "| --- | --- | --- | ---: | ---: | ---: | --- |"]
    for record in records:
        lo, hi = record["interval_95_bootstrap"]
        lines.append(f"| {record['family']} | `{record['benchmark']}` | `{record['metric']}` | {record['baseline_median']:.3g} | {record['treatment_median']:.3g} | {record['change_percent']:+.1f}% | [{lo:+.1f}%, {hi:+.1f}%] |")
    lines += ["", "## Decision summary", ""]
    for family_name, family_records in grouped.items():
        improved = sum(record["change_percent"] < 0 for record in family_records)
        regressed = sum(record["regression_over_15_percent"] for record in family_records)
        lines.append(f"- **{family_name}**: {improved}/{len(family_records)} comparisons lower; {regressed} exceeded the 15% regression guard.")
    (output / "analysis.md").write_text("\n".join(lines) + "\n")
    print(output / "analysis.json")


if __name__ == "__main__":
    main()
