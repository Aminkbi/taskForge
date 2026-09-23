#!/usr/bin/env python3
"""Validate and summarize the latest-state TaskForge control comparison."""

import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "noisy-neighbor": "taskforge-no-fairness",
    "delayed-backlog": "taskforge-no-admission",
    "hot-dependency": "taskforge-no-dependency-budget",
    "retry-storm": "taskforge-no-adaptive",
}
METRICS = ("p99_ms", "throughput_per_second", "jain_fairness", "slo_violations")
SOURCE_COMMIT = "0cc8a5699df839946f9aa4dcbc5cfbe05653ca57"
SEEDS = (20260923, 20260924)
REPETITIONS = 3


def digest(data):
    return hashlib.sha256(data).hexdigest()


def median(values):
    return statistics.median(values)


def display(metric, value):
    if metric == "jain_fairness":
        return f"{value:.3f}"
    if metric == "slo_violations":
        return f"{value:.1f}"
    return f"{value:.1f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    data = Path(args.data)
    metadata = json.loads((data / "metadata.json").read_text())
    assert metadata["status"] == "completed"
    assert metadata["source_commit"] == SOURCE_COMMIT
    assert tuple(metadata["seeds"]) == SEEDS
    assert metadata["repetitions"] == REPETITIONS
    assert metadata["scale"] == 8
    assert metadata["plan_sha256"] == digest((ROOT / "research/third-wave/control-comparison/plan.md").read_bytes())
    assert metadata["runner_sha256"] == digest((ROOT / "scripts/third-wave-controls.py").read_bytes())
    assert len(metadata["cells"]) == 72
    assert {path.name for path in (data / "raw").glob("*.json.gz")} == {
        cell["result_file"] for cell in metadata["cells"]
    }
    observations = {}
    by_arm = defaultdict(list)
    for cell in metadata["cells"]:
        compressed = (data / "raw" / cell["result_file"]).read_bytes()
        assert digest(compressed) == cell["sha256"]
        result = json.loads(gzip.decompress(compressed))
        assert result["environment"]["build_sha"] == metadata["source_commit"]
        assert result["environment"]["hostname"] == "research-host"
        assert len(result["samples"]) == result["manifest"]["tasks"]
        assert result["manifest"]["name"] == cell["workload"]
        assert result["variant"]["name"] == cell["variant"]
        assert result["seed"] == cell["seed"]
        summary = result["summary"]
        values = {
            "p99_ms": summary["completion"]["p99"] / 1_000_000,
            "throughput_per_second": summary["throughput_per_second"],
            "jain_fairness": summary["jain_fairness"],
            "slo_violations": summary["starvation_slo_violations"],
        }
        key = (cell["workload"], cell["seed"], cell["repetition"], cell["variant"])
        assert key not in observations
        assert cell["result_file"] == "--".join(map(str, (
            cell["workload"], cell["variant"], cell["seed"], cell["repetition"]
        ))) + ".json.gz"
        observations[key] = values
        by_arm[(cell["workload"], cell["variant"])].append(values)

    expected = {
        (workload, seed, repetition, variant)
        for workload, ablation in CASES.items()
        for seed in metadata["seeds"]
        for repetition in range(metadata["repetitions"])
        for variant in ("taskforge-fifo-static", "taskforge-full", ablation)
    }
    assert set(observations) == expected
    arm_rows = []
    contrast_rows = []
    for workload, ablation in CASES.items():
        for variant in ("taskforge-fifo-static", "taskforge-full", ablation):
            rows = by_arm[(workload, variant)]
            arm_rows.append({"workload": workload, "variant": variant, "blocks": len(rows),
                             "median": {metric: median([row[metric] for row in rows]) for metric in METRICS}})
        for comparison in ("taskforge-fifo-static", ablation):
            deltas = {metric: [] for metric in METRICS}
            for seed in metadata["seeds"]:
                for repetition in range(metadata["repetitions"]):
                    left = observations[(workload, seed, repetition, "taskforge-full")]
                    right = observations[(workload, seed, repetition, comparison)]
                    for metric in METRICS:
                        deltas[metric].append(left[metric] - right[metric])
            contrast_rows.append({
                "workload": workload, "contrast": "full minus " + comparison,
                "blocks": len(next(iter(deltas.values()))),
                "median_difference": {metric: median(values) for metric, values in deltas.items()},
                "observed_range": {metric: [min(values), max(values)] for metric, values in deltas.items()},
            })
    result = {"source_commit": metadata["source_commit"], "method": "six paired seed/repetition blocks per workload; descriptive medians and observed ranges", "arms": arm_rows, "contrasts": contrast_rows}
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    (output / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = [
        "# Latest-state overload-control comparison", "",
        "All arms use TaskForge commit `" + metadata["source_commit"][:7] + "`. Values are medians of six runs on one host; they are descriptive, not significance tests.", "",
        "## Arm medians", "",
        "| Workload | Arm | p99 completion (ms) | Throughput (tasks/s) | Jain equality | SLO violations |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in arm_rows:
        values = row["median"]
        lines.append("| " + row["workload"] + " | " + row["variant"] + " | " + " | ".join(display(metric, values[metric]) for metric in METRICS) + " |")
    lines += ["", "## Paired contrasts", "", "Differences are full controls minus the named arm within the same seed and repetition. Negative p99 and violation differences are favorable; positive throughput and Jain differences are favorable.", "", "| Workload | Contrast | p99 difference (ms) | Throughput difference (tasks/s) | Jain difference | Violation difference |", "| --- | --- | ---: | ---: | ---: | ---: |"]
    for row in contrast_rows:
        values = row["median_difference"]
        lines.append("| " + row["workload"] + " | " + row["contrast"] + " | " + " | ".join("%+.1f" % values[metric] if metric != "jain_fairness" else "%+.3f" % values[metric] for metric in METRICS) + " |")
    lines += ["", "Observed paired ranges are in `analysis.json`. This short closed-loop harness does not model dependency collapse or support a production SLA claim.", ""]
    (output / "analysis.md").write_text("\n".join(lines))
    print(output / "analysis.json")


if __name__ == "__main__":
    main()
