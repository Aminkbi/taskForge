#!/usr/bin/env python3
"""Run the fixed latest-state TaskForge control comparisons."""

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
REVISION = "0cc8a56"
SEEDS = (20260923, 20260924)
REPETITIONS = 3
SCALE = 8
CASES = (
    ("noisy-neighbor", "taskforge-no-fairness"),
    ("delayed-backlog", "taskforge-no-admission"),
    ("hot-dependency", "taskforge-no-dependency-budget"),
    ("retry-storm", "taskforge-no-adaptive"),
)
COMMON_ARMS = ("taskforge-fifo-static", "taskforge-full")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def run(*args, cwd=ROOT, env=None):
    return subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, check=True).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-container", required=True)
    parser.add_argument("--redis-addr", default="localhost:6379")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    destination = Path(args.output).resolve()
    destination.mkdir(parents=True, exist_ok=False)
    raw = destination / "raw"
    raw.mkdir()
    revision = run("git", "rev-parse", REVISION)
    archive = subprocess.check_output(("git", "archive", revision), cwd=ROOT)
    metadata = {
        "status": "running",
        "source_commit": revision,
        "source_tree": run("git", "rev-parse", revision + "^{tree}"),
        "source_archive_sha256": sha(archive),
        "plan_sha256": sha((ROOT / "research/third-wave/control-comparison/plan.md").read_bytes()),
        "runner_sha256": sha(Path(__file__).read_bytes()),
        "go_version": run("go", "version"),
        "host": {"os": platform.system(), "architecture": platform.machine(), "logical_cpus": os.cpu_count()},
        "redis_image": run("docker", "inspect", "--format", "{{.Image}}", args.redis_container),
        "redis_topology": "dedicated standalone Redis 7.4 container over loopback; persistence disabled",
        "redis_addr": args.redis_addr,
        "redis_db": 14,
        "scale": SCALE,
        "seeds": SEEDS,
        "repetitions": REPETITIONS,
        "cells": [],
    }

    def save():
        (destination / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    save()
    environment = dict(os.environ, GOCACHE="/tmp/taskforge-gocache-controls", TASKFORGE_BUILD_SHA=revision)
    environment.pop("GOFLAGS", None)
    with tempfile.TemporaryDirectory(prefix="taskforge-controls-") as temporary:
        source = Path(temporary) / "source"
        source.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(source, filter="data")
        binary = Path(temporary) / "experiment"
        run("go", "-C", "research", "build", "-trimpath", "-buildvcs=false", "-o", str(binary), "./cmd/experiment", cwd=source, env=environment)
        metadata["binary_sha256"] = sha(binary.read_bytes())
        save()
        for workload, ablation in CASES:
            for seed in SEEDS:
                for repetition in range(REPETITIONS):
                    variants = list(COMMON_ARMS + (ablation,))
                    random.Random(seed * 100 + repetition).shuffle(variants)
                    for position, variant in enumerate(variants):
                        cell_dir = Path(temporary) / "cell"
                        cell_dir.mkdir(exist_ok=True)
                        command = (
                            str(binary), "-manifest", workload, "-variant", variant,
                            "-seed", str(seed), "-scale", str(SCALE), "-compact",
                            "-hostname-label", "research-host", "-redis-addr", args.redis_addr,
                            "-redis-db", "14", "-output", str(cell_dir),
                        )
                        print(f"{workload} seed={seed} repetition={repetition} arm={variant}", flush=True)
                        result = subprocess.run(command, cwd=source / "research", env=environment, capture_output=True, text=True)
                        if result.returncode:
                            metadata["status"] = "failed"
                            metadata["failure"] = {"workload": workload, "seed": seed, "repetition": repetition, "variant": variant, "stderr": result.stderr[-3000:]}
                            save()
                            raise SystemExit(f"cell failed: {workload}/{variant}/{seed}/{repetition}")
                        filename = f"{workload}--{variant}--{seed}.json"
                        path = cell_dir / filename
                        data = path.read_bytes()
                        observation = json.loads(data)
                        assert observation["schema"] == "taskforge-experiment/v2"
                        assert observation["manifest"]["name"] == workload
                        assert observation["variant"]["name"] == variant
                        assert observation["seed"] == seed
                        assert observation["environment"]["build_sha"] == revision
                        assert observation["environment"]["hostname"] == "research-host"
                        assert len(observation["samples"]) == observation["manifest"]["tasks"]
                        archive_name = f"{workload}--{variant}--{seed}--{repetition}.json.gz"
                        compressed = gzip.compress(data, mtime=0)
                        (raw / archive_name).write_bytes(compressed)
                        metadata["cells"].append({
                            "workload": workload, "seed": seed, "repetition": repetition,
                            "variant": variant, "position": position, "result_file": archive_name,
                            "sha256": sha(compressed),
                        })
                        path.unlink()
                        save()
    assert len(metadata["cells"]) == len(CASES) * len(SEEDS) * REPETITIONS * 3
    metadata["status"] = "completed"
    save()
    print(f"completed {len(metadata['cells'])} cells at {revision}", flush=True)


if __name__ == "__main__":
    main()
